"""
memguard.py — Memory Guardian for Animebot VPS Deployments
==========================================================

PURPOSE
-------
Prevents Linux OOM-killer from shutting down the VPS during FFmpeg encoding.
On a typical deployment (8 GB RAM + 4 GB swap, 4 vCPU), a single episode
encode cycle (Hdri → 1080p → 720p → 480p) can exhaust all 12 GB because:

    1. The source file (~1–1.5 GB for a 1080p MKV) stays in Linux page cache
       across all four quality encodes.
    2. Python's glibc arena retains Pyrofork (Pyrogram) MTProto upload buffers
       long after the upload finishes — gc.collect() alone doesn't release them
       back to the OS.
    3. The default kernel `vm.swappiness=60` prefers swapping application
       memory over dropping clean page cache, making things worse.
    4. FFmpeg has no built-in virtual-memory cap — a stalled or misbehaving
       encode can allocate without bound until the kernel intervenes.

This module provides **five cooperative defense layers**, each usable
independently or together:

    Layer 1 — Observation    : get_available_mb(), get_swap_free_mb(),
                               is_low_ram(), get_memory_snapshot()
    Layer 2 — Python Heap    : reclaim_memory()
    Layer 3 — Page Cache     : drop_page_cache(), drop_system_caches()
    Layer 4 — Async Gating   : wait_for_ram()
    Layer 5 — Process Limits : set_memory_limit()

INTEGRATION POINTS
------------------
This module is imported by:

    bot/core/ffencoder.py
        - Pre-encode:  reclaim_memory() + wait_for_ram() + is_low_ram()
        - Spawn:       set_memory_limit(pid) on the FFmpeg child
        - Post-encode: drop_page_cache() + reclaim_memory()

    bot/core/pipelines/ongoing.py
    bot/core/pipelines/batch.py
    bot/core/pipelines/movie.py
        - Between quality encode+upload cycles: reclaim_memory() + drop_page_cache()

    run.sh
        - Kernel tuning at startup (vm.swappiness, vm.vfs_cache_pressure,
          drop_caches) — not called from Python, but documented here for
          completeness.

PLATFORM NOTES
--------------
All functions degrade gracefully on non-Linux platforms (macOS, Windows):
    - /proc/meminfo reads return safe fallback values (9999 MB)
    - posix_fadvise() is silently skipped if unavailable
    - prlimit64() is silently skipped if unavailable
    - malloc_trim() is silently skipped if libc.so.6 is not loadable
This allows the bot to run on Windows/macOS for development while only
activating memory guards in production (Linux VPS).

CONFIGURATION
-------------
All thresholds are centralised in the `MemoryConfig` dataclass at the top
of this file.  Change them there — no need to grep through the codebase.

    MemoryConfig.MIN_RAM_ENCODE_MB      512   Minimum free RAM to start encode
    MemoryConfig.RAM_WAIT_TIMEOUT_SEC   300   Max seconds to wait for RAM
    MemoryConfig.RAM_CHECK_INTERVAL_SEC  15   Polling interval inside wait loop
    MemoryConfig.LOW_RAM_THRESHOLD_MB  1024   Below this, auto-downgrade preset
    MemoryConfig.FFMPEG_VMEM_LIMIT_GB     4   Virtual memory cap per FFmpeg process

FUTURE EXTENSIBILITY
--------------------
To add a new memory defense layer:
    1. Add a function in the appropriate section below.
    2. Add its threshold to MemoryConfig.
    3. Import and call it from the relevant pipeline module.
    4. Update the docstring above.

To adjust thresholds without modifying code:
    - Set environment variables (see MemoryConfig.__post_init__).
    - Example: MAX_ENCODE_RAM=768 in config.env → overrides MIN_RAM_ENCODE_MB.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Standard library imports — no third-party dependencies in this module.
# This is intentional: memguard must be importable even if pip packages are
# broken, since it's called very early in the encode path.
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import gc
import os
from dataclasses import dataclass, field
from typing import Optional

from bot import LOGS


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════
#
# All tuneable thresholds live here.  Environment variable overrides allow
# changing behaviour without editing code (useful for different VPS tiers).
#
# Naming convention for env vars:
#   MG_<THRESHOLD_NAME>   e.g.  MG_MIN_RAM_ENCODE_MB=768
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class MemoryConfig:
    """
    Centralised configuration for all memory guard thresholds.

    Every field has a sensible default for an 8 GB + 4 GB swap VPS.
    Each can be overridden by an environment variable prefixed with ``MG_``.

    Examples::

        # In config.env or shell:
        MG_MIN_RAM_ENCODE_MB=768        # raise the encode gate to 768 MB
        MG_FFMPEG_VMEM_LIMIT_GB=3       # tighter cap on a 4 GB VPS
        MG_RAM_WAIT_TIMEOUT_SEC=600     # wait longer before skipping quality

    Attributes
    ----------
    MIN_RAM_ENCODE_MB : int
        Minimum megabytes of *available* RAM (MemAvailable from /proc/meminfo)
        required before an FFmpeg encode process is spawned.  If the system has
        less than this, the encode gate either waits or skips the quality.
        Default: 512 MB — sufficient for x264 ``-preset ultrafast`` at 1080p
        with ``-threads 1``.

    RAM_WAIT_TIMEOUT_SEC : int
        Maximum seconds to block inside ``wait_for_ram()`` before giving up
        and returning False (which causes the quality to be skipped).
        Default: 300 (5 minutes).

    RAM_CHECK_INTERVAL_SEC : int
        How often ``wait_for_ram()`` re-checks available RAM while waiting.
        Shorter intervals are more responsive but cost a tiny bit of CPU.
        Default: 15 seconds.

    LOW_RAM_THRESHOLD_MB : int
        When available RAM drops below this, FFEncoder auto-downgrades the
        x264 preset from ``-preset fast`` (or medium/slow) to ``-preset
        ultrafast``.  Ultrafast uses ~40% less memory at the cost of ~20%
        larger output files.
        Default: 1024 MB (1 GB).

    FFMPEG_VMEM_LIMIT_GB : int
        Hard cap on virtual memory (RLIMIT_AS) applied to each FFmpeg child
        process via ``prlimit64()``.  If FFmpeg exceeds this, ``malloc()``
        returns NULL and FFmpeg exits with an error — instead of the kernel
        OOM-killing the entire VPS.
        Default: 4 GB — generous for 1080p single-threaded x264.

    NON_LINUX_FALLBACK_MB : int
        Value returned by ``get_available_mb()`` on non-Linux platforms
        (Windows, macOS).  Set high so memory gates never block during
        development.
        Default: 9999 MB.
    """

    # ── Encode gate ──────────────────────────────────────────────────────────
    MIN_RAM_ENCODE_MB: int = 512
    RAM_WAIT_TIMEOUT_SEC: int = 300
    RAM_CHECK_INTERVAL_SEC: int = 15

    # ── Preset auto-downgrade ────────────────────────────────────────────────
    LOW_RAM_THRESHOLD_MB: int = 1024

    # ── Process virtual memory cap ───────────────────────────────────────────
    FFMPEG_VMEM_LIMIT_GB: int = 4

    # ── Development fallback ─────────────────────────────────────────────────
    NON_LINUX_FALLBACK_MB: int = 9999

    def __post_init__(self) -> None:
        """
        Override any field from environment variables prefixed with ``MG_``.

        This runs automatically after the dataclass is constructed.
        Only integer fields are overridden;  non-numeric env values are
        silently ignored (logged as a warning).
        """
        _prefix = "MG_"
        for _field_name in self.__dataclass_fields__:
            _env_key = f"{_prefix}{_field_name}"
            _env_val = os.getenv(_env_key)
            if _env_val is not None:
                try:
                    setattr(self, _field_name, int(_env_val))
                    LOGS.info(
                        f"🔧 MemoryConfig: {_field_name} overridden to "
                        f"{_env_val} via ${_env_key}"
                    )
                except ValueError:
                    LOGS.warning(
                        f"⚠️ MemoryConfig: ignoring non-integer ${_env_key}="
                        f"'{_env_val}'"
                    )

    @property
    def FFMPEG_VMEM_LIMIT_BYTES(self) -> int:
        """Derived: FFMPEG_VMEM_LIMIT_GB converted to bytes for prlimit64."""
        return self.FFMPEG_VMEM_LIMIT_GB * 1024 * 1024 * 1024


# ── Module-level singleton ───────────────────────────────────────────────────
# Imported once at startup; env-var overrides take effect immediately.
# All functions in this module read from this instance.
config = MemoryConfig()

# ── Public aliases for backward compatibility ────────────────────────────────
# Other modules (ffencoder.py) import these names directly.
# If you rename config fields, keep these aliases to avoid breaking imports.
MIN_RAM_ENCODE_MB = config.MIN_RAM_ENCODE_MB
FFMPEG_VMEM_LIMIT = config.FFMPEG_VMEM_LIMIT_BYTES


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: MEMORY OBSERVATION
# ═════════════════════════════════════════════════════════════════════════════
#
# Pure-read functions that inspect the system's memory state.
# None of these have side effects — safe to call at any time.
#
# All functions return safe fallback values on non-Linux platforms so the
# bot can run on Windows/macOS during development without modification.
# ═════════════════════════════════════════════════════════════════════════════

def _read_meminfo_field(field_name: str) -> Optional[int]:
    """
    Read a single field from ``/proc/meminfo`` and return its value in kB.

    Parameters
    ----------
    field_name : str
        Exact field name as it appears in /proc/meminfo, e.g. "MemAvailable",
        "SwapFree", "MemTotal".  Must include the colon in the comparison
        (added automatically).

    Returns
    -------
    int or None
        Value in kB, or None if the field was not found or /proc/meminfo
        is not readable (non-Linux).

    Notes
    -----
    /proc/meminfo is a pseudo-file regenerated by the kernel on every read.
    Each read is essentially free (no disk I/O) and always returns the
    current state — no caching needed.

    Example /proc/meminfo line::

        MemAvailable:    3456789 kB
    """
    try:
        with open("/proc/meminfo", "r") as f:
            _target = f"{field_name}:"
            for line in f:
                if line.startswith(_target):
                    # Split "MemAvailable:    3456789 kB" → ["MemAvailable:", "3456789", "kB"]
                    parts = line.split()
                    return int(parts[1])
    except (FileNotFoundError, ValueError, IndexError, PermissionError):
        # FileNotFoundError → non-Linux (Windows, macOS)
        # ValueError/IndexError → unexpected format (shouldn't happen)
        # PermissionError → restricted container
        pass
    return None


def get_available_mb() -> int:
    """
    Return available system RAM in megabytes.

    Uses ``MemAvailable`` from ``/proc/meminfo``, which represents the amount
    of memory the kernel considers available for new allocations *without
    swapping*.  This accounts for:

        - Free memory (MemFree)
        - Reclaimable page cache (Cached + Buffers)
        - Minus: shared memory (Shmem) and dirty pages that can't be reclaimed

    This is the correct metric for OOM-prevention gating because it tells us
    how much the *kernel* thinks it can give us, not just free pages.

    Returns
    -------
    int
        Available RAM in MB.  Returns ``config.NON_LINUX_FALLBACK_MB`` (9999)
        on non-Linux platforms so gates never block during development.

    Examples
    --------
    >>> avail = get_available_mb()
    >>> if avail < 512:
    ...     print("Low memory!")
    """
    kb = _read_meminfo_field("MemAvailable")
    if kb is not None:
        return kb // 1024
    return config.NON_LINUX_FALLBACK_MB


def get_swap_free_mb() -> int:
    """
    Return free swap space in megabytes.

    Useful for logging and diagnostics.  When SwapFree approaches zero AND
    MemAvailable is also low, the next allocation will trigger the OOM killer.

    Returns
    -------
    int
        Free swap in MB.  Returns ``config.NON_LINUX_FALLBACK_MB`` on
        non-Linux.
    """
    kb = _read_meminfo_field("SwapFree")
    if kb is not None:
        return kb // 1024
    return config.NON_LINUX_FALLBACK_MB


def get_total_ram_mb() -> int:
    """
    Return total physical RAM in megabytes.

    Useful for logging baseline system capacity.

    Returns
    -------
    int
        Total RAM in MB, or ``config.NON_LINUX_FALLBACK_MB`` on non-Linux.
    """
    kb = _read_meminfo_field("MemTotal")
    if kb is not None:
        return kb // 1024
    return config.NON_LINUX_FALLBACK_MB


def get_memory_snapshot() -> dict:
    """
    Return a comprehensive snapshot of the system's memory state.

    Intended for diagnostics and periodic logging (e.g., the hourly
    ``_log_memory_usage()`` scheduler in ``__main__.py``).

    Returns
    -------
    dict
        Keys: total_mb, available_mb, swap_free_mb, used_pct, is_low_ram.

    Examples
    --------
    >>> snap = get_memory_snapshot()
    >>> LOGS.info(f"RAM: {snap['available_mb']}MB free of {snap['total_mb']}MB")
    """
    total = get_total_ram_mb()
    avail = get_available_mb()
    swap  = get_swap_free_mb()

    # Avoid division by zero on non-Linux
    used_pct = round((1 - avail / total) * 100, 1) if total > 0 else 0.0

    return {
        "total_mb":     total,
        "available_mb": avail,
        "swap_free_mb": swap,
        "used_pct":     used_pct,
        "is_low_ram":   avail < config.LOW_RAM_THRESHOLD_MB,
    }


def is_low_ram(threshold_mb: Optional[int] = None) -> bool:
    """
    Quick check: is available RAM below the threshold?

    Parameters
    ----------
    threshold_mb : int, optional
        Custom threshold in MB.  Defaults to ``config.LOW_RAM_THRESHOLD_MB``
        (1024 MB).

    Returns
    -------
    bool
        True if available RAM is below the threshold.

    Usage
    -----
    Called by ``FFEncoder.start_encode()`` to decide whether to auto-downgrade
    the x264 preset from ``-preset fast`` to ``-preset ultrafast``::

        if is_low_ram():
            ffcode = ffcode.replace("-preset fast", "-preset ultrafast")
    """
    _thresh = threshold_mb if threshold_mb is not None else config.LOW_RAM_THRESHOLD_MB
    return get_available_mb() < _thresh


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: PYTHON HEAP RECLAMATION
# ═════════════════════════════════════════════════════════════════════════════
#
# Python's default memory allocator (pymalloc) sits on top of glibc's malloc.
# When Python objects are freed (e.g., Pyrofork upload buffers), pymalloc
# marks the memory as free in its internal pools but does NOT return it to
# the OS.  From the kernel's perspective, the process's RSS stays the same.
#
# gc.collect() sweeps unreachable objects → pymalloc frees them internally.
# malloc_trim(0) tells glibc to return contiguous free arena pages to the OS
# via madvise(MADV_DONTNEED).
#
# Together, they can recover 500 MB–2 GB after a large file upload.
# ═════════════════════════════════════════════════════════════════════════════

def reclaim_memory() -> None:
    """
    Force Python to release memory back to the operating system.

    Two-step process:

    1. **gc.collect()** — Run a full garbage collection cycle.  This sweeps
       all unreachable Python objects (especially large byte buffers from
       Pyrofork uploads) and returns them to pymalloc's free pools.

    2. **malloc_trim(0)** — Ask glibc to scan its memory arenas and return
       any contiguous free pages back to the OS via ``madvise(MADV_DONTNEED)``.
       This is the step that actually reduces the process's RSS as seen by
       ``htop`` and ``free -m``.

    When to call
    ------------
    - After each TgUploader.upload() completes (upload buffers freed)
    - Before starting a new FFmpeg encode (clear the decks)
    - Between quality iterations in the encode loop

    Platform behaviour
    ------------------
    - Linux: both steps execute.
    - Non-Linux: gc.collect() runs; malloc_trim is silently skipped.

    Performance
    -----------
    gc.collect() is fast (~1–5 ms for typical heap sizes).
    malloc_trim(0) can take 10–100 ms on a large heap.  This is acceptable
    between encode+upload cycles (which take minutes) but should NOT be
    called in a tight loop.
    """
    # Step 1: sweep unreachable Python objects
    gc.collect()

    # Step 2: return freed glibc arena pages to the OS
    try:
        _libc = ctypes.cdll.LoadLibrary("libc.so.6")
        _libc.malloc_trim(0)
    except (OSError, AttributeError):
        # OSError → non-Linux (libc.so.6 not found)
        # AttributeError → libc loaded but malloc_trim not exported (musl libc)
        pass


async def areclaim_memory() -> None:
    """
    Async-safe wrapper around ``reclaim_memory()``.

    The synchronous ``reclaim_memory()`` calls ``malloc_trim(0)`` directly,
    which is a blocking syscall.  After a 1080p / batch / movie encode the
    glibc arenas can hold 1–4 GB of fragmented free space, and walking them
    can freeze the event loop for **5–30 minutes** — long enough that the
    subsequent upload never starts and the bot appears hung.

    Always ``await`` this helper from inside a coroutine instead of calling
    ``reclaim_memory()`` directly between encode → upload steps.
    """
    from asyncio import get_event_loop
    await get_event_loop().run_in_executor(None, reclaim_memory)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: PAGE CACHE MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════
#
# Linux aggressively caches file contents in RAM (page cache).  When FFmpeg
# reads a 1.5 GB source file, those pages stay in cache even after FFmpeg is
# done.  The kernel considers cached pages "reclaimable" but with default
# swappiness=60, it often prefers to swap out APPLICATION memory first.
#
# Explicitly evicting files from cache via posix_fadvise(DONTNEED) tells the
# kernel "I'm done with this file, you can reclaim its pages immediately."
# This is more targeted than drop_caches and doesn't affect other processes.
# ═════════════════════════════════════════════════════════════════════════════

def drop_page_cache(filepath: str) -> bool:
    """
    Evict a single file from the Linux page cache.

    Uses ``posix_fadvise(fd, 0, size, POSIX_FADV_DONTNEED)`` to advise the
    kernel that the application no longer needs the file's cached pages.
    The kernel marks them as "not needed" and will reclaim them before
    swapping out application memory.

    Parameters
    ----------
    filepath : str
        Absolute or relative path to the file to evict.  Must exist and be
        readable.  Typically the source MKV file after encoding a quality.

    Returns
    -------
    bool
        True if the eviction was performed, False if skipped (non-Linux,
        file not found, permission denied).

    Why this matters
    ----------------
    A 1080p SubsPlease episode is ~1.3 GB.  Without eviction, this stays in
    page cache across all 4 quality encodes:

        Hdri encode → 1.3 GB cached
        1080p encode → still 1.3 GB cached (cumulative)
        720p encode → still 1.3 GB cached
        480p encode → still 1.3 GB cached

    With eviction after each encode, the 1.3 GB is freed before the next
    quality starts.

    Notes
    -----
    - ``posix_fadvise`` is advisory — the kernel may ignore it under memory
      pressure.  In practice it's highly effective.
    - This does NOT delete the file — it only removes its *cached copy*
      from RAM.  The file is still on disk.
    - Safe to call on any file, even if it's currently open by FFmpeg
      (though you should call it AFTER FFmpeg finishes).
    """
    try:
        fd = os.open(filepath, os.O_RDONLY)
        try:
            size = os.fstat(fd).st_size
            # POSIX_FADV_DONTNEED = 4 (defined in <fcntl.h>)
            # This is a constant on all Linux kernels ≥ 2.5.60.
            _POSIX_FADV_DONTNEED = 4
            os.posix_fadvise(fd, 0, size, _POSIX_FADV_DONTNEED)
            LOGS.info(
                f"🧹 Dropped page cache: {os.path.basename(filepath)} "
                f"({size // 1024 // 1024} MB)"
            )
            return True
        finally:
            os.close(fd)
    except AttributeError:
        # posix_fadvise not available (Windows, macOS)
        return False
    except FileNotFoundError:
        # File already deleted (e.g., by TgUploader.upload())
        return False
    except OSError as e:
        # Permission denied, disk error, etc.
        LOGS.warning(f"⚠️ drop_page_cache failed for {filepath}: {e}")
        return False


def drop_system_caches() -> bool:
    """
    Ask the kernel to drop ALL clean page/dentry/inode caches system-wide.

    Equivalent to::

        sync && echo 3 > /proc/sys/vm/drop_caches

    This is a **heavy operation** — it affects ALL processes, not just the
    bot.  Use sparingly:

    - ✅ Once at startup (run.sh already does this)
    - ✅ As a last resort before a critical encode when RAM is very low
    - ❌ NOT between every quality — use ``drop_page_cache()`` instead

    Parameters
    ----------
    None

    Returns
    -------
    bool
        True if caches were dropped, False if the operation failed (not root,
        not Linux, restricted container).

    Prerequisites
    -------------
    Requires root (``CAP_SYS_ADMIN``) to write to ``/proc/sys/vm/drop_caches``.
    Most VPS deployments run the bot as root, so this typically works.
    Docker containers may need ``--privileged`` or the capability explicitly
    granted.

    How it works
    ------------
    ``echo 3 > /proc/sys/vm/drop_caches`` tells the kernel to:

        1 = free page cache
        2 = free dentry/inode cache
        3 = free both (1 + 2)

    ``sync`` is called first to flush dirty pages to disk — dropping dirty
    pages would cause data loss.
    """
    try:
        # Flush dirty pages to disk first (safety requirement)
        os.sync()

        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")

        LOGS.info("🧹 Dropped ALL system caches via /proc/sys/vm/drop_caches")
        return True
    except PermissionError:
        LOGS.warning(
            "⚠️ drop_system_caches: permission denied — "
            "run as root or grant CAP_SYS_ADMIN"
        )
        return False
    except FileNotFoundError:
        # Not Linux — /proc/sys/vm/drop_caches doesn't exist
        return False
    except OSError as e:
        LOGS.warning(f"⚠️ drop_system_caches failed: {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: ASYNC RAM GATING
# ═════════════════════════════════════════════════════════════════════════════
#
# The async gate is the primary defense against OOM during encoding.
# Instead of blindly starting an FFmpeg process when RAM is low, we
# wait for memory to free up (e.g., from a completing upload or the
# kernel reclaiming page cache), or skip the quality entirely.
#
# Flow:
#   1. Check available RAM
#   2. If sufficient → return True immediately
#   3. If insufficient → try reclaim_memory() + optional drop_system_caches()
#   4. Poll every RAM_CHECK_INTERVAL_SEC seconds
#   5. If RAM frees up within RAM_WAIT_TIMEOUT_SEC → return True
#   6. If timeout → return False (caller should skip this quality)
# ═════════════════════════════════════════════════════════════════════════════

async def wait_for_ram(
    min_mb: Optional[int] = None,
    timeout: Optional[int] = None,
    label: str = "",
) -> bool:
    """
    Async gate: wait until at least ``min_mb`` MB of RAM is available.

    This is the primary OOM-prevention mechanism.  Called by
    ``FFEncoder.start_encode()`` before spawning an FFmpeg process.

    Parameters
    ----------
    min_mb : int, optional
        Minimum available RAM in MB.  Defaults to ``config.MIN_RAM_ENCODE_MB``
        (512 MB).

    timeout : int, optional
        Maximum seconds to wait.  Defaults to ``config.RAM_WAIT_TIMEOUT_SEC``
        (300 seconds / 5 minutes).

    label : str, optional
        Human-readable label for log messages, e.g. "1080p Episode_03.mkv".
        Makes it easy to correlate log entries with specific encode tasks.

    Returns
    -------
    bool
        True if RAM became available within the timeout.
        False if the timeout was reached — the caller should skip this
        quality and continue to the next one.

    Side effects
    ------------
    - Calls ``reclaim_memory()`` on each iteration (gc + malloc_trim)
    - Logs warnings every ``RAM_CHECK_INTERVAL_SEC`` seconds while waiting
    - Logs an error if the timeout is reached

    Example
    -------
    ::

        ram_ok = await wait_for_ram(min_mb=512, label="1080p My_Anime_E01")
        if not ram_ok:
            LOGS.error("Skipping 1080p encode — not enough RAM")
            return None
    """
    # ── Resolve defaults from config ─────────────────────────────────────────
    _min  = min_mb if min_mb is not None else config.MIN_RAM_ENCODE_MB
    _tout = timeout if timeout is not None else config.RAM_WAIT_TIMEOUT_SEC
    _intv = config.RAM_CHECK_INTERVAL_SEC

    # ── Format label for log messages ────────────────────────────────────────
    _label_suffix = f" [{label}]" if label else ""

    # ── Fast path: enough RAM already available ──────────────────────────────
    avail = get_available_mb()
    if avail >= _min:
        return True

    # ── Slow path: wait for RAM to free up ───────────────────────────────────
    LOGS.warning(
        f"⚠️ Low RAM: {avail} MB available, need {_min} MB{_label_suffix}. "
        f"Waiting up to {_tout}s..."
    )

    elapsed = 0
    while elapsed < _tout:
        # Try to reclaim memory while waiting — this may free Pyrofork
        # upload buffers or other Python heap allocations.
        reclaim_memory()

        await asyncio.sleep(_intv)
        elapsed += _intv

        avail = get_available_mb()
        if avail >= _min:
            LOGS.info(
                f"✅ RAM freed: {avail} MB available — "
                f"proceeding with encode{_label_suffix}"
            )
            return True

        # Log progress so the admin can see what's happening in real time
        LOGS.warning(
            f"⏳ Waiting for RAM: {avail} MB / {_min} MB needed "
            f"({elapsed}s / {_tout}s){_label_suffix}"
        )

    # ── Timeout reached — not enough RAM ─────────────────────────────────────
    final_avail = get_available_mb()
    swap_free   = get_swap_free_mb()
    LOGS.error(
        f"❌ RAM timeout after {_tout}s: only {final_avail} MB available "
        f"(swap free: {swap_free} MB) — cannot safely start "
        f"encode{_label_suffix}"
    )
    return False


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: PROCESS MEMORY LIMITING
# ═════════════════════════════════════════════════════════════════════════════
#
# Even with the async gate, a misbehaving FFmpeg encode (e.g., corrupted
# input causing infinite loop, or an unexpected codec path) could allocate
# memory without bound.  Setting RLIMIT_AS (Address Space limit) on the
# FFmpeg child process provides a hard safety net:
#
#   - If FFmpeg's total virtual memory exceeds the limit, malloc() returns
#     NULL, and FFmpeg prints an error and exits with a non-zero code.
#   - This is infinitely preferable to the kernel OOM-killing the entire
#     VPS (which also kills the bot, MongoDB, sshd, etc.).
#
# We use prlimit64() syscall via ctypes because:
#   1. It can set limits on an EXISTING process (by PID) — we call it
#      right after subprocess_exec spawns FFmpeg.
#   2. No subprocess overhead (vs. running `prlimit` binary).
#   3. No dependency on the `resource` module (which can only set limits
#      on the CURRENT process, not children).
# ═════════════════════════════════════════════════════════════════════════════

def set_memory_limit(
    pid: int,
    limit_bytes: Optional[int] = None,
) -> bool:
    """
    Set the virtual memory (address space) limit for an existing process.

    Uses the Linux ``prlimit64()`` syscall via ctypes to set ``RLIMIT_AS``
    (address space limit) on the given PID.

    Parameters
    ----------
    pid : int
        Process ID of the FFmpeg child process (from ``proc.pid``).
        Must be a child of the current process (security requirement).

    limit_bytes : int, optional
        Maximum virtual memory in bytes.  Defaults to
        ``config.FFMPEG_VMEM_LIMIT_BYTES`` (4 GB).

    Returns
    -------
    bool
        True if the limit was set successfully.
        False if prlimit64 failed (logged as warning, non-fatal).

    How it works
    ------------
    ``prlimit64(pid, RLIMIT_AS, &new_limit, NULL)`` sets both the soft and
    hard limits to the specified value.  FFmpeg (as a child process) cannot
    raise its own hard limit above what we set.

    Why RLIMIT_AS and not RLIMIT_RSS?
    ----------------------------------
    ``RLIMIT_RSS`` (resident set size) is **not enforced** by modern Linux
    kernels — it's advisory only.  ``RLIMIT_AS`` (address space / virtual
    memory) IS enforced: malloc() returns NULL when the limit is hit.

    Notes
    -----
    - Only the parent process (or root) can set limits on a child.
    - The limit applies to the entire address space, including shared
      libraries and thread stacks.  4 GB is generous enough that normal
      x264 encoding never hits it.
    - If FFmpeg hits the limit, it logs an error to stderr (captured by
      ``FFEncoder.start_encode()``) and exits with a non-zero code, which
      the pipeline handles as a normal encode failure.
    """
    _limit = limit_bytes if limit_bytes is not None else config.FFMPEG_VMEM_LIMIT_BYTES

    try:
        # ── Define the rlimit64 struct ───────────────────────────────────────
        # Matches: struct rlimit { rlim_t rlim_cur; rlim_t rlim_max; };
        # rlim_t is unsigned 64-bit on 64-bit Linux.
        class _rlimit(ctypes.Structure):
            _fields_ = [
                ("rlim_cur", ctypes.c_ulonglong),  # soft limit
                ("rlim_max", ctypes.c_ulonglong),  # hard limit
            ]

        # ── Load libc ────────────────────────────────────────────────────────
        _lib_name = ctypes.util.find_library("c")
        if not _lib_name:
            LOGS.warning(
                f"⚠️ set_memory_limit: libc not found — "
                f"cannot set limit for PID {pid}"
            )
            return False

        _libc = ctypes.CDLL(_lib_name, use_errno=True)

        # ── Set RLIMIT_AS (resource number 9 on Linux) ───────────────────────
        # RLIMIT_AS = 9 is defined in <sys/resource.h> and is stable across
        # all Linux kernel versions ≥ 2.6.
        _RLIMIT_AS = 9

        _new = _rlimit(_limit, _limit)
        _ret = _libc.prlimit64(
            pid,           # target process
            _RLIMIT_AS,    # which resource to limit
            ctypes.byref(_new),  # new limits
            None,          # old limits (NULL = don't read)
        )

        if _ret == 0:
            LOGS.info(
                f"🔒 Memory limit set: PID {pid} → "
                f"{_limit // 1024 // 1024} MB (RLIMIT_AS)"
            )
            return True
        else:
            _errno = ctypes.get_errno()
            # Common error codes:
            #   ESRCH (3)  = process doesn't exist (already exited)
            #   EPERM (1)  = permission denied (not parent or not root)
            #   EINVAL (22) = invalid resource number or limit value
            LOGS.warning(
                f"⚠️ prlimit64 failed for PID {pid}: "
                f"errno={_errno} (non-fatal, encode continues without cap)"
            )
            return False

    except Exception as e:
        # Catch-all for any ctypes weirdness — never crash the encode path
        LOGS.warning(
            f"⚠️ set_memory_limit failed for PID {pid}: {e} "
            f"(non-fatal, encode continues without cap)"
        )
        return False
