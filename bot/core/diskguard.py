"""
diskguard.py — Disk-space Guardian for Animebot VPS Deployments
================================================================

PURPOSE
-------
Prevents the bot from filling the VPS root partition during downloads and
encodes — the failure mode that caused MongoDB write errors, pyrofork
session corruption, sshd login lockouts, and crashes of co-tenant bots
sharing the same VPS disk.

Symptom in production:
    1. A batch torrent (12+ episodes, 1080p BDRip) downloads ~25 GB to
       ./downloads/batch/.
    2. The "Hdri" pseudo-encode is `-c:v copy` so it produces a near-
       source-size output (~1.5 GB) per episode beside the source.
    3. Per-quality outputs accumulate before each upload finishes.
    4. Disk hits 100% → MongoDB writes fail → pyrofork sessions cannot
       checkpoint → other services (other bots, sshd) cannot write logs.
    5. SSH access to the VPS is lost until the operator clears space via
       the provider's web console.

This module adds a lightweight pre-flight check at every disk-pressure
entry point (download start, encode start, periodic cleanup) so the bot
fails LOUDLY and EARLY instead of taking the whole VPS down.

INTEGRATION POINTS
------------------
This module is imported by:

    bot/core/tordownload.py
        - Pre-download: assert_disk_free_or_skip() before aria2c spawn

    bot/core/ffencoder.py
        - Pre-encode: assert_disk_for_encode() based on source file size

    bot/__main__.py
        - auto_cleanup(): aggressive_cleanup() when usage > AGGRESSIVE_PCT

    bot/__init__.py
        - Startup: log_disk_snapshot() so the operator sees baseline

CONFIGURATION
-------------
All thresholds are read from environment variables (via getenv) so they
can be tuned per-VPS without code changes:

    DG_MIN_FREE_GB           5    Minimum free GB to allow a new download
    DG_ENCODE_HEADROOM_X     3    Free space required = source_size * X
    DG_AGGRESSIVE_PCT        85   At/above this fill %, aggressive cleanup
    DG_CRITICAL_PCT          92   At/above this fill %, refuse all new work
    DG_WAIT_TIMEOUT_SEC      300  Max seconds wait_for_disk() will block
    DG_CHECK_INTERVAL_SEC    15   Polling interval inside wait loop

PLATFORM NOTES
--------------
shutil.disk_usage() works on Linux, macOS, and Windows. The aggressive
cleanup uses standard os.walk + os.remove which work everywhere. There
are no /proc dependencies in this module.
"""

from __future__ import annotations

import os
import shutil
import time
from asyncio import sleep as asleep
from os import getenv, path as ospath

from bot import LOGS


# ─────────────────────────────────────────────────────────────────────────────
# Configuration — all thresholds environment-overridable
# ─────────────────────────────────────────────────────────────────────────────

def _envi(key: str, default: int) -> int:
    """Parse an int env var, fall back silently to default on bad value."""
    try:
        v = getenv(key)
        return int(v) if v is not None and str(v).strip() else default
    except (ValueError, TypeError):
        return default


# Minimum free GB on the work partition to allow starting a NEW download.
# Below this, the new download is refused and the task is re-queued.
MIN_FREE_GB = _envi("DG_MIN_FREE_GB", 5)

# Free-space multiplier required for an encode: free_bytes >= source_size * X.
# x264 + Opus output is usually 0.3-0.7x source, so 3x leaves room for the
# source + new output + temp prog file + small safety margin.
ENCODE_HEADROOM_X = _envi("DG_ENCODE_HEADROOM_X", 3)

# Fill % at/above which auto_cleanup runs in aggressive mode (deletes
# everything inside downloads/, encode/, thumbs/, torrents/ regardless
# of mtime). Only triggered by the periodic scheduler, not by encodes
# in flight (those use assert_disk_for_encode instead).
AGGRESSIVE_PCT = _envi("DG_AGGRESSIVE_PCT", 85)

# Fill % at/above which the bot refuses to start any new download or
# encode, even if MIN_FREE_GB / ENCODE_HEADROOM_X look fine in absolute
# terms. Stops the cascade BEFORE the disk hits 100%.
CRITICAL_PCT = _envi("DG_CRITICAL_PCT", 92)

WAIT_TIMEOUT_SEC = _envi("DG_WAIT_TIMEOUT_SEC", 300)
CHECK_INTERVAL_SEC = _envi("DG_CHECK_INTERVAL_SEC", 15)


# ─────────────────────────────────────────────────────────────────────────────
# Observation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _disk_usage(path: str = ".") -> tuple[int, int, int]:
    """
    Return (total, used, free) in BYTES for the filesystem containing path.

    Falls back to "." if path does not exist (e.g. a download dir that hasn't
    been created yet). Never raises — returns (0, 0, 0) on any error so the
    caller can decide whether to treat that as "fail-open" or "fail-safe".
    """
    try:
        target = path if ospath.exists(path) else "."
        u = shutil.disk_usage(target)
        return u.total, u.used, u.free
    except Exception as e:
        LOGS.warning(f"diskguard: shutil.disk_usage({path!r}) failed: {e}")
        return 0, 0, 0


def get_free_gb(path: str = ".") -> float:
    """Free GB on the filesystem containing path (0.0 on error)."""
    _, _, free = _disk_usage(path)
    return free / (1024 ** 3)


def get_used_pct(path: str = ".") -> float:
    """Percentage of the filesystem used (0.0–100.0). Returns 0 on error."""
    total, used, _ = _disk_usage(path)
    if total <= 0:
        return 0.0
    return (used / total) * 100.0


def get_disk_snapshot(path: str = ".") -> dict:
    """Diagnostic snapshot suitable for logging or /status output."""
    total, used, free = _disk_usage(path)
    return {
        "path": path,
        "total_gb": round(total / (1024 ** 3), 2),
        "used_gb": round(used / (1024 ** 3), 2),
        "free_gb": round(free / (1024 ** 3), 2),
        "used_pct": round((used / total) * 100.0, 1) if total else 0.0,
    }


def log_disk_snapshot(prefix: str = "💾 Disk", path: str = ".") -> None:
    """One-line LOGS.info dump of current disk state."""
    s = get_disk_snapshot(path)
    LOGS.info(
        f"{prefix}: {s['used_gb']}GB / {s['total_gb']}GB used "
        f"({s['used_pct']}%, {s['free_gb']}GB free) on {s['path']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight assertions
# ─────────────────────────────────────────────────────────────────────────────

def is_disk_critical(path: str = ".") -> bool:
    """True if used % >= CRITICAL_PCT — refuse all new work."""
    return get_used_pct(path) >= CRITICAL_PCT


def is_disk_aggressive(path: str = ".") -> bool:
    """True if used % >= AGGRESSIVE_PCT — periodic cleanup goes nuclear."""
    return get_used_pct(path) >= AGGRESSIVE_PCT


def assert_disk_free_or_skip(path: str = ".", label: str = "download") -> tuple[bool, str]:
    """
    Pre-flight check before starting a new download.

    Returns (ok, reason). If ok is False, the caller should skip / re-queue
    the task. Reason is a short human-readable explanation suitable for
    LOGS.warning and rep.report.
    """
    if is_disk_critical(path):
        snap = get_disk_snapshot(path)
        return False, (
            f"disk critical ({snap['used_pct']}% used, only "
            f"{snap['free_gb']}GB free) — refusing new {label}"
        )

    free_gb = get_free_gb(path)
    if free_gb < MIN_FREE_GB:
        return False, (
            f"only {free_gb:.1f}GB free (< MIN_FREE_GB={MIN_FREE_GB}GB) — "
            f"refusing new {label}"
        )

    return True, "ok"


def assert_disk_for_encode(source_path: str, label: str = "encode") -> tuple[bool, str]:
    """
    Pre-flight check before starting an FFmpeg encode.

    Required free space = max(MIN_FREE_GB * 1024^3, source_size * ENCODE_HEADROOM_X).
    The headroom multiplier covers: source kept on disk + output + small
    temp prog file + safety margin against accidental near-source-size
    output (e.g. when -c:v copy slips through).

    Returns (ok, reason). On False, the caller should skip the encode and
    re-queue the task as 'pending' so cleanup has a chance to free space.
    """
    if is_disk_critical(source_path):
        snap = get_disk_snapshot(source_path)
        return False, (
            f"disk critical ({snap['used_pct']}% used, only "
            f"{snap['free_gb']}GB free) — refusing new {label}"
        )

    try:
        src_size = ospath.getsize(source_path) if ospath.exists(source_path) else 0
    except OSError:
        src_size = 0

    needed = max(MIN_FREE_GB * (1024 ** 3), src_size * ENCODE_HEADROOM_X)
    _, _, free = _disk_usage(source_path)

    if free < needed:
        return False, (
            f"need {needed / (1024**3):.1f}GB free for {label} of "
            f"{src_size / (1024**3):.2f}GB source, only "
            f"{free / (1024**3):.1f}GB available"
        )

    return True, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# Async waiting — used by callers that prefer to block briefly rather than
# fail immediately (e.g. a small encode that might fit after the current
# upload finishes and frees its file).
# ─────────────────────────────────────────────────────────────────────────────

async def wait_for_disk(
    min_gb: float = None,
    timeout: int = None,
    label: str = "encode",
    path: str = ".",
) -> bool:
    """
    Block (asyncio-style) until at least min_gb of free space is available
    on path's filesystem, or until timeout seconds elapse. Returns True on
    success, False if the timeout fires.

    NOTE: This is a *cooperative* wait. It does NOT actively delete files
    — it just polls. If you want eager cleanup while waiting, run
    aggressive_cleanup() before calling this.
    """
    target = float(min_gb) if min_gb is not None else float(MIN_FREE_GB)
    deadline = time.monotonic() + (timeout if timeout is not None else WAIT_TIMEOUT_SEC)

    while True:
        free = get_free_gb(path)
        if free >= target:
            return True
        if time.monotonic() >= deadline:
            LOGS.warning(
                f"⏱ wait_for_disk({label}): timeout after {WAIT_TIMEOUT_SEC}s — "
                f"only {free:.1f}GB free, need {target:.1f}GB"
            )
            return False

        LOGS.info(
            f"⏳ wait_for_disk({label}): {free:.1f}GB free, "
            f"need {target:.1f}GB — re-checking in {CHECK_INTERVAL_SEC}s"
        )
        await asleep(CHECK_INTERVAL_SEC)


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup helpers — called by bot/__main__.py auto_cleanup
# ─────────────────────────────────────────────────────────────────────────────

# Folders the bot writes to and is responsible for cleaning. Anything in
# these directories is fair game for stale-file removal.
WORK_FOLDERS = ("downloads", "encode", "thumbs", "torrents")


def cleanup_stale(stale_age_sec: int) -> tuple[int, int]:
    """
    Walk WORK_FOLDERS bottom-up and remove files older than stale_age_sec
    plus any empty subdirectories left behind.

    Returns (files_removed, dirs_removed). Never raises — individual
    failures are logged at WARNING level and skipped.
    """
    now = time.time()
    files_removed = 0
    dirs_removed = 0

    for folder in WORK_FOLDERS:
        if not ospath.isdir(folder):
            continue
        for dirpath, _, filenames in os.walk(folder, topdown=False):
            for fname in filenames:
                fpath = ospath.join(dirpath, fname)
                try:
                    if (now - ospath.getmtime(fpath)) > stale_age_sec:
                        os.remove(fpath)
                        files_removed += 1
                except Exception as e:
                    LOGS.warning(f"diskguard.cleanup_stale: skip {fpath}: {e}")
            # Don't remove the top-level folder itself, only sub-dirs
            if dirpath != folder:
                try:
                    if not os.listdir(dirpath):
                        os.rmdir(dirpath)
                        dirs_removed += 1
                except Exception:
                    pass

    return files_removed, dirs_removed


def aggressive_cleanup() -> tuple[int, int]:
    """
    Last-resort cleanup when disk usage exceeds AGGRESSIVE_PCT.

    Removes EVERYTHING inside WORK_FOLDERS regardless of mtime. This is
    safe because:
      - downloads/, encode/, torrents/ are scratch space — files there
        are either being processed (in which case the running encode/
        upload will fail and re-queue) or already abandoned.
      - thumbs/ are regenerated on demand from URLs in DB.

    Active encodes will fail with "file not found" and the pipeline's
    retry/re-queue logic kicks in — far better than the alternative
    (MongoDB write failure → bot crash → SSH lockout).

    Returns (files_removed, dirs_removed).
    """
    files_removed = 0
    dirs_removed = 0
    LOGS.warning(
        "🧨 aggressive_cleanup: disk usage above threshold — "
        "wiping all scratch folders regardless of file age"
    )

    for folder in WORK_FOLDERS:
        if not ospath.isdir(folder):
            continue
        for dirpath, _, filenames in os.walk(folder, topdown=False):
            for fname in filenames:
                fpath = ospath.join(dirpath, fname)
                try:
                    os.remove(fpath)
                    files_removed += 1
                except Exception as e:
                    LOGS.warning(f"diskguard.aggressive_cleanup: skip {fpath}: {e}")
            if dirpath != folder:
                try:
                    os.rmdir(dirpath)
                    dirs_removed += 1
                except Exception:
                    pass

    return files_removed, dirs_removed
