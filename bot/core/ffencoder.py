# bot/core/ffencoder.py
from re import findall
from math import floor
from time import time
from os import path as ospath, makedirs
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove, rename as aiorename
from asyncio import (
    sleep as asleep,
    create_task,
    wait_for,
    TimeoutError as AioTimeoutError,
)
from asyncio.subprocess import PIPE, DEVNULL
import shlex as _shlex
import os as _os

from bot import Var, bot_loop, ffpids_cache, LOGS
from .func_utils import mediainfo, convertBytes, convertTime, editMessage
from .reporter import rep
from .memguard import (
    get_available_mb,
    wait_for_ram,
    drop_page_cache,
    reclaim_memory,
    areclaim_memory,
    set_memory_limit,
    is_low_ram,
    MIN_RAM_ENCODE_MB,
    FFMPEG_VMEM_LIMIT,
)
from .diskguard import assert_disk_for_encode, get_disk_snapshot

ffargs = {
    "Hdri": Var.FFCODE_Hdri,
    "1080": Var.FFCODE_1080,
    "720": Var.FFCODE_720,
    "480": Var.FFCODE_480,
}

# Simple monotonic counter for unique temp-file names within a process.
# No UUID needed — counter + pid is collision-safe for concurrent encodes.
_job_counter = 0


def _next_job_id() -> str:
    global _job_counter
    _job_counter += 1
    return f"{_os.getpid()}_{_job_counter}"


# Maximum wall-clock seconds a single FFmpeg encode is allowed to run.
# A 1080p episode should never take more than 2 hours. If it does, the
# process has stalled (disk full, corrupted input, deadlock) and will
# hold RAM indefinitely via the PIPE buffer. We kill it and fail fast.
_ENCODE_TIMEOUT = 7200  # 2 hours


class FFEncoder:
    def __init__(self, message, path, name, qual, output_dir=None, display_name=None):
        self.__proc = None
        self.is_cancelled = False
        self.message = message
        self.__name = name

        # display_name: clean anime title for the progress bar.
        self.__display = display_name if display_name else name
        self.__qual = qual
        self.dl_path = path

        # Temp files live inside output_dir (or "encode/" as fallback).
        # This removes the dependency on a separate encode/ scratch folder
        # when the caller supplies an output_dir (ongoing/batch/movie pipelines).
        _job = _next_job_id()
        _work_dir = output_dir if output_dir else "encode"
        makedirs(_work_dir, exist_ok=True)

        self.__prog_file = ospath.join(_work_dir, f"prog_{_job}.txt")
        self.__in_tmp = ospath.join(_work_dir, f"in_{_job}.mkv")
        self.__out_tmp = ospath.join(_work_dir, f"out_{_job}.mkv")

        # Final output also lands in _work_dir (the quality sub-directory).
        self.out_path = ospath.join(_work_dir, name)
        self.__start_time = time()
        self.__total_time = None

    async def progress(self):
        # By the time progress() runs, dl_path has already been renamed to
        # __in_tmp by start_encode() — read duration from __in_tmp.
        self.__total_time = await mediainfo(self.__in_tmp, get_duration=True)
        if not isinstance(self.__total_time, (int, float)) or self.__total_time == 0:
            self.__total_time = 1.0

        last_update = 0   # Telegram edit cadence (every 8s)
        last_log = 0      # Console log cadence (every 30s — separate so
                          # SSH operators see encode progress without
                          # spamming the log file every 8s).

        while not (self.__proc is None or self.is_cancelled):
            try:
                async with aiopen(self.__prog_file, "r") as p:
                    text = await p.read()
            except Exception:
                await asleep(2)
                continue

            if text:
                out_time_matches = findall(r"out_time_ms=(\d+)", text)
                size_matches = findall(r"total_size=(\d+)", text)
                progress_matches = findall(r"progress=(\w+)", text)

                done_ms = int(out_time_matches[-1]) if out_time_matches else 0
                size = int(size_matches[-1]) if size_matches else 0
                done_sec = done_ms / 1_000_000

                elapsed = time() - self.__start_time
                speed = size / max(elapsed, 0.01)
                percent = min(
                    round((done_sec / max(self.__total_time, 0.01)) * 100, 2),
                    99.99,
                )
                tsize = (size / done_sec * self.__total_time) if done_sec > 5 else 0
                eta = max((tsize - size) / max(speed, 0.01), 0)
                bar = "█" * floor(percent / 8) + "▒" * (12 - floor(percent / 8))

                if time() - last_update >= 8:
                    last_update = time()
                    progress_str = (
                        f"<b>ᴀɴɪᴍᴇ ɴᴀᴍᴇ :</b> <b>{self.__display}</b>\n\n"
                        f"<blockquote>‣ <b>sᴛᴀᴛᴜs :</b> ᴇɴᴄᴏᴅɪɴɢ "
                        f"<code>[{bar}]</code> {percent}%</blockquote>\n"
                        f"<blockquote>‣ <b>sɪᴢᴇ :</b> {convertBytes(size)} out of ~ {convertBytes(tsize)}\n"
                        f"‣ <b>sᴘᴇᴇᴅ :</b> {convertBytes(speed)}/s\n"
                        f"‣ <b>ᴛɪᴍᴇ ᴛᴏᴏᴋ :</b> {convertTime(elapsed)}\n"
                        f"‣ <b>ᴛɪᴍᴇ ʟᴇғᴛ :</b> {convertTime(eta)}</blockquote>\n"
                        f"<blockquote>‣ <b>Qᴜᴀʟɪᴛʏ:</b> "
                        f"<code>{self.__qual} ({Var.QUALS.index(self.__qual) + 1 if self.__qual in Var.QUALS else '?'} / {len(Var.QUALS)})</code></blockquote>"
                    )
                    await editMessage(self.message, progress_str)

                # ── Console progress line (every 30s) ─────────────────────
                if time() - last_log >= 30:
                    last_log = time()
                    LOGS.info(
                        f"📈 encode [{self.__qual}] {self.__name} — "
                        f"[{bar}] {percent}% | "
                        f"size {convertBytes(size)}/~{convertBytes(tsize)} | "
                        f"speed {convertBytes(speed)}/s | "
                        f"elapsed {convertTime(elapsed)} | eta {convertTime(eta)}"
                    )

                if progress_matches and progress_matches[-1] == "end":
                    LOGS.info(
                        f"📈 encode [{self.__qual}] {self.__name} — "
                        f"[{'█' * 12}] 100% done in {convertTime(time() - self.__start_time)}"
                    )
                    break

            await asleep(2)

    async def start_encode(self):
        # ── Pre-encode: disk-space pre-flight ─────────────────────────────────
        ok, reason = assert_disk_for_encode(self.dl_path, label=f"{self.__qual} encode")
        if not ok:
            snap = get_disk_snapshot(self.dl_path)
            LOGS.error(
                f"⛔ FFEncoder: refusing [{self.__qual}] {self.__name} — "
                f"{reason} (snapshot: {snap})"
            )
            try:
                await editMessage(
                    self.message,
                    f"<b>Encode Skipped — Disk Full</b>\n\n"
                    f"<blockquote>{reason}\n\n"
                    f"Free: {snap['free_gb']}GB / {snap['total_gb']}GB "
                    f"({snap['used_pct']}% used)</blockquote>",
                )
            except Exception:
                pass
            return None

        if ospath.exists(self.__prog_file):
            await aioremove(self.__prog_file)

        async with aiopen(self.__prog_file, "w+"):
            LOGS.info("Progress Temp Generated !")

        # ── Pre-encode: reclaim memory from previous cycle ────────────────────
        reclaim_memory()

        # ── Pre-encode: wait for enough RAM ───────────────────────────────────
        _is_copy = self.__qual == "Hdri"  # stream copy uses minimal RAM
        if not _is_copy:
            _needed = MIN_RAM_ENCODE_MB
            _avail = get_available_mb()
            if _avail < _needed:
                LOGS.warning(
                    f"⚠️ [{self.__qual}] Only {_avail}MB free — "
                    f"attempting system cache drop before encode"
                )
                from .memguard import drop_system_caches

                drop_system_caches()
                _avail = get_available_mb()

            ram_ok = await wait_for_ram(
                min_mb=_needed,
                timeout=300,
                label=f"{self.__qual} {self.__name}",
            )
            if not ram_ok:
                LOGS.error(
                    f"❌ Skipping [{self.__qual}] encode — insufficient RAM "
                    f"({get_available_mb()}MB available, need {_needed}MB)"
                )
                return None

        # Move source into work dir with a unique name
        await aiorename(self.dl_path, self.__in_tmp)

        if self.__qual not in ffargs:
            LOGS.error(
                f"FFEncoder: unknown quality '{self.__qual}' — not in ffargs. Aborting."
            )
            await aiorename(self.__in_tmp, self.dl_path)
            return None

        ffcode = ffargs[self.__qual].format(self.__in_tmp, self.__prog_file, self.__out_tmp)

        # ── Auto-downgrade preset when RAM is tight ───────────────────────────
        if not _is_copy and is_low_ram():
            for _old_preset in ("-preset fast", "-preset medium", "-preset slow"):
                if _old_preset in ffcode:
                    ffcode = ffcode.replace(_old_preset, "-preset ultrafast", 1)
                    LOGS.warning(
                        f"⚠️ [{self.__qual}] Auto-downgraded to ultrafast "
                        f"(only {get_available_mb()}MB free)"
                    )
                    break

        # Inject audio channel normalization for libopus when needed.
        if "libopus" in ffcode and "-af " not in ffcode:
            ffcode = ffcode.replace(
                "-c:a libopus",
                "-af aformat=channel_layouts='7.1|5.1|stereo|mono' -c:a libopus",
                1,
            )

        LOGS.info(f"FFCode: {ffcode}")

        # Security: split shell string into argv list and use exec (no shell interpreter).
        try:
            _ffargv = _shlex.split(ffcode)
        except ValueError:
            _ffargv = ffcode.split()  # fallback: naive split

        self.__proc = await create_subprocess_exec(
            *_ffargv,
            stdout=DEVNULL,  # was PIPE — caused unbounded RAM growth
            stderr=PIPE,     # keep stderr for error reporting only
        )
        pid = self.__proc.pid
        ffpids_cache.append(pid)

        # ── Cap ffmpeg's virtual memory to prevent system-wide OOM ────────────
        if not _is_copy:
            set_memory_limit(pid, FFMPEG_VMEM_LIMIT)

        # Run progress() concurrently with ffmpeg.
        _progress_task = create_task(self.progress())

        stderr_bytes = b""
        timed_out = False
        try:
            _, stderr_bytes = await wait_for(
                self.__proc.communicate(),
                timeout=_ENCODE_TIMEOUT,
            )
        except AioTimeoutError:
            timed_out = True
            LOGS.warning(
                f"FFmpeg [{self.__qual}] exceeded {_ENCODE_TIMEOUT}s timeout — killing."
            )
            try:
                self.__proc.kill()
                _, stderr_bytes = await self.__proc.communicate()
            except Exception:
                pass

        return_code = self.__proc.returncode
        self.__proc = None  # signal progress() to stop looping
        await _progress_task

        if pid in ffpids_cache:
            ffpids_cache.remove(pid)

        # Always restore source file to its original path, if it still exists
        if ospath.exists(self.__in_tmp):
            await aiorename(self.__in_tmp, self.dl_path)

        try:
            await aioremove(self.__prog_file)
        except Exception:
            pass

        # ── Post-encode: evict source file from page cache ────────────────────
        if ospath.exists(self.dl_path):
            drop_page_cache(self.dl_path)

        # ── Force Python heap cleanup between qualities ───────────────────────
        await areclaim_memory()

        if self.is_cancelled:
            return None

        if timed_out:
            await rep.report(
                f"❌ FFmpeg [{self.__qual}] killed after {_ENCODE_TIMEOUT // 3600}h timeout "
                f"(stalled encode): {self.__name}",
                "error",
            )
            try:
                await aioremove(self.__out_tmp)
            except Exception:
                pass
            return None

        if return_code == 0 and ospath.exists(self.__out_tmp):
            await aiorename(self.__out_tmp, self.out_path)
            return self.out_path

        stderr = (stderr_bytes or b"").decode().strip()
        await rep.report(f"FFmpeg failed [{self.__qual}]: {stderr[-300:]}", "error")
        try:
            await aioremove(self.__out_tmp)
        except Exception:
            pass
        return None

    async def cancel_encode(self):
        self.is_cancelled = True
        if self.__proc is not None:
            try:
                self.__proc.kill()
            except Exception:
                pass
