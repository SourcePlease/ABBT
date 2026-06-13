"""
tordownload.py — aria2c-based torrent downloader.

Replaces the torrentp/libtorrent backend with aria2c subprocess calls.
aria2c gives explicit control over:
  - disk cache size (--disk-cache)         → caps RAM used for write buffering
  - connection limits (--max-connection-per-server)
  - seed ratio (--seed-ratio=0)            → stop seeding immediately on finish
  - file allocation (--file-allocation=none) → no pre-allocation stall

Public interface is identical to the old TorDownloader so no callers change:
    TorDownloader(base_dir, use_stable_dir=False)
    await .download(torrent, name, stat_msg, anime_name)
    .download_dir  → property
"""

import asyncio
import os
import re
import time
from math import floor
from os import path as ospath, listdir, walk, makedirs
from uuid import uuid4

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, remove as aioremove, mkdir
from aiohttp import ClientSession

from bot import LOGS
from bot.core.func_utils import handle_logs, editMessage, convertBytes
from bot.core.diskguard import assert_disk_free_or_skip, get_disk_snapshot

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm"}

# aria2c binary name — change to full path if not in $PATH
ARIA2C = "aria2c"


# Progress bar: 10 blocks wide
_BAR_LEN = 10


def _progress_bar(pct: float) -> str:
    filled = floor(pct / 10)
    return "█" * filled + "▒" * (_BAR_LEN - filled)


def _find_video_result(base_dir: str, new_entries: set) -> str | None:
    """
    Given new entries created by a torrent download, return the best path:
    - 1 subdirectory, no files → return that directory (named batch folder)
    - Multiple video files directly in base_dir → return base_dir itself
    - 1 video file → return that file
    - Mixed dirs+files → return base_dir if multiple videos found inside
    """
    dirs  = [ospath.join(base_dir, e) for e in new_entries
             if ospath.isdir(ospath.join(base_dir, e))]
    files = [ospath.join(base_dir, e) for e in new_entries
             if ospath.isfile(ospath.join(base_dir, e))]

    # Case 1: exactly 1 named subfolder, no loose files → batch in named folder
    if len(dirs) == 1 and not files:
        return dirs[0]

    # Case 2: multiple video files dumped directly into base_dir
    videos_direct = [f for f in files if ospath.splitext(f)[1].lower() in VIDEO_EXTS]
    if len(videos_direct) > 1:
        return base_dir

    # Case 3: collect all files from any subdirs + direct files
    all_files = list(files)
    for d in dirs:
        for root, _, fnames in walk(d):
            for fn in fnames:
                all_files.append(ospath.join(root, fn))

    if not all_files:
        return None
    videos = [f for f in all_files if ospath.splitext(f)[1].lower() in VIDEO_EXTS]
    pool = videos if videos else all_files

    if len(pool) > 1:
        return base_dir
    return pool[0] if pool else None


# Keep old name as alias for any code that may call it directly
_find_largest_video = _find_video_result


class TorDownloader:
    def __init__(self, base_dir="./downloads", use_stable_dir=False):
        """
        use_stable_dir=True  → download directly into base_dir (no UUID subdir).
                               Used for batch downloads where the path is already
                               a stable named directory.
        use_stable_dir=False → create a UUID subdir inside base_dir (default).
                               Used for ongoing single-episode downloads to prevent
                               concurrent workers from mixing files.
        """
        self.__torpath = "torrents/"
        if use_stable_dir:
            self.__downdir = base_dir
            makedirs(self.__downdir, exist_ok=True)
        else:
            self.__job_id  = uuid4().hex[:12]
            self.__downdir = ospath.join(base_dir, self.__job_id)
            makedirs(self.__downdir, exist_ok=True)

    @property
    def download_dir(self) -> str:
        return self.__downdir

    @handle_logs
    async def download(self, torrent, name=None, stat_msg=None, anime_name=""):
        """
        Download a torrent file or magnet link via aria2c.
        If stat_msg is provided, updates it with a live progress bar.
        """
        # ── Disk pre-flight ────────────────────────────────────────────────
        # Refuse to start if the work partition is already near-full. This
        # is the single check that prevents a runaway batch from filling /
        # and taking down MongoDB + sshd + co-tenant bots. The caller
        # treats False as "re-queue this task and try again later".
        ok, reason = assert_disk_free_or_skip(self.__downdir, label="download")
        if not ok:
            snap = get_disk_snapshot(self.__downdir)
            LOGS.warning(
                f"⛔ tordownload: skipping {name or torrent[:60]} — {reason} "
                f"(snapshot: {snap})"
            )
            if stat_msg is not None:
                try:
                    await editMessage(
                        stat_msg,
                        f" **Download Skipped — Disk Full**\n\n"
                        f"<blockquote>{reason}\n\n"
                        f"Free: {snap['free_gb']}GB / {snap['total_gb']}GB "
                        f"({snap['used_pct']}% used)</blockquote>",
                    )
                except Exception:
                    pass
            return None
        if torrent.startswith("magnet:"):
            makedirs(self.__downdir, exist_ok=True)
            entries_before = set(listdir(self.__downdir))
            await self._run_aria2c(
                torrent, is_magnet=True,
                stat_msg=stat_msg, anime_name=anime_name
            )
            entries_after = set(listdir(self.__downdir))
            new_entries   = entries_after - entries_before
            all_entries   = entries_after
            if new_entries:
                r = _find_video_result(self.__downdir, new_entries)
                if r:
                    return r
            if all_entries:
                r = _find_video_result(self.__downdir, all_entries)
                if r:
                    return r
            return ospath.join(self.__downdir, name) if name else None

        elif torfile := await self.get_torfile(torrent):
            makedirs(self.__downdir, exist_ok=True)
            entries_before = set(listdir(self.__downdir))
            await self._run_aria2c(
                torfile, is_magnet=False,
                stat_msg=stat_msg, anime_name=anime_name
            )
            try:
                await aioremove(torfile)
            except Exception:
                pass
            entries_after = set(listdir(self.__downdir))
            new_entries   = entries_after - entries_before
            all_entries   = entries_after
            if new_entries:
                r = _find_video_result(self.__downdir, new_entries)
                if r:
                    return r
            if all_entries:
                r = _find_video_result(self.__downdir, all_entries)
                if r:
                    return r
            return None
        else:
            from bot.core.reporter import rep as _rep
            await _rep.report(f"❌ get_torfile returned None for: {torrent[:80]}", "error")

    async def _run_aria2c(self, source, is_magnet, stat_msg, anime_name):
        """
        Spawn aria2c as a subprocess, stream its stdout line by line,
        parse progress, and update stat_msg every 5% or 30 seconds.

        Key aria2c flags:
          --disk-cache=64M          cap write-back buffer → controls RAM spike
          --seed-ratio=0            stop seeding immediately after download
          --file-allocation=none    no pre-allocation stall on large files
          --max-connection-per-server=4
          --split=4                 4 connections per file for speed
          --bt-stop-timeout=300     give up if no peers for 5 minutes
          --quiet=false             keep progress output flowing
          --summary-interval=5     print status every 5 seconds
        """
        makedirs(self.__downdir, exist_ok=True)

        cmd = [
            ARIA2C,
            "--dir", self.__downdir,
            "--disk-cache=16M",          # reduced from 64M — limits write buffer RAM
            "--seed-ratio=0",
            "--seed-time=0",
            "--file-allocation=none",
            "--max-connection-per-server=4",
            "--split=4",
            "--bt-stop-timeout=300",
            "--summary-interval=5",
            "--console-log-level=notice",
            "--quiet=false",
            "--no-conf=true",
            source,
        ]

        _name       = anime_name or "Downloading..."
        last_pct    = -5.0
        last_update = 0.0
        last_log    = 0.0   # Console log cadence — separate from Telegram
                            # cadence so SSH operators see download progress.
        _start_time = time.time()

        # Regex patterns for aria2c progress output:
        # Download progress: "[#abc123 1.2GiB/4.5GiB(27%) CN:4 DL:2.3MiB ETA:10m]"
        # Or the simple summary line: "27% (1.2GiB/4.5GiB)"
        _pct_re = re.compile(r'((\d+)%)')
        _dl_re = re.compile(r'DL:([0-9.]+\w+)')
        _eta_re   = re.compile(r'ETA:([^\]]+)')
        _cn_re = re.compile(r'CN:(\d+)')
        _size_re = re.compile(r'([0-9.]+\w+)/([0-9.]+\w+)')

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            async for raw_line in proc.stdout:
                line = raw_line.decode(errors="replace").strip()
                if not line:
                    continue

                # Parse percentage
                pct_m = _pct_re.search(line)
                if not pct_m:
                    continue
                #pct = float(pct_m.group(1).replace('%', ''))
                pct = float(pct_m.group(2))

                now      = time.time()
                bar      = _progress_bar(pct)
                dl_m     = _dl_re.search(line)
                eta_m    = _eta_re.search(line)
                cn_m     = _cn_re.search(line)
                size_m   = _size_re.search(line)
                speed    = dl_m.group(1)  if dl_m  else "?"
                eta      = eta_m.group(1).strip() if eta_m else "?"
                conns    = cn_m.group(1)  if cn_m  else "?"
                cur_size = size_m.group(1) if size_m else "?"
                tot_size = size_m.group(2) if size_m else "?"
                elapsed  = now - _start_time

                # ── Telegram edit (every 5% OR every 30s) ─────────────────
                if pct - last_pct >= 5.0 or (now - last_update >= 30 and pct != last_pct):
                    last_pct    = pct
                    last_update = now
                    if stat_msg:
                        txt = (
                            f"<b>📥 {_name}</b>\n\n"
                            f"<blockquote>"
                            f"<b>Status:</b> Downloading\n"
                            f"[<code>{bar}</code>] <b>{pct:.0f}%</b>\n"
                            f"<b>Size:</b> {cur_size} / {tot_size}\n"
                            f"<b>Speed:</b> {speed}  "
                            f"<b>ETA:</b> {eta}  "
                            f"<b>Conns:</b> {conns}"
                            f"</blockquote>"
                        )
                        try:
                            await editMessage(stat_msg, txt)
                        except Exception:
                            pass

                # ── Console progress line (every 30s) ─────────────────────
                # Short, grep-friendly so an SSH operator can see download
                # progress without opening Telegram. Cadence is decoupled
                # from the Telegram edit so a stalled download still shows
                # a heartbeat in the log.
                if now - last_log >= 30:
                    last_log = now
                    LOGS.info(
                        f"📥 download {_name[:60]} — "
                        f"[{bar}] {pct:.0f}% | "
                        f"size {cur_size}/{tot_size} | "
                        f"speed {speed} | "
                        f"elapsed {int(elapsed)}s | eta {eta} | "
                        f"conns {conns}"
                    )

            await proc.wait()
            # aria2c exit codes treated as success:
            #   0  = completed normally
            #   7  = download not found / already in queue
            #   13 = files already exist and are complete on disk
            if proc.returncode not in (0, 7, 13):
                from bot.core.reporter import rep as _rep
                await _rep.report(
                    f"aria2c exited with code {proc.returncode} for: {source[:80]}",
                    "warning", log=False
                )

        except FileNotFoundError:
            from bot.core.reporter import rep as _rep
            await _rep.report(
                "aria2c not found — install it with: apt install aria2",
                "error"
            )
            raise

    @handle_logs
    async def get_torfile(self, url):
        # Guard: if a local file path is accidentally passed instead of a URL,
        # return None immediately rather than crashing with InvalidUrlClientError.
        if not url.startswith("http://") and not url.startswith("https://"):
            from bot.core.reporter import rep as _rep
            await _rep.report(
                f"⚠️ get_torfile skipped — not a valid URL: {url[:80]}", "warning", log=False
            )
            return None
        if not await aiopath.isdir(self.__torpath):
            await mkdir(self.__torpath)
        tor_name = url.split('/')[-1]
        des_dir  = ospath.join(self.__torpath, tor_name)
        async with ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiopen(des_dir, 'wb') as file:
                        async for chunk in response.content.iter_any():
                            await file.write(chunk)
                    return des_dir
        return None
