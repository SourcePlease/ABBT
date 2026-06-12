from time import time
from asyncio import sleep as asleep, get_event_loop
from traceback import format_exc
from math import floor
from os import path as ospath
import gc
import ctypes

from aiofiles.os import remove as aioremove
from pyrogram.errors import FloodWait

from bot import bot, Var, LOGS
from .func_utils import editMessage, sendMessage, convertBytes, convertTime
from .reporter import rep


def _trim_heap():
    """
    Force glibc to return unused arena memory to the OS.

    Python's pymalloc holds onto freed memory in its own arena pool and never
    calls free() back to the OS. After uploading a large file (e.g. a 1-2GB
    BDRip episode), Pyrofork's MTProto send buffers are freed into Python's
    heap but the OS sees no reduction in RSS. Over a 12-episode batch this
    accumulates to 6-7GB of phantom RSS.

    gc.collect()        — sweep unreachable Python objects into the free pool
    malloc_trim(0)      — tell glibc to release contiguous free arena pages
                          back to the OS via madvise(MADV_DONTNEED)

    This drops RSS from ~6-7GB back to ~130MB between episodes, keeping peak
    RAM usage per episode at ~750-900MB instead of accumulating indefinitely.
    """
    gc.collect()
    try:
        ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
    except Exception:
        pass  # non-Linux or stripped libc — skip silently


class TgUploader:
    def __init__(self, message, upload_bot=None, file_store=None):
        self.cancelled  = False
        self.message    = message
        self.__name     = ""
        self.__display  = ""   # clean anime title for progress bar
        self.__qual     = ""
        self.__client  = upload_bot if upload_bot else bot
        self.__file_store = file_store if file_store else Var.FILE_STORE
        self.__start   = time()
        self.__updater = time()

    def set_display_name(self, name: str):
        """Set the clean anime title shown in the upload progress bar."""
        self.__display = name
        return self

    async def upload(self, path, qual, caption: str = None, _flood_retries: int = 0):
        self.__name    = ospath.basename(path)
        # Always show the actual filename in the progress bar so the admin
        # can see exactly which episode/quality is being uploaded.
        self.__display = self.__name
        self.__qual = qual
        _caption = caption if caption else f"<b>{self.__name}</b>"
        try:
            if Var.AS_DOC:
                msg = await self.__client.send_document(
                    chat_id=self.__file_store,
                    document=path,
                    thumb="bot/thumb.jpg" if ospath.exists("bot/thumb.jpg") else (
                          "thumb.jpg"     if ospath.exists("thumb.jpg")     else None),
                    caption=_caption,
                    force_document=True,
                    progress=self.progress_status
                )
            else:
                msg = await self.__client.send_video(
                    chat_id=self.__file_store,
                    video=path,
                    thumb="bot/thumb.jpg" if ospath.exists("bot/thumb.jpg") else (
                          "thumb.jpg"     if ospath.exists("thumb.jpg")     else None),
                    caption=_caption,
                    progress=self.progress_status
                )
            # File deletion happens HERE — only after a successful upload —
            # not in a finally block that runs on FloodWait, which would delete
            # the file before the retry can re-upload it.
            await aioremove(path)

            # Release Pyrofork's MTProto send buffers back to the OS.
            # malloc_trim(0) is a blocking syscall — on a 1-2GB BDRip file
            # it can walk all glibc arena pages and block the event loop for
            # 10-60 minutes (root cause of the 13:10 scheduler miss + OOM).
            # Offload to a thread executor so the event loop stays free.
            await get_event_loop().run_in_executor(None, _trim_heap)

            return msg

        except FloodWait as e:
            if _flood_retries >= 5:
                LOGS.error(
                    f"⛔ FloodWait retry limit (5) reached for {self.__name} "
                    f"[{self.__qual}] — giving up"
                )
                await rep.report(
                    f"FloodWait retry limit reached for {self.__name} — giving up",
                    "error",
                )
                raise e
            _wait = e.value * 1.5
            # FIX: Previously this handler was completely silent — no LOGS line,
            # no rep.report — so a 600-1800s FloodWait looked like the bot had
            # hung mid-upload. Now we log to BOTH the local file/console (LOGS)
            # and the Telegram log channel (rep.report) so the admin can see
            # exactly when a sleep starts, how long it is, and which retry it is.
            LOGS.warning(
                f"⏳ FloodWait on upload [{self.__qual}] {self.__name} "
                f"— Telegram asked for {int(e.value)}s, sleeping {int(_wait)}s "
                f"(retry {_flood_retries + 1}/5)"
            )
            await rep.report(
                f"⏳ FloodWait {int(e.value)}s on upload [{self.__qual}] "
                f"{self.__name} — sleeping {int(_wait)}s "
                f"(retry {_flood_retries + 1}/5)",
                "warning",
            )
            await asleep(_wait)
            LOGS.info(
                f"⏰ FloodWait sleep done — resuming upload "
                f"[{self.__qual}] {self.__name} (retry {_flood_retries + 1}/5)"
            )
            return await self.upload(path, qual, caption=caption, _flood_retries=_flood_retries + 1)

        except Exception as e:
            # Pyrogram TCPTransport / connection-drop errors are transient — retry with
            # a fresh connection. These show up as:
            #   "unable to perform operation on <TCPTransport closed=True ...>"
            #   "Value after * must be an iterable, not NoneType"
            # Both indicate the underlying MTProto session dropped mid-upload and
            # Pyrogram hasn't reconnected yet. Waiting briefly lets it recover.
            _err_str = str(e)
            _is_tcp_drop = (
                "TCPTransport" in _err_str
                or "handler is closed" in _err_str
                or ("iterable" in _err_str and "NoneType" in _err_str)
            )
            if _is_tcp_drop and _flood_retries < 5:
                _wait = 10 * (_flood_retries + 1)   # 10s, 20s, 30s, 40s, 50s
                # FIX: was log=False — TCP drops were invisible in local logs,
                # so a stuck upload looked identical to a silent FloodWait.
                # Now also write to LOGS so the admin can grep log.txt.
                LOGS.warning(
                    f"⚠️ TCP drop on upload [{self.__qual}] {self.__name} "
                    f"(retry {_flood_retries + 1}/5) — waiting {_wait}s for "
                    f"reconnect... (err={type(e).__name__}: {_err_str[:120]})"
                )
                await rep.report(
                    f"⚠️ TCP drop on upload [{self.__name}] (retry {_flood_retries + 1}/5) "
                    f"— waiting {_wait}s for reconnect...",
                    "warning",
                )
                await asleep(_wait)
                LOGS.info(
                    f"🔁 TCP reconnect wait done — retrying upload "
                    f"[{self.__qual}] {self.__name} (retry {_flood_retries + 1}/5)"
                )
                # Reset the upload timer so progress bar doesn't show stale speed
                self.__start   = time()
                self.__updater = time()
                return await self.upload(path, qual, caption=caption, _flood_retries=_flood_retries + 1)
            await rep.report(format_exc(), "error")
            raise e

    async def progress_status(self, current, total):
        if self.cancelled:
            self.__client.stop_transmission()
        now  = time()
        diff = now - self.__start
        if (now - self.__updater) >= 7 or current == total:
            self.__updater = now
            percent = round(current / total * 100, 2)
            speed   = current / diff
            eta     = round((total - current) / speed)
            bar     = floor(percent / 8) * "█" + (12 - floor(percent / 8)) * "▒"
            progress_str = (
                f"<b>ᴀɴɪᴍᴇ ɴᴀᴍᴇ :</b> <b>{self.__display}</b>\n\n"
                f"<blockquote>‣ <b>sᴛᴀᴛᴜs :</b> ᴜᴘʟᴏᴀᴅɪɴɢ\n"
                f"    <code>[{bar}]</code> {percent}%</blockquote>\n"
                f"<blockquote>‣ <b>sɪᴢᴇ :</b> {convertBytes(current)} out of ~ {convertBytes(total)}\n"
                f"‣ <b>sᴘᴇᴇᴅ :</b> {convertBytes(speed)}/s\n"
                f"‣ <b>ᴛɪᴍᴇ ᴛᴏᴏᴋ :</b> {convertTime(diff)}\n"
                f"‣ <b>ᴛɪᴍᴇ ʟᴇғᴛ :</b> {convertTime(eta)}</blockquote>\n"
                f"<blockquote>‣ <b>Qᴜᴀʟɪᴛʏ:</b> "
                f"<code>{self.__qual} ({Var.QUALS.index(self.__qual) + 1 if self.__qual in Var.QUALS else '?'} / {len(Var.QUALS)})</code></blockquote>"
            )
            await editMessage(self.message, progress_str)
