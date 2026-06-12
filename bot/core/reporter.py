from asyncio import sleep as asleep, Semaphore   # FIX #1: use async sleep, not blocking time.sleep
from pyrogram.errors import (
    FloodWait,
    UserIsBlocked,
    PeerIdInvalid,
    ChatWriteForbidden,
    ChannelInvalid,
    ChatIdInvalid,
    ChannelPrivate,
)
from bot import Var, LOGS, bot
import traceback

_LOG_SEM = Semaphore(2)

# Errors that mean "this log destination is unreachable" — recoverable
# silently, no traceback spam. Notably ChannelInvalid is what Telegram
# raises when the bot has never seen the channel (so its peer cache is
# empty), which is the most common cause of LOG_CHANNEL noise.
_UNREACHABLE_ERRORS = (
    UserIsBlocked,
    PeerIdInvalid,
    ChatWriteForbidden,
    ChannelInvalid,
    ChatIdInvalid,
    ChannelPrivate,
)


class Reporter:
    def __init__(self, client, chat_id, log):
        self.__client = client
        self.__cid = chat_id
        self.__logger = log
        # One-shot peer-cache warm-up flag: if a send fails because the
        # bot hasn't seen this peer yet, we'll try get_chat(cid) ONCE to
        # warm pyrogram's storage and then retry the send. After that
        # attempt (success or fail) we never warm again to avoid spam.
        self.__warmed = False
        # Sticky disable: after we've definitively confirmed the log
        # channel is unreachable, stop trying so we don't keep filling
        # local logs with the same warning every error.
        self.__disabled = False

    async def _warm_and_retry(self, body: str) -> bool:
        """Try to populate pyrogram's peer cache via get_chat then retry
        the send once. Returns True if the retry succeeded."""
        if self.__warmed:
            return False
        self.__warmed = True
        try:
            await self.__client.get_chat(self.__cid)
        except Exception as e:
            self.__logger.warning(
                f"Reporter: warm-up get_chat({self.__cid}) failed: {e}"
            )
            return False
        try:
            await self.__client.send_message(self.__cid, body)
            self.__logger.info(
                f"Reporter: warmed peer cache for {self.__cid} and "
                f"retried log send successfully."
            )
            return True
        except Exception as e:
            self.__logger.warning(
                f"Reporter: retry after warm-up still failed: {e}"
            )
            return False

    async def report(self, msg, log_type, log=True):
        txt = [f"[{log_type.upper()}] {msg}", log_type.lower()]

        if txt[1] == "error":
            self.__logger.error(txt[0])
        elif txt[1] == "warning":
            self.__logger.warning(txt[0])
        elif txt[1] == "critical":
            self.__logger.critical(txt[0])
        else:
            self.__logger.info(txt[0])

        if log and self.__cid != 0 and not self.__disabled:
            async with _LOG_SEM:
                body = f"{txt[0][:4096]}"
                try:
                    await self.__client.send_message(self.__cid, body)
                except FloodWait as f:
                    self.__logger.warning(str(f))
                    await asleep(f.value * 1.5)
                except _UNREACHABLE_ERRORS as e:
                    # Try a one-shot peer-cache warm-up on the very first
                    # unreachable error — handles the common case where
                    # the bot just started and pyrogram hasn't indexed
                    # the log channel yet.
                    if not self.__warmed and await self._warm_and_retry(body):
                        return
                    # Still unreachable: log a single warning and stop
                    # spamming. The admin can fix the LOG_CHANNEL config
                    # / make the bot a member, then restart.
                    self.__disabled = True
                    self.__logger.warning(
                        f"Reporter: log channel {self.__cid} unreachable "
                        f"({type(e).__name__}: {e}). "
                        f"Disabling further log forwarding for this run; "
                        f"local logs will continue to record everything."
                    )
                except Exception:
                    self.__logger.error(traceback.format_exc())

# Ongoing bot reports to log channel via main bot
rep = Reporter(bot, Var.LOG_CHANNEL, LOGS)

# Batch and movie pipelines report via their dedicated bots
# Falls back to main bot if the dedicated bot token isn't configured
from bot import batch_bot as _batch_bot, movie_bot as _movie_bot
batch_rep = Reporter(_batch_bot if _batch_bot else bot, Var.LOG_CHANNEL, LOGS)
movie_rep = Reporter(_movie_bot if _movie_bot else bot, Var.LOG_CHANNEL, LOGS)
