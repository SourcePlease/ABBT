from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
from json import loads as jloads
from re import findall
from math import floor
from os import path as ospath
from time import time
# FIX #2: Removed blocking `sleep` import entirely.
# All FloodWait handlers now use `await asleep(...)` to avoid freezing the event loop.
from traceback import format_exc
from asyncio import sleep as asleep, create_subprocess_shell, gather as agather
from asyncio.subprocess import PIPE
from base64 import urlsafe_b64encode, urlsafe_b64decode

from aiohttp import ClientSession
from aiofiles import open as aiopen
from aioshutil import rmtree as aiormtree
from html_telegraph_poster import TelegraphPoster
from feedparser import parse as feedparse
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import InlineKeyboardButton
from pyrogram.errors import (
    MessageNotModified, FloodWait, UserNotParticipant, ReplyMarkupInvalid,
    MessageIdInvalid, UserIsBlocked, PeerIdInvalid, InputUserDeactivated,
    ChatWriteForbidden,
)

from bot import bot, bot_loop, LOGS, Var
from .reporter import rep

# One shared executor for the entire process lifetime.
# Fixed at 4 workers — these tasks are all I/O-bound (feedparser, aiohttp)
# and spend >95% of their time waiting on network/disk, not burning CPU.
# Using cpu_count()*N here inflates to 8-16 threads on a VPS and competes
# with ffmpeg for CPU time, contributing to the OOM problem.
_executor = ThreadPoolExecutor(max_workers=4)


def handle_logs(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception:
            await rep.report(format_exc(), "error")
    return wrapper


async def sync_to_async(func, *args, wait=True, **kwargs):
    # FIX #18: reuse module-level _executor instead of creating a new one every call.
    pfunc = partial(func, *args, **kwargs)
    future = bot_loop.run_in_executor(_executor, pfunc)
    return await future if wait else future


def new_task(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return bot_loop.create_task(func(*args, **kwargs))
    return wrapper


async def getfeed(link, index=0):
    try:
        feed = await sync_to_async(feedparse, link)
        return feed.entries[index]
    except IndexError:
        return None
    except Exception as e:
        LOGS.error(format_exc())
        return None


@handle_logs
async def aio_urldownload(link):
    async with ClientSession() as sess:
        async with sess.get(link) as data:
            image = await data.read()
    path = f"thumbs/{link.split('/')[-1]}"
    # FIX #11: `".jpg" or ".png"` short-circuits to just ".jpg" (truthy string),
    # so .png URLs always had ".jpg" appended, corrupting the filename.
    # Use a proper tuple so both extensions are checked.
    if not path.endswith((".jpg", ".png")):
        path += ".jpg"
    async with aiopen(path, "wb") as f:
        await f.write(image)
    return path


@handle_logs
async def get_telegraph(out):
    client = TelegraphPoster(use_api=True)
    client.create_api_token("Mediainfo")
    uname = Var.BRAND_UNAME.lstrip('@')
    page = client.post(
        title="Mediainfo",
        author=uname,
        author_url=f"https://t.me/{uname}",
        text=f"""<pre>
{out}
</pre>
""",
    )
    return page.get("url")


async def sendMessage(chat, text, buttons=None, get_error=False, **kwargs):
    try:
        if isinstance(chat, int):
            return await bot.send_message(chat_id=chat, text=text, disable_web_page_preview=True,
                                          disable_notification=False, reply_markup=buttons, **kwargs)
        else:
            return await chat.reply(text=text, quote=True, disable_web_page_preview=True,
                                    disable_notification=False, reply_markup=buttons, **kwargs)
    except FloodWait as f:
        await rep.report(f, "warning")
        # FIX #2: was blocking sleep() — freezes the entire event loop for the duration.
        await asleep(f.value * 1.2)
        return await sendMessage(chat, text, buttons, get_error, **kwargs)
    except ReplyMarkupInvalid:
        return await sendMessage(chat, text, None, get_error, **kwargs)
    except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated, ChatWriteForbidden):
        # Recipient blocked bot, deactivated account, or chat is inaccessible — silent drop.
        return None
    except Exception as e:
        await rep.report(format_exc(), "error")
        if get_error:
            raise e
        return str(e)


async def editMessage(msg, text, buttons=None, get_error=False, **kwargs):
    try:
        # Guard: sendMessage returns str(e) on failure — never try to edit a string
        if not msg or isinstance(msg, str):
            return None
        kwargs.pop('reply_markup', None)
        return await msg.edit_text(text=text, disable_web_page_preview=True,
                                   reply_markup=buttons, **kwargs)
    except FloodWait as f:
        await rep.report(f, "warning")
        # FIX #2: was blocking sleep() — freezes the entire event loop for the duration.
        await asleep(f.value * 1.2)
        return await editMessage(msg, text, buttons, get_error, **kwargs)
    except ReplyMarkupInvalid:
        return await editMessage(msg, text, None, get_error, **kwargs)
    except (MessageNotModified, MessageIdInvalid):
        pass
    except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated, ChatWriteForbidden):
        # Recipient blocked bot or chat inaccessible — silent drop.
        return None
    except Exception as e:
        await rep.report(format_exc(), "error")
        if get_error:
            raise e
        return str(e)


async def encode(string):
    return (urlsafe_b64encode(string.encode("ascii")).decode("ascii")).strip("=")


async def decode(b64_str):
    return urlsafe_b64decode((b64_str.strip("=") + "=" * (-len(b64_str.strip("=")) % 4)).encode("ascii")).decode("ascii")


# ── Force Subscription ────────────────────────────────────────────────────────

async def _get_bot_type(client) -> str:
    """Determine which bot type a client is (ongoing/completed/movie)."""
    from bot import bot as _main, batch_bot as _batch, movie_bot as _movie
    if client is _movie:
        return "movie"
    if client is _batch:
        return "completed"
    return "ongoing"


async def is_subscribed(client, user_id):
    """Enhanced force subscription checker with request mode support."""
    from bot.core.database import db

    bot_type   = await _get_bot_type(client)
    channel_ids = await db.show_channels(bot_type=bot_type)
    if not channel_ids:
        return True

    # Owner always has access
    if user_id == Var.OWNER_ID:
        return True

    for channel_id in channel_ids:
        if not await is_sub(client, user_id, channel_id):
            return False
    return True


async def is_sub(client, user_id, channel_id):
    """Check if user is subscribed to channel (with request mode support).
    Returns True (skip) if the bot itself is not a member of the channel.

    REQUEST-MODE FIX: Telegram returns ChatMemberStatus.LEFT (not a
    UserNotParticipant exception) for users who have only sent a join
    request. The previous version only consulted the request-list inside
    the UserNotParticipant branch, so request-mode users always fell
    through to `return False` and were told to "Join Channel" again even
    after their request was pending. We now treat LEFT — and any
    successful response that is not OWNER/ADMIN/MEMBER/RESTRICTED/BANNED
    — exactly the same as UserNotParticipant.
    """
    from bot.core.database import db
    from pyrogram.errors import ChannelInvalid, ChannelPrivate, ChatAdminRequired

    JOINED  = {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER}
    BLOCKED = {ChatMemberStatus.RESTRICTED, ChatMemberStatus.BANNED}

    try:
        member = await client.get_chat_member(channel_id, user_id)
        status = member.status
        if status in JOINED:
            return True
        if status in BLOCKED:
            return False
        # status is LEFT (or any other non-joined / non-blocked status)
        # — fall through to the request-mode check below.
    except UserNotParticipant:
        pass  # fall through to the request-mode check below
    except (ChannelInvalid, ChannelPrivate, ChatAdminRequired):
        # Bot is not a member of this channel or channel is invalid for this bot.
        # Skip silently — don't block the user and don't spam logs.
        return True
    except Exception as e:
        await rep.report(f"Error in is_sub for user {user_id}, channel {channel_id}: {str(e)}", "error")
        return False

    # ── Not joined → in REQUEST MODE, accept a pending join request as "in" ──
    mode = await db.get_channel_mode(channel_id)
    if mode == "on":
        has_requested = await db.req_user_exist(channel_id, user_id)
        await rep.report(
            f"🔍 Request check - User {user_id}, Channel {channel_id}, "
            f"Has requested: {has_requested}",
            "info", log=False,
        )
        return has_requested
    return False


async def get_fsubs(uid, txtargs, client=None):
    """Generate force subscription message and buttons."""
    from bot.core.database import db
    from bot import bot as _main_bot
    from pyrogram.errors import ChannelInvalid, ChannelPrivate, ChatAdminRequired
    _client = client or _main_bot

    txt = "<b><i>🔒 You must join our channels to access files!</i></b>\n\n"
    btns = []
    bot_type    = await _get_bot_type(_client)
    channel_ids = await db.show_channels(bot_type=bot_type)

    for no, chat_id in enumerate(channel_ids, start=1):
        try:
            chat = await _client.get_chat(chat_id)
        except (ChannelInvalid, ChannelPrivate, ChatAdminRequired):
            # This bot is not a member of this channel — skip it entirely
            continue
        except Exception as e:
            await rep.report(f"Error processing channel {chat_id}: {str(e)}", "error")
            continue

        try:
            mode = await db.get_channel_mode(chat_id)

            # REQUEST-MODE FIX: get_chat_member returning successfully does
            # NOT mean the user has joined — Telegram returns status=LEFT
            # for users who have only sent a join request. The previous
            # version unconditionally set sta="✅ Joined" on any successful
            # response, hiding the real state and forever showing the
            # "Request to Join" button (or worse, falsely marking them as
            # joined). We now inspect member.status explicitly.
            try:
                member = await _client.get_chat_member(chat_id=chat_id, user_id=uid)
                member_status = member.status
            except UserNotParticipant:
                member_status = None  # explicitly: user is not in channel
            except Exception as err:
                await rep.report(f"Error checking membership for {chat_id}: {str(err)}", "error")
                member_status = "__error__"

            if member_status == "__error__":
                sta = "❌ Error"
            elif member_status in {ChatMemberStatus.OWNER,
                                   ChatMemberStatus.ADMINISTRATOR,
                                   ChatMemberStatus.MEMBER}:
                sta = "✅ Joined"
            elif member_status in {ChatMemberStatus.RESTRICTED,
                                   ChatMemberStatus.BANNED}:
                # User can't act on a join button — don't render one.
                sta = "🚫 Restricted"
            else:
                # member_status is LEFT or None → user is NOT in the channel.
                if mode == "on":
                    has_requested = await db.req_user_exist(chat_id, uid)
                    sta = "⏳ Request Sent" if has_requested else "❌ Not Requested"
                else:
                    sta = "❌ Not Joined"

                # REQUEST-MODE FIX: ask db for a link cached for THIS mode
                # only. A link previously created with creates_join_request=
                # False (e.g. when the channel was first added in NORMAL
                # mode) makes Telegram show "Join Channel" instead of
                # "Apply to Join Channel" even after the admin toggles
                # the channel to REQUEST MODE — see screenshots in PR #N.
                # Passing the current mode forces regeneration when needed.
                link = await db.get_invite_link(chat_id, expected_mode=mode)
                if not link:
                    try:
                        if mode == "on":
                            # REQUEST MODE: even for public channels we MUST
                            # create a request-style invite link, because
                            # https://t.me/<username> always lets users
                            # join directly and bypasses approval.
                            invite = await _client.create_chat_invite_link(
                                chat_id=chat_id, creates_join_request=True
                            )
                            link = invite.invite_link
                            await db.store_invite_link(chat_id, link, mode="on")
                        else:
                            if chat.username:
                                link = f"https://t.me/{chat.username}"
                                await db.store_invite_link(chat_id, link, mode="off")
                            else:
                                invite = await _client.create_chat_invite_link(chat_id)
                                link = invite.invite_link
                                await db.store_invite_link(chat_id, link, mode="off")
                    except Exception as e:
                        await rep.report(f"Error creating invite link for {chat_id}: {str(e)}", "error")
                        link = f"https://t.me/c/{str(chat_id)[4:]}"

                # Only show a join button when the user can still act on it.
                # "⏳ Request Sent" already means they did their part.
                if sta not in ("✅ Joined", "⏳ Request Sent"):
                    button_text = "📝 Request to Join" if mode == "on" else "🔗 Join Channel"
                    btns.append([InlineKeyboardButton(f"{button_text} - {chat.title}", url=link)])

            mode_text = "REQUEST MODE" if mode == "on" else "NORMAL MODE"
            txt += f"<b>{no}. {chat.title}</b>\n"
            txt += f"   • <b>Status:</b> <code>{sta}</code>\n"
            txt += f"   • <b>Mode:</b> <code>{mode_text}</code>\n\n"

        except Exception as err:
            await rep.report(f"Error processing channel {chat_id}: {str(err)}", "error")
            continue

    btns.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh_fsub")])
    if len(txtargs) > 1 and txtargs[1]:
        btns.append([InlineKeyboardButton('✅ Done! Get My Files', url=f'https://t.me/{(await _client.get_me()).username}?start={txtargs[1]}')])

    return txt, btns


async def is_fsubbed(uid, client=None):
    """Legacy function — redirects to enhanced version."""
    from bot import bot as _main_bot
    return await is_subscribed(client or _main_bot, uid)


async def mediainfo(file, get_json=False, get_duration=False):
    """
    Get media info for a file.
    Tries mediainfo first, falls back to ffprobe (always available with ffmpeg).
    get_duration=True  → returns float seconds (used by FFEncoder progress bar)
    get_json=True      → returns telegraph URL with full media info
    """
    from asyncio.subprocess import create_subprocess_exec

    # ── Fast path: duration via ffprobe (always available, no mediainfo needed) ─
    if get_duration:
        try:
            process = await create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_entries", "format=duration",
                file,
                stdout=PIPE, stderr=PIPE
            )
            stdout, _ = await process.communicate()
            return float(jloads(stdout.decode())["format"]["duration"])
        except Exception:
            return 1440.0  # 24 min fallback — progress bar still works

    # ── Full info path: try mediainfo, fall back to ffprobe ──────────────────
    try:
        process = await create_subprocess_exec(
            "mediainfo", file, "--Output=HTML",
            stdout=PIPE, stderr=PIPE
        )
        stdout, _ = await process.communicate()
        if process.returncode == 0 and stdout.strip():
            return await get_telegraph(stdout.decode())
    except FileNotFoundError:
        pass  # mediainfo not installed — fall through to ffprobe
    except Exception:
        await rep.report(format_exc(), "error")

    # ffprobe fallback for full info
    try:
        process = await create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams", "-show_format",
            file,
            stdout=PIPE, stderr=PIPE
        )
        stdout, _ = await process.communicate()
        if process.returncode == 0:
            return await get_telegraph(stdout.decode())
    except Exception:
        await rep.report(format_exc(), "error")

    return ""


async def clean_up():
    # FIX #3: Original was `(await aiormtree(d) for d in ...)` — a generator
    # expression that is never iterated, so absolutely nothing was ever deleted.
    # Fixed by awaiting each removal explicitly, wrapped in individual try/except
    # so one missing directory doesn't abort the others.
    async def _rm(d):
        try:
            await aiormtree(d)
        except Exception:
            pass

    try:
        await agather(_rm("downloads"), _rm("thumbs"), _rm("encode"))
    except Exception as e:
        LOGS.error(str(e))


def convertTime(s: int) -> str:
    m, s = divmod(int(s), 60)
    hr, m = divmod(m, 60)
    days, hr = divmod(hr, 24)
    convertedTime = (f"{int(days)}d, " if days else "") + \
                    (f"{int(hr)}h, " if hr else "") + \
                    (f"{int(m)}m, " if m else "") + \
                    (f"{int(s)}s, " if s else "")
    return convertedTime[:-2]


def convertBytes(sz) -> str:
    if not sz:
        return ""
    sz = int(sz)
    ind = 0
    Units = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P'}
    while sz > 2**10:
        sz /= 2**10
        ind += 1
    return f"{round(sz, 2)} {Units[ind]}B"
