"""
cmds.py — Core user + admin commands (clean rewrite)
"""

from asyncio import sleep as asleep
from pyrogram import filters
from pyrogram.filters import command, private, user, forwarded
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import os
import shutil
from pyrogram import filters
from bot import bot, Var
from bot.core.func_utils import sendMessage, convertBytes
from bot.core.decorators import new_task
from bot import bot, bot_loop, Var, admin
from bot.core.database import db
from bot.core.func_utils import decode, is_fsubbed, get_fsubs, editMessage, sendMessage, new_task, convertTime
from bot.core.auto_animes import get_animes
from bot.modules.index import update_index
from bot.core.reporter import rep


# ── /start ────────────────────────────────────────────────────────────────────

@bot.on_message(command('start') & private)
@new_task
async def start_msg(client, message):
    uid = message.from_user.id
    from_user = message.from_user
    txtargs = message.text.split()

    await db.add_user(uid)

    if await db.is_banned(uid):
        return await sendMessage(message, "<b>⛔ You are banned from using this bot.</b>")

    temp = await sendMessage(message, "<b>Connecting...</b>")

    if not await is_fsubbed(uid, client):
        txt, btns = await get_fsubs(uid, txtargs, client)
        return await editMessage(temp, txt, InlineKeyboardMarkup(btns))

    if len(txtargs) <= 1:
        await temp.delete()
        btns = []
        for elem in Var.START_BUTTONS.split():
            try:
                bt, link = elem.split('|', maxsplit=1)
            except Exception:
                continue
            if btns and len(btns[-1]) == 1:
                btns[-1].insert(1, InlineKeyboardButton(bt, url=link))
            else:
                btns.append([InlineKeyboardButton(bt, url=link)])

        smsg = Var.START_MSG.format(
            first_name=from_user.first_name,
            last_name=from_user.last_name or "",
            mention=from_user.mention,
            user_id=from_user.id
        )
        if Var.START_PHOTO:
            await message.reply_photo(
                photo=Var.START_PHOTO,
                caption=smsg,
                reply_markup=InlineKeyboardMarkup(btns) if btns else None
            )
        else:
            await sendMessage(message, smsg, InlineKeyboardMarkup(btns) if btns else None)
        return

    # File retrieval via deep link
    try:
        arg = (await decode(txtargs[1])).split('-')
    except Exception as e:
        await rep.report(f"User {uid} decode error: {e}", "error")
        return await editMessage(temp, "<b>Invalid link.</b>")

    if len(arg) == 4 and arg[0] == 'get':
        # NEW compact format:  get-{abs_store}-{f_id}-{l_id}
        #   arg[1]=abs_store (~10^12), arg[2]=f_id (~10^4), arg[3]=l_id (~10^4)
        # OLD legacy format:   get-{f*s}-{l*s}-{s}
        #   arg[1]=f*s (~10^14), arg[2]=l*s (~10^14), arg[3]=s (~10^12)
        # Discriminant: in NEW, arg[3] is a small msg ID (<10^9);
        #               in OLD, arg[3] is the large store ID (>10^10). Unambiguous.
        try:
            _a1, _a2, _a3 = int(arg[1]), int(arg[2]), int(arg[3])
            if _a3 < 10**9:
                # NEW compact: get-{abs_store}-{f_id}-{l_id}
                _abs_store = _a1
                f_id       = _a2
                l_id       = _a3
            else:
                # OLD legacy: get-{f*s}-{l*s}-{s}
                _abs_store = _a3
                f_id = int(_a1 / _abs_store)
                l_id = int(_a2 / _abs_store)
            _ch = -_abs_store          # always negative for Telegram supergroups
            if not (f_id > 0 and l_id >= f_id):
                raise ValueError("Invalid message ID range")
        except Exception as _de:
            await rep.report(f"User {uid} batch decode error: {_de}", "error")
            return await editMessage(temp, "<b>Invalid batch link.</b>")

        # Helper: always deliver a visible message to the user.
        # Never delete temp without first showing the user something.
        async def _reply_err(text: str):
            try:
                await editMessage(temp, text)
            except Exception:
                try:
                    await message.reply(text)
                except Exception:
                    pass

        try:
            from bot import batch_bot as _bb, movie_bot as _mb

            ids = list(range(f_id, l_id + 1))
            await rep.report(
                f"🔍 Batch fetch: channel={_ch} ids={f_id}..{l_id} "
                f"total={len(ids)} batch_bot={'yes' if _bb else 'no'}", "info", log=False
            )

            # Pick the right fetch client based on which store the link points to
            _abs_ch = abs(_ch)
            if _abs_ch == abs(Var.MOVIE_FILE_STORE) and _mb:
                _primary_fetch = _mb
                _primary_label = "movie_bot"
            elif _abs_ch == abs(Var.BATCH_FILE_STORE) and _bb:
                _primary_fetch = _bb
                _primary_label = "batch_bot"
            else:
                _primary_fetch = client
                _primary_label = "main_bot"

            async def _fetch_with(fetch_cl, label: str):
                """Pre-resolve peer, then fetch all msgs. Returns non-empty list."""
                # Pre-resolve peer: avoids PeerIdInvalid / fresh session failures
                try:
                    await fetch_cl.get_chat(_ch)
                except Exception as _pe:
                    await rep.report(f"[{label}] get_chat({_ch}) → {_pe}", "warning", log=False)

                result = []
                for _cs in range(0, len(ids), 200):
                    _chunk = ids[_cs:_cs + 200]
                    _msgs  = await fetch_cl.get_messages(_ch, message_ids=_chunk)
                    non_empty = sum(1 for m in _msgs if not m.empty)
                    await rep.report(
                        f"[{label}] got {len(_msgs)} msgs, non-empty={non_empty}", "info", log=False
                    )
                    for _m in _msgs:
                        if not _m.empty:
                            result.append(_m)
                return result

            raw       = []
            last_err  = None

            try:
                raw = await _fetch_with(_primary_fetch, _primary_label)
                await rep.report(f"✅ {_primary_label} → {len(raw)} files", "info", log=False)
            except Exception as _be:
                last_err = _be
                await rep.report(f"{_primary_label} failed: {_be}", "warning", log=False)

            if not raw and _primary_fetch is not client:
                try:
                    raw = await _fetch_with(client, "main_bot")
                    await rep.report(f"✅ main_bot fallback → {len(raw)} files", "info", log=False)
                except Exception as _me:
                    last_err = _me
                    await rep.report(f"main_bot failed: {_me}", "error")

            if not raw:
                err_detail = f"\n<code>{last_err}</code>" if last_err else ""
                await rep.report(
                    f"⚠️ 0 files for user {uid} — ch={_ch} ids={f_id}..{l_id} err={last_err}", "error"
                )
                return await _reply_err(
                    f"<b>Files not found.</b>\n"
                    f"<i>Channel:</i> <code>{_ch}</code>  "
                    f"<i>Msgs:</i> <code>{f_id}–{l_id}</code>"
                    f"{err_detail}"
                )

            # Files retrieved — safe to delete temp now
            try:
                await temp.delete()
            except Exception:
                pass

            sent = []
            for _m in raw:
                try:
                    nm = await _m.copy(message.chat.id, reply_markup=None)
                    sent.append(nm)
                    await asleep(0.5)
                except Exception as _ce:
                    await rep.report(f"copy msg {_m.id} failed: {_ce}", "warning", log=False)
                    if "USER_IS_BLOCKED" in str(_ce):
                        break

            if not sent:
                return await message.reply(
                    "<b>Files could not be forwarded.</b>\n"
                    "<i>Check the log channel for details.</i>"
                )

            if Var.AUTO_DEL and sent:
                bot_loop.create_task(_auto_delete_batch(client, sent, message, txtargs[1]))

        except Exception as e:
            await rep.report(f"User {uid} batch unhandled error: {e}", "error")
            await _reply_err(f"<b>Error fetching files.</b>\n<code>{e}</code>")



    elif len(arg) == 3 and arg[0] == 'get':
        # 3-part links, two possible formats:
        # NEW compact:  get-{abs_store}-{msg_id}              arg[1]~10^12, arg[2]~10^4
        # OLD legacy:   get-{msg_id*abs(store)}-{abs(store)}  arg[1]~10^14, arg[2]~10^12
        # OLD batch v0: get-{first*store}-{last*store}         both large, no store embedded
        # Discriminant: in NEW, arg[2] is a small msg ID (<10^9);
        #               in OLD, arg[2] is the large store ID (>10^10). Unambiguous.
        _decoded_as_single = False
        fid = 0
        _ch  = None
        try:
            _a1, _a2 = int(arg[1]), int(arg[2])
            if _a2 < 10**9:
                # NEW compact: get-{abs_store}-{msg_id}
                _ch  = -_a1
                fid  = _a2
                _decoded_as_single = True
            else:
                # OLD legacy: arg[2] should equal abs(FILE_STORE) or abs(BATCH_FILE_STORE)
                for _store in (Var.BATCH_FILE_STORE, Var.FILE_STORE):
                    _abs = abs(int(_store))
                    if _a2 == _abs:
                        fid  = int(_a1 / _abs)
                        _ch  = -_abs
                        _decoded_as_single = True
                        break
        except Exception:
            pass

        if _decoded_as_single:
            try:
                from bot import batch_bot as _bb2, movie_bot as _mb2
                _abs_ch2 = abs(_ch)
                if _abs_ch2 == abs(Var.MOVIE_FILE_STORE) and _mb2:
                    _fetch_cl = _mb2
                elif _abs_ch2 == abs(Var.BATCH_FILE_STORE) and _bb2:
                    _fetch_cl = _bb2
                else:
                    _fetch_cl = client
                # FIX: when _fetch_cl is batch_bot/movie_bot the user may have only
                # ever spoken to the main bot, so the source channel AND the user
                # peer are not in this client's session cache yet — get_messages or
                # message.copy() will raise PEER_ID_INVALID. Warm both peers first.
                if _fetch_cl is not client:
                    try:
                        await _fetch_cl.get_chat(_ch)
                    except Exception:
                        pass
                    try:
                        await _fetch_cl.get_users(message.chat.id)
                    except Exception:
                        pass
                msg = await _fetch_cl.get_messages(_ch, message_ids=fid)
                if msg.empty:
                    return await editMessage(temp, "<b>File not found.</b>")
                nmsg = await msg.copy(message.chat.id, reply_markup=None)
                await temp.delete()
                if Var.AUTO_DEL:
                    bot_loop.create_task(_auto_delete(client, nmsg, message, txtargs[1]))
            except Exception as e:
                await rep.report(f"User {uid} file fetch error: {e}", "error")
                await editMessage(temp, "<b>File not found.</b>")
        else:
            # Old batch range format — try both stores
            _ch = None
            f_id = l_id = 0
            for _store in (Var.BATCH_FILE_STORE, Var.FILE_STORE):
                try:
                    _abs = abs(int(_store))
                    _f = int(int(arg[1]) / _abs)
                    _l = int(int(arg[2]) / _abs)
                    if _f > 0 and _l >= _f:
                        f_id, l_id, _ch = _f, _l, int(_store)
                        break
                except Exception:
                    continue
            if _ch is None:
                return await editMessage(temp, "<b>Invalid link.</b>")
            try:
                await temp.delete()
                from bot import batch_bot as _bb3, movie_bot as _mb3
                _abs_ch3 = abs(_ch)
                if _abs_ch3 == abs(Var.MOVIE_FILE_STORE) and _mb3:
                    _fetch_cl2 = _mb3
                elif _abs_ch3 == abs(Var.BATCH_FILE_STORE) and _bb3:
                    _fetch_cl2 = _bb3
                else:
                    _fetch_cl2 = client
                ids = list(range(f_id, l_id + 1))
                sent = []
                # Force peer resolution before get_messages / message.copy.
                # batch_bot/movie_bot may not have the source channel OR the
                # destination user in its session cache yet — the user only ever
                # spoke to the main bot — which raises PEER_ID_INVALID on
                # get_messages (channel) or messages.SendMedia (user).
                if _fetch_cl2 is not client:
                    try:
                        await _fetch_cl2.get_chat(_ch)
                    except Exception:
                        pass
                    try:
                        await _fetch_cl2.get_users(message.chat.id)
                    except Exception:
                        pass
                for chunk_start in range(0, len(ids), 200):
                    chunk = ids[chunk_start:chunk_start + 200]
                    msgs = await _fetch_cl2.get_messages(_ch, message_ids=chunk)
                    for m in msgs:
                        if not m.empty:
                            nm = await m.copy(message.chat.id, reply_markup=None)
                            sent.append(nm)
                            await asleep(0.5)
                if Var.AUTO_DEL and sent:
                    bot_loop.create_task(_auto_delete_batch(client, sent, message, txtargs[1]))
            except Exception as e:
                await rep.report(f"User {uid} old batch fetch error: {e}", "error")
                await editMessage(message, "<b>Error fetching files.</b>")
    else:
        await editMessage(temp, "<b>Invalid link.</b>")


async def _purge_chat(client, chat_id: int, known_msg_ids: list = None):
    """
    Delete messages in bot's private chat with a user.
    Bots can only delete their OWN messages — not user messages.
    known_msg_ids: list of message IDs to delete (bot's messages).
    Also attempts to delete user messages via revoke (works in private chats).
    """
    if known_msg_ids:
        # Delete in chunks of 100
        for i in range(0, len(known_msg_ids), 100):
            chunk = known_msg_ids[i:i+100]
            try:
                await client.delete_messages(chat_id, chunk, revoke=True)
            except Exception:
                # Try one by one if bulk fails
                for mid in chunk:
                    try:
                        await client.delete_messages(chat_id, [mid], revoke=True)
                    except Exception:
                        pass


async def _auto_delete(client, file_msg, user_msg, start_arg):
    del_timer = await db.get_del_timer()
    file_name = (
        (file_msg.document and file_msg.document.file_name) or
        (file_msg.video and file_msg.video.file_name) or
        "File"
    )
    note = await sendMessage(
        user_msg,
        f"<b>⏰ {file_name} will be deleted in {convertTime(del_timer)}. Forward to Saved Messages to keep it!</b>"
    )
    await asleep(del_timer)
    # Delete: file, note, user's /start message
    msg_ids = [file_msg.id, note.id, user_msg.id]
    await _purge_chat(client, user_msg.chat.id, known_msg_ids=msg_ids)


async def _auto_delete_batch(client, file_msgs: list, user_msg, start_arg):
    """Auto-delete all files from a batch link after the timer."""
    del_timer = await db.get_del_timer()
    count = len(file_msgs)
    note = await sendMessage(
        user_msg,
        f"<b>⏰ {count} file(s) will be deleted in {convertTime(del_timer)}. "
        f"Forward to Saved Messages to keep them!</b>"
    )
    await asleep(del_timer)
    # Delete: all files, note, user's /start message
    msg_ids = [m.id for m in file_msgs] + [note.id, user_msg.id]
    await _purge_chat(client, user_msg.chat.id, known_msg_ids=msg_ids)


# ── User info helper ──────────────────────────────────────────────────────────
# /users command removed — user count is now exposed via /settings → Users
# (see bot/modules/settings.py). The function below is kept so the settings
# panel can reuse it.

async def get_users(client, message):
    """Legacy entry point kept for backward import compatibility."""
    msg = await sendMessage(message, "<b>Fetching...</b>")
    users = await db.full_userbase()
    await editMessage(msg, f"<b>👥 Total users: {len(users)}</b>")


async def get_user_count() -> int:
    """Return the total number of users known to the database."""
    users = await db.full_userbase()
    return len(users)


# ── /batch and /genlink — generate deep links for FILE_STORE messages ────────

async def _get_msg_id_from_input(client, message):
    """
    Extract message ID and channel ID from either:
    - A forwarded message from FILE_STORE or BATCH_FILE_STORE channel
    - A t.me/c/CHANNEL_ID/MSG_ID link (private channel)
    - A t.me/USERNAME/MSG_ID link (public channel)
    Returns (msg_id, channel_id) or (None, None) if invalid.
    """
    import re as _re

    # Forwarded message — channel ID comes from forward_from_chat
    if message.forward_from_chat:
        ch_id = message.forward_from_chat.id
        # Accept forwards from either FILE_STORE or BATCH_FILE_STORE
        if ch_id in (Var.FILE_STORE, Var.BATCH_FILE_STORE):
            return message.forward_from_message_id, ch_id
        return None, None

    # Link — extract channel_id and msg_id from URL
    if message.text:
        # Private channel: t.me/c/1234567890/42
        m = _re.search(r't\.me/c/(\d+)/(\d+)', message.text)
        if m:
            # Pyrogram uses negative ID with -100 prefix for channels
            ch_id = int('-100' + m.group(1))
            msg_id = int(m.group(2))
            return msg_id, ch_id
        # Public channel: t.me/username/42 — resolve via channel username
        m2 = _re.search(r't\.me/([A-Za-z][\w_]+)/(\d+)', message.text)
        if m2:
            try:
                chat = await client.get_chat(m2.group(1))
                return int(m2.group(2)), chat.id
            except Exception:
                return None, None
    return None, None


@bot.on_message(command('batch') & private & admin)
@new_task
async def batch_cmd(client, message):
    """
    Re-generate or list batch deep links from DB.

    Usage:
      /batch          — shows the 10 most recent batch links
      /batch <ani_id> — re-generates the link for a specific anime ID

    The batch pipeline auto-saves the first/last FILE_STORE message IDs when
    it finishes, so no manual forwarding is needed.
    """
    from bot.core.func_utils import encode as _encode

    args = message.text.split()
    bot_info = await client.get_me()

    async def _make_batch_link_from_db(record: dict) -> str | None:
        """Build a ?start= link from a DB batch_links record."""
        try:
            f_id = record["first_msg_id"]
            l_id = record["last_msg_id"]
            store = record["file_store"]
            b64 = await _encode(f"get-{abs(store)}-{f_id}-{l_id}")
            return f"https://telegram.me/{bot_info.username}?start={b64}"
        except Exception:
            return None

    if len(args) >= 2:
        # /batch <ani_id> — re-generate link for specific anime
        try:
            ani_id = int(args[1])
        except ValueError:
            return await sendMessage(message, "<b>Usage: /batch [ani_id]</b>")

        record = await db.get_batch_link(ani_id)
        if not record:
            return await sendMessage(
                message,
                f"<b>❌ No batch link saved for ani_id {ani_id}.</b>\n"
                f"<i>Run the batch pipeline first.</i>"
            )
        link = await _make_batch_link_from_db(record)
        if not link:
            return await sendMessage(message, "<b>❌ Failed to generate link.</b>")

        count = record["last_msg_id"] - record["first_msg_id"] + 1
        share_url = f"https://telegram.me/share/url?url={link}"
        btns = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=share_url)]])
        await sendMessage(
            message,
            f"<b>📦 Batch Link — ani_id {ani_id} ({count} file(s))</b>\n\n{link}",
            btns
        )
    else:
        # /batch — list latest 10 completed batches
        records = await db.get_latest_batch_links(limit=10)
        if not records:
            return await sendMessage(
                message,
                "<b>No batch links saved yet.</b>\n"
                "<i>Run a batch pipeline to auto-generate one.</i>"
            )

        txt = "<b>📦 Recent Batch Links</b>\n\n"
        btns_list = []
        for rec in records:
            link = await _make_batch_link_from_db(rec)
            if not link:
                continue
            count = rec["last_msg_id"] - rec["first_msg_id"] + 1
            ani_id = rec.get("ani_id", "?")
            txt += f"<b>Ani ID {ani_id}</b> — {count} file(s)\n{link}\n\n"
            share_url = f"https://telegram.me/share/url?url={link}"
            btns_list.append([InlineKeyboardButton(
                f"🔁 Share — ID {ani_id}", url=share_url
            )])

        await sendMessage(message, txt, InlineKeyboardMarkup(btns_list) if btns_list else None)


@bot.on_message(command('genlink') & private & admin)
@new_task
async def genlink_cmd(client, message):
    """
    Generate a deep link for a single file from FILE_STORE.
    Usage: /genlink  — then forward the message from FILE_STORE.
    """
    from bot.core.func_utils import encode as _encode

    while True:
        try:
            fwd_msg = await client.ask(
                chat_id=message.from_user.id,
                text="<b>Forward the message from DB Channel (with Quotes)\nor send the DB Channel post link</b>",
                filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                timeout=60
            )
        except Exception:
            return
        msg_id, msg_ch = await _get_msg_id_from_input(client, fwd_msg)
        if msg_id:
            break
        await fwd_msg.reply("❌ That message is not from the DB Channel. Try again.", quote=True)

    # FIX #3 + compact: use new get-{abs(store)}-{msg_id} format.
    # Produces ~28 b64 chars — well under Telegram's 64-char deep-link limit.
    b64        = await _encode(f"get-{abs(msg_ch)}-{msg_id}")
    bot_info   = await client.get_me()
    link       = f"https://telegram.me/{bot_info.username}?start={b64}"
    share_url  = f"https://telegram.me/share/url?url={link}"
    btns       = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL", url=share_url)]])
    await fwd_msg.reply_text(
        f"<b>✅ File Link</b>\n\n{link}",
        quote=True, reply_markup=btns
    )


# ── Fetch control ─────────────────────────────────────────────────────────────
#
# /pause, /resume, /reboot and /clearqueue were removed in favour of the
# inline /settings → 📊 Dashboard & Queue sub-menu (see bot/modules/dashboard.py).
# The underlying primitives (`ani_cache['fetch_animes']`, `db.reboot()`,
# `task_queue.clear_*_tasks()`) are still used directly from there.


# ── RSS / task management ─────────────────────────────────────────────────────
#
# /addlink, /addtask, /rtask and /addmagnet were removed in favour of the
# inline /settings → 📋 RSS & Manual Tasks sub-menu. All logic now lives in
# bot/modules/rss_tasks.py.


# ── Queue management ──────────────────────────────────────────────────────────
#
# /queue and the queue_refresh / queue_pick / queue_act / queue_clear_all_do
# callbacks were removed in favour of the inline /settings → 📊 Dashboard &
# Queue → 📋 Queue sub-panel (see bot/modules/dashboard.py). The captions,
# keyboards and scope-clear logic now live there in self-contained form.


# ── Log ───────────────────────────────────────────────────────────────────────

@bot.on_message(command('log') & private & admin)
@new_task
async def send_log(client, message):
    await message.reply_document("log.txt", quote=True)


# ── Delete timer ──────────────────────────────────────────────────────────────
#
# /dlt_time and /check_dlt_time were removed in favour of the inline
# /settings → ⏱ Auto Delete sub-menu (see bot/modules/settings.py).
# Keep the db helpers (`db.get_del_timer` / `db.set_del_timer`) — the
# settings callback uses them directly.


# ── Channel connections ───────────────────────────────────────────────────────
# Flow:
#  Step 1 — /connect [channel_id]         → bot verifies channel, asks for anime name
#  Step 2 — user sends anime name (text)  → bot shows AniList search results as buttons
#  Step 3 — user picks anime              → bot asks Ongoing / Completed / Movie
#  Step 4 — user picks type               → bot saves connection, shows upload options
#
# State is stored in DB as a pending_connection with a "step" field.
# ─────────────────────────────────────────────────────────────────────────────

_ANILIST_PAGE_SIZE = 8   # buttons shown per page
_ANILIST_FETCH_MAX = 20  # total fetched from AniList per search


async def _search_anilist_multi(query_name: str) -> list:
    """
    Search AniList for up to _ANILIST_FETCH_MAX anime matching the query.
    Returns a list of dicts: {id, title_romaji, title_english, year, format, status}
    Caller paginates using _ANILIST_PAGE_SIZE.
    """
    import aiohttp, re as _re
    QUERY = """
    query ($search: String, $perPage: Int) {
      Page(perPage: $perPage) {
        media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
          id
          title { romaji english }
          startDate { year }
          format
          status
        }
      }
    }
    """
    clean = _re.sub(r'[\[\](){}:;!@#$%^&*+=<>?/\\|`~]', ' ', query_name)
    clean = _re.sub(r'\s+', ' ', clean).strip()
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                "https://graphql.anilist.co",
                json={"query": QUERY, "variables": {"search": clean, "perPage": _ANILIST_FETCH_MAX}},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return []
                body = await resp.json(content_type=None)
                media_list = (((body or {}).get("data") or {}).get("Page") or {}).get("media") or []
                results = []
                for m in media_list:
                    titles = m.get("title") or {}
                    results.append({
                        "id":            m.get("id"),
                        "title_romaji":  titles.get("romaji") or "",
                        "title_english": titles.get("english") or "",
                        "year":          (m.get("startDate") or {}).get("year") or 0,
                        "format":        m.get("format") or "",
                        "status":        m.get("status") or "",
                    })
                return results
    except Exception:
        return []


def _build_anilist_buttons(results: list, channel_id: int, page: int = 0) -> InlineKeyboardMarkup:
    """
    Build paginated anime selection buttons.
    Shows _ANILIST_PAGE_SIZE entries per page.
    Button label uses English title only, falling back to Romaji if English is missing.
    """
    total      = len(results)
    total_pages = -(-total // _ANILIST_PAGE_SIZE)   # ceiling division
    start      = page * _ANILIST_PAGE_SIZE
    chunk      = results[start:start + _ANILIST_PAGE_SIZE]

    fmt_map = {"TV": "📺", "MOVIE": "🎬", "OVA": "📼", "ONA": "🌐", "SPECIAL": "⭐"}
    rows = []
    for r in chunk:
        # English name first; only fall back to Romaji if English is absent
        label    = r["title_english"] or r["title_romaji"]
        year_tag = f" [{r['year']}]" if r["year"] else ""
        fmt_icon = fmt_map.get(r["format"], "📺")
        btn_label = f"{fmt_icon} {label}{year_tag}"[:64]

        # Callback data must stay under 64 bytes total
        # Format: pc_pickani|{ani_id}|{channel_id}|{safe_name}
        # Fixed overhead: "pc_pickani|" (11) + "|" (1) + "|" (1) = 13
        # ani_id max 7 digits, channel_id max 13 digits = 20 more = 33 total overhead
        _cb_prefix = f"pc_pickani|{r['id']}|{channel_id}|"
        _max_name  = max(0, 64 - len(_cb_prefix.encode()))
        safe_name  = (r["title_english"] or r["title_romaji"])[:_max_name]
        rows.append([InlineKeyboardButton(
            btn_label,
            callback_data=f"{_cb_prefix}{safe_name}"
        )])

    # Pagination nav row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pc_anipg|{channel_id}|{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pc_anipg|{channel_id}|{page + 1}"))
    if nav:
        rows.append(nav)

    page_label = f"Page {page + 1}/{total_pages}" if total_pages > 1 else ""
    if page_label:
        rows.append([InlineKeyboardButton(f"📄 {page_label}", callback_data="noop")])

    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="close")])
    return InlineKeyboardMarkup(rows)


# ── STEP 1: /connect [channel_id]   (alias: /connectchannel) ────────────────
# Both /connect and /connectchannel trigger this — the help text in
# bot/modules/useless.py historically advertised /connectchannel, so we keep
# that alias to avoid a "command silently does nothing" UX bug.

# NOTE: The /connect and /connectchannel slash commands were removed.
# The Connect flow is now triggered from
#   /settings → 📺 Channel Management → 🔗 Connect
# which calls connect_step1(client, message, channel_id_str) directly.
# The function is kept as a plain helper so the existing step-2/3/4
# callback chain continues to work unchanged.
@new_task
async def connect_step1(client, message, channel_id_str: str | None = None):
    """Step 1 — Admin provides channel ID (without -100).

    `channel_id_str` is supplied by the /settings panel after the admin
    types the channel ID. When None, falls back to message.command parsing
    (kept for backwards compatibility — no current call site uses it).
    """
    if channel_id_str is None:
        cmd_args = message.command[1:] if message.command else []
        arg = (cmd_args[0] if cmd_args else "").strip()
    else:
        arg = (channel_id_str or "").strip()

    if not arg or not arg.lstrip('-').isdigit():
        return await sendMessage(
            message,
            "<b>📺 Connect a Channel — Step 1 of 2</b>\n\n"
            "<b>Send the numeric channel ID</b> (without the <code>-100</code> prefix).\n\n"
            "<i>ℹ️ Forward any channel message to @userinfobot to get the ID.</i>"
        )

    # Normalise: add -100 prefix for supergroups/channels if not already there
    raw_id = arg.lstrip('-')
    channel_id = int(f"-100{raw_id}")

    status_msg = await sendMessage(message, f"<b>🔍 Verifying channel</b> <code>{channel_id}</code>...")

    try:
        ch = await client.get_chat(channel_id)
        # Verify bot can actually post
        test = await client.send_message(channel_id, "🔗 Verifying bot access...")
        await test.delete()
    except Exception as e:
        return await editMessage(
            status_msg,
            f"<b>❌ Cannot access channel <code>{channel_id}</code></b>\n\n"
            f"<code>{e}</code>\n\n"
            f"<i>Make sure:\n"
            f"• The channel ID is correct (without -100)\n"
            f"• The bot is added as an admin with post permission</i>"
        )

    # Save pending with step=anime_name, store channel info
    await db.add_pending_connection(
        message.from_user.id, "", "",
        extra={"channel_id": channel_id, "channel_title": ch.title, "step": "anime_name"}
    )

    await editMessage(
        status_msg,
        f"<b>✅ Channel verified!</b>\n"
        f"<b>Channel:</b> {ch.title} (<code>{channel_id}</code>)\n\n"
        f"<b>📺 Step 2 of 3 — Send the anime name</b>\n"
        f"<i>Type and send the anime name you want to connect to this channel.\n"
        f"Example: <code>Attack on Titan</code></i>"
    )


# ── STEP 2: User sends anime name (text reply) ────────────────────────────────

@bot.on_message(filters.private & admin & filters.text & ~filters.command([
    "start","help","shell","eval","stats","update","downloads","fixbatchlink","restart",
    "batch","genlink","users","admins","add_admin","deladmin","ban","unban","status","set",
    "index","connect","connectchannel","listconnections","removeconnection","removechannel",
    "processbatch","pause","resume",
    "cancel","broadcast","fsub","adfsub","rmfsub","listfsub","addlink","addtask","rtask",
    "addmagnet"
]), group=2)
@new_task
async def connect_step2_anime_name(client, message):
    """Step 2 — Catch anime name text when pending step=anime_name."""
    pending = await db.get_pending_connection(message.from_user.id)
    if not pending or pending.get("extra", {}).get("step") != "anime_name":
        return

    anime_query = message.text.strip()
    extra       = pending.get("extra", {})
    channel_id  = extra.get("channel_id")

    status_msg = await sendMessage(
        message,
        f"<b>🔍 Searching AniList for:</b> <code>{anime_query}</code>..."
    )

    try:
        results = await _search_anilist_multi(anime_query)
    except Exception as e:
        await editMessage(
            status_msg,
            f"<b>❌ AniList search failed:</b> <code>{e}</code>\n\n"
            f"<i>Send the anime name again to retry.</i>"
        )
        return

    if not results:
        await editMessage(
            status_msg,
            f"<b>⚠️ No AniList results for:</b> <code>{anime_query}</code>\n\n"
            f"<i>Try a different spelling or shorter name.\n"
            f"Send another anime name to search again.</i>"
        )
        return

    # Store results in pending for pagination
    extra["search_results"] = results
    extra["search_query"]   = anime_query
    try:
        await db.add_pending_connection(message.from_user.id, "", "", extra=extra)
    except Exception as e:
        await editMessage(
            status_msg,
            f"<b>❌ Failed to save search state:</b> <code>{e}</code>\n\n"
            f"<i>Send the anime name again to retry.</i>"
        )
        return

    kb = _build_anilist_buttons(results, channel_id, page=0)
    total = len(results)
    await editMessage(
        status_msg,
        f"<b>📺 Step 2 of 3 — Pick the correct anime</b>\n"
        f"<b>Search:</b> <code>{anime_query}</code>  •  <b>{total} result(s)</b>\n\n"
        f"<i>Tap the anime that matches your channel:</i>",
        kb
    )

# ── Pagination for AniList search results ────────────────────────────────────

@bot.on_callback_query(admin & filters.regex(r"^pc_anipg\|"))
async def pc_anipg_cb(client, query):
    """Prev/Next page through AniList search results."""
    parts      = query.data.split("|")
    channel_id = int(parts[1])
    page       = int(parts[2])

    pending = await db.get_pending_connection(query.from_user.id)
    if not pending:
        await query.answer("Session expired. Run /connect again.", show_alert=True)
        return

    results     = pending.get("extra", {}).get("search_results", [])
    anime_query = pending.get("extra", {}).get("search_query", "")

    if not results:
        await query.answer("Results expired. Send the anime name again.", show_alert=True)
        return

    kb = _build_anilist_buttons(results, channel_id, page=page)
    total = len(results)
    await query.edit_message_text(
        f"<b>📺 Step 2 of 3 — Pick the correct anime</b>\n"
        f"<b>Search:</b> <code>{anime_query}</code>  •  <b>{total} result(s)</b>\n\n"
        f"<i>Tap the anime that matches your channel:</i>",
        reply_markup=kb
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^noop$"))
async def noop_cb(client, query):
    """No-op for display-only buttons like page indicators."""
    await query.answer()


# ── STEP 3a: User picks anime from AniList results ────────────────────────────

@bot.on_callback_query(admin & filters.regex(r"^pc_pickani\|"))
@new_task
async def connect_step3_pick_anime(client, query):
    """Step 3a — Anime selected; ask for type (ongoing/completed/movie)."""
    parts      = query.data.split("|")
    ani_id     = int(parts[1])
    channel_id = int(parts[2])
    safe_name  = parts[3] if len(parts) > 3 else ""

    # Fetch full title from AniList to store the correct canonical name
    import aiohttp, re as _re
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                "https://graphql.anilist.co",
                json={
                    "query": "query($id:Int){Media(id:$id,type:ANIME){id title{romaji english}}}",
                    "variables": {"id": ani_id}
                },
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                body  = await resp.json(content_type=None)
                media = ((body or {}).get("data") or {}).get("Media") or {}
                titles = media.get("title") or {}
                # Prefer English title — clean and unambiguous
                # Fall back to Romaji but strip any brackets AniList adds (e.g. [Oshi no Ko])
                eng    = titles.get("english") or ""
                romaji = titles.get("romaji") or ""
                romaji_clean = _re.sub(r'^[\[\(【]|[\]\)】]$', '', romaji).strip()
                anime_name = eng or romaji_clean or safe_name
    except Exception:
        anime_name = safe_name

    # Fetch channel title from DB pending (already stored in step 1)
    pending = await db.get_pending_connection(query.from_user.id)
    ch_title = (pending or {}).get("extra", {}).get("channel_title", "Unknown")

    # Update pending: store anime name, move to db_type step
    await db.add_pending_connection(
        query.from_user.id, anime_name, "",
        extra={"channel_id": channel_id, "channel_title": ch_title, "step": "db_type", "ani_id": ani_id}
    )

    safe = anime_name[:40]
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("📡 Ongoing  —  Airing / Weekly Episodes",   callback_data=f"pc_dbtype|{safe}|ongoing")],
        [InlineKeyboardButton("📦 Completed  —  Finished / BDRip Batch",  callback_data=f"pc_dbtype|{safe}|completed")],
        [InlineKeyboardButton("🎬 Movie  —  Single Anime Film",            callback_data=f"pc_dbtype|{safe}|movie")],
        [InlineKeyboardButton("⬅️ Back — Search Again",                    callback_data="pc_searchagain")],
    ])
    await query.edit_message_text(
        f"<b>📺 Step 3 of 3 — Select upload type</b>\n\n"
        f"<b>Anime:</b> <code>{anime_name}</code>\n"
        f"<b>Channel:</b> {ch_title}\n\n"
        f"<b>Ongoing</b> — auto-uploads new episodes from weekly RSS\n"
        f"<b>Completed</b> — uploads full season/BDRip batch from Nyaa\n"
        f"<b>Movie</b> — single anime film",
        reply_markup=btns
    )
    await query.answer()


@bot.on_callback_query(admin & filters.regex(r"^pc_searchagain$"))
async def pc_searchagain(client, query):
    """Let admin search again without restarting."""
    pending = await db.get_pending_connection(query.from_user.id)
    if not pending:
        await query.answer("Session expired. Run /connect again.", show_alert=True)
        return
    # Reset step back to anime_name
    extra = pending.get("extra", {})
    extra["step"] = "anime_name"
    await db.add_pending_connection(query.from_user.id, "", "", extra=extra)
    await query.edit_message_text(
        f"<b>📺 Step 2 of 3 — Send the anime name</b>\n"
        f"<b>Channel:</b> {extra.get('channel_title', '?')}\n\n"
        f"<i>Type and send the anime name to search again.</i>"
    )
    await query.answer()


# ── STEP 3b: Type selected → save & show upload menu ─────────────────────────

@bot.on_callback_query(admin & filters.regex(r"^pc_dbtype\|"))
async def pc_dbtype_cb(client, query):
    parts      = query.data.split("|")
    anime_name = parts[1]
    db_type    = parts[2]   # 'ongoing' | 'completed' | 'movie'

    pending = await db.get_pending_connection(query.from_user.id)
    if not pending:
        await query.answer("Session expired. Run /connect again.", show_alert=True)
        return

    extra         = pending.get("extra", {})
    channel_id    = extra.get("channel_id")
    channel_title = extra.get("channel_title", "?")
    ani_id        = extra.get("ani_id")

    if not channel_id:
        await query.answer("Channel info missing. Run /connect again.", show_alert=True)
        return

    # Auto-fetch the invite link from the dedicated channel so the
    # "▶️ Watch Now" button works without any manual setup.
    _invite_link = ""
    try:
        _chat = await bot.get_chat(channel_id)
        _invite_link = _chat.invite_link or ""
        if not _invite_link:
            _invite_link = await bot.export_chat_invite_link(channel_id)
    except Exception as _ile:
        from bot.core.reporter import rep
        await rep.report(f"Could not fetch invite link for {channel_title}: {_ile}", "warning", log=False)
    await db.add_anime_channel(anime_name, channel_id, channel_title, invite_link=_invite_link, db_type=db_type, ani_id=ani_id)
    await db.remove_pending_connection(query.from_user.id)
    await update_index()

    db_label = {"ongoing": "📡 Ongoing", "completed": "📦 Completed", "movie": "🎬 Movie"}.get(db_type, db_type)
    safe     = anime_name[:40]

    if db_type == "movie":
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Upload All Movies",     callback_data=f"pc_upmovie_all|{safe}")],
            [InlineKeyboardButton("🎞 Upload Specific Movie",  callback_data=f"pc_upmovie_pick|{safe}")],
            [InlineKeyboardButton("🔔 Just Save (Auto RSS)",   callback_data=f"pc_skip|{safe}")],
        ])
    else:
        btns = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload All Episodes",            callback_data=f"pc_upall|{safe}|{db_type}")],
            [InlineKeyboardButton("📁 Upload Specific Season",         callback_data=f"pc_upseason|{safe}")],
            [InlineKeyboardButton("🎬 Upload Specific Episode",        callback_data=f"pc_upep|{safe}")],
            [InlineKeyboardButton("🗑 Remove Connection",              callback_data=f"pc_remove|{safe}")],
            [InlineKeyboardButton("🔔 Just Save (Future Updates Only)", callback_data=f"pc_skip|{safe}")],
        ])

    await query.edit_message_text(
        f"<b>✅ Channel connected successfully!</b>\n\n"
        f"<b>Anime:</b> <code>{anime_name}</code>\n"
        f"<b>Channel:</b> {channel_title}\n"
        f"<b>Type:</b> {db_label}\n\n"
        f"<b>What would you like to do now?</b>",
        reply_markup=btns
    )
    await query.answer("✅ Connected!")


# ── Upload action callbacks ───────────────────────────────────────────────────

@bot.on_callback_query(admin & filters.regex(r"^pc_skip\|"))
async def pc_skip_cb(client, query):
    anime_name = query.data.split("|", 1)[1]
    await query.edit_message_text(
        f"<b>✅ Connection saved for:</b> <code>{anime_name}</code>\n\n"
        f"<i>Bot will auto-upload future episodes from RSS.</i>"
    )
    await query.answer()


@bot.on_callback_query(admin & filters.regex(r"^pc_upall\|"))
async def pc_upall_cb(client, query):
    from bot.modules.channel_manager import _queue_upload
    parts      = query.data.split("|")
    anime_name = parts[1]
    db_type    = parts[2] if len(parts) > 2 else "ongoing"
    await query.edit_message_text(
        f"<b>🔍 Searching for all episodes of:</b> <code>{anime_name}</code>\n"
        f"<i>This may take a moment...</i>"
    )
    await query.answer()
    await _queue_upload(query, anime_name, season=None, episode=None, mode="all", is_batch=True)


@bot.on_callback_query(admin & filters.regex(r"^pc_upseason\|"))
@new_task
async def pc_upseason_cb(client, query):
    from bot.modules.channel_manager import _get_seasons, _season_keyboard
    anime_name = query.data.split("|", 1)[1]
    await query.answer()
    seasons, is_exact, is_continuous = await _get_seasons(anime_name)
    label = f"{len(seasons)} seasons via MAL" if is_exact else "navigate to your season"
    kb = _season_keyboard(seasons, page=0,
                          cb_prefix=f"pc_upseason_do|{anime_name[:40]}",
                          cancel_cb=f"pc_skip|{anime_name}")
    await query.edit_message_text(
        f"<b>📁 Select season — <code>{anime_name}</code></b>\n<i>({label})</i>",
        reply_markup=kb
    )


@bot.on_callback_query(admin & filters.regex(r"^pc_upseason_do\|.*\|\d+$"))
async def pc_upseason_do_cb(client, query):
    from bot.modules.channel_manager import _queue_upload
    parts      = query.data.split("|")
    anime_name = parts[1]
    season     = int(parts[2])
    await query.edit_message_text(
        f"<b>🔍 Searching Season {season} of:</b> <code>{anime_name}</code>..."
    )
    await query.answer()
    await _queue_upload(query, anime_name, season=season, episode=None, mode="season", is_batch=True)


@bot.on_callback_query(admin & filters.regex(r"^pc_upseason_do\|.*_page\|\d+$"))
@new_task
async def pc_upseason_page_cb(client, query):
    from bot.modules.channel_manager import _get_seasons, _season_keyboard
    raw        = query.data
    page_part  = raw.rsplit("_page|", 1)
    page       = int(page_part[1])
    anime_name = page_part[0].split("|", 1)[1]
    seasons, is_exact, is_continuous = await _get_seasons(anime_name)
    label = f"{len(seasons)} seasons via MAL" if is_exact else "navigate to your season"
    kb = _season_keyboard(seasons, page=page,
                          cb_prefix=f"pc_upseason_do|{anime_name[:40]}",
                          cancel_cb=f"pc_skip|{anime_name}")
    await query.edit_message_text(
        f"<b>📁 Select season — <code>{anime_name}</code></b>\n<i>({label})</i>",
        reply_markup=kb
    )
    await query.answer()


@bot.on_callback_query(admin & filters.regex(r"^pc_upep\|"))
async def pc_upep_cb(client, query):
    anime_name = query.data.split("|", 1)[1]
    await db.set_pending_episode(query.from_user.id, anime_name)
    await query.edit_message_text(
        f"<b>🎬 Upload specific episode for:</b>\n<code>{anime_name}</code>\n\n"
        f"Reply with:\n"
        f"• Episode number: <code>5</code> or <code>S02E05</code>\n"
        f"• Nyaa.si URL: <code>https://nyaa.si/view/1549326</code>\n"
        f"• Magnet link"
    )
    await query.answer()


@bot.on_callback_query(admin & filters.regex(r"^pc_upmovie_all\|"))
@new_task
async def pc_upmovie_all_cb(client, query):
    from bot.core.auto_animes import get_animes
    from bot.modules.channel_manager import get_franchise_movies, _get_search_names, _search_movie_torrent
    anime_name = query.data.split("|", 1)[1]
    await query.edit_message_text(f"<b>🔍 Fetching movies for:</b> <code>{anime_name}</code>...")
    await query.answer()

    movies       = await get_franchise_movies(anime_name)
    search_names = await _get_search_names(anime_name)

    if not movies:
        await query.edit_message_text(
            f"<b>⚠️ No movies found for:</b> <code>{anime_name}</code>\n\n"
            f"<i>Try /addmagnet with direct Nyaa.si URLs.</i>"
        )
        return

    queued, lines = 0, []
    for mv in movies:
        candidates = await _search_movie_torrent(search_names, movie_title=mv["title"])
        if not candidates:
            lines.append(f"⚠️ {mv['title']} ({mv['year']}) — no torrent")
            continue
        _, t_title, t_url = candidates[0]
        bot_loop.create_task(get_animes(t_title, t_url, force=True, is_movie=True))
        queued += 1
        lines.append(f"✅ {mv['title']} ({mv['year']})")

    _lines_text = "\n".join(lines)
    await query.edit_message_text(
        f"<b>🎬 Queued {queued}/{len(movies)} movies for:</b> <code>{anime_name}</code>\n\n"
        f"<blockquote>{_lines_text}</blockquote>"
    )


@bot.on_callback_query(admin & filters.regex(r"^pc_upmovie_pick\|"))
@new_task
async def pc_upmovie_pick_cb(client, query):
    from bot.modules.channel_manager import get_franchise_movies
    anime_name = query.data.split("|", 1)[1]
    await query.edit_message_text(f"<b>🔍 Loading movies for:</b> <code>{anime_name}</code>...")
    await query.answer()
    movies = await get_franchise_movies(anime_name)
    if not movies:
        await query.edit_message_text(
            f"<b>⚠️ No movies found for:</b> <code>{anime_name}</code>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Close", callback_data="close")]])
        )
        return
    await _show_pc_movie_pick_page(query, anime_name, movies, page=0)


async def _show_pc_movie_pick_page(query, anime_name: str, movies: list, page: int):
    PAGE  = 10
    start = page * PAGE
    chunk = movies[start:start + PAGE]
    total = len(movies)
    pages = -(-total // PAGE)
    rows  = []
    for mv in chunk:
        year_tag = f" ({mv['year']})" if mv["year"] else ""
        rows.append([InlineKeyboardButton(
            f"🎬 {mv['title']}{year_tag}"[:64],
            callback_data=f"pc_upmovie_single|{anime_name[:30]}|{mv['id']}"
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pc_upmovie_page|{anime_name[:35]}|{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pc_upmovie_page|{anime_name[:35]}|{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Close", callback_data="close")])
    page_label = f"Page {page+1}/{pages}" if pages > 1 else ""
    await query.edit_message_text(
        f"<b>🎬 Select a movie — <code>{anime_name}</code></b>\n"
        f"<i>{total} movie(s){(' — ' + page_label) if page_label else ''}</i>",
        reply_markup=InlineKeyboardMarkup(rows)
    )


@bot.on_callback_query(admin & filters.regex(r"^pc_upmovie_page\|"))
@new_task
async def pc_upmovie_page_cb(client, query):
    from bot.modules.channel_manager import get_franchise_movies
    parts = query.data.split("|")
    anime_name, page = parts[1], int(parts[2])
    movies = await get_franchise_movies(anime_name)
    await query.answer()
    await _show_pc_movie_pick_page(query, anime_name, movies, page=page)


@bot.on_callback_query(admin & filters.regex(r"^pc_upmovie_single\|"))
@new_task
async def pc_upmovie_single_cb(client, query):
    from bot.core.auto_animes import get_animes
    from bot.modules.channel_manager import get_franchise_movies, _get_search_names, _search_movie_torrent
    parts      = query.data.split("|")
    anime_name = parts[1]
    ani_id     = int(parts[2])
    await query.answer("🔍 Searching...")

    movies     = await get_franchise_movies(anime_name)
    mv_record  = next((m for m in movies if m["id"] == ani_id), None)
    mv_title   = mv_record["title"] if mv_record else anime_name
    mv_year    = mv_record["year"]  if mv_record else ""

    search_names = await _get_search_names(anime_name)
    candidates   = await _search_movie_torrent(search_names, movie_title=mv_title)

    if not candidates:
        await query.edit_message_text(
            f"<b>⚠️ No torrent for:</b> <code>{mv_title}</code>\n\n"
            f"<i>Try /addmagnet with a direct Nyaa.si URL.</i>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"pc_upmovie_pick|{anime_name[:40]}")]])
        )
        return

    _, t_title, t_url = candidates[0]
    bot_loop.create_task(get_animes(t_title, t_url, force=True, is_movie=True))
    await query.edit_message_text(
        f"<b>✅ Queued:</b> <code>{mv_title}{' (' + str(mv_year) + ')' if mv_year else ''}</code>\n"
        f"<b>Torrent:</b> <code>{t_title}</code>"
    )


# ── Remove connection (with full DB data purge) ───────────────────────────────

@bot.on_callback_query(admin & filters.regex(r"^pc_remove\|"))
async def pc_remove_cb(client, query):
    """Confirm before removing from listconnections flow."""
    anime_name = query.data.split("|", 1)[1]
    btns = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Remove Everything", callback_data=f"pc_confirm_remove|{anime_name[:40]}"),
            InlineKeyboardButton("❌ Cancel",                  callback_data="close"),
        ]
    ])
    await query.edit_message_text(
        f"<b>⚠️ Remove connection for:</b>\n<code>{anime_name}</code>\n\n"
        f"<i>This will also delete all stored episode data for this anime from the database.</i>",
        reply_markup=btns
    )
    await query.answer()


@bot.on_callback_query(admin & filters.regex(r"^pc_confirm_remove\|"))
@new_task
async def pc_confirm_remove_cb(client, query):
    anime_name = query.data.split("|", 1)[1]
    success, deleted_eps = await db.remove_anime_channel_and_data(anime_name)
    if success:
        from bot.modules.index import update_index
        await update_index()
        await query.edit_message_text(
            f"<b>✅ Connection removed for:</b> <code>{anime_name}</code>\n"
            f"<b>Deleted episode records:</b> {deleted_eps}\n\n"
            f"<i>All data for this anime has been purged from the database.</i>"
        )
    else:
        await query.answer("Failed to remove. Check /listconnections.", show_alert=True)
    await query.answer()


# NOTE: The /removeconnection and /removechannel slash commands were removed.
# Removal is now triggered from
#   /settings → 📺 Channel Management → 🗑 Remove Connection
# which calls remove_connection(client, message, anime_name) directly.
@new_task
async def remove_connection(client, message, anime_name: str | None = None):
    if anime_name is None:
        cmd_args = message.command[1:] if message.command else []
        anime_name = " ".join(cmd_args).strip()
    else:
        anime_name = (anime_name or "").strip()
    if not anime_name:
        return await sendMessage(
            message,
            "<b>🗑 Remove Connection</b>\n\n"
            "<b>Send the anime name</b> to remove its channel link."
        )
    success, deleted_eps = await db.remove_anime_channel_and_data(anime_name)
    if success:
        from bot.modules.index import update_index
        await update_index()
        await sendMessage(
            message,
            f"<b>✅ Connection removed for:</b> <code>{anime_name}</code>\n"
            f"<b>Deleted episode records:</b> {deleted_eps}"
        )
    else:
        await sendMessage(
            message,
            f"<b>❌ No connection found for:</b> <code>{anime_name}</code>\n"
            f"<i>Open the Channel Management → List Connections panel to see "
            f"all linked channels.</i>"
        )



# ── Callbacks ─────────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^close$"))
async def close_cb(client, query):
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.answer()


# Assuming 'admin' and 'private' filters are defined in your cmds.py
@bot.on_message(filters.command('diskstatus') & private & admin)
@new_task
async def disk_status_cmd(client, message):
    """Shows disk usage for the downloads directory."""
    path = "./downloads"
    if not os.path.exists(path):
        path = "."
        
    try:
        usage = shutil.disk_usage(path)
        total = convertBytes(usage.total)
        used = convertBytes(usage.used)
        free = convertBytes(usage.free)
        pct = round((usage.used / usage.total) * 100, 1)
        
        text = (
            f"💾 **Disk Status**\n\n"
            f"**Total:** {total}\n"
            f"**Used:** {used} ({pct}%)\n"
            f"**Free:** {free}\n"
            f"**Path:** `{os.path.abspath(path)}`"
        )
        await sendMessage(message, text)
    except Exception as e:
        await sendMessage(message, f"❌ Error getting disk status: {e}")
