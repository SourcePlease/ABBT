from json import loads as jloads
from os import path as ospath, execl
from sys import executable
from datetime import datetime
from math import ceil

from aiohttp import ClientSession
from pyrogram.filters import command, private

from bot import Var, bot, ffQueue, admin
from bot.core.text_utils import TextEditor
from bot.core.reporter import rep
from bot.core.func_utils import new_task, sendMessage
from bot.core.database import db

SCHEDULE_PHOTO = "https://graph.org/file/bd0dea0fae723c48b279c-76e285fcf95b537017.jpg"
CAPTION_LIMIT  = 1024


def convert_to_12hr_format(time_24hr):
    try:
        time_obj  = datetime.strptime(time_24hr, "%H:%M")
        time_12hr = time_obj.strftime("%I:%M %p")
        return time_12hr.lstrip("0")
    except Exception:
        return time_24hr + " hrs"


async def generate_schedule_entries():
    """
    Fetch today's schedule from SubsPlease and return formatted lines.

    Each anime is looked up on AniList to get the English title.
    Failures are caught per-item — a broken AniList lookup for one anime
    never drops any other entry from the schedule.
    """
    async with ClientSession() as ses:
        res = await ses.get(
            "https://subsplease.org/api/?f=schedule&h=true&tz=Asia/Kolkata"
        )
        raw = jloads(await res.text())

    # SubsPlease returns {"schedule": [...]} — handle both list and dict formats
    schedule_data = raw.get("schedule", raw) if isinstance(raw, dict) else raw
    if isinstance(schedule_data, dict):
        # Some API versions return a day-keyed dict; flatten all values
        items = []
        for v in schedule_data.values():
            if isinstance(v, list):
                items.extend(v)
    else:
        items = list(schedule_data)

    lines = []
    for item in items:
        sp_title = item.get("title", "Unknown")
        time_str = convert_to_12hr_format(item.get("time", ""))

        # Try AniList for English title — fall back to SubsPlease title on any error
        display_title = sp_title
        try:
            aname = TextEditor(sp_title)
            await aname.load_anilist()
            _eng = (aname.adata.get("title") or {}).get("english")
            if _eng and _eng.strip():
                display_title = _eng.strip()
        except Exception:
            pass  # keep SubsPlease title — never drop the entry

        lines.append(f"<b>➤</b> {display_title} — <b>{time_str}</b>")

    return lines


def _split_into_messages(lines: list, header: str) -> list[str]:
    """
    Split schedule lines into caption-sized chunks.
    First chunk gets the header; all chunks respect CAPTION_LIMIT.
    """
    messages = []
    current  = header + "\n"

    for line in lines:
        candidate = current + line + "\n"
        if len(candidate) > CAPTION_LIMIT:
            messages.append(current.rstrip())
            current = line + "\n"
        else:
            current = candidate

    if current.strip():
        messages.append(current.rstrip())

    return messages


async def _delete_old_schedule(channel_id: int):
    """
    Delete previously posted schedule messages for this channel.
    IDs are stored in DB by save_schedule_posts; clears DB record after deletion.
    Also unpins whatever is currently pinned if it was ours.
    """
    old_ids = await db.get_schedule_posts(channel_id)
    if not old_ids:
        return

    # Delete all tracked messages (photo + any continuation texts)
    for msg_id in old_ids:
        try:
            await bot.delete_messages(channel_id, msg_id)
        except Exception:
            pass  # already deleted or not found — fine

    await db.delete_schedule_posts(channel_id)


async def _post_schedule(channel_id: int):
    """
    Delete the previous schedule, then post today's schedule.

    Posts as:
      - Message 1: photo + schedule caption (pinned)
      - Message 2+: plain text continuation if schedule exceeds CAPTION_LIMIT
    All message IDs are saved to DB so the next run can delete them cleanly.
    """
    header = "<b>\U0001D5E7\U0001D5FC\U0001D5F1\U0001D5EE\U0001D606 \U0001D400\U0001D40D\U0001D404\U0001D40C\U0001D400 \U0001D411\U0001D402\U0001D40C\U0001D402\U0001D400\U0001D416\U0001D402 \U0001D4E6\U0001D4EC\U0001D4EE\U0001D4F3\U0001D4F5\U0001D4F5\U0001D4EE [\u026Bst]</b>"
    # Use literal bold text to avoid unicode issues
    header = "<b>𝗧𝗼𝗱𝗮𝘆 𝗔𝗻𝗶𝗺𝗲 𝗥𝗲𝗹𝗲𝗮𝘀𝗲 𝗦𝗰𝗵𝗲𝗱𝘂𝗹𝗲 [ɪsᴛ]</b>"
    lines  = await generate_schedule_entries()

    if not lines:
        await rep.report("Schedule: no entries returned from SubsPlease", "warning", log=False)
        return None

    # ── Delete old schedule messages ──────────────────────────────────────────
    await _delete_old_schedule(channel_id)

    parts    = _split_into_messages(lines, header)
    sent_ids = []

    # Part 1 — photo + caption (pinned)
    first_msg = await bot.send_photo(
        channel_id,
        photo=SCHEDULE_PHOTO,
        caption=parts[0]
    )
    sent_ids.append(first_msg.id)

    try:
        pin_msg = await first_msg.pin()
        await pin_msg.delete()   # delete the "pinned a message" service message
    except Exception:
        pass

    # Parts 2+ — plain text continuation
    for part in parts[1:]:
        cont = await bot.send_message(channel_id, part)
        sent_ids.append(cont.id)

    # ── Persist message IDs for next run to delete ────────────────────────────
    await db.save_schedule_posts(channel_id, sent_ids)
    await rep.report(
        f"📅 Schedule posted ({len(lines)} anime, {len(parts)} message(s))",
        "info", log=False
    )
    return first_msg


# NOTE: The /schedule slash command was removed. The "Post Now" action
# now lives under /settings → 📅 Schedule (see show_sched_menu /
# handle_sched_action below). The original command body is preserved as
# `post_schedule_now()` so the panel can call it directly.
async def post_schedule_now(client, parent_message):
    """Generate today's schedule and post to MAIN_CHANNEL.

    `parent_message` is the message used as a parent for the status reply
    (typically the panel photo or the user's /settings invocation).
    Returns (ok: bool, summary: str).
    """
    temp = await sendMessage(parent_message, "<b><i>Generating schedule...</i></b>")
    try:
        msg = await _post_schedule(Var.MAIN_CHANNEL)
        if msg:
            await temp.edit("<b>✅ Schedule posted successfully!</b>")
            return True, "Schedule posted."
        else:
            await temp.edit("<b>❌ No schedule data available.</b>")
            return False, "No schedule data available."
    except Exception as err:
        await rep.report(f"Error in schedule command: {str(err)}", "error")
        await temp.edit(f"<b>❌ Error posting schedule:</b>\n<code>{str(err)}</code>")
        return False, str(err)


async def upcoming_animes():
    if Var.SEND_SCHEDULE:
        try:
            await _post_schedule(Var.MAIN_CHANNEL)
        except Exception as err:
            await rep.report(str(err), "error")

    # Wait for active FFmpeg encodes to finish before restarting
    from bot import ongoing_encode_lock, batch_encode_lock
    import asyncio
    _waited = 0
    while (_waited < 3600 and
           (ongoing_encode_lock._value < 1 or batch_encode_lock._value < 1)):
        await asyncio.sleep(30)
        _waited += 30

    if not ffQueue.empty():
        await ffQueue.join()
    await rep.report("Auto Restarting..!!", "info")
    execl(executable, executable, "-m", "bot")


# ─────────────────────────────────────────────────────────────────────────────
# /settings → 📅 Schedule sub-panel.
#
# Owns every callback for the Schedule section so that
# bot/modules/settings.py stays focused on routing only. Replaces the
# standalone admin command:
#
#   /schedule  → 📅 Schedule → 📤 Post Now
#
# settings.py forwards these to this module:
#
#   s:menu:sched            → show_sched_menu(client, query)
#   s:sched:<action>        → handle_sched_action(client, query, parts)
# ─────────────────────────────────────────────────────────────────────────────
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup


SCHED_MENU_CAPTION = (
    "<b>📅 Schedule</b>\n\n"
    "Post today's anime release schedule (sourced from SubsPlease) to "
    "<code>MAIN_CHANNEL</code>.\n\n"
    "• <b>📤 Post Now</b> — fetch &amp; publish today's schedule "
    "(deletes the previous schedule message and pins the new one)\n\n"
    "<i>The bot also posts the schedule automatically on its daily "
    "<code>upcoming_animes()</code> cycle when "
    "<code>SEND_SCHEDULE=True</code> in config.env.</i>"
)


def _kb_sched_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Post Now", callback_data="s:sched:post")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_sched_back():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:sched"),
        InlineKeyboardButton("🏠 Home",  callback_data="s:home"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ]])


async def show_sched_menu(client, query):
    """Render the Schedule sub-menu (entry from s:menu:sched)."""
    from bot.modules.settings import _safe_edit_caption as _safe
    try:
        await _safe(query.message, SCHED_MENU_CAPTION, _kb_sched_menu())
    except Exception:
        pass
    return await query.answer()


async def handle_sched_action(client, query, parts):
    """Dispatch s:sched:<action> callbacks.

    parts = ["s", "sched", <action>, ...]
    """
    from bot.modules.settings import _safe_edit_caption as _safe
    if len(parts) < 3:
        return await query.answer("Bad action", show_alert=True)
    action = parts[2]

    if action == "post":
        await query.answer("Generating schedule...")
        # Use the panel as the parent message so the status reply lives
        # next to the panel in the same DM.
        try:
            ok, summary = await post_schedule_now(client, query.message)
        except Exception as e:
            await rep.report(f"sched post error: {e}", "error")
            try:
                await _safe(
                    query.message,
                    f"<b>📅 Schedule</b>\n\n"
                    f"<b>❌ Error:</b> <code>{e}</code>",
                    _kb_sched_back(),
                )
            except Exception:
                pass
            return
        body = (
            f"<b>📅 Schedule</b>\n\n"
            + ("<b>✅ Done.</b> " if ok else "<b>❌ Failed.</b> ")
            + f"<i>{summary}</i>"
        )
        try:
            await _safe(query.message, body, _kb_sched_back())
        except Exception:
            pass
        return

    return await query.answer("Unknown action", show_alert=True)
