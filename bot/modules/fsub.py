"""
fsub.py — Force-subscription channel helpers.

The user-facing /addchnl /delchnl /listchnl /fsub_mode text commands have
been removed; all admin access now goes through /settings → Force Sub
(see bot/modules/settings.py). This module exposes pure async helpers
that the settings UI calls.

It also still owns `refresh_fsub_callback`, which powers the 🔄 Refresh
button shown to regular users when they hit the force-subscription gate
(see bot/core/func_utils.py:get_fsubs).
"""

from pyrogram import Client, filters
from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
)

from bot import bot
from bot.core.database import db
from bot.core.func_utils import editMessage, get_fsubs


BOT_TYPE_LABELS = {
    "ongoing":   "📡 Ongoing",
    "completed": "📦 Completed",
    "movie":     "🎬 Movie",
    "all":       "🌐 All Bots",
}


async def _safe_answer(query, text="", show_alert=False):
    """Wrap query.answer() — silently ignores QueryIdInvalid (callback >10s old)."""
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception:
        pass


# ── Channel-add helpers ──────────────────────────────────────────────────────
async def fsub_validate_channel(client, raw_input: str) -> tuple[bool, dict | str]:
    """
    Validate a raw channel ID input from the user.
    Accepts forms: 1234567890, -1001234567890, 1001234567890.
    Returns (True, {"chat_id", "title", "link"}) on success,
    or (False, "<error message HTML>") on failure.
    """
    raw = raw_input.strip().lstrip("-")
    if raw.startswith("100"):
        raw = raw[3:]
    try:
        short_id = int(raw)
        chat_id  = int(f"-100{short_id}")
    except ValueError:
        return False, "<b>❌ Invalid channel ID.</b> Send a numeric ID."

    existing = await db.show_channels()
    if chat_id in existing:
        return False, (
            f"<b>⚠️ Channel already in force-sub list.</b>\n"
            f"<code>{chat_id}</code>"
        )

    try:
        chat = await client.get_chat(chat_id)
    except Exception as e:
        return False, f"<b>❌ Cannot read that channel.</b>\n<code>{e}</code>"

    if chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP):
        return False, "<b>❌ Only channels and supergroups are allowed.</b>"

    try:
        bot_member = await client.get_chat_member(chat.id, "me")
        if bot_member.status not in (ChatMemberStatus.ADMINISTRATOR,
                                     ChatMemberStatus.OWNER):
            return False, "<b>❌ The bot must be an admin in that channel.</b>"
    except Exception as e:
        return False, f"<b>❌ Bot is not in that channel.</b>\n<code>{e}</code>"

    # New channels start in NORMAL mode by default (db.get_channel_mode
    # returns "off" when no doc exists). Tag the cached link with mode="off"
    # so get_invite_link() knows to regenerate a request-mode link the
    # first time the admin toggles this channel to REQUEST MODE.
    try:
        if chat.username:
            link = f"https://t.me/{chat.username}"
            await db.store_invite_link(chat_id, link, mode="off")
        else:
            invite = await client.create_chat_invite_link(chat.id)
            link = invite.invite_link
            await db.store_invite_link(chat_id, link, mode="off")
    except Exception:
        link = f"https://t.me/c/{str(chat.id)[4:]}"

    return True, {"chat_id": chat_id, "title": chat.title, "link": link}


async def fsub_save_channel(chat_id: int, bot_type: str) -> None:
    """Persist a validated force-sub channel with its bot-type binding."""
    await db.add_channel(chat_id, bot_type=bot_type)


# ── Remove ──────────────────────────────────────────────────────────────────
async def fsub_remove_channel(chat_id: int) -> tuple[bool, str]:
    """Remove a single force-sub channel by ID."""
    existing = await db.show_channels()
    if chat_id not in existing:
        return False, f"<b>⚠️ <code>{chat_id}</code> is not in the force-sub list.</b>"
    await db.rem_channel(chat_id)
    return True, f"<b>✅ Removed <code>{chat_id}</code> from force-sub.</b>"


async def fsub_remove_all() -> int:
    """Remove every force-sub channel. Returns the count cleared."""
    existing = await db.show_channels()
    for ch_id in existing:
        await db.rem_channel(ch_id)
    return len(existing)


# ── List rendering ──────────────────────────────────────────────────────────
async def render_fsub_list(client) -> str:
    """Build the HTML body for the Force Sub → List view."""
    cursor = db.db.force_sub_channels.find({})
    docs = await cursor.to_list(length=100)

    if not docs:
        return (
            "<b>📡 Force-Sub Channels</b>\n\n"
            "<i>No channels configured. Use the <b>Add</b> button to add one.</i>"
        )

    lines = [f"<b>📡 Force-Sub Channels ({len(docs)})</b>\n"]
    for no, doc in enumerate(docs, start=1):
        ch_id    = doc["channel_id"]
        bot_type = doc.get("bot_type", "all")
        try:
            chat = await client.get_chat(ch_id)
            mode = await db.get_channel_mode(ch_id)
            mode_emoji = "🟢" if mode == "on" else "🔴"
            mode_text  = "REQUEST" if mode == "on" else "NORMAL"
            link = await db.get_invite_link(ch_id)
            if not link:
                link = (f"https://t.me/{chat.username}" if chat.username
                        else f"https://t.me/c/{str(ch_id)[4:]}")
            lines.append(
                f"{no}. <a href='{link}'>{chat.title}</a>\n"
                f"   • <code>{ch_id}</code>\n"
                f"   • Bot: {BOT_TYPE_LABELS.get(bot_type, bot_type)}\n"
                f"   • Mode: {mode_emoji} <code>{mode_text}</code>"
            )
        except Exception:
            lines.append(
                f"{no}. ⚠️ Unavailable\n"
                f"   • <code>{ch_id}</code>\n"
                f"   • Bot: {BOT_TYPE_LABELS.get(bot_type, bot_type)}"
            )
    return "\n\n".join(lines)


# ── Mode picker helpers ─────────────────────────────────────────────────────
async def fsub_mode_picker_data(client) -> list[dict]:
    """Return [{ch_id, label}] for the mode-picker keyboard."""
    channels = await db.show_channels()
    out = []
    for ch_id in channels:
        try:
            chat = await client.get_chat(ch_id)
            mode = await db.get_channel_mode(ch_id)
            emoji = "🟢" if mode == "on" else "🔴"
            mode_text = "REQUEST" if mode == "on" else "NORMAL"
            label = f"{emoji} {chat.title} ({mode_text})"
        except Exception:
            label = f"⚠️ {ch_id} (Unavailable)"
        out.append({"ch_id": ch_id, "label": label})
    return out


async def fsub_toggle_channel_mode(ch_id: int) -> tuple[str, str]:
    """
    Toggle a channel's force-sub mode.
    Returns (new_mode, new_mode_human_text).

    CREATE-ONCE-REUSE: each mode has its own cached invite link
    (db.invite_links.on / .off). Toggling between modes simply switches
    which cached link the gate hands out — the previously-built link for
    the OTHER mode stays in the cache and is reused next time the admin
    flips back. So we do NOT clear the link on toggle any more; only the
    very first gate render in a brand-new mode actually calls
    create_chat_invite_link.
    """
    current = await db.get_channel_mode(ch_id)
    new = "off" if current == "on" else "on"
    await db.set_channel_mode(ch_id, new)
    human = "REQUEST MODE (🟢)" if new == "on" else "NORMAL MODE (🔴)"
    return new, human


# ── User-facing refresh callback (KEEP — used by force-sub gate) ────────────
@bot.on_callback_query(filters.regex("refresh_fsub"))
async def refresh_fsub_callback(client: Client, callback_query: CallbackQuery):
    """🔄 Refresh button on the user-facing force-sub gate."""
    user_id = callback_query.from_user.id
    try:
        txtargs = ["start"]
        txt, btns = await get_fsubs(user_id, txtargs, client)
        await editMessage(callback_query.message, txt, InlineKeyboardMarkup(btns))
        await _safe_answer(callback_query, "✅ Status refreshed!")
    except Exception:
        await _safe_answer(callback_query, "❌ Error refreshing status!", show_alert=True)
