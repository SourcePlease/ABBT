"""
index.py — Anime channel index, matching manga bot format.

Separate indexes for each channel type:
  - Ongoing   → MAIN_CHANNEL
  - Completed → BATCH_MAIN_CHANNEL  
  - Movie     → MOVIE_MAIN_CHANNEL

Admin commands:
  /index              — show all index slot status
  /index add <id>     — register a slot (asks channel type via buttons)
  /index remove <id>  — remove a slot
  /index clear        — clear all slots for a type
  /index refresh      — force-rebuild all indexes
"""

from traceback import format_exc

from pyrogram.enums import ParseMode
from pyrogram.filters import command, private
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, Var, admin
from bot.core.database import db
from bot.core.func_utils import new_task, sendMessage
from bot.core.reporter import rep

MESSAGE_LIMIT = 4000

CHANNEL_TYPES = {
    "ongoing":   {"label": "📡 Ongoing",   "channel": lambda: Var.MAIN_CHANNEL},
    "completed": {"label": "📦 Completed", "channel": lambda: Var.BATCH_MAIN_CHANNEL},
    "movie":     {"label": "🎬 Movie",     "channel": lambda: Var.MOVIE_MAIN_CHANNEL},
}


def _build_slots(anime_list: list, title: str = "Anime Index") -> list[str]:
    slots = []
    current = [f"🗂 <b>{title}</b>\n"]

    for anime in sorted(anime_list, key=lambda x: x["anime_name"].lower()):
        channel_id_clean = str(anime["channel_id"]).replace("-100", "")
        link = f"https://t.me/c/{channel_id_clean}/1"
        line = f'• <a href="{link}">{anime["anime_name"]}</a>'
        test = "\n".join(current + [line])
        if len(test) > MESSAGE_LIMIT:
            slots.append("\n".join(current))
            current = [f"🗂 <b>{title} (cont.)</b>\n", line]
        else:
            current.append(line)

    if current:
        slots.append("\n".join(current))

    return slots if slots else [f"🗂 <b>{title}</b>\n\n<i>No anime added yet.</i>"]


async def update_index(channel_type: str = None):
    """Rebuild index for one or all channel types."""
    types_to_update = [channel_type] if channel_type else list(CHANNEL_TYPES.keys())

    for ct in types_to_update:
        try:
            message_ids = await db.get_index_message_ids(ct)
            if not message_ids:
                continue

            ct_info = CHANNEL_TYPES[ct]
            channel_id = ct_info["channel"]()

            # Get anime for this channel type
            all_anime = await db.get_all_indexed_anime()
            anime_list = [a for a in all_anime if a.get("db_type", "ongoing") == ct]

            slots = _build_slots(anime_list, title=f"{ct_info['label']} Index")

            # Auto-create extra slots if needed
            if len(slots) > len(message_ids):
                needed = len(slots) - len(message_ids)
                await rep.report(f"📋 {ct} index needs {needed} more slot(s), auto-creating...", "info")
                _bot_client = bot
                for _ in range(needed):
                    try:
                        new_msg = await _bot_client.send_message(
                            chat_id=channel_id,
                            text=f"🗂 <b>{ct_info['label']} Index (cont.)</b>\n\n<i>loading...</i>",
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        message_ids.append(new_msg.id)
                        await db.set_index_message_ids(message_ids, ct)
                        await rep.report(f"✅ Auto-created {ct} index slot (msg ID: {new_msg.id})", "info")
                    except Exception as e:
                        await rep.report(f"❌ Failed to auto-create {ct} index slot: {e}", "error")
                        break

            for i, (text, mid) in enumerate(zip(slots, message_ids)):
                try:
                    await bot.edit_message_text(
                        chat_id=channel_id,
                        message_id=mid,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    await rep.report(f"{ct} index slot {i+1} (msg {mid}) update failed: {e}", "error")

        except Exception:
            await rep.report(format_exc(), "error")


# ── Pending add state ─────────────────────────────────────────────────────────
# Stores {user_id: [msg_id1, msg_id2, ...]} waiting for channel type selection
_pending_add: dict = {}


@bot.on_message(command('uindex') & private & admin)
@new_task
async def uindex_command(client, message):
    """Force update all 3 channel indexes."""
    msg = await sendMessage(message, "<b>🔄 Updating all indexes...</b>")
    results = []
    for ct, info in CHANNEL_TYPES.items():
        ids = await db.get_index_message_ids(ct)
        if not ids:
            results.append(f"{info['label']}: ⚠️ No slots registered")
            continue
        try:
            await update_index(ct)
            results.append(f"{info['label']}: ✅ Updated ({len(ids)} slot(s))")
        except Exception as e:
            results.append(f"{info['label']}: ❌ Failed — {e}")
    await msg.edit(
        "<b>📋 Index Update Complete</b>\n\n" +
        "\n".join(results)
    )



@new_task
async def index_command(client, message):
    args = message.text.split()[1:]

    if not args:
        # Show status for all types
        lines = ["<b>📋 Index Status</b>\n"]
        for ct, info in CHANNEL_TYPES.items():
            ids = await db.get_index_message_ids(ct)
            lines.append(f"{info['label']}: <b>{len(ids)}</b> slot(s) — IDs: {', '.join(f'<code>{i}</code>' for i in ids) or 'none'}")
        lines.append(
            "\n<b>Commands:</b>\n"
            "/index add &lt;id1&gt; [id2]... — add slot(s) (will ask channel type)\n"
            "/index remove &lt;id&gt; — remove a slot\n"
            "/index refresh — rebuild all\n"
            "/index clear — clear slots for a type"
        )
        return await sendMessage(message, "\n".join(lines))

    sub = args[0].lower()

    # ── /index add <id> [id2] ... ─────────────────────────────────────────────
    if sub == "add":
        if len(args) < 2:
            return await sendMessage(message,
                "<b>Usage: /index add &lt;id1&gt; [id2] ...</b>\n"
                "Example: <code>/index add 101 102</code>"
            )
        try:
            new_ids = [int(x) for x in args[1:]]
        except ValueError:
            return await sendMessage(message, "<b>❌ All IDs must be numbers.</b>")

        # Store pending and ask channel type
        _pending_add[message.from_user.id] = new_ids
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📡 Ongoing",   callback_data=f"idx_add_type|ongoing"),
            InlineKeyboardButton("📦 Completed", callback_data=f"idx_add_type|completed"),
            InlineKeyboardButton("🎬 Movie",     callback_data=f"idx_add_type|movie"),
        ]])
        await sendMessage(
            message,
            f"<b>Which channel do these message IDs belong to?</b>\n"
            f"IDs: {', '.join(f'<code>{i}</code>' for i in new_ids)}",
            reply_markup=kb
        )

    # ── /index remove <id> ────────────────────────────────────────────────────
    elif sub == "remove":
        if len(args) < 2:
            return await sendMessage(message, "<b>Usage: /index remove &lt;id&gt;</b>")
        try:
            rem_id = int(args[1])
        except ValueError:
            return await sendMessage(message, "<b>❌ ID must be a number.</b>")

        removed_from = []
        for ct in CHANNEL_TYPES:
            ids = await db.get_index_message_ids(ct)
            if rem_id in ids:
                ids.remove(rem_id)
                await db.set_index_message_ids(ids, ct)
                removed_from.append(ct)

        if removed_from:
            await sendMessage(message, f"<b>✅ Removed <code>{rem_id}</code> from: {', '.join(removed_from)}</b>")
        else:
            await sendMessage(message, f"<b>⚠️ ID <code>{rem_id}</code> not found in any index.</b>")

    # ── /index refresh ────────────────────────────────────────────────────────
    elif sub == "refresh":
        msg = await sendMessage(message, "<b>🔄 Rebuilding all indexes...</b>")
        await update_index()
        await msg.edit("<b>✅ All indexes refreshed.</b>")

    # ── /index clear ──────────────────────────────────────────────────────────
    elif sub == "clear":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📡 Ongoing",   callback_data="idx_clear|ongoing"),
            InlineKeyboardButton("📦 Completed", callback_data="idx_clear|completed"),
            InlineKeyboardButton("🎬 Movie",     callback_data="idx_clear|movie"),
            InlineKeyboardButton("🗑 All",       callback_data="idx_clear|all"),
        ]])
        await sendMessage(message, "<b>Clear index slots for which channel?</b>", reply_markup=kb)

    else:
        await sendMessage(message,
            "<b>Unknown subcommand.</b>\n\n"
            "/index — status\n"
            "/index add &lt;id&gt; — add slot\n"
            "/index remove &lt;id&gt; — remove slot\n"
            "/index refresh — force rebuild\n"
            "/index clear — clear slots"
        )


@bot.on_callback_query(admin & __import__('pyrogram').filters.regex(r"^idx_add_type\|"))
async def idx_add_type_cb(client, query):
    ct = query.data.split("|", 1)[1]
    user_id = query.from_user.id
    new_ids = _pending_add.pop(user_id, [])

    if not new_ids:
        return await query.answer("Session expired. Run /index add again.", show_alert=True)

    ct_info = CHANNEL_TYPES.get(ct)
    if not ct_info:
        return await query.answer("Unknown type.", show_alert=True)

    channel_id = ct_info["channel"]()
    message_ids = await db.get_index_message_ids(ct)
    added, failed = [], []

    for new_id in new_ids:
        if new_id in message_ids:
            failed.append(f"⚠️ <code>{new_id}</code> — already registered")
            continue
        try:
            await bot.edit_message_text(
                chat_id=channel_id,
                message_id=new_id,
                text=f"🗂 <b>{ct_info['label']} Index</b>\n\n<i>loading...</i>",
                parse_mode=ParseMode.HTML
            )
            message_ids.append(new_id)
            added.append(new_id)
        except Exception as e:
            failed.append(f"❌ <code>{new_id}</code> — {e}")

    if added:
        await db.set_index_message_ids(message_ids, ct)
        await update_index(ct)

    lines = [f"✅ <code>{i}</code> added to {ct_info['label']}" for i in added] + failed
    await query.edit_message_text(
        f"<b>Index slots — {len(added)}/{len(new_ids)} added to {ct_info['label']}</b>\n\n"
        + "\n".join(lines)
        + f"\n\n<b>Total {ct_info['label']} slots: {len(message_ids)}</b>"
    )
    await query.answer()


@bot.on_callback_query(admin & __import__('pyrogram').filters.regex(r"^idx_clear\|"))
async def idx_clear_cb(client, query):
    ct = query.data.split("|", 1)[1]
    if ct == "all":
        for t in CHANNEL_TYPES:
            await db.set_index_message_ids([], t)
        await query.edit_message_text("<b>✅ All index slots cleared.</b>")
    else:
        await db.set_index_message_ids([], ct)
        ct_info = CHANNEL_TYPES.get(ct, {})
        await query.edit_message_text(f"<b>✅ {ct_info.get('label', ct)} index slots cleared.</b>")
    await query.answer()
