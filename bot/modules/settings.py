"""
settings.py — Inline /settings panel for owner & admins.

Provides a paginated, button-driven UI for the actions that previously lived
behind the standalone /ban, /unban, /banlist, /add_admin, /deladmin, /admins,
/users, /addchnl, /delchnl, /listchnl and /fsub_mode commands. Those text
commands have been removed; everything is now reached through this single
/settings entry point.

Layout:
  /settings → photo + caption + main menu
    [Ban | Admins]
    [Users | Force Sub]
    Ban       →  [Ban | Unban | List]
    Admins    →  [Add | Remove | List]
    Users     →  total user count from the database
    Force Sub →  [Add | Remove] [List | Mode]
        Add    → asks for channel ID, then pick which bot enforces it
        Remove → asks for channel ID (or `all`), with a Cancel button
        List   → renders all force-sub channels with mode
        Mode   → per-channel toggle between NORMAL and REQUEST

The banner image URL is read from `Var.SETTINGS_PHOTO_URL` (config.env key
`SETTINGS_PHOTO_URL`). Defaults to the project banner if unset.

This module owns ONLY the UI layer — all real work is delegated to helpers
in bot/modules/banuser.py, bot/modules/admin.py, bot/modules/cmds.py and
bot/modules/fsub.py.
"""

from pyrogram import filters
from pyrogram.filters import command, private
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import MessageNotModified

from bot import bot, Var, admin
from bot.core.func_utils import new_task


# Text filter for /settings input (Ban / Unban / Add admin / Remove admin /
# Force-Sub Add / Force-Sub Remove). The handler short-circuits when the
# user has no pending state; the exclude list keeps slash-commands out.
SETTINGS_INPUT_FILTER = (
    filters.private & admin & filters.text & ~filters.command([
        "start", "help", "shell", "eval", "stats", "update", "downloads",
        "fixbatchlink", "restart", "batch", "genlink", "settings",
        "set", "index",
        "connect", "connectchannel", "listconnections", "removeconnection",
        "removechannel",
        "cancel", "broadcast", "dbroadcast", "pbroadcast",
        "fsub", "adfsub", "rmfsub", "listfsub",
        "schedule",
        "log", "clearlogs", "importusers", "users", "admins", "ban",
        "unban", "banlist", "add_admin", "deladmin", "addchnl", "delchnl",
        "listchnl", "fsub_mode",
    ])
)

from bot.core.database import db
from bot.core.func_utils import convertTime
from bot.modules.banuser import (
    do_ban, do_unban, do_unban_all, render_ban_list,
)
from bot.modules.admin import (
    do_add_admin, do_remove_admin, do_remove_all_admins, render_admin_list,
)
from bot.modules.cmds import get_user_count
from bot.modules.fsub import (
    BOT_TYPE_LABELS,
    fsub_validate_channel, fsub_save_channel,
    fsub_remove_channel, fsub_remove_all,
    render_fsub_list,
    fsub_mode_picker_data, fsub_toggle_channel_mode,
)


# ── In-memory pending-input state ─────────────────────────────────────────────
# user_id -> {
#   "action":  "ban" | "unban" | "add_admin" | "del_admin"
#            | "fsub_add" | "fsub_del" | "fsub_add_pick_type",
#   "chat_id": int,
#   "msg_id":  int,
#   "data":    dict,   # multi-step extras (e.g. resolved fsub channel info)
# }
_pending: dict[int, dict] = {}


# ── Caption builders ──────────────────────────────────────────────────────────
HOME_CAPTION = (
    "<b>⚙️ Settings Panel</b>\n\n"
    "Manage bot users, gating and the task pipeline from one place.\n\n"
    "• <b>Ban</b> — block a user from using the bot\n"
    "• <b>Admins</b> — promote / demote bot admins\n"
    "• <b>Users</b> — show how many users are in the database\n"
    "• <b>Force Sub</b> — manage required-join channels\n"
    "• <b>Auto Delete</b> — view / set the file-delete timer\n"
    "• <b>Channel Management</b> — connect / list / remove "
    "anime → channel links\n"
    "• <b>Dashboard &amp; Queue</b> — bot status, task queue, "
    "pause/resume, reboot cache\n"
    "• <b>RSS &amp; Tasks</b> — feeds, manual queue, retry / clean\n"
    "• <b>Schedule</b> — post today's anime release schedule\n\n"
    "<i>This panel is available to the owner and to admins.</i>"
)

BAN_CAPTION = (
    "<b>🚫 Ban Menu</b>\n\n"
    "• <b>Ban</b> — block a user by ID\n"
    "• <b>Unban</b> — restore a previously banned user\n"
    "• <b>List</b> — show every banned user"
)

ADMIN_CAPTION = (
    "<b>👮 Admin Menu</b>\n\n"
    "• <b>Add</b> — promote a user to admin\n"
    "• <b>Remove</b> — demote an admin\n"
    "• <b>List</b> — show every current admin\n\n"
    "<i>Once added, an admin can use every admin-gated command.</i>"
)

FSUB_CAPTION = (
    "<b>📡 Force Sub Menu</b>\n\n"
    "Channels users must join before the bot will serve them.\n\n"
    "• <b>Add</b> — register a new force-sub channel\n"
    "• <b>Remove</b> — drop a channel by ID (or <code>all</code>)\n"
    "• <b>List</b> — show every channel with its mode\n"
    "• <b>Mode</b> — flip a channel between NORMAL and REQUEST\n"
    "• <b>Relink</b> — wipe the cached invite link so a fresh one is "
    "generated next time (use after a Telegram revoke)\n\n"
    "<i>The bot must be admin in the channel before adding it.</i>"
)


def _ad_caption(seconds: int) -> str:
    """Caption for the Auto Delete sub-menu — shows AUTO_DEL state + timer."""
    auto_state = "🟢 ON" if Var.AUTO_DEL else "🔴 OFF"
    return (
        "<b>⏱ Auto Delete</b>\n\n"
        "Files sent to users are auto-deleted after a configurable timer.\n\n"
        f"• <b>Status:</b> <code>AUTO_DEL = {auto_state}</code>\n"
        f"• <b>Current timer:</b> <code>{convertTime(seconds)}</code> "
        f"(<code>{seconds}s</code>)\n\n"
        "<i>Tap <b>Set Timer</b> to change. Minimum 10 seconds. "
        "AUTO_DEL is read from <code>config.env</code> and requires a "
        "bot restart to flip.</i>"
    )


PROMPT_TEXTS = {
    "ban":       "<b>🚫 Send the user ID to ban.</b>\n\n"
                 "Reply with a single numeric Telegram ID. Tap <b>Cancel</b> to abort.",
    "unban":     "<b>♻️ Send the user ID to unban.</b>\n\n"
                 "Send a single numeric ID, or type <code>all</code> to clear "
                 "the entire ban list. Tap <b>Cancel</b> to abort.",
    "add_admin": "<b>➕ Send the user ID to promote to admin.</b>\n\n"
                 "Reply with a single numeric Telegram ID. Tap <b>Cancel</b> to abort.",
    "del_admin": "<b>➖ Send the user ID to remove from admins.</b>\n\n"
                 "Send a single numeric ID, or type <code>all</code> to clear "
                 "every admin. Tap <b>Cancel</b> to abort.",
    "fsub_add":  "<b>➕ Send the channel ID to add as force-sub.</b>\n\n"
                 "Send the numeric channel ID (with or without the <code>-100</code> "
                 "prefix). Tap <b>Cancel</b> to abort.",
    "fsub_del":  "<b>➖ Send the channel ID to remove from force-sub.</b>\n\n"
                 "Send a single numeric ID (with the <code>-100</code> prefix), "
                 "or type <code>all</code> to clear every channel. "
                 "Tap <b>Cancel</b> to abort.",
    "ad_set":    "<b>⏱ Send the new auto-delete timer.</b>\n\n"
                 "Reply with a number of seconds (minimum <code>10</code>). "
                 "Examples: <code>60</code> for 1 min, <code>600</code> for "
                 "10 min, <code>3600</code> for 1 hour. "
                 "Tap <b>Cancel</b> to abort.",
}


# ── Keyboard builders ─────────────────────────────────────────────────────────
def _kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚫 Ban",      callback_data="s:menu:ban"),
            InlineKeyboardButton("👮 Admins",   callback_data="s:menu:admin"),
        ],
        [
            InlineKeyboardButton("👥 Users",    callback_data="s:users"),
            InlineKeyboardButton("📡 Force Sub", callback_data="s:menu:fsub"),
        ],
        [
            InlineKeyboardButton("⏱ Auto Delete",        callback_data="s:menu:ad"),
            InlineKeyboardButton("📺 Channel Management", callback_data="s:menu:cmgr"),
        ],
        [InlineKeyboardButton("📊 Dashboard & Queue", callback_data="s:menu:dq")],
        [InlineKeyboardButton("📋 RSS & Manual Tasks", callback_data="s:menu:rt")],
        [InlineKeyboardButton("📅 Schedule", callback_data="s:menu:sched")],
        [InlineKeyboardButton("❌ Close", callback_data="s:close")],
    ])


def _kb_ban_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚫 Ban",   callback_data="s:ask:ban"),
            InlineKeyboardButton("♻️ Unban", callback_data="s:ask:unban"),
            InlineKeyboardButton("📋 List",  callback_data="s:list:ban"),
        ],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add",     callback_data="s:ask:add_admin"),
            InlineKeyboardButton("➖ Remove",  callback_data="s:ask:del_admin"),
            InlineKeyboardButton("📋 List",    callback_data="s:list:admin"),
        ],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_fsub_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add",    callback_data="s:ask:fsub_add"),
            InlineKeyboardButton("➖ Remove", callback_data="s:ask:fsub_del"),
        ],
        [
            InlineKeyboardButton("📋 List",   callback_data="s:list:fsub"),
            InlineKeyboardButton("⚙️ Mode",   callback_data="s:menu:fsub_mode"),
        ],
        [
            InlineKeyboardButton("🔗 Relink", callback_data="s:menu:fsub_relink"),
        ],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_ad_menu() -> InlineKeyboardMarkup:
    """Auto Delete sub-menu — set timer, back, close."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ Set Timer", callback_data="s:ask:ad_set")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_fsub_relink(picker_rows: list[dict]) -> InlineKeyboardMarkup:
    """Build keyboard for picking which channel's invite-link cache to wipe.

    'Relink All' nukes the cache for every force-sub channel in one tap.
    Each per-channel button calls db.clear_invite_link(ch_id) so the
    next force-sub gate generates a fresh link with the right
    creates_join_request flag for that channel's current mode.
    """
    rows: list[list[InlineKeyboardButton]] = []
    for row in picker_rows:
        rows.append([InlineKeyboardButton(
            row["label"][:60],
            callback_data=f"s:fsub_relink:{row['ch_id']}",
        )])
    if picker_rows:
        rows.append([InlineKeyboardButton(
            "♻️ Relink ALL channels",
            callback_data="s:fsub_relink:all",
        )])
    rows.append([
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:fsub"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ])
    return InlineKeyboardMarkup(rows)


def _kb_fsub_type_picker() -> InlineKeyboardMarkup:
    """Bot-type picker shown after the user supplies a valid channel ID."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(BOT_TYPE_LABELS["ongoing"],
                                 callback_data="s:fsub_type:ongoing"),
            InlineKeyboardButton(BOT_TYPE_LABELS["completed"],
                                 callback_data="s:fsub_type:completed"),
        ],
        [
            InlineKeyboardButton(BOT_TYPE_LABELS["movie"],
                                 callback_data="s:fsub_type:movie"),
            InlineKeyboardButton(BOT_TYPE_LABELS["all"],
                                 callback_data="s:fsub_type:all"),
        ],
        [InlineKeyboardButton("✖️ Cancel", callback_data="s:cancel:fsub")],
    ])


def _kb_fsub_mode(picker_rows: list[dict]) -> InlineKeyboardMarkup:
    """Build keyboard for picking which channel's mode to flip."""
    rows: list[list[InlineKeyboardButton]] = []
    for row in picker_rows:
        rows.append([InlineKeyboardButton(
            row["label"][:60],  # Telegram caps button text
            callback_data=f"s:fsub_toggle:{row['ch_id']}",
        )])
    rows.append([
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:fsub"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ])
    return InlineKeyboardMarkup(rows)


def _kb_cancel(action: str) -> InlineKeyboardMarkup:
    """Cancel-only keyboard shown while the panel waits for a typed ID."""
    if action in ("ban", "unban"):
        parent = "ban"
    elif action in ("add_admin", "del_admin"):
        parent = "admin"
    elif action == "ad_set":
        parent = "ad"
    else:  # fsub_add, fsub_del
        parent = "fsub"
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✖️ Cancel", callback_data=f"s:cancel:{parent}"),
    ]])


def _kb_back(parent: str) -> InlineKeyboardMarkup:
    """Back-only keyboard used after a result is displayed."""
    if parent == "home":
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Home",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ]])
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back",  callback_data=f"s:menu:{parent}"),
        InlineKeyboardButton("🏠 Home",  callback_data="s:home"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ]])


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _safe_edit_caption(message, caption: str, reply_markup):
    """Edit a photo-message caption, swallowing harmless errors."""
    try:
        await message.edit_caption(caption=caption, reply_markup=reply_markup)
    except MessageNotModified:
        pass


def _is_authorised(user_id: int, admin_ids: list[int]) -> bool:
    return user_id == Var.OWNER_ID or user_id in admin_ids


# ── /settings entry point ─────────────────────────────────────────────────────
@bot.on_message(command('settings') & private & admin)
@new_task
async def settings_command(client, message):
    try:
        await client.send_photo(
            chat_id=message.chat.id,
            photo=Var.SETTINGS_PHOTO_URL,
            caption=HOME_CAPTION,
            reply_markup=_kb_home(),
        )
    except Exception:
        # Photo URL unreachable — fall back to a plain text panel
        await message.reply(HOME_CAPTION, reply_markup=_kb_home(), quote=True)


# ── Callback router ───────────────────────────────────────────────────────────
@bot.on_callback_query(filters.regex(r"^s:"))
async def settings_callbacks(client, query):
    # Authorisation gate — settings is owner + admin only.
    from bot.core.database import db as _db
    admin_ids = await _db.get_all_admins()
    if not _is_authorised(query.from_user.id, admin_ids):
        return await query.answer("⛔ Not authorised.", show_alert=True)

    data  = query.data or ""
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    try:
        # ── Top-level navigation ─────────────────────────────────────────────
        if action == "close":
            _pending.pop(query.from_user.id, None)
            try:
                await query.message.delete()
            except Exception:
                pass
            return await query.answer("Closed")

        if action == "home":
            _pending.pop(query.from_user.id, None)
            await _safe_edit_caption(query.message, HOME_CAPTION, _kb_home())
            return await query.answer()

        if action == "menu" and len(parts) >= 3:
            _pending.pop(query.from_user.id, None)
            sub = parts[2]
            if sub == "ban":
                await _safe_edit_caption(query.message, BAN_CAPTION, _kb_ban_menu())
            elif sub == "admin":
                await _safe_edit_caption(query.message, ADMIN_CAPTION, _kb_admin_menu())
            elif sub == "fsub":
                await _safe_edit_caption(query.message, FSUB_CAPTION, _kb_fsub_menu())
            elif sub == "fsub_mode":
                rows = await fsub_mode_picker_data(client)
                if not rows:
                    await _safe_edit_caption(
                        query.message,
                        "<b>⚙️ Force Sub Mode</b>\n\n"
                        "<i>No force-sub channels configured. "
                        "Add one first via <b>Add</b>.</i>",
                        _kb_back("fsub"),
                    )
                else:
                    await _safe_edit_caption(
                        query.message,
                        "<b>⚙️ Force Sub Mode</b>\n\n"
                        "Tap a channel to flip its mode.\n\n"
                        "🟢 = <b>REQUEST</b> — users send a join request\n"
                        "🔴 = <b>NORMAL</b> — users must join directly",
                        _kb_fsub_mode(rows),
                    )
            elif sub == "fsub_relink":
                # Reuse fsub_mode_picker_data — same rows we need (ch_id + label).
                rows = await fsub_mode_picker_data(client)
                if not rows:
                    await _safe_edit_caption(
                        query.message,
                        "<b>🔗 Relink Force-Sub</b>\n\n"
                        "<i>No force-sub channels configured. "
                        "Add one first via <b>Add</b>.</i>",
                        _kb_back("fsub"),
                    )
                else:
                    await _safe_edit_caption(
                        query.message,
                        "<b>🔗 Relink Force-Sub</b>\n\n"
                        "Tap a channel to wipe its cached invite link. "
                        "The next force-sub gate will request a fresh link "
                        "from Telegram with the correct mode flag.\n\n"
                        "<i>Use this after revoking an invite link in "
                        "Telegram, or after switching a channel's mode.</i>",
                        _kb_fsub_relink(rows),
                    )
            elif sub == "ad":
                seconds = await db.get_del_timer()
                await _safe_edit_caption(
                    query.message, _ad_caption(seconds), _kb_ad_menu(),
                )
            elif sub == "dq":
                # Dashboard & Queue lives in its own module so settings.py
                # stays compact — see bot/modules/dashboard.py.
                from bot.modules.dashboard import show_dq_menu
                return await show_dq_menu(client, query)
            elif sub == "rt":
                # RSS & Manual Tasks lives in its own module so settings.py
                # stays compact — see bot/modules/rss_tasks.py.
                from bot.modules.rss_tasks import show_rt_menu
                return await show_rt_menu(client, query)
            elif sub == "cmgr":
                # Channel Management lives in its own module so settings.py
                # stays compact — see bot/modules/channel_manager.py.
                from bot.modules.channel_manager import show_cmgr_menu
                return await show_cmgr_menu(client, query)
            elif sub == "sched":
                # Schedule lives in its own module so settings.py stays
                # compact — see bot/modules/up_posts.py.
                from bot.modules.up_posts import show_sched_menu
                return await show_sched_menu(client, query)
            else:
                return await query.answer("Unknown menu", show_alert=True)
            return await query.answer()

        # ── Stats ────────────────────────────────────────────────────────────
        if action == "users":
            _pending.pop(query.from_user.id, None)
            count = await get_user_count()
            await _safe_edit_caption(
                query.message,
                f"<b>👥 Total Users</b>\n\n"
                f"There are <b>{count}</b> users in the database.",
                _kb_back("home"),
            )
            return await query.answer()

        # ── Lists ────────────────────────────────────────────────────────────
        if action == "list" and len(parts) >= 3:
            _pending.pop(query.from_user.id, None)
            kind = parts[2]
            if kind == "ban":
                body = await render_ban_list(client)
                await _safe_edit_caption(query.message, body, _kb_back("ban"))
            elif kind == "admin":
                body = await render_admin_list(client)
                await _safe_edit_caption(query.message, body, _kb_back("admin"))
            elif kind == "fsub":
                body = await render_fsub_list(client)
                await _safe_edit_caption(query.message, body, _kb_back("fsub"))
            else:
                return await query.answer("Unknown list", show_alert=True)
            return await query.answer()

        # ── Ask-for-input ────────────────────────────────────────────────────
        if action == "ask" and len(parts) >= 3:
            kind = parts[2]
            if kind not in PROMPT_TEXTS:
                return await query.answer("Unknown action", show_alert=True)
            _pending[query.from_user.id] = {
                "action":  kind,
                "chat_id": query.message.chat.id,
                "msg_id":  query.message.id,
                "data":    {},
            }
            await _safe_edit_caption(query.message, PROMPT_TEXTS[kind], _kb_cancel(kind))
            return await query.answer("Send the ID now")

        # ── Cancel pending input ─────────────────────────────────────────────
        if action == "cancel" and len(parts) >= 3:
            parent = parts[2]
            _pending.pop(query.from_user.id, None)
            if parent == "ban":
                await _safe_edit_caption(query.message, BAN_CAPTION, _kb_ban_menu())
            elif parent == "admin":
                await _safe_edit_caption(query.message, ADMIN_CAPTION, _kb_admin_menu())
            elif parent == "fsub":
                await _safe_edit_caption(query.message, FSUB_CAPTION, _kb_fsub_menu())
            elif parent == "ad":
                seconds = await db.get_del_timer()
                await _safe_edit_caption(
                    query.message, _ad_caption(seconds), _kb_ad_menu(),
                )
            return await query.answer("Cancelled")

        # ── Force-sub: pick bot-type after channel ID was validated ──────────
        if action == "fsub_type" and len(parts) >= 3:
            bot_type = parts[2]
            state = _pending.get(query.from_user.id)
            if (not state or state.get("action") != "fsub_add_pick_type"
                or bot_type not in BOT_TYPE_LABELS):
                return await query.answer("Session expired. Start again.", show_alert=True)
            info = state.get("data") or {}
            ch_id = info.get("chat_id")
            title = info.get("title", "—")
            link  = info.get("link", "")
            if ch_id is None:
                return await query.answer("Session expired. Start again.", show_alert=True)
            await fsub_save_channel(ch_id, bot_type)
            _pending.pop(query.from_user.id, None)
            body = (
                f"<b>✅ Force-Sub Channel Added</b>\n\n"
                f"<b>📺 Channel:</b> <a href='{link}'>{title}</a>\n"
                f"<b>🆔 ID:</b> <code>{ch_id}</code>\n"
                f"<b>🤖 Bot:</b> {BOT_TYPE_LABELS[bot_type]}\n"
                f"<b>🔧 Mode:</b> <code>OFF</code> (NORMAL)\n\n"
                f"<i>💡 Use <b>Mode</b> to switch this channel to REQUEST.</i>"
            )
            await _safe_edit_caption(query.message, body, _kb_back("fsub"))
            return await query.answer("Added")

        # ── Force-sub: toggle a channel's mode ──────────────────────────────
        if action == "fsub_toggle" and len(parts) >= 3:
            try:
                ch_id = int(parts[2])
            except ValueError:
                return await query.answer("Bad channel ID.", show_alert=True)
            new_mode, human = await fsub_toggle_channel_mode(ch_id)
            try:
                chat = await client.get_chat(ch_id)
                title = chat.title
            except Exception:
                title = f"Channel {ch_id}"
            body = (
                f"<b>✅ Mode Updated</b>\n\n"
                f"<b>📺 Channel:</b> <code>{title}</code>\n"
                f"<b>🆔 ID:</b> <code>{ch_id}</code>\n"
                f"<b>🔧 New Mode:</b> <code>{human}</code>\n\n"
                + ("<i>Users can now send join requests.</i>" if new_mode == "on"
                   else "<i>Users must now join the channel directly.</i>")
            )
            await _safe_edit_caption(
                query.message, body,
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ Mode List",
                                          callback_data="s:menu:fsub_mode")],
                    [InlineKeyboardButton("⬅️ Back",
                                          callback_data="s:menu:fsub"),
                     InlineKeyboardButton("❌ Close",
                                          callback_data="s:close")],
                ]),
            )
            return await query.answer(f"Mode → {new_mode.upper()}")

        # ── Dashboard & Queue (everything proxied to dashboard.py) ───────────
        if action == "dq":
            from bot.modules.dashboard import handle_dq_action
            return await handle_dq_action(client, query, parts)

        # ── RSS & Manual Tasks (everything proxied to rss_tasks.py) ──────────
        if action == "rt":
            from bot.modules.rss_tasks import handle_rt_action
            return await handle_rt_action(client, query, parts)

        # ── Channel Management (everything proxied to channel_manager.py) ────
        if action == "cmgr":
            from bot.modules.channel_manager import handle_cmgr_action
            return await handle_cmgr_action(
                client, query, parts, _pending, _safe_edit_caption,
            )

        # ── Schedule (everything proxied to up_posts.py) ─────────────────────
        if action == "sched":
            from bot.modules.up_posts import handle_sched_action
            return await handle_sched_action(client, query, parts)

        # ── Force-sub: relink (wipe cached invite link) ──────────────────────
        if action == "fsub_relink" and len(parts) >= 3:
            target = parts[2]
            if target == "all":
                rows = await fsub_mode_picker_data(client)
                cleared = 0
                for r in rows:
                    try:
                        await db.clear_invite_link(r["ch_id"])
                        cleared += 1
                    except Exception:
                        pass
                body = (
                    f"<b>♻️ Relink ALL — Done</b>\n\n"
                    f"Wiped invite-link cache for <b>{cleared}</b> "
                    f"force-sub channel(s).\n\n"
                    f"<i>Next force-sub gate will generate fresh links.</i>"
                )
                await _safe_edit_caption(query.message, body, _kb_back("fsub"))
                return await query.answer(f"Relinked {cleared}")
            try:
                ch_id = int(target)
            except ValueError:
                return await query.answer("Bad channel ID.", show_alert=True)
            try:
                await db.clear_invite_link(ch_id)
            except Exception as e:
                return await query.answer(f"Failed: {e}", show_alert=True)
            try:
                chat = await client.get_chat(ch_id)
                title = chat.title
            except Exception:
                title = f"Channel {ch_id}"
            body = (
                f"<b>♻️ Relink — Done</b>\n\n"
                f"<b>📺 Channel:</b> <code>{title}</code>\n"
                f"<b>🆔 ID:</b> <code>{ch_id}</code>\n\n"
                f"<i>Cached invite link wiped. The next force-sub gate "
                f"will generate a fresh link with the channel's current "
                f"mode flag.</i>"
            )
            await _safe_edit_caption(
                query.message, body,
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Relink List",
                                          callback_data="s:menu:fsub_relink")],
                    [InlineKeyboardButton("⬅️ Back",
                                          callback_data="s:menu:fsub"),
                     InlineKeyboardButton("❌ Close",
                                          callback_data="s:close")],
                ]),
            )
            return await query.answer("Relinked")

        await query.answer()
    except Exception as e:
        try:
            await query.answer(f"Error: {e}", show_alert=True)
        except Exception:
            pass


# ── Text input catcher (only fires when the user has a pending action) ────────
#
# Decorated so that Bot 1 picks it up automatically. (For Bot 2 / Bot 3 the
# same handler is also wired in bot/modules/register_handlers.py via the
# shared-handlers list, since Bot 2 / 3 do not import this module's
# decorators.) Group=3 so it does not fight with the connect-step2 catcher.
@bot.on_message(SETTINGS_INPUT_FILTER, group=3)
@new_task
async def settings_input_catcher(client, message):
    state = _pending.get(message.from_user.id)
    if not state:
        return  # not in a settings flow — let other handlers (if any) act

    text = (message.text or "").strip()
    if not text:
        return

    action  = state["action"]
    chat_id = state["chat_id"]
    msg_id  = state["msg_id"]

    # Always remove the user's typed ID — sensitive stuff should not linger.
    try:
        await message.delete()
    except Exception:
        pass

    # Locate the panel message (for caption editing)
    try:
        panel = await client.get_messages(chat_id, msg_id)
    except Exception:
        panel = None

    # ── RSS & Manual Tasks input — forward to its own module ─────────────────
    # All `rt_*` pending actions are owned by bot/modules/rss_tasks.py so the
    # tasks panel can grow without bloating this dispatcher.
    if action.startswith("rt_"):
        from bot.modules.rss_tasks import handle_rt_input
        return await handle_rt_input(
            client, message, state, panel, _safe_edit_caption, text,
        )

    # ── Channel Management input — forward to its own module ─────────────────
    # All `cmgr_*` pending actions are owned by bot/modules/channel_manager.py
    # so the channel-management panel can grow without bloating this dispatcher.
    if action.startswith("cmgr_"):
        from bot.modules.channel_manager import handle_cmgr_input
        return await handle_cmgr_input(
            client, message, state, panel, _safe_edit_caption, text, _pending,
        )

    async def _show(parent: str, body: str, kb: InlineKeyboardMarkup | None = None):
        markup = kb if kb is not None else _kb_back(parent)
        if panel:
            await _safe_edit_caption(panel, body, markup)
        else:
            await client.send_message(chat_id, body, reply_markup=markup)

    # ── Ban ───────────────────────────────────────────────────────────────────
    if action == "ban":
        try:
            uid = int(text)
        except ValueError:
            return await _show("ban", f"<b>⚠️ Invalid ID</b>\n\n"
                                       f"<code>{text}</code> is not a number.")
        ok, msg = await do_ban(uid)
        _pending.pop(message.from_user.id, None)
        return await _show("ban", f"<b>🚫 Ban</b>\n\n{msg}")

    # ── Unban ─────────────────────────────────────────────────────────────────
    if action == "unban":
        if text.lower() == "all":
            n = await do_unban_all()
            _pending.pop(message.from_user.id, None)
            return await _show("ban",
                f"<b>♻️ Unban All</b>\n\nCleared <b>{n}</b> banned user(s).")
        try:
            uid = int(text)
        except ValueError:
            return await _show("ban", f"<b>⚠️ Invalid ID</b>\n\n"
                                       f"<code>{text}</code> is not a number "
                                       f"(send <code>all</code> to clear all).")
        ok, msg = await do_unban(uid)
        _pending.pop(message.from_user.id, None)
        return await _show("ban", f"<b>♻️ Unban</b>\n\n{msg}")

    # ── Add admin ─────────────────────────────────────────────────────────────
    if action == "add_admin":
        try:
            uid = int(text)
        except ValueError:
            return await _show("admin", f"<b>⚠️ Invalid ID</b>\n\n"
                                         f"<code>{text}</code> is not a number.")
        ok, msg = await do_add_admin(uid)
        _pending.pop(message.from_user.id, None)
        return await _show("admin", f"<b>➕ Add Admin</b>\n\n{msg}")

    # ── Remove admin ──────────────────────────────────────────────────────────
    if action == "del_admin":
        if text.lower() == "all":
            n = await do_remove_all_admins()
            _pending.pop(message.from_user.id, None)
            return await _show("admin",
                f"<b>➖ Remove All</b>\n\nRemoved <b>{n}</b> admin(s).")
        try:
            uid = int(text)
        except ValueError:
            return await _show("admin", f"<b>⚠️ Invalid ID</b>\n\n"
                                         f"<code>{text}</code> is not a number "
                                         f"(send <code>all</code> to clear all).")
        ok, msg = await do_remove_admin(uid)
        _pending.pop(message.from_user.id, None)
        return await _show("admin", f"<b>➖ Remove Admin</b>\n\n{msg}")

    # ── Force Sub: add (step 1 = collect channel ID, then ask bot type) ──────
    if action == "fsub_add":
        ok, result = await fsub_validate_channel(client, text)
        if not ok:
            _pending.pop(message.from_user.id, None)
            return await _show("fsub", f"<b>➕ Add Force-Sub</b>\n\n{result}")
        # result is dict: {chat_id, title, link}
        # Keep state and switch action to 'pick_type'
        state["action"] = "fsub_add_pick_type"
        state["data"]   = result
        body = (
            f"<b>📺 Channel:</b> <a href='{result['link']}'>{result['title']}</a>\n"
            f"<b>🆔 ID:</b> <code>{result['chat_id']}</code>\n\n"
            f"<b>Which bot should enforce this fsub?</b>"
        )
        if panel:
            await _safe_edit_caption(panel, body, _kb_fsub_type_picker())
        return

    # ── Auto Delete: set timer ────────────────────────────────────────────────
    if action == "ad_set":
        try:
            seconds = int(text)
        except ValueError:
            return await _show("ad",
                f"<b>⚠️ Invalid Timer</b>\n\n"
                f"<code>{text}</code> is not a number. "
                f"Send the timer in seconds (minimum <code>10</code>).",
                _kb_ad_menu())
        if seconds < 10:
            return await _show("ad",
                f"<b>⚠️ Too Short</b>\n\n"
                f"Minimum allowed timer is <code>10</code> seconds. "
                f"You sent <code>{seconds}</code>.",
                _kb_ad_menu())
        await db.set_del_timer(seconds)
        _pending.pop(message.from_user.id, None)
        body = (
            f"<b>✅ Auto-Delete Timer Updated</b>\n\n"
            f"<b>New timer:</b> <code>{convertTime(seconds)}</code> "
            f"(<code>{seconds}s</code>)\n\n"
            f"<i>Files sent to users will now be auto-deleted after "
            f"this duration (when AUTO_DEL is enabled).</i>"
        )
        return await _show("ad", body, _kb_ad_menu())

    # ── Force Sub: remove ─────────────────────────────────────────────────────
    if action == "fsub_del":
        if text.lower() == "all":
            n = await fsub_remove_all()
            _pending.pop(message.from_user.id, None)
            return await _show("fsub",
                f"<b>➖ Remove All Force-Sub</b>\n\n"
                f"Cleared <b>{n}</b> channel(s).")
        try:
            ch_id = int(text)
        except ValueError:
            return await _show("fsub",
                f"<b>⚠️ Invalid Channel ID</b>\n\n"
                f"<code>{text}</code> is not a number "
                f"(send <code>all</code> to clear every channel).")
        ok, msg = await fsub_remove_channel(ch_id)
        _pending.pop(message.from_user.id, None)
        return await _show("fsub", f"<b>➖ Remove Force-Sub</b>\n\n{msg}")

    # Unknown — drop state to avoid the panel getting stuck
    _pending.pop(message.from_user.id, None)


def has_pending_settings_input(user_id: int) -> bool:
    """Used by other text catchers to back off when settings is mid-flow."""
    return user_id in _pending
