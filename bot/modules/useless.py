"""
useless.py — /help command (paginated photo + buttons)

The standalone /check_dlt_time command was removed; the same info is
now reached via /settings → ⏱ Auto Delete (see bot/modules/settings.py).
"""

from pyrogram import filters
from pyrogram.filters import command, private
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import MessageNotModified

from bot import bot, Var, admin
from bot.core.database import db
from bot.core.func_utils import new_task, sendMessage, convertTime


# ──────────────────────────────────────────────────────────────────────────────
# /help — photo + paginated category sections
# ──────────────────────────────────────────────────────────────────────────────
HELP_PHOTO = "https://graph.org/file/b4864a63946e9b1e84238-ccb51f7ec7e7c11458.jpg"

HELP_HOME = (
    "<b>📖 Fixed Anime — Help Center</b>\n\n"
    "Self-hosted Telegram bot that auto-downloads, encodes and uploads anime "
    "episodes, batches and movies in multiple resolutions.\n\n"
    "<b>Pick a category to view its commands:</b>\n"
    "• <b>👑 Owner</b> — restart, eval, broadcast, system\n"
    "• <b>⚙️ Admin</b> — RSS, tasks, channels, queue, users\n"
    "• <b>👥 Users</b> — what regular users can do\n\n"
    "Use the <b>◀ Prev</b> / <b>Next ▶</b> buttons to flip pages inside a "
    "category. Tap <b>🏠 Home</b> to return here, or <b>❌ Close</b> to dismiss."
)

# Each section is a list of pages. Caption limit on Telegram is 1024 chars,
# so every page below is kept well under ~950 chars (HTML tags included).
HELP_PAGES = {
    "owner": [
        (
            "<b>👑 Owner Commands</b>\n\n"
            "/restart — Restart the bot\n"
            "/update — Pull latest update from Git\n"
            "/stats — System stats (disk, CPU, RAM)\n"
            "/shell <code>[cmd]</code> — Run shell command\n"
            "/eval <code>[code]</code> — Evaluate Python\n"
            "/log — Get log file\n"
            "/clearlogs — Clear the log file\n\n"
            "<b>Admin Roster</b>\n"
            "/settings → Admins → Add / Remove / List\n"
            "(promote, demote and list bot admins)\n\n"
            "<b>Broadcast</b>\n"
            "/broadcast — Reply to a message to send to all users\n"
            "/pbroadcast — Broadcast and pin\n"
            "/dbroadcast <code>[sec]</code> — Broadcast with auto-delete"
        ),
    ],
    "admin": [
        # ── Page 1: RSS & Tasks ───────────────────────────────────────────────
        (
            "<b>⚙️ Admin Commands</b>\n\n"
            "<b>Dashboard &amp; Queue</b>\n"
            "/settings → 📊 <b>Dashboard &amp; Queue</b>\n"
            "  • Status — bot dashboard (queue, disk, active tasks)\n"
            "  • Queue — current encode/upload queue + clear actions\n"
            "  • Pause / Resume — RSS anime fetching\n"
            "  • Reboot Cache — clear in-memory anime cache\n\n"
            "<b>RSS &amp; Manual Tasks</b>\n"
            "/settings → 📋 <b>RSS &amp; Manual Tasks</b>\n"
            "  • Add RSS Link — register a new feed URL\n"
            "  • Add Task / Retry Task — force-process a feed entry by URL + idx\n"
            "  • Add Magnet — process a magnet or nyaa.si URL\n"
            "  • Process Batch — pick a skipped batch or paste a link\n"
            "  • Retry Failed — re-queue every failed task\n"
            "  • Process Pending — kick all pending tasks now\n"
            "  • Clean Tasks — purge done records older than 7 days"
        ),
        # ── Page 2: Channels, File Links, Users, Misc ─────────────────────────
        (
            "<b>⚙️ Admin Commands</b>\n\n"
            "<b>Channel Management</b>\n"
            "<i>(Channel linking is now under /settings → 📺 Channel "
            "Management — Connect / List / Remove)</i>\n\n"
            "<b>File Links</b>\n"
            "/genlink — Generate a shareable file link\n"
            "/batch — Batch link generator\n"
            "/index — Manage indexed feeds\n\n"
            "<b>Users / Bans / Admins / Force Sub / Auto Delete / Dashboard</b>\n"
            "/settings — Open the inline panel for:\n"
            "  • <b>Ban</b> — Ban / Unban / List\n"
            "  • <b>Admins</b> — Add / Remove / List\n"
            "  • <b>Users</b> — total user count\n"
            "  • <b>Force Sub</b> — Add / Remove / List / Mode / Relink\n"
            "  • <b>Auto Delete</b> — view / set the file-delete timer\n"
            "  • <b>Channel Management</b> — Connect / List / Remove anime → channel\n"
            "  • <b>Dashboard &amp; Queue</b> — status, queue, pause/resume, reboot\n"
            "  • <b>RSS &amp; Manual Tasks</b> — feeds, manual queue, retry / clean\n"
            "  • <b>Schedule</b> — post today's anime release schedule\n"
            "/importusers — Import users from another source"
        ),
    ],
    "users": [
        (
            "<b>👥 User Commands</b>\n\n"
            "/start — Start the bot and get the welcome message\n"
            "/start <code>[token]</code> — Open a shared file/batch link\n\n"
            "<b>How to get files</b>\n"
            "1. Tap an episode / batch link shared by an admin\n"
            "2. The bot opens with <code>/start [token]</code>\n"
            "3. Complete force-subscription if the bot asks\n"
            "4. Receive your file directly in this DM\n\n"
            "<b>Notes</b>\n"
            "• Files may auto-delete after a short time — forward them to "
            "Saved Messages to keep a copy.\n"
            "• If you cannot start the bot, make sure you have joined every "
            "channel listed in the force-sub prompt.\n"
            "• Admin / owner commands are not available to regular users."
        ),
    ],
}

SECTION_LABELS = {
    "owner": "👑 Owner",
    "admin": "⚙️ Admin",
    "users": "👥 Users",
}


def _home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👑 Owner", callback_data="help:sec:owner:0"),
            InlineKeyboardButton("⚙️ Admin", callback_data="help:sec:admin:0"),
        ],
        [InlineKeyboardButton("👥 Users", callback_data="help:sec:users:0")],
        [InlineKeyboardButton("❌ Close", callback_data="help:close")],
    ])


def _section_keyboard(section: str, page: int) -> InlineKeyboardMarkup:
    pages = HELP_PAGES[section]
    total = len(pages)
    nav_row = []
    if total > 1:
        prev_page = (page - 1) % total
        next_page = (page + 1) % total
        nav_row.append(InlineKeyboardButton("◀ Prev", callback_data=f"help:sec:{section}:{prev_page}"))
        nav_row.append(InlineKeyboardButton(f"{page + 1}/{total}", callback_data="help:noop"))
        nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"help:sec:{section}:{next_page}"))

    rows = []
    if nav_row:
        rows.append(nav_row)
    rows.append([
        InlineKeyboardButton("🏠 Home", callback_data="help:home"),
        InlineKeyboardButton("❌ Close", callback_data="help:close"),
    ])
    return InlineKeyboardMarkup(rows)


def _section_caption(section: str, page: int) -> str:
    pages = HELP_PAGES[section]
    page = page % len(pages)
    body = pages[page]
    if len(pages) > 1:
        body = body + f"\n\n<i>Page {page + 1} of {len(pages)} — use ◀ / ▶ to flip</i>"
    # Hard safety cap below Telegram's 1024-char caption limit
    if len(body) > 1020:
        body = body[:1017] + "..."
    return body


@bot.on_message(command('help') & private & admin)
@new_task
async def help_command(client, message):
    try:
        await client.send_photo(
            chat_id=message.chat.id,
            photo=HELP_PHOTO,
            caption=HELP_HOME,
            reply_markup=_home_keyboard(),
        )
    except Exception:
        # Fallback to a plain message if the photo URL is unreachable
        await sendMessage(message, HELP_HOME, buttons=_home_keyboard())


@bot.on_callback_query(filters.regex(r"^help:"))
async def help_callbacks(client, query):
    data = query.data or ""
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    try:
        if action == "noop":
            await query.answer()
            return

        if action == "close":
            await query.answer("Closed")
            try:
                await query.message.delete()
            except Exception:
                pass
            return

        if action == "home":
            try:
                await query.message.edit_caption(caption=HELP_HOME, reply_markup=_home_keyboard())
            except MessageNotModified:
                pass
            await query.answer()
            return

        if action == "sec" and len(parts) >= 4:
            section = parts[2]
            try:
                page = int(parts[3])
            except ValueError:
                page = 0
            if section not in HELP_PAGES:
                await query.answer("Unknown section", show_alert=True)
                return
            try:
                await query.message.edit_caption(
                    caption=_section_caption(section, page),
                    reply_markup=_section_keyboard(section, page),
                )
            except MessageNotModified:
                pass
            await query.answer(SECTION_LABELS.get(section, section))
            return

        await query.answer()
    except Exception as e:
        try:
            await query.answer(f"Error: {e}", show_alert=True)
        except Exception:
            pass
