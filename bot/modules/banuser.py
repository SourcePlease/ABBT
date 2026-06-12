"""
banuser.py — Ban management helpers used by /settings → Ban submenu.

The user-facing /ban /unban /banlist text commands have been removed; all
access now goes through the inline /settings panel (bot/modules/settings.py).
The async helpers below are imported by settings.py and called from the
callback / text-input flow.
"""

from bot import Var
from bot.core.database import db


async def do_ban(uid: int) -> tuple[bool, str]:
    """
    Ban a single user ID.
    Returns (ok, message). `ok=False` carries a human-readable reason.
    """
    admin_ids  = await db.get_all_admins()
    banned_ids = await db.get_ban_users()

    if uid == Var.OWNER_ID:
        return False, f"⛔ <code>{uid}</code> is the owner — refused."
    if uid in admin_ids:
        return False, f"⛔ <code>{uid}</code> is an admin — refused."
    if uid in banned_ids:
        return False, f"⚠️ <code>{uid}</code> is already banned."

    await db.add_ban_user(uid)
    return True, f"✅ <code>{uid}</code> banned."


async def do_unban(uid: int) -> tuple[bool, str]:
    """Unban a single user ID."""
    banned_ids = await db.get_ban_users()
    if uid not in banned_ids:
        return False, f"⚠️ <code>{uid}</code> is not in the ban list."
    await db.del_ban_user(uid)
    return True, f"✅ <code>{uid}</code> unbanned."


async def do_unban_all() -> int:
    """Unban every user. Returns the count cleared."""
    banned_ids = await db.get_ban_users()
    for uid in banned_ids:
        await db.del_ban_user(uid)
    return len(banned_ids)


async def get_ban_list() -> list[int]:
    """Return the current list of banned user IDs."""
    return await db.get_ban_users()


async def render_ban_list(client) -> str:
    """Build the HTML body for the Ban → List view."""
    banned_ids = await get_ban_list()
    if not banned_ids:
        return "<b>🚫 Ban list</b>\n\n<i>No users are currently banned.</i>"

    lines = []
    for uid in banned_ids:
        try:
            user = await client.get_users(uid)
            mention = f'<a href="tg://user?id={uid}">{user.first_name}</a>'
        except Exception:
            mention = "Unknown"
        lines.append(f"• {mention} — <code>{uid}</code>")

    return f"<b>🚫 Banned Users ({len(banned_ids)})</b>\n\n" + "\n".join(lines)
