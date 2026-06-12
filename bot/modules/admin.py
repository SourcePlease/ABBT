"""
admin.py — Admin management helpers used by /settings → Admins submenu.

The user-facing /add_admin /deladmin /admins text commands have been removed;
all access now goes through the inline /settings panel
(bot/modules/settings.py). The async helpers below are imported by settings.py
and called from the callback / text-input flow.

Once a user is added here, they immediately gain access to every command
gated by the `admin` filter (see bot/__init__.py).
"""

from bot import Var
from bot.core.database import db


async def do_add_admin(uid: int) -> tuple[bool, str]:
    """Promote a user to admin."""
    if uid == Var.OWNER_ID:
        return False, f"⚠️ <code>{uid}</code> is the owner — already privileged."

    existing = await db.get_all_admins()
    if uid in existing:
        return False, f"⚠️ <code>{uid}</code> is already an admin."

    await db.add_admin(uid)
    return True, f"✅ <code>{uid}</code> added as admin."


async def do_remove_admin(uid: int) -> tuple[bool, str]:
    """Demote an admin."""
    existing = await db.get_all_admins()
    if uid not in existing:
        return False, f"⚠️ <code>{uid}</code> is not in the admin list."
    await db.del_admin(uid)
    return True, f"✅ <code>{uid}</code> removed from admins."


async def do_remove_all_admins() -> int:
    """Demote every admin. Returns the count removed."""
    existing = await db.get_all_admins()
    for uid in existing:
        await db.del_admin(uid)
    return len(existing)


async def get_admin_ids() -> list[int]:
    """Return the current list of admin user IDs (excluding the owner)."""
    return await db.get_all_admins()


async def render_admin_list(client) -> str:
    """Build the HTML body for the Admins → List view."""
    admin_ids = await get_admin_ids()
    if not admin_ids:
        return (
            "<b>👮 Admins</b>\n\n"
            "<i>No admins set yet. Use the <b>Add</b> button to add one.</i>"
        )

    lines = []
    for uid in admin_ids:
        try:
            u = await client.get_users(uid)
            mention = f'<a href="tg://user?id={uid}">{u.first_name}</a>'
        except Exception:
            mention = f"<code>{uid}</code>"
        lines.append(f"• {mention} — <code>{uid}</code>")

    return f"<b>👮 Admins ({len(admin_ids)})</b>\n\n" + "\n".join(lines)
