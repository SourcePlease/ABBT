"""
dashboard.py — /settings → 📊 Dashboard & Queue sub-panel.

This module owns every callback for the Dashboard & Queue section, so
that bot/modules/settings.py stays focused on routing only and does not
balloon. It replaces the standalone admin commands:

  /status     → 📊 Dashboard & Queue → Status
  /queue      → 📊 Dashboard & Queue → Queue
  /clearqueue → 📊 Dashboard & Queue → Queue → Clear Failed/Done/Pending/All
  /pause      → 📊 Dashboard & Queue → Pause Fetching
  /resume     → 📊 Dashboard & Queue → Resume Fetching
  /reboot     → 📊 Dashboard & Queue → Reboot Cache

settings.py forwards two callback patterns into this module:

  s:menu:dq             → show_dq_menu(client, query)
  s:dq:<action>[:args]  → handle_dq_action(client, query, parts)

The full action set is documented inline on each branch of
handle_dq_action().
"""

from datetime import datetime
from shutil import disk_usage
from traceback import format_exc

from pyrogram.errors import MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import ani_cache
from bot.core.database import db
from bot.core.func_utils import convertBytes
from bot.core.reporter import rep
from bot.core.task_queue import task_queue, batch_task_queue


# ── Caption constants ─────────────────────────────────────────────────────────
DQ_MENU_CAPTION = (
    "<b>📊 Dashboard &amp; Queue</b>\n\n"
    "Inspect and control the bot's task pipeline from one place.\n\n"
    "• <b>Status</b> — full dashboard (queue counts, active tasks, "
    "disk, recent done/failed)\n"
    "• <b>Queue</b> — task queue with per-scope clear actions "
    "(Failed / Done / Pending / All)\n"
    "• <b>Pause / Resume</b> — suspend or restart RSS anime fetching\n"
    "• <b>Reboot Cache</b> — wipe the in-memory anime dedup cache "
    "(forces re-detect of every RSS item)\n\n"
    "<i>Pause/Resume only affects new RSS detections — in-flight encodes "
    "and uploads keep running.</i>"
)


# ── Keyboards ─────────────────────────────────────────────────────────────────
def _kb_dq_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Status", callback_data="s:dq:status"),
            InlineKeyboardButton("📋 Queue",  callback_data="s:dq:queue"),
        ],
        [
            InlineKeyboardButton("⏸ Pause Fetching",  callback_data="s:dq:pause"),
            InlineKeyboardButton("▶️ Resume Fetching", callback_data="s:dq:resume"),
        ],
        [InlineKeyboardButton("♻️ Reboot Cache", callback_data="s:dq:reboot")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_status() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="s:dq:status_refresh")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:dq"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_queue() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🗑 Clear Failed",
                                 callback_data="s:dq:queue_pick:clear_failed"),
            InlineKeyboardButton("🗑 Clear Done",
                                 callback_data="s:dq:queue_pick:clear_done"),
        ],
        [
            InlineKeyboardButton("⚠️ Clear Pending",
                                 callback_data="s:dq:queue_pick:clear_pending"),
            InlineKeyboardButton("💣 Clear All",
                                 callback_data="s:dq:queue_pick:clear_all"),
        ],
        [InlineKeyboardButton("🔄 Refresh", callback_data="s:dq:queue_refresh")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:dq"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_scope(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📡 Ongoing",
                                 callback_data=f"s:dq:queue_act:{action}:ongoing"),
            InlineKeyboardButton("📦 Completed",
                                 callback_data=f"s:dq:queue_act:{action}:batch"),
            InlineKeyboardButton("🔀 Both",
                                 callback_data=f"s:dq:queue_act:{action}:both"),
        ],
        [InlineKeyboardButton("« Back to Queue", callback_data="s:dq:queue")],
    ])


def _kb_wipe_confirm(scope: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, wipe",
                             callback_data=f"s:dq:queue_wipe:{scope}"),
        InlineKeyboardButton("❌ Cancel", callback_data="s:dq:queue"),
    ]])


# ── Caption builders ──────────────────────────────────────────────────────────
async def _status_caption() -> str:
    """Full bot status dashboard — queue counts + active + disk + recent."""
    try:
        stats = await task_queue.queue_stats()
    except Exception:
        await rep.report(format_exc(), "error")
        return "<b>❌ Failed to fetch status. Check logs.</b>"

    counts        = stats.get("counts", {})
    active        = stats.get("active", [])
    recent_done   = stats.get("recent_done", [])
    recent_failed = stats.get("recent_failed", [])

    pending   = counts.get("pending", 0)
    enc_count = counts.get("encoding", 0)
    upl_count = counts.get("uploading", 0)
    dl_count  = counts.get("downloading", 0)
    done      = counts.get("done", 0)
    failed    = counts.get("failed", 0)

    try:
        usage = disk_usage("./downloads")
        disk_info = (
            f"💾 <b>Disk:</b> {convertBytes(usage.used)} used / "
            f"{convertBytes(usage.total)} total "
            f"({round(usage.used/usage.total*100, 1)}%)"
        )
    except Exception:
        disk_info = "💾 <b>Disk:</b> unavailable"

    try:
        from bot import ffQueue as ffQ, ongoing_encode_lock, batch_encode_lock
        mem_queue    = ffQ.qsize()
        ongoing_busy = 1 - ongoing_encode_lock._value
        batch_busy   = 1 - batch_encode_lock._value
        encoding_lock = (
            f"🔒 Ongoing: {'encoding' if ongoing_busy else 'idle'} | "
            f"Batch: {'encoding' if batch_busy else 'idle'}"
        )
    except Exception:
        mem_queue     = "?"
        encoding_lock = "?"

    txt  = "<b>🤖 Bot Status Dashboard</b>\n" + ("─" * 28) + "\n\n"
    txt += "<b>📋 Task Queue</b>\n"
    txt += f"  ⏳ Pending:     <code>{pending}</code>\n"
    txt += f"  ⬇️ Downloading: <code>{dl_count}</code>\n"
    txt += f"  ⚙️ Encoding:    <code>{enc_count}</code>\n"
    txt += f"  📤 Uploading:   <code>{upl_count}</code>\n"
    txt += f"  ✅ Done:        <code>{done}</code>\n"
    txt += f"  ❌ Failed:      <code>{failed}</code>\n\n"

    txt += "<b>🔧 Encoder</b>\n"
    txt += f"  Memory queue:  <code>{mem_queue}</code>\n"
    txt += f"  Lock status:   {encoding_lock}\n\n"

    txt += disk_info + "\n\n"

    if active:
        txt += "<b>⚡ Active Tasks</b>\n"
        for t in active[:5]:
            name_short = t['name'][:42] + "…" if len(t['name']) > 42 else t['name']
            dq = ", ".join(t.get('quals_done', [])) or "none"
            txt += (
                f"  • <code>{name_short}</code>\n"
                f"    Status: {t['status']} | Done: {dq} | "
                f"Retries: {t.get('retry_count', 0)}\n"
            )
        txt += "\n"

    if recent_done:
        txt += "<b>✅ Recently Completed</b>\n"
        for t in recent_done[:5]:
            name_short = t['name'][:38] + "…" if len(t['name']) > 38 else t['name']
            when = t.get('updated_at')
            ts   = when.strftime("%d %b %H:%M") if isinstance(when, datetime) else "?"
            quals = ", ".join(t.get('quals_done', [])) or "?"
            txt += f"  • <code>{name_short}</code> [{quals}] @ {ts}\n"
        txt += "\n"

    if recent_failed:
        txt += "<b>❌ Recent Failures</b>\n"
        for t in recent_failed[:3]:
            name_short = t['name'][:38] + "…" if len(t['name']) > 38 else t['name']
            err = (t.get('error') or "unknown")[:55]
            txt += f"  • <code>{name_short}</code>\n    ↳ {err}\n"
        txt += "\n"

    fetch_state = "▶️ RUNNING" if ani_cache.get('fetch_animes', True) else "⏸ PAUSED"
    txt += f"<b>📡 RSS Fetch:</b> <code>{fetch_state}</code>\n"
    txt += f"<i>Last refreshed: {datetime.now().strftime('%H:%M:%S')}</i>"
    return txt


async def _queue_caption() -> str:
    """Combined queue table — Ongoing + Completed (batch) + recent activity."""
    og_stats  = await task_queue.queue_stats()
    og_counts = og_stats.get("counts", {})
    og_active = (og_counts.get("downloading", 0) + og_counts.get("encoding", 0)
                 + og_counts.get("uploading", 0))

    bt_stats  = await batch_task_queue.queue_stats()
    bt_counts = bt_stats.get("counts", {})
    bt_active = (bt_counts.get("downloading", 0) + bt_counts.get("encoding", 0)
                 + bt_counts.get("uploading", 0))

    total   = sum(og_counts.values()) + sum(bt_counts.values())
    active  = og_active + bt_active
    pending = og_counts.get("pending", 0) + bt_counts.get("pending", 0)
    done    = og_counts.get("done", 0)    + bt_counts.get("done", 0)
    failed  = og_counts.get("failed", 0)  + bt_counts.get("failed", 0)

    sep = "─" * 28
    txt = (
        f"<b>📋 Task Queue</b>\n"
        f"<b>{sep}</b>\n"
        f"<b>All</b> — Total: <b>{total}</b>  |  Active: <b>{active}</b>  |  "
        f"Pending: <b>{pending}</b>  |  Done: <b>{done}</b>  |  Failed: <b>{failed}</b>\n"
        f"<b>{sep}</b>\n"
        f"<b>📡 Ongoing</b> — Active: <b>{og_active}</b>  |  "
        f"Pending: <b>{og_counts.get('pending', 0)}</b>  |  "
        f"Done: <b>{og_counts.get('done', 0)}</b>  |  "
        f"Failed: <b>{og_counts.get('failed', 0)}</b>\n"
        f"<b>📦 Completed</b> — Active: <b>{bt_active}</b>  |  "
        f"Pending: <b>{bt_counts.get('pending', 0)}</b>  |  "
        f"Done: <b>{bt_counts.get('done', 0)}</b>  |  "
        f"Failed: <b>{bt_counts.get('failed', 0)}</b>\n"
        f"<b>{sep}</b>\n"
    )

    if og_stats["active"] or bt_stats["active"]:
        txt += "\n<b>🔄 Currently Running:</b>\n"
        for t in og_stats["active"][:5]:
            txt += f"  📡 <code>{t['name'][:42]}</code> [{t['status']}]\n"
        for t in bt_stats["active"][:5]:
            txt += f"  📦 <code>{t['name'][:42]}</code> [{t['status']}]\n"

    all_failed = og_stats["recent_failed"] + bt_stats["recent_failed"]
    if all_failed:
        txt += "\n<b>❌ Recent Failed:</b>\n"
        for t in all_failed[:5]:
            kind = "📦" if t in bt_stats["recent_failed"] else "📡"
            err  = (t.get("error") or "")[:50]
            txt += f"  {kind} <code>{t['name'][:38]}</code>\n    ↳ {err}\n"

    all_done = og_stats["recent_done"] + bt_stats["recent_done"]
    if all_done:
        txt += "\n<b>✅ Recently Done:</b>\n"
        for t in all_done[:5]:
            kind = "📦" if t in bt_stats["recent_done"] else "📡"
            txt += f"  {kind} <code>{t['name'][:42]}</code>\n"

    return txt


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _safe_edit(message, caption: str, reply_markup):
    """Edit a photo-message caption, swallowing harmless 'not modified' errors."""
    try:
        await message.edit_caption(caption=caption, reply_markup=reply_markup)
    except MessageNotModified:
        pass


def _scope_queues(scope: str):
    """Resolve scope keyword to the list of task_queue collections to act on."""
    if scope == "ongoing":
        return [task_queue]
    if scope == "batch":
        return [batch_task_queue]
    return [task_queue, batch_task_queue]  # both


# ── Public entry points (called from settings.py settings_callbacks) ──────────
async def show_dq_menu(client, query):
    """Render the Dashboard & Queue home sub-menu (s:menu:dq)."""
    await _safe_edit(query.message, DQ_MENU_CAPTION, _kb_dq_menu())
    return await query.answer()


async def handle_dq_action(client, query, parts: list):
    """Dispatch every s:dq:<action>[:args] callback.

    parts is the colon-split of query.data, e.g.:
        s:dq:status                          -> ['s','dq','status']
        s:dq:queue_pick:clear_failed         -> ['s','dq','queue_pick','clear_failed']
        s:dq:queue_act:clear_done:ongoing    -> ['s','dq','queue_act','clear_done','ongoing']
        s:dq:queue_wipe:both                 -> ['s','dq','queue_wipe','both']
    """
    if len(parts) < 3:
        return await query.answer("Bad action", show_alert=True)
    action = parts[2]

    # ── Status ────────────────────────────────────────────────────────────────
    if action in ("status", "status_refresh"):
        if action == "status_refresh":
            await query.answer("Refreshing…")
        else:
            await _safe_edit(query.message,
                             "<b>📊 Fetching status…</b>", _kb_status())
        body = await _status_caption()
        await _safe_edit(query.message, body, _kb_status())
        if action != "status_refresh":
            await query.answer()
        return

    # ── Queue ─────────────────────────────────────────────────────────────────
    if action in ("queue", "queue_refresh"):
        if action == "queue_refresh":
            await query.answer("Refreshing…")
        body = await _queue_caption()
        await _safe_edit(query.message, body, _kb_queue())
        if action != "queue_refresh":
            await query.answer()
        return

    # ── Queue: pick scope for a clear-action ──────────────────────────────────
    if action == "queue_pick" and len(parts) >= 4:
        sub = parts[3]
        labels = {
            "clear_failed":  "🗑 Clear Failed Tasks",
            "clear_done":    "🗑 Clear Done Tasks",
            "clear_pending": "⚠️ Clear Pending / Stuck Tasks",
            "clear_all":     "💣 Clear Entire Queue",
        }
        label = labels.get(sub, sub)
        await _safe_edit(query.message,
                         f"<b>{label}</b>\n\nApply to which queue?",
                         _kb_scope(sub))
        return await query.answer()

    # ── Queue: execute a scoped action ────────────────────────────────────────
    if action == "queue_act" and len(parts) >= 5:
        sub_action = parts[3]
        scope      = parts[4]

        # 'clear_all' goes through an explicit wipe-confirmation step.
        if sub_action == "clear_all":
            scope_label = {"ongoing": "📡 Ongoing", "batch": "📦 Completed",
                           "both": "🔀 Both"}.get(scope, scope)
            await _safe_edit(
                query.message,
                f"<b>⚠️ Wipe ALL tasks from {scope_label}?</b>\n"
                f"<i>This cannot be undone.</i>",
                _kb_wipe_confirm(scope),
            )
            return await query.answer()

        deleted = 0
        for tq in _scope_queues(scope):
            col = await tq._col()
            if sub_action == "clear_failed":
                deleted += (await col.delete_many({"status": "failed"})).deleted_count
            elif sub_action == "clear_done":
                deleted += (await col.delete_many({"status": "done"})).deleted_count
            elif sub_action == "clear_pending":
                deleted += (await col.delete_many({"status": {"$in": [
                    "pending", "downloading", "encoding", "uploading"
                ]}})).deleted_count

        labels = {"clear_failed": "failed", "clear_done": "done",
                  "clear_pending": "pending/stuck"}
        await query.answer(
            f"🗑 Deleted {deleted} {labels.get(sub_action, '')} task(s).",
            show_alert=True,
        )
        body = await _queue_caption()
        return await _safe_edit(query.message, body, _kb_queue())

    # ── Queue: confirmed 'wipe ALL' ───────────────────────────────────────────
    if action == "queue_wipe" and len(parts) >= 4:
        scope = parts[3]
        deleted = 0
        for tq in _scope_queues(scope):
            col = await tq._col()
            deleted += (await col.delete_many({})).deleted_count
        await query.answer(f"💣 Wiped {deleted} task(s).", show_alert=True)
        body = await _queue_caption()
        return await _safe_edit(query.message, body, _kb_queue())

    # ── Pause / Resume RSS fetching ───────────────────────────────────────────
    if action == "pause":
        ani_cache['fetch_animes'] = False
        await query.answer("⏸ Paused")
        body = (
            DQ_MENU_CAPTION
            + "\n\n<b>⏸ RSS anime fetching paused.</b>\n"
              "<i>In-flight encodes / uploads are unaffected.</i>"
        )
        return await _safe_edit(query.message, body, _kb_dq_menu())

    if action == "resume":
        ani_cache['fetch_animes'] = True
        await query.answer("▶️ Resumed")
        body = (
            DQ_MENU_CAPTION
            + "\n\n<b>▶️ RSS anime fetching resumed.</b>"
        )
        return await _safe_edit(query.message, body, _kb_dq_menu())

    # ── Reboot in-memory anime cache ──────────────────────────────────────────
    if action == "reboot":
        try:
            await db.reboot()
        except Exception as e:
            return await query.answer(f"Reboot failed: {e}", show_alert=True)
        await query.answer("♻️ Cache cleared")
        body = (
            DQ_MENU_CAPTION
            + "\n\n<b>♻️ In-memory anime cache cleared.</b>\n"
              "<i>RSS items will be re-detected on the next fetch cycle.</i>"
        )
        return await _safe_edit(query.message, body, _kb_dq_menu())

    return await query.answer("Unknown action", show_alert=True)
