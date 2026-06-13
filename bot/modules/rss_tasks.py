"""
modules/rss_tasks.py

RSS & Manual Tasks sub-panel for /settings.

Owns the entire UI + dispatch for what used to be:
    /addlink         — add an RSS feed URL
    /addtask         — force-process a feed entry
    /rtask           — retry a feed entry
    /addmagnet       — process a magnet / nyaa.si URL directly
    /processbatch    — list & process skipped batches (or a custom link)
    /retryfailed     — re-queue every failed task
    /processpending  — kick all pending tasks now
    /cleantasks      — purge done task records older than 7 days

All eight standalone admin commands were removed; everything is reached
through the inline keyboard registered as the "📋 RSS & Tasks" button on
the /settings home menu (see bot/modules/settings.py).

Public entry points (called from settings.py):
    show_rt_menu(client, query)
    handle_rt_action(client, query, parts)
    handle_rt_input(client, message, state, panel, safe_edit, text)

Pending state actions (set in settings.py's `_pending` dict):
    rt_addlink | rt_addtask | rt_rtask | rt_addmagnet | rt_processbatch
"""

from datetime import datetime, timedelta
from traceback import format_exc

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import MessageNotModified

from bot import bot_loop, Var
from bot.core.func_utils import getfeed
from bot.core.reporter import rep
from bot.core.task_queue import task_queue


# ── Captions ──────────────────────────────────────────────────────────────────
RT_CAPTION = (
    "<b>📋 RSS &amp; Manual Tasks</b>\n\n"
    "Manage RSS feeds and queue tasks by hand.\n\n"
    "• <b>Add RSS Link</b> — register a new RSS feed URL\n"
    "• <b>Add Task</b> — force-process a feed entry by URL + index\n"
    "• <b>Retry Task</b> — retry a feed entry by URL + index\n"
    "• <b>Add Magnet</b> — process a magnet link or nyaa.si URL\n"
    "• <b>Process Batch</b> — pick a skipped batch or paste a link\n"
    "• <b>Retry Failed</b> — re-queue every failed task\n"
    "• <b>Process Pending</b> — kick all pending tasks now\n"
    "• <b>Clean Tasks</b> — purge done task records older than 7 days\n\n"
    "<i>All actions are admin-only.</i>"
)

PROMPT_TEXTS = {
    "addlink": (
        "<b>➕ Send the RSS feed URL.</b>\n\n"
        "Send a single <code>https://</code> URL from an allowed source "
        "(nyaa.si, subsplease.org, erai-raws.info, animetosho.org, etc.).\n\n"
        "Tap <b>Cancel</b> to abort."
    ),
    "addtask": (
        "<b>📋 Send <code>&lt;feed_url&gt; [index]</code>.</b>\n\n"
        "Examples:\n"
        "• <code>https://nyaa.si/?page=rss&amp;u=user</code>\n"
        "• <code>https://nyaa.si/?page=rss&amp;u=user 3</code>\n\n"
        "Index defaults to <code>0</code> (newest entry). "
        "Tap <b>Cancel</b> to abort."
    ),
    "rtask": (
        "<b>🔁 Send <code>&lt;feed_url&gt; [index]</code> to retry.</b>\n\n"
        "Same format as Add Task. The entry is queued again with "
        "<code>force=True</code>. Tap <b>Cancel</b> to abort."
    ),
    "addmagnet": (
        "<b>🧲 Send a magnet link or nyaa.si URL.</b>\n\n"
        "Examples:\n"
        "• <code>magnet:?xt=urn:btih:...</code>\n"
        "• <code>https://nyaa.si/view/123456</code>\n"
        "• <code>https://nyaa.si/download/123456.torrent</code>\n\n"
        "The torrent name is auto-detected. Tap <b>Cancel</b> to abort."
    ),
    "processbatch": (
        "<b>📦 Send a torrent URL or magnet link.</b>\n\n"
        "Examples:\n"
        "• <code>magnet:?xt=urn:btih:...</code>\n"
        "• <code>https://nyaa.si/download/123456.torrent</code>\n\n"
        "<i>Or go back and pick a skipped batch from the inbox list.</i>"
    ),
}


# ── Keyboards ─────────────────────────────────────────────────────────────────
def _kb_rt_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add RSS Link",   callback_data="s:rt:ask:addlink"),
            InlineKeyboardButton("📋 Add Task",      callback_data="s:rt:ask:addtask"),
        ],
        [
            InlineKeyboardButton("🔁 Retry Task",    callback_data="s:rt:ask:rtask"),
            InlineKeyboardButton("🧲 Add Magnet",    callback_data="s:rt:ask:addmagnet"),
        ],
        [
            InlineKeyboardButton("📦 Process Batch", callback_data="s:rt:batch:list:0"),
        ],
        [
            InlineKeyboardButton("♻️ Retry Failed",     callback_data="s:rt:do:retryfailed"),
            InlineKeyboardButton("▶️ Process Pending",  callback_data="s:rt:do:processpending"),
        ],
        [
            InlineKeyboardButton("🗑 Clean Tasks (>7d)", callback_data="s:rt:do:cleantasks"),
        ],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_rt_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✖️ Cancel", callback_data="s:rt:cancel"),
    ]])


def _kb_rt_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:rt"),
        InlineKeyboardButton("🏠 Home",  callback_data="s:home"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ]])


def _kb_batch_list(rows: list, page: int, total_pages: int) -> InlineKeyboardMarkup:
    """One button per skipped batch + paste-link + nav."""
    kb: list[list[InlineKeyboardButton]] = []
    for s in rows:
        label = f"📦 {s['short_id']} — {s['name'][:40]}"
        kb.append([InlineKeyboardButton(
            label[:60], callback_data=f"s:rt:batch:proc:{s['short_id']}",
        )])

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️ Prev",
                callback_data=f"s:rt:batch:list:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}",
            callback_data="s:rt:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Next ▶️",
                callback_data=f"s:rt:batch:list:{page+1}"))
        kb.append(nav)

    kb.append([InlineKeyboardButton("📝 Paste Custom Link",
        callback_data="s:rt:ask:processbatch")])
    kb.append([
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:rt"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ])
    return InlineKeyboardMarkup(kb)


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _safe_edit(message, caption: str, reply_markup):
    try:
        await message.edit_caption(caption=caption, reply_markup=reply_markup)
    except MessageNotModified:
        pass


_ALLOWED_RSS_PREFIXES = (
    "https://nyaa.si", "https://subsplease.org", "https://erai-raws.info",
    "https://animetosho.org", "https://www.tokyotosho.info",
    "https://animetime.cc", "https://horriblesubs.info",
)


# ── Action implementations (return body string for the panel) ─────────────────
async def _do_addlink(text: str) -> str:
    url = text.strip()
    if not url.startswith("https://"):
        return "<b>❌ Only HTTPS URLs are allowed.</b>"
    if not any(url.startswith(p) for p in _ALLOWED_RSS_PREFIXES):
        return (
            "<b>⚠️ Domain not in allowlist.</b>\n"
            "<b>Allowed:</b> nyaa.si, subsplease.org, erai-raws.info, animetosho.org\n"
            "<b>Contact bot owner to add new domains.</b>"
        )
    Var.RSS_ITEMS.append(url)
    return (
        f"<b>✅ RSS link added.</b>\n"
        f"<b>Active feeds:</b> {len(Var.RSS_ITEMS)}\n\n"
        f"<code>{url}</code>"
    )


async def _do_addtask(text: str, retry: bool = False) -> str:
    args = text.split()
    if not args:
        return "<b>⚠️ Empty input. Send <code>&lt;url&gt; [idx]</code>.</b>"
    url = args[0]
    index = int(args[1]) if len(args) > 1 and args[1].isdigit() else 0
    info = await getfeed(url, index)
    if not info:
        return "<b>❌ No task found at that feed/index.</b>"
    from bot.core.auto_animes import get_animes
    bot_loop.create_task(get_animes(info.title, info.link, True))
    label = "Task retried" if retry else "Task added"
    return f"<b>✅ {label}:</b> <code>{info.title}</code>"


async def _do_addmagnet(text: str) -> str:
    from urllib.parse import parse_qs, urlparse, unquote

    magnet = text.strip()
    if not magnet:
        return "<b>⚠️ Empty input.</b>"

    name = "Unknown Anime"
    try:
        if magnet.startswith("magnet:"):
            parsed = parse_qs(urlparse(magnet).query)
            name = unquote(parsed['dn'][0]) if 'dn' in parsed else "Unknown Anime"
        elif "nyaa.si" in magnet:
            try:
                import re as _re
                from aiohttp import ClientSession as _CS
                _dl_id = _re.search(r'/download/(\d+)', magnet)
                view_url = (f"https://nyaa.si/view/{_dl_id.group(1)}"
                            if _dl_id else magnet)
                async with _CS() as _sess:
                    async with _sess.get(view_url, timeout=10) as _r:
                        _html = await _r.text()
                _title_m = _re.search(r'<title>(.*?)</title>', _html,
                                      _re.IGNORECASE)
                if _title_m:
                    name = _re.sub(r'\s*(::|-)?\s*Nyaa\s*$', '',
                                   _title_m.group(1),
                                   flags=_re.IGNORECASE).strip()
                if not name or name == "Unknown Anime":
                    _h = _re.search(
                        r'<h3[^>]*class="panel-title"[^>]*>(.*?)</h3>',
                        _html, _re.DOTALL,
                    )
                    if _h:
                        name = _re.sub(r'<[^>]+>', '', _h.group(1)).strip()
            except Exception as _e:
                await rep.report(f"Nyaa title fetch failed: {_e}", "warning")
    except Exception:
        pass

    from bot.core.auto_animes import get_animes
    bot_loop.create_task(get_animes(name, magnet, True))
    return f"<b>✅ Magnet task started:</b> <code>{name}</code>"


async def _do_processbatch_link(text: str) -> str:
    """Process a custom torrent URL / magnet pasted by the admin."""
    torrent = text.strip()
    if not torrent:
        return "<b>⚠️ Empty input.</b>"

    name = (torrent.split('dn=')[1].split('&')[0] if 'dn=' in torrent
            else torrent.split('/')[-1])
    name = name.replace('+', ' ').replace('%20', ' ')[:100]

    from bot.core.auto_animes import process_batch_torrent
    bot_loop.create_task(process_batch_torrent(name, torrent))
    return (
        f"<b>📦 Batch torrent queued!</b>\n\n"
        f"<b>Name:</b> <code>{name}</code>\n\n"
        f"<i>Processing will begin shortly...</i>"
    )


async def _do_processbatch_id(short_id: str) -> str:
    """Process a skipped batch by short ID."""
    from bot.core.auto_animes import process_batch_torrent
    from bot.core.database import batch_db   # FIXED: use batch_db instead of db

    short_id = short_id.zfill(3)
    doc = await batch_db.get_skipped_batch(short_id)   # FIXED
    if not doc:
        return (
            f"<b>❌ No skipped batch with ID <code>{short_id}</code>.</b>\n"
            f"<i>It may have been processed already.</i>"
        )
    torrent = doc["torrent"]
    name = doc["name"]
    await batch_db.delete_skipped_batch(short_id)   # FIXED

    bot_loop.create_task(process_batch_torrent(name, torrent))
    return (
        f"<b>📦 Batch <code>{short_id}</code> queued!</b>\n\n"
        f"<b>Name:</b> <code>{name}</code>\n\n"
        f"<i>Processing will begin shortly...</i>"
    )


async def _do_retryfailed() -> str:
    try:
        col = await task_queue._col()
        result = await col.update_many(
            {"status": "failed", "retry_count": {"$lt": 10}},
            {"$set": {"status": "pending", "retry_count": 0}},
        )
        count = result.modified_count

        resumable = await task_queue.get_resumable_tasks()
        from bot.core.auto_animes import get_animes
        for task_doc in resumable:
            bot_loop.create_task(get_animes(
                task_doc["name"], task_doc["torrent"],
                force=True, task_id=str(task_doc["_id"]),
                source_priority=task_doc.get("source_priority", 1),
                quals_done=task_doc.get("quals_done", []),
            ))
        return (
            f"♻️ <b>Re-queued {count} failed task(s).</b>\n"
            f"<i>They will be picked up on the next fetch cycle.</i>"
        )
    except Exception:
        await rep.report(format_exc(), "error")
        return "<b>❌ Error re-queuing failed tasks. Check logs.</b>"


async def _do_processpending() -> str:
    from bot.core.auto_animes import get_animes
    from bot.core.task_queue import (
        task_queue as _tq, batch_task_queue as _btq,
    )

    try:
        total = 0
        lines = []

        og_col = await _tq._col()
        og_pending = await og_col.find(
            {"status": "pending", "retry_count": {"$lt": 3}},
        ).to_list(length=50)
        for t in og_pending:
            bot_loop.create_task(get_animes(
                t["name"], t["torrent"],
                force=True, task_id=str(t["_id"]),
                source_priority=t.get("source_priority", 1),
                quals_done=t.get("quals_done", []),
            ))
            total += 1
            lines.append(f"📡 <code>{t['name'][:50]}</code>")

        bt_col = await _btq._col()
        bt_pending = await bt_col.find(
            {"status": "pending", "retry_count": {"$lt": 3}},
        ).to_list(length=50)
        for t in bt_pending:
            bot_loop.create_task(get_animes(
                t["name"], t["torrent"],
                force=True, task_id=str(t["_id"]),
                source_priority=t.get("source_priority", 1),
                quals_done=t.get("quals_done", []),
                is_batch=True,
            ))
            total += 1
            lines.append(f"📦 <code>{t['name'][:50]}</code>")

        if total == 0:
            return "✅ <b>No pending tasks found.</b>"

        preview = "\n".join(lines[:10])
        if total > 10:
            preview += f"\n<i>...and {total - 10} more</i>"
        return f"▶️ <b>Resuming {total} pending task(s):</b>\n\n{preview}"
    except Exception:
        await rep.report(format_exc(), "error")
        return "<b>❌ Error processing pending tasks.</b>"


async def _do_cleantasks() -> str:
    try:
        col = await task_queue._col()
        cutoff = datetime.utcnow() - timedelta(days=7)
        result = await col.delete_many(
            {"status": "done", "updated_at": {"$lt": cutoff}},
        )
        return (
            f"🗑️ <b>Cleaned {result.deleted_count} completed task record(s)</b> "
            f"(older than 7 days)."
        )
    except Exception:
        await rep.report(format_exc(), "error")
        return "<b>❌ Error cleaning tasks. Check logs.</b>"


# ── Public entry points ───────────────────────────────────────────────────────
async def show_rt_menu(client, query):
    """Render the RSS & Tasks home menu (called from settings.py menu router)."""
    # Settings.py already pops _pending before forwarding here.
    await _safe_edit(query.message, RT_CAPTION, _kb_rt_menu())
    return await query.answer()


async def _show_batch_list(client, query, page: int = 0):
    from bot.core.database import batch_db   # FIXED: use batch_db instead of db
    skipped = await batch_db.get_all_skipped_batches()   # FIXED

    if not skipped:
        body = (
            "<b>📦 Process Batch</b>\n\n"
            "<i>No skipped batches in the inbox.</i>\n\n"
            "Tap <b>Paste Custom Link</b> to feed a torrent URL or magnet "
            "directly."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Paste Custom Link",
                callback_data="s:rt:ask:processbatch")],
            [
                InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:rt"),
                InlineKeyboardButton("❌ Close", callback_data="s:close"),
            ],
        ])
        await _safe_edit(query.message, body, kb)
        return await query.answer()

    PER_PAGE = 8
    total_pages = max(1, (len(skipped) + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PER_PAGE
    rows = skipped[start:start + PER_PAGE]

    body = (
        f"<b>📦 Skipped Batch Inbox</b>\n\n"
        f"<b>{len(skipped)}</b> batch(es) waiting. Tap one to process, or "
        f"<b>Paste Custom Link</b> for a direct URL/magnet."
    )
    await _safe_edit(query.message, body, _kb_batch_list(rows, page, total_pages))
    return await query.answer()


async def handle_rt_action(client, query, parts: list):
    """Handle every `s:rt:*` callback."""
    # `_pending` lives in settings.py — import lazily to avoid a circular import.
    from bot.modules.settings import _pending

    if len(parts) < 3:
        return await query.answer()

    sub = parts[2]

    # ── Ask-for-input ────────────────────────────────────────────────────────
    if sub == "ask" and len(parts) >= 4:
        kind = parts[3]
        if kind not in PROMPT_TEXTS:
            return await query.answer("Unknown action", show_alert=True)
        _pending[query.from_user.id] = {
            "action":  f"rt_{kind}",
            "chat_id": query.message.chat.id,
            "msg_id":  query.message.id,
            "data":    {},
        }
        await _safe_edit(query.message, PROMPT_TEXTS[kind], _kb_rt_cancel())
        return await query.answer("Send input now")

    # ── Cancel ───────────────────────────────────────────────────────────────
    if sub == "cancel":
        _pending.pop(query.from_user.id, None)
        await _safe_edit(query.message, RT_CAPTION, _kb_rt_menu())
        return await query.answer("Cancelled")

    # ── One-tap actions ──────────────────────────────────────────────────────
    if sub == "do" and len(parts) >= 4:
        kind = parts[3]
        _pending.pop(query.from_user.id, None)
        await query.answer("Working...")
        if kind == "retryfailed":
            body = await _do_retryfailed()
        elif kind == "processpending":
            body = await _do_processpending()
        elif kind == "cleantasks":
            body = await _do_cleantasks()
        else:
            return await query.answer("Unknown action", show_alert=True)
        await _safe_edit(query.message, body, _kb_rt_back())
        return

    # ── Batch flow ───────────────────────────────────────────────────────────
    if sub == "batch" and len(parts) >= 4:
        op = parts[3]
        if op == "list":
            page = int(parts[4]) if len(parts) >= 5 and parts[4].isdigit() else 0
            return await _show_batch_list(client, query, page)
        if op == "proc" and len(parts) >= 5:
            short_id = parts[4]
            await query.answer("Queued")
            body = await _do_processbatch_id(short_id)
            await _safe_edit(query.message, body, _kb_rt_back())
            return

    # ── No-op (page indicator) ───────────────────────────────────────────────
    if sub == "noop":
        return await query.answer()

    return await query.answer()


async def handle_rt_input(client, message, state, panel, safe_edit, text: str):
    """Process a text reply for a pending `rt_*` action.

    Called from settings.py's `settings_input_catcher` when
    `state['action']` starts with `rt_`. The caller has already deleted
    the user's message and resolved the panel (may be None).
    """
    from bot.modules.settings import _pending

    action = state["action"]
    chat_id = state["chat_id"]

    async def _show(body: str, kb: InlineKeyboardMarkup | None = None):
        markup = kb if kb is not None else _kb_rt_back()
        if panel:
            await safe_edit(panel, body, markup)
        else:
            await client.send_message(chat_id, body, reply_markup=markup)

    if action == "rt_addlink":
        body = await _do_addlink(text)
    elif action == "rt_addtask":
        body = await _do_addtask(text, retry=False)
    elif action == "rt_rtask":
        body = await _do_addtask(text, retry=True)
    elif action == "rt_addmagnet":
        body = await _do_addmagnet(text)
    elif action == "rt_processbatch":
        body = await _do_processbatch_link(text)
    else:
        body = "<b>⚠️ Unknown action — session reset.</b>"

    _pending.pop(message.from_user.id, None)
    await _show(body)
