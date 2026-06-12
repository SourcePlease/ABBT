"""
dev.py — Owner-only developer tools: shell, eval, stats, update
"""

import io
import re
import os
import asyncio
import subprocess
import contextlib
import platform
from datetime import datetime

import psutil
import aiofiles
from asyncio.subprocess import PIPE, create_subprocess_exec
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, Var, bot_loop
from bot.core.func_utils import new_task

BOOT_TIME = datetime.fromtimestamp(psutil.boot_time())


def _fmt(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024


async def _run(*cmd):
    proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    return proc.returncode == 0, (stdout or stderr).decode().strip()


# ── /shell ─────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("shell") & filters.user(Var.OWNER_ID))
async def shell_handler(client, message):
    parts = message.text.split(None, 1)
    if len(parts) == 1:
        return await message.reply("<b>Usage: /shell [command]</b>")
    # FIX: was `subprocess.getoutput(parts[1])` — a synchronous call that
    # blocks the asyncio event loop for the entire duration of the shell
    # command. Long-running owner commands (`tar`, `du -sh /`, `apt update`)
    # would freeze the bot — schedulers miss runs, encoders stall, RSS
    # fetcher silently drops episodes. Use create_subprocess_shell so the
    # event loop stays responsive and other tasks keep ticking.
    proc = await asyncio.create_subprocess_shell(
        parts[1], stdout=PIPE, stderr=PIPE,
    )
    stdout, stderr = await proc.communicate()
    result = (stdout or stderr).decode(errors="replace").strip() or "✅ No output."
    if len(result) > 4000:
        return await message.reply_document(
            ("output.txt", result.encode()), caption="Output too long."
        )
    await message.reply(f"<pre>{result}</pre>")


# ── /eval ──────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("eval") & filters.user(Var.OWNER_ID))
async def eval_handler(client, message):
    text = message.text
    match = re.search(r"```(?:python)?\n([\s\S]+?)```", text)
    code = match.group(1) if match else text.split(None, 1)[1] if len(text.split(None, 1)) > 1 else None
    if not code:
        return await message.reply("<b>Usage: /eval [code]</b>")
    buf = io.StringIO()
    # Restrict builtins: block __import__ and dangerous builtins to reduce RCE escalation
    _safe_builtins = {k: v for k, v in __builtins__.items() if k not in (
        "__import__", "open", "compile", "exec", "eval", "breakpoint",
        "__loader__", "__spec__", "__build_class__"
    )} if isinstance(__builtins__, dict) else {}
    _safe_globals = {
        "__builtins__": _safe_builtins,
        "bot": bot, "client": client, "message": message,
    }
    try:
        with contextlib.redirect_stdout(buf):
            exec(code, _safe_globals)
        output = buf.getvalue() or "✅ No output."
    except Exception as e:
        output = f"❌ {e}"
    if len(output) > 4000:
        return await message.reply_document(("eval.txt", output.encode()), caption="Output too long.")
    await message.reply(f"<pre>{output}</pre>")


# ── /stats ─────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("stats") & filters.user(Var.OWNER_ID))
async def stats_handler(client, message):
    disk  = psutil.disk_usage("/")
    mem   = psutil.virtual_memory()
    net   = psutil.net_io_counters()
    uptime = datetime.now() - BOOT_TIME
    up_str = f"{uptime.days*24 + uptime.seconds//3600}h {uptime.seconds//60%60}m"

    ffver = subprocess.getoutput("ffmpeg -version").split('\n')[0].split()[2] if subprocess.getoutput("which ffmpeg") else "N/A"

    # FIX: psutil.cpu_percent(interval=1) blocks the event loop for 1 second,
    # freezing every other coroutine (RSS scans, encoding progress edits, user
    # commands) while /stats runs. Run it on a worker thread instead.
    cpu_pct = await asyncio.to_thread(psutil.cpu_percent, 1)

    text = (
        "<b>📊 System Stats</b>\n\n"
        f"<b>💾 Disk:</b> {_fmt(disk.used)} / {_fmt(disk.total)} ({disk.percent}%)\n"
        f"<b>🧠 RAM:</b> {_fmt(mem.used)} / {_fmt(mem.total)} ({mem.percent}%)\n"
        f"<b>⚡ CPU:</b> {cpu_pct}% ({psutil.cpu_count()} cores)\n"
        f"<b>🌐 Net:</b> ↑{_fmt(net.bytes_sent)} ↓{_fmt(net.bytes_recv)}\n"
        f"<b>🐍 Python:</b> {platform.python_version()}\n"
        f"<b>🎬 FFmpeg:</b> {ffver}\n"
        f"<b>⏱ Uptime:</b> {up_str}"
    )
    await message.reply(text)


# ── /update ────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("update") & filters.user(Var.OWNER_ID))
async def update_handler(client, message):
    repo = os.getenv("UPSTREAM_REPO")
    branch = os.getenv("UPSTREAM_BRANCH", "main")
    if not repo:
        return await message.reply("<b>⚠️ UPSTREAM_REPO not set.</b>")

    status = await message.reply("<i>Checking for updates...</i>")

    if not os.path.exists(".git"):
        await _run("git", "init", "-q")
        await _run("git", "remote", "add", "origin", repo)

    await _run("git", "fetch", "origin", branch)
    _, local  = await _run("git", "rev-parse", "HEAD")
    _, remote = await _run("git", "rev-parse", f"origin/{branch}")

    if local.strip() == remote.strip():
        return await status.edit("<b>✅ Already up to date.</b>")

    await status.edit("<b>📥 Update found, applying...</b>")
    ok, out = await _run("git", "reset", "--hard", f"origin/{branch}")
    if not ok:
        return await status.edit(f"<b>❌ Failed:</b>\n<pre>{out}</pre>")

    _, commit = await _run("git", "log", "-1", "--pretty=format:%h - %s")
    _, diff   = await _run("git", "diff", "--stat", "HEAD~1..HEAD")

    async with aiofiles.open(".restartmsg", "w") as f:
        await f.write(f"{status.chat.id}\n{status.id}\n")

    await status.edit(
        f"<b>✅ Updated!</b>\n\n<b>Commit:</b> <code>{commit}</code>\n\n"
        f"<b>Changes:</b>\n<pre>{diff}</pre>\n\n<i>Restarting...</i>"
    )
    os.execvp("python3", ["python3", "-m", "bot"])


# ── /downloads ─────────────────────────────────────────────────────────────

# Path registry — maps short numeric IDs to full paths.
# Telegram limits callback_data to 64 bytes so we can never put full paths there.
# Capped at 500 entries (evicts oldest) to prevent unbounded memory growth.
_dl_path_registry: dict = {}
_dl_path_counter:  int  = 0
_DL_REG_MAX = 500

def _dl_reg(path: str) -> str:
    """Register a path and return its short numeric ID string."""
    global _dl_path_counter
    for k, v in _dl_path_registry.items():
        if v == path:
            return k
    # Evict oldest entry when cap is reached
    if len(_dl_path_registry) >= _DL_REG_MAX:
        oldest_key = next(iter(_dl_path_registry))
        del _dl_path_registry[oldest_key]
    _dl_path_counter += 1
    key = str(_dl_path_counter)
    _dl_path_registry[key] = path
    return key

def _dl_get(key: str) -> str:
    """Resolve a short ID back to its full path."""
    return _dl_path_registry.get(key, "./downloads")


@bot.on_message(filters.command("downloads") & filters.user(Var.OWNER_ID))
async def downloads_handler(client, message):
    """Browse the downloads folder tree via inline buttons."""
    await _send_folder_view(message, "./downloads", reply=False)


@bot.on_callback_query(filters.user(Var.OWNER_ID) & filters.regex(r"^dlb:"))
async def downloads_browse(client, callback):
    path = _dl_get(callback.data.split("dlb:", 1)[1])
    await _send_folder_view(callback.message, path, reply=True)
    await callback.answer()


@bot.on_callback_query(filters.user(Var.OWNER_ID) & filters.regex(r"^dlu:"))
async def downloads_back(client, callback):
    path = _dl_get(callback.data.split("dlu:", 1)[1])
    await _send_folder_view(callback.message, path, reply=True)
    await callback.answer()


@bot.on_callback_query(filters.user(Var.OWNER_ID) & filters.regex(r"^dld:"))
async def downloads_delete(client, callback):
    import shutil
    target = _dl_get(callback.data.split("dld:", 1)[1])
    parent = os.path.dirname(target.rstrip("/"))
    if not parent or not os.path.exists(parent):
        parent = "./downloads"
    try:
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
        await callback.answer("✅ Deleted")
    except Exception as e:
        await callback.answer(f"❌ {e}", show_alert=True)
        return
    await _send_folder_view(callback.message, parent, reply=True)


async def _send_folder_view(msg_or_edit, path: str, reply: bool):
    """Render a folder listing as inline buttons and send/edit the message."""
    path = os.path.normpath(path)

    _allowed_roots = (
        os.path.normpath("./downloads"),
        os.path.normpath("./encode"),
    )
    if not any(path.startswith(r) for r in _allowed_roots):
        path = "./downloads"

    if not os.path.isdir(path):
        text = f"<b>❌ Not a directory:</b> <code>{path}</code>"
        if reply:
            await msg_or_edit.edit_text(text)
        else:
            await msg_or_edit.reply(text)
        return

    try:
        entries = sorted(os.scandir(path), key=lambda e: (not e.is_dir(), e.name.lower()))
    except PermissionError:
        entries = []

    total_size = 0
    file_count  = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            try:
                total_size += os.path.getsize(os.path.join(root, f))
                file_count  += 1
            except Exception:
                pass

    _rel  = os.path.relpath(path, ".")
    _dash = "─" * 28
    text  = f"<b>📁 {_rel}</b>\n<b>{_dash}</b>\n<b>Files:</b> {file_count}  |  <b>Size:</b> {_fmt(total_size)}\n<b>{_dash}</b>"

    buttons = []
    for entry in entries:
        size_str = ""
        if entry.is_file():
            try:
                size_str = f"  [{_fmt(entry.stat().st_size)}]"
            except Exception:
                pass
        icon  = "📁" if entry.is_dir() else "🎬" if entry.name.endswith((".mkv", ".mp4", ".avi")) else "📄"
        label = f"{icon} {entry.name}{size_str}"
        # Register path to get a short ID — keeps callback_data well under 64 bytes
        pid = _dl_reg(entry.path)
        row = []
        if entry.is_dir():
            row.append(InlineKeyboardButton(label, callback_data=f"dlb:{pid}"))
        else:
            row.append(InlineKeyboardButton(label, callback_data="dl_noop"))
        row.append(InlineKeyboardButton("🗑", callback_data=f"dld:{pid}"))
        buttons.append(row)

    nav = []
    parent = os.path.dirname(path)
    _allowed_roots_norm = [os.path.normpath("./downloads"), os.path.normpath("./encode")]
    if path not in _allowed_roots_norm and os.path.isdir(parent):
        ppid = _dl_reg(parent)
        nav.append(InlineKeyboardButton("⬆️ Up", callback_data=f"dlu:{ppid}"))
    cur_pid = _dl_reg(path)
    nav.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"dlb:{cur_pid}"))
    nav.append(InlineKeyboardButton("❌ Close",   callback_data="close"))
    buttons.append(nav)

    kb = InlineKeyboardMarkup(buttons)

    if reply:
        try:
            await msg_or_edit.edit_text(text, reply_markup=kb)
        except Exception:
            await msg_or_edit.reply(text, reply_markup=kb)
    else:
        await msg_or_edit.reply(text, reply_markup=kb)


@bot.on_callback_query(filters.regex("^dl_noop$"))
async def dl_noop(client, callback):
    await callback.answer("This is a file — use 🗑 to delete it.")


# ── /fixbatchlink ───────────────────────────────────────────────────────────

@bot.on_message(filters.command("fixbatchlink") & filters.user(Var.OWNER_ID))
async def fixbatchlink_handler(client, message):
    """
    Usage: /fixbatchlink <anime name>
    Regenerates correct quality buttons from DB and edits the channel post.
    Use this when links were generated with the wrong file store channel.
    """
    from bot.core.database import batch_db
    from bot.core.func_utils import encode as _enc
    from bot.core.auto_animes import QUAL_LABELS
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    args = message.text.split(None, 1)
    if len(args) < 2:
        return await message.reply(
            "<b>Usage:</b> <code>/fixbatchlink Anime Name</code>\n\n"
            "Regenerates quality buttons on the channel post using the correct file store."
        )

    anime_name = args[1].strip()
    status = await message.reply(
        f"<b>Searching batch data for:</b> <code>{anime_name}</code>"
    )

    try:
        if batch_db.db is None:
            await batch_db.connect()

        from bot.core.database import db as _main_db
        if _main_db.db is None:
            await _main_db.connect()

        # Step 1: Resolve ani_id via AniList — most reliable
        _ani_id    = None
        batch_data = None
        try:
            from bot.core.text_utils import AniLister
            _anidata = await AniLister(anime_name).get_anidata()
            _ani_id  = _anidata.get("id")
        except Exception:
            pass

        # Step 2: Search batch_links in batch_db first, then main_db
        if _ani_id:
            batch_data = await batch_db.db["batch_links"].find_one({"ani_id": _ani_id})
            if not batch_data:
                batch_data = await _main_db.db["batch_links"].find_one({"ani_id": _ani_id})

        # Step 3: Fuzzy scan all batch_links in both DBs as last resort
        if not batch_data:
            _sl = anime_name.lower()
            for _bdb in (batch_db.db, _main_db.db):
                async for _doc in _bdb["batch_links"].find({}):
                    _t = (str(_doc.get("ani_id", "")) + " " +
                          _doc.get("anime_title", "")).lower()
                    if _sl in _t or any(w in _t for w in _sl.split()):
                        batch_data = _doc
                        _ani_id    = _doc.get("ani_id")
                        break
                if batch_data:
                    break

        # Step 4: Still nothing — show all stored entries
        if not batch_data:
            _all = []
            for _bdb in (batch_db.db, _main_db.db):
                _all += await _bdb["batch_links"].find(
                    {}, sort=[("updated_at", -1)]
                ).to_list(length=15)
            _lines = "\n".join(
                f"ani_id <code>{b.get('ani_id')}</code> — {str(b.get('updated_at',''))[:16]}"
                for b in _all[:15]
            )
            _hint = f"\n<b>AniList resolved:</b> <code>{_ani_id}</code>" if _ani_id else ""
            return await status.edit(
                f"<b>No batch data found for:</b> <code>{anime_name}</code>{_hint}\n\n"
                f"<b>Stored entries:</b>\n{_lines}"
            )

    except Exception as e:
        return await status.edit(f"<b>DB error:</b> <code>{e}</code>")

    file_store = batch_data.get("file_store") or Var.BATCH_FILE_STORE
    _abs_store  = abs(int(file_store))
    _bot_me     = await client.get_me()

    # Detect which season key was used — try all s*_first_* keys,
    # fall back to old unseasoned format (first_Hdri) for pre-season-fix batches.
    _season_key = "s1"
    for _k in batch_data.keys():
        if _k.startswith("s") and "_first_" in _k:
            _season_key = _k.split("_first_")[0]
            break

    _btn_row1 = []  # top row:    480p  | 720p
    _btn_row2 = []  # bottom row: 1080p | HDRip

    for qual in ["480", "720", "1080", "Hdri"]:
        f_id = batch_data.get(f"{_season_key}_first_{qual}") or batch_data.get(f"first_{qual}")
        l_id = batch_data.get(f"{_season_key}_last_{qual}")  or batch_data.get(f"last_{qual}")
        if not f_id or not l_id:
            continue
        try:
            _b64  = await _enc(f"get-{_abs_store}-{f_id}-{l_id}")
            _link = f"https://telegram.me/{_bot_me.username}?start={_b64}"
            _btn  = InlineKeyboardButton(QUAL_LABELS.get(qual, qual), url=_link)
            if qual in ("480", "720"):
                _btn_row1.append(_btn)
            else:
                _btn_row2.append(_btn)
        except Exception as _le:
            await message.reply(f"<b>Failed to generate [{qual}] link:</b> <code>{_le}</code>")

    if not _btn_row1 and not _btn_row2:
        return await status.edit(
            "<b>No per-quality message IDs found in DB.</b>\n"
            "The batch may not have completed or stored without quality ranges."
        )

    _rows = []
    if _btn_row1: _rows.append(_btn_row1)
    if _btn_row2: _rows.append(_btn_row2)
    _kb = InlineKeyboardMarkup(_rows)

    # Find post channel
    _ch_details = None
    try:
        from bot.core.text_utils import AniLister
        _anidata = await AniLister(anime_name).get_anidata()
        _titles  = _anidata.get("title", {})
        for _lname in [_titles.get("romaji"), _titles.get("english"), anime_name]:
            if not _lname:
                continue
            _ch_details = await _main_db.find_channel_by_anime_title(_lname)
            if _ch_details:
                break
    except Exception:
        pass

    _post_channel = _ch_details["channel_id"] if _ch_details else Var.BATCH_MAIN_CHANNEL

    await status.edit(
        f"<b>Links regenerated for:</b> <code>{anime_name}</code>\n"
        f"<b>ani_id:</b> <code>{_ani_id}</code>  "
        f"<b>store:</b> <code>{file_store}</code>\n"
        f"<b>channel:</b> <code>{_post_channel}</code>\n\n"
        "<b>Reply with the message ID</b> of the channel post to update,\n"
        "or send <code>skip</code> to just see the buttons here.",
        reply_markup=_kb
    )

    from bot import ani_cache
    ani_cache.setdefault("_pending_fixbatch", {})[message.from_user.id] = {
        "kb": _kb,
        "channel": _post_channel,
        "ani_id": _ani_id,
    }


@bot.on_message(filters.private & filters.user(Var.OWNER_ID) & filters.text & ~filters.command(["start", "help", "shell", "eval", "stats", "update", "downloads", "fixbatchlink", "restart", "batch", "genlink", "settings", "set", "index", "connect", "listconnections", "removeconnection", "cancel", "broadcast", "dbroadcast", "pbroadcast", "fsub", "adfsub", "rmfsub", "listfsub", "importusers", "log", "schedule", "addchnl", "delchnl", "listchnl", "fsub_mode", "clearlogs"]))
async def fixbatchlink_reply(client, message):
    """Handle the message ID reply for /fixbatchlink."""
    from bot import ani_cache
    pending = ani_cache.get("_pending_fixbatch", {}).get(message.from_user.id)
    if not pending:
        return

    text = message.text.strip().lower()
    if text == "skip":
        ani_cache["_pending_fixbatch"].pop(message.from_user.id, None)
        return await message.reply("<b>Skipped — buttons were shown above.</b>")

    try:
        msg_id = int(text)
    except ValueError:
        return  # not a number, not our handler

    ani_cache["_pending_fixbatch"].pop(message.from_user.id, None)

    try:
        await client.edit_message_reply_markup(
            chat_id=pending["channel"],
            message_id=msg_id,
            reply_markup=pending["kb"]
        )
        await message.reply("<b>Post buttons updated successfully!</b>")
    except Exception as e:
        await message.reply(f"<b>Failed to edit post:</b> <code>{e}</code>")


# ── /importusers ──────────────────────────────────────────────────────────────
# Usage:
#   /importusers                              → show usage
#   /importusers inspect [uri] [db] [col]     → show raw sample docs (diagnostic)
#   /importusers [uri] [db] [col]             → import users
#
# uri: full MongoDB URI or "same" to reuse current bot URI
# db:  database name  (required)
# col: collection name (default: users)
# ─────────────────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("importusers") & filters.private & filters.user(Var.OWNER_ID))
@new_task
async def importusers_handler(client, message):
    from motor.motor_asyncio import AsyncIOMotorClient
    from bot.core.database import db as _main_db

    args = message.text.split()[1:]

    if not args:
        return await message.reply(
            "<b>📥 Import Users from Another Bot DB</b>\n\n"
            "<b>Usage:</b>\n"
            "• <code>/importusers [uri] [db] [col]</code>\n"
            "• <code>/importusers same [db] [col]</code>\n\n"
            "<b>Diagnostic (see raw docs first):</b>\n"
            "• <code>/importusers inspect [uri] [db] [col]</code>\n"
            "• <code>/importusers inspect same [db] [col]</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/importusers same filesharexbot users</code>\n"
            "<code>/importusers inspect same filesharexbot users</code>"
        )

    # ── Check for inspect mode ────────────────────────────────────────────────
    inspect_mode = args[0].lower() == "inspect"
    if inspect_mode:
        args = args[1:]

    if not args:
        return await message.reply("<b>❌ Please provide at least a database name.</b>")

    # ── Parse uri / db / col ──────────────────────────────────────────────────
    src_uri = None
    if args[0].lower() == "same":
        src_uri = None   # use current bot URI
        args = args[1:]
    elif args[0].startswith("mongodb"):
        src_uri = args[0]
        args = args[1:]

    if not args:
        return await message.reply("<b>❌ Please provide the database name.</b>")

    src_db  = args[0]
    src_col = args[1] if len(args) > 1 else "users"

    status = await message.reply(
        f"<b>🔄 Connecting...</b>\n"
        f"<b>URI:</b> <code>{'Current bot URI' if not src_uri else src_uri[:50] + '...'}</code>\n"
        f"<b>DB:</b> <code>{src_db}</code>  <b>Col:</b> <code>{src_col}</code>\n"
        f"<b>Mode:</b> {'🔍 Inspect' if inspect_mode else '📥 Import'}"
    )

    # ── Connect ───────────────────────────────────────────────────────────────
    try:
        _uri = src_uri if src_uri else Var.MONGO_URI
        src_client = AsyncIOMotorClient(_uri, serverSelectionTimeoutMS=8000)
        await src_client.admin.command("ping")
        src_col_obj = src_client[src_db][src_col]
    except Exception as e:
        return await status.edit(f"<b>❌ Connection failed:</b>\n<code>{e}</code>")

    # ── Fetch raw docs (include _id so we can inspect all fields) ────────────
    try:
        src_docs = await src_col_obj.find({}).to_list(length=None)
    except Exception as e:
        src_client.close()
        return await status.edit(f"<b>❌ Failed to fetch documents:</b>\n<code>{e}</code>")

    src_client.close()

    if not src_docs:
        return await status.edit(
            f"<b>⚠️ No documents found in</b> <code>{src_db}.{src_col}</code>\n"
            f"<i>Check the database and collection names.</i>"
        )

    # ── INSPECT MODE: show raw sample docs ───────────────────────────────────
    if inspect_mode:
        sample = src_docs[:3]
        lines = [f"<b>🔍 {len(src_docs)} doc(s) in <code>{src_db}.{src_col}</code></b>\n"]
        for i, doc in enumerate(sample, 1):
            # Show all field names and their values (truncated)
            fields = []
            for k, v in doc.items():
                v_str = str(v)[:40]
                fields.append(f"  <code>{k}</code>: <code>{v_str}</code>")
            lines.append(f"<b>Doc {i}:</b>\n" + "\n".join(fields))
        lines.append(
            f"\n<b>💡 Once you know the user_id field name, run:</b>\n"
            f"<code>/importusers {'same' if not src_uri else src_uri[:30]} {src_db} {src_col}</code>"
        )
        return await status.edit("\n".join(lines))

    # ── IMPORT MODE ───────────────────────────────────────────────────────────
    if _main_db.db is None:
        await _main_db.connect()

    existing_ids = set(await _main_db.full_userbase())
    total_src    = len(src_docs)

    await status.edit(
        f"<b>📊 Found {total_src} doc(s) in source.</b>\n"
        f"<b>Already in your DB:</b> {len(existing_ids)}\n"
        f"<b>⏳ Importing...</b>"
    )

    imported = skipped = failed = 0

    for doc in src_docs:
        # Try every common field name for user ID
        uid = (
            doc.get("user_id") or
            doc.get("id") or
            doc.get("tg_id") or
            doc.get("telegram_id") or
            doc.get("chat_id") or
            # _id is an ObjectId by default but some bots store int user_id as _id
            (doc.get("_id") if isinstance(doc.get("_id"), int) else None)
        )

        if not uid:
            failed += 1
            continue

        try:
            uid = int(uid)
        except (ValueError, TypeError):
            failed += 1
            continue

        if uid in existing_ids:
            skipped += 1
            continue

        try:
            await _main_db.add_user(
                user_id    = uid,
                username   = doc.get("username"),
                first_name = doc.get("first_name") or doc.get("name"),
                last_name  = doc.get("last_name"),
            )
            existing_ids.add(uid)
            imported += 1
        except Exception:
            failed += 1

    await status.edit(
        f"<b>✅ Import Complete!</b>\n\n"
        f"<b>Source:</b> <code>{src_db}.{src_col}</code>\n"
        f"{'─' * 28}\n"
        f"<b>Total in source:</b> {total_src}\n"
        f"<b>✅ Imported (new):</b> {imported}\n"
        f"<b>⏭ Skipped (exists):</b> {skipped}\n"
        f"<b>❌ Failed (no ID found):</b> {failed}\n"
        f"{'─' * 28}\n"
        f"<b>Total in your DB now:</b> {len(existing_ids)}\n\n"
        + (f"<i>💡 Some docs had no recognisable user ID field.\n"
           f"Run <code>/importusers inspect ...</code> to see raw doc structure.</i>"
           if failed else "")
    )


# ── /log ───────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("log") & filters.user(Var.OWNER_ID))
async def log_handler(client, message):
    """
    Send the current log file and all available rotated backups.
    log.txt       → current
    log.txt.1     → previous run / last rotation
    log.txt.2     → older
    log.txt.3     → oldest kept
    """
    import os
    log_files = []
    for name in ["log.txt.3", "log.txt.2", "log.txt.1", "log.txt"]:
        if os.path.exists(name) and os.path.getsize(name) > 0:
            log_files.append(name)

    if not log_files:
        return await message.reply("<b>📭 No log files found.</b>")

    status = await message.reply(f"<b>📋 Sending {len(log_files)} log file(s)...</b>")

    for fpath in log_files:
        size = os.path.getsize(fpath)
        label = {
            "log.txt":   "📋 Current log",
            "log.txt.1": "📋 Previous log",
            "log.txt.2": "📋 Older log",
            "log.txt.3": "📋 Oldest log",
        }.get(fpath, fpath)
        try:
            await client.send_document(
                message.chat.id,
                document=fpath,
                caption=f"<b>{label}</b>  |  <code>{_fmt(size)}</code>",
                file_name=fpath,
            )
        except Exception as e:
            await message.reply(f"<b>❌ Failed to send {fpath}:</b> <code>{e}</code>")

    await status.delete()


@bot.on_message(filters.command("clearlogs") & filters.user(Var.OWNER_ID))
async def clearlogs_handler(client, message):
    """Delete all rotated log backups (keeps log.txt intact for current run)."""
    import os
    deleted = []
    for name in ["log.txt.1", "log.txt.2", "log.txt.3"]:
        if os.path.exists(name):
            try:
                os.remove(name)
                deleted.append(name)
            except Exception as e:
                await message.reply(f"<b>❌ Failed to delete {name}:</b> <code>{e}</code>")
    if deleted:
        await message.reply(f"<b>🗑 Deleted:</b> <code>{', '.join(deleted)}</code>")
    else:
        await message.reply("<b>📭 No backup log files to delete.</b>")
