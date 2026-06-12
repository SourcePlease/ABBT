"""
pipelines/movie.py
==================
Full encode+upload pipeline for a single anime movie.

Differences from the ongoing (episode) pipeline
------------------------------------------------
- Uses MovieEditor → AniList MOVIE format query
- Downloads to ./downloads/movies/<safe_title>/
- Single video file only — no episode loop or folder walk
- Filename format: "Title (Year) [qual].mkv"   (no SxxExx prefix)
- Dedup key: (anilist_id, "movie") stored in movie_db
- Posts ONE card with quality buttons (same 2×2 grid)
- File splitting: files > 1.9 GiB are split by ffmpeg -c copy into 1.9 GiB
  segments and uploaded as a range deep-link (get-{store}-{first}-{last})

Posting behaviour
-----------------
- If a dedicated movie channel is connected: post there, notify MOVIE_MAIN_CHANNEL
- Otherwise: post directly to MOVIE_MAIN_CHANNEL
"""

import os as _os
import re as _re
from asyncio.subprocess import PIPE
from traceback import format_exc

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, batch_encode_lock, batch_dl_lock
from bot.core.tordownload import TorDownloader
from bot.core.database import db
from bot.core.func_utils import editMessage, sendMessage, convertBytes, encode
from bot.core.text_utils import MovieEditor, detect_audio, _normalize_anime_title
from bot.core.ffencoder import FFEncoder
from bot.core.tguploader import TgUploader
from bot.core.reporter import movie_rep
from bot.core.task_queue import movie_task_queue, MAX_RETRIES

from .constants import QUAL_LABELS, AUDIO_LABELS, VIDEO_EXTS
from .helpers import _qual_file_store, _make_link, _qual_btns_to_keyboard, extra_utils, hdri_passthrough
from bot.core.memguard import reclaim_memory, areclaim_memory, drop_page_cache
# Telegram MTProto file size limit (2 GiB).  We stay 100 MiB under it.
_TG_SPLIT_BYTES = int(1.9 * 1024 ** 3)


def _safe_dl_path(base_root: str, *parts: str) -> str:
    root   = _os.path.realpath(base_root)
    joined = _os.path.realpath(_os.path.join(base_root, *parts))
    if not joined.startswith(root + _os.sep) and joined != root:        raise ValueError(f"Path traversal blocked: {joined!r} escapes {root!r}")
    return joined


async def _split_file(src: str, base_name: str, movie_dir: str) -> list:
    """
    Split src into 1.9 GiB segments using ffmpeg stream-copy.

    Returns a list of output file paths in order.
    Part filename format: [qual]_N title [@brand].mkv
    e.g. "[1080p]_1 Demon Slayer Mugen Train [@Team_Warlords].mkv"

    Falls back to [src] (no split) if ffmpeg exits non-zero.
    """
    import asyncio
    _ext  = _os.path.splitext(base_name)[1] or ".mkv"
    _stem = _os.path.splitext(base_name)[0]
    _tmp  = "split_tmp"
    _pat  = _os.path.join(movie_dir, f"{_tmp}_part%03d{_ext}")

    # Use exec (no shell) to avoid injection via src/pat paths
    _proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-i", src, "-c", "copy", "-f", "segment",
        "-segment_size", str(_TG_SPLIT_BYTES), "-reset_timestamps", "1",
        _pat, "-y",
        stdout=PIPE, stderr=PIPE,
    )
    await _proc.communicate()
    if _proc.returncode != 0:
        return [src]

    _raw_parts = sorted([
        _os.path.join(movie_dir, f)
        for f in _os.listdir(movie_dir)
        if _re.search(rf"{_tmp}_part\d{{3}}{_re.escape(_ext)}", f)
    ])
    if not _raw_parts:
        return [src]

    # Rename: insert _{part_number} after the first ] (quality tag)
    _renamed = []
    for i, raw in enumerate(_raw_parts, 1):
        bracket_end = _stem.find("]")
        if bracket_end != -1:
            new_stem = _stem[:bracket_end + 1] + f"_{i} " + _stem[bracket_end + 1:].lstrip()
        else:
            new_stem = f"{_stem}_part{i}"
        new_path = _os.path.join(movie_dir, f"{new_stem}{_ext}")
        _os.rename(raw, new_path)
        _renamed.append(new_path)
    _os.remove(src)
    return _renamed


async def _run_movie_pipeline(
    name: str,
    torrent: str,
    force: bool,
    task_id: str,
    source_priority: int,
    target_channel: int,
    file_store: int,
    log_channel: int,
    upload_bot,
):
    """Full encode+upload pipeline for a single anime movie."""
    from bot.core.database import movie_db

    try:
        # ── 1. Load AniList movie metadata ────────────────────────────────────
        movieInfo = MovieEditor(name)
        await movieInfo.load_anilist()

        ani_id = movieInfo.adata.get("id")
        audio  = detect_audio(name)
        title  = movieInfo.get_title()
        year   = movieInfo.get_year()

        # ── 2. Dedup guard ────────────────────────────────────────────────────
        _dedup_key = f"{ani_id}_movie_{audio}" if ani_id else f"{name}_movie"
        if not force:
            if movie_db.db is None:
                await movie_db.connect()
            if await movie_db.db["movie_data"].find_one({"dedup_key": _dedup_key}):
                await movie_rep.report(
                    f"🎬 Skipping already-uploaded movie: {title}", "info", log=False
                )
                await movie_task_queue.mark_done(task_id)
                return

        await movie_task_queue.update_task(
            task_id, status="downloading", ani_id=ani_id, ep_no="movie"
        )
        await movie_rep.report(f"🎬 Movie pipeline started: {title} ({year})", "info")

        # ── 3. Dedicated channel lookup ───────────────────────────────────────
        _titles = movieInfo.adata.get("title", {})
        _lookup_names = [n for n in [
            _titles.get("romaji"), _titles.get("english"), _normalize_anime_title(name),        ] if n and n.strip()]
        channel_details = None
        for lname in _lookup_names:
            channel_details = await db.find_channel_by_anime_title(lname, db_type="movie")
            if channel_details:
                break

        _post_channel = channel_details["channel_id"] if channel_details else target_channel
        stat_channel  = log_channel or _post_channel
        stat_msg      = await sendMessage(stat_channel, f"<b>🎬 Downloading movie: {title}</b>")

        # Warm peer caches before any send_photo / send_message — dedicated movie
        # channels added via /addchannel are NOT pre-verified at startup so the
        # first post can hit PEER_ID_INVALID after a cold restart.
        from .helpers import _warm_peer, _safe_send
        await _warm_peer(upload_bot, _post_channel)
        if target_channel and target_channel != _post_channel:
            await _warm_peer(upload_bot, target_channel)

        # ── 4. Download ───────────────────────────────────────────────────────
        _safe_title = _re.sub(r"[^\w\s-]", "", title).strip().replace(" ", "_")[:50]
        _movie_dir  = _safe_dl_path("./downloads/movies", _safe_title)
        _os.makedirs(_movie_dir, exist_ok=True)

        async with batch_dl_lock:
            dl = await TorDownloader(_movie_dir, use_stable_dir=True).download(
                torrent, name, stat_msg=stat_msg, anime_name=title
            )
        if not dl or not _os.path.exists(dl):
            retry = await movie_task_queue.increment_retry(task_id)
            await movie_rep.report(
                f"Movie download failed (retry {retry}/{MAX_RETRIES}): {name}", "error"
            )
            await stat_msg.delete()
            await movie_task_queue.update_task(
                task_id, status="pending" if retry < MAX_RETRIES else "failed"
            )
            return

        # ── 5. Resolve single video file ──────────────────────────────────────
        # Movies always arrive as a single file. If a folder appears, pick the
        # largest video file inside it (most likely the main feature).
        if _os.path.isdir(dl):
            _candidates = []
            for _root, _, _fnames in _os.walk(dl):
                for _fn in _fnames:
                    if _os.path.splitext(_fn)[1].lower() in VIDEO_EXTS:
                        _fp = _os.path.join(_root, _fn)
                        _candidates.append((_os.path.getsize(_fp), _fp))
            if not _candidates:                await movie_rep.report(f"No video file found in movie download: {name}", "error")
                await stat_msg.delete()
                await movie_task_queue.update_task(
                    task_id, status="failed", error="No video file found"
                )
                return
            _candidates.sort(reverse=True)
            _src_file = _candidates[0][1]
        else:
            _src_file = dl

        await movie_rep.report(f"🎬 Movie source: {_os.path.basename(_src_file)}", "info", log=False)

        # ── 6. Poster and captions ────────────────────────────────────────────
        poster_url = await movieInfo.get_poster(upload_bot=upload_bot)
        _main_cap  = await movieInfo.get_main_caption(audio)
        _ded_cap   = await movieInfo.get_dedicated_caption(audio)

        # ── 7. Create the channel post upfront ───────────────────────────────
        await movie_task_queue.update_task(task_id, status="encoding")
        if poster_url:
            _movie_post = await _safe_send(
                upload_bot, upload_bot.send_photo, _post_channel,
                photo=poster_url, caption=_ded_cap,
                _label="movie post send_photo",
            )
        else:
            _movie_post = await _safe_send(
                upload_bot, upload_bot.send_message, _post_channel,
                text=_ded_cap, _label="movie post send_message",
            )

        # ── 8. Per-quality encode → split if > 1.9 GiB → upload ──────────────
        ENCODE_QUALS = [q for q in Var.QUALS if q != "Hdri"]
        ALL_QUALS    = ([q for q in ["Hdri"] if q in Var.QUALS]) + ENCODE_QUALS
        qual_links: dict = {}

        # Fetch bot username once — reused for range deep-link construction
        _bot_uname = (await upload_bot.get_me()).username

        for q_idx, qual in enumerate(ALL_QUALS, 1):
            _fname = await movieInfo.get_upname(qual)
            await editMessage(
                stat_msg,
                f"<b>🎬 {title}</b>\n\n"
                f"<blockquote>⚙️ Encoding [{qual}] ({q_idx}/{len(ALL_QUALS)})</blockquote>",
            )

            _mv_qual_dir = _os.path.join(_movie_dir, qual)
            _os.makedirs(_mv_qual_dir, exist_ok=True)
            try:
                if qual == 'Hdri':
                    out_path = await hdri_passthrough(_src_file, _mv_qual_dir, _fname)
                else:
                    async with batch_encode_lock:
                        out_path = await FFEncoder(
                            stat_msg, _src_file, _fname, qual,
                            output_dir=_mv_qual_dir, display_name=title,
                        ).start_encode()
            except Exception as _ee:
                retry = await movie_task_queue.increment_retry(task_id)
                await movie_rep.report(
                    f"Movie encode error [{qual}]: {_ee} (retry {retry}/{MAX_RETRIES})", "error"
                )
                await stat_msg.delete()
                await movie_task_queue.update_task(
                    task_id,
                    status="pending" if retry < MAX_RETRIES else "failed",
                    error=str(_ee)[:300],
                )
                return

            if not out_path:
                retry = await movie_task_queue.increment_retry(task_id)
                await movie_rep.report(
                    f"Movie encode returned None [{qual}] (retry {retry}/{MAX_RETRIES})", "error"
                )
                await stat_msg.delete()
                await movie_task_queue.update_task(
                    task_id, status="pending" if retry < MAX_RETRIES else "failed"
                )
                return

            await movie_task_queue.update_task(task_id, status="uploading")

            # ── Split large files, upload all parts ───────────────────────────
            _fsize = _os.path.getsize(out_path)
            _ql_label = QUAL_LABELS.get(qual, qual)
            _file_caption = (
                f"<b>🎬 {title}</b>\n"
                f"<b>{'─' * 28}</b>\n"
                f"<b>➤ Year     :</b> {year}\n"
                f"<b>➤ Quality  :</b> {_ql_label}\n"
                f"<b>➤ Audio    :</b> {AUDIO_LABELS.get(audio, audio)}\n"
                f"<b>{'─' * 28}</b>\n"
                f"<code>{_os.path.basename(out_path)}</code>"
            )

            if _fsize > _TG_SPLIT_BYTES:                n_parts = -(-_fsize // _TG_SPLIT_BYTES)
                await editMessage(
                    stat_msg,
                    f"<b>🎬 {title}</b>\n\n"
                    f"<blockquote>✂️ [{_ql_label}] {convertBytes(_fsize)} — "
                    f"splitting into {n_parts} parts...</blockquote>",
                )
                _parts = await _split_file(out_path, _os.path.basename(out_path), _mv_qual_dir)
            else:
                _parts = [out_path]

            _mv_q_store = _qual_file_store(qual, file_store, pipeline="movie")
            _part_ids   = []
            for _pi, _part_path in enumerate(_parts, 1):
                _part_label = f"Part {_pi}/{len(_parts)}" if len(_parts) > 1 else ""
                await editMessage(
                    stat_msg,
                    f"<b>🎬 {title}</b>\n\n"
                    f"<blockquote>📤 [{_ql_label}] {_part_label} "
                    f"({q_idx}/{len(ALL_QUALS)})</blockquote>",
                )
                try:
                    _pmsg = await (
                        TgUploader(stat_msg, upload_bot=upload_bot, file_store=_mv_q_store)
                        .set_display_name(title)
                        .upload(_part_path, qual)
                    )
                    _part_ids.append(_pmsg.id)
                    bot_loop.create_task(extra_utils(_pmsg.id, _part_path))
                except Exception as _ue:
                    await movie_rep.report(
                        f"Movie upload error [{qual}] part {_pi}: {_ue}", "error"
                    )
                    retry = await movie_task_queue.increment_retry(task_id)
                    await stat_msg.delete()
                    await movie_task_queue.update_task(
                        task_id, status="pending" if retry < MAX_RETRIES else "failed"
                    )
                    return

            if not _part_ids:
                continue

            if len(_part_ids) == 1:
                _link = await _make_link(_part_ids[0], file_store=_mv_q_store, upload_bot=upload_bot)
            else:
                _abs_store = abs(file_store)
                _b64 = await encode(f"get-{_abs_store}-{_part_ids[0]}-{_part_ids[-1]}")
                _link = f"https://telegram.me/{_bot_uname}?start={_b64}"
            qual_links[qual] = _link

            # Live-update the channel post keyboard as each quality finishes
            _live_kb = _qual_btns_to_keyboard(qual_links)
            try:
                await editMessage(
                    _movie_post,
                    _movie_post.caption.html if _movie_post.caption else _ded_cap,
                    _live_kb,
                )
            except Exception:
                pass

            # Memory reclaim between movie quality encode+upload cycles
            # FIX: synchronous reclaim_memory() blocks event loop via
            # malloc_trim — use the async helper instead.
            await areclaim_memory()
            if _os.path.exists(_src_file):
                drop_page_cache(_src_file)

        # ── 9. Final keyboard ─────────────────────────────────────────────────
        _final_kb = _qual_btns_to_keyboard(qual_links)
        try:
            await editMessage(
                _movie_post,
                _movie_post.caption.html if _movie_post.caption else _ded_cap,
                _final_kb,
            )
        except Exception:
            pass

        # ── 10. Notify MOVIE_MAIN_CHANNEL if posted to dedicated channel ──────
        if channel_details:
            _invite   = channel_details.get("invite_link")
            _notify_cap = (
                f"<b>{title}</b>\n"
                f"<b>{'─' * 28}</b>\n"
                f"<b>➤ Year:</b> {year}\n"
                f"<b>➤ Duration:</b> {movieInfo.get_duration()} min\n"
                f"<b>➤ Qualities:</b> {', '.join(QUAL_LABELS.get(q, q) for q in ALL_QUALS)}\n"
                f"<b>➤ Audio:</b> {AUDIO_LABELS.get(audio, audio)}\n"
                f"<b>{'─' * 28}</b>\n"
                f"<blockquote>Available in dedicated channel.</blockquote>"
            )
            _notify_kb = (
                InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Now", url=_invite)]])
                if _invite else None
            )
            if poster_url:
                await _safe_send(                    upload_bot, upload_bot.send_photo, target_channel,
                    photo=poster_url,
                    caption=_notify_cap, reply_markup=_notify_kb,
                    _label="movie notify send_photo",
                )
            else:
                await _safe_send(
                    upload_bot, upload_bot.send_message, target_channel,
                    text=_notify_cap, reply_markup=_notify_kb,
                    _label="movie notify send_message",
                )

        # ── 11. Persist dedup record ──────────────────────────────────────────
        try:
            if movie_db.db is None:
                await movie_db.connect()
            await movie_db.db["movie_data"].update_one(
                {"dedup_key": _dedup_key},
                {"$set": {
                    "dedup_key":   _dedup_key,
                    "ani_id":      ani_id,
                    "title":       title,
                    "year":        year,
                    "audio":       audio,
                    "torrent_url": torrent,
                    "qual_links":  qual_links,
                }},
                upsert=True,
            )
        except Exception as _dbe:
            await movie_rep.report(f"Movie DB save error: {_dbe}", "warning", log=False)

        await movie_task_queue.mark_done(task_id)
        await stat_msg.delete()

        # ── 12. Cleanup ───────────────────────────────────────────────────────
        try:
            from aioshutil import rmtree as _aiorm
            if _movie_dir and _os.path.isdir(_movie_dir):
                await _aiorm(_movie_dir)
        except Exception:
            pass

        await movie_rep.report(
            f"✅ Movie done: {title} ({year}) — {len(qual_links)} quality(s)", "info"
        )

    except Exception:
        await movie_rep.report(format_exc(), "error")
        if task_id:            retry = await movie_task_queue.increment_retry(task_id)
            await movie_task_queue.update_task(
                task_id, status="pending" if retry < MAX_RETRIES else "failed"
      )
