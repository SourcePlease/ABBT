"""
pipelines/ongoing.py
====================
Full encode+upload pipeline for a single airing anime episode sourced from RSS.

Entry point
-----------
    await _run_pipeline(name, torrent, force, task_id, source_priority,
                        quals_done, is_batch,
                        target_channel, file_store, log_channel)

Posting behaviour
-----------------
- If the anime has a dedicated channel (connected via /addchannel):
    1. Post the episode card + quality buttons to the dedicated channel.
    2. Post a "Watch Now" notification to MAIN_CHANNEL.
- If no dedicated channel:
    1. Post the episode card + quality buttons directly to MAIN_CHANNEL.

Quality loop
------------
For each quality in Var.QUALS:
  1. Encode with FFEncoder (Hdri = stream copy, skips the shared encode lock).
  2. Upload to FILE_STORE (or per-quality store) with TgUploader.
  3. Send/edit the channel post — the post is sent immediately after the first
     quality finishes so subscribers see it right away, then subsequent qualities
     add new buttons by editing the reply_markup in place.
  4. Save the message ID to DB via db.saveAnime().

Resume behaviour
----------------
quals_done is the list of qualities already uploaded (from a previous run that
was interrupted mid-episode).  Those qualities are skipped and their links are
reconstructed from DB so the final post includes ALL quality buttons.
"""

import os as _os
import re as _re
import time as _time
from traceback import format_exc

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, ani_cache, ongoing_encode_lock, ongoing_dl_lock, LOGS  # ani_cache kept for fetch_animes flag only
from bot.core.tordownload import TorDownloader
from bot.core.database import db
from bot.core.func_utils import editMessage, sendMessage, convertBytes
from bot.core.text_utils import TextEditor, detect_audio, _normalize_anime_title
from bot.core.ffencoder import FFEncoder
from bot.core.tguploader import TgUploader
from bot.core.reporter import rep
from bot.core.task_queue import task_queue, MAX_RETRIES
from bot.core.memguard import reclaim_memory, areclaim_memory, drop_page_cache, get_available_mb

from .constants import (
    QUAL_LABELS, AUDIO_LABELS, VIDEO_EXTS,
    SKIP_WORDS, SKIP_FOLDERS, STICKER_MAIN,
)
from .helpers import (
    _qual_file_store, _make_link, _qual_btns_to_keyboard, extra_utils,
    hdri_passthrough,
    _warm_peer, _safe_send,
)
from .filters import _is_batch_task


def _safe_dl_path(base_root: str, *parts: str) -> str:
    """Build a download path and assert it stays under base_root."""
    root   = _os.path.realpath(base_root)
    joined = _os.path.realpath(_os.path.join(base_root, *parts))
    if not joined.startswith(root + _os.sep) and joined != root:
        raise ValueError(f"Path traversal blocked: {joined!r} escapes {root!r}")
    return joined


async def _run_pipeline(
    name: str,
    torrent: str,
    force: bool,
    task_id: str,
    source_priority: int,
    quals_done: list,
    is_batch: bool,
    target_channel: int = None,
    file_store: int = None,
    log_channel: int = None,
    upload_bot=None,
):
    """Full encode+upload pipeline for one ongoing RSS episode."""
    if target_channel is None: target_channel = Var.MAIN_CHANNEL
    if file_store     is None: file_store     = Var.FILE_STORE
    if log_channel    is None: log_channel    = Var.LOG_CHANNEL
    if upload_bot     is None: upload_bot     = bot

    try:
        aniInfo = TextEditor(name)
        await aniInfo.load_anilist()
        audio  = detect_audio(name)
        ani_id = aniInfo.adata.get('id')
        ep_no  = aniInfo.pdata.get("episode_number")

        # ── Absolute → relative episode number correction ─────────────────────
        # SubsPlease and some groups use absolute numbering (e.g. JJK ep 58 = S3E11).
        # AniList gives us episodes-per-season; if ep_no > season total we query
        # the franchise relation graph to subtract previous seasons' episode counts.
        _total_eps_this_season = aniInfo.adata.get("episodes")
        if ep_no and _total_eps_this_season:
            ep_no_int = int(ep_no)
            if ep_no_int > _total_eps_this_season:
                try:
                    import aiohttp as _aio
                    _franchise_query = """
                    query($id:Int){
                      Media(id:$id,type:ANIME){
                        relations{
                          edges{
                            relationType
                            node{ id episodes format }
                          }
                        }
                      }
                    }"""
                    async with _aio.ClientSession() as sess:
                        async with sess.post(
                            "https://graphql.anilist.co",
                            json={"query": _franchise_query, "variables": {"id": ani_id}},
                            timeout=_aio.ClientTimeout(total=8),
                        ) as r:
                            fdata = await r.json(content_type=None)
                    edges = (
                        ((fdata or {}).get("data") or {})
                        .get("Media", {})
                        .get("relations", {})
                        .get("edges", [])
                    )
                    prev_eps = sum(
                        int(e["node"].get("episodes") or 0)
                        for e in edges
                        if e.get("relationType") == "PREQUEL"
                        and e.get("node", {}).get("format") in ("TV", "TV_SHORT")
                        and e.get("node", {}).get("episodes")
                    )
                    if prev_eps > 0:
                        relative = ep_no_int - prev_eps
                        if 1 <= relative <= _total_eps_this_season:
                            ep_no = relative
                            aniInfo.pdata["episode_number"] = ep_no
                except Exception:
                    pass

        session_key = f"{ani_id}_{ep_no}_{audio}"
        # Dedup via DB instead of in-memory dict — no RAM accumulation
        if not force and ani_id and ep_no:
            if await db.is_episode_done(ani_id, ep_no, audio):
                return

        # ── Skip if batch pipeline is actively processing this anime ──────────
        # Prevents duplicate posts when Upload All triggers batch download while
        # RSS simultaneously picks up individual episodes for the same show.
        if ani_id and not force:
            try:
                _bt_col = await batch_task_queue._col()
                _active_batch = await _bt_col.find_one({
                    "ani_id": {"$in": [ani_id, str(ani_id)]},
                    "status": {"$in": ["pending", "downloading", "encoding", "uploading"]},
                })
                if _active_batch:
                    await rep.report(
                        f"⏭ Skipping {name[:50]} — batch task active for this anime",
                        "info", log=False,
                    )
                    return
            except Exception:
                pass

        pending_quals = list(Var.QUALS)
        if not force:
            if ani_data := await db.getAnime(ani_id):
                qual_data = ani_data.get(str(ep_no), {}) if ep_no else {}
                pending_quals = [q for q in pending_quals if not qual_data.get(f"{q}_{audio}")]
        pending_quals = [q for q in pending_quals if q not in quals_done]

        if not pending_quals and ep_no:
            # ani_cache['completed'] write removed — DB is the source of truth
            return

        await task_queue.update_task(
            task_id, status="downloading",
            ani_id=ani_id, ep_no=str(ep_no) if ep_no else None,
        )
        await rep.report(f"New Anime Torrent Found!\n\n{name}", "info")

        # ── Build display title (Eng | Romaji) ────────────────────────────────
        try:
            _sn_raw = aniInfo.pdata.get("anime_season", "1")
            if isinstance(_sn_raw, list):
                _sn_raw = _sn_raw[-1]
            aniInfo.adata["seasonNumber"] = int(str(_sn_raw).strip() or "1")
        except (ValueError, TypeError):
            pass

        poster_url = await aniInfo.get_poster(upload_bot=upload_bot)
        aniInfo._cached_poster_url = poster_url
        banner_url = await aniInfo.get_banner()
        _post_photo = poster_url or banner_url

        _titles = aniInfo.adata.get("title", {})

        def _clean_ani_title(t):
            t = _re.sub(r'[【】「」『』〔〕［］\[\]]', ' ', t)
            t = _re.sub(
                r'\s*(Season\s*\d+|S\d{1,2}|Part\s*\d+|Cour\s*\d+|\d+(st|nd|rd|th)\s+Season)\s*$',
                '', t, flags=_re.IGNORECASE,
            )
            return _re.sub(r'\s+', ' ', t).strip()

        _eng = _clean_ani_title(_titles.get("english") or "")
        _rom = _clean_ani_title(_titles.get("romaji") or "")
        if _eng and _rom and _eng.lower() != _rom.lower():
            anime_title = f"{_eng} | {_rom}"
        elif _eng:
            anime_title = _eng
        elif _rom:
            anime_title = _rom
        else:
            anime_title = _normalize_anime_title(name) or name

        # ── Dedicated channel lookup ──────────────────────────────────────────
        _lookup_names = [n for n in [
            _titles.get("romaji"), _titles.get("english"), _normalize_anime_title(name),
        ] if n and n.strip()]
        channel_details = None
        for lname in _lookup_names:
            channel_details = await db.find_channel_by_anime_title(lname, db_type="ongoing")
            if not channel_details:
                channel_details = await db.find_channel_by_anime_title(lname, db_type="completed")
            if channel_details:
                break

        stat_channel = log_channel or (
            channel_details['channel_id'] if channel_details else target_channel
        )
        stat_msg = await sendMessage(stat_channel, "<b>Downloading...</b>")

        _dl_name = (
            _titles.get("english") or _titles.get("romaji") or
            _normalize_anime_title(name) or name
        )
        _safe_dl_name = _re.sub(r'[^\w\s-]', ' ', _dl_name)
        _safe_dl_name = _re.sub(r'\s+', ' ', _safe_dl_name).strip().replace(' ', '_')[:50]
        _ongoing_base = _safe_dl_path("./downloads/ongoing", _safe_dl_name)
        _os.makedirs(_ongoing_base, exist_ok=True)

        async with ongoing_dl_lock:
            dl = await TorDownloader(_ongoing_base, use_stable_dir=True).download(
                torrent, name, stat_msg=stat_msg, anime_name=_dl_name
            )
        if not dl or not _os.path.exists(dl):
            retry = await task_queue.increment_retry(task_id)
            await rep.report(f"Download failed (retry {retry}/{MAX_RETRIES}): {name}", "error")
            await stat_msg.delete()
            await task_queue.update_task(
                task_id, status="pending" if retry < MAX_RETRIES else "failed"
            )
            return

        # ── Collect video files ───────────────────────────────────────────────
        if _os.path.isdir(dl):
            _batch_files = []
            for _root, _, _fnames in _os.walk(dl):
                if _os.path.basename(_root).lower().strip() in SKIP_FOLDERS:
                    continue
                for _fn in sorted(_fnames):
                    if _os.path.splitext(_fn)[1].lower() not in VIDEO_EXTS:
                        continue
                    _fnl = _fn.lower()
                    if any(sw in _fnl for sw in SKIP_WORDS):
                        continue
                    if _re.match(r'^(op|ed|nced|ncop)(v\d+)?.', _fnl):
                        continue
                    _batch_files.append(_os.path.join(_root, _fn))
            _batch_files = sorted(_batch_files)
            is_batch_folder = True
        else:
            _batch_files    = [dl]
            is_batch_folder = False

        await task_queue.update_task(task_id, status="encoding")

        # ── Check source files still exist (survive restart) ──────────────────
        _missing = [f for f in _batch_files if not _os.path.exists(f)]
        if _missing:
            await rep.report(
                f"Source file(s) missing after restart — re-queueing: {name}", "warning"
            )
            await stat_msg.delete()
            await task_queue.update_task(task_id, status="pending", error=None)
            try:
                from aioshutil import rmtree as _aiorm2
                miss_dir = _os.path.dirname(dl.rstrip(_os.sep))
                if miss_dir and miss_dir != "./downloads" and _os.path.isdir(miss_dir):
                    await _aiorm2(miss_dir)
            except Exception:
                pass
            from .workers import _ongoing_queue, _ongoing_counter
            await _ongoing_queue.put((
                source_priority, next(_ongoing_counter),
                name, torrent, True, task_id, source_priority, quals_done, is_batch,
            ))
            return

        # ── Episode-by-episode encode + upload loop ───────────────────────────
        ep_posts: dict = {}
        _audio = detect_audio(name)

        for _file_idx, _current_file in enumerate(_batch_files):
            if is_batch_folder:
                _fname = _os.path.basename(_current_file)
                _ep_match = (
                    _re.search(r'[-_\s]\s*(\d{1,4})\s*(?:v\d)?(?:\s|$|\.|\[|\()', _fname) or
                    _re.search(r'\[(\d{1,4})\]', _fname) or
                    _re.search(r'(?<!\d)(\d{1,4})(?!\d)', _fname)
                )
                _ep_num = int(_ep_match.group(1)) if _ep_match else (_file_idx + 1)
                _use_aniInfo = TextEditor(name)
                _use_aniInfo.adata = aniInfo.adata
                _use_aniInfo.pdata = dict(aniInfo.pdata)
                _use_aniInfo.pdata["episode_number"] = _ep_num
                _ep_label = f"episode {_ep_num:02d}"
            else:
                _ep_num      = ep_no
                _use_aniInfo = aniInfo
                _ep_label    = f"ep {ep_no}"

            _ani_id_e = _use_aniInfo.adata.get("id")
            _ep_no_e  = _use_aniInfo.pdata.get("episode_number")

            _ep_pending_quals = list(Var.QUALS)
            qual_links: dict  = {}
            if not force and _ani_id_e and _ep_no_e:
                if _ani_data := await db.getAnime(_ani_id_e):
                    _qual_data = _ani_data.get(str(_ep_no_e), {})
                    _ep_pending_quals = [
                        q for q in _ep_pending_quals if not _qual_data.get(f"{q}_{_audio}")
                    ]
                    # Pre-populate qual_links for already-uploaded qualities so the
                    # final post shows ALL buttons (handles crash-before-post case)
                    for _dq in Var.QUALS:
                        _dq_post_id = _qual_data.get(f"{_dq}_{_audio}")
                        if _dq_post_id and _dq not in _ep_pending_quals:
                            qual_links[_dq] = await _make_link(
                                _dq_post_id,
                                file_store=_qual_file_store(_dq, file_store, pipeline="ongoing"),
                                upload_bot=upload_bot,
                            )
            _ep_pending_quals = [q for q in _ep_pending_quals if q not in (quals_done or [])]

            if not _ep_pending_quals:
                if not qual_links:
                    await rep.report(
                        f"⏭ Skipping {_ep_label} — all qualities already uploaded.",
                        "info", log=False,
                    )
                    continue
                await rep.report(
                    f"⏭ {_ep_label} already encoded — re-sending channel post with all buttons.",
                    "info", log=False,
                )

            _ep_caption = await _use_aniInfo.get_caption(is_main_channel=True)

            # Resolve dedicated channel invite link once before the quality loop.
            # get_chat() also primes Pyrofork's peer cache for _ded_ch_id so the
            # later send_photo() call doesn't fail with PEER_ID_INVALID after a
            # fresh /addchannel + restart.
            _ded_ch_id = channel_details["channel_id"] if channel_details else None
            _invite    = ""
            if _ded_ch_id:
                _invite = channel_details.get("invite_link") or ""
                try:
                    _ded_chat = await upload_bot.get_chat(_ded_ch_id)
                    if not _invite:
                        _invite = _ded_chat.invite_link or ""
                        if not _invite:
                            _invite = await upload_bot.export_chat_invite_link(_ded_ch_id)
                except Exception as _wp_e:
                    LOGS.warning(
                        f"⚠️ ongoing: failed to warm dedicated channel "
                        f"{_ded_ch_id}: {type(_wp_e).__name__}: {_wp_e}"
                    )

            # Always warm the main target channel before posting too — same
            # peer-cache class of bug shows up after any cold restart.
            await _warm_peer(upload_bot, target_channel)

            _ded_post_msg = None
            _ep_main_post = None
            _post_sent    = False

            for q_idx, qual in enumerate(_ep_pending_quals, 1):
                filename = await _use_aniInfo.get_upname(qual)
                await editMessage(
                    stat_msg,
                    f"<b>{anime_title}</b>\n\n"
                    f"<blockquote>⚙️ Encoding [{qual}] — {_ep_label} "
                    f"({q_idx}/{len(_ep_pending_quals)})</blockquote>",
                )
                bot_loop.create_task(task_queue.update_task(task_id, status="encoding"))

                _qual_dir = _os.path.join(_ongoing_base, qual)
                _os.makedirs(_qual_dir, exist_ok=True)

                LOGS.info(
                    f"⚙️ ongoing: encode start [{qual}] {_ep_label} "
                    f"({q_idx}/{len(_ep_pending_quals)}) — {filename}"
                )
                try:
                    if qual == 'Hdri':
                        # ── Hdri = ORIGINAL quality, no re-encode ─────────────
                        # We hardlink the source under the proper filename and
                        # upload it as-is. Instant publish (no ffmpeg wait) and
                        # zero quality loss. The hardlink leaves _current_file
                        # intact for the subsequent 1080/720/480 encodes.
                        await editMessage(
                            stat_msg,
                            f"<b>{anime_title}</b>\n\n"
                            f"<blockquote>📎 Publishing [Hdri] — {_ep_label} "
                            f"(original quality, no re-encode)</blockquote>",
                        )
                        out_path = await hdri_passthrough(
                            _current_file, _qual_dir, filename
                        )
                    else:
                        async with ongoing_encode_lock:
                            out_path = await FFEncoder(
                                stat_msg, _current_file, filename, qual,
                                output_dir=_qual_dir, display_name=anime_title,
                            ).start_encode()
                except Exception as e:
                    retry = await task_queue.increment_retry(task_id)
                    await rep.report(
                        f"Encode error [{qual}] {_ep_label}: {e} (retry {retry}/{MAX_RETRIES})",
                        "error",
                    )
                    await stat_msg.delete()
                    await task_queue.update_task(
                        task_id,
                        status="pending" if retry < MAX_RETRIES else "failed",
                        error=str(e)[:300],
                    )
                    return

                if not out_path:
                    retry = await task_queue.increment_retry(task_id)
                    await rep.report(
                        f"Encode returned None [{qual}] {_ep_label} (retry {retry}/{MAX_RETRIES})",
                        "error",
                    )
                    await stat_msg.delete()
                    await task_queue.update_task(
                        task_id, status="pending" if retry < MAX_RETRIES else "failed"
                    )
                    return

                try:
                    _enc_size_mb = _os.path.getsize(out_path) // (1024 * 1024)
                except Exception:
                    _enc_size_mb = -1
                LOGS.info(
                    f"✅ ongoing: encode complete [{qual}] {_ep_label} — "
                    f"{_enc_size_mb}MB → {out_path}"
                )

                await editMessage(
                    stat_msg,
                    f"<b>{anime_title}</b>\n\n"
                    f"<blockquote>📤 Uploading [{qual}] — {_ep_label}...</blockquote>",
                )
                bot_loop.create_task(task_queue.update_task(task_id, status="uploading"))

                _ongoing_file_caption = await _use_aniInfo.get_caption(
                    is_main_channel=False, qual=qual
                )
                _q_file_store = _qual_file_store(qual, file_store, pipeline="ongoing")

                # Warm the file-store peer too — covers the rare case where
                # FILE_STORE_HDRI / FILE_STORE_1080 were set after startup.
                await _warm_peer(upload_bot, _q_file_store)

                LOGS.info(
                    f"📤 ongoing: upload start [{qual}] {_ep_label} → "
                    f"file_store={_q_file_store}"
                )
                try:
                    msg = await (
                        TgUploader(stat_msg, upload_bot=upload_bot, file_store=_q_file_store)
                        .set_display_name(anime_title)
                        .upload(out_path, qual, caption=_ongoing_file_caption)
                    )
                except Exception as e:
                    retry = await task_queue.increment_retry(task_id)
                    await rep.report(
                        f"Upload error [{qual}] {_ep_label}: {e} (retry {retry}/{MAX_RETRIES})",
                        "error",
                    )
                    await stat_msg.delete()
                    await task_queue.update_task(
                        task_id,
                        status="pending" if retry < MAX_RETRIES else "failed",
                        error=str(e)[:300],
                    )
                    return

                LOGS.info(
                    f"✅ ongoing: upload complete [{qual}] {_ep_label} — "
                    f"msg_id={msg.id}"
                )

                qual_links[qual] = await _make_link(
                    msg.id, file_store=_q_file_store, upload_bot=upload_bot
                )
                # Await saveAnime — fire-and-forget caused re-encodes after OOM kill
                if _ani_id_e and _ep_no_e:
                    await db.saveAnime(_ani_id_e, _ep_no_e, qual, msg.id, _audio)
                bot_loop.create_task(task_queue.mark_qual_done(task_id, qual))
                bot_loop.create_task(extra_utils(msg.id, out_path))

                # ── Memory reclaim between qualities ──────────────────────────
                # After encoding + uploading each quality, force Python to
                # release Pyrofork's MTProto send buffers and evict the source
                # file from page cache.  Without this, memory accumulates
                # across the 4-quality loop (Hdri → 1080 → 720 → 480) and can
                # consume 8GB+ on a VPS, triggering OOM.
                # FIX: use the async helper — synchronous reclaim_memory() can
                # block the event loop for minutes via malloc_trim(0).
                await areclaim_memory()
                if _os.path.exists(_current_file):
                    drop_page_cache(_current_file)

                _cur_kb = _qual_btns_to_keyboard(qual_links)

                if not _post_sent:
                    # ── First quality done — send the initial channel post ─────
                    LOGS.info(
                        f"📢 ongoing: posting channel card after [{qual}] {_ep_label} → "
                        f"ded={_ded_ch_id} main={target_channel}"
                    )
                    await rep.report(
                        f"📤 Sending channel post after [{qual}] — {_ep_label}",
                        "info", log=False,
                    )
                    if _ded_ch_id:
                        if _post_photo:
                            _ded_post_msg = await _safe_send(
                                upload_bot, upload_bot.send_photo, _ded_ch_id,
                                photo=_post_photo,
                                caption=_ep_caption, reply_markup=_cur_kb,
                                _label="ded send_photo",
                            )
                        else:
                            _ded_post_msg = await _safe_send(
                                upload_bot, upload_bot.send_message, _ded_ch_id,
                                text=_ep_caption, reply_markup=_cur_kb,
                                _label="ded send_message",
                            )
                        try:
                            await _safe_send(
                                upload_bot, upload_bot.send_sticker, _ded_ch_id,
                                sticker=STICKER_MAIN, _label="ded send_sticker",
                            )
                        except Exception as _stk_e:
                            LOGS.warning(f"sticker send failed (ded): {_stk_e}")
                        _notify_kb = (
                            InlineKeyboardMarkup([[
                                InlineKeyboardButton("▶️ Watch Now", url=_invite)
                            ]]) if _invite else _cur_kb
                        )
                        if _post_photo:
                            _ep_main_post = await _safe_send(
                                upload_bot, upload_bot.send_photo, target_channel,
                                photo=_post_photo,
                                caption=_ep_caption, reply_markup=_notify_kb,
                                _label="main send_photo",
                            )
                        else:
                            _ep_main_post = await _safe_send(
                                upload_bot, upload_bot.send_message, target_channel,
                                text=_ep_caption, reply_markup=_notify_kb,
                                _label="main send_message",
                            )
                    else:
                        if _post_photo:
                            _ep_main_post = await _safe_send(
                                upload_bot, upload_bot.send_photo, target_channel,
                                photo=_post_photo,
                                caption=_ep_caption, reply_markup=_cur_kb,
                                _label="main send_photo",
                            )
                        else:
                            _ep_main_post = await _safe_send(
                                upload_bot, upload_bot.send_message, target_channel,
                                text=_ep_caption, reply_markup=_cur_kb,
                                _label="main send_message",
                            )
                        try:
                            await _safe_send(
                                upload_bot, upload_bot.send_sticker, target_channel,
                                sticker=STICKER_MAIN, _label="main send_sticker",
                            )
                        except Exception as _stk_e:
                            LOGS.warning(f"sticker send failed (main): {_stk_e}")
                    _post_sent = True
                    LOGS.info(
                        f"✅ ongoing: channel card posted [{qual}] {_ep_label} "
                        f"(ded_msg={_ded_post_msg.id if _ded_post_msg else None}, "
                        f"main_msg={_ep_main_post.id if _ep_main_post else None})"
                    )

                else:
                    # ── Subsequent qualities — edit the keyboard in place ──────
                    if _ded_post_msg:
                        try:
                            await upload_bot.edit_message_reply_markup(
                                _ded_ch_id, _ded_post_msg.id, reply_markup=_cur_kb
                            )
                        except Exception as _e:
                            await rep.report(
                                f"⚠️ Failed to edit ded-channel keyboard [{qual}]: {_e}",
                                "warning", log=False,
                            )
                    if _ep_main_post and not _ded_ch_id:
                        try:
                            await upload_bot.edit_message_reply_markup(
                                target_channel, _ep_main_post.id, reply_markup=_cur_kb
                            )
                        except Exception as _e:
                            await rep.report(
                                f"⚠️ Failed to edit main-channel keyboard [{qual}]: {_e}",
                                "warning", log=False,
                            )

            if _ep_main_post:
                ep_posts[_ep_num] = {
                    'main_id':    _ep_main_post.id,
                    'qual_links': qual_links,
                    'ep_label':   _ep_label,
                }

            try:
                if _os.path.isfile(_current_file):
                    _os.remove(_current_file)
            except Exception:
                pass

        # ── Finalise ──────────────────────────────────────────────────────────
        await task_queue.mark_done(task_id)
        await stat_msg.delete()

        _dedup_key_done = torrent or name
        bot_loop.create_task(db.mark_torrent_seen(_dedup_key_done))
        if name and name != _dedup_key_done:
            bot_loop.create_task(db.mark_torrent_seen(name))

        try:
            from aioshutil import rmtree as _aiormtree
            if _ongoing_base and _os.path.isdir(_ongoing_base):
                await _aiormtree(_ongoing_base)
        except Exception:
            pass

        # ani_cache['completed'] write removed — DB is the source of truth

    except Exception:
        await rep.report(format_exc(), "error")
        if task_id:
            _err_tq = (
                __import__('bot.core.task_queue', fromlist=['batch_task_queue']).batch_task_queue
                if locals().get("is_batch") else task_queue
            )
            retry  = await _err_tq.increment_retry(task_id)
            status = "pending" if retry < MAX_RETRIES else "failed"
            await _err_tq.update_task(task_id, status=status)
