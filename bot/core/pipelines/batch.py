"""
pipelines/batch.py
==================
Full encode+upload pipeline for completed/batch anime releases.

Handles
-------
- BDRip / Bluray season packs  (e.g. "Demon Slayer [BD][1080p]")
- Complete season torrents      (e.g. "Attack on Titan S4 Complete")
- Single episodes that route here because a dedicated channel is connected

Pipeline overview
-----------------
1.  Download ALL episodes to ./downloads/batch/<safe_name>/Season_N/
2.  Stream-validate every file with ffprobe — switch to alt torrent on failure
3.  [Hdri] Remux (stream copy) all episodes → batch-upload header+files+sticker
4.  [1080/720/480] Encode one episode at a time → upload immediately after encode
5.  After each quality: update the index post's button grid live (2×2 keyboard)
6.  After all qualities: send/edit a notify post on MAIN/BATCH_MAIN_CHANNEL
7.  Send ending sticker + ending card to the dedicated channel
8.  Cleanup: delete all encoded files and the download folder

Chunk processing
----------------
Large seasons (>ENCODE_CHUNK eps) are split into chunks so encoded files
don't accumulate on disk while waiting.  The last tiny chunk (<5 eps) is
merged into the previous one to avoid lone trailing episodes.

Resume / restart safety
-----------------------
- The index post ID is persisted in batch_db under ani_id AND torrent name so
  future restarts find the same post without creating a duplicate.
- Episode-quality combos that are already in DB are skipped (qual_links rebuilt
  from DB so the index keyboard stays complete).
- Alt torrents (stored in task DB) are popped on probe/encode failures and the
  entire batch re-queued from scratch with the fallback torrent.
"""

import os as _os
import re as _re_ep
import time as _time
from traceback import format_exc

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, ani_cache, batch_encode_lock, batch_dl_lock
from bot.core.tordownload import TorDownloader
from bot.core.database import db, batch_db
from bot.core.func_utils import editMessage, sendMessage, encode
from bot.core.text_utils import TextEditor, detect_audio, _normalize_anime_title
from bot.core.ffencoder import FFEncoder
from bot.core.tguploader import TgUploader
from bot.core.reporter import batch_rep
from bot.core.task_queue import batch_task_queue, MAX_RETRIES

from .constants import QUAL_LABELS, AUDIO_LABELS, VIDEO_EXTS, SKIP_FOLDERS
from .helpers import (
    _qual_file_store, _make_link, _qual_btns_to_keyboard,
    _send_ending_post, extra_utils, hdri_passthrough,
    _warm_peer, _safe_send,
)
from bot.core.memguard import reclaim_memory, areclaim_memory, drop_page_cache, get_available_mb

# ── Module-level counter for batch queue tie-breaking ────────────────────────
import itertools as _itertools
_batch_counter = _itertools.count()

BATCH_STICKER = "CAACAgUAAxUAAWmyyxy1E7jGCxo3hNPNhnwOyHbuAAL_IAAC5n-ZVSGceeVLAz58OgQ"


def _safe_dl_path(base_root: str, *parts: str) -> str:
    """Build a download path and assert it stays under base_root."""
    root   = _os.path.realpath(base_root)
    joined = _os.path.realpath(_os.path.join(base_root, *parts))
    if not joined.startswith(root + _os.sep) and joined != root:
        raise ValueError(f"Path traversal blocked: {joined!r} escapes {root!r}")
    return joined


async def _probe_ok(fpath: str) -> bool:
    """Return True if ffprobe finds at least one video stream in fpath."""
    import asyncio as _aio
    try:
        proc = await _aio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            fpath,
            stdout=_aio.subprocess.PIPE,
            stderr=_aio.subprocess.PIPE,
        )
        out, _ = await _aio.wait_for(proc.communicate(), timeout=30)
        return b"video" in out
    except Exception:
        return False


async def _run_batch_pipeline(
    name: str,    torrent: str,
    force: bool,
    task_id: str,
    source_priority: int,
    target_channel: int,
    file_store: int,
    log_channel: int,
    upload_bot,
):
    """
    Batch pipeline: download → probe → encode per quality → upload → post index.
    See module docstring for full overview.
    """
    ENCODE_QUALS = [q for q in ['1080', '720', '480'] if q in Var.QUALS]
    ALL_QUALS    = ([q for q in ['Hdri'] if q in Var.QUALS]) + ENCODE_QUALS

    # ── File-classification sets ──────────────────────────────────────────────
    SPECIAL_WORDS      = {"special", "specials", "extra", "extras", "ova", "ovas", "oad", "preview"}
    SKIP_ONLY          = {"ncop", "nced", "oped", "pv"}
    SKIP_FOLDER_KEYWORDS = {
        "director's cut", "directors cut", "director cut",
        "directorcut", " dc ", "(dc)", "[dc]",
    }

    try:
        # ── 1. AniList metadata ───────────────────────────────────────────────
        aniInfo = TextEditor(name)
        await aniInfo.load_anilist()
        audio   = detect_audio(name)
        ani_id  = aniInfo.adata.get('id')
        _titles = aniInfo.adata.get("title", {})

        def _clean_title(t):
            t = _re_ep.sub(r'[【】「」『』〔〕［］\[\]]', ' ', t)
            t = _re_ep.sub(
                r'\s*(Season\s*\d+|S\d{1,2}|Part\s*\d+|Cour\s*\d+|\d+(st|nd|rd|th)\s+Season)\s*$',
                '', t, flags=_re_ep.IGNORECASE,
            )
            return _re_ep.sub(r'\s+', ' ', t).strip()

        _eng = _clean_title(_titles.get("english") or "")
        _rom = _clean_title(_titles.get("romaji") or "")
        if _eng and _rom and _eng.lower() != _rom.lower():
            anime_title = f"{_eng} | {_rom}"
        elif _eng:
            anime_title = _eng
        elif _rom:
            anime_title = _rom
        else:
            anime_title = _normalize_anime_title(name) or name
        # Season number → injected into adata so card shows the correct pill
        _season_raw = aniInfo.pdata.get("anime_season", "1")
        if isinstance(_season_raw, list):
            _season_raw = str(_season_raw[-1])
        try:
            _season_raw = str(int(_season_raw))
        except (ValueError, TypeError):
            _season_raw = str(_season_raw) if _season_raw else "1"
        _season_key = f"s{_season_raw}"
        try:
            aniInfo.adata["seasonNumber"] = int(_season_raw)
        except (ValueError, TypeError):
            pass

        poster_url = await aniInfo.get_poster(upload_bot=upload_bot)
        aniInfo._cached_poster_url = poster_url
        banner_url = await aniInfo.get_banner()

        _lookup_names = [n for n in [
            _titles.get("romaji"), _titles.get("english"), _normalize_anime_title(name),
        ] if n and n.strip()]
        channel_details = None

        # ani_id lookup first (most reliable), fall back to name matching
        if ani_id:
            try:
                for _ch in await db.get_all_anime_channels():
                    if str(_ch.get("ani_id")) == str(ani_id):
                        channel_details = _ch
                        break
            except Exception:
                pass
        if not channel_details:
            for lname in _lookup_names:
                channel_details = await db.find_channel_by_anime_title(lname, db_type="completed")
                if not channel_details:
                    channel_details = await db.find_channel_by_anime_title(lname, db_type="ongoing")
                if channel_details:
                    break

        await batch_task_queue.update_task(task_id, status="downloading", ani_id=ani_id)
        await batch_rep.report(f"📦 Batch pipeline started: {anime_title}", "info")

        stat_channel = log_channel or target_channel
        stat_msg = await sendMessage(stat_channel, f"<b>📥 Downloading batch: {anime_title}</b>")

        # ── 2. Download ───────────────────────────────────────────────────────
        _safe_name     = _re_ep.sub(r'[^\w\s-]', ' ', name)
        _safe_name     = _re_ep.sub(r'\s+', ' ', _safe_name).strip().replace(' ', '_')[:50]
        _season_folder = f"Season_{_season_raw}"
        _batch_base    = _safe_dl_path("./downloads/batch", _safe_name, _season_folder)
        _os.makedirs(_batch_base, exist_ok=True)

        # If torrent is already a local folder path (from run_batch_on_folder), skip download
        _torrent_is_folder = (
            torrent
            and (_os.path.isabs(torrent) or torrent.startswith("./"))
            and not torrent.startswith("magnet:")
            and not torrent.startswith("http")
        )
        if _torrent_is_folder:
            if _os.path.isdir(torrent):
                dl = torrent
                await editMessage(stat_msg, f"<b>📂 Using pre-downloaded folder: {anime_title}</b>")
            else:
                await batch_rep.report(
                    f"⚠️ Pre-downloaded folder missing: {torrent} — marking failed", "error"
                )
                await stat_msg.delete()
                await batch_task_queue.update_task(
                    task_id, status="failed",
                    error="Pre-downloaded folder missing — re-run Upload All",
                )
                return
        else:
            async with batch_dl_lock:
                dl = await TorDownloader(_batch_base, use_stable_dir=True).download(
                    torrent, name, stat_msg=stat_msg, anime_name=anime_title
                )
            if not dl or not _os.path.exists(dl):
                retry = await batch_task_queue.increment_retry(task_id)
                await batch_rep.report(
                    f"Batch download failed (retry {retry}/{MAX_RETRIES}): {name}", "error"
                )
                await stat_msg.delete()
                await batch_task_queue.update_task(
                    task_id, status="pending" if retry < MAX_RETRIES else "failed"
                )
                return

        # ── 3. Recover stranded temp files from a crashed previous encode ─────
        if _os.path.isdir(dl):
            import glob as _glob
            for _qd_name in ['Hdri', '1080', '720', '480']:
                _qd_path = _os.path.join(_batch_base, _qd_name)
                if not _os.path.isdir(_qd_path):
                    continue
                for _stranded in _glob.glob(_os.path.join(_qd_path, 'in_*.mkv')):
                    _base = _os.path.basename(_stranded)
                    _dest = _os.path.join(dl, _base.replace('in_', 'recovered_', 1))
                    try:
                        _os.rename(_stranded, _dest)
                        await batch_rep.report(
                            f"♻️ Recovered stranded temp: {_base}", "info", log=False
                        )
                    except Exception as _e:
                        await batch_rep.report(f"⚠️ Recovery failed {_base}: {_e}", "warning", log=False)

        # ── 4. Collect source files ───────────────────────────────────────────
        if _os.path.isdir(dl):
            _src_files  = []
            _spec_files = []
            for _root, _, _fnames in _os.walk(dl):
                _folder_name = _os.path.basename(_root).lower().strip()
                if _folder_name in SKIP_FOLDERS:
                    continue
                if any(kw in _folder_name for kw in SKIP_FOLDER_KEYWORDS):
                    await batch_rep.report(
                        f"🗂 Skipping repack folder: {_os.path.basename(_root)}", "info", log=False
                    )
                    continue
                for _fn in sorted(_fnames):
                    if _os.path.splitext(_fn)[1].lower() not in VIDEO_EXTS:
                        continue
                    _fnl = _fn.lower()
                    if any(sw in _fnl for sw in SKIP_ONLY):
                        continue
                    if _re_ep.match(r'^(op|ed|nced|ncop)(v\d+)?.', _fnl):
                        continue
                    if any(sw in _fnl for sw in SPECIAL_WORDS):
                        _spec_files.append(_os.path.join(_root, _fn))
                    else:
                        _src_files.append(_os.path.join(_root, _fn))
            _src_files  = sorted(_src_files,  key=lambda x: _os.path.basename(x))
            _spec_files = sorted(_spec_files, key=lambda x: _os.path.basename(x))

            # Season filter — keep only files that match the target season number
            _ep_season_int = 0
            try:
                _ep_season_int = int(str(aniInfo.pdata.get("anime_season", "")).strip())
            except (ValueError, TypeError):
                pass

            if _ep_season_int > 0:
                _szn_patterns = [
                    rf'[Ss]0*{_ep_season_int}[^0-9]',
                    rf'[Ss]eason\s*0*{_ep_season_int}',
                ]
                _other_szn_pats = [r'[Ss]eason\s*0*(\d+)', r'[Ss]0*(\d+)[^0-9]']
                def _matches_season(fpath):
                    fn     = _os.path.basename(fpath)
                    folder = _os.path.basename(_os.path.dirname(fpath))
                    if any(_re_ep.search(p, fn) for p in _szn_patterns):
                        return True
                    if any(_re_ep.search(p, folder) for p in _szn_patterns):
                        return True
                    for _pat in _other_szn_pats:
                        _m = _re_ep.search(_pat, folder)
                        if _m:
                            try:
                                if int(_m.group(1)) != _ep_season_int:
                                    return False
                            except (IndexError, ValueError):
                                pass
                    return True

                _filtered = [f for f in _src_files if _matches_season(f)]
                if _filtered:
                    _removed = len(_src_files) - len(_filtered)
                    if _removed > 0:
                        await batch_rep.report(
                            f"🗂 Season filter: kept {len(_filtered)}, removed {_removed} from other seasons",
                            "info", log=False,
                        )
                    _src_files = _filtered
                else:
                    await batch_rep.report(
                        f"⚠️ Season filter found 0 matches for S{_ep_season_int} — keeping all",
                        "warning", log=False,
                    )
        else:
            _src_files  = [dl]
            _spec_files = []

        if not _src_files:
            await batch_rep.report(f"No video files found in batch: {name}", "error")
            await stat_msg.delete()
            await batch_task_queue.update_task(task_id, status="failed", error="No video files found")
            return

        # ── 5. Stream validation (ffprobe all files) ──────────────────────────
        _corrupt_files = [_os.path.basename(f) for f in _src_files if not await _probe_ok(f)]
        if _corrupt_files:
            await batch_rep.report(
                f"🚨 {len(_corrupt_files)}/{len(_src_files)} file(s) failed stream probe "
                f"for {anime_title}: {_corrupt_files[:5]}",
                "error",
            )
            _alt = await batch_task_queue.pop_alt_torrent(task_id)
            if _alt:
                _alt_name, _alt_url = _alt
                await batch_rep.report(f"🔄 Falling back to alt torrent: {_alt_name[:70]}", "info")
                import shutil as _st_probe
                try:
                    if _os.path.isdir(_batch_base):
                        _st_probe.rmtree(_batch_base, ignore_errors=True)
                except Exception:
                    pass
                from .workers import _batch_queue, _batch_counter
                _alt_task_id = await batch_task_queue.enqueue(_alt_name, _alt_url, source_priority)
                await _batch_queue.put((
                    max(0, source_priority - 1), next(_batch_counter),
                    _alt_name, _alt_url, True, _alt_task_id, source_priority, [], False,
                ))
                await batch_task_queue.update_task(
                    task_id, status="failed",
                    error=f"Corrupt files: {_corrupt_files[:3]} — retried with alt",
                )
                await stat_msg.delete()
                return
            else:
                await batch_rep.report(
                    f"❌ No alt torrent for {anime_title} — proceeding (corrupt files skipped per-quality)",
                    "warning",
                )

        # ── 6. Single-file backfill (RSS single ep → check for missing eps) ───
        if len(_src_files) == 1 and ani_id and channel_details:
            await batch_rep.report(
                f"📄 Single episode via RSS — checking for missing season episodes: {name[:60]}",
                "info", log=False,
            )
            try:
                from bot.modules.channel_manager import (
                    _get_sequel_chain, _get_aired_episodes_from_jikan,
                    _get_search_names, _search_nyaa_rss_season, _search_nyaa,
                    _search_nyaa_html, _sanitize_nyaa_query, _is_non_english,
                    _torrent_priority, _season_start_year_cache,
                )
                _ep_season_detect = aniInfo.pdata.get("anime_season", "1")
                if isinstance(_ep_season_detect, list):
                    _ep_season_detect = _ep_season_detect[0]
                try:
                    _cur_season = int(str(_ep_season_detect).strip()) or 1
                except (ValueError, TypeError):
                    _cur_season = 1

                _uploaded = set()
                _ep_data_miss = await batch_db.get_batch_ep_links(ani_id, season=f"s{_cur_season}")
                if _ep_data_miss:
                    _uploaded = {int(k) for k in _ep_data_miss.keys()}

                import re as _re_folder_eps
                _already_in_folder: set = set()
                if _os.path.isdir(_batch_base):
                    for _rf, _, _rfns in _os.walk(_batch_base):
                        for _rfn in _rfns:
                            if _os.path.splitext(_rfn)[1].lower() not in VIDEO_EXTS:
                                continue
                            _ep_m = _re_folder_eps.search(
                                r'[Ss]\d+[Ee](\d+)|[-\s](\d{1,3})\s*[\(\[]', _rfn
                            )
                            if _ep_m:
                                _already_in_folder.add(int(_ep_m.group(1) or _ep_m.group(2)))

                _mal_id_miss = aniInfo.adata.get("idMal")
                _aired_miss  = 0
                _release_year_miss = (aniInfo.adata.get("startDate") or {}).get("year")
                if _mal_id_miss:
                    try:
                        _chain_miss = await _get_sequel_chain(int(_mal_id_miss))
                        _s_mal_miss = _chain_miss[_cur_season - 1] if _cur_season <= len(_chain_miss) else _chain_miss[-1]
                        _aired_miss = await _get_aired_episodes_from_jikan(_s_mal_miss) or 0
                        _sy = _season_start_year_cache.get(_s_mal_miss)
                        if _sy:
                            _release_year_miss = _sy
                    except Exception:
                        pass

                _all_eps_miss = list(range(1, (_aired_miss or 1) + 1))
                _skip_eps     = _uploaded | _already_in_folder
                _missing      = sorted(ep for ep in _all_eps_miss if ep not in _skip_eps)

                _already_dl_ep = None
                _m = _re_ep.search(r'[Ss]\d+[Ee](\d+)|[-\s](\d{1,3})\s*[\(\[]', name)
                if _m:
                    _already_dl_ep = int(_m.group(1) or _m.group(2))
                if _already_dl_ep in _missing:
                    _missing.remove(_already_dl_ep)

                if _missing:
                    await batch_rep.report(
                        f"📥 Downloading {len(_missing)} missing S{_cur_season:02d} ep(s): "
                        f"EP{_missing[0]:02d}–EP{_missing[-1]:02d}",
                        "info", log=False,
                    )
                    _snames_miss = await _get_search_names(anime_title)
                    _miss_cands = {}
                    _rss_res = await _search_nyaa_rss_season(
                        _snames_miss, season=_cur_season, release_year=_release_year_miss
                    )
                    for _mt, _mu in _rss_res:
                        _mm = _re_ep.search(r'[Ss]\d+[Ee](\d+)|[-\s](\d{1,3})\s*[\(\[]', _mt)
                        _me = int(_mm.group(1) or _mm.group(2)) if _mm else None
                        if _me and _me in _missing:
                            if _me not in _miss_cands or _torrent_priority(_mt) < _miss_cands[_me][0]:
                                _miss_cands[_me] = (_torrent_priority(_mt), _mt, _mu)

                    for _mep in _missing:
                        if _mep not in _miss_cands:
                            for _sn in _snames_miss[:2]:
                                _hq = _sanitize_nyaa_query(f"{_sn} s{_cur_season:02d}e{_mep:02d}")
                                for _het, _heu in await _search_nyaa_html(
                                    _hq, season=_cur_season,
                                    release_year=_release_year_miss, max_pages=5,
                                ):
                                    _tl3 = _het.lower()
                                    if any(p in _tl3 for p in [
                                        f"s{_cur_season:02d}e{_mep:02d}",
                                        f"- {_mep:02d} ", f"e{_mep:02d}",
                                    ]):
                                        _miss_cands[_mep] = (_torrent_priority(_het), _het, _heu)
                                        break
                                if _mep in _miss_cands:
                                    break

                        if _mep not in _miss_cands:
                            await batch_rep.report(
                                f"⚠️ S{_cur_season:02d}E{_mep:02d} not found — skipping",
                                "warning", log=False,
                            )
                            continue

                        _, _mdl_title, _mdl_url = _miss_cands[_mep]
                        await batch_rep.report(
                            f"⬇️ S{_cur_season:02d}E{_mep:02d}: {_mdl_title[:55]}", "info", log=False
                        )
                        try:
                            _mdl = await TorDownloader(_batch_base, use_stable_dir=True).download(
                                _mdl_url, _mdl_title
                            )
                            if _mdl and _os.path.exists(_mdl):
                                await batch_rep.report(
                                    f"✅ S{_cur_season:02d}E{_mep:02d} downloaded", "info", log=False
                                )
                        except Exception as _de:
                            await batch_rep.report(
                                f"⚠️ S{_cur_season:02d}E{_mep:02d} download error: {_de}",                                "warning", log=False,
                            )

                    dl = _batch_base
                    await batch_rep.report(
                        f"📂 Backfill done — scanning full folder: {_batch_base}", "info", log=False
                    )
                else:
                    await batch_rep.report(
                        f"✅ No missing eps for S{_cur_season:02d} — processing EP{_already_dl_ep:02d} alone",
                        "info", log=False,
                    )
            except Exception as _miss_err:
                await batch_rep.report(
                    f"⚠️ Missing-episode check failed (non-fatal): {_miss_err}",
                    "warning", log=False,
                )

        # ── 7. Build ep_info list ─────────────────────────────────────────────
        _anilist_eps = aniInfo.adata.get("episodes")
        await batch_rep.report(
            f"✅ Downloaded {len(_src_files)} episode(s) for: {anime_title}"
            + (f" (AniList planned total: {_anilist_eps})" if _anilist_eps else ""),
            "info", log=False,
        )

        ep_info = []
        for _idx, _file in enumerate(_src_files):
            _fname = _os.path.basename(_file)
            _m = (
                _re_ep.search(r'S\d+E(\d+)', _fname, _re_ep.IGNORECASE) or
                _re_ep.search(r'\s-\s(\d{1,4})\s*[\(\[]', _fname) or
                _re_ep.search(r'[-_]\s*(\d{1,4})\s*(?:v\d)?\s*[-_\.\s\(\[]', _fname) or
                _re_ep.search(r'\[(\d{1,3})\]', _fname)
            )
            _ep_num = int(_m.group(1)) if _m else (_idx + 1)
            if _ep_num > 500:  # sanity check — matched a hash or year
                _ep_num = _idx + 1
            _ep_ai       = TextEditor(name)
            _ep_ai.adata = aniInfo.adata
            _ep_ai.pdata = dict(aniInfo.pdata)
            _ep_ai.pdata["episode_number"] = _ep_num
            ep_info.append({"ep_num": _ep_num, "aniInfo": _ep_ai, "src": _file})

        ep_info = sorted(ep_info, key=lambda x: x["ep_num"])

        # total_eps = highest episode number (not file count, which may be filtered)
        total_eps = ep_info[-1]["ep_num"] if ep_info else len(_src_files)

        # ── 8. Chunk large seasons ────────────────────────────────────────────
        _chunk_size  = Var.ENCODE_CHUNK
        _total_files = len(ep_info)
        _chunks = [ep_info[i:i + _chunk_size] for i in range(0, _total_files, _chunk_size)]
        _MIN_TAIL = 5
        if len(_chunks) > 1 and len(_chunks[-1]) < _MIN_TAIL:
            _chunks[-2] = _chunks[-2] + _chunks[-1]
            _chunks.pop()
        if len(_chunks) > 1:
            await batch_rep.report(
                f"📦 {anime_title}: {_total_files} eps → {len(_chunks)} chunk(s) of ≤{_chunk_size}",
                "info", log=False,
            )

        await batch_task_queue.update_task(task_id, status="encoding")

        ep_links: dict = {e["ep_num"]: {} for e in ep_info}

        # Reload already-uploaded links from DB (resume support)
        if ani_id:
            _saved = await batch_db.get_batch_ep_links(ani_id, season=_season_key)
            for _en, _qmap in _saved.items():
                for _ql, _lnk in _qmap.items():
                    if _ql in ALL_QUALS:
                        ep_links.setdefault(int(_en), {})[_ql] = _lnk
            _resumed = sum(1 for ep in ep_links.values() for v in ep.values() if v)
            if _resumed:
                await batch_rep.report(
                    f"♻️ Resume: {_resumed} ep/quality combo(s) already done.", "info", log=False
                )

        qual_ranges: dict = {}
        _bot_me_username = (await upload_bot.get_me()).username

        # ── 9. Create or reuse the channel index post ─────────────────────────
        _post_channel = channel_details['channel_id'] if channel_details else target_channel

        # Warm peer caches before any send_photo / send_message — dedicated batch
        # channels added via /addchannel are NOT pre-verified at startup so the
        # first post can hit PEER_ID_INVALID after a cold restart.
        await _warm_peer(upload_bot, _post_channel)
        if target_channel and target_channel != _post_channel:
            await _warm_peer(upload_bot, target_channel)
        _index_caption = (
            f"<b>{anime_title}</b>\n"
            f"<b>{'─' * 28}</b>\n"
            f"<b>➤ Season - {str(_season_raw).zfill(2)}</b>\n"
            f"<b>➤ Episodes - {total_eps}</b>\n"
            f"<b>➤ Quality: Multi [{AUDIO_LABELS.get(audio, audio)}]</b>\n"
            f"<b>{'─' * 28}</b>\n"
            f"<blockquote>⏳ Encoding in progress — quality buttons will appear as each quality finishes.</blockquote>"        )

        _index_post      = None
        _batch_qual_links: dict = {}

        # Try to recover existing post (ani_id first, then torrent name fallback)
        _saved_bl = None
        if ani_id:
            _saved_bl = await batch_db.get_batch_link(ani_id, file_store=file_store, season=_season_key)
        if not _saved_bl:
            _saved_bl = await batch_db.get_batch_link_by_name(name, season=_season_key)

        _saved_post_id = (_saved_bl or {}).get("index_post_id")
        _saved_post_ch = (_saved_bl or {}).get("index_post_channel")
        _saved_store   = abs(int((_saved_bl or {}).get("file_store", Var.BATCH_FILE_STORE)))

        if _saved_post_id and _saved_post_ch:
            try:
                _index_post = await upload_bot.get_messages(_saved_post_ch, _saved_post_id)
                if _index_post and not _index_post.empty:
                    await batch_rep.report(
                        f"♻️ Reusing existing channel post (id={_saved_post_id})", "info"
                    )
                    for _rq in ALL_QUALS:
                        _rf = (_saved_bl or {}).get(f"first_{_rq}")
                        _rl = (_saved_bl or {}).get(f"last_{_rq}")
                        if _rf and _rl:
                            try:
                                _rb64 = await encode(f"get-{_saved_store}-{_rf}-{_rl}")
                                _batch_qual_links[_rq] = f"https://telegram.me/{_bot_me_username}?start={_rb64}"
                            except Exception:
                                pass
                else:
                    _index_post = None
            except Exception:
                _index_post = None

        if _index_post is None:
            # Season separator sticker if a previous season post exists
            _SEASON_SEP_STICKER = "CAACAgUAAxUAAWnCPX6BxLo6v-iczliDqTBRPNskAAL_IAAC5n-ZVSGceeVLAz58OgQ"
            try:
                if _season_raw and str(_season_raw).isdigit() and int(_season_raw) > 1:
                    _prev_sk = f"s{int(_season_raw) - 1}"
                    _prev_bl = await batch_db.get_batch_link(ani_id, file_store=file_store, season=_prev_sk) if ani_id else None
                    if (_prev_bl or {}).get("index_post_id"):
                        await upload_bot.send_sticker(_post_channel, sticker=_SEASON_SEP_STICKER)
            except Exception:
                pass

            if poster_url:                _index_post = await _safe_send(
                    upload_bot, upload_bot.send_photo, _post_channel,
                    photo=poster_url, caption=_index_caption,
                    _label="batch index send_photo",
                )
            else:
                _index_post = await _safe_send(
                    upload_bot, upload_bot.send_message, _post_channel,
                    text=_index_caption, _label="batch index send_message",
                )

            _post_meta = {
                "index_post_id":      _index_post.id,
                "index_post_channel": _post_channel,
                "file_store":         file_store,
                "torrent_name":       name,
            }
            if ani_id:
                await batch_db.save_batch_link(
                    ani_id, 0, 0, file_store, season=_season_key, extra=_post_meta
                )
            await batch_db.save_batch_link_by_name(name, _post_meta, season=_season_key)

        async def _rebuild_index_keyboard(extra_qual: str = None, extra_link: str = None):
            """Add extra_qual → extra_link and redraw the index post keyboard."""
            nonlocal _batch_qual_links
            if extra_qual and extra_link:
                _batch_qual_links[extra_qual] = extra_link
            _kb = _qual_btns_to_keyboard(_batch_qual_links)
            try:
                await editMessage(
                    _index_post,
                    _index_post.caption.html if _index_post.caption else _index_caption,
                    _kb,
                )
            except Exception:
                pass

        # ── 10. Quality loop ──────────────────────────────────────────────────
        for _chunk_idx, _chunk_ep_info in enumerate(_chunks):
            if len(_chunks) > 1:
                _cs = _chunk_ep_info[0]["ep_num"]
                _ce = _chunk_ep_info[-1]["ep_num"]
                await batch_rep.report(
                    f"🔢 Chunk {_chunk_idx + 1}/{len(_chunks)}: E{_cs:02d}–E{_ce:02d} ({len(_chunk_ep_info)} eps)",
                    "info", log=False,
                )
            ep_info = _chunk_ep_info  # narrow scope for this chunk

            for qual in ALL_QUALS:
                _pending = [ep for ep in ep_info if not ep_links[ep["ep_num"]].get(qual)]
                if not _pending:
                    # DB says all eps uploaded — but verify the first message still
                    # exists in the file store channel before trusting the resume.
                    # An OOM kill or manual channel wipe can leave DB stale.
                    _resume_verified = False
                    if ani_id:
                        _bl = await batch_db.get_batch_link(
                            ani_id, file_store=file_store, season=_season_key
                        )
                        _sf = (_bl or {}).get(f"first_{qual}")
                        _sl = (_bl or {}).get(f"last_{qual}")
                        _skip_store = abs(int((_bl or {}).get("file_store", file_store)))
                        if _sf:
                            try:
                                _verify_msg = await upload_bot.get_messages(_skip_store, _sf)
                                if _verify_msg and not _verify_msg.empty:
                                    _resume_verified = True
                            except Exception:
                                pass

                    if not _resume_verified:
                        # Messages are gone — DB is stale. Clear ep_links for this
                        # quality so the full encode+upload runs again.
                        await batch_rep.report(
                            f"⚠️ [{qual}] DB says done but messages missing in file store — "
                            f"re-uploading.", "warning", log=False
                        )
                        for _ep in ep_info:
                            ep_links[_ep["ep_num"]].pop(qual, None)
                        # Clear stale DB entry for this quality
                        if ani_id:
                            try:
                                await batch_db.clear_batch_qual(ani_id, qual, season=_season_key)
                            except Exception:
                                pass
                        # Fall through to the encode+upload block below
                    else:
                        await batch_rep.report(
                            f"⏭ [{qual}] already fully uploaded — skipping.", "info", log=False
                        )
                        if _sf and _sl:
                            try:
                                _b64_skip = await encode(f"get-{_skip_store}-{_sf}-{_sl}")
                                _skip_link = f"https://telegram.me/{_bot_me_username}?start={_b64_skip}"
                                qual_ranges[qual] = (_sf, _sl)
                                await _rebuild_index_keyboard(extra_qual=qual, extra_link=_skip_link)
                            except Exception as _ske:
                                await batch_rep.report(
                                    f"Skip button restore failed [{qual}]: {_ske}", "warning", log=False                                )
                        continue

                # Re-check _pending after possible stale-cache invalidation above
                _pending = [ep for ep in ep_info if not ep_links[ep["ep_num"]].get(qual)]
                if not _pending:
                    continue

                await editMessage(
                    stat_msg,
                    f"<b>📦 {anime_title}</b>\n\n"
                    f"<blockquote>⚙️ Processing [{qual}] — {total_eps} episode(s)</blockquote>",
                )

                if qual == 'Hdri':
                    # ── Hdri: encode-then-upload per episode (no accumulation) ─
                    _hdri_dir   = _os.path.join(_batch_base, 'Hdri')
                    _os.makedirs(_hdri_dir, exist_ok=True)
                    _hdri_failed_eps: list = []
                    _hdri_qs = _qual_file_store('Hdri', file_store, pipeline="batch")
                    _hdri_season = int(_season_raw) if str(_season_raw).isdigit() else 1
                    _hdri_year   = (aniInfo.adata.get("startDate") or {}).get("year")

                    # Lazy-load Nyaa helpers (only needed when per-episode retry fires)
                    _nyaa_helpers = None
                    async def _load_nyaa_helpers():
                        nonlocal _nyaa_helpers
                        if _nyaa_helpers is not None:
                            return _nyaa_helpers
                        from bot.modules.channel_manager import (
                            _get_search_names, _search_nyaa, _search_nyaa_html,
                            _search_nyaa_rss_season, _sanitize_nyaa_query,
                            _torrent_priority, _is_non_english,
                        )
                        _nyaa_helpers = (
                            _get_search_names, _search_nyaa, _search_nyaa_html,
                            _search_nyaa_rss_season, _sanitize_nyaa_query,
                            _torrent_priority, _is_non_english,
                        )
                        return _nyaa_helpers

                    # ── First pass: encode only, collect failures ─────────────
                    # We do a lightweight encode-only pass first so we can detect
                    # corrupt sources and trigger alt-torrent fallback BEFORE we
                    # have sent the header message to Telegram.
                    _probe_pass_failed: list = []
                    for _ep in ep_info:
                        _src   = _ep["src"]
                        _ep_ai = _ep["aniInfo"]
                        _ep_ai.pdata["episode_number"] = _ep["ep_num"]                        
                        _fname = await _ep_ai.get_upname('Hdri')
                        _hdri_expected = _os.path.join(_hdri_dir, _fname)

                        if not _os.path.exists(_src):
                            await batch_rep.report(
                                f"Source missing [Hdri] E{_ep['ep_num']:02d} — skipping", "error"
                            )
                            continue

                        _ep_idx_hdri = ep_info.index(_ep)
                        await editMessage(
                            stat_msg,
                            f"<b>📦 {anime_title}</b>\n\n"
                            f"<blockquote>⚙️ Encoding [Hdri] E{_ep['ep_num']:02d}"
                            f" ({_ep_idx_hdri + 1}/{len(ep_info)})</blockquote>",
                        )

                        if _os.path.exists(_hdri_expected) and _os.path.getsize(_hdri_expected) > 0:
                            pass  # already encoded from a previous interrupted run
                        else:
                            try:
                                _enc_out = await hdri_passthrough(_src, _hdri_dir, _fname)
                            except Exception:
                                _enc_out = None

                            if not _enc_out:
                                await batch_rep.report(
                                    f"Hdri encode failed E{_ep['ep_num']:02d} — will attempt per-ep re-download",
                                    "warning",
                                )
                                _probe_pass_failed.append(_ep)

                    _hdri_failed_eps = list(_probe_pass_failed)

                    # DB alt check (preferred over per-episode Nyaa patching)
                    _still_failed: list = []
                    if _hdri_failed_eps:
                        _db_alt = await batch_task_queue.pop_alt_torrent(task_id)
                        if _db_alt:
                            _db_alt_name, _db_alt_url = _db_alt
                            await batch_rep.report(
                                f"🔄 {len(_hdri_failed_eps)} Hdri encode(s) failed — "
                                f"switching to DB alt: {_db_alt_name[:70]}",
                                "warning",
                            )
                            import shutil as _st_hdri_alt
                            try:
                                if _os.path.isdir(_batch_base):
                                    _st_hdri_alt.rmtree(_batch_base, ignore_errors=True)
                            except Exception:                                pass
                            from .workers import _batch_queue, _batch_counter as _bc
                            _db_alt_tid = await batch_task_queue.enqueue(
                                _db_alt_name, _db_alt_url, source_priority
                            )
                            await _batch_queue.put((
                                max(0, source_priority - 1), next(_bc),
                                _db_alt_name, _db_alt_url, True,
                                _db_alt_tid, source_priority, [], False,
                            ))
                            await batch_task_queue.update_task(
                                task_id, status="failed",
                                error=f"Hdri encode failed on {len(_hdri_failed_eps)} ep(s) — retried with DB alt",
                            )
                            await stat_msg.delete()
                            return

                    # No DB alt → per-episode Nyaa replacement
                    if _hdri_failed_eps:
                        try:
                            (
                                _get_search_names, _search_nyaa, _search_nyaa_html,
                                _search_nyaa_rss_season, _sanitize_nyaa_query,
                                _torrent_priority, _is_non_english,
                            ) = await _load_nyaa_helpers()
                            _ep_search_names = await _get_search_names(anime_title)

                            _BATCH_KW = {
                                "batch", "complete", "season pack", "complete series",
                                "complete season", "bd box", "bdremux", "bd remux",
                            }

                            def _is_batch_title(t: str) -> bool:
                                return any(k in t.lower() for k in _BATCH_KW)

                            for _fep in _hdri_failed_eps:
                                _fep_num   = _fep["ep_num"]
                                _fep_ai    = _fep["aniInfo"]
                                _fep_fname = await _fep_ai.get_upname('Hdri')
                                _ep_cands: list = []

                                # Strategy 1: RSS season scan
                                _ep_rss = await _search_nyaa_rss_season(
                                    _ep_search_names, season=_hdri_season, release_year=_hdri_year
                                )
                                for _et, _eu in _ep_rss:
                                    if _is_non_english(_et) or _is_batch_title(_et):
                                        continue
                                    _em = _re_ep.search(
                                        r'[Ss]\d+[Ee](\d+)|[-\s](\d{1,3})\s*[\(\[]', _et                                    )
                                    if _em and int(_em.group(1) or _em.group(2)) == _fep_num:
                                        _ep_cands.append((_torrent_priority(_et), _et, _eu))

                                # Strategy 2: Targeted HTML search
                                if not _ep_cands:
                                    for _sn in _ep_search_names[:2]:
                                        _hq = _sanitize_nyaa_query(
                                            f"{_sn} s{_hdri_season:02d}e{_fep_num:02d}"
                                        )
                                        for _et, _eu in await _search_nyaa_html(
                                            _hq, season=_hdri_season,
                                            release_year=_hdri_year, max_pages=5,
                                        ):
                                            if _is_non_english(_et) or _is_batch_title(_et):
                                                continue
                                            _tl = _et.lower()
                                            if any(p in _tl for p in [
                                                f"s{_hdri_season:02d}e{_fep_num:02d}",
                                                f"- {_fep_num:02d} ", f"e{_fep_num:02d}",
                                            ]):
                                                _ep_cands.append((_torrent_priority(_et), _et, _eu))
                                        if _ep_cands:
                                            break

                                # Strategy 3: Generic Nyaa search
                                if not _ep_cands:
                                    for _et, _eu in await _search_nyaa(
                                        _ep_search_names, season=_hdri_season, episode=_fep_num
                                    ):
                                        if _is_non_english(_et) or _is_batch_title(_et):
                                            continue
                                        _tl = _et.lower()
                                        _season_ok = any(p in _tl for p in [
                                            f"s{_hdri_season:02d}e{_fep_num:02d}",
                                            f"- {_fep_num:02d} ", f"e{_fep_num:02d}",
                                        ])
                                        _season_wrong = any(
                                            f"s{_s:02d}" in _tl
                                            for _s in range(1, 10) if _s != _hdri_season
                                        )
                                        if _season_ok and not _season_wrong:
                                            _ep_cands.append((_torrent_priority(_et), _et, _eu))

                                if not _ep_cands:
                                    await batch_rep.report(
                                        f"❌ No replacement found for E{_fep_num:02d} — skipping Hdri",
                                        "warning", log=False,
                                    )
                                    _still_failed.append(_fep)
                                    continue

                                _ep_cands.sort()
                                _, _rep_title, _rep_url = _ep_cands[0]
                                await batch_rep.report(
                                    f"⬇️ Downloading replacement E{_fep_num:02d}: {_rep_title[:60]}",
                                    "info", log=False,
                                )
                                try:
                                    async with batch_dl_lock:
                                        _rep_src = await TorDownloader(
                                            _batch_base, use_stable_dir=True
                                        ).download(_rep_url, _rep_title,
                                                   stat_msg=stat_msg, anime_name=anime_title)
                                except Exception as _rep_dl_err:
                                    await batch_rep.report(
                                        f"⚠️ Download failed for replacement E{_fep_num:02d}: {_rep_dl_err}",
                                        "warning", log=False,
                                    )
                                    _still_failed.append(_fep)
                                    continue

                                if not _rep_src or not _os.path.exists(_rep_src):
                                    _still_failed.append(_fep)
                                    continue

                                try:
                                    if _os.path.exists(_fep["src"]):
                                        _os.remove(_fep["src"])
                                except Exception:
                                    pass
                                _fep["src"] = _rep_src

                                _rep_fname = await _fep_ai.get_upname('Hdri')
                                _rep_expected = _os.path.join(_hdri_dir, _rep_fname)
                                if not (_os.path.exists(_rep_expected) and _os.path.getsize(_rep_expected) > 0):
                                    try:
                                        _rep_out = await hdri_passthrough(_rep_src, _hdri_dir, _rep_fname)
                                    except Exception:
                                        _rep_out = None

                                    if not _rep_out:
                                        _still_failed.append(_fep)

                        except Exception as _per_ep_err:
                            await batch_rep.report(
                                f"⚠️ Per-episode retry error: {_per_ep_err}", "warning"
                            )
                            _still_failed = _hdri_failed_eps
                    # Majority-failure → find alternate batch torrent
                    _fail_ratio = len(_still_failed) / max(len(ep_info), 1)
                    if len(ep_info) > 0 and _fail_ratio > 0.5:
                        await batch_rep.report(
                            f"⚠️ {len(_still_failed)}/{len(ep_info)} Hdri encode(s) failed "
                            f"({int(_fail_ratio * 100)}%) — searching Nyaa for alternate batch...",
                            "warning",
                        )
                        try:
                            (
                                _get_search_names, _search_nyaa, _search_nyaa_html,
                                _search_nyaa_rss_season, _sanitize_nyaa_query,
                                _torrent_priority, _is_non_english,
                            ) = await _load_nyaa_helpers()
                            import shutil as _shutil
                            _alt_search_names = await _get_search_names(anime_title)
                            _alt_cands: list  = []
                            for _at, _au in await _search_nyaa_rss_season(
                                _alt_search_names, season=_hdri_season, release_year=_hdri_year
                            ):
                                if not _is_non_english(_at):
                                    _alt_cands.append((_torrent_priority(_at), _at, _au))
                            for _sn in _alt_search_names[:2]:
                                _hq_batch = _sanitize_nyaa_query(f"{_sn} season {_hdri_season} batch")
                                for _at, _au in await _search_nyaa_html(
                                    _hq_batch, season=_hdri_season, max_pages=3,
                                    release_year=_hdri_year,
                                ):
                                    if not _is_non_english(_at):
                                        _alt_cands.append((_torrent_priority(_at), _at, _au))
                            _seen_urls: set = set()
                            _deduped: list  = []
                            for _item in sorted(_alt_cands):
                                if _item[2] not in _seen_urls and _item[2] != torrent:
                                    _seen_urls.add(_item[2])
                                    _deduped.append(_item)

                            if _deduped:
                                _alt_prio, _alt_title, _alt_url = _deduped[0]
                                await batch_rep.report(
                                    f"🔄 Found alternate: {_alt_title[:70]} — re-queuing...", "info"
                                )
                                try:
                                    _corrupt_root = _os.path.join(
                                        "./downloads/batch",
                                        _batch_base.rstrip("/").rsplit("/", 2)[-3]
                                        if _batch_base.count("/") >= 3
                                        else _os.path.basename(_batch_base.rstrip("/").rsplit("/", 1)[0]),
                                    )
                                    if _os.path.isdir(_corrupt_root):                                        _shutil.rmtree(_corrupt_root, ignore_errors=True)
                                except Exception:
                                    pass
                                from .workers import _batch_queue, _batch_counter as _bc2
                                _new_tid = await batch_task_queue.enqueue(
                                    _alt_title, _alt_url, source_priority
                                )
                                await _batch_queue.put((
                                    source_priority, next(_bc2),
                                    _alt_title, _alt_url, True,
                                    _new_tid, source_priority, [], False,
                                ))
                                await batch_task_queue.update_task(task_id, status="completed")
                                return
                        except Exception as _alt_err:
                            await batch_rep.report(
                                f"⚠️ Alternate batch search failed: {_alt_err}", "warning"
                            )

                    elif _still_failed:
                        _sf_nums = [e["ep_num"] for e in _still_failed]
                        await batch_rep.report(
                            f"❌ Hdri remux failed for {len(_still_failed)} ep(s): {_sf_nums} — "
                            f"stopping and switching to alt torrent",
                            "error",
                        )
                        import shutil as _shutil_hdri_fail
                        try:
                            if _os.path.isdir(_batch_base):
                                _shutil_hdri_fail.rmtree(_batch_base, ignore_errors=True)
                        except Exception:
                            pass
                        _hdri_fail_alt = await batch_task_queue.pop_alt_torrent(task_id)
                        if _hdri_fail_alt:
                            from .workers import _batch_queue, _batch_counter as _bc3
                            _hfa_name, _hfa_url = _hdri_fail_alt
                            _hfa_tid = await batch_task_queue.enqueue(
                                _hfa_name, _hfa_url, source_priority
                            )
                            await _batch_queue.put((
                                max(0, source_priority - 1), next(_bc3),
                                _hfa_name, _hfa_url, True,
                                _hfa_tid, source_priority, [], False,
                            ))
                        else:
                            await batch_rep.report(
                                "⚠️ No alt torrent available — Hdri block abandoned", "warning", log=False
                            )
                        await batch_task_queue.update_task(
                            task_id, status="failed",                            error=f"Hdri remux failed on {_sf_nums} — retried with alt",
                        )
                        await stat_msg.delete()
                        return

                    # ── All encodes done → stream-upload one at a time ─────────
                    # Scan the Hdri dir for all encoded files in episode order.
                    # Upload each immediately and delete from disk — no accumulation.
                    # Any zero-byte file = encode silently failed → treat as unrecoverable.
                    _hdri_encoded = []
                    _hdri_zero_byte: list[str] = []
                    for _fn in sorted(_os.listdir(_hdri_dir)):
                        if _os.path.splitext(_fn)[1].lower() not in {'.mkv', '.mp4'}:
                            continue
                        _fp = _os.path.join(_hdri_dir, _fn)
                        if _os.path.getsize(_fp) > 0:
                            _hdri_encoded.append(_fp)
                        else:
                            _hdri_zero_byte.append(_fn)
                            try:
                                _os.remove(_fp)
                            except Exception:
                                pass

                    # Merge zero-byte discoveries into _still_failed so the abort
                    # logic below has a single complete picture of what's missing.
                    if _hdri_zero_byte:
                        await batch_rep.report(
                            f"❌ [Hdri] {len(_hdri_zero_byte)} zero-byte encode(s) detected "
                            f"after all retries: {_hdri_zero_byte}",
                            "error",
                        )
                        # Any ep that produced a zero-byte file wasn't in _still_failed yet
                        # (it went through encode without raising an exception but wrote nothing).
                        # Mark the whole quality as unrecoverable so the abort fires below.
                        _still_failed = _still_failed or [{}]  # sentinel so len > 0

                    # Hard abort: if ANY eps are unrecoverable the batch would be
                    # incomplete — don't upload a partial Hdri set.
                    if _still_failed:
                        _sf_ep_nums = [e.get('ep_num', '?') for e in _still_failed if e]
                        await batch_rep.report(
                            f"❌ [Hdri] {len(_still_failed)} ep(s) unrecoverable after all retries "
                            f"(EP{_sf_ep_nums}) — aborting Hdri to avoid incomplete batch.",
                            "error",
                        )
                        # Clean up whatever was encoded so far
                        import shutil as _sh_abort
                        try:
                            if _os.path.isdir(_hdri_dir):                                _sh_abort.rmtree(_hdri_dir, ignore_errors=True)
                        except Exception:
                            pass
                        # Try one last DB alt torrent before giving up entirely
                        _last_alt = await batch_task_queue.pop_alt_torrent(task_id)
                        if _last_alt:
                            _la_name, _la_url = _last_alt
                            await batch_rep.report(
                                f"🔄 Last-resort alt torrent: {_la_name[:70]}", "info"
                            )
                            import shutil as _sh_la
                            try:
                                if _os.path.isdir(_batch_base):
                                    _sh_la.rmtree(_batch_base, ignore_errors=True)
                            except Exception:
                                pass
                            from .workers import _batch_queue, _batch_counter as _bc_la
                            _la_tid = await batch_task_queue.enqueue(_la_name, _la_url, source_priority)
                            await _batch_queue.put((
                                max(0, source_priority - 1), next(_bc_la),
                                _la_name, _la_url, True,
                                _la_tid, source_priority, [], False,
                            ))
                            await batch_task_queue.update_task(
                                task_id, status="failed",
                                error=f"Hdri incomplete ({len(_still_failed)} ep(s) failed) — retried with last alt",
                            )
                        else:
                            await batch_task_queue.update_task(
                                task_id, status="failed",
                                error=f"Hdri incomplete ({len(_still_failed)} ep(s) failed) — no alt available",
                            )
                        await stat_msg.delete()
                        return

                    if _hdri_encoded:
                        _ql_h = QUAL_LABELS.get('Hdri', 'Hdri')
                        _al_h = AUDIO_LABELS.get(audio, audio)
                        _hdr_h_cap = (
                            f"➤ <b>{anime_title}</b>\n"
                            f"➤ <b>Episodes - {total_eps}</b>\n"
                            f"➤ <b>[Audio - {_al_h}][Quality - {_ql_h}]</b>"
                        )
                        try:
                            _hdr_h_photo = poster_url or banner_url
                            if _hdr_h_photo:
                                _hdr_h_msg = await upload_bot.send_photo(
                                    _hdri_qs, photo=_hdr_h_photo, caption=_hdr_h_cap
                                )
                            else:                                _hdr_h_msg = await upload_bot.send_message(_hdri_qs, _hdr_h_cap)
                        except Exception:
                            _hdr_h_msg = await upload_bot.send_message(_hdri_qs, _hdr_h_cap)
                        _hdri_first_id = _hdr_h_msg.id
                        _hdri_ep_msg   = _hdr_h_msg  # fallback if no eps upload

                        # Build ep_num → aniInfo lookup for captions
                        _ep_ai_map = {e["ep_num"]: e["aniInfo"] for e in ep_info}

                        for _hidx, _hout in enumerate(_hdri_encoded):
                            # Resolve episode number from filename
                            _hfn  = _os.path.basename(_hout)
                            _hm   = (
                                _re_ep.search(r'S\d+E(\d+)', _hfn, _re_ep.IGNORECASE) or
                                _re_ep.search(r'\s-\s(\d{1,4})\s*[\(\[]', _hfn) or
                                _re_ep.search(r'[-_]\s*(\d{1,4})\s*(?:v\d)?\s*[-_\.\s\(\[]', _hfn) or
                                _re_ep.search(r'\[(\d{1,3})\]', _hfn)
                            )
                            _hep_num = int(_hm.group(1)) if _hm else (_hidx + 1)
                            if _hep_num > 500:
                                _hep_num = _hidx + 1

                            _hep_ai = _ep_ai_map.get(_hep_num)
                            if _hep_ai is None:
                                # fallback: use first available aniInfo
                                _hep_ai = next(iter(_ep_ai_map.values()))
                            _hep_ai.pdata["episode_number"] = _hep_num

                            _hdri_ep_caption = await _hep_ai.get_caption(is_main_channel=False, qual='Hdri')
                            await editMessage(
                                stat_msg,
                                f"<b>📦 {anime_title}</b>\n\n"
                                f"<blockquote>📤 [Hdri] E{_hep_num:02d} ({_hidx + 1}/{len(_hdri_encoded)})</blockquote>",
                            )
                            await batch_task_queue.update_task(task_id, status="uploading")
                            try:
                                _hdri_ep_msg = await (
                                    TgUploader(stat_msg, upload_bot=upload_bot, file_store=_hdri_qs)
                                    .set_display_name(anime_title)
                                    .upload(_out, 'Hdri', caption=_hdri_ep_caption)
                                )
                            except Exception as _hdri_ue:
                                retry = await batch_task_queue.increment_retry(task_id)
                                await batch_rep.report(
                                    f"Hdri upload error E{_hep_num:02d}: {_hdri_ue} (retry {retry}/{MAX_RETRIES})",
                                    "error",
                                )
                                await stat_msg.delete()
                                await batch_task_queue.update_task(
                                    task_id,                                    status="pending" if retry < MAX_RETRIES else "failed",
                                    error=str(_hdri_ue)[:300],
                                )
                                return

                            _hdri_ep_link = await _make_link(
                                _hdri_ep_msg.id, file_store=_hdri_qs, upload_bot=upload_bot
                            )
                            ep_links[_hep_num]['Hdri'] = _hdri_ep_link
                            if ani_id:
                                await db.saveAnime(ani_id, _hep_num, 'Hdri', _hdri_ep_msg.id, audio)
                                bot_loop.create_task(
                                    batch_db.save_batch_ep_link(
                                        ani_id, _hep_num, 'Hdri', _hdri_ep_link, season=_season_key
                                    )
                                )
                            await batch_task_queue.mark_qual_done(task_id, 'Hdri')
                            bot_loop.create_task(extra_utils(_hdri_ep_msg.id, _hout))

                            await areclaim_memory()
                            if _os.path.exists(_ep['src']):
                                drop_page_cache(_ep['src'])

                        try:
                            _stk_h = await upload_bot.send_sticker(_hdri_qs, sticker=BATCH_STICKER)
                            _hdri_last_id = _stk_h.id
                        except Exception:
                            _hdri_last_id = _hdri_ep_msg.id

                        try:
                            if ani_id:
                                _existing_hdri_bl = await batch_db.get_batch_link(
                                    ani_id, file_store=_hdri_qs, season=_season_key
                                )
                                _db_hf = (_existing_hdri_bl or {}).get("first_Hdri")
                                _db_hl = (_existing_hdri_bl or {}).get("last_Hdri")
                                _true_hf = _db_hf if _db_hf else _hdri_first_id
                                _true_hl = max(_db_hl, _hdri_last_id) if _db_hl else _hdri_last_id
                                await batch_db.save_batch_link(
                                    ani_id, _true_hf, _true_hl, _hdri_qs,
                                    season=_season_key,
                                    extra={"first_Hdri": _true_hf, "last_Hdri": _true_hl},
                                )                            
                            else:
                                _true_hf, _true_hl = _hdri_first_id, _hdri_last_id
                            qual_ranges['Hdri'] = (_true_hf, _true_hl)
                            _b64_hdri = await encode(f"get-{abs(_hdri_qs)}-{_true_hf}-{_true_hl}")
                            _hdri_link = f"https://telegram.me/{_bot_me_username}?start={_b64_hdri}"
                            await _rebuild_index_keyboard(extra_qual='Hdri', extra_link=_hdri_link)
                        except Exception as _hdri_le:
                            await batch_rep.report(
                                f"Hdri persist/button failed: {_hdri_le}", "warning", log=False
                            )

                    if _os.path.isdir(_hdri_dir):
                        try:
                            import shutil as _sh_h
                            _sh_h.rmtree(_hdri_dir, ignore_errors=True)
                        except Exception:
                            pass

                else:
                    # ── 1080 / 720 / 480: encode-then-upload per episode ──────
                    _qual_dir = _os.path.join(_batch_base, qual)
                    _os.makedirs(_qual_dir, exist_ok=True)

                    _ql_label = QUAL_LABELS.get(qual, qual)
                    _al_label = AUDIO_LABELS.get(audio, audio)
                    _hdr_cap  = (
                        f"➤ <b>{anime_title}</b>\n"
                        f"➤ <b>Episodes - {total_eps}</b>\n"
                        f"➤ <b>[Audio - {_al_label}][Quality - {_ql_label}]</b>"
                    )
                    _enc_qs = _qual_file_store(qual, file_store, pipeline="batch")
                    try:
                        _hdr_photo = poster_url or banner_url
                        if _hdr_photo:
                            _hdr_msg = await upload_bot.send_photo(
                                _enc_qs, photo=_hdr_photo, caption=_hdr_cap
                            )
                        else:
                            _hdr_msg = await upload_bot.send_message(_enc_qs, _hdr_cap)
                    except Exception:
                        _hdr_msg = await upload_bot.send_message(_enc_qs, _hdr_cap)
                    _first_id = _hdr_msg.id

                    for _ep_idx, _ep in enumerate(ep_info):
                        _ep_num = _ep["ep_num"]
                        _ep_ai  = _ep["aniInfo"]
                        _src    = _ep["src"]
                        _fname  = await _ep_ai.get_upname(qual)
                        _out_expected = _os.path.join(_qual_dir, _fname)
                        if ep_links[_ep_num].get(qual):
                            continue

                        if _os.path.exists(_out_expected) and _os.path.getsize(_out_expected) > 0:
                            _out = _out_expected
                        else:
                            if not _os.path.exists(_src):
                                await batch_rep.report(
                                    f"Source missing [{qual}] E{_ep_num:02d}: {_src} — skipping",
                                    "error",
                                )
                                continue

                            await editMessage(
                                stat_msg,
                                f"<b>📦 {anime_title}</b>\n\n"
                                f"<blockquote>⚙️ Encoding [{qual}] E{_ep_num:02d} ({_ep_idx + 1}/{total_eps})</blockquote>",
                            )
                            await batch_task_queue.update_task(task_id, status="encoding")

                            try:
                                async with batch_encode_lock:
                                    _out = await FFEncoder(
                                        stat_msg, _src, _fname, qual,
                                        output_dir=_qual_dir, display_name=anime_title,
                                    ).start_encode()
                            except Exception as _ee:
                                _exc_alt = await batch_task_queue.pop_alt_torrent(task_id)
                                if _exc_alt:
                                    _exc_name, _exc_url = _exc_alt
                                    await batch_rep.report(
                                        f"🔄 Encode exception E{_ep_num:02d} — switching to alt: {_exc_name[:70]}",
                                        "warning",
                                    )
                                    import shutil as _st_exc
                                    try:
                                        if _os.path.isdir(_batch_base):
                                            _st_exc.rmtree(_batch_base, ignore_errors=True)
                                    except Exception:
                                        pass
                                    from .workers import _batch_queue, _batch_counter as _bc4
                                    _exc_tid = await batch_task_queue.enqueue(
                                        _exc_name, _exc_url, source_priority
                                    )
                                    await _batch_queue.put((
                                        max(0, source_priority - 1), next(_bc4),
                                        _exc_name, _exc_url, True,
                                        _exc_tid, source_priority, [], False,
                                    ))                                    
                                    await batch_task_queue.update_task(
                                        task_id, status="failed",
                                        error=f"Encode exception [{qual}] E{_ep_num:02d} — retried with alt",
                                    )
                                    await stat_msg.delete()
                                    return
                                else:
                                    retry = await batch_task_queue.increment_retry(task_id)
                                    await batch_rep.report(
                                        f"Encode error [{qual}] E{_ep_num:02d}: {_ee} (retry {retry}/{MAX_RETRIES})",
                                        "error",
                                    )
                                    await stat_msg.delete()
                                    await batch_task_queue.update_task(
                                        task_id,
                                        status="pending" if retry < MAX_RETRIES else "failed",
                                        error=str(_ee)[:300],
                                    )
                                    return

                            if not _out:
                                _enc_alt = await batch_task_queue.pop_alt_torrent(task_id)
                                if _enc_alt:
                                    _enc_name, _enc_url = _enc_alt
                                    from .workers import _batch_queue, _batch_counter as _bc5
                                    import shutil as _st_enc
                                    try:
                                        if _os.path.isdir(_batch_base):
                                            _st_enc.rmtree(_batch_base, ignore_errors=True)
                                    except Exception:
                                        pass
                                    _enc_tid = await batch_task_queue.enqueue(
                                        _enc_name, _enc_url, source_priority
                                    )
                                    await _batch_queue.put((
                                        max(0, source_priority - 1), next(_bc5),
                                        _enc_name, _enc_url, True,
                                        _enc_tid, source_priority, [], False,
                                    ))
                                    await batch_task_queue.update_task(
                                        task_id, status="failed",
                                        error=f"Encode failed [{qual}] E{_ep_num:02d} — retried with alt",
                                    )
                                    await stat_msg.delete()
                                    return
                                else:
                                    retry = await batch_task_queue.increment_retry(task_id)
                                    await batch_rep.report(
                                        f"❌ Encode returned None [{qual}] E{_ep_num:02d} (retry {retry}/{MAX_RETRIES})",
                                        "error",                                    )
                                    await stat_msg.delete()
                                    await batch_task_queue.update_task(
                                        task_id, status="pending" if retry < MAX_RETRIES else "failed"
                                    )
                                    return

                        # Upload immediately after encode
                        _ep_file_caption = await _ep_ai.get_caption(is_main_channel=False, qual=qual)
                        await editMessage(
                            stat_msg,
                            f"<b>📦 {anime_title}</b>\n\n"
                            f"<blockquote>📤 [{qual}] E{_ep_num:02d} ({_ep_idx + 1}/{total_eps})</blockquote>",
                        )
                        await batch_task_queue.update_task(task_id, status="uploading")
                        _q_store = _qual_file_store(qual, file_store, pipeline="batch")
                        try:
                            _ep_msg = await (
                                TgUploader(stat_msg, upload_bot=upload_bot, file_store=_q_store)
                                .set_display_name(anime_title)
                                .upload(_out, qual, caption=_ep_file_caption)
                            )
                        except Exception as _ue:
                            retry = await batch_task_queue.increment_retry(task_id)
                            await batch_rep.report(
                                f"Upload error [{qual}] E{_ep_num:02d}: {_ue} (retry {retry}/{MAX_RETRIES})",
                                "error",
                            )
                            await stat_msg.delete()
                            await batch_task_queue.update_task(
                                task_id,
                                status="pending" if retry < MAX_RETRIES else "failed",
                                error=str(_ue)[:300],
                            )
                            return

                        _ep_link = await _make_link(_ep_msg.id, file_store=_q_store, upload_bot=upload_bot)
                        ep_links[_ep_num][qual] = _ep_link
                        if ani_id:
                            await db.saveAnime(ani_id, _ep_num, qual, _ep_msg.id, audio)
                            bot_loop.create_task(
                                batch_db.save_batch_ep_link(
                                    ani_id, _ep_num, qual, _ep_link, season=_season_key
                                )
                            )
                        await batch_task_queue.mark_qual_done(task_id, qual)
                        bot_loop.create_task(extra_utils(_ep_msg.id, _out))

                        await areclaim_memory()
                        if _os.path.exists(_src):
                            drop_page_cache(_src)

                        if qual == ALL_QUALS[-1]:
                            try:
                                if _os.path.exists(_src):
                                    _os.remove(_src)
                            except Exception:
                                pass

                    # Closing sticker
                    try:
                        _stk_msg = await upload_bot.send_sticker(_enc_qs, sticker=BATCH_STICKER)
                        _last_id = _stk_msg.id
                    except Exception:
                        _last_id = _first_id

                    await batch_rep.report(
                        f"✅ [{qual}] done — all ep(s) uploaded for {anime_title}", "info", log=False
                    )

                    # Persist first/last and update index keyboard
                    try:
                        if ani_id:
                            _existing_bl = await batch_db.get_batch_link(
                                ani_id, file_store=_enc_qs, season=_season_key
                            )
                            _db_f = (_existing_bl or {}).get(f"first_{qual}")
                            _db_l = (_existing_bl or {}).get(f"last_{qual}")
                            _tf   = _db_f if _db_f else _first_id
                            _tl   = max(_db_l, _last_id) if _db_l else _last_id
                            await batch_db.save_batch_link(
                                ani_id, _tf, _tl, _enc_qs,
                                season=_season_key,
                                extra={f"first_{qual}": _tf, f"last_{qual}": _tl},
                            )
                        else:
                            _tf, _tl = _first_id, _last_id
                        qual_ranges[qual] = (_tf, _tl)
                        _b64_live = await encode(f"get-{abs(_enc_qs)}-{_tf}-{_tl}")
                        _live_link = f"https://telegram.me/{_bot_me_username}?start={_b64_live}"
                        await _rebuild_index_keyboard(extra_qual=qual, extra_link=_live_link)
                    except Exception as _le:
                        await batch_rep.report(
                            f"Live button update/persist failed [{qual}]: {_le}", "warning", log=False
                        )

                    # Delete encoded files immediately after upload
                    _done_dir = _os.path.join(_batch_base, qual)
                    if _os.path.isdir(_done_dir):
                        try:
                            import shutil as _shutil
                            _shutil.rmtree(_done_dir, ignore_errors=True)
                        except Exception:
                            pass

            # Chunk complete notification
            if len(_chunks) > 1 and channel_details:
                _cs = _chunk_ep_info[0]["ep_num"]
                _ce = _chunk_ep_info[-1]["ep_num"]
                _chunk_cap = (
                    f"<b>{anime_title}</b>\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<b>➤ Episodes:</b> {_cs}–{_ce} ready\n"
                    f"<b>➤ Qualities:</b> {', '.join(QUAL_LABELS.get(q, q) for q in ALL_QUALS)}\n"
                    f"<b>➤ Audio:</b> {AUDIO_LABELS.get(audio, audio)}\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<blockquote>Part {_chunk_idx + 1} of {len(_chunks)} available.</blockquote>"
                )
                _chunk_invite = channel_details.get('invite_link')
                _chunk_kb = _qual_btns_to_keyboard(_batch_qual_links) or (
                    InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Now", url=_chunk_invite)]])
                    if _chunk_invite else None
                )
                try:
                    if poster_url:
                        await upload_bot.send_photo(
                            channel_details['channel_id'],
                            photo=poster_url, caption=_chunk_cap, reply_markup=_chunk_kb,
                        )
                    else:
                        await upload_bot.send_message(
                            channel_details['channel_id'],
                            text=_chunk_cap, reply_markup=_chunk_kb,
                        )
                except Exception as _ce2:
                    await batch_rep.report(f"Chunk post failed: {_ce2}", "warning", log=False)

        # Restore full ep_info for final steps
        ep_info = sorted([e for chunk in _chunks for e in chunk], key=lambda x: x["ep_num"])

        # ── 11. Persist overall first/last range ──────────────────────────────
        _all_first = min((v[0] for v in qual_ranges.values() if v[0]), default=None)
        _all_last  = max((v[1] for v in qual_ranges.values() if v[1]), default=None)
        if _all_first and _all_last and ani_id:
            try:
                await batch_db.save_batch_link(
                    ani_id, _all_first, _all_last, file_store, season=_season_key                )
            except Exception:
                pass

        # ── 12. Final index caption update ────────────────────────────────────
        _final_caption = (
            f"<b>{anime_title}</b>\n"
            f"<b>{'─' * 28}</b>\n"
            f"<b>➤ Season - {str(_season_raw).zfill(2)}</b>\n"
            f"<b>➤ Episodes - {total_eps}</b>\n"
            f"<b>➤ Quality: Multi [{AUDIO_LABELS.get(audio, audio)}]</b>\n"
            f"<b>{'─' * 28}</b>\n"
            f"<blockquote>Tap a quality to get all episodes.</blockquote>"
        )
        try:
            await editMessage(_index_post, _final_caption, _qual_btns_to_keyboard(_batch_qual_links))
        except Exception:
            pass

        # ── 13. Notify main channel ───────────────────────────────────────────
        if channel_details:
            _ani_status = aniInfo.adata.get("status", "")
            _is_releasing = _ani_status == "RELEASING"
            _notify_target = Var.MAIN_CHANNEL if _is_releasing else target_channel

            _invite = channel_details.get('invite_link') or ""
            if not _invite:
                try:
                    _ded_chat = await upload_bot.get_chat(channel_details['channel_id'])
                    _invite   = _ded_chat.invite_link or ""
                    if not _invite:
                        _invite = await upload_bot.export_chat_invite_link(channel_details['channel_id'])
                    if _invite:
                        await db.add_anime_channel(
                            channel_details.get('anime_name', anime_title),
                            channel_details['channel_id'],
                            channel_details.get('channel_title', ''),
                            invite_link=_invite,
                            db_type=channel_details.get('db_type', 'completed'),
                            ani_id=channel_details.get('ani_id'),
                        )
                except Exception as _ile:
                    await batch_rep.report(f"Live invite_link fetch failed: {_ile}", "warning", log=False)

            import html as _html_esc
            _synopsis = (aniInfo.adata.get("description") or "")
            _synopsis = _re_ep.sub(r'<[^>]+>', ' ', _synopsis)
            _synopsis = _re_ep.sub(r'\s+', ' ', _synopsis).strip()
            _synopsis = _html_esc.escape(_synopsis)
            if len(_synopsis) > 800:                _synopsis = _synopsis[:800] + "..."

            if _is_releasing and total_eps <= 3:
                _ep_num_n  = ep_info[0]["ep_num"] if ep_info else 1
                _notify_cap = (
                    f"<b>{anime_title}</b>\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<b>➤ Season - {str(_season_raw).zfill(2)}</b>\n"
                    f"<b>➤ Episode - {str(_ep_num_n).zfill(2)}</b>\n"
                    f"<b>➤ Quality: Multi [{AUDIO_LABELS.get(audio, audio)}]</b>\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<collapse title=\"Synopsis\">📌 {_synopsis}</collapse>"
                )
            else:
                _notify_cap = (
                    f"<b>{anime_title}</b>\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<b>➤ Season - {str(_season_raw).zfill(2)}</b>\n"
                    f"<b>➤ Episodes - {total_eps}</b>\n"
                    f"<b>➤ Quality: Multi [{AUDIO_LABELS.get(audio, audio)}]</b>\n"
                    f"<b>{'─' * 28}</b>\n"
                    f"<collapse title=\"Synopsis\">📌 {_synopsis}</collapse>"
                )

            _notify_kb = (
                InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Now", url=_invite)]])
                if _invite else None
            )
            _is_ongoing_ep  = _is_releasing and total_eps <= 3
            _notify_post_id = (_saved_bl or {}).get("notify_post_id")
            _notify_sent    = False

            if _notify_post_id and not _is_ongoing_ep:
                try:
                    _existing = await upload_bot.get_messages(_notify_target, _notify_post_id)
                    if _existing and not _existing.empty:
                        await editMessage(_existing, _notify_cap, _notify_kb)
                        _notify_sent = True
                except Exception:
                    pass

            if not _notify_sent:
                await _warm_peer(upload_bot, _notify_target)
                if poster_url:
                    _notify_msg = await _safe_send(
                        upload_bot, upload_bot.send_photo, _notify_target,
                        photo=poster_url,
                        caption=_notify_cap, reply_markup=_notify_kb,                        _label="batch notify send_photo",
                    )
                else:
                    _notify_msg = await _safe_send(
                        upload_bot, upload_bot.send_message, _notify_target,
                        text=_notify_cap, reply_markup=_notify_kb,
                        _label="batch notify send_message",
                    )
                if not _is_ongoing_ep:
                    _nm = {"notify_post_id": _notify_msg.id}
                    if ani_id:
                        await batch_db.save_batch_link(
                            ani_id, 0, 0, file_store, season=_season_key, extra=_nm
                        )
                    await batch_db.save_batch_link_by_name(name, _nm, season=_season_key)

            # ── 14. Season-end sticker + ending card ──────────────────────────
            _ch_id_end = channel_details['channel_id']

            # Delete previous ending post
            _prev_ending_id = await db.get_ending_post(_ch_id_end)
            if _prev_ending_id:
                try:
                    await upload_bot.delete_messages(_ch_id_end, _prev_ending_id)
                except Exception:
                    pass
                await db.delete_ending_post_record(_ch_id_end)

            # Delete previous ending sticker (stored under separate key)
            _sticker_key = f"sticker_{_ch_id_end}"
            try:
                _stk_doc = await db.db.ending_posts.find_one({"channel_id": _sticker_key})
                if _stk_doc:
                    try:
                        await upload_bot.delete_messages(_ch_id_end, _stk_doc["msg_id"])
                    except Exception:
                        pass
                    await db.db.ending_posts.delete_one({"channel_id": _sticker_key})
            except Exception:
                pass

            _STICKER_ONGOING  = "CAACAgUAAxUAAWm9LwbV3biNH2kenobxHituCVqCAAJdHAACeS2RVSI15ydYuGKoOgQ"
            _STICKER_FINISHED = "CAACAgUAAxUAAWm9LwaQVAmf7gfKXMb8MLJvMx_6AAJYHAAC1-CRVbNpn83SnQOqOgQ"
            _end_sticker = _STICKER_ONGOING if _ani_status == "RELEASING" else _STICKER_FINISHED
            try:
                _stk_msg = await upload_bot.send_sticker(_ch_id_end, sticker=_end_sticker)
                await db.db.ending_posts.update_one(
                    {"channel_id": _sticker_key},
                    {"$set": {"channel_id": _sticker_key, "msg_id": _stk_msg.id}},
                    upsert=True,                )
            except Exception:
                pass

            _ending_id = await _send_ending_post(upload_bot, _ch_id_end)
            if _ending_id:
                await db.save_ending_post(_ch_id_end, _ending_id)

        await batch_rep.report(
            f"✅ Batch complete: {anime_title} ({total_eps} eps, {len(ALL_QUALS)} qualities)", "info"
        )
        await batch_task_queue.mark_done(task_id)
        await stat_msg.delete()

        # ── 15. Specials / OVAs ───────────────────────────────────────────────
        if _spec_files:
            await batch_rep.report(
                f"🎬 Processing {len(_spec_files)} special(s) for: {anime_title}",
                "info", log=False,
            )
            _sp_stat = await sendMessage(stat_channel, f"<b>🎬 Processing specials: {anime_title}</b>")
            _dash    = "─" * 28

            for _sp_idx, _sp_file in enumerate(_spec_files, 1):
                _sp_fnl = _os.path.basename(_sp_file).lower()
                _sp_type = (
                    "OVA"     if ("ova" in _sp_fnl or "oad" in _sp_fnl)
                    else "Special" if ("special" in _sp_fnl or "specials" in _sp_fnl)
                    else "Extra"
                )
                _sp_in_prog = (
                    f"<b>📦 {anime_title}</b>\n"
                    f"<b>{_dash}</b>\n"
                    f"<b>➤ Season {_season_raw}</b>\n"
                    f"<b>➤ {_sp_type} {_sp_idx:02d}</b>\n"
                    f"<b>➤ Audio:</b> {AUDIO_LABELS.get(audio, audio)}\n"
                    f"<b>{_dash}</b>\n"
                    f"<blockquote>⏳ Uploading qualities...</blockquote>"
                )
                if poster_url:
                    _sp_post = await upload_bot.send_photo(
                        _post_channel, photo=poster_url, caption=_sp_in_prog
                    )
                else:
                    _sp_post = await upload_bot.send_message(_post_channel, text=_sp_in_prog)

                _sp_qual_links: dict = {}

                _sp_ep_ai       = TextEditor(name)
                _sp_ep_ai.adata = aniInfo.adata
                _sp_ep_ai.pdata = dict(aniInfo.pdata)
                _sp_ep_ai.pdata["episode_number"] = _sp_idx

                for _sp_qual in ALL_QUALS:
                    _sp_qual_dir = _os.path.join(_batch_base, f"sp_{_sp_qual}")
                    _os.makedirs(_sp_qual_dir, exist_ok=True)
                    _sp_fname    = await _sp_ep_ai.get_upname(_sp_qual)
                    _sp_expected = _os.path.join(_sp_qual_dir, _sp_fname)

                    if _os.path.exists(_sp_expected) and _os.path.getsize(_sp_expected) > 0:
                        _sp_out = _sp_expected
                    else:
                        await editMessage(
                            _sp_stat,
                            f"<b>🎬 {anime_title}</b>\n\n"
                            f"<blockquote>⚙️ Encoding [{_sp_qual}] {_sp_type} {_sp_idx:02d}</blockquote>",
                        )
                        try:
                            async with batch_encode_lock:
                                if _sp_qual == 'Hdri':
                                    _sp_out = await hdri_passthrough(_sp_file, _sp_qual_dir, _sp_fname)
                                else:
                                    _sp_out = await FFEncoder(
                                        _sp_stat, _sp_file, _sp_fname, _sp_qual,
                                        output_dir=_sp_qual_dir, display_name=anime_title,
                                    ).start_encode()
                        except Exception as _sp_ee:
                            await batch_rep.report(
                                f"Special encode error [{_sp_qual}] {_sp_type} {_sp_idx:02d}: {_sp_ee}",
                                "error",
                            )
                            continue
                        if not _sp_out:
                            await batch_rep.report(
                                f"Special encode returned None [{_sp_qual}] {_sp_type} {_sp_idx:02d}",
                                "error",
                            )
                            continue

                    await editMessage(
                        _sp_stat,
                        f"<b>🎬 {anime_title}</b>\n\n"
                        f"<blockquote>📤 Uploading [{_sp_qual}] {_sp_type} {_sp_idx:02d}</blockquote>",
                    )
                    _sp_file_caption = await _sp_ep_ai.get_caption(is_main_channel=False, qual=_sp_qual)
                    _sp_q_store = _qual_file_store(_sp_qual, file_store, pipeline="batch")
                    try:
                        _sp_msg = await (
                            TgUploader(_sp_stat, upload_bot=upload_bot, file_store=_sp_q_store)
                            .set_display_name(anime_title)
                            .upload(_sp_out, _sp_qual, caption=_sp_file_caption)
                        )
                    except Exception as _sp_ue:
                        await batch_rep.report(
                            f"Special upload error [{_sp_qual}] {_sp_type} {_sp_idx:02d}: {_sp_ue}",
                            "error",
                        )
                        continue

                    _sp_link = await _make_link(_sp_msg.id, file_store=_sp_q_store, upload_bot=upload_bot)
                    _sp_qual_links[_sp_qual] = _sp_link

                    _sp_kb = _qual_btns_to_keyboard(_sp_qual_links)
                    try:
                        await editMessage(
                            _sp_post,
                            _sp_post.caption.html if _sp_post.caption else _sp_in_prog,
                            _sp_kb,
                        )
                    except Exception:
                        pass

                _sp_done_cap = (
                    f"<b>📦 {anime_title}</b>\n"
                    f"<b>{_dash}</b>\n"
                    f"<b>➤ Season {_season_raw}</b>\n"
                    f"<b>➤ {_sp_type} {_sp_idx:02d}</b>\n"
                    f"<b>➤ Audio:</b> {AUDIO_LABELS.get(audio, audio)}\n"
                    f"<b>{_dash}</b>\n"
                    f"<blockquote>Tap a quality to watch.</blockquote>"
                )
                try:
                    await editMessage(
                        _sp_post, _sp_done_cap, _qual_btns_to_keyboard(_sp_qual_links)
                    )
                except Exception:
                    pass
                await batch_rep.report(
                    f"✅ {_sp_type} {_sp_idx:02d} done for: {anime_title}", "info", log=False
                )

            await _sp_stat.delete()

        # ── 16. Cleanup ───────────────────────────────────────────────────────
        try:
            from aioshutil import rmtree as _aiorm
            if _batch_base and _os.path.isdir(_batch_base):
                await _aiorm(_batch_base)
        except Exception:
            pass

    except Exception:
        await batch_rep.report(format_exc(), "error")
        if task_id:
            retry = await batch_task_queue.increment_retry(task_id)
            await batch_task_queue.update_task(
                task_id, status="pending" if retry < MAX_RETRIES else "failed"
            )
