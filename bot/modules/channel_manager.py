"""
channel_manager.py — Interactive anime channel browser

/listconnections  → shows all anime as buttons (paginated)
Click anime       → shows info + action buttons
Action buttons:
  - Upload All Seasons    → queues all episodes across all seasons
  - Upload Season X       → queues all episodes of a specific season
  - Upload All Episodes   → same as all seasons (alias)
  - Upload Episode        → asks for specific episode number
  - Remove Connection     → removes channel link
  - Back                  → returns to anime list
"""

import re
from traceback import format_exc

from pyrogram import filters
from pyrogram.filters import command, private
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, admin
from bot.core.database import db, batch_db
from bot.core.func_utils import new_task, sendMessage, editMessage, getfeed
from bot.core.auto_animes import get_animes, run_batch_on_folder
from bot.core.reporter import rep
from bot.core.text_utils import TextEditor, FRANCHISE_MOVIES_QUERY

# ── Per-anime pipeline lock ───────────────────────────────────────────────────
# Ensures only one anime's full multi-season pipeline runs at a time.
# Key: anime_name (str) → asyncio.Lock
# A second /queueall for a different anime will wait until the first is fully done.
import asyncio as _asyncio
_anime_pipeline_locks: dict = {}
_PIPELINE_LOCK_MAX = 50   # cap to prevent unbounded growth

def _get_anime_lock(anime_name: str):
    if anime_name not in _anime_pipeline_locks:
        # Evict oldest lock if at capacity (oldest = first inserted)
        if len(_anime_pipeline_locks) >= _PIPELINE_LOCK_MAX:
            _oldest = next(iter(_anime_pipeline_locks))
            if not _anime_pipeline_locks[_oldest].locked():
                del _anime_pipeline_locks[_oldest]
        _anime_pipeline_locks[anime_name] = _asyncio.Lock()
    return _anime_pipeline_locks[anime_name]

_UPLOAD_SEARCH_SEM = _asyncio.Semaphore(1)

# ── Jikan caches ──────────────────────────────────────────────────────────────
# Bounded LRU (cap 2000 entries each). Even though individual values are tiny,
# unbounded plain dicts grew slowly across long uptimes (one entry per anime
# ever seen). LRU eviction keeps memory flat while preserving hot keys.
from collections import OrderedDict as _CM_OD


class _CM_LruCache(_CM_OD):
    """OrderedDict-based LRU with write-time eviction. Cap 2000 entries."""
    _MAX = 2000

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        while len(self) > self._MAX:
            self.popitem(last=False)

    def __getitem__(self, key):
        v = super().__getitem__(key)
        self.move_to_end(key)
        return v


_season_count_cache: _CM_LruCache = _CM_LruCache()   # mal_id -> int (number of seasons)
_aired_eps_cache:    _CM_LruCache = _CM_LruCache()   # mal_id -> int (episodes confirmed aired)
_jikan_status_cache: _CM_LruCache = _CM_LruCache()   # mal_id -> str (jikan status string, lowercased)


async def _run_seasons_sequentially(
    seasons_ready: list,
    anime_name: str,
    pending_season_downloaders=None,
):
    """
    Process multiple seasons one at a time: download next season concurrently
    while current season is encoding/uploading, then chain.

    pending_season_downloaders: list of async callables () -> (sname, sfolder)|None
    """
    from asyncio import sleep as _asl, create_task as _ct
    from bot.core.task_queue import batch_task_queue as _btq

    pending = list(pending_season_downloaders or [])

    async def _wait_done(sname, season_num, next_label):
        # ── Phase 1: wait for the task to APPEAR in MongoDB ──────────────────
        # When a season is queued via get_animes() / bot_loop.create_task(), the
        # task doesn't exist in MongoDB yet — it sits in the asyncio _batch_queue
        # until _batch_worker picks it up and calls batch_task_queue.enqueue().
        # If we skip this phase, _wait_done finds nothing, waits 60 s, declares
        # "done", and fires S(n+1)'s lazy download while S(n) hasn't even started.
        _appear_waited = 0
        _APPEAR_TIMEOUT = 600   # wait up to 10 min for task to appear
        _APPEAR_POLL    = 15    # check every 15 s
        _task_appeared  = False
        while _appear_waited < _APPEAR_TIMEOUT:
            await _asl(_APPEAR_POLL)
            _appear_waited += _APPEAR_POLL
            try:
                _col = await _btq._col()
                # Look for the task in ANY non-done/failed state (pending, downloading, etc.)
                # Anchor the regex so "Anime S01" doesn't match "Anime S02".
                # Escape special regex chars in sname, then match start-of-string
                # or require the name to match exactly (not as a substring of another season).
                import re as _re_sn
                _sname_esc = _re_sn.escape(sname.strip())
                _probe = await _col.find_one(
                    {"name": {"$regex": f"^{_sname_esc}$", "$options": "i"}},
                    sort=[("_id", -1)]
                )
                if _probe is not None:
                    _task_appeared = True
                    await rep.report(
                        f"⏳ S{season_num:02d} task appeared in queue (status={_probe.get('status')}) — waiting for completion",
                        "info", log=False
                    )
                    break
            except Exception as _pe:
                await rep.report(f"⚠️ Season appear-poll error: {_pe}", "warning", log=False)

        if not _task_appeared:
            await rep.report(
                f"⚠️ S{season_num:02d} task never appeared in MongoDB after {_APPEAR_TIMEOUT}s — proceeding anyway",
                "warning", log=False
            )

        # ── Phase 2: wait for the task to FINISH (status = done / failed) ────
        _waited = 0
        _FINISH_POLL    = 30
        _FINISH_CONFIRM = 60   # task must be gone/done for 60 s before we proceed
        _gone_since     = 0
        while True:
            await _asl(_FINISH_POLL)
            _waited += _FINISH_POLL
            try:
                _col = await _btq._col()
                _task = await _col.find_one(
                    {"name": {"$regex": f"^{_re_sn.escape(sname.strip())}$", "$options": "i"},
                     "status": {"$nin": ["done", "failed"]}},
                    sort=[("_id", -1)]
                )
                if _task is None:
                    _gone_since += _FINISH_POLL
                    if _gone_since >= _FINISH_CONFIRM:
                        await rep.report(
                            f"✅ S{season_num:02d} done — starting {next_label}",
                            "info", log=False
                        )
                        return
                else:
                    # Task still active — reset the confirmation counter
                    _gone_since = 0
            except Exception as _pe:
                await rep.report(f"⚠️ Season poll error: {_pe}", "warning", log=False)
                if _waited >= 300:
                    return

    all_seasons = list(seasons_ready)
    idx = 0
    # Background download task for the next lazy season.
    # Started after the current season finishes so the download overlaps with
    # the NEXT season's encode/upload rather than running ahead of everything.
    # Correct flow (e.g. 3 seasons):
    #   S1 finishes → start S3 download in background → start S2 pipeline
    #   S2 finishes → await S3 background task (should already be done) → start S3 pipeline
    _bg_dl_task = None   # asyncio.Task | None
    _bg_dl_label = None  # "S03" etc., for logging
    while idx < len(all_seasons):
        sname, sfolder = all_seasons[idx]
        season_num = idx + 1
        await rep.report(
            f"🔗 Starting S{season_num:02d} pipeline — {sname}", "info", log=False
        )
        await run_batch_on_folder(sname, sfolder)

        more_known   = idx < len(all_seasons) - 1
        more_pending = bool(pending)

        if more_known or more_pending:
            next_s_num = season_num + 1

            # Wait for current season to fully finish BEFORE doing anything with
            # the next season.  run_batch_on_folder() only enqueues the work; the
            # actual encode+upload happens asynchronously in _batch_worker, so we
            # must poll MongoDB until the task reaches done/failed.
            await _wait_done(sname, season_num, f"S{next_s_num:02d}")

            # ── Case 1: more known seasons still in all_seasons ──────────────
            # Start the next lazy download in the background NOW so it runs
            # concurrently with the next known season's encode/upload.
            # We do NOT await it here — that happens in Case 2 below.
            if more_pending and more_known:
                _next_dl_s_num = len(all_seasons) + 1  # e.g. S3 when all_seasons=[S1,S2]
                _bg_dl_label = f"S{_next_dl_s_num:02d}"
                _next_downloader = pending.pop(0)
                await rep.report(
                    f"⬇️ S{season_num:02d} complete — starting {_bg_dl_label} download in background...",
                    "info", log=False
                )
                _bg_dl_task = _ct(_next_downloader())

            # ── Case 2: no more known seasons — collect the background result ─
            # If a background task was started earlier, await it now.
            # If no background task exists but there are still pending downloaders,
            # run the next one directly (covers edge-cases like _UPFRONT_SEASONS=1).
            elif more_pending and not more_known:
                if _bg_dl_task is not None:
                    await rep.report(
                        f"⏳ S{season_num:02d} complete — waiting for {_bg_dl_label} download to finish...",
                        "info", log=False
                    )
                    _result = await _bg_dl_task
                    _bg_dl_task = None
                else:
                    _next_downloader = pending.pop(0)
                    await rep.report(
                        f"⬇️ S{season_num:02d} complete — now downloading S{next_s_num:02d}...",
                        "info", log=False
                    )
                    _result = await _next_downloader()

                if _result:
                    all_seasons.append(_result)
                else:
                    await rep.report(
                        f"⚠️ S{next_s_num:02d} download yielded nothing — stopping chain",
                        "warning", log=False
                    )
                    break
        idx += 1

    # Cancel any stray background download task (shouldn't happen, but be safe)
    if _bg_dl_task is not None and not _bg_dl_task.done():
        _bg_dl_task.cancel()

    await rep.report(
        f"✅ All {len(all_seasons)} season(s) completed for {anime_name}",
        "info", log=False
    )

# ── Jikan sequel-chain cache (bounded LRU, cap 2000) ─────────────────────────
_sequel_chain_cache: _CM_LruCache = _CM_LruCache()   # root_mal_id -> [mal_id_s1, mal_id_s2, ...]

# ── Jikan season start-year cache (bounded LRU, cap 2000) ────────────────────
_season_start_year_cache: _CM_LruCache = _CM_LruCache()  # mal_id -> int (year season started airing)

# ── Non-English language filter — applied to ALL search results ───────────────
# Any torrent title containing these strings is rejected globally.
# Keeps only English sub, Dual Audio, and Multi-Audio releases.
_NON_ENG_KEYWORDS = {
    "sub. español", "sub español", "[español]", "(español)", "español",
    "spanish sub", "spanish dub",
    "french sub", "french dub", "[french]", "(french)", "vostfr",
    "german sub", "german dub", "[german]", "(german)",
    "italian sub", "italian dub", "[italian]", "(italian)",
    "portuguese sub", "[portuguese]", "(portuguese)",
    "arabic sub", "[arabic]", "(arabic)",
    "turkish sub", "[turkish]", "(turkish)",
    "russian sub", "[russian]", "(russian)",
    "polish sub", "[polish]", "(polish)",
    "indonesian sub", "[indonesian]", "(indonesian)",
    "malay sub", "[malay]",
    "thai sub", "[thai]",
    "vietnamese sub", "[vietnamese]",
    "hindi sub", "[hindi]", "(hindi)",
    "chinese sub", "[chinese]", "(chinese)",
    "korean sub", "[korean]", "(korean)",
}

def _is_non_english(title: str) -> bool:
    tl = title.lower()
    if any(k in tl for k in _NON_ENG_KEYWORDS):
        return True
    # Reject torrents where the group tag contains non-ASCII characters
    # e.g. [尋], [愛], [中文] — these are CJK/non-English fansub groups
    import re as _re_ne
    group = _re_ne.match(r'^\[([^\]]+)\]', title)
    if group and any(ord(c) > 127 for c in group.group(1)):
        return True
    return False

async def _is_tv_season_entry(mal_id: int, sess) -> bool:
    """
    Return True if this MAL entry is a main TV season (type=TV or TV_Short on Jikan).
    OVAs, ONAs, Specials, Movies, and spin-offs that are not TV format are excluded.
    Cached per mal_id to avoid redundant requests.
    """
    if mal_id in _jikan_status_cache and _jikan_status_cache.get(f"_type_{mal_id}"):
        return _jikan_status_cache[f"_type_{mal_id}"] in ("TV", "TV Short")
    try:
        from asyncio import sleep as _slt
        url = f"https://api.jikan.moe/v4/anime/{mal_id}"
        for _att in range(3):
            async with sess.get(url) as r:
                if r.status == 429:
                    await _slt(2)
                    continue
                if r.status != 200:
                    return True  # assume TV on error so we don't drop entries silently
                body = await r.json()
                break
        else:
            return True
        entry_type = ((body.get("data") or {}).get("type") or "TV")
        _jikan_status_cache[f"_type_{mal_id}"] = entry_type
        await _slt(0.4)
        return entry_type in ("TV", "TV Short")
    except Exception:
        return True  # safe fallback: include on error


async def _get_season_count_from_jikan(mal_id: int) -> int:
    """
    Walk the MAL SEQUEL chain via Jikan v4 to count how many TV seasons exist.

    Works correctly for split series (AoT → 4, MHA → 6, SAO → 4).
    Skips OVA/ONA/Special/spin-off entries (e.g. Slime Diaries for Tensura)
    by verifying each sequel's type is TV before counting it.
    Returns 1 for continuous series (One Piece, Naruto) — they are a single
    MAL entry with no sequels, so we fall back to text input for those.

    Result is cached per mal_id so repeated calls cost nothing.
    """
    if mal_id in _season_count_cache:
        return _season_count_cache[mal_id]

    from aiohttp import ClientSession, ClientTimeout

    visited: set = set()
    queue: list  = [mal_id]
    count        = 0
    MAX_DEPTH    = 15          # safety cap — no anime has more than 15 distinct MAL entries
    TIMEOUT      = ClientTimeout(total=8)

    async with ClientSession(timeout=TIMEOUT) as sess:
        while queue and count < MAX_DEPTH:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            # Only count this entry if it is a TV season (skip OVA/specials/spin-offs)
            if current_id != mal_id:  # always count the root
                if not await _is_tv_season_entry(current_id, sess):
                    continue
            count += 1

            try:
                url = f"https://api.jikan.moe/v4/anime/{current_id}/relations"
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        break
                    body = await resp.json()
            except Exception:
                break

            for rel in (body.get("data") or []):
                if rel.get("relation") != "Sequel":
                    continue
                for entry in rel.get("entry", []):
                    if entry.get("type") == "anime":
                        seq_id = entry.get("mal_id")
                        if seq_id and seq_id not in visited:
                            queue.append(seq_id)

            # Jikan rate-limit: 3 req/sec burst, 60/min sustained
            from asyncio import sleep as _sl
            await _sl(0.4)

    result = max(count, 1)
    _season_count_cache[mal_id] = result
    await rep.report(f"Jikan season count: mal_id={mal_id} → {result} season(s)", "info", log=False)
    return result


async def _get_sequel_chain(root_mal_id: int) -> list[int]:
    """
    Walk the MAL SEQUEL chain and return an ORDERED list of MAL IDs for
    TV seasons only: [s1_mal_id, s2_mal_id, s3_mal_id, ...]

    Non-TV entries encountered in the chain (OVA, ONA, Special, spin-offs
    like Slime Diaries for Tensura) are skipped — they are real sequel
    entries on MAL but should not occupy a numbered season slot. The BFS
    still follows their relations so we don't lose seasons that come after
    a non-TV node in the chain.

    Cached per root_mal_id.
    """
    if root_mal_id in _sequel_chain_cache:
        return _sequel_chain_cache[root_mal_id]

    from aiohttp import ClientSession, ClientTimeout
    TIMEOUT   = ClientTimeout(total=8)
    visited:  list = []   # TV-only ordered list — season slots
    seen:     set  = set()
    queue:    list = [root_mal_id]
    MAX_DEPTH = 15

    async with ClientSession(timeout=TIMEOUT) as sess:
        while queue and len(visited) < MAX_DEPTH:
            current_id = queue.pop(0)
            if current_id in seen:
                continue
            seen.add(current_id)

            # Always include the root entry; for every other node verify it
            # is a TV season before adding it to the numbered slot list.
            # We still fetch its relations regardless so we can follow the
            # chain past OVA/spinoff nodes.
            is_tv = (current_id == root_mal_id) or await _is_tv_season_entry(current_id, sess)
            if is_tv:
                visited.append(current_id)

            body = None
            for _attempt in range(3):
                try:
                    url = f"https://api.jikan.moe/v4/anime/{current_id}/relations"
                    async with sess.get(url) as resp:
                        if resp.status == 429:
                            from asyncio import sleep as _sl2
                            await _sl2(2)
                            continue
                        if resp.status != 200:
                            # Non-retryable error for this node — skip but keep walking
                            break
                        body = await resp.json()
                        break
                except Exception:
                    from asyncio import sleep as _sl3
                    await _sl3(1)

            if not body:
                # Skip this node but don't abort the whole chain
                from asyncio import sleep as _sl4
                await _sl4(0.4)
                continue

            for rel in (body.get("data") or []):
                if rel.get("relation") != "Sequel":
                    continue
                for entry in rel.get("entry", []):
                    if entry.get("type") == "anime":
                        seq_id = entry.get("mal_id")
                        if seq_id and seq_id not in seen:
                            queue.append(seq_id)

            from asyncio import sleep as _sl
            await _sl(0.4)

    result = visited if visited else [root_mal_id]
    _sequel_chain_cache[root_mal_id] = result
    # Also update season count cache as a side effect
    _season_count_cache[root_mal_id] = len(result)
    await rep.report(
        f"Jikan sequel chain: root={root_mal_id} → {len(result)} season(s): {result}",
        "info", log=False
    )
    return result


async def _get_spinoffs_for_season(mal_id: int) -> list[dict]:
    """
    Return a list of non-main-TV related entries for a single MAL season entry.

    Walks Jikan v4 /anime/{id}/relations and collects every SEQUEL or
    SIDE_STORY entry whose type is NOT a main TV season (those are already
    covered by _get_sequel_chain).

    Includes: OVA, ONA, Special, Movie, and spin-off TV series
    (e.g. Tensura Nikki for Slime — its MAL relation is SIDE_STORY).

    Each returned dict:
        {
            "mal_id":   int,
            "title":    str,
            "type":     str,   # "OVA", "ONA", "Special", "TV" (spin-off), etc.
            "relation": str,   # "SEQUEL" | "SIDE_STORY"
        }

    Results cached in _jikan_status_cache to avoid redundant requests.
    """
    _cache_key = f"_spinoffs_{mal_id}"
    if _cache_key in _jikan_status_cache:
        return _jikan_status_cache[_cache_key]

    from aiohttp import ClientSession, ClientTimeout
    from asyncio import sleep as _sl
    TIMEOUT = ClientTimeout(total=8)
    results: list[dict] = []

    try:
        async with ClientSession(timeout=TIMEOUT) as sess:
            url = f"https://api.jikan.moe/v4/anime/{mal_id}/relations"
            body = None
            for _att in range(3):
                async with sess.get(url) as r:
                    if r.status == 429:
                        await _sl(2)
                        continue
                    if r.status != 200:
                        break
                    body = await r.json()
                    break

            if not body:
                _jikan_status_cache[_cache_key] = []
                return []

            WANTED_RELATIONS = {"Sequel", "Side Story"}

            for rel in (body.get("data") or []):
                relation_type = rel.get("relation", "")
                if relation_type not in WANTED_RELATIONS:
                    continue

                for entry in rel.get("entry", []):
                    if entry.get("type") != "anime":
                        continue
                    entry_mal_id = entry.get("mal_id")
                    if not entry_mal_id:
                        continue

                    # Fetch the type of this entry
                    entry_type  = "Unknown"
                    entry_title = entry.get("name", "")
                    await _sl(0.4)
                    try:
                        async with sess.get(
                            f"https://api.jikan.moe/v4/anime/{entry_mal_id}"
                        ) as dr:
                            if dr.status == 200:
                                d = await dr.json()
                                data = d.get("data") or {}
                                entry_type  = data.get("type") or "Unknown"
                                entry_title = data.get("title") or entry_title
                                # Cache type for _is_tv_season_entry reuse
                                _jikan_status_cache[f"_type_{entry_mal_id}"] = entry_type
                    except Exception:
                        pass

                    # Skip entries that are numbered main TV seasons —
                    # those are already handled by _get_sequel_chain.
                    # Only a Sequel-typed TV entry would be a main season;
                    # SIDE_STORY TV entries are spin-off series (keep them).
                    if relation_type == "Sequel" and entry_type in ("TV", "TV Short"):
                        continue

                    results.append({
                        "mal_id":   entry_mal_id,
                        "title":    entry_title,
                        "type":     entry_type,
                        "relation": relation_type.upper().replace(" ", "_"),
                    })

    except Exception as _e:
        await rep.report(
            f"_get_spinoffs_for_season({mal_id}) error: {_e}", "warning", log=False
        )

    _jikan_status_cache[_cache_key] = results
    if results:
        await rep.report(
            f"🎞 Spin-offs/OVAs for mal_id={mal_id}: "
            + ", ".join(f"{r['title']} ({r['type']})" for r in results),
            "info", log=False,
        )
    return results


async def _queue_spinoffs_after_all_seasons(
    anime_name: str,
    mal_id_root: int,
    search_names: list[str],
) -> None:
    """
    Called once after ALL main seasons have finished uploading.

    Iterates the full MAL sequel chain. For each season, fetches its related
    OVA / Special / ONA / spin-off entries via _get_spinoffs_for_season() and
    queues the best matching Nyaa torrent for each one not yet processed.

    Ordering mirrors AniList's Relations panel:
        S1 spin-offs → S2 spin-offs → S3 spin-offs → …

    Each spin-off is queued with is_batch=True (treated as a mini-batch since
    they're usually 1–13 eps, not ongoing singles) and force=True so re-runs
    of Upload All don't skip them via dedup.
    """
    from asyncio import sleep as _sl

    await rep.report(
        f"🎞 Checking for OVAs/spin-offs after all seasons of {anime_name}...",
        "info", log=False,
    )

    try:
        chain = await _get_sequel_chain(mal_id_root)
    except Exception as _ce:
        await rep.report(
            f"_queue_spinoffs: sequel chain failed: {_ce}", "warning", log=False
        )
        return

    seen_spinoff_ids: set = set()
    queued_count = 0

    for season_idx, season_mal_id in enumerate(chain):
        season_num = season_idx + 1
        try:
            spinoffs = await _get_spinoffs_for_season(season_mal_id)
        except Exception:
            spinoffs = []

        for so in spinoffs:
            so_mal_id = so["mal_id"]
            if so_mal_id in seen_spinoff_ids:
                continue
            seen_spinoff_ids.add(so_mal_id)

            so_title = so["title"]
            so_type  = so["type"]
            so_rel   = so["relation"]

            await rep.report(
                f"🎞 Found {so_type} after S{season_num:02d}: {so_title} "
                f"(mal_id={so_mal_id}, relation={so_rel})",
                "info", log=False,
            )

            # ── Build search name list for this spin-off ───────────────────
            # Fetch AniList title variants (English + Romaji) for precise search
            so_search_names: list[str] = []
            try:
                import aiohttp as _aio
                _gql = """
                    query($id:Int){
                        Media(idMal:$id,type:ANIME){
                            title{ romaji english }
                        }
                    }
                """
                async with _aio.ClientSession() as _sess:
                    async with _sess.post(
                        "https://graphql.anilist.co",
                        json={"query": _gql, "variables": {"id": so_mal_id}},
                        timeout=_aio.ClientTimeout(total=8),
                    ) as _r:
                        if _r.status == 200:
                            _d = await _r.json()
                            _t = (
                                ((_d.get("data") or {}).get("Media") or {})
                                .get("title") or {}
                            )
                            for _variant in [_t.get("english"), _t.get("romaji")]:
                                if _variant and _variant.strip():
                                    _v = _variant.strip().lower()
                                    if _v not in so_search_names:
                                        so_search_names.append(_v)
            except Exception:
                pass

            # Fallback: use MAL title from Jikan
            if not so_search_names and so_title:
                so_search_names.append(so_title.lower())

            if not so_search_names:
                await rep.report(
                    f"⚠️ No search names for spin-off '{so_title}' — skipping",
                    "warning", log=False,
                )
                continue

            await _sl(0.5)

            # ── Search Nyaa ────────────────────────────────────────────────
            candidates: list = []
            seen_urls:  set  = set()

            try:
                for title, url in await _search_nyaa(
                    so_search_names, season=None, episode=None
                ):
                    if url not in seen_urls:
                        candidates.append((_torrent_priority(title), title, url))
                        seen_urls.add(url)
            except Exception:
                pass

            for _sn in so_search_names[:2]:
                try:
                    for title, url in await _search_nyaa_html(
                        _sanitize_nyaa_query(_sn),
                        season=None,
                        max_pages=2,
                        release_year=None,
                    ):
                        if url not in seen_urls:
                            candidates.append((_torrent_priority(title), title, url))
                            seen_urls.add(url)
                except Exception:
                    pass

            if not candidates:
                await rep.report(
                    f"⚠️ No Nyaa results for {so_type} '{so_title}' — skipping",
                    "info", log=False,
                )
                continue

            candidates.sort(key=lambda x: x[0])

            # Prefer a batch/complete pack; otherwise take the best single
            _batch_cands = [
                (p, t, u) for p, t, u in candidates
                if any(k in t.lower() for k in [
                    "batch", "complete", "bdrip", "bd rip",
                    "bluray", "blu-ray", "[bd]", "(bd)",
                ])
            ]
            if _batch_cands:
                _batch_cands.sort(key=lambda x: x[0])
                _, best_title, best_url = _batch_cands[0]
            else:
                _, best_title, best_url = candidates[0]

            await rep.report(
                f"✅ Queuing {so_type} '{so_title}': {best_title}",
                "info", log=False,
            )

            from bot.core.auto_animes import get_animes as _get_animes_so
            _so_alts = [u for _, _, u in (_batch_cands if _batch_cands else candidates)[1:3]]
            bot_loop.create_task(
                _get_animes_so(best_title, best_url, force=True, is_batch=True,
                               alt_torrents=_so_alts)
            )
            queued_count += 1
            await _sl(1.0)

    if queued_count:
        await rep.report(
            f"🎞 Queued {queued_count} OVA/spin-off torrent(s) after {anime_name}",
            "info", log=False,
        )
    else:
        await rep.report(
            f"ℹ️ No OVA/spin-off torrents found for {anime_name}",
            "info", log=False,
        )


async def _get_aired_episodes_from_jikan(mal_id: int) -> int | None:
    """
    Return the number of episodes that have actually aired for this MAL entry.

    MAL's 'episodes' field = planned total (stays null/0 mid-season).
    This function walks Jikan's /anime/{id}/episodes list and counts only
    entries where aired=True — giving the real current episode count.

    Returns:
        int  — confirmed aired episode count
        None — API unavailable (caller falls back to AniList planned total)

    Cached per mal_id so resume / multi-season loops cost nothing extra.
    """
    if mal_id in _aired_eps_cache:
        return _aired_eps_cache[mal_id]

    from aiohttp import ClientSession, ClientTimeout
    TIMEOUT = ClientTimeout(total=8)

    try:
        async with ClientSession(timeout=TIMEOUT) as sess:
            url = f"https://api.jikan.moe/v4/anime/{mal_id}"
            async with sess.get(url) as resp:
                if resp.status != 200:
                    return None
                body = await resp.json()

        data    = (body or {}).get("data") or {}
        planned = data.get("episodes") or 0
        status  = (data.get("status") or "").lower()

        # Cache status so the season loop can skip unreleased seasons
        # without an extra API call.
        _jikan_status_cache[mal_id] = status

        # Cache start year as a side effect — no extra request needed.
        # Used by the season loop to set correct per-season release_year for
        # Nyaa search filtering (e.g. S2 of an anime that started in 2025
        # should search with 2025, not S1's 2023 start year).
        _aired_from = ((data.get("aired") or {}).get("from") or "")
        if _aired_from:
            try:
                _start_yr = int(_aired_from[:4])
                _season_start_year_cache[mal_id] = _start_yr
            except (ValueError, TypeError):
                pass

        if status == "finished airing" and planned > 0:
            # Complete series — planned == aired, no need to walk episode list
            result = planned
        else:
            # Currently airing — walk paginated episode list counting aired entries.
            # NOTE: Jikan's 'aired' field is a date string (e.g. "2026-01-05T00:00:00+00:00")
            # for episodes that have aired, and null for future/unaired episodes.
            # We compare against today's date to get the true aired count.
            import datetime as _dt_jk
            _now_utc = _dt_jk.datetime.now(_dt_jk.timezone.utc)
            aired_count = 0
            page = 1
            async with ClientSession(timeout=TIMEOUT) as sess:
                while True:
                    ep_url = f"https://api.jikan.moe/v4/anime/{mal_id}/episodes?page={page}"
                    async with sess.get(ep_url) as r:
                        if r.status != 200:
                            break
                        ep_body = await r.json()
                    eps = (ep_body or {}).get("data") or []
                    if not eps:
                        break
                    for ep in eps:
                        _aired_val = ep.get("aired")
                        if not _aired_val:
                            continue
                        # aired is a date string — parse and compare to now
                        try:
                            # Handle ISO format with or without timezone
                            _aired_str = str(_aired_val)
                            if _aired_str.endswith("+00:00") or _aired_str.endswith("Z"):
                                _ep_dt = _dt_jk.datetime.fromisoformat(
                                    _aired_str.replace("Z", "+00:00")
                                )
                            else:
                                _ep_dt = _dt_jk.datetime.fromisoformat(_aired_str).replace(
                                    tzinfo=_dt_jk.timezone.utc
                                )
                            if _ep_dt <= _now_utc:
                                aired_count += 1
                        except (ValueError, TypeError):
                            # If we can't parse, treat any non-null aired as aired
                            aired_count += 1
                    pagination = (ep_body or {}).get("pagination") or {}
                    if not pagination.get("has_next_page", False):
                        break
                    page += 1
                    from asyncio import sleep as _sl
                    await _sl(0.4)   # Jikan rate-limit: 3 req/s burst, 60/min

            result = aired_count if aired_count > 0 else (planned or None)

        if result:
            _aired_eps_cache[mal_id] = result
            await rep.report(
                f"Jikan aired eps: mal_id={mal_id} \u2192 {result} (status={status})",
                "info", log=False
            )
        return result

    except Exception as _e:
        await rep.report(f"Jikan aired-eps fetch failed for mal_id={mal_id}: {_e}", "warning", log=False)
        return None


PAGE_SIZE = 8  # anime buttons per page

def _cb_safe(anime_name: str, prefix: str, suffix: str = "") -> str:
    """
    Build a callback_data string that stays within Telegram's 64-byte limit.
    Truncates anime_name at the byte level — [:N] truncates by chars which
    can overflow with multi-byte Unicode (e.g. em-dashes, CJK chars).
    prefix: e.g. "acm_upall|"
    suffix: e.g. "|2" for season callbacks
    """
    max_name_bytes = 64 - len(prefix.encode()) - len(suffix.encode()) - 1
    name_bytes = anime_name.encode("utf-8")
    if len(name_bytes) > max_name_bytes:
        # Truncate at byte boundary without splitting a multi-byte char
        truncated = name_bytes[:max_name_bytes].decode("utf-8", errors="ignore")
    else:
        truncated = anime_name
    return f"{prefix}{truncated}{suffix}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _anime_list_keyboard(mappings: list, page: int = 0) -> InlineKeyboardMarkup:
    """Build paginated anime button grid."""
    start = page * PAGE_SIZE
    end   = start + PAGE_SIZE
    chunk = mappings[start:end]

    rows = []
    # 2 buttons per row
    for i in range(0, len(chunk), 2):
        row = []
        for m in chunk[i:i+2]:
            safe = m['anime_name'][:32]
            row.append(InlineKeyboardButton(
                safe, callback_data=f"acm_info|{m['anime_name'][:40]}"
            ))
        rows.append(row)

    # Pagination row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"acm_page|{page-1}"))
    if end < len(mappings):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"acm_page|{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("❌ Close", callback_data="close")])
    return InlineKeyboardMarkup(rows)


async def _anime_info_keyboard(anime_name: str, anilist_id: int | None, seasons: list,
                               db_type: str = "ongoing") -> InlineKeyboardMarkup:
    """Build action buttons for a specific anime. db_type: 'ongoing'|'completed'|'movie'."""
    rows = []
    if db_type == "movie":
        rows.append([InlineKeyboardButton("🎬 Upload All Movies",    callback_data=_cb_safe(anime_name, "acm_upmovie_all|"))])
        rows.append([InlineKeyboardButton("🎞 Upload Specific Movie", callback_data=_cb_safe(anime_name, "acm_upmovie_pick|"))])
    else:
        rows.append([InlineKeyboardButton("📤 Upload All",             callback_data=_cb_safe(anime_name, "acm_upall|"))])
        rows.append([InlineKeyboardButton("📁 Upload Specific Season", callback_data=_cb_safe(anime_name, "acm_pickseason|"))])
        rows.append([InlineKeyboardButton("🎬 Upload Specific Episode",callback_data=_cb_safe(anime_name, "acm_upep|"))])
    rows.append([InlineKeyboardButton("🗑 Remove Connection",      callback_data=_cb_safe(anime_name, "acm_remove|"))])
    rows.append([InlineKeyboardButton("🧹 Clear Data",             callback_data=_cb_safe(anime_name, "acm_cleardata|"))])
    rows.append([InlineKeyboardButton("⬅️ Back",                   callback_data="acm_back|0")])
    return InlineKeyboardMarkup(rows)


async def _get_seasons(anime_name: str) -> tuple[list, bool, bool]:
    """
    Return (seasons_list, is_exact, is_continuous) for the season picker.

    is_exact=True     → Jikan found a known SEQUEL chain (e.g. AoT [1,2,3,4])
    is_exact=False    → Continuous series or API unavailable
    is_continuous=True → Long-running single-entry anime (One Piece, Naruto, Bleach)
                         Detected by: episodes>100 OR (episodes=None AND status=RELEASING)
                         AND Jikan count=1
    """
    is_continuous = False
    try:
        aniInfo = TextEditor(anime_name)
        await aniInfo.load_anilist()
        mal_id = aniInfo.adata.get("idMal")
        ep_count = aniInfo.adata.get("episodes")
        status   = aniInfo.adata.get("status", "")

        if mal_id:
            count = await _get_season_count_from_jikan(int(mal_id))
            if count > 1:
                return list(range(1, count + 1)), True, False
            # count=1 — check if it's genuinely a long-running continuous series
            start_year = ((aniInfo.adata.get("startDate") or {}).get("year") or 9999)
            import datetime
            current_year = datetime.datetime.now().year
            years_airing = current_year - start_year
            # Continuous if: 100+ episodes OR (ongoing AND started 2+ years ago)
            if (ep_count and ep_count > 100) or (ep_count is None and years_airing >= 2):
                is_continuous = True
    except Exception:
        pass
    # Continuous series or API failure — offer 1..30 as navigable default
    return list(range(1, 31)), False, is_continuous


PAGE_SEASONS = 10  # max season buttons per page

def _season_keyboard(seasons: list, page: int, cb_prefix: str, cancel_cb: str) -> InlineKeyboardMarkup:
    """
    Build a paginated season-picker keyboard.

    seasons    : full list of season numbers e.g. [1,2,...,30]
    page       : 0-indexed current page
    cb_prefix  : callback_data prefix for each season button
                 button fires  cb_prefix|{season}
    cancel_cb  : callback_data for the ❌ Cancel button
    """
    start  = page * PAGE_SEASONS
    chunk  = seasons[start:start + PAGE_SEASONS]
    rows   = []

    # Season buttons — 5 per row so 2 rows max per page
    for i in range(0, len(chunk), 5):
        rows.append([
            InlineKeyboardButton(f"S{s:02d}", callback_data=f"{cb_prefix}|{s}")
            for s in chunk[i:i+5]
        ])

    # Prev / Next nav — only shown when there are more pages
    total_pages = (len(seasons) + PAGE_SEASONS - 1) // PAGE_SEASONS
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{cb_prefix}_page|{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{cb_prefix}_page|{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=cancel_cb)])
    return InlineKeyboardMarkup(rows)


# ── /listconnections ──────────────────────────────────────────────────────────

# NOTE: The /listconnections slash command was removed. The list is now
# rendered via /settings → 📺 Channel Management → 📋 List Connections,
# which calls this function directly with the panel's chat as `message`.
@new_task
async def list_connections_cmd(client, message):
    mappings = await db.get_all_anime_channels()
    if not mappings:
        return await sendMessage(
            message,
            "<b>No channels connected yet.</b>\n"
            "<i>Use /settings → 📺 Channel Management → 🔗 Connect to add one.</i>"
        )
    # Sort alphabetically
    mappings.sort(key=lambda x: x['anime_name'].lower())
    keyboard = _anime_list_keyboard(mappings, page=0)
    await sendMessage(
        message,
        f"<b>📺 Connected Anime Channels ({len(mappings)})</b>\n"
        f"<i>Select an anime to manage it:</i>",
        keyboard
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^acm_page\|(\d+)$"))
async def acm_page(client, query):
    page = int(query.data.split("|")[1])
    mappings = await db.get_all_anime_channels()
    mappings.sort(key=lambda x: x['anime_name'].lower())
    keyboard = _anime_list_keyboard(mappings, page=page)
    await query.edit_message_reply_markup(keyboard)
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_info\|"))
async def acm_info(client, query):
    anime_name = query.data.split("|", 1)[1]
    mappings   = await db.get_all_anime_channels()
    info       = next((m for m in mappings if m['anime_name'] == anime_name), None)

    if not info:
        await query.answer("Anime not found.", show_alert=True)
        return

    # Use stored ani_id if available, otherwise search by name
    stored_ani_id = info.get("ani_id")

    try:
        if stored_ani_id:
            # Fetch directly by ID — fast and accurate
            import aiohttp
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    "https://graphql.anilist.co",
                    json={
                        "query": """query($id:Int){Media(id:$id,type:ANIME){
                            id title{romaji english} status episodes averageScore
                        }}""",
                        "variables": {"id": stored_ani_id}
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    body  = await resp.json(content_type=None)
                    media = ((body or {}).get("data") or {}).get("Media") or {}
            ani_id    = media.get("id", stored_ani_id)
            titles    = media.get("title") or {}
            eng_title = titles.get("english") or titles.get("romaji") or anime_name
            status    = media.get("status", "N/A")
            episodes  = media.get("episodes", "N/A")
            score     = media.get("averageScore", "N/A")
        else:
            # Fall back to name search
            aniInfo = TextEditor(anime_name)
            await aniInfo.load_anilist()
            ani_id    = aniInfo.adata.get('id', 'N/A')
            titles    = aniInfo.adata.get('title', {})
            eng_title = titles.get('english') or titles.get('romaji') or anime_name
            status    = aniInfo.adata.get('status', 'N/A')
            episodes  = aniInfo.adata.get('episodes', 'N/A')
            score     = aniInfo.adata.get('averageScore', 'N/A')
    except Exception:
        ani_id = stored_ani_id or 'N/A'
        eng_title = anime_name
        status = episodes = score = 'N/A'

    # ── Gather per-season upload data ─────────────────────────────────────
    uploaded_eps = 0
    season_summary_lines = []
    QUAL_DISPLAY = {"Hdri": "HDRi", "1080": "1080p", "720": "720p", "480": "480p"}

    try:
        if stored_ani_id:
            # Batch pipeline: check s1..s10
            for _sn in range(1, 11):
                _sk = f"s{_sn}"
                _ep_data = await batch_db.get_batch_ep_links(stored_ani_id, season=_sk)
                if not _ep_data:
                    continue
                ep_count = len(_ep_data)
                uploaded_eps += ep_count

                # Collect all qualities present across all episodes this season
                quals_seen: set = set()
                for _ep_quals in _ep_data.values():
                    if isinstance(_ep_quals, dict):
                        for q in _ep_quals.keys():
                            quals_seen.add(QUAL_DISPLAY.get(q, q))

                qual_str = " · ".join(sorted(quals_seen, key=lambda q: ["HDRi","1080p","720p","480p"].index(q) if q in ["HDRi","1080p","720p","480p"] else 99)) if quals_seen else "—"
                ep_range = ""
                if ep_count > 0:
                    ep_nums = sorted(_ep_data.keys())
                    ep_range = f"EP {ep_nums[0]}–{ep_nums[-1]}" if len(ep_nums) > 1 else f"EP {ep_nums[0]}"
                season_summary_lines.append(
                    f"  <b>S{_sn:02d}</b>  {ep_count} ep  |  {ep_range}  |  {qual_str}"
                )

        # Ongoing pipeline fallback (no batch_ep_links stored)
        if uploaded_eps == 0 and str(ani_id).isdigit():
            _ong_data = await db.getAnime(ani_id)
            if _ong_data:
                uploaded_eps = len(_ong_data)
                # _ong_data: { ep_number: { "720_Sub": post_id, ... } }
                quals_seen: set = set()
                for _ep_quals in _ong_data.values():
                    if isinstance(_ep_quals, dict):
                        for qk in _ep_quals.keys():
                            q_part = qk.split("_")[0]
                            quals_seen.add(QUAL_DISPLAY.get(q_part, q_part))
                qual_str = " · ".join(sorted(quals_seen, key=lambda q: ["HDRi","1080p","720p","480p"].index(q) if q in ["HDRi","1080p","720p","480p"] else 99)) if quals_seen else "—"
                ep_nums = sorted(_ong_data.keys())
                ep_range = f"EP {ep_nums[0]}–{ep_nums[-1]}" if len(ep_nums) > 1 else f"EP {ep_nums[0]}"
                season_summary_lines.append(
                    f"  <b>Ongoing</b>  {uploaded_eps} ep  |  {ep_range}  |  {qual_str}"
                )
    except Exception:
        pass

    ch_id_clean = str(info['channel_id']).replace('-100', '')
    ch_link = f"https://t.me/c/{ch_id_clean}/1"

    season_block = ""
    if season_summary_lines:
        season_block = f"\n<b>📦 Uploaded Seasons:</b>\n" + "\n".join(season_summary_lines) + "\n"

    text = (
        f"<b>📺 {eng_title}</b>\n"
        f"{'─'*30}\n"
        f"<b>Stored as:</b> <code>{anime_name}</code>\n"
        f"<b>AniList ID:</b> <code>{ani_id}</code>\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Total Episodes:</b> {episodes}\n"
        f"<b>Score:</b> {score}/100\n"
        f"{'─'*30}\n"
        f"<b>Channel ID:</b> <code>{info['channel_id']}</code>\n"
        f"<b>Channel:</b> <a href='{ch_link}'>{info.get('channel_title','?')}</a>\n"
        f"<b>Invite Link:</b> {info.get('invite_link') or 'Not set'}\n"
        f"<b>Uploaded Episodes:</b> {uploaded_eps}\n"
        f"{season_block}"
    )

    _db_type  = info.get("db_type", "ongoing")
    keyboard  = await _anime_info_keyboard(anime_name, ani_id, [], db_type=_db_type)
    _type_icon = {"ongoing": "📡", "completed": "📦", "movie": "🎬"}.get(_db_type, "📺")
    text = text.replace("<b>📺", f"<b>{_type_icon}", 1)
    await query.edit_message_text(text, reply_markup=keyboard, disable_web_page_preview=True)
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_back\|"))
async def acm_back(client, query):
    page = int(query.data.split("|")[1])
    mappings = await db.get_all_anime_channels()
    mappings.sort(key=lambda x: x['anime_name'].lower())
    keyboard = _anime_list_keyboard(mappings, page=page)
    await query.edit_message_text(
        f"<b>📺 Connected Anime Channels ({len(mappings)})</b>\n"
        f"<i>Select an anime to manage it:</i>",
        reply_markup=keyboard
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_remove\|"))
async def acm_remove(client, query):
    """Step 1 — show soft-unlink confirmation card.

    This is a SOFT unlink, not a wipe. Only the channel mapping is
    dropped; episode records, batch links and Telegram videos are
    untouched, so users keep accessing previously posted episodes.
    For the destructive purge, use the dedicated 🧹 Clear Data action.
    """
    anime_name = query.data.split("|", 1)[1]
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔌 Yes, Unlink", callback_data=_cb_safe(anime_name, "acm_confirm_remove|")),
            InlineKeyboardButton("❌ Cancel",      callback_data=_cb_safe(anime_name, "acm_info|")),
        ]
    ])
    await query.edit_message_text(
        f"<b>🔌 Unlink channel for:</b>\n<code>{anime_name}</code>\n\n"
        f"<b>This is a soft unlink — only the channel ↔ anime mapping "
        f"is removed.</b>\n\n"
        f"<b>What will change:</b>\n"
        f"• Bot will <b>stop posting new episodes</b> of this anime to "
        f"the channel\n"
        f"• RSS / auto-fetch for this anime will pause\n"
        f"• Batch pipeline will not process new episodes for it\n\n"
        f"<b>What stays intact (nothing is deleted):</b>\n"
        f"• All episode records — file IDs, post links, message IDs\n"
        f"• Batch links and batch episode entries\n"
        f"• Schedule / ending posts, seen torrents\n"
        f"• Local download folder on disk\n"
        f"• All videos already uploaded to the Telegram channel\n\n"
        f"<i>Users can still access the old episodes via the existing "
        f"Get Episode buttons and deep links. Use 🧹 Clear Data instead "
        f"if you want to purge episode records.</i>",
        reply_markup=keyboard
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_confirm_remove\|"))
async def acm_confirm_remove(client, query):
    """Step 2 — perform the soft unlink (drops only anime_channels)."""
    anime_name = query.data.split("|", 1)[1]
    await query.edit_message_text(
        f"<b>🔌 Unlinking:</b> <code>{anime_name}</code>\n\n"
        f"<i>Please wait...</i>"
    )
    try:
        success = await db.remove_anime_channel(anime_name)
    except Exception as e:
        await rep.report(f"acm unlink error: {e}", "error")
        return await query.edit_message_text(
            f"<b>❌ Unlink failed:</b> <code>{anime_name}</code>\n\n<code>{e}</code>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="acm_back|0")
            ]])
        )

    if success:
        mappings = await db.get_all_anime_channels()
        mappings.sort(key=lambda x: x['anime_name'].lower())
        keyboard = _anime_list_keyboard(mappings, page=0)
        await query.edit_message_text(
            f"<b>✅ Unlinked:</b> <code>{anime_name}</code>\n"
            f"<i>Bot will stop posting new episodes for this anime. "
            f"Episode records, batch links and Telegram videos are "
            f"intact — users can still access the old episodes.</i>\n\n"
            f"<b>📺 Connected Anime Channels ({len(mappings)})</b>\n"
            f"<i>Select an anime to manage it:</i>",
            reply_markup=keyboard
        )
    else:
        await query.edit_message_text(
            f"<b>❌ Failed to unlink:</b> <code>{anime_name}</code>\n"
            f"<i>Anime not found in database.</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="acm_back|0")
            ]])
        )
    await query.answer()


# ── Clear Data ────────────────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^acm_cleardata\|"))
@new_task
async def acm_cleardata(client, query):
    """Step 1 — ask which season to clear."""
    anime_name = query.data.split("|", 1)[1]
    seasons, is_exact, is_continuous = await _get_seasons(anime_name)

    rows = []
    # Season buttons — up to 5 seasons shown, plus "All Seasons"
    season_row = []
    for s in seasons[:10]:
        season_row.append(InlineKeyboardButton(
            f"S{s:02d}", callback_data=_cb_safe(anime_name, "acm_clearseason|", f"|{s}")
        ))
        if len(season_row) == 5:
            rows.append(season_row)
            season_row = []
    if season_row:
        rows.append(season_row)

    rows.append([InlineKeyboardButton("🗂 All Seasons", callback_data=_cb_safe(anime_name, "acm_clearseason|", "|all"))])
    rows.append([InlineKeyboardButton("❌ Cancel",      callback_data=_cb_safe(anime_name, "acm_info|"))])

    await query.edit_message_text(
        f"<b>🧹 Clear Data for:</b> <code>{anime_name}</code>\n\n"
        f"<i>Select which season's episode data to clear.\n"
        f"This removes stored upload links and resume data — it does NOT delete channel posts.</i>",
        reply_markup=InlineKeyboardMarkup(rows)
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_clearseason\|"))
@new_task
async def acm_clearseason(client, query):
    """Step 2 — confirm clear for chosen season."""
    parts = query.data.split("|")
    anime_name = parts[1]
    season_val = parts[2]  # "1", "2", ... or "all"

    label = f"Season {season_val}" if season_val != "all" else "All Seasons"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"✅ Yes, Clear {label}",
            callback_data=_cb_safe(anime_name, "acm_confirmclear|", f"|{season_val}")
        ),
        InlineKeyboardButton("❌ Cancel", callback_data=_cb_safe(anime_name, "acm_info|"))
    ]])

    await query.edit_message_text(
        f"<b>⚠️ Confirm Clear Data</b>\n\n"
        f"<b>Anime:</b> <code>{anime_name}</code>\n"
        f"<b>Season:</b> {label}\n\n"
        f"<i>This will delete all stored episode upload links and resume progress for {label}.\n"
        f"Channel posts will NOT be deleted.</i>",
        reply_markup=keyboard
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_confirmclear\|"))
@new_task
async def acm_confirmclear(client, query):
    """Step 3 — perform the clear."""
    parts = query.data.split("|")
    anime_name = parts[1]
    season_val = parts[2]

    mappings = await db.get_all_anime_channels()
    info = next((m for m in mappings if m['anime_name'] == anime_name), None)
    ani_id = info.get("ani_id") if info else None

    # If ani_id not stored in channel doc, look it up from AniList
    if not ani_id:
        try:
            from bot.core.text_utils import TextEditor as _TE
            _ai = _TE(anime_name)
            await _ai.load_anilist()
            ani_id = _ai.adata.get("id")
            await rep.report(f"🔍 Looked up ani_id={ani_id} for {anime_name}", "info", log=False)
        except Exception:
            pass

    deleted = 0

    # ── Resolve the correct AniList ID for the target season ─────────────
    # The channel mapping stores the S1/root AniList ID, but the pipeline
    # saves batch_ep_links keyed by whichever AniList ID the torrent name
    # resolved to (e.g. S3's own ID). We must use that same ID when deleting.
    _season_ani_id = ani_id  # fallback to root
    if season_val != "all" and ani_id:
        try:
            _mal_id_root = None
            # Get MAL ID from AniList data
            _ai_tmp = TextEditor(anime_name)
            await _ai_tmp.load_anilist()
            _mal_id_root = _ai_tmp.adata.get("idMal")
            if _mal_id_root:
                _chain = await _get_sequel_chain(int(_mal_id_root))
                _s_idx = int(season_val) - 1
                _season_mal = _chain[_s_idx] if _s_idx < len(_chain) else None
                if _season_mal:
                    # Fetch AniList ID for this season's MAL ID
                    import aiohttp
                    _gql = """query($id:Int){Media(idMal:$id,type:ANIME){id}}"""
                    async with aiohttp.ClientSession() as _sess:
                        async with _sess.post(
                            "https://graphql.anilist.co",
                            json={"query": _gql, "variables": {"id": _season_mal}},
                            timeout=aiohttp.ClientTimeout(total=8)
                        ) as _r:
                            if _r.status == 200:
                                _rd = await _r.json()
                                _season_ani_id = ((_rd.get("data") or {}).get("Media") or {}).get("id") or ani_id
        except Exception:
            _season_ani_id = ani_id  # safe fallback

    # Try all possible ani_id formats (both the season-specific and root IDs)
    _ani_ids = []
    for _raw_id in {_season_ani_id, ani_id}:
        if _raw_id:
            _ani_ids.extend([_raw_id, str(_raw_id)])
            if str(_raw_id).isdigit():
                _ani_ids.append(int(_raw_id))
    _ani_ids = list(dict.fromkeys(_ani_ids))  # deduplicate preserving order

    if _ani_ids:
        if season_val == "all":
            # ── Delete ALL data for this anime ────────────────────────────
            # 1. All season-keyed batch_ep_links
            for s in range(1, 11):
                for _id in _ani_ids:
                    r = await batch_db.db.batch_ep_links.delete_one({"ani_id": f"{_id}_s{s}"})
                    deleted += r.deleted_count
            # 2. Old-format batch_ep_links (integer key)
            for _id in _ani_ids:
                r = await batch_db.db.batch_ep_links.delete_one({"ani_id": _id})
                deleted += r.deleted_count
            # 3. batch_links — delete all season docs (new + legacy schema)
            for _id in _ani_ids:
                # New schema: ani_id_raw field
                r = await batch_db.db.batch_links.delete_many({"ani_id_raw": _id})
                deleted += r.deleted_count
                # Old legacy key prefix e.g. "150672_*"
                _id_str = str(_id)
                if _id_str.isdigit():
                    r = await batch_db.db.batch_links.delete_many(
                        {"ani_id": {"$regex": f"^{_id_str}_", "$options": ""}}
                    )
                    deleted += r.deleted_count
            # 4. batch_links by torrent name (fallback-keyed docs)
            r = await batch_db.db.batch_links.delete_many({"torrent_name": {"$regex": anime_name, "$options": "i"}})
            deleted += r.deleted_count
            # 5. anime_data (ongoing pipeline episode records)
            for _id in _ani_ids:
                r = await db.db.anime_data.delete_many({"anime_id": _id})
                deleted += r.deleted_count
        else:
            sk = f"s{season_val}"

            # 1. Season-keyed batch_ep_links
            for _id in _ani_ids:
                r = await batch_db.db.batch_ep_links.delete_one({"ani_id": f"{_id}_{sk}"})
                deleted += r.deleted_count
            # 2. Old-format (only for s1)
            if season_val == "1":
                for _id in _ani_ids:
                    r = await batch_db.db.batch_ep_links.delete_one({"ani_id": _id})
                    deleted += r.deleted_count

            # 3. batch_links — new schema: delete per-season doc directly
            for _id in _ani_ids:
                _id_str = str(_id)
                if _id_str.isdigit():
                    # New key: "{ani_id}_{file_store}_{season}"
                    r = await batch_db.db.batch_links.delete_many(
                        {"ani_id": {"$regex": f"^{_id_str}_.*_{sk}$", "$options": ""}}
                    )
                    deleted += r.deleted_count
                # ani_id_raw + season
                r = await batch_db.db.batch_links.delete_many(
                    {"ani_id_raw": _id, "season": sk}
                )
                deleted += r.deleted_count

            # Legacy schema: unset season-prefixed fields from old single doc
            _legacy_unset = {k: "" for k in [
                f"{sk}_index_post_id",    f"{sk}_index_post_channel",
                f"{sk}_notify_post_id",
                f"{sk}_first_Hdri",       f"{sk}_last_Hdri",
                f"{sk}_first_1080",       f"{sk}_last_1080",
                f"{sk}_first_720",        f"{sk}_last_720",
                f"{sk}_first_480",        f"{sk}_last_480",
            ]}
            for _id in _ani_ids:
                r = await batch_db.db.batch_links.update_many(
                    {"ani_id_raw": _id, "season": {"$exists": False}},
                    {"$unset": _legacy_unset}
                )
                deleted += r.modified_count

            # Torrent-name keyed docs
            r = await batch_db.db.batch_links.delete_many(
                {"torrent_name": {"$regex": anime_name, "$options": "i"}, "season": sk}
            )
            deleted += r.deleted_count
            r = await batch_db.db.batch_links.update_many(
                {"torrent_name": {"$regex": anime_name, "$options": "i"}, "season": {"$exists": False}},
                {"$unset": _legacy_unset}
            )
            deleted += r.modified_count

            # 4. anime_data (ongoing pipeline) — delete episode records
            for _id in _ani_ids:
                r = await db.db.anime_data.delete_many({"anime_id": _id})
                deleted += r.deleted_count

    # ── Delete downloaded files from disk ────────────────────────────────
    import os as _os
    import re as _re_safe
    from aioshutil import rmtree as _aiorm

    # Build the safe folder name the same way auto_animes.py does
    # Use anime_name directly for folder path — same as channel_manager download
    # side and auto_animes batch pipeline. Using AniList title caused mismatches
    # (e.g. "...Dance_of_Spring_Shunkas" vs "...Dance_of_Spr") leaving orphan folders.
    _dl_title = anime_name

    _safe = _re_safe.sub(r"[^\w\s-]", " ", _dl_title)
    _safe = _re_safe.sub(r"\s+", " ", _safe).strip().replace(" ", "_")[:50]

    _dirs_deleted = 0
    # Only delete the shared download folder when clearing ALL seasons.
    # For a single-season clear the folder may contain files from other
    # seasons that are still needed, so we leave it on disk.
    if season_val == "all":
        for _base in ["./downloads/batch", "./downloads/ongoing", "./downloads/movies"]:
            _target = _os.path.join(_base, _safe)
            if _os.path.isdir(_target):
                try:
                    await _aiorm(_target)
                    _dirs_deleted += 1
                    await rep.report(f"🗑 Deleted download folder: {_target}", "info", log=False)
                except Exception as _de:
                    await rep.report(f"Failed to delete {_target}: {_de}", "warning", log=False)

    label = f"Season {season_val}" if season_val != "all" else "All Seasons"

    # ── Clear in-memory cache for this anime ─────────────────────────────
    # So the episode count refreshes immediately and Upload All starts fresh
    if ani_id:
        from bot.core.auto_animes import ani_cache
        keys_to_remove = [k for k in ani_cache.get('ongoing', {})
                          if k.startswith(f"{ani_id}_")]
        for k in keys_to_remove:
            ani_cache['ongoing'].pop(k, None)
            ani_cache['completed'].pop(k, None)

        # Also clear seen_torrents from DB for this anime so it can be re-found
        try:
            _seen_col = db.db["seen_torrents"]
            _r = await _seen_col.delete_many({"key": {"$regex": anime_name, "$options": "i"}})
            deleted += _r.deleted_count
        except Exception:
            pass

    await query.edit_message_text(
        f"<b>✅ Cleared {label} data for:</b> <code>{anime_name}</code>\n"
        f"<b>Removed DB records:</b> {deleted}\n"
        f"<b>Removed download folders:</b> {_dirs_deleted}\n\n"
        f"<i>You can now re-upload this season fresh.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))
        ]])
    )
    await query.answer()




@bot.on_callback_query(filters.regex(r"^acm_upall\|"))
async def acm_upload_all(client, query):
    anime_name = query.data.split("|", 1)[1]
    await query.answer("🔄 Queuing all seasons...", show_alert=False)
    _mappings  = await db.get_all_anime_channels()
    _info      = next((m for m in _mappings if m['anime_name'] == anime_name), {})
    _db_type   = _info.get("db_type", "ongoing")
    _is_batch  = _db_type == "completed"
    bot_loop.create_task(_queue_upload(query, anime_name, season=None, episode=None, mode="all", is_batch=_is_batch))


@bot.on_callback_query(filters.regex(r"^acm_upmovie_all\|"))
@new_task
async def acm_upmovie_all(client, query):
    """Queue all franchise movies sequentially in release order."""
    from bot.core.auto_animes import get_animes
    anime_name = query.data.split("|", 1)[1]

    await query.edit_message_text(
        f"<b>🔍 Fetching movie list for:</b> <code>{anime_name}</code>\n"
        f"<i>Loading franchise data from AniList...</i>"
    )
    await query.answer()

    movies = await get_franchise_movies(anime_name)
    search_names = await _get_search_names(anime_name)

    if not movies:
        await query.edit_message_text(
            f"<b>⚠️ No movies found in AniList for:</b> <code>{anime_name}</code>\n\n"
            f"<i>Try /addmagnet with direct Nyaa.si URLs.</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))
            ]])
        )
        return

    queued, skipped = 0, 0
    lines = []
    for mv in movies:
        candidates = await _search_movie_torrent(search_names, movie_title=mv["title"])
        if not candidates:
            skipped += 1
            lines.append(f"⚠️ {mv['title']} ({mv['year']}) — no torrent found")
            continue
        _, t_title, t_url = candidates[0]
        bot_loop.create_task(get_animes(t_title, t_url, force=True, is_movie=True))
        queued += 1
        lines.append(f"✅ {mv['title']} ({mv['year']})")

    summary = "\n".join(lines)
    await query.edit_message_text(
        f"<b>🎬 Queued {queued}/{len(movies)} movies for:</b> <code>{anime_name}</code>\n\n"
        f"<blockquote>{summary}</blockquote>\n\n"
        f"<i>Movies will be uploaded in release order.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))
        ]])
    )


@bot.on_callback_query(filters.regex(r"^acm_upmovie_pick\|"))
@new_task
async def acm_upmovie_pick(client, query):
    """Show paginated list of franchise movies — one button per movie."""
    anime_name = query.data.split("|", 1)[1]

    await query.edit_message_text(
        f"<b>🔍 Loading movies for:</b> <code>{anime_name}</code>\n"
        f"<i>Fetching from AniList...</i>"
    )
    await query.answer()

    movies = await get_franchise_movies(anime_name)

    if not movies:
        await query.edit_message_text(
            f"<b>⚠️ No movies found for:</b> <code>{anime_name}</code>\n\n"
            f"<i>AniList returned no movie relations for this franchise.\n"
            f"Use /addmagnet with a direct Nyaa.si URL instead.</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))
            ]])
        )
        return

    await _show_movie_pick_page(query, anime_name, movies, page=0)


async def _show_movie_pick_page(query, anime_name: str, movies: list, page: int):
    """Render one page of the movie picker (10 per page, one button per line)."""
    PAGE = 10
    start  = page * PAGE
    chunk  = movies[start:start + PAGE]
    total  = len(movies)
    pages  = -(-total // PAGE)   # ceiling division

    rows = []
    for mv in chunk:
        year_tag = f" ({mv['year']})" if mv["year"] else ""
        label    = f"🎬 {mv['title']}{year_tag}"[:64]
        # encode ani_id into callback so single-movie handler knows which to queue
        rows.append([InlineKeyboardButton(
            label,
            callback_data=_cb_safe(anime_name, "acm_upmovie_single|", "|{mv['id']}")
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=_cb_safe(anime_name, "acm_upmovie_page|", "|{page-1}")))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=_cb_safe(anime_name, "acm_upmovie_page|", "|{page+1}")))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))])

    page_label = f"Page {page+1}/{pages}" if pages > 1 else ""
    await query.edit_message_text(
        f"<b>🎬 Select a movie — <code>{anime_name}</code></b>\n"
        f"<i>{total} movie(s) in franchise{(' — ' + page_label) if page_label else ''}</i>",
        reply_markup=InlineKeyboardMarkup(rows)
    )


@bot.on_callback_query(filters.regex(r"^acm_upmovie_page\|"))
@new_task
async def acm_upmovie_page(client, query):
    """Prev/Next page navigation for the movie picker."""
    parts      = query.data.split("|")
    anime_name = parts[1]
    page       = int(parts[2])
    movies     = await get_franchise_movies(anime_name)
    await query.answer()
    await _show_movie_pick_page(query, anime_name, movies, page=page)


@bot.on_callback_query(filters.regex(r"^acm_upmovie_single\|"))
@new_task
async def acm_upmovie_single(client, query):
    """Queue a single specific movie by AniList ID."""
    from bot.core.auto_animes import get_animes
    parts      = query.data.split("|")
    anime_name = parts[1]
    ani_id     = int(parts[2])

    await query.answer("🔍 Searching for torrent...")

    # Find the movie record to get its title for a precise search
    movies     = await get_franchise_movies(anime_name)
    mv_record  = next((m for m in movies if m["id"] == ani_id), None)
    mv_title   = mv_record["title"] if mv_record else anime_name
    mv_year    = mv_record["year"]  if mv_record else ""

    search_names = await _get_search_names(anime_name)
    candidates   = await _search_movie_torrent(search_names, movie_title=mv_title)

    if not candidates:
        await query.edit_message_text(
            f"<b>⚠️ No torrent found for:</b>\n<code>{mv_title}</code>\n\n"
            f"<i>Try /addmagnet with a direct Nyaa.si URL or magnet link.</i>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_upmovie_pick|"))
            ]])
        )
        return

    _, t_title, t_url = candidates[0]
    bot_loop.create_task(get_animes(t_title, t_url, force=True, is_movie=True))

    await query.edit_message_text(
        f"<b>✅ Queued:</b>\n"
        f"<code>{mv_title}{' (' + str(mv_year) + ')' if mv_year else ''}</code>\n\n"
        f"<b>Torrent:</b> <code>{t_title}</code>\n\n"
        f"<i>Download → encode → upload starting shortly.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_upmovie_pick|"))
        ]])
    )


@bot.on_callback_query(filters.regex(r"^acm_pickseason\|"))
@new_task
async def acm_pick_season(client, query):
    """Open the paginated season picker (page 0)."""
    anime_name = query.data.split("|", 1)[1]
    seasons, is_exact, is_continuous = await _get_seasons(anime_name)
    label = f"{len(seasons)} seasons via MAL" if is_exact else "navigate to your season"
    kb = _season_keyboard(seasons, page=0,
                          cb_prefix=f"acm_upseason|{anime_name[:40]}",
                          cancel_cb=f"acm_info|{anime_name[:40]}")
    await query.edit_message_text(
        f"<b>📁 Select season — <code>{anime_name}</code></b>\n"
        f"<i>({label})</i>",
        reply_markup=kb
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_upseason\|.*\|\d+$"))
async def acm_upload_season(client, query):
    """User picked a season number — queue the upload."""
    parts      = query.data.split("|")
    anime_name = parts[1]
    season     = int(parts[2])
    await query.answer(f"🔄 Queuing Season {season}...", show_alert=False)
    _mappings  = await db.get_all_anime_channels()
    _info      = next((m for m in _mappings if m['anime_name'] == anime_name), {})
    _db_type   = _info.get("db_type", "ongoing")
    _is_batch  = _db_type == "completed"
    bot_loop.create_task(_queue_upload(query, anime_name, season=season, episode=None, mode="season", is_batch=_is_batch))


@bot.on_callback_query(filters.regex(r"^acm_upseason\|.*_page\|\d+$"))
@new_task
async def acm_season_page(client, query):
    """Prev/Next page navigation for the season picker."""
    # callback: acm_upseason|{anime_name}_page|{page}
    raw        = query.data  # e.g. "acm_upseason|One Piece_page|2"
    page_part  = raw.rsplit("_page|", 1)
    page       = int(page_part[1])
    inner      = page_part[0]                    # "acm_upseason|One Piece"
    anime_name = inner.split("|", 1)[1]
    seasons, is_exact, is_continuous = await _get_seasons(anime_name)
    label = f"{len(seasons)} seasons via MAL" if is_exact else "navigate to your season"
    kb = _season_keyboard(seasons, page=page,
                          cb_prefix=f"acm_upseason|{anime_name[:40]}",
                          cancel_cb=f"acm_info|{anime_name[:40]}")
    await query.edit_message_text(
        f"<b>📁 Select season — <code>{anime_name}</code></b>\n"
        f"<i>({label})</i>",
        reply_markup=kb
    )
    await query.answer()


@bot.on_callback_query(filters.regex(r"^acm_upep\|"))
async def acm_upload_episode(client, query):
    anime_name = query.data.split("|", 1)[1]
    # Ask for episode number
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data=_cb_safe(anime_name, "acm_info|"))]
    ])
    await query.edit_message_text(
        f"<b>🎬 Upload specific episode for:</b>\n<code>{anime_name}</code>\n\n"
        f"Reply with:\n"
        f"• Episode number: <code>5</code> or <code>S02E05</code>\n"
        f"• Nyaa.si URL: <code>https://nyaa.si/view/1549326</code>\n"
        f"• Magnet link\n\n"
        f"<i>Or use /addtask to manually queue from RSS.</i>",
        reply_markup=keyboard
    )
    # Store pending state in DB so we can catch the reply
    await db.set_pending_episode(query.from_user.id, anime_name)
    await query.answer()


@bot.on_message(filters.private & admin, group=1)
@new_task
async def acm_ep_reply(client, message):
    """Catch episode number for upload episode flow — send the number or a Nyaa/magnet URL."""
    if not message.text or message.text.startswith("/"):
        return
    pending = await db.get_pending_episode(message.from_user.id)
    if not pending:
        return

    anime_name = pending["anime_name"]
    action     = pending.get("action", "episode")
    ep_input   = message.text.strip()
    await db.clear_pending_episode(message.from_user.id)

    # ── Season text input (continuous series like One Piece) ──────────────
    if action == "season":
        if not ep_input.isdigit() or int(ep_input) < 1:
            return await sendMessage(
                message,
                "<b>❌ Please reply with a valid season number (e.g. <code>2</code> or <code>14</code>).</b>"
            )
        season = int(ep_input)
        status_msg = await sendMessage(
            message,
            f"<b>🔍 Searching Season {season} of:</b> <code>{anime_name}</code>\n"
            f"<i>This may take a moment...</i>"
        )
        _mappings2 = await db.get_all_anime_channels()
        _info2     = next((m for m in _mappings2 if m['anime_name'] == anime_name), {})
        _db_type2  = _info2.get("db_type", "ongoing")
        _is_batch2 = _db_type2 == "completed"
        bot_loop.create_task(_queue_upload(status_msg, anime_name, season=season, episode=None, mode="season", is_batch=_is_batch2))
        return

    # ── Allow pasting a direct Nyaa URL or magnet link ────────────────────
    if ep_input.startswith("magnet:") or "nyaa.si" in ep_input:
        torrent_url = ep_input
        # Extract torrent name from magnet dn= param if possible
        from urllib.parse import urlparse, parse_qs, unquote
        try:
            parsed = parse_qs(urlparse(torrent_url).query)
            tname = unquote(parsed['dn'][0]) if 'dn' in parsed else anime_name
        except Exception:
            tname = anime_name
        bot_loop.create_task(get_animes(tname, torrent_url, force=True))
        return await sendMessage(
            message,
            f"<b>✅ Queuing from URL:</b> <code>{tname}</code>\n"
            f"<i>Processing will start shortly.</i>"
        )

    # ── Parse episode number ──────────────────────────────────────────────
    import re
    match = re.search(r'[Ss]?(\d{1,2})[Ee](\d{1,3})', ep_input)
    if match:
        season  = int(match.group(1))
        episode = int(match.group(2))
    elif ep_input.isdigit():
        season  = 1
        episode = int(ep_input)
    else:
        return await sendMessage(message, "<b>❌ Invalid format. Use <code>5</code>, <code>S01E05</code>, a magnet link, or a nyaa.si URL.</b>")

    status_msg = await sendMessage(
        message,
        f"<b>🔍 Searching RSS for:</b> <code>{anime_name}</code> S{season:02d}E{episode:02d}..."
    )

    # ── Search Nyaa (RSS for recent + HTML for archive) ──────────────────
    search_names = await _get_search_names(anime_name)
    triggered = 0
    ep_candidates = []

    # RSS — for currently airing episodes
    from bot.core.func_utils import getfeed_all as _getfeed_all_cm1
    for rss_url in Var.RSS_ITEMS:
        # One fetch per feed (was 150 fetches per feed)
        for info in await _getfeed_all_cm1(rss_url, max_entries=150):
            title_lower = info.title.lower()
            if not any(n in title_lower for n in search_names):
                continue
            ep_patterns = [f"e{episode:02d}", f"- {episode:02d} ", f"- {episode:02d}[",
                           f"[{episode:02d}]", f" {episode:02d} "]
            if not any(p in title_lower for p in ep_patterns):
                continue
            if season:
                if (f"s{season:02d}" not in title_lower and f"s{season}" not in title_lower
                        and f"season {season}" not in title_lower):
                    continue
            ep_candidates.append((_torrent_priority(info.title), info.title, info.link))

    # Get release year for this anime from AniList
    _ep_release_year: int | None = None
    try:
        from bot.core.text_utils import TextEditor as _TE2
        _ai2 = _TE2(anime_name)
        await _ai2.load_anilist()
        _ep_release_year = (_ai2.adata.get("startDate") or {}).get("year")
    except Exception:
        pass

    # Nyaa RSS — additional search
    nyaa_ep = await _search_nyaa(search_names, season=season, episode=episode)
    for title, url in nyaa_ep:
        ep_candidates.append((_torrent_priority(title), title, url))

    # Nyaa HTML — for older episodes not in RSS — year-filtered
    for n in search_names[:2]:
        q = _sanitize_nyaa_query(n)
        html_ep = await _search_nyaa_html(q, season=season, release_year=_ep_release_year)
        import re as _re_ep2
        for title, url in html_ep:
            tl = title.lower()
            ep_pats = [f"e{episode:02d}", f"- {episode:02d} ", f"- {episode:02d}[",
                       f"[{episode:02d}]", f" {episode:02d} "]
            if any(p in tl for p in ep_pats):
                ep_candidates.append((_torrent_priority(title), title, url))

    ep_candidates.sort(key=lambda x: x[0])
    for _, t_title, t_url in ep_candidates[:3]:  # queue top 3 best matches
        bot_loop.create_task(get_animes(t_title, t_url, force=True))
        triggered += 1

    if triggered:
        await editMessage(
            status_msg,
            f"<b>✅ Found & queued {triggered} torrent(s) for:</b>\n"
            f"<code>{anime_name}</code> S{season:02d}E{episode:02d}\n\n"
            f"<i>Upload will start shortly.</i>"
        )
    else:
        await editMessage(
            status_msg,
            f"<b>⚠️ Episode not found in RSS:</b>\n"
            f"<code>{anime_name}</code> S{season:02d}E{episode:02d}\n\n"
            f"<b>Tip:</b> Paste a Nyaa.si URL or magnet link directly to force-upload:\n"
            f"<i>e.g. https://nyaa.si/view/1549326</i>"
        )


async def _get_search_names(anime_name: str) -> list[str]:
    """
    Return Romaji and English name variants only.
    Nyaa.si only indexes in Romaji/English so native scripts are excluded.
    Order: english -> romaji -> stored name (if ASCII).
    English first since it's cleaner — romaji for Oshi no Ko has brackets.
    """
    import re as _re_sn
    seen: set = set()
    ordered: list = []

    def _is_ascii_name(s: str) -> bool:
        if not s:
            return False
        return sum(1 for c in s if ord(c) < 128) / len(s) > 0.8

    def _clean(s: str) -> str:
        """Strip brackets/special chars that break Nyaa searches."""
        s = _re_sn.sub(r'[【】「」『』〔〕［］\[\]\(\)]', ' ', s)
        return _re_sn.sub(r'\s+', ' ', s).strip()

    def _add(val: str):
        v = _clean(val).lower()
        if v and v not in seen:
            seen.add(v)
            ordered.append(v)

    try:
        aniInfo = TextEditor(anime_name)
        await aniInfo.load_anilist()
        titles = aniInfo.adata.get("title", {})
        # English first — cleaner, no brackets
        english = titles.get("english")
        if english and _is_ascii_name(english):
            _add(english)
        romaji = titles.get("romaji")
        if romaji and _is_ascii_name(romaji):
            _add(romaji)
        if anime_name and _is_ascii_name(anime_name):
            _add(anime_name)
        if not ordered and anime_name:
            _add(anime_name)
    except Exception:
        if anime_name and anime_name.strip():
            _add(anime_name)
    return ordered


def _sanitize_nyaa_query(name: str) -> str:
    """
    Strip characters that break Nyaa URL queries (brackets, apostrophes, colons, etc.)
    and collapse extra spaces. Returns a '+'-joined query string.
    """
    import re as _re
    # Strip Japanese/square brackets AND special chars
    cleaned = _re.sub(r'[\u3010\u3011\u300c\u300d\u300e\u300f\u3014\u3015\uff3b\uff3d\[\]\(\)]', ' ', name)
    cleaned = _re.sub(r"['\"\:\!\?\&]", "", cleaned)
    cleaned = _re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned.replace(' ', '+')


async def _search_nyaa(search_names: list, season: int | None = None, episode: int | None = None) -> list:
    """
    Search Nyaa.si RSS using feedparser (same library used for SubsPlease/Erai RSS).
    feedparser handles HTTP, redirects, encoding and User-Agent automatically —
    avoids the silent aiohttp connection failures that caused empty results.
    Returns list of (title, torrent_url) tuples.
    """
    from bot.core.func_utils import sync_to_async
    from feedparser import parse as feedparse
    import re

    results = []
    seen_urls: set = set()

    def _is_ascii_enough(s: str) -> bool:
        try:
            s.encode('ascii')
            return True
        except UnicodeEncodeError:
            return sum(1 for c in s if ord(c) < 128) / max(len(s), 1) > 0.8

    def _matches_episode(title_lower: str, ep: int) -> bool:
        ep_pats = [
            f"e{ep:02d}", f"- {ep:02d} ", f"- {ep:02d}[",
            f"[{ep:02d}]", f" {ep:02d} ", f"({ep:02d})",
        ]
        if any(p in title_lower for p in ep_pats):
            return True
        range_match = re.search(r'(?<!\d)(\d{1,3})\s*[-~]\s*(\d{1,3})(?!\d)', title_lower)
        if range_match:
            start, end = int(range_match.group(1)), int(range_match.group(2))
            if end > start and start <= ep <= end:
                return True
        if any(k in title_lower for k in ["batch", "complete series", "complete season",
                                           "bdrip", "bd rip", "bluray", "blu-ray"]):
            return True
        return False

    def _broad_match(title_lower: str) -> bool:
        if any(n in title_lower for n in search_names if n):
            return True
        for name in ascii_names:
            words = [w for w in name.split() if len(w) > 3]
            if len(words) >= 2 and sum(1 for w in words if w in title_lower) >= min(2, len(words)):
                return True
        return False

    # Preserve romaji-first order from _get_search_names
    ascii_names = [n for n in search_names if n and _is_ascii_enough(n)]
    if not ascii_names:
        ascii_names = [min((n for n in search_names if n), key=len, default="")]

    # Re-sort: Romaji first, then English, then others
    # _get_search_names puts romaji first but set() loses order — restore it
    def _query_priority(name: str) -> int:
        """Lower = query first. Romaji=0, English=1, others=2"""
        n = name.lower()
        # Romaji: typically contains Japanese romanization patterns
        # English: usually shorter and contains common English words
        # Simple heuristic: if it matches the stored anime_name exactly → romaji
        # We just use position in search_names as a proxy (romaji added first)
        try:
            idx = search_names.index(name)
            return idx  # preserves _get_search_names order (romaji=0)
        except ValueError:
            return 99

    ascii_names = sorted(ascii_names, key=_query_priority)

    # Build queries: full name + short fallback (first 2 significant words)
    queries_seen: set = set()
    queries: list = []
    for name in ascii_names:
        q = _sanitize_nyaa_query(name)
        if q and q not in queries_seen:
            queries_seen.add(q)
            queries.append(q)
        words = [w for w in name.split() if len(w) > 2]
        if len(words) >= 2:
            short = _sanitize_nyaa_query(" ".join(words[:2]))
            if short and short not in queries_seen:
                queries_seen.add(short)
                queries.append(short)

    skip_kw = ["vol.", "volume", "巻", " manga", "novel", "ost", "scan", " ch.", "comic"]

    # Only search English-translated category (c=1_2) — filters non-English at server level
    search_urls = []
    for q in queries:
        search_urls.append(f"https://nyaa.si/?f=0&c=1_2&q={q}&page=rss")

    for url in search_urls:
        try:
            feed = await sync_to_async(feedparse, url)
            if not feed or not feed.entries:
                continue

            for entry in feed.entries:
                title = getattr(entry, 'title', None)
                if not title:
                    continue

                # Get torrent URL: prefer enclosure (.torrent file), fall back to view link
                torrent_url = None
                for enc in getattr(entry, 'enclosures', []):
                    href = enc.get('url', '') or enc.get('href', '')
                    if href:
                        torrent_url = href
                        break
                if not torrent_url:
                    # Fall back to direct view link (auto_animes resolves it)
                    torrent_url = getattr(entry, 'link', None)
                if not torrent_url or torrent_url in seen_urls:
                    continue
                seen_urls.add(torrent_url)

                title_lower = title.lower()
                if not _broad_match(title_lower):
                    continue
                if any(k in title_lower for k in skip_kw):
                    continue
                if _is_non_english(title):
                    continue
                if season:
                    has_season_tag = (
                        f"s{season:02d}" in title_lower or
                        f"s{season}" in title_lower or
                        f"season {season}" in title_lower
                    )
                    import re as _re_bd_sn
                    is_batch_untagged = any(k in title_lower for k in [
                        "batch", "complete", "bdrip", "bd rip", "bluray", "blu-ray"
                    ]) or bool(_re_bd_sn.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', title_lower))
                    # Check if title mentions a DIFFERENT season
                    _other_season = any(
                        f"s{sx:02d}" in title_lower or f"s{sx}" in title_lower
                        or f"season {sx}" in title_lower
                        or f"{sx}nd season" in title_lower or f"{sx}rd season" in title_lower
                        or f"{sx}th season" in title_lower
                        for sx in range(1, 10) if sx != season
                    )
                    if season == 1:
                        # S1: allow tagged S1 OR untagged batch — but NOT if it mentions another season
                        if _other_season:
                            continue
                        if not has_season_tag and not is_batch_untagged:
                            continue
                    else:
                        if not has_season_tag:
                            continue
                if episode and not _matches_episode(title_lower, episode):
                    continue

                results.append((title, torrent_url))

        except Exception as e:
            await rep.report(f"Nyaa RSS fetch failed for {url[:80]}: {e}", "warning", log=False)
            continue

    return results


async def _search_nyaa_rss_season(search_names: list, season: int, release_year: int | None = None) -> list:
    """
    Find ALL episodes of a specific season using targeted Nyaa RSS queries.
    If release_year is provided, only includes entries published in that year
    (or release_year+1 to catch late releases / cross-year seasons).
    """
    from bot.core.func_utils import sync_to_async
    from feedparser import parse as feedparse

    results = []
    seen_urls: set = set()

    # Build season-specific queries
    _ordinals = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th", 5: "5th", 6: "6th"}
    _ord = _ordinals.get(season, f"{season}th")
    queries = []
    for n in search_names[:2]:
        q_base = _sanitize_nyaa_query(n)
        queries.append(f"{q_base}+s{season:02d}")
        queries.append(f"{q_base}+s{season}")
        queries.append(f"{q_base}+season+{season}")
        if season > 1:
            # Also search "2nd Season" format — used by many Nyaa uploaders
            queries.append(f"{q_base}+{_ord.replace(' ', '+')}+season")
            queries.append(f"{q_base}+{season}nd+season" if season == 2
                           else f"{q_base}+{season}rd+season" if season == 3
                           else f"{q_base}+{season}th+season")
        if season == 1:
            queries.append(q_base)
    queries = list(dict.fromkeys(queries))

    def _fetch_rss(url):
        try:
            return feedparse(url)
        except Exception:
            return None

    # Accept entries from release_year up to current year
    # Handles shows like Fate/strange Fake where EP01 aired Dec 2024
    # but EP02+ aired Jan 2026 — a 2-year span
    import datetime as _dt_yr
    _current_year = _dt_yr.datetime.utcnow().year
    _valid_years = set(range(release_year, _current_year + 1)) if release_year else set()

    for q in queries:
        # Only English-translated (c=1_2)
        if True:
            url = f"https://nyaa.si/?page=rss&f=0&c=1_2&q={q}"
            try:
                feed = await sync_to_async(_fetch_rss, url)
                if not feed or not feed.entries:
                    continue
                for entry in feed.entries:
                    title = getattr(entry, 'title', None)
                    if not title:
                        continue

                    # ── Year filter ───────────────────────────────────────────
                    if _valid_years:
                        pub = getattr(entry, 'published_parsed', None)
                        if pub and pub.tm_year not in _valid_years:
                            continue

                    torrent_url = None
                    for enc in getattr(entry, 'enclosures', []):
                        href = enc.get('url', '') or enc.get('href', '')
                        if href:
                            torrent_url = href
                            break
                    if not torrent_url:
                        torrent_url = getattr(entry, 'link', None)
                    if not torrent_url or torrent_url in seen_urls:
                        continue
                    tl = title.lower()
                    has_tag = (f"s{season:02d}" in tl or f"s{season}e" in tl
                               or f"season {season}" in tl)
                    is_batch = any(k in tl for k in ["batch", "complete", "bdrip", "bluray"])
                    is_video = any(k in tl for k in [
                        "1080p", "720p", "480p", ".mkv", ".mp4",
                        "hevc", "x264", "x265", "web-dl", "webrip", "avc"
                    ])
                    other_s = any(
                        f"s{sx:02d}" in tl or f"s{sx}e" in tl or f"season {sx}" in tl
                        for sx in range(1, 10) if sx != season
                    )
                    if other_s:
                        continue
                    if not has_tag and not is_batch and not is_video:
                        continue
                    seen_urls.add(torrent_url)
                    results.append((title, torrent_url))
            except Exception:
                continue

    return results


async def _search_nyaa_html(query: str, season: int | None = None, max_pages: int = 3, release_year: int | None = None) -> list:
    """
    Scrape Nyaa.si HTML search results — finds OLD torrents not in RSS.
    Fetches up to max_pages pages (75 results each).
    release_year is accepted for API compatibility but not appended to the query
    (Nyaa titles don't contain years; filtering is done post-fetch via season tags).
    """
    from bot.core.func_utils import sync_to_async
    import re as _re

    # Append release year to query for tighter Nyaa results.
    # If the anime started in a prior year but is still airing (spans to
    # current year), use current year so recent episodes aren't filtered out.
    # release_year is intentionally NOT appended to the query.
    # Nyaa torrent titles don't contain years, so appending it returns 0 results.
    # Year-based filtering is handled post-fetch by the season tag checks above.

    results = []
    seen_urls: set = set()

    def _fetch_html(url: str) -> str:
        import urllib.request
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            return f"__ERROR__:{e}"

    # Only English-translated (c=1_2) — non-English filtered at server level
    for page in range(1, max_pages + 1):
        url = f"https://nyaa.si/?f=0&c=1_2&q={query}&p={page}"
        try:
            html = await sync_to_async(_fetch_html, url)
            if not html:
                break
            if isinstance(html, str) and html.startswith("__ERROR__:"):
                await rep.report(f"Nyaa HTML fetch error for {url[:60]}: {html[10:]}", "warning", log=False)
                break

            rows = (
                _re.findall(r'href="/view/(\d+)"[^>]*>([^<]{5,200})</a>', html) or
                _re.findall(r'/view/(\d+)[^>]*title="([^"]{5,200})"', html) or
                []
            )

            # If no rows found on this page, stop paginating this category
            if not rows:
                break

            new_on_page = 0
            for nyaa_id, title in rows:
                title = title.strip()
                if not title:
                    continue
                torrent_url = f"https://nyaa.si/download/{nyaa_id}.torrent"
                if torrent_url in seen_urls:
                    continue
                seen_urls.add(torrent_url)
                new_on_page += 1

                tl = title.lower()
                if season:
                    has_tag = (f"s{season:02d}" in tl or f"s{season}e" in tl
                               or f"season {season}" in tl)
                    is_batch = any(k in tl for k in ["batch", "bdrip", "bluray", "complete"])
                    is_video = any(k in tl for k in ["1080p", "720p", "480p", ".mkv", ".mp4", "hevc", "avc", "h.264", "h.265", "x264", "x265", "web-dl", "webrip", "blu-ray"])
                    other_s = any(
                        f"s{sx:02d}" in tl or f"s{sx}e" in tl or f"season {sx}" in tl
                        for sx in range(1, 10) if sx != season
                    )
                    if other_s:
                        continue  # mentions a different season — skip
                    if _is_non_english(title):
                        continue
                    if season == 1:
                        # Accept: explicitly tagged S01, batch, OR any video file
                        # (untagged S1 episodes like "Dead Account - 01 1080p")
                        if not has_tag and not is_batch and not is_video:
                            continue
                    else:
                        # S2+: accept tagged or untagged video files
                        if not has_tag and not is_batch and not is_video:
                            continue

                results.append((title, torrent_url))

            # If fewer results than a full page, no more pages exist
            if new_on_page < 70:
                break

        except Exception as e:
            await rep.report(f"Nyaa HTML parse error (page {page}): {e}", "warning", log=False)
            break

    return results


def _torrent_priority(title: str) -> int:
    """
    Lower score = higher priority.

    Priority order for batch selection:
      1. Dual/Multi audio  (strong -30 bonus — always preferred over Sub)
      2. BD/BDRip source   (-100 bonus)
      3. Group tier:
           Ember = 0
           Anime Time = 5   ← BD dual specialist, preferred over SubsPlease
           SubsPlease = 10
           Erai-Raws = 20
           notkaleido = 25
           Other known (NC-Raws, LostYears, etc.) = 30
           Judas/MTBB/Yameii = 35
           Unknown = 60
      4. Batch bonus: single episode = +50
      5. Quality: 1080p=0, 720p=1, 480p=2

    Example scores (lower = better):
      [Anime Time] BD Dual 1080p batch : -100 + 5 - 30 + 0 + 0 = -125  ← wins
      [Ember] BD Sub 1080p batch       : -100 + 0 +  0 + 0 + 0 = -100
      [SubsPlease] web Sub single      :    0 +10 +  0 +50 + 0 =  +60
    """
    import re as _re_pr
    t = title.lower()

    # ── Source tier: BD beats web ─────────────────────────────────────────
    is_bd = any(k in t for k in ["bdrip", "bd rip", "bluray", "blu-ray", "bd box", "bdremux"]) or             bool(_re_pr.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', t))
    source_score = -100 if is_bd else 0

    # ── Audio — checked before group so Dual always beats Sub of same group ─
    is_dual = any(k in t for k in ["dual audio", "dual-audio", "dualaudio", "multi-audio",
        "multi-subs", "multi subs", "dual aac", "dual dts", "dual flac",
        "jpn-eng", "eng-jpn", "eng+jpn", "2 audio"])
    audio_score = -30 if is_dual else 0

    # ── Group tier ────────────────────────────────────────────────────────
    if "[ember]" in t or "ember_encodes" in t or "ember encodes" in t:
        group_score = 0
    elif "anime time" in t or "[anime time]" in t:
        # BD Dual Audio specialist — rank just below Ember, above SubsPlease
        group_score = 5
    elif "[subsplease]" in t or "subsplease" in t:
        group_score = 10
    elif "[erai-raws]" in t or "erai-raws" in t or "[erai]" in t:
        group_score = 20
    elif "[notkaleido" in t or "notkaleido" in t or "[kaleido-mini]" in t or "kaleido-mini" in t:
        group_score = 25
    elif any(g in t for g in [
        "nc-raws", "nc raws", "ncraws",
        "lostyears", "lost years", "loststar",
        "bonkai77", "bonkai",
        "varyg", "refined", "pizzasubs",
        "exiled-destiny", "e-d",
    ]):
        group_score = 30
    elif any(g in t for g in [
        "[yameii]", "yameii",
        "[judas]", "judas",
        "[mtbb]", "mtbb",
        "[shaddrag]", "shaddrag",
        "[cerberus]", "cerberus",
    ]):
        group_score = 35
    else:
        group_score = 60

    # ── Batch/single bonus ────────────────────────────────────────────────
    is_batch = is_bd or any(k in t for k in [
        "batch", "complete", "bd rip", "bd box", "season pack",
        "complete series", "complete season",
    ])
    batch_score = 0 if is_batch else 50

    # ── Quality ───────────────────────────────────────────────────────────
    if "1080" in t:   qual_score = 0
    elif "720" in t:  qual_score = 1
    elif "480" in t:  qual_score = 2
    else:             qual_score = 3

    return source_score + group_score + audio_score + batch_score + qual_score


def _movie_torrent_priority(title: str) -> int:
    """
    Lower score = higher priority for movies.
    """
    t = title.lower()
    if "[ember]" in t or "ember_encodes" in t:
        group_score = 0
    elif "[subsplease]" in t:
        group_score = 10
    elif "[erai-raws]" in t or "erai-raws" in t:
        group_score = 20
    elif "[judas]" in t or "[yameii]" in t:
        group_score = 30
    else:
        group_score = 60

    is_dual = any(k in t for k in ["dual audio", "dual-audio", "dualaudio", "multi-audio"])
    audio_score = 0 if is_dual else 40

    is_bd = any(k in t for k in ["bdrip", "bd rip", "bluray", "blu-ray", "bdremux"])
    bd_score = 0 if is_bd else 20

    if "1080" in t:   qual_score = 0
    elif "720" in t:  qual_score = 1
    elif "480" in t:  qual_score = 2
    else:             qual_score = 3

    return group_score + audio_score + bd_score + qual_score


async def _search_movie_torrent(search_names: list[str], movie_title: str | None = None) -> list[tuple]:
    """
    Search Nyaa for a specific movie torrent using HTML (full archive) + RSS.
    Returns [(priority, title, url)] sorted by _movie_torrent_priority (lower = better).
    movie_title: if provided, used as the primary search term.
    """
    movie_kw = ["movie", "film", "gekijouban", "theatrical", "(movie)", "(film)",
                "bdrip", "bd rip", "bluray", "hdrip"]

    terms = []
    if movie_title:
        terms.append(_sanitize_nyaa_query(movie_title))
    for n in search_names:
        terms.append(_sanitize_nyaa_query(n))

    results = []
    seen_urls: set = set()

    from feedparser import parse as feedparse
    from bot.core.func_utils import sync_to_async

    def _valid_movie(tl):
        if not any(n in tl for n in search_names):
            if not (movie_title and movie_title.lower() in tl):
                return False
        if not any(k in tl for k in movie_kw + ["1080p", "720p", "480p", ".mkv", ".mp4"]):
            return False
        if any(k in tl for k in ["manga", "novel", "ost", "scan", "comic"]):
            return False
        return True

    # ── HTML search (full archive — finds old movie batches) ──────────────
    for term in terms[:3]:
        html_res = await _search_nyaa_html(term, season=None)
        for t_title, t_url in html_res:
            if t_url in seen_urls:
                continue
            tl = t_title.lower()
            if not _valid_movie(tl):
                continue
            seen_urls.add(t_url)
            results.append((_movie_torrent_priority(t_title), t_title, t_url))

    # ── RSS search (recent releases) ──────────────────────────────────────
    for term in terms[:3]:
        # Only English-translated (c=1_2)
        if True:
            url = f"https://nyaa.si/?page=rss&f=0&c=1_2&q={term}"
            try:
                feed = await sync_to_async(feedparse, url)
                for entry in (feed.entries if feed else []):
                    t_title = getattr(entry, "title", None)
                    t_url   = None
                    for enc in getattr(entry, "enclosures", []):
                        t_url = enc.get("url") or enc.get("href")
                        if t_url: break
                    if not t_url:
                        t_url = getattr(entry, "link", None)
                    if not t_title or not t_url or t_url in seen_urls:
                        continue
                    tl = t_title.lower()
                    if not _valid_movie(tl):
                        continue
                    seen_urls.add(t_url)
                    results.append((_movie_torrent_priority(t_title), t_title, t_url))
            except Exception:
                continue

    results.sort(key=lambda x: x[0])
    return results


async def _queue_upload(query, anime_name: str, season: int | None, episode: int | None, mode: str, is_batch: bool = False):
    """
    Queue upload with smart priority.
    mode="all"    -> queue all seasons S1..SN in order (batch preferred per season)
    mode="season" -> queue one specific season
    mode="episode"-> queue one specific episode
    """
    async def _edit(text, reply_markup=None):
        """Edit message regardless of whether query is a CallbackQuery or Message."""
        try:
            if hasattr(query, "edit_message_text"):
                await query.edit_message_text(text, reply_markup=reply_markup,
                                               disable_web_page_preview=True)
            elif hasattr(query, "edit_text"):
                await query.edit_text(text, reply_markup=reply_markup,
                                      disable_web_page_preview=True)
        except Exception:
            pass
    import re as _re
    import datetime as _dt
    search_names = await _get_search_names(anime_name)
    await rep.report(
        f"\U0001f50d Searching for: {anime_name} (mode={mode})\nVariants (romaji-first): {', '.join(search_names)}", "info"
    )

    # ── Load AniList data once — reused for release year, status, mal_id, episode count ──
    # Avoids 3 separate AniList HTTP calls for the same anime across the season loop.
    _release_year:   int | None = None
    _anilist_status: str        = ""
    _anilist_total:  int        = 0
    _mal_id_cached:  int | None = None
    try:
        from bot.core.text_utils import TextEditor as _TE
        _ai = _TE(anime_name)
        await _ai.load_anilist()
        _release_year   = (_ai.adata.get("startDate") or {}).get("year")
        _anilist_status = _ai.adata.get("status", "")
        _anilist_total  = _ai.adata.get("episodes") or 0
        _mal_id_cached  = _ai.adata.get("idMal")
        if _release_year:
            await rep.report(f"📅 Release year: {_release_year}", "info", log=False)

        # ── Override is_batch if anime is still airing ────────────────────
        # If db_type="completed" was set by mistake for a still-airing anime,
        # force individual episode mode so batch torrents are never used.
        if is_batch and _anilist_status == "RELEASING":
            is_batch = False
            await rep.report(
                f"📡 {anime_name} is RELEASING on AniList — switching to episode mode (ignoring db_type=completed)",
                "info", log=False
            )
    except Exception:
        pass

    triggered = 0

    # ── mode="all": queue each season S1 -> SN in order ───────────────────
    if mode == "all":
        seasons, is_exact, is_continuous = await _get_seasons(anime_name)
        # For ongoing (non-batch) single-season anime, only search S01
        # is_exact=False + not is_continuous = Jikan returned 1 season (no sequel chain)
        # No point searching S02-S30 for anime like Fate/Strange Fake, Dead Account etc.
        # Cap seasons_to_queue:
        # - Jikan exact chain (is_exact=True)  → use exact count from MAL
        # - Ongoing single-season (is_exact=False, not continuous) → S01 only
        # - Continuous series (One Piece etc.) → S01 only (no real seasons)
        # - Batch completed multi-season → use full seasons list
        if not is_batch and not is_exact and not is_continuous:
            seasons_to_queue = [1]
        elif is_exact:
            seasons_to_queue = seasons  # e.g. [1, 2, 3, 4] from Jikan
        else:
            seasons_to_queue = [1]      # continuous or unknown → S01 only
        _max_season = max(seasons_to_queue) if seasons_to_queue else 1

        # Check for a combined all-seasons batch first — e.g. "Complete Series" torrent.
        # Run regardless of db_type: if a complete-series torrent exists, always prefer it
        # over season-by-season processing. is_batch=True forces the batch pipeline.
        combined = []
        await _UPLOAD_SEARCH_SEM.acquire()
        if _anilist_status != "RELEASING":
            # Only search for complete-series packs for finished anime —
            # an airing anime won't have a complete collection yet.
            nyaa_all = await _search_nyaa(search_names, season=None, episode=None)
            for n in search_names[:2]:
                q = _sanitize_nyaa_query(n)
                html_all = await _search_nyaa_html(q, season=None)
                nyaa_all.extend(html_all)
            combined = [(p, t, u) for p, t, u in [((_torrent_priority(t)), t, u) for t, u in nyaa_all]
                        if any(k in t.lower() for k in ["complete series", "complete collection", "all seasons"])
                        and not any(f"season {s}" in t.lower() for s in range(1, 10))]
        if combined:
            combined.sort(key=lambda x: x[0])
            _, title, url = combined[0]
            _comb_alts = [u for _, _, u in combined[1:3]]
            bot_loop.create_task(get_animes(title, url, force=True, is_batch=True,
                                            alt_torrents=_comb_alts))
            triggered += 1
            await rep.report(f"\u2705 Combined batch queued for {anime_name}: {title}", "info", log=False)
        else:
            # Search season by season: S1, S2 only upfront (S3+ downloaded lazily
            # while the previous season is being processed, 1 season at a time).
            # _seasons_ready collects (season_name, folder) for sequential chaining.
            _seasons_ready: list = []
            _UPFRONT_SEASONS = 2   # download S1 + S2 now; rest are lazy
            for s in seasons_to_queue[:_UPFRONT_SEASONS]:
                s_cands = []
                # ── Resolve per-season MAL ID and start year BEFORE any searches ──
                # Previously the Jikan call was inside the individual-episode else
                # branch, so broad searches ran with S1's year (e.g. 2023) even
                # for S3 which started in 2025 — filtering out all S3 torrents from
                # the HTML search. Fix: do it here so _season_release_year is
                # correct before _search_nyaa_rss_season and _search_nyaa_html.
                _season_release_year = _release_year  # fallback to S1 year
                _season_mal_id_early = None
                _season_total_eps_early = 0
                try:
                    if _mal_id_cached:
                        _chain_early = await _get_sequel_chain(int(_mal_id_cached))
                        # If the chain is shorter than expected (Jikan API error/rate-limit),
                        # don't fall back to _chain_early[-1] for seasons beyond the chain —
                        # that reuses S1's MAL ID which gives wrong episode counts and status.
                        # Instead leave _season_mal_id_early=None so the not-yet-aired check
                        # and episode cap use safe fallbacks.
                        if s <= len(_chain_early):
                            _season_mal_id_early = _chain_early[s - 1]
                        else:
                            _season_mal_id_early = None  # chain incomplete — skip safely
                        _jikan_early = await _get_aired_episodes_from_jikan(_season_mal_id_early)
                        if _jikan_early:
                            _season_total_eps_early = _jikan_early
                        _s_year_early = _season_start_year_cache.get(_season_mal_id_early)
                        if _s_year_early:
                            _season_release_year = _s_year_early
                            if _s_year_early != _release_year:
                                await rep.report(
                                    f"S{s:02d} release year: {_s_year_early} (S1 was {_release_year})",
                                    "info", log=False
                                )
                except Exception:
                    pass

                # ── Warn if chain was incomplete but still proceed ────────────
                # If Jikan returned fewer seasons than expected, log a warning
                # but still search Nyaa — we just won't have an episode cap.
                # The batch/episode season filters (_batch_season_ok) will still
                # prevent wrong-season torrents from being picked.
                if s > 1 and _season_mal_id_early is None:
                    await rep.report(
                        f"⚠️ S{s:02d} MAL ID unknown (sequel chain incomplete) — searching without episode cap",
                        "warning", log=False
                    )

                # ── Skip seasons that haven't started airing yet ──────────────
                # Jikan status "not yet aired" means the season is announced but
                # no episodes exist — queuing it would pull S(n-1) torrents instead.
                if _season_mal_id_early:
                    _s_status = _jikan_status_cache.get(_season_mal_id_early, "")
                    if _s_status in ("not yet aired", "upcoming"):
                        await rep.report(
                            f"⏭️ S{s:02d} not yet aired (mal_id={_season_mal_id_early}) — skipping",
                            "info", log=False
                        )
                        continue
                    # "currently airing" but 0 episodes have aired yet — also skip.
                    # This happens when MAL lists the season as airing but Jikan's
                    # episode list has nothing with an aired date yet.
                    if _s_status == "currently airing" and _season_total_eps_early == 0:
                        await rep.report(
                            f"⏭️ S{s:02d} currently airing but 0 eps aired yet (mal_id={_season_mal_id_early}) — skipping",
                            "info", log=False
                        )
                        continue

                # Targeted RSS season search — year-filtered for precision
                nyaa_s = await _search_nyaa_rss_season(search_names, season=s, release_year=_season_release_year)
                _rss_seen = set()
                for title, url in nyaa_s:
                    if url not in _rss_seen:
                        s_cands.append((_torrent_priority(title), title, url))
                        _rss_seen.add(url)
                await rep.report(f"S{s:02d} targeted RSS found {len(nyaa_s)} result(s)", "info", log=False)

                # Also run generic RSS for any results the targeted query missed
                nyaa_generic = await _search_nyaa(search_names, season=s, episode=None)
                for title, url in nyaa_generic:
                    if url not in _rss_seen:
                        s_cands.append((_torrent_priority(title), title, url))
                        _rss_seen.add(url)
                await rep.report(f"S{s:02d} generic RSS found {len(nyaa_generic)} additional result(s)", "info", log=False)

                # Nyaa RSS batch-specific search
                _batch_search_names = list(search_names)
                if s > 1:
                    ordinals = {2: "2nd", 3: "3rd", 4: "4th", 5: "5th", 6: "6th"}
                    ord_s = ordinals.get(s, f"{s}th")
                    for n in list(search_names):
                        _batch_search_names.append(f"{n} {ord_s} season")
                        _batch_search_names.append(f"{n} season {s}")
                else:
                    for n in list(search_names):
                        _batch_search_names.append(f"{n} batch")
                        _batch_search_names.append(f"{n} season 1")
                        _batch_search_names.append(f"{n} bdrip")

                nyaa_batch = await _search_nyaa(_batch_search_names, season=s, episode=None)
                import re as _re_bd_q
                for title, url in nyaa_batch:
                    _tl_b = title.lower()
                    _is_bd_b = (
                        any(k in _tl_b for k in ["batch", "complete", "bdrip", "bd rip", "bluray", "blu-ray"]) or
                        bool(_re_bd_q.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', _tl_b)) or
                        (bool(_re_bd_q.search(r'[Ss]eason\s*\d+|[Ss]\d{1,2}[^\d]', title)) and
                         not bool(_re_bd_q.search(r'[-_\s][eE]?(\d{1,4})[\s\[\(\._-]', title)))
                    )
                    if _is_bd_b:
                        s_cands.append((_torrent_priority(title), title, url))
                await rep.report(f"S{s:02d} batch RSS found {len(nyaa_batch)} result(s)", "info", log=False)

                # Nyaa HTML search — year-filtered, multi-page
                # Cap at 3 pages (225 results max) — 10 pages × multiple queries
                # was causing OOM kills on small VPS instances by holding hundreds
                # of full HTML pages in memory simultaneously.
                # Also stop querying once we have enough candidates from RSS already.
                _html_pages = 2
                _html_seen_urls = {u for _, _, u in s_cands}
                _html_queries = []
                _s_ordinals = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th", 5: "5th", 6: "6th"}
                _s_ord = _s_ordinals.get(s, f"{s}th")
                for n in search_names[:2]:
                    _html_queries.append(_sanitize_nyaa_query(n))
                    _html_queries.append(_sanitize_nyaa_query(f"{n} s{s:02d}"))
                    if s > 1:
                        _html_queries.append(_sanitize_nyaa_query(f"{n} {_s_ord} season"))
                        _html_queries.append(_sanitize_nyaa_query(f"{n} season {s}"))
                _html_queries = list(dict.fromkeys(_html_queries))
                for q in _html_queries:
                    # Skip further HTML queries if we already have plenty of candidates
                    if len(s_cands) >= 20:
                        break
                    html_results = await _search_nyaa_html(q, season=s, release_year=_season_release_year, max_pages=_html_pages)
                    for title, url in html_results:
                        if url in _html_seen_urls:
                            continue
                        tl = title.lower()
                        if is_continuous:
                            if any(k in tl for k in ["1080p", "720p", "480p", ".mkv", ".mp4", "bdrip", "batch"]):
                                s_cands.append((_torrent_priority(title), title, url))
                                _html_seen_urls.add(url)
                        else:
                            if any(k in tl for k in ["1080p", "720p", "480p", ".mkv", ".mp4", "batch", "bdrip", "bd rip", "bluray", "blu-ray", "complete"]):
                                s_cands.append((_torrent_priority(title), title, url))
                                _html_seen_urls.add(url)
                await rep.report(f"S{s:02d} HTML found {len(s_cands)} total candidates", "info", log=False)

                if not s_cands:
                    await rep.report(f"⚠️ No torrents found for {anime_name} S{s:02d} — skipping", "info")
                    if not is_exact and is_continuous:
                        break
                    continue

                s_cands.sort(key=lambda x: x[0])

                # Always prefer a batch torrent if one exists — regardless of db_type.
                # is_batch only controls which pipeline processes it, not whether we
                # look for one. A BDRip batch for an "ongoing"-typed channel should
                # still be grabbed as a batch rather than queuing 12 individual episodes.
                import re as _re_bd_s
                def _is_season_batch(title: str) -> bool:
                    tl = title.lower()
                    if any(k in tl for k in ["batch", "complete", "bdrip", "bd rip", "bluray", "blu-ray", "blu ray"]):
                        return True
                    # Catch bare [BD] / (BD) tags — e.g. "[Anime Time] Show (Season 1) [BD][Dual Audio]"
                    if _re_bd_s.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', tl):
                        return True
                    # Catch "Season N" in title without episode number — likely a full season pack
                    # e.g. "Reincarnated as a Sword (Season 1) [1080p]" with no "- 01" pattern
                    _has_ep_num = bool(
                        _re_bd_s.search(r'[-_\s][eE]?(\d{1,4})[\s\[\(\._-]', title) or
                        _re_bd_s.search(r'[Ss]\d{1,2}[Ee]\d{1,3}', title)  # S02E12 format
                    )
                    _has_season = bool(_re_bd_s.search(r'[Ss]eason\s*\d+|[Ss]\d{1,2}[^\d]', title))
                    if _has_season and not _has_ep_num:
                        return True
                    return False
                def _batch_season_ok(title: str) -> bool:
                    """
                    For S1: accept tagged S1 OR untagged (e.g. "Oshi no Ko - 01~11 Batch").
                    For S2+: REQUIRE explicit season tag matching s. Untagged = S1, reject.
                    Always reject if a different season is explicitly mentioned.
                    """
                    import re as _re_sok
                    tl = title.lower()

                    # Check if title explicitly mentions a different season — always reject
                    for sx in range(1, 10):
                        if sx == s:
                            continue
                        pat = (
                            r'(?<![a-z0-9])(s0*' + str(sx) + r'(?:[^0-9]|$)' +
                            r'|season\s*0*' + str(sx) + r'(?:\b|$)' +
                            r'|' + str(sx) + r'(?:st|nd|rd|th)\s*season)'
                        )
                        if _re_sok.search(pat, tl):
                            return False

                    if s == 1:
                        # S1: accept tagged or untagged
                        return True
                    else:
                        # S2+: must have explicit season tag for this season
                        pat_this = (
                            r'(?<![a-z0-9])(s0*' + str(s) + r'(?:[^0-9]|$)' +
                            r'|season\s*0*' + str(s) + r'(?:\b|$)' +
                            r'|' + str(s) + r'(?:st|nd|rd|th)\s*season)'
                        )
                        return bool(_re_sok.search(pat_this, tl))

                def _batch_anime_match(title: str) -> bool:
                    """Verify batch torrent actually belongs to this anime by checking
                    if any search variant appears in the title."""
                    tl = title.lower()
                    return any(n.lower() in tl for n in search_names)

                batch_s = [(p, t, u) for p, t, u in s_cands
                           if _is_season_batch(t) and _batch_season_ok(t) and _batch_anime_match(t)]
                if batch_s:
                    import re as _re_part_sort
                    def _part_num_key(item):
                        m = _re_part_sort.search(r'part\s*(\d+)', item[1].lower())
                        return int(m.group(1)) if m else 1
                    # Sort: primary = torrent priority (lower=better), secondary = part number
                    # ascending so Part 1 is always picked before Part 2 at equal priority.
                    batch_s.sort(key=lambda x: (x[0], _part_num_key(x)))
                    _, title, url = batch_s[0]
                    _bs_alts = [u for _, _, u in batch_s[1:3]]
                    # Force is_batch=True so it routes to _run_batch_pipeline
                    # even if the channel was registered as db_type="ongoing"
                    bot_loop.create_task(get_animes(title, url, force=True, is_batch=True,
                                                    alt_torrents=_bs_alts))
                    triggered += 1
                    await rep.report(f"✅ S{s:02d} batch queued for {anime_name}: {title}", "info", log=False)
                else:
                    # No batch available — queue individual episodes only.
                    # ── Smart resume: check DB for already-uploaded episodes ──
                    # Looks up both batch_ep_links (batch pipeline) and
                    # anime_data (ongoing pipeline) to find what's done,
                    # then only queues the missing episodes starting from
                    # the first gap. Also clears any stale ending post so
                    # the channel is ready to receive new episode posts.
                    _mappings_resume = await db.get_all_anime_channels()
                    _info_resume = next((m for m in _mappings_resume if m['anime_name'] == anime_name), None)
                    _ani_id_resume = _info_resume.get("ani_id") if _info_resume else None

                    _already_uploaded: set = set()
                    if _ani_id_resume:
                        # Check batch_ep_links first (batch pipeline stores per-season data)
                        _ep_data = await batch_db.get_batch_ep_links(_ani_id_resume, season=f"s{s}")
                        if _ep_data:
                            _already_uploaded = set(int(k) for k in _ep_data.keys())
                        # Fall back to anime_data (ongoing pipeline)
                        if not _already_uploaded:
                            _ong_data = await db.getAnime(_ani_id_resume)
                            if _ong_data:
                                _already_uploaded = {int(k) for k in _ong_data.keys() if str(k).isdigit()}

                    def _ep_num(title):
                        tl = title.lower()
                        # S01E01 format (most reliable)
                        m = _re.search(r's\d+\s*e(\d+)', tl)
                        if m: return int(m.group(1))
                        # notKaleido format: "- 01 (S01E01)" — grab the explicit SxxExx
                        m = _re.search(r'\(s\d+e(\d+)\)', tl)
                        if m: return int(m.group(1))
                        # SubsPlease format: "- 10 (1080p)"
                        m = _re.search(r'-\s*(\d{1,4})\s*[\(\[]', tl)
                        if m: return int(m.group(1))
                        # Bracketed: [01]
                        m = _re.search(r'\[(\d{2,4})\]', tl)
                        if m: return int(m.group(1))
                        return None

                    import re as _re_epmap
                    def _ep_season_ok(title: str) -> bool:
                        """
                        Same season-correctness logic as _batch_season_ok but for
                        individual episode torrents.
                        S1: accept tagged S01 or untagged.
                        S2+: must have explicit season tag for s, or no tag at all
                             is only ok if no other season is mentioned.
                        """
                        tl = title.lower()
                        # Reject if a different season is mentioned
                        for sx in range(1, 10):
                            if sx == s:
                                continue
                            pat = (
                                r'(?<![a-z0-9])(s0*' + str(sx) + r'(?:[^0-9]|$)' +
                                r'|season\s*0*' + str(sx) + r'(?:\b|$)' +
                                r'|' + str(sx) + r'(?:st|nd|rd|th)\s*season)'
                            )
                            if _re_epmap.search(pat, tl):
                                return False
                        if s == 1:
                            return True
                        # S2+: accept if explicitly tagged for this season OR no season tag at all
                        # (untagged single episodes are ambiguous but safer to include)
                        pat_this = (
                            r'(?<![a-z0-9])(s0*' + str(s) + r'(?:[^0-9]|$)' +
                            r'|season\s*0*' + str(s) + r'(?:\b|$)' +
                            r'|' + str(s) + r'(?:st|nd|rd|th)\s*season)'
                        )
                        _has_any_season = bool(_re_epmap.search(
                            r'(?<![a-z0-9])s0*[1-9](?:[^0-9]|$)|season\s*0*[1-9]|[1-9](?:st|nd|rd|th)\s*season',
                            tl
                        ))
                        # If it has a season tag it must match s; if no season tag, accept
                        return bool(_re_epmap.search(pat_this, tl)) or not _has_any_season

                    ep_map = {}
                    for prio, title, url in s_cands:
                        if _is_non_english(title):
                            continue
                        if not _ep_season_ok(title):
                            continue
                        ep = _ep_num(title)
                        if ep is None: continue
                        if ep not in ep_map or prio < ep_map[ep][0]:
                            ep_map[ep] = (prio, title, url)

                    # ── Use pre-resolved season data (Jikan called earlier) ───
                    # _season_mal_id_early and _season_total_eps_early were resolved
                    # at the top of the season loop iteration before the searches,
                    # so _season_release_year was already correct for HTML filtering.
                    _total_eps = _season_total_eps_early or _anilist_total or 0
                    if _total_eps:
                        _s_status_log = _jikan_status_cache.get(_season_mal_id_early, "") if _season_mal_id_early else ""
                        await rep.report(
                            f"S{s:02d} episode cap: {_total_eps} aired"
                            + (f" | status: {_s_status_log}" if _s_status_log else "")
                            + (f" (mal_id={_season_mal_id_early})" if _season_mal_id_early else ""),
                            "info", log=False
                        )

                    if _total_eps:
                        # Always cap to confirmed aired count — prevents queuing future/unaired episodes
                        # even when Nyaa already has torrents pre-seeded for them
                        _all_eps = list(range(1, _total_eps + 1))
                    else:
                        # No aired count available — use what Nyaa found
                        _all_eps = sorted(ep_map.keys())

                    # Only queue episodes not yet uploaded
                    _eps_to_queue = sorted(
                        ep for ep in _all_eps
                        if ep not in _already_uploaded
                    )

                    # ── Skip season entirely if fully uploaded ──────────
                    if not _eps_to_queue and _already_uploaded:
                        await rep.report(
                            f"✅ S{s:02d} fully uploaded ({len(_already_uploaded)} ep(s)) — skipping",
                            "info", log=False
                        )
                        continue

                    if _already_uploaded:
                        _last_done = max(_already_uploaded)
                        _next_ep   = min(_eps_to_queue) if _eps_to_queue else None
                        await rep.report(
                            f"📋 S{s:02d} resume: {len(_already_uploaded)} ep(s) done "
                            f"(last: EP{_last_done:02d})"
                            + (f", queuing from EP{_next_ep:02d}" if _next_ep else " — all caught up"),
                            "info", log=False
                        )
                        # Delete stale ending post so channel is ready for new episodes
                        if _info_resume and _eps_to_queue:
                            _ch_id_resume = _info_resume.get("channel_id")
                            if _ch_id_resume:
                                _old_ending = await db.get_ending_post(_ch_id_resume)
                                if _old_ending:
                                    try:
                                        await bot.delete_messages(_ch_id_resume, _old_ending)
                                    except Exception:
                                        pass
                                    await db.delete_ending_post_record(_ch_id_resume)
                    else:
                        await rep.report(
                            f"🆕 S{s:02d} fresh start: queuing {len(_eps_to_queue)} episode(s) from EP01",
                            "info", log=False
                        )

                    # ── Download-all-then-process-once ──────────────────
                    # Download every episode torrent into the shared stable folder
                    # sequentially, then fire run_batch_on_folder exactly ONCE on
                    # the completed folder.  This avoids the "each single-torrent
                    # pipeline re-scans the folder and re-uploads all prior files"
                    # bug that occurred when each episode was queued separately.
                    import re as _re_safe2
                    _safe_ep = _re_safe2.sub(r'[^\w\s-]', ' ', anime_name)
                    _safe_ep = _re_safe2.sub(r'\s+', ' ', _safe_ep).strip().replace(' ', '_')[:50]
                    # Structure: downloads/batch/AnimeName/Season_N/
                    _dl_folder = f"./downloads/batch/{_safe_ep}/Season_{s}"

                    import os as _os_dl
                    _os_dl.makedirs(_dl_folder, exist_ok=True)

                    _downloaded_urls: set = set()
                    _dl_count = 0

                    for i, ep in enumerate(_eps_to_queue):

                        # ── Step 1: use ep_map if available ──────────────
                        if ep in ep_map:
                            _, title, url = ep_map[ep]
                        else:
                            # ── Step 2: targeted search for missing ep ───
                            await rep.report(
                                f"🔍 S{s:02d}E{ep:02d} not in broad search — trying targeted...",
                                "info", log=False
                            )
                            _ep_cands = []
                            _ep_seen: set = set()

                            # Check RSS feeds — skip for old completed anime
                            import datetime as _dt_rss
                            _anime_is_recent = (
                                _anilist_status == "RELEASING" or
                                (_season_release_year and _season_release_year >= _dt_rss.datetime.utcnow().year - 1)
                            )
                            from bot.core.func_utils import getfeed_all as _getfeed_all_cm2
                            for _rss_url in (Var.RSS_ITEMS if _anime_is_recent else []):
                                # One fetch per feed (was 150 fetches per feed)
                                for _info in await _getfeed_all_cm2(_rss_url, max_entries=150):
                                    _tl = _info.title.lower()
                                    if not any(n in _tl for n in search_names):
                                        continue
                                    if _is_non_english(_info.title):
                                        continue
                                    _ep_pats = [
                                        f"s{s:02d}e{ep:02d}", f"- {ep:02d} ",
                                        f"- {ep:02d}[", f"[{ep:02d}]", f"e{ep:02d}"
                                    ]
                                    if not any(p in _tl for p in _ep_pats):
                                        continue
                                    if _info.link not in _ep_seen:
                                        _ep_cands.append((_torrent_priority(_info.title), _info.title, _info.link))
                                        _ep_seen.add(_info.link)

                            # Nyaa HTML targeted search — cap at 2 pages for per-episode fallback
                            # (it's a specific SxxExx query so page 1 will have the result if it exists)
                            _fb_pages = 2
                            for _n in search_names[:2]:
                                _hq = _sanitize_nyaa_query(f"{_n} s{s:02d}e{ep:02d}")
                                for _et, _eu in await _search_nyaa_html(_hq, season=s, release_year=_season_release_year, max_pages=_fb_pages):
                                    if _eu not in _ep_seen and not _is_non_english(_et):
                                        _tl2 = _et.lower()
                                        if not any(n in _tl2 for n in search_names):
                                            continue
                                        if any(p in _tl2 for p in [f"s{s:02d}e{ep:02d}", f"- {ep:02d} ", f"e{ep:02d}"]):
                                            _ep_cands.append((_torrent_priority(_et), _et, _eu))
                                            _ep_seen.add(_eu)

                            if not _ep_cands:
                                await rep.report(
                                    f"⚠️ S{s:02d}E{ep:02d} — no torrent found, skipping",
                                    "warning", log=False
                                )
                                continue
                            _ep_cands.sort(key=lambda x: x[0])
                            _, title, url = _ep_cands[0]

                        if url in _downloaded_urls:
                            continue
                        _downloaded_urls.add(url)

                        # Download directly into the shared folder
                        await rep.report(
                            f"⬇️ S{s:02d}E{ep:02d} ({i+1}/{len(_eps_to_queue)}): downloading {title[:55]}",
                            "info", log=False
                        )
                        try:
                            from bot.core.tordownload import TorDownloader as _TDL
                            _dl = await _TDL(_dl_folder, use_stable_dir=True).download(
                                url, title
                            )
                            if _dl and _os_dl.path.exists(_dl):
                                _dl_count += 1
                                await rep.report(
                                    f"✅ S{s:02d}E{ep:02d} downloaded ({_dl_count}/{len(_eps_to_queue)})",
                                    "info", log=False
                                )
                            else:
                                await rep.report(
                                    f"⚠️ S{s:02d}E{ep:02d} download failed — skipping",
                                    "warning", log=False
                                )
                        except Exception as _dle:
                            await rep.report(
                                f"⚠️ S{s:02d}E{ep:02d} download error: {_dle}",
                                "warning", log=False
                            )

                    # ── All downloads done — fire ONE pipeline run ────────
                    if _dl_count > 0:
                        await rep.report(
                            f"📂 All {_dl_count} episode(s) downloaded for {anime_name} S{s:02d} "
                            f"→ queuing single batch pipeline run on: {_dl_folder}",
                            "info", log=False
                        )
                        # Pass season tag in name so _run_batch_pipeline resolves correct AniList season
                        _season_tagged_name = f"{anime_name} S{s:02d}" if s > 1 else anime_name
                        # Store for sequential chaining (started below, after loop)
                        _seasons_ready.append((_season_tagged_name, _dl_folder))
                        triggered += 1
                    else:
                        await rep.report(
                            f"⚠️ No episodes downloaded for {anime_name} S{s:02d} — nothing to process",
                            "warning", log=False
                        )

                # For continuous series (One Piece, Naruto etc) — only search S1
                # since there are no real seasons, just one long series
                if not is_exact and is_continuous:
                    break

            # ── Build lazy downloaders for remaining seasons (S3+) ────────
            # Each is an async callable that runs the full search+download for
            # one season and returns (sname, folder) or None.
            _pending_downloaders = []
            for _lazy_s in seasons_to_queue[_UPFRONT_SEASONS:]:
                # Capture loop variable by default-arg binding
                def _make_downloader(_s=_lazy_s):
                    async def _download_season_lazy():
                        import re as _re_lz, os as _os_lz
                        _lz_release_year = _release_year
                        _lz_mal_id = None
                        _lz_total_eps = 0
                        try:
                            if _mal_id_cached:
                                _ch_lz = await _get_sequel_chain(int(_mal_id_cached))
                                if len(_ch_lz) >= _s:
                                    _lz_mal_id = _ch_lz[_s - 1]
                                    _lz_release_year = await _get_season_start_year(_lz_mal_id) or _release_year
                                    _lz_total_eps = _aired_eps_cache.get(_lz_mal_id, 0)
                                    _lz_status = _jikan_status_cache.get(_lz_mal_id, "")
                                    if _lz_status in ("not yet aired", "upcoming"):
                                        await rep.report(
                                            f"⏭️ S{_s:02d} not yet aired — skipping lazy download",
                                            "info", log=False
                                        )
                                        return None
                                    if _lz_status == "currently airing" and _lz_total_eps == 0:
                                        await rep.report(
                                            f"⏭️ S{_s:02d} currently airing but 0 eps aired yet — skipping lazy download",
                                            "info", log=False
                                        )
                                        return None
                        except Exception:
                            pass

                        # Search torrents for this season
                        _lz_cands = []
                        _lz_seen: set = set()
                        _lz_rss = await _search_nyaa_rss_season(search_names, season=_s, release_year=_lz_release_year)
                        for _t, _u in _lz_rss:
                            if _u not in _lz_seen:
                                _lz_cands.append((_torrent_priority(_t), _t, _u))
                                _lz_seen.add(_u)
                        _lz_generic = await _search_nyaa(search_names, season=_s, episode=None)
                        for _t, _u in _lz_generic:
                            if _u not in _lz_seen:
                                _lz_cands.append((_torrent_priority(_t), _t, _u))
                                _lz_seen.add(_u)
                        for _n in search_names[:2]:
                            _hq = _sanitize_nyaa_query(f"{_n} season {_s}")
                            for _t, _u in await _search_nyaa_html(_hq, season=_s, release_year=_lz_release_year):
                                if _u not in _lz_seen and not _is_non_english(_t):
                                    _lz_cands.append((_torrent_priority(_t), _t, _u))
                                    _lz_seen.add(_u)

                        await rep.report(
                            f"S{_s:02d} lazy search found {len(_lz_cands)} candidate(s)",
                            "info", log=False
                        )

                        # Check already uploaded
                        _lz_already: set = set()
                        _lz_mappings = await db.get_all_anime_channels()
                        _lz_info = next((m for m in _lz_mappings if m["anime_name"] == anime_name), None)
                        _lz_ani_id = _lz_info.get("ani_id") if _lz_info else None
                        if _lz_ani_id:
                            _lz_ep_data = await batch_db.get_batch_ep_links(_lz_ani_id, season=f"s{_s}")
                            if _lz_ep_data:
                                _lz_already = set(int(k) for k in _lz_ep_data.keys())
                            if not _lz_already:
                                _lz_ong = await db.getAnime(_lz_ani_id)
                                if _lz_ong:
                                    _lz_already = {int(k) for k in _lz_ong.keys() if str(k).isdigit()}

                        if _lz_already and _lz_total_eps and len(_lz_already) >= _lz_total_eps:
                            await rep.report(
                                f"✅ S{_s:02d} already fully uploaded — skipping lazy download",
                                "info", log=False
                            )
                            return None

                        # Build ep_map and download
                        import re as _re_lzep
                        def _lz_ep_num(title):
                            tl = title.lower()
                            for pat in [r's\d+\s*e(\d+)', r'\(s\d+e(\d+)\)', r'-\s*(\d{1,4})\s*[\(\[]', r'\[(\d{2,4})\]']:
                                m = _re_lzep.search(pat, tl)
                                if m:
                                    return int(m.group(1))
                            return None

                        _lz_ep_map = {}
                        for _p, _t, _u in _lz_cands:
                            if _is_non_english(_t):
                                continue
                            _ep = _lz_ep_num(_t)
                            if _ep is None:
                                continue
                            if _ep not in _lz_ep_map or _p < _lz_ep_map[_ep][0]:
                                _lz_ep_map[_ep] = (_p, _t, _u)

                        _lz_all = list(range(1, _lz_total_eps + 1)) if _lz_total_eps else sorted(_lz_ep_map.keys())
                        _lz_queue = sorted(ep for ep in _lz_all if ep not in _lz_already)

                        if not _lz_queue:
                            await rep.report(f"S{_s:02d} nothing to download (lazy)", "info", log=False)
                            return None

                        _safe = _re_lz.sub(r'[^\w\s-]', ' ', anime_name)
                        _safe = _re_lz.sub(r'\s+', ' ', _safe).strip().replace(' ', '_')[:50]
                        # Structure: downloads/batch/AnimeName/Season_N/
                        _lz_folder = f"./downloads/batch/{_safe}/Season_{_s}"
                        _os_lz.makedirs(_lz_folder, exist_ok=True)
                        _lz_dl_count = 0
                        _lz_dl_seen: set = set()

                        for _ep in _lz_queue:
                            if _ep not in _lz_ep_map:
                                continue
                            _, _t, _u = _lz_ep_map[_ep]
                            if _u in _lz_dl_seen:
                                continue
                            _lz_dl_seen.add(_u)
                            try:
                                from bot.core.tordownload import TorDownloader as _LZTDL
                                _dl = await _LZTDL(_lz_folder, use_stable_dir=True).download(_u, _t)
                                if _dl and _os_lz.path.exists(_dl):
                                    _lz_dl_count += 1
                            except Exception as _lze:
                                await rep.report(f"⚠️ S{_s:02d}E{_ep:02d} lazy dl error: {_lze}", "warning", log=False)

                        if _lz_dl_count == 0:
                            await rep.report(f"⚠️ S{_s:02d} lazy: nothing downloaded", "warning", log=False)
                            return None

                        await rep.report(
                            f"📂 S{_s:02d} lazy: {_lz_dl_count} ep(s) downloaded → {_lz_folder}",
                            "info", log=False
                        )
                        _lz_name = f"{anime_name} S{_s:02d}" if _s > 1 else anime_name
                        return (_lz_name, _lz_folder)
                    return _download_season_lazy
                _pending_downloaders.append(_make_downloader())

            # ── Sequential season pipeline (with per-anime lock) ──────────
            # Acquire a lock so a second anime's pipeline waits until this one
            # is 100% done (all seasons posted). Different anime = different lock.
            _anime_lock = _get_anime_lock(anime_name)

            async def _run_locked():
                async with _anime_lock:
                    if _seasons_ready:
                        await _run_seasons_sequentially(
                            _seasons_ready, anime_name,
                            pending_season_downloaders=_pending_downloaders
                        )
                    elif _pending_downloaders:
                        # Nothing upfront downloaded (S1 found nothing) but lazy seasons exist
                        _first = await _pending_downloaders.pop(0)()
                        if _first:
                            await _run_seasons_sequentially(
                                [_first], anime_name,
                                pending_season_downloaders=_pending_downloaders
                            )

                    # ── OVA / Spin-off sequencing ─────────────────────────
                    # After ALL main seasons have finished uploading, scan
                    # each season's MAL entry for related OVAs, Specials,
                    # ONAs, and spin-off series (SEQUEL non-TV + SIDE_STORY).
                    # Each one found on Nyaa is queued so it posts right after
                    # the last main season — same order as AniList Relations.
                    if _mal_id_cached:
                        try:
                            await _queue_spinoffs_after_all_seasons(
                                anime_name,
                                int(_mal_id_cached),
                                search_names,
                            )
                        except Exception as _so_err:
                            await rep.report(
                                f"⚠️ Spin-off queue error for {anime_name}: {_so_err}",
                                "warning", log=False,
                            )

            if _seasons_ready or _pending_downloaders:
                _UPLOAD_SEARCH_SEM.release()
                bot_loop.create_task(_run_locked())
            else:
                _UPLOAD_SEARCH_SEM.release()

        if not triggered:
            await query.edit_message_text(
                f"<b>\u26a0\ufe0f No torrents found for:</b> <code>{anime_name}</code>\n\n"
                f"<b>Searched:</b> RSS + Nyaa.si (S1 onwards)\n"
                f"<b>Variants:</b> <code>{', '.join(search_names)}</code>\n\n"
                f"<i>Try /addmagnet with a direct Nyaa.si URL or magnet link.</i>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Back", callback_data=_cb_safe(anime_name, "acm_info|"))]])
            )
            return

        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Back", callback_data=_cb_safe(anime_name, "acm_info|"))]])
        try:
            await query.edit_message_text(
                f"<b>\u2705 Queued {triggered} torrent(s) for:</b> <code>{anime_name}</code>\n"
                f"<i>Searching S1 \u2192 latest, batch preferred per season.</i>",
                reply_markup=keyboard
            )
        except Exception:
            pass
        return

    # ── mode="season" or single episode ───────────────────────────────────
    used_batch = False
    candidates = []
    _cand_seen: set = set()

    def _add_cand(title, url):
        if url not in _cand_seen and not _is_non_english(title):
            candidates.append((_torrent_priority(title), title, url))
            _cand_seen.add(url)

    if mode == "season" and season:
        for title, url in await _search_nyaa_rss_season(search_names, season, release_year=_release_year):
            _add_cand(title, url)
        await rep.report(f"Season {season} targeted RSS: {len(candidates)} result(s)", "info", log=False)

    # Generic RSS — catches anything targeted query missed
    for title, url in await _search_nyaa(search_names, season, episode):
        _add_cand(title, url)

    # HTML search for old episodes not in RSS — year-filtered
    for n in search_names[:2]:
        q = _sanitize_nyaa_query(n)
        for title, url in await _search_nyaa_html(q, season=season, release_year=_release_year):
            _add_cand(title, url)

    if not candidates:
        await query.edit_message_text(
            f"<b>\u26a0\ufe0f No torrents found for:</b> <code>{anime_name}</code>\n\n"
            f"<b>Searched:</b> Nyaa.si (HTML)\n"
            f"<b>Variants:</b> <code>{', '.join(search_names)}</code>\n\n"
            f"<i>Try /addmagnet with a direct Nyaa.si URL or magnet link.</i>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Back", callback_data=_cb_safe(anime_name, "acm_info|"))]])
        )
        return

    candidates.sort(key=lambda x: x[0])

    if mode == "season":
        import re as _re_bc
        def _is_batch_cand(title: str) -> bool:
            tl = title.lower()
            if any(k in tl for k in ["batch", "complete", "bdrip", "bd rip", "bluray", "blu-ray", "bd box", "season pack"]):
                return True
            if _re_bc.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', tl):
                return True
            if (bool(_re_bc.search(r'[Ss]eason\s*\d+|[Ss]\d{1,2}[^\d]', title)) and
                    not bool(_re_bc.search(r'[-_\s][eE]?(\d{1,4})[\s\[\(\._-]', title))):
                return True
            return False
        def _season_batch_ok_mode(title: str, target_s: int) -> bool:
            """
            Season correctness for mode=season batch selection.
            S1: accept tagged S01 or untagged.
            S2+: must have explicit tag matching target_s. Untagged = S1, reject.
            Always reject if a different season is explicitly mentioned.
            """
            import re as _re_sbo
            tl = title.lower()
            for sx in range(1, 10):
                if sx == target_s: continue
                pat = (r'(?<![a-z0-9])(s0*' + str(sx) + r'(?:[^0-9]|$)' +
                       r'|season\s*0*' + str(sx) + r'(?:\b|$)' +
                       r'|' + str(sx) + r'(?:st|nd|rd|th)\s*season)')
                if _re_sbo.search(pat, tl): return False
            if target_s == 1: return True
            pat_this = (r'(?<![a-z0-9])(s0*' + str(target_s) + r'(?:[^0-9]|$)' +
                        r'|season\s*0*' + str(target_s) + r'(?:\b|$)' +
                        r'|' + str(target_s) + r'(?:st|nd|rd|th)\s*season)')
            return bool(_re_sbo.search(pat_this, tl))

        _target_season = season if season else 1
        batch_candidates = [
            (p, t, u) for p, t, u in candidates
            if _is_batch_cand(t) and _season_batch_ok_mode(t, _target_season)
        ]
        if batch_candidates:
            _bc_sorted = sorted(batch_candidates, key=lambda x: x[0])
            _, title, url = _bc_sorted[0]
            _bc_alts = [u for _, _, u in _bc_sorted[1:3]]
            bot_loop.create_task(get_animes(title, url, force=True, is_batch=is_batch,
                                            alt_torrents=_bc_alts))
            triggered += 1
    else:
        if candidates:
            best = candidates[0]
            _best_alts = [u for _, _, u in candidates[1:3]]
            bot_loop.create_task(get_animes(best[1], best[2], force=True, is_batch=is_batch,
                                            alt_torrents=_best_alts))
            triggered += 1

    if triggered:
        method = "batch torrent" if used_batch else f"{triggered} episode(s)"
        text = (
            f"<b>\u2705 Queued {method} for:</b> <code>{anime_name}</code>\n"
            f"<i>Priority: Batch+Dual \u2192 Batch \u2192 Dual \u2192 Sub singles</i>"
        )
    else:
        text = (
            f"<b>\u26a0\ufe0f Torrents found but no episode numbers detected for:</b> <code>{anime_name}</code>\n\n"
            f"<i>Try /addmagnet with a direct Nyaa.si URL or magnet link.</i>"
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Back", callback_data=_cb_safe(anime_name, "acm_info|"))]])
    try:
        await query.edit_message_text(text, reply_markup=keyboard)
    except Exception:
        pass
    except Exception:
        await rep.report(format_exc(), "error")
        await query.answer("Error queuing upload.", show_alert=True)


# ─────────────────────────────────────────────────────────────────────────────
# /settings → 📺 Channel Management sub-panel.
#
# Owns every callback for the Channel Management section so that
# bot/modules/settings.py stays focused on routing only and does not
# balloon. Replaces the standalone admin commands:
#
#   /connect [channel_id] (alias /connectchannel)
#       → 📺 Channel Management → 🔗 Connect
#   /listconnections
#       → 📺 Channel Management → 📋 List Connections
#   /removeconnection [anime] (alias /removechannel)
#       → 📺 Channel Management → 🗑 Remove Connection
#
# settings.py forwards these to this module:
#
#   s:menu:cmgr             → show_cmgr_menu(client, query)
#   s:cmgr:<action>[:args]  → handle_cmgr_action(client, query, parts,
#                                                pending, safe_edit)
#   pending action == "cmgr_*" text input
#                           → handle_cmgr_input(client, message, state,
#                                               panel, safe_edit, text,
#                                               pending)
# ─────────────────────────────────────────────────────────────────────────────
from pyrogram.errors import MessageNotModified as _CMGR_MNotMod  # alias
from bot.modules.cmds import (
    connect_step1 as _cmgr_connect_step1,
)


CMGR_MENU_CAPTION = (
    "<b>📺 Channel Management</b>\n\n"
    "Link an anime to a channel so the bot posts new episodes there.\n\n"
    "• <b>🔗 Connect</b> — link a new anime to a channel "
    "(asks for the channel ID, then the anime name)\n"
    "• <b>📋 List Connections</b> — show every linked channel "
    "(paginated; tap an anime to manage / upload / remove)\n"
    "• <b>🗑 Remove Connection</b> — unlink an anime by name "
    "(stops new episode posts; episode records, batch links and "
    "Telegram videos stay intact)\n\n"
    "<i>Admin-only. Channel-level operations require the bot to be an "
    "admin in the target channel with post permission.</i>"
)


CMGR_PROMPT_TEXTS = {
    "cmgr_connect": (
        "<b>🔗 Connect a Channel — Step 1 of 2</b>\n\n"
        "<b>Send the numeric channel ID</b> "
        "(without the <code>-100</code> prefix).\n\n"
        "<i>ℹ️ Forward any channel message to @userinfobot to get the ID. "
        "After the bot verifies it can post, you'll be asked for the anime "
        "name.</i>\n\n"
        "Tap <b>Cancel</b> to abort."
    ),
    "cmgr_remove": (
        "<b>🗑 Remove Connection — Step 1 of 2</b>\n\n"
        "<b>Send the anime name</b> to unlink it from its channel.\n\n"
        "<i>This is a soft unlink — the bot just stops posting new "
        "episodes for this anime. Existing episode records, batch links "
        "and Telegram videos are NOT touched, so users can still access "
        "the old episodes. You'll get a preview and a confirmation "
        "button before anything actually happens.</i>\n\n"
        "Tap <b>Cancel</b> to abort."
    ),
}


def _kb_cmgr_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 Connect",          callback_data="s:cmgr:ask:connect"),
            InlineKeyboardButton("📋 List Connections", callback_data="s:cmgr:list"),
        ],
        [InlineKeyboardButton("🗑 Remove Connection",   callback_data="s:cmgr:ask:remove")],
        [
            InlineKeyboardButton("⬅️ Back",  callback_data="s:home"),
            InlineKeyboardButton("❌ Close", callback_data="s:close"),
        ],
    ])


def _kb_cmgr_cancel():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✖️ Cancel", callback_data="s:cmgr:cancel"),
    ]])


def _kb_cmgr_back():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back",  callback_data="s:menu:cmgr"),
        InlineKeyboardButton("🏠 Home",  callback_data="s:home"),
        InlineKeyboardButton("❌ Close", callback_data="s:close"),
    ]])


async def _cmgr_safe_edit(message, caption, reply_markup, safe_edit):
    """Edit panel caption — safe_edit injected from settings.py."""
    try:
        await safe_edit(message, caption, reply_markup)
    except _CMGR_MNotMod:
        pass


async def _cmgr_lookup_channel(name: str) -> dict | None:
    """
    Case-insensitive lookup of an anime_channels record by name.

    There is no `db.get_anime_channel(name)` helper, so we iterate
    `db.get_all_anime_channels()` and match locally. Returns the
    channel dict (same shape as get_all_anime_channels entries) or
    None if no exact (case-insensitive) match is found.
    """
    needle = (name or "").strip().lower()
    if not needle:
        return None
    try:
        for ch in await db.get_all_anime_channels():
            if (ch.get("anime_name") or "").strip().lower() == needle:
                return ch
    except Exception as e:
        await rep.report(f"_cmgr_lookup_channel error: {e}", "error")
    return None


async def show_cmgr_menu(client, query):
    """Render the Channel Management sub-menu (entry from s:menu:cmgr)."""
    # Re-import safely to avoid a circular import at module load time.
    from bot.modules.settings import _safe_edit_caption as _safe
    await _cmgr_safe_edit(query.message, CMGR_MENU_CAPTION, _kb_cmgr_menu(), _safe)
    return await query.answer()


async def handle_cmgr_action(client, query, parts, pending, safe_edit):
    """Dispatch s:cmgr:<action> callbacks.

    parts = ["s", "cmgr", <action>, ...]
    pending = settings._pending dict (in-memory state)
    safe_edit = settings._safe_edit_caption helper
    """
    if len(parts) < 3:
        return await query.answer("Bad action", show_alert=True)
    action = parts[2]

    # ── Cancel a pending input ────────────────────────────────────────────────
    if action == "cancel":
        pending.pop(query.from_user.id, None)
        await _cmgr_safe_edit(query.message, CMGR_MENU_CAPTION, _kb_cmgr_menu(), safe_edit)
        return await query.answer("Cancelled")

    # ── Confirm Unlink (only valid right after the remove preview step) ──────
    #
    # Soft unlink: drops the anime_channels record so the bot stops posting
    # new episodes for this anime, but leaves anime_data, batch_links,
    # batch_ep_links, schedule_posts, ending_posts, seen_torrents AND the
    # local download folder fully intact. Users can still access old
    # episodes via existing post links / Get Episode buttons.
    if action == "unlink":
        state = pending.get(query.from_user.id)
        if (
            not state
            or state.get("action") != "cmgr_remove"
            or state.get("data", {}).get("stage") != "confirm"
        ):
            return await query.answer(
                "Nothing to confirm — open Remove again.", show_alert=True
            )
        anime_name = state["data"].get("anime_name", "").strip()
        pending.pop(query.from_user.id, None)
        if not anime_name:
            return await query.answer("Missing anime name.", show_alert=True)
        await query.answer("Unlinking…")
        try:
            # Soft delete — only removes the channel mapping, no data purge.
            success = await db.remove_anime_channel(anime_name)
            if success:
                body = (
                    f"<b>✅ Unlinked:</b> <code>{anime_name}</code>\n\n"
                    f"<i>Channel disconnected. Bot will stop posting new "
                    f"episodes for this anime. All existing episode records, "
                    f"batch links and Telegram videos are intact — users can "
                    f"still access the old episodes.</i>\n\n"
                    + CMGR_MENU_CAPTION
                )
            else:
                body = (
                    f"<b>❌ Failed to unlink:</b> <code>{anime_name}</code>\n"
                    f"<i>Anime not found in database.</i>\n\n"
                    + CMGR_MENU_CAPTION
                )
            await _cmgr_safe_edit(query.message, body, _kb_cmgr_menu(), safe_edit)
        except Exception as e:
            await rep.report(f"cmgr unlink error: {e}", "error")
            await _cmgr_safe_edit(
                query.message,
                f"<b>❌ Unlink failed</b>\n\n<code>{e}</code>",
                _kb_cmgr_back(),
                safe_edit,
            )
        return

    # ── Ask for input (channel_id / anime name) ──────────────────────────────
    if action == "ask" and len(parts) >= 4:
        kind = parts[3]
        if kind == "connect":
            pending_key = "cmgr_connect"
        elif kind == "remove":
            pending_key = "cmgr_remove"
        else:
            return await query.answer("Unknown action", show_alert=True)

        pending[query.from_user.id] = {
            "action":  pending_key,
            "chat_id": query.message.chat.id,
            "msg_id":  query.message.id,
            "data":    {},
        }
        await _cmgr_safe_edit(
            query.message,
            CMGR_PROMPT_TEXTS[pending_key],
            _kb_cmgr_cancel(),
            safe_edit,
        )
        return await query.answer("Waiting for input")

    # ── List connections (sends a fresh paginated message) ───────────────────
    if action == "list":
        # Tell the user the list is being sent in a separate message — the
        # paginated browser uses its own callbacks (acm_page / acm_info /
        # acm_back / acm_remove / acm_upall / ...) which expect a plain
        # message rather than a photo+caption panel.
        try:
            await list_connections_cmd(client, query.message)
        except Exception as e:
            await rep.report(f"cmgr list error: {e}", "error")
            return await query.answer(f"Error: {e}", show_alert=True)
        await _cmgr_safe_edit(
            query.message,
            CMGR_MENU_CAPTION
            + "\n\n<i>📋 List sent as a separate message above.</i>",
            _kb_cmgr_menu(),
            safe_edit,
        )
        return await query.answer("List sent")

    return await query.answer("Unknown action", show_alert=True)


async def handle_cmgr_input(client, message, state, panel, safe_edit, text, pending):
    """Handle text typed by the admin while a cmgr_* action is pending.

    Forwarded from bot/modules/settings.py settings_input_catcher.
    """
    action = state["action"]
    chat_id = state["chat_id"]

    async def _show(body, kb=None):
        markup = kb if kb is not None else _kb_cmgr_back()
        if panel:
            try:
                await safe_edit(panel, body, markup)
            except _CMGR_MNotMod:
                pass
        else:
            await client.send_message(chat_id, body, reply_markup=markup)

    # ── Connect: validate channel_id, then hand off to the existing
    #     connect_step1 → connect_step2_anime_name → ... pipeline ───────────
    if action == "cmgr_connect":
        # Strip whitespace and any leading "-100" the user may have typed
        raw = text.strip().lstrip("-")
        if raw.startswith("100") and len(raw) > 3 and raw[3:].isdigit():
            raw = raw[3:]
        if not raw.isdigit():
            return await _show(
                f"<b>⚠️ Invalid channel ID</b>\n\n"
                f"<code>{text}</code> is not a numeric channel ID.\n\n"
                f"<i>Send the numeric ID without the <code>-100</code> prefix, "
                f"or tap Back to abort.</i>"
            )
        # Clear panel pending — connect_step1 sets its own DB pending state
        # and from here the user types the anime name which connect_step2
        # picks up directly.
        pending.pop(message.from_user.id, None)
        try:
            await _cmgr_connect_step1(client, message, channel_id_str=raw)
        except Exception as e:
            await rep.report(f"cmgr_connect error: {e}", "error")
            return await _show(
                f"<b>❌ Connect failed</b>\n\n<code>{e}</code>"
            )
        # connect_step1 sends its own status / next-step message in chat;
        # restore the panel to the cmgr menu so the admin can navigate.
        return await _show(CMGR_MENU_CAPTION, _kb_cmgr_menu())

    # ── Remove: 2-step soft unlink (preview → tap Yes, Unlink) ───────────────
    #
    # This is a SOFT unlink, not a wipe. Only the anime_channels mapping is
    # dropped — episode records, batch links, schedule entries, seen
    # torrents and the local download folder are all left untouched, so
    # users keep accessing previously posted episodes via existing post
    # links / Get Episode buttons. Bot just stops auto-posting NEW
    # episodes for this anime.
    #
    # Stage "ask"     → user just typed the anime name. Resolve it against
    #                   the DB; if found, show an unlink preview with
    #                   channel info and a 🔌 Yes, Unlink / ❌ Cancel
    #                   keyboard. Advance the pending state to "confirm".
    # Stage "confirm" → user typed something while waiting for confirmation.
    #                   Do NOT re-resolve or auto-act; just remind them to
    #                   tap a button (typing has no effect at this stage).
    if action == "cmgr_remove":
        stage = (state.get("data") or {}).get("stage", "ask")

        if stage == "confirm":
            pending_anime = (state.get("data") or {}).get("anime_name", "?")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔌 Yes, Unlink", callback_data="s:cmgr:unlink"),
                InlineKeyboardButton("❌ Cancel",      callback_data="s:cmgr:cancel"),
            ]])
            return await _show(
                f"<b>⏳ Awaiting confirmation for:</b> "
                f"<code>{pending_anime}</code>\n\n"
                f"<i>Typing has no effect now — tap 🔌 Yes, Unlink to "
                f"proceed or ❌ Cancel to abort.</i>",
                kb,
            )

        # stage == "ask": resolve the typed name against the DB
        anime_name = text.strip()
        if not anime_name:
            return await _show(
                "<b>⚠️ Empty name</b>\n\n"
                "<i>Send the anime name, or tap Back to abort.</i>"
            )

        record = await _cmgr_lookup_channel(anime_name)
        if not record:
            return await _show(
                f"<b>❌ No connection found for:</b> <code>{anime_name}</code>\n\n"
                f"<i>Check the spelling, or open </i><b>📋 List Connections</b>"
                f"<i> to see every linked anime.</i>"
            )

        # Save resolved canonical name + advance stage to "confirm"
        state["data"] = {
            "stage":      "confirm",
            "anime_name": record["anime_name"],
        }

        ch_title = record.get("channel_title") or "?"
        ch_id    = record.get("channel_id") or "?"
        db_type  = record.get("db_type") or "ongoing"

        preview = (
            f"<b>🔌 Confirm Unlink</b>\n\n"
            f"<b>Anime:</b> <code>{record['anime_name']}</code>\n"
            f"<b>Channel:</b> {ch_title} (<code>{ch_id}</code>)\n"
            f"<b>Type:</b> {db_type}\n\n"
            f"<b>This is a soft unlink — only the channel ↔ anime mapping "
            f"is removed.</b>\n\n"
            f"<b>What will change:</b>\n"
            f"• Bot will <b>stop posting new episodes</b> of this anime to "
            f"the channel\n"
            f"• RSS / auto-fetch for this anime will pause\n"
            f"• Batch pipeline will not process new episodes for it\n\n"
            f"<b>What stays intact (nothing is deleted):</b>\n"
            f"• All episode records — file IDs, post links, message IDs\n"
            f"• Batch links and batch episode entries\n"
            f"• Schedule / ending posts, seen torrents\n"
            f"• Local download folder on disk\n"
            f"• All videos already uploaded to the Telegram channel\n\n"
            f"<i>Users can still access the old episodes via the existing "
            f"Get Episode buttons and deep links. Tap 🔌 Yes, Unlink to "
            f"proceed, or ❌ Cancel to abort.</i>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔌 Yes, Unlink", callback_data="s:cmgr:unlink"),
            InlineKeyboardButton("❌ Cancel",      callback_data="s:cmgr:cancel"),
        ]])
        return await _show(preview, kb)

    # Unknown — drop state to avoid the panel getting stuck
    pending.pop(message.from_user.id, None)
