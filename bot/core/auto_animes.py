"""
auto_animes.py
==============
Public routing layer for the anime processing pipeline.

This module is the single import point used by __main__.py and admin commands.
All implementation lives in bot/core/pipelines/:

    pipelines/filters.py   — task classification (batch? movie? skip?)
    pipelines/helpers.py   — link building, keyboard layout, ending post
    pipelines/workers.py   — asyncio queues + _ongoing_worker + _batch_worker
    pipelines/rss.py       — fetch_animes, fetch_movies, cache warming
    pipelines/ongoing.py   — _run_pipeline  (single RSS episode)
    pipelines/batch.py     — _run_batch_pipeline  (BDRip / season pack)
    pipelines/movie.py     — _run_movie_pipeline  (anime movie)

Key behaviours
--------------
- Every anime ALWAYS gets posted to MAIN_CHANNEL (with download buttons).
- If a dedicated channel is connected, ALSO posts there; MAIN_CHANNEL gets a
  "Watch Now" link instead.
- Persistent task queue: survives restarts, retries on failure (MAX_RETRIES=3).
- RSS feeds sorted by source priority before processing.
- [Batch] torrents skipped unless admin uses /processbatch.
- Quality-level duplicate guard: only re-encodes missing qualities.
"""

import itertools
import re as _re
import time as _time
from traceback import format_exc

from bot import bot, bot_loop, Var, ani_cache
from bot.core.reporter import rep
from bot.core.task_queue import (
    task_queue, batch_task_queue, movie_task_queue, MAX_RETRIES,
)

# ── Pipeline sub-modules ─────────────────────────────────────────────────────
from bot.core.pipelines.filters import (
    _is_batch_task, _is_movie_task, should_skip, dual_audio_allowed,
)
from bot.core.pipelines.workers import (
    _ongoing_queue, _batch_queue, _movie_queue,
    _ongoing_counter, _batch_counter,
    _ongoing_worker, _batch_worker,
)
from bot.core.pipelines.rss import (
    fetch_animes, fetch_movies,
    _warm_cache, _resume_pending_tasks, _resume_movie_tasks,
)
from bot.core.pipelines.helpers import (
    extra_utils, _make_link, _qual_btns_to_keyboard,
    _send_ending_post, _build_ending_keyboard,
)


def _safe_dl_path(base_root: str, *parts: str) -> str:
    """Build a download path and assert it stays under base_root."""
    import os as _os
    root   = _os.path.realpath(base_root)
    joined = _os.path.realpath(_os.path.join(base_root, *parts))
    if not joined.startswith(root + _os.sep) and joined != root:
        raise ValueError(f"Path traversal blocked: {joined!r} escapes {root!r}")
    return joined


async def get_animes(
    name: str,
    torrent: str,
    force: bool = False,
    task_id: str = None,
    source_priority: int = 1,
    quals_done: list = None,
    is_batch: bool = False,
    is_movie: bool = False,
    alt_torrents: list = None,
):
    """
    Router — classifies a torrent and enqueues it to the correct worker.

    Parameters
    ----------
    name            : torrent name as it appears in the RSS feed
    torrent         : magnet link, .torrent URL, or Nyaa page URL
    force           : bypass dedup and quality checks (Upload All)
    task_id         : existing DB task ID (resume path)
    source_priority : 0=SubsPlease, 1=Ember, 2=Erai-raws (lower = first)
    quals_done      : qualities already uploaded (resume)
    is_batch        : treat as a completed/batch release
    is_movie        : treat as an anime movie
    alt_torrents    : fallback torrent URLs if primary download is corrupt
    """
    if quals_done is None:
        quals_done = []

    try:
        # ── Resolve Nyaa page URL to a direct .torrent link ───────────────────
        if torrent and "nyaa.si/view/" in torrent and not torrent.endswith(".torrent"):
            try:
                from aiohttp import ClientSession as _CS
                async with _CS() as sess:
                    async with sess.get(torrent) as r:
                        _html = await r.text()
                _dl_match = _re.search(r'href="(/download/\d+\.torrent)"', _html)
                if _dl_match:
                    torrent = "https://nyaa.si" + _dl_match.group(1)
            except Exception as _e:
                await rep.report(f"Nyaa URL resolve failed (using original): {_e}", "warning", log=False)

        # ── Early skip filter (non-video, non-English) ────────────────────────
        if should_skip(name):
            return

        # ── Dual-audio gate ───────────────────────────────────────────────────
        from bot.core.database import db
        if not await dual_audio_allowed(name, is_batch, is_movie, force, db):
            await rep.report(
                f"⏭ Dual audio single skipped (no dedicated channel): {name[:60]}",
                "info", log=False,
            )
            return

        # ── Dedup for ongoing (non-batch, non-forced) tasks ───────────────────
        # Previously used an in-memory dict (seen_titles_ongoing) that grew to
        # 3000 entries and ballooned VMS. Now goes straight to MongoDB:
        # seen_torrents has a TTL index (48h) so no manual eviction needed.
        if not _is_batch_task(name, is_batch) and not force:
            dedup_key = torrent or name
            if await db.is_torrent_seen(dedup_key):
                return
            if name and name != dedup_key and await db.is_torrent_seen(name):
                return
            # Mark seen immediately so concurrent RSS tasks don't double-queue
            await db.mark_torrent_seen(dedup_key)
            if name and name != dedup_key:
                await db.mark_torrent_seen(name)

        # ── [Batch]-tagged torrent guard ──────────────────────────────────────
        if (not _is_batch_task(name, is_batch)
                and _re.search(r'[\(\[]\s*batch\s*[\)\]]', name, _re.IGNORECASE)
                and not is_batch and not force):
            from bot.core.text_utils import TextEditor, _normalize_anime_title
            _bi = TextEditor(name)
            await _bi.load_anilist()
            _bt = _bi.adata.get("title", {})
            _bnames = [n for n in [
                _bt.get("romaji"), _bt.get("english"), _normalize_anime_title(name),
            ] if n and n.strip()]

            # FIX: was `any(await db.find_channel_by_anime_title(bn) for bn in _bnames)`
            # That creates a plain generator of coroutine objects — any() evaluates
            # them as truthy without awaiting, so it ALWAYS returns True regardless
            # of whether a channel is connected. Fixed by awaiting each call explicitly.
            _is_connected = False
            for bn in _bnames:
                if await db.find_channel_by_anime_title(bn):
                    _is_connected = True
                    break

            if not _is_connected:
                try:
                    await db.save_skipped_batch(name, torrent)
                except Exception:
                    pass
                return
            is_batch = True
            force    = True

        # ── Enqueue to persistent task DB ─────────────────────────────────────
        if _is_movie_task(name, is_movie):
            _tq = movie_task_queue
        elif _is_batch_task(name, is_batch):
            _tq = batch_task_queue
        else:
            _tq = task_queue

        if task_id is None:
            _enq_alts = (alt_torrents or []) if _is_batch_task(name, is_batch) else None
            task_id = await _tq.enqueue(
                name, torrent, source_priority, alt_torrents=_enq_alts
            )
        await _tq.update_task(task_id, is_batch=is_batch, force=force)

        # ── Route to the correct asyncio worker queue ─────────────────────────
        if _is_movie_task(name, is_movie):
            await _movie_queue.put((name, torrent, force, task_id, source_priority))
        elif _is_batch_task(name, is_batch):
            await _batch_queue.put((
                source_priority, next(_batch_counter),
                name, torrent, force, task_id, source_priority, quals_done, is_batch,
            ))
        else:
            await _ongoing_queue.put((
                source_priority, next(_ongoing_counter),
                name, torrent, force, task_id, source_priority, quals_done, is_batch,
            ))

    except Exception:
        await rep.report(format_exc(), "error")
        if task_id:
            if _is_movie_task(name, locals().get("is_movie", False)):
                _err_tq = movie_task_queue
            elif _is_batch_task(name, is_batch):
                _err_tq = batch_task_queue
            else:
                _err_tq = task_queue
            retry  = await _err_tq.increment_retry(task_id)
            status = "pending" if retry < MAX_RETRIES else "failed"
            await _err_tq.update_task(task_id, status=status)


async def run_batch_on_folder(
    anime_name: str,
    folder_path: str,
    source_priority: int = 1,
):
    """
    Process a pre-downloaded folder through the full batch pipeline.

    Called by Upload All (individual episode mode) — channel_manager downloads
    every episode torrent into the shared stable folder, then calls this once
    so the pipeline encodes the complete set of files in a single run.
    """
    task_id = await batch_task_queue.enqueue(anime_name, folder_path, source_priority)
    await _batch_queue.put((
        source_priority, next(_batch_counter),
        anime_name, folder_path, True,
        task_id, source_priority, [], True,
    ))
    await rep.report(
        f"📂 run_batch_on_folder queued: {anime_name} → {folder_path}",
        "info", log=False,
    )


async def process_batch_torrent(name: str, torrent: str, source_priority: int = 1):
    """Called by the /processbatch admin command."""
    from bot.core.reporter import batch_rep
    await batch_rep.report(f"📦 Batch processing triggered:\n\n{name}", "info")
    task_id = await batch_task_queue.enqueue(name, torrent, source_priority)
    bot_loop.create_task(
        get_animes(
            name, torrent, force=True, task_id=task_id,
            source_priority=source_priority, is_batch=True,
        )
    )
