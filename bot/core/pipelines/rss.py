"""
pipelines/rss.py
================
RSS polling loops and startup initialisation for the bot's anime feed.

Functions
---------
fetch_animes        — main RSS loop (60-second poll), runs for the bot's lifetime
fetch_movies        — movie RSS loop (6-hour poll)
_warm_cache         — pre-populates ani_cache from DB on startup
_resume_pending_tasks — resets stuck tasks and re-queues interrupted batch tasks
_resume_movie_tasks — resets stuck movie tasks and re-queues them

Concurrency
-----------
_rss_sem (Semaphore(5)) limits the number of get_animes() coroutines running
at the same time.  Without it, each 60-second RSS tick can spawn up to 75 tasks
per feed which hit AniList + Jikan + MongoDB simultaneously, spiking RAM.
"""

import asyncio
import time as _time

from asyncio import sleep as asleep, Semaphore

from bot import bot_loop, Var, ani_cache  # ani_cache used for fetch_animes flag only
from bot.core.func_utils import getfeed
from bot.core.reporter import rep, movie_rep
from bot.core.task_queue import batch_task_queue, movie_task_queue
from bot.core.database import db
from .constants import RSS_PRIORITY_MAP


# ── RSS scan concurrency gate ─────────────────────────────────────────────────
# Allows at most 5 concurrent get_animes() calls from the RSS scan loop.
_rss_sem: Semaphore = Semaphore(5)


def _rss_priority(feed_url: str) -> int:
    """Return the source priority for a feed URL (lower = processed first)."""
    url_lower = feed_url.lower()
    for keyword, prio in RSS_PRIORITY_MAP.items():
        if keyword in url_lower:
            return prio
    return 2  # unknown source = lowest priority


async def _gated_get_animes(*args, **kwargs):
    """Rate-limiter wrapper around get_animes() for RSS scan tasks."""
    async with _rss_sem:
        # Import here to avoid circular import (rss → auto_animes → rss)
        from bot.core.auto_animes import get_animes
        await get_animes(*args, **kwargs)


async def _warm_cache():
    """
    No-op — episode dedup now goes directly to MongoDB (db.is_episode_done).
    Kept as a stub so callers don't need to change.
    Previously this loaded up to 2000 entries into ani_cache['ongoing'] /
    ani_cache['completed'] which contributed to the VMS balloon.
    """
    await rep.report(
        "✅ Cache warmed with 0 existing episode(s) (0 kept in hot cache)",
        "info", log=False
    )


async def _resume_pending_tasks():
    """
    Startup routine that:
      1. Populates the in-memory dedup cache from DB (prevents re-queuing of
         already-seen torrents on the first RSS scan after a restart).
      2. Resets any tasks left in downloading/encoding/uploading state back to
         pending (they were interrupted by the previous bot process).
      3. Re-queues all batch tasks that were pending or interrupted.
    """
    # Dedup is now DB-only (db.is_torrent_seen) — no in-memory cache to warm.
    # Log how many seen_torrents docs exist for diagnostics.
    try:
        seen_count = await db.db.seen_torrents.count_documents({})
        await rep.report(
            f"🔒 Dedup cache warmed: {seen_count} seen torrent(s)",
            "info", log=False
        )
    except Exception:
        pass

    # Reset stuck tasks in both queues
    await batch_task_queue.reset_stuck_tasks()

    # Re-queue interrupted batch tasks
    resumable = await batch_task_queue.get_resumable_tasks()
    if resumable:
        from bot.core.auto_animes import get_animes
        await rep.report(
            f"🔄 Resuming {len(resumable)} batch task(s) from last session...",
            "info", log=False
        )
        for t in resumable:
            await get_animes(
                t["name"], t["torrent"],
                task_id=str(t["_id"]),
                source_priority=t.get("source_priority", 1),
                quals_done=t.get("quals_done", []),
                is_batch=True,
                force=True,
            )
    else:
        await rep.report("✅ No batch tasks to resume.", "info", log=False)


async def _resume_movie_tasks():
    """Reset stuck movie tasks and re-queue pending ones on bot startup."""
    try:
        await movie_task_queue.reset_stuck_tasks()
        resumable = await movie_task_queue.get_resumable_tasks()
        if resumable:
            from bot.core.auto_animes import get_animes
            await rep.report(
                f"🎬 Resuming {len(resumable)} movie task(s)...", "info", log=False
            )
            for t in resumable:
                await get_animes(
                    t["name"], t["torrent"],
                    task_id=str(t["_id"]),
                    source_priority=t.get("source_priority", 1),
                    is_movie=True,
                    force=True,
                )
        else:
            await rep.report("🎬 No movie tasks to resume.", "info", log=False)
    except Exception:
        from traceback import format_exc
        await rep.report(format_exc(), "error")


# Movie dedup is now handled by db.is_torrent_seen() / db.mark_torrent_seen()
# using the same seen_torrents collection as ongoing dedup.
# _movie_seen dict, _movie_already_seen(), and _mark_movie_seen() removed.


def _get_movie_rss_items() -> list[str]:
    """Return movie RSS feed URLs from env, defaulting to Nyaa's movie category."""
    from os import getenv
    raw = getenv("MOVIE_RSS_ITEMS", "https://nyaa.si/?page=rss&c=1_2&f=0&q=movie")
    return raw.split()


async def fetch_animes():
    """
    Main RSS polling loop — runs for the lifetime of the bot.

    Startup sequence
    ----------------
    1. _warm_cache()            — fill ani_cache from DB
    2. _resume_pending_tasks()  — reset stuck tasks, re-queue batch tasks
    3. First-pass seed          — scan all RSS feeds and mark current entries as
                                  seen WITHOUT queueing them.  Prevents a fresh
                                  start from re-processing the last 2 weeks of
                                  episodes that are already in the DB.

    Main loop (every 60 seconds)
    ----------------------------
    - Skip torrents published more than 4 hours ago (RSS runs every 60s so
      anything older was already seen, or was missed and needs Upload All).
    - Pre-dedup in Python before creating a task (~80% of RSS entries at steady
      state are already in seen — avoiding asyncio task creation saves heap).
    - Create a gated task for new entries.
    """
    await rep.report("Fetch Animes Started !!", "info", log=False)
    await _warm_cache()
    await _resume_pending_tasks()

    # ── First-pass seed ───────────────────────────────────────────────────────
    # Check whether the seen_torrents collection already has entries from a
    # previous run. If it does, skip seeding — DB already knows what's been seen.
    # If empty (genuine fresh start), scan RSS and mark current entries as seen
    # WITHOUT queuing them so we don't re-process the last 2 weeks of releases.
    try:
        _seen_count = await db.db.seen_torrents.count_documents({})
        _is_fresh_start = _seen_count == 0
    except Exception:
        _is_fresh_start = False  # on DB error, skip seed to avoid duplicate posts
    if _is_fresh_start:
        await rep.report("🌱 First scan — seeding dedup DB (no queue)...", "info", log=False)
        seeded = 0
        now_t = _time.gmtime()
        seed_year, seed_mon = now_t.tm_year, now_t.tm_mon
        rss_sorted = sorted(Var.RSS_ITEMS, key=_rss_priority)
        for link in rss_sorted:
            for idx in range(10):
                info = await getfeed(link, idx)
                if not info:
                    break
                pub = getattr(info, 'published_parsed', None)
                if pub and (pub.tm_year != seed_year or pub.tm_mon != seed_mon):
                    continue
                key = info.link or info.title
                if key:
                    await db.mark_torrent_seen(key)
                    seeded += 1
                if info.title and info.title != key:
                    await db.mark_torrent_seen(info.title)
        await rep.report(
            f"✅ Seeded {seeded} torrent(s) — next scan will queue new ones.",
            "info", log=False
        )

    # ── Main loop ─────────────────────────────────────────────────────────────
    while True:
        await asleep(60)
        if not ani_cache['fetch_animes']:
            continue

        now_t = _time.gmtime()
        rss_sorted = sorted(Var.RSS_ITEMS, key=_rss_priority)

        for link in rss_sorted:
            for idx in range(75):  # SubsPlease caps at 75; enough for ~2 weeks
                info = await getfeed(link, idx)
                if not info:
                    break

                # Skip torrents published more than 4 hours ago
                pub = getattr(info, 'published_parsed', None)
                if pub:
                    pub_ts = _time.mktime(pub)
                    if _time.time() - pub_ts > 14400:
                        continue

                # DB dedup — avoids spawning a full pipeline task for already-seen
                # torrents. db.is_torrent_seen() is a single indexed find_one (~1ms).
                rss_key = info.link or info.title
                if await db.is_torrent_seen(rss_key):
                    await asleep(0)  # yield to event loop
                    continue
                if info.title and info.title != rss_key and await db.is_torrent_seen(info.title):
                    await asleep(0)
                    continue

                bot_loop.create_task(
                    _gated_get_animes(
                        info.title, info.link,
                        source_priority=_rss_priority(link)
                    )
                )
                await asleep(3)


async def fetch_movies():
    """
    Movie RSS polling loop — runs every 6 hours.

    Movies release far less frequently than ongoing episodes so a 60-second
    interval would be wasteful.  6 hours gives good coverage with minimal load.

    On first run, seeds the dedup cache from DB + current feed entries without
    queuing anything (same pattern as fetch_animes) so a restart doesn't
    re-queue everything already in the DB.
    """
    from bot.core.database import movie_db

    await movie_rep.report("🎬 Movie fetch loop started (6 hr interval)", "info", log=False)

    # Warm movie dedup cache from DB
    try:
        if movie_db.db is None:
            await movie_db.connect()
        # Movie dedup is now DB-only. Log count for diagnostics.
        _mv_seen_count = await movie_db.db.seen_torrents.count_documents({})
        await movie_rep.report(
            f"🎬 Movie dedup cache seeded ({_mv_seen_count} entries)", "info", log=False
        )
    except Exception as e:
        await movie_rep.report(f"Movie cache seed failed (non-critical): {e}", "warning", log=False)

    # First-pass seed — mark current feed entries without queuing
    for rss_url in _get_movie_rss_items():
        for idx in range(20):
            info = await getfeed(rss_url, idx)
            if not info:
                break
            key = info.link or info.title
            if key:
                await db.mark_torrent_seen(key)
                if info.title and info.title != key:
                    await db.mark_torrent_seen(info.title)
    await movie_rep.report("🎬 Movie feed seeded — future releases will be queued.", "info", log=False)

    # ── Main loop ─────────────────────────────────────────────────────────────
    while True:
        await asleep(6 * 3600)
        await movie_rep.report("🎬 Scanning movie RSS feeds...", "info", log=False)
        queued = 0

        for rss_url in _get_movie_rss_items():
            for idx in range(30):
                info = await getfeed(rss_url, idx)
                if not info:
                    break
                key = info.link or info.title
                if not key or await db.is_torrent_seen(key):
                    continue

                # Apply the same non-video skip filter as ongoing RSS
                tl = (info.title or "").lower()
                if any(k in tl for k in ["vol.", "volume", "manga", "novel", "ost", "scan", "comic"]):
                    continue

                await db.mark_torrent_seen(key)
                if info.title and info.title != key:
                    await db.mark_torrent_seen(info.title)

                from bot.core.auto_animes import get_animes
                bot_loop.create_task(get_animes(info.title, info.link, is_movie=True))
                queued += 1
                await asleep(5)

        await movie_rep.report(
            f"🎬 Movie scan done — {queued} new release(s) queued.", "info", log=False
        )
