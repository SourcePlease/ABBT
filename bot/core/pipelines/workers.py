"""
pipelines/workers.py
====================
Asyncio queues and worker coroutines that serialise pipeline execution.

Queue topology
--------------
_ongoing_queue  PriorityQueue  (source_priority, counter, *task_args)
    └── _ongoing_worker()  — processes one RSS episode at a time

_batch_queue    PriorityQueue  (source_priority, counter, *task_args)
    └── _batch_worker()   — processes one batch/movie task at a time
                            (movies are handled inline to cap concurrent DLs)

_movie_queue    Queue          (name, torrent, force, task_id, src_prio)
    └── drained by _batch_worker() when _batch_queue is empty

Priority ordering
-----------------
Items use (source_priority, monotonic_counter) as the sort key so that:
  - Lower source_priority = processed first (SubsPlease=0 beats Ember=1)
  - Counter is a FIFO tie-breaker within the same priority tier
"""

import asyncio
import itertools
from asyncio import PriorityQueue as _PQueue, Queue as _AQueue
from traceback import format_exc

from bot import bot, Var, bot_loop, LOGS
from bot.core.reporter import rep, batch_rep
from bot.core.task_queue import task_queue, batch_task_queue, movie_task_queue, MAX_RETRIES

# ── Monotonic counters (FIFO tie-breakers inside the priority queues) ─────────
_ongoing_counter = itertools.count()
_batch_counter   = itertools.count()

# ── The three asyncio queues ─────────────────────────────────────────────────
# Each item in _ongoing_queue / _batch_queue is a 9-tuple:
#   (source_priority, counter, name, torrent, force, task_id,
#    source_priority_copy, quals_done, is_batch)
# source_priority_copy == source_priority — duplicated so pipeline functions
# receive it as a positional arg without re-reading from the tuple head.
#
# Each item in _movie_queue is a 5-tuple:
#   (name, torrent, force, task_id, source_priority)
_ongoing_queue: _PQueue = _PQueue()
_batch_queue:   _PQueue = _PQueue()
_movie_queue:   _AQueue = _AQueue()


async def _ongoing_worker():
    """
    Worker for ongoing/airing anime (single RSS episodes).

    Pulls from _ongoing_queue (PriorityQueue) — SubsPlease items are always
    processed before Ember or Erai-raws items at the same priority tier.
    Acquires no locks itself; locking is done inside _run_pipeline.
    """
    # Import here to break the circular dependency:
    #   workers → ongoing → workers (via queue.put())
    from bot.core.pipelines.ongoing import _run_pipeline

    LOGS.info("Ongoing Worker started")
    while True:
        _prio, _ctr, name, torrent, force, task_id, source_priority, quals_done, is_batch = (
            await _ongoing_queue.get()
        )
        try:
            await rep.report(f"⚙️ Ongoing Worker picked up: {name}", "info", log=False)
            await _run_pipeline(
                name, torrent, force, task_id, source_priority, quals_done,
                is_batch=False,
                target_channel=Var.MAIN_CHANNEL,
                file_store=Var.FILE_STORE,
                log_channel=Var.LOG_CHANNEL,
            )
        except Exception:
            await rep.report(format_exc(), "error")
        finally:
            _ongoing_queue.task_done()


async def _batch_worker():
    """
    Worker for completed/batch anime (BDRips, season packs) AND movies.

    A single coroutine handles both pipeline types so at most one download and
    one encode run on this lane simultaneously — prevents concurrent-download OOM.

    Priority:
      1. Batch tasks (_batch_queue) — checked first on every loop iteration.
      2. Movie tasks (_movie_queue) — picked up when batch queue is empty.
      3. Block on _batch_queue with a 30-second timeout to stay responsive to
         movie tasks that arrive while waiting.
    """
    from bot.core.pipelines.batch import _run_batch_pipeline
    from bot.core.pipelines.movie import _run_movie_pipeline
    from bot import batch_bot, movie_bot

    LOGS.info("Batch/Movie Worker started")

    while True:
        # ── 1. Non-blocking check for a batch task ────────────────────────
        try:
            item = _batch_queue.get_nowait()
        except Exception:
            item = None

        if item is not None:
            _b_prio, _b_ctr, name, torrent, force, task_id, source_priority, quals_done, is_batch = item
            try:
                await batch_rep.report(f"⚙️ Batch Worker picked up: {name}", "info", log=False)
                _upload_bot = batch_bot if batch_bot else bot
                await _run_batch_pipeline(
                    name, torrent, force, task_id, source_priority,
                    target_channel=Var.BATCH_MAIN_CHANNEL,
                    file_store=Var.BATCH_FILE_STORE,
                    log_channel=Var.BATCH_LOG_CHANNEL,
                    upload_bot=_upload_bot,
                )
            except Exception:
                await batch_rep.report(format_exc(), "error")
            finally:
                _batch_queue.task_done()
            continue

        # ── 2. Non-blocking check for a movie task ────────────────────────
        try:
            mv_item = _movie_queue.get_nowait()
        except Exception:
            mv_item = None

        if mv_item is not None:
            mv_name, mv_torrent, mv_force, mv_task_id, mv_prio = mv_item
            try:
                await batch_rep.report(f"🎬 Batch Worker picking up movie: {mv_name}", "info", log=False)
                _mv_bot = movie_bot or batch_bot or bot
                await _run_movie_pipeline(
                    mv_name, mv_torrent, mv_force, mv_task_id, mv_prio,
                    target_channel=Var.MOVIE_MAIN_CHANNEL,
                    file_store=Var.MOVIE_FILE_STORE,
                    log_channel=Var.MOVIE_LOG_CHANNEL,
                    upload_bot=_mv_bot,
                )
            except Exception:
                await batch_rep.report(format_exc(), "error")
            finally:
                _movie_queue.task_done()
            continue

        # ── 3. Both queues empty — block on batch queue (30-second timeout) ──
        # The timeout lets us re-check _movie_queue for tasks that arrive while
        # we are blocked waiting for a new batch item.
        try:
            item = await asyncio.wait_for(_batch_queue.get(), timeout=30)
        except asyncio.TimeoutError:
            continue  # nothing arrived — loop back and re-check both queues

        _b_prio, _b_ctr, name, torrent, force, task_id, source_priority, quals_done, is_batch = item
        try:
            await batch_rep.report(f"⚙️ Batch Worker picked up: {name}", "info", log=False)
            _upload_bot = batch_bot if batch_bot else bot
            await _run_batch_pipeline(
                name, torrent, force, task_id, source_priority,
                target_channel=Var.BATCH_MAIN_CHANNEL,
                file_store=Var.BATCH_FILE_STORE,
                log_channel=Var.BATCH_LOG_CHANNEL,
                upload_bot=_upload_bot,
            )
        except Exception:
            await batch_rep.report(format_exc(), "error")
        finally:
            _batch_queue.task_done()
