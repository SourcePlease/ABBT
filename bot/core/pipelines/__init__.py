"""
bot/core/pipelines/
====================
The three encode-upload pipelines, shared helpers, and worker queues.

Sub-modules
-----------
filters   — _is_batch_task, _is_movie_task, skip_keywords, dual-audio gate
helpers   — _make_link, _qual_btns_to_keyboard, _qual_file_store,
            _send_ending_post, _build_ending_keyboard, extra_utils
workers   — _ongoing_worker, _batch_worker, the three asyncio queues
rss       — fetch_animes, fetch_movies, _warm_cache, _resume_pending_tasks
ongoing   — _run_pipeline   (single RSS episode → MAIN_CHANNEL)
batch     — _run_batch_pipeline  (BDRip/season pack → BATCH_MAIN_CHANNEL)
movie     — _run_movie_pipeline  (anime movie → MOVIE_MAIN_CHANNEL)
"""
