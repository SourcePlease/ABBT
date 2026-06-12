from asyncio import create_task, create_subprocess_exec, create_subprocess_shell, run as asyrun, all_tasks, gather, sleep as asleep, current_task
from aiofiles import open as aiopen
from pyrogram import idle
from pyrogram.filters import command, user
from os import path as ospath, execl, kill
import os
import sys
from sys import executable
from signal import SIGKILL

from bot import bot, Var, bot_loop, sch, LOGS, ffpids_cache, FF_WORKERS
from bot.core.auto_animes import fetch_animes
from bot.core.func_utils import clean_up, new_task, editMessage
from bot.modules.up_posts import upcoming_animes

# ── FIX #1: The original code defined two functions both named `restart()`.
# Python silently overwrites the first definition (the /restart command handler)
# with the second (the post-reboot message editor), so the /restart command
# never worked and the post-reboot message was never edited.
# Fix: give each function a unique, descriptive name.

@bot.on_message(command('restart') & user(Var.OWNER_ID))
@new_task
async def restart_cmd(client, message):
    """Handler for the /restart command — kills running encodes, saves state, re-execs."""
    rmessage = await message.reply('<i>Restarting...</i>')
    if sch.running:
        sch.shutdown(wait=False)
    await clean_up()
    if len(ffpids_cache) != 0:
        for pid in ffpids_cache:
            try:
                LOGS.info(f"Process ID : {pid}")
                kill(pid, SIGKILL)
            except (OSError, ProcessLookupError):
                LOGS.error("Killing Process Failed !!")
                continue
    await (await create_subprocess_exec('python3', 'update.py')).wait()
    async with aiopen(".restartmsg", "w") as f:
        await f.write(f"{rmessage.chat.id}\n{rmessage.id}\n")
    execl(executable, executable, "-m", "bot")


async def check_restart_msg():
    """Called once on startup — edits the 'Restarting…' message to 'Restarted!' if present."""
    import aiofiles
    if not ospath.isfile(".restartmsg"):
        return

    async with aiofiles.open(".restartmsg", "r") as f:
        data = (await f.read()).split("\n")
        chat_id, msg_id = int(data[0]), int(data[1])

    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="<i>Restarted!</i>")
    except Exception as e:
        LOGS.error(e)

    os.remove(".restartmsg")


async def queue_loop():
    """
    Start the two dedicated workers:
      - Ongoing worker: processes single RSS episodes → posts to MAIN_CHANNEL
      - Batch worker:   processes BDRip/batch seasons → posts to BATCH_MAIN_CHANNEL
                        also handles all movie tasks (no separate movie worker)

    Each worker processes one task at a time (1 download then 1 encode),
    enforced by ongoing_dl_lock/ongoing_encode_lock and batch_dl_lock/batch_encode_lock.
    """
    from bot.core.auto_animes import _ongoing_worker, _batch_worker
    LOGS.info("Queue Loop Started — ongoing + batch workers (movies handled by batch worker)")
    bot_loop.create_task(_ongoing_worker())
    bot_loop.create_task(_batch_worker())
    while True:
        await asleep(60)


async def auto_cleanup():
    """
    Periodic cleanup scheduler — runs every 30 minutes.

    Behaviour:
      1. Stale-file sweep across downloads/, encode/, thumbs/, torrents/
         (stale = mtime older than 30 minutes).
      2. If disk usage is at/above DG_AGGRESSIVE_PCT, switch to aggressive
         mode and wipe ALL files in those folders regardless of age. This
         is the last line of defence before the partition fills and takes
         down MongoDB / sshd / co-tenant bots.
      3. Prune empty sub-directories left behind by failed batch tasks.
      4. Purge task_queue 'done' records older than 7 days.

    The previous schedule (every 6 hours, 2-hour stale threshold) was
    useless during an active batch — by the time it ran the disk was
    already full. The new cadence (30 min / 30 min) tracks an in-flight
    batch closely enough to keep the bot ahead of the disk-fill curve.
    """
    from datetime import datetime, timedelta
    from bot.core.diskguard import (
        cleanup_stale, aggressive_cleanup, is_disk_aggressive,
        log_disk_snapshot, get_disk_snapshot,
    )

    LOGS.info("🧹 Auto-cleanup started...")
    log_disk_snapshot(prefix="💾 Disk (pre-cleanup)")

    stale_age = 30 * 60  # 30 minutes — was 2 hours
    cleaned_files, cleaned_dirs = cleanup_stale(stale_age)

    # If the partition is still hot after the gentle sweep, go aggressive.
    if is_disk_aggressive():
        snap = get_disk_snapshot()
        LOGS.warning(
            f"🧨 Disk at {snap['used_pct']}% (≥ aggressive threshold) — "
            f"escalating to aggressive_cleanup"
        )
        agg_files, agg_dirs = aggressive_cleanup()
        cleaned_files += agg_files
        cleaned_dirs += agg_dirs

    # Purge old task records (DB-side; cheap, helps Mongo storage too)
    try:
        from bot.core.task_queue import task_queue
        col = await task_queue._col()
        cutoff = datetime.utcnow() - timedelta(days=7)
        result = await col.delete_many({"status": "done", "updated_at": {"$lt": cutoff}})
        purged = result.deleted_count
    except Exception as e:
        LOGS.warning(f"Task record purge failed: {e}")
        purged = 0

    log_disk_snapshot(prefix="💾 Disk (post-cleanup)")
    LOGS.info(
        f"🧹 Cleanup done — {cleaned_files} file(s), {cleaned_dirs} empty "
        f"dir(s) removed, {purged} old task record(s) purged."
    )


async def _kill_orphan_ffmpeg():
    """
    Kill any ffmpeg processes left over from a previous bot run.
    When the bot is SIGKILLed, its child ffmpeg processes become orphans and
    keep consuming RAM/CPU — each restart stacks more orphans until the VPS OOMs.
    This runs once at startup before anything else allocates memory.
    """
    import subprocess
    try:
        result = subprocess.run(
            ["pgrep", "-f", "ffmpeg"],
            capture_output=True, text=True
        )
        pids = [int(p) for p in result.stdout.split() if p.strip().isdigit()]
        if pids:
            LOGS.info(f"🔪 Killing {len(pids)} orphan ffmpeg process(es): {pids}")
            for pid in pids:
                try:
                    os.kill(pid, 9)
                except (ProcessLookupError, PermissionError):
                    pass
        else:
            LOGS.info("✅ No orphan ffmpeg processes found.")
    except FileNotFoundError:
        pass  # pgrep not available — skip silently
    except Exception as e:
        LOGS.warning(f"ffmpeg orphan cleanup failed (non-critical): {e}")


async def _log_memory_usage():
    """
    Log system and process memory usage every hour.
    Helps diagnose slow memory leaks and confirm that cache caps are working.
    psutil is already in requirements.txt.
    """
    try:
        import psutil
        proc = psutil.Process()
        mem = proc.memory_info()
        sys_mem = psutil.virtual_memory()
        # Also count ffmpeg children
        ffmpeg_rss = 0
        ffmpeg_count = 0
        for child in proc.children(recursive=True):
            try:
                if 'ffmpeg' in child.name().lower():
                    ffmpeg_rss += child.memory_info().rss
                    ffmpeg_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        LOGS.info(
            f"📊 Memory: Bot RSS={mem.rss / 1024 / 1024:.0f}MB, "
            f"VMS={mem.vms / 1024 / 1024:.0f}MB | "
            f"FFmpeg: {ffmpeg_count} proc(s), {ffmpeg_rss / 1024 / 1024:.0f}MB | "
            f"System: {sys_mem.used / 1024 / 1024:.0f}MB/{sys_mem.total / 1024 / 1024:.0f}MB "
            f"({sys_mem.percent}% used, {sys_mem.available / 1024 / 1024:.0f}MB free)"
        )
    except ImportError:
        pass  # psutil not installed — handled gracefully
    except Exception as e:
        LOGS.warning(f"Memory logging failed: {e}")


async def main():
    await _kill_orphan_ffmpeg()
    sch.add_job(upcoming_animes, "cron", hour=0, minute=30)
    # Cleanup cadence dropped from 6 hours → 30 minutes so we can keep ahead
    # of an in-flight batch download. See bot/core/diskguard.py for the
    # aggressive-mode escalation logic.
    sch.add_job(auto_cleanup, "interval", minutes=30, id="auto_cleanup")
    sch.add_job(_log_memory_usage, "interval", hours=1, id="memory_monitor")
    await bot.start()

    # ── Import shared handler references once ────────────────────────────────
    from pyrogram import filters as _f
    from pyrogram.handlers import MessageHandler as _MH, CallbackQueryHandler as _CBH

    # Start batch bot if a separate token was configured
    from bot import batch_bot as _batch_bot
    if _batch_bot is None:
        import bot as _bot_module
        _bot_module.batch_bot = bot
        LOGS.info("Batch bot: using main bot (no BATCH_BOT_TOKEN set)")
    else:
        await _batch_bot.start()
        LOGS.info("Batch bot started with separate token")

    # Start movie bot if a separate token was configured.
    from bot import movie_bot as _movie_bot, batch_bot as _batch_bot_ref
    if _movie_bot is None:
        import bot as _bot_module2
        _bot_module2.movie_bot = _batch_bot_ref if _batch_bot_ref else bot
        _fallback_name = "batch bot" if _batch_bot_ref else "main bot"
        LOGS.info(f"Movie bot: using {_fallback_name} (no MOVIE_BOT_TOKEN set)")
    else:
        await _movie_bot.start()
        LOGS.info("Movie bot started with separate token")

    # ── Register all commands on Bot 2 and Bot 3 ─────────────────────────────
    from bot import batch_bot as _bb_final, movie_bot as _mb_final
    from bot.modules.register_handlers import register_all
    await register_all(_bb_final, _mb_final)
    LOGS.info("All handlers registered on Bot 2 and Bot 3")

    # FIX #1: call check_restart_msg() (was restart() — the wrong function)
    await check_restart_msg()

    # Verify each bot has access to its file store channel AND its log
    # channel. Use the correct bot client for each:
    #   FILE_STORE        / LOG_CHANNEL        → main bot
    #   BATCH_FILE_STORE  / BATCH_LOG_CHANNEL  → batch_bot (fallback main)
    #   MOVIE_FILE_STORE  / MOVIE_LOG_CHANNEL  → movie_bot (fallback main)
    #
    # Verifying log channels here serves two purposes:
    #   (1) it warms pyrogram's peer cache, so the first error report
    #       doesn't fail with [400 CHANNEL_INVALID] (which used to dump
    #       a full traceback into the local log);
    #   (2) it surfaces a clear startup warning if the LOG_CHANNEL is
    #       misconfigured / the bot isn't a member, instead of silently
    #       breaking later when the first error fires.
    from bot import batch_bot as _vbb, movie_bot as _vmb
    _verify_targets: list[tuple[int, object, str]] = [
        (Var.FILE_STORE,        bot,                                                 "file store"),
        (Var.BATCH_FILE_STORE,  _vbb if (_vbb and _vbb is not bot) else bot,         "file store"),
        (Var.MOVIE_FILE_STORE,  _vmb if (_vmb and _vmb is not bot) else bot,         "file store"),
        (Var.LOG_CHANNEL,       bot,                                                 "log channel"),
        (Var.BATCH_LOG_CHANNEL, _vbb if (_vbb and _vbb is not bot) else bot,         "log channel"),
        (Var.MOVIE_LOG_CHANNEL, _vmb if (_vmb and _vmb is not bot) else bot,         "log channel"),
    ]
    # Dedup (cid, client) pairs — multiple roles may map to the same id.
    _seen: set[tuple[int, int]] = set()
    for _cid, _client, _kind in _verify_targets:
        if not _cid:
            continue
        _key = (_cid, id(_client))
        if _key in _seen:
            continue
        _seen.add(_key)
        try:
            _test = await _client.send_message(
                _cid, f"✅ Bot started — {_kind} verified."
            )
            await _test.delete()
            LOGS.info(
                f"{_kind.capitalize()} {_cid} verified OK (via {_client.name})"
            )
        except Exception as _ce:
            LOGS.warning(
                f"Could not verify {_kind} {_cid} via {_client.name}: {_ce}"
            )

    LOGS.info('Auto Anime Bot Started!')
    await _log_memory_usage()  # baseline snapshot
    sch.start()
    bot_loop.create_task(queue_loop())
    from bot.core.auto_animes import fetch_movies, _resume_movie_tasks
    await _resume_movie_tasks()
    bot_loop.create_task(fetch_movies())

    # FIX: fetch_animes() is an infinite `while True: await asleep(60)` loop
    # that NEVER returns. The previous `await fetch_animes()` blocked main()
    # here forever, so the `await idle()` below was unreachable — meaning
    # pyrogram's SIGINT/SIGTERM handler was never installed and Ctrl-C could
    # not stop the bot cleanly. Run fetch_animes as a background task instead,
    # then block on idle() which properly handles Ctrl-C / SIGTERM.
    bot_loop.create_task(fetch_animes())

    await idle()  # blocks until SIGINT or SIGTERM
    LOGS.info('🛑 Shutdown signal received — stopping clients...')

    # Stop scheduler first so no new jobs fire during teardown
    try:
        if sch.running:
            sch.shutdown(wait=False)
    except Exception as _se:
        LOGS.warning(f"Scheduler shutdown failed: {_se}")

    # Stop all 3 bot clients (each wrapped so one failure doesn't block the rest)
    for _name, _cl in (("main bot", bot),):
        try:
            await _cl.stop()
        except Exception as _e:
            LOGS.warning(f"{_name}.stop() failed: {_e}")
    from bot import batch_bot as _batch_bot2
    if _batch_bot2 and _batch_bot2 is not bot:
        try:
            await _batch_bot2.stop()
        except Exception as _e:
            LOGS.warning(f"batch_bot.stop() failed: {_e}")
    from bot import movie_bot as _movie_bot2
    if _movie_bot2 and _movie_bot2 is not bot:
        try:
            await _movie_bot2.stop()
        except Exception as _e:
            LOGS.warning(f"movie_bot.stop() failed: {_e}")

    # Kill any FFmpeg children we spawned so they don't outlive the bot.
    if ffpids_cache:
        LOGS.info(f"🔪 Killing {len(ffpids_cache)} live ffmpeg process(es) before exit")
        for _pid in list(ffpids_cache):
            try:
                kill(_pid, SIGKILL)
            except (OSError, ProcessLookupError):
                pass

    # FIX: cancel only OTHER tasks. Cancelling the current `main()` task here
    # would raise CancelledError on the next await and skip clean_up().
    _self = current_task()
    _pending = [t for t in all_tasks() if t is not _self and not t.done()]
    for t in _pending:
        t.cancel()
    if _pending:
        try:
            await gather(*_pending, return_exceptions=True)
        except Exception:
            pass

    try:
        await clean_up()
    except Exception as _ce:
        LOGS.warning(f"clean_up() failed: {_ce}")
    LOGS.info('✅ Finished AutoCleanUp — bye.')


import pyrogram.utils
pyrogram.utils.MIN_CHANNEL_ID = -1009147483647

if __name__ == '__main__':
    # Wrap in try/except as a safety net — if the user hits Ctrl-C BEFORE
    # idle() has installed its signal handler (e.g. during startup connection
    # to MongoDB), KeyboardInterrupt propagates here instead and we still
    # exit cleanly with code 0 rather than dumping a traceback.
    _exit_code = 0
    try:
        bot_loop.run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        LOGS.info('🛑 Hard shutdown requested (Ctrl-C during startup) — exiting.')
    except Exception:
        # FIX: previously, if main() raised a non-KeyboardInterrupt error
        # (auth failure, network blip, MongoDB timeout, AuthKeyUnregistered,
        # missing channel, etc.), the traceback was silently swallowed
        # because `sys.exit(0)` inside the `finally:` block replaced the
        # original exception with a fresh `SystemExit`. Operators saw the
        # bot exit a few seconds after startup with NO clue why.
        # Log the full traceback explicitly and exit with code 1 so process
        # supervisors (systemd, docker restart=on-failure, pm2) can react
        # and the operator can read the actual error.
        LOGS.exception('💥 Unhandled exception in main() — bot is exiting:')
        _exit_code = 1
    finally:
        # Drain leftover tasks before loop.close() so Pyrogram's
        # Dispatcher.handler_worker coroutines don't get GC'd against a
        # closed loop and emit "RuntimeError: Event loop is closed" /
        # "Task was destroyed but it is pending!" noise at exit time.
        try:
            try:
                _leftover = [t for t in all_tasks(bot_loop) if not t.done()]
                if _leftover:
                    # debug-level: 14+ "leftover" tasks at startup-failure
                    # exit is normal (apscheduler timers, motor pools,
                    # pyrogram session) — INFO would scare operators into
                    # thinking the drain itself is the problem.
                    LOGS.debug(f"Draining {len(_leftover)} leftover task(s) before loop close")
                    for _t in _leftover:
                        _t.cancel()
                    bot_loop.run_until_complete(
                        gather(*_leftover, return_exceptions=True)
                    )
            except Exception as _de:
                LOGS.warning(f"Task drain failed (non-critical): {_de}")
            try:
                bot_loop.run_until_complete(bot_loop.shutdown_asyncgens())
            except Exception:
                pass
            try:
                bot_loop.close()
            except Exception:
                pass
        except Exception:
            pass
    # FIX: sys.exit() MUST be outside `finally:` — when it sat inside, a
    # `SystemExit` raised here suppressed any in-flight exception from the
    # try block (Python's "exception during finally" rule), hiding the
    # original startup error. Now any exception is logged above first,
    # then we exit with the correct code.
    sys.exit(_exit_code)
