# Fixed-Anime — Production Hardening Changelog

This document summarises every production fix shipped on top of the upstream
`MatizTech/Auto-Anime-Bot` codebase. If you are deploying on a small VPS
(2–8 GB RAM, < 50 GB disk), read this entire file before running the bot.

---

## 1. Hdri (HDRip) is now the **original** file — no re-encode

**What changed**

The `Hdri` quality is no longer passed through `ffmpeg`. The downloaded source
file is published to Telegram as-is, under the proper Hdri filename, via a
filesystem **hardlink** (or a copy if the filesystem rejects hardlinks, e.g.
cross-mount). The source inode stays intact for the subsequent 1080 / 720 /
480 encodes — deleting the hardlink only drops the extra dirent.

**Why**

`Hdri` literally means *HDRip* — the original, untouched rip. Re-encoding it
defeats the entire point of offering that quality. The previous "stream copy"
implementation (`-c copy`) was the right idea but a different bug filled the
disk (see §2 — that is now solved by cleanup, not by quality reduction).

**Where**

- `bot/core/pipelines/helpers.py` → new `hdri_passthrough(src, out_dir, name)`
- `bot/core/pipelines/ongoing.py` → calls passthrough instead of `FFEncoder`
  for the `Hdri` quality

**User-visible effect**

For ongoing single-episode tasks, `Hdri` is uploaded **immediately after
download finishes**, with **zero quality loss** and no encode wait. The
1080 / 720 / 480 encodes start straight after the Hdri upload.

> Status: implemented for the **ongoing** pipeline. The **batch** and
> **movie** pipelines still use the old re-encode path for Hdri — they will be
> migrated in a follow-up commit.

---

## 2. VPS-disk-fill / SSH-lockout cascade

**Symptom (before fix)**

Source download was 1.4 GB. Stream-copy Hdri produced another 1.4 GB. Then
1080 / 720 / 480 encodes ran in parallel without proper cleanup, accumulating
2–4 GB of temporary files on top. Disk hit 100 %, swap died, **SSH locked out**.

**What changed**

- `bot/core/diskguard.py` — new module with three knobs:
  - `DG_MIN_FREE_GB`     (default `5`) — refuse work below this absolute floor
  - `DG_CRITICAL_PCT`    (default `92`) — trigger aggressive cleanup at this %
  - `DG_ENCODE_HEADROOM_X` (default `2.5`) — required free space = `source × X`
- `bot/core/tordownload.py` — disk pre-flight before each download
- `bot/core/ffencoder.py` — disk pre-flight before each encode (returns `None`
  → caller re-queues task as `pending`)
- `bot/__main__.py` — `auto_cleanup` interval reduced from **6 h → 30 min**
  with aggressive escalation when free space drops below the critical %

**Where to tune**

```env
# config.env
DG_MIN_FREE_GB=5
DG_CRITICAL_PCT=92
DG_ENCODE_HEADROOM_X=2.5
```

---

## 3. Faster encodes (1080 / 720 / 480)

**Symptom (before fix)**

A 400 MB 1080p episode was encoding at **~ 200 KB/s**, with **~ 30 min ETA**.
Cause: every `FFCODE_*` line had `-threads 1 -x264-params threads=1`, forcing
single-threaded x264 even on multi-core VPS.

**What changed (`config.env`)**

| Quality | Old preset | New preset | Old `-threads` | New `-threads` | Old CRF | New CRF |
| ------- | ---------- | ---------- | -------------- | -------------- | ------- | ------- |
| 1080    | `fast`     | `superfast`| `1`            | *auto*         | `23`    | `24`    |
| 720     | `ultrafast`| `ultrafast`| `1`            | *auto*         | `23`    | `25`    |
| 480     | `ultrafast`| `ultrafast`| `1`            | *auto*         | `26`    | `27`    |

`-threads 1` is gone, ffmpeg now uses all available cores. CRF was nudged
upward by 1–2 to keep file sizes sane after the speed bump (visually identical
for animation content). Expected speed-up: **3–6 ×** depending on core count.

**RAM impact**

Multi-threaded x264 at 1080p uses ~ 150–300 MB more RSS than single-thread.
Still well within the existing `MIN_RAM_ENCODE_MB` gate and the 4 GB
`RLIMIT_AS` cap — no change needed.

---

## 4. Console progress logging

**Symptom (before fix)**

Telegram log channel showed live progress (every 8 s for encode, every 5 % for
download), but operators SSH'd into the VPS saw a **silent 3 + minute gap**
between `encode start` and `encode complete`. Impossible to tell from the
shell whether ffmpeg had stalled.

**What changed**

- `bot/core/ffencoder.py` `progress()` — emits `LOGS.info()` every 30 s with
  bar / % / size / speed / elapsed / eta. Also a final `100 % done in <t>`
  line on completion.
- `bot/core/tordownload.py` `_run_aria2c()` — same cadence: bar / % / size /
  speed / elapsed / eta / connection count.

**Format example**

```
📈 encode [1080] S04E04 — [██████▒▒▒▒▒▒] 47.2% | size 12.4MiB/~26.3MiB | speed 78.5KiB/s | elapsed 2m38s | eta 2m54s
📥 download Slime S04E01 — [█████▒▒▒▒▒] 53% | size 762MiB/1.4GiB | speed 4.2MiB/s | elapsed 184s | eta 2m41s | conns 8
```

Telegram cadence is unchanged — the new console line is purely for SSH
visibility.

---

## 5. Misc safety & startup fixes

| Commit  | Subject |
| ------- | ------- |
| `9cce47b` | Refuse to start (or warn) when `FFCODE_<quality>` uses `-c:v copy` (case- and whitespace-insensitive) |
| `7753220` | Make the copy-codec detection regex case- and whitespace-insensitive |
| `6307506` | Downgrade copy-codec guard from `SystemExit` to a warning so a single bad config line does not brick the bot |
| `0a11116` | Add `numpy`, `Pillow`, `httpx` to `requirements.txt` (transitive deps that pip resolved on dev but not on a fresh VPS) |
| `d0aca87` | Surface silent `FloodWait` and TCP-drop sleeps in the upload pipeline as `LOGS.warning()` so multi-minute upload pauses are visible in the log |

---

## 6. Operator quick reference

```bash
# Pull latest
cd ~/Fixed-Anime
git pull origin Main

# Watch live progress in two panes
tail -F /var/log/fixed-anime.log | grep -E '📈|📥'   # progress only
tail -F /var/log/fixed-anime.log                     # full log

# Disk health snapshot (matches what diskguard sees)
df -h /                                              # raw disk
du -sh ~/Fixed-Anime/encode ~/Fixed-Anime/downloads  # bot-owned dirs

# Force a cleanup right now (if you're about to run out)
rm -rf ~/Fixed-Anime/encode/*  ~/Fixed-Anime/downloads/*
```

If a download or encode is **skipped with "insufficient disk"**, that is the
diskguard doing its job. Either free space or lower `DG_MIN_FREE_GB`.

---

## 7. Upload never starts after a 1080p / batch / movie encode

**Symptom**

After a 1080p (or any non-`Hdri`) encode hits `100% done`, the log shows
one final line:

```
🧹 Dropped page cache: <source>.mkv (NNN MB)
```

…and then **silence**.  The `📤 ongoing: upload start [1080p] …` line
never appears, no FloodWait, no traceback.  The bot looks alive
(`/status` still answers) but the queued task never advances and
`htop` shows the encoder process gone but RSS still high.

**Root cause**

`bot/core/ffencoder.py` (and the per-quality cleanup blocks in
`pipelines/ongoing.py`, `pipelines/batch.py`, `pipelines/movie.py`)
called `reclaim_memory()` synchronously between encode and upload.

`reclaim_memory()` runs `gc.collect()` (fast) **and**
`malloc_trim(0)` (slow).  On a freshly-emptied 1–3 GB encode arena,
`malloc_trim(0)` walks every glibc arena page and can block the
calling thread for **5–30 minutes** on a small VPS.  Because it
was called directly on the asyncio thread, the entire event loop
froze — Pyrofork couldn't start the next `send_document`, schedulers
missed runs, no log line appeared, and operators thought the upload
was "stuck".

`bot/core/tguploader.py` already knew about this (line 95) and ran
its own `_trim_heap` via `run_in_executor`.  The encode-side calls
were left synchronous by mistake.

**Fix**

- `bot/core/memguard.py` — added `async def areclaim_memory()` that
  offloads `reclaim_memory()` to a thread executor.
- `bot/core/ffencoder.py:start_encode()` — `reclaim_memory()` →
  `await areclaim_memory()` after the post-encode `drop_page_cache`.
- `bot/core/pipelines/ongoing.py` (1 site) — same change.
- `bot/core/pipelines/batch.py` (2 sites: Hdri loop + main quality
  loop) — same change.
- `bot/core/pipelines/movie.py` (1 site) — same change.

After the fix, the upload starts within a few seconds of
`drop_page_cache` instead of being held for tens of minutes.

---

## 8. Card generator print() spam removed

`assets/card_generator.py` was emitting a `[card] +X.XXs  …` timeline
on every cover-image render (≈10 lines per episode) plus a
`[card] Skipping untrusted cover URL: …` line on every cache miss.
Replaced both `print()` calls with no-ops; re-enable locally with
`_ck = print` while debugging.  Production logs are noticeably cleaner.

---

## 9. Small-bug sweep

A second pass found a handful of "papercut" issues that don't crash the bot
but degrade reliability and operator UX. All fixed in one pass.

### 9.1 `/shell` was blocking the asyncio event loop

**File:** `bot/modules/dev.py` → `shell_handler`

**Was:** `result = subprocess.getoutput(parts[1])` — synchronous. Owner ran
`/shell apt update` → bot froze for 30+ seconds. Schedulers missed runs,
encoders stalled, RSS fetcher silently dropped episodes.

**Now:** `await asyncio.create_subprocess_shell(parts[1], stdout=PIPE, stderr=PIPE)`
— event loop stays responsive while shell command executes.

### 9.2 Thumbnail download required system `wget` / `cp` binaries

**File:** `bot/__init__.py` → "Thumbnail setup" block

**Was:** `subprocess.run(["wget", "-q", Var.THUMB, "-O", "thumb.jpg"])` and
`subprocess.run(["cp", "bot/thumb.jpg", "thumb.jpg"])`. On minimal containers
(alpine, distroless, scratch-based) `wget` is often missing — thumb fetch
silently failed, every uploaded file got a default blank thumb. `cp` is
universal but still a needless fork.

**Now:** Pure stdlib — `shutil.copy()` for the local file, `urllib.request.urlopen()`
for the URL fetch. URL scheme is validated to refuse anything that isn't
`http(s)` so a hostile `THUMB=file:///etc/passwd` can't be used to read
arbitrary files into the upload thumbnail. Also wrapped in `try/except`
so a bad URL just logs a warning instead of swallowing exceptions silently.

### 9.3 `print(e)` debug spam in sample-clip generator

**File:** `bot/func.py` (line ~91)

**Was:** A leftover `print(e)` inside an exception handler. The error
bypassed the configured logger — no timestamp, no log channel, no level
filter. Operators couldn't tell sample-clip failures apart from other
stdout noise.

**Now:** `log.error(f"sample-clip postprocess: {e}")` — routed through the
module logger like every other error in the file.

### 9.4 Shutdown noise — `RuntimeError: Event loop is closed`

**File:** `bot/__main__.py` (the `if __name__ == '__main__':` block)

**Symptom (reported from production VPS):**

```
Exception ignored in: <coroutine object Dispatcher.handler_worker at 0x...>
  File "uvloop/loop.pyx", line 705, in uvloop.loop.Loop._check_closed
RuntimeError: Event loop is closed
[asyncio | ERROR] - Task was destroyed but it is pending!
task: <Task pending name='Task-152' coro=<Event.wait() running ...>
```

**Was:** After `main()` returned, we called `bot_loop.close()` immediately.
But Pyrogram's `Dispatcher` spawns N `handler_worker` tasks (one per
worker, default 4) that block on `await queue.get()`. Even though
`bot.stop()` signals them to exit, there's a tiny window where the
worker coroutines haven't been fully reaped before the loop closes.
When CPython's GC then collects them, their internal cleanup tries
to schedule a callback on the (now-closed) loop and raises
`RuntimeError: Event loop is closed`. Same story for any session
keepalive or `Event.wait()` coroutine still pending.

The bot already exited cleanly — but operators saw a wall of red
tracebacks at shutdown and assumed something was broken.

**Now:** Before `bot_loop.close()`, gather every remaining task on the
loop, cancel them, and run the loop until they all complete. Then
call `loop.shutdown_asyncgens()` to drain any async generators. Finally
close. The shutdown is now silent — operators see only the expected
`✅ Finished AutoCleanUp — bye.` line.

```python
_leftover = [t for t in all_tasks(bot_loop) if not t.done()]
if _leftover:
    for _t in _leftover:
        _t.cancel()
    bot_loop.run_until_complete(gather(*_leftover, return_exceptions=True))
bot_loop.run_until_complete(bot_loop.shutdown_asyncgens())
bot_loop.close()
```

### 9.5 Silent startup-failure exits (the *real* "bot just dies" bug)

**File:** `bot/__main__.py` (the `if __name__ == '__main__':` block)

**Symptom (reported from production VPS):** bot exits 3–5 seconds after
startup with:

```
[bot | INFO] - Local thumbnail loaded from bot/thumb.jpg
[bot | INFO] - No orphan ffmpeg processes found.
[apscheduler.scheduler | INFO] - Adding job tentatively (×3)
root@...:~/Fixed-Anime#
```

…and **no exception, no traceback, no error line**. Operators have no
idea why the bot died and assume the bot is broken.

**Was:** `sys.exit(0)` was placed *inside* the `finally:` block, and the
`try:` only had a narrow `except (KeyboardInterrupt, SystemExit):`
clause. Any other exception raised inside `main()` (and there are
many during startup — `AuthKeyUnregistered`, MongoDB ServerSelection
timeout, missing channel, invalid bot token, FloodWait on `bot.start()`,
permission denied on `Var.FILE_STORE`, etc.) would:

1. Skip the narrow `except` clause (not a `KeyboardInterrupt`).
2. Fall into `finally:`.
3. Hit `sys.exit(0)`.
4. Per Python semantics, the new `SystemExit` from `sys.exit(0)`
   **replaces** the in-flight exception. The original traceback is
   discarded.
5. Process exits with code 0 — nothing written to stderr.

This bug existed in the original upstream codebase. It only became
visible after the §9.4 shutdown-noise fix removed the cosmetic
"Event loop is closed" cascade that was previously masking the
underlying issue.

**Now:**

1. Added a broad `except Exception:` clause that calls
   `LOGS.exception(...)` to print the *full* traceback through the
   configured logger (so it goes to the log channel as well as stderr).
2. Moved `sys.exit(...)` **outside** the `finally:` block.
3. Track `_exit_code` (0 on clean shutdown, 1 on unhandled exception)
   so process supervisors (systemd `Restart=on-failure`, docker
   `restart=on-failure`, pm2) can react correctly. Previously every
   exit was code 0 — supervisors thought the bot had finished its
   work and stopped restarting it.
4. Demoted the "Draining N leftover task(s)" line from `INFO` to
   `DEBUG` — at a startup-failure exit it's normal to have 14+
   leftover tasks (apscheduler timers, motor pools, pyrogram session
   bootstrap), and an `INFO` line was scaring operators into thinking
   the drain itself was the problem.

After this fix, a startup failure looks like:

```
[bot | INFO] - Adding job tentatively (×3)
[bot | ERROR] - 💥 Unhandled exception in main() — bot is exiting:
Traceback (most recent call last):
  File ".../bot/__main__.py", line 212, in main
    await bot.start()
  ...
pyrogram.errors.exceptions.unauthorized_401.AuthKeyUnregistered: ...
```

— the operator can finally see *why* the bot died.

### 9.6 12 bare `except:` clauses across 6 files

**Files:** `bot/modules/cmds.py` (×5), `bot/modules/fsub.py` (×4),
`bot/modules/banuser.py`, `bot/modules/admin.py`, `bot/modules/force_subscription.py`,
`assets/card_generator.py`.

**Was:** `except:` (catches *everything*, including `KeyboardInterrupt` and
`SystemExit`). PEP 8 violation — `Ctrl+C` during one of these blocks would
get swallowed, leaving the bot un-killable from the terminal.

**Now:** `except Exception:` everywhere. Functionally identical for the
common case but no longer hijacks process-level signals. No behaviour change
for users.

---

## 10. Known follow-ups

- Migrate `Hdri` passthrough into `bot/core/pipelines/batch.py` and
  `bot/core/pipelines/movie.py` (currently still re-encode).
- Optional `/diskstatus` admin command — live disk + diskguard threshold
  readout in the Telegram log channel on demand.
- Rotate the secrets that were leaked in `config.env` git history
  (`API_HASH`, three bot tokens, `MONGO_URI` password, `UPSTREAM_REPO` PAT).
  This is **outside the scope of code fixes** and is the operator's
  responsibility.
