# Miko Anime Bot — `Auto-Anime-V2`

Latest feature branch — everything from V1 plus a full settings-panel refactor for channel management, scheduling, and reporter resilience.

> Mirror of [`MikoxYae/Fixed-Anime`](https://github.com/MikoxYae/Fixed-Anime), rebuilt from a clean tree. Original credentials that were committed in the source repo have been **scrubbed** from `config.env` / `config.env.example`. Fill them in locally before running.

---

## What's on this branch

This is the **current default** branch. It contains everything in `Auto-Anime-V1` plus the settings-panel rebuild and reporter resilience pass:

**Settings panel rebuild**

- New **📺 Channel Management** sub-panel inside `/settings` — covers everything that used to be `/connect`, `/connectchannel`, `/listconnections`, `/removeconnection`, `/removechannel`. Those standalone commands have been **removed** in favour of the inline panel.
- New **📅 Schedule** sub-panel inside `/settings` — replaces the old `/schedule` command.
- The 🗑 Remove flow is now a **soft unlink** (no episode data is touched on the linked channel) and ships with a 2-step confirmation (`preview → ✅ Yes Wipe`) to avoid accidental wipes.
- Fixed an `AttributeError` in `get_anime_channel` that surfaced when the paginator's 🗑 button was tapped on certain rows.

**Reporter / log-channel resilience**

- `reporter.py` no longer floods the console with `CHANNEL_INVALID` tracebacks when `LOG_CHANNEL` becomes inaccessible (deleted, bot kicked, channel ID changed). The error is logged once and subsequent reports degrade quietly.
- `LOG_CHANNEL` is **warmed at startup** — the bot resolves and validates the channel before the first report is sent, so the first error doesn't appear minutes after launch deep inside an encode log.

**Everything from V1 still applies** — Hdri passthrough, diskguard, multi-threaded encodes, `areclaim_memory()` fix, async `/shell`, stdlib thumbnail, silent-exit fix, bare-except sweep, etc.

---

## Tech stack

- **Telegram client:** [pyrofork](https://github.com/Mayuri-Chan/pyrofork) (>= 2.3.0)
- **Database:** MongoDB (Atlas works) via `motor`
- **Video:** FFmpeg with `libx264` + `libopus`
- **Scheduler:** APScheduler
- **HTTP:** aiohttp + httpx
- **Misc:** torrentp (aria2c), anitopy, feedparser, Pillow, numpy, uvloop (Linux only)

## Repository layout

```
Auto-Anime-V2/
├── bot/
│   ├── __init__.py        env loading, Pyrofork client setup, locks/queues
│   ├── __main__.py        entrypoint: starts bots, scheduler, RSS loop
│   ├── core/
│   │   ├── auto_animes.py orchestrates ongoing/batch/movie fetch loops
│   │   ├── database.py    MongoDB layer
│   │   ├── ffencoder.py   FFmpeg wrapper with progress reporting
│   │   ├── tordownload.py torrent / magnet downloader
│   │   ├── tguploader.py  Telegram upload helpers
│   │   ├── task_queue.py  persistent (Mongo-backed) task queue
│   │   ├── text_utils.py  title parsing & caption builders
│   │   ├── func_utils.py  async helpers
│   │   ├── memguard.py    memory-pressure guard around encodes
│   │   ├── diskguard.py   disk pre-flight checks
│   │   ├── reporter.py    log-channel reporter (FloodWait-aware)
│   │   └── pipelines/     ongoing.py · batch.py · movie.py · rss.py · workers.py · helpers.py
│   └── modules/           Telegram command handlers (admin / settings / dashboard / …)
├── assets/                fonts, banner generator, brand artwork
├── probe_streams.py       debug helper: dump FFprobe stream info
├── update.py              /update helper: re-clones UPSTREAM_REPO
├── run.sh                 auto-restart wrapper with kernel-mem tuning
├── Dockerfile             python:3.10-slim + ffmpeg + libmagic + git
└── docker-compose.yml
```

---

## Requirements

- **Python 3.9+** (3.11 recommended)
- **FFmpeg** with `libx264` and `libopus`
- **MongoDB** (Atlas free tier works)
- **VPS** with: 2 vCPU · 4 GB RAM (7 GB recommended) · 30 GB disk
- **Telegram credentials** — `API_ID` + `API_HASH` from <https://my.telegram.org>
- **Bot token(s)** from [@BotFather](https://t.me/BotFather) — at least one; up to three for ongoing / batch / movie split

## Quick start

```bash
git clone -b Auto-Anime-V2 https://github.com/MikoxYae/Miko-Anime-Bot.git
cd Miko-Anime-Bot
pip install -r requirements.txt
cp config.env.example config.env   # fill in your values
python3 -m bot
```

Or with Docker:

```bash
docker compose up -d --build
```

The minimum keys to fill in `config.env`:

| Key            | Description                                        |
| -------------- | -------------------------------------------------- |
| `API_ID`       | Telegram API ID                                    |
| `API_HASH`     | Telegram API hash                                  |
| `BOT_TOKEN`    | Main bot token (ongoing pipeline)                  |
| `MONGO_URI`    | MongoDB connection string                          |
| `OWNER_ID`     | Your Telegram user ID                              |
| `MAIN_CHANNEL` | Channel where ongoing episode posts are sent       |
| `FILE_STORE`   | Private channel where encoded files are stored     |
| `LOG_CHANNEL`  | Channel where bot reports & errors are logged      |

## Disk / memory tuning

| Key                     | Default | Purpose                                                     |
| ----------------------- | ------- | ----------------------------------------------------------- |
| `DG_MIN_FREE_GB`        | `5`     | Refuse new downloads if free space falls below this         |
| `DG_ENCODE_HEADROOM_X`  | `3`     | Required free = `source_size × X` before an encode starts   |
| `DG_AGGRESSIVE_PCT`     | `85`    | At/above this fill %, periodic cleanup wipes everything     |
| `DG_CRITICAL_PCT`       | `92`    | At/above this fill %, refuse all new downloads + encodes    |
| `DG_WAIT_TIMEOUT_SEC`   | `300`   | Max wait for `wait_for_disk()` polling                      |
| `DG_CHECK_INTERVAL_SEC` | `15`    | Polling cadence inside `wait_for_disk()`                    |
| `ALLOW_COPY_CODECS`     | unset   | Set `1` to allow `-c:v copy` in any FFCODE (read warnings)  |

## Commands

Run `/help` inside the bot for the live list. Highlights:

**Owner only**
- `/restart`, `/update`, `/log`, `/stats`, `/shell`, `/eval`
- `/broadcast`, `/pbroadcast`, `/dbroadcast`

**Admin**
- `/settings` — inline panel for Ban / Admins / Users / Force Sub / Auto Delete / Dashboard & Queue / RSS & Manual Tasks
- Channel management is reachable from the settings panel on V2; older branches still expose individual commands.

---

## Security notice

If you previously ran the source repo with leaked credentials in `config.env` history, **rotate them now** — Telegram bot tokens, MongoDB password, and any GitHub PATs in `UPSTREAM_REPO`.

## Credits

- Original project: [MatizTech / Auto-Anime-Bot](https://github.com/MatizTech/Auto-Anime-Bot)
- Source fork: [MikoxYae / Fixed-Anime](https://github.com/MikoxYae/Fixed-Anime)
- This mirror: maintained by **MikoxYae**
