from os import path as ospath, mkdir, getenv
import re
import subprocess
from logging import INFO, ERROR, FileHandler, StreamHandler, basicConfig, getLogger
from traceback import format_exc
from asyncio import Queue, Lock, Semaphore
from multiprocessing import cpu_count as _cpu_count
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.types import ChatMemberUpdated
import pyrogram.utils
pyrogram.utils.MIN_CHANNEL_ID = -1009147483647
from dotenv import load_dotenv

# FIX #2: uvloop is Linux/macOS-only. On Windows it isn't available and will
# crash the bot at import time with ModuleNotFoundError. Guard it gracefully
# so the bot still runs on Windows (development) and uses uvloop in production.
try:
    from uvloop import install
    install()
except ImportError:
    pass  # Windows or uvloop not installed — asyncio default loop is used

from logging.handlers import RotatingFileHandler as _RotatingFileHandler

basicConfig(
    format="[%(asctime)s] [%(name)s | %(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
    datefmt="%m/%d/%Y, %H:%M:%S %p",
    handlers=[
        # Rotate at 10MB, keep 3 backups → max 40MB total log history on disk.
        # Backups are named log.txt.1, log.txt.2, log.txt.3 (oldest = .3).
        # This ensures we never lose a crash that happened in a previous run
        # while also preventing the log from growing unbounded.
        _RotatingFileHandler(
            'log.txt',
            maxBytes=10 * 1024 * 1024,  # 10 MB per file
            backupCount=3,
            encoding='utf-8',
        ),
        StreamHandler(),
    ],
    level=INFO,
)

getLogger("pyrogram").setLevel(ERROR)
LOGS = getLogger(__name__)
load_dotenv('config.env')

ani_cache = {
    # Controls the RSS fetch loop — set to False via /pause, True via /resume
    'fetch_animes': True,
    # 'ongoing' and 'completed' dicts removed — episode dedup now goes directly
    # to MongoDB (anime_data collection) via db.is_episode_done().
    # 'seen_titles_ongoing' removed — torrent dedup now goes directly to
    # MongoDB (seen_torrents collection) via db.is_torrent_seen().
    # This eliminates the 2000-3000 entry dicts that caused VMS balloon.
}
ffpids_cache = list()

# FF_WORKERS controls how many FFmpeg encodes run concurrently.
# Default: min(cpu_count/2, 2) — safe ceiling for shared VPS instances.
# Each x264 encode with -threads 1 uses ~1 OS thread + ~1.5 GB RAM.
# Production VPS (4 vCPU / 7.7 GB): MAX_WORKERS=2 set in config.env.
# Increase MAX_WORKERS only if you have spare RAM (budget ~1.5 GB per worker).
_cores = _cpu_count()
_default_workers = max(1, min(_cores // 2, 2))
FF_WORKERS = int(getenv("MAX_WORKERS", str(_default_workers)))

ffLock    = Semaphore(FF_WORKERS)
ffQueue   = Queue()
ff_queued = dict()

# Shared encode semaphore — only 1 ffmpeg process runs at a time across
# both ongoing and batch pipelines. On a 2-core VPS, 2 concurrent encodes
# each spawn ~8 OS threads, saturating both cores and causing OOM/timeouts.
# Both names alias the same Semaphore(1) so all call sites work unchanged.
# Downloads can still proceed in parallel — only encoding is serialised.
# ffLock (above) is kept for the status command's worker-count display.
_encode_lock        = Semaphore(1)
ongoing_encode_lock = _encode_lock   # alias — same lock
batch_encode_lock   = _encode_lock   # alias — same lock

# Per-pipeline download locks — prevent concurrent torrent downloads from
# the same pipeline hammering the seedbox / disk simultaneously.
ongoing_dl_lock = Lock()
batch_dl_lock   = Lock()


class Var:
    API_ID, API_HASH, BOT_TOKEN = getenv("API_ID"), getenv("API_HASH"), getenv("BOT_TOKEN")
    MONGO_URI = getenv("MONGO_URI")

    # FIX #12: Reference the module-level FF_WORKERS instead of re-computing cpu_count().
    MAX_WORKERS = FF_WORKERS

    DB_NAME       = getenv("DB_NAME", "anime_bot")
    BATCH_DB_NAME = getenv("BATCH_DB_NAME") or (getenv("DB_NAME", "anime_bot") + "_batch")

    # /settings panel banner (used by bot/modules/settings.py)
    SETTINGS_PHOTO_URL = getenv(
        "SETTINGS_PHOTO_URL",
        "https://graph.org/file/b4864a63946e9b1e84238-ccb51f7ec7e7c11458.jpg",
    )

    # FIX #17: The original `exit(1)` ran inside the class body at import time,
    # meaning any import error above it would be misreported as "missing variables".
    # The check is now a proper function called explicitly after the class is defined.

    RSS_ITEMS = getenv("RSS_ITEMS", "https://subsplease.org/rss/?r=1080").split()
    FSUB_CHATS = list(map(int, getenv('FSUB_CHATS', '').split())) if getenv('FSUB_CHATS') else []
    BACKUP_CHANNEL = getenv("BACKUP_CHANNEL") or ""

    # ── Ongoing anime (airing, RSS-sourced) ──────────────────────────────────
    MAIN_CHANNEL  = int(getenv("MAIN_CHANNEL"))
    FILE_STORE    = int(getenv("FILE_STORE"))
    LOG_CHANNEL   = int(getenv("LOG_CHANNEL") or 0)
    # Per-quality file store channels (ongoing). Falls back to FILE_STORE if unset.
    FILE_STORE_HDRI = int(getenv("FILE_STORE_HDRI") or getenv("FILE_STORE"))
    FILE_STORE_1080 = int(getenv("FILE_STORE_1080") or getenv("FILE_STORE"))
    FILE_STORE_720  = int(getenv("FILE_STORE_720")  or getenv("FILE_STORE"))
    FILE_STORE_480  = int(getenv("FILE_STORE_480")  or getenv("FILE_STORE"))

    # ── Completed/batch anime (BDRip, finished shows) ────────────────────────
    BATCH_BOT_TOKEN    = getenv("BATCH_BOT_TOKEN") or ""
    BATCH_MAIN_CHANNEL = int(getenv("BATCH_MAIN_CHANNEL") or getenv("MAIN_CHANNEL"))
    BATCH_FILE_STORE   = int(getenv("BATCH_FILE_STORE") or getenv("FILE_STORE"))
    BATCH_LOG_CHANNEL  = int(getenv("BATCH_LOG_CHANNEL") or getenv("LOG_CHANNEL") or 0)
    # Per-quality file store channels (batch). Falls back to BATCH_FILE_STORE if unset.
    BATCH_FILE_STORE_HDRI = int(getenv("BATCH_FILE_STORE_HDRI") or getenv("BATCH_FILE_STORE") or getenv("FILE_STORE"))
    BATCH_FILE_STORE_1080 = int(getenv("BATCH_FILE_STORE_1080") or getenv("BATCH_FILE_STORE") or getenv("FILE_STORE"))
    BATCH_FILE_STORE_720  = int(getenv("BATCH_FILE_STORE_720")  or getenv("BATCH_FILE_STORE") or getenv("FILE_STORE"))
    BATCH_FILE_STORE_480  = int(getenv("BATCH_FILE_STORE_480")  or getenv("BATCH_FILE_STORE") or getenv("FILE_STORE"))

    # ── Movies ───────────────────────────────────────────────────────────────
    # MOVIE_MAIN_CHANNEL : where movie announcement posts go (fallback when no dedicated channel)
    # MOVIE_FILE_STORE   : private channel where encoded movie files are stored
    # MOVIE_BOT_TOKEN    : optional separate bot token for movie uploads (avoids rate limits)
    # MOVIE_DB_NAME      : separate MongoDB DB for movie task persistence
    MOVIE_BOT_TOKEN    = getenv("MOVIE_BOT_TOKEN") or ""
    MOVIE_MAIN_CHANNEL = int(getenv("MOVIE_MAIN_CHANNEL") or getenv("MAIN_CHANNEL"))
    MOVIE_FILE_STORE   = int(getenv("MOVIE_FILE_STORE") or getenv("FILE_STORE"))
    MOVIE_LOG_CHANNEL  = int(getenv("MOVIE_LOG_CHANNEL") or getenv("LOG_CHANNEL") or 0)
    MOVIE_DB_NAME      = getenv("MOVIE_DB_NAME") or (getenv("DB_NAME", "anime_bot") + "_movies")
    # Per-quality file store channels (movie). Falls back to MOVIE_FILE_STORE if unset.
    MOVIE_FILE_STORE_HDRI = int(getenv("MOVIE_FILE_STORE_HDRI") or getenv("MOVIE_FILE_STORE") or getenv("FILE_STORE"))
    MOVIE_FILE_STORE_1080 = int(getenv("MOVIE_FILE_STORE_1080") or getenv("MOVIE_FILE_STORE") or getenv("FILE_STORE"))    MOVIE_FILE_STORE_720  = int(getenv("MOVIE_FILE_STORE_720")  or getenv("MOVIE_FILE_STORE") or getenv("FILE_STORE"))
    MOVIE_FILE_STORE_480  = int(getenv("MOVIE_FILE_STORE_480")  or getenv("MOVIE_FILE_STORE") or getenv("FILE_STORE"))

    # Owner system instead of multiple admins
    OWNER    = getenv("OWNER", "Lucifer3000")
    OWNER_ID = int(getenv("OWNER_ID", "917790252"))

    SEND_SCHEDULE = getenv("SEND_SCHEDULE", "False").lower() == "true"
    BRAND_UNAME   = getenv("BRAND_UNAME", "@username")
    # FIX: All four defaults previously used `-map 0 -ac 2` which copies ALL
    # streams (video + every audio track + all subs + attachments). On multi-
    # stream sources (e.g. Dual Audio BDRips with 8+ streams), FFmpeg errors
    # with "put stream 0:N" because it can't downmix multiple audio streams
    # to stereo simultaneously. Fixed with selective stream mapping:
    #   -map 0:v       → first/best video stream only
    #   -map 0:a       → first/best audio stream only
    #   -map 0:s:m:language:eng:?  → English subs if present, ?: = don't fail if missing
    # NOTE: -threads 1 -x264-params threads=1 caps each encode to 1 OS thread.
    # With MAX_WORKERS=2 this means max 2 threads total from x264 — safe for
    # a 4-vCPU VPS with 7.7 GB RAM shared with the manga bot.
    FFCODE_Hdri   = getenv("FFCODE_Hdri") or (
        "ffmpeg -hwaccel auto -i '{}' -progress '{}' -nostats -loglevel warning "
        "-map 0:v -map 0:a -map 0:s:m:language:eng:? "
        "-c:v libx264 -pix_fmt yuv420p -vf \"scale=640:360:flags=lanczos\" "
        "-crf 32 -preset veryfast -tune animation -threads 1 -x264-params threads=1 "
        "-c:a libopus -b:a 48k -ac 2 "
        "-c:s copy -movflags +faststart '{}' -y"
    )
    FFCODE_1080   = getenv("FFCODE_1080") or (
        "ffmpeg -hwaccel auto -i '{}' -progress '{}' -nostats -loglevel warning "
        "-map 0:v -map 0:a -map 0:s:m:language:eng:? "
        "-c:v libx264 -pix_fmt yuv420p -vf \"scale=1920:1080:flags=lanczos\" "
        "-crf 26 -preset veryfast -tune animation -threads 1 -x264-params threads=1 "
        "-c:a libopus -b:a 128k -ac 2 "
        "-c:s copy -movflags +faststart '{}' -y"
    )
    FFCODE_720    = getenv("FFCODE_720")  or (
        "ffmpeg -hwaccel auto -i '{}' -progress '{}' -nostats -loglevel warning "
        "-map 0:v -map 0:a -map 0:s:m:language:eng:? "
        "-c:v libx264 -pix_fmt yuv420p -vf \"scale=1280:720:flags=lanczos\" "
        "-crf 26 -preset veryfast -tune animation -threads 1 -x264-params threads=1 "
        "-c:a libopus -b:a 96k -ac 2 "
        "-c:s copy -movflags +faststart '{}' -y"
    )
    FFCODE_480    = getenv("FFCODE_480")  or (
        "ffmpeg -hwaccel auto -i '{}' -progress '{}' -nostats -loglevel warning "
        "-map 0:v -map 0:a -map 0:s:m:language:eng:? "
        "-c:v libx264 -pix_fmt yuv420p -vf \"scale=854:480:flags=lanczos\" "
        "-crf 30 -preset veryfast -tune animation -threads 1 -x264-params threads=1 "
        "-c:a libopus -b:a 64k -ac 2 "        "-c:s copy -movflags +faststart '{}' -y"
    )

    # FIX #13: Original default "480 720 1080 Hdri " had a trailing space.
    # Trailing spaces in env var defaults are subtle bugs — cleaned up.
    QUALS = getenv("QUALS", "Hdri 1080 720 480").split()
    # Max episodes to encode+upload per chunk before moving to next chunk
    # Remaining downloaded files stay on disk until their chunk is reached
    ENCODE_CHUNK = int(getenv("ENCODE_CHUNK", "20"))

    AS_DOC       = getenv("AS_DOC", "True").lower() == "true"
    THUMB        = getenv("THUMB", "https://te.legra.ph/file/621c8d40f9788a1db7753.jpg")
    AUTO_DEL     = getenv("AUTO_DEL", "True").lower() == "true"
    DEL_TIMER    = int(getenv("DEL_TIMER", "600"))
    START_PHOTO  = getenv("START_PHOTO", "https://te.legra.ph/file/120de4dbad87fb20ab862.jpg")
    START_MSG    = getenv("START_MSG", "<b>Hey {first_name}</b>,\n\n    <i>I am Auto Animes Store & Automater Encoder Build with ❤️ !!</i>")
    START_BUTTONS = getenv("START_BUTTONS", "UPDATES|https://telegram.me/Matiz_Tech SUPPORT|https://t.me/+p78fp4UzfNwzYzQ5")

    # ── Ending post — sent after every episode post (ongoing) and batch complete ──
    # Format: "Label|url Label2|url2" — each pair is one button, pairs on same
    # line share a row, use newline separator (\n) in env for new rows.
    # Default layout matches the image:
    #   [🎬 Anime Channel] [📚 Manga Channel]
    #   [⚔️ Team Warlords]
    ENDING_BUTTONS = getenv(
        "ENDING_BUTTONS",
        "🎬 Anime Channel|https://t.me/TeamWarlords_Anime 📚 Manga Channel|https://t.me/TeamWarlords_Manga\n⚔️ Team Warlords|https://t.me/Team_Warlords_Official"
    )
    ENDING_IMAGE = getenv("ENDING_IMAGE", "")   # optional photo URL/file_id for ending card

    WAIT_MSG    = "<b>Please wait...</b>"
    REPLY_ERROR = "<b>Pʟᴇᴀsᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ ɪᴛ.</b>"


# FIX #17: Validate required env vars AFTER the class is fully defined, not
# inside the class body. This way a genuine import error above won't be masked
# by a misleading "Important Variables Missing" critical log.
def _check_required_vars():
    missing = [name for name, val in [
        ("API_ID",    Var.API_ID),
        ("API_HASH",  Var.API_HASH),
        ("BOT_TOKEN", Var.BOT_TOKEN),
        ("MONGO_URI", Var.MONGO_URI),
    ] if not val]
    if missing:
        LOGS.critical(f"Important Variables Missing: {', '.join(missing)}. Fill config.env and retry. Exiting.")
        raise SystemExit(1)

_check_required_vars()

# ── FFCODE copy-codec safety check ───────────────────────────────────────────
# When FFCODE_<quality> contains `-c:v copy` (or the legacy `-vcodec copy`),
# ffmpeg copies the source video stream byte-for-byte instead of re-encoding.
# For a 1080p source this can produce a ~1.4 GB "Hdri" output instead of the
# expected ~80 MB re-encode. Uploading those huge files back-to-back to the
# same Telegram channel is what triggers FLOOD_WAITs of 10–30 minutes (which
# the silent-FloodWait fix in tguploader.py only makes *visible*, not avoided).
#
# Behaviour:
#   - If no FFCODE_* uses copy-video → nothing happens (normal path).
#   - If at least one does and ALLOW_COPY_CODECS is NOT set → refuse to start
#     with a clear LOGS.critical naming each affected quality.
#   - If at least one does and ALLOW_COPY_CODECS=1 (or true/yes/on) → start
#     anyway but emit a LOGS.warning naming each affected quality so the
#     resulting rate-limit risk is at least expected, not surprising.
_COPY_VIDEO_RE = re.compile(r"-(?:c:v|vcodec)\s+copy\b", re.IGNORECASE)

def _check_copy_codecs():
    allow_copy = getenv("ALLOW_COPY_CODECS", "0").strip().lower() in ("1", "true", "yes", "on")
    # Detection is regex-based and case-insensitive so trivial formatting
    # variants (uppercase COPY, tabs, multiple spaces, `-vcodec` instead of
    # `-c:v`) cannot bypass the guard.
    quals_using_copy = [
        q for q, ffcode in (
            ("Hdri", Var.FFCODE_Hdri),
            ("1080", Var.FFCODE_1080),
            ("720",  Var.FFCODE_720),
            ("480",  Var.FFCODE_480),
        )
        if _COPY_VIDEO_RE.search(ffcode or "")
    ]
    if not quals_using_copy:
        return
    if allow_copy:
        LOGS.warning(
            "⚠️ ALLOW_COPY_CODECS=1 — the following qualities use `-c:v copy` "
            "and will produce near-source-size outputs (often 1+ GB each). "
            "This is the documented cause of the VPS-disk-fill / SSH-lockout "
            "incident — make sure you have plenty of free disk and that "
            "diskguard MIN_FREE_GB is tuned for your VPS: "
            + ", ".join(quals_using_copy)
        )
    else:
        # RE-PROMOTED from warning → SystemExit.
        #
        # The temporary downgrade (commit 6307506) caused a real production
        # outage: with FFCODE_Hdri=`-c:v copy`, every HDRip output was source-
        # sized (~1.5 GB / 1080p episode). A 12-episode batch consumed 25-40 GB
        # of scratch space — filling the VPS root partition, taking down        # MongoDB, every co-tenant bot, and SSH access itself.
        #
        # The bot now refuses to start unless EITHER:
        #   (a) the FFCODE_<quality> env var uses a real encoder, OR
        #   (b) the operator explicitly opts in via ALLOW_COPY_CODECS=1
        #       AND has tuned diskguard thresholds for their VPS.
        #
        # The double opt-in (env var + explicit allow flag) prevents anyone
        # from accidentally re-introducing the disk-fill regression by editing
        # only one of the two.
        LOGS.critical(
            "❌ Refusing to start: the following qualities have `-c:v copy` "
            "in their FFCODE and will produce near-source-size outputs "
            "(often 1+ GB each), which has previously filled the VPS root "
            "partition and locked the operator out of SSH: "
            + ", ".join(quals_using_copy)
            + ". Either:\n"
            "  (1) fix the FFCODE_<quality> env var to use a real encoder "
            "(e.g. `-c:v libx264 ... -crf 32 -preset veryfast`), OR\n"
            "  (2) accept the risk by setting ALLOW_COPY_CODECS=1 AND "
            "verifying that diskguard (DG_MIN_FREE_GB / DG_CRITICAL_PCT) is "
            "tuned for your VPS scratch volume."
        )
        raise SystemExit(1)

_check_copy_codecs()


# ── Disk-space baseline log ──────────────────────────────────────────────────
# Print one line of disk usage at startup so the operator can see the baseline
# in the log. Subsequent encode/upload paths call diskguard.assert_*() to
# refuse new work when space runs out.
try:
    from bot.core.diskguard import log_disk_snapshot as _log_disk_snapshot
    _log_disk_snapshot(prefix="💾 Disk (startup)")
except Exception as _de:
    LOGS.warning(f"diskguard startup log skipped: {_de}")


# ── Admin filter ──────────────────────────────────────────────────────────────

async def admin_filter(_, client, update):
    """Custom filter to check if user is admin or owner."""
    try:
        user = getattr(update, "from_user", None)
        if user is None:
            return False
        user_id = user.id
        if user_id == Var.OWNER_ID:
            return True        from bot.core.database import db
        return await db.is_admin(user_id)
    except Exception:
        return False

admin = filters.create(admin_filter)


# ── Thumbnail setup ───────────────────────────────────────────────────────────
#
# FIX: was `subprocess.run(["cp", ...])` and `subprocess.run(["wget", ...])`.
# Both rely on system binaries that may be missing on minimal containers
# (alpine, distroless, scratch-based images), where the bot would silently
# start without a thumbnail. Use Python stdlib (shutil + urllib) so no
# external binary is required. URL scheme is validated to refuse anything
# other than http(s) — protects against file:// / ftp:// / data:// abuse if
# someone sets THUMB to a hostile value.

import shutil as _shutil

if ospath.exists("bot/thumb.jpg"):
    try:
        _shutil.copy("bot/thumb.jpg", "thumb.jpg")
        LOGS.info("Local thumbnail loaded from bot/thumb.jpg")
    except Exception as _e:
        LOGS.warning(f"Could not copy local thumbnail: {_e}")
elif Var.THUMB and not ospath.exists("thumb.jpg"):
    try:
        from urllib.request import urlopen, Request
        from urllib.parse import urlparse
        _u = urlparse(Var.THUMB)
        if _u.scheme in ("http", "https"):
            _req = Request(Var.THUMB, headers={"User-Agent": "Fixed-Anime/1.0"})
            with urlopen(_req, timeout=15) as _r, open("thumb.jpg", "wb") as _f:
                _shutil.copyfileobj(_r, _f)
            LOGS.info("Thumbnail downloaded from URL")
        else:
            LOGS.warning(f"Refusing THUMB with non-http(s) scheme: {_u.scheme!r}")
    except Exception as _e:
        LOGS.warning(f"Failed to download thumbnail from {Var.THUMB!r}: {_e}")

for _d in ("encode/", "thumbs/", "downloads/"):
    if not ospath.isdir(_d):
        mkdir(_d)


# ── Event loop + Pyrogram clients ────────────────────────────────────────────

try:
    try:        bot_loop = asyncio.get_running_loop()
    except RuntimeError:
        bot_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(bot_loop)

    # Ongoing anime bot (handles RSS episodes, commands, file delivery)
    bot = Client(
        name="AutoAniAdvance",
        api_id=Var.API_ID,
        api_hash=Var.API_HASH,
        bot_token=Var.BOT_TOKEN,
        plugins=dict(root="bot/modules"),
        parse_mode=ParseMode.HTML
    )

    # Batch/completed anime bot — separate token to avoid rate-limit conflicts.
    # If BATCH_BOT_TOKEN is not set, reuses the main bot (single-bot mode).
    if Var.BATCH_BOT_TOKEN:
        batch_bot = Client(
            name="AutoAniBatch",
            api_id=Var.API_ID,
            api_hash=Var.API_HASH,
            bot_token=Var.BATCH_BOT_TOKEN,
            parse_mode=ParseMode.HTML
        )
    else:
        batch_bot = None  # will be set to `bot` in __main__.main() after startup

    # Movie bot — optional separate token for movie uploads.
    # Falls back to main bot if MOVIE_BOT_TOKEN is not set.
    if Var.MOVIE_BOT_TOKEN:
        movie_bot = Client(
            name="AutoAniMovies",
            api_id=Var.API_ID,
            api_hash=Var.API_HASH,
            bot_token=Var.MOVIE_BOT_TOKEN,
            parse_mode=ParseMode.HTML
        )
    else:
        movie_bot = None  # will be set to `bot` in __main__.main() after startup

    sch = AsyncIOScheduler(timezone="Asia/Kolkata")

except Exception as ee:
    LOGS.error(str(ee))
    raise SystemExit(1)


# ── Force Subscription Event Handlers ────────────────────────────────────────
@bot.on_chat_member_updated()
async def handle_chat_members(client, chat_member_updated: ChatMemberUpdated):
    """Handle member updates for force subscription channels."""
    try:
        from bot.core.database import db
        chat_id = chat_member_updated.chat.id

        if await db.reqChannel_exist(chat_id):
            old_member = chat_member_updated.old_chat_member

            if not old_member:
                return

            # FIX #14: Original code only checked ChatMemberStatus.MEMBER,
            # missing ADMINISTRATOR and OWNER — they would not be removed from
            # the request list when they left. Check all non-bot member statuses.
            leaving_statuses = {
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            }
            if old_member.status in leaving_statuses:
                user_id = old_member.user.id
                if await db.req_user_exist(chat_id, user_id):
                    await db.del_req_user(chat_id, user_id)
                    LOGS.info(f"Removed user {user_id} from request list for channel {chat_id}")
    except Exception as e:
        LOGS.error(f"Error in handle_chat_members: {e}")


@bot.on_chat_join_request()
async def handle_join_request(client, chat_join_request):
    """Handle join requests for force subscription channels."""
    try:
        from bot.core.database import db
        chat_id = chat_join_request.chat.id
        user_id = chat_join_request.from_user.id

        if await db.reqChannel_exist(chat_id):
            if not await db.req_user_exist(chat_id, user_id):
                await db.req_user(chat_id, user_id)
                LOGS.info(f"Added user {user_id} to request list for channel {chat_id}")
    except Exception as e:
        LOGS.error(f"Error in handle_join_request: {e}")
