"""
pipelines/helpers.py
====================
Stateless utility functions shared by all three pipelines.

Functions
---------
_qual_file_store        — resolve per-quality file-store channel ID
_make_link              — build a bot deep-link for a FILE_STORE message
_qual_btns_to_keyboard  — build the canonical 2×2 quality button grid
_build_ending_keyboard  — parse ENDING_BUTTONS env var into an InlineKeyboard
_send_ending_post       — send the ending card to a channel
_warm_peer              — prime Pyrogram's peer cache (PEER_ID_INVALID guard)
_safe_send              — wrap a send call and retry once after warming on PEER_ID_INVALID
extra_utils             — fire-and-forget post-upload side effects
"""

import os as _os
import shutil as _shutil
from asyncio import sleep as asleep, to_thread as _to_thread

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import PeerIdInvalid, ChannelInvalid

from bot import bot, Var, bot_loop, LOGS
from bot.core.func_utils import encode
from bot.core.reporter import rep


# ── Hdri pass-through (no re-encode) ──────────────────────────────────────────
# Hdri = "HDRip" = original-quality remux.  We deliberately skip ffmpeg here and
# expose the source file under the proper Telegram filename via a hardlink (or
# a copy if the filesystem rejects hardlinks, e.g. cross-mount).  The source
# file stays intact for the subsequent 1080/720/480 encodes — deleting our
# hardlink only drops the extra dirent, not the underlying inode.
async def hdri_passthrough(src_path: str, out_dir: str, filename: str) -> str | None:
    """
    Make `src_path` available at `<out_dir>/<filename>` without re-encoding.

    Returns the destination path on success, or None if the source is missing
    or both hardlink + copy fail.  Safe to call in an async context — does the
    blocking work in a thread.
    """
    if not src_path or not _os.path.isfile(src_path):
        LOGS.error(f"hdri_passthrough: source missing → {src_path}")
        return None

    _os.makedirs(out_dir, exist_ok=True)
    dst_path = _os.path.join(out_dir, filename)

    # If a stale dst exists from a crashed previous run, drop it first
    try:
        if _os.path.exists(dst_path):
            _os.remove(dst_path)
    except Exception as e:
        LOGS.warning(f"hdri_passthrough: could not remove stale {dst_path}: {e}")

    def _link_or_copy():
        try:
            _os.link(src_path, dst_path)            # cheap — same inode
            return "hardlink"
        except OSError:
            _shutil.copy2(src_path, dst_path)        # fallback — uses disk
            return "copy"

    try:
        method = await _to_thread(_link_or_copy)
        size_mb = _os.path.getsize(dst_path) // (1024 * 1024)
        LOGS.info(
            f"📎 hdri_passthrough: {method} {src_path} → {dst_path} ({size_mb} MB)"
        )
        return dst_path
    except Exception as e:
        LOGS.error(f"hdri_passthrough: failed to publish Hdri: {e}")
        return None


from .constants import QUAL_LABELS


# ── Per-quality file store resolver ──────────────────────────────────────────
# Maps (pipeline, qual) → the Var attribute name for that channel.
# Falls back to the pipeline-level FILE_STORE when no per-quality channel is set.
_QUAL_STORE_ATTRS = {
    "ongoing": {"Hdri": "FILE_STORE_HDRI", "1080": "FILE_STORE_1080",
                "720":  "FILE_STORE_720",  "480":  "FILE_STORE_480"},
    "batch":   {"Hdri": "BATCH_FILE_STORE_HDRI", "1080": "BATCH_FILE_STORE_1080",
                "720":  "BATCH_FILE_STORE_720",  "480":  "BATCH_FILE_STORE_480"},
    "movie":   {"Hdri": "MOVIE_FILE_STORE_HDRI", "1080": "MOVIE_FILE_STORE_1080",
                "720":  "MOVIE_FILE_STORE_720",  "480":  "MOVIE_FILE_STORE_480"},
}


def _qual_file_store(qual: str, default_store: int, pipeline: str = "ongoing") -> int:
    """
    Return the file-store channel ID for this quality + pipeline combination.

    Looks up the optional per-quality env var (e.g. FILE_STORE_1080).  If the
    variable is unset or zero the pipeline-level default_store is returned.
    This lets admins split different qualities across different private channels
    without changing any pipeline logic.
    """
    attr = (_QUAL_STORE_ATTRS.get(pipeline) or {}).get(qual)
    if attr:
        v = getattr(Var, attr, None)
        if v:
            return v
    return default_store


# ── Bot username cache (keyed by Pyrogram client identity) ────────────────────
# When batch_bot and movie_bot use different tokens their usernames differ.
# Caching by id(client) instead of a single string prevents the wrong username
# being embedded in deep-links (which would make them non-functional).
_bot_usernames: dict = {}  # { id(pyrogram_client): username_str }


async def _make_link(msg_id: int, file_store: int = None, upload_bot=None) -> str:
    """
    Build a bot deep-link for the given FILE_STORE message ID.

    Format:  https://telegram.me/<botname>?start=<base64(get-{abs_store}-{msg_id})>

    The compact `get-{store}-{id}` payload stays well under Telegram's 64-char
    deep-link limit.  Batch range links use `get-{store}-{first}-{last}`.
    """
    client = upload_bot if upload_bot else bot
    key = id(client)
    if key not in _bot_usernames:
        _bot_usernames[key] = (await client.get_me()).username
    fs = file_store if file_store else Var.FILE_STORE
    encoded = await encode(f"get-{abs(fs)}-{msg_id}")
    return f"https://telegram.me/{_bot_usernames[key]}?start={encoded}"


# ── Quality button layout ─────────────────────────────────────────────────────
# Canonical row assignment for the 2×2 grid:
#   Row 0: 480p | 720p
#   Row 1: 1080p | Hdrip
_QUAL_ROW = {"480": 0, "720": 0, "1080": 1, "Hdri": 1}


def _qual_btns_to_keyboard(qual_links: dict) -> InlineKeyboardMarkup | None:
    """
    Build the standard 2×2 quality button grid from a {qual: url} dict.

    Layout:
        [ 480p  ]  [ 720p  ]
        [ 1080p ]  [ Hdrip ]

    Absent qualities are omitted — the grid degrades gracefully.
    An empty qual_links returns None (caller should send no keyboard).
    """
    row0, row1 = [], []
    for qual in ("480", "720", "1080", "Hdri"):  # fixed display order
        url = qual_links.get(qual)
        if not url:
            continue
        btn = InlineKeyboardButton(QUAL_LABELS.get(qual, qual), url=url)
        (row0 if _QUAL_ROW[qual] == 0 else row1).append(btn)
    rows = [r for r in (row0, row1) if r]
    return InlineKeyboardMarkup(rows) if rows else None


# ── Ending post ───────────────────────────────────────────────────────────────

def _build_ending_keyboard() -> InlineKeyboardMarkup | None:
    """
    Parse ENDING_BUTTONS into an InlineKeyboardMarkup.

    Env-var format:
        "Label|url Label2|url2\\nLabel3|url3"
        - Space-separated pairs share a row
        - Literal \\n (backslash-n in the env string) starts a new row

    Returns None when ENDING_BUTTONS is empty or unparseable.
    """
    raw = (Var.ENDING_BUTTONS or "").strip()
    if not raw:
        return None
    rows = []
    for line in raw.split("\\n"):
        line = line.strip()
        if not line:
            continue
        row = []
        for pair in line.split(" "):
            pair = pair.strip()
            if "|" not in pair:
                continue
            label, url = pair.split("|", 1)
            if label.strip() and url.strip():
                row.append(InlineKeyboardButton(label.strip(), url=url.strip()))
        if row:
            rows.append(row)
    return InlineKeyboardMarkup(rows) if rows else None


async def _send_ending_post(client, channel_id: int) -> int | None:
    """
    Send the ending card to channel_id.

    Uses ENDING_IMAGE (photo) when set, otherwise sends an invisible LTR-mark
    so Telegram accepts a keyboard-only message.

    Returns the new message ID (stored in DB so the next run can delete it
    before posting a fresh ending card).
    """
    kb = _build_ending_keyboard()
    try:
        if Var.ENDING_IMAGE:
            msg = await client.send_photo(
                channel_id, photo=Var.ENDING_IMAGE, caption="", reply_markup=kb
            )
        else:
            # U+200E Left-to-Right Mark — Telegram requires non-empty text
            msg = await client.send_message(
                channel_id, text="\u200e", reply_markup=kb
            )
        return msg.id
    except Exception:
        return None


# ── Peer warming + safe send ──────────────────────────────────────────────────
#
# Why this exists
# ---------------
# Pyrofork's MTProto session caches "peer access hashes" per chat. After a bot
# restart (or after a brand-new dedicated channel is connected via /addchannel)
# the bot may not yet have the peer cached, and the first send_photo / send_message
# to that channel fails with PEER_ID_INVALID.
#
# Our startup verification only warms FILE_STORE / BATCH_FILE_STORE / MOVIE_FILE_STORE
# — it does NOT warm dedicated channels (because they're added at runtime via
# /addchannel and stored in the DB).
#
# When the pipeline finished encoding and tried to post the channel card to a
# fresh dedicated channel, it raised PEER_ID_INVALID, the worker swallowed the
# traceback to the log channel (which itself may have failed), and the encoded
# file just sat on disk. From the user's point of view the bot "died after
# encoding" — really it just couldn't post.
#
# `_warm_peer()` calls get_chat() which forces Pyrofork to resolve the peer and
# cache its access hash. `_safe_send()` wraps a send_* coroutine and, if it
# fails with PEER_ID_INVALID / ChannelInvalid, warms the peer and retries once.

async def _warm_peer(client, chat_id) -> bool:
    """
    Force Pyrofork to resolve and cache the access hash for chat_id.

    Returns True on success, False on any failure (caller can still attempt
    to send — failure here just means the cache miss persists).
    """
    if not chat_id:
        return False
    try:
        await client.get_chat(chat_id)
        return True
    except Exception as e:
        await rep.report(
            f"⚠️ _warm_peer failed for {chat_id}: {type(e).__name__}: {e}",
            "warning", log=False,
        )
        return False


async def _safe_send(client, send_fn, chat_id, *args, _label: str = "", **kwargs):
    """
    Call send_fn(chat_id, *args, **kwargs) with one PEER_ID_INVALID retry.

    On PeerIdInvalid / ChannelInvalid we warm the peer via get_chat() and
    retry exactly once. All other exceptions propagate immediately so the
    caller's existing error handling (rep.report + retry counter) still runs.
    """
    try:
        return await send_fn(chat_id, *args, **kwargs)
    except (PeerIdInvalid, ChannelInvalid) as e:
        LOGS.warning(
            f"⚠️ {_label or send_fn.__name__} → {chat_id} hit "
            f"{type(e).__name__} — warming peer and retrying once"
        )
        warmed = await _warm_peer(client, chat_id)
        if not warmed:
            await asleep(1.0)
        return await send_fn(chat_id, *args, **kwargs)


# ── Post-upload side effects ──────────────────────────────────────────────────

async def extra_utils(msg_id: int, out_path: str):
    """
    Fire-and-forget hook called after each successful file upload.

    Currently just logs the event.  Extend this to add thumbnail generation,
    analytics pings, or any other side effect that shouldn't block the pipeline.
    """
    try:
        await rep.report(f"Extra Utils for msg_id={msg_id}", "info", log=False)
    except Exception as e:
        await rep.report(f"Error in extra_utils: {e}", "error")
