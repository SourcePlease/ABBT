"""
pipelines/constants.py
======================
Shared constants referenced by every pipeline module.

Keeping them here (rather than in each pipeline) means a single edit updates
all three pipelines at once and avoids circular imports.
"""

# ── Quality display labels (Unicode bold digits) ──────────────────────────────
QUAL_LABELS = {
    'Hdri': '𝗛𝗗𝗥𝗶𝗽',
    '1080': '𝟭𝟬𝟴𝟬𝗽',
    '720':  '𝟳𝟮𝟬𝗽',
    '480':  '𝟰𝟴𝟬𝗽',
}

# ── Audio track display labels ────────────────────────────────────────────────
AUDIO_LABELS = {
    'Sub':         '🇯🇵 Sub',
    'Dual':        '🎌 Dual',
    'Multi-Audio': '🌐 Multi',
}

# ── RSS feed priority map (lower = processed first) ───────────────────────────
# SubsPlease is the canonical ongoing-anime source. Ember and Erai-raws are
# secondary; they carry the same shows occasionally and serve as fallbacks.
RSS_PRIORITY_MAP = {
    "subsplease": 0,
    "ember":      1,
    "erai-raws":  2,
}

# ── Channel post sticker file IDs ─────────────────────────────────────────────
# These are Telegram sticker file IDs — replace if the sticker pack changes.
STICKER_DEDICATED = "CAACAgUAAxkBAAEPRkposlhdldSDTJtDtIG1UPqyLh1xegADFQAClP0pVztrIQO4kT1INgQ"
STICKER_MAIN      = "CAACAgUAAxUAAWnCPX624169OQG3chHaQqohS3NiAAJdHAACeS2RVSI15ydYuGKoOgQ"

# ── Video file extensions the pipelines recognise ────────────────────────────
VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm"}

# ── Skip keywords: filenames/folder names that are never episode files ────────
SKIP_WORDS   = {"extra", "extras", "ncop", "nced", "oped", "pv", "special", "preview"}
SKIP_FOLDERS = {
    "nc", "nced", "ncop", "extras", "extra", "pv", "pvs",
    "creditless", "clean", "scans", "scan", "bonus", "bonuses",
    # Quality encode output sub-dirs — never treat them as source files
    "hdri", "1080", "720", "480",
}
