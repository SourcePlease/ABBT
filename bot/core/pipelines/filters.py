"""
pipelines/filters.py
====================
Pure classification helpers that decide which pipeline a torrent belongs to,
and gate functions (dual-audio filter, skip-keyword filter) that short-circuit
processing before any download starts.

All functions are synchronous unless AniList lookups are required (prefixed
with `await`).  No I/O beyond in-memory string operations.
"""

import re as _re

from bot.core.text_utils import _normalize_anime_title


# ── Keyword list: torrent names that are never anime video episodes ───────────
# This list intentionally covers non-English subtitle releases — the bot targets
# English/Dual/Multi audio only.  Language codes follow Nyaa tag conventions.
SKIP_KEYWORDS = [
    "vol.", "volume", "巻", " manga", "novel", "ost", "soundtrack",
    "bd box", "blu-ray box", "scan", " ch.", "comic",
    # Non-English subtitle releases
    "sub. español", "sub español", "[español]", "(español)",
    "spanish sub", "spanish dub",
    "french sub", "french dub", "[french]", "(french)", "vostfr",
    "german sub", "german dub", "[german]", "(german)",
    "italian sub", "italian dub", "[italian]", "(italian)",
    "portuguese sub", "[portuguese]", "(portuguese)",
    "arabic sub", "[arabic]", "(arabic)",
    "turkish sub", "[turkish]", "(turkish)",
    "russian sub", "[russian]", "(russian)",
    "polish sub", "[polish]", "(polish)",
    "indonesian sub", "[indonesian]", "(indonesian)",
    "malay sub", "[malay]",
    "thai sub", "[thai]",
    "vietnamese sub", "[vietnamese]",
    "hindi sub", "[hindi]", "(hindi)",
    "chinese sub", "[chinese]", "(chinese)",
    "korean sub", "[korean]", "(korean)",
]

# Keywords that indicate a batch/completed release regardless of the is_batch flag
_BATCH_KEYWORDS = [
    "bdrip", "bd rip", "bluray", "blu-ray", "bd box",
    "bdremux", "bd remux", "remux",
    "complete series", "complete season", "season pack",
]

# Keywords that identify a dual/multi audio track in the release name
_DUAL_KEYWORDS = [
    "dual audio", "dual-audio", "dualaudio", "multi-audio",
    "dual aac", "dual dts", "dual flac", "jpn-eng", "eng-jpn",
    "eng+jpn", "2 audio",
]

# Keywords that mark a movie release
_MOVIE_KEYWORDS = [
    " movie", " film", " gekijouban", " theatrical",
    "(movie)", "(film)", "[movie]", "[film]",
]

# Keywords that mark a special/OVA release (used inside the dual-audio gate)
_SPECIAL_KEYWORDS = [
    " ova", "(ova)", "[ova]", " oav", " oad",
    " special", "(special)", "[special]",
]


def should_skip(name: str) -> bool:
    """Return True if `name` matches a skip keyword — torrent should be ignored."""
    tl = name.lower()
    return any(kw in tl for kw in SKIP_KEYWORDS)


def _is_batch_task(name: str, is_batch: bool) -> bool:
    """
    True if this task should route to the batch worker (completed anime).

    Returns True when:
    - The caller explicitly set is_batch=True  (admin /processbatch command)
    - The name contains known BDRip/Bluray/season-pack keywords
    - The name contains a bare [BD] or (BD) tag  (e.g. "[Anime Time] Show [BD]")
    """
    if is_batch:
        return True
    tl = name.lower()
    if any(k in tl for k in _BATCH_KEYWORDS):
        return True
    # Bare [BD]/(BD) — avoid matching e.g. "Rebuild" which contains "bd"
    if _re.search(r'(?:^|[\[\(\s])bd(?:[\]\)\s]|$)', tl):
        return True
    return False


def _is_movie_task(name: str, is_movie: bool = False) -> bool:
    """
    True if this torrent is an anime movie.

    Checks the explicit flag first, then scans for title keywords used on Nyaa
    for movie releases (Film, Movie, Gekijouban, Theatrical).
    """
    if is_movie:
        return True
    tl = name.lower()
    return any(k in tl for k in _MOVIE_KEYWORDS)


def _is_dual_audio(name: str) -> bool:
    """Return True if the release name advertises a dual/multi audio track."""
    tl = name.lower()
    return any(k in tl for k in _DUAL_KEYWORDS)


def _is_batch_release(name: str, is_batch: bool) -> bool:
    """
    True if the release is a batch/BDRip regardless of the is_batch flag.
    Used inside the dual-audio gate to decide whether to allow a dual-audio single.
    """
    tl = name.lower()
    return _is_batch_task(name, is_batch) or any(k in tl for k in [
        "batch", "complete", "bdrip", "bd rip", "bluray", "blu-ray",
    ])


def _is_special_or_movie(name: str, is_movie: bool = False) -> bool:
    """Return True if the release is an OVA, special, or movie."""
    if _is_movie_task(name, is_movie):
        return True
    tl = name.lower()
    return any(k in tl for k in _SPECIAL_KEYWORDS + _MOVIE_KEYWORDS)


async def dual_audio_allowed(name: str, is_batch: bool, is_movie: bool,
                              force: bool, db) -> bool:
    """
    Decide whether a dual-audio single episode is allowed through.

    Policy:
      - Batch/BDRip releases:  always allowed  (admin explicitly added them)
      - Special/OVA/Movie:     always allowed
      - force=True:            always allowed   (/upload all)
      - Single TV episode:     allowed ONLY if the anime has a connected channel
                               (admin explicitly set one up, so they want dual coverage)
    
    Returns True → process the torrent.
    Returns False → skip it (log a message to the caller).
    """
    if not _is_dual_audio(name):
        # Not a dual-audio release at all — gate doesn't apply
        return True
    if _is_batch_release(name, is_batch) or _is_special_or_movie(name, is_movie) or force:
        return True

    # Single TV dual-audio episode — check for dedicated channel
    try:
        from bot.core.text_utils import TextEditor
        info = TextEditor(name)
        await info.load_anilist()
        titles = info.adata.get("title", {})
        lookup_names = [n for n in [
            titles.get("romaji"),
            titles.get("english"),
            _normalize_anime_title(name),
        ] if n and n.strip()]
        for lname in lookup_names:
            if await db.find_channel_by_anime_title(lname, db_type="ongoing"):
                return True
            if await db.find_channel_by_anime_title(lname, db_type="completed"):
                return True
    except Exception:
        pass
    return False
