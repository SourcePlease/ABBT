from calendar import month_name
from datetime import datetime
from random import choice
from asyncio import sleep as asleep
from aiohttp import ClientSession
from anitopy import parse
import re
import io
import importlib.util as _ilu
from pathlib import Path as _Path

from bot import Var, bot
from .database import db
from .ffencoder import ffargs
from .func_utils import handle_logs
from .reporter import rep

# Load card_generator from animebot/assets/ (outside the bot package)
_cg_path = _Path(__file__).parents[2] / "assets" / "card_generator.py"
_cg_spec = _ilu.spec_from_file_location("card_generator", _cg_path)
_cg = _ilu.module_from_spec(_cg_spec)
_cg_spec.loader.exec_module(_cg)
make_anime_card  = _cg.make_anime_card
normalise_jikan  = _cg.normalise_jikan
normalise_anilist = _cg.normalise_anilist

# Caption format for dedicated channels (without synopsis)
DEDICATED_CAPTION_FORMAT = """
<b>{title}</b>
<b>──────────────────────────────</b>
<b>➤ Season - {season}</b>
<b>➤ Episode - {ep_no}</b>
<b>➤ Quality: Multi [{audio}]</b>
<b>──────────────────────────────</b>
"""

# Caption format for main channel (with synopsis in blockquote)
MAIN_CAPTION_FORMAT = """
<b>{title}</b>
<b>──────────────────────────────</b>
<b>➤ Season - {season}</b>
<b>➤ Episode - {ep_no}</b>
<b>➤ Quality: Multi [{audio}]</b>
<b>────────────────────────────</b>
<blockquote expandable>‣ Synopsis : {synopsis}</blockquote>
"""

# ── Movie caption formats ──────────────────────────────────────────────────────
MOVIE_MAIN_CAPTION_FORMAT = """
<b>🎬 {title}</b>
<b>──────────────────────────────</b>
<b>➤ Year     : {year}</b>
<b>➤ Duration : {duration} min</b>
<b>➤ Quality  : Multi [{audio}]</b>
<b>────────────────────────────</b>
<blockquote expandable>‣ Synopsis : {synopsis}</blockquote>
"""

MOVIE_DEDICATED_CAPTION_FORMAT = """
<b>🎬 {title}</b>
<b>──────────────────────────────</b>
<b>➤ Year     : {year}</b>
<b>➤ Duration : {duration} min</b>
<b>➤ Quality  : Multi [{audio}]</b>
<b>──────────────────────────────</b>
"""

GENRES_EMOJI = {"Action": "👊", "Adventure": choice(['🪂', '🧗‍♀']), "Comedy": "🤣", "Drama": " 🎭", "Ecchi": choice(['💋', '🥵']), "Fantasy": choice(['🧞', '🧞‍♂', '🧞‍♀','🌗']), "Hentai": "🔞", "Horror": "☠", "Mahou Shoujo": "☯", "Mecha": "🤖", "Music": "🎸", "Mystery": "🔮", "Psychological": "♟", "Romance": "💞", "Sci-Fi": "🛸", "Slice of Life": choice(['☘','🍁']), "Sports": "⚽️", "Supernatural": "🫧", "Thriller": "🔥"}

ANIME_GRAPHQL_QUERY = """
query ($id: Int, $search: String, $seasonYear: Int) {
  Media(id: $id, type: ANIME, format_not_in: [MOVIE, MUSIC, MANGA, NOVEL, ONE_SHOT], search: $search, seasonYear: $seasonYear) {
    id
    idMal
    title {
      romaji
      english
      native
    }
    type
    format
    status(version: 2)
    description(asHtml: false)
    startDate {
      year
      month
      day
    }
    endDate {
      year
      month
      day
    }
    season
    seasonYear
    episodes
    duration
    chapters
    volumes
    countryOfOrigin
    source
    hashtag
    trailer {
      id
      site
      thumbnail
    }
    updatedAt
    coverImage {
      extraLarge
      large
    }
    bannerImage
    genres
    synonyms
    averageScore
    meanScore
    popularity
    trending
    favourites
    studios {
      nodes {
         name
         siteUrl
      }
    }
    isAdult
    relations {
      edges {
        relationType
        node { id format }
      }
    }
    nextAiringEpisode {
      airingAt
      timeUntilAiring
      episode
    }
    airingSchedule {
      edges {
        node {
          airingAt
          timeUntilAiring
          episode
        }
      }
    }
    externalLinks {
      url
      site
    }
    siteUrl
  }
}
"""

# ── Separate AniList query for movies (format: MOVIE only) ───────────────────
MOVIE_GRAPHQL_QUERY = """
query ($search: String) {
  Media(type: ANIME, format: MOVIE, search: $search) {
    id
    idMal
    title {
      romaji
      english
      native
    }
    format
    status(version: 2)
    description(asHtml: false)
    startDate { year month day }
    duration
    coverImage { extraLarge large }
    bannerImage
    genres
    averageScore
    siteUrl
  }
}
"""

# ── AniList query: fetch all movies in a franchise ───────────────────────────
FRANCHISE_MOVIES_QUERY = """
query ($id: Int, $search: String) {
  Media(id: $id, type: ANIME, search: $search) {
    id
    idMal
    title { romaji english }
    format
    startDate { year month day }
    relations {
      edges {
        relationType
        node {
          id
          idMal
          title { romaji english }
          format
          startDate { year month day }
          duration
          coverImage { large }
          status
          description(asHtml: false)
        }
      }
    }
  }
}
"""

# In-memory AniList cache — avoids repeated API calls for the same anime.
# Bounded LRU: oldest entries evicted once the cap is reached so the cache
# can't grow unbounded across long uptimes (was a slow memory leak).
from collections import OrderedDict as _OD


class _LruCache(_OD):
    """Tiny OrderedDict-based LRU with write-time eviction."""
    _MAX = 500

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        while len(self) > self._MAX:
            self.popitem(last=False)

    def __getitem__(self, key):
        v = super().__getitem__(key)
        self.move_to_end(key)
        return v


_anilist_cache: _LruCache = _LruCache()

# ── Abbreviated title aliases ─────────────────────────────────────────────────
# Maps shortened/group-specific torrent names → real AniList-searchable title.
# Add entries here whenever a group uses an abbreviation AniList can't resolve.
# Format: { "normalized_lower": "AniList searchable title" }
TITLE_ALIASES: dict[str, str] = {
    "lastame":        "Higeki no Genkyou to Naru Saikyou Gedou Last Boss Joou wa Tami no Tame ni Tsukushimasu",
    "lastame s2":     "Higeki no Genkyou to Naru Saikyou Gedou Last Boss Joou wa Tami no Tame ni Tsukushimasu Season 2",
    # Add more as needed: "abbreviation": "Full AniList title"
}


def _normalize_anime_title(name: str) -> str:
    """
    Strip torrent junk from a title so AniList can find it.
    E.g. '[SubsPlease] World's End Harem - 01 (1080p) [ABC]' → "World's End Harem"
    Also handles BDRip style: '[BDRip] World's End Harem [1080P-HEVC,...]' → "World's End Harem"
    """
    import re
    title = re.sub(r'^\[.*?\]\s*', '', name).strip()
    title = re.sub(r'\s*[\(\[].*', '', title).strip()
    title = re.sub(r'\s*-\s*\d+.*', '', title).strip()
    title = re.sub(r'\s+[Ss]\d+.*', '', title).strip()
    title = re.sub(r'\s*\|.*', '', title).strip()
    return title


# Bounded LRU (cap 500) — was an unbounded dict; over long uptimes every
# failed AniList lookup leaked an entry. Same eviction semantics as
# _anilist_cache so memory stays flat regardless of how many anime have failed.
_anilist_failed_cache: _LruCache = _LruCache()
_FAILED_CACHE_TTL = 3600  # seconds before a failed lookup is retried


def _titles_match(query: str, result_titles: dict) -> bool:
    """Check if AniList result is actually relevant to the query using word overlap."""
    q = query.lower().strip()
    for t in result_titles.values():
        if not t:
            continue
        t = t.lower().strip()
        q_words = set(w for w in q.split() if len(w) > 2)
        t_words = set(w for w in t.split() if len(w) > 2)
        if q_words & t_words:
            return True
    return False


async def _fetch_jikan(mal_id: int) -> dict:
    """
    Fetch /anime/{mal_id} from Jikan v4.
    Returns the `data` sub-dict on success, empty dict on failure.
    """
    url = f"https://api.jikan.moe/v4/anime/{mal_id}"
    try:
        async with ClientSession() as sess:
            async with sess.get(url) as r:
                if r.status != 200:
                    return {}
                body = await r.json(content_type=None)
                return body.get("data") or {}
    except Exception as e:
        await rep.report(f"Jikan fetch failed for mal_id={mal_id}: {e}", "warning", log=False)
        return {}


class AniLister:
    def __init__(self, anime_name: str, year: int = None) -> None:
        self.__api = "https://graphql.anilist.co"
        self.__ani_name = anime_name

    async def _post(self, variables: dict):
        async with ClientSession() as sess:
            async with sess.post(
                self.__api,
                json={'query': ANIME_GRAPHQL_QUERY, 'variables': variables}
            ) as resp:
                try:
                    body = await resp.json(content_type=None)
                except Exception:
                    body = {}
                return (resp.status, body, resp.headers)

    async def post_data(self):
        return await self._post({'search': self.__ani_name})

    async def get_anidata(self, _retries: int = 0):
        cache_key = self.__ani_name.lower()

        if cache_key in _anilist_cache:
            return _anilist_cache[cache_key]
        if cache_key in _anilist_failed_cache:
            import time as _t_fc
            if _t_fc.time() - _anilist_failed_cache[cache_key] < _FAILED_CACHE_TTL:
                return {}
            # TTL expired — remove stale entry and retry
            del _anilist_failed_cache[cache_key]

        try:
            from bot.core.database import db as _db
            _db_cached = await _db.db.anilist_cache.find_one({"cache_key": cache_key})
            if _db_cached and _db_cached.get("data"):
                _anilist_cache[cache_key] = _db_cached["data"]
        except Exception:
            pass

        import re as _re_san
        _search_name = _re_san.sub(r'[\[\](){}:;!@#$%^&*+=<>?/\\|`~]', ' ', self.__ani_name)
        _search_name = _re_san.sub(r'\s+', ' ', _search_name).strip()

        res_code, resp_json, res_heads = await self._post({'search': _search_name})

        if res_code in (400, 404):
            partial = ((resp_json or {}).get('data') or {}).get('Media') or {}
            anilist_year = (partial.get('startDate') or {}).get('year')
            if anilist_year:
                await rep.report(
                    f"AniList Pass 2: {self.__ani_name} — retrying with startDate year {anilist_year}",
                    "warning", log=False
                )
                res_code, resp_json, res_heads = await self._post(
                    {'search': self.__ani_name, 'seasonYear': anilist_year}
                )

        if res_code == 200:
            result = ((resp_json or {}).get('data') or {}).get('Media') or {}
            if result:
                _anilist_cache[cache_key] = result
                try:
                    from bot.core.database import db as _db
                    await _db.db.anilist_cache.update_one(
                        {"cache_key": cache_key},
                        {"$set": {"cache_key": cache_key, "data": result,
                                  "updated_at": __import__("datetime").datetime.utcnow()}},
                        upsert=True
                    )
                except Exception:
                    pass
            else:
                import time as _t_fc2
                _anilist_failed_cache[cache_key] = _t_fc2.time()
            return result
        elif res_code in (400, 404):
            import time as _t_fc3
            _anilist_failed_cache[cache_key] = _t_fc3.time()
            return {}
        elif res_code == 429:
            if _retries >= 5:
                await rep.report(f"AniList 429: max retries reached for {self.__ani_name}", "error")
                return {}
            # Cap Retry-After at 5 min — external server could send arbitrarily large value
            f_timer = min(int(res_heads.get('Retry-After', 60)), 300)
            await rep.report(f"AniList API FloodWait: {res_code}, Sleeping for {f_timer}s !!", "error")
            await asleep(f_timer)
            return await self.get_anidata(_retries + 1)
        elif res_code in [500, 501, 502]:
            if _retries >= 5:
                await rep.report(f"AniList 5xx: max retries reached for {self.__ani_name}", "error")
                return {}
            _backoff = min(5 * (2 ** _retries), 120)  # exponential backoff, cap 2 min
            await rep.report(f"AniList Server API Error: {res_code}, Waiting {_backoff}s to Try Again !!", "error")
            await asleep(_backoff)
            return await self.get_anidata(_retries + 1)
        else:
            try:
                from bot.core.database import db as _db
                _cached = await _db.db.anilist_cache.find_one({"cache_key": cache_key})
                if _cached and _cached.get("data"):
                    await rep.report(
                        f"⚠️ AniList down (HTTP {res_code}) — using DB cache for: {self.__ani_name}",
                        "warning", log=False
                    )
                    _anilist_cache[cache_key] = _cached["data"]
                    return _cached["data"]
            except Exception:
                pass
            await rep.report(f"AniList API Error: {res_code}", "error", log=False)
            return {}


import re

_nlp = None
_spacy_available = False

def _load_spacy():
    global _nlp, _spacy_available
    if _nlp is not None:
        return
    try:
        import spacy as _spacy
        _nlp = _spacy.load("en_core_web_sm")
        _spacy_available = True
    except Exception:
        _nlp = None
        _spacy_available = False


ANIME_STOP_WORDS = {
    "of", "the", "a", "an", "with", "does", "not", "in", "on", "at", "and", "to"
}

async def shorten_title(anime_title: str, max_len: int):
    """Shorten a title to fit within max_len *bytes* (UTF-8).
    Telegram enforces a 255-byte filename limit, not a character limit,
    so CJK/Unicode titles must be measured in bytes."""
    clean_title = anime_title.replace("/", " ")
    clean_title = re.sub(r'[<>:"\\|?*]', '', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()

    if len(clean_title.encode("utf-8")) <= max_len:
        return clean_title

    words = clean_title.split()
    while words:
        candidate = " ".join(words)
        if len(candidate.encode("utf-8")) <= max_len:
            return candidate
        words.pop()

    # Fallback: hard-truncate by bytes without splitting a multibyte char
    encoded = clean_title.encode("utf-8")[:max_len]
    return encoded.decode("utf-8", errors="ignore").rsplit(" ", 1)[0]


def detect_audio(name: str) -> str:
    n = name.lower()
    multi_keywords = [
        "multi-audio", "multiaudio", "multi audio",
        "multi-subs", "multi subs"
    ]
    dual_keywords = [
        "dual", "dual-audio", "dualaudio", "dual audio",
        "dual aac", "dual dts", "dual flac",
        "jpn-eng", "eng-jpn", "japanese english", "eng+jpn",
        "audios (2)", "audio (2)", "2 audio"
    ]
    if any(k in n for k in multi_keywords):
        return "Multi-Audio"
    if any(k in n for k in dual_keywords):
        return "Dual"
    return "Sub"


class TextEditor:
    def __init__(self, name):
        self.__name = name
        self.adata = {}
        self.pdata = parse(name)

    async def load_anilist(self):
        cache_names = []

        clean_raw = _normalize_anime_title(self.__name)

        # ── Alias lookup — resolve abbreviated group names first ──────────────
        _alias_key = clean_raw.lower().strip() if clean_raw else ""
        if _alias_key in TITLE_ALIASES:
            _alias_query = TITLE_ALIASES[_alias_key]
            adata = await AniLister(_alias_query, datetime.now().year).get_anidata()
            if adata:
                self.adata = adata
                await self._resolve_correct_season()
                return

        if clean_raw:
            cache_names.append(clean_raw)
            adata = await AniLister(clean_raw, datetime.now().year).get_anidata()
            if adata and _titles_match(clean_raw, adata.get("title") or {}):
                self.adata = adata
                await self._resolve_correct_season()
                return

            # Build fallback queries
            _no_dash  = clean_raw.replace(' - ', ' ').strip()
            _colon    = clean_raw.replace(' - ', ': ', 1).strip()
            _first    = re.split(r'\s*[-:]\s*', clean_raw)[0].strip()

            for query in dict.fromkeys([_no_dash, _colon, _first]):  # deduplicated, order preserved
                if not query or query in cache_names:
                    continue
                cache_names.append(query)
                adata = await AniLister(query, datetime.now().year).get_anidata()
                if adata and _titles_match(query, adata.get("title") or {}):
                    self.adata = adata
                    await self._resolve_correct_season()
                    return

        for option in [(False, False), (False, True), (True, False), (True, True)]:
            ani_name = await self.parse_name(*option)
            if not ani_name or ani_name in cache_names:
                continue
            cache_names.append(ani_name)
            adata = await AniLister(ani_name, datetime.now().year).get_anidata()
            if adata and _titles_match(ani_name, adata.get("title") or {}):
                self.adata = adata
                break

        await self._resolve_correct_season()

    async def _resolve_correct_season(self):
        if not self.adata:
            return

        _s_raw = self.pdata.get("anime_season", "1")
        if isinstance(_s_raw, list):
            _s_raw = _s_raw[-1]
        try:
            target_season = int(str(_s_raw).strip())
        except (ValueError, TypeError):
            target_season = 1
        if target_season <= 1:
            return

        if not (self.adata.get("relations") or {}).get("edges"):
            _norm_key = _normalize_anime_title(self.__name).lower() if self.__name else ""
            for _ck in [self.__name.lower(), _norm_key]:
                if _ck and _ck in _anilist_cache:
                    del _anilist_cache[_ck]
            if _norm_key:
                _fresh = await AniLister(_normalize_anime_title(self.__name), datetime.now().year).get_anidata()
                if _fresh and (_fresh.get("relations") or {}).get("edges"):
                    self.adata = _fresh

        import aiohttp as _aio
        _SEQUEL_Q = """
        query($id:Int){
          Media(id:$id,type:ANIME){
            id idMal title{romaji english native}
            status(version:2) episodes
            startDate{year month day}
            coverImage{extraLarge large}
            bannerImage description(asHtml:false)
            genres averageScore
            relations{
              edges{
                relationType
                node{id format}
              }
            }
          }
        }"""
        current = self.adata
        for _ in range(target_season - 1):
            _cur_id = current.get("id")
            if not _cur_id:
                break
            _edges = (current.get("relations") or {}).get("edges") or []
            _sequel_id = next(
                (e["node"]["id"] for e in _edges
                 if e.get("relationType") == "SEQUEL"
                 and e.get("node", {}).get("format") in ("TV", "TV_SHORT")),
                None
            )
            if not _sequel_id:
                break
            try:
                _sequel_data = None
                try:
                    from bot.core.database import db as _db_sq
                    _sq_cached = await _db_sq.db.anilist_cache.find_one({"cache_key": f"id:{_sequel_id}"})
                    if _sq_cached and _sq_cached.get("data"):
                        _sequel_data = _sq_cached["data"]
                except Exception:
                    pass

                if not _sequel_data:
                    async with _aio.ClientSession() as _sess:
                        async with _sess.post(
                            "https://graphql.anilist.co",
                            json={"query": _SEQUEL_Q, "variables": {"id": _sequel_id}},
                            timeout=_aio.ClientTimeout(total=8)
                        ) as _r:
                            if _r.status != 200:
                                break
                            _rd = await _r.json(content_type=None)
                    _sequel_data = ((_rd.get("data") or {}).get("Media")) or {}
                    if _sequel_data:
                        try:
                            from bot.core.database import db as _db_sq2
                            await _db_sq2.db.anilist_cache.update_one(
                                {"cache_key": f"id:{_sequel_id}"},
                                {"$set": {"cache_key": f"id:{_sequel_id}", "data": _sequel_data,
                                          "updated_at": __import__("datetime").datetime.utcnow()}},
                                upsert=True
                            )
                        except Exception:
                            pass

                if _sequel_data:
                    current = _sequel_data
            except Exception:
                break
        if current is not self.adata:
            self.adata = current

    @handle_logs
    async def get_id(self):
        if (ani_id := self.adata.get('id')) and str(ani_id).isdigit():
            return ani_id

    @handle_logs
    async def parse_name(self, no_s=False, no_y=False):
        anime_name = self.pdata.get("anime_title")
        anime_season = self.pdata.get("anime_season")
        anime_year = self.pdata.get("anime_year")
        if anime_name:
            pname = anime_name
            if not no_s and self.pdata.get("episode_number") and anime_season:
                pname += f" {anime_season}"
            if not no_y and anime_year:
                pname += f" {anime_year}"
            return pname
        return anime_name

    @handle_logs
    async def get_banner(self):
        try:
            banner = self.adata.get("bannerImage")
            if banner:
                return banner
            cover = self.adata.get("coverImage", {})
            if cover.get("extraLarge"):
                return cover["extraLarge"]
            if cover.get("large"):
                return cover["large"]
            if ani_id := await self.get_id():
                return f"https://img.anili.st/media/{ani_id}"
            return None
        except Exception:
            return None

    async def get_poster(self, upload_bot=None):
        """
        Build and return an anime info card as a Telegram file_id.

        Data priority
        ─────────────
        0. In-pipeline cache (_cached_poster_url) — set by _run_pipeline /
           _run_batch_pipeline after the first call so a 24-episode batch doesn't
           regenerate and re-upload an identical card 24 times.
        1. MAL via Jikan  (uses self.adata["idMal"])
        2. AniList  (self.adata already loaded by load_anilist())
        3. Raw cover URL if card generation fails
        4. AniList name-search cover as last real fallback

        upload_bot: the Pyrogram client that will ultimately send the returned
                    file_id.  The card is uploaded via this same bot so the
                    file_id is valid for it.  Defaults to the main bot.
        """
        try:
            # ── 0. In-pipeline cache — skip all generation on repeat calls ──
            if getattr(self, '_cached_poster_url', None):
                return self._cached_poster_url

            # ── Reload adata if it's empty (e.g. load_anilist() timed out earlier)
            if not self.adata:
                try:
                    await self.load_anilist()
                    await rep.report(f"🔄 Retried AniList load in get_poster for: {self.__name}", "info", log=False)
                except Exception:
                    pass

            # ── Resolve base cover URL from AniList adata ──────────────────
            _al_cover = (self.adata.get("coverImage") or {})
            _cover_url = (
                _al_cover.get("extraLarge")
                or _al_cover.get("large")
                or (f"https://img.anili.st/media/{await self.get_id()}"
                    if await self.get_id() else None)
            )

            # ── 2. Try MAL (Jikan) as primary card data source ─────────────
            card_data = None
            mal_id = self.adata.get("idMal")

            if mal_id:
                jikan_data = await _fetch_jikan(mal_id)
                if jikan_data:
                    mal_cover = (
                        (jikan_data.get("images") or {}).get("jpg", {}).get("large_image_url")
                        or (jikan_data.get("images") or {}).get("jpg", {}).get("image_url")
                    )
                    _cover_url = mal_cover or _cover_url
                    card_data = normalise_jikan(jikan_data)
                    await rep.report(f"🎨 Card data from MAL for: {self.__name}", "info", log=False)

            # ── 3. AniList fallback if Jikan failed ────────────────────────
            if not card_data and self.adata:
                card_data = normalise_anilist(self.adata)
                await rep.report(
                    f"🎨 Card data from AniList (MAL unavailable) for: {self.__name}",
                    "warning", log=False
                )

            # ── Propagate externally-injected seasonNumber into card_data ──
            # _run_pipeline sets aniInfo.adata["seasonNumber"] = <season int>
            # BEFORE calling get_poster(), but normalise_jikan/anilist re-derives
            # season from the raw Jikan/AniList payload which doesn't carry it.
            # Override here so the Season pill on the card always matches reality.
            if card_data and self.adata.get("seasonNumber"):
                try:
                    _sn = int(self.adata["seasonNumber"])
                    card_data["season_number_label"] = f"Season {_sn}"
                except (ValueError, TypeError):
                    pass

            # ── Inject current episode number for the episode pill on the card
            # episode_number can be a list (e.g. batch [1, 24]) — always resolve to int
            if card_data and self.pdata.get("episode_number"):
                _raw_ep = self.pdata["episode_number"]
                if isinstance(_raw_ep, list):
                    # For a batch, show the last (highest) episode number on the pill
                    _raw_ep = _raw_ep[-1] if _raw_ep else None
                try:
                    card_data["current_episode"] = int(_raw_ep) if _raw_ep is not None else None
                except (ValueError, TypeError):
                    card_data["current_episode"] = None

            # ── 4. Generate card & upload to get file_id ──────────────────
            if card_data and _cover_url:
                try:
                    import asyncio as _asyncio
                    import tempfile as _tempfile
                    import os as _os
                    jpeg_bytes = await _asyncio.get_event_loop().run_in_executor(
                        None, make_anime_card, card_data, _cover_url
                    )
                    # Write to a temp file — Pyrogram reliably accepts file paths;
                    # BytesIO can be rejected with "Invalid file" on some versions.
                    _tmp_fd, _tmp_path = _tempfile.mkstemp(suffix=".jpg")
                    try:
                        _os.write(_tmp_fd, jpeg_bytes)
                        _os.close(_tmp_fd)
                        _card_bot = upload_bot or bot
                        msg = await _card_bot.send_photo(
                            chat_id=Var.LOG_CHANNEL,
                            photo=_tmp_path,
                            disable_notification=True,
                        )
                    finally:
                        try:
                            _os.unlink(_tmp_path)
                        except Exception:
                            pass
                    return msg.photo.file_id
                except Exception as e:
                    await rep.report(
                        f"⚠️ Card generation failed for {self.__name}: {e} — using raw cover",
                        "error"
                    )

            # ── 5. Raw cover URL fallback ──────────────────────────────────
            if _cover_url:
                await rep.report(f"🎨 Using raw cover URL for: {self.__name}", "info", log=False)
                return _cover_url

            # ── 6. AniList name-search cover (last resort before giving up) ─
            try:
                import aiohttp as _aiohttp
                _search_query = """
                query($search:String){
                  Media(search:$search,type:ANIME){
                    id
                    coverImage{ extraLarge large }
                  }
                }"""
                async with _aiohttp.ClientSession() as _s:
                    async with _s.post(
                        "https://graphql.anilist.co",
                        json={"query": _search_query,
                              "variables": {"search": _normalize_anime_title(self.__name)}},
                        timeout=_aiohttp.ClientTimeout(total=8)
                    ) as _r:
                        _rdata = await _r.json(content_type=None)
                _media = ((_rdata or {}).get("data") or {}).get("Media") or {}
                _fb_id = _media.get("id")
                _fb_cover = (_media.get("coverImage") or {})
                _fb_url = (
                    _fb_cover.get("extraLarge")
                    or _fb_cover.get("large")
                    or (f"https://img.anili.st/media/{_fb_id}" if _fb_id else None)
                )
                if _fb_url:
                    await rep.report(f"🎨 AniList name-search cover fallback for: {self.__name}", "warning", log=False)
                    return _fb_url
            except Exception:
                pass

            await rep.report(f"❌ No cover found at all for: {self.__name}", "error")
            return None

        except Exception as e:
            await rep.report(f"❌ Error getting poster: {str(e)}", "error")
            _cover = (self.adata.get("coverImage") or {})
            _cover_url = _cover.get("extraLarge") or _cover.get("large")
            if _cover_url:
                return _cover_url
            if ani_id := await self.get_id():
                return f"https://img.anili.st/media/{ani_id}"
            return None

    @handle_logs
    async def get_upname(self, qual=""):
        anime_season = self.pdata.get("anime_season", "01")
        if isinstance(anime_season, list):
            anime_season = list(dict.fromkeys(anime_season))[0]
        season_num = str(anime_season).zfill(2) if anime_season else "01"

        _raw_ep = self.pdata.get("episode_number", "01")
        if isinstance(_raw_ep, list):
            _raw_ep = _raw_ep[-1] if _raw_ep else "01"
        _RESOLUTION_VALUES = {"480", "720", "1080", "2160", "4320"}
        if str(_raw_ep) in _RESOLUTION_VALUES:
            _raw_ep = "01"
        episode_num = str(_raw_ep).zfill(2)

        titles = self.adata.get("title", {})
        clean_title = titles.get("english") or titles.get("romaji") or titles.get("native") or self.pdata.get("anime_title", "Unknown Anime")

        # Strip Japanese brackets and normalize
        clean_title = re.sub(r'[【】「」『』\[\]]', ' ', clean_title)
        clean_title = re.sub(r'\s+', ' ', clean_title).strip()

        brand = Var.BRAND_UNAME.strip("@")
        audio = detect_audio(self.__name)

        qual_label = "HDRip" if qual == "Hdri" else f"{qual}p"
        static_part = f" [{qual_label}] [{audio}] [@{brand}].mkv"

        # Use only the first 2 words of the title for a clean short filename.
        # Caption (which has the full title) is handled separately.
        words = clean_title.split()
        short_title = " ".join(words[:2]) if words else clean_title

        filename = f"S{season_num}E{episode_num} - {short_title}{static_part}"
        filename = re.sub(r'\s+', " ", filename).strip()
        return filename

    @handle_logs
    async def get_caption(self, is_main_channel=False, qual=""):
        titles = self.adata.get("title", {})
        _eng    = titles.get("english") or ""
        _romaji = titles.get("romaji") or ""
        _native = titles.get("native") or ""

        import re as _re_strip

        def _clean(t):
            # Strip season suffixes e.g. "Season 2", "S2", "2nd Season"
            t = _re_strip.sub(
                r'\s*(Season\s*\d+|S\d{1,2}|Part\s*\d+|Cour\s*\d+|\d+(st|nd|rd|th)\s+Season)\s*$',
                '', t, flags=_re_strip.IGNORECASE
            ).strip()
            # Strip Japanese brackets
            t = _re_strip.sub(r'[【】「」『』\[\]]', '', t).strip()
            t = _re_strip.sub(r'\s+', ' ', t).strip()
            return t

        _eng_clean    = _clean(_eng)
        _romaji_clean = _clean(_romaji)

        # Title format: "Eng Name | Romaji Name"
        # If eng and romaji are the same (or one is missing), show just one.
        if _eng_clean and _romaji_clean and _eng_clean.lower() != _romaji_clean.lower():
            title = f"{_eng_clean} | {_romaji_clean}"
        elif _eng_clean:
            title = _eng_clean
        elif _romaji_clean:
            title = _romaji_clean
        else:
            title = _native or "Unknown"

        season = self.pdata.get("anime_season", "01")
        if isinstance(season, list):
            season = list(dict.fromkeys(season))[0]
        season = str(season).zfill(2) if season else "01"

        _raw_ep2 = self.pdata.get("episode_number", "01")
        if isinstance(_raw_ep2, list):
            _raw_ep2 = _raw_ep2[-1] if _raw_ep2 else "01"
        if str(_raw_ep2) in {"480", "720", "1080", "2160", "4320"}:
            _raw_ep2 = "01"
        episode = str(_raw_ep2).zfill(2)

        if is_main_channel:
            import re as _re_html
            synopsis = self.adata.get("description", "No synopsis available.") or "No synopsis available."
            synopsis = _re_html.sub(r'<[^>]+>', ' ', synopsis)
            synopsis = _re_html.sub(r'\s+', ' ', synopsis).strip()
            import html as _html_mod
            synopsis = _html_mod.escape(synopsis)  # prevent injection of stray HTML entities
            if len(synopsis) > 600:
                synopsis = synopsis[:600] + "..."

            audio = detect_audio(self.__name)
            caption = MAIN_CAPTION_FORMAT.format(
                title=title,
                season=season,
                ep_no=episode,
                synopsis=synopsis,
                audio=audio
            )
            # Hard guard: Telegram media caption limit is 1024 chars.
            # If still too long (very long title + long synopsis), trim synopsis further.
            while len(caption) > 1020 and len(synopsis) > 100:
                synopsis = synopsis[: len(synopsis) - 100] + "..."
                caption = MAIN_CAPTION_FORMAT.format(
                    title=title, season=season, ep_no=episode,
                    synopsis=synopsis, audio=audio
                )
            return caption
        else:
            audio = detect_audio(self.__name)
            brand = Var.BRAND_UNAME.strip("@")
            qual_label = "HDRip" if qual == "Hdri" else (f"{qual}p" if qual else next(
                (f"{q}p" for q in ["2160", "1080", "720", "480"] if q in self.__name), "720p"
            ))
            full_filename = f"S{season}E{episode} - {title} [{qual_label}] [{audio}] [@{brand}].mkv"
            full_filename = re.sub(r'\s+', ' ', full_filename).strip()
            return f"<code>{full_filename}</code>"

# ── MovieEditor ───────────────────────────────────────────────────────────────

class MovieEditor:
    """
    AniList metadata loader and caption/filename generator for anime movies.
    """

    def __init__(self, name: str):
        self.__name = name
        self.adata: dict = {}
        self._loaded = False

    async def load_anilist(self, _retries: int = 0):
        if self._loaded:
            return
        cache_key = f"movie:{self.__name.lower()}"
        if cache_key in _anilist_cache:
            self.adata = _anilist_cache[cache_key]
            self._loaded = True
            return
        # Check DB cache first
        try:
            from bot.core.database import db as _db_mv
            _db_cached = await _db_mv.db.anilist_cache.find_one({"cache_key": cache_key})
            if _db_cached and _db_cached.get("data"):
                _anilist_cache[cache_key] = _db_cached["data"]
                self.adata = _db_cached["data"]
                self._loaded = True
                return
        except Exception:
            pass
        try:
            async with ClientSession() as sess:
                async with sess.post(
                    "https://graphql.anilist.co",
                    json={"query": MOVIE_GRAPHQL_QUERY,
                          "variables": {"search": _normalize_anime_title(self.__name)}}
                ) as resp:
                    _status = resp.status
                    _headers = resp.headers
                    body = await resp.json(content_type=None)
            if _status == 429:
                if _retries < 3:
                    _wait = min(int(_headers.get("Retry-After", 60)), 300)
                    await rep.report(f"MovieEditor AniList 429 — waiting {_wait}s", "warning", log=False)
                    await asleep(_wait)
                    return await self.load_anilist(_retries + 1)
                await rep.report(f"MovieEditor AniList 429 max retries: {self.__name}", "error")
            elif _status in (500, 501, 502):
                if _retries < 3:
                    await asleep(5 * (2 ** _retries))
                    return await self.load_anilist(_retries + 1)
            result = ((body or {}).get("data") or {}).get("Media") or {}
            if result:
                _anilist_cache[cache_key] = result
                self.adata = result
                # Persist to DB cache
                try:
                    from bot.core.database import db as _db_mv2
                    await _db_mv2.db.anilist_cache.update_one(
                        {"cache_key": cache_key},
                        {"$set": {"cache_key": cache_key, "data": result,
                                  "updated_at": __import__("datetime").datetime.utcnow()}},
                        upsert=True
                    )
                except Exception:
                    pass
        except Exception as e:
            await rep.report(f"MovieEditor AniList error: {e}", "error")
        self._loaded = True

    def get_title(self) -> str:
        titles = self.adata.get("title", {})
        return (titles.get("english") or titles.get("romaji")
                or titles.get("native") or _normalize_anime_title(self.__name))

    def get_year(self) -> str:
        sd = self.adata.get("startDate") or {}
        return str(sd.get("year") or "N/A")

    def get_duration(self) -> str:
        return str(self.adata.get("duration") or "N/A")

    async def get_poster(self) -> str | None:
        cover = self.adata.get("coverImage") or {}
        return cover.get("extraLarge") or cover.get("large")

    async def get_banner(self) -> str | None:
        return self.adata.get("bannerImage")

    def get_synopsis(self) -> str:
        import re as _re
        raw = self.adata.get("description") or "No synopsis available."
        clean = _re.sub(r"<[^>]+>", " ", raw)
        clean = _re.sub(r"\s+", " ", clean).strip()
        return clean[:800] + "..." if len(clean) > 800 else clean

    async def get_upname(self, qual: str = "") -> str:
        title      = self.get_title()
        safe_title = re.sub(r'[<>:"\\|?*]', "", title.replace("/", " ")).strip()[:60]
        brand      = Var.BRAND_UNAME.strip("@")
        qual_label = "HDRip" if qual == "Hdri" else (f"{qual}p" if qual else "")
        qual_tag   = f"[{qual_label}]" if qual_label else ""
        return f"{qual_tag}{safe_title}[@{brand}].mkv"

    async def get_main_caption(self, audio: str = "Sub") -> str:
        return MOVIE_MAIN_CAPTION_FORMAT.format(
            title    = self.get_title(),
            year     = self.get_year(),
            duration = self.get_duration(),
            audio    = audio,
            synopsis = self.get_synopsis(),
        )

    async def get_dedicated_caption(self, audio: str = "Sub", qual: str = "") -> str:
        title = self.get_title()
        brand = Var.BRAND_UNAME.strip("@")
        qual_label = "HDRip" if qual == "Hdri" else (f"{qual}p" if qual else "HDRip")
        full_filename = f"{title} [{qual_label}] [{audio}] [@{brand}].mkv"
        full_filename = re.sub(r'\s+', ' ', full_filename).strip()
        return f"<code>{full_filename}</code>"
