"""
assets/card_generator.py  — v8

Changes vs v7:
  - Season pill: always shown — fallback to "Season 1" if no season number detected
  - Episode pill: shown next to season pill when current_episode is provided in card_data
  - Format field: top meta now shows actual format (TV / ONA / OVA / Movie / Special)
    instead of hardcoded "TV"

Public API (unchanged):
    make_anime_card(card_data, cover_url) -> bytes
    normalise_jikan(data)  -> dict
    normalise_anilist(data) -> dict
"""
from __future__ import annotations
import io, math, re, time as _time
from pathlib import Path
from typing import Optional

# Trusted parent domains for cover image fetching.
# All subdomains of these are allowed (e.g. cdn.*, api-cdn.*, s4.*, img.*).
# This prevents SSRF to internal/private endpoints while covering all real
# MAL and AniList CDN variants Jikan/AniList actually return.
_TRUSTED_IMG_DOMAINS = (
    "myanimelist.net",   # cdn.myanimelist.net, api-cdn.myanimelist.net, etc.
    "anilist.co",        # s4.anilist.co and any future anilist subdomains
    "anili.st",          # img.anili.st (AniList image proxy)
    "media.kitsu.app",
    "media.kitsu.io",
    "simkl.in",
    "artworks.thetvdb.com",
)


def _is_trusted_image_url(url: str) -> bool:
    """Return True only if url is https:// on a trusted image domain or subdomain."""
    try:
        from urllib.parse import urlparse as _up
        p = _up(url)
        if p.scheme != "https":
            return False
        host = p.netloc.split(":")[0].lower()  # strip port if present
        return any(
            host == d or host.endswith("." + d)
            for d in _TRUSTED_IMG_DOMAINS
        )
    except Exception:
        return False


try:
    import httpx as _httpx
    def _http_get(url, timeout=12):
        # follow_redirects=False: prevents SSRF via redirect to internal endpoints
        r = _httpx.get(url, timeout=timeout, follow_redirects=False)
        # Accept one redirect only if destination is also a trusted domain
        if r.status_code in (301, 302, 303, 307, 308):
            _loc = r.headers.get("location", "")
            if _is_trusted_image_url(_loc):
                r = _httpx.get(_loc, timeout=timeout, follow_redirects=False)
            else:
                raise ValueError(f"Redirect to untrusted domain blocked: {_loc!r}")
        r.raise_for_status()
        return r.content
except ImportError:
    import urllib.request as _urllib
    def _http_get(url, timeout=12):
        with _urllib.urlopen(url, timeout=timeout) as r:
            return r.read()

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageEnhance

_F           = Path(__file__).parent / "fonts"
POPPINS_BOLD = _F / "Poppins-Bold.ttf"
POPPINS_MED  = _F / "Poppins-Medium.ttf"
POPPINS_REG  = _F / "Poppins-Regular.ttf"
NOTO_BOLD    = _F / "NotoSansCJK-Bold.ttc"
ANLOGO_PATH  = Path(__file__).parent / "anime_nexus_logo.png"
WLLOGO_PATH  = Path(__file__).parent / "warlords_logo.png"

def _font(p, s):
    try:    return ImageFont.truetype(str(p), s)
    except Exception: return ImageFont.load_default()

VIO     = (149,  76, 255)
BLU     = ( 76, 150, 255)
PNK     = (255,  76, 160)
DARK    = ( 10,   8,  22)
WHITE   = (255, 255, 255)
TXT_HI  = (245, 242, 255)
TXT_MID = (185, 175, 215)
TXT_LO  = (110, 100, 145)
GOLD    = (255, 200,  45)

STATUS_C = {
    "Currently Airing": ( 40, 210,  90),
    "Finished Airing":  ( 90, 140, 255),
    "Not yet aired":    (230, 170,  35),
    "Unknown":          (100,  95, 130),
}

STATUS_LABEL = {
    "Currently Airing": "Airing",
    "Finished Airing":  "Finished",
    "Not yet aired":    "Not Aired",
    "Unknown":          "Unknown",
}

# AniList format → display label
_FORMAT_LABEL = {
    "TV":              "TV",
    "TV_SHORT":        "TV",
    "ONA":             "ONA",
    "OVA":             "OVA",
    "MOVIE":           "Movie",
    "SPECIAL":         "Special",
    "MUSIC":           "Music",
}

GENRE_C = [(149,76,255),(76,150,255),(235,70,155),(30,185,135),(225,145,35)]

def _tsz(draw, txt, fnt):
    bb = draw.textbbox((0,0), txt, font=fnt)
    return bb[2]-bb[0], bb[3]-bb[1]

def _fetch_image(url: str) -> Optional[Image.Image]:
    if not _is_trusted_image_url(url):
        # Log and skip — don't fetch from unknown domains
        # Quietly skip untrusted cover URLs — was a noisy print() in earlier
        # versions; the rejection is rare and not actionable for the operator.
        pass
        return None
    try:
        data = _http_get(url, timeout=12)
        return Image.open(io.BytesIO(data)).convert("RGBA")
    except Exception:
        return None

def _wrap(draw, text, fnt, max_w, max_lines=2):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        t = (cur+" "+w).strip()
        if _tsz(draw, t, fnt)[0] > max_w and cur:
            lines.append(cur); cur = w
            if len(lines) >= max_lines: break
        else: cur = t
    if cur and len(lines) < max_lines: lines.append(cur)
    if lines:
        last = lines[-1]
        while _tsz(draw, last+"…", fnt)[0] > max_w and len(last)>1: last=last[:-1]
        if lines[-1] != last: lines[-1] = last+"…"
    return lines

def _rrect_mask(w, h, r):
    m = Image.new("L",(w,h),0)
    ImageDraw.Draw(m).rounded_rectangle([0,0,w-1,h-1],radius=r,fill=255)
    return m

def _draw_star(draw, cx, cy, r_out, r_in, fill):
    pts = []
    for i in range(10):
        angle = math.radians(-90 + i*36)
        r = r_out if i%2==0 else r_in
        pts.append((cx + r*math.cos(angle), cy + r*math.sin(angle)))
    draw.polygon(pts, fill=fill)

# ── Normalisation ──────────────────────────────────────────────────────────────
_RE_S = re.compile(
    r'\s*(Season\s*\d+|S\d{1,2}|Part\s*\d+|Cour\s*\d+'
    r'|\d+(?:st|nd|rd|th)\s+Season|[:\-]\s*\d+(?:st|nd|rd|th)?\s*Season'
    r'|\b(II|III|IV|VI{0,3}|IX|X)\b|\s+\d+$)\s*$', re.IGNORECASE)
_RE_JP_S = re.compile(r'\s*第?\d+期\s*$')
_RE_BR_S = re.compile(r'\s*(Season\s*\d+|\d+(?:st|nd|rd|th)\s+Season)\s*$', re.IGNORECASE)

def _strip_season(t):
    prev=None
    while t!=prev: prev=t; t=_RE_S.sub('',t).strip()
    t=_RE_JP_S.sub('',t).strip()
    t=_RE_BR_S.sub('',t).strip()
    return t

def _clean_syn(raw):
    if not raw: return ""
    t=re.sub(r'<[^>]+>',' ',raw); t=re.sub(r'\s+',' ',t).strip()
    return re.sub(r'\(Source:[^)]+\)','',t).strip()

def normalise_jikan(data: dict) -> dict:
    st=[s["name"] for s in data.get("studios",[])]
    sr=(data.get("season") or "").capitalize()
    _S={"Finished Airing":"Finished Airing","Currently Airing":"Currently Airing","Not yet aired":"Not yet aired"}
    raw_title = data.get("title_english") or data.get("title") or ""
    # ── Explicit override wins — set by pipeline before calling get_poster() ──
    season_num = data.get("seasonNumber") or data.get("season_number") or None
    if not season_num:
        sn_m = re.search(r'Season\s*(\d+)|S(\d{1,2})\b|\b(\d+)(?:st|nd|rd|th)\s+Season', raw_title, re.IGNORECASE)
        season_num = int(sn_m.group(1) or sn_m.group(2) or sn_m.group(3)) if sn_m else None
    # Catch trailing numbers e.g. "Yamishibai 16", "Overlord 4"
    # Only treat as season if 2-20 to avoid false positives from episode counts
    if not season_num:
        _trail = re.search(r'\s+(\d{1,2})$', raw_title.strip())
        if _trail:
            _n = int(_trail.group(1))
            if 2 <= _n <= 20:
                season_num = _n
    # Always show a season pill — fallback to Season 1
    snl = f"Season {season_num}" if season_num else "Season 1"
    # Strip square brackets from english title (e.g. "[Oshi No Ko]" → "Oshi no Ko")
    clean_en = _strip_season(raw_title)
    clean_en = re.sub(r'^\[|\]$', '', clean_en).strip()
    clean_en = re.sub(r'\s+', ' ', clean_en).strip()
    # Strip season suffix from JP title (e.g. "【推しの子】 第3期" → "【推しの子】")
    clean_jp = data.get("title_japanese") or ""
    clean_jp = _RE_JP_S.sub('', clean_jp).strip()
    # Format: Jikan uses "type" field — TV, ONA, OVA, Movie, Special, Music
    fmt = data.get("type") or "TV"
    return {"title_en": clean_en,
            "title_jp": clean_jp,
            "studio":   st[0] if st else "Unknown Studio",
            "score":    data.get("score"),
            "genres":   [g["name"] for g in data.get("genres",[])[:5]],
            "status":   _S.get(data.get("status",""),"Unknown"),
            "season":   f"{sr} {data.get('year') or ''}".strip(),
            "episodes": data.get("episodes"),
            "synopsis": _clean_syn(data.get("synopsis") or ""),
            "season_number_label": snl,
            "format":   fmt}

def normalise_anilist(data: dict) -> dict:
    ti=data.get("title",{}); st=(data.get("studios") or {}).get("nodes",[])
    sc=data.get("averageScore")
    _S={"FINISHED":"Finished Airing","RELEASING":"Currently Airing",
        "NOT_YET_RELEASED":"Not yet aired","CANCELLED":"Unknown","HIATUS":"Unknown",
        "Finished":"Finished Airing","Releasing":"Currently Airing","Not Yet Released":"Not yet aired"}
    sr=(data.get("season") or "").capitalize()
    season_num = data.get("seasonNumber") or data.get("season_number")
    if not season_num:
        raw_title = ti.get("english") or ti.get("romaji") or ""
        sn_m = re.search(r'Season\s*(\d+)|S(\d{1,2})\b|\b(\d+)(?:st|nd|rd|th)\s+Season', raw_title, re.IGNORECASE)
        season_num = int(sn_m.group(1) or sn_m.group(2) or sn_m.group(3)) if sn_m else None
        # Catch trailing numbers e.g. "Yamishibai 16", "Overlord 4"
        # Only treat as season if 2-20 to avoid false positives from episode counts
        if not season_num:
            _trail = re.search(r'\s+(\d{1,2})$', raw_title.strip())
            if _trail:
                _n = int(_trail.group(1))
                if 2 <= _n <= 20:
                    season_num = _n
    # Always show a season pill — fallback to Season 1
    snl = f"Season {season_num}" if season_num else "Season 1"
    # Format: AniList uses "format" field — TV, TV_SHORT, ONA, OVA, MOVIE, SPECIAL, MUSIC
    raw_fmt = data.get("format") or "TV"
    fmt = _FORMAT_LABEL.get(raw_fmt, raw_fmt)
    return {"title_en": _strip_season(ti.get("english") or ti.get("romaji") or "Unknown"),
            "title_jp": ti.get("native") or "",
            "studio":   st[0]["name"] if st else "Unknown Studio",
            "score":    round(sc/10,1) if sc else None,
            "genres":   (data.get("genres") or [])[:5],
            "status":   _S.get(data.get("status",""),"Unknown"),
            "season":   f"{sr} {data.get('seasonYear') or ''}".strip(),
            "episodes": data.get("episodes"),
            "synopsis": _clean_syn(data.get("description") or ""),
            "season_number_label": snl,
            "format":   fmt}

def _build_bg(cover, W, H):
    cw,ch=cover.size; sc=max(W/cw,H/ch)
    nw,nh=int(cw*sc),int(ch*sc)
    bg=cover.resize((nw,nh),Image.LANCZOS)
    ox,oy=(nw-W)//2,(nh-H)//2
    bg_cropped=bg.crop((ox,oy,ox+W,oy+H)).convert("RGB")
    bg.close()
    sm=bg_cropped.resize((W//4,H//4),Image.BILINEAR).filter(ImageFilter.GaussianBlur(10))
    bg_cropped.close()
    bg_up=sm.resize((W,H),Image.BILINEAR)
    sm.close()
    bg_bright=ImageEnhance.Brightness(bg_up).enhance(0.30)
    bg_up.close()
    bg_color=ImageEnhance.Color(bg_bright).enhance(1.6)
    bg_bright.close()
    result=bg_color.convert("RGBA")
    bg_color.close()
    return result

def _build_cover_card(cover, cw, ch, radius=18):
    PAD=10; tw,th=cw+PAD*2,ch+PAD*2
    iw,ih=cover.size; sc=max(cw/iw,ch/ih)
    nw,nh=int(iw*sc),int(ih*sc)
    img_rs=cover.resize((nw,nh),Image.LANCZOS)
    ox,oy=(nw-cw)//2,(nh-ch)//2
    img=img_rs.crop((ox,oy,ox+cw,oy+ch)).convert("RGBA")
    img_rs.close()
    img.putalpha(_rrect_mask(cw,ch,radius))
    out=Image.new("RGBA",(tw,th),(0,0,0,0))
    gc=Image.new("RGBA",(tw,th),(0,0,0,0)); gd=ImageDraw.Draw(gc)
    for exp,a in [(16,25),(10,50),(5,90)]:
        gd.rounded_rectangle([PAD-exp,PAD-exp,PAD+cw+exp,PAD+ch+exp],
                              radius=radius+exp,fill=(*VIO,a))
    gc_blur=gc.filter(ImageFilter.GaussianBlur(10))
    gc.close()
    out2=Image.alpha_composite(out,gc_blur)
    gc_blur.close(); out.close()
    bc=Image.new("RGBA",(tw,th),(0,0,0,0))
    ImageDraw.Draw(bc).rounded_rectangle([PAD-2,PAD-2,PAD+cw+2,PAD+ch+2],
                                          radius=radius+2,outline=(*VIO,170),width=2)
    out3=Image.alpha_composite(out2,bc)
    bc.close(); out2.close()
    out3.paste(img,(PAD,PAD),mask=img)
    img.close()
    return out3

def _score_bar(card, draw, x, y, score, bar_w=160, bar_h=6):
    draw.rounded_rectangle([x,y,x+bar_w,y+bar_h],radius=3,fill=(*WHITE,20))
    filled=max(1,int(bar_w*min(score,10)/10))
    t=np.linspace(0,1,filled,dtype=np.float32)
    rc=(VIO[0]+(BLU[0]-VIO[0])*t).astype(np.uint8)
    gc_=(VIO[1]+(BLU[1]-VIO[1])*t).astype(np.uint8)
    bc=(VIO[2]+(BLU[2]-VIO[2])*t).astype(np.uint8)
    ba=np.stack([rc,gc_,bc,np.full(filled,255,np.uint8)],axis=1)
    ba=np.repeat(ba[None,:,:],bar_h,axis=0)
    bi=Image.fromarray(ba,"RGBA"); bm=_rrect_mask(filled,bar_h,3)
    card.paste(bi,(x,y),mask=bm)
    bi.close()
    return ImageDraw.Draw(card)

def make_anime_card(card_data: dict, cover_url: str) -> bytes:
    # FIX: removed the [card] +X.XXs print() debug timeline.  It cluttered
    # production logs with ~10 lines per episode and offered no value once
    # the card pipeline stabilised.  Re-enable locally with `_ck = print`
    # while debugging if needed.
    def _ck(_l): pass

    W,H = 1280,720
    BAR_H = 74
    BAR_Y = H - BAR_H
    PAD   = 52

    CC_W=320; CC_H=480; GLOW=10; CC_MARGIN=40
    CC_X = W - CC_W - GLOW*2 - CC_MARGIN
    CC_Y = (BAR_Y - CC_H - GLOW*2) // 2

    META_W = CC_X - PAD - 30

    fJP    = _font(NOTO_BOLD,    20)
    fTXL   = _font(POPPINS_BOLD, 64)
    fTL    = _font(POPPINS_BOLD, 52)
    fTM    = _font(POPPINS_BOLD, 44)
    fTS    = _font(POPPINS_BOLD, 36)
    fMeta  = _font(POPPINS_MED,  20)
    fStud  = _font(POPPINS_MED,  18)
    fGenr  = _font(POPPINS_BOLD, 20)
    fScN   = _font(POPPINS_BOLD, 46)
    fScL   = _font(POPPINS_BOLD, 26)
    fSyn   = _font(POPPINS_REG,  17)
    fBar   = _font(POPPINS_BOLD, 22)
    fBarB  = _font(POPPINS_BOLD, 16)
    fSnPil = _font(POPPINS_BOLD, 24)
    _ck("fonts loaded")

    title_en         = card_data.get("title_en", "Unknown")
    title_jp         = card_data.get("title_jp", "")
    studio           = card_data.get("studio", "Unknown Studio")
    score_val        = card_data.get("score")
    genres           = card_data.get("genres", [])
    status           = card_data.get("status", "Unknown")
    season           = card_data.get("season", "")
    episodes         = card_data.get("episodes")
    synopsis         = card_data.get("synopsis", "")
    ani_id           = card_data.get("id")
    season_num_label = card_data.get("season_number_label", "Season 1")
    # Safely resolve current_episode — may arrive as a list from batch pipelines
    _ce_raw = card_data.get("current_episode")
    if isinstance(_ce_raw, list):
        _ce_raw = _ce_raw[-1] if _ce_raw else None
    try:
        current_episode = int(_ce_raw) if _ce_raw is not None else None
    except (ValueError, TypeError):
        current_episode = None
    fmt              = card_data.get("format", "TV")

    _ck("fetching cover")
    cover_raw=_fetch_image(cover_url)
    _ck(f"cover fetched size={cover_raw.size if cover_raw else None}")

    if cover_raw:
        _ck("building bg")
        bg=_build_bg(cover_raw,W,H); _ck("bg done")
    else:
        bg=Image.new("RGBA",(W,H),(*DARK,255))

    card=bg.copy()
    bg.close()

    lw=int(W*0.70); tv=np.linspace(0,1,lw,dtype=np.float32)**1.4
    av=(165*(1-tv)).astype(np.uint8)
    lv=np.zeros((H,W,4),dtype=np.uint8); lv[:,:,:3]=DARK; lv[:,:lw,3]=av[None,:]
    lv_img=Image.fromarray(lv,"RGBA")
    card=Image.alpha_composite(card,lv_img)
    lv_img.close()
    tb=np.linspace(0,1,H,dtype=np.float32)**2
    ab=(155*tb[::-1]).astype(np.uint8)
    bv=np.zeros((H,W,4),dtype=np.uint8); bv[:,:,:3]=DARK; bv[:,:,3]=ab[:,None]
    bv_img=Image.fromarray(bv,"RGBA")
    card=Image.alpha_composite(card,bv_img)
    bv_img.close()
    _ck("overlays done")

    if cover_raw:
        _ck("building cover card")
        cc=_build_cover_card(cover_raw,CC_W,CC_H,radius=18)
        card.paste(cc,(CC_X,CC_Y),mask=cc)
        cc.close()
        cover_raw.close()
        _ck("cover card done")

    draw=ImageDraw.Draw(card)

    ta=np.linspace(0,1,W,dtype=np.float32)
    ra=np.where(ta<.5,VIO[0]+(BLU[0]-VIO[0])*(ta*2),BLU[0]+(PNK[0]-BLU[0])*((ta-.5)*2))
    ga=np.where(ta<.5,VIO[1]+(BLU[1]-VIO[1])*(ta*2),BLU[1]+(PNK[1]-BLU[1])*((ta-.5)*2))
    ba_=np.where(ta<.5,VIO[2]+(BLU[2]-VIO[2])*(ta*2),BLU[2]+(PNK[2]-BLU[2])*((ta-.5)*2))
    acc=np.zeros((4,W,4),dtype=np.uint8)
    for row,alpha in enumerate([255,200,130,60]):
        acc[row,:,0]=ra.astype(np.uint8); acc[row,:,1]=ga.astype(np.uint8)
        acc[row,:,2]=ba_.astype(np.uint8); acc[row,:,3]=alpha
    acc_img=Image.fromarray(acc,"RGBA")
    card.paste(acc_img,(0,0),mask=acc_img)
    acc_img.close()
    draw=ImageDraw.Draw(card)

    nc=len(title_en)
    fT = fTXL if nc<=12 else fTL if nc<=20 else fTM if nc<=30 else fTS
    tlines=_wrap(draw,title_en,fT,META_W,max_lines=2)
    lh=_tsz(draw,"Ag",fT)[1]

    PILL_H   = 50
    PILL_PAD = 22

    def _draw_pill(draw, x, y, label, col, font,
                   icon_fn=None, icon_w=0, fill_alpha=70, outline_alpha=240, outline_w=2):
        lw2, lh2 = _tsz(draw, label, font)
        icon_gap  = (icon_w + 10) if icon_w else 0
        inner_w   = icon_gap + lw2
        plw       = PILL_PAD + inner_w + PILL_PAD
        draw.rounded_rectangle([x, y, x+plw, y+PILL_H],
                                radius=PILL_H//2,
                                fill=(*col, fill_alpha),
                                outline=(*col, outline_alpha),
                                width=outline_w)
        content_x = x + (plw - inner_w) // 2
        mid_y = y + PILL_H // 2
        if icon_fn and icon_w:
            icon_fn(draw, content_x, mid_y, col)
            content_x += icon_w + 10
        draw.text((content_x + lw2 // 2, mid_y), label, font=font,
                  anchor="mm", fill=(255,255,255,255))
        return x + plw, plw

    cy = PAD - 8

    # Row 1 — top meta
    meta_parts = []
    if season:    meta_parts.append(season)
    meta_parts.append(fmt)
    if episodes:  meta_parts.append(f"{episodes} Episodes")
    top_meta = " • ".join(meta_parts)
    draw.text((PAD, cy), top_meta, font=fMeta, fill=TXT_LO)
    cy += _tsz(draw,"Ag",fMeta)[1] + 20

    # Row 2 — JP title
    if title_jp:
        # Strip season suffixes from JP title for display (e.g. "第3期", "Second Season")
        _jp_display = _RE_JP_S.sub('', title_jp).strip()
        _jp_display = _RE_BR_S.sub('', _jp_display).strip()
        _jp_display = re.sub(r'\s+', ' ', _jp_display).strip()
        jp_h = _tsz(draw,"Ag",fJP)[1]
        draw.rounded_rectangle([PAD, cy+2, PAD+3, cy+jp_h], radius=2, fill=VIO)
        jp = _jp_display
        while _tsz(draw,jp,fJP)[0] > META_W-16 and len(jp)>3: jp=jp[:-1]
        if jp != _jp_display: jp += "…"
        draw.text((PAD+12, cy), jp, font=fJP, fill=TXT_MID)
        cy += jp_h + 14

    # Row 3 — EN title
    for line in tlines:
        draw.text((PAD+2, cy+2), line, font=fT, fill=(0,0,0,90))
        draw.text((PAD,   cy),   line, font=fT, fill=(255,255,255,255))
        cy += lh + 2

    # Row 4a — Season pill + Episode pill
    cy += 18
    pill_x = PAD
    pill_x, _ = _draw_pill(draw, pill_x, cy, season_num_label, VIO, fSnPil,
                            fill_alpha=50, outline_alpha=220, outline_w=2)
    if current_episode:
        pill_x += 12
        ep_label = f"Episode {int(current_episode):02d}"
        _draw_pill(draw, pill_x, cy, ep_label, BLU, fSnPil,
                   fill_alpha=50, outline_alpha=220, outline_w=2)
    cy += PILL_H + 18

    # Row 4b — Studios
    draw.text((PAD, cy), f"Studios: {studio}", font=fStud, fill=TXT_MID)
    cy += _tsz(draw,"Ag",fStud)[1] + 16

    # Divider
    draw.line([(PAD, cy), (PAD+META_W, cy)], fill=(*WHITE,30), width=1)
    cy += 16

    # Row 5 — Score
    try:
        pct_str = f"{int(round(float(score_val)*10))}%" if score_val is not None else "N/A"
    except (TypeError, ValueError):
        pct_str = "N/A"
        score_val = None  # disable bar too
    draw.text((PAD, cy), "Score", font=fScL, fill=TXT_LO)
    cy += _tsz(draw, "Ag", fScL)[1] + 6

    pct_bb   = draw.textbbox((PAD, cy), pct_str, font=fScN)
    pct_top  = pct_bb[1]
    pct_bot  = pct_bb[3]
    TEXT_MID = (pct_top + pct_bot) // 2

    score_h = _tsz(draw, "Ag", fScN)[1]
    SR = score_h // 2; SRI = SR // 2 + 2
    _draw_star(draw, PAD + SR, TEXT_MID, SR, SRI, GOLD)

    sx = PAD + SR * 2 + 14
    draw.text((sx+1, cy+1), pct_str, font=fScN, fill=(0,0,0,80))
    draw.text((sx,   cy),   pct_str, font=fScN, fill=(255,255,255,255))

    BAR_H_PX = 8
    bar_x = sx + _tsz(draw, pct_str, fScN)[0] + 28
    bar_y = TEXT_MID - BAR_H_PX // 2
    BAR_W = min(180, PAD + META_W - bar_x)
    if score_val and BAR_W > 20:
        draw = _score_bar(card, draw, bar_x, bar_y, score_val, bar_w=BAR_W, bar_h=BAR_H_PX)

    cy = pct_bot + 18

    # Row 6 — Genre pills
    if genres:
        gx = PAD
        for i, g in enumerate(genres):
            col = GENRE_C[i % len(GENRE_C)]
            lw2, _ = _tsz(draw, g, fGenr)
            plw_est = PILL_PAD + lw2 + PILL_PAD
            if gx + plw_est > PAD + META_W and gx > PAD:
                gx = PAD
                cy += PILL_H + 8
            next_x, _ = _draw_pill(draw, gx, cy, g, col, fGenr,
                                   fill_alpha=80, outline_alpha=255, outline_w=2)
            gx = next_x + 10
        cy += PILL_H + 16

    # Row 7 — Synopsis
    syn_lh = 22
    if synopsis:
        avail = BAR_Y - cy - 14
        ml = max(1, min(5, avail // syn_lh))
        for line in _wrap(draw, synopsis, fSyn, META_W, max_lines=ml):
            draw.text((PAD, cy), line, font=fSyn, fill=TXT_LO)
            cy += syn_lh

    # ── Bottom bar ────────────────────────────────────────────────────────────
    sep = np.zeros((1,W,4), dtype=np.uint8)
    sep[0,:,0]=sep[0,:,1]=sep[0,:,2]=255
    sep[0,:,3] = (60*(1-np.linspace(0,1,W,dtype=np.float32)**1.5)).astype(np.uint8)
    sep_img = Image.fromarray(sep,"RGBA")
    card.paste(sep_img,(0,BAR_Y-1), mask=sep_img.split()[3])
    sep_img.close()

    bar_bg = Image.new("RGBA",(W,BAR_H),(22,18,48,252))
    card.paste(bar_bg,(0,BAR_Y))
    bar_bg.close()
    draw = ImageDraw.Draw(card)

    mid    = BAR_Y + BAR_H//2
    pill_y = mid - PILL_H//2
    bx     = PAD

    ICO = 24

    def _circle_crop(img, size):
        img = img.resize((size, size), Image.LANCZOS).convert("RGBA")
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).ellipse([0, 0, size-1, size-1], fill=255)
        img.putalpha(mask)
        return img

    AN_ICO = 38
    try:
        _an_logo = _circle_crop(Image.open(str(ANLOGO_PATH)).convert("RGBA"), AN_ICO)
    except Exception:
        _an_logo = None

    def _anlogo_icon(draw, ix, mid_y, col):
        if _an_logo is None:
            bb = draw.textbbox((0, 0), "A", font=fBarB)
            draw.text((ix + (AN_ICO - (bb[2]-bb[0]))//2 - bb[0],
                       int(mid_y) - (bb[3]-bb[1])//2 - bb[1]),
                      "A", font=fBarB, fill=(255,255,255,255))
            return
        paste_x = int(ix)
        paste_y = int(mid_y) - AN_ICO // 2
        card.paste(_an_logo, (paste_x, paste_y), mask=_an_logo)

    WL_ICO = 38
    try:
        _wl_logo = _circle_crop(Image.open(str(WLLOGO_PATH)).convert("RGBA"), WL_ICO)
    except Exception:
        _wl_logo = None

    def _wllogo_icon(draw, ix, mid_y, col):
        if _wl_logo is None:
            W2 = 22; x0 = int(ix) + (WL_ICO - W2) // 2
            top = int(mid_y) - 9; bot = int(mid_y) + 9; base = bot - 6
            draw.rectangle([x0, base, x0+W2, bot], fill=(*GOLD, 255))
            col_w = W2 // 3
            for i in range(3):
                lx = x0+i*col_w; rx = lx+col_w; mx = (lx+rx)//2
                draw.polygon([(lx,base),(mx,top if i==1 else top+5),(rx,base)], fill=(*GOLD,255))
            return
        paste_x = int(ix)
        paste_y = int(mid_y) - WL_ICO // 2
        card.paste(_wl_logo, (paste_x, paste_y), mask=_wl_logo)

    # Status pill — no dot, short label
    dc = STATUS_C.get(status, STATUS_C["Unknown"])
    status_label = STATUS_LABEL.get(status, "Unknown")
    bx,_ = _draw_pill(draw, bx, pill_y, status_label, dc, fBar,
                      fill_alpha=60, outline_alpha=240, outline_w=2)
    bx += 14

    bx,_ = _draw_pill(draw, bx, pill_y, "Anime Nexus", BLU, fBar,
                      _anlogo_icon, AN_ICO, fill_alpha=60, outline_alpha=240, outline_w=2)
    bx += 14

    bx,_ = _draw_pill(draw, bx, pill_y, "Warlords", VIO, fBar,
                      _wllogo_icon, WL_ICO, fill_alpha=60, outline_alpha=240, outline_w=2)

    if ani_id:
        aid = f"AniList #{ani_id}"
        aw,ah = _tsz(draw, aid, fScL)
        draw.text((W-aw-PAD, mid-ah//2), aid, font=fScL, fill=TXT_LO)

    _ck("serialising")
    out=io.BytesIO()
    card.convert("RGB").save(out,format="JPEG",quality=95,optimize=True)
    card.close()
    _ck("done")
    return out.getvalue()
