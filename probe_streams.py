#!/usr/bin/env python3
"""
probe_streams.py
Run on VPS:
    python3 probe_streams.py /path/to/file.mkv
    python3 probe_streams.py /path/to/batch/folder/
"""
import json, subprocess, sys, os

UNMUXABLE = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvdsub", "pgssub"}
PROB_COPY  = {"eac3", "truehd", "dtshd", "dts-hd", "pcm_bluray", "mlp"}

R="\033[91m"; Y="\033[93m"; G="\033[92m"; C="\033[96m"; B="\033[1m"; X="\033[0m"

def probe(path):
    r = subprocess.run(
        ["ffprobe","-v","quiet","-print_format","json","-show_streams", path],
        capture_output=True
    )
    if r.returncode != 0:
        print(f"{R}ffprobe failed for: {path}{X}")
        return []
    return json.loads(r.stdout).get("streams", [])

def classify(s):
    ctype  = s.get("codec_type","")
    cname  = s.get("codec_name","").lower()
    idx    = s.get("index")
    tags   = s.get("tags", {})
    lang   = tags.get("language","?")
    title  = tags.get("title","")

    if ctype == "attachment":
        return idx, "SKIP (attachment)", Y, cname, lang, title
    if ctype == "video" and cname == "mjpeg":
        return idx, "SKIP (cover art)", Y, cname, lang, title
    if cname in UNMUXABLE:
        return idx, "SKIP (unmuxable sub)", Y, cname, lang, title
    if cname in PROB_COPY:
        return idx, "⚠ RISKY copy", R, cname, lang, title
    return idx, "OK  (copy-safe)", G, cname, lang, title

def analyze(path):
    print(f"\n{B}{C}{'─'*60}{X}")
    print(f"{B}File: {path}{X}")
    streams = probe(path)
    if not streams:
        return
    print(f"Total streams: {len(streams)}\n")
    print(f"  {'Idx':<4} {'Type':<12} {'Codec':<22} {'Lang':<6} {'Status':<26} Title")
    print(f"  {'─'*4} {'─'*12} {'─'*22} {'─'*6} {'─'*26} {'─'*20}")

    skip_pre = []
    risky    = []
    ok       = []

    for s in streams:
        idx, status, color, cname, lang, title = classify(s)
        ctype = s.get("codec_type","unknown")
        print(f"  {str(idx):<4} {ctype:<12} {cname:<22} {lang:<6} {color}{status}{X} {title[:40]}")
        if "SKIP" in status:
            skip_pre.append(idx)
        elif "RISKY" in status:
            risky.append(idx)
        else:
            ok.append(idx)

    print()
    if skip_pre:
        print(f"  {Y}Pre-excluded (bot skips automatically):{X} streams {skip_pre}")
    if risky:
        print(f"  {R}Risky for -c copy (likely trigger retry loop):{X} streams {risky}")
    if ok:
        print(f"  {G}Copy-safe streams:{X} {ok}")

    # Simulate what the bot would map after pre-exclusion
    remaining = [s['index'] for s in streams if s['index'] not in skip_pre]
    risky_remaining = [i for i in risky if i not in skip_pre]
    if risky_remaining:
        print(f"\n  {R}⚠ These streams will likely cause Hdri copy to fail: {risky_remaining}{X}")
        print(f"  {Y}→ Bot will retry, exclude them, then fall back to libx264 re-encode{X}")
    else:
        print(f"\n  {G}✓ No problematic streams — Hdri stream-copy should succeed{X}")

def find_videos(path):
    VIDEO = {".mkv",".mp4",".avi",".mov",".m4v"}
    if os.path.isfile(path):
        return [path]
    results = []
    for root, _, files in os.walk(path):
        for f in files:
            if os.path.splitext(f)[1].lower() in VIDEO:
                results.append(os.path.join(root, f))
    return sorted(results)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 probe_streams.py <file_or_folder>")
        sys.exit(1)
    target = sys.argv[1]
    files  = find_videos(target)
    if not files:
        print(f"{R}No video files found at: {target}{X}")
        sys.exit(1)
    for f in files:
        analyze(f)
    print(f"\n{B}Done. {len(files)} file(s) scanned.{X}\n")
