#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run.sh — Auto-restart wrapper for Animebot
#
# USAGE:
#   screen -S animebot bash /root/animebot/run.sh
#
# WHY THIS EXISTS:
#   The Linux OOM-killer sends SIGKILL (signal 9) when RAM is exhausted.
#   SIGKILL cannot be caught — the process dies instantly with "Killed".
#   This wrapper detects the crash and restarts within 5 seconds.
#
# EXTRA HARDENING:
#   - Sets MAX_WORKERS=1 when free RAM < 1.5 GB to prevent OOM during encode
#   - Kills orphan ffmpeg processes before each restart
#   - Logs every crash with timestamp to crash.log
# ─────────────────────────────────────────────────────────────────────────────

set -o pipefail

BOT_DIR="/root/animebot"
LOG_FILE="$BOT_DIR/crash.log"
RESTART_DELAY=5      # seconds between crash and restart
MAX_CONSECUTIVE=10   # give up after this many back-to-back crashes

cd "$BOT_DIR" || { echo "Cannot cd to $BOT_DIR"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────
# KERNEL MEMORY TUNING — prevents OOM-kills during FFmpeg encoding
#
# Problem: on an 8GB + 4GB swap VPS, a single 1080p+720p+480p encode cycle
# can fill all 12GB because:
#   1. Linux caches source files in page cache (~1-1.5GB per episode)
#   2. Default swappiness=60 prefers swapping APP memory over dropping cache
#   3. FFmpeg + Python heap + 3 Pyrogram bot clients add up to ~3-4GB
#
# Fixes:
#   vm.swappiness=10           → strongly prefer dropping file cache over swap
#   vm.vfs_cache_pressure=200  → aggressively reclaim inode/dentry cache
#   drop_caches=3              → flush ALL cached pages on fresh start
#   vm.overcommit_memory=0     → heuristic overcommit (default, safe)
# ─────────────────────────────────────────────────────────────────────────────
echo "[$(date '+%Y/%m/%d %H:%M:%S')] 🔧 Tuning kernel memory parameters..."
sysctl -w vm.swappiness=10 2>/dev/null || true
sysctl -w vm.vfs_cache_pressure=200 2>/dev/null || true
sysctl -w vm.overcommit_memory=0 2>/dev/null || true
sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
echo "[$(date '+%Y/%m/%d %H:%M:%S')] ✅ Kernel tuning done (swappiness=10, vfs_cache_pressure=200)"

consecutive=0

while true; do
    # Kill any ffmpeg orphans left over from a previous crashed run
    pkill -9 -f "ffmpeg" 2>/dev/null || true

    # Cap MAX_WORKERS to 1 when RAM is tight to prevent encode OOM.
    # Threshold raised to 2 GB: 2 ffmpeg procs × ~400 MB each + Python overhead
    # easily exceeds 1.5 GB, so we now throttle earlier at 2 GB free.
    # With MAX_WORKERS=1, ongoing_encode_lock and batch_encode_lock become the
    # SAME lock (see bot/__init__.py), so only 1 ffmpeg runs at a time.
    FREE_KB=$(awk '/MemAvailable/ {print $2}' /proc/meminfo 2>/dev/null || echo 9999999)
    FREE_MB=$((FREE_KB / 1024))
    if [ "$FREE_MB" -lt 2048 ]; then
        export MAX_WORKERS=1
        echo "[$(date '+%Y/%m/%d %H:%M:%S')] ⚠️  Only ${FREE_MB}MB free — forcing MAX_WORKERS=1 (single encode lock)" | tee -a "$LOG_FILE"
    else
        unset MAX_WORKERS
    fi

    echo "[$(date '+%Y/%m/%d %H:%M:%S')] 🚀 Starting bot (run #$((consecutive + 1)))..." | tee -a "$LOG_FILE"
    python3 -m bot
    EXIT_CODE=$?

    TS="[$(date '+%Y/%m/%d %H:%M:%S')]"

    # Exit code 0 = clean /restart or Ctrl+C — don't loop
    if [ "$EXIT_CODE" -eq 0 ]; then
        echo "$TS ✅ Bot stopped cleanly (exit 0)." | tee -a "$LOG_FILE"
        exit 0
    fi

    if [ "$EXIT_CODE" -eq 137 ]; then
        echo "$TS 💥 OOM-killed (SIGKILL/137) — restarting in ${RESTART_DELAY}s..." | tee -a "$LOG_FILE"
    else
        echo "$TS ❌ Crashed (exit $EXIT_CODE) — restarting in ${RESTART_DELAY}s..." | tee -a "$LOG_FILE"
    fi

    consecutive=$((consecutive + 1))
    if [ "$consecutive" -ge "$MAX_CONSECUTIVE" ]; then
        echo "$TS 🛑 $MAX_CONSECUTIVE crashes in a row — stopping to prevent thrash." | tee -a "$LOG_FILE"
        echo "$TS    Fix the issue then rerun: screen -S animebot bash $BOT_DIR/run.sh" | tee -a "$LOG_FILE"
        exit 1
    fi

    sleep "$RESTART_DELAY"
done
