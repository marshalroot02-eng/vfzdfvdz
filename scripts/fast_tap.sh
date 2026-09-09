#!/system/bin/sh
# Native on-device fast tap auto-clicker for TikTok Live Mobile
# Usage: /data/local/tmp/fast_tap.sh <center_x> <center_y> <jitter_px> <taps_per_burst> <burst_sleep_sec> <duration_sec>

X=${1:-450}
Y=${2:-650}
JITTER=${3:-25}
BURST_COUNT=${4:-10}
BURST_SLEEP=${5:-2}
DURATION=${6:-3600}

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))
TOTAL_LIKES=0

echo "Starting native TikTok Live auto-tap loop at ($X, $Y)..."

while [ $(date +%s) -lt $END_TIME ]; do
    # Verify Native TikTok is foreground window before tapping
    if ! dumpsys window | grep -E 'mCurrentFocus|mFocusedApp' | grep -qE 'com\.zhiliaoapp\.musically|com\.ss\.android\.ugc\.trill'; then
        sleep 1
        continue
    fi

    # Perform burst of taps
    i=0
    while [ $i -lt $BURST_COUNT ]; do
        # Random jitter
        OFFSET_X=$(( (RANDOM % (JITTER * 2)) - JITTER ))
        OFFSET_Y=$(( (RANDOM % (JITTER * 2)) - JITTER ))
        TAP_X=$(( X + OFFSET_X ))
        TAP_Y=$(( Y + OFFSET_Y ))
        
        input tap $TAP_X $TAP_Y
        if command -v usleep >/dev/null 2>&1; then
            usleep 120000
        else
            sleep 0.12 2>/dev/null || sleep 0.1 2>/dev/null || true
        fi
        i=$((i + 1))
        TOTAL_LIKES=$((TOTAL_LIKES + 1))
    done
    
    # Rest between bursts
    sleep $BURST_SLEEP
done

echo "Fast tap finished. Total likes generated: $TOTAL_LIKES"
