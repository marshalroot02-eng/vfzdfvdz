#!/usr/bin/env bash
# ==============================================================================
# TikTok Booster - Android 14 / API 34 Emulator Session & Scrcpy Runner
# ==============================================================================
set -e

echo "=== [1/4] Android 14 Boot & Version Verification ==="
chmod +x ./scripts/*.sh 2>/dev/null || true
adb devices
export ANDROID_SERIAL=$(adb devices | grep -m 1 'emulator-' | awk '{print $1}')
echo "Using Android Device: ${ANDROID_SERIAL:-default}"

SDK_VER=$(adb shell getprop ro.build.version.sdk 2>/dev/null | tr -d '\r')
REL_VER=$(adb shell getprop ro.build.version.release 2>/dev/null | tr -d '\r')
BOOT_DONE=$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')
WM_SIZE=$(adb shell wm size 2>/dev/null | tr -d '\r')
WM_DENSITY=$(adb shell wm density 2>/dev/null | tr -d '\r')

echo "=================================================="
echo "SDK Version:      $SDK_VER"
echo "Android Release:  $REL_VER"
echo "Boot Completed:   $BOOT_DONE"
echo "Display Size:     $WM_SIZE"
echo "Display Density:  $WM_DENSITY"
echo "=================================================="

if [ "$SDK_VER" != "34" ]; then
    echo "[ERROR] Expected Android 14 (API 34) but got SDK $SDK_VER!"
    exit 1
fi
echo "[PASS] Android 14 (API 34) verified successfully!"

# Enable root access on AVD (required for app session backup/restore on /data/data)
echo "Enabling ADB root permissions on Android 14 emulator..."
adb root 2>/dev/null || true
adb wait-for-device 2>/dev/null || true
sleep 1

# Configure Clean HD Portrait Display Resolution (720x1280 @ 240 DPI)
echo "Configuring HD Android Display Resolution (720x1280 @ 240dpi)..."
adb shell wm size 720x1280 || true
adb shell wm density 240 || true
sleep 1

# 2. Pre-install Official Standard TikTok APK
echo "=== [2/5] Checking Official Standard TikTok APK Installation ==="
adb shell pm uninstall com.zhiliaoapp.musically.go 2>/dev/null || true
if ! adb shell pm list packages | grep -q 'package:com.zhiliaoapp.musically$'; then
    if [ -f "/tmp/tiktok.apk" ]; then
        echo "Installing official TikTok APK from /tmp/tiktok.apk..."
        adb install -r -d -g /tmp/tiktok.apk || true
    fi
fi

# 3. Fast Tap script
echo "=== [3/5] Setting Up Fast Tap Acceleration Script ==="
if [ -f "./scripts/fast_tap.sh" ]; then
    chmod +x ./scripts/fast_tap.sh
    adb push ./scripts/fast_tap.sh /data/local/tmp/fast_tap.sh
    adb shell chmod +x /data/local/tmp/fast_tap.sh
    echo "[PASS] fast_tap.sh installed on device."
fi

# 3. Setup & Launch Official Scrcpy v2.4 Server
echo "=== [3/4] Setting Up Official Scrcpy v2.4 Server ==="
if [ -f "./scripts/setup_scrcpy.sh" ]; then
    chmod +x ./scripts/setup_scrcpy.sh
    ./scripts/setup_scrcpy.sh
    echo "[PASS] scrcpy-server v2.4 launched on port 27183."
fi

# 4. Optional Pre-Flight VPN & Android Egress Integration Verification
if [ "${RUN_VPN_TESTS:-0}" = "1" ] || [ "${RUN_VPN_TESTS:-}" = "true" ] || [ "${TEST_MODE:-}" = "vpn" ]; then
    echo "=== [4/5] Executing Full PIA VPN & Android Egress Integration Test Suite ==="
    export PYTHONPATH="${PYTHONPATH:-.}:."
    python -u tests/integration_vpn_network_test.py
    echo "[PASS] All PIA VPN & Android egress integration tests completed successfully!"
fi

# 5. Run TikTok Booster Python Orchestrator
echo "=== [5/5] Starting TikTok Booster Orchestrator ==="
RUNNER_INDEX="${RUNNER_INDEX:-0}"
STREAM_URL="${STREAM_URL:-https://www.tiktok.com/@tiktok/live}"
DURATION_MIN="${DURATION_MIN:-60}"
LIKES_RATE="${LIKES_RATE:-120}"
VPN_LOCATION="${VPN_LOCATION:-}"

ARGS=(
  --stream-url "$STREAM_URL"
  --duration "$DURATION_MIN"
  --likes-per-min "$LIKES_RATE"
  --runner-index "$RUNNER_INDEX"
)
if [ -n "$VPN_LOCATION" ]; then
  ARGS+=(--vpn-location "$VPN_LOCATION")
fi

export PYTHONPATH="${PYTHONPATH:-.}:."
python -m src.main "${ARGS[@]}"
