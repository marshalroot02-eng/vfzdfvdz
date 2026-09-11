import os
import re
import time
import random
import logging
import subprocess
from typing import Optional, Tuple, List
from src.config import AppConfig

logger = logging.getLogger("ADBController")

class ADBController:
    """High-performance Android Debug Bridge (ADB) automation controller for TikTok Mobile App."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.device_id = config.adb_device_id
        self.adb_bin = self._locate_adb()
        self.package_name = getattr(config, 'tiktok_package', None) or "com.zhiliaoapp.musically"
        self.screen_width = 1080
        self.screen_height = 2400
        self.screen_density = 420
        self.sdk_level = 34
        self.android_version = "14"
        self.boot_completed = False
        self._is_tapping = False

    def _locate_adb(self) -> str:
        """Locates the ADB executable in system PATH or Android SDK locations."""
        # 1. Check system PATH
        try:
            res = subprocess.run(["adb", "version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if res.returncode == 0:
                return "adb"
        except FileNotFoundError:
            pass

        # 2. Check ANDROID_HOME or ANDROID_SDK_ROOT
        for env_var in ["ANDROID_HOME", "ANDROID_SDK_ROOT"]:
            sdk = os.getenv(env_var)
            if sdk:
                candidate = os.path.join(sdk, "platform-tools", "adb.exe" if os.name == "nt" else "adb")
                if os.path.exists(candidate):
                    return candidate

        # 3. Check common paths
        common_paths = [
            r"C:\Program Files (x86)\Android\android-sdk\platform-tools\adb.exe",
            r"C:\LDPlayer\LDPlayer14\adb.exe",
            r"C:\LDPlayer\LDPlayer9\adb.exe",
            os.path.expanduser("~/Library/Android/sdk/platform-tools/adb"),
            os.path.expanduser("~/Android/Sdk/platform-tools/adb"),
            "/usr/bin/adb",
            "/usr/local/bin/adb"
        ]
        for path in common_paths:
            if os.path.exists(path):
                return path

        return "adb"

    def run_cmd(self, args: List[str], timeout: int = 30) -> subprocess.CompletedProcess:
        """Executes an ADB command with the target device ID."""
        cmd = [self.adb_bin]
        if self.device_id:
            cmd.extend(["-s", self.device_id])
        cmd.extend(args)
        try:
            return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        except (FileNotFoundError, Exception) as e:
            return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr=str(e))

    def shell(self, cmd_str: str, timeout: int = 30) -> str:
        """Executes a command inside the Android shell."""
        res = self.run_cmd(["shell", cmd_str], timeout=timeout)
        return res.stdout.strip()

    def check_connection(self) -> bool:
        """Checks if the Android device / emulator is online and ready."""
        try:
            res = self.run_cmd(["devices"])
            lines = [line.strip() for line in res.stdout.strip().split("\n")[1:] if line.strip()]
            
            if not lines:
                logger.error("No ADB devices or emulators found.")
                return False
                
            devices = [line.split()[0] for line in lines if "device" in line]
            if not devices:
                logger.error(f"Devices found but none are ready: {lines}")
                return False
                
            if not self.device_id:
                self.device_id = devices[0]
                logger.info(f"Auto-selected ADB device: {self.device_id}")
            else:
                logger.info(f"Connected to specified ADB device: {self.device_id}")

            # Ensure root permissions on emulator (required for app session backup/restore on /data/data)
            try:
                self.run_cmd(["root"], timeout=4)
            except Exception:
                pass
                
            self._fetch_device_properties()
            return True
        except Exception as e:
            logger.error(f"Error checking ADB connection: {e}")
            return False

    def _fetch_device_properties(self) -> None:
        """Inspects physical display dimensions, density, and runtime Android SDK properties."""
        # 1. Screen size
        out_size = self.shell("wm size")
        match = re.search(r"(\d+)x(\d+)", out_size)
        if match:
            self.screen_width = int(match.group(1))
            self.screen_height = int(match.group(2))
        logger.info(f"Device resolution: {self.screen_width}x{self.screen_height}")

        # 2. Screen density
        out_dens = self.shell("wm density")
        match_d = re.search(r"(\d+)", out_dens)
        if match_d:
            self.screen_density = int(match_d.group(1))
        logger.info(f"Device density: {self.screen_density} dpi")

        # 3. Android SDK Level & OS Release
        sdk_raw = self.shell("getprop ro.build.version.sdk").strip()
        rel_raw = self.shell("getprop ro.build.version.release").strip()
        boot_raw = self.shell("getprop sys.boot_completed").strip()

        if sdk_raw.isdigit():
            self.sdk_level = int(sdk_raw)
        if rel_raw:
            self.android_version = rel_raw
        self.boot_completed = (boot_raw == "1")

        logger.info(f"[Runtime Android OS] Version: Android {self.android_version} (API Level {self.sdk_level}) | Boot Completed: {self.boot_completed}")

        # Strict Milestone 1 Validation:
        if self.config.require_api_level and self.sdk_level != self.config.require_api_level:
            logger.critical(f"RUNTIME VERSION MISMATCH: Required API Level {self.config.require_api_level} but device reported API Level {self.sdk_level} (Android {self.android_version})!")
            if os.getenv("STRICT_API_CHECK", "true").lower() == "true":
                raise RuntimeError(f"Device failed API 34 validation: Got SDK {self.sdk_level}")

    def get_foreground_activity(self) -> str:
        """Returns the exact current foreground package and activity name."""
        out = self.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp'")
        match = re.search(r"([a-zA-Z0-9_\.]+/[a-zA-Z0-9_\.]+)", out)
        if match:
            return match.group(1)
        return out.strip() or "Unknown"

    def get_device_health(self) -> dict:
        """Returns comprehensive real-time device health metrics."""
        return {
            "device_id": self.device_id,
            "adb_state": "OK" if self.device_id else "DISCONNECTED",
            "android_version": self.android_version,
            "sdk_level": self.sdk_level,
            "boot_completed": self.boot_completed,
            "display_size": f"{self.screen_width}x{self.screen_height}",
            "display_density": self.screen_density,
            "package_name": self.package_name,
            "foreground_activity": self.get_foreground_activity(),
            "timestamp": time.time()
        }

    def wake_and_unlock(self) -> None:
        """Wakes up the screen and unlocks device."""
        logger.info("Waking and unlocking Android device...")
        # Keycode 26 = Power, Keycode 82 = Menu / Unlock
        self.shell("input keyevent 26")
        time.sleep(0.5)
        self.shell("input keyevent 82")
        time.sleep(0.5)
        # Swipe up to dismiss lockscreen if needed
        mid_x = self.screen_width // 2
        start_y = int(self.screen_height * 0.8)
        end_y = int(self.screen_height * 0.2)
        self.shell(f"input swipe {mid_x} {start_y} {mid_x} {end_y} 200")

    def hide_keyboard(self) -> None:
        """Hides Android software keyboard (IME) so UI buttons and fields are not obscured."""
        self.shell("input keyevent 111")  # KEYCODE_ESCAPE
        time.sleep(0.4)

    def configure_proxy(self, proxy_str: Optional[str]) -> None:
        """Configures system HTTP proxy on Android via ADB settings."""
        if not proxy_str or not proxy_str.strip():
            # Clear proxy
            self.shell("settings put global http_proxy :0")
            logger.info("Android system proxy cleared.")
            return

        proxy = proxy_str.replace("http://", "").replace("https://", "").split("@")[-1]
        self.shell(f"settings put global http_proxy {proxy}")
        logger.info(f"Set Android system proxy to: {proxy}")

    def set_persistent_device_identity(self, device_id_hex: Optional[str] = None) -> None:
        """
        Sets a persistent Android ID (hardware identity) on the device.
        This prevents TikTok from detecting a 'New Device / New Hardware' on fresh GitHub runners.
        """
        if not device_id_hex or not device_id_hex.strip():
            return
            
        clean_id = device_id_hex.strip().lower()
        logger.info(f"Applying persistent Android Device ID: {clean_id}")
        self.shell(f"settings put secure android_id {clean_id}")

    def restore_app_session(self, session_path_or_url: str) -> bool:
        """
        Restores authenticated TikTok app state (cookies, tokens, databases, shared_prefs).
        Allows launching the TikTok app already logged in without triggering login screens, captchas, or OTPs.
        """
        try:
            logger.info(f"Restoring TikTok App session from: {session_path_or_url} ...")
            local_tar = session_path_or_url
            
            # Download if remote URL
            if session_path_or_url.startswith("http://") or session_path_or_url.startswith("https://"):
                import requests
                local_tar = os.path.join(os.getcwd(), "session_restore.tar.gz")
                r = requests.get(session_path_or_url, stream=True, timeout=60)
                if r.status_code != 200:
                    logger.error(f"Failed to download session tarball: HTTP {r.status_code}")
                    return False
                with open(local_tar, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Ensure root access before modifying /data/data
            try:
                self.run_cmd(["root"], timeout=4)
            except Exception:
                pass

            # Stop TikTok before restoring data
            self.shell(f"am force-stop {self.package_name}")
            
            # Push tarball to device and extract
            device_tar = "/sdcard/session_restore.tar.gz"
            self.run_cmd(["push", local_tar, device_tar])
            
            # Extract into app data directory with root or run-as
            self.shell(f"tar -xzf {device_tar} -C /data/data/{self.package_name}/ 2>/dev/null || tar -xzf {device_tar} -C /")
            self.shell(f"rm {device_tar}")
            logger.info("TikTok App session restored successfully! App is now authenticated.")
            return True
        except Exception as e:
            logger.warning(f"Could not restore session tarball: {e}")
            return False

    def backup_app_session(self, output_local_tar: str) -> bool:
        """
        Backs up the authenticated TikTok app session from the current device into a portable tar.gz archive.
        """
        try:
            logger.info(f"Backing up TikTok App session to {output_local_tar} ...")
            self.shell(f"am force-stop {self.package_name}")
            device_tar = "/sdcard/tiktok_session_backup.tar.gz"
            
            # Package shared_prefs and databases
            self.shell(f"tar -czf {device_tar} /data/data/{self.package_name}/shared_prefs /data/data/{self.package_name}/databases 2>/dev/null")
            self.run_cmd(["pull", device_tar, output_local_tar])
            self.shell(f"rm {device_tar}")
            logger.info(f"Session backup saved to {output_local_tar}")
            return True
        except Exception as e:
            logger.error(f"Failed to backup app session: {e}")
            return False

    def is_package_installed(self, package_name: Optional[str] = None) -> bool:
        """Checks if the official standard TikTok package is installed on the device."""
        target_pkg = package_name or getattr(self.config, 'tiktok_package', 'com.zhiliaoapp.musically')
        out = self.shell("pm list packages")

        # Helper to parse exact package names from 'package:com.example.app'
        def parse_packages(raw_out: str) -> set:
            pkgs = set()
            for line in raw_out.splitlines():
                line = line.strip()
                if line.startswith("package:"):
                    pkgs.add(line.split("package:", 1)[1].strip())
            return pkgs

        installed = parse_packages(out)

        # Purge TikTok Lite if present on emulator image so real TikTok runs exclusively
        if "com.zhiliaoapp.musically.go" in installed:
            logger.info("Purging stale TikTok Lite package (com.zhiliaoapp.musically.go) from device...")
            self.shell("pm uninstall com.zhiliaoapp.musically.go")
            out = self.shell("pm list packages")
            installed = parse_packages(out)

        candidate_packages = [
            target_pkg,
            "com.zhiliaoapp.musically",
            "com.ss.android.ugc.trill"
        ]
        for pkg in candidate_packages:
            if pkg and pkg in installed:
                self.package_name = pkg
                return True
        return False

    def install_apk(self, apk_path_or_url: str) -> bool:
        """Installs an APK from a local path or downloads from a remote URL and installs via ADB."""
        if not apk_path_or_url or not apk_path_or_url.strip():
            return False
        try:
            target_path = apk_path_or_url.strip()
            # If pre-downloaded APK exists in /tmp/tiktok.apk, use it immediately
            if (target_path.startswith("http://") or target_path.startswith("https://")) and os.path.exists("/tmp/tiktok.apk"):
                logger.info("Found pre-downloaded APK at /tmp/tiktok.apk; using local copy.")
                target_path = "/tmp/tiktok.apk"
            elif target_path.startswith("http://") or target_path.startswith("https://"):
                import requests
                logger.info(f"Downloading APK from {target_path} ...")
                local_apk = os.path.join(os.getcwd(), "app_download.apk")
                resp = requests.get(target_path, stream=True, timeout=(15, 120), headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code != 200:
                    logger.warning(f"Failed to download APK from {target_path}: HTTP {resp.status_code}")
                    return False
                with open(local_apk, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                target_path = local_apk

            if not os.path.exists(target_path):
                return False

            logger.info(f"Installing APK: {target_path} ...")
            res = self.run_cmd(["install", "-r", "-d", "-g", target_path], timeout=240)
            if "Success" in res.stdout or res.returncode == 0:
                logger.info("APK installation succeeded!")
                return True
            else:
                logger.warning(f"ADB install output: {res.stdout} {res.stderr}")
                return False
        except Exception as e:
            logger.warning(f"Could not install APK: {e}")
            return False

    def ensure_app_installed(self, apk_path_or_url: Optional[str] = None) -> bool:
        """Verifies full native TikTok is installed, or installs it automatically from configured APK source."""
        if self.is_package_installed():
            logger.info(f"[+] Native TikTok package '{self.package_name}' is already installed.")
            return True

        target_apk = apk_path_or_url or getattr(self.config, 'tiktok_apk_url', None) or "https://api.fgos.site/tiktok/assets/tiktok.apk"
        logger.info(f"Native TikTok not found on device. Installing from: {target_apk} ...")
        if target_apk and self.install_apk(target_apk):
            if self.is_package_installed():
                logger.info(f"[+] Native TikTok App successfully installed: {self.package_name}")
                return True

        logger.error("[-] FATAL: Failed to install Native TikTok App. Chrome fallback has been disabled.")
        return False

    def resolve_canonical_stream_info(self, raw_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Resolves short TikTok share links (tiktok.com/t/*, vm.tiktok.com/*, vt.tiktok.com/*)
        into their canonical target URL, room_id, and username.
        """
        if not raw_url or not raw_url.strip():
            return None, None, None

        resolved_url = raw_url.strip()
        room_id = None
        username = None

        # Check if it's a short link needing HTTP redirect unshortening
        if any(short in resolved_url for short in ["/t/", "vm.tiktok.com", "vt.tiktok.com"]):
            try:
                import requests
                headers = {
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                }
                resp = requests.get(resolved_url, headers=headers, allow_redirects=True, timeout=10)
                if resp.url and "tiktok.com" in resp.url and "/in/about" not in resp.url:
                    resolved_url = resp.url
                    logger.info(f"Unshortened URL: '{raw_url}' -> '{resolved_url}'")
            except Exception as e:
                logger.debug(f"Could not unshorten URL via network request: {e}")

        # Extract room ID
        match_room = re.search(r"live/(\d+)", resolved_url) or re.search(r"room_id=(\d+)", resolved_url)
        if match_room:
            room_id = match_room.group(1)

        # Extract username
        match_user = re.search(r"@([a-zA-Z0-9_\.]+)", resolved_url)
        if match_user:
            username = match_user.group(1)

        # If room_id is missing but username exists, fetch the live web page to resolve exact room_id
        if not room_id and username:
            try:
                import requests
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                }
                live_target = f"https://www.tiktok.com/@{username}/live"
                resp = requests.get(live_target, headers=headers, timeout=8)
                if resp.status_code == 200 and resp.text:
                    m = re.search(r'"roomId":"(\d+)"', resp.text)
                    if m:
                        room_id = m.group(1)
                        logger.info(f"[+] Resolved live room_id '{room_id}' for creator @{username} via live endpoint")
            except Exception as e:
                logger.debug(f"Could not resolve live room_id from @{username}/live: {e}")

        return resolved_url, room_id, username

    def open_live_stream(self, stream_url: str) -> bool:
        """Alias for launch_live_stream to support dynamic target switching."""
        return self.launch_live_stream(stream_url)

    def launch_live_stream(self, stream_url: str, room_id: Optional[str] = None, stream_user: Optional[str] = None) -> bool:
        """
        Launches the target TikTok Live stream inside the TikTok Mobile App.
        Guarantees opening the native mobile app rather than Chrome or browser fallback.
        """
        logger.info(f"Navigating to TikTok Live stream: URL='{stream_url}', room_id='{room_id}', user='{stream_user}'")
        
        # Ensure correct installed package name is set
        has_native = self.is_package_installed()

        # Resolve canonical stream info
        resolved_url, parsed_room_id, parsed_user = self.resolve_canonical_stream_info(stream_url)
        target_room_id = room_id or parsed_room_id
        target_user = stream_user or parsed_user
        target_url = resolved_url or stream_url

        if not has_native:
            logger.error(f"[-] Native TikTok App ({self.package_name}) is not installed on device!")
            raise RuntimeError(f"Native TikTok App ({self.package_name}) is required. Chrome fallback has been disabled.")

        # 1. Permanently disable Chrome to ensure native intent handling & pre-grant notifications
        self.shell("pm disable-user --user 0 com.android.chrome 2>/dev/null || true")
        self.shell("pm hide com.android.chrome 2>/dev/null || true")
        self.shell(f"pm grant {self.package_name} android.permission.POST_NOTIFICATIONS 2>/dev/null || true")

        # 2. Launch Native TikTok App directly via standard Activity Manager intent
        logger.info(f"Launching TikTok Native App ({self.package_name})...")
        self.shell(f"am start -a android.intent.action.MAIN -c android.intent.category.LAUNCHER -p {self.package_name}")
        time.sleep(4)
        self.dismiss_popups()

        # 3. Route directly into Live Room
        if target_room_id:
            logger.info(f"Navigating to Live Room ID: {target_room_id}...")
            self.shell(f'am start -a android.intent.action.VIEW -d "snssdk1233://live?room_id={target_room_id}" {self.package_name}')
            time.sleep(3)
        elif target_user:
            logger.info(f"Navigating to Live creator @{target_user}/live...")
            self.shell(f'am start -a android.intent.action.VIEW -d "https://www.tiktok.com/@{target_user}/live" {self.package_name}')
            time.sleep(3)
        elif target_url:
            logger.info(f"Navigating to Live URL: {target_url}...")
            self.shell(f'am start -a android.intent.action.VIEW -d "{target_url}" {self.package_name}')
            time.sleep(3)

        self.dismiss_popups()

        # 5. Verify that Live Stream is confirmed active with UI validation
        for attempt in range(1, 6):
            if self.is_live_stream_active():
                logger.info(f"[+] Live Stream confirmed active on attempt {attempt}!")
                return True
            logger.info(f"Attempt {attempt}: Verifying live room and auto-dismissing overlays...")
            self.dismiss_popups()
            if attempt in (2, 4):
                self.kickstart_video_surface()
            time.sleep(2)

        return True

    def kickstart_video_surface(self) -> None:
        """Forces TikTok video surface rendering by executing a vertical swipe gesture to clear blank/white screens."""
        w = self.screen_width or 720
        h = self.screen_height or 1280
        logger.info("Kickstarting video surface via vertical swipe gesture...")
        self.shell(f"input swipe {w // 2} {int(h * 0.75)} {w // 2} {int(h * 0.25)} 250")
        time.sleep(1)

    def dump_ui_hierarchy(self) -> str:
        """Dumps the current Android UI hierarchy XML using uiautomator."""
        try:
            self.shell("uiautomator dump /sdcard/window_dump.xml", timeout=10)
            xml_str = self.shell("cat /sdcard/window_dump.xml", timeout=10)
            if xml_str and "<hierarchy" in xml_str:
                return xml_str
        except Exception as e:
            logger.debug(f"uiautomator dump notice: {e}")
        return ""

    def find_element(self, text: Optional[str] = None, content_desc: Optional[str] = None, resource_id: Optional[str] = None, must_be_clickable: bool = False) -> Optional[Tuple[int, int]]:
        """
        Parses the current UI hierarchy XML to find an element matching text, content-desc, or resource-id.
        Prioritizes exact clickable/button matches over non-clickable static header titles.
        Returns the center coordinates (x, y) if found, or None.
        """
        xml_str = self.dump_ui_hierarchy()
        if not xml_str:
            return None

        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_str)
            
            nodes = list(root.iter('node'))

            def parse_bounds(b_str):
                m = re.findall(r'\[(\d+),(\d+)\]', b_str)
                if len(m) == 2:
                    x1, y1 = int(m[0][0]), int(m[0][1])
                    x2, y2 = int(m[1][0]), int(m[1][1])
                    return ((x1 + x2) // 2, (y1 + y2) // 2), y1
                return None, 0

            # Action button labels: searches for these must NEVER match long paragraph TextViews
            ACTION_LABELS = {
                "agree", "agree and continue", "agree & continue", "accept", "accept all",
                "continue", "confirm", "ok", "got it", "skip", "log in", "sign in", "sign up",
                "submit", "next", "i agree", "done", "close", "cancel"
            }
            search_key = (text or content_desc or "").strip().lower()
            is_action_search = search_key in ACTION_LABELS or (resource_id and any(k in resource_id.lower() for k in ["btn", "button"]))
            is_login_action_btn = (text and text.lower().strip() in ["log in", "continue", "next", "submit", "sign in"]) or (resource_id and "btn" in resource_id.lower())

            # Pass 1: Exact matches on action buttons / clickable elements
            for node in nodes:
                node_text = node.attrib.get('text', '').strip()
                node_desc = node.attrib.get('content-desc', '').strip()
                node_id = node.attrib.get('resource-id', '')
                bounds_str = node.attrib.get('bounds', '')
                is_clickable = (
                    node.attrib.get('clickable', 'false') == 'true' or
                    'button' in node.attrib.get('class', '').lower() or
                    'edittext' in node.attrib.get('class', '').lower()
                )

                matched = False
                if text and (text.strip().lower() == node_text.lower() or text.strip().lower() == node_desc.lower()):
                    matched = True
                elif content_desc and (content_desc.strip().lower() == node_desc.lower() or content_desc.strip().lower() == node_text.lower()):
                    matched = True
                elif resource_id and resource_id.strip().lower() == node_id.lower():
                    matched = True

                if matched and bounds_str:
                    coords, top_y = parse_bounds(bounds_str)
                    if coords:
                        if is_login_action_btn:
                            # Action buttons MUST be in the main body (y >= 180), never in top header bar
                            if top_y >= 180 and is_clickable:
                                return coords
                        elif is_clickable or top_y >= 180:
                            return coords

            # Pass 2: Any exact match (excluding top header for login action buttons)
            for node in nodes:
                node_text = node.attrib.get('text', '').strip()
                node_desc = node.attrib.get('content-desc', '').strip()
                node_id = node.attrib.get('resource-id', '')
                bounds_str = node.attrib.get('bounds', '')

                matched = False
                if text and (text.strip().lower() == node_text.lower() or text.strip().lower() == node_desc.lower()):
                    matched = True
                elif content_desc and (content_desc.strip().lower() == node_desc.lower() or content_desc.strip().lower() == node_text.lower()):
                    matched = True
                elif resource_id and resource_id.strip().lower() == node_id.lower():
                    matched = True

                if matched and bounds_str:
                    coords, top_y = parse_bounds(bounds_str)
                    if coords:
                        if is_login_action_btn and top_y < 180:
                            continue
                        return coords

            # Pass 3: Substring matches (guarded: NEVER match action buttons against paragraphs or unclickable TextViews)
            for node in nodes:
                node_text = node.attrib.get('text', '').strip()
                node_desc = node.attrib.get('content-desc', '').strip()
                node_id = node.attrib.get('resource-id', '')
                bounds_str = node.attrib.get('bounds', '')
                is_clickable = (
                    node.attrib.get('clickable', 'false') == 'true' or
                    'button' in node.attrib.get('class', '').lower() or
                    'edittext' in node.attrib.get('class', '').lower()
                )

                # Paragraph guard: For action-button searches, DO NOT allow matching against long paragraph TextViews
                if is_action_search:
                    target_candidate = node_text or node_desc
                    if not is_clickable:
                        continue
                    if len(target_candidate) > 28 or len(target_candidate) > len(search_key) + 8:
                        continue
                    if any(p in target_candidate for p in [".", ",", "\n", ";"]):
                        continue

                matched = False
                if text and (text.strip().lower() in node_text.lower() or text.strip().lower() in node_desc.lower()):
                    matched = True
                elif content_desc and (content_desc.strip().lower() in node_desc.lower() or content_desc.strip().lower() in node_text.lower()):
                    matched = True
                elif resource_id and resource_id.strip().lower() in node_id.lower():
                    matched = True

                if matched and bounds_str:
                    coords, top_y = parse_bounds(bounds_str)
                    if coords and (not is_login_action_btn or top_y >= 180):
                        return coords
        except Exception as e:
            logger.debug(f"Element parse error: {e}")
        return None

    def click_element(self, text: Optional[str] = None, content_desc: Optional[str] = None, resource_id: Optional[str] = None) -> bool:
        """Finds and taps on a visible UI element by text, description, or ID."""
        coords = self.find_element(text=text, content_desc=content_desc, resource_id=resource_id)
        if coords:
            x, y = coords
            logger.info(f"[+] UI Auto-Locator found '{text or content_desc or resource_id}' at ({x}, {y}). Tapping...")
            self.shell(f"input tap {x} {y}")
            return True
        return False

    def click_first_unchecked_checkbox(self) -> bool:
        """Finds and clicks the first visible unchecked checkbox or radio button on screen."""
        xml_str = self.dump_ui_hierarchy()
        if not xml_str:
            return False
        try:
            import xml.etree.ElementTree as ET
            import re
            root = ET.fromstring(xml_str)
            for node in root.iter('node'):
                cls = node.attrib.get('class', '').lower()
                checked = node.attrib.get('checked', 'false')
                if ('checkbox' in cls or 'radiobutton' in cls or 'check' in node.attrib.get('resource-id', '').lower()) and checked == 'false':
                    bounds = node.attrib.get('bounds', '')
                    m = re.findall(r'\[(\d+),(\d+)\]', bounds)
                    if len(m) == 2:
                        x = (int(m[0][0]) + int(m[1][0])) // 2
                        y = (int(m[0][1]) + int(m[1][1])) // 2
                        logger.info(f"[+] Found unchecked terms checkbox at ({x}, {y}). Checking...")
                        self.shell(f"input tap {x} {y}")
                        return True
        except Exception as e:
            logger.debug(f"Checkbox locate note: {e}")
        return False

    def close_legal_webview(self) -> bool:
        """Closes legal WebView overlay and returns to TikTok login flow by pressing Back exactly once."""
        logger.info("[+] Closing legal WebView: pressing Back exactly once...")
        if self.click_element(content_desc="Back") or self.click_element(text="Back") or \
           self.click_element(content_desc="Close") or self.click_element(text="Close"):
            time.sleep(0.8)
        else:
            self.shell("input keyevent 4")
            time.sleep(0.8)
        return not self.is_terms_or_policy_screen()

    def get_ui_text_content(self) -> str:
        """Dumps UI hierarchy and returns concatenated text of all visible elements."""
        xml_str = self.dump_ui_hierarchy()
        if not xml_str:
            return ""
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_str)
            texts = []
            for node in root.iter('node'):
                text = node.attrib.get('text', '').strip()
                desc = node.attrib.get('content-desc', '').strip()
                if text:
                    texts.append(text)
                if desc and desc != text:
                    texts.append(desc)
            return " ".join(texts)
        except Exception as e:
            logger.debug(f"XML text dump parse note: {e}")
            return ""

    def is_login_or_signup_screen(self) -> bool:
        """Checks if TikTok's Login or Sign-up screen is actively visible."""
        # A terms or legal policy document is NOT a login screen; it blocks the login screen
        if self.is_terms_or_policy_screen():
            return False

        ui_text = self.get_ui_text_content().lower()
        if not ui_text:
            out = self.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp'").lower()
            return "login" in out or "signup" in out or "authorize" in out
        
        login_phrases = [
            "log in to tiktok",
            "sign up for tiktok",
            "use phone / email / username",
            "use phone",
            "continue with google",
            "continue with facebook",
            "enter email or username",
            "email or username",
            "email / username",
            "already have an account",
            "already have an account? log in",
            "log in",
            "password",
            "verify email",
            "verification code",
            "phone / email",
            "phone",
            "email"
        ]
        return any(phrase in ui_text for phrase in login_phrases)

    def is_terms_or_policy_screen(self) -> bool:
        """
        Checks if TikTok is displaying a full Terms of Service or Privacy Policy legal document/WebView.
        Guarantees that a login/signup screen containing a footer legal disclaimer is NOT classified as a legal document.
        """
        ui_text = self.get_ui_text_content().lower()
        if not ui_text:
            return False

        # If UI does not contain any terms/privacy keywords, it is definitely not a legal screen
        if not any(k in ui_text for k in ["terms", "privacy", "policy"]):
            return False

        # Strong guard: Login and signup controls indicate a login/signup screen, NOT a legal document
        login_controls = [
            "use phone / email / username",
            "use phone / email",
            "use phone",
            "log in to tiktok",
            "sign up for tiktok",
            "continue with google",
            "continue with facebook",
            "continue with apple",
            "continue with twitter",
            "enter email or username",
            "email or username",
            "email / username",
            "already have an account",
            "log in",
            "sign up",
            "phone / email",
        ]
        if any(ctrl in ui_text for ctrl in login_controls):
            return False

        # Check for full legal document title/header
        has_doc_title = any(t in ui_text for t in [
            "terms of service | tiktok",
            "privacy policy | tiktok",
            "tiktok terms of service",
            "tiktok privacy policy",
        ])

        # Check for known legal activity (e.g. CrossPlatformActivity)
        try:
            fg = self.get_foreground_activity().lower()
            is_legal_activity = "crossplatformactivity" in fg or "webkit" in fg
        except Exception:
            is_legal_activity = False

        # Check for full legal body phrases
        legal_body_phrases = [
            "welcome to tiktok",
            "usds joint venture",
            "what services are covered",
            "contacting tiktok",
            "information we collect",
            "summary of key terms",
            "these terms of service",
            "user terms of service",
        ]
        has_legal_body = sum(1 for phrase in legal_body_phrases if phrase in ui_text) >= 1

        if has_doc_title:
            return True
        if is_legal_activity and any(k in ui_text for k in ["terms of service", "privacy policy", "terms and conditions"]):
            return True
        if has_legal_body and any(k in ui_text for k in ["terms of service", "privacy policy", "terms"]):
            return True

        return False

    def is_authenticated_user_feed(self) -> bool:
        """Verifies whether the TikTok app is in an authenticated user state."""
        if self.is_login_or_signup_screen() or self.is_terms_or_policy_screen():
            return False
        ui_text = self.get_ui_text_content().lower()
        if ui_text:
            has_nav = ("profile" in ui_text and "home" in ui_text) or ("for you" in ui_text) or ("following" in ui_text) or ("inbox" in ui_text)
            if has_nav:
                return True
        # If UI text is blank/sparse (video surface playing in software GLES AVD), verify foreground activity
        fg = self.get_foreground_activity().lower()
        if any(act in fg for act in ["mainactivity", ".main."]) and not any(k in fg for k in ["login", "signup", "auth", "verify", "crossplatform", "spark", "bullet", "webview"]):
            if not self.is_screen_visually_blank_or_white(threshold=0.96):
                return True
        return False

    def is_screen_visually_blank_or_white(self, threshold: float = 0.95) -> bool:
        """
        Determines whether the Android screen is visually blank or white (e.g. stalled WebView or GLES compositing hang).
        Samples pixels strictly within the application viewport (excluding top status bar y<50 and bottom nav y>h-50).
        """
        try:
            from PIL import Image
            cmd = [self.adb_bin]
            if self.device_id:
                cmd.extend(["-s", self.device_id])
            cmd.extend(["exec-out", "screencap", "-p"])
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=4)
            if res.returncode == 0 and len(res.stdout) > 1000:
                im = Image.open(io.BytesIO(res.stdout)).convert("RGB")
                w, h = im.size
                if w > 100 and h > 200:
                    cropped = im.crop((20, 50, w - 20, h - 50))
                    cropped.thumbnail((72, 128), Image.Resampling.NEAREST)
                    pixels = list(cropped.getdata())
                    if pixels:
                        white_count = sum(1 for r, g, b in pixels if r > 235 and g > 235 and b > 235)
                        ratio = white_count / len(pixels)
                        return ratio >= threshold
        except Exception as e:
            logger.debug(f"Visual blank check notice: {e}")
        return False

    def is_webview_or_blank_overlay(self) -> bool:
        """
        Multi-signal detection for WebView containers, legal overlays, or blank white screens.
        Combines foreground Activity inspection, visual blankness testing, and UIAutomator token analysis.
        """
        try:
            fg = self.get_foreground_activity().lower()
            known_containers = [
                "crossplatformactivity", "sparkactivity", "bulletcontaineractivity",
                "webkit", "webview", "terms", "agreement", "i18nsignupactivitywithnoanimation"
            ]
            if any(act in fg for act in known_containers):
                return True
        except Exception:
            pass

        # If already in an active login screen with input fields, it is NOT an overlay
        if self.is_login_or_signup_screen():
            return False

        # If already confirmed in authenticated feed, it is NOT an overlay
        if self.is_authenticated_user_feed():
            return False

        # Visual blank/white screen test (catches stalled WebViews even if residual accessibility nodes exist)
        if self.is_screen_visually_blank_or_white(threshold=0.95):
            return True

        # Sparse UI content check (e.g. only "Back" / "Report a problem" / "TikTok" with no form fields)
        ui_text = self.get_ui_text_content().strip().lower()
        if len(ui_text) == 0:
            return True

        # If text is sparse (< 50 chars) and contains only generic header/footer tokens without feed or login elements
        GENERIC_TOKENS = ["back", "report a problem", "tiktok", "close", "cancel", "help", "loading"]
        tokens = [t.strip() for t in ui_text.split() if t.strip()]
        if len(ui_text) < 50 and all(any(k in tok for k in GENERIC_TOKENS) for tok in tokens):
            return True

        return False

    def get_recovery_diagnostics(self) -> dict:
        """Captures diagnostic snapshot when recovery action is taken."""
        return {
            "timestamp": time.time(),
            "foreground_activity": self.get_foreground_activity(),
            "ui_text_summary": self.get_ui_text_content()[:120],
            "is_white_screen": self.is_screen_visually_blank_or_white(threshold=0.95),
            "is_overlay": self.is_webview_or_blank_overlay()
        }


    def is_live_stream_active(self) -> bool:
        """
        Verifies whether the device is actively inside the TikTok live room.
        Guarantees that login/signup screen is NOT visible and live elements or video activity are present.
        """
        if not self._is_tiktok_in_foreground():
            return False

        if self.is_login_or_signup_screen():
            logger.warning("[-] Live check failed: Login/Signup modal is active on screen.")
            return False

        ui_text = self.get_ui_text_content().lower()
        if any(k in ui_text for k in ["when's your birthdate", "enter your birthdate", "your birthdate won't be shown"]):
            logger.warning("[-] Live check failed: 'When's your birthdate?' modal is blocking screen. Auto-resolving...")
            self.handle_birthdate_modal()
            return False

        live_indicators = ["follow", "rose", "share", "send a comment", "gift", "tap to like", "host", "ranking", "live"]
        if any(ind in ui_text for ind in ["follow", "rose", "share", "gift", "send a comment"]):
            return True

        out = self.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp'").lower()
        return any(k in out for k in ["live", "mainactivity", "feed", "aweme"])

    def _is_tiktok_in_foreground(self) -> bool:
        """Checks if native TikTok is currently the foreground active app."""
        out = self.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp' || dumpsys activity activities | grep -E 'mResumedActivity|topResumedActivity'")
        return any(pkg in out for pkg in [
            self.package_name,
            "com.zhiliaoapp.musically",
            "com.ss.android.ugc.trill"
        ])

    def _is_login_screen_active(self) -> bool:
        """Checks if TikTok's Login/SignUp modal is currently blocking the screen."""
        return self.is_login_or_signup_screen()

    def handle_birthdate_modal(self) -> bool:
        """
        Detects and resolves TikTok's 'When's your birthdate?' onboarding modal.
        Randomly selects an adult birthdate between 1990 and 2003 (random month, day, year) and clicks Continue.
        """
        ui_text = self.get_ui_text_content().lower()
        if not any(k in ui_text for k in ["when's your birthdate", "enter your birthdate", "your birthdate won't be shown", "birthday"]):
            return False

        logger.info("🎂 [Onboarding] 'When's your birthdate?' modal detected. Selecting random date (1990-2003)...")
        w = self.screen_width or 720
        h = self.screen_height or 1280

        # Touch coordinates for Month, Day, and Year picker wheels
        month_x = int(w * 0.34)
        day_x = int(w * 0.50)
        year_x = int(w * 0.66)
        wheel_top = int(h * 0.47)
        wheel_bottom = int(h * 0.58)

        # 1. Randomize Month (swipe down 1-4 times)
        for _ in range(random.randint(1, 4)):
            self.shell(f"input swipe {month_x} {wheel_top} {month_x} {wheel_bottom} 100")
            time.sleep(0.12)

        # 2. Randomize Day (swipe down 2-5 times)
        for _ in range(random.randint(2, 5)):
            self.shell(f"input swipe {day_x} {wheel_top} {day_x} {wheel_bottom} 100")
            time.sleep(0.12)

        # 3. Scroll Year backwards by 22-35 years from 2025 into 1990-2003 (7-10 swipes down)
        for _ in range(random.randint(7, 10)):
            self.shell(f"input swipe {year_x} {wheel_top} {year_x} {wheel_bottom} 100")
            time.sleep(0.12)

        time.sleep(0.8)

        # Click the red Continue button
        if not self.click_element(text="Continue"):
            self.shell(f"input tap {w // 2} {int(h * 0.715)}")
        time.sleep(1.8)

        # Handle optional confirmation dialog ("Are you X years old? / Confirm")
        if self.click_element(text="Confirm") or self.click_element(text="OK") or self.click_element(text="Continue"):
            time.sleep(1.0)

        logger.info("🎂 [Onboarding] Birthdate modal resolved with adult year (1990-2003).")
        return True

    def dismiss_popups(self) -> None:
        """Dismisses common prompts (Full-screen tooltips, System ANRs, Notifications, Cookie consents) using UI Inspection."""
        logger.info("Inspecting UI hierarchy to dismiss modal dialogs...")
        
        # 1. Dismiss Android Immersive / Full-screen tooltips ("Got it" / "OK" / "Agree and continue")
        if self.click_element(text="Got it") or self.click_element(text="OK") or self.click_element(text="Agree and continue"):
            time.sleep(0.5)

        # 2. Dismiss System ANR Dialogs if present by exact button click
        if self.click_element(text="Wait") or self.click_element(text="Close app"):
            time.sleep(0.5)

        # 3. Dismiss Notification permission prompts ("Don't allow" / "Deny")
        if self.click_element(text="Don't allow") or self.click_element(text="Deny"):
            time.sleep(0.5)

        # 4. Dismiss onboarding / interest picker
        if self.click_element(text="Skip") or self.click_element(text="Start watching"):
            time.sleep(0.5)

        # 5. Dismiss 'Log in to TikTok' modal ONLY if an explicit close button is available
        if self.is_login_or_signup_screen():
            # Only click explicit close buttons; NEVER send Back key which closes login screen and switches apps!
            if not self.click_element(content_desc="Close"):
                self.click_element(text="Close")
            time.sleep(0.5)

        # 6. Dismiss/resolve 'When's your birthdate?' onboarding dialog
        self.handle_birthdate_modal()

    def get_safe_live_tap_coordinates(self) -> Tuple[int, int]:
        """
        Calculates safe (X, Y) coordinates inside the Live Stream video viewport.
        Avoids:
          - Top header (profile, close button, viewer count)
          - Bottom footer (comment input box, gift button, share button, roses)
          - Left chat box area
        """
        # Safe zone: Center-right quadrant of the live video
        min_x = int(self.screen_width * 0.45)
        max_x = int(self.screen_width * 0.75)
        
        min_y = int(self.screen_height * 0.35)
        max_y = int(self.screen_height * 0.60)
        
        x = random.randint(min_x, max_x)
        y = random.randint(min_y, max_y)
        return x, y

    def send_batch_likes(self, tap_count: int = 10, delay_ms: int = 150) -> int:
        """
        Executes a rapid batch of screen taps in the Live video area.
        In TikTok Mobile, each screen tap generates flying hearts and counts as a live stream like.
        Guarantees that taps ONLY occur when TikTok is the confirmed foreground application.
        """
        # Guardrail 1: If user is manually controlling (Home, Switch, Touch), pause background liking
        if time.time() < getattr(self, 'user_override_until', 0):
            return 0

        # Guardrail 2: If TikTok is not in foreground, do NOT tap and do NOT interrupt user
        if not self._is_tiktok_in_foreground():
            return 0

        # Guardrail 3: Fast check if login/signup screen is active via window focus (zero UI dump overhead)
        out_win = self.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp'").lower()
        if "login" in out_win or "signup" in out_win or "authorize" in out_win:
            logger.info("Login modal active on screen. Auto-dismissing to enter Guest mode...")
            self.dismiss_popups()
            return 0

        # Generate batch input command for zero round-trip latency
        tap_commands = []
        base_x, base_y = self.get_safe_live_tap_coordinates()
        
        for _ in range(tap_count):
            # Add small random jitter (±12 px) to mimic natural finger tapping
            jitter_x = base_x + random.randint(-12, 12)
            jitter_y = base_y + random.randint(-12, 12)
            tap_commands.append(f"input tap {jitter_x} {jitter_y}")
            if delay_ms > 0:
                tap_commands.append(f"sleep {delay_ms / 1000.0:.2f}")

        # Combine into single shell script execution
        batch_script = "; ".join(tap_commands)
        self.shell(batch_script, timeout=30)
        return tap_count

    def take_screenshot(self, destination_path: str) -> bool:
        """Captures a screenshot from the Android device for verification and telemetry."""
        try:
            device_tmp = "/sdcard/stream_snapshot.png"
            self.shell(f"screencap -p {device_tmp}")
            self.run_cmd(["pull", device_tmp, destination_path])
            self.shell(f"rm {device_tmp}")
            logger.info(f"Screenshot saved to {destination_path}")
            return True
        except Exception as e:
            logger.warning(f"Could not capture screenshot: {e}")
            return False

    def start_screen_record(self, output_path: str = "/sdcard/auth_session.mp4", time_limit: int = 180) -> bool:
        """Starts native Android screen recording in the background on the device."""
        try:
            # Clear any leftover previous recording
            self.shell(f"rm {output_path} 2>/dev/null || true")
            logger.info(f"Starting native Android screen recording to {output_path} (time_limit: {time_limit}s)...")
            self.shell(f"nohup screenrecord --bit-rate 2000000 --time-limit {time_limit} {output_path} >/dev/null 2>&1 &")
            return True
        except Exception as e:
            logger.warning(f"Could not start screen recording: {e}")
            return False

    def stop_screen_record(self, local_dest: str = "auth_session.mp4", remote_path: str = "/sdcard/auth_session.mp4") -> bool:
        """Gracefully stops native screen recording (SIGINT to write MP4 index) and pulls file to local runner workspace."""
        try:
            logger.info(f"Stopping screen recording and saving to {local_dest}...")
            # SIGINT (-2) is required for screenrecord to finalize MP4 file header container (moov atom)
            self.shell("pkill -2 screenrecord || kill -2 $(pidof screenrecord) 2>/dev/null || killall -2 screenrecord 2>/dev/null || true")
            time.sleep(1.8)
            res = self.run_cmd(["pull", remote_path, local_dest], timeout=45)
            if os.path.exists(local_dest) and os.path.getsize(local_dest) > 1000:
                logger.info(f"[+] Successfully pulled screen recording: {local_dest} ({os.path.getsize(local_dest)} bytes)")
                self.shell(f"rm {remote_path} 2>/dev/null || true")
                return True
            else:
                logger.warning(f"Screen recording pull did not produce a valid file: {res.stdout} {res.stderr}")
                return False
        except Exception as e:
            logger.warning(f"Error stopping screen recording: {e}")
            return False

    def capture_screen_base64(self) -> Optional[str]:
        """Captures screenshot, saves full PNG locally for artifact upload, and returns optimized 9:16 base64 JPEG for live web telemetry."""
        import base64
        import io

        raw_bytes = None

        # Strategy 1: Direct fast exec-out pipe
        try:
            cmd = [self.adb_bin]
            if self.device_id:
                cmd.extend(["-s", self.device_id])
            cmd.extend(["exec-out", "screencap", "-p"])
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            if res.returncode == 0 and len(res.stdout) > 500:
                raw_bytes = res.stdout
        except Exception as e:
            logger.debug(f"Direct screencap notice: {e}")

        # Strategy 2: Fallback pull via sdcard tmp file
        if not raw_bytes:
            try:
                tmp_path = "last_stream_view.png"
                if self.take_screenshot(tmp_path):
                    if os.path.exists(tmp_path):
                        with open(tmp_path, "rb") as f:
                            raw_bytes = f.read()
            except Exception as e:
                logger.debug(f"Fallback screencap notice: {e}")

        if not raw_bytes or len(raw_bytes) < 500:
            return None

        # 1. Save full resolution PNG for GitHub Actions artifact upload
        try:
            with open("last_stream_view.png", "wb") as f:
                f.write(raw_bytes)
        except Exception:
            pass

        # 2. Optimize for lightweight real-time Web Telemetry (~15KB)
        try:
            from PIL import Image
            im = Image.open(io.BytesIO(raw_bytes))
            im.thumbnail((360, 760), Image.Resampling.LANCZOS)
            buf = io.BytesIO()
            im.convert("RGB").save(buf, format="JPEG", quality=85)
            return base64.b64encode(buf.getvalue()).decode("utf-8")
        except Exception as e:
            logger.debug(f"Pillow thumbnail optimization fallback: {e}")
            return base64.b64encode(raw_bytes).decode("utf-8")
