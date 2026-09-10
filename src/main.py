import sys
import os
import time
import signal
import logging
import uuid
import threading
import shlex
import requests
from datetime import datetime
from rich.console import Console
from rich.logging import RichHandler

from src.config import config
from src.models import RunnerState, RunnerRegistration, RunnerHeartbeat
from src.adb_controller import ADBController
from src.vpn_service import VPNService
from src.auto_login import AutoLoginManager
from src.stream_forwarder import ScrcpyStreamForwarder

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger("TikTokBoosterRunner")

class TikTokBoosterOrchestrator:
    """Master TikTok Booster orchestrator with deterministic State Machine & Backend Telemetry Synchronization."""

    def __init__(self):
        self.config = config
        repo_env = os.getenv("GITHUB_REPOSITORY", "tiktok-live-booster")
        repo_short = repo_env.split("/")[-1].strip() or "tiktok-live-booster"
        self.runner_key = os.getenv('RUNNER_KEY') or f"{repo_short}_runner_{self.config.runner_index}"
        self.session_uuid = self.config.session_uuid or f"session_{uuid.uuid4().hex[:12]}"
        self.current_state = RunnerState.INITIALIZING
        self.previous_state = None
        self.is_running = True
        self.total_likes_sent = 0
        self.start_time = time.time()
        self.last_heartbeat_time = 0
        self.auto_recovery_event = threading.Event()
        self.recovery_action = None

        self.recent_logs = []

        self.adb = ADBController(self.config)
        self.vpn = VPNService(self.config)
        self.auto_login = AutoLoginManager(self.config, self.adb)
        self.stream_forwarder = ScrcpyStreamForwarder(self.config.backend_url, self.runner_key, token=self.config.runner_secret, command_callback=self._handle_live_ws_command, adb_controller=self.adb)

        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

        # Continuous background heartbeat loop ensuring runner telemetry never starves during long tasks
        self.heartbeat_thread = threading.Thread(target=self._background_heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def add_step_log(self, step: str, message: str, level: str = "INFO"):
        """Records a timestamped runner operational step log sent to the backend/WebSocket."""
        ts = datetime.utcnow().strftime("%H:%M:%S")
        entry = {
            "timestamp": ts,
            "step": step,
            "message": message,
            "level": level.upper()
        }
        self.recent_logs.append(entry)
        if len(self.recent_logs) > 60:
            self.recent_logs = self.recent_logs[-60:]
        if level.upper() == "ERROR":
            logger.error(f"[{step}] {message}")
        elif level.upper() == "WARNING":
            logger.warning(f"[{step}] {message}")
        else:
            logger.info(f"[{step}] {message}")

    def _report_account_cooldown(self, acc_id: int, reason: str, duration_minutes: int = 30):
        """Notifies central backend to place account in safety cooldown to prevent IP-wide burning."""
        try:
            url = f"{self.config.backend_url}/api/accounts/{acc_id}/cooldown"
            headers = {"Content-Type": "application/json"}
            if self.config.runner_secret:
                headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                headers["X-Runner-Secret"] = self.config.runner_secret
            payload = {
                "reason": reason,
                "duration_minutes": duration_minutes,
                "runner_key": self.runner_key
            }
            requests.post(url, json=payload, headers=headers, timeout=4)
            self.add_step_log("COOLDOWN", f"Account #{acc_id} placed in {duration_minutes}m safety cooldown", "WARNING")
        except Exception as e:
            logger.debug(f"Account cooldown report note: {e}")

    def _handle_exit(self, signum, frame):
        logger.warning("Shutdown signal received. Exiting gracefully...")
        self.is_running = False
        self.transition_state(RunnerState.STOPPING, reason="SIGINT/SIGTERM shutdown signal received")
        self.stream_forwarder.stop()
        self.send_heartbeat()
        self._notify_stop()
        self._notify_workflow_done(status="cancelled")

    def transition_state(self, new_state: RunnerState, reason: str = ""):
        """Logs deterministic state transition and immediately transmits high-speed telemetry update to backend."""
        if self.current_state != new_state:
            self.previous_state = self.current_state
            self.current_state = new_state
            ts = datetime.utcnow().isoformat() + "Z"
            self.current_reason = reason or f"Transitioned to {new_state.value}"
            prev_val = self.previous_state.value if self.previous_state else "INIT"
            self.add_step_log("STATE", f"{prev_val} -> {new_state.value}: {self.current_reason}")
            logger.info(f"[STATE_TRANSITION] runner={self.runner_key} session={self.session_uuid} previous={prev_val} new={self.current_state.value} reason='{self.current_reason}' timestamp={ts}")
            
            # Immediately notify central backend (fast sub-50ms transmission without taking heavy screenshot)
            try:
                self.send_heartbeat(include_screenshot=False, reason=self.current_reason)
            except Exception as e:
                logger.debug(f"Immediate state transition heartbeat notice: {e}")

    def register_runner(self) -> bool:
        """Registers this cloud Android worker with the central backend API."""
        url = f"{self.config.backend_url}/api/runners/register"
        payload = {
            "runner_key": self.runner_key,
            "cluster_repo": os.getenv("GITHUB_REPOSITORY", "kashifjutt7456-art/tiktok-live-booster"),
            "runner_index": self.config.runner_index,
            "session_uuid": self.session_uuid,
            "android_version": self.adb.android_version,
            "sdk_level": self.adb.sdk_level,
            "display_width": self.adb.screen_width,
            "display_height": self.adb.screen_height,
            "display_density": self.adb.screen_density,
            "target_stream_url": self.config.stream_url,
            "workflow_run_id": int(os.getenv("GITHUB_RUN_ID", 0)) or None
        }

        try:
            logger.info(f"Registering runner via {url} ...")
            res = requests.post(url, json=payload, timeout=8)
            if res.status_code == 200:
                logger.info(f"[+] Runner {self.runner_key} registered successfully with backend!")
                return True
            else:
                logger.warning(f"Registration response {res.status_code}: {res.text}")
        except Exception as e:
            logger.debug(f"Registration fallback note: {e}")
        return False

    def _refresh_network_telemetry(self, force: bool = False) -> dict:
        """
        Periodically resolves runner public egress IP, provider/org, and approximate location (refreshes every 3 minutes).
        Dual-source architecture: Attempts direct egress query from inside Android emulator first,
        falling back to host runner process with multi-provider failover (ip-api -> ipwho.is).
        """
        now = time.time()
        if not force and hasattr(self, '_net_telemetry_cache') and (now - getattr(self, '_net_telemetry_last_check', 0)) < 180:
            return self._net_telemetry_cache

        telemetry = {
            "public_ip": "Unavailable",
            "ip_provider": "Unavailable",
            "ip_location": "Unavailable",
            "ip_updated_at": datetime.utcnow().isoformat() + "Z",
            "egress_source": "unknown"
        }

        # Source 1: Direct Android Emulator Egress via ADB
        resolved_from_emulator = False
        if hasattr(self, 'adb') and self.adb and getattr(self.adb, 'device_id', None):
            try:
                raw = self.adb.shell("toybox wget -q -O - 'http://ip-api.com/json/?fields=status,query,org,isp,city,country' 2>/dev/null || wget -q -O - 'http://ip-api.com/json/?fields=status,query,org,isp,city,country' 2>/dev/null || curl -s -m 4 'http://ip-api.com/json/?fields=status,query,org,isp,city,country'")
                if raw and "{" in raw:
                    data = json.loads(raw[raw.find("{"):raw.rfind("}")+1])
                    if data.get("status") == "success" and data.get("query"):
                        telemetry["public_ip"] = data.get("query")
                        telemetry["ip_provider"] = data.get("org") or data.get("isp") or "Unavailable"
                        city = data.get("city")
                        country = data.get("country")
                        telemetry["ip_location"] = f"{city}, {country}" if (city and country) else (city or country or "Unavailable")
                        telemetry["ip_updated_at"] = datetime.utcnow().isoformat() + "Z"
                        telemetry["egress_source"] = "android_emulator"
                        resolved_from_emulator = True
                if not resolved_from_emulator:
                    raw_fb = self.adb.shell("toybox wget -q -O - 'https://ipwho.is/' 2>/dev/null || wget -q -O - 'https://ipwho.is/' 2>/dev/null || curl -s -m 4 'https://ipwho.is/'")
                    if raw_fb and "{" in raw_fb:
                        data_fb = json.loads(raw_fb[raw_fb.find("{"):raw_fb.rfind("}")+1])
                        if data_fb.get("success") and data_fb.get("ip"):
                            telemetry["public_ip"] = data_fb.get("ip")
                            conn = data_fb.get("connection", {})
                            telemetry["ip_provider"] = conn.get("org") or conn.get("isp") or "Unavailable"
                            city = data_fb.get("city")
                            country = data_fb.get("country")
                            telemetry["ip_location"] = f"{city}, {country}" if (city and country) else (city or country or "Unavailable")
                            telemetry["ip_updated_at"] = datetime.utcnow().isoformat() + "Z"
                            telemetry["egress_source"] = "android_emulator"
                            resolved_from_emulator = True
            except Exception as e:
                logger.debug(f"Direct emulator egress lookup notice: {e}")

        # Source 2: Host Runner Process Egress (Fallback if emulator query unavailable or failed)
        if not resolved_from_emulator:
            primary_ok = False
            try:
                res = requests.get("http://ip-api.com/json/?fields=status,query,org,isp,city,country", timeout=4)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("status") == "success" and data.get("query"):
                        telemetry["public_ip"] = data.get("query")
                        telemetry["ip_provider"] = data.get("org") or data.get("isp") or "Unavailable"
                        city = data.get("city")
                        country = data.get("country")
                        telemetry["ip_location"] = f"{city}, {country}" if (city and country) else (city or country or "Unavailable")
                        telemetry["ip_updated_at"] = datetime.utcnow().isoformat() + "Z"
                        telemetry["egress_source"] = "host_runner"
                        primary_ok = True
            except Exception as e:
                logger.debug(f"Primary host IP lookup exception: {e}")

            # Provider Fallback: ipwho.is if primary failed, rate-limited, or returned non-success status
            if not primary_ok:
                try:
                    res = requests.get("https://ipwho.is/", timeout=4)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("success") and data.get("ip"):
                            telemetry["public_ip"] = data.get("ip")
                            conn = data.get("connection", {})
                            telemetry["ip_provider"] = conn.get("org") or conn.get("isp") or "Unavailable"
                            city = data.get("city")
                            country = data.get("country")
                            telemetry["ip_location"] = f"{city}, {country}" if (city and country) else (city or country or "Unavailable")
                            telemetry["ip_updated_at"] = datetime.utcnow().isoformat() + "Z"
                            telemetry["egress_source"] = "host_runner"
                except Exception as e2:
                    logger.debug(f"Fallback host IP lookup exception: {e2}")

        self._net_telemetry_cache = telemetry
        self._net_telemetry_last_check = now
        return telemetry

    def _background_heartbeat_loop(self):
        """Continuously transmits background heartbeats every 4s so runner stays online during any long task."""
        while self.is_running:
            time.sleep(4.0)
            if not self.is_running:
                break
            try:
                if time.time() - self.last_heartbeat_time >= 3.5:
                    self.send_heartbeat(include_screenshot=False, reason=getattr(self, 'current_reason', 'Running'))
            except Exception as e:
                logger.debug(f"Background heartbeat note: {e}")

    def send_heartbeat(self, include_screenshot=False, reason: str = "") -> list:
        """Transmits state heartbeat to central backend and retrieves pending control commands."""
        self.last_heartbeat_time = time.time()
        url = f"{self.config.backend_url}/api/telemetry/heartbeat"
        screenshot_b64 = None
        if include_screenshot:
            screenshot_b64 = self.adb.capture_screen_base64()

        foreground_act = self.adb.get_foreground_activity()
        elapsed = int(time.time() - self.start_time)

        payload = {
            "runner_id": self.config.runner_index,
            "runner_key": self.runner_key,
            "session_uuid": self.session_uuid,
            "repo": os.getenv("GITHUB_REPOSITORY", "kashifjutt7456-art/tiktok-live-booster"),
            "account": "Active Live Session",
            "status": self.current_state.value,
            "state": self.current_state.value,
            "reason": reason or getattr(self, 'current_reason', f"State: {self.current_state.value}"),
            "likes_sent": self.total_likes_sent,
            "elapsed_seconds": elapsed,
            "screenshot_b64": screenshot_b64,
            "foreground_activity": foreground_act,
            "package_name": self.adb.package_name,
            "adb_state": "OK" if self.adb.device_id else "DISCONNECTED",
            "app_state": "RUNNING" if self.adb._is_tiktok_in_foreground() else "BACKGROUND",
            "screen_state": self.stream_forwarder.stream_state,
            "control_state": "CONNECTED" if self.stream_forwarder.control_socket else "POLLING",
            "log_snippet": f"{self.runner_key} | {self.current_state.value} | Stream: {self.stream_forwarder.stream_state} | Likes: {self.total_likes_sent}",
            "device_timestamp": datetime.utcnow().isoformat() + "Z",
            "recent_logs": list(self.recent_logs)
        }

        # Include resolved public egress IP telemetry
        payload.update(self._refresh_network_telemetry())

        # Include structured VPN telemetry
        if hasattr(self, 'vpn') and self.vpn:
            payload.update(self.vpn.get_telemetry())

        try:
            res = requests.post(url, json=payload, timeout=6)
            if res.status_code == 200:
                data = res.json()
                commands = data.get("commands", [])
                self._execute_commands(commands)

                # FGOS Pattern: Dynamic Live Target Sync via Heartbeat
                new_target = data.get("target_stream_url")
                if new_target and isinstance(new_target, str) and new_target.strip():
                    new_target = new_target.strip()
                    if new_target != self.config.stream_url:
                        logger.info(f"[TARGET_SWITCH] Live target updated from backend: {self.config.stream_url} -> {new_target}")
                        self.config.stream_url = new_target
                        if self.current_state in [RunnerState.RUNNING, RunnerState.READY, RunnerState.LOGGED_IN, RunnerState.WATCHING]:
                            logger.info(f"[TARGET_SWITCH] Dynamically redirecting active Android player to {new_target} without rebooting OS!")
                            self.adb.open_live_stream(new_target)
                            self.transition_state(RunnerState.RUNNING, reason=f"Dynamically switched to live stream {new_target}")

                return commands
        except Exception as e:
            logger.debug(f"Heartbeat network notice: {e}")
        return []

    def _handle_live_ws_command(self, payload: dict):
        """Immediately executes live control commands received directly over the Scrcpy WebSocket."""
        action = payload.get("action")
        logger.info(f"[LIVE_WS_COMMAND] Executing action {action} directly")
        self.adb.user_override_until = time.time() + 45
        if action in ["stop", "shutdown"]:
            self.is_running = False
            self.transition_state(RunnerState.STOPPED, reason="Stopped by operator via WebSocket")
            self.stream_forwarder.stop()
            self._notify_stop()
        elif action in ["restart", "restart_session"]:
            self.adb.shell(f"am force-stop {self.adb.package_name}")
            self.transition_state(RunnerState.STARTING, reason="Restart requested by operator via WebSocket")
            self.recovery_action = "restart"
            self.auto_recovery_event.set()
        elif action in ["retry", "retry_login"]:
            self.recovery_action = "retry"
            self.auto_recovery_event.set()
        elif action in ["resolve", "mark_resolved"]:
            self.recovery_action = "resolve"
            self.auto_recovery_event.set()

    def _execute_commands(self, commands: list):
        """Executes remote control commands received from the dashboard and sends acknowledgements."""
        if not commands:
            return

        self.adb.user_override_until = time.time() + 25

        for cmd in commands:
            cmd_id = cmd.get("id")
            action = cmd.get("action", "tap")
            logger.info(f"COMMAND_RECEIVED: ID={cmd_id} Action={action}")

            success = False
            err_msg = None

            try:
                if action in ["touch", "tap"]:
                    x = int(cmd.get("x", self.adb.screen_width // 2))
                    y = int(cmd.get("y", self.adb.screen_height // 2))
                    logger.info(f"Executing touch tap at ({x}, {y})")
                    self.adb.shell(f"input tap {x} {y}")
                    success = True
                elif action == "swipe":
                    x1 = int(cmd.get("x1", self.adb.screen_width // 2))
                    y1 = int(cmd.get("y1", int(self.adb.screen_height * 0.75)))
                    x2 = int(cmd.get("x2", self.adb.screen_width // 2))
                    y2 = int(cmd.get("y2", int(self.adb.screen_height * 0.25)))
                    logger.info(f"Executing swipe ({x1}, {y1}) -> ({x2}, {y2})")
                    self.adb.shell(f"input swipe {x1} {y1} {x2} {y2} 250")
                    success = True
                elif action == "key":
                    keycode = int(cmd.get("keycode", 4))
                    self.adb.shell(f"input keyevent {keycode}")
                    success = True
                elif action == "text" and cmd.get("text"):
                    text_val = str(cmd.get("text"))
                    try:
                        text_val.encode('ascii')
                        formatted = text_val.replace(" ", "%s")
                        quoted = shlex.quote(formatted)
                        self.adb.shell(f"input text {quoted}")
                        success = True
                    except UnicodeEncodeError:
                        logger.warning(f"[-] Text contains non-ASCII characters not supported by Android shell input: {text_val}")
                        err_msg = "Non-ASCII characters not supported by Android native input"
                        success = False
                elif action == "burst":
                    self.adb.send_batch_likes(tap_count=50, delay_ms=80)
                    success = True
                elif action in ["reload", "restart_app"]:
                    self.adb.shell(f"am force-stop {self.adb.package_name}")
                    time.sleep(1)
                    self.adb.launch_live_stream(self.config.stream_url, self.config.room_id, self.config.stream_user)
                    success = True
                elif action in ["stop", "shutdown"]:
                    logger.info(f"Stopping runner {self.runner_key} by operator command...")
                    self.is_running = False
                    self.transition_state(RunnerState.STOPPED, reason="Stopped by operator command")
                    self.stream_forwarder.stop()
                    self._notify_stop()
                    success = True
                elif action in ["restart", "restart_session"]:
                    logger.info(f"Restarting runner {self.runner_key} by operator command...")
                    self.adb.shell(f"am force-stop {self.adb.package_name}")
                    self.transition_state(RunnerState.STARTING, reason="Restart requested by operator")
                    self.recovery_action = "restart"
                    self.auto_recovery_event.set()
                    success = True
                elif action in ["retry", "retry_login"]:
                    logger.info(f"Operator requested retry for {self.runner_key}...")
                    self.recovery_action = "retry"
                    self.auto_recovery_event.set()
                    success = True
                elif action in ["resolve", "mark_resolved"]:
                    logger.info(f"Operator marked manual intervention resolved for {self.runner_key}...")
                    self.recovery_action = "resolve"
                    self.auto_recovery_event.set()
                    success = True
            except Exception as e:
                err_msg = str(e)
                logger.error(f"Failed to execute dashboard command {cmd_id}: {e}")

            # Send ack via canonical ACK route
            if cmd_id:
                try:
                    ack_url = f"{self.config.backend_url}/api/runners/{self.runner_key}/commands/{cmd_id}/ack"
                    headers = {}
                    if self.config.runner_secret:
                        headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                        headers["X-Runner-Secret"] = self.config.runner_secret
                    requests.post(ack_url, json={"success": success, "error": err_msg}, headers=headers, timeout=3)
                except Exception:
                    pass

    def _ack_command(self, cmd_id, status, error_message=None):
        if not cmd_id:
            return
        try:
            url = f"{self.config.backend_url}/api/runners/{self.runner_key}/commands/{cmd_id}/ack"
            headers = {}
            if self.config.runner_secret:
                headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                headers["X-Runner-Secret"] = self.config.runner_secret
            requests.post(url, json={"command_id": cmd_id, "status": status, "error": error_message}, headers=headers, timeout=3)
        except Exception:
            pass

    def _notify_stop(self):
        try:
            url = f"{self.config.backend_url}/api/runners/{self.config.runner_index}/stop"
            headers = {}
            if self.config.runner_secret:
                headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                headers["X-Runner-Secret"] = self.config.runner_secret
            requests.post(url, json={"runner_key": self.runner_key, "session_uuid": self.session_uuid}, headers=headers, timeout=4)
        except Exception:
            pass

    def _notify_workflow_done(self, status="success"):
        """FGOS Teardown Hook: Notifies central backend that this runner instance completed its workflow."""
        try:
            url = f"{self.config.backend_url}/api/runners/workflow-done"
            headers = {"Content-Type": "application/json"}
            if self.config.runner_secret:
                headers["x-runner-secret"] = self.config.runner_secret
            payload = {
                "repo_name": os.getenv("GITHUB_REPOSITORY", "kashifjutt7456-art/tiktok-live-booster"),
                "run_id": int(os.getenv("GITHUB_RUN_ID", 0)) or None,
                "status": status,
                "runner_index": self.config.runner_index
            }
            requests.post(url, json=payload, headers=headers, timeout=5)
            logger.info(f"[TEARDOWN] Notified central backend that workflow finished with status: {status}")
        except Exception as e:
            logger.debug(f"Workflow done notice: {e}")

    def start(self):
        """Starts the booster session."""
        return self.run_session()

    def run_session(self):
        """Orchestrates Milestone 1 automated Live Stream attendance and like burst session."""
        console.rule("[bold magenta]TikTok Booster Android 14 Runner Engine[/bold magenta]")
        logger.info(f"Runner Key: {self.runner_key} | Session UUID: {self.session_uuid}")
        logger.info(f"Target Stream: {self.config.stream_url or self.config.stream_user or self.config.room_id}")

        self.transition_state(RunnerState.INITIALIZING, reason="Runner process spawned; reading runtime configuration")

        # 1. VPN Setup
        if self.config.vpn_provider != "none":
            vpn_ok = self.vpn.setup_vpn()
            if not vpn_ok and self.config.vpn_provider == "pia":
                logger.error("[-] PIA VPN failed to connect. Halting runner to prevent unprotected traffic.")
                self.transition_state(RunnerState.ERROR, reason="PIA VPN initialization failed")
                sys.exit(1)

        # 2. ADB & Android 14 Connectivity
        self.transition_state(RunnerState.ADB_CONNECTING, reason="Establishing ADB connection to Android 14 AVD")
        if not self.adb.check_connection():
            logger.error("ADB connection failed!")
            self.transition_state(RunnerState.ERROR, reason="ADB connection failed to reach device")
            sys.exit(1)

        self.transition_state(RunnerState.ADB_CONNECTED, reason="ADB connected and authorized")
        self.transition_state(RunnerState.ANDROID_READY, reason=f"Android 14 system boot completed (API {self.adb.sdk_level})")

        # 3. Register in Central Backend & Start Real-Time Scrcpy Stream Forwarder
        self.register_runner()
        self.stream_forwarder.start_background()
        self.stream_forwarder.wait_until_connected(timeout=2.0)
        self.send_heartbeat(include_screenshot=True, reason="Scrcpy forwarder connected; initial screen snapshot taken")

        self.adb.wake_and_unlock()

        # 4. App Installation Verification (Native TikTok Mandatory)
        self.transition_state(RunnerState.APP_STARTING, reason="Verifying Native TikTok APK installation and launching app")
        if not self.adb.ensure_app_installed():
            logger.critical("[-] FATAL: Native TikTok Mobile App is not installed and failed to install. Halting runner.")
            self.transition_state(RunnerState.ERROR, reason="Native TikTok Mobile App missing or installation failed")
            sys.exit(1)

        # Verify initial emulator network egress
        if self.config.vpn_provider != "none":
            egress = self.vpn.verify_android_egress(self.adb)
            if self.config.vpn_provider == "pia" and not egress.get("has_internet"):
                logger.error("[-] Android emulator has no Internet connectivity through VPN. Halting.")
                self.transition_state(RunnerState.ERROR, reason="Android emulator has no Internet via VPN")
                sys.exit(1)

        # 5. Dynamic Account Assignment & In-App Authentication with Account Rotation
        candidate_accounts = self._fetch_candidate_accounts()
        authenticated_account = None

        if candidate_accounts and len(candidate_accounts) > 0:
            self.add_step_log("ACCOUNTS", f"Loaded {len(candidate_accounts)} candidate account(s) for rotation pool")
            logger.info(f"[+] Loaded {len(candidate_accounts)} candidate enabled account(s) for runner rotation pool.")
            
            def auth_callback(phase_name: str, phase_reason: str):
                state_mapping = {
                    "STARTING": RunnerState.STARTING,
                    "LOGIN_REQUIRED": RunnerState.LOGIN_REQUIRED,
                    "LOGIN_STARTED": RunnerState.LOGIN_STARTED,
                    "LOGIN_SUBMITTED": RunnerState.LOGIN_SUBMITTED,
                    "LOGIN_SUBMITTING": RunnerState.LOGIN_SUBMITTING,
                    "LOGIN_VERIFYING": RunnerState.LOGIN_VERIFYING,
                    "2FA_REQUIRED": RunnerState.TWO_FA_REQUIRED,
                    "AUTHENTICATED": RunnerState.AUTHENTICATED,
                    "LOGGED_IN": RunnerState.LOGGED_IN,
                    "LOGIN_FAILED": RunnerState.LOGIN_FAILED,
                    "LOGIN_CHALLENGE": RunnerState.LOGIN_CHALLENGE,
                    "LOGIN_RATE_LIMITED": RunnerState.LOGIN_RATE_LIMITED,
                    "LOGIN_BLOCKED": RunnerState.LOGIN_BLOCKED,
                }
                mapped_state = state_mapping.get(phase_name, RunnerState.LOGIN_SUBMITTING if "SUBMIT" in phase_name else RunnerState.LOGIN_REQUIRED)
                self.add_step_log("AUTH_PHASE", f"{phase_name}: {phase_reason}")
                self.transition_state(mapped_state, reason=f"{phase_name}: {phase_reason}")
                self.send_heartbeat(include_screenshot=True, reason=f"{phase_name}: {phase_reason}")

            for idx, account in enumerate(candidate_accounts):
                acc_id = account.get("id")
                acc_email = account.get("email") or account.get("username")
                masked_email = f"{acc_email[:3]}***@{acc_email.split('@')[-1]}" if "@" in str(acc_email) else str(acc_email)
                
                self.add_step_log("ROTATION", f"Candidate #{idx+1}/{len(candidate_accounts)}: {masked_email} (ID #{acc_id})")
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 [Account Rotation] Evaluating Candidate #{idx+1}/{len(candidate_accounts)}: {masked_email} (ID #{acc_id})")
                logger.info(f"{'='*60}")

                # 1. Connect or align to dedicated account VPN location
                if self.config.vpn_provider == "pia":
                    target_loc = account.get("vpn_location") or self.config.vpn_location
                    if target_loc:
                        exact_cfg = self.vpn.get_exact_location_config(target_loc)
                        if exact_cfg and self.vpn.current_location != os.path.basename(exact_cfg).replace('.ovpn', ''):
                            self.add_step_log("VPN", f"Aligning to dedicated account city: {target_loc}")
                            logger.info(f"🌐 [VPN Pinning] Connecting to account city '{target_loc}' ({os.path.basename(exact_cfg)})...")
                            self.vpn.connect_openvpn(exact_cfg)
                            self.vpn.verify_android_egress(self.adb)
                            self._refresh_network_telemetry(force=True)
                    elif idx > 0:
                        self.add_step_log("VPN", f"Rotating VPN IP for Candidate #{idx+1}")
                        logger.info(f"Rotating PIA VPN IP for Candidate #{idx+1}...")
                        self.vpn.rotate_vpn()
                        self.vpn.verify_android_egress(self.adb)
                        self._refresh_network_telemetry(force=True)

                # 2. Clean slate: Wipe app data
                self.add_step_log("CLEANUP", f"Wiping app data for clean login slate")
                logger.info(f"Wiping TikTok cache and state for clean slate...")
                self.adb.shell(f"pm clear {self.adb.package_name}")
                time.sleep(2)

                # 3. Set persistent randomized device identity
                base_dev = account.get("device_id")
                dev_id = f"{base_dev}_{uuid.uuid4().hex[:6]}" if base_dev else f"dev_{acc_id}_{uuid.uuid4().hex[:12]}"
                self.adb.set_persistent_device_identity(dev_id)
                self.add_step_log("DEVICE", f"Randomized device fingerprint: {dev_id[:16]}...")

                # 4. Configure Proxy if assigned
                if account.get("proxy"):
                    self.add_step_log("PROXY", f"Applying proxy: {account.get('proxy')}")
                    self.adb.configure_proxy(account.get("proxy"))

                # 5. Attempt login
                curr_ip_label = f" (IP: {self.vpn.current_ip})" if self.vpn.current_ip else ""
                self.transition_state(RunnerState.LOGIN_REQUIRED, reason=f"Starting login for candidate #{idx+1}: {masked_email}{curr_ip_label}")
                auth_success = self.auto_login.authenticate_account(account, state_callback=auth_callback)

                if auth_success:
                    self.add_step_log("AUTH_SUCCESS", f"Account {masked_email} authenticated successfully!")
                    logger.info(f"🎉 [Account Rotation] SUCCESS: Account {masked_email} authenticated into main feed!")
                    authenticated_account = account
                    self.transition_state(RunnerState.LOGGED_IN, reason=f"Account {masked_email} authenticated into feed")
                    self.send_heartbeat(include_screenshot=True, reason=f"Account {masked_email} authenticated")
                    break
                else:
                    fail_reason = getattr(self.auto_login, 'last_failure_reason', None)
                    self.add_step_log("AUTH_FAILURE", f"Account {masked_email} outcome: {fail_reason or self.current_state.value}", "WARNING")
                    logger.warning(f"⚠️ [Account Rotation] Account {masked_email} did not authenticate ({self.current_state}). Recording outcome...")

                    # IP Circuit-Breaker: Prevent burning subsequent accounts on dirty/rate-limited IP!
                    if fail_reason == "IP_RATE_LIMITED":
                        if acc_id:
                            self._report_account_cooldown(acc_id, "Maximum number of attempts reached (IP rate-limited by TikTok)", 60)
                        self.add_step_log("COOLDOWN", f"Account {masked_email} placed in 60m auto-cooldown due to attempt limit", "WARNING")

                        can_rotate_ip = (self.config.vpn_provider == "pia") or any(c.get("proxy") for c in candidate_accounts[idx+1:])
                        if can_rotate_ip and self.config.vpn_provider == "pia":
                            self.add_step_log("VPN", "Rotating PIA VPN egress IP to clear rate limit for next candidate...")
                            self.vpn.rotate_vpn()
                            self.vpn.verify_android_egress(self.adb)
                            self._refresh_network_telemetry(force=True)
                        elif not can_rotate_ip and idx + 1 >= len(candidate_accounts):
                            self.add_step_log("CIRCUIT_BREAKER", "IP Rate-Limit detected and all candidates exhausted.", "WARNING")

                    self.send_heartbeat(include_screenshot=True, reason=f"Account {masked_email} login outcome: {self.current_state}")
                    time.sleep(2)

            if authenticated_account:
                self._run_stream_session(account=authenticated_account)
            else:
                logger.error("[-] All candidate accounts failed authentication or are in cooldown. Guest Viewer mode disabled.")
                self.add_step_log("AUTH_FAILED", "All candidate accounts failed authentication. Halting runner (Guest mode disabled).", "ERROR")
                self.transition_state(RunnerState.LOGIN_FAILED, reason="Authentication failed for all candidate accounts; guest mode disabled")
                self.send_heartbeat(include_screenshot=True, reason="All candidate accounts failed authentication")
                self._notify_workflow_done(status="failed")
                sys.exit(1)
        else:
            logger.error("[-] No candidate accounts assigned in backend. Guest Viewer mode disabled.")
            self.add_step_log("AUTH_FAILED", "No accounts assigned. Halting runner (Guest mode disabled).", "ERROR")
            self.transition_state(RunnerState.LOGIN_FAILED, reason="No accounts assigned; guest mode disabled")
            self.send_heartbeat(include_screenshot=True, reason="No accounts assigned")
            self._notify_workflow_done(status="failed")
            sys.exit(1)

    def _run_manual_recovery_loop(self, reason: str = ""):
        """
        Interactive Recovery State:
        Keeps remote scrcpy screen, ADB, and telemetry alive so operator can intervene,
        solve CAPTCHA, type credentials, and click 'Retry' or 'Mark Resolved'.
        """
        st = RunnerState.ACTION_REQUIRED if any(k in reason.lower() for k in ["challenge", "captcha", "puzzle", "2fa", "blocked"]) else RunnerState.LOGIN_REQUIRED
        self.transition_state(st, reason=reason)
        logger.info(f"=== [Manual Recovery Mode] Entered {st.value}. Remote screen is LIVE at dashboard ===")
        
        self.recovery_action = None
        self.auto_recovery_event.clear()
        
        while self.is_running:
            self.send_heartbeat(include_screenshot=True, reason=getattr(self, 'current_reason', reason))
            
            if self.recovery_action == "resolve":
                logger.info("[Manual Recovery] Operator marked challenge resolved! Checking foreground state...")
                self.recovery_action = None
                if self.adb.is_authenticated_user_feed() or self.adb.is_live_stream_active():
                    self.transition_state(RunnerState.READY, reason="Operator resolved challenge; app ready")
                    self._run_stream_session(account=None)
                    return
                else:
                    logger.info("[Manual Recovery] Proceeding to live stream following operator resolution...")
                    self.transition_state(RunnerState.READY, reason="Operator resolved challenge; launching live room")
                    self._run_stream_session(account=None)
                    return
            elif self.recovery_action == "retry":
                logger.info("[Manual Recovery] Operator requested retry! Attempting re-authentication...")
                self.recovery_action = None
                candidate_accounts = self._fetch_candidate_accounts()
                if candidate_accounts and len(candidate_accounts) > 0:
                    self.transition_state(RunnerState.LOGIN_REQUIRED, reason="Retrying authentication with assigned test account")
                    auth_success = self.auto_login.authenticate_account(candidate_accounts[0])
                    if auth_success:
                        self.transition_state(RunnerState.LOGGED_IN, reason="Authentication succeeded on retry")
                        self._run_stream_session(account=candidate_accounts[0])
                        return
                    else:
                        self.transition_state(RunnerState.ACTION_REQUIRED, reason="Retry did not succeed; remote screen remains live")
                else:
                    self.transition_state(RunnerState.READY, reason="No account assigned; running in Guest mode")
                    self._run_stream_session(account=None)
                    return
            elif self.recovery_action == "restart":
                logger.info("[Manual Recovery] Restarting session...")
                self.recovery_action = None
                self.transition_state(RunnerState.STARTING, reason="Restarting session by operator request")
                return self.run_session()

            self.auto_recovery_event.wait(timeout=2.5)
            self.auto_recovery_event.clear()

        # 6. Session Finished
        self.transition_state(RunnerState.STOPPED, reason="Session duration completed cleanly")
        self.send_heartbeat(include_screenshot=True, reason="Final session completion heartbeat")
        self._notify_stop()
        logger.info(f"Milestone 1 Session Finished! Total likes: {self.total_likes_sent}")

    def _fetch_assigned_account(self) -> dict:
        """Fetches assigned TikTok account with decrypted secrets from central backend."""
        try:
            url = f"{self.config.backend_url}/api/accounts/runner-assignment/{self.runner_key}"
            headers = {}
            if self.config.runner_secret:
                headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                headers["X-Runner-Secret"] = self.config.runner_secret
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if data.get("has_account") and data.get("account"):
                    return data.get("account")
        except Exception as e:
            logger.debug(f"Backend account assignment fetch note: {e}")
        return None

    def _fetch_candidate_accounts(self) -> list:
        """Fetches candidate enabled TikTok accounts for rotation from central backend API."""
        try:
            url = f"{self.config.backend_url}/api/accounts/runner-assignment/{self.runner_key}"
            headers = {}
            if self.config.runner_secret:
                headers["Authorization"] = f"Bearer {self.config.runner_secret}"
                headers["X-Runner-Secret"] = self.config.runner_secret
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                data = res.json()
                accounts = []
                # 1. Primary candidate account
                if data.get("has_account") and data.get("account"):
                    accounts.append(data.get("account"))
                # 2. Additional pool candidates for auto-failover & rotation
                for acc in data.get("accounts_pool", []):
                    if not any(a.get("id") == acc.get("id") for a in accounts):
                        accounts.append(acc)
                return accounts
        except Exception as e:
            logger.debug(f"Backend candidate accounts fetch note: {e}")
        return []

    def _run_stream_session(self, account=None):
        acc_label = f"[{account.get('username')}]" if account and isinstance(account, dict) and account.get('username') else (f"[{account.username}]" if account and hasattr(account, 'username') else "[Guest-Viewer]")
        logger.info(f"=== Starting Session for {acc_label} ===")

        # 1. Identity & Proxy
        if account and isinstance(account, dict):
            if account.get("device_id"):
                self.adb.set_persistent_device_identity(account.get("device_id"))
            if account.get("proxy"):
                self.adb.configure_proxy(account.get("proxy"))
        elif account and hasattr(account, 'device_id') and account.device_id:
            self.adb.set_persistent_device_identity(account.device_id)

        # 2. Launch Target Stream
        self.transition_state(RunnerState.OPENING_LIVE, reason=f"Opening target live stream room: {self.config.stream_url}")
        self.adb.launch_live_stream(
            stream_url=self.config.stream_url,
            room_id=self.config.room_id,
            stream_user=self.config.stream_user
        )

        time.sleep(3)
        self.adb.dismiss_popups()

        if self.adb.is_login_or_signup_screen():
            logger.info("Screen is on login/signup page. Auto-dismissing to enter Live Room...")
            self.adb.dismiss_popups()
            # Re-trigger live room navigation intent
            if self.config.stream_url:
                self.adb.shell(f'am start -a android.intent.action.VIEW -d "{self.config.stream_url}" {self.adb.package_name}')
                time.sleep(2)

        if self.adb.is_live_stream_active():
            self.transition_state(RunnerState.WATCHING, reason="TikTok Live stream player confirmed active and receiving video")
        else:
            logger.info("Live player not confirmed active yet; kickstarting video surface...")
            self.adb.kickstart_video_surface()
            time.sleep(2)
            if self.adb.is_live_stream_active():
                self.transition_state(RunnerState.WATCHING, reason="TikTok Live stream player confirmed active after kickstart")
            else:
                self.transition_state(RunnerState.OPENING_LIVE, reason="Waiting for live player buffer to confirm active stream")

        self.send_heartbeat(include_screenshot=True, reason="Live room loaded, starting auto-liker loop")

        duration_seconds = self.config.duration_minutes * 60
        start_time = time.time()
        taps_per_burst = 10
        bursts_per_minute = max(1, self.config.likes_per_minute // taps_per_burst)
        interval_between_bursts = 60.0 / bursts_per_minute

        last_burst_time = 0
        last_heartbeat_time = 0
        last_screenshot_time = 0
        last_stream_reopen_time = time.time()

        self.transition_state(RunnerState.RUNNING, reason=f"Auto-liker active at {self.config.likes_per_minute} likes/min target")

        while self.is_running and (time.time() - start_time) < duration_seconds:
            now = time.time()

            # Auto-reconnect if stream dropped
            if now > getattr(self.adb, 'user_override_until', 0) and (now - last_stream_reopen_time) >= 20:
                if not self.adb.is_live_stream_active():
                    self.transition_state(RunnerState.RECOVERING, reason="Live player inactive; dismissing overlays and re-launching room")
                    self.adb.dismiss_popups()
                    if self.config.room_id:
                        self.adb.shell(f'am start -a android.intent.action.VIEW -d "snssdk1233://live?room_id={self.config.room_id}" {self.adb.package_name}')
                    elif self.config.stream_url:
                        self.adb.shell(f'am start -a android.intent.action.VIEW -d "{self.config.stream_url}" {self.adb.package_name}')
                else:
                    self.transition_state(RunnerState.RUNNING, reason="Live player active and tapping")
                last_stream_reopen_time = now

            # Execute Heart Likes Burst (high-speed loop with zero UI-dump overhead)
            if now - last_burst_time >= interval_between_bursts:
                if self.adb._is_tiktok_in_foreground():
                    taps = self.adb.send_batch_likes(tap_count=taps_per_burst, delay_ms=120)
                    self.total_likes_sent += taps
                last_burst_time = now

            # Send Telemetry & Process Remote Commands every 2.5s; throttle heavy screencap to once every 30s
            if now - last_heartbeat_time >= 2.5:
                take_shot = (now - last_screenshot_time >= 30.0)
                self.send_heartbeat(include_screenshot=take_shot)
                if take_shot:
                    last_screenshot_time = now
                last_heartbeat_time = now

            time.sleep(0.3)

        if not self.is_running:
            logger.info("Session stopped early by operator command. Exiting cleanly.")
            self.transition_state(RunnerState.STOPPED, reason="Session terminated by operator")
            self.stream_forwarder.stop()
            self._notify_stop()
            sys.exit(0)

        logger.info(f"Session finished after {int(time.time() - start_time)} seconds. Total likes sent: {self.total_likes_sent}")
        self.transition_state(RunnerState.COMPLETED, reason=f"Session duration completed normally ({self.config.duration_minutes}m)")
        self.stream_forwarder.stop()
        self.send_heartbeat(include_screenshot=True, reason="Session duration completed normally")
        self._notify_workflow_done(status="success")

def main():
    orchestrator = TikTokBoosterOrchestrator()
    orchestrator.start()

if __name__ == "__main__":
    main()

