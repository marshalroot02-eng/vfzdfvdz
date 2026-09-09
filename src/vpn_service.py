import os
import re
import time
import json
import random
import logging
import subprocess
from typing import Optional, List, Dict, Tuple, Any
from src.config import AppConfig

logger = logging.getLogger("VPNService")

class VPNState:
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    ROTATING = "ROTATING"
    FAILED = "FAILED"

class VPNService:
    """
    Manages high-reliability OpenVPN (Private Internet Access) and NordVPN connections
    on GitHub Actions Ubuntu runners with Android 14 emulator egress verification.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.provider = config.vpn_provider.lower()
        self.token = config.vpn_token
        self.country = config.vpn_country
        self.location = getattr(config, "vpn_location", None) or os.getenv("VPN_LOCATION", None)
        self.openvpn_config_dir = config.openvpn_config_dir
        self.openvpn_auth_file = config.openvpn_auth_file
        self.openvpn_log_file = "/tmp/openvpn.log"
        
        self.state = VPNState.DISCONNECTED
        self.available_configs: List[str] = self._discover_openvpn_configs()
        self.used_configs: set = set()
        
        self.current_location: Optional[str] = None
        self.current_ip: Optional[str] = None
        self.current_city: Optional[str] = None
        self.current_country: Optional[str] = None
        self.current_isp: Optional[str] = None
        self.connection_time: Optional[float] = None
        self.last_android_egress_match: Optional[bool] = None
        self.last_failure_reason: Optional[str] = None

    def setup_vpn(self) -> bool:
        """Initializes and connects to the specified VPN provider."""
        if self.provider == "none" or not self.provider:
            self.state = VPNState.DISCONNECTED
            logger.info("ℹ️ VPN is disabled ('none'). Using default runner network.")
            self.get_current_ip_info()
            return True

        if self.provider == "pia":
            return self._setup_pia()
        elif self.provider == "nordvpn":
            return self._setup_nordvpn()
        else:
            logger.warning(f"⚠️ Unknown VPN provider '{self.provider}'. Proceeding with default network.")
            self.state = VPNState.FAILED
            self.last_failure_reason = f"Unknown provider: {self.provider}"
            return False

    def _discover_openvpn_configs(self) -> List[str]:
        """Discovers all available .ovpn configuration files."""
        configs = []
        if os.path.isdir(self.openvpn_config_dir):
            for f in os.listdir(self.openvpn_config_dir):
                if f.endswith('.ovpn'):
                    configs.append(os.path.join(self.openvpn_config_dir, f))
        return sorted(configs)

    def _country_filtered_configs(self, country: Optional[str] = None) -> List[str]:
        """
        Returns .ovpn configs matching a country/region prefix.
        Normalizes inputs like 'us_california', 'us_chicago', 'us', 'uk_london', 'de'
        to ensure broad rotation pool within the desired jurisdiction.
        """
        if not self.available_configs:
            self.available_configs = self._discover_openvpn_configs()
            
        if not self.available_configs:
            return []

        if not country or not country.strip():
            return self.available_configs

        raw = country.strip().lower()
        c = raw
        for sep in ('_', '-'):
            if sep in c:
                c = c.split(sep, 1)[0]
                break

        prefixes = (f"{c}_", f"{c}-", f"{raw}_", f"{raw}-")
        exact = f"{c}.ovpn"
        matches = []
        for path in self.available_configs:
            name = os.path.basename(path).lower()
            if name == exact or any(name.startswith(p) for p in prefixes) or raw in name:
                matches.append(path)

        return matches if matches else self.available_configs

    def is_openvpn_running(self) -> bool:
        """Checks if the OpenVPN daemon process is running."""
        try:
            out = subprocess.check_output(["pgrep", "-l", "openvpn"], text=True)
            return "openvpn" in out.lower()
        except Exception:
            return False

    def is_tun0_active(self) -> bool:
        """Checks if tun0 network interface exists and is up."""
        try:
            out = subprocess.check_output(["ip", "addr", "show", "tun0"], text=True)
            return "inet" in out or "UP" in out
        except Exception:
            return os.path.exists("/sys/class/net/tun0")

    def _setup_pia(self) -> bool:
        """Configures credentials and connects via PIA OpenVPN daemon."""
        logger.info("🔧 [PIA VPN] Initializing OpenVPN daemon setup...")
        self.state = VPNState.CONNECTING

        # 1. Ensure auth file exists
        if not os.path.isfile(self.openvpn_auth_file):
            if self.config.pia_user and self.config.pia_pass:
                try:
                    os.makedirs(os.path.dirname(self.openvpn_auth_file), exist_ok=True)
                    with open(self.openvpn_auth_file, "w", encoding="utf-8") as f:
                        f.write(f"{self.config.pia_user}\n{self.config.pia_pass}\n")
                    subprocess.run(["sudo", "chmod", "600", self.openvpn_auth_file], check=False)
                except Exception as e:
                    logger.error(f"❌ Could not create OpenVPN auth file: {e}")
            else:
                logger.warning(f"⚠️ OpenVPN auth file '{self.openvpn_auth_file}' not found and PIA_USER/PIA_PASS not in env.")

        # 2. Check if OpenVPN is already running
        if self.is_openvpn_running() and self.is_tun0_active():
            logger.info("✅ [PIA VPN] OpenVPN tunnel already running on host.")
            self.state = VPNState.CONNECTED
            self.get_current_ip_info()
            return True

        # 3. Connect to preferred country or random server
        success = self.rotate_vpn(preferred_country=self.country)
        if not success:
            self.state = VPNState.FAILED
            self.last_failure_reason = "OpenVPN initial connection failed or timed out"
        return success

    def get_exact_location_config(self, target_location: str) -> Optional[str]:
        """
        Finds the exact .ovpn configuration matching a specific city/location (e.g. 'us_california', 'us_chicago').
        Prevents location hopping by locking to a dedicated server profile.
        """
        if not target_location or not target_location.strip():
            return None
        if not self.available_configs:
            self.available_configs = self._discover_openvpn_configs()

        loc = target_location.strip().lower().replace(" ", "_")
        target_name = f"{loc}.ovpn" if not loc.endswith(".ovpn") else loc

        # 1. Exact filename match
        for path in self.available_configs:
            if os.path.basename(path).lower() == target_name:
                return path

        # 2. Substring/prefix match (e.g. 'california' matches 'us_california.ovpn')
        for path in self.available_configs:
            base = os.path.basename(path).lower().replace(".ovpn", "")
            if loc == base or loc in base or base.endswith(f"_{loc}"):
                return path

        return None

    def get_pinned_config_for_entity(self, entity_id: Optional[str] = None, country: Optional[str] = None, location: Optional[str] = None) -> Optional[str]:
        """
        Deterministically assigns and locks a specific city/location profile to an entity
        (TikTok account username or runner_key) so it NEVER hops or switches locations across sessions.
        """
        target_loc = location or self.location
        if target_loc:
            exact = self.get_exact_location_config(target_loc)
            if exact:
                return exact

        eligible = self._country_filtered_configs(country or self.country)
        if not eligible:
            return None

        # Stable hash: Entity ALWAYS maps to the exact same config index across runs
        pin_key = (entity_id or getattr(self.config, 'runner_key', None) or getattr(self.config, 'runner_id', '0')).strip().lower()
        import hashlib
        stable_idx = int(hashlib.md5(pin_key.encode('utf-8')).hexdigest(), 16) % len(eligible)
        return eligible[stable_idx]

    def connect_openvpn(self, config_path: Optional[str] = None) -> bool:
        """Connects via OpenVPN using the specified or deterministic .ovpn config profile."""
        if not self.available_configs:
            self.available_configs = self._discover_openvpn_configs()

        if not config_path:
            # 1. Attempt deterministic pinning per runner/account so location never switches
            pin_key = getattr(self.config, 'runner_key', None) or getattr(self.config, 'runner_id', '0')
            config_path = self.get_pinned_config_for_entity(entity_id=pin_key, country=self.country, location=self.location)

            # 2. Fallback to first available unused config
            if not config_path:
                eligible = self._country_filtered_configs(self.country)
                unused = [c for c in eligible if c not in self.used_configs]
                if not unused:
                    self.used_configs.difference_update(eligible)
                    unused = eligible
                if not unused:
                    logger.error("❌ [PIA VPN] No .ovpn configuration files available.")
                    self.state = VPNState.FAILED
                    self.last_failure_reason = "No .ovpn configuration files found"
                    return False
                config_path = unused[0]

        self.used_configs.add(config_path)
        server_name = os.path.basename(config_path).replace('.ovpn', '')
        self.current_location = server_name
        self.state = VPNState.CONNECTING
        logger.info(f"🌐 [PIA VPN] Connecting to profile: {server_name} ...")

        try:
            # Clear old log file
            subprocess.run(["sudo", "rm", "-f", self.openvpn_log_file], capture_output=True, timeout=5)

            # Start OpenVPN daemon
            cmd = [
                "sudo", "openvpn",
                "--config", config_path,
                "--auth-user-pass", self.openvpn_auth_file,
                "--auth-nocache",
                "--daemon",
                "--log", self.openvpn_log_file
            ]
            subprocess.run(cmd, capture_output=True, timeout=10)

            # Monitor log for connection status (up to 30 seconds)
            start_t = time.time()
            for i in range(15):
                time.sleep(2)
                try:
                    res = subprocess.run(["sudo", "cat", self.openvpn_log_file], capture_output=True, text=True, timeout=5)
                    log_content = res.stdout
                    if "Initialization Sequence Completed" in log_content:
                        self.connection_time = time.time()
                        logger.info(f"✅ [PIA VPN] Initialization Sequence Completed for {server_name}!")
                        
                        # Verify tun0
                        time.sleep(2)
                        if not self.is_tun0_active():
                            logger.warning("⚠️ tun0 interface not immediately visible after initialization. Waiting 2s...")
                            time.sleep(2)

                        # Verify IP
                        info = self.get_current_ip_info()
                        if info.get("ip") != "Unknown":
                            self.state = VPNState.CONNECTED
                            logger.info(f"🎉 [PIA VPN] Connected! Host Public IP: {self.current_ip} ({self.current_city}, {self.current_country}) | ISP: {self.current_isp}")
                            return True
                        else:
                            logger.warning("⚠️ Connected to OpenVPN but public IP reflection failed.")
                            self.state = VPNState.CONNECTED
                            return True

                    if "AUTH_FAILED" in log_content:
                        logger.error(f"❌ [PIA VPN] AUTH_FAILED for {server_name}. Please verify PIA_USER and PIA_PASS.")
                        self.state = VPNState.FAILED
                        self.last_failure_reason = f"AUTH_FAILED on {server_name}"
                        return False
                except Exception:
                    pass

            logger.warning(f"⚠️ [PIA VPN] Connection timed out after 30s for {server_name}.")
            self.state = VPNState.FAILED
            self.last_failure_reason = f"Timeout connecting to {server_name}"
            return False
        except Exception as e:
            logger.error(f"❌ [PIA VPN] Failed to launch OpenVPN process: {e}")
            self.state = VPNState.FAILED
            self.last_failure_reason = str(e)
            return False

    def disconnect(self) -> bool:
        """Gracefully disconnects VPN and releases network routes."""
        logger.info("🔌 [VPN] Disconnecting tunnel and resetting routes...")
        try:
            if self.provider == "nordvpn":
                subprocess.run(["nordvpn", "disconnect"], capture_output=True, timeout=10)
            else:
                subprocess.run(["sudo", "killall", "openvpn"], capture_output=True, timeout=10)
            time.sleep(3) # Route recovery delay
            self.state = VPNState.DISCONNECTED
            self.current_ip = None
            logger.info("✅ [VPN] Tunnel disconnected.")
            return True
        except Exception as e:
            logger.debug(f"VPN disconnect notice: {e}")
            return False

    def rotate_vpn(self, preferred_country: Optional[str] = None, max_retries: int = 3) -> bool:
        """
        Kills current VPN connection, selects a new unused regional server from the pool,
        and establishes a new tunnel with verified fresh public IP (bounded retries).
        """
        logger.info("🔄 [VPN Rotation] Initiating server rotation...")
        self.state = VPNState.ROTATING
        
        target_country = preferred_country or self.country

        for attempt in range(1, max_retries + 1):
            self.disconnect()
            time.sleep(2)

            eligible = self._country_filtered_configs(target_country)
            unused = [c for c in eligible if c not in self.used_configs]
            if not unused:
                self.used_configs.difference_update(eligible)
                unused = eligible

            if unused:
                chosen = random.choice(unused)
                logger.info(f"🔄 [VPN Rotation] Attempt {attempt}/{max_retries}: Connecting to {os.path.basename(chosen)}...")
                if self.connect_openvpn(chosen):
                    logger.info(f"🎉 [VPN Rotation] Successfully rotated to: {self.current_location} (IP: {self.current_ip})")
                    return True
            else:
                logger.warning(f"⚠️ No eligible configs for country '{target_country}', falling back to global pool.")
                if self.connect_openvpn():
                    return True

        logger.error(f"❌ [VPN Rotation] Failed to establish VPN after {max_retries} attempts.")
        self.state = VPNState.FAILED
        self.last_failure_reason = f"Rotation failed after {max_retries} attempts"
        return False

    def get_current_ip_info(self) -> Dict[str, str]:
        """Fetches and verifies the active external public IP and ISP on the host runner."""
        import requests
        endpoints = [
            ("http://ip-api.com/json/?fields=query,city,country,isp",
             lambda d: (d.get("query"), d.get("city", "Unknown"), d.get("country", "Unknown"), d.get("isp", ""))),
            ("https://ipapi.co/json/",
             lambda d: (d.get("ip"), d.get("city", "Unknown"), d.get("country_name", "Unknown"), d.get("org", ""))),
            ("https://ipinfo.io/json",
             lambda d: (d.get("ip"), d.get("city", "Unknown"), d.get("country", "Unknown"), d.get("org", "")))
        ]

        for url, parser in endpoints:
            try:
                resp = requests.get(url, timeout=8)
                if resp.status_code == 200:
                    ip, city, country, isp = parser(resp.json())
                    if ip:
                        self.current_ip = ip
                        self.current_city = city
                        self.current_country = country
                        self.current_isp = isp
                        logger.info(f"📍 [Host Network] IP: {ip} | Location: {city}, {country} | ISP: {isp}")
                        return {"ip": ip, "city": city, "country": country, "isp": isp}
            except Exception:
                continue

        logger.warning("⚠️ Could not verify host public IP.")
        return {"ip": "Unknown", "city": "Unknown", "country": "Unknown", "isp": "Unknown"}

    def verify_android_egress(self, adb) -> Dict[str, Any]:
        """
        CRITICAL VERIFICATION:
        Compares host runner public IP and Android emulator public IP.
        Outputs explicit telemetry:
            HOST_PUBLIC_IP=...
            ANDROID_PUBLIC_IP=...
            VPN_PUBLIC_IP=...
            ANDROID_EGRESS_MATCH=true/false
        """
        logger.info("🔍 [Android Egress Verification] Comparing host VPN IP with Android emulator IP...")
        host_info = self.get_current_ip_info()
        host_ip = host_info.get("ip", "Unknown")

        android_ip = "Unknown"
        android_isp = "Unknown"
        has_internet = False
        match = False

        # 1. Query IP inside Android emulator via ADB (supports Toybox wget, busybox wget, or curl)
        try:
            cmd = (
                "toybox wget -q -O - 'http://ip-api.com/json/?fields=query,city,country,isp' 2>/dev/null || "
                "wget -q -O - 'http://ip-api.com/json/?fields=query,city,country,isp' 2>/dev/null || "
                "curl -s -m 8 'http://ip-api.com/json/?fields=query,city,country,isp'"
            )
            raw = adb.shell(cmd)
            if raw and "{" in raw:
                try:
                    data = json.loads(raw[raw.find("{"):raw.rfind("}")+1])
                    android_ip = data.get("query", "Unknown")
                    android_isp = data.get("isp", "Unknown")
                    has_internet = True
                except Exception:
                    # Fallback regex
                    m = re.search(r'"query"\s*:\s*"([^"]+)"', raw)
                    if m:
                        android_ip = m.group(1)
                        has_internet = True
        except Exception as e:
            logger.debug(f"Android egress check notice: {e}")

        # If curl failed inside emulator, test raw connectivity
        if not has_internet:
            try:
                ping_out = adb.shell("ping -c 1 -W 3 8.8.8.8")
                if "1 packets transmitted, 1" in ping_out or "bytes from" in ping_out:
                    has_internet = True
            except Exception:
                pass

        if host_ip != "Unknown" and android_ip != "Unknown" and host_ip == android_ip:
            match = True
            self.last_android_egress_match = True
        else:
            match = False
            self.last_android_egress_match = False

        # Output required telemetry format
        logger.info(f"{'='*60}")
        logger.info(f"HOST_PUBLIC_IP={host_ip}")
        logger.info(f"ANDROID_PUBLIC_IP={android_ip}")
        logger.info(f"VPN_PUBLIC_IP={self.current_ip or host_ip}")
        logger.info(f"ANDROID_EGRESS_MATCH={'true' if match else 'false'}")
        logger.info(f"ANDROID_HAS_INTERNET={'true' if has_internet else 'false'}")
        logger.info(f"{'='*60}")

        if match:
            logger.info("✅ [CONFIRMED] Android emulator internet traffic is strictly egressing through the host PIA VPN tunnel!")
        elif self.provider != "none":
            logger.error(f"❌ [MISMATCH] Android emulator IP ({android_ip}) does not match Host VPN IP ({host_ip})!")

        return {
            "host_ip": host_ip,
            "android_ip": android_ip,
            "match": match,
            "has_internet": has_internet,
            "android_isp": android_isp
        }

    def get_telemetry(self) -> Dict[str, Any]:
        """Returns structured telemetry data for dashboard / backend heartbeats."""
        return {
            "vpn_provider": self.provider,
            "vpn_state": self.state,
            "vpn_location": self.current_location,
            "vpn_public_ip": self.current_ip,
            "vpn_city": self.current_city,
            "vpn_country": self.current_country,
            "vpn_isp": self.current_isp,
            "android_egress_match": self.last_android_egress_match,
            "failure_reason": self.last_failure_reason
        }

    def _setup_nordvpn(self) -> bool:
        """Configures and connects via NordVPN CLI."""
        logger.info("Initializing NordVPN connection...")
        if not self.token:
            logger.error("NordVPN token is missing! Please provide VPN_TOKEN in secrets.")
            self.state = VPNState.FAILED
            self.last_failure_reason = "Missing VPN_TOKEN"
            return False

        try:
            self.state = VPNState.CONNECTING
            subprocess.run(["nordvpn", "login", "--token", self.token], check=True)
            subprocess.run(["nordvpn", "set", "technology", "NordLynx"], check=False)
            target = self.country if self.country else "United_States"
            subprocess.run(["nordvpn", "connect", target], check=True)
            time.sleep(3)
            self.state = VPNState.CONNECTED
            self.get_current_ip_info()
            return True
        except Exception as e:
            logger.error(f"NordVPN setup failed: {e}")
            self.state = VPNState.FAILED
            self.last_failure_reason = str(e)
            return False
