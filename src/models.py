from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import json

class RunnerState(str, Enum):
    INITIALIZING = "INITIALIZING"
    ADB_CONNECTING = "ADB_CONNECTING"
    ADB_CONNECTED = "ADB_CONNECTED"
    BOOTING = "BOOTING"
    ANDROID_BOOTING = "ANDROID_BOOTING"
    ANDROID_READY = "ANDROID_READY"
    READY = "READY"
    STARTING = "STARTING"
    APP_LAUNCHING = "APP_LAUNCHING"
    APP_STARTING = "APP_STARTING"
    APP_STARTED = "APP_STARTED"
    LOGIN_REQUIRED = "LOGIN_REQUIRED"
    ACTION_REQUIRED = "ACTION_REQUIRED"
    LOGIN_STARTED = "LOGIN_STARTED"
    LOGIN_SUBMITTED = "LOGIN_SUBMITTED"
    LOGIN_VERIFYING = "LOGIN_VERIFYING"
    LOGIN_SUBMITTING = "LOGIN_SUBMITTING"
    TWO_FA_REQUIRED = "2FA_REQUIRED"
    LOGGED_IN = "LOGGED_IN"
    LOGIN_FAILED = "LOGIN_FAILED"
    LOGIN_CHALLENGE = "LOGIN_CHALLENGE"
    LOGIN_RATE_LIMITED = "LOGIN_RATE_LIMITED"
    LOGIN_BLOCKED = "LOGIN_BLOCKED"
    AUTHENTICATED = "AUTHENTICATED"
    OPENING_LIVE = "OPENING_LIVE"
    TARGET_OPENING = "TARGET_OPENING"
    TARGET_VERIFIED = "TARGET_VERIFIED"
    WATCHING = "WATCHING"
    RUNNING = "RUNNING"
    RECOVERING = "RECOVERING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ERROR = "ERROR"

class RunnerRegistration(BaseModel):
    runner_key: str
    cluster_repo: str
    runner_index: int
    session_uuid: str
    android_version: str = "14"
    sdk_level: int = 34
    display_width: int = 1080
    display_height: int = 2400
    display_density: int = 420
    target_stream_url: str = ""
    workflow_run_id: Optional[int] = None

class RunnerHeartbeat(BaseModel):
    runner_key: str
    session_uuid: str
    state: RunnerState
    previous_state: Optional[RunnerState] = None
    account_username: Optional[str] = None
    likes_sent: int = 0
    elapsed_seconds: int = 0
    screenshot_b64: Optional[str] = None
    stream_url: Optional[str] = None
    package_name: Optional[str] = None
    foreground_activity: Optional[str] = None
    adb_state: str = "OK"
    app_state: str = "RUNNING"
    screen_state: str = "STREAMING"
    control_state: str = "CONNECTED"
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    log_snippet: Optional[str] = None

class TikTokAccount(BaseModel):
    id: str = Field(default="", description="Account identifier or row number")
    username: str = Field(default="", description="TikTok username or email")
    password: Optional[str] = Field(default=None, description="TikTok password")
    cookies_raw: Optional[str] = Field(default=None, description="Raw cookies JSON or sessionid string")
    session_backup_url: Optional[str] = Field(default=None, description="Cloud URL or base64 to saved app data session tarball")
    device_id: Optional[str] = Field(default=None, description="Persistent Android ID / hardware ID hex string")
    proxy: Optional[str] = Field(default=None, description="Proxy in format http://user:pass@host:port or host:port")
    vpn_location: Optional[str] = Field(default=None, description="Preferred PIA VPN City/Region (e.g. us_california, us_chicago, uk_london)")
    status: str = Field(default="Idle", description="Current account status")
    last_active: Optional[str] = Field(default=None, description="Timestamp of last activity")
    assigned_runner: Optional[str] = Field(default=None, description="Runner identifier currently using account")

    def get_cookies_list(self) -> List[Dict[str, Any]]:
        """Parses cookies_raw into Playwright cookie format."""
        if not self.cookies_raw:
            return []
        
        raw = self.cookies_raw.strip()
        
        # Format 1: Direct JSON array of cookies
        if raw.startswith("[") and raw.endswith("]"):
            try:
                cookies = json.loads(raw)
                formatted = []
                for c in cookies:
                    cookie = {
                        "name": c.get("name"),
                        "value": c.get("value"),
                        "domain": c.get("domain", ".tiktok.com"),
                        "path": c.get("path", "/"),
                    }
                    if "secure" in c:
                        cookie["secure"] = bool(c["secure"])
                    if "httpOnly" in c:
                        cookie["httpOnly"] = bool(c["httpOnly"])
                    if "sameSite" in c:
                        cookie["sameSite"] = c["sameSite"]
                    formatted.append(cookie)
                return formatted
            except Exception:
                pass
        
        # Format 2: Direct JSON object (key-value pairs)
        if raw.startswith("{") and raw.endswith("}"):
            try:
                cookies_obj = json.loads(raw)
                return [
                    {
                        "name": k,
                        "value": str(v),
                        "domain": ".tiktok.com",
                        "path": "/",
                    }
                    for k, v in cookies_obj.items()
                ]
            except Exception:
                pass

        # Format 3: Cookie string 'sessionid=abc12345; ttwid=xyz; ...' or pure sessionid token
        cookies = []
        if "=" in raw:
            for item in raw.split(";"):
                if "=" in item:
                    name, val = item.strip().split("=", 1)
                    cookies.append({
                        "name": name.strip(),
                        "value": val.strip(),
                        "domain": ".tiktok.com",
                        "path": "/",
                    })
        else:
            # Assumed to be pure sessionid token
            cookies.append({
                "name": "sessionid",
                "value": raw,
                "domain": ".tiktok.com",
                "path": "/",
                "secure": True,
                "httpOnly": True,
            })
            
        return cookies

    def get_playwright_proxy(self) -> Optional[Dict[str, str]]:
        """Parses proxy string into Playwright proxy dictionary."""
        if not self.proxy or not self.proxy.strip():
            return None
            
        p = self.proxy.strip()
        if not (p.startswith("http://") or p.startswith("https://") or p.startswith("socks5://")):
            p = f"http://{p}"
            
        from urllib.parse import urlparse
        parsed = urlparse(p)
        
        server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}" if parsed.port else f"{parsed.scheme}://{parsed.hostname}"
        proxy_dict = {"server": server}
        
        if parsed.username:
            proxy_dict["username"] = parsed.username
        if parsed.password:
            proxy_dict["password"] = parsed.password
            
        return proxy_dict
