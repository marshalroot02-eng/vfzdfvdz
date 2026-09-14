"""
TikTok Booster - Automated In-App Authentication & Onboarding Engine
Deterministic, instrumented login state machine supporting:
- Clean session reset (pm clear)
- Native UI Hierarchy element locating & clicking
- In-App Email & Password Entry with soft keyboard auto-dismissal
- Automated Gmail IMAP 2FA Code Extraction with timestamp freshness & auto-resend
- Native Android Screen Recording (.mp4) and step-by-step screenshot captures
- CAPTCHA / Puzzle Challenge Detection (LOGIN_BLOCKED)
- Authoritative Post-Auth UI Validation (LOGIN_SUCCESS vs LOGIN_FAILED)
"""

import os
import time
import hashlib
import logging
import requests
from typing import Optional, Callable
from src.config import AppConfig
from src.adb_controller import ADBController
from src.email_service import GmailVerificationService

logger = logging.getLogger("AutoLoginManager")

class AutoLoginManager:
    def __init__(self, config: AppConfig, adb: ADBController):
        self.config = config
        self.adb = adb
        self.last_failure_reason = None

    def _generate_device_id(self, account_identifier: str) -> str:
        """Generates a deterministic 16-hex Android ID based on account name."""
        return hashlib.sha256(account_identifier.encode("utf-8")).hexdigest()[:16]

    def _capture_checkpoint(self, name: str) -> None:
        """Captures milestone screenshot into auth_recordings/ and mirrors to last_stream_view.png."""
        try:
            os.makedirs("auth_recordings", exist_ok=True)
            filepath = os.path.join("auth_recordings", f"{name}.png")
            self.adb.take_screenshot(filepath)
            if os.path.exists(filepath):
                import shutil
                shutil.copyfile(filepath, "last_stream_view.png")
        except Exception as e:
            logger.debug(f"Checkpoint save note: {e}")

    def authenticate_account(self, account: dict, state_callback: Optional[Callable[[str, str], None]] = None) -> bool:
        """
        Public entry point for account authentication.
        Instruments native screen recording and ensures video artifacts are pulled.
        """
        self.adb.start_screen_record("/sdcard/auth_session.mp4", time_limit=180)
        try:
            return self._execute_authentication(account, state_callback)
        finally:
            self.adb.stop_screen_record("auth_session.mp4", "/sdcard/auth_session.mp4")
            self._capture_checkpoint("08_final_state")

    def _execute_authentication(self, account: dict, state_callback: Optional[Callable[[str, str], None]] = None) -> bool:
        """
        Executes the complete automated login state machine:
        1. Clean State (pm clear)
        2. Detect Login/Sign-up screen
        3. Click 'Use phone / email / username' -> 'Email / Username' tab
        4. Enter username + password with keyboard auto-dismissal -> Submit
        5. Explicitly verify one of:
           A. Authenticated user feed -> AUTHENTICATED / LOGIN_SUCCESS
           B. Email 2FA -> 2FA_REQUIRED -> Gmail IMAP -> Submit -> Verify -> Resend on error
           C. Incorrect credentials -> LOGIN_FAILED
           D. CAPTCHA / Challenge -> LOGIN_BLOCKED
           E. Login screen remains visible -> LOGIN_FAILED (LOGIN_SCREEN_STILL_VISIBLE)
        """
        username = account.get("username") or account.get("email") or "guest"
        password = account.get("password") or ""
        gmail_addr = account.get("gmail_address")
        gmail_pwd = account.get("gmail_app_password")
        acc_id = account.get("id")

        masked_acc = f"{username[:3]}...@{username.split('@')[-1]}" if "@" in username else username
        logger.info(f"=== [Auth State Machine] Starting In-App Authentication for {masked_acc} ===")

        def report(st: str, reason: str = ""):
            logger.info(f"[AUTH_PHASE] {st}: {reason}")
            if state_callback:
                state_callback(st, reason)
            if acc_id:
                try:
                    url = f"{self.config.backend_url}/api/accounts/{acc_id}/login-status"
                    requests.post(url, json={"login_status": st, "error_message": reason}, timeout=3)
                except Exception:
                    pass

        # 1. Clean Slate: Wipe previous app data
        logger.info(f"Clearing existing application data and authentication state for {self.adb.package_name}...")
        self.adb.shell(f"pm clear {self.adb.package_name}")
        time.sleep(2)

        # 2. Set persistent device identity
        device_id = account.get("device_id") or self._generate_device_id(username)
        self.adb.set_persistent_device_identity(device_id)

        # 3. Configure Proxy if assigned
        if account.get("proxy"):
            self.adb.configure_proxy(account.get("proxy"))

        # 4. Launch clean TikTok Application & Ensure Foreground
        report("STARTING", "Starting clean TikTok native Android activity")
        if not self._ensure_tiktok_foreground():
            logger.error(f"[-] TikTok could not be brought to foreground for {masked_acc}.")
            report("LOGIN_FAILED", "TikTok failed to enter foreground")
            return False

        width = self.adb.screen_width or 720
        height = self.adb.screen_height or 1280

        # 5. Dismiss initial onboarding prompts (Terms, Interests, Swipe Up)
        self._dismiss_initial_onboarding(width, height)
        self._capture_checkpoint("01_app_started")

        if not password:
            logger.info(f"No password provided for {masked_acc}. Proceeding in Guest mode.")
            report("LIVE_BROWSING_READY", "Running as Guest Viewer")
            return True

        # 6. Navigate into Login Screen
        report("LOGIN_REQUIRED", "Detecting login screen and navigating to Email login tab")
        self._ensure_tiktok_foreground()

        # If Terms of Service / Privacy Policy prompt is showing, close/agree to it!
        for _ in range(3):
            if not self.adb.is_terms_or_policy_screen():
                break
            logger.info("Detected Terms/Privacy legal overlay before login navigation. Dismissing...")
            self.handle_terms_and_conditions(width, height)
            self.adb.close_legal_webview()
            time.sleep(1.0)
        
        # Check if birthdate modal is already on screen
        if self.adb.handle_birthdate_modal():
            time.sleep(2)
            self.handle_terms_and_conditions(width, height)

        # Check if already on login screen, else tap Profile in bottom right
        if not self.adb.is_login_or_signup_screen():
            logger.info("Opening Profile tab to trigger login prompt...")
            if not self.adb.click_element(text="Profile"):
                if self.adb._is_tiktok_in_foreground():
                    self.adb.shell(f"input tap {int(width * 0.90)} {int(height * 0.96)}")
            time.sleep(3)

        if self.adb.handle_birthdate_modal():
            time.sleep(2)
            self.handle_terms_and_conditions(width, height)

        # Check if on "Sign up for TikTok" screen, and click "Already have an account? Log in"
        ui_text = self.adb.get_ui_text_content().lower()
        if "sign up for tiktok" in ui_text or "already have an account" in ui_text:
            logger.info("Sign up screen detected. Clicking 'Log in' switch...")
            if not (self.adb.click_element(text="Log in") or self.adb.click_element(text="Already have an account")):
                if self.adb._is_tiktok_in_foreground():
                    self.adb.shell(f"input tap {int(width * 0.70)} {int(height * 0.94)}")
            time.sleep(2.5)

        # Click "Use phone / email / username"
        logger.info("Clicking 'Use phone / email / username' option...")
        if not (self.adb.click_element(text="Use phone / email / username") or 
                self.adb.click_element(text="Use phone") or 
                self.adb.click_element(content_desc="Use phone / email / username")):
            if self.adb._is_tiktok_in_foreground():
                self.adb.shell(f"input tap {width // 2} {int(height * 0.36)}")
        time.sleep(3)

        # CRITICAL: When clicking continue with email/phone, TikTok displays Terms & Conditions popup.
        # We MUST explicitly AGREE to it before adding email, never dismiss!
        logger.info("Checking for Terms & Conditions agreement popup before entering email...")
        self.handle_terms_and_conditions(width, height)
        time.sleep(1.0)

        # If clicking "Use phone / email / username" presented "When's your birthdate?", resolve it!
        if self.adb.handle_birthdate_modal():
            time.sleep(2.5)
            self.handle_terms_and_conditions(width, height)

        self._capture_checkpoint("02_login_navigated")

        # Select 'Email / Username' tab
        logger.info("Selecting 'Email / Username' tab...")
        self.handle_terms_and_conditions(width, height)
        if not (self.adb.click_element(text="Email / Username") or 
                self.adb.click_element(text="Email or username") or 
                self.adb.click_element(text="Email")):
            if self.adb._is_tiktok_in_foreground():
                self.adb.shell(f"input tap {int(width * 0.72)} {int(height * 0.12)}")
        time.sleep(2)
        self.handle_terms_and_conditions(width, height)

        # Focus Email input field & type username
        logger.info("Entering username/email into input field...")
        self._ensure_tiktok_foreground()
        found_field = (self.adb.click_element(text="Email or username") or 
                       self.adb.click_element(text="Enter email or username") or 
                       self.adb.click_element(resource_id="email_input") or
                       self.adb.click_element(content_desc="Email or username"))
        if not found_field:
            logger.info("Email field not clicked by text, using field coordinates (width // 2, height * 0.20)...")
            self._ensure_tiktok_foreground()
            self.adb.shell(f"input tap {width // 2} {int(height * 0.20)}")
        time.sleep(1)

        clean_user = username.replace(" ", "").strip()
        self.adb.shell(f"input text {clean_user}")
        time.sleep(1)
        self.adb.hide_keyboard()
        time.sleep(1)
        self._capture_checkpoint("03_email_entered")

        # Click the red "Continue" / "Next" button at the bottom of the Email step
        logger.info("Clicking 'Continue' / 'Next' button...")
        auth_request_time = time.time()
        if not (self.adb.click_element(text="Continue") or 
                self.adb.click_element(text="Next") or 
                self.adb.click_element(text="Log in") or 
                self.adb.click_element(resource_id="login_btn")):
            self.adb.shell(f"input tap {width // 2} {int(height * 0.94)}")
        
        # Wait up to 10 seconds for Password screen or 2FA challenge to appear
        logger.info("Waiting for password or verification screen after email submission...")
        password_entered = False
        wait_start = time.time()
        
        while time.time() - wait_start < 10:
            if self.adb.handle_birthdate_modal():
                time.sleep(2)
            ui_content = self.adb.get_ui_text_content().lower()

            # Immediate detection of rate limit right under email input!
            if any(rate_msg in ui_content for rate_msg in ["maximum number of attempts", "too many attempts", "try again later", "frequent requests"]):
                logger.error(f"[-] [LOGIN_RATE_LIMITED] TikTok rate limit reached for {masked_acc} ('Maximum attempts reached').")
                self.last_failure_reason = "IP_RATE_LIMITED"
                report("LOGIN_RATE_LIMITED", "Maximum number of attempts reached (IP rate-limited by TikTok)")
                self._capture_checkpoint("04_rate_limited")
                return False
            
            # Check if "Log in with password" switch is present
            if "log in with password" in ui_content:
                logger.info("Clicking 'Log in with password' switch...")
                self.adb.click_element(text="Log in with password")
                time.sleep(2)
                ui_content = self.adb.get_ui_text_content().lower()

            # Check if Password field is present
            if "password" in ui_content or "enter password" in ui_content:
                logger.info("Password screen detected. Entering password...")
                report("LOGIN_SUBMITTING", "Submitting account credentials to TikTok")
                
                if not (self.adb.click_element(text="Enter password") or 
                        self.adb.click_element(text="Password") or 
                        self.adb.click_element(resource_id="password_input")):
                    self.adb.shell(f"input tap {width // 2} {int(height * 0.20)}")
                time.sleep(1)

                escaped_pwd = password.replace(" ", "%s").replace("&", "\\&").strip()
                self.adb.shell(f"input text {escaped_pwd}")
                time.sleep(1)
                self.adb.hide_keyboard()
                time.sleep(1)
                self._capture_checkpoint("04_password_entered")

                # Click the red 'Log in' / 'Continue' submit button at bottom of Password step
                logger.info("Clicking 'Log in' submit button...")
                auth_request_time = time.time()
                if not (self.adb.click_element(text="Log in") or 
                        self.adb.click_element(text="Continue") or 
                        self.adb.click_element(resource_id="login_btn") or
                        self.adb.click_element(resource_id="btn_login")):
                    self.adb.shell(f"input tap {width // 2} {int(height * 0.94)}")
                time.sleep(4)
                password_entered = True
                break

            # If 2FA or Authenticated feed appeared directly, break to outcome loop
            if any(k in ui_content for k in ["enter 6-digit code", "verification code", "following", "for you", "home"]):
                break

            time.sleep(2)

        # 7. Post-Submission Outcome Evaluation Loop (Up to 45s)
        logger.info("Evaluating login submission outcome...")
        outcome_start = time.time()
        
        while time.time() - outcome_start < 40:
            if self.adb.handle_birthdate_modal():
                time.sleep(2)
            ui_content = self.adb.get_ui_text_content().lower()
            logger.info(f"[Auth Monitor] Active UI elements summary: {ui_content[:100]}...")

            # Outcome A: Authenticated TikTok feed/profile is visible
            if self.adb.is_authenticated_user_feed():
                logger.info(f"[+] [LOGIN_SUCCESS] Account {masked_acc} authenticated into main feed!")
                self._dismiss_post_login_prompts()
                self._capture_checkpoint("07_auth_success")
                report("AUTHENTICATED", "User authenticated into main feed")
                return True

            # Outcome B: Email 2FA / Verification code screen
            if "enter 6-digit code" in ui_content or "digit code" in ui_content or "verification code" in ui_content or "verify" in ui_content:
                logger.info("[2FA_REQUIRED] TikTok requested email verification code.")
                report("2FA_REQUIRED", "Email verification code requested by TikTok")
                self._capture_checkpoint("05_2fa_prompted")
                
                if gmail_addr and gmail_pwd:
                    logger.info(f"Querying Gmail IMAP SSL for {gmail_addr} (min_timestamp: {auth_request_time:.0f})...")
                    email_srv = GmailVerificationService(gmail_addr, gmail_pwd)
                    code = email_srv.fetch_tiktok_verification_code(timeout_seconds=60, check_interval=3, min_timestamp=auth_request_time)
                    if code:
                        logger.info(f"Typing retrieved verification code '{code[:2]}****' into TikTok...")
                        self.adb._2fa_in_flight = True
                        self.adb._2fa_in_flight_time = time.time()
                        try:
                            self.adb.shell(f"input text {code[0]}")
                            time.sleep(0.3)
                            self.adb.shell(f"input text {code[1:]}")
                            if state_callback:
                                state_callback("LOGIN_SUBMITTING", f"Submitted 2FA code {code[:2]}****")
                            self._capture_checkpoint("06_2fa_code_submitted")
                            if self.validate_post_2fa_transition(masked_acc, width, height, state_callback):
                                return True
                        finally:
                            self.adb._2fa_in_flight = False
                        
                        # If resend is needed, check UI for resend
                        ui_post = self.adb.get_ui_text_content().lower()
                        if "resend code" in ui_post or "resend" in ui_post:
                            logger.info("Attempting to click 'Resend code' to request a brand-new code...")
                            if self.adb.click_element(text="Resend code") or self.adb.click_element(text="Resend"):
                                time.sleep(3)
                                resend_time = time.time()
                                report("LOGIN_SUBMITTING", "Requested fresh 2FA code via Resend")
                                new_code = email_srv.fetch_tiktok_verification_code(timeout_seconds=50, check_interval=3, min_timestamp=resend_time)
                                if new_code and new_code != code:
                                    logger.info(f"Submitting newly resent 2FA code '{new_code[:2]}****'...")
                                    for _ in range(6):
                                        self.adb.shell("input keyevent 67")  # Backspace
                                    self.adb._2fa_in_flight = True
                                    self.adb.shell(f"input text {new_code}")
                                    self._capture_checkpoint("06_2fa_resent_submitted")
                                    report("LOGIN_SUBMITTING", f"Submitted resent 2FA code {new_code[:2]}****")
                                    time.sleep(2)
                                    try:
                                        if self.validate_post_2fa_transition(masked_acc, width, height, state_callback):
                                            return True
                                    finally:
                                        self.adb._2fa_in_flight = False
                    else:
                        logger.warning("[-] Gmail 2FA code retrieval timed out.")
                        report("LOGIN_FAILED", "2FA code timeout from Gmail IMAP")
                        self._capture_checkpoint("06_2fa_timeout")
                        return False
                else:
                    logger.warning("[-] 2FA required but no Gmail App Password configured.")
                    report("LOGIN_BLOCKED", "2FA required but Gmail credentials missing")
                    self._capture_checkpoint("06_2fa_no_creds")
                    return False

            # Outcome C1: Rate limited / Maximum attempts reached (IP level block)
            if any(rate_msg in ui_content for rate_msg in ["maximum number of attempts", "too many attempts", "try again later", "frequent requests"]):
                logger.error(f"[-] [LOGIN_RATE_LIMITED] TikTok rate limit reached for {masked_acc} (IP flagged/blocked).")
                self.last_failure_reason = "IP_RATE_LIMITED"
                report("LOGIN_RATE_LIMITED", "Maximum number of attempts reached (IP rate-limited by TikTok)")
                self._capture_checkpoint("06_rate_limited")
                return False

            # Outcome C2: Incorrect credentials error
            if any(err_msg in ui_content for err_msg in ["incorrect password", "account doesn't exist", "wrong password"]):
                logger.error(f"[-] [LOGIN_FAILED] Invalid credentials reported by TikTok for {masked_acc}.")
                self.last_failure_reason = "INVALID_CREDENTIALS"
                report("LOGIN_FAILED", "Invalid credentials reported by TikTok")
                self._capture_checkpoint("06_invalid_creds")
                return False

            # Outcome D: CAPTCHA / Puzzle challenge
            if any(c in ui_content for c in ["slide to complete", "select 2 objects", "security check", "puzzle", "captcha"]):
                logger.warning(f"[-] [LOGIN_BLOCKED] Security challenge presented by TikTok.")
                report("LOGIN_BLOCKED", "Interactive CAPTCHA/Puzzle challenge detected")
                self._capture_checkpoint("06_captcha_blocked")
                return False

            time.sleep(3)

        # Outcome E: Timeout with login screen still visible
        if self.adb.is_login_or_signup_screen():
            logger.error("[-] [LOGIN_FAILED] Login screen remains visible after timeout (LOGIN_SCREEN_STILL_VISIBLE).")
            report("LOGIN_FAILED", "LOGIN_SCREEN_STILL_VISIBLE")
            self._capture_checkpoint("07_login_screen_stuck")
            return False

        # Final verification check: dismiss any blocking popups first!
        self._dismiss_post_login_prompts()
        if self.adb.is_authenticated_user_feed() or self.adb.is_live_stream_active():
            report("AUTHENTICATED", "User authenticated into main feed")
            self._capture_checkpoint("07_auth_success")
            return True

        # Fallback: If trapped in a stuck WebView / blank overlay before declaring failure
        if self.adb.is_webview_or_blank_overlay():
            logger.info("Trapped in WebView or blank overlay at final check. Performing rescue via Back + MainActivity launch...")
            self.adb.shell("input keyevent 4")
            time.sleep(1.5)
            self.adb.shell("am start -n com.zhiliaoapp.musically/com.ss.android.ugc.aweme.main.MainActivity")
            time.sleep(2.5)
            self._dismiss_post_login_prompts()
            if self.adb.is_authenticated_user_feed() or self.adb.verify_account_profile_authenticated(width, height):
                report("AUTHENTICATED", "User authenticated into main feed after overlay rescue")
                self._capture_checkpoint("07_auth_success")
                return True

        logger.error("[-] [LOGIN_FAILED] Application did not reach authenticated state.")
        report("LOGIN_FAILED", "App not in authenticated feed")
        self._capture_checkpoint("07_auth_failed")
        return False

    def _forensic_investigate_2fa(
        self,
        code: str,
        masked_acc: str,
        width: int,
        height: int,
        state_callback: Optional[Callable[[str, str], None]] = None
    ) -> bool:
        """
        Comprehensive Forensic Investigation of the 2FA SparkActivity Hang.
        Captures exact timestamps, UI state, IME focus, submit button status,
        emulator network connectivity, logcat, and process lifecycle across States A, B, C, D, E.
        CRITICAL: Never presses Enter/Back, never bypasses auth, never declares AUTHENTICATED
        without positive profile verification.
        """
        import datetime
        import xml.etree.ElementTree as ET
        t_start = time.time()

        def now_iso():
            return datetime.datetime.utcnow().isoformat() + "Z"

        def log_info(msg):
            logger.info(f"[FORENSIC] {msg}")

        def dump_ime_and_focus():
            focus = self.adb.shell("dumpsys window | grep -E 'mCurrentFocus|mFocusedApp'")
            ime = self.adb.shell("dumpsys input_method | grep -E 'mServedView|mInputShown|mCurFocusedWindow|mImeWindowVis'")
            return focus.strip(), ime.strip()

        def check_submit_button_info(xml_str):
            if not xml_str:
                return False, False, "No XML"
            try:
                root = ET.fromstring(xml_str)
                submit_keywords = ["submit", "continue", "verify", "done", "next", "confirm", "log in", "login"]
                for node in root.iter("node"):
                    txt = node.attrib.get("text", "").lower()
                    desc = node.attrib.get("content-desc", "").lower()
                    cls = node.attrib.get("class", "")
                    if any(k in txt or k in desc for k in submit_keywords):
                        enabled = node.attrib.get("enabled", "false").lower() == "true"
                        return True, enabled, f"class={cls}, text='{txt}', desc='{desc}', enabled={enabled}"
            except Exception as e:
                return False, False, f"Parse error: {e}"
            return False, False, "Not found"

        def test_emulator_network():
            dns_ip = self.adb.shell("getprop net.dns1")
            ping_ip = self.adb.shell("ping -c 1 -W 2 8.8.8.8 2>&1 | grep -E 'transmitted|received|loss'")
            ping_dns = self.adb.shell("ping -c 1 -W 2 google.com 2>&1 | grep -E 'transmitted|received|loss'")
            curl_tt = self.adb.shell("curl -I -s --connect-timeout 4 https://api.tiktokv.com/ 2>&1 | head -n 3 || echo 'CURL_FAIL'")
            return {
                "dns_server": dns_ip.strip(),
                "ping_8.8.8.8": ping_ip.strip(),
                "ping_google_com": ping_dns.strip(),
                "curl_api_tiktokv": curl_tt.strip()
            }

        def record_state(state_name: str, screenshot_filename: str):
            ts = now_iso()
            elapsed = time.time() - t_start
            fg = self.adb.get_foreground_activity()
            xml = self.adb.dump_ui_hierarchy()
            ui_text = self.adb.get_ui_text_content().lower()[:120].replace("\\n", " ").strip()
            focus, ime = dump_ime_and_focus()
            ps = self.adb.shell("ps -A | grep -E 'musically' | awk '{print $2, $8, $9}'")
            net = test_emulator_network()
            btn_exists, btn_enabled, btn_desc = check_submit_button_info(xml)

            # Save state screenshot
            self.adb.take_screenshot(f"auth_recordings/{screenshot_filename}")

            # Logcat slice
            logcat_slice = self.adb.shell("logcat -d -t 60 -v time | grep -E -i 'musically|Spark|Lynx|WebView|chromium|InputMethod|Cronet|ttnet|passport|auth'")

            log_info(f"=== {state_name} (elapsed={elapsed:.2f}s, timestamp={ts}) ===")
            log_info(f"  Activity: {fg}")
            log_info(f"  UI text summary: '{ui_text}' (XML len={len(xml)})")
            log_info(f"  Focused Element: {focus}")
            log_info(f"  IME / Keyboard State: {ime}")
            log_info(f"  Submit Button: exists={btn_exists}, enabled={btn_enabled} ({btn_desc})")
            log_info(f"  Process State: {ps}")
            log_info(f"  Emulator Network: {net}")
            if logcat_slice:
                lines = [l.strip() for l in logcat_slice.splitlines() if l.strip()][-8:]
                log_info(f"  Recent Logcat ({len(lines)} lines):")
                for l in lines:
                    log_info(f"    {l[:140]}")
            return {
                "state": state_name,
                "timestamp": ts,
                "elapsed": elapsed,
                "activity": fg,
                "ui_text": ui_text,
                "focus": focus,
                "ime": ime,
                "submit_button": (btn_exists, btn_enabled, btn_desc),
                "network": net
            }

        log_info("=" * 60)
        log_info("STARTING FORENSIC INVESTIGATION OF SPARKACTIVITY 2FA HANG")
        log_info("=" * 60)

        # Baseline: 2FA screen detected
        t0_appear = now_iso()
        log_info(f"1. 2FA Screen Active at {t0_appear}")
        fg_init = self.adb.get_foreground_activity()
        focus_init, ime_init = dump_ime_and_focus()
        net_init = test_emulator_network()
        log_info(f"  Initial Activity: {fg_init}")
        log_info(f"  Initial Focus: {focus_init}")
        log_info(f"  Initial IME: {ime_init}")
        log_info(f"  Initial Network: {net_init}")

        # Clear logcat for clean forensic trace
        self.adb.shell("logcat -c")

        # Input Commit Investigation:
        # Step 1: Type 1st digit
        t1_digit = now_iso()
        log_info(f"2. Entering first digit '{code[0]}' at {t1_digit}")
        self.adb.shell(f"input text {code[0]}")
        time.sleep(0.3)
        focus_after_d1, ime_after_d1 = dump_ime_and_focus()
        log_info(f"  Focus after digit 1: {focus_after_d1}")

        # Step 2: Type digits 2 through 5 (creating State A: 5 digits typed, before 6th digit)
        log_info(f"3. Entering digits 2 through 5: '{code[1:5]}'")
        self.adb.shell(f"input text {code[1:5]}")
        time.sleep(0.5)

        # STATE A: BEFORE typing 6th digit
        record_state("STATE_A_BEFORE_6TH_DIGIT", "state_A_before_6th_digit.png")

        # Step 3: Type 6th digit (creating State B: immediately after 6th digit)
        t6_digit = now_iso()
        log_info(f"4. Entering sixth digit '{code[5]}' at {t6_digit}")
        self.adb.shell(f"input text {code[5]}")
        if state_callback:
            state_callback("LOGIN_SUBMITTING", f"Submitted 2FA code {code[:2]}****")
        self._capture_checkpoint("06_2fa_code_submitted")

        # STATE B: Immediately AFTER typing 6th digit
        record_state("STATE_B_AFTER_6TH_DIGIT", "state_B_immediately_after_6th_digit.png")

        # STATE C: 1.5 seconds later (initial transition)
        time.sleep(1.5)
        record_state("STATE_C_AFTER_6TH_DIGIT", "state_C_2s_after_6th_digit.png")

        # Full Logcat Dump & Extraction
        log_info("5. Dumping and filtering comprehensive 2FA logcat...")
        full_logcat = self.adb.shell("logcat -d -v time | grep -E -i 'Spark|Lynx|WebView|chromium|Aweme|bytedance|ttnet|Cronet|passport|account|login|auth|ticket|token|net|ssl|http|timeout|connect'")

        # Sanitize logcat (mask any credentials)
        sanitized_logcat = full_logcat.replace(code, "******")
        if masked_acc and "@" in masked_acc:
            raw_acc = masked_acc.split("***@")[0]
            if raw_acc:
                sanitized_logcat = sanitized_logcat.replace(raw_acc, "***")

        try:
            os.makedirs("auth_recordings", exist_ok=True)
            with open("auth_recordings/forensic_logcat_2fa.txt", "w", encoding="utf-8") as f:
                f.write(sanitized_logcat)
            log_info(f"Saved filtered logcat to auth_recordings/forensic_logcat_2fa.txt ({len(sanitized_logcat)} chars)")
        except Exception as e:
            logger.debug(f"Logcat save note: {e}")

        log_info("=== KEY LOGCAT EVIDENCE (Last 25 lines) ===")
        for line in [l.strip() for l in sanitized_logcat.splitlines() if l.strip()][-25:]:
            log_info(f"  {line[:150]}")

        log_info("=" * 60)
        log_info("FORENSIC OBSERVATION PHASE COMPLETE. ENTERING BOUNDED TRANSITION VALIDATION.")
        log_info("=" * 60)

        # Enter bounded transition validation (max_checks=25, ~60s bounded wait)
        # Guarantees: ZERO Back keys during active 2FA, ZERO fake auth, controlled WebView wake
        return self.validate_post_2fa_transition(masked_acc, width, height, state_callback, max_checks=25)

    def validate_post_2fa_transition(
        self, 
        masked_acc: str = "***", 
        width: int = 720, 
        height: int = 1280, 
        state_callback: Optional[Callable[[str, str], None]] = None,
        max_checks: int = 35
    ) -> bool:
        """
        Bounded Post-2FA Authentication Completion & Recovery State Machine.
        1. Waits for 2FA verification to complete; NEVER sends Back keyevents, swipes,
           or launches MainActivity while TikTok is actively verifying.
        2. Detects and explicitly Agrees to Terms & Conditions modals.
        3. Verifies genuine authenticated state (never accepts unauthenticated guest
           MainActivity as proof of success).
        4. Captures diagnostic snapshot before executing bounded recovery for genuinely
           hung overlays.
        """
        def report(st, msg):
            if state_callback:
                state_callback(st, msg)

        recovery_attempts = 0
        for check_idx in range(max_checks):
            time.sleep(2.5)

            fg = self.adb.get_foreground_activity()
            fg_lower = fg.lower()
            ui_post = self.adb.get_ui_text_content().lower()
            ui_summary = ui_post[:80].replace('\n', ' ').strip()
            is_2fa_active = self.adb.is_2fa_actively_processing()
            is_overlay = self.adb.is_webview_or_blank_overlay() if not is_2fa_active else False
            is_authenticated = self.adb.is_authenticated_user_feed()

            # Fix #5: White Screen Diagnostics
            # Distinguishes: A. Active 2FA, B. Genuine stuck overlay, C. Authentication failure, D. Completed auth
            logger.info(
                f"[2FA_WAIT] check=#{check_idx + 1}/{max_checks} activity={fg} "
                f"active_2fa={is_2fa_active} overlay={is_overlay} "
                f"auth_detected={is_authenticated} ui='{ui_summary}'"
            )

            # Fix #1 & Fix #4: Never interrupt active 2FA
            # Invariant: If 2FA is actively processing, NEVER trigger recovery, Back key, or warm launch
            if is_2fa_active:
                logger.info(f"[2FA_WAIT] active_2fa=true overlay=false action=WAIT (TikTok is actively processing 2FA for {masked_acc})")
                report("LOGIN_SUBMITTING", f"Verifying 2FA ({check_idx + 1}/{max_checks})...")
                continue

            # Fix #6: Check for genuine terminal TikTok rejection ("Incorrect code", "Code expired")
            terminal_rejections = ["incorrect code", "code expired", "wrong code", "enter correct code", "verification rejected"]
            if any(err_kw in ui_post for err_kw in terminal_rejections):
                logger.warning(f"[-] TikTok rejected 2FA code for {masked_acc}. UI message: {ui_post[:100]}")
                self._capture_checkpoint("06_2fa_code_rejected")
                report("LOGIN_FAILED", "2FA code rejected by TikTok")
                return False

            # Fix #6: Terminal rate limit on 2FA
            terminal_rate_limits = ["maximum number of attempts", "too many attempts", "try again later", "frequent requests"]
            if any(rate_msg in ui_post for rate_msg in terminal_rate_limits):
                logger.error(f"[-] [LOGIN_RATE_LIMITED] 2FA attempt limit reached for {masked_acc}.")
                self.last_failure_reason = "IP_RATE_LIMITED"
                self._capture_checkpoint("06_2fa_rate_limited")
                report("LOGIN_RATE_LIMITED", "Maximum attempts reached on 2FA")
                return False

            # Controlled Vision Fix: If SparkActivity remains blank white after initial transit, tap center to wake compositor
            if "sparkactivity" in fg_lower and len(ui_post) == 0 and check_idx == 3:
                logger.info("[2FA_CONTROL] SparkActivity is blank. Tapping center to wake WebView compositor...")
                self.adb.shell(f"input tap {width // 2} {height // 2}")
                time.sleep(1.2)
                ui_post = self.adb.get_ui_text_content().lower()

            # Controlled Vision Fix: Identity Verification (IDV) method selection ("Email", "Verify with email")
            if any(k in ui_post for k in ["verify your identity", "choose a method", "try another method", "suspicious_login"]):
                logger.info(f"[2FA_CONTROL] IDV verification screen detected for {masked_acc}.")
                if any(em in ui_post for em in ["email", "verify by email", "send code"]):
                    logger.info("[2FA_CONTROL] Selecting 'Email' verification method on IDV...")
                    self.adb.click_element(text="Email") or self.adb.click_element(text="Verify by email") or self.adb.click_element(text="Send code")
                    time.sleep(2.0)
                    ui_post = self.adb.get_ui_text_content().lower()

            # Controlled Vision Fix: Secondary challenge requiring manual/external intervention ("another device", "security questions")
            if any(c in ui_post for c in ["another device", "security questions", "scan qr code", "identity verification failed"]):
                logger.warning(f"[-] [LOGIN_CHALLENGE] TikTok secondary challenge requires manual intervention for {masked_acc}: {ui_post[:100]}")
                self._capture_checkpoint("06_idv_challenge_blocked")
                report("LOGIN_CHALLENGE", "Secondary verification requires manual intervention")
                return False

            # Check for and AGREE to Terms & Conditions modal if loaded post-2FA
            if "universalpopupactivity" in fg_lower or any(k in ui_post for k in ["terms of service", "privacy policy", "terms and conditions", "terms of use", "agree and continue"]):
                logger.info("[2FA_POST_LOGIN] Terms & Conditions modal presented. Explicitly AGREEING...")
                self.handle_terms_and_conditions(width, height)
                time.sleep(2.0)
                ui_post = self.adb.get_ui_text_content().lower()

            # Post-login onboarding / consent cues ("save login info", "sync contacts")
            if any(cue in ui_post for cue in ["save login info", "save your login info", "sync contacts", "allow notifications"]):
                logger.info(f"[+] [LOGIN_SUCCESS] Post-login onboarding prompt detected for {masked_acc}!")
                self._dismiss_post_login_prompts()
                self._capture_checkpoint("07_auth_success")
                report("AUTHENTICATED", "2FA verified into main feed")
                return True

            # Fix #2 & Fix #3: Genuine authentication verification into feed
            # NOTE: NEVER accept is_live_stream_active() as proof of account authentication!
            if self.adb.is_authenticated_user_feed():
                logger.info(f"[+] [LOGIN_SUCCESS] Positive authenticated feed confirmed for {masked_acc}!")
                self._dismiss_post_login_prompts()
                self._capture_checkpoint("07_auth_success")
                report("AUTHENTICATED", "2FA verified into main feed")
                return True

            # Fix #3: If on MainActivity, verify Profile tab for positive authenticated evidence
            if any(act in fg_lower for act in ["mainactivity", ".main."]):
                logger.info(f"[2FA_POST_LOGIN] MainActivity detected. Verifying Profile tab for positive authentication ({masked_acc})...")
                if self.adb.verify_account_profile_authenticated(width, height):
                    logger.info(f"[+] [LOGIN_SUCCESS] Profile tab confirmed positive authenticated session for {masked_acc}!")
                    self._capture_checkpoint("07_auth_success")
                    report("AUTHENTICATED", "2FA verified into main feed")
                    return True
                else:
                    logger.warning("[-] Profile check indicates unauthenticated guest mode. Continuing wait...")

            # Fix #1 & Fix #4: Bounded Post-2FA White Screen / Overlay Recovery State Machine
            # Trigger early recovery at check >= 3 (~8-10s post-submission) if overlay is stuck
            if check_idx >= 3 and recovery_attempts < 2 and not is_2fa_active:
                if self.adb.is_webview_or_blank_overlay():
                    recovery_attempts += 1
                    diag = self.adb.get_recovery_diagnostics() if hasattr(self.adb, 'get_recovery_diagnostics') else {}
                    logger.info(f"[2FA_POST_LOGIN] Overlay recovery #{recovery_attempts}/2 triggered: {diag}")
                    report("LOGIN_SUBMITTING", f"Overlay recovery #{recovery_attempts} (waking feed)...")
                    self.adb.take_screenshot(f"auth_recordings/recovery_diag_{recovery_attempts}.png")

                    # Action A: Tap bottom consent area in case an HTML webview button is present
                    w = self.adb.screen_width or width or 720
                    h = self.adb.screen_height or height or 1280
                    self.adb.shell(f"input tap {w // 2} {int(h * 0.90)}")
                    time.sleep(1.5)
                    if self.adb.is_authenticated_user_feed():
                        logger.info("[+] [LOGIN_SUCCESS] Authenticated feed verified after consent tap!")
                        self._dismiss_post_login_prompts()
                        self._capture_checkpoint("07_auth_success")
                        report("AUTHENTICATED", "2FA verified into main feed")
                        return True

                    # Action B: Single controlled Back keyevent to dismiss overlay
                    logger.info("[2FA_POST_LOGIN] Sending single Back keyevent to dismiss overlay...")
                    self.adb.shell("input keyevent 4")
                    time.sleep(2.0)
                    if self.adb.is_authenticated_user_feed():
                        logger.info("[+] [LOGIN_SUCCESS] Authenticated feed verified after dismissing overlay!")
                        self._dismiss_post_login_prompts()
                        self._capture_checkpoint("07_auth_success")
                        report("AUTHENTICATED", "2FA verified into main feed")
                        return True

                    # Action C: Warm-launch MainActivity to bring root to foreground
                    logger.info("[2FA_POST_LOGIN] Warm-launching MainActivity to restore feed...")
                    self.adb.shell("am start -n com.zhiliaoapp.musically/com.ss.android.ugc.aweme.main.MainActivity")
                    time.sleep(2.5)
                    if self.adb.verify_account_profile_authenticated(width, height):
                        logger.info("[+] [LOGIN_SUCCESS] Profile tab confirmed authenticated session after warm MainActivity launch!")
                        self._dismiss_post_login_prompts()
                        self._capture_checkpoint("07_auth_success")
                        report("AUTHENTICATED", "2FA verified into main feed")
                        return True

        # Final check after bounded timeout expires
        self.adb.take_screenshot("auth_recordings/07_2fa_timeout_diag.png")
        if self.adb.is_authenticated_user_feed():
            self._dismiss_post_login_prompts()
            self._capture_checkpoint("07_auth_success")
            report("AUTHENTICATED", "2FA verified into main feed")
            return True
        fg_final = self.adb.get_foreground_activity().lower()
        if any(act in fg_final for act in ["mainactivity", ".main."]) and self.adb.verify_account_profile_authenticated(width, height):
            self._dismiss_post_login_prompts()
            self._capture_checkpoint("07_auth_success")
            report("AUTHENTICATED", "2FA verified into main feed")
            return True

        logger.error(f"[-] [LOGIN_FAILED] 2FA verification timed out after {max_checks} checks.")
        report("LOGIN_FAILED", "2FA verification timed out")
        return False

    def handle_terms_and_conditions(self, width: int = 720, height: int = 1280) -> bool:
        """
        Detects, SCROLLS, and explicitly ACCEPTS TikTok Terms of Service / Privacy Policy.
        Crucial: As identified, TikTok requires scrolling down the terms to accept them.
        Dismissing or closing without accepting causes TikTok to block the session post-2FA
        with a white loading screen.
        """
        ui_text = self.adb.get_ui_text_content().lower()
        fg_act = self.adb.get_foreground_activity().lower()
        terms_detected = "universalpopupactivity" in fg_act or any(k in ui_text for k in [
            "terms of service", "privacy policy", "terms and conditions", 
            "terms of use", "by continuing, you agree", "agree to tiktok", 
            "agree and continue"
        ])
        if not terms_detected:
            return False

        # Guard: If on a normal login/signup screen without a legal document overlay, do not process
        if self.adb.is_login_or_signup_screen() and not self.adb.is_terms_or_policy_screen():
            return False

        logger.info("[TERMS_AGREEMENT] Detected Terms of Service / Legal prompt. Handling...")
        w = self.adb.screen_width or width or 720
        h = self.adb.screen_height or height or 1280

        agreement_buttons = [
            "Agree and continue", "Agree & continue", "Accept all", "Accept",
            "I agree", "Continue", "Confirm", "Got it", "OK"
        ]

        agreed = False

        # Step 1: ALWAYS try agree buttons FIRST (consent dialog has buttons; WebView does not)
        for scroll_idx in range(3):
            # Check for any unselected checkbox / radio button
            if self.adb.click_first_unchecked_checkbox():
                time.sleep(0.5)

            # Check for any visible agreement button
            for btn in agreement_buttons:
                if self.adb.click_element(text=btn) or self.adb.click_element(content_desc=btn):
                    logger.info(f"[TERMS_AGREEMENT] [+] Tapped agreement button '{btn}'.")
                    agreed = True
                    time.sleep(1.5)
                    break

            if not agreed:
                for res_id in ["agree_btn", "btn_agree", "confirm_btn", "tv_agree"]:
                    if self.adb.click_element(resource_id=res_id):
                        logger.info(f"[TERMS_AGREEMENT] [+] Tapped agreement button by ID '{res_id}'.")
                        agreed = True
                        time.sleep(1.5)
                        break

            if agreed:
                break

            # If an agree button might be on a consent bottom sheet, swipe up once
            ui_check = self.adb.get_ui_text_content().lower()
            if any(k in ui_check for k in ["agree and continue", "terms of use", "by continuing"]):
                self.adb.shell(f"input swipe {w // 2} {int(h * 0.75)} {w // 2} {int(h * 0.25)} 250")
                time.sleep(0.8)
            else:
                break

        # Step 2: If agreed, check if a second confirmation dialog appeared
        if agreed:
            ui_after = self.adb.get_ui_text_content().lower()
            for btn in ["Agree and continue", "Accept all", "Accept", "I agree", "Continue", "Confirm"]:
                if btn.lower() in ui_after:
                    self.adb.click_element(text=btn)
                    time.sleep(1.0)
            # Verify the terms screen is actually gone
            if self.adb.is_terms_or_policy_screen():
                logger.warning("[TERMS_AGREEMENT] Terms screen still visible after clicking agree. Retrying...")
                self.adb.click_first_unchecked_checkbox()
                time.sleep(0.3)
                for btn in agreement_buttons:
                    if self.adb.click_element(text=btn):
                        time.sleep(1.5)
                        break
            return not self.adb.is_terms_or_policy_screen()

        # Step 3: No agree button found. Check if this is a genuine FULL_LEGAL_DOCUMENT
        if self.adb.is_terms_or_policy_screen():
            logger.info("[TERMS_AGREEMENT] Verified full legal text WebView. Dismissing via back arrow exactly once...")
            self.adb.close_legal_webview()
            time.sleep(1.2)
            return not self.adb.is_terms_or_policy_screen()

        # Step 4: Not an agreement dialog and not a verified legal document (e.g. login screen with footer) -> Do nothing!
        return False

    def _dismiss_initial_onboarding(self, width: int = 720, height: int = 1280) -> None:
        """Dismisses splash, terms, interest selection, tutorial swipe overlays, and birthdate modal."""
        w = self.adb.screen_width or width or 720
        h = self.adb.screen_height or height or 1280
        # Auto-agree to Terms of Service overlay if presented
        self.handle_terms_and_conditions(w, h)

        if self.adb.click_element(text="Agree and continue") or self.adb.click_element(text="Accept all") or self.adb.click_element(text="I agree"):
            time.sleep(1.5)
        if self.adb.click_element(text="Skip") or self.adb.click_element(text="Choose your interests"):
            time.sleep(1.5)
        if self.adb.click_element(text="Start watching"):
            time.sleep(1.5)
        # Handle birthdate onboarding modal if prompted
        self.adb.handle_birthdate_modal()
        # Swipe up to clear initial tutorial overlay within actual screen dimensions
        self.adb.shell(f"input swipe {w // 2} {int(h * 0.75)} {w // 2} {int(h * 0.25)} 250")
        time.sleep(1)
        self.adb.dismiss_popups()

    def _dismiss_post_login_prompts(self) -> None:
        """Dismisses post-login prompts: Save info, Notifications, Sync contacts, Birthdate modal, Tutorial."""
        time.sleep(1.0)
        ui = self.adb.get_ui_text_content().lower()
        # Safety guard: NEVER click generic dismiss buttons if on error or 2FA challenge screen!
        if any(bad in ui for bad in ["incorrect", "code", "resend", "verify", "enter password", "too many attempts", "maximum number"]):
            return

        # Auto-agree to Terms of Service / Privacy Policy if presented
        self.handle_terms_and_conditions()

        self.adb.handle_birthdate_modal()
        for prompt_btn in [
            "Save", "Not now", "Don't allow", "Deny", "Skip", "Start watching",
            "Got it", "Cancel", "Never", "While using the app", "Only this time",
            "Dismiss", "Later", "Close", "Agree and continue", "Accept all", "I agree", "Accept"
        ]:
            if self.adb.click_element(text=prompt_btn):
                time.sleep(0.6)
                self.adb.handle_birthdate_modal()

        # Dismiss swipe up tutorial if shown
        ui = self.adb.get_ui_text_content().lower()
        if "swipe up" in ui:
            w = self.adb.screen_width or 720
            h = self.adb.screen_height or 1280
            self.adb.shell(f"input swipe {w // 2} {int(h * 0.75)} {w // 2} {int(h * 0.25)} 250")
            time.sleep(0.8)

    def _ensure_tiktok_foreground(self) -> bool:
        """Guarantees native TikTok is actively running in the foreground before performing UI actions."""
        if self.adb._is_tiktok_in_foreground():
            return True

        logger.info("TikTok not in foreground. Dismissing any background overlays (Google search, etc.)...")
        # Press Back key then Home key to close any open search widget or launcher overlay
        self.adb.shell("input keyevent 4")  # Back
        time.sleep(0.5)
        self.adb.shell("input keyevent 3")  # Home
        time.sleep(0.5)

        for attempt in range(4):
            logger.info(f"Bringing TikTok to foreground (Attempt {attempt+1}/4)...")
            self.adb.shell(f"monkey -p {self.adb.package_name} -c android.intent.category.LAUNCHER 1")
            self.adb.shell(f"am start -a android.intent.action.MAIN -c android.intent.category.LAUNCHER -p {self.adb.package_name}")
            time.sleep(4)
            if self.adb._is_tiktok_in_foreground():
                logger.info(f"[+] TikTok confirmed in foreground on attempt {attempt+1}.")
                return True

        logger.error("[-] Failed to bring TikTok to foreground after 4 attempts.")
        return False
