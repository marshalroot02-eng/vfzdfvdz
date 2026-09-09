"""
TikTok Booster - Automated In-App Authentication & Onboarding Engine
Deterministic, instrumented login state machine supporting:
- Clean session reset (pm clear)
- Native UI Hierarchy element locating & clicking
- In-App Email & Password Entry with soft keyboard auto-dismissal
- Automated Gmail IMAP 2FA Code Extraction
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

    def authenticate_account(self, account: dict, state_callback: Optional[Callable[[str, str], None]] = None) -> bool:
        """
        Executes the complete automated login state machine:
        1. Clean State (pm clear)
        2. Detect Login/Sign-up screen
        3. Click 'Use phone / email / username' -> 'Email / Username' tab
        4. Enter username + password with keyboard auto-dismissal -> Submit
        5. Explicitly verify one of:
           A. Authenticated user feed -> AUTHENTICATED / LOGIN_SUCCESS
           B. Email 2FA -> 2FA_REQUIRED -> Gmail IMAP -> Submit -> Verify
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

        # 4. Launch clean TikTok Application
        report("STARTING", "Starting clean TikTok native Android activity")
        self.adb.shell(f"am start -a android.intent.action.MAIN -c android.intent.category.LAUNCHER -p {self.adb.package_name}")
        time.sleep(6)

        width = self.adb.screen_width or 720
        height = self.adb.screen_height or 1280

        # 5. Dismiss initial onboarding prompts (Terms, Interests, Swipe Up)
        self._dismiss_initial_onboarding(width, height)

        if not password:
            logger.info(f"No password provided for {masked_acc}. Proceeding in Guest mode.")
            report("LIVE_BROWSING_READY", "Running as Guest Viewer")
            return True

        # 6. Navigate into Login Screen
        report("LOGIN_REQUIRED", "Detecting login screen and navigating to Email login tab")
        
        # Check if birthdate modal is already on screen
        if self.adb.handle_birthdate_modal():
            time.sleep(2)

        # Check if already on login screen, else tap Profile in bottom right
        if not self.adb.is_login_or_signup_screen():
            logger.info("Opening Profile tab to trigger login prompt...")
            if not self.adb.click_element(text="Profile"):
                self.adb.shell(f"input tap {int(width * 0.90)} {int(height * 0.96)}")
            time.sleep(3)

        if self.adb.handle_birthdate_modal():
            time.sleep(2)

        # Check if on "Sign up for TikTok" screen, and click "Already have an account? Log in"
        ui_text = self.adb.get_ui_text_content().lower()
        if "sign up for tiktok" in ui_text or "already have an account" in ui_text:
            logger.info("Sign up screen detected. Clicking 'Log in' switch...")
            if not (self.adb.click_element(text="Log in") or self.adb.click_element(text="Already have an account")):
                self.adb.shell(f"input tap {int(width * 0.70)} {int(height * 0.94)}")
            time.sleep(2.5)

        # Click "Use phone / email / username"
        logger.info("Clicking 'Use phone / email / username' option...")
        if not (self.adb.click_element(text="Use phone / email / username") or 
                self.adb.click_element(text="Use phone") or 
                self.adb.click_element(content_desc="Use phone / email / username")):
            self.adb.shell(f"input tap {width // 2} {int(height * 0.36)}")
        time.sleep(3)

        # If clicking "Use phone / email / username" presented "When's your birthdate?", resolve it!
        if self.adb.handle_birthdate_modal():
            time.sleep(2.5)

        # Select 'Email / Username' tab
        logger.info("Selecting 'Email / Username' tab...")
        if not (self.adb.click_element(text="Email / Username") or 
                self.adb.click_element(text="Email or username") or 
                self.adb.click_element(text="Email")):
            self.adb.shell(f"input tap {int(width * 0.72)} {int(height * 0.12)}")
        time.sleep(2)

        # Focus Email input field & type username
        logger.info("Entering username/email into input field...")
        if not (self.adb.click_element(text="Email or username") or 
                self.adb.click_element(text="Enter email or username") or 
                self.adb.click_element(resource_id="email_input")):
            self.adb.shell(f"input tap {width // 2} {int(height * 0.20)}")
        time.sleep(1)

        clean_user = username.replace(" ", "").strip()
        self.adb.shell(f"input text {clean_user}")
        time.sleep(1)
        self.adb.hide_keyboard()
        time.sleep(1)

        # Click the red "Continue" / "Next" button at the bottom of the Email step
        logger.info("Clicking 'Continue' / 'Next' button...")
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

                # Click the red 'Log in' / 'Continue' submit button at bottom of Password step
                logger.info("Clicking 'Log in' submit button...")
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

        # 7. Post-Submission Outcome Evaluation Loop (Up to 40s)
        logger.info("Evaluating login submission outcome...")
        outcome_start = time.time()
        
        while time.time() - outcome_start < 35:
            if self.adb.handle_birthdate_modal():
                time.sleep(2)
            ui_content = self.adb.get_ui_text_content().lower()
            logger.info(f"[Auth Monitor] Active UI elements summary: {ui_content[:100]}...")
            report("LOGIN_SUBMITTING", "Evaluating authentication response...")

            # Outcome A: Authenticated TikTok feed/profile is visible
            if self.adb.is_authenticated_user_feed():
                logger.info(f"[+] [LOGIN_SUCCESS] Account {masked_acc} authenticated into main feed!")
                self._dismiss_post_login_prompts()
                report("AUTHENTICATED", "User authenticated into main feed")
                return True

            # Outcome B: Email 2FA / Verification code screen
            if "enter 6-digit code" in ui_content or "digit code" in ui_content or "verification code" in ui_content or "verify" in ui_content:
                logger.info("[2FA_REQUIRED] TikTok requested email verification code.")
                report("2FA_REQUIRED", "Email verification code requested by TikTok")
                
                if gmail_addr and gmail_pwd:
                    logger.info(f"Querying Gmail IMAP SSL for {gmail_addr}...")
                    email_srv = GmailVerificationService(gmail_addr, gmail_pwd)
                    code = email_srv.fetch_tiktok_verification_code(timeout_seconds=45)
                    if code:
                        logger.info(f"Typing retrieved verification code '{code[:2]}****' into TikTok...")
                        self.adb.shell(f"input text {code}")
                        time.sleep(1)
                        self.adb.hide_keyboard()
                        time.sleep(2)
                        report("LOGIN_SUBMITTING", f"Submitted 2FA code {code[:2]}****")
                        time.sleep(4)
                        if self.adb.is_authenticated_user_feed():
                            logger.info(f"[+] [LOGIN_SUCCESS] 2FA verified successfully for {masked_acc}!")
                            self._dismiss_post_login_prompts()
                            report("AUTHENTICATED", "2FA verified into main feed")
                            return True
                    else:
                        logger.warning("[-] Gmail 2FA code retrieval timed out.")
                        report("LOGIN_FAILED", "2FA code timeout from Gmail IMAP")
                        self.adb.take_screenshot("login_failure_view.png")
                        return False
                else:
                    logger.warning("[-] 2FA required but no Gmail App Password configured.")
                    report("LOGIN_BLOCKED", "2FA required but Gmail credentials missing")
                    self.adb.take_screenshot("login_failure_view.png")
                    return False

            # Outcome C1: Rate limited / Maximum attempts reached (IP level block)
            if any(rate_msg in ui_content for rate_msg in ["maximum number of attempts", "too many attempts", "try again later", "frequent requests"]):
                logger.error(f"[-] [LOGIN_RATE_LIMITED] TikTok rate limit reached for {masked_acc} (IP flagged/blocked).")
                self.last_failure_reason = "IP_RATE_LIMITED"
                report("LOGIN_RATE_LIMITED", "Maximum number of attempts reached (IP rate-limited by TikTok)")
                self.adb.take_screenshot("login_failure_view.png")
                return False

            # Outcome C2: Incorrect credentials error
            if any(err_msg in ui_content for err_msg in ["incorrect password", "account doesn't exist", "wrong password"]):
                logger.error(f"[-] [LOGIN_FAILED] Invalid credentials reported by TikTok for {masked_acc}.")
                self.last_failure_reason = "INVALID_CREDENTIALS"
                report("LOGIN_FAILED", "Invalid credentials reported by TikTok")
                self.adb.take_screenshot("login_failure_view.png")
                return False

            # Outcome D: CAPTCHA / Puzzle challenge
            if any(c in ui_content for c in ["slide to complete", "select 2 objects", "security check", "puzzle", "captcha"]):
                logger.warning(f"[-] [LOGIN_BLOCKED] Security challenge presented by TikTok.")
                report("LOGIN_BLOCKED", "Interactive CAPTCHA/Puzzle challenge detected")
                self.adb.take_screenshot("login_failure_view.png")
                return False

            time.sleep(3)

        # Outcome E: Timeout with login screen still visible
        if self.adb.is_login_or_signup_screen():
            logger.error("[-] [LOGIN_FAILED] Login screen remains visible after timeout (LOGIN_SCREEN_STILL_VISIBLE).")
            report("LOGIN_FAILED", "LOGIN_SCREEN_STILL_VISIBLE")
            self.adb.take_screenshot("login_failure_view.png")
            return False

        # Final verification check
        if self.adb.is_authenticated_user_feed():
            self._dismiss_post_login_prompts()
            report("AUTHENTICATED", "User authenticated into main feed")
            return True

        logger.error("[-] [LOGIN_FAILED] Application did not reach authenticated state.")
        report("LOGIN_FAILED", "App not in authenticated feed")
        self.adb.take_screenshot("login_failure_view.png")
        return False

    def _dismiss_initial_onboarding(self, width: int = 720, height: int = 1280) -> None:
        """Dismisses splash, terms, interest selection, tutorial swipe overlays, and birthdate modal."""
        w = self.adb.screen_width or width or 720
        h = self.adb.screen_height or height or 1280
        if self.adb.click_element(text="Agree and continue") or self.adb.click_element(text="Agree"):
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
        """Dismisses post-login prompts: Save info, Notifications, Sync contacts, Birthdate modal."""
        time.sleep(1.5)
        self.adb.handle_birthdate_modal()
        if self.adb.click_element(text="Save") or self.adb.click_element(text="Not now"):
            time.sleep(1)
        if self.adb.click_element(text="Don't allow") or self.adb.click_element(text="Deny"):
            time.sleep(1)
        if self.adb.click_element(text="Not now") or self.adb.click_element(text="Cancel"):
            time.sleep(1)
