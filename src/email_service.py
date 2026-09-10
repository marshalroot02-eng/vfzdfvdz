"""
TikTok Booster - Gmail IMAP 2FA Verification Code Extractor
Securely queries Gmail over IMAP SSL using Gmail App Passwords to retrieve
6-digit verification codes sent by TikTok during in-app authentication.
"""

import re
import time
import imaplib
import email
import email.utils
from email.header import decode_header
from datetime import datetime, timezone
import logging

logger = logging.getLogger("EmailService")

class GmailVerificationService:
    def __init__(self, gmail_address: str, app_password: str):
        self.gmail_address = (gmail_address or "").strip()
        self.app_password = (app_password or "").replace(" ", "").strip()

    def fetch_tiktok_verification_code(self, timeout_seconds: int = 60, check_interval: int = 3, min_timestamp: float = None) -> str:
        """
        Polls Gmail inbox over IMAP SSL for incoming 6-digit TikTok verification codes.
        Ensures codes are fresh (not from an earlier attempt) by checking message Date against min_timestamp.
        Returns the 6-digit string if found, or None if timed out.
        """
        if not self.gmail_address or not self.app_password:
            logger.warning("Gmail address or App Password missing. Cannot retrieve 2FA code.")
            return None

        masked_email = f"{self.gmail_address[:3]}...@{self.gmail_address.split('@')[-1]}" if "@" in self.gmail_address else "gmail_user"
        logger.info(f"=== [2FA IMAP Service] Waiting for fresh TikTok Verification Code for {masked_email} (timeout: {timeout_seconds}s) ===")

        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
                mail.login(self.gmail_address, self.app_password)
                mail.select("INBOX")

                # Search unread emails or recent emails
                status, messages = mail.search(None, '(OR FROM "TikTok" SUBJECT "TikTok")')
                if status != "OK" or not messages[0] or not messages[0].strip():
                    status, messages = mail.search(None, 'ALL')
                if status == "OK" and messages[0]:
                    msg_ids = messages[0].split()
                    # Check the latest 5 messages
                    for msg_id in reversed(msg_ids[-5:]):
                        res, msg_data = mail.fetch(msg_id, "(RFC822)")
                        for response_part in msg_data:
                            if isinstance(response_part, tuple):
                                msg = email.message_from_bytes(response_part[1])
                                subject_parts = decode_header(msg.get("Subject", "") or "")
                                subject = "".join(
                                    p.decode(enc or "utf-8", errors="ignore") if isinstance(p, bytes) else str(p)
                                    for p, enc in subject_parts
                                )

                                # Verify that email is fresh (received recently, not from an old prior attempt)
                                date_hdr = msg.get("Date")
                                if date_hdr:
                                    try:
                                        msg_dt = email.utils.parsedate_to_datetime(date_hdr)
                                        msg_ts = msg_dt.timestamp()
                                        now_ts = datetime.now(timezone.utc).timestamp()
                                        age_secs = now_ts - msg_ts

                                        if min_timestamp and msg_ts < (min_timestamp - 10):
                                            logger.info(f"[-] Skipping old email from prior session (email time: {msg_dt.strftime('%H:%M:%S')}, min required: {datetime.fromtimestamp(min_timestamp, timezone.utc).strftime('%H:%M:%S')}). Waiting for new code...")
                                            continue

                                        if age_secs > 180:
                                            logger.info(f"[-] Skipping stale email from {age_secs:.0f}s ago. Waiting for newly delivered code...")
                                            continue
                                    except Exception as ex:
                                        logger.debug(f"Date header parse note: {ex}")

                                # Extract body text
                                body = ""
                                if msg.is_multipart():
                                    for part in msg.walk():
                                        content_type = part.get_content_type()
                                        if content_type == "text/plain":
                                            payload = part.get_payload(decode=True)
                                            if payload:
                                                body += payload.decode("utf-8", errors="ignore")
                                else:
                                    payload = msg.get_payload(decode=True)
                                    if payload:
                                        body = payload.decode("utf-8", errors="ignore")

                                full_text = f"{subject} {body}"
                                # Match 6 digit code pattern
                                matches = re.findall(r'(?:code|is|enter)\s*:?\s*(\d{6})\b', full_text, re.IGNORECASE)
                                if not matches:
                                    matches = re.findall(r'\b(\d{6})\b', full_text)

                                if matches:
                                    code = matches[0]
                                    logger.info(f"[+] [2FA CODE FOUND] Successfully retrieved fresh TikTok verification code: {code} (date: {date_hdr or 'recent'})")
                                    try:
                                        mail.close()
                                        mail.logout()
                                    except Exception:
                                        pass
                                    return code

                try:
                    mail.close()
                    mail.logout()
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"IMAP poll note: {e}")

            time.sleep(check_interval)

        logger.warning(f"[-] [2FA TIMEOUT] No TikTok verification code received after {timeout_seconds}s.")
        return None
