from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Optional

import httpx

from api.config import RESEND_API_KEY

logger = logging.getLogger(__name__)

RESEND_URL = "https://api.resend.com/emails"
FROM_ADDRESS = "Form4 <alerts@form4.app>"


def send_email(
    to: str,
    subject: str,
    html: str,
    *,
    reply_to: Optional[str] = None,
) -> bool:
    """Send a transactional email via Resend. Returns True on success."""
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set, skipping email to %s", to)
        return False

    payload: dict = {
        "from": FROM_ADDRESS,
        "to": [to],
        "subject": subject,
        "html": html,
    }
    if reply_to:
        payload["reply_to"] = reply_to

    try:
        resp = httpx.post(
            RESEND_URL,
            json=payload,
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return True
        logger.error("Resend API error %d: %s", resp.status_code, resp.text)
        return False
    except httpx.HTTPError as exc:
        logger.error("Resend request failed: %s", exc)
        return False


def generate_unsubscribe_token(user_id: str) -> str:
    """Generate a signed token for one-click unsubscribe links."""
    if not RESEND_API_KEY:
        return ""
    return hmac.new(
        RESEND_API_KEY.encode(),
        user_id.encode(),
        hashlib.sha256,
    ).hexdigest()[:32]


def verify_unsubscribe_token(user_id: str, token: str) -> bool:
    """Verify an unsubscribe token."""
    expected = generate_unsubscribe_token(user_id)
    return hmac.compare_digest(expected, token)


def build_notification_email(title: str, body: str, unsubscribe_url: str = "") -> str:
    """Build HTML email for a single notification."""
    unsub = ""
    if unsubscribe_url:
        unsub = f'<p style="margin-top:24px;font-size:12px;color:#888;"><a href="{unsubscribe_url}" style="color:#888;">Unsubscribe from email alerts</a></p>'

    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:560px;margin:0 auto;padding:24px;background:#0A0A0F;color:#E8E8ED;">
      <div style="margin-bottom:16px;">
        <span style="font-size:18px;font-weight:bold;color:#E8E8ED;">Form<span style="color:#3B82F6;">4</span></span>
      </div>
      <div style="background:#12121A;border:1px solid #2A2A3A;border-radius:8px;padding:20px;">
        <h2 style="margin:0 0 8px;font-size:16px;color:#E8E8ED;">{title}</h2>
        <p style="margin:0;font-size:14px;color:#8888A0;line-height:1.5;">{body}</p>
      </div>
      {unsub}
    </div>
    """


def build_digest_email(
    notifications: list[dict],
    unsubscribe_url: str = "",
) -> str:
    """Build HTML email for a daily digest of notifications."""
    items_html = ""
    for n in notifications:
        items_html += f"""
        <div style="padding:12px 0;border-bottom:1px solid #2A2A3A;">
          <div style="font-size:14px;font-weight:600;color:#E8E8ED;">{n['title']}</div>
          <div style="font-size:13px;color:#8888A0;margin-top:4px;">{n['body']}</div>
        </div>
        """

    unsub = ""
    if unsubscribe_url:
        unsub = f'<p style="margin-top:24px;font-size:12px;color:#888;"><a href="{unsubscribe_url}" style="color:#888;">Unsubscribe from email alerts</a></p>'

    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:560px;margin:0 auto;padding:24px;background:#0A0A0F;color:#E8E8ED;">
      <div style="margin-bottom:16px;">
        <span style="font-size:18px;font-weight:bold;color:#E8E8ED;">Form<span style="color:#3B82F6;">4</span></span>
        <span style="margin-left:8px;font-size:13px;color:#8888A0;">Daily Digest</span>
      </div>
      <div style="background:#12121A;border:1px solid #2A2A3A;border-radius:8px;padding:16px;">
        <div style="font-size:12px;color:#55556A;margin-bottom:8px;">{len(notifications)} new alert{"s" if len(notifications) != 1 else ""}</div>
        {items_html}
      </div>
      {unsub}
    </div>
    """
