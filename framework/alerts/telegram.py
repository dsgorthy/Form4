"""Telegram alert sender — direct HTTPS POST, no python-telegram-bot dep.

Used as the recipient channel for trade-candidate alerts in `execution_mode:
alert_only`. Replaces the 2026-05-02 iMessage attempt (Messages.app AppleScript
hangs over SSH due to GUI-session requirements).

Why Telegram (this time, for trade alerts specifically):
  - Free; no per-message cost
  - HTTP-only — works from any server, no GUI / cred / browser session needed
  - Native iOS push via Telegram app; also web, desktop, watch
  - Markdown formatting + clickable links in messages
  - 2026-05-02 "no push channel" decision was about ops alerts (which can
    be noisy); high-conviction trade alerts (~5-20/day max) are categorically
    different and the architectural exception is defensible.

Setup (one-time):
  1. In Telegram, message @BotFather → /newbot → name + username → get token
  2. Message your new bot → /start (Telegram requires this before bot can DM you)
  3. Run scripts/telegram_setup.py to fetch chat_id from /getUpdates
  4. Set TELEGRAM_ALERT_BOT_TOKEN + TELEGRAM_ALERT_CHAT_ID in .env

Throttle: this module does NOT throttle. Caller (trade_alert.py) is
responsible for dedup. Trade alerts are low-volume by design — a single
candidate produces one alert; dedup against trade_decision_audit ensures
we don't re-alert the same candidate on subsequent scan cycles.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_telegram(
    text: str,
    *,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
) -> bool:
    """POST a message to Telegram. Returns True on 200, False otherwise.

    bot_token / chat_id default to TELEGRAM_ALERT_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID
    env vars. Both must be set for the send to fire — logs a WARNING and
    returns False if either is missing (so cw_runner can detect unconfigured
    state and fall back to the file log).
    """
    bot_token = bot_token or os.environ.get("TELEGRAM_ALERT_BOT_TOKEN")
    chat_id = chat_id or os.environ.get("TELEGRAM_ALERT_CHAT_ID")
    if not bot_token or not chat_id:
        logger.warning(
            "send_telegram: missing bot_token or chat_id (env "
            "TELEGRAM_ALERT_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID); "
            "skipping send. Would have sent: %r", text[:200],
        )
        return False
    if not text:
        return False

    url = TELEGRAM_API.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    try:
        r = httpx.post(url, data=payload, timeout=10.0)
    except httpx.TimeoutException:
        logger.warning("send_telegram: HTTP timeout (10s)")
        return False
    except httpx.HTTPError as exc:
        logger.warning("send_telegram: HTTP error %s", exc)
        return False

    if r.status_code == 200:
        return True

    # Common failure cases worth surfacing in logs:
    #   401: bad bot_token
    #   400: bad chat_id, or unparseable Markdown
    #   403: bot was blocked or chat was deleted
    #   429: rate-limit (Telegram allows ~30 msg/sec to a single chat)
    body = (r.text or "")[:300]
    logger.warning(
        "send_telegram: HTTP %d — %s", r.status_code, body,
    )
    return False


def send_self_test() -> bool:
    """One-line liveness probe."""
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return send_telegram(f"_form4 telegram alive @ {ts}_")
