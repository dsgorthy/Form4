"""ntfy.sh alert sender — direct HTTP POST, no account / no tokens.

Used as the recipient channel for trade-candidate alerts in `execution_mode:
alert_only`. Replaces the failed Telegram (bot-token 401) and iMessage
(GUI-session hang) attempts.

Why ntfy.sh for trade alerts:
  - Free; public ntfy.sh server. No account, no token, no 2FA.
  - HTTP-only — works from any environment.
  - Native iOS app (App Store: "ntfy") pushes to phone in 1-2 seconds.
  - Topic-based: anyone subscribed to the topic gets the message. We use a
    long-random topic name so it functions as a shared secret.

Security model:
  ntfy.sh is a public broadcast service. The topic name IS the access
  credential — anyone who guesses it can read your alerts AND publish to
  them. Use a long random topic (12+ chars) and DO NOT put it in any public
  repo / chat log. We treat NTFY_ALERT_TOPIC the same as a token.

Setup:
  1. iOS: install "ntfy" from App Store (free, no account)
  2. Open the app → Subscribe to topic → enter the value from
     NTFY_ALERT_TOPIC env var
  3. Done — sends arrive as iOS notifications

Throttle: this module does NOT throttle. Caller is responsible for dedup.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NTFY_BASE = "https://ntfy.sh"


def send_ntfy(
    text: str,
    *,
    topic: Optional[str] = None,
    title: Optional[str] = None,
    priority: int = 3,        # 1=min, 2=low, 3=default, 4=high, 5=urgent
    tags: Optional[list[str]] = None,
    click_url: Optional[str] = None,
) -> bool:
    """POST a message to ntfy.sh/<topic>. Returns True on 2xx, False otherwise.

    topic defaults to NTFY_ALERT_TOPIC env var. If unset, logs WARNING and
    returns False (cw_runner can detect this and fall back to NDJSON log).

    title: optional iOS notification title (the "from" line). Body is `text`.
    priority: 3 = default sound; 5 = persistent + urgent sound.
    tags: emoji shortcodes (e.g., ["chart_with_upwards_trend"]) prepended
          to title on iOS.
    click_url: tapping the notification opens this URL.
    """
    topic = topic or os.environ.get("NTFY_ALERT_TOPIC")
    if not topic:
        logger.warning(
            "send_ntfy: NTFY_ALERT_TOPIC unset; skipping. Would have sent: %r",
            text[:200],
        )
        return False
    if not text:
        return False

    url = f"{NTFY_BASE}/{topic}"
    headers = {
        "Priority": str(priority),
    }
    if title:
        headers["Title"] = title
    if tags:
        headers["Tags"] = ",".join(tags)
    if click_url:
        headers["Click"] = click_url

    try:
        r = httpx.post(url, content=text.encode("utf-8"),
                       headers=headers, timeout=10.0)
    except httpx.TimeoutException:
        logger.warning("send_ntfy: HTTP timeout (10s)")
        return False
    except httpx.HTTPError as exc:
        logger.warning("send_ntfy: HTTP error %s", exc)
        return False

    if 200 <= r.status_code < 300:
        return True
    logger.warning("send_ntfy: HTTP %d — %s", r.status_code, (r.text or "")[:300])
    return False


def send_self_test() -> bool:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return send_ntfy(
        f"form4 ntfy channel alive @ {ts}",
        title="form4 alert channel",
        tags=["white_check_mark"],
        priority=2,
    )
