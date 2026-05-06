"""Critical alert sink: email-to-SMS via Resend.

Plain-text email to a carrier SMS gateway address (e.g. 5551234567@vtext.com)
delivers in 1-5 minutes — sufficient for catching live-money trading
failures within the same market session, without managing Pushover or Twilio
credentials. The Resend pipeline used by api/email.py is reused directly
(no circular dependency: this module reads RESEND_API_KEY from env, not from
api/config.py).

Only severity=critical alerts are routed here — info/warn/error stay
file-only in logs/alerts.ndjson.

Throttle: same (component, message_hash) cannot fire more than once per 15
minutes. Cache lives at logs/alerts_throttle.json so it survives runner
restarts. Throttle is intentional — the same fault can keep firing every
scan cycle, and a 5-minute SMS storm helps no one.

Configuration (env vars):
  CRITICAL_ALERT_SMS_TO      — carrier gateway, e.g. "5551234567@vtext.com"
  CRITICAL_ALERT_EMAIL_TO    — full email backup (optional, longer body)
  RESEND_API_KEY             — same key used by api/email.py
  CRITICAL_ALERT_SMS_DISABLE — truthy = no SMS (test bypass)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

RESEND_URL = "https://api.resend.com/emails"
FROM_ADDRESS = "Form4 <alerts@form4.app>"
THROTTLE_SECONDS = 15 * 60      # 15 minutes
THROTTLE_LOCK = threading.Lock()


def _throttle_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "logs" / "alerts_throttle.json"


def _hash_key(component: str, message: str) -> str:
    h = hashlib.sha256(f"{component}|{message}".encode()).hexdigest()
    return h[:16]


def _load_throttle() -> dict[str, float]:
    p = _throttle_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _save_throttle(data: dict[str, float]) -> None:
    p = _throttle_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        p.write_text(json.dumps(data))
    except Exception as e:
        logger.warning("alerts_throttle write failed: %s", e)


def _is_throttled(component: str, message: str) -> bool:
    key = _hash_key(component, message)
    now = datetime.now(timezone.utc).timestamp()
    with THROTTLE_LOCK:
        data = _load_throttle()
        # Garbage-collect old entries while we're here (keep file small).
        data = {k: ts for k, ts in data.items() if now - ts < THROTTLE_SECONDS * 4}
        last = data.get(key, 0.0)
        if now - last < THROTTLE_SECONDS:
            return True
        data[key] = now
        _save_throttle(data)
    return False


def _build_text_body(component: str, message: str, extra: dict | None) -> str:
    """Plain-text body that fits in an SMS. Carrier gateways truncate around
    140-160 chars after stripping HTML, so keep it tight."""
    body = f"[FORM4] {component}: {message}"
    if extra:
        # Append the most useful extras (ticker, qty, etc.) — capped.
        kvs = " | ".join(f"{k}={v}" for k, v in extra.items() if v not in (None, ""))
        if kvs:
            body = f"{body} | {kvs}"
    # Hard cap the whole thing — carrier gateways will silently chop.
    if len(body) > 280:
        body = body[:277] + "..."
    return body


def _send_via_resend(to: str, subject: str, body_text: str) -> bool:
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        logger.warning("RESEND_API_KEY not set — critical SMS to %s skipped", to)
        return False
    payload = {
        "from": FROM_ADDRESS,
        "to": [to],
        "subject": subject,
        # Both text and html so SMS gateways have something to render even
        # if they strip HTML.
        "text": body_text,
        "html": f"<pre>{body_text}</pre>",
    }
    try:
        resp = httpx.post(
            RESEND_URL, json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return True
        logger.error("Resend SMS error %d: %s", resp.status_code, resp.text[:300])
        return False
    except httpx.HTTPError as exc:
        logger.error("Resend SMS request failed: %s", exc)
        return False


def send_critical_sms(component: str, message: str, **extra) -> bool:
    """Send a critical alert to the configured SMS gateway. Throttled.
    Returns True on send (or throttled — both count as "handled"); False on
    config-missing or send-error. Never raises."""
    if str(os.getenv("CRITICAL_ALERT_SMS_DISABLE", "")).lower() in ("1", "true", "yes"):
        return True

    to_sms = os.getenv("CRITICAL_ALERT_SMS_TO", "").strip()
    to_email = os.getenv("CRITICAL_ALERT_EMAIL_TO", "").strip()
    if not to_sms and not to_email:
        # Not configured — silent no-op; the NDJSON entry still happened.
        return False

    if _is_throttled(component, message):
        return True

    body = _build_text_body(component, message, extra or None)
    subject = f"[FORM4-CRIT] {component[:40]}"

    ok_any = False
    if to_sms:
        if _send_via_resend(to_sms, subject, body):
            ok_any = True
    if to_email:
        # Same body to the longer-form inbox — the operator can read full
        # extras here even when the SMS truncated.
        full_body = body
        if extra:
            full_body += "\n\nExtras:\n" + "\n".join(
                f"  {k}: {v}" for k, v in extra.items()
            )
        if _send_via_resend(to_email, subject, full_body):
            ok_any = True

    return ok_any


# CLI for one-off testing:
#   python3 -m framework.alerts.sms test "this is a test"
if __name__ == "__main__":
    import sys
    # Load .env when invoked from CLI — the runner already does this at
    # startup, but standalone calls need it explicitly.
    try:
        from dotenv import load_dotenv
        repo = Path(__file__).resolve().parents[2]
        load_dotenv(repo / ".env")
    except Exception:
        pass
    component = sys.argv[1] if len(sys.argv) > 1 else "framework.alerts.sms.test"
    message = sys.argv[2] if len(sys.argv) > 2 else "test critical alert"
    ok = send_critical_sms(component, message, source="cli")
    if ok:
        print(f"sent → {os.getenv('CRITICAL_ALERT_SMS_TO', '?')}")
    else:
        print("not sent (check CRITICAL_ALERT_SMS_TO + RESEND_API_KEY in .env)")
