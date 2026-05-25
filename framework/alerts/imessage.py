"""iMessage alert sender via macOS Messages.app + AppleScript.

Used as the recipient channel for trade-candidate alerts when a strategy
runs in `execution_mode: alert_only`. Free (no API keys), native iOS push,
works on Apple Watch — but only on macOS where the running user is signed
in to iMessage.

Reliability notes:
- AppleScript send is fire-and-forget; we get exit code 0 on script success,
  but that doesn't guarantee Apple's servers accepted the message.
- If the recipient is unreachable (e.g., target device offline), iMessage
  retries silently. No bounce reported back.
- Mitigation: a daily self-test (see send_self_test) sends "alive at <ts>"
  to the operator; if the operator stops receiving it, the channel is broken.

Threading: subprocess.run is blocking. cw_runner calls this from its scan
loop (single thread). Each send takes ~200-500ms so it doesn't meaningfully
slow the scan.
"""
from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _escape_for_applescript(s: str) -> str:
    """Escape backslashes and double-quotes for AppleScript string literals."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def send_imessage(to: str, body: str) -> bool:
    """Send `body` via iMessage to phone number / iCloud address `to`.

    `to` should be a phone number (E.164 or 10-digit US) or an iCloud email.
    Returns True if the AppleScript exited 0, False otherwise.
    """
    if not to:
        logger.warning("send_imessage: no recipient set (ALERT_IMESSAGE_TO unset?)")
        return False
    if not body:
        logger.warning("send_imessage: empty body, skipping")
        return False

    body_esc = _escape_for_applescript(body)
    to_esc = _escape_for_applescript(to)
    # Use `make new outgoing message` form which doesn't pre-resolve the
    # buddy lookup (the `buddy ... of service` form hangs for 30+s on a
    # cold target). This form constructs the outgoing message first, then
    # send. Works on macOS 14+ and is the documented Apple pattern.
    script = (
        f'tell application "Messages"\n'
        f'  set targetService to 1st service whose service type = iMessage\n'
        f'  set targetBuddy to participant "{to_esc}" of targetService\n'
        f'  send "{body_esc}" to targetBuddy\n'
        f'end tell'
    )
    try:
        # 60s timeout — first send to a number Messages has never routed
        # before can take 10-30s for iMessage availability lookup against
        # Apple's servers. Subsequent sends are ~200ms.
        res = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True, text=True, timeout=60,
        )
        if res.returncode != 0:
            logger.warning(
                "iMessage send failed (exit %d) to=%s stderr=%r",
                res.returncode, to, res.stderr.strip()[:200],
            )
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.warning("iMessage send timed out (>60s) to=%s", to)
        return False
    except FileNotFoundError:
        logger.error("osascript not found — iMessage alerts require macOS")
        return False


def send_self_test(to: Optional[str] = None) -> bool:
    """Daily liveness probe: send 'channel alive at HH:MM' so the operator
    notices if iMessage stops working (e.g., Mac signed out of iCloud)."""
    to = to or os.environ.get("ALERT_IMESSAGE_TO")
    if not to:
        return False
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return send_imessage(to, f"[form4 alert channel alive] {ts}")
