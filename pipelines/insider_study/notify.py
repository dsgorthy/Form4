"""Telegram notification helper for insider signal analysis pipeline."""

import requests
import os

BOT_TOKEN = "8676824600:AAHcTkRFmRL25HwW1OC-l1jPyoDmYiu69u0"
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send(text: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    try:
        resp = requests.post(URL, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }, timeout=10)
        if resp.status_code != 200:
            # Retry without markdown
            resp = requests.post(URL, json={
                "chat_id": CHAT_ID,
                "text": text,
            }, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"Telegram send error: {e}")
        return False


def phase_start(phase: str, details: str = ""):
    """Notify that a phase has started."""
    msg = f"🔬 *Insider Analysis*\n▶️ Starting: {phase}"
    if details:
        msg += f"\n{details}"
    send(msg)


def phase_end(phase: str, summary: str):
    """Notify that a phase has completed."""
    msg = f"🔬 *Insider Analysis*\n✅ Completed: {phase}\n\n{summary}"
    send(msg)


def progress(phase: str, pct: float, details: str = ""):
    """Send a progress update during a long-running step."""
    bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
    msg = f"🔬 *Insider Analysis*\n⏳ {phase}\n{bar} {pct:.1f}%"
    if details:
        msg += f"\n{details}"
    send(msg)


def error(phase: str, err: str):
    """Send an error alert."""
    msg = f"🔬 *Insider Analysis*\n❌ ERROR in {phase}\n\n{err}"
    send(msg)


if __name__ == "__main__":
    send("🔬 *Insider Signal Analysis* — Notification test. Pipeline ready.")
