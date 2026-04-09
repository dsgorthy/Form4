#!/usr/bin/env python3
"""Trial email sequence — sends lifecycle emails to users based on account age.

Runs daily via launchd. Queries Clerk for all users, checks created_at to
determine which email to send, tracks sent emails in notifications.db to
avoid duplicates.

Email schedule:
    Day 0  — Welcome
    Day 3  — Value (top signals from their first 3 days)
    Day 5  — Urgency (2 days left on trial)
    Day 7  — Trial ended (grace period begins)
    Day 14 — Hard gate (grace period over)
    Day 30 — Win-back (what they missed)

Usage:
    python3 pipelines/trial_emails.py              # normal run
    python3 pipelines/trial_emails.py --dry-run     # preview without sending
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

_root_env = Path(__file__).resolve().parent.parent / ".env"
if _root_env.exists():
    load_dotenv(_root_env)

from api.email import send_email, generate_unsubscribe_token
from api.email_templates import (
    APP_URL,
    EMAIL_SEQUENCE,
    welcome_email,
    value_email,
    urgency_email,
    trial_ended_email,
    hard_gate_email,
    win_back_email,
)
from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")

# How many days of tolerance around the target day (handles cron timing drift)
DAY_TOLERANCE = 1

# ───────────────────────────────────────────────────────────────────
# Schema for tracking sent trial emails
# ───────────────────────────────────────────────────────────────────

TRIAL_EMAILS_SCHEMA = """\
CREATE TABLE IF NOT EXISTS sent_trial_emails (
    user_id TEXT NOT NULL,
    email_name TEXT NOT NULL,
    sent_at TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, email_name)
);
"""


def _ensure_schema(conn) -> None:
    """Create sent_trial_emails table if it doesn't exist."""
    conn.execute(TRIAL_EMAILS_SCHEMA)
    conn.commit()


# ───────────────────────────────────────────────────────────────────
# Clerk user listing
# ───────────────────────────────────────────────────────────────────


def _fetch_all_clerk_users() -> list[dict]:
    """Fetch all users from Clerk API (paginated)."""
    if not CLERK_SECRET_KEY:
        logger.error("CLERK_SECRET_KEY not set")
        return []

    import httpx

    users: list[dict] = []
    limit = 100
    offset = 0

    with httpx.Client(timeout=30) as client:
        while True:
            resp = client.get(
                "https://api.clerk.com/v1/users",
                headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
                params={"limit": limit, "offset": offset, "order_by": "-created_at"},
            )
            if resp.status_code != 200:
                logger.error("Clerk API error %d: %s", resp.status_code, resp.text)
                break

            batch = resp.json()
            if not batch:
                break

            users.extend(batch)

            if len(batch) < limit:
                break
            offset += limit

    logger.info("Fetched %d users from Clerk", len(users))
    return users


def _get_user_email(user_data: dict) -> str | None:
    """Extract primary email from Clerk user data."""
    addrs = user_data.get("email_addresses", [])
    primary_id = user_data.get("primary_email_address_id")
    for addr in addrs:
        if addr.get("id") == primary_id:
            return addr.get("email_address")
    return addrs[0].get("email_address") if addrs else None


# ───────────────────────────────────────────────────────────────────
# Top signals query (for value + win-back emails)
# ───────────────────────────────────────────────────────────────────


def _get_top_signals(days_back: int = 7, limit: int = 5) -> list[dict]:
    """Fetch top insider signals from the last N days for email content."""
    try:
        from api.db import DB_PATH as INSIDERS_DB_PATH

        conn = get_connection(readonly=True)
        cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        rows = conn.execute(
            """
            SELECT t.ticker,
                   COALESCE(i.display_name, i.name) AS insider_name,
                   t.trade_type,
                   SUM(t.value) AS value,
                   tr.return_7d
            FROM trades t
            LEFT JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.filing_date >= ?
              AND t.trans_code IN ('P', 'S')
              AND tr.return_7d IS NOT NULL
            GROUP BY t.ticker, t.insider_id, t.trade_type
            ORDER BY ABS(tr.return_7d) DESC
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()
        conn.close()

        return [
            {
                "ticker": r["ticker"],
                "insider_name": r["insider_name"],
                "trade_type": r["trade_type"],
                "value": r["value"],
                "return_7d": round(r["return_7d"] * 100, 1) if r["return_7d"] else None,
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("Failed to fetch top signals: %s", exc)
        return []


# ───────────────────────────────────────────────────────────────────
# Email dispatch
# ───────────────────────────────────────────────────────────────────


def _build_email(email_name: str, user_id: str) -> tuple[str, str] | None:
    """Build (subject, html) for a given email. Returns None if data unavailable."""
    unsub_token = generate_unsubscribe_token(user_id)
    unsub_url = f"{APP_URL}/api/v1/notifications/unsubscribe?user={user_id}&token={unsub_token}"

    if email_name == "welcome":
        return welcome_email(unsub_url)
    elif email_name == "value":
        signals = _get_top_signals(days_back=3)
        return value_email(signals, unsub_url)
    elif email_name == "urgency":
        return urgency_email(unsub_url)
    elif email_name == "trial_ended":
        return trial_ended_email(unsub_url)
    elif email_name == "hard_gate":
        return hard_gate_email(unsub_url)
    elif email_name == "win_back":
        signals = _get_top_signals(days_back=30, limit=5)
        return win_back_email(signals, unsub_url)
    return None


def process_user(
    user_data: dict,
    conn,
    dry_run: bool = False,
) -> int:
    """Check which emails to send for a user. Returns count of emails sent."""
    user_id = user_data.get("id", "")
    created_at = user_data.get("created_at")
    if not user_id or not created_at:
        return 0

    # Skip pro users (already paying)
    public_meta = user_data.get("public_metadata", {})
    if public_meta.get("tier") == "pro":
        return 0

    # Calculate account age in days
    # Clerk returns created_at as ms timestamp
    created_ts = created_at / 1000 if created_at > 1e12 else created_at
    age_days = (datetime.utcnow().timestamp() - created_ts) / 86400

    email = _get_user_email(user_data)
    if not email:
        return 0

    sent = 0
    for email_name, target_day in EMAIL_SEQUENCE:
        # Check if we're in the right window for this email
        if age_days < target_day - 0.5:
            continue  # Too early
        if age_days > target_day + DAY_TOLERANCE + 0.5:
            continue  # Too late (missed window)

        # Check if already sent
        existing = conn.execute(
            "SELECT 1 FROM sent_trial_emails WHERE user_id = ? AND email_name = ?",
            (user_id, email_name),
        ).fetchone()
        if existing:
            continue

        # Build and send
        result = _build_email(email_name, user_id)
        if not result:
            continue

        subject, html = result

        if dry_run:
            logger.info("[DRY RUN] Would send '%s' to %s (day %.1f)", email_name, email, age_days)
        else:
            success = send_email(email, subject, html)
            if success:
                conn.execute(
                    "INSERT OR IGNORE INTO sent_trial_emails (user_id, email_name) VALUES (?, ?)",
                    (user_id, email_name),
                )
                conn.commit()
                logger.info("Sent '%s' to %s (day %.1f)", email_name, email, age_days)
            else:
                logger.error("Failed to send '%s' to %s", email_name, email)
                continue

        sent += 1

    return sent


# ───────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Trial email sequence runner")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    args = parser.parse_args()

    # Open notifications DB for tracking
    conn = get_connection(readonly=False)
    _ensure_schema(conn)

    users = _fetch_all_clerk_users()
    if not users:
        logger.warning("No users fetched — check CLERK_SECRET_KEY")
        conn.close()
        return

    total_sent = 0
    for user_data in users:
        total_sent += process_user(user_data, conn, dry_run=args.dry_run)

    conn.close()

    prefix = "[DRY RUN] " if args.dry_run else ""
    logger.info("%sDone. %d email(s) sent across %d users.", prefix, total_sent, len(users))


if __name__ == "__main__":
    main()
