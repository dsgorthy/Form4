#!/usr/bin/env python3
"""Account email sequence — four emails to a free account, by account age.

    Day 0   welcome    what the account does, who they already follow
    Day 3   your_week  what the people they follow filed (or the market, if nobody)
    Day 10  pro_once   what Pro adds; the only Pro pitch there is
    Day 30  win_back   only if there has been no sign-in for two weeks

Runs every 6 hours under Dagster (ops_trial_emails_6h). Lists Clerk users,
works out each account's age, sends whatever is due inside its window and
records it in sent_trial_emails so nothing goes twice. Paying accounts and
live comps get nothing.

The file keeps its old name because a Dagster asset, a registry entry and
this table are keyed on it. What it sent until 2026-09-17 was a six-step
TRIAL funnel — "your trial starts now", "2 days left", "your trial has
ended", "your grace period has ended" — to people who never chose a trial,
because every account was one by age. Accounts are free now (api/auth.py)
and the sequence says so.

Usage:
    python3 pipelines/trial_emails.py              # normal run
    python3 pipelines/trial_emails.py --dry-run    # preview without sending
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

from api.comp import comp_lapsed
from api.email import send_email, generate_unsubscribe_token
from api.email_templates import (
    APP_URL,
    EMAIL_SEQUENCE,
    WIN_BACK_QUIET_DAYS,
    welcome_email,
    your_week_email,
    pro_once_email,
    win_back_email,
)
from api.filters import MEANINGFUL_CLASSES
from pipelines.generate_stocktwits_posts import price_is_foreign
from api.public_fields import STRATEGY_LABELS
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
# Schema for tracking sent emails (name kept: the table has history)
# ───────────────────────────────────────────────────────────────────

TRIAL_EMAILS_SCHEMA = """\
CREATE TABLE IF NOT EXISTS sent_trial_emails (
    user_id TEXT NOT NULL,
    email_name TEXT NOT NULL,
    sent_at TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, email_name)
)
"""


def _ensure_schema(conn) -> None:
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
    addrs = user_data.get("email_addresses", [])
    primary_id = user_data.get("primary_email_address_id")
    for addr in addrs:
        if addr.get("id") == primary_id:
            return addr.get("email_address")
    return addrs[0].get("email_address") if addrs else None


# ───────────────────────────────────────────────────────────────────
# Pure rules — tested
# ───────────────────────────────────────────────────────────────────

def is_paying(public_meta: dict) -> bool:
    """A paid or live-comped account gets none of this."""
    return public_meta.get("tier") in ("pro", "pro_plus") and not comp_lapsed(public_meta)


def win_back_due(last_sign_in_ms: int | None, now: datetime) -> bool:
    """The day-30 note is for an account that has gone quiet: no sign-in for
    WIN_BACK_QUIET_DAYS. Someone who was here on Tuesday knows what happened."""
    if not last_sign_in_ms:
        return True
    last = datetime.utcfromtimestamp(last_sign_in_ms / 1000)
    return (now - last) >= timedelta(days=WIN_BACK_QUIET_DAYS)


def in_window(age_days: float, target_day: int) -> bool:
    return (target_day - 0.5) <= age_days <= (target_day + DAY_TOLERANCE + 0.5)


# ───────────────────────────────────────────────────────────────────
# What the emails are about: the account's follows
# ───────────────────────────────────────────────────────────────────

def _follows(conn, user_id: str) -> tuple[list[str], list[str], list[int]]:
    """(display names, tickers, insider ids) this account follows."""
    rows = conn.execute(
        """SELECT w.ticker, w.insider_id, COALESCE(i.display_name, i.name) AS insider_name
             FROM watchlist w
             LEFT JOIN insiders i ON i.insider_id = w.insider_id
            WHERE w.user_id = ?
            ORDER BY w.added_at""",
        (user_id,),
    ).fetchall()
    names, tickers, insiders = [], [], []
    for r in rows:
        if r["insider_id"]:
            insiders.append(int(r["insider_id"]))
            names.append(r["insider_name"] or "an insider")
        elif r["ticker"]:
            tickers.append(r["ticker"])
            names.append(r["ticker"])
    return names, tickers, insiders


def _recent_filings(conn, tickers: list[str], insiders: list[int], days: int, limit: int = 5) -> list[dict]:
    """Discretionary filings by the followed tickers/insiders, largest first."""
    if not tickers and not insiders:
        return []
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    classes = tuple(sorted(MEANINGFUL_CLASSES))
    rows = conn.execute(
        f"""SELECT t.ticker, COALESCE(i.display_name, i.name) AS insider_name, t.trade_type,
                   SUM(t.value) AS value, SUM(t.qty) AS qty, MAX(t.filing_date) AS filing_date
              FROM trades t
              LEFT JOIN insiders i ON i.insider_id = t.insider_id
             WHERE t.filing_date >= ?
               AND t.signal_class IN ({",".join("?" * len(classes))})
               AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
               AND t.superseded_by IS NULL
               AND (t.ticker = ANY(?) OR t.insider_id = ANY(?))
             GROUP BY t.ticker, t.insider_id, i.display_name, i.name, t.trade_type
             ORDER BY SUM(t.value) DESC
             LIMIT ?""",
        (cutoff, *classes, tickers or [""], insiders or [-1], limit * 3),
    ).fetchall()
    return _drop_foreign_priced(conn, [dict(r) for r in rows])[:limit]


def _drop_foreign_priced(conn, rows: list[dict]) -> list[dict]:
    """A filing priced in another currency is not a headline. UMC's CFO
    filed 1.6M Taiwan-listed shares at NT$142-145 on 2026-09-16; against
    the $22.54 ADR it read as a $228.8M sale and would have led the day-3
    email for every account that follows nobody. Same rule, same function,
    as the StockTwits generator: a filed price 3x off our close either way."""
    tickers = sorted({r["ticker"] for r in rows if r.get("ticker")})
    if not tickers:
        return rows
    closes = {
        rec["ticker"]: rec["close"]
        for rec in conn.execute(
            """SELECT DISTINCT ON (d.ticker) d.ticker, d.close
                 FROM prices.daily_prices d
                WHERE d.ticker = ANY(?)
                ORDER BY d.ticker, d.date DESC""",
            (tickers,),
        ).fetchall()
    }
    kept = []
    for r in rows:
        qty = r.get("qty") or 0
        price = (r.get("value") or 0) / qty if qty else None
        if price_is_foreign(price, closes.get(r.get("ticker"))):
            logger.info("dropped %s from the email: filed price %.2f against our close %.2f",
                        r.get("ticker"), price, float(closes[r["ticker"]]))
            continue
        kept.append(r)
    return kept


def _book_latest_entry(conn, strategy: str) -> dict | None:
    """The chosen book's most recent entry, for the welcome example."""
    row = conn.execute(
        """SELECT ticker, entry_date, entry_price FROM strategy_portfolio
            WHERE strategy = ? AND execution_source IN ('alert', 'simulated') AND is_live = FALSE
            ORDER BY entry_date DESC LIMIT 1""",
        (strategy,),
    ).fetchone()
    if not row:
        return None
    return {"kind": "book", "label": STRATEGY_LABELS.get(strategy, strategy),
            "ticker": row["ticker"], "entry_date": row["entry_date"], "entry_price": row["entry_price"]}


def _top_filings(conn, days: int, limit: int = 5) -> list[dict]:
    """The market's largest discretionary filings — for an account that
    follows nobody yet."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    classes = tuple(sorted(MEANINGFUL_CLASSES))
    rows = conn.execute(
        f"""SELECT t.ticker, COALESCE(i.display_name, i.name) AS insider_name, t.trade_type,
                   SUM(t.value) AS value, SUM(t.qty) AS qty, MAX(t.filing_date) AS filing_date
              FROM trades t
              LEFT JOIN insiders i ON i.insider_id = t.insider_id
             WHERE t.filing_date >= ?
               AND t.signal_class IN ({",".join("?" * len(classes))})
               AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
               AND t.superseded_by IS NULL
               AND COALESCE(i.is_entity, 0) = 0
             GROUP BY t.ticker, t.insider_id, i.display_name, i.name, t.trade_type
             ORDER BY SUM(t.value) DESC
             LIMIT ?""",
        (cutoff, *classes, limit * 3),
    ).fetchall()
    return _drop_foreign_priced(conn, [dict(r) for r in rows])[:limit]


# ───────────────────────────────────────────────────────────────────
# Email dispatch
# ───────────────────────────────────────────────────────────────────

def _build_email(email_name: str, user_data: dict, conn) -> tuple[str, str] | None:
    user_id = user_data.get("id", "")
    unsub_token = generate_unsubscribe_token(user_id)
    # Param must be `user_id` — that's the name the endpoint declares
    # (api/routers/notifications.py). `user` returns 422.
    unsub_url = f"{APP_URL}/api/v1/notifications/unsubscribe?user_id={user_id}&token={unsub_token}"

    names, tickers, insiders = _follows(conn, user_id)
    following = bool(names)

    if email_name == "welcome":
        # The strategy book chosen in onboarding lives in Clerk unsafe metadata.
        key = (user_data.get("unsafe_metadata") or {}).get("defaultStrategy")
        example = None
        recent = _recent_filings(conn, tickers, insiders, days=90, limit=1) if following else []
        if recent:
            example = {"kind": "filing", **recent[0]}
        elif key:
            example = _book_latest_entry(conn, key)
        return welcome_email(names, STRATEGY_LABELS.get(key), unsub_url, example)
    if email_name == "your_week":
        items = _recent_filings(conn, tickers, insiders, days=3) if following else _top_filings(conn, days=3)
        return your_week_email(items, following, unsub_url)
    if email_name == "pro_once":
        return pro_once_email(unsub_url)
    if email_name == "win_back":
        items = _recent_filings(conn, tickers, insiders, days=30) if following else _top_filings(conn, days=30)
        return win_back_email(items, following, unsub_url)
    return None


def process_user(user_data: dict, conn, dry_run: bool = False, now: datetime | None = None) -> int:
    """Send whatever is due for one account. Returns the number sent."""
    now = now or datetime.utcnow()
    user_id = user_data.get("id", "")
    created_at = user_data.get("created_at")
    if not user_id or not created_at:
        return 0
    if is_paying(user_data.get("public_metadata") or {}):
        return 0

    created_ts = created_at / 1000 if created_at > 1e12 else created_at
    age_days = (now.timestamp() - created_ts) / 86400

    email = _get_user_email(user_data)
    if not email:
        return 0

    sent = 0
    for email_name, target_day in EMAIL_SEQUENCE:
        if not in_window(age_days, target_day):
            continue
        if email_name == "win_back" and not win_back_due(user_data.get("last_sign_in_at"), now):
            continue
        existing = conn.execute(
            "SELECT 1 FROM sent_trial_emails WHERE user_id = ? AND email_name = ?",
            (user_id, email_name),
        ).fetchone()
        if existing:
            continue

        result = _build_email(email_name, user_data, conn)
        if not result:
            continue
        subject, html = result

        if dry_run:
            logger.info("[DRY RUN] Would send '%s' to %s (day %.1f)", email_name, email, age_days)
        else:
            if not send_email(email, subject, html):
                logger.error("Failed to send '%s' to %s", email_name, email)
                continue
            conn.execute(
                "INSERT OR IGNORE INTO sent_trial_emails (user_id, email_name) VALUES (?, ?)",
                (user_id, email_name),
            )
            conn.commit()
            logger.info("Sent '%s' to %s (day %.1f)", email_name, email, age_days)
        sent += 1
    return sent


# ───────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Account email sequence runner")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    args = parser.parse_args()

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
