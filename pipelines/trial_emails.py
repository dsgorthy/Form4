#!/usr/bin/env python3
"""Account email sequence — five emails to a free account, by account age.

    Day 0   welcome       what the account does, who they already follow
    Day 1   founder_note  plain text from Derek: what did you come here to do?
                          Sent from and replied to support@form4.app; replies
                          are read by pipelines/support_replies.py
    Day 3   your_week     what the people they follow filed (or the market, if nobody)
    Day 10  pro_once   what Pro adds; the only Pro pitch there is
    Day 30  win_back   only if there has been no sign-in for two weeks

Runs hourly under Dagster (ops_trial_emails_hourly). Lists Clerk users,
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
from api.email import SUPPORT_ADDRESS, send_email, generate_unsubscribe_token
from api.email_templates import (
    APP_URL,
    EMAIL_SEQUENCE,
    FOUNDER_NOTE_SENDER,
    WIN_BACK_QUIET_DAYS,
    founder_note_email,
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


# THE WELCOME IS NOT WINDOWED LIKE THE OTHERS. It goes as soon as the job sees
# the account, and it is still owed for WELCOME_LATEST_DAYS if the job was
# down. A day-0 window of +/-1.5 days meant one outage lost it for good: an
# account created 2026-10-27, mid-outage, never got one. Past this it would
# arrive as a "welcome" to someone who has been here a while, so it is dropped.
WELCOME_LATEST_DAYS = 3


def welcome_due(age_days: float) -> bool:
    return 0 <= age_days <= WELCOME_LATEST_DAYS


# The founder note goes a day after signup -- the first hourly run past 24 h,
# so about the time of day they were last on the site -- and is owed for the
# same reason the welcome is: an outage must not lose it. Past this the
# account is a backfill (founder_backfill), which words it differently.
FOUNDER_NOTE_LATEST_DAYS = 4


def founder_note_due(age_days: float) -> bool:
    return 1 <= age_days <= FOUNDER_NOTE_LATEST_DAYS


#: Derek's own and test accounts. The founder note asks a stranger what they
#: came here to do; it does not go to the person who wrote it.
INTERNAL_DOMAINS = ("sidequestgroup.com", "form4.app")
INTERNAL_LOCAL_PARTS = ("derek.gorthy",)


def is_internal(email: str) -> bool:
    local, _, domain = (email or "").lower().partition("@")
    return domain in INTERNAL_DOMAINS or local.split("+")[0] in INTERNAL_LOCAL_PARTS


def is_due(email_name: str, target_day: int, age_days: float) -> bool:
    if email_name == "welcome":
        return welcome_due(age_days)
    if email_name == "founder_note":
        return founder_note_due(age_days)
    return in_window(age_days, target_day)


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

def _unsubscribe_url(user_id: str) -> str:
    # Param must be `user_id` — that's the name the endpoint declares
    # (api/routers/notifications.py). `user` returns 422.
    return (f"{APP_URL}/api/v1/notifications/unsubscribe"
            f"?user_id={user_id}&token={generate_unsubscribe_token(user_id)}")


def _build_email(email_name: str, user_data: dict, conn) -> tuple[str, str] | None:
    """(subject, body). The body is html for every email except the founder
    note, which is plain text (see _send)."""
    if email_name == "founder_note":
        return founder_note_email(first_name=user_data.get("first_name") or "")

    user_id = user_data.get("id", "")
    unsub_url = _unsubscribe_url(user_id)

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


def _send(email_name: str, to: str, subject: str, body: str, user_id: str) -> bool:
    if email_name == "founder_note":
        # Plain text from Derek, from and replied to support@ so the answer
        # lands where pipelines/support_replies.py reads it. The one-click
        # unsubscribe header is kept: it is a header, not a footer, and it is
        # how anyone opts out of this sequence.
        return send_email(to, subject, html=None, text=body,
                          from_address=f"{FOUNDER_NOTE_SENDER} <{SUPPORT_ADDRESS}>",
                          reply_to=SUPPORT_ADDRESS,
                          unsubscribe_url=_unsubscribe_url(user_id), campaign=email_name)
    return send_email(to, subject, body,
                      unsubscribe_url=_unsubscribe_url(user_id), campaign=email_name)


def process_user(user_data: dict, conn, dry_run: bool = False, now: datetime | None = None) -> int:
    """Send the first email due for one account. Returns the number sent (0 or 1).

    ONE PER RUN. A welcome missed during an outage and the founder note can
    both be owed at day 2.5, and two emails in the same minute read as a
    machine. The next hourly run sends the other; every window is days wide.
    A failed send stops the account for this run too, so the sequence never
    goes out of order."""
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

    for email_name, target_day in EMAIL_SEQUENCE:
        if not is_due(email_name, target_day, age_days):
            continue
        if email_name == "win_back" and not win_back_due(user_data.get("last_sign_in_at"), now):
            continue
        if email_name == "founder_note" and is_internal(email):
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
        subject, body = result

        if dry_run:
            logger.info("[DRY RUN] Would send '%s' to %s (day %.1f)", email_name, email, age_days)
            return 1
        if not _send(email_name, email, subject, body, user_id):
            logger.error("Failed to send '%s' to %s", email_name, email)
            return 0
        conn.execute(
            "INSERT OR IGNORE INTO sent_trial_emails (user_id, email_name) VALUES (?, ?)",
            (user_id, email_name),
        )
        conn.commit()
        logger.info("Sent '%s' to %s (day %.1f)", email_name, email, age_days)
        return 1
    return 0


def founder_backfill(users: list[dict], conn, since: datetime, dry_run: bool = True,
                     now: datetime | None = None, exclude: tuple[str, ...] = ()) -> list[str]:
    """The founder note, worded for accounts that signed up before it existed.

    Accounts created on or after `since` whose day-1 window has passed (the
    sequence handles younger ones); never a paying, internal or already-asked
    account, and never an address in `exclude` (someone who filed a
    chargeback is not asked for feedback). Dry run by default: it prints the
    list, and Derek approves the list before anything goes. Returns the
    addresses sent to (or that would be, on a dry run)."""
    now = now or datetime.utcnow()
    skip = {e.strip().lower() for e in exclude if e.strip()}
    chosen: list[str] = []
    for user_data in users:
        user_id, created_at = user_data.get("id", ""), user_data.get("created_at")
        if not user_id or not created_at:
            continue
        created = datetime.utcfromtimestamp(created_at / 1000 if created_at > 1e12 else created_at)
        if created < since or (now - created).total_seconds() / 86400 <= FOUNDER_NOTE_LATEST_DAYS:
            continue
        if is_paying(user_data.get("public_metadata") or {}):
            continue
        email = _get_user_email(user_data)
        if not email or is_internal(email) or email.lower() in skip:
            continue
        if conn.execute(
            "SELECT 1 FROM sent_trial_emails WHERE user_id = ? AND email_name = ?",
            (user_id, "founder_note"),
        ).fetchone():
            continue
        subject, body = founder_note_email(first_name=user_data.get("first_name") or "", signed_up=created)
        if dry_run:
            logger.info("[DRY RUN] Would send the founder note (backfill) to %s, signed up %s",
                        email, created.date())
        else:
            if not _send("founder_note", email, subject, body, user_id):
                logger.error("Failed to send the founder note (backfill) to %s", email)
                continue
            conn.execute(
                "INSERT OR IGNORE INTO sent_trial_emails (user_id, email_name) VALUES (?, ?)",
                (user_id, "founder_note"),
            )
            conn.commit()
            logger.info("Sent the founder note (backfill) to %s", email)
        chosen.append(email)
    return chosen


# ───────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Account email sequence runner")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--founder-backfill", action="store_true",
                        help="Send ONLY the founder note, to older accounts (see founder_backfill)")
    parser.add_argument("--since", help="With --founder-backfill: accounts created on/after YYYY-MM-DD")
    parser.add_argument("--exclude", default="",
                        help="With --founder-backfill: comma-separated addresses never to send to")
    args = parser.parse_args()
    if args.founder_backfill and not args.since:
        parser.error("--founder-backfill needs --since YYYY-MM-DD")

    conn = get_connection(readonly=False)
    _ensure_schema(conn)

    users = _fetch_all_clerk_users()
    if not users:
        logger.warning("No users fetched — check CLERK_SECRET_KEY")
        conn.close()
        return

    if args.founder_backfill:
        chosen = founder_backfill(users, conn, datetime.strptime(args.since, "%Y-%m-%d"),
                                  dry_run=args.dry_run, exclude=tuple(args.exclude.split(",")))
        conn.close()
        logger.info("%sFounder note backfill: %d account(s).",
                    "[DRY RUN] " if args.dry_run else "", len(chosen))
        return

    total_sent = 0
    for user_data in users:
        total_sent += process_user(user_data, conn, dry_run=args.dry_run)

    conn.close()
    prefix = "[DRY RUN] " if args.dry_run else ""
    logger.info("%sDone. %d email(s) sent across %d users.", prefix, total_sent, len(users))


if __name__ == "__main__":
    main()
