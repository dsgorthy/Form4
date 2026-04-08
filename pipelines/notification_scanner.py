#!/usr/bin/env python3
"""Notification scanner — detects events and creates notifications for subscribed Pro users.

Run every 15 minutes via launchd. Separate daily digest cron at 8 AM ET.

Usage:
    python3 pipelines/notification_scanner.py              # scan for new events
    python3 pipelines/notification_scanner.py --digest      # send daily digest emails
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_connection, ConnectionWrapper
from api.email import build_digest_email, build_notification_email, send_email
from api.notifications_db import get_connection as get_notif_connection
from api.notifications_db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Clerk user email lookup (for email dispatch)
# ---------------------------------------------------------------------------

CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")


def _get_user_email(user_id: str) -> str | None:
    """Fetch primary email from Clerk API."""
    if not CLERK_SECRET_KEY:
        return None
    try:
        import httpx

        resp = httpx.get(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            addrs = data.get("email_addresses", [])
            primary_id = data.get("primary_email_address_id")
            for addr in addrs:
                if addr.get("id") == primary_id:
                    return addr.get("email_address")
            if addrs:
                return addrs[0].get("email_address")
    except Exception as exc:
        logger.warning("Failed to fetch email for %s: %s", user_id, exc)
    return None


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _open_insiders_db() -> ConnectionWrapper:
    return get_connection(readonly=True)


def _open_notifications_db() -> ConnectionWrapper:
    return get_notif_connection()


def _get_watermark(nconn: ConnectionWrapper, event_type: str) -> str | None:
    row = nconn.execute(
        "SELECT last_processed_date FROM scan_watermarks WHERE event_type = ?",
        (event_type,),
    ).fetchone()
    return row["last_processed_date"] if row else None


def _set_watermark(nconn: ConnectionWrapper, event_type: str, date: str) -> None:
    nconn.execute(
        "INSERT OR REPLACE INTO scan_watermarks (event_type, last_processed_date) VALUES (?, ?)",
        (event_type, date),
    )


def _get_subscribed_users(nconn: ConnectionWrapper, event_type: str) -> list[dict]:
    """Get all users who have this event type enabled."""
    rows = nconn.execute(
        f"""SELECT user_id, email_enabled, email_frequency, min_trade_value, min_insider_tier
            FROM notification_preferences
            WHERE {event_type} = 1 AND (email_enabled = 1 OR in_app_enabled = 1)""",
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Rate limiting — priority tiers and caps
# ---------------------------------------------------------------------------

# Per-scan-cycle caps by event type (highest priority first)
CYCLE_CAPS: dict[str, int] = {
    "portfolio_alert": 10,         # P0 — portfolio entry/exit, highest priority
    "watchlist_activity": 20,      # P1 — user opted in, generous but bounded
    "high_value_filing": 10,       # P2 — high signal
    "congress_convergence": 5,     # P3 — rare, actionable
    "cluster_formation": 5,        # P4 — moderate signal
    "activity_spike": 3,           # P5 — noisiest
}

DAILY_CAP = 50  # Max notifications per user per day across all types

# In-memory counters reset each scan cycle
_cycle_counts: dict[tuple[str, str], int] = {}  # (user_id, event_type) -> count this cycle
_daily_counts: dict[str, int | None] = {}  # user_id -> count today (cached)


def _reset_cycle_counts() -> None:
    _cycle_counts.clear()
    _daily_counts.clear()


def _get_daily_count(nconn: ConnectionWrapper, user_id: str) -> int:
    """Count notifications created today for a user."""
    if user_id not in _daily_counts:
        row = nconn.execute(
            "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND created_at >= date('now')",
            (user_id,),
        ).fetchone()
        _daily_counts[user_id] = row["cnt"]
    return _daily_counts[user_id]


def _check_budget(nconn: ConnectionWrapper, user_id: str, event_type: str) -> bool:
    """Return True if this user can receive another notification of this type."""
    # Check daily cap
    daily = _get_daily_count(nconn, user_id)
    if daily >= DAILY_CAP:
        return False
    # Check per-cycle cap
    cycle_key = (user_id, event_type)
    cycle_count = _cycle_counts.get(cycle_key, 0)
    max_per_cycle = CYCLE_CAPS.get(event_type, 5)
    return cycle_count < max_per_cycle


def _record_sent(user_id: str, event_type: str) -> None:
    """Increment counters after a notification is inserted."""
    cycle_key = (user_id, event_type)
    _cycle_counts[cycle_key] = _cycle_counts.get(cycle_key, 0) + 1
    _daily_counts[user_id] = (_daily_counts.get(user_id) or 0) + 1


def _insert_notification(
    nconn: ConnectionWrapper,
    user_id: str,
    event_type: str,
    title: str,
    body: str,
    ticker: str | None,
    dedup_key: str,
) -> bool:
    """Insert notification if within budget, returns True if inserted."""
    if not _check_budget(nconn, user_id, event_type):
        return False
    try:
        cur = nconn.execute(
            """INSERT OR IGNORE INTO notifications
               (user_id, event_type, title, body, ticker, dedup_key)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, event_type, title, body, ticker, dedup_key),
        )
        inserted = cur.rowcount > 0
        if inserted:
            _record_sent(user_id, event_type)
        return inserted
    except Exception:
        return False


def _dedup_key(event_type: str, *parts: str) -> str:
    raw = f"{event_type}:{'|'.join(parts)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Detection functions
# ---------------------------------------------------------------------------


def scan_high_value_filings(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Detect Tier 2+ insider buys/sells above user's $ threshold."""
    watermark = _get_watermark(nconn, "high_value_filing") or (
        datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    rows = iconn.execute(
        """SELECT MIN(t.trade_id) AS trade_id,
                  t.insider_id, t.ticker, MAX(t.company) AS company,
                  MAX(COALESCE(i.display_name, i.name)) AS insider_name,
                  MAX(t.title) AS title, t.trade_type, t.trade_date,
                  MAX(t.filing_date) AS filing_date,
                  SUM(t.value) AS total_value,
                  MAX(itr.score_tier) AS score_tier
           FROM trades t
           JOIN insiders i ON t.insider_id = i.insider_id
           LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
           WHERE t.filing_date > ? AND t.filing_date <= ?
             AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
             AND itr.score_tier >= 2
           GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date
           ORDER BY total_value DESC""",
        (watermark, latest),
    ).fetchall()

    count = 0
    users = _get_subscribed_users(nconn, "high_value_filing")

    for row in rows:
        r = dict(row)
        for user in users:
            if r["total_value"] < user["min_trade_value"]:
                continue
            if r["score_tier"] < user["min_insider_tier"]:
                continue

            title_str = r["title"] or "Insider"
            action = "bought" if r["trade_type"] == "buy" else "sold"
            value_fmt = f"${r['total_value']:,.0f}"
            title = f"{r['ticker']}: {title_str} {action} {value_fmt}"
            body = f"{r['insider_name']} ({title_str}) at {r['company']} {action} {value_fmt} worth of {r['ticker']} on {r['trade_date']}"
            dedup = _dedup_key("hvf", user["user_id"], str(r["trade_id"]), r["trade_date"])

            if _insert_notification(nconn, user["user_id"], "high_value_filing", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    _set_watermark(nconn, "high_value_filing", latest)
    nconn.commit()
    return count


def scan_cluster_formations(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Detect 2+ insiders trading same ticker within 14-day window."""
    watermark = _get_watermark(nconn, "cluster_formation") or (
        datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    rows = iconn.execute(
        """SELECT t.ticker, t.trade_type, MAX(t.company) AS company,
                  COUNT(DISTINCT COALESCE(t.effective_insider_id, t.insider_id)) AS insider_count,
                  SUM(t.value) AS total_value,
                  MAX(t.filing_date) AS latest_filing
           FROM trades t
           WHERE t.filing_date > ? AND t.filing_date <= ?
             AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
           GROUP BY t.ticker, t.trade_type
           HAVING COUNT(DISTINCT COALESCE(t.effective_insider_id, t.insider_id)) >= 2""",
        (watermark, latest),
    ).fetchall()

    count = 0
    users = _get_subscribed_users(nconn, "cluster_formation")

    for row in rows:
        r = dict(row)
        action = "buying" if r["trade_type"] == "buy" else "selling"
        value_fmt = f"${r['total_value']:,.0f}"
        title = f"Cluster: {r['insider_count']} insiders {action} {r['ticker']}"
        body = f"{r['insider_count']} insiders {action} {r['ticker']} ({r['company']}) totaling {value_fmt}"
        for user in users:
            dedup = _dedup_key("clf", user["user_id"], r["ticker"], r["trade_type"], r["latest_filing"])
            if _insert_notification(nconn, user["user_id"], "cluster_formation", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    _set_watermark(nconn, "cluster_formation", latest)
    nconn.commit()
    return count


def scan_activity_spikes(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Detect tickers with activity 2x+ above 90-day baseline.
    Only considers open-market trades (P/S) and excludes routine/10b5-1 sells."""
    # Recent 7 days — open-market only, exclude routine
    recent = iconn.execute(
        """SELECT ticker, trade_type, MAX(company) AS company,
                  SUM(value) AS recent_value,
                  COUNT(DISTINCT insider_id) AS recent_insiders,
                  MAX(filing_date) AS latest_filing
           FROM trades
           WHERE filing_date BETWEEN date(?, '-7 days') AND ?
             AND trans_code IN ('P', 'S')
             AND (is_duplicate = 0 OR is_duplicate IS NULL)
             AND (is_routine != 1 OR is_routine IS NULL)
           GROUP BY ticker, trade_type""",
        (latest, latest),
    ).fetchall()

    # Baseline (90 days, excluding recent 7) — same filters
    baseline = {}
    for row in iconn.execute(
        """SELECT ticker, trade_type, SUM(value) / 90.0 AS daily_avg
           FROM trades
           WHERE filing_date BETWEEN date(?, '-90 days') AND date(?, '-8 days')
             AND trans_code IN ('P', 'S')
             AND (is_duplicate = 0 OR is_duplicate IS NULL)
             AND (is_routine != 1 OR is_routine IS NULL)
           GROUP BY ticker, trade_type""",
        (latest, latest),
    ).fetchall():
        baseline[(row["ticker"], row["trade_type"])] = row["daily_avg"]

    count = 0
    users = _get_subscribed_users(nconn, "activity_spike")

    for row in recent:
        r = dict(row)
        key = (r["ticker"], r["trade_type"])
        daily_avg = baseline.get(key, 0)
        weekly_baseline = daily_avg * 7
        if weekly_baseline <= 0:
            continue
        ratio = r["recent_value"] / weekly_baseline
        if ratio < 5.0:
            continue

        action = "buy" if r["trade_type"] == "buy" else "sell"
        title = f"Activity Spike: {r['ticker']} {action} at {ratio:.1f}x baseline"
        body = f"{r['ticker']} ({r['company']}) {action} activity is {ratio:.1f}x above its 90-day average with {r['recent_insiders']} insiders active"

        for user in users:
            dedup = _dedup_key("asp", user["user_id"], r["ticker"], r["trade_type"], r["latest_filing"])
            if _insert_notification(nconn, user["user_id"], "activity_spike", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    nconn.commit()
    return count


def scan_congress_convergence(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Detect tickers where insiders and politicians both bought recently."""
    # Check if congress_trades table exists
    table_check = iconn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'congress_trades'"
    ).fetchone()
    if not table_check:
        return 0

    rows = iconn.execute(
        """SELECT ins.ticker, ins.company,
                  ins.insider_buys, ins.insider_total_value,
                  pol.politician_buys, pol.politician_total_value_estimate,
                  LEAST(ins.first_date, pol.first_date) AS first_date,
                  GREATEST(ins.last_date, pol.last_date) AS last_date
           FROM (
               SELECT ticker, MAX(company) AS company,
                      COUNT(*) AS insider_buys,
                      SUM(value) AS insider_total_value,
                      MIN(trade_date) AS first_date,
                      MAX(trade_date) AS last_date
               FROM trades
               WHERE trade_type = 'buy'
                 AND trade_date >= date(?, '-30 days')
                 AND trade_date <= ?
                 AND (is_duplicate = 0 OR is_duplicate IS NULL)
               GROUP BY ticker
           ) ins
           INNER JOIN (
               SELECT ticker,
                      COUNT(*) AS politician_buys,
                      COALESCE(SUM(value_estimate), 0) AS politician_total_value_estimate,
                      MIN(trade_date) AS first_date,
                      MAX(trade_date) AS last_date
               FROM congress_trades
               WHERE trade_type = 'buy'
                 AND trade_date >= date(?, '-30 days')
               GROUP BY ticker
           ) pol ON ins.ticker = pol.ticker
           ORDER BY ins.insider_total_value DESC
           LIMIT 20""",
        (latest, latest, latest),
    ).fetchall()

    count = 0
    users = _get_subscribed_users(nconn, "congress_convergence")

    for row in rows:
        r = dict(row)
        title = f"Convergence: {r['ticker']} insiders + politicians buying"
        body = f"{r['ticker']} ({r['company']}): {r['insider_buys']} insider buys + {r['politician_buys']} politician buys in last 30 days"

        for user in users:
            week = datetime.strptime(latest, "%Y-%m-%d").isocalendar()[1]
            dedup = _dedup_key("ccv", user["user_id"], r["ticker"], str(week))
            if _insert_notification(nconn, user["user_id"], "congress_convergence", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    nconn.commit()
    return count


def scan_watchlist_activity(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Notify users about any new filings on their watched tickers."""
    watermark = _get_watermark(nconn, "watchlist_activity") or (
        datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # Get all watched tickers across all users
    all_watchlist = nconn.execute(
        """SELECT w.user_id, w.ticker
           FROM watchlist w
           JOIN notification_preferences np ON w.user_id = np.user_id
           WHERE np.watchlist_activity = 1""",
    ).fetchall()

    if not all_watchlist:
        _set_watermark(nconn, "watchlist_activity", latest)
        nconn.commit()
        return 0

    # Build user->tickers map and unique tickers set
    user_tickers: dict[str, set[str]] = {}
    all_tickers: set[str] = set()
    for row in all_watchlist:
        user_tickers.setdefault(row["user_id"], set()).add(row["ticker"])
        all_tickers.add(row["ticker"])

    # Query new filings for watched tickers
    placeholders = ",".join("?" for _ in all_tickers)
    new_filings = iconn.execute(
        f"""SELECT MIN(t.trade_id) AS trade_id,
                   t.insider_id, t.ticker, MAX(t.company) AS company,
                   MAX(COALESCE(i.display_name, i.name)) AS insider_name,
                   MAX(t.title) AS title, t.trade_type, t.trade_date,
                   MAX(t.filing_date) AS filing_date,
                   SUM(t.value) AS total_value
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            WHERE t.filing_date > ? AND t.filing_date <= ?
              AND t.ticker IN ({placeholders})
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
            GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date
            ORDER BY MAX(t.filing_date) DESC""",
        [watermark, latest] + list(all_tickers),
    ).fetchall()

    count = 0
    for row in new_filings:
        r = dict(row)
        action = "bought" if r["trade_type"] == "buy" else "sold"
        value_fmt = f"${r['total_value']:,.0f}"
        title_str = r["title"] or "Insider"
        title = f"Watchlist: {r['ticker']} — {title_str} {action} {value_fmt}"
        body = f"New filing on {r['ticker']}: {r['insider_name']} ({title_str}) {action} {value_fmt}"

        for user_id, tickers in user_tickers.items():
            if r["ticker"] not in tickers:
                continue
            dedup = _dedup_key("wla", user_id, str(r["trade_id"]), r["trade_date"])
            if _insert_notification(nconn, user_id, "watchlist_activity", title, body, r["ticker"], dedup):
                count += 1
                # Get user prefs for email
                user_pref = nconn.execute(
                    "SELECT email_enabled, email_frequency FROM notification_preferences WHERE user_id = ?",
                    (user_id,),
                ).fetchone()
                if user_pref:
                    _maybe_send_realtime_email(nconn, dict(user_pref) | {"user_id": user_id}, title, body)

    _set_watermark(nconn, "watchlist_activity", latest)
    nconn.commit()
    return count


# ---------------------------------------------------------------------------
# Email dispatch
# ---------------------------------------------------------------------------

_email_cache: dict[str, str | None] = {}


def _maybe_send_realtime_email(nconn: ConnectionWrapper, user: dict, title: str, body: str) -> None:
    """Send email immediately if user has realtime frequency enabled."""
    if not user.get("email_enabled"):
        return
    if user.get("email_frequency") != "realtime":
        return

    user_id = user["user_id"]
    if user_id not in _email_cache:
        _email_cache[user_id] = _get_user_email(user_id)

    email = _email_cache.get(user_id)
    if not email:
        return

    html = build_notification_email(title, body)
    send_email(email, f"Form4: {title}", html)


def scan_portfolio_alerts(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Detect new entries/exits in the Form4 Insider Portfolio strategy."""
    watermark = _get_watermark(nconn, "portfolio_alert") or (
        datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # Check for new entries (trades that started after watermark)
    new_entries = iconn.execute(
        """SELECT id, ticker, insider_name, entry_date, entry_price,
                  signal_quality, position_size
           FROM strategy_portfolio
           WHERE strategy = 'form4_insider'
             AND entry_date > ? AND entry_date <= ?
           ORDER BY entry_date DESC""",
        (watermark, latest),
    ).fetchall()

    # Check for new exits (trades that closed after watermark)
    new_exits = iconn.execute(
        """SELECT id, ticker, insider_name, exit_date, exit_price,
                  entry_price, pnl_pct, pnl_dollar, exit_reason
           FROM strategy_portfolio
           WHERE strategy = 'form4_insider'
             AND status = 'closed'
             AND exit_date > ? AND exit_date <= ?
           ORDER BY exit_date DESC""",
        (watermark, latest),
    ).fetchall()

    count = 0
    users = _get_subscribed_users(nconn, "portfolio_alert")

    for row in new_entries:
        r = dict(row)
        quality = r["signal_quality"] or "?"
        title = f"Portfolio Entry: {r['ticker']} at ${r['entry_price']:.2f}"
        body = (
            f"Form4 Portfolio entered {r['ticker']} at ${r['entry_price']:.2f} "
            f"on {r['entry_date']}. Signal quality: {quality}. "
            f"Insider: {r['insider_name'] or 'Unknown'}."
        )
        for user in users:
            dedup = _dedup_key("pfe", user["user_id"], str(r["id"]), r["entry_date"])
            if _insert_notification(nconn, user["user_id"], "portfolio_alert", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    for row in new_exits:
        r = dict(row)
        pnl_pct = (r["pnl_pct"] or 0) * 100
        pnl_sign = "+" if pnl_pct >= 0 else ""
        reason_labels = {
            "time_exit": "30-day hold complete",
            "trailing_stop": "trailing stop hit",
            "stop_loss": "hard stop hit",
        }
        reason = reason_labels.get(r["exit_reason"] or "", r["exit_reason"] or "closed")
        title = f"Portfolio Exit: {r['ticker']} {pnl_sign}{pnl_pct:.1f}%"
        body = (
            f"Form4 Portfolio exited {r['ticker']} at ${r['exit_price']:.2f} "
            f"({pnl_sign}{pnl_pct:.1f}%). Reason: {reason}."
        )
        for user in users:
            dedup = _dedup_key("pfx", user["user_id"], str(r["id"]), r["exit_date"])
            if _insert_notification(nconn, user["user_id"], "portfolio_alert", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    _set_watermark(nconn, "portfolio_alert", latest)
    nconn.commit()
    return count


def send_daily_digests(nconn: ConnectionWrapper) -> int:
    """Send daily digest emails for users with unread notifications and daily frequency."""
    users = nconn.execute(
        """SELECT DISTINCT np.user_id
           FROM notification_preferences np
           WHERE np.email_enabled = 1 AND np.email_frequency = 'daily'""",
    ).fetchall()

    sent = 0
    for row in users:
        user_id = row["user_id"]
        notifications = nconn.execute(
            """SELECT title, body, event_type, ticker, created_at
               FROM notifications
               WHERE user_id = ? AND emailed = 0
               ORDER BY created_at DESC
               LIMIT 50""",
            (user_id,),
        ).fetchall()

        if not notifications:
            continue

        email = _get_user_email(user_id)
        if not email:
            continue

        items = [dict(n) for n in notifications]
        html = build_digest_email(items)
        subject = f"Form4 Daily Digest — {len(items)} new alert{'s' if len(items) != 1 else ''}"

        if send_email(email, subject, html):
            nconn.execute(
                "UPDATE notifications SET emailed = 1 WHERE user_id = ? AND emailed = 0",
                (user_id,),
            )
            nconn.commit()
            sent += 1

    return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Form4 notification scanner")
    parser.add_argument("--digest", action="store_true", help="Send daily digest emails")
    args = parser.parse_args()

    init_db()

    if args.digest:
        nconn = _open_notifications_db()
        try:
            sent = send_daily_digests(nconn)
            logger.info("Daily digest: sent %d emails", sent)
        finally:
            nconn.close()
        return

    # Normal scan
    iconn = _open_insiders_db()
    nconn = _open_notifications_db()

    try:
        latest_row = iconn.execute("SELECT MAX(filing_date) AS d FROM trades").fetchone()
        latest = latest_row["d"]
        if not latest:
            logger.info("No trades in database, nothing to scan")
            return

        logger.info("Scanning events up to %s", latest)

        _reset_cycle_counts()

        # Process in priority order: P0 (portfolio) → P5 (spikes)
        # Higher-priority events fill the daily budget first
        results = {}
        results["portfolio_alert"] = scan_portfolio_alerts(iconn, nconn, latest)
        results["watchlist_activity"] = scan_watchlist_activity(iconn, nconn, latest)
        results["high_value_filing"] = scan_high_value_filings(iconn, nconn, latest)
        results["congress_convergence"] = scan_congress_convergence(iconn, nconn, latest)
        results["cluster_formation"] = scan_cluster_formations(iconn, nconn, latest)
        results["activity_spike"] = scan_activity_spikes(iconn, nconn, latest)

        total = sum(results.values())
        logger.info("Scan complete: %d new notifications", total)
        for event_type, count in results.items():
            if count > 0:
                logger.info("  %s: %d", event_type, count)

    finally:
        iconn.close()
        nconn.close()


if __name__ == "__main__":
    main()
