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
# public_fields imports nothing — the scanner runs on Studio's host
# Python, which has no fastapi, so importing api.gating here would
# take the whole job down.
from api.public_fields import ACTIVE_STRATEGIES, PRO_ALERT_EVENTS, strategy_label
from api.filters import MEANINGFUL_CLASSES
from api.notifications_db import get_connection as get_notif_connection
from api.notifications_db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
from pipelines.alert_filters import any_filter_matches  # noqa: E402

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
        if resp.status_code == 404:
            # Definitively absent, not unreachable. Distinguishing the two
            # matters: an outage should not silence a paying subscriber, but a
            # deleted account should not be treated as Pro forever either.
            _TIER_CACHE[user_id] = _TIER_GONE
            return None
        if resp.status_code == 200:
            data = resp.json()
            _TIER_CACHE[user_id] = (data.get("public_metadata") or {}).get("tier", "free")
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


def _as_date_str(value) -> str:
    """Date part of a watermark, for tables whose columns are still text dates.

    The watermark is a timestamp so the filing-driven scanners can advance
    intraday. strategy_portfolio and congress_trades have no timestamp column,
    and their events are daily anyway — a simulated position opens a handful of
    times a day, politicians file periodic reports — so those scanners compare
    on the date part and lose nothing.
    """
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def _default_watermark(latest):
    """One day back, when a scanner has no watermark yet.

    `latest` is a timestamp for the filing-driven scanners and a date string
    for the daily ones, so this cannot assume either type. Over-scanning
    replays events that dedup_key already suppresses; under-scanning drops
    them permanently, so a day back is the safe direction to err.
    """
    if hasattr(latest, "strftime"):
        return latest - timedelta(days=1)
    return (datetime.strptime(str(latest)[:10], "%Y-%m-%d")
            - timedelta(days=1)).strftime("%Y-%m-%d")


def _get_watermark(nconn: ConnectionWrapper, event_type: str) -> str | None:
    row = nconn.execute(
        "SELECT last_processed_at FROM scan_watermarks WHERE event_type = ?",
        (event_type,),
    ).fetchone()
    return row["last_processed_at"] if row else None


def _set_watermark(nconn: ConnectionWrapper, event_type: str, date: str) -> None:
    nconn.execute(
        # Both columns through the transition: last_processed_at is what the
        # scanners now read, last_processed_date is kept in step so reverting
        # to the previous scanner does not replay a day of events.
        "INSERT INTO scan_watermarks (event_type, last_processed_at, last_processed_date) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT (event_type) DO UPDATE SET "
        "  last_processed_at = excluded.last_processed_at, "
        "  last_processed_date = excluded.last_processed_date",
        (event_type, date, _as_date_str(date)),
    )


def _get_user_filters(nconn: ConnectionWrapper, user_id: str, event_type: str) -> list[dict]:
    """A user's enabled filters for this event type, conditions attached.

    event_type IS NULL on a filter means "every event type", so a user can
    write one rule that applies everywhere rather than repeating it per type.
    """
    filters = nconn.execute(
        """SELECT filter_id, name FROM alert_filters
            WHERE user_id = ? AND enabled
              AND (event_type IS NULL OR event_type = ?)""",
        (user_id, event_type),
    ).fetchall()
    out = []
    for f in filters:
        conds = nconn.execute(
            "SELECT field, op, value FROM alert_filter_conditions WHERE filter_id = ?",
            (f["filter_id"],),
        ).fetchall()
        out.append({"filter_id": f["filter_id"], "name": f["name"],
                    "conditions": [dict(c) for c in conds]})
    return out


#: tier per user, populated as a side effect of the email lookup we already do.
_TIER_CACHE: dict[str, str] = {}

#: The account no longer exists in Clerk. Six preference rows on Studio point
#: at deleted users and have been generating notifications for nobody.
_TIER_GONE = "__deleted__"


def _is_pro(user_id: str) -> bool:
    """Pro, Pro+ or an active trial. Reads the tier the email fetch cached.

    Fails OPEN when the tier cannot be determined. If Clerk is unreachable or
    the key is unset, the cache stays empty, and treating that as "free" would
    silence a paying subscriber's alerts because of an outage on our side. The
    opposite mistake — a free account receiving a cluster alert during a Clerk
    outage — costs nothing and ends when the outage does.
    """
    if user_id not in _TIER_CACHE:
        _get_user_email(user_id)
    if user_id not in _TIER_CACHE:
        logger.warning("tier unknown for %s — treating as Pro for this cycle", user_id)
        return True
    return _TIER_CACHE[user_id] in ("pro", "pro_plus", "trial")


def _account_exists(user_id: str) -> bool:
    """False once Clerk has told us the account is gone."""
    if user_id not in _TIER_CACHE:
        _get_user_email(user_id)
    return _TIER_CACHE.get(user_id) != _TIER_GONE


def _get_subscribed_users(nconn: ConnectionWrapper, event_type: str) -> list[dict]:
    """Users with this event type enabled, each with their filters attached.

    Enforces the alert line at SEND time as well as at write time. Write-time
    checks alone leave the lapsed-subscriber case wrong: someone who set up
    cluster alerts while paying would keep receiving them for free forever,
    because their preferences row does not know their subscription ended.

    Two rules, both from api.gating:

      - a Pro-only event type is skipped entirely for a non-Pro account
      - min_insider_tier is ignored for them, rather than applied

    The second is the one that is easy to get backwards. Leaving the stored
    default of 2 in place would silently filter a free user's alerts by our own
    grade — so someone following NVDA would never hear about a filing by an
    insider we happen to grade below tier 2, on a company they explicitly asked
    to be told about. Free means the raw event, which means no quality filter at
    all rather than one they did not choose.
    """
    rows = nconn.execute(
        f"""SELECT user_id, email_enabled, email_frequency, min_trade_value, min_insider_tier
            FROM notification_preferences
            WHERE {event_type} = 1 AND (email_enabled = 1 OR in_app_enabled = 1)""",
    ).fetchall()
    users = []
    for r in rows:
        u = dict(r)
        if not _account_exists(u["user_id"]):
            continue
        pro = _is_pro(u["user_id"])
        if event_type in PRO_ALERT_EVENTS and not pro:
            continue
        if not pro:
            u["min_insider_tier"] = None
        u["filters"] = _get_user_filters(nconn, u["user_id"], event_type) if pro else []
        users.append(u)
    return users


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
        _default_watermark(latest)
    )

    rows = iconn.execute(
        """SELECT MIN(t.trade_id) AS trade_id,
                  t.insider_id, t.ticker, MAX(t.company) AS company,
                  MAX(COALESCE(i.display_name, i.name)) AS insider_name,
                  MAX(t.title) AS title, t.trade_type, t.trade_date,
                  MAX(t.filing_date) AS filing_date,
                  SUM(t.value) AS total_value,
                  MAX(t.pit_grade) AS pit_grade
           FROM trades t
           JOIN insiders i ON t.insider_id = i.insider_id
           WHERE t.ingested_at > ? AND t.ingested_at <= ?
             AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
             AND t.pit_grade IN ('A+', 'A', 'B')
           GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date
           ORDER BY total_value DESC""",
        (watermark, latest),
    ).fetchall()

    # Legacy tier mapping, kept ONLY for users who have not defined filters.
    # It collapses A+ and A into tier 3, so "only A+" is inexpressible — which
    # is why alert_filters compares grades directly instead.
    _GRADE_TO_TIER = {"A+": 3, "A": 3, "B": 2}

    count = 0
    users = _get_subscribed_users(nconn, "high_value_filing")

    for row in rows:
        r = dict(row)
        r_tier = _GRADE_TO_TIER.get(r.get("pit_grade"), 1)
        for user in users:
            # A user with filters is governed by them alone; the legacy
            # thresholds are the fallback, not an additional gate on top.
            # A user with filters is governed by them ALONE. The legacy
            # threshold + tier pair is the fallback for users who have not
            # defined any, not an extra gate stacked on top — otherwise a
            # filter for "A+ only" would still be silently narrowed by
            # whatever min_insider_tier happened to be set to.
            if user.get("filters"):
                if not any_filter_matches(r, user["filters"]):
                    continue
            else:
                if r["total_value"] < user["min_trade_value"]:
                    continue
                if r_tier < user["min_insider_tier"]:
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
        _default_watermark(latest)
    )

    rows = iconn.execute(
        """SELECT t.ticker, t.trade_type, MAX(t.company) AS company,
                  COUNT(DISTINCT COALESCE(t.effective_insider_id, t.insider_id)) AS insider_count,
                  SUM(t.value) AS total_value,
                  MAX(t.filing_date) AS latest_filing
           FROM trades t
           WHERE t.ingested_at > ? AND t.ingested_at <= ?
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
    # Text date columns on this side, and the underlying events are daily,
    # so compare on the date part. latest_ts is kept so the watermark
    # written back stays a timestamp for every event type.
    latest_ts = latest
    latest = _as_date_str(latest)

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
    # Text date columns on this side, and the underlying events are daily,
    # so compare on the date part. latest_ts is kept so the watermark
    # written back stays a timestamp for every event type.
    latest_ts = latest
    latest = _as_date_str(latest)

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


def should_notify_watchlist(signal_class: str | None,
                           user_wants_all: bool) -> bool:
    """Does this filing clear the watchlist default for this user?

    THE DEFAULT IS MEANINGFUL FILINGS ONLY. 71.6% of Form 4s are mechanical:
    10b5-1 plans set up months ago, compensation grants, tax withheld on
    vesting, option exercises. Someone watching a ticker wants to know when an
    insider made a DECISION about it, not when payroll ran — unfiltered, an
    active ticker produces roughly three mechanical filings per real one.

    `signal_class` is the only correct input. The boolean columns do not work:
    `is_tax_sale` is set on 2,025 rows against 470,417 filings classified as
    tax withholding, and 184,121 compensation grants plus 220,692 option
    exercises carry `trade_type = 'buy'`, so anything reading "buy" as
    "bought" reports shares an insider was handed as a purchase.

    Unclassifiable filings are DELIVERED. Failing open costs one extra alert;
    failing closed silently drops a filing a user explicitly asked to see.
    """
    if user_wants_all:
        return True
    if signal_class is None or str(signal_class).strip() == "":
        return True          # fail open — see docstring
    return signal_class in MEANINGFUL_CLASSES


def scan_watchlist_activity(iconn: ConnectionWrapper, nconn: ConnectionWrapper, latest: str) -> int:
    """Notify users about any new filings on their watched tickers."""
    watermark = _get_watermark(nconn, "watchlist_activity") or (
        _default_watermark(latest)
    )

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
                   SUM(t.value) AS total_value,
                   t.signal_class
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            WHERE t.ingested_at > ? AND t.ingested_at <= ?
              AND t.ticker IN ({placeholders})
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
            GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date,
                     t.signal_class
            ORDER BY MAX(t.filing_date) DESC""",
        [watermark, latest] + list(all_tickers),
    ).fetchall()

    # THE DEFAULT IS MEANINGFUL FILINGS ONLY.
    #
    # 71.6% of Form 4s are mechanical: 10b5-1 plans set up months ago,
    # compensation grants, tax withholding on vesting, option exercises. A user
    # who watches a ticker wants to know when someone made a DECISION about it,
    # not when payroll ran. Watching one active ticker unfiltered is roughly
    # three alerts for every one worth reading.
    #
    # `signal_class` is the only correct source for this. The boolean columns
    # do not work: is_tax_sale is set on 2,025 rows against 470,417 filings
    # classified as tax withholding, and 184k compensation grants plus 221k
    # option exercises are stored with trade_type = 'buy', so anything reading
    # "buy" as "bought" reports shares an insider was handed as a purchase.
    #
    # Opt out per user with notification_preferences.watchlist_all_filings.
    unfiltered_users = {
        r["user_id"] for r in nconn.execute(
            "SELECT user_id FROM notification_preferences "
            "WHERE watchlist_all_filings = 1"
        ).fetchall()
    }

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
            if not should_notify_watchlist(
                    r.get("signal_class"), user_id in unfiltered_users):
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
    """Detect new entries and exits in the three published strategies.

    This scanned `strategy = 'form4_insider'` until 2026-08-18 — a
    backtest-only book that was retired months earlier and whose last entry was
    2026-03-13. So the Pro "portfolio alerts" toggle, described to subscribers
    as alerts on our strategies, had never fired for a strategy we actually
    run. It now reads ACTIVE_STRATEGIES, which is the same list the product
    publishes.

    DEDUP MUST NOT USE THE ROW ID. simulate_strategy_portfolio rebuilds each
    book with `DELETE FROM strategy_portfolio WHERE strategy = ?` followed by a
    fresh INSERT, so `id` is a different number every night for the same trade.
    Keying on it would re-alert every open position on every rebuild — roughly
    150 notifications per subscriber per night. (strategy, ticker, date) is
    stable across rebuilds, and a strategy cannot enter the same ticker twice
    on the same day.

    Backtest rows are excluded for the same reason the retired book was: they
    are research output, not something that happened.
    """
    if not ACTIVE_STRATEGIES:
        return 0
    placeholders = ",".join("?" for _ in ACTIVE_STRATEGIES)
    live_sources = ("simulated", "paper", "live")
    source_ph = ",".join("?" for _ in live_sources)
    # Text date columns on this side, and the underlying events are daily,
    # so compare on the date part. latest_ts is kept so the watermark
    # written back stays a timestamp for every event type.
    latest_ts = latest
    latest = _as_date_str(latest)

    # Date string on both sides: entry_date/exit_date are text, and the stored
    # watermark is now a timestamp.
    watermark = _as_date_str(
        _get_watermark(nconn, "portfolio_alert") or _default_watermark(latest)
    )

    # Check for new entries (trades that started after watermark)
    new_entries = iconn.execute(
        f"""SELECT id, strategy, ticker, insider_name, entry_date, entry_price,
                   signal_quality, position_size
            FROM strategy_portfolio
            WHERE strategy IN ({placeholders})
              AND execution_source IN ({source_ph})
              AND entry_date > ? AND entry_date <= ?
            ORDER BY entry_date DESC""",
        (*ACTIVE_STRATEGIES, *live_sources, watermark, latest),
    ).fetchall()

    # Check for new exits (trades that closed after watermark)
    new_exits = iconn.execute(
        f"""SELECT id, strategy, ticker, insider_name, exit_date, exit_price,
                   entry_price, pnl_pct, pnl_dollar, exit_reason
            FROM strategy_portfolio
            WHERE strategy IN ({placeholders})
              AND execution_source IN ({source_ph})
              AND status = 'closed'
              AND exit_date > ? AND exit_date <= ?
            ORDER BY exit_date DESC""",
        (*ACTIVE_STRATEGIES, *live_sources, watermark, latest),
    ).fetchall()

    count = 0
    users = _get_subscribed_users(nconn, "portfolio_alert")

    for row in new_entries:
        r = dict(row)
        label = strategy_label(r["strategy"])
        insider = r["insider_name"] or "An insider"
        title = f"{label}: bought {r['ticker']} at ${r['entry_price']:.2f}"
        body = (
            f"{insider} — {r['ticker']} at ${r['entry_price']:.2f} on "
            f"{r['entry_date']}."
        )
        for user in users:
            dedup = _dedup_key("pfe", user["user_id"], r["strategy"],
                               r["ticker"], r["entry_date"])
            if _insert_notification(nconn, user["user_id"], "portfolio_alert", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    for row in new_exits:
        r = dict(row)
        pnl_pct = (r["pnl_pct"] or 0) * 100
        pnl_sign = "+" if pnl_pct >= 0 else ""
        # The values the simulator actually writes (simulate_strategy_portfolio
        # lines 409/433), not the ones this map used to guess at. The stop
        # threshold is deliberately not quoted here: it lives in the simulator,
        # and importing that module into a launchd job to read one constant is
        # how the scanner would die the next time the simulator grows a
        # dependency Studio's host Python does not have. Every one of
        # `time_exit`/`trailing_stop`/`stop_loss` was absent from the table, so
        # every exit alert rendered a raw code — "Time." — instead of a reason.
        # The `_stale` variants mean the exit was priced off a stale bar.
        reason_labels = {
            "time": "hold period complete",
            "time_stale": "hold period complete, priced off a stale bar",
            "stop": "stop hit",
            "stop_stale": "stop hit, priced off a stale bar",
        }
        reason = reason_labels.get(r["exit_reason"] or "", r["exit_reason"] or "closed")
        label = strategy_label(r["strategy"])
        title = f"{label}: sold {r['ticker']} {pnl_sign}{pnl_pct:.1f}%"
        body = (
            f"Closed {r['ticker']} at ${r['exit_price']:.2f}, "
            f"{pnl_sign}{pnl_pct:.1f}%. {reason.capitalize()}."
        )
        for user in users:
            dedup = _dedup_key("pfx", user["user_id"], r["strategy"],
                               r["ticker"], r["exit_date"])
            if _insert_notification(nconn, user["user_id"], "portfolio_alert", title, body, r["ticker"], dedup):
                count += 1
                _maybe_send_realtime_email(nconn, user, title, body)

    _set_watermark(nconn, "portfolio_alert", latest_ts)
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
        # NOW(), not MAX(filing_date): the old bound was a date, so a scan
        # could only advance a day at a time no matter how often it ran.
        latest_row = iconn.execute("SELECT NOW() AS d FROM trades LIMIT 1").fetchone()
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
