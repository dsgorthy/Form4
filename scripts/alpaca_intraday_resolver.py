#!/usr/bin/env python3
"""Intraday Alpaca order resolver — backstop for the WebSocket listener.

The primary mechanism for fill confirmation is the WebSocket stream
(framework/oms/alpaca_stream_listener.py). This script is the safety net:
every 5 min during market hours, look at any order_audit rows whose
fill_status is still pending/timeout/unknown, query Alpaca directly for
their terminal state, and resolve them.

Catches:
  - Orders the WebSocket missed during a reconnect window
  - Orders submitted while the listener was down for maintenance
  - Orders whose state changed via the Alpaca dashboard or another channel
  - The cw_runner timeout class of false-positives (filled but never
    confirmed within the 300s polling window)

Scope: only resolves order_audit rows. Doesn't do full position reconciliation
(that's the daily 13:30 PT alpaca_reconcile.py — different concern).

Usage:
    python3 scripts/alpaca_intraday_resolver.py
    python3 scripts/alpaca_intraday_resolver.py --strategy quality_momentum
    python3 scripts/alpaca_intraday_resolver.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection
from framework.execution.paper import PaperBackend, PAPER_API_BASE, LIVE_API_BASE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


ACCOUNTS = [
    {"name": "quality_momentum", "env_prefix": "QUALITY_MOMENTUM", "live": False},
    {"name": "reversal_dip",     "env_prefix": "REVERSAL_DIP",     "live": False},
    {"name": "tenb51_surprise",  "env_prefix": "TENB51_SURPRISE",  "live": False},
]

# Statuses we want to resolve. Anything in 'filled', 'rejected', 'canceled'
# is already terminal. We look for everything else within a recent window.
NON_TERMINAL_STATUSES = ("pending", "timeout", "accepted", "new",
                         "partially_filled", None, "")
LOOKBACK_HOURS = 24


def is_market_open() -> bool:
    from zoneinfo import ZoneInfo
    pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    if pt.weekday() >= 5:
        return False
    open_t = pt.replace(hour=6, minute=30, second=0, microsecond=0)
    close_t = pt.replace(hour=13, minute=0, second=0, microsecond=0)
    return open_t <= pt <= close_t


def get_alpaca(account: dict) -> PaperBackend:
    key = os.environ[f"ALPACA_API_KEY_{account['env_prefix']}"]
    secret = os.environ[f"ALPACA_API_SECRET_{account['env_prefix']}"]
    base = LIVE_API_BASE if account["live"] else PAPER_API_BASE
    return PaperBackend(api_key=key, api_secret=secret, base_url=base)


def fetch_pending_orders(conn, strategy: str, hours: int = LOOKBACK_HOURS) -> list[dict]:
    """All order_audit rows for this strategy with non-terminal fill_status,
    from the last `hours` hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = conn.execute(
        """SELECT order_id, alpaca_order_id, ticker, side, qty, fill_status,
                  submitted_at::text, decided_at::text
           FROM order_audit
           WHERE strategy = ?
             AND submitted_at >= ?
             AND alpaca_order_id IS NOT NULL
             AND (fill_status IN ('pending', 'timeout', 'accepted', 'new', 'partially_filled')
                  OR fill_status IS NULL OR fill_status = '')
           ORDER BY submitted_at DESC""",
        (strategy, cutoff),
    ).fetchall()
    return [dict(r) for r in rows]


def resolve_order(conn, alpaca: PaperBackend, strategy: str, audit_row: dict,
                  dry_run: bool) -> Optional[str]:
    """Query Alpaca for one order's actual state; update DB if terminal.
    Returns the new fill_status if changed, else None."""
    alpaca_order_id = audit_row["alpaca_order_id"]
    ticker = audit_row["ticker"]

    try:
        # PaperBackend.base_url already includes /v2 — path is just /orders/{id}
        order = alpaca._request("GET", f"/orders/{alpaca_order_id}")
    except Exception as e:
        logger.warning("[%s] failed to query %s for %s: %s",
                       strategy, alpaca_order_id, ticker, e)
        return None

    alpaca_status = order.get("status")
    if alpaca_status not in ("filled", "rejected", "canceled", "expired", "done_for_day"):
        # Still in flight — don't touch
        return None

    new_fill_status = "filled" if alpaca_status == "filled" else alpaca_status
    filled_qty = order.get("filled_qty")
    filled_avg_price = order.get("filled_avg_price")
    filled_at = order.get("filled_at")
    reason = order.get("reject_reason") or order.get("cancel_reason")

    logger.info("[%s] RESOLVING %s %s: %s (alpaca_order_id=%s)",
                strategy, audit_row["side"], ticker, alpaca_status, alpaca_order_id)

    if dry_run:
        return new_fill_status

    if new_fill_status == "filled":
        conn.execute(
            """UPDATE order_audit
               SET fill_status = 'filled',
                   fill_qty = ?,
                   fill_price = ?,
                   filled_at = ?
               WHERE alpaca_order_id = ?""",
            (
                float(filled_qty) if filled_qty else None,
                float(filled_avg_price) if filled_avg_price else None,
                filled_at, alpaca_order_id,
            ),
        )
        # Sync portfolio entry_price
        if filled_avg_price and filled_qty:
            conn.execute(
                """UPDATE strategy_portfolio
                   SET entry_price = ?,
                       shares = ?,
                       actual_fill_price = ?
                   WHERE strategy = ? AND ticker = ?
                     AND status = 'open'
                     AND execution_source IN ('paper', 'live')
                     AND (actual_fill_price IS NULL OR
                          ABS(actual_fill_price - ?) > 0.0001)""",
                (
                    float(filled_avg_price), int(float(filled_qty)),
                    float(filled_avg_price),
                    strategy, ticker, float(filled_avg_price),
                ),
            )
    else:
        # rejected / canceled / expired — close any speculative open row
        conn.execute(
            """UPDATE order_audit
               SET fill_status = ?,
                   rejection_reason = COALESCE(?, rejection_reason)
               WHERE alpaca_order_id = ?""",
            (new_fill_status, reason, alpaca_order_id),
        )
        if audit_row["side"] == "buy":
            conn.execute(
                """UPDATE strategy_portfolio
                   SET status = 'closed',
                       exit_date = CURRENT_DATE,
                       exit_reason = ?,
                       pnl_pct = 0.0,
                       pnl_dollar = 0.0
                   WHERE strategy = ? AND ticker = ?
                     AND status = 'open'
                     AND execution_source IN ('paper', 'live')""",
                (f"alpaca_{new_fill_status}", strategy, ticker),
            )

    conn.commit()
    return new_fill_status


def run(strategies: list[str], dry_run: bool, force: bool):
    if not force and not is_market_open():
        logger.info("Outside market hours — no-op (use --force to override)")
        return

    summary = {}
    conn = get_connection()
    for account in ACCOUNTS:
        if strategies and account["name"] not in strategies:
            continue
        try:
            alpaca = get_alpaca(account)
        except KeyError as e:
            logger.warning("[%s] credentials missing: %s", account["name"], e)
            continue

        pending = fetch_pending_orders(conn, account["name"])
        n_resolved = 0
        n_filled = 0
        n_rejected = 0
        for row in pending:
            result = resolve_order(conn, alpaca, account["name"], row, dry_run)
            if result:
                n_resolved += 1
                if result == "filled":
                    n_filled += 1
                else:
                    n_rejected += 1
        summary[account["name"]] = {
            "n_pending_checked": len(pending),
            "n_resolved": n_resolved,
            "n_filled": n_filled,
            "n_rejected_or_canceled": n_rejected,
        }
        logger.info("[%s] pending=%d resolved=%d (filled=%d, rejected=%d)",
                    account["name"], len(pending), n_resolved, n_filled, n_rejected)
    conn.close()

    if any(s["n_resolved"] > 0 for s in summary.values()):
        logger.info("Resolver done — wrote updates")
    else:
        logger.info("Resolver done — all order_audit rows already terminal")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", action="append",
                   help="Limit to specific strategies (repeatable)")
    p.add_argument("--dry-run", action="store_true",
                   help="Query Alpaca but don't update DB")
    p.add_argument("--force", action="store_true",
                   help="Run outside market hours (manual debugging)")
    args = p.parse_args()
    run(args.strategy or [], args.dry_run, args.force)


if __name__ == "__main__":
    main()
