#!/usr/bin/env python3
"""Import strategy_portfolio entries as 'actual' rows in trade_decision_audit.

The strategy_portfolio table holds every position the strategy has ever
entered — across the strategy's lifetime, mixing backtest entries (2020-2026)
with live-runner entries (2026-04 onwards). The conviction logic and filter
set has evolved over that timespan, so a deterministic walk-forward
SIMULATION using *current* code disagrees with the historical entries.
That's correct behavior, not a bug.

This script imports each strategy_portfolio entry as one audit row with
source='actual', stage='conviction', passed=true, plus a feature_snapshot
of what was recorded on the position. Lets the admin diagnostics view
display the THREE sources side by side:

  source=actual      — historical entry (backtest or live, mixed code versions)
  source=simulation  — what current cw_runner code would do (deterministic)
  source=live        — what cw_runner did this run (post-deploy)

Re-running is safe: ON CONFLICT-style dedup via in-memory existing-keys check.

Usage:
    python3 scripts/backfill_actual_from_portfolio.py
    python3 scripts/backfill_actual_from_portfolio.py --strategy quality_momentum
    python3 scripts/backfill_actual_from_portfolio.py --replace   # nuke prior 'actual' rows first
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STRATEGIES = ["quality_momentum", "reversal_dip", "tenb51_surprise"]


def import_strategy(conn, strategy: str, replace: bool) -> int:
    """Import one strategy. Returns rows inserted."""
    if replace:
        n = conn.execute(
            "DELETE FROM trade_decision_audit WHERE strategy = ? AND source = 'actual'",
            (strategy,),
        ).rowcount
        conn.commit()
        logger.info("[%s] cleared %d existing actual rows", strategy, n or 0)

    rows = conn.execute(
        """SELECT entry_date, trade_id, ticker, entry_price, target_hold,
                  stop_pct, hold_days, exit_date, exit_price, pnl_pct,
                  pnl_dollar, signal_quality, insider_pit_n, insider_pit_wr,
                  insider_name, status, exit_reason
             FROM strategy_portfolio
            WHERE strategy = ?
            ORDER BY entry_date""",
        (strategy,),
    ).fetchall()
    if not rows:
        logger.info("[%s] no portfolio entries", strategy)
        return 0

    # Pre-load existing actual keys to avoid duplicates on re-run
    existing = set()
    cur = conn.execute(
        "SELECT trade_id, ticker, filing_date FROM trade_decision_audit "
        "WHERE strategy = ? AND source = 'actual'",
        (strategy,),
    )
    for r in cur.fetchall():
        existing.add((r["trade_id"], r["ticker"], r["filing_date"]))

    batch = []
    for r in rows:
        d = dict(r)
        tid = d.get("trade_id")
        ticker = d.get("ticker")
        entry_date = d.get("entry_date")
        if not (ticker and entry_date):
            continue
        if (tid, ticker, entry_date) in existing:
            continue

        run_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"actual/{strategy}/{entry_date}/{ticker}/{tid}",
        ))
        try:
            ts = datetime.strptime(entry_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc,
            )
        except ValueError:
            ts = datetime.now(timezone.utc)

        snapshot = {
            "entry_price": d.get("entry_price"),
            "target_hold": d.get("target_hold"),
            "hold_days": d.get("hold_days"),
            "stop_pct": d.get("stop_pct"),
            "exit_date": d.get("exit_date"),
            "exit_price": d.get("exit_price"),
            "pnl_pct": d.get("pnl_pct"),
            "pnl_dollar": d.get("pnl_dollar"),
            "signal_quality": d.get("signal_quality"),
            "insider_pit_n": d.get("insider_pit_n"),
            "insider_pit_wr": d.get("insider_pit_wr"),
            "insider_name": d.get("insider_name"),
            "status": d.get("status"),
            "exit_reason": d.get("exit_reason"),
            "from": "strategy_portfolio",
        }
        # Reason summarizes the realized outcome
        reason_parts = [f"entered ${d.get('entry_price') or 0:.2f}"]
        if d.get("status") == "closed":
            reason_parts.append(f"exited ${d.get('exit_price') or 0:.2f}")
            if d.get("pnl_pct") is not None:
                reason_parts.append(f"P&L {d['pnl_pct']*100:+.1f}%")
            if d.get("exit_reason"):
                reason_parts.append(f"reason={d['exit_reason']}")
        elif d.get("status") == "open":
            reason_parts.append("OPEN")
        reason = " · ".join(reason_parts)

        # One row per actual entry, at conviction stage (it cleared all earlier
        # filters since the live/backtest runner DID enter it).
        batch.append((
            run_id, strategy, ticker, tid, entry_date, strategy,
            "conviction", True, reason, None, None,
            json.dumps(snapshot), "actual", ts,
        ))

    if not batch:
        logger.info("[%s] all %d portfolio entries already imported (no-op)",
                    strategy, len(rows))
        return 0

    try:
        conn.executemany(
            """INSERT INTO trade_decision_audit
                  (run_id, strategy, ticker, trade_id, filing_date, thesis,
                   stage, passed, reason, pit_grade, conviction, feature_snapshot,
                   source, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)""",
            batch,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("[%s] insert failed: %s", strategy, e)
        return 0

    logger.info("[%s] inserted %d actual rows (from %d portfolio entries)",
                strategy, len(batch), len(rows))
    return len(batch)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=STRATEGIES,
                   help="Only this strategy (default: all)")
    p.add_argument("--replace", action="store_true",
                   help="Delete prior actual rows for the strategy first")
    args = p.parse_args()

    conn = get_connection()
    total = 0
    for s in STRATEGIES:
        if args.strategy and s != args.strategy:
            continue
        total += import_strategy(conn, s, args.replace)
    conn.close()
    logger.info("Done. %d total actual rows inserted.", total)


if __name__ == "__main__":
    main()
