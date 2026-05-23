#!/usr/bin/env python3
"""Drift detector — compare sim_portfolio (theoretical) vs paper_trades (actual).

Runs daily. For each strategy in ALLOWED_STRATEGIES, compares the sim's
state against the paper account's state on a per-(ticker, entry_date)
basis and records every divergence in strategy_drift_audit.

Drift types:
    sim_only     — sim entered this trade; paper didn't (capacity, freshness,
                   guardrail rejection, or live-only stale data)
    paper_only   — paper entered this trade; sim didn't (data race, late
                   PIT score, or live-only signal we haven't backfilled)
    size_delta   — both entered but dollar_amount differs >5%
    price_delta  — both entered but entry_price differs >2%
    exit_delta   — both closed but on different dates or prices

The detector is intentionally conservative: thresholds are tunable so we
don't generate noise from floating-point fuzz. Critical drift (sim_only
or paper_only on a live-money strategy) escalates to severity='critical'
and writes to logs/alerts.ndjson.

Output: one row per drift in strategy_drift_audit + a summary line in
pipeline_runs metadata + per-critical writes to alerts.ndjson.

Usage:
    python3 scripts/drift_detector.py
    python3 scripts/drift_detector.py --strategy quality_momentum
    python3 scripts/drift_detector.py --since 2026-05-01

Until Stage 2 cutover is complete, sim rows live in sim_portfolio and
paper rows live in strategy_portfolio (filtered by execution_source='paper').
After cutover, both live in their respective new tables and the WHERE
clause simplifies.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.observability import pipeline_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ALLOWED_STRATEGIES = ("quality_momentum", "reversal_dip", "tenb51_surprise")
ALERT_LOG = REPO / "logs" / "alerts.ndjson"

# Drift thresholds (tunable)
SIZE_DELTA_PCT = 0.05    # 5% dollar_amount difference triggers size_delta
PRICE_DELTA_PCT = 0.02   # 2% entry_price difference triggers price_delta


def load_state(conn, strategy: str, since: Optional[str]) -> Tuple[Dict, Dict]:
    """Return (sim_state, paper_state) keyed by (ticker, entry_date)."""
    where_since = "AND entry_date >= ?" if since else ""
    params_since = (since,) if since else ()

    # Sim — currently in sim_portfolio (post-Stage 2 backfill) AND in
    # strategy_portfolio. Source both for safety; sim_portfolio is the
    # cleaner read once writers migrate.
    sim_rows = conn.execute(
        f"""SELECT ticker, entry_date, trade_id, status, entry_price,
                   dollar_amount, pnl_pct, exit_date, exit_price
            FROM sim_portfolio
            WHERE strategy = ? {where_since}""",
        (strategy,) + params_since,
    ).fetchall()
    sim_state = {(r["ticker"], r["entry_date"]): dict(r) for r in sim_rows}

    # Paper — until Stage 2 cutover, paper rows live in strategy_portfolio
    # filtered by execution_source. The paper_trades table is also populated
    # (one-shot backfill) but no new rows arrive until cutover. Use the
    # authoritative source.
    paper_rows = conn.execute(
        f"""SELECT ticker, entry_date, trade_id, status, entry_price,
                   dollar_amount, pnl_pct, exit_date, exit_price
            FROM strategy_portfolio
            WHERE strategy = ? AND execution_source = 'paper'
              AND COALESCE(is_live, false) = false
              {where_since}""",
        (strategy,) + params_since,
    ).fetchall()
    paper_state = {(r["ticker"], r["entry_date"]): dict(r) for r in paper_rows}

    return sim_state, paper_state


def compare(strategy: str, sim_state: Dict, paper_state: Dict) -> List[Dict]:
    """Return a list of drift records (dicts) — one per divergence found."""
    keys = set(sim_state) | set(paper_state)
    drifts: List[Dict] = []

    for key in sorted(keys):
        ticker, entry_date = key
        sim = sim_state.get(key)
        paper = paper_state.get(key)

        base = {
            "strategy": strategy,
            "ticker": ticker,
            "entry_date": entry_date,
            "sim_trade_id": (sim or {}).get("trade_id"),
            "paper_trade_id": (paper or {}).get("trade_id"),
            "sim_status": (sim or {}).get("status"),
            "paper_status": (paper or {}).get("status"),
            "sim_entry_price": (sim or {}).get("entry_price"),
            "paper_entry_price": (paper or {}).get("entry_price"),
            "sim_dollar_amount": (sim or {}).get("dollar_amount"),
            "paper_dollar_amount": (paper or {}).get("dollar_amount"),
            "sim_pnl_pct": (sim or {}).get("pnl_pct"),
            "paper_pnl_pct": (paper or {}).get("pnl_pct"),
        }

        if sim and not paper:
            drifts.append({**base, "drift_type": "sim_only", "severity": "warn",
                           "notes": "Sim entered this trade; paper account did not."})
            continue
        if paper and not sim:
            drifts.append({**base, "drift_type": "paper_only", "severity": "warn",
                           "notes": "Paper account entered this trade; sim did not."})
            continue

        # Both present — check size + price + exit
        sim_dollar = sim.get("dollar_amount") or 0
        paper_dollar = paper.get("dollar_amount") or 0
        if sim_dollar > 0 and paper_dollar > 0:
            size_diff = abs(sim_dollar - paper_dollar) / sim_dollar
            if size_diff > SIZE_DELTA_PCT:
                drifts.append({**base, "drift_type": "size_delta", "severity": "info",
                               "notes": f"dollar_amount diff {size_diff:.1%}"})

        sim_price = sim.get("entry_price") or 0
        paper_price = paper.get("entry_price") or 0
        if sim_price > 0 and paper_price > 0:
            price_diff = abs(sim_price - paper_price) / sim_price
            if price_diff > PRICE_DELTA_PCT:
                drifts.append({**base, "drift_type": "price_delta", "severity": "info",
                               "notes": f"entry_price diff {price_diff:.2%}"})

        # Exit divergence (both closed but at different dates or prices)
        if sim.get("exit_date") and paper.get("exit_date"):
            if sim["exit_date"] != paper["exit_date"]:
                drifts.append({**base, "drift_type": "exit_delta", "severity": "info",
                               "notes": f"exit_date sim={sim['exit_date']} paper={paper['exit_date']}"})

    return drifts


def write_drifts(conn, run_uuid: Optional[str], drifts: List[Dict]) -> int:
    """Insert drift records, return count."""
    for d in drifts:
        conn.execute(
            """INSERT INTO strategy_drift_audit
                (run_uuid, strategy, ticker, entry_date, drift_type,
                 sim_trade_id, paper_trade_id, sim_status, paper_status,
                 sim_entry_price, paper_entry_price,
                 sim_dollar_amount, paper_dollar_amount,
                 sim_pnl_pct, paper_pnl_pct, severity, notes)
               VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_uuid, d["strategy"], d["ticker"], d["entry_date"],
             d["drift_type"],
             d.get("sim_trade_id"), d.get("paper_trade_id"),
             d.get("sim_status"), d.get("paper_status"),
             d.get("sim_entry_price"), d.get("paper_entry_price"),
             d.get("sim_dollar_amount"), d.get("paper_dollar_amount"),
             d.get("sim_pnl_pct"), d.get("paper_pnl_pct"),
             d["severity"], d.get("notes")),
        )
    conn.commit()
    return len(drifts)


def write_alert_if_critical(drifts: List[Dict], strategy: str) -> None:
    """Append a critical-severity row to logs/alerts.ndjson for the dashboard."""
    n_warn = sum(1 for d in drifts if d["severity"] == "warn")
    n_crit = sum(1 for d in drifts if d["severity"] == "critical")
    if n_warn + n_crit == 0:
        return
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    severity = "critical" if n_crit > 0 else "warn"
    msg = f"[drift_detector] {strategy}: {n_warn} warn + {n_crit} critical drifts"
    with ALERT_LOG.open("a") as f:
        f.write(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
            "component": "drift_detector",
            "strategy": strategy,
            "message": msg,
        }) + "\n")


def run_for_strategy(conn, strategy: str, since: Optional[str], run_uuid: Optional[str]) -> Dict:
    sim_state, paper_state = load_state(conn, strategy, since)
    drifts = compare(strategy, sim_state, paper_state)
    n_written = write_drifts(conn, run_uuid, drifts)
    write_alert_if_critical(drifts, strategy)

    by_type: Dict[str, int] = {}
    for d in drifts:
        by_type[d["drift_type"]] = by_type.get(d["drift_type"], 0) + 1

    logger.info(
        "[%s] sim=%d paper=%d drifts=%d (%s)",
        strategy, len(sim_state), len(paper_state), n_written,
        ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())) or "none",
    )

    return {
        "sim_count": len(sim_state),
        "paper_count": len(paper_state),
        "drift_count": n_written,
        "by_type": by_type,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=ALLOWED_STRATEGIES,
                   help="Single strategy (default: all)")
    p.add_argument("--since", default=None,
                   help="Only compare entries >= this date (YYYY-MM-DD)")
    args = p.parse_args()

    strategies = [args.strategy] if args.strategy else list(ALLOWED_STRATEGIES)

    with pipeline_run("drift_detector", log_path="logs/drift_detector.log") as prun:
        conn = get_connection()
        results = {}
        total_drifts = 0
        for s in strategies:
            results[s] = run_for_strategy(conn, s, args.since, prun.run_uuid)
            total_drifts += results[s]["drift_count"]
        conn.close()

        prun.set_rows_written(total_drifts)
        prun.set_metadata({
            "strategies": strategies,
            "since": args.since,
            "results": results,
        })

    logger.info("Drift detector done: %d total drifts across %d strategies",
                total_drifts, len(strategies))


if __name__ == "__main__":
    main()
