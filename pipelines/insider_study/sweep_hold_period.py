#!/usr/bin/env python3
"""Sweep a strategy's holding period through the real simulator.

WHY THIS AND NOT SQL

A SQL holding-period curve assumes a pure time exit. The live strategies do
not have one: simulate_strategy_portfolio applies a stop loss, a trailing
stop, a concurrency cap and a capital constraint, and those interact with
hold length in ways an average-over-all-filings query cannot see. A shorter
hold frees a slot sooner, which changes which later filings get taken at all.

So this drives the actual simulator, changing exactly one parameter.

It calls simulate_one_strategy directly rather than run(), which means it
never wipes and never writes — a sweep must not touch strategy_portfolio.

Usage:
    python3 pipelines/insider_study/sweep_hold_period.py
    python3 pipelines/insider_study/sweep_hold_period.py --strategy reversal_dip
"""
from __future__ import annotations

import argparse
import copy
import logging
import statistics
import sys
from datetime import date
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402
from pipelines.insider_study.simulate_strategy_portfolio import (  # noqa: E402
    STRATEGY_CONFIG, simulate_one_strategy,
)

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

HOLDS = (5, 10, 21, 42, 60, 90, 120, 180)


def spy_return(conn, d0: str, d1: str) -> float | None:
    """SPY total return between two dates, for alpha."""
    # Both scalar subqueries come back unnamed, and the compat layer keys rows
    # by column name — two blank names collapse to one key. Alias them.
    row = conn.execute(
        """SELECT (SELECT close FROM prices.daily_prices
                    WHERE ticker='SPY' AND date >= ? ORDER BY date LIMIT 1) AS p0,
                  (SELECT close FROM prices.daily_prices
                    WHERE ticker='SPY' AND date >= ? ORDER BY date LIMIT 1) AS p1""",
        (d0, d1)).fetchone()
    if not row:
        return None
    p0, p1 = row["p0"], row["p1"]
    if not p0 or not p1:
        return None
    return p1 / p0 - 1.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="quality_momentum",
                    choices=list(STRATEGY_CONFIG))
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    sc = STRATEGY_CONFIG[args.strategy]
    base = yaml.safe_load(sc["yaml"].read_text())
    start, end = sc["start_date"], args.end or date.today().isoformat()
    conn = get_connection()

    print("\n" + "=" * 88)
    print(f"  HOLD SWEEP — {args.strategy}, {start} .. {end}, real simulator")
    print("  (stop loss, trailing stop, concurrency and capital all active)")
    print("=" * 88)
    print(f"    {'hold':>5} {'trades':>7} {'mean%':>7} {'med%':>7} {'win%':>6} "
          f"{'alpha%':>7} {'medα%':>7} {'stops':>6} {'final$':>11} {'ret%':>7}")

    spy_cache: dict[tuple[str, str], float | None] = {}
    rows_out = []
    for h in HOLDS:
        cfg = copy.deepcopy(base)
        cfg.setdefault("exit", {})["hold_days"] = h
        for th in cfg.get("theses") or []:
            th.setdefault("exit", {})["hold_days"] = h

        closed, held, equity = simulate_one_strategy(
            conn, args.strategy, cfg, start, end)
        if not closed:
            print(f"    {h:>5} {'0':>7}   (no trades)")
            continue

        rets = [c.pnl_pct * 100 for c in closed]
        alphas = []
        for c in closed:
            key = (c.entry_date, c.exit_date)
            if key not in spy_cache:
                spy_cache[key] = spy_return(conn, c.entry_date, c.exit_date)
            s = spy_cache[key]
            if s is not None:
                alphas.append(c.pnl_pct * 100 - s * 100)

        stops = sum(1 for c in closed if c.exit_reason != "time")
        start_eq = float(base.get("starting_capital", 100000))
        total_ret = (equity / start_eq - 1) * 100
        rows_out.append((h, statistics.mean(alphas) if alphas else 0, total_ret))
        print(f"    {h:>5} {len(closed):>7} {statistics.mean(rets):>6.2f} "
              f"{statistics.median(rets):>6.2f} "
              f"{100*sum(1 for r in rets if r > 0)/len(rets):>5.1f} "
              f"{(statistics.mean(alphas) if alphas else 0):>6.2f} "
              f"{(statistics.median(alphas) if alphas else 0):>6.2f} "
              f"{stops:>6} {equity:>10,.0f} {total_ret:>6.1f}")

    if rows_out:
        by_alpha = max(rows_out, key=lambda r: r[1])
        by_total = max(rows_out, key=lambda r: r[2])
        print(f"\n    best mean alpha : {by_alpha[0]}td ({by_alpha[1]:+.2f}%)")
        print(f"    best total return: {by_total[0]}td ({by_total[2]:+.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
