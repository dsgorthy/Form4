#!/usr/bin/env python3
"""Can anything beat V3 as it is currently configured?

V3 in production is career_grade in (A+, A) + above_sma50 + above_sma200 +
conviction >= 1.5, held 42 trading days. Measured through the real simulator
over 2023-2026 that is 55 trades, +8.48% mean alpha, +3.66% median alpha,
69.1% win rate. That is the bar. It is a real bar — an independent cohort
measurement on 15k filings put the same filter at +3.46pp over 8 of 9 years.

This tests variants against it, through the same simulator, changing one thing
at a time. Ranked on MEDIAN alpha, not mean: the hold sweep showed mean alpha
rising monotonically to 180 trading days while the median went to -10.63% and
the win rate to 51.4%. Mean rewards a right tail that a 10-position portfolio
will not reliably capture.

Every variant runs the full walk-forward with stop loss, trailing stop,
concurrency cap and capital constraint active, and none of them write.

Usage:
    python3 pipelines/insider_study/sweep_variants.py
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
from pipelines.insider_study.sweep_hold_period import spy_return  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")


def variants() -> list[tuple[str, dict]]:
    """(label, mutations) — each applied to a deep copy of the base config."""
    return [
        ("V3 baseline (current)",        {}),
        ("A+ only",                      {"filters": {"career_grade": ["A+"]}}),
        ("drop momentum filter",         {"filters": {"above_sma50": None,
                                                      "above_sma200": None}}),
        ("sma50 only (drop sma200)",     {"filters": {"above_sma200": None}}),
        ("conviction >= 3",              {"min_conviction": 3.0}),
        ("conviction >= 5",              {"min_conviction": 5.0}),
        ("conviction >= 7",              {"min_conviction": 7.0}),
        ("grade A+/A/B",                 {"filters": {"career_grade": ["A+", "A", "B"]}}),
        ("no grade filter",              {"filters": {"career_grade": None}}),
        ("20 concurrent",                {"max_concurrent": 20}),
        ("5 concurrent",                 {"max_concurrent": 5}),
    ]


def apply(cfg: dict, mut: dict) -> dict:
    out = copy.deepcopy(cfg)
    for k, v in mut.items():
        if k == "filters":
            f = out.setdefault("filters", {})
            for fk, fv in v.items():
                if fv is None:
                    f.pop(fk, None)
                else:
                    f[fk] = fv
            for th in out.get("theses") or []:
                tf = th.setdefault("filters", {})
                for fk, fv in v.items():
                    if fv is None:
                        tf.pop(fk, None)
                    else:
                        tf[fk] = fv
        else:
            out[k] = v
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="quality_momentum")
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    sc = STRATEGY_CONFIG[args.strategy]
    base = yaml.safe_load(sc["yaml"].read_text())
    start, end = sc["start_date"], args.end or date.today().isoformat()
    conn = get_connection()
    cache: dict[tuple[str, str], float | None] = {}

    print("\n" + "=" * 92)
    print(f"  VARIANT SWEEP — {args.strategy}, {start} .. {end}, real simulator")
    print("  ranked on MEDIAN alpha; mean rewards a tail a 10-slot portfolio cannot bank")
    print("=" * 92)
    print(f"    {'variant':<26} {'n':>4} {'mean α':>8} {'med α':>8} {'win%':>6} "
          f"{'stops':>6} {'final$':>11} {'ret%':>7}")

    results = []
    for label, mut in variants():
        cfg = apply(base, mut)
        try:
            closed, _, equity = simulate_one_strategy(conn, args.strategy, cfg, start, end)
        except Exception as e:
            print(f"    {label:<26} ERROR {e}")
            continue
        if not closed:
            print(f"    {label:<26} {'0':>4}   (no trades)")
            continue
        alphas = []
        for c in closed:
            key = (c.entry_date, c.exit_date)
            if key not in cache:
                cache[key] = spy_return(conn, c.entry_date, c.exit_date)
            if cache[key] is not None:
                alphas.append(c.pnl_pct * 100 - cache[key] * 100)
        if not alphas:
            continue
        rets = [c.pnl_pct * 100 for c in closed]
        med = statistics.median(alphas)
        results.append((label, len(closed), statistics.mean(alphas), med,
                        100 * sum(1 for r in rets if r > 0) / len(rets),
                        sum(1 for c in closed if c.exit_reason != "time"), equity))
        print(f"    {label:<26} {len(closed):>4} {statistics.mean(alphas):>7.2f} "
              f"{med:>7.2f} {100*sum(1 for r in rets if r>0)/len(rets):>5.1f} "
              f"{sum(1 for c in closed if c.exit_reason!='time'):>6} "
              f"{equity:>10,.0f} {(equity/float(base.get('starting_capital',100000))-1)*100:>6.1f}")

    if results:
        baseline = next((r for r in results if r[0].startswith("V3 baseline")), None)
        best = max(results, key=lambda r: r[3])
        print(f"\n    best median alpha: {best[0]} ({best[3]:+.2f}%, n={best[1]})")
        if baseline and best[0] != baseline[0]:
            print(f"    vs V3 baseline   : {baseline[3]:+.2f}% "
                  f"-> {best[3]:+.2f}%  ({best[3]-baseline[3]:+.2f}pp)")
        elif baseline:
            print("    nothing tested beats the current configuration")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
