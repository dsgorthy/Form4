#!/usr/bin/env python3
"""
Phase 2: Insider Track Record Scoring
--------------------------------------
Rank insiders by historical performance, segment into tiers,
and measure the lift from filtering by track record.

Uses training period (2021-2024) to build track records,
then validates on test period (2025-2026).
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy import stats as scipy_stats

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
OUTPUT_DIR = Path(__file__).parent / "data"

# We split the training period into two halves for track record building:
# - "Track record build" period: 2021-2022 (build insider histories)
# - "Track record use" period: 2023-2024 (test if track records predict)
# - "OOS" period: 2025-2026 (final validation)
TR_BUILD_START = "2021-01-01"
TR_BUILD_END = "2022-12-31"
TR_USE_START = "2023-01-01"
TR_USE_END = "2024-12-31"
OOS_START = "2025-01-01"
OOS_END = "2026-12-31"


def compute_metrics(returns: list[float]) -> dict:
    if not returns or len(returns) < 2:
        return {"n": len(returns), "mean": np.mean(returns) if returns else 0,
                "median": 0, "win_rate": 0, "sharpe": 0, "t_stat": 0, "p_value": 1.0}
    arr = np.array(returns)
    n = len(arr)
    mean = np.mean(arr)
    std = np.std(arr, ddof=1)
    wins = np.sum(arr > 0)
    sharpe = (mean / std) * np.sqrt(min(252, n)) if std > 0 else 0
    t_stat, p_value = scipy_stats.ttest_1samp(arr, 0)
    return {"n": n, "mean": float(mean), "median": float(np.median(arr)),
            "win_rate": float(wins / n), "sharpe": float(sharpe),
            "t_stat": float(t_stat), "p_value": float(p_value)}


def build_track_records(db: sqlite3.Connection, start: str, end: str) -> dict[int, dict]:
    """
    Build per-insider track records from buy trades in the given period.
    Returns: insider_id -> {n_trades, win_rate_7d, mean_abnormal_7d, sharpe, ...}
    """
    rows = db.execute("""
        SELECT t.insider_id, tr.abnormal_7d, tr.abnormal_30d, tr.return_7d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.trade_date >= ? AND t.trade_date <= ?
          AND tr.abnormal_7d IS NOT NULL
    """, (start, end)).fetchall()

    by_insider = defaultdict(list)
    for insider_id, abn_7d, abn_30d, ret_7d in rows:
        by_insider[insider_id].append({
            "abnormal_7d": abn_7d,
            "abnormal_30d": abn_30d,
            "return_7d": ret_7d,
        })

    records = {}
    for insider_id, trades in by_insider.items():
        abn_7d_list = [t["abnormal_7d"] for t in trades if t["abnormal_7d"] is not None]
        if len(abn_7d_list) < 2:
            continue

        arr = np.array(abn_7d_list)
        n = len(arr)
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        wins = np.sum(arr > 0)

        records[insider_id] = {
            "n_trades": n,
            "win_rate_7d": float(wins / n),
            "mean_abnormal_7d": float(mean),
            "std_7d": float(std),
            "sharpe_7d": float((mean / std) * np.sqrt(min(n, 20))) if std > 0 else 0,
        }

    return records


def assign_tiers(records: dict[int, dict]) -> dict[int, dict]:
    """
    Assign tiers based on Sharpe and win rate.
    Tier 1 (top 10%), Tier 2 (top 25%), Tier 3 (top 50%), Tier 4 (bottom 50%)
    """
    if not records:
        return records

    # Score = Sharpe * sqrt(N) / 2 + win_rate bonus
    for iid, rec in records.items():
        # N-adjusted score: penalize low sample sizes
        n_adj = min(rec["n_trades"], 20) / 20.0
        rec["score"] = rec["sharpe_7d"] * n_adj + (rec["win_rate_7d"] - 0.5) * 2

    scores = sorted(records.values(), key=lambda x: x["score"], reverse=True)
    thresholds = {
        1: np.percentile([s["score"] for s in scores], 90),
        2: np.percentile([s["score"] for s in scores], 75),
        3: np.percentile([s["score"] for s in scores], 50),
    }

    for iid, rec in records.items():
        if rec["score"] >= thresholds[1]:
            rec["tier"] = 1
        elif rec["score"] >= thresholds[2]:
            rec["tier"] = 2
        elif rec["score"] >= thresholds[3]:
            rec["tier"] = 3
        else:
            rec["tier"] = 4

    return records


def evaluate_by_tier(db: sqlite3.Connection, records: dict[int, dict],
                     start: str, end: str, label: str) -> dict:
    """
    Evaluate buy trade performance in a period, grouped by insider tier.
    """
    rows = db.execute("""
        SELECT t.insider_id, tr.abnormal_7d, tr.return_7d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.trade_date >= ? AND t.trade_date <= ?
          AND tr.abnormal_7d IS NOT NULL
    """, (start, end)).fetchall()

    tier_returns = defaultdict(list)
    unscored_returns = []

    for insider_id, abn_7d, ret_7d in rows:
        if insider_id in records:
            tier = records[insider_id]["tier"]
            tier_returns[tier].append(abn_7d)
        else:
            unscored_returns.append(abn_7d)

    results = {"label": label, "tiers": {}, "all_scored": {}, "unscored": {}}

    # Per-tier metrics
    for tier in [1, 2, 3, 4]:
        rets = tier_returns.get(tier, [])
        results["tiers"][tier] = compute_metrics(rets)
        n_insiders = sum(1 for r in records.values() if r["tier"] == tier)
        results["tiers"][tier]["n_insiders"] = n_insiders

    # All scored combined
    all_scored = []
    for tier_rets in tier_returns.values():
        all_scored.extend(tier_rets)
    results["all_scored"] = compute_metrics(all_scored)

    # Unscored
    results["unscored"] = compute_metrics(unscored_returns)

    return results


def format_report(build_results: dict, use_results: dict, oos_results: dict,
                  records: dict) -> str:
    lines = []
    lines.append("# Phase 2: Insider Track Record Scoring")
    lines.append("")
    lines.append("Track records built on 2021-2022 buy trades.")
    lines.append("Tested on 2023-2024 (in-sample use) and 2025-2026 (OOS).")
    lines.append("")

    # Tier distribution
    tier_counts = defaultdict(int)
    for r in records.values():
        tier_counts[r["tier"]] += 1
    lines.append("## Insider Tier Distribution (from 2021-2022 track records)")
    for tier in [1, 2, 3, 4]:
        pct = tier_counts[tier] / len(records) * 100 if records else 0
        lines.append(f"  Tier {tier}: {tier_counts[tier]} insiders ({pct:.1f}%)")
    lines.append("")

    for result_set, period_label in [(build_results, "Build Period (2021-2022)"),
                                      (use_results, "Use Period (2023-2024)"),
                                      (oos_results, "OOS Period (2025-2026)")]:
        lines.append(f"## Performance by Tier — {period_label}")
        lines.append("")
        header = f"{'Tier':<15} | {'N Trades':>10} | {'N Insiders':>10} | {'Mean Abn':>10} | {'Median':>10} | {'WR':>8} | {'Sharpe':>8} | {'p-val':>8}"
        lines.append(header)
        lines.append("-" * len(header))
        for tier in [1, 2, 3, 4]:
            m = result_set["tiers"].get(tier, {"n": 0, "mean": 0, "median": 0, "win_rate": 0, "sharpe": 0, "p_value": 1, "n_insiders": 0})
            lines.append(
                f"Tier {tier:<10} | {m.get('n', 0):>10} | {m.get('n_insiders', 0):>10} | "
                f"{m.get('mean', 0):>+9.2%} | {m.get('median', 0):>+9.2%} | "
                f"{m.get('win_rate', 0):>7.1%} | {m.get('sharpe', 0):>8.2f} | "
                f"{m.get('p_value', 1):>8.4f}"
            )

        m = result_set["all_scored"]
        lines.append(
            f"{'All Scored':<15} | {m['n']:>10} | {'':>10} | "
            f"{m['mean']:>+9.2%} | {m['median']:>+9.2%} | "
            f"{m['win_rate']:>7.1%} | {m['sharpe']:>8.2f} | "
            f"{m['p_value']:>8.4f}"
        )
        m = result_set["unscored"]
        lines.append(
            f"{'Unscored':<15} | {m['n']:>10} | {'':>10} | "
            f"{m['mean']:>+9.2%} | {m['median']:>+9.2%} | "
            f"{m['win_rate']:>7.1%} | {m['sharpe']:>8.2f} | "
            f"{m['p_value']:>8.4f}"
        )
        lines.append("")

    # Lift analysis
    lines.append("## Lift Analysis")
    lines.append("")
    for result_set, period_label in [(use_results, "2023-2024"), (oos_results, "2025-2026")]:
        t1 = result_set["tiers"].get(1, {})
        all_s = result_set["all_scored"]
        unscored = result_set["unscored"]
        if t1.get("n", 0) > 0 and all_s.get("n", 0) > 0:
            lift_vs_all = ((t1["mean"] - all_s["mean"]) / abs(all_s["mean"]) * 100) if all_s["mean"] != 0 else 0
            lift_vs_unscored = ((t1["mean"] - unscored["mean"]) / abs(unscored["mean"]) * 100) if unscored["mean"] != 0 else 0
            lines.append(f"  {period_label}:")
            lines.append(f"    Tier 1 vs All Scored: {lift_vs_all:+.1f}% lift in mean abnormal return")
            lines.append(f"    Tier 1 vs Unscored: {lift_vs_unscored:+.1f}% lift in mean abnormal return")
            lines.append(f"    Tier 1 Sharpe: {t1.get('sharpe', 0):.2f} vs All: {all_s['sharpe']:.2f}")
    lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 60)
    print("Phase 2: Insider Track Record Scoring")
    print("=" * 60)

    db = sqlite3.connect(str(DB_PATH))

    # Step 1: Build track records from 2021-2022
    print("\nBuilding track records from 2021-2022...")
    records = build_track_records(db, TR_BUILD_START, TR_BUILD_END)
    print(f"  {len(records)} insiders with 2+ buy trades in build period")

    # Step 2: Assign tiers
    records = assign_tiers(records)
    tier_counts = defaultdict(int)
    for r in records.values():
        tier_counts[r["tier"]] += 1
    for tier in [1, 2, 3, 4]:
        print(f"  Tier {tier}: {tier_counts[tier]} insiders")

    # Step 3: Evaluate on build period (sanity check)
    print("\nEvaluating on build period (2021-2022)...")
    build_results = evaluate_by_tier(db, records, TR_BUILD_START, TR_BUILD_END, "Build")

    # Step 4: Evaluate on use period (2023-2024)
    print("Evaluating on use period (2023-2024)...")
    use_results = evaluate_by_tier(db, records, TR_USE_START, TR_USE_END, "Use")

    # Step 5: Evaluate on OOS (2025-2026)
    print("Evaluating on OOS period (2025-2026)...")
    oos_results = evaluate_by_tier(db, records, OOS_START, OOS_END, "OOS")

    # Generate report
    report = format_report(build_results, use_results, oos_results, records)
    print("\n" + report)

    # Save
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / "track_record_report.txt", "w") as f:
        f.write(report)

    # Save raw data
    serializable_records = {str(k): v for k, v in records.items()}
    with open(OUTPUT_DIR / "track_records.json", "w") as f:
        json.dump({
            "records": serializable_records,
            "build_results": build_results,
            "use_results": use_results,
            "oos_results": oos_results,
        }, f, indent=2, default=str)

    print(f"\nSaved to {OUTPUT_DIR}/track_record_*.json/txt")

    db.close()
    return records, build_results, use_results, oos_results


if __name__ == "__main__":
    main()
