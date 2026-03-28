#!/usr/bin/env python3
"""
Phase 1: Insider Signal Analysis
---------------------------------
Comprehensive comparison of 4 signal types across multiple hold windows:
  - Buy + Individual (single insider)
  - Buy + Cluster (2+ insiders within 30-day window)
  - Sell + Individual
  - Sell + Cluster

For each: N, mean/median return, abnormal return, win rate, Sharpe, t-stat, p-value.
Also breaks down by insider seniority and trade value.

Train period: 2021-2024 (2020 excluded for COVID distortion)
"""

from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from scipy import stats as scipy_stats

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"

# Train period
TRAIN_START = "2021-01-01"
TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"
TEST_END = "2026-12-31"

# Hold windows to analyze
HOLD_WINDOWS = {
    "7d": ("return_7d", "spy_return_7d", "abnormal_7d"),
    "30d": ("return_30d", "spy_return_30d", "abnormal_30d"),
    "90d": ("return_90d", "spy_return_90d", "abnormal_90d"),
}

# Cluster detection: 2+ distinct insiders trading the same ticker within 30 days
CLUSTER_WINDOW_DAYS = 30
CLUSTER_MIN_INSIDERS = 2


def connect_db():
    return sqlite3.connect(str(DB_PATH))


def build_cluster_flags(db: sqlite3.Connection, trade_type: str, start: str, end: str) -> dict[int, bool]:
    """
    For each trade, determine if it's part of a cluster (2+ distinct insiders
    trading the same ticker within 30 calendar days).
    Returns dict: trade_id -> is_cluster
    """
    rows = db.execute("""
        SELECT trade_id, insider_id, ticker, trade_date
        FROM trades
        WHERE trade_type = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY ticker, trade_date
    """, (trade_type, start, end)).fetchall()

    # Group by ticker
    by_ticker = defaultdict(list)
    for trade_id, insider_id, ticker, trade_date in rows:
        by_ticker[ticker].append((trade_id, insider_id, trade_date))

    cluster_flags = {}
    for ticker, trades in by_ticker.items():
        # Pre-parse dates once
        parsed = [(tid, iid, datetime.strptime(td, "%Y-%m-%d")) for tid, iid, td in trades]
        # sorted by date already (ORDER BY ticker, trade_date)
        n = len(parsed)
        for i in range(n):
            tid, iid, td = parsed[i]
            nearby_insiders = {iid}
            # Scan forward/backward within window (sorted, so can break early)
            for j in range(i - 1, -1, -1):
                if (td - parsed[j][2]).days > CLUSTER_WINDOW_DAYS:
                    break
                nearby_insiders.add(parsed[j][1])
            for j in range(i + 1, n):
                if (parsed[j][2] - td).days > CLUSTER_WINDOW_DAYS:
                    break
                nearby_insiders.add(parsed[j][1])
            cluster_flags[tid] = len(nearby_insiders) >= CLUSTER_MIN_INSIDERS

    return cluster_flags


def compute_metrics(returns: list[float]) -> dict:
    """Compute standard performance metrics for a list of returns."""
    if not returns or len(returns) < 2:
        return {
            "n": len(returns),
            "mean": np.mean(returns) if returns else 0,
            "median": np.median(returns) if returns else 0,
            "std": 0, "sharpe": 0, "win_rate": 0,
            "t_stat": 0, "p_value": 1.0,
        }
    arr = np.array(returns)
    mean = np.mean(arr)
    std = np.std(arr, ddof=1)
    n = len(arr)
    wins = np.sum(arr > 0)

    sharpe = 0
    if std > 0:
        # Annualize: assume ~34 trades/year for event-driven
        sharpe = (mean / std) * np.sqrt(min(252, n))

    t_stat, p_value = scipy_stats.ttest_1samp(arr, 0)

    return {
        "n": n,
        "mean": float(mean),
        "median": float(np.median(arr)),
        "std": float(std),
        "sharpe": float(sharpe),
        "win_rate": float(wins / n) if n > 0 else 0,
        "t_stat": float(t_stat),
        "p_value": float(p_value),
    }


def analyze_signals(period_label: str, start: str, end: str) -> dict:
    """
    Run full signal analysis for a given period.
    Returns nested dict: signal_type -> hold_window -> metrics
    """
    db = connect_db()

    results = {}

    for trade_type in ["buy", "sell"]:
        # Build cluster flags
        print(f"  Building cluster flags for {trade_type}s ({start} to {end})...")
        cluster_flags = build_cluster_flags(db, trade_type, start, end)

        # Fetch trades with returns
        query = """
            SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.value,
                   t.is_csuite, t.title_weight, t.title,
                   tr.return_7d, tr.return_30d, tr.return_90d,
                   tr.spy_return_7d, tr.spy_return_30d, tr.spy_return_90d,
                   tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trade_type = ? AND t.trade_date >= ? AND t.trade_date <= ?
        """
        rows = db.execute(query, (trade_type, start, end)).fetchall()
        print(f"  {trade_type}: {len(rows)} trades with returns")

        # Split into individual vs cluster
        for signal_label, is_cluster_target in [("individual", False), ("cluster", True)]:
            signal_key = f"{trade_type}_{signal_label}"
            results[signal_key] = {}

            # Filter trades
            signal_trades = []
            for row in rows:
                tid = row[0]
                is_cluster = cluster_flags.get(tid, False)
                if is_cluster == is_cluster_target:
                    signal_trades.append(row)

            print(f"    {signal_key}: {len(signal_trades)} trades")

            # Compute metrics for each hold window
            for window_name, (ret_col, spy_col, abn_col) in HOLD_WINDOWS.items():
                col_idx = {"return_7d": 8, "return_30d": 9, "return_90d": 10,
                          "spy_return_7d": 11, "spy_return_30d": 12, "spy_return_90d": 13,
                          "abnormal_7d": 14, "abnormal_30d": 15, "abnormal_90d": 16}

                ret_idx = col_idx[ret_col]
                abn_idx = col_idx[abn_col]

                # For sells: invert returns (profit from price decline)
                multiplier = -1.0 if trade_type == "sell" else 1.0

                raw_returns = [row[ret_idx] * multiplier for row in signal_trades if row[ret_idx] is not None]
                abnormal_returns = [row[abn_idx] * multiplier for row in signal_trades if row[abn_idx] is not None]

                metrics = compute_metrics(raw_returns)
                abn_metrics = compute_metrics(abnormal_returns)

                results[signal_key][window_name] = {
                    "raw": metrics,
                    "abnormal": abn_metrics,
                }

            # Breakdown by seniority
            seniority_buckets = {
                "C-Suite": [],
                "Director": [],
                "Officer": [],
                "Other": [],
            }
            for row in signal_trades:
                title_weight = row[6] or 0
                is_csuite = row[5]
                title = (row[7] or "").lower()
                if is_csuite or title_weight >= 3.0:
                    seniority_buckets["C-Suite"].append(row)
                elif "director" in title or title_weight >= 1.5:
                    seniority_buckets["Director"].append(row)
                elif title_weight >= 1.0:
                    seniority_buckets["Officer"].append(row)
                else:
                    seniority_buckets["Other"].append(row)

            results[signal_key]["by_seniority"] = {}
            for seniority, trades in seniority_buckets.items():
                multiplier = -1.0 if trade_type == "sell" else 1.0
                abn_7d = [r[14] * multiplier for r in trades if r[14] is not None]
                results[signal_key]["by_seniority"][seniority] = {
                    "n": len(trades),
                    "abnormal_7d": compute_metrics(abn_7d),
                }

            # Breakdown by trade value
            value_buckets = {
                "< $100K": [],
                "$100K-$1M": [],
                "$1M-$5M": [],
                "$5M+": [],
            }
            for row in signal_trades:
                val = abs(row[4]) if row[4] else 0
                if val < 100_000:
                    value_buckets["< $100K"].append(row)
                elif val < 1_000_000:
                    value_buckets["$100K-$1M"].append(row)
                elif val < 5_000_000:
                    value_buckets["$1M-$5M"].append(row)
                else:
                    value_buckets["$5M+"].append(row)

            results[signal_key]["by_value"] = {}
            for bucket, trades in value_buckets.items():
                multiplier = -1.0 if trade_type == "sell" else 1.0
                abn_7d = [r[14] * multiplier for r in trades if r[14] is not None]
                results[signal_key]["by_value"][bucket] = {
                    "n": len(trades),
                    "abnormal_7d": compute_metrics(abn_7d),
                }

    db.close()
    return results


def format_results(results: dict, label: str) -> str:
    """Format results into a readable report string."""
    lines = []
    lines.append(f"# Signal Analysis — {label}")
    lines.append("")

    # Main comparison matrix
    lines.append("## Signal Type x Hold Window Matrix")
    lines.append("")
    lines.append("### Abnormal Returns (vs SPY)")
    lines.append("")

    header = f"{'Signal Type':<25} | {'Window':>8} | {'N':>6} | {'Mean':>8} | {'Median':>8} | {'WR':>6} | {'Sharpe':>7} | {'t-stat':>7} | {'p-val':>7}"
    lines.append(header)
    lines.append("-" * len(header))

    for signal_key in ["buy_individual", "buy_cluster", "sell_individual", "sell_cluster"]:
        if signal_key not in results:
            continue
        for window in ["7d", "30d", "90d"]:
            if window not in results[signal_key]:
                continue
            m = results[signal_key][window]["abnormal"]
            lines.append(
                f"{signal_key:<25} | {window:>8} | {m['n']:>6} | "
                f"{m['mean']:>+7.2%} | {m['median']:>+7.2%} | "
                f"{m['win_rate']:>5.1%} | {m['sharpe']:>7.2f} | "
                f"{m['t_stat']:>7.2f} | {m['p_value']:>7.4f}"
            )
        lines.append("")

    # Raw returns
    lines.append("### Raw Returns")
    lines.append("")
    lines.append(header)
    lines.append("-" * len(header))

    for signal_key in ["buy_individual", "buy_cluster", "sell_individual", "sell_cluster"]:
        if signal_key not in results:
            continue
        for window in ["7d", "30d", "90d"]:
            if window not in results[signal_key]:
                continue
            m = results[signal_key][window]["raw"]
            lines.append(
                f"{signal_key:<25} | {window:>8} | {m['n']:>6} | "
                f"{m['mean']:>+7.2%} | {m['median']:>+7.2%} | "
                f"{m['win_rate']:>5.1%} | {m['sharpe']:>7.2f} | "
                f"{m['t_stat']:>7.2f} | {m['p_value']:>7.4f}"
            )
        lines.append("")

    # Seniority breakdown
    lines.append("## Breakdown by Insider Seniority (7d Abnormal)")
    lines.append("")
    for signal_key in ["buy_individual", "buy_cluster", "sell_individual", "sell_cluster"]:
        if signal_key not in results or "by_seniority" not in results[signal_key]:
            continue
        lines.append(f"### {signal_key}")
        for seniority, data in results[signal_key]["by_seniority"].items():
            m = data["abnormal_7d"]
            lines.append(
                f"  {seniority:<12}: N={m['n']:>6}, Mean={m['mean']:>+7.2%}, "
                f"WR={m['win_rate']:>5.1%}, Sharpe={m['sharpe']:>6.2f}"
            )
        lines.append("")

    # Value breakdown
    lines.append("## Breakdown by Trade Value (7d Abnormal)")
    lines.append("")
    for signal_key in ["buy_individual", "buy_cluster", "sell_individual", "sell_cluster"]:
        if signal_key not in results or "by_value" not in results[signal_key]:
            continue
        lines.append(f"### {signal_key}")
        for bucket, data in results[signal_key]["by_value"].items():
            m = data["abnormal_7d"]
            lines.append(
                f"  {bucket:<12}: N={m['n']:>6}, Mean={m['mean']:>+7.2%}, "
                f"WR={m['win_rate']:>5.1%}, Sharpe={m['sharpe']:>6.2f}"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    import json

    print("=" * 60)
    print("Phase 1: Insider Signal Analysis")
    print("=" * 60)

    # Training period
    print(f"\nAnalyzing TRAINING period ({TRAIN_START} to {TRAIN_END})...")
    train_results = analyze_signals("Training", TRAIN_START, TRAIN_END)
    train_report = format_results(train_results, f"Training ({TRAIN_START} to {TRAIN_END})")

    # Test period (for reference — will be used in Phase 6)
    print(f"\nAnalyzing TEST period ({TEST_START} to {TEST_END})...")
    test_results = analyze_signals("Test", TEST_START, TEST_END)
    test_report = format_results(test_results, f"Test ({TEST_START} to {TEST_END})")

    # Save raw results
    output_dir = Path(__file__).parent / "data"
    output_dir.mkdir(exist_ok=True)

    with open(output_dir / "signal_analysis_train.json", "w") as f:
        json.dump(train_results, f, indent=2, default=str)
    with open(output_dir / "signal_analysis_test.json", "w") as f:
        json.dump(test_results, f, indent=2, default=str)

    # Print reports
    print("\n")
    print(train_report)
    print("\n" + "=" * 80 + "\n")
    print(test_report)

    # Save text reports
    with open(output_dir / "signal_analysis_report.txt", "w") as f:
        f.write(train_report)
        f.write("\n\n" + "=" * 80 + "\n\n")
        f.write(test_report)

    print(f"\nResults saved to {output_dir}/signal_analysis_*.json")
    return train_results, test_results


if __name__ == "__main__":
    main()
