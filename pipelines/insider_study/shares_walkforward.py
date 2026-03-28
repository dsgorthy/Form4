#!/usr/bin/env python3
"""
Shares Walk-Forward Validation for Insider Cluster-Buy Strategy
================================================================
Addresses Board conditional verdicts by providing:
1. Train/test split on shares (2020-2022 vs 2023-2025)
2. Per-year filtered Sharpe on the 204 filtered events
3. -15% stop-loss applied via daily price data
4. Max drawdown with 5% position sizing

Reuses load_and_filter_data() from exit_strategy_analysis.py.

Author: Claude Opus 4.6
Date: 2026-02-28
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRICES_DIR = os.path.join(SCRIPT_DIR, "data", "prices")
REPORT_PATH = os.path.join(SCRIPT_DIR, "..", "..", "reports", "SHARES_WALKFORWARD_REPORT.md")
RESULTS_JSON_PATH = os.path.join(SCRIPT_DIR, "data", "shares_walkforward_results.json")

# Import shared data loader
sys.path.insert(0, SCRIPT_DIR)
from exit_strategy_analysis import load_and_filter_data, compute_stats

# Position sizing
PORTFOLIO_VALUE = 30_000
POSITION_SIZE_PCT = 0.05  # 5% per trade
ANNUAL_TRADING_DAYS = 252
HOLD_DAYS = 7
STOP_LOSS = -0.15  # -15%

# Walk-forward split
TRAIN_END = date(2022, 12, 31)   # Train: 2020-01 to 2022-12
TEST_START = date(2023, 1, 1)    # Test: 2023-01 to 2025-12


def load_daily_prices(ticker: str) -> list[dict]:
    """Load daily OHLCV from the prices directory."""
    path = os.path.join(PRICES_DIR, f"{ticker}.csv")
    if not os.path.exists(path):
        return []
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                ts = r["timestamp"][:10]
                rows.append({
                    "date": datetime.strptime(ts, "%Y-%m-%d").date(),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                })
            except (ValueError, KeyError):
                continue
    return sorted(rows, key=lambda x: x["date"])


def simulate_share_trade_with_stop(event: dict, stop_loss: float | None) -> dict:
    """
    Simulate a shares trade: enter at open on entry_date, hold 7 trading days,
    with optional intraday stop-loss using daily low prices.

    Returns dict with trade metrics.
    """
    ticker = event["ticker"]
    entry_date = event["entry_date"]
    entry_price = event["entry_price"]
    spy_return = event["spy_return"]  # already as fraction

    prices = load_daily_prices(ticker)
    if not prices:
        # Fall back to original event data (no stop applied)
        ar = event["abnormal_return"]
        return {
            "ticker": ticker,
            "entry_date": entry_date,
            "abnormal_return": ar,
            "trade_return": event["trade_return"],
            "stop_hit": False,
            "exit_day": HOLD_DAYS,
            "has_daily_data": False,
        }

    # Build date->price lookup
    price_map = {p["date"]: p for p in prices}

    # Find the entry date and the next HOLD_DAYS trading days
    trading_dates = [p["date"] for p in prices if p["date"] >= entry_date]
    if not trading_dates:
        ar = event["abnormal_return"]
        return {
            "ticker": ticker,
            "entry_date": entry_date,
            "abnormal_return": ar,
            "trade_return": event["trade_return"],
            "stop_hit": False,
            "exit_day": HOLD_DAYS,
            "has_daily_data": False,
        }

    # Take entry day + next HOLD_DAYS trading days
    hold_dates = trading_dates[:HOLD_DAYS + 1]

    stop_hit = False
    exit_price = None
    exit_day = HOLD_DAYS

    for i, d in enumerate(hold_dates):
        bar = price_map[d]

        if stop_loss is not None and i > 0:
            # Check if intraday low breaches stop
            stop_price = entry_price * (1.0 + stop_loss)
            if bar["low"] <= stop_price:
                exit_price = stop_price  # Assume fill at stop level
                stop_hit = True
                exit_day = i
                break

    if exit_price is None:
        # No stop hit — use close of last hold day
        if len(hold_dates) > HOLD_DAYS:
            last_bar = price_map[hold_dates[HOLD_DAYS]]
            exit_price = last_bar["close"]
        else:
            last_bar = price_map[hold_dates[-1]]
            exit_price = last_bar["close"]
        exit_day = min(HOLD_DAYS, len(hold_dates) - 1)

    trade_return = (exit_price - entry_price) / entry_price
    abnormal_return = trade_return - spy_return

    return {
        "ticker": ticker,
        "entry_date": entry_date,
        "abnormal_return": abnormal_return,
        "trade_return": trade_return,
        "stop_hit": stop_hit,
        "exit_day": exit_day,
        "has_daily_data": True,
    }


def compute_max_drawdown(returns: list[float], sizing_pct: float = 0.05) -> float:
    """
    Compute max portfolio drawdown with position sizing.
    Each trade risks sizing_pct of portfolio.
    """
    if not returns:
        return 0.0

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    for r in returns:
        pnl = sizing_pct * r
        equity += pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd

    return max_dd


def compute_max_consecutive_losses(returns: list[float]) -> int:
    """Count max consecutive losing trades (abnormal_return < 0)."""
    max_streak = 0
    current = 0
    for r in returns:
        if r < 0:
            current += 1
            max_streak = max(max_streak, current)
        else:
            current = 0
    return max_streak


def compute_period_stats(events: list[dict], label: str) -> dict:
    """Compute full stats for a period."""
    if not events:
        return {"label": label, "n": 0}

    returns = [e["abnormal_return"] for e in events]
    arr = np.array(returns)
    n = len(arr)
    mean = float(np.mean(arr))
    median = float(np.median(arr))
    std = float(np.std(arr, ddof=1)) if n > 1 else 0.0

    periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS
    sharpe = (mean / std) * math.sqrt(periods_per_year) if std > 0 else 0.0

    win_rate = float(np.mean(arr > 0))
    max_dd = compute_max_drawdown(returns, POSITION_SIZE_PCT)
    max_consec = compute_max_consecutive_losses(returns)
    stops_hit = sum(1 for e in events if e.get("stop_hit", False))

    # t-test
    t_stat = (mean / (std / math.sqrt(n))) if std > 0 and n > 1 else 0.0

    return {
        "label": label,
        "n": n,
        "mean_ar": mean,
        "median_ar": median,
        "std": std,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "max_dd_pct": max_dd * 100,
        "max_consecutive_losses": max_consec,
        "t_statistic": t_stat,
        "stops_hit": stops_hit,
        "stops_pct": stops_hit / n if n > 0 else 0,
    }


def main():
    print("=" * 70)
    print("  SHARES WALK-FORWARD VALIDATION — Insider Cluster Buy")
    print("=" * 70)

    # Load filtered events
    events = load_and_filter_data()
    print(f"\nLoaded {len(events)} filtered events")

    # Simulate all trades with -15% stop
    print(f"\nSimulating trades with {STOP_LOSS*100:.0f}% stop-loss using daily prices...")
    results_with_stop = []
    results_no_stop = []
    for ev in events:
        r_stop = simulate_share_trade_with_stop(ev, STOP_LOSS)
        r_nostop = simulate_share_trade_with_stop(ev, None)
        results_with_stop.append(r_stop)
        results_no_stop.append(r_nostop)

    daily_data_count = sum(1 for r in results_with_stop if r.get("has_daily_data"))
    print(f"  Events with daily price data: {daily_data_count}/{len(events)}")
    print(f"  Stop-loss hits: {sum(1 for r in results_with_stop if r['stop_hit'])}")

    # Sort by entry_date for sequential analysis
    results_with_stop.sort(key=lambda x: x["entry_date"])
    results_no_stop.sort(key=lambda x: x["entry_date"])

    # Split train / test
    train_stop = [r for r in results_with_stop if r["entry_date"] <= TRAIN_END]
    test_stop = [r for r in results_with_stop if r["entry_date"] >= TEST_START]
    train_nostop = [r for r in results_no_stop if r["entry_date"] <= TRAIN_END]
    test_nostop = [r for r in results_no_stop if r["entry_date"] >= TEST_START]

    print(f"\n--- Walk-Forward Split ---")
    print(f"  Train (2020-2022): {len(train_stop)} events")
    print(f"  Test  (2023-2025): {len(test_stop)} events")

    # Compute period stats
    all_stop = compute_period_stats(results_with_stop, "All (with -15% stop)")
    all_nostop = compute_period_stats(results_no_stop, "All (no stop)")
    train_s = compute_period_stats(train_stop, "Train 2020-2022 (with stop)")
    test_s = compute_period_stats(test_stop, "Test 2023-2025 (with stop)")
    train_ns = compute_period_stats(train_nostop, "Train 2020-2022 (no stop)")
    test_ns = compute_period_stats(test_nostop, "Test 2023-2025 (no stop)")

    # Per-year filtered Sharpe (on the 204 filtered events, with stop)
    year_stats = {}
    for year in range(2020, 2026):
        yr_events = [r for r in results_with_stop
                     if r["entry_date"].year == year]
        if yr_events:
            ys = compute_period_stats(yr_events, str(year))
            year_stats[year] = ys

    # Print results
    print(f"\n{'=' * 70}")
    print("  WALK-FORWARD RESULTS — Shares with -15% Stop")
    print(f"{'=' * 70}")

    def print_stats(s: dict):
        if s["n"] == 0:
            print(f"  {s['label']}: No events")
            return
        print(f"\n  {s['label']}:")
        print(f"    N events:              {s['n']}")
        print(f"    Mean Abnormal Return:  {s['mean_ar']*100:+.2f}%")
        print(f"    Median AR:             {s['median_ar']*100:+.2f}%")
        print(f"    Std Dev:               {s['std']*100:.2f}%")
        print(f"    Annualized Sharpe:     {s['sharpe']:.2f}")
        print(f"    Win Rate:              {s['win_rate']*100:.1f}%")
        print(f"    Max Portfolio DD:      {s['max_dd_pct']:.2f}%")
        print(f"    Max Consec Losses:     {s['max_consecutive_losses']}")
        print(f"    t-statistic:           {s['t_statistic']:.2f}")
        print(f"    Stops Hit:             {s['stops_hit']} ({s['stops_pct']*100:.1f}%)")

    print_stats(all_stop)
    print_stats(train_s)
    print_stats(test_s)

    print(f"\n  --- Comparison: No Stop ---")
    print_stats(all_nostop)
    print_stats(train_ns)
    print_stats(test_ns)

    # Degradation
    if train_s["n"] > 0 and test_s["n"] > 0 and train_s["sharpe"] != 0:
        deg = (test_s["sharpe"] - train_s["sharpe"]) / abs(train_s["sharpe"]) * 100
        print(f"\n  Walk-Forward Degradation: {deg:+.1f}%")
        print(f"    Train Sharpe: {train_s['sharpe']:.2f} → Test Sharpe: {test_s['sharpe']:.2f}")

    # Per-year table
    print(f"\n{'=' * 70}")
    print("  PER-YEAR FILTERED SHARPE (204 events, -15% stop)")
    print(f"{'=' * 70}")
    print(f"\n  {'Year':<6} {'N':>4} {'Mean AR':>10} {'Win Rate':>10} {'Sharpe':>8} {'Max DD':>8} {'Stops':>6}")
    print(f"  {'-'*6} {'-'*4} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*6}")
    for year in sorted(year_stats.keys()):
        ys = year_stats[year]
        print(f"  {year:<6} {ys['n']:>4} {ys['mean_ar']*100:>+9.2f}% {ys['win_rate']*100:>9.1f}% {ys['sharpe']:>8.2f} {ys['max_dd_pct']:>7.2f}% {ys['stops_hit']:>6}")

    # Generate markdown report
    report = []
    report.append("# Shares Walk-Forward Validation — Insider Cluster Buy\n")
    report.append(f"**Date:** {date.today().isoformat()}")
    report.append(f"**Dataset:** {len(events)} filtered events (cluster 2+, $5M+, quality 2.0+)")
    report.append(f"**Split:** Train 2020-2022 | Test 2023-2025")
    report.append(f"**Stop-loss:** -15% intraday (daily low check)\n")

    report.append("## Walk-Forward Results (Shares, -15% Stop)\n")
    report.append("| Metric | Train (2020-2022) | Test (2023-2025) | All |")
    report.append("|--------|-------------------|-------------------|-----|")

    def fmt(val, fmt_str):
        return fmt_str.format(val)

    for metric, key, f in [
        ("N", "n", "{}"),
        ("Mean Abnormal Return", "mean_ar", "{:+.2%}"),
        ("Median AR", "median_ar", "{:+.2%}"),
        ("Annualized Sharpe", "sharpe", "{:.2f}"),
        ("Win Rate", "win_rate", "{:.1%}"),
        ("Max Portfolio DD (5%)", "max_dd_pct", "{:.2f}%"),
        ("Max Consec Losses", "max_consecutive_losses", "{}"),
        ("t-statistic", "t_statistic", "{:.2f}"),
        ("Stops Hit", "stops_hit", "{}"),
    ]:
        report.append(f"| {metric} | {fmt(train_s[key], f)} | {fmt(test_s[key], f)} | {fmt(all_stop[key], f)} |")

    report.append(f"\n## Per-Year Filtered Sharpe (N=204, -15% stop)\n")
    report.append("| Year | N | Mean AR | Win Rate | Sharpe | Max DD | Stops Hit |")
    report.append("|------|---|---------|----------|--------|--------|-----------|")
    for year in sorted(year_stats.keys()):
        ys = year_stats[year]
        report.append(f"| {year} | {ys['n']} | {ys['mean_ar']:+.2%} | {ys['win_rate']:.1%} | {ys['sharpe']:.2f} | {ys['max_dd_pct']:.2f}% | {ys['stops_hit']} |")

    report.append(f"\n## Stop-Loss Impact\n")
    report.append("| Metric | No Stop | With -15% Stop |")
    report.append("|--------|---------|----------------|")
    for metric, key, f in [
        ("Sharpe (all)", "sharpe", "{:.2f}"),
        ("Mean AR (all)", "mean_ar", "{:+.2%}"),
        ("Max Portfolio DD", "max_dd_pct", "{:.2f}%"),
        ("Max Consec Losses", "max_consecutive_losses", "{}"),
    ]:
        report.append(f"| {metric} | {fmt(all_nostop[key], f)} | {fmt(all_stop[key], f)} |")

    # Write report
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(report) + "\n")
    print(f"\nReport saved: {REPORT_PATH}")

    # Save results as JSON for backtest_latest.json consumption
    output = {
        "walk_forward_shares": {
            "train": {
                "period": "2020-2022",
                "n": train_s["n"],
                "sharpe": round(train_s["sharpe"], 2),
                "mean_ar": round(train_s["mean_ar"], 4),
                "win_rate": round(train_s["win_rate"], 4),
                "max_dd_pct": round(train_s["max_dd_pct"], 2),
                "max_consecutive_losses": train_s["max_consecutive_losses"],
                "t_statistic": round(train_s["t_statistic"], 2),
            },
            "test": {
                "period": "2023-2025",
                "n": test_s["n"],
                "sharpe": round(test_s["sharpe"], 2),
                "mean_ar": round(test_s["mean_ar"], 4),
                "win_rate": round(test_s["win_rate"], 4),
                "max_dd_pct": round(test_s["max_dd_pct"], 2),
                "max_consecutive_losses": test_s["max_consecutive_losses"],
                "t_statistic": round(test_s["t_statistic"], 2),
            },
            "all": {
                "n": all_stop["n"],
                "sharpe": round(all_stop["sharpe"], 2),
                "mean_ar": round(all_stop["mean_ar"], 4),
                "win_rate": round(all_stop["win_rate"], 4),
                "max_dd_pct": round(all_stop["max_dd_pct"], 2),
            },
            "degradation_pct": round(
                (test_s["sharpe"] - train_s["sharpe"]) / abs(train_s["sharpe"]) * 100, 1
            ) if train_s["sharpe"] != 0 else None,
        },
        "per_year_filtered_sharpe": {
            str(year): {
                "n": ys["n"],
                "sharpe": round(ys["sharpe"], 2),
                "mean_ar": round(ys["mean_ar"], 4),
                "win_rate": round(ys["win_rate"], 4),
                "max_dd_pct": round(ys["max_dd_pct"], 2),
            }
            for year, ys in sorted(year_stats.items())
        },
        "stop_loss_comparison": {
            "with_stop": {
                "sharpe": round(all_stop["sharpe"], 2),
                "mean_ar": round(all_stop["mean_ar"], 4),
                "max_dd_pct": round(all_stop["max_dd_pct"], 2),
            },
            "no_stop": {
                "sharpe": round(all_nostop["sharpe"], 2),
                "mean_ar": round(all_nostop["mean_ar"], 4),
                "max_dd_pct": round(all_nostop["max_dd_pct"], 2),
            },
        },
    }

    with open(RESULTS_JSON_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"JSON results saved: {RESULTS_JSON_PATH}")

    print(f"\n{'=' * 70}")
    print("  DONE")
    print(f"{'=' * 70}")

    return output


if __name__ == "__main__":
    main()
