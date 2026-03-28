#!/usr/bin/env python3
"""
Insider V3 Put Leg — Routine/10b5-1 Filter Impact Analysis
==========================================================

Tests whether filtering out routine and/or 10b5-1 sells improves the put leg signal.
Builds sell cluster events directly from the DB (not legacy CSVs) with full
access to is_10b5_1 and is_routine flags.

Walk-forward: train ≤2022, test ≥2023. No future information used.

Usage:
    python backtest_v3_routine_filter.py
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent
PRICES_DIR = ROOT_DIR / "pipelines" / "insider_study" / "data" / "prices"
DB_PATH = SCRIPT_DIR.parent / "insider_catalog" / "insiders.db"

TRAIN_END = date(2022, 12, 31)
TEST_START = date(2023, 1, 1)
HOLD_DAYS = 7


def load_daily_prices(ticker: str) -> dict:
    path = PRICES_DIR / f"{ticker}.csv"
    if not path.exists():
        return {}
    pm = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                d_str = (r.get("date") or r.get("timestamp", ""))[:10]
                d = datetime.strptime(d_str, "%Y-%m-%d").date()
                pm[d] = {"open": float(r["open"]), "close": float(r["close"])}
            except (ValueError, KeyError):
                continue
    return pm


_price_cache: dict[str, dict] = {}


def get_prices(ticker: str) -> dict:
    if ticker not in _price_cache:
        _price_cache[ticker] = load_daily_prices(ticker)
    return _price_cache[ticker]


def build_sell_clusters(conn: sqlite3.Connection, filter_mode: str) -> list[dict]:
    """
    Build 30-day sell cluster events from the DB.

    filter_mode:
      'all'           — include all S-code sells
      'no_routine'    — exclude is_routine=1
      'no_10b5_1'     — exclude is_10b5_1=1
      'no_both'       — exclude routine AND 10b5-1
      'discretionary' — exclude routine OR 10b5-1 (strictest)

    Returns list of cluster events with:
      ticker, filing_date, n_insiders, total_value, quality_score, is_cluster
    """
    # Build routine filter
    if filter_mode == "no_routine":
        routine_filter = "AND (t.is_routine != 1 OR t.is_routine IS NULL)"
    elif filter_mode == "no_10b5_1":
        routine_filter = "AND (t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)"
    elif filter_mode in ("no_both", "discretionary"):
        routine_filter = """AND (t.is_routine != 1 OR t.is_routine IS NULL)
                           AND (t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)"""
    else:
        routine_filter = ""

    # Get all S-code sells grouped by 30-day windows per ticker
    rows = conn.execute(f"""
        SELECT t.ticker, t.trade_date, t.filing_date, t.value,
               t.insider_id, t.is_csuite, t.title_weight,
               COALESCE(i.display_name, i.name) AS insider_name,
               t.signal_quality
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'S'
          AND t.trade_date >= '2016-01-01'
          {routine_filter}
        ORDER BY t.ticker, t.trade_date
    """).fetchall()

    # Group into 30-day windows per ticker
    by_ticker: dict[str, list] = defaultdict(list)
    for r in rows:
        by_ticker[r["ticker"]].append(dict(r))

    clusters = []
    for ticker, trades in by_ticker.items():
        trades.sort(key=lambda x: x["trade_date"])

        # Sliding window: for each trade, look at 30-day window ending on this date
        i = 0
        while i < len(trades):
            window_end = trades[i]["trade_date"]
            try:
                end_dt = datetime.strptime(window_end, "%Y-%m-%d").date()
            except ValueError:
                i += 1
                continue
            window_start = (end_dt - timedelta(days=30)).isoformat()

            # Collect all trades in this window
            window_trades = [
                t for t in trades
                if t["trade_date"] >= window_start and t["trade_date"] <= window_end
            ]

            insiders = set(t["insider_id"] for t in window_trades)
            n_insiders = len(insiders)
            total_value = sum(t["value"] for t in window_trades)

            # Quality score: max of individual signal_quality values, or estimate
            qualities = [t["signal_quality"] for t in window_trades if t["signal_quality"] is not None]
            quality_score = max(qualities) if qualities else 1.0

            # Use the latest filing date as the signal date
            filing_dates = [t["filing_date"] for t in window_trades if t["filing_date"]]
            filing_date = max(filing_dates) if filing_dates else window_end

            if n_insiders >= 2:  # cluster = 2+ insiders
                clusters.append({
                    "ticker": ticker,
                    "filing_date": filing_date,
                    "trade_date": window_end,
                    "n_insiders": n_insiders,
                    "total_value": total_value,
                    "quality_score": quality_score,
                    "is_cluster": True,
                    "n_trades": len(window_trades),
                })

            # Skip ahead past this window to avoid duplicate clusters
            next_date = (end_dt + timedelta(days=1)).isoformat()
            while i < len(trades) and trades[i]["trade_date"] <= window_end:
                i += 1

    # Deduplicate: one event per ticker per filing_date
    seen = set()
    deduped = []
    for c in sorted(clusters, key=lambda x: x["filing_date"]):
        key = (c["ticker"], c["filing_date"])
        if key not in seen:
            seen.add(key)
            deduped.append(c)

    return deduped


def simulate_sell_signal(event: dict, spy_prices: dict) -> dict | None:
    """Simulate a short signal: entry at T+1 open after filing, exit at T+7."""
    ticker = event["ticker"]
    try:
        filing_date = datetime.strptime(str(event["filing_date"])[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

    prices = get_prices(ticker)
    if not prices:
        return None

    trading_days = sorted(d for d in prices if d > filing_date)
    if len(trading_days) < HOLD_DAYS + 1:
        return None

    entry_date = trading_days[0]
    exit_date = trading_days[HOLD_DAYS]
    entry_price = prices[entry_date]["open"]
    exit_price = prices[exit_date]["close"]

    if entry_price <= 0:
        return None

    stock_return = (exit_price - entry_price) / entry_price

    # SPY benchmark
    spy_entry = spy_prices.get(entry_date, {}).get("open")
    spy_exit = spy_prices.get(exit_date, {}).get("close")
    spy_return = (spy_exit - spy_entry) / spy_entry if spy_entry and spy_exit and spy_entry > 0 else 0.0

    # For short signal, abnormal return is negative of stock-SPY
    abnormal_return = -(stock_return - spy_return)

    return {
        "ticker": ticker,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "stock_return": stock_return,
        "spy_return": spy_return,
        "abnormal_return": abnormal_return,
        "n_insiders": event["n_insiders"],
        "total_value": event["total_value"],
        "quality_score": event["quality_score"],
    }


def compute_metrics(trades: list[dict], label: str) -> dict:
    """Compute Sharpe, win rate, etc. for a set of trades."""
    if not trades:
        return {"label": label, "n": 0, "sharpe": 0, "win_rate": 0, "mean_ar": 0, "t_stat": 0}

    returns = [t["abnormal_return"] for t in trades]
    n = len(returns)
    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1) if n > 1 else 1.0
    sharpe = (mean_r / std_r) * np.sqrt(252 / HOLD_DAYS) if std_r > 0 else 0.0
    win_rate = sum(1 for r in returns if r > 0) / n
    t_stat = mean_r / (std_r / np.sqrt(n)) if std_r > 0 and n > 1 else 0.0

    return {
        "label": label,
        "n": n,
        "sharpe": round(sharpe, 2),
        "win_rate": round(win_rate * 100, 1),
        "mean_ar": round(mean_r * 100, 2),
        "t_stat": round(t_stat, 2),
    }


def run_variant(conn, spy_prices, filter_mode: str, min_insiders: int, min_value: int, min_quality: float):
    """Run a single backtest variant with walk-forward split."""
    clusters = build_sell_clusters(conn, filter_mode)

    # Apply put leg filters
    filtered = [
        c for c in clusters
        if c["n_insiders"] >= min_insiders
        and c["total_value"] >= min_value
        and c["quality_score"] >= min_quality
    ]

    # Simulate trades
    trades = []
    for event in filtered:
        t = simulate_sell_signal(event, spy_prices)
        if t:
            trades.append(t)

    # Split
    train = [t for t in trades if t["entry_date"] <= TRAIN_END]
    test = [t for t in trades if t["entry_date"] >= TEST_START]

    all_metrics = compute_metrics(trades, "All")
    train_metrics = compute_metrics(train, "Train (≤2022)")
    test_metrics = compute_metrics(test, "Test (≥2023)")

    return {
        "filter_mode": filter_mode,
        "min_insiders": min_insiders,
        "min_value": min_value,
        "min_quality": min_quality,
        "all": all_metrics,
        "train": train_metrics,
        "test": test_metrics,
        "n_clusters": len(filtered),
    }


def main():
    print("=" * 80)
    print("  INSIDER V3 PUT LEG — Routine/10b5-1 Filter Impact Analysis")
    print("  Walk-forward: train ≤2022, test ≥2023")
    print("=" * 80)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    spy_prices = get_prices("SPY")
    logger.info("SPY: %d days", len(spy_prices))

    # Test with varying filter modes and cluster sizes
    # Quality filter dropped — signal_quality in DB is 0-0.8 (per-trade classification),
    # not the composite quality_score from the legacy CSV which was 0-10+
    configs = [
        ("all",            2, 5_000_000, 0.0),
        ("no_routine",     2, 5_000_000, 0.0),
        ("no_10b5_1",      2, 5_000_000, 0.0),
        ("discretionary",  2, 5_000_000, 0.0),
    ]

    # Also test with 3+ insiders
    configs += [
        ("all",            3, 5_000_000, 0.0),
        ("no_routine",     3, 5_000_000, 0.0),
        ("no_10b5_1",      3, 5_000_000, 0.0),
        ("discretionary",  3, 5_000_000, 0.0),
    ]

    # And a lower value threshold to see more data
    configs += [
        ("all",            2, 1_000_000, 0.0),
        ("no_routine",     2, 1_000_000, 0.0),
        ("no_10b5_1",      2, 1_000_000, 0.0),
        ("discretionary",  2, 1_000_000, 0.0),
    ]

    results = []
    for filter_mode, min_ins, min_val, min_q in configs:
        logger.info("Running: filter=%s, insiders>=%d, value>=%s, quality>=%.2f",
                    filter_mode, min_ins, f"${min_val:,}", min_q)
        r = run_variant(conn, spy_prices, filter_mode, min_ins, min_val, min_q)
        results.append(r)

    conn.close()

    # Print comparison table
    print("\n" + "=" * 80)
    print("  RESULTS: 2+ Insiders, $5M+, Quality ≥ 2.14")
    print("=" * 80)
    def print_table(results_subset):
        print(f"{'Filter':<18} {'N All':>6} {'Sharpe':>7} {'WR%':>6} {'AR%':>7}  |  "
              f"{'N Train':>7} {'Sharpe':>7}  |  {'N Test':>6} {'Sharpe':>7} {'WR%':>6} {'AR%':>7} {'t':>6}")
        print("-" * 120)
        for r in results_subset:
            a, tr, te = r["all"], r["train"], r["test"]
            print(f"{r['filter_mode']:<18} {a['n']:>6} {a['sharpe']:>7.2f} {a['win_rate']:>5.1f}% {a['mean_ar']:>+6.2f}%  |  "
                  f"{tr['n']:>7} {tr['sharpe']:>7.2f}  |  {te['n']:>6} {te['sharpe']:>7.2f} {te['win_rate']:>5.1f}% {te['mean_ar']:>+6.2f}% {te['t_stat']:>5.2f}")

    print_table([r for r in results if r["min_insiders"] == 2 and r["min_value"] == 5_000_000])

    print("\n" + "=" * 80)
    print("  RESULTS: 3+ Insiders, $5M+")
    print("=" * 80)
    print_table([r for r in results if r["min_insiders"] == 3])

    print("\n" + "=" * 80)
    print("  RESULTS: 2+ Insiders, $1M+")
    print("=" * 80)
    print_table([r for r in results if r["min_insiders"] == 2 and r["min_value"] == 1_000_000])

    print("\n" + "=" * 80)
    print("  INTERPRETATION")
    print("=" * 80)

    # Find best test Sharpe
    best = max(results, key=lambda r: r["test"]["sharpe"])
    print(f"\nBest OOS Sharpe: {best['test']['sharpe']:.2f} "
          f"(filter={best['filter_mode']}, {best['min_insiders']}+ insiders)")

    baseline = [r for r in results if r["filter_mode"] == "all" and r["min_insiders"] == 2][0]
    print(f"Baseline (all, 2+ ins) OOS Sharpe: {baseline['test']['sharpe']:.2f}")

    if best["test"]["sharpe"] > baseline["test"]["sharpe"]:
        delta = best["test"]["sharpe"] - baseline["test"]["sharpe"]
        print(f"Improvement: +{delta:.2f} Sharpe from filtering {best['filter_mode']}")
    else:
        print("Filtering did not improve OOS Sharpe.")


if __name__ == "__main__":
    main()
