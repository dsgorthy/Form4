#!/usr/bin/env python3
"""
V3 Sweep — Put Leg Subgroups + Smart Insider Buy Leg
=====================================================

Part A: Put leg — Analyze existing grid search results (real Theta Data)
  + validate top configs with stock-return walk-forward

Part B: Buy leg — Join events to insider_track_records,
  test filter cascades with walk-forward

Usage:
    python sweep_v3.py
    python sweep_v3.py --part buy
    python sweep_v3.py --part put
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent
STUDY_DIR = ROOT_DIR / "pipelines" / "insider_study"
PRICES_DIR = STUDY_DIR / "data" / "prices"
DB_PATH = SCRIPT_DIR.parent / "insider_catalog" / "insiders.db"
REPORT_DIR = ROOT_DIR / "reports" / "insider_v2"

GRID_SEARCH_SELLS = STUDY_DIR / "data" / "grid_search_results_sells.csv"
SELL_EVENTS_CSV = STUDY_DIR / "data" / "results_sells_7d.csv"
BUY_EVENTS_CSV = STUDY_DIR / "data" / "events_v2_buys.csv"

ANNUAL_TRADING_DAYS = 252
TRAIN_END = date(2022, 12, 31)
TEST_START = date(2023, 1, 1)

# Position sizing
BUY_POSITION_PCT = 0.05
PUT_POSITION_PCT = 0.01


# ═══════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ═══════════════════════════════════════════════════════════════════

def load_daily_prices(ticker: str) -> dict:
    path = PRICES_DIR / f"{ticker}.csv"
    if not path.exists():
        return {}
    pm = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            try:
                d = datetime.strptime(r["timestamp"][:10], "%Y-%m-%d").date()
                pm[d] = {"open": float(r["open"]), "high": float(r["high"]),
                         "low": float(r["low"]), "close": float(r["close"])}
            except (ValueError, KeyError):
                continue
    return pm


def compute_stats(returns: list[float], hold_days: int = 7, label: str = "") -> dict:
    if not returns:
        return {"label": label, "n": 0, "sharpe": 0, "win_rate": 0, "mean_ar": 0,
                "t_stat": 0, "max_dd_pct": 0}
    arr = np.array(returns)
    n = len(arr)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1)) if n > 1 else 0.001
    sharpe = (mean / std) * math.sqrt(ANNUAL_TRADING_DAYS / hold_days) if std > 0 else 0
    wr = float(np.mean(arr > 0))
    t = mean / (std / n**0.5) if std > 0 and n > 1 else 0
    # Max DD with position sizing
    equity = 1.0; peak = 1.0; max_dd = 0.0
    for r in arr:
        equity += BUY_POSITION_PCT * r
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak if peak > 0 else 0)
    return {
        "label": label, "n": n, "mean_ar": mean, "median_ar": float(np.median(arr)),
        "std": std, "sharpe": sharpe, "win_rate": wr, "t_stat": t,
        "max_dd_pct": max_dd * 100,
    }


def normalize_name(name: str) -> str:
    """Match backfill.py name normalization."""
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"\b(jr\.?|sr\.?|iii|ii|iv|v|esq\.?|phd|md)\b", "", n)
    n = re.sub(r"[^a-z\s]", "", n)
    return " ".join(n.split())


# ═══════════════════════════════════════════════════════════════════
# PART A: PUT LEG SUBGROUP ANALYSIS
# ═══════════════════════════════════════════════════════════════════

def run_put_sweep():
    """
    1. Load existing grid search results (real Theta Data options pricing)
    2. Find Pareto-optimal configs (max Sharpe for given N)
    3. For top configs, run stock-return walk-forward to validate signal OOS
    """
    print("\n" + "=" * 70)
    print("  PART A: PUT LEG SUBGROUP ANALYSIS")
    print("=" * 70)

    # ── Load grid search results ──
    with open(GRID_SEARCH_SELLS) as f:
        grid = list(csv.DictReader(f))
    logger.info("Grid search configs: %d", len(grid))

    # Parse numeric fields
    for r in grid:
        r["_n"] = int(r["n_trades"])
        r["_sharpe"] = float(r["sharpe"])
        r["_wr"] = float(r["win_rate"])
        r["_mean"] = float(r["mean_return"])
        r["_med"] = float(r["median_return"])
        r["_pnl"] = float(r["total_dollar_pnl"])
        r["_dd"] = float(r["max_drawdown"])
        r["_ins"] = int(r["min_insiders"])
        r["_val"] = int(r["min_value"])
        r["_q"] = float(r["min_quality"])

    # ── Filter: N >= 50, positive Sharpe ──
    viable = [r for r in grid if r["_n"] >= 50 and r["_sharpe"] > 0]
    viable.sort(key=lambda r: -r["_sharpe"])
    logger.info("Viable configs (N>=50, Sharpe>0): %d / %d", len(viable), len(grid))

    # ── Pareto frontier: for each N bucket, best Sharpe ──
    print("\n--- Top 25 configs by Sharpe (N >= 50) ---")
    print(f"{'#':>3} | {'Ins':>3} | {'Val':>6} | {'Q':>4} | {'Hold':>4} | {'DTE':>11} | "
          f"{'Strike':>10} | {'Stop':>5} | {'N':>5} | {'Sharpe':>7} | {'WR':>5} | "
          f"{'Mean':>8} | {'Median':>8} | {'$PnL':>9}")
    print("-" * 120)
    for i, r in enumerate(viable[:25]):
        vs = f"${r['_val']/1e6:.1f}M"
        ss = r["stop_loss"] if r["stop_loss"] != "none" else "none"
        print(f"{i+1:>3} | {r['_ins']:>3} | {vs:>6} | {r['_q']:>4.1f} | "
              f"{r['hold_days']:>4}d | {r['dte_type']:>11} | {r['strike_type']:>10} | "
              f"{ss:>5} | {r['_n']:>5} | {r['_sharpe']:>7.2f} | {r['_wr']:>4.1%} | "
              f"{r['_mean']:>+7.2%} | {r['_med']:>+7.2%} | ${r['_pnl']:>8,.0f}")

    # ── Aggregate by signal filter (avg across all strategy params) ──
    print("\n--- Signal filter ranking (avg Sharpe across strategy params, N>=50) ---")
    signal_stats = defaultdict(list)
    for r in grid:
        if r["_n"] >= 50:
            key = (r["_ins"], r["_val"], r["_q"])
            signal_stats[key].append(r["_sharpe"])

    ranked_signals = sorted(signal_stats.items(), key=lambda x: -np.mean(x[1]))
    print(f"{'Ins':>3} | {'Val':>6} | {'Q':>4} | {'Avg Sharpe':>11} | {'% Positive':>11} | {'N configs':>9}")
    print("-" * 60)
    for (mi, mv, mq), sharpes in ranked_signals:
        vs = f"${int(mv)/1e6:.1f}M"
        pos_pct = sum(1 for s in sharpes if s > 0) / len(sharpes) if sharpes else 0
        print(f"{mi:>3} | {vs:>6} | {mq:>4.1f} | {np.mean(sharpes):>11.3f} | "
              f"{pos_pct:>10.1%} | {len(sharpes):>9}")

    # ── Aggregate by hold period ──
    print("\n--- Hold period ranking (N>=50 configs only) ---")
    hold_stats = defaultdict(list)
    for r in grid:
        if r["_n"] >= 50:
            hold_stats[r["hold_days"]].append(r["_sharpe"])
    for hd in ["7", "14", "30", "60"]:
        s = hold_stats.get(hd, [])
        if s:
            print(f"  {hd}d: avg Sharpe={np.mean(s):.3f}, "
                  f"{sum(1 for x in s if x > 0)}/{len(s)} positive")

    # ── Aggregate by strike type ──
    print("\n--- Strike ranking (N>=50 configs only) ---")
    strike_stats = defaultdict(list)
    for r in grid:
        if r["_n"] >= 50:
            strike_stats[r["strike_type"]].append(r["_sharpe"])
    for st in ["5pct_itm", "atm", "5pct_otm", "10pct_otm"]:
        s = strike_stats.get(st, [])
        if s:
            print(f"  {st}: avg Sharpe={np.mean(s):.3f}, "
                  f"{sum(1 for x in s if x > 0)}/{len(s)} positive")

    # ── Stock-return walk-forward for top signal configs ──
    print("\n--- Walk-forward validation (stock returns) for top signal filters ---")
    sell_events = pd.read_csv(SELL_EVENTS_CSV)
    sell_events["entry_date"] = pd.to_datetime(sell_events["entry_date"], errors="coerce")
    sell_events["_entry_date"] = sell_events["entry_date"].dt.date

    # Test the top signal filter combos
    wf_results = []
    test_filters = [
        (3, 5_000_000, 2.14, "3ins $5M q2.14 (grid #1)"),
        (2, 5_000_000, 2.14, "2ins $5M q2.14 (grid #2)"),
        (3, 5_000_000, 1.50, "3ins $5M q1.5"),
        (3, 1_000_000, 2.14, "3ins $1M q2.14"),
        (5, 1_000_000, 2.14, "5ins $1M q2.14"),
        (5, 5_000_000, 0.00, "5ins $5M any_q"),
        (2, 1_000_000, 0.00, "2ins $1M any_q (V2 baseline)"),
        (3, 5_000_000, 0.00, "3ins $5M any_q"),
        (5, 500_000, 2.14, "5ins $500K q2.14"),
        (2, 5_000_000, 1.50, "2ins $5M q1.5"),
    ]

    for min_ins, min_val, min_q, label in test_filters:
        mask = (
            (sell_events["n_insiders"] >= min_ins) &
            (sell_events["total_value"] >= min_val) &
            (sell_events["quality_score"] >= min_q if min_q > 0 else True)
        )
        filtered = sell_events[mask].copy()
        if filtered.empty:
            continue

        # Abnormal return is for the LONG side; for shorts, negate it
        all_returns = (-filtered["abnormal_return"] / 100).tolist()
        train_returns = (-filtered[filtered["_entry_date"] <= TRAIN_END]["abnormal_return"] / 100).tolist()
        test_returns = (-filtered[filtered["_entry_date"] >= TEST_START]["abnormal_return"] / 100).tolist()

        all_s = compute_stats(all_returns, 7, f"All {label}")
        train_s = compute_stats(train_returns, 7, f"Train {label}")
        test_s = compute_stats(test_returns, 7, f"Test {label}")

        # Look up matching grid search Sharpe for this signal combo
        grid_sharpes = [r["_sharpe"] for r in viable
                        if r["_ins"] == min_ins and r["_val"] == min_val
                        and abs(r["_q"] - min_q) < 0.01]
        best_grid_sharpe = max(grid_sharpes) if grid_sharpes else 0

        deg = ((test_s["sharpe"] - train_s["sharpe"]) / abs(train_s["sharpe"]) * 100
               if train_s["sharpe"] != 0 else 0)

        wf_results.append({
            "filter": label,
            "min_insiders": min_ins,
            "min_value": min_val,
            "min_quality": min_q,
            "n_all": all_s["n"],
            "n_train": train_s["n"],
            "n_test": test_s["n"],
            "sharpe_all": all_s["sharpe"],
            "sharpe_train": train_s["sharpe"],
            "sharpe_test": test_s["sharpe"],
            "wr_all": all_s["win_rate"],
            "wr_test": test_s["win_rate"],
            "mean_ar_all": all_s["mean_ar"],
            "mean_ar_test": test_s["mean_ar"],
            "degradation_pct": deg,
            "best_grid_sharpe": best_grid_sharpe,
        })

    print(f"\n{'Filter':<30} | {'N_all':>5} | {'N_test':>6} | {'Sh_all':>6} | "
          f"{'Sh_train':>8} | {'Sh_test':>7} | {'WR_test':>7} | {'Degrad':>7} | {'Grid Best':>9}")
    print("-" * 120)
    for r in sorted(wf_results, key=lambda x: -x["sharpe_test"]):
        print(f"{r['filter']:<30} | {r['n_all']:>5} | {r['n_test']:>6} | "
              f"{r['sharpe_all']:>6.2f} | {r['sharpe_train']:>8.2f} | "
              f"{r['sharpe_test']:>7.2f} | {r['wr_test']:>6.1%} | "
              f"{r['degradation_pct']:>+6.1f}% | {r['best_grid_sharpe']:>9.2f}")

    return wf_results


# ═══════════════════════════════════════════════════════════════════
# PART B: BUY LEG — SMART INSIDER FILTER
# ═══════════════════════════════════════════════════════════════════

def build_insider_lookup(db_path: Path) -> tuple[dict, dict, dict]:
    """
    Build lookups from insiders.db:
    1. name_normalized -> (insider_id, score_tier, buy_win_rate_7d, buy_avg_abnormal_7d, buy_count)
    2. (insider_id, ticker) -> trade_count at that company
    3. insider_id -> primary_title
    """
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    # Track records
    c.execute("""
        SELECT i.name_normalized, i.insider_id,
               COALESCE(tr.score_tier, 0), COALESCE(tr.buy_win_rate_7d, 0),
               COALESCE(tr.buy_avg_abnormal_7d, 0), COALESCE(tr.buy_count, 0),
               COALESCE(tr.score, 0), COALESCE(tr.primary_title, '')
        FROM insiders i
        LEFT JOIN insider_track_records tr ON i.insider_id = tr.insider_id
        WHERE i.name_normalized IS NOT NULL AND i.name_normalized != ''
    """)
    name_to_record = {}
    for name_norm, iid, tier, wr7, abn7, bc, score, title in c.fetchall():
        # Keep the best record per normalized name (some names may appear multiple times)
        if name_norm not in name_to_record or tier > name_to_record[name_norm][1]:
            name_to_record[name_norm] = (iid, tier, wr7, abn7, bc, score, title)

    logger.info("Insider records: %d unique normalized names", len(name_to_record))

    # Company depth
    c.execute("SELECT insider_id, ticker, trade_count FROM insider_companies")
    company_depth = {}
    for iid, ticker, tc in c.fetchall():
        company_depth[(iid, ticker)] = tc

    logger.info("Company depth records: %d", len(company_depth))

    conn.close()
    return name_to_record, company_depth


def score_event(event_row: dict, name_to_record: dict, company_depth: dict) -> dict:
    """
    Score a buy event based on insider track records.
    Returns enriched event dict with insider quality metrics.
    """
    ticker = event_row.get("ticker", "")
    insider_names_raw = str(event_row.get("insider_names", ""))
    names = [n.strip() for n in insider_names_raw.split(";") if n.strip()]

    best_tier = 0
    best_wr = 0.0
    best_abn = 0.0
    best_depth = 0
    any_tier2_plus = False
    any_depth5_plus = False
    any_wr60_plus = False
    matched_insiders = 0

    for name in names:
        norm = normalize_name(name)
        rec = name_to_record.get(norm)
        if rec is None:
            continue

        iid, tier, wr7, abn7, bc, score, title = rec
        matched_insiders += 1
        depth = company_depth.get((iid, ticker), 0)

        if tier > best_tier:
            best_tier = tier
        if wr7 > best_wr:
            best_wr = wr7
        if abn7 > best_abn:
            best_abn = abn7
        if depth > best_depth:
            best_depth = depth

        if tier >= 2:
            any_tier2_plus = True
        if depth >= 5:
            any_depth5_plus = True
        if wr7 >= 0.60:
            any_wr60_plus = True

    return {
        **event_row,
        "best_tier": best_tier,
        "best_wr_7d": best_wr,
        "best_abn_7d": best_abn,
        "best_depth": best_depth,
        "any_tier2_plus": any_tier2_plus,
        "any_depth5_plus": any_depth5_plus,
        "any_wr60_plus": any_wr60_plus,
        "matched_insiders": matched_insiders,
    }


def simulate_buy_trade(event: dict, spy_prices: dict, stop_loss: float = -0.10) -> dict | None:
    """Simulate a buy trade with stop-loss. Returns dict with trade metrics."""
    ticker = event["ticker"]
    filing_date_str = str(event["filing_date"])[:10]
    try:
        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

    prices = load_daily_prices(ticker)
    if not prices:
        return None

    trading_days = sorted(d for d in prices if d > filing_date)
    if not trading_days:
        return None

    entry_date = trading_days[0]
    entry_price = prices[entry_date]["open"]
    if entry_price <= 0:
        return None

    hold_dates = trading_days[:8]  # 7 trading days
    exit_price = None
    exit_date = None
    stop_hit = False

    for i, d in enumerate(hold_dates):
        bar = prices[d]
        if stop_loss is not None and i > 0:
            if bar["low"] <= entry_price * (1.0 + stop_loss):
                exit_price = entry_price * (1.0 + stop_loss)
                exit_date = d
                stop_hit = True
                break

    if exit_price is None:
        exit_date = hold_dates[-1] if hold_dates else entry_date
        exit_price = prices[exit_date]["close"]

    trade_return = (exit_price - entry_price) / entry_price

    # SPY benchmark
    spy_return = 0.0
    spy_entry_days = sorted(d for d in spy_prices if d >= entry_date)
    spy_exit_days = sorted(d for d in spy_prices if d >= exit_date)
    if spy_entry_days and spy_exit_days:
        try:
            se = spy_prices[spy_entry_days[0]]["open"]
            sx = spy_prices[spy_exit_days[0]]["close"]
            spy_return = (sx - se) / se if se > 0 else 0
        except (KeyError, ZeroDivisionError):
            pass

    abnormal = trade_return - spy_return
    return {
        "ticker": ticker,
        "entry_date": entry_date,
        "abnormal_return": abnormal,
        "trade_return": trade_return,
        "stop_hit": stop_hit,
    }


def run_buy_sweep():
    """
    Test buy leg filter cascades with walk-forward:
    - Tier-based filters (insider track record quality)
    - Cluster + value filters
    - Combined filters
    """
    print("\n" + "=" * 70)
    print("  PART B: BUY LEG — SMART INSIDER FILTER")
    print("=" * 70)

    # Build insider lookups
    name_to_record, company_depth = build_insider_lookup(DB_PATH)

    # Load buy events
    buy_events = pd.read_csv(BUY_EVENTS_CSV)
    logger.info("Buy events loaded: %d", len(buy_events))

    # Score all events
    print("Scoring events against insider track records...")
    scored = []
    for _, row in buy_events.iterrows():
        scored.append(score_event(row.to_dict(), name_to_record, company_depth))

    scored_df = pd.DataFrame(scored)

    # Stats on matching
    matched = scored_df["matched_insiders"] > 0
    tier2 = scored_df["any_tier2_plus"]
    depth5 = scored_df["any_depth5_plus"]
    wr60 = scored_df["any_wr60_plus"]
    print(f"  Events with matched insiders: {matched.sum()} / {len(scored_df)} ({matched.mean():.1%})")
    print(f"  Events with tier 2+ insider:  {tier2.sum()} ({tier2.mean():.1%})")
    print(f"  Events with depth 5+ insider: {depth5.sum()} ({depth5.mean():.1%})")
    print(f"  Events with WR > 60% insider: {wr60.sum()} ({wr60.mean():.1%})")

    # Load SPY prices
    spy_prices = load_daily_prices("SPY")
    logger.info("SPY prices: %d days", len(spy_prices))

    # Define filter cascades
    filters = [
        # (label, filter_fn)
        ("V2 baseline (all)",
         lambda df: df),
        ("Cluster 2+",
         lambda df: df[df["n_insiders"] >= 2]),
        ("Cluster 2+, $5M+",
         lambda df: df[(df["n_insiders"] >= 2) & (df["total_value"] >= 5_000_000)]),
        ("Cluster 2+, $5M+, q>=2.0 (V1)",
         lambda df: df[(df["n_insiders"] >= 2) & (df["total_value"] >= 5_000_000) & (df["quality_score"] >= 2.0)]),
        ("Tier 2+ insider",
         lambda df: df[df["any_tier2_plus"]]),
        ("Tier 2+ + cluster 2+",
         lambda df: df[df["any_tier2_plus"] & (df["n_insiders"] >= 2)]),
        ("Tier 2+ + depth 5+",
         lambda df: df[df["any_tier2_plus"] & df["any_depth5_plus"]]),
        ("Tier 2+ + cluster + depth",
         lambda df: df[df["any_tier2_plus"] & (df["n_insiders"] >= 2) & df["any_depth5_plus"]]),
        ("Tier 2+ + WR>60%",
         lambda df: df[df["any_tier2_plus"] & df["any_wr60_plus"]]),
        ("Tier 2+ + WR>60% + depth",
         lambda df: df[df["any_tier2_plus"] & df["any_wr60_plus"] & df["any_depth5_plus"]]),
        ("Tier 2+ + cluster + WR>60%",
         lambda df: df[df["any_tier2_plus"] & (df["n_insiders"] >= 2) & df["any_wr60_plus"]]),
        ("Tier 2+ + $2M+",
         lambda df: df[df["any_tier2_plus"] & (df["total_value"] >= 2_000_000)]),
        ("Tier 2+ + $2M+ + q>=1.5",
         lambda df: df[df["any_tier2_plus"] & (df["total_value"] >= 2_000_000) & (df["quality_score"] >= 1.5)]),
        ("Tier 2+ + $5M+",
         lambda df: df[df["any_tier2_plus"] & (df["total_value"] >= 5_000_000)]),
        ("$2M+ q>=1.5",
         lambda df: df[(df["total_value"] >= 2_000_000) & (df["quality_score"] >= 1.5)]),
        ("$1M+ q>=2.0",
         lambda df: df[(df["total_value"] >= 1_000_000) & (df["quality_score"] >= 2.0)]),
        ("Cluster 2+ + $2M+ + q>=1.5",
         lambda df: df[(df["n_insiders"] >= 2) & (df["total_value"] >= 2_000_000) & (df["quality_score"] >= 1.5)]),
        ("WR>60% (any tier)",
         lambda df: df[df["any_wr60_plus"]]),
        ("Depth 5+ (any tier)",
         lambda df: df[df["any_depth5_plus"]]),
        ("Best: Tier 2+ + cluster + $2M+ + q>=1.5",
         lambda df: df[df["any_tier2_plus"] & (df["n_insiders"] >= 2) & (df["total_value"] >= 2_000_000) & (df["quality_score"] >= 1.5)]),
    ]

    # Run each filter cascade
    print("\nSimulating trades for each filter cascade...")
    results = []

    for label, filter_fn in filters:
        filtered = filter_fn(scored_df)
        if filtered.empty:
            results.append({"filter": label, "n_all": 0, "n_train": 0, "n_test": 0,
                            "sharpe_all": 0, "sharpe_train": 0, "sharpe_test": 0,
                            "wr_all": 0, "wr_test": 0, "degradation_pct": 0})
            continue

        # Simulate trades
        trades = []
        for _, row in filtered.iterrows():
            t = simulate_buy_trade(row.to_dict(), spy_prices)
            if t:
                trades.append(t)

        if not trades:
            results.append({"filter": label, "n_all": 0, "n_train": 0, "n_test": 0,
                            "sharpe_all": 0, "sharpe_train": 0, "sharpe_test": 0,
                            "wr_all": 0, "wr_test": 0, "degradation_pct": 0})
            continue

        all_ret = [t["abnormal_return"] for t in trades]
        train_ret = [t["abnormal_return"] for t in trades if t["entry_date"] <= TRAIN_END]
        test_ret = [t["abnormal_return"] for t in trades if t["entry_date"] >= TEST_START]

        all_s = compute_stats(all_ret, 7, label)
        train_s = compute_stats(train_ret, 7, f"Train {label}")
        test_s = compute_stats(test_ret, 7, f"Test {label}")

        deg = ((test_s["sharpe"] - train_s["sharpe"]) / abs(train_s["sharpe"]) * 100
               if train_s["sharpe"] != 0 else 0)

        results.append({
            "filter": label,
            "n_events": len(filtered),
            "n_all": all_s["n"],
            "n_train": train_s["n"],
            "n_test": test_s["n"],
            "sharpe_all": all_s["sharpe"],
            "sharpe_train": train_s["sharpe"],
            "sharpe_test": test_s["sharpe"],
            "wr_all": all_s["win_rate"],
            "wr_train": train_s["win_rate"],
            "wr_test": test_s["win_rate"],
            "mean_ar_all": all_s["mean_ar"],
            "mean_ar_test": test_s["mean_ar"],
            "max_dd_all": all_s["max_dd_pct"],
            "max_dd_test": test_s["max_dd_pct"],
            "t_stat_all": all_s["t_stat"],
            "degradation_pct": deg,
        })
        logger.info("  %s: N=%d (train=%d, test=%d), Sharpe all=%.2f train=%.2f test=%.2f",
                     label, all_s["n"], train_s["n"], test_s["n"],
                     all_s["sharpe"], train_s["sharpe"], test_s["sharpe"])

    # Print results table
    print(f"\n{'=' * 140}")
    print("  BUY LEG FILTER CASCADE — Walk-Forward Results (7d hold, -10% stop)")
    print(f"{'=' * 140}")
    print(f"{'Filter':<42} | {'N_all':>5} | {'N_test':>6} | {'Sh_all':>6} | "
          f"{'Sh_train':>8} | {'Sh_test':>7} | {'WR_test':>7} | {'DD_test':>7} | {'Degrad':>7}")
    print("-" * 140)

    for r in sorted(results, key=lambda x: -x.get("sharpe_test", 0)):
        if r["n_all"] == 0:
            continue
        print(f"{r['filter']:<42} | {r['n_all']:>5} | {r['n_test']:>6} | "
              f"{r['sharpe_all']:>6.2f} | {r['sharpe_train']:>8.2f} | "
              f"{r['sharpe_test']:>7.2f} | {r.get('wr_test', 0):>6.1%} | "
              f"{r.get('max_dd_test', 0):>6.2f}% | {r['degradation_pct']:>+6.1f}%")

    print(f"{'=' * 140}")
    return results


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="V3 Sweep — Put subgroups + Smart insider buy")
    parser.add_argument("--part", choices=["put", "buy", "both"], default="both")
    parser.add_argument("--output", type=Path, default=REPORT_DIR / "v3_sweep_results.json")
    args = parser.parse_args()

    output = {}

    if args.part in ("put", "both"):
        put_results = run_put_sweep()
        output["put_leg"] = put_results

    if args.part in ("buy", "both"):
        buy_results = run_buy_sweep()
        output["buy_leg"] = buy_results

    # Save results
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)
    logger.info("Results saved: %s", args.output)
    print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
