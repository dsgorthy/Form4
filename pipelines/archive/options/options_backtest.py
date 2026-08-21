#!/usr/bin/env python3
"""
Phase 4: Options Grid Search Backtest
--------------------------------------
Backtests insider cluster buy events using call options.
Reads cached option EOD data from theta_cache.db (pulled by options_pull.py).

Grid: 4 holds x 2 DTE types x 4 strikes x 4 stop-losses x 2 pricing modes = 256 configs.

Usage:
    python options_backtest.py
    python options_backtest.py --top 30
    python options_backtest.py --spread-filter 0.15

Author: Claude Opus 4.6
Date: 2026-03-10
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from theta_client import (
    CacheDB,
    get_eod_date,
    find_nearest_expiration,
    find_nearest_strike,
    add_trading_days,
)

# ─────────────────────────────────────────────
# Configuration — matches options_pull.py (LOCKED)
# ─────────────────────────────────────────────

STRIKES = {
    "5pct_itm": 0.95,
    "atm": 1.00,
    "5pct_otm": 1.05,
    "10pct_otm": 1.10,
}

HOLD_DTE_MAP = {
    7:  (14, 21),
    14: (28, 45),
    30: (50, 60),
    60: (90, 120),
}

HOLD_DAYS = [7, 14, 30, 60]
DTE_TYPES = ["tight", "comfortable"]
STRIKE_TYPES = list(STRIKES.keys())
STOP_LOSSES = [-0.25, -0.50, -0.75, None]

DEFAULT_SPREAD_FILTER = 0.20  # 20%
NOTIONAL_PER_TRADE = 1000  # $1,000 fixed notional

BUY_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_cluster_buy_events() -> list[dict]:
    """Load cluster buy events from the aggregated CSV."""
    with open(BUY_EVENTS_CSV) as f:
        events = list(csv.DictReader(f))
    events = [e for e in events if e.get("is_cluster") == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_exit_date"] = datetime.strptime(e["exit_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_exit_price"] = float(e["exit_price"])
        e["_trade_return"] = float(e["trade_return"])
    return events


def parse_expirations(raw: list[dict] | str | None) -> list[date]:
    """Parse expirations from cache (could be list of dicts or '__NONE__')."""
    if raw is None or raw == "__NONE__":
        return []
    result = []
    for r in raw:
        try:
            if isinstance(r, dict):
                exp_str = r.get("expiration", "").strip().strip('"')
            else:
                exp_str = str(r).strip().strip('"')
            result.append(datetime.strptime(exp_str, "%Y-%m-%d").date())
        except (ValueError, KeyError):
            continue
    return sorted(result)


def parse_strikes(raw: list[dict] | str | None) -> list[float]:
    """Parse strikes from cache."""
    if raw is None or raw == "__NONE__":
        return []
    result = []
    for r in raw:
        try:
            if isinstance(r, dict):
                s = r.get("strike", "").strip().strip('"')
            else:
                s = str(r).strip().strip('"')
            result.append(float(s))
        except (ValueError, KeyError):
            continue
    return sorted(result)


# ─────────────────────────────────────────────
# Pricing Helpers
# ─────────────────────────────────────────────

def get_bid(row: dict) -> float | None:
    """Extract bid price from EOD row."""
    try:
        val = float(str(row.get("bid", "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def get_ask(row: dict) -> float | None:
    """Extract ask price from EOD row."""
    try:
        val = float(str(row.get("ask", "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def get_mid(row: dict) -> float | None:
    """Extract midpoint price from EOD row."""
    bid = get_bid(row)
    ask = get_ask(row)
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return None


def get_low(row: dict) -> float | None:
    """Extract low price from EOD row."""
    try:
        val = float(str(row.get("low", "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def get_close(row: dict) -> float | None:
    """Extract close price from EOD row."""
    try:
        val = float(str(row.get("close", "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def get_entry_price(row: dict, mode: str) -> float | None:
    """Get entry price based on pricing mode. Conservative=ask, Optimistic=mid."""
    if mode == "conservative":
        return get_ask(row)
    else:
        return get_mid(row)


def get_exit_price(row: dict, mode: str) -> float | None:
    """Get exit price based on pricing mode. Conservative=bid, Optimistic=mid."""
    if mode == "conservative":
        return get_bid(row)
    else:
        return get_mid(row)


def compute_spread_pct(row: dict) -> float | None:
    """Compute (ask - bid) / midpoint spread percentage."""
    bid = get_bid(row)
    ask = get_ask(row)
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid


# ─────────────────────────────────────────────
# Contract Resolution
# ─────────────────────────────────────────────

def resolve_contract(cache: CacheDB, ticker: str, entry_date: date,
                     entry_price: float, hold_days: int, dte_type: str,
                     strike_type: str) -> dict | None:
    """
    Resolve the option contract for a given event + config.
    Returns dict with expiration, strike, cache_key, exit_date or None.
    """
    # Get expirations from cache
    exp_raw = cache.get(f"opt_exp|{ticker}")
    expirations = parse_expirations(exp_raw)
    if not expirations:
        return None

    # Determine target DTE
    tight_dte, comf_dte = HOLD_DTE_MAP[hold_days]
    target_dte = tight_dte if dte_type == "tight" else comf_dte

    # Find nearest expiration
    matched_exp = find_nearest_expiration(expirations, entry_date, target_dte)
    if matched_exp is None:
        return None

    # Compute exit date
    exit_date = add_trading_days(entry_date, hold_days)

    # Exit must be before expiration
    if exit_date >= matched_exp:
        return None

    # Get strikes from cache
    exp_str = matched_exp.strftime("%Y-%m-%d")
    strikes_raw = cache.get(f"opt_strikes|{ticker}|{exp_str}")
    strikes = parse_strikes(strikes_raw)
    if not strikes:
        return None

    # Find nearest strike
    strike_mult = STRIKES[strike_type]
    target_strike = entry_price * strike_mult
    real_strike = find_nearest_strike(strikes, target_strike)
    if real_strike is None:
        return None

    # Build cache key (matching options_pull.py logic)
    pull_start = entry_date - timedelta(days=2)
    pull_end = exit_date + timedelta(days=5)
    cache_key = (
        f"opt_eod_daily|{ticker}|{exp_str}|{real_strike}|C"
        f"|{pull_start.strftime('%Y-%m-%d')}|{pull_end.strftime('%Y-%m-%d')}"
    )

    return {
        "expiration": matched_exp,
        "strike": real_strike,
        "exit_date": exit_date,
        "cache_key": cache_key,
    }


# ─────────────────────────────────────────────
# Single Trade Simulation
# ─────────────────────────────────────────────

def simulate_trade(eod_rows: list[dict], entry_date: date, exit_date: date,
                   stop_pct: float | None, pricing_mode: str,
                   spread_filter: float) -> dict | None:
    """
    Simulate a single option trade.

    Returns dict with trade details or None if skipped.
    The 'skip_reason' key indicates why a trade was skipped if applicable.
    """
    if not eod_rows:
        return {"skip": "no_data"}

    # Build date-indexed rows
    rows_by_date = {}
    sorted_rows = []
    for row in eod_rows:
        d = get_eod_date(row)
        if d is not None:
            rows_by_date[d] = row
            sorted_rows.append((d, row))
    sorted_rows.sort(key=lambda x: x[0])

    if not sorted_rows:
        return {"skip": "no_data"}

    # Find entry-day row (exact match or first row >= entry_date)
    entry_row = rows_by_date.get(entry_date)
    if entry_row is None:
        for d, row in sorted_rows:
            if d >= entry_date:
                entry_row = row
                entry_date = d  # adjust to actual date
                break
    if entry_row is None:
        return {"skip": "no_data"}

    # Check spread filter at entry
    spread_pct = compute_spread_pct(entry_row)
    if spread_pct is None:
        return {"skip": "no_data"}
    if spread_pct > spread_filter:
        return {"skip": "spread", "spread_pct": spread_pct}

    # Get entry price
    entry_px = get_entry_price(entry_row, pricing_mode)
    if entry_px is None or entry_px <= 0:
        return {"skip": "no_data"}

    # Position sizing: $1,000 notional, options trade in 100-share contracts
    cost_per_contract = entry_px * 100
    num_contracts = int(NOTIONAL_PER_TRADE // cost_per_contract)
    if num_contracts <= 0:
        return {"skip": "zero_contracts", "entry_px": entry_px}

    # Walk through hold period checking for stop-loss
    stopped_out = False
    stop_exit_px = None
    stop_date = None

    if stop_pct is not None:
        stop_level = entry_px * (1.0 + stop_pct)
        for d, row in sorted_rows:
            if d <= entry_date:
                continue
            if d > exit_date:
                break
            low = get_low(row)
            if low is not None and low <= stop_level:
                stopped_out = True
                stop_exit_px = stop_level
                stop_date = d
                break

    # Determine exit price
    if stopped_out:
        final_exit_px = stop_exit_px
        exit_reason = "stop_loss"
        actual_exit_date = stop_date
    else:
        # Find exit-day row
        exit_row = rows_by_date.get(exit_date)
        if exit_row is None:
            # Try nearby dates
            for d, row in sorted_rows:
                if d >= exit_date:
                    exit_row = row
                    exit_date = d
                    break
        if exit_row is None:
            # Use last available row
            if sorted_rows:
                exit_row = sorted_rows[-1][1]
                exit_date = sorted_rows[-1][0]
            else:
                return {"skip": "no_data"}

        final_exit_px = get_exit_price(exit_row, pricing_mode)
        if final_exit_px is None or final_exit_px <= 0:
            # Fallback: try close price
            final_exit_px = get_close(exit_row)
            if final_exit_px is None or final_exit_px <= 0:
                return {"skip": "no_data"}
        exit_reason = "time"
        actual_exit_date = exit_date

    # Compute returns
    pct_return = (final_exit_px - entry_px) / entry_px
    dollar_pnl = (final_exit_px - entry_px) * num_contracts * 100

    return {
        "entry_px": entry_px,
        "exit_px": final_exit_px,
        "num_contracts": num_contracts,
        "pct_return": pct_return,
        "dollar_pnl": dollar_pnl,
        "exit_reason": exit_reason,
        "spread_pct": spread_pct,
        "actual_exit_date": str(actual_exit_date),
    }


# ─────────────────────────────────────────────
# Grid Sweep
# ─────────────────────────────────────────────

def compute_max_drawdown(dollar_pnls: list[float]) -> float:
    """Compute max drawdown on sequential cumulative P&L."""
    if not dollar_pnls:
        return 0.0
    cum = np.cumsum(dollar_pnls)
    peak = np.maximum.accumulate(cum)
    # Drawdown relative to initial capital (NOTIONAL_PER_TRADE as proxy)
    # Use absolute drawdown from peak
    drawdowns = cum - peak
    return float(np.min(drawdowns)) if len(drawdowns) > 0 else 0.0


def run_sweep(events: list[dict], cache: CacheDB,
              spread_filter: float) -> tuple[list[dict], dict]:
    """
    Run the full grid sweep across all configs.

    Returns:
        - List of result rows (one per config)
        - Dict mapping config key -> list of trade details (for top-N export)
    """
    total_events = len(events)
    pricing_modes = ["conservative", "optimistic"]

    # Pre-resolve contracts for all events x (hold, dte_type, strike_type)
    # to avoid redundant cache lookups
    print("Pre-resolving contracts...")
    t0 = time.monotonic()

    # Key: (event_idx, hold_days, dte_type, strike_type)
    contracts = {}
    eod_cache = {}  # cache_key -> parsed EOD rows

    resolved = 0
    no_contract = 0

    for i, ev in enumerate(events):
        ticker = ev["_ticker"]
        entry_date = ev["_entry_date"]
        entry_price = ev["_entry_price"]

        for hold_days in HOLD_DAYS:
            for dte_type in DTE_TYPES:
                for strike_type in STRIKE_TYPES:
                    contract = resolve_contract(
                        cache, ticker, entry_date, entry_price,
                        hold_days, dte_type, strike_type,
                    )
                    key = (i, hold_days, dte_type, strike_type)
                    if contract is None:
                        contracts[key] = None
                        no_contract += 1
                        continue

                    contracts[key] = contract
                    resolved += 1

                    # Pre-fetch EOD data
                    ck = contract["cache_key"]
                    if ck not in eod_cache:
                        raw = cache.get(ck)
                        if raw is None or raw == "__NONE__":
                            eod_cache[ck] = None
                        else:
                            eod_cache[ck] = raw

        if (i + 1) % 500 == 0:
            print(f"  Pre-resolved {i + 1}/{total_events} events...")

    elapsed = time.monotonic() - t0
    print(f"  Resolved {resolved} contracts ({no_contract} no-contract) in {elapsed:.1f}s")
    print(f"  Unique EOD series cached: {len(eod_cache)}")

    # Run grid sweep
    print("\nRunning grid sweep...")
    t0 = time.monotonic()

    results = []
    all_trades = {}  # config_key -> list of trade dicts
    config_count = 0
    total_configs = len(HOLD_DAYS) * len(DTE_TYPES) * len(STRIKE_TYPES) * len(STOP_LOSSES) * len(pricing_modes)

    for hold_days in HOLD_DAYS:
        for dte_type in DTE_TYPES:
            for strike_type in STRIKE_TYPES:
                for stop_loss in STOP_LOSSES:
                    for pricing_mode in pricing_modes:
                        config_count += 1
                        config_key = f"{hold_days}d|{dte_type}|{strike_type}|stop={stop_loss}|{pricing_mode}"

                        trades_list = []
                        n_skipped_spread = 0
                        n_skipped_no_data = 0
                        n_skipped_zero_contracts = 0
                        spreads = []
                        dollar_pnls = []
                        pct_returns = []

                        for i, ev in enumerate(events):
                            ckey = (i, hold_days, dte_type, strike_type)
                            contract = contracts.get(ckey)
                            if contract is None:
                                n_skipped_no_data += 1
                                continue

                            eod_rows = eod_cache.get(contract["cache_key"])
                            if eod_rows is None:
                                n_skipped_no_data += 1
                                continue

                            result = simulate_trade(
                                eod_rows,
                                ev["_entry_date"],
                                contract["exit_date"],
                                stop_loss,
                                pricing_mode,
                                spread_filter,
                            )

                            if result is None:
                                n_skipped_no_data += 1
                                continue

                            skip = result.get("skip")
                            if skip == "no_data":
                                n_skipped_no_data += 1
                            elif skip == "spread":
                                n_skipped_spread += 1
                            elif skip == "zero_contracts":
                                n_skipped_zero_contracts += 1
                            else:
                                # Valid trade
                                pct_returns.append(result["pct_return"])
                                dollar_pnls.append(result["dollar_pnl"])
                                spreads.append(result["spread_pct"])
                                trades_list.append({
                                    "ticker": ev["_ticker"],
                                    "entry_date": str(ev["_entry_date"]),
                                    "strike": contract["strike"],
                                    "expiration": str(contract["expiration"]),
                                    "entry_px": result["entry_px"],
                                    "exit_px": result["exit_px"],
                                    "num_contracts": result["num_contracts"],
                                    "pct_return": result["pct_return"],
                                    "dollar_pnl": result["dollar_pnl"],
                                    "exit_reason": result["exit_reason"],
                                    "spread_pct": result["spread_pct"],
                                })

                        n_trades = len(pct_returns)
                        all_trades[config_key] = trades_list

                        if n_trades > 0:
                            arr = np.array(pct_returns)
                            mean_ret = float(np.mean(arr))
                            median_ret = float(np.median(arr))
                            std_ret = float(np.std(arr, ddof=1)) if n_trades > 1 else 0.0
                            win_rate = float(np.sum(arr > 0) / n_trades)
                            sharpe = (mean_ret / std_ret) * np.sqrt(min(252, n_trades)) if std_ret > 0 else 0.0
                            total_dollar_pnl = float(np.sum(dollar_pnls))
                            avg_dollar_pnl = float(np.mean(dollar_pnls))
                            avg_spread = float(np.mean(spreads))
                            max_dd = compute_max_drawdown(dollar_pnls)
                        else:
                            mean_ret = median_ret = win_rate = sharpe = 0.0
                            total_dollar_pnl = avg_dollar_pnl = avg_spread = max_dd = 0.0

                        results.append({
                            "hold_days": hold_days,
                            "dte_type": dte_type,
                            "strike_type": strike_type,
                            "stop_loss": stop_loss if stop_loss is not None else "none",
                            "pricing_mode": pricing_mode,
                            "n_trades": n_trades,
                            "n_skipped_spread": n_skipped_spread,
                            "n_skipped_no_data": n_skipped_no_data,
                            "n_skipped_zero_contracts": n_skipped_zero_contracts,
                            "win_rate": round(win_rate, 4),
                            "mean_return": round(mean_ret, 6),
                            "median_return": round(median_ret, 6),
                            "sharpe": round(float(sharpe), 4),
                            "total_dollar_pnl": round(total_dollar_pnl, 2),
                            "avg_dollar_pnl": round(avg_dollar_pnl, 2),
                            "avg_spread_pct": round(avg_spread, 4),
                            "max_drawdown": round(max_dd, 2),
                        })

                        if config_count % 32 == 0:
                            elapsed = time.monotonic() - t0
                            print(f"  [{config_count}/{total_configs}] configs done ({elapsed:.1f}s)")

    # Sort by Sharpe descending
    results.sort(key=lambda r: -r["sharpe"])

    elapsed = time.monotonic() - t0
    print(f"  Sweep complete: {total_configs} configs in {elapsed:.1f}s")

    return results, all_trades


# ─────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────

def write_sweep_csv(results: list[dict], path: str):
    """Write sweep results to CSV."""
    if not results:
        print("No results to write.")
        return
    fieldnames = list(results[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Sweep CSV written: {path} ({len(results)} rows)")


def write_top_trades_json(results: list[dict], all_trades: dict, path: str, top_n: int = 5):
    """Write detailed trades for top N configs by Sharpe."""
    top_configs = results[:top_n]
    output = {}
    for r in top_configs:
        stop_str = str(r["stop_loss"])
        config_key = f"{r['hold_days']}d|{r['dte_type']}|{r['strike_type']}|stop={stop_str}|{r['pricing_mode']}"
        # Handle the "none" string from results vs None in all_trades keys
        alt_key = f"{r['hold_days']}d|{r['dte_type']}|{r['strike_type']}|stop={r['stop_loss']}|{r['pricing_mode']}"
        trades = all_trades.get(config_key) or all_trades.get(alt_key, [])
        output[config_key] = {
            "summary": r,
            "trades": trades,
        }
    with open(path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"Top {top_n} trades JSON written: {path}")


def print_summary_table(results: list[dict], top_n: int = 20):
    """Print a formatted summary table of top configs."""
    print()
    print("=" * 130)
    print("OPTIONS BACKTEST — TOP CONFIGS BY SHARPE")
    print("=" * 130)
    print()

    header = (
        f"{'#':>3} | {'Hold':>4} | {'DTE':>11} | {'Strike':>10} | {'Stop':>5} | "
        f"{'Price':>12} | {'N':>5} | {'WR':>6} | {'Mean':>8} | {'Med':>8} | "
        f"{'Sharpe':>7} | {'Total$':>9} | {'Avg$':>7} | {'MaxDD':>9} | {'Spread':>6}"
    )
    print(header)
    print("-" * 130)

    for i, r in enumerate(results[:top_n]):
        stop_str = "none" if r["stop_loss"] == "none" else f"{float(r['stop_loss']):.0%}"
        print(
            f"{i+1:>3} | {r['hold_days']:>4}d | {r['dte_type']:>11} | {r['strike_type']:>10} | "
            f"{stop_str:>5} | {r['pricing_mode']:>12} | {r['n_trades']:>5} | "
            f"{r['win_rate']:>5.1%} | {r['mean_return']:>+7.2%} | {r['median_return']:>+7.2%} | "
            f"{r['sharpe']:>7.2f} | ${r['total_dollar_pnl']:>8,.0f} | "
            f"${r['avg_dollar_pnl']:>6,.0f} | ${r['max_drawdown']:>8,.0f} | "
            f"{r['avg_spread_pct']:>5.1%}"
        )

    # Summary stats
    print()
    print("-" * 130)
    total_with_trades = sum(1 for r in results if r["n_trades"] > 0)
    avg_n = np.mean([r["n_trades"] for r in results if r["n_trades"] > 0]) if total_with_trades > 0 else 0
    positive_sharpe = sum(1 for r in results if r["sharpe"] > 0)
    print(f"Configs with trades: {total_with_trades}/{len(results)} | "
          f"Positive Sharpe: {positive_sharpe}/{len(results)} | "
          f"Avg trades/config: {avg_n:.0f}")

    # Conservative vs Optimistic comparison
    cons = [r for r in results if r["pricing_mode"] == "conservative" and r["n_trades"] > 0]
    opti = [r for r in results if r["pricing_mode"] == "optimistic" and r["n_trades"] > 0]
    if cons and opti:
        avg_sharpe_cons = np.mean([r["sharpe"] for r in cons])
        avg_sharpe_opti = np.mean([r["sharpe"] for r in opti])
        print(f"Avg Sharpe — Conservative: {avg_sharpe_cons:.3f} | Optimistic: {avg_sharpe_opti:.3f}")

    print("=" * 130)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Phase 4: Options Grid Search Backtest")
    parser.add_argument("--top", type=int, default=20, help="Number of top configs to display (default: 20)")
    parser.add_argument("--spread-filter", type=float, default=DEFAULT_SPREAD_FILTER,
                        help=f"Max spread %% at entry (default: {DEFAULT_SPREAD_FILTER:.0%})")
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 4: Options Grid Search Backtest")
    print("=" * 60)
    print(f"  Spread filter: {args.spread_filter:.0%}")
    print(f"  Notional per trade: ${NOTIONAL_PER_TRADE:,}")
    print(f"  Grid: {len(HOLD_DAYS)} holds x {len(DTE_TYPES)} DTE x "
          f"{len(STRIKE_TYPES)} strikes x {len(STOP_LOSSES)} stops x 2 pricing = "
          f"{len(HOLD_DAYS) * len(DTE_TYPES) * len(STRIKE_TYPES) * len(STOP_LOSSES) * 2} configs")
    print()

    # Load events
    print("Loading cluster buy events...")
    events = load_cluster_buy_events()
    print(f"  {len(events)} cluster buy events loaded")

    # Open cache
    cache = CacheDB()
    print(f"  Cache entries: {cache.count()}")
    print()

    # Run sweep
    results, all_trades = run_sweep(events, cache, args.spread_filter)
    cache.close()

    # Write outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    sweep_path = os.path.join(OUTPUT_DIR, "sweep_options_buys.csv")
    write_sweep_csv(results, sweep_path)

    trades_path = os.path.join(OUTPUT_DIR, "options_backtest_trades.json")
    write_top_trades_json(results, all_trades, trades_path, top_n=5)

    # Print summary
    print_summary_table(results, top_n=args.top)


if __name__ == "__main__":
    main()
