#!/usr/bin/env python3
"""
Options grid sweep: test multiple strike/expiry combinations using cached Theta Data.

Tests:
  Strikes: ATM (0%), 5% OTM, 10% OTM
  Expiries: 30 DTE, 45 DTE, 90 DTE
  Hold: 60 trading days or +100% profit target
  Sides: both buy→calls and sell→puts

All data comes from the 147MB Theta Data cache — no API calls needed.
"""

from __future__ import annotations

import statistics
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from options_backtest import (
    DB_PATH, load_cache, save_cache,
    get_expirations, get_strikes, get_option_eod, get_fair_price, get_eod_date,
    add_trading_days, load_qualified_insiders,
    WINDOWS, ANNUALIZE, MAX_HOLD_TRADING_DAYS, PROFIT_TARGET,
)

PRICES_DIR = Path(__file__).resolve().parent.parent.parent / "pipelines" / "insider_study" / "data" / "prices"

# Grid parameters
STRIKE_OFFSETS = {
    "ATM":      0.00,
    "5% OTM":   0.05,
    "10% OTM":  0.10,
}

DTE_TARGETS = {
    "30 DTE":  30,
    "45 DTE":  45,
    "90 DTE":  90,
}

# DTE search windows (min, max calendar days)
DTE_WINDOWS = {
    30:  (20, 45),
    45:  (35, 60),
    90:  (75, 120),
}


def find_contract(symbol, stock_price, filing_date, right, otm_pct, target_dte, cache):
    """Find option contract for given strike offset and DTE target."""
    expirations = get_expirations(symbol, cache)
    if not expirations:
        return None

    target_expiry = filing_date + timedelta(days=target_dte)
    min_dte, max_dte = DTE_WINDOWS[target_dte]
    min_exp = filing_date + timedelta(days=min_dte)
    max_exp = filing_date + timedelta(days=max_dte)
    valid_exps = [e for e in expirations if min_exp <= e <= max_exp]

    if not valid_exps:
        return None

    best_exp = min(valid_exps, key=lambda e: abs((e - target_expiry).days))
    strikes = get_strikes(symbol, best_exp, cache)
    if not strikes:
        return None

    if right == "C":
        target_strike = stock_price * (1 + otm_pct)
    else:
        target_strike = stock_price * (1 - otm_pct)

    best_strike = min(strikes, key=lambda s: abs(s - target_strike))

    return {"expiration": best_exp, "strike": best_strike, "right": right,
            "actual_dte": (best_exp - filing_date).days}


def simulate_trade(ticker, filing_date, stock_price, right, otm_pct, target_dte, cache):
    """Simulate one options trade with given parameters."""
    contract = find_contract(ticker, stock_price, filing_date, right, otm_pct, target_dte, cache)
    if not contract:
        return None

    entry_date = add_trading_days(filing_date, 1)
    max_exit = min(
        add_trading_days(entry_date, MAX_HOLD_TRADING_DAYS),
        contract["expiration"] - timedelta(days=5),
    )
    if max_exit <= entry_date:
        return None

    eod_rows = get_option_eod(ticker, contract["expiration"], contract["strike"],
                              right, entry_date, max_exit, cache)
    if not eod_rows:
        return None

    daily = {}
    for row in eod_rows:
        d = get_eod_date(row)
        p = get_fair_price(row)
        if d and p and p > 0:
            daily[d] = p

    if not daily:
        return None

    sorted_dates = sorted(daily.keys())
    entry_candidates = [d for d in sorted_dates if d >= entry_date]
    if not entry_candidates:
        return None

    actual_entry = entry_candidates[0]
    entry_price = daily[actual_entry]
    if entry_price <= 0.01:
        return None

    exit_price, exit_date, exit_reason = None, None, "time_exit"
    days_held = 0
    for d in sorted_dates:
        if d <= actual_entry:
            continue
        days_held += 1
        price = daily[d]
        if price >= entry_price * (1 + PROFIT_TARGET):
            exit_price, exit_date, exit_reason = price, d, "profit_target"
            break
        if days_held >= MAX_HOLD_TRADING_DAYS or d >= max_exit:
            exit_price, exit_date, exit_reason = price, d, "time_exit"
            break

    if exit_price is None and sorted_dates and sorted_dates[-1] > actual_entry:
        exit_price = daily[sorted_dates[-1]]
        exit_date = sorted_dates[-1]
        exit_reason = "data_end"

    if exit_price is None or exit_date is None:
        return None

    pnl_pct = (exit_price - entry_price) / entry_price
    hold_days = (exit_date - actual_entry).days

    return {
        "pnl_pct": pnl_pct,
        "win": pnl_pct > 0,
        "entry_price": entry_price,
        "exit_reason": exit_reason,
        "hold_days": hold_days,
        "dte": contract["actual_dte"],
    }


def load_spy():
    path = PRICES_DIR / "SPY.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
    df.columns = [c.lower() for c in df.columns]
    return df


def main():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cache = load_cache()
    print(f"Cache: {len(cache):,} entries\n", flush=True)

    for trade_type in ["buy", "sell"]:
        right = "C" if trade_type == "buy" else "P"
        side_label = "BUY → CALLS" if trade_type == "buy" else "SELL → PUTS"

        print(f"Loading {trade_type} insiders...")
        qualified = load_qualified_insiders(conn, trade_type)
        total = sum(len(tt) for _, _, _, _, tt in qualified)
        print(f"  {len(qualified)} insiders, {total} test trades\n")

        # Flatten all test trades
        all_trades = []
        for insider_id, name, best_window, train_sharpe, test_trades in qualified:
            for t in test_trades:
                all_trades.append(t)

        # Run grid
        print(f"{'='*80}")
        print(f"  {side_label} — STRIKE x EXPIRY GRID")
        print(f"{'='*80}")

        grid_results = {}

        for dte_label, target_dte in DTE_TARGETS.items():
            for strike_label, otm_pct in STRIKE_OFFSETS.items():
                key = f"{strike_label} @ {dte_label}"
                results = []
                no_contract = 0

                for i, trade in enumerate(all_trades):
                    if i % 2000 == 0 and i > 0:
                        print(f"    {key}: {i}/{len(all_trades)}...", flush=True)
                        save_cache(cache)

                    r = simulate_trade(
                        trade["ticker"], trade["filing_date"], trade["stock_price"],
                        right, otm_pct, target_dte, cache,
                    )
                    if r:
                        results.append(r)
                    else:
                        no_contract += 1

                grid_results[key] = results
                n = len(results)
                if n == 0:
                    print(f"  {key}: no trades")
                    continue

                wr = sum(1 for r in results if r["win"]) / n
                avg = statistics.mean([r["pnl_pct"] for r in results])
                med = statistics.median([r["pnl_pct"] for r in results])
                std = statistics.stdev([r["pnl_pct"] for r in results]) if n > 1 else 0
                sharpe = (avg / std) * (252 / 60) ** 0.5 if std > 0 else 0
                avg_hold = statistics.mean([r["hold_days"] for r in results])
                pt_pct = sum(1 for r in results if r["exit_reason"] == "profit_target") / n

                # Filter out cheap options for a "realistic" view
                realistic = [r for r in results if r["entry_price"] >= 1.0]
                r_n = len(realistic)
                if r_n > 0:
                    r_wr = sum(1 for r in realistic if r["win"]) / r_n
                    r_avg = statistics.mean([r["pnl_pct"] for r in realistic])
                    r_med = statistics.median([r["pnl_pct"] for r in realistic])
                else:
                    r_wr, r_avg, r_med = 0, 0, 0

                print(f"\n  {key}:")
                print(f"    N={n:,} (no-contract={no_contract:,}) | WR={wr*100:.1f}% | "
                      f"Avg={avg*100:+.1f}% | Med={med*100:+.1f}% | Sharpe={sharpe:.2f}")
                print(f"    Avg hold={avg_hold:.0f}d | Profit target hit={pt_pct*100:.1f}%")
                print(f"    Realistic (prem>=$1): N={r_n} | WR={r_wr*100:.1f}% | "
                      f"Avg={r_avg*100:+.1f}% | Med={r_med*100:+.1f}%")

        # Summary table
        print(f"\n{'='*80}")
        print(f"  {side_label} — SUMMARY TABLE")
        print(f"{'='*80}")
        print(f"  {'Config':<22} {'N':>6} {'WR':>7} {'Avg':>8} {'Med':>8} {'Sharpe':>7} {'PT%':>6} {'Hold':>6}")
        print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*8} {'-'*8} {'-'*7} {'-'*6} {'-'*6}")

        for key, results in grid_results.items():
            n = len(results)
            if n == 0:
                continue
            wr = sum(1 for r in results if r["win"]) / n
            avg = statistics.mean([r["pnl_pct"] for r in results])
            med = statistics.median([r["pnl_pct"] for r in results])
            std = statistics.stdev([r["pnl_pct"] for r in results]) if n > 1 else 0
            sharpe = (avg / std) * (252 / 60) ** 0.5 if std > 0 else 0
            pt = sum(1 for r in results if r["exit_reason"] == "profit_target") / n
            hold = statistics.mean([r["hold_days"] for r in results])
            print(f"  {key:<22} {n:>6,} {wr*100:>6.1f}% {avg*100:>+7.1f}% {med*100:>+7.1f}% {sharpe:>7.2f} {pt*100:>5.1f}% {hold:>5.0f}d")

        # Realistic summary (premium >= $1)
        print(f"\n  {side_label} — REALISTIC (premium >= $1.00)")
        print(f"  {'Config':<22} {'N':>6} {'WR':>7} {'Avg':>8} {'Med':>8} {'Sharpe':>7}")
        print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*8} {'-'*8} {'-'*7}")

        for key, results in grid_results.items():
            realistic = [r for r in results if r["entry_price"] >= 1.0]
            n = len(realistic)
            if n == 0:
                continue
            wr = sum(1 for r in realistic if r["win"]) / n
            avg = statistics.mean([r["pnl_pct"] for r in realistic])
            med = statistics.median([r["pnl_pct"] for r in realistic])
            std = statistics.stdev([r["pnl_pct"] for r in realistic]) if n > 1 else 0
            sharpe = (avg / std) * (252 / 60) ** 0.5 if std > 0 else 0
            print(f"  {key:<22} {n:>6,} {wr*100:>6.1f}% {avg*100:>+7.1f}% {med*100:>+7.1f}% {sharpe:>7.2f}")

        print()

    conn.close()
    save_cache(cache)
    print("Done.")


if __name__ == "__main__":
    main()
