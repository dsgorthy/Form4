#!/usr/bin/env python3
"""
Re-run options backtest from cache and compute detailed metrics:
  - SPY comparison over same hold periods
  - Average hold time (calls vs puts)
  - Data source breakdown
  - Per-window and per-tier analysis

Uses the 147MB Theta Data cache, so no API calls needed.
"""

from __future__ import annotations

import json
import statistics
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# Reuse everything from options_backtest
from options_backtest import (
    DB_PATH, CACHE_PATH, WINDOWS, ANNUALIZE,
    load_cache, simulate_option_trade, load_qualified_insiders,
    OptionsTrade,
)

PRICES_DIR = Path(__file__).resolve().parent.parent.parent / "pipelines" / "insider_study" / "data" / "prices"

# ── SPY benchmark ────────────────────────────────────────────────────────

_spy_df = None

def load_spy():
    global _spy_df
    if _spy_df is not None:
        return _spy_df
    path = PRICES_DIR / "SPY.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
    df.columns = [c.lower() for c in df.columns]
    _spy_df = df
    return df


def spy_return_over_period(entry_date: date, exit_date: date) -> float | None:
    """Compute SPY total return over a date range."""
    spy = load_spy()
    if spy is None:
        return None
    entry_ts = pd.Timestamp(entry_date)
    exit_ts = pd.Timestamp(exit_date)
    # Find nearest trading days
    entry_candidates = spy.index[spy.index >= entry_ts]
    exit_candidates = spy.index[spy.index >= exit_ts]
    if len(entry_candidates) == 0 or len(exit_candidates) == 0:
        return None
    entry_price = float(spy.loc[entry_candidates[0], "close"])
    exit_price = float(spy.loc[exit_candidates[0], "close"])
    if entry_price <= 0:
        return None
    return (exit_price - entry_price) / entry_price


def run_analysis():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cache = load_cache()
    print(f"Cache: {len(cache):,} entries")

    results = {}
    for trade_type in ["buy", "sell"]:
        label = "CALL" if trade_type == "buy" else "PUT"
        print(f"\nProcessing {label} trades from cache...")

        qualified = load_qualified_insiders(conn, trade_type)
        total_test = sum(len(tt) for _, _, _, _, tt in qualified)
        print(f"  {len(qualified)} qualified insiders, {total_test} test trades")

        trades = []
        no_contract = 0
        processed = 0

        for insider_id, name, best_window, train_sharpe, test_trades in qualified:
            for trade in test_trades:
                processed += 1
                if processed % 2000 == 0:
                    print(f"  {processed}/{total_test}...")

                result = simulate_option_trade(
                    ticker=trade["ticker"],
                    insider_name=name,
                    insider_id=insider_id,
                    trade_type=trade_type,
                    filing_date=trade["filing_date"],
                    stock_price=trade["stock_price"],
                    cache=cache,
                )
                if result:
                    trades.append(result)
                else:
                    no_contract += 1

        print(f"  {len(trades)} trades, {no_contract} no-contract")
        results[trade_type] = trades

    conn.close()

    # ── Analysis ──────────────────────────────────────────────────────

    for trade_type, trades in results.items():
        label = "CALLS (Buy Signal)" if trade_type == "buy" else "PUTS (Sell Signal)"
        print(f"\n{'='*70}")
        print(f"  {label} — DETAILED ANALYSIS")
        print(f"{'='*70}")

        if not trades:
            print("  No trades")
            continue

        n = len(trades)

        # 1. Hold time
        hold_days = []
        for t in trades:
            delta = (t.exit_date - t.entry_date).days
            hold_days.append(delta)

        avg_hold = statistics.mean(hold_days)
        med_hold = statistics.median(hold_days)
        min_hold = min(hold_days)
        max_hold = max(hold_days)

        print(f"\n  HOLD TIME:")
        print(f"    Average: {avg_hold:.1f} calendar days")
        print(f"    Median:  {med_hold:.0f} calendar days")
        print(f"    Range:   {min_hold}-{max_hold} days")

        # Hold time by exit reason
        from collections import Counter
        for reason in ["profit_target", "time_exit", "data_end"]:
            reason_trades = [t for t in trades if t.exit_reason == reason]
            if reason_trades:
                r_hold = statistics.mean([(t.exit_date - t.entry_date).days for t in reason_trades])
                print(f"    {reason}: {len(reason_trades)} trades, avg {r_hold:.0f} days")

        # 2. SPY comparison
        print(f"\n  SPY COMPARISON (same hold period per trade):")
        spy_returns = []
        option_returns = []
        paired = []

        for t in trades:
            spy_ret = spy_return_over_period(t.entry_date, t.exit_date)
            if spy_ret is not None:
                spy_returns.append(spy_ret)
                option_returns.append(t.pnl_pct)
                paired.append((t, spy_ret))

        if spy_returns:
            avg_spy = statistics.mean(spy_returns)
            med_spy = statistics.median(spy_returns)
            avg_opt = statistics.mean(option_returns)
            med_opt = statistics.median(option_returns)
            spy_wr = sum(1 for r in spy_returns if r > 0) / len(spy_returns)

            # What if you just held SPY for each period?
            # With 1% portfolio per trade (options sizing), $300 per trade
            portfolio = 30000
            opt_per_trade = portfolio * 0.01  # $300
            total_opt_pnl = sum(r * opt_per_trade for r in option_returns)
            total_spy_pnl = sum(r * opt_per_trade for r in spy_returns)

            print(f"    Paired trades (with SPY data): {len(paired)}")
            print(f"    ")
            print(f"    {'Metric':<30} {'Options':>12} {'SPY B&H':>12} {'Alpha':>12}")
            print(f"    {'-'*30} {'-'*12} {'-'*12} {'-'*12}")
            print(f"    {'Avg return':<30} {avg_opt*100:>+11.1f}% {avg_spy*100:>+11.1f}% {(avg_opt-avg_spy)*100:>+11.1f}%")
            print(f"    {'Median return':<30} {med_opt*100:>+11.1f}% {med_spy*100:>+11.1f}% {(med_opt-med_spy)*100:>+11.1f}%")
            print(f"    {'Win rate':<30} {sum(1 for r in option_returns if r>0)/len(option_returns)*100:>11.1f}% {spy_wr*100:>11.1f}%")
            print(f"    {'Total $ (1% sizing, $30K)':<30} ${total_opt_pnl:>+10,.0f} ${total_spy_pnl:>+10,.0f} ${total_opt_pnl-total_spy_pnl:>+10,.0f}")

            # SPY comparison by exit reason
            print(f"\n    By exit reason:")
            for reason in ["profit_target", "time_exit", "data_end"]:
                rp = [(t, s) for t, s in paired if t.exit_reason == reason]
                if rp:
                    r_opt = statistics.mean([t.pnl_pct for t, _ in rp])
                    r_spy = statistics.mean([s for _, s in rp])
                    print(f"      {reason}: opt={r_opt*100:+.1f}%, spy={r_spy*100:+.1f}%, alpha={((r_opt-r_spy)*100):+.1f}%")

            # Correlation between SPY and options returns
            if len(spy_returns) > 10:
                corr = np.corrcoef(option_returns, spy_returns)[0, 1]
                print(f"\n    Correlation (options vs SPY): {corr:.3f}")

            # What % of the time did options beat SPY?
            beat_spy = sum(1 for o, s in zip(option_returns, spy_returns) if o > s)
            print(f"    Options beat SPY: {beat_spy}/{len(paired)} ({beat_spy/len(paired)*100:.1f}%)")

        else:
            print("    No SPY data available for comparison")

        # 3. Annual return estimate
        print(f"\n  ANNUALIZED PORTFOLIO ESTIMATE:")
        avg_pnl = statistics.mean([t.pnl_pct for t in trades])
        # Estimate trades per year: test period is ~25% of ~5 year span = 1.25 years
        trades_per_year = n / 1.25
        # With 1% sizing, each trade risks $300 on a $30K portfolio
        annual_return_pct = avg_pnl * trades_per_year * 0.01  # 1% sizing
        print(f"    Trades/year (est): ~{trades_per_year:.0f}")
        print(f"    Avg return/trade: {avg_pnl*100:+.1f}%")
        print(f"    Annual portfolio return (1% sizing): {annual_return_pct*100:+.1f}%")
        print(f"    Annual $ return ($30K portfolio): ${annual_return_pct * 30000:+,.0f}")

        # 4. Year-by-year breakdown
        print(f"\n  YEAR-BY-YEAR:")
        years = sorted(set(t.entry_date.year for t in trades))
        print(f"    {'Year':<6} {'N':>6} {'WR':>8} {'Avg P&L':>10} {'Med P&L':>10} {'Avg Hold':>10}")
        print(f"    {'-'*6} {'-'*6} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
        for year in years:
            yt = [t for t in trades if t.entry_date.year == year]
            yn = len(yt)
            ywr = sum(1 for t in yt if t.win) / yn
            yavg = statistics.mean([t.pnl_pct for t in yt])
            ymed = statistics.median([t.pnl_pct for t in yt])
            yhold = statistics.mean([(t.exit_date - t.entry_date).days for t in yt])
            print(f"    {year:<6} {yn:>6} {ywr*100:>7.1f}% {yavg*100:>+9.1f}% {ymed*100:>+9.1f}% {yhold:>9.0f}d")

        # 5. Entry premium analysis
        print(f"\n  ENTRY PREMIUM ANALYSIS:")
        premiums = [t.entry_price for t in trades]
        print(f"    Avg premium: ${statistics.mean(premiums):.2f}")
        print(f"    Median premium: ${statistics.median(premiums):.2f}")
        print(f"    Range: ${min(premiums):.2f} - ${max(premiums):.2f}")

        # Win rate by premium bucket
        for label, lo, hi in [("$0-$1", 0, 1), ("$1-$3", 1, 3), ("$3-$7", 3, 7), ("$7+", 7, 9999)]:
            bucket = [t for t in trades if lo <= t.entry_price < hi]
            if bucket:
                bwr = sum(1 for t in bucket if t.win) / len(bucket)
                bavg = statistics.mean([t.pnl_pct for t in bucket])
                print(f"    {label}: N={len(bucket):>5}, WR={bwr*100:.1f}%, Avg={bavg*100:+.1f}%")

    # ── Data source summary ──────────────────────────────────────────

    print(f"\n{'='*70}")
    print(f"  DATA SOURCES")
    print(f"{'='*70}")
    print(f"  Options pricing: Theta Data local terminal (http://127.0.0.1:25503)")
    print(f"    - Real historical EOD data (close, bid, ask, volume)")
    print(f"    - Coverage: 2012-present, all US equities with listed options")
    print(f"    - Price: bid/ask midpoint when close=0, else close price")
    print(f"    - Plan: Value (local REST API, rate-limited)")
    print(f"  ")
    print(f"  Stock prices: Alpaca historical bars (CSV cache)")
    print(f"    - {PRICES_DIR}")
    print(f"    - Used for: entry stock price (T+1 open), SPY benchmark")
    print(f"  ")
    print(f"  Insider data: EDGAR Form 4 bulk filings (SQLite catalog)")
    print(f"    - {DB_PATH}")
    print(f"    - Buy trades: 55K, Sell trades: 396K")
    print(f"  ")
    print(f"  Cache: {CACHE_PATH}")
    print(f"    - {CACHE_PATH.stat().st_size / 1e6:.0f} MB, {len(load_cache()):,} entries")
    print(f"  ")
    print(f"  Alpaca options API (NOT used for backtest):")
    print(f"    - /v2/options/contracts: contract discovery for live trading")
    print(f"    - /v1beta1/options/bars: intraday bars for live monitoring")
    print(f"    - Used by: options_leg.py (cluster buy overlay)")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    run_analysis()
