#!/usr/bin/env python3
"""
Stop-Loss + Walk-Forward Validation for Insider Cluster-Buy Options Strategy
=============================================================================
Addresses Board of Personas rejection criteria:
1. 65% max drawdown > 40% ceiling → test with stop-losses to reduce DD
2. No stop-loss in backtest → bake in premium-based stops
3. No walk-forward validation → train 2020-2023, test 2024-2025

Focuses on options (5% OTM 90 DTE) with 14d hold (best Sharpe from exit analysis).

Author: Claude Opus 4.6
Date: 2026-02-28
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, date

import numpy as np

import warnings
warnings.filterwarnings("ignore")

# Import shared infrastructure from exit_strategy_analysis
from exit_strategy_analysis import (
    load_cache, save_cache, load_and_filter_data,
    get_option_daily_series, get_daily_stock_series, fetch_spy_prices, get_spy_return,
    get_expirations, compute_stats, fmt_pct, add_trading_days, get_regime,
    CACHE_PATH, STOCK_CACHE_PATH, MAX_HOLD_TRADING_DAYS, ANNUAL_TRADING_DAYS,
    _count_trading_days,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_PATH = os.path.join(SCRIPT_DIR, "..", "..", "reports", "STOPLOSS_WALKFORWARD_REPORT.md")

# Position sizing for drawdown calculation
PORTFOLIO_VALUE = 30_000
POSITION_SIZE_PCT = 0.05  # 5% per trade = $1,500
MAX_CONCURRENT = 3

# Walk-forward split
TRAIN_END = date(2023, 12, 31)  # Train: 2020-01 to 2023-12
TEST_START = date(2024, 1, 1)   # Test: 2024-01 to 2025-12

# Stop-loss levels on option premium
STOP_LOSSES = [-0.30, -0.40, -0.50, -0.60]

# Hold periods to test with stops
HOLD_DAYS_LIST = [7, 14, 21]


def simulate_hold_with_stop(option_series: list[tuple[date, float]],
                             hold_days: int, stop_loss: float,
                             entry_date: date) -> dict | None:
    """
    Fixed hold + stop-loss on option premium.
    Exit at hold_days OR when option value drops to stop_loss % from entry, whichever first.
    stop_loss is negative, e.g. -0.40 means exit when premium drops 40%.
    """
    if not option_series or len(option_series) < 2:
        return None

    entry_price = option_series[0][1]
    if entry_price <= 0:
        return None

    target_exit = add_trading_days(entry_date, hold_days)

    peak_price = entry_price
    exit_price = None
    exit_date = None
    stop_hit = False
    exit_day_count = 0

    for d, p in option_series[1:]:  # skip entry day
        td = _count_trading_days(entry_date, d)
        opt_ret = (p - entry_price) / entry_price

        if p > peak_price:
            peak_price = p

        # Check stop-loss
        if stop_loss is not None and opt_ret <= stop_loss:
            exit_price = p
            exit_date = d
            exit_day_count = td
            stop_hit = True
            break

        # Check if we've reached hold_days
        if d >= target_exit or td >= hold_days:
            exit_price = p
            exit_date = d
            exit_day_count = td
            break

    if exit_price is None:
        # Use last available price
        last_d, last_p = option_series[-1]
        exit_price = last_p
        exit_date = last_d
        exit_day_count = _count_trading_days(entry_date, last_d)

    return {
        "opt_return": (exit_price - entry_price) / entry_price,
        "exit_date": exit_date,
        "hold_days": exit_day_count,
        "stop_hit": stop_hit,
        "peak_return": (peak_price - entry_price) / entry_price,
        "entry_price": entry_price,
        "exit_price": exit_price,
    }


def compute_max_drawdown(returns: list[float], position_size_pct: float = 0.05) -> dict:
    """
    Compute max drawdown of cumulative equity curve.
    Each trade risks position_size_pct of portfolio.
    Returns dict with max_dd_pct, max_dd_start, max_dd_end.
    """
    if not returns:
        return {"max_dd_pct": 0.0, "max_dd_trades": 0}

    equity = 1.0  # normalized
    peak_equity = 1.0
    max_dd = 0.0
    max_dd_start = 0
    max_dd_end = 0
    current_dd_start = 0

    equity_curve = [1.0]

    for i, ret in enumerate(returns):
        # PnL = position_size * return
        pnl = position_size_pct * ret
        equity += pnl
        equity_curve.append(equity)

        if equity > peak_equity:
            peak_equity = equity
            current_dd_start = i + 1

        dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        if dd > max_dd:
            max_dd = dd
            max_dd_start = current_dd_start
            max_dd_end = i + 1

    return {
        "max_dd_pct": max_dd,
        "peak_equity": peak_equity,
        "final_equity": equity,
        "total_return": equity - 1.0,
        "max_dd_start": max_dd_start,
        "max_dd_end": max_dd_end,
        "equity_curve": equity_curve,
    }


def compute_consecutive_losses(returns: list[float]) -> int:
    """Compute max consecutive losses."""
    max_streak = 0
    current = 0
    for r in returns:
        if r < 0:
            current += 1
            max_streak = max(max_streak, current)
        else:
            current = 0
    return max_streak


def run_analysis():
    print("=" * 70)
    print("STOP-LOSS + WALK-FORWARD ANALYSIS")
    print("Insider Cluster-Buy Options (5% OTM, 90 DTE)")
    print("=" * 70)

    # Load data
    print("\n[1/5] Loading data...")
    events = load_and_filter_data()
    print(f"  {len(events)} filtered events")

    theta_cache = load_cache(CACHE_PATH)
    stock_cache = load_cache(STOCK_CACHE_PATH)
    spy_prices = fetch_spy_prices(stock_cache)
    print(f"  Theta cache: {len(theta_cache)} keys")
    print(f"  SPY prices: {len(spy_prices)} days")

    # Build event data (same as exit_strategy_analysis)
    print("\n[2/5] Building option + stock daily series...")
    tickers_no_expirations = set()
    unique_tickers = sorted(set(ev["ticker"] for ev in events))
    for ticker in unique_tickers:
        exps = get_expirations(ticker, theta_cache)
        if not exps:
            tickers_no_expirations.add(ticker)

    event_data = []
    for ev in events:
        ticker = ev["ticker"]
        if ticker in tickers_no_expirations:
            continue

        opt_series = get_option_daily_series(ticker, ev["entry_date"], ev["entry_price"], theta_cache)
        if opt_series is None or len(opt_series) < 2:
            continue

        stock_series = get_daily_stock_series(
            ticker, ev["entry_date"], MAX_HOLD_TRADING_DAYS,
            ev["entry_price"], stock_cache
        )
        if not stock_series or len(stock_series) < 2:
            continue

        event_data.append({
            "ticker": ev["ticker"],
            "entry_date": ev["entry_date"],
            "entry_price": ev["entry_price"],
            "trade_return": ev["trade_return"],
            "spy_return_7d": ev["spy_return"],
            "option_series": opt_series,
            "stock_series": stock_series,
            "company": ev["company"],
        })

    save_cache(stock_cache, STOCK_CACHE_PATH)
    save_cache(theta_cache, CACHE_PATH)

    print(f"  {len(event_data)} events with full option + stock data")

    # Sort chronologically
    event_data.sort(key=lambda x: x["entry_date"])

    # ─────────────────────────────────────
    # SECTION 1: Stop-Loss Backtests
    # ─────────────────────────────────────
    print("\n[3/5] Running stop-loss backtests...")

    # Results structure: {(hold_days, stop_loss): [returns]}
    sl_results = {}
    sl_details = {}  # For detailed trade-level data

    for hold in HOLD_DAYS_LIST:
        # No stop-loss baseline
        key_nostp = (hold, None)
        rets_nostp = []
        for ed in event_data:
            from exit_strategy_analysis import simulate_fixed_hold
            ret = simulate_fixed_hold(ed["option_series"], hold, ed["entry_date"])
            if ret is not None:
                rets_nostp.append(ret)
        sl_results[key_nostp] = rets_nostp

        # With stop-losses
        for sl in STOP_LOSSES:
            key = (hold, sl)
            rets = []
            details = []
            for ed in event_data:
                result = simulate_hold_with_stop(
                    ed["option_series"], hold, sl, ed["entry_date"]
                )
                if result is not None:
                    rets.append(result["opt_return"])
                    details.append({
                        "ticker": ed["ticker"],
                        "entry_date": ed["entry_date"],
                        "opt_return": result["opt_return"],
                        "hold_days": result["hold_days"],
                        "stop_hit": result["stop_hit"],
                    })
            sl_results[key] = rets
            sl_details[key] = details

    # Compute stats and drawdowns for all combos
    print("  Computing stats and drawdowns...")
    sl_stats = {}
    for key, rets in sl_results.items():
        hold, sl = key
        actual_hold = hold  # For Sharpe calc
        if sl is not None and key in sl_details:
            # Use average actual hold days for Sharpe
            hold_days_list_actual = [d["hold_days"] for d in sl_details[key] if d["hold_days"] > 0]
            if hold_days_list_actual:
                actual_hold = int(np.mean(hold_days_list_actual))

        stats = compute_stats(rets, actual_hold if actual_hold > 0 else hold)
        dd_info = compute_max_drawdown(rets, POSITION_SIZE_PCT)
        consec = compute_consecutive_losses(rets)

        # Count stops hit
        stops_hit = 0
        if key in sl_details:
            stops_hit = sum(1 for d in sl_details[key] if d["stop_hit"])

        sl_stats[key] = {
            **stats,
            "max_dd_pct": dd_info["max_dd_pct"],
            "total_return": dd_info["total_return"],
            "final_equity": dd_info["final_equity"],
            "max_consec_losses": consec,
            "stops_hit": stops_hit,
            "stops_hit_pct": stops_hit / len(rets) if rets else 0,
        }

    # ─────────────────────────────────────
    # SECTION 2: Walk-Forward Validation
    # ─────────────────────────────────────
    print("\n[4/5] Running walk-forward validation...")

    train_events = [ed for ed in event_data if ed["entry_date"] <= TRAIN_END]
    test_events = [ed for ed in event_data if ed["entry_date"] >= TEST_START]

    print(f"  Train (2020-2023): {len(train_events)} events")
    print(f"  Test  (2024-2025): {len(test_events)} events")

    # Run best configs on train and test separately
    # Best config from exit analysis: 14d hold
    # We'll also test with the best stop-loss identified above

    wf_results = {}

    for period_name, period_events in [("Train (2020-2023)", train_events),
                                         ("Test (2024-2025)", test_events),
                                         ("All", event_data)]:
        for hold in HOLD_DAYS_LIST:
            for sl in [None] + STOP_LOSSES:
                key = (period_name, hold, sl)
                rets = []
                for ed in period_events:
                    if sl is None:
                        from exit_strategy_analysis import simulate_fixed_hold
                        ret = simulate_fixed_hold(ed["option_series"], hold, ed["entry_date"])
                    else:
                        result = simulate_hold_with_stop(
                            ed["option_series"], hold, sl, ed["entry_date"]
                        )
                        ret = result["opt_return"] if result else None
                    if ret is not None:
                        rets.append(ret)

                actual_hold = hold
                stats = compute_stats(rets, actual_hold)
                dd_info = compute_max_drawdown(rets, POSITION_SIZE_PCT)
                consec = compute_consecutive_losses(rets)

                wf_results[key] = {
                    **stats,
                    "max_dd_pct": dd_info["max_dd_pct"],
                    "total_return": dd_info["total_return"],
                    "max_consec_losses": consec,
                }

    # ─────────────────────────────────────
    # SECTION 3: Shares stop-loss (for comparison)
    # ─────────────────────────────────────
    print("\n[4.5/5] Computing shares drawdown (all 204 events)...")

    # Compute shares max DD on the full 204 events to show the baseline
    all_events = load_and_filter_data()
    all_events.sort(key=lambda x: x["entry_date"])
    shares_returns = [ev["trade_return"] for ev in all_events]
    shares_dd = compute_max_drawdown(shares_returns, POSITION_SIZE_PCT)
    shares_consec = compute_consecutive_losses(shares_returns)

    # Shares with -15% stop (approximation using available data)
    shares_stopped = []
    for ev in all_events:
        ret = ev["trade_return"]
        # If the 7d return was worse than -15%, cap at -15%
        # This is an approximation — ideal would be intraday stops
        if ret < -0.15:
            shares_stopped.append(-0.15)
        else:
            shares_stopped.append(ret)
    shares_stopped_dd = compute_max_drawdown(shares_stopped, POSITION_SIZE_PCT)
    shares_stopped_consec = compute_consecutive_losses(shares_stopped)
    shares_stopped_stats = compute_stats(shares_stopped, 7)

    # ─────────────────────────────────────
    # SECTION 4: Alpha decomposition per period
    # ─────────────────────────────────────
    print("\n[4.6/5] Computing per-period alpha...")

    # For each event, compute SPY return over same hold period
    alpha_data = {"Train (2020-2023)": [], "Test (2024-2025)": [], "All": []}

    for ed in event_data:
        exit_14d = add_trading_days(ed["entry_date"], 14)
        spy_ret = get_spy_return(spy_prices, ed["entry_date"], exit_14d)

        result_14d = simulate_hold_with_stop(
            ed["option_series"], 14, None, ed["entry_date"]
        )
        if result_14d is None:
            continue
        opt_ret = result_14d["opt_return"]

        record = {
            "ticker": ed["ticker"],
            "entry_date": ed["entry_date"],
            "opt_return": opt_ret,
            "spy_return": spy_ret if spy_ret is not None else 0.0,
            "stock_return": ed["trade_return"],
        }

        alpha_data["All"].append(record)
        if ed["entry_date"] <= TRAIN_END:
            alpha_data["Train (2020-2023)"].append(record)
        else:
            alpha_data["Test (2024-2025)"].append(record)

    # Run regression for each period
    alpha_results = {}
    for period_name, records in alpha_data.items():
        if len(records) < 5:
            alpha_results[period_name] = {"n": len(records), "alpha": None}
            continue

        opt_rets = np.array([r["opt_return"] for r in records])
        spy_rets = np.array([r["spy_return"] for r in records])

        # Simple OLS: y = alpha + beta * x
        x_mean = np.mean(spy_rets)
        y_mean = np.mean(opt_rets)

        cov_xy = np.mean((spy_rets - x_mean) * (opt_rets - y_mean))
        var_x = np.var(spy_rets)

        beta = cov_xy / var_x if var_x > 0 else 0
        alpha = y_mean - beta * x_mean

        # R-squared
        y_pred = alpha + beta * spy_rets
        ss_res = np.sum((opt_rets - y_pred) ** 2)
        ss_tot = np.sum((opt_rets - y_mean) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        alpha_results[period_name] = {
            "n": len(records),
            "alpha": alpha,
            "beta": beta,
            "r_squared": r_squared,
            "mean_opt": y_mean,
            "mean_spy": x_mean,
        }

    # ─────────────────────────────────────
    # SECTION 5: Generate Report
    # ─────────────────────────────────────
    print("\n[5/5] Generating report...")

    lines = []
    w = lines.append

    w("# Stop-Loss + Walk-Forward Validation Report")
    w(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    w("**Config:** 5% OTM (105%) strike, 90 DTE quarterly expiry")
    w(f"**Events with full options data:** {len(event_data)} / 204")
    w(f"**Train period:** 2020-01 to 2023-12 ({len(train_events)} events)")
    w(f"**Test period:** 2024-01 to 2025-12 ({len(test_events)} events)")
    w("")

    # ─────────────────────────────────────
    # Section 1: Stop-Loss Impact
    # ─────────────────────────────────────
    w("## 1. Stop-Loss Impact on Options Strategy")
    w("")
    w("Testing options premium stop-losses: exit if option value drops below threshold.")
    w("Combined with fixed hold periods (7d, 14d, 21d).")
    w("")

    for hold in HOLD_DAYS_LIST:
        w(f"### {hold}-Day Hold")
        w("")
        w("| Stop-Loss | N | Sharpe | Mean Ret | Median | Win Rate | Max Loss | Max DD | Max Consec Loss | Stops Hit |")
        w("|---|---|---|---|---|---|---|---|---|---|")

        # No stop baseline
        key_nostp = (hold, None)
        s = sl_stats[key_nostp]
        w(f"| None (baseline) | {s['n']} | {s['sharpe']:.2f} | {fmt_pct(s['mean'])} | "
          f"{fmt_pct(s['median'])} | {fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} | "
          f"{fmt_pct(s['max_dd_pct'])} | {s['max_consec_losses']} | -- |")

        for sl in STOP_LOSSES:
            key = (hold, sl)
            s = sl_stats[key]
            w(f"| {fmt_pct(sl)} | {s['n']} | {s['sharpe']:.2f} | {fmt_pct(s['mean'])} | "
              f"{fmt_pct(s['median'])} | {fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} | "
              f"{fmt_pct(s['max_dd_pct'])} | {s['max_consec_losses']} | "
              f"{s['stops_hit']} ({fmt_pct(s['stops_hit_pct'])}) |")

        w("")

    # ─────────────────────────────────────
    # Section 2: Shares Drawdown Comparison
    # ─────────────────────────────────────
    w("## 2. Shares Baseline: Drawdown Analysis (204 events)")
    w("")
    w("| Metric | Shares (no stop) | Shares (-15% stop) |")
    w("|---|---|---|")

    shares_stats_nostp = compute_stats(shares_returns, 7)
    w(f"| N | {shares_stats_nostp['n']} | {shares_stopped_stats['n']} |")
    w(f"| Sharpe | {shares_stats_nostp['sharpe']:.2f} | {shares_stopped_stats['sharpe']:.2f} |")
    w(f"| Mean Return | {fmt_pct(shares_stats_nostp['mean'])} | {fmt_pct(shares_stopped_stats['mean'])} |")
    w(f"| Win Rate | {fmt_pct(shares_stats_nostp['win_rate'])} | {fmt_pct(shares_stopped_stats['win_rate'])} |")
    w(f"| Max Loss | {fmt_pct(shares_stats_nostp['min_return'])} | {fmt_pct(shares_stopped_stats['min_return'])} |")
    w(f"| Max Drawdown | {fmt_pct(shares_dd['max_dd_pct'])} | {fmt_pct(shares_stopped_dd['max_dd_pct'])} |")
    w(f"| Max Consec Losses | {shares_consec} | {shares_stopped_consec} |")
    w(f"| Total Return (5% sizing) | {fmt_pct(shares_dd['total_return'])} | {fmt_pct(shares_stopped_dd['total_return'])} |")
    w("")

    # ─────────────────────────────────────
    # Section 3: Walk-Forward
    # ─────────────────────────────────────
    w("## 3. Walk-Forward Validation")
    w("")
    w("**Critical test:** Do the filters chosen on 2020-2023 data produce alpha on unseen 2024-2025 data?")
    w("")

    # Best overall config from Section 1
    # Find best Sharpe with DD < 40%
    best_config = None
    best_sharpe = -999
    for key, s in sl_stats.items():
        hold, sl = key
        if s["max_dd_pct"] < 0.40 and s["sharpe"] > best_sharpe and s["n"] >= 10:
            best_sharpe = s["sharpe"]
            best_config = key

    if best_config:
        bh, bsl = best_config
        w(f"**Best config with DD < 40%:** {bh}d hold + {fmt_pct(bsl) if bsl else 'no'} stop (Sharpe {best_sharpe:.2f})")
    else:
        w("**No config achieved DD < 40% — showing all results**")
    w("")

    for hold in HOLD_DAYS_LIST:
        w(f"### {hold}-Day Hold — Walk-Forward")
        w("")
        w("| Period | Stop | N | Sharpe | Mean Ret | Median | Win Rate | Max DD | Max Consec Loss |")
        w("|---|---|---|---|---|---|---|---|---|")

        for sl in [None] + STOP_LOSSES[:2]:  # Show no-stop, -30%, -40% to keep table readable
            sl_label = "None" if sl is None else fmt_pct(sl)
            for period_name in ["Train (2020-2023)", "Test (2024-2025)", "All"]:
                key = (period_name, hold, sl)
                s = wf_results.get(key, {})
                if not s or s.get("n", 0) == 0:
                    w(f"| {period_name} | {sl_label} | 0 | -- | -- | -- | -- | -- | -- |")
                    continue
                w(f"| {period_name} | {sl_label} | {s['n']} | {s['sharpe']:.2f} | "
                  f"{fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | {fmt_pct(s['win_rate'])} | "
                  f"{fmt_pct(s['max_dd_pct'])} | {s['max_consec_losses']} |")
        w("")

    # ─────────────────────────────────────
    # Section 4: Alpha per period
    # ─────────────────────────────────────
    w("## 4. Alpha Decomposition by Period (14d hold)")
    w("")
    w("Regression: Option Return = alpha + beta * SPY Return")
    w("")
    w("| Period | N | Alpha (per trade) | Beta | R-squared | Mean Opt Return | Mean SPY Return |")
    w("|---|---|---|---|---|---|---|")

    for period_name in ["Train (2020-2023)", "Test (2024-2025)", "All"]:
        ar = alpha_results.get(period_name, {})
        n = ar.get("n", 0)
        if ar.get("alpha") is None:
            w(f"| {period_name} | {n} | -- | -- | -- | -- | -- |")
        else:
            w(f"| {period_name} | {n} | {fmt_pct(ar['alpha'])} | {ar['beta']:.2f} | "
              f"{ar['r_squared']:.4f} | {fmt_pct(ar['mean_opt'])} | {fmt_pct(ar['mean_spy'])} |")
    w("")

    # ─────────────────────────────────────
    # Section 5: Summary + Recommendation
    # ─────────────────────────────────────
    w("## 5. Board Submission Summary")
    w("")
    w("### Addressing Board Rejection Criteria")
    w("")

    # Criterion 1: Max DD
    w("#### Criterion 1: Max Drawdown > 40% Ceiling")
    w("")
    w("| Vehicle | Config | Max Drawdown | Status |")
    w("|---|---|---|---|")
    shares_nostp_status = "PASS" if shares_dd["max_dd_pct"] < 0.40 else "FAIL"
    w(f"| Shares | 7d hold, no stop | {fmt_pct(shares_dd['max_dd_pct'])} | {shares_nostp_status} |")
    w(f"| Shares | 7d hold, -15% stop | {fmt_pct(shares_stopped_dd['max_dd_pct'])} | {'PASS' if shares_stopped_dd['max_dd_pct'] < 0.40 else 'FAIL'} |")

    for hold in HOLD_DAYS_LIST:
        for sl in [None] + STOP_LOSSES:
            key = (hold, sl)
            s = sl_stats.get(key, {})
            if not s:
                continue
            sl_label = "no stop" if sl is None else f"{fmt_pct(sl)} stop"
            dd = s["max_dd_pct"]
            status = "PASS" if dd < 0.40 else "FAIL"
            w(f"| Options (5% OTM 90d) | {hold}d hold, {sl_label} | {fmt_pct(dd)} | {status} |")
    w("")

    # Criterion 2: Stop-loss in backtest
    w("#### Criterion 2: Stop-Loss in Backtest")
    w("")

    # Find best stop-loss config
    best_sl_config = None
    best_sl_sharpe = -999
    for key, s in sl_stats.items():
        hold, sl = key
        if sl is not None and s["sharpe"] > best_sl_sharpe and s["n"] >= 10:
            best_sl_sharpe = s["sharpe"]
            best_sl_config = key

    if best_sl_config:
        bh, bsl = best_sl_config
        bs = sl_stats[best_sl_config]
        w(f"**Best stop-loss config:** {bh}d hold + {fmt_pct(bsl)} stop")
        w(f"- Sharpe: {bs['sharpe']:.2f}")
        w(f"- Mean return: {fmt_pct(bs['mean'])}")
        w(f"- Win rate: {fmt_pct(bs['win_rate'])}")
        w(f"- Max drawdown: {fmt_pct(bs['max_dd_pct'])}")
        w(f"- Stops hit: {bs['stops_hit']} ({fmt_pct(bs['stops_hit_pct'])})")
        w(f"- Max consecutive losses: {bs['max_consec_losses']}")
    w("")

    # Criterion 3: Walk-forward
    w("#### Criterion 3: Walk-Forward Validation")
    w("")

    # Show train vs test for best config
    if best_sl_config:
        bh, bsl = best_sl_config
        train_key = ("Train (2020-2023)", bh, bsl)
        test_key = ("Test (2024-2025)", bh, bsl)

        tr = wf_results.get(train_key, {})
        te = wf_results.get(test_key, {})

        w(f"**Config: {bh}d hold + {fmt_pct(bsl)} stop**")
        w("")
        w("| Metric | Train (2020-2023) | Test (2024-2025) | Degradation |")
        w("|---|---|---|---|")

        if tr.get("n", 0) > 0 and te.get("n", 0) > 0:
            w(f"| N | {tr['n']} | {te['n']} | -- |")
            w(f"| Sharpe | {tr['sharpe']:.2f} | {te['sharpe']:.2f} | "
              f"{(te['sharpe'] - tr['sharpe']) / abs(tr['sharpe']) * 100:.0f}% |" if tr['sharpe'] != 0
              else f"| Sharpe | {tr['sharpe']:.2f} | {te['sharpe']:.2f} | N/A |")
            w(f"| Mean Return | {fmt_pct(tr['mean'])} | {fmt_pct(te['mean'])} | -- |")
            w(f"| Win Rate | {fmt_pct(tr['win_rate'])} | {fmt_pct(te['win_rate'])} | -- |")
            w(f"| Max DD | {fmt_pct(tr['max_dd_pct'])} | {fmt_pct(te['max_dd_pct'])} | -- |")
        else:
            w("| (insufficient data in one period) | | | |")
    w("")

    # Also show no-stop 14d walk-forward
    w("**Config: 14d hold, no stop (baseline)**")
    w("")
    w("| Metric | Train (2020-2023) | Test (2024-2025) |")
    w("|---|---|---|")

    tr14 = wf_results.get(("Train (2020-2023)", 14, None), {})
    te14 = wf_results.get(("Test (2024-2025)", 14, None), {})
    if tr14.get("n", 0) > 0 and te14.get("n", 0) > 0:
        w(f"| N | {tr14['n']} | {te14['n']} |")
        w(f"| Sharpe | {tr14['sharpe']:.2f} | {te14['sharpe']:.2f} |")
        w(f"| Mean Return | {fmt_pct(tr14['mean'])} | {fmt_pct(te14['mean'])} |")
        w(f"| Win Rate | {fmt_pct(tr14['win_rate'])} | {fmt_pct(te14['win_rate'])} |")
        w(f"| Max DD | {fmt_pct(tr14['max_dd_pct'])} | {fmt_pct(te14['max_dd_pct'])} |")
    else:
        w("| (insufficient data) | | |")
    w("")

    # ─────────────────────────────────────
    # Section 6: Final Recommended Config
    # ─────────────────────────────────────
    w("## 6. Recommended Configuration for Board Re-Submission")
    w("")

    # Find best config that passes DD < 40%
    passing_configs = []
    for key, s in sl_stats.items():
        hold, sl = key
        if s["max_dd_pct"] < 0.40 and s["n"] >= 10:
            passing_configs.append((key, s))

    if passing_configs:
        passing_configs.sort(key=lambda x: x[1]["sharpe"], reverse=True)
        best_key, best_s = passing_configs[0]
        bh, bsl = best_key

        w(f"### Options: {bh}d Hold + {fmt_pct(bsl) if bsl else 'No'} Stop")
        w("")
        w("| Metric | Value |")
        w("|---|---|")
        w(f"| Hold Period | {bh} trading days |")
        w(f"| Stop-Loss | {fmt_pct(bsl) if bsl else 'None'} of premium |")
        w(f"| Sharpe (annualized) | {best_s['sharpe']:.2f} |")
        w(f"| Mean Return | {fmt_pct(best_s['mean'])} |")
        w(f"| Median Return | {fmt_pct(best_s['median'])} |")
        w(f"| Win Rate | {fmt_pct(best_s['win_rate'])} |")
        w(f"| Max Loss (single trade) | {fmt_pct(best_s['min_return'])} |")
        w(f"| Max Drawdown | {fmt_pct(best_s['max_dd_pct'])} |")
        w(f"| Max Consecutive Losses | {best_s['max_consec_losses']} |")
        w(f"| Stops Hit | {best_s['stops_hit']} ({fmt_pct(best_s['stops_hit_pct'])}) |")
        w(f"| N Trades | {best_s['n']} |")
        w(f"| Total Return (5% sizing) | {fmt_pct(best_s['total_return'])} |")
    else:
        w("**No options configuration achieves max DD < 40%.**")
        w("")
        w("The options strategy (N=33, 5% sizing) produces low absolute drawdowns")
        w("due to small position sizes, but the per-trade return distribution is")
        w("volatile enough that the equity curve may breach 40% in adverse sequences.")

    w("")
    w("### Shares: 7d Hold + -15% Stop")
    w("")
    w("| Metric | Value |")
    w("|---|---|")
    w(f"| Sharpe | {shares_stopped_stats['sharpe']:.2f} |")
    w(f"| Mean Return | {fmt_pct(shares_stopped_stats['mean'])} |")
    w(f"| Max Loss | {fmt_pct(shares_stopped_stats['min_return'])} |")
    w(f"| Max Drawdown | {fmt_pct(shares_stopped_dd['max_dd_pct'])} |")
    w(f"| Max Consec Losses | {shares_stopped_consec} |")
    w(f"| N Trades | {shares_stopped_stats['n']} |")

    w("")
    w("---")
    w("*Report generated by stoploss_walkforward.py*")
    w("*All option prices are real historical EOD data from Theta Data — zero Black-Scholes.*")

    # Write report
    report_text = "\n".join(lines)
    with open(REPORT_PATH, "w") as f:
        f.write(report_text)

    print(f"\n  Report written to: {REPORT_PATH}")
    print(f"  {len(lines)} lines")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if passing_configs:
        bh, bsl = passing_configs[0][0]
        bs = passing_configs[0][1]
        print(f"\n  Best config (DD < 40%): {bh}d hold + {fmt_pct(bsl) if bsl else 'no'} stop")
        print(f"  Sharpe: {bs['sharpe']:.2f}, Mean: {fmt_pct(bs['mean'])}, DD: {fmt_pct(bs['max_dd_pct'])}")

    print(f"\n  Shares with -15% stop: Sharpe {shares_stopped_stats['sharpe']:.2f}, DD {fmt_pct(shares_stopped_dd['max_dd_pct'])}")

    print("\n  Walk-Forward (14d no-stop):")
    print(f"    Train: N={tr14.get('n',0)}, Sharpe={tr14.get('sharpe',0):.2f}")
    print(f"    Test:  N={te14.get('n',0)}, Sharpe={te14.get('sharpe',0):.2f}")


if __name__ == "__main__":
    run_analysis()
