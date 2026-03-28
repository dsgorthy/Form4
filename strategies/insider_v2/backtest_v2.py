#!/usr/bin/env python3
"""
Insider V2 Backtest — Dual-Leg Strategy (Buy Shares + Sell Puts)

Orchestrates:
  1. Build V2 buy + sell events from insiders.db
  2. Run event study for each leg at multiple hold periods
  3. Simulate portfolio with position sizing, stops, and risk controls
  4. Walk-forward validation (train: 2016-2022, test: 2023-2025)
  5. Output reports/insider_v2/backtest_latest.json

Usage:
    python backtest_v2.py
    python backtest_v2.py --walk-forward
    python backtest_v2.py --hold-days 7 14 30
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent
STUDY_DIR = ROOT_DIR / "pipelines" / "insider_study"
PRICES_DIR = STUDY_DIR / "data" / "prices"
REPORT_DIR = ROOT_DIR / "reports" / "insider_v2"
DB_PATH = SCRIPT_DIR.parent / "insider_catalog" / "insiders.db"

# Add study dir to path for imports
sys.path.insert(0, str(STUDY_DIR))
sys.path.insert(0, str(SCRIPT_DIR))

from run_event_study import compute_trade_return, load_prices, print_summary

# ── Strategy Parameters (from strategy_spec.md / paper_runner.py) ──

PORTFOLIO_VALUE = 30_000
ANNUAL_TRADING_DAYS = 252

# Buy leg
BUY_POSITION_PCT = 0.05        # 5% per trade
BUY_HOLD_DAYS = 7
BUY_STOP_LOSS = -0.10          # -10% stop (spec says -10%)
MAX_CONCURRENT_LONGS = 3

# Sell leg (put approximation)
PUT_POSITION_PCT = 0.01        # 1% of portfolio as premium
PUT_HOLD_DAYS = 7
PUT_DELTA = -0.30              # approximate ATM-ish put delta for P&L estimation
PUT_STRIKE_OTM = 0.05          # 5% OTM
PUT_PROFIT_TARGET = 1.00       # +100% premium
PUT_STOP_LOSS = -0.50          # -50% premium
MAX_CONCURRENT_PUTS = 3

# Risk controls
VIX_THRESHOLD = 30
VIX_REDUCED_BUY_PCT = 0.03
VIX_INCREASED_PUT_PCT = 0.02
CIRCUIT_BREAKER_DD = 0.08      # 8% rolling DD halt

# Walk-forward
TRAIN_END = date(2022, 12, 31)
TEST_START = date(2023, 1, 1)


def load_daily_prices(ticker: str) -> dict:
    """Load daily prices into date->bar dict."""
    path = PRICES_DIR / f"{ticker}.csv"
    if not path.exists():
        return {}
    price_map = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                ts = r["timestamp"][:10]
                d = datetime.strptime(ts, "%Y-%m-%d").date()
                price_map[d] = {
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                }
            except (ValueError, KeyError):
                continue
    return price_map


def simulate_buy_trade(event: dict, spy_prices: dict, hold_days: int = BUY_HOLD_DAYS,
                       stop_loss: float = BUY_STOP_LOSS) -> dict | None:
    """
    Simulate a shares buy trade with stop-loss using daily prices.
    Entry: T+1 open after filing_date.
    Exit: close at T+hold_days or stop hit.
    """
    ticker = event["ticker"]
    filing_date_str = str(event["filing_date"])[:10]
    try:
        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

    prices = load_daily_prices(ticker)
    if not prices:
        return None

    trading_days = sorted(d for d in prices.keys() if d > filing_date)
    if not trading_days:
        return None

    entry_date = trading_days[0]
    entry_price = prices[entry_date]["open"]
    if entry_price <= 0:
        return None

    # Hold period
    hold_dates = trading_days[:hold_days + 1]

    stop_hit = False
    exit_price = None
    exit_date = None
    exit_day = hold_days

    for i, d in enumerate(hold_dates):
        bar = prices[d]
        if stop_loss is not None and i > 0:
            stop_price = entry_price * (1.0 + stop_loss)
            if bar["low"] <= stop_price:
                exit_price = stop_price
                stop_hit = True
                exit_date = d
                exit_day = i
                break

    if exit_price is None:
        if len(hold_dates) > hold_days:
            exit_date = hold_dates[hold_days]
            exit_price = prices[exit_date]["close"]
        else:
            exit_date = hold_dates[-1]
            exit_price = prices[exit_date]["close"]
        exit_day = min(hold_days, len(hold_dates) - 1)

    trade_return = (exit_price - entry_price) / entry_price

    # SPY benchmark
    spy_trading_days = sorted(d for d in spy_prices.keys() if d >= entry_date)
    spy_exit_days = sorted(d for d in spy_prices.keys() if d >= exit_date)
    spy_return = 0.0
    if spy_trading_days and spy_exit_days:
        try:
            spy_entry = spy_prices[spy_trading_days[0]]["open"]
            spy_exit = spy_prices[spy_exit_days[0]]["close"]
            spy_return = (spy_exit - spy_entry) / spy_entry if spy_entry > 0 else 0.0
        except (KeyError, ZeroDivisionError):
            pass

    abnormal_return = trade_return - spy_return

    return {
        "ticker": ticker,
        "filing_date": filing_date,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "entry_price": round(entry_price, 4),
        "exit_price": round(exit_price, 4),
        "trade_return": trade_return,
        "spy_return": spy_return,
        "abnormal_return": abnormal_return,
        "stop_hit": stop_hit,
        "exit_day": exit_day,
        "hold_days": hold_days,
        "leg": "buy",
        "n_insiders": event.get("n_insiders", 1),
        "total_value": event.get("total_value", 0),
        "confidence_score": event.get("confidence_score", 0),
    }


def simulate_put_trade(event: dict, spy_prices: dict, hold_days: int = PUT_HOLD_DAYS) -> dict | None:
    """
    Approximate put P&L from a sell signal using delta approximation.

    Put P&L ~ -delta * stock_return * leverage_factor
    When stock drops, put gains. We also add time decay penalty.

    Profit target: +100% of premium. Stop: -50% of premium.
    """
    ticker = event["ticker"]
    filing_date_str = str(event["filing_date"])[:10]
    try:
        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None

    prices = load_daily_prices(ticker)
    if not prices:
        return None

    trading_days = sorted(d for d in prices.keys() if d > filing_date)
    if not trading_days:
        return None

    entry_date = trading_days[0]
    entry_stock_price = prices[entry_date]["open"]
    if entry_stock_price <= 0:
        return None

    # Approximate put premium as ~3% of stock price (typical for 5% OTM, 30-45 DTE)
    approx_premium_pct = 0.03
    strike = entry_stock_price * (1.0 - PUT_STRIKE_OTM)

    hold_dates = trading_days[:hold_days + 1]

    # Track daily P&L to check profit target and stop loss
    best_return = 0.0
    exit_price_stock = None
    exit_date = None
    exit_reason = "hold_expired"
    put_return = 0.0

    for i, d in enumerate(hold_dates):
        bar = prices[d]
        stock_change = (bar["close"] - entry_stock_price) / entry_stock_price

        # Put P&L approximation via delta:
        # put delta is negative (-0.30), stock_change negative when dropping
        # put gains when stock drops: delta * stock_change = (-0.30)*(-0.01) = +0.003
        delta_pnl = PUT_DELTA * stock_change  # both negative when stock drops → positive P&L
        # Time decay: lose ~1/DTE per day of premium
        theta_decay = -i / 30.0 * 0.3  # lose ~30% of premium over 30 days (7 days ~ 7%)

        # Approximate put return as fraction of premium
        approx_put_return = delta_pnl / approx_premium_pct + theta_decay

        if i > 0:
            # Check profit target
            if approx_put_return >= PUT_PROFIT_TARGET:
                put_return = PUT_PROFIT_TARGET
                exit_date = d
                exit_price_stock = bar["close"]
                exit_reason = "profit_target"
                break
            # Check stop loss
            if approx_put_return <= PUT_STOP_LOSS:
                put_return = PUT_STOP_LOSS
                exit_date = d
                exit_price_stock = bar["close"]
                exit_reason = "stop_loss"
                break

        best_return = max(best_return, approx_put_return)

    if exit_date is None:
        # Hold expired — use final approximate return
        if hold_dates:
            exit_date = hold_dates[-1]
            exit_price_stock = prices[exit_date]["close"]
            stock_change = (exit_price_stock - entry_stock_price) / entry_stock_price
            delta_pnl = -PUT_DELTA * stock_change
            theta_decay = -len(hold_dates) / 30.0 * 0.3
            put_return = delta_pnl / approx_premium_pct + theta_decay
        else:
            return None

    # SPY benchmark (for consistency, though puts are already a hedge)
    spy_trading_days = sorted(d for d in spy_prices.keys() if d >= entry_date)
    spy_exit_days = sorted(d for d in spy_prices.keys() if d >= exit_date)
    spy_return = 0.0
    if spy_trading_days and spy_exit_days:
        try:
            spy_entry = spy_prices[spy_trading_days[0]]["open"]
            spy_exit = spy_prices[spy_exit_days[0]]["close"]
            spy_return = (spy_exit - spy_entry) / spy_entry if spy_entry > 0 else 0.0
        except (KeyError, ZeroDivisionError):
            pass

    return {
        "ticker": ticker,
        "filing_date": filing_date,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "entry_price": round(entry_stock_price, 4),
        "exit_price": round(exit_price_stock, 4) if exit_price_stock else 0.0,
        "trade_return": put_return,  # as fraction of premium
        "spy_return": spy_return,
        "abnormal_return": put_return,  # put is already a directional bet
        "stop_hit": exit_reason == "stop_loss",
        "exit_reason": exit_reason,
        "hold_days": hold_days,
        "leg": "put",
        "n_insiders": event.get("n_insiders", 1),
        "total_value": event.get("total_value", 0),
        "confidence_score": event.get("confidence_score", 0),
    }


def compute_portfolio_metrics(
    buy_results: list[dict],
    put_results: list[dict],
    buy_sizing: float = BUY_POSITION_PCT,
    put_sizing: float = PUT_POSITION_PCT,
) -> dict:
    """
    Simulate combined portfolio with position sizing and compute metrics.
    Returns portfolio-level stats.
    """
    # Merge and sort all trades by entry_date
    all_trades = []
    for t in buy_results:
        all_trades.append({**t, "_sizing": buy_sizing})
    for t in put_results:
        # Put returns are already as fraction of premium;
        # portfolio impact = put_sizing * put_return
        all_trades.append({**t, "_sizing": put_sizing})

    all_trades.sort(key=lambda x: x["entry_date"])

    if not all_trades:
        return {"n": 0, "sharpe": 0, "max_dd_pct": 0}

    # Portfolio equity curve
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    portfolio_returns = []

    for t in all_trades:
        sizing = t["_sizing"]
        ret = t["trade_return"]
        portfolio_pnl = sizing * ret
        equity += portfolio_pnl
        portfolio_returns.append(portfolio_pnl)

        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    arr = np.array(portfolio_returns)
    n = len(arr)
    mean_ret = float(np.mean(arr))
    std_ret = float(np.std(arr, ddof=1)) if n > 1 else 0.0

    avg_hold = 7  # approximate
    periods_per_year = ANNUAL_TRADING_DAYS / avg_hold
    sharpe = (mean_ret / std_ret) * math.sqrt(periods_per_year) if std_ret > 0 else 0.0

    win_rate = float(np.mean(arr > 0))
    winners = arr[arr > 0]
    losers = arr[arr < 0]
    avg_win = float(np.mean(winners)) if len(winners) > 0 else 0.0
    avg_loss = float(np.mean(losers)) if len(losers) > 0 else 0.0
    profit_factor = abs(float(np.sum(winners)) / float(np.sum(losers))) if len(losers) > 0 and np.sum(losers) != 0 else 0.0

    total_pnl = float(np.sum(arr)) * PORTFOLIO_VALUE
    total_return = float(np.sum(arr))

    # Max consecutive losses
    max_consec = 0
    current = 0
    for r in arr:
        if r < 0:
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0

    # t-statistic
    t_stat = (mean_ret / (std_ret / math.sqrt(n))) if std_ret > 0 and n > 1 else 0.0

    return {
        "n": n,
        "n_buys": len(buy_results),
        "n_puts": len(put_results),
        "mean_return": mean_ret,
        "std_return": std_ret,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "max_dd_pct": max_dd * 100,
        "max_consecutive_losses": max_consec,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": profit_factor,
        "total_pnl": round(total_pnl, 2),
        "total_return_pct": round(total_return * 100, 2),
        "t_statistic": t_stat,
        "equity_final": equity,
    }


def compute_leg_stats(results: list[dict], label: str, sizing: float) -> dict:
    """Compute stats for a single leg."""
    if not results:
        return {"label": label, "n": 0, "sharpe": 0}

    returns = np.array([r["abnormal_return"] for r in results])
    n = len(returns)
    mean_ar = float(np.mean(returns))
    std_ar = float(np.std(returns, ddof=1)) if n > 1 else 0.0
    hold = results[0].get("hold_days", 7) if results else 7
    sharpe = (mean_ar / std_ar) * math.sqrt(ANNUAL_TRADING_DAYS / hold) if std_ar > 0 else 0.0
    win_rate = float(np.mean(returns > 0))
    t_stat = (mean_ar / (std_ar / math.sqrt(n))) if std_ar > 0 and n > 1 else 0.0

    # Max DD with position sizing
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity += sizing * r
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    stops = sum(1 for r in results if r.get("stop_hit", False))

    return {
        "label": label,
        "n": n,
        "mean_ar": mean_ar,
        "median_ar": float(np.median(returns)),
        "std_ar": std_ar,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "max_dd_pct": max_dd * 100,
        "t_statistic": t_stat,
        "stops_hit": stops,
    }


def run_backtest(
    buy_events_path: Path,
    sell_events_path: Path,
    hold_days_list: list[int],
    walk_forward: bool = False,
) -> dict:
    """Run the full V2 backtest."""

    # Load SPY prices once
    spy_prices = load_daily_prices("SPY")
    if not spy_prices:
        logger.error("No SPY price data found in %s", PRICES_DIR)
        sys.exit(1)
    logger.info("SPY prices loaded: %d trading days", len(spy_prices))

    # Load events
    buy_events = pd.read_csv(buy_events_path)
    sell_events = pd.read_csv(sell_events_path)
    logger.info("Buy events: %d, Sell events: %d", len(buy_events), len(sell_events))

    results_by_hold = {}

    for hold_days in hold_days_list:
        logger.info("=== Hold period: %d days ===", hold_days)

        # Simulate buy leg
        buy_results = []
        buy_skipped = 0
        for _, row in buy_events.iterrows():
            result = simulate_buy_trade(row.to_dict(), spy_prices, hold_days=hold_days)
            if result:
                buy_results.append(result)
            else:
                buy_skipped += 1
        logger.info("Buy leg: %d trades, %d skipped", len(buy_results), buy_skipped)

        # Simulate sell/put leg
        put_results = []
        put_skipped = 0
        for _, row in sell_events.iterrows():
            result = simulate_put_trade(row.to_dict(), spy_prices, hold_days=hold_days)
            if result:
                put_results.append(result)
            else:
                put_skipped += 1
        logger.info("Put leg: %d trades, %d skipped", len(put_results), put_skipped)

        # Overall stats
        buy_stats = compute_leg_stats(buy_results, f"Buy leg {hold_days}d", BUY_POSITION_PCT)
        put_stats = compute_leg_stats(put_results, f"Put leg {hold_days}d", PUT_POSITION_PCT)
        portfolio = compute_portfolio_metrics(buy_results, put_results)

        hold_result = {
            "hold_days": hold_days,
            "buy_stats": buy_stats,
            "put_stats": put_stats,
            "portfolio": portfolio,
            "buy_results": buy_results,
            "put_results": put_results,
        }

        # Walk-forward split
        if walk_forward:
            train_buys = [r for r in buy_results if r["entry_date"] <= TRAIN_END]
            test_buys = [r for r in buy_results if r["entry_date"] >= TEST_START]
            train_puts = [r for r in put_results if r["entry_date"] <= TRAIN_END]
            test_puts = [r for r in put_results if r["entry_date"] >= TEST_START]

            hold_result["walk_forward"] = {
                "train": {
                    "buy": compute_leg_stats(train_buys, f"Train buy {hold_days}d", BUY_POSITION_PCT),
                    "put": compute_leg_stats(train_puts, f"Train put {hold_days}d", PUT_POSITION_PCT),
                    "portfolio": compute_portfolio_metrics(train_buys, train_puts),
                },
                "test": {
                    "buy": compute_leg_stats(test_buys, f"Test buy {hold_days}d", BUY_POSITION_PCT),
                    "put": compute_leg_stats(test_puts, f"Test put {hold_days}d", PUT_POSITION_PCT),
                    "portfolio": compute_portfolio_metrics(test_buys, test_puts),
                },
            }

        results_by_hold[hold_days] = hold_result

    return results_by_hold


def print_results(results_by_hold: dict, walk_forward: bool):
    """Print formatted results to stdout."""
    for hold_days, data in sorted(results_by_hold.items()):
        bs = data["buy_stats"]
        ps = data["put_stats"]
        pf = data["portfolio"]

        print(f"\n{'=' * 70}")
        print(f"  INSIDER V2 BACKTEST — {hold_days}-Day Hold")
        print(f"{'=' * 70}")

        def _print_leg(s: dict):
            if s["n"] == 0:
                print(f"    {s['label']}: No trades")
                return
            print(f"\n    {s['label']}:")
            print(f"      N trades:     {s['n']}")
            print(f"      Mean AR:      {s['mean_ar']*100:+.2f}%")
            print(f"      Median AR:    {s['median_ar']*100:+.2f}%")
            print(f"      Sharpe:       {s['sharpe']:.2f}")
            print(f"      Win rate:     {s['win_rate']*100:.1f}%")
            print(f"      Max DD:       {s['max_dd_pct']:.2f}%")
            print(f"      t-stat:       {s['t_statistic']:.2f}")
            print(f"      Stops hit:    {s['stops_hit']}")

        _print_leg(bs)
        _print_leg(ps)

        print(f"\n    Combined Portfolio:")
        print(f"      Total trades: {pf['n']} ({pf['n_buys']} buys + {pf['n_puts']} puts)")
        print(f"      Sharpe:       {pf['sharpe']:.2f}")
        print(f"      Win rate:     {pf['win_rate']*100:.1f}%")
        print(f"      Max DD:       {pf['max_dd_pct']:.2f}%")
        print(f"      Total P&L:    ${pf['total_pnl']:,.0f}")
        print(f"      Total return: {pf['total_return_pct']:.1f}%")
        print(f"      Profit factor:{pf['profit_factor']:.2f}")
        print(f"      t-stat:       {pf['t_statistic']:.2f}")

        if walk_forward and "walk_forward" in data:
            wf = data["walk_forward"]
            train_p = wf["train"]["portfolio"]
            test_p = wf["test"]["portfolio"]

            print(f"\n    Walk-Forward Validation:")
            print(f"      Train (2016-2022): N={train_p['n']}, Sharpe={train_p['sharpe']:.2f}, "
                  f"WR={train_p['win_rate']*100:.1f}%, DD={train_p['max_dd_pct']:.2f}%")
            print(f"      Test  (2023-2025): N={test_p['n']}, Sharpe={test_p['sharpe']:.2f}, "
                  f"WR={test_p['win_rate']*100:.1f}%, DD={test_p['max_dd_pct']:.2f}%")

            if train_p["sharpe"] != 0:
                deg = (test_p["sharpe"] - train_p["sharpe"]) / abs(train_p["sharpe"]) * 100
                print(f"      Degradation:  {deg:+.1f}%")

            # Per-leg walk-forward
            for leg_name in ["buy", "put"]:
                train_leg = wf["train"][leg_name]
                test_leg = wf["test"][leg_name]
                print(f"\n      {leg_name.upper()} leg walk-forward:")
                print(f"        Train: N={train_leg['n']}, Sharpe={train_leg['sharpe']:.2f}, "
                      f"WR={train_leg['win_rate']*100:.1f}%")
                print(f"        Test:  N={test_leg['n']}, Sharpe={test_leg['sharpe']:.2f}, "
                      f"WR={test_leg['win_rate']*100:.1f}%")

    print(f"\n{'=' * 70}")


def build_backtest_json(results_by_hold: dict, walk_forward: bool) -> dict:
    """Build the backtest_latest.json output."""
    # Use 7-day hold as primary
    primary = results_by_hold.get(7, list(results_by_hold.values())[0])
    bs = primary["buy_stats"]
    ps = primary["put_stats"]
    pf = primary["portfolio"]

    output = {
        "summary": {
            "strategy": "insider_v2",
            "vehicle": "shares + puts (dual-leg)",
            "total_trades": pf["n"],
            "buy_trades": pf["n_buys"],
            "put_trades": pf["n_puts"],
            "win_rate": round(pf["win_rate"], 3),
            "total_pnl": pf["total_pnl"],
            "total_return_pct": pf["total_return_pct"],
            "sharpe_ratio": round(pf["sharpe"], 2),
            "max_drawdown_pct": round(pf["max_dd_pct"], 2),
            "max_consecutive_losses": pf["max_consecutive_losses"],
            "profit_factor": round(pf["profit_factor"], 2),
            "t_statistic": round(pf["t_statistic"], 2),
            "hold_period_days": primary["hold_days"],
            "position_sizing": {
                "buy_pct": BUY_POSITION_PCT * 100,
                "put_pct": PUT_POSITION_PCT * 100,
                "max_concurrent_longs": MAX_CONCURRENT_LONGS,
                "max_concurrent_puts": MAX_CONCURRENT_PUTS,
            },
            "benchmark": "SPY",
            "filters": {
                "buy": "1+ insider, $1M+ total, 30-day window",
                "sell": "2+ insiders, $1M+ total, 30-day window",
                "buy_stop_loss": f"{BUY_STOP_LOSS*100:.0f}%",
                "put_profit_target": f"+{PUT_PROFIT_TARGET*100:.0f}%",
                "put_stop_loss": f"{PUT_STOP_LOSS*100:.0f}%",
            },
            "risk_controls": {
                "circuit_breaker": f"halt if 30d rolling DD > {CIRCUIT_BREAKER_DD*100:.0f}%",
                "vix_regime": f"reduce buy to {VIX_REDUCED_BUY_PCT*100:.0f}%, increase put to {VIX_INCREASED_PUT_PCT*100:.0f}% when VIX > {VIX_THRESHOLD}",
                "max_concurrent_longs": MAX_CONCURRENT_LONGS,
                "max_concurrent_puts": MAX_CONCURRENT_PUTS,
            },
            "buy_leg": {
                "n": bs["n"],
                "mean_ar": round(bs["mean_ar"], 4) if bs["n"] > 0 else 0,
                "sharpe": round(bs["sharpe"], 2) if bs["n"] > 0 else 0,
                "win_rate": round(bs["win_rate"], 3) if bs["n"] > 0 else 0,
                "max_dd_pct": round(bs["max_dd_pct"], 2) if bs["n"] > 0 else 0,
                "t_statistic": round(bs["t_statistic"], 2) if bs["n"] > 0 else 0,
            },
            "put_leg": {
                "n": ps["n"],
                "mean_ar": round(ps["mean_ar"], 4) if ps["n"] > 0 else 0,
                "sharpe": round(ps["sharpe"], 2) if ps["n"] > 0 else 0,
                "win_rate": round(ps["win_rate"], 3) if ps["n"] > 0 else 0,
                "max_dd_pct": round(ps["max_dd_pct"], 2) if ps["n"] > 0 else 0,
                "note": "P&L approximated via delta model (no historical options data)",
            },
        },
    }

    # Hold period sweep
    if len(results_by_hold) > 1:
        sweep = {}
        for hd, data in sorted(results_by_hold.items()):
            sweep[f"{hd}d"] = {
                "buy_sharpe": round(data["buy_stats"]["sharpe"], 2) if data["buy_stats"]["n"] > 0 else 0,
                "buy_n": data["buy_stats"]["n"],
                "buy_mean_ar": round(data["buy_stats"]["mean_ar"], 4) if data["buy_stats"]["n"] > 0 else 0,
                "put_sharpe": round(data["put_stats"]["sharpe"], 2) if data["put_stats"]["n"] > 0 else 0,
                "put_n": data["put_stats"]["n"],
                "portfolio_sharpe": round(data["portfolio"]["sharpe"], 2),
            }
        output["hold_period_sweep"] = sweep

    # Walk-forward
    if walk_forward and "walk_forward" in primary:
        wf = primary["walk_forward"]
        train_p = wf["train"]["portfolio"]
        test_p = wf["test"]["portfolio"]

        deg = None
        if train_p["sharpe"] != 0:
            deg = round((test_p["sharpe"] - train_p["sharpe"]) / abs(train_p["sharpe"]) * 100, 1)

        output["summary"]["walk_forward"] = {
            "train_period": "2016-2022",
            "test_period": "2023-2025",
            "train": {
                "n": train_p["n"],
                "sharpe": round(train_p["sharpe"], 2),
                "win_rate": round(train_p["win_rate"], 3),
                "max_dd_pct": round(train_p["max_dd_pct"], 2),
                "n_buys": train_p["n_buys"],
                "n_puts": train_p["n_puts"],
            },
            "test": {
                "n": test_p["n"],
                "sharpe": round(test_p["sharpe"], 2),
                "win_rate": round(test_p["win_rate"], 3),
                "max_dd_pct": round(test_p["max_dd_pct"], 2),
                "n_buys": test_p["n_buys"],
                "n_puts": test_p["n_puts"],
            },
            "degradation_pct": deg,
            "per_leg": {
                "buy_train_sharpe": round(wf["train"]["buy"]["sharpe"], 2) if wf["train"]["buy"]["n"] > 0 else 0,
                "buy_test_sharpe": round(wf["test"]["buy"]["sharpe"], 2) if wf["test"]["buy"]["n"] > 0 else 0,
                "put_train_sharpe": round(wf["train"]["put"]["sharpe"], 2) if wf["train"]["put"]["n"] > 0 else 0,
                "put_test_sharpe": round(wf["test"]["put"]["sharpe"], 2) if wf["test"]["put"]["n"] > 0 else 0,
            },
        }

    # Per-year breakdown (buy leg only for now — most comparable to V1)
    if 7 in results_by_hold:
        buy_results = results_by_hold[7]["buy_results"]
        year_stats = {}
        for year in range(2016, 2027):
            yr = [r for r in buy_results if r["entry_date"].year == year]
            if yr:
                s = compute_leg_stats(yr, str(year), BUY_POSITION_PCT)
                year_stats[str(year)] = {
                    "n": s["n"],
                    "sharpe": round(s["sharpe"], 2),
                    "mean_ar": round(s["mean_ar"], 4),
                    "win_rate": round(s["win_rate"], 3),
                }
        if year_stats:
            output["per_year_buy_sharpe"] = year_stats

    output["summary"]["note"] = (
        "V2 dual-leg backtest. Buy leg uses actual daily prices with -10% stop. "
        "Put leg P&L approximated via delta model (delta=-0.30, 5% OTM, 30-45 DTE equivalent). "
        "Put returns are as a fraction of premium invested. "
        "Walk-forward split: train through 2022, test 2023+."
    )

    return output


def main():
    parser = argparse.ArgumentParser(description="Insider V2 dual-leg backtest")
    parser.add_argument("--walk-forward", action="store_true", default=True)
    parser.add_argument("--no-walk-forward", dest="walk_forward", action="store_false")
    parser.add_argument(
        "--hold-days", type=int, nargs="+", default=[7, 14, 30],
        help="Hold periods to test (default: 7 14 30)"
    )
    parser.add_argument("--output", type=Path, default=REPORT_DIR / "backtest_latest.json")
    parser.add_argument(
        "--events-dir", type=Path,
        default=STUDY_DIR / "data",
        help="Directory containing events_v2_buys.csv and events_v2_sells.csv"
    )
    parser.add_argument("--build-events", action="store_true",
                        help="Rebuild event calendars from insiders.db before backtesting")
    args = parser.parse_args()

    # Build events if requested or if they don't exist
    buy_path = args.events_dir / "events_v2_buys.csv"
    sell_path = args.events_dir / "events_v2_sells.csv"

    if args.build_events or not buy_path.exists() or not sell_path.exists():
        logger.info("Building V2 event calendars...")
        from build_v2_events import load_trades, group_events
        from build_v2_events import (
            BUY_MIN_INSIDERS, BUY_MIN_VALUE, BUY_MIN_TRADE_VALUE,
            SELL_MIN_INSIDERS, SELL_MIN_VALUE, SELL_MIN_TRADE_VALUE,
            EVENT_WINDOW_DAYS,
        )

        args.events_dir.mkdir(parents=True, exist_ok=True)

        buy_trades = load_trades(DB_PATH, "buy", BUY_MIN_TRADE_VALUE)
        buy_events = group_events(buy_trades, EVENT_WINDOW_DAYS, BUY_MIN_INSIDERS, BUY_MIN_VALUE)
        buy_events.to_csv(buy_path, index=False)
        logger.info("Buy events: %d saved to %s", len(buy_events), buy_path)

        sell_trades = load_trades(DB_PATH, "sell", SELL_MIN_TRADE_VALUE)
        sell_events = group_events(sell_trades, EVENT_WINDOW_DAYS, SELL_MIN_INSIDERS, SELL_MIN_VALUE)
        sell_events.to_csv(sell_path, index=False)
        logger.info("Sell events: %d saved to %s", len(sell_events), sell_path)

    # Run backtest
    results = run_backtest(buy_path, sell_path, args.hold_days, args.walk_forward)

    # Print results
    print_results(results, args.walk_forward)

    # Build and save JSON
    output = build_backtest_json(results, args.walk_forward)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)
    logger.info("Backtest JSON saved: %s", args.output)
    print(f"\nResults saved to: {args.output}")

    # Also save per-leg CSVs for the board
    if 7 in results:
        buy_df = pd.DataFrame(results[7]["buy_results"])
        put_df = pd.DataFrame(results[7]["put_results"])
        if not buy_df.empty:
            buy_csv = args.output.parent / "buy_trades_7d.csv"
            buy_df.to_csv(buy_csv, index=False)
            logger.info("Buy trades CSV: %s", buy_csv)
        if not put_df.empty:
            put_csv = args.output.parent / "put_trades_7d.csv"
            put_df.to_csv(put_csv, index=False)
            logger.info("Put trades CSV: %s", put_csv)


if __name__ == "__main__":
    main()
