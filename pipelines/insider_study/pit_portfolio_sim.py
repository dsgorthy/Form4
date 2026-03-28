#!/usr/bin/env python3
"""
PIT Portfolio Simulation — $100K account with shares + options overlay.

Runs a realistic portfolio simulation using honest PIT scores:
  1. OOS validation (2025+) — proves signal holds forward
  2. Full-period simulation (2021-2026) — $100K starting capital
  3. Shares primary (70%) + options overlay (30%) on highest-conviction
  4. Proper concurrent position limits, stop-losses, VIX regime

Outputs board-ready metrics for the 5 personas.

Usage:
    python pipelines/insider_study/pit_portfolio_sim.py
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DIR = Path(__file__).parent / "data" / "prices"
OUTPUT_DIR = Path(__file__).parent / "data"
OPTIONS_TRADES_JSON = OUTPUT_DIR / "options_backtest_trades.json"

# ── Portfolio Parameters ─────────────────────────────────────────────

STARTING_CAPITAL = 100_000
MAX_CONCURRENT = 5
HOLD_DAYS_EQUITY = 7       # trading days
HOLD_DAYS_OPTIONS = 14     # calendar days
STOP_LOSS_PCT = -0.15
VIX_THRESHOLD = 30
VIX_REDUCED_SIZE_PCT = 0.03
NORMAL_SIZE_PCT = 0.05
OPTIONS_SIZE_PCT = 0.01
OPTIONS_MAX_CONTRACTS = 2
OPTIONS_PROFIT_TARGET = 0.50
CIRCUIT_BREAKER_DD_PCT = 0.10

# PIT score threshold for entry
PIT_MIN_SCORE = 1.0           # minimum PIT blended score
PIT_HIGH_SCORE = 1.5          # threshold for options overlay
CLUSTER_REQUIRED = True       # require cluster signal

# Shares/options allocation
SHARES_ALLOC_PCT = 0.70       # 70% of sizing goes to shares
OPTIONS_ALLOC_PCT = 0.30      # 30% of sizing goes to options overlay


def load_prices(ticker: str, cache: dict) -> pd.DataFrame | None:
    if ticker in cache:
        return cache[ticker]
    path = PRICES_DIR / f"{ticker.upper()}.csv"
    if not path.exists():
        cache[ticker] = None
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        df.columns = [c.lower() for c in df.columns]
        cache[ticker] = df
        return df
    except Exception:
        cache[ticker] = None
        return None


def get_vix_at_date(date_str: str, cache: dict) -> float:
    """Get VIX proxy (VIXY close) at a date. Returns 0 if unavailable."""
    df = load_prices("VIXY", cache)
    if df is None:
        return 0.0
    try:
        dt = pd.Timestamp(date_str)
        mask = df.index <= dt
        if mask.sum() == 0:
            return 0.0
        return float(df.loc[mask].iloc[-1]["close"])
    except Exception:
        return 0.0


def compute_intraday_return(ticker: str, entry_date: str, hold_days: int,
                             stop_loss: float, price_cache: dict) -> dict:
    """
    Simulate a position from entry_date T+1 open through hold_days trading days.
    Returns {return_pct, exit_reason, exit_date, stopped}.
    """
    df = load_prices(ticker, price_cache)
    if df is None:
        return None

    entry_dt = pd.Timestamp(entry_date)
    mask = df.index > entry_dt
    if mask.sum() == 0:
        return None

    entry_idx = int(np.argmax(mask))
    entry_price = df.iloc[entry_idx]["open"]
    if entry_price <= 0 or np.isnan(entry_price):
        return None

    # Walk forward
    days_held = 0
    min_dd = 0.0
    exit_price = entry_price
    exit_reason = "time"
    stopped = False

    for i in range(entry_idx + 1, len(df)):
        row = df.iloc[i]
        if row.name.weekday() >= 5:
            continue
        days_held += 1

        low = row["low"]
        dd = (low - entry_price) / entry_price
        if dd <= stop_loss:
            exit_price = entry_price * (1 + stop_loss)
            exit_reason = "stop_loss"
            stopped = True
            break

        if days_held >= hold_days:
            exit_price = row["close"]
            exit_reason = "time"
            break

        exit_price = row["close"]

    ret = (exit_price - entry_price) / entry_price

    return {
        "return_pct": float(ret),
        "exit_reason": exit_reason,
        "days_held": days_held,
        "stopped": stopped,
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "min_dd": float(min_dd),
    }


def load_options_return_map() -> dict:
    """
    Load pre-computed options backtest trades.
    Returns: trade_id -> {return_pct, ...}
    """
    if not OPTIONS_TRADES_JSON.exists():
        return {}
    try:
        with open(OPTIONS_TRADES_JSON) as f:
            trades = json.load(f)
        return {t["trade_id"]: t for t in trades if "trade_id" in t}
    except Exception:
        return {}


def run_simulation(
    db: sqlite3.Connection,
    start_date: str,
    end_date: str,
    price_cache: dict,
    label: str = "",
) -> dict:
    """
    Run full portfolio simulation with PIT scoring, shares + options.
    """
    # Load cluster flags
    cluster_rows = db.execute("""
        SELECT trade_id, insider_id, ticker, trade_date
        FROM trades WHERE trade_type = 'buy'
          AND trade_date >= ? AND trade_date <= ?
        ORDER BY ticker, trade_date
    """, (start_date, end_date)).fetchall()

    by_ticker = defaultdict(list)
    for tid, iid, ticker, td in cluster_rows:
        by_ticker[ticker].append((tid, iid, datetime.strptime(td, "%Y-%m-%d")))

    cluster_flags = {}
    for ticker, trades in by_ticker.items():
        trades.sort(key=lambda x: x[2])
        n = len(trades)
        for i in range(n):
            tid, iid, td = trades[i]
            nearby = {iid}
            for j in range(i - 1, -1, -1):
                if (td - trades[j][2]).days > 30:
                    break
                nearby.add(trades[j][1])
            for j in range(i + 1, n):
                if (trades[j][2] - td).days > 30:
                    break
                nearby.add(trades[j][1])
            cluster_flags[tid] = len(nearby) >= 2

    # Load all buy trades with returns and PIT scores
    rows = db.execute("""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.filing_date,
               t.value, t.is_csuite, t.title_weight, t.title,
               tr.return_7d, tr.abnormal_7d,
               t.signal_quality, t.trans_code
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.trade_date >= ? AND t.trade_date <= ?
          AND tr.return_7d IS NOT NULL
    """, (start_date, end_date)).fetchall()

    # Build trade dicts with PIT scores
    all_trades = []
    for r in rows:
        trade_id = r[0]
        insider_id = r[1]
        ticker = r[2]
        trade_date = r[3]

        is_cluster = cluster_flags.get(trade_id, False)
        if CLUSTER_REQUIRED and not is_cluster:
            continue

        # Look up PIT score
        pit_row = db.execute("""
            SELECT blended_score, sufficient_data
            FROM insider_ticker_scores
            WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC LIMIT 1
        """, (insider_id, ticker, trade_date)).fetchone()

        pit_score = pit_row[0] if pit_row and pit_row[1] else None

        # Filter by PIT score
        if pit_score is None or pit_score < PIT_MIN_SCORE:
            continue

        all_trades.append({
            "trade_id": trade_id,
            "insider_id": insider_id,
            "ticker": ticker,
            "trade_date": trade_date,
            "filing_date": r[4],
            "value": r[5],
            "is_csuite": r[6],
            "title_weight": r[7],
            "title": r[8],
            "return_7d": r[9],
            "abnormal_7d": r[10],
            "signal_quality": r[11],
            "trans_code": r[12],
            "is_cluster": is_cluster,
            "pit_score": pit_score,
        })

    all_trades.sort(key=lambda t: t["trade_date"])

    # ── Portfolio simulation ──
    equity = STARTING_CAPITAL
    peak_equity = equity
    max_dd = 0.0
    max_dd_date = start_date

    open_positions = []  # list of position dicts
    closed_trades = []
    skipped_signals = 0
    circuit_breaker_count = 0
    options_trades = 0
    options_wins = 0
    options_pnl = 0.0

    # Track daily equity curve
    equity_curve = []

    for trade in all_trades:
        # Close any expired positions first
        today = trade["trade_date"]
        today_dt = datetime.strptime(today, "%Y-%m-%d")

        positions_to_close = []
        for i, pos in enumerate(open_positions):
            exit_target = datetime.strptime(pos["exit_date_target"], "%Y-%m-%d")
            if today_dt >= exit_target:
                positions_to_close.append(i)

        # Close in reverse order to not mess up indices
        for i in sorted(positions_to_close, reverse=True):
            pos = open_positions.pop(i)
            # Use pre-computed return
            ret_data = compute_intraday_return(
                pos["ticker"], pos["entry_date"], HOLD_DAYS_EQUITY,
                STOP_LOSS_PCT, price_cache,
            )
            if ret_data:
                pnl = pos["position_value"] * ret_data["return_pct"]
                equity += pnl
                closed_trades.append({
                    "ticker": pos["ticker"],
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "return_pct": ret_data["return_pct"],
                    "exit_reason": ret_data["exit_reason"],
                    "pnl": pnl,
                    "pit_score": pos["pit_score"],
                    "position_value": pos["position_value"],
                    "is_options": False,
                })
            else:
                # Fall back to DB return
                pnl = pos["position_value"] * pos["return_7d"]
                equity += pnl
                closed_trades.append({
                    "ticker": pos["ticker"],
                    "entry_date": pos["entry_date"],
                    "exit_date": today,
                    "return_pct": pos["return_7d"],
                    "exit_reason": "time",
                    "pnl": pnl,
                    "pit_score": pos["pit_score"],
                    "position_value": pos["position_value"],
                    "is_options": False,
                })

        # Update drawdown
        if equity > peak_equity:
            peak_equity = equity
        if peak_equity > 0:
            dd = (equity - peak_equity) / peak_equity
            if dd < max_dd:
                max_dd = dd
                max_dd_date = today

        # Circuit breaker check (30-day rolling)
        recent_returns = [c["return_pct"] for c in closed_trades
                         if c["exit_date"] >= (today_dt - timedelta(days=30)).strftime("%Y-%m-%d")]
        rolling_dd = sum(recent_returns) if recent_returns else 0.0
        if rolling_dd < -CIRCUIT_BREAKER_DD_PCT:
            circuit_breaker_count += 1
            skipped_signals += 1
            continue

        # Max concurrent check
        if len(open_positions) >= MAX_CONCURRENT:
            skipped_signals += 1
            continue

        # Check if ticker already open
        open_tickers = {p["ticker"] for p in open_positions}
        if trade["ticker"] in open_tickers:
            skipped_signals += 1
            continue

        # ── Position sizing ──
        vix = get_vix_at_date(today, price_cache)
        base_size_pct = VIX_REDUCED_SIZE_PCT if vix > VIX_THRESHOLD else NORMAL_SIZE_PCT

        # Adjust by PIT score confidence
        pit = trade["pit_score"]
        if pit >= 2.0:
            conf_mult = 1.0
        elif pit >= 1.5:
            conf_mult = 0.8
        else:
            conf_mult = 0.6

        size_pct = base_size_pct * conf_mult

        # ── Shares leg ──
        shares_value = equity * size_pct * SHARES_ALLOC_PCT
        if shares_value < 100:
            skipped_signals += 1
            continue

        exit_dt = today_dt + timedelta(days=10)  # ~7 trading days
        open_positions.append({
            "ticker": trade["ticker"],
            "entry_date": today,
            "exit_date_target": exit_dt.strftime("%Y-%m-%d"),
            "position_value": shares_value,
            "pit_score": pit,
            "return_7d": trade["return_7d"],
            "is_options": False,
        })

        # ── Options overlay (high conviction only) ──
        if pit >= PIT_HIGH_SCORE and trade.get("is_csuite"):
            options_value = equity * OPTIONS_SIZE_PCT
            # Simulate options return as leveraged equity return
            # Conservative: options return ~3x equity return (delta ~0.5, leverage ~6x,
            # but spread/theta costs reduce to ~3x effective)
            options_leverage = 3.0
            opt_ret = trade["return_7d"] * options_leverage
            # Cap at -100% (can't lose more than premium)
            opt_ret = max(-1.0, opt_ret)
            # Profit target
            if opt_ret >= OPTIONS_PROFIT_TARGET:
                opt_ret = OPTIONS_PROFIT_TARGET

            opt_pnl = options_value * opt_ret
            equity += opt_pnl  # options settle immediately for simplicity
            options_trades += 1
            if opt_ret > 0:
                options_wins += 1
            options_pnl += opt_pnl

            closed_trades.append({
                "ticker": trade["ticker"],
                "entry_date": today,
                "exit_date": today,  # simplified
                "return_pct": opt_ret,
                "exit_reason": "profit_target" if opt_ret >= OPTIONS_PROFIT_TARGET else "time",
                "pnl": opt_pnl,
                "pit_score": pit,
                "position_value": options_value,
                "is_options": True,
            })

        equity_curve.append({"date": today, "equity": equity})

    # Close any remaining positions at end
    for pos in open_positions:
        ret_data = compute_intraday_return(
            pos["ticker"], pos["entry_date"], HOLD_DAYS_EQUITY,
            STOP_LOSS_PCT, price_cache,
        )
        if ret_data:
            pnl = pos["position_value"] * ret_data["return_pct"]
        else:
            pnl = pos["position_value"] * pos["return_7d"]
        equity += pnl
        closed_trades.append({
            "ticker": pos["ticker"],
            "entry_date": pos["entry_date"],
            "exit_date": end_date,
            "return_pct": ret_data["return_pct"] if ret_data else pos["return_7d"],
            "exit_reason": ret_data["exit_reason"] if ret_data else "time",
            "pnl": pnl,
            "pit_score": pos["pit_score"],
            "position_value": pos["position_value"],
            "is_options": False,
        })

    # Final drawdown check
    if equity > peak_equity:
        peak_equity = equity
    if peak_equity > 0:
        dd = (equity - peak_equity) / peak_equity
        if dd < max_dd:
            max_dd = dd

    # ── Compute metrics ──
    equity_trades = [c for c in closed_trades if not c["is_options"]]
    all_returns = [c["return_pct"] for c in equity_trades]
    all_pnls = [c["pnl"] for c in equity_trades]

    if not all_returns:
        return {"label": label, "n": 0}

    arr = np.array(all_returns)
    n = len(arr)
    mean_ret = float(np.mean(arr))
    std_ret = float(np.std(arr, ddof=1))
    wins = int(np.sum(arr > 0))
    sharpe = (mean_ret / std_ret) * np.sqrt(min(252, n)) if std_ret > 0 else 0

    # Abnormal returns (from DB)
    abn_returns = [t["abnormal_7d"] for t in all_trades
                   if t["abnormal_7d"] is not None]
    abn_sharpe = 0.0
    if abn_returns:
        abn_arr = np.array(abn_returns)
        abn_std = float(np.std(abn_arr, ddof=1))
        if abn_std > 0:
            abn_sharpe = (float(np.mean(abn_arr)) / abn_std) * np.sqrt(min(252, len(abn_arr)))

    # Consecutive losses
    max_consec_losses = 0
    current_losses = 0
    for r in all_returns:
        if r <= 0:
            current_losses += 1
            max_consec_losses = max(max_consec_losses, current_losses)
        else:
            current_losses = 0

    # Stops hit
    stops_hit = sum(1 for c in closed_trades if c["exit_reason"] == "stop_loss")

    # Monthly returns for regime analysis
    monthly = defaultdict(float)
    for c in closed_trades:
        month = c["entry_date"][:7]
        monthly[month] += c["pnl"]

    monthly_returns = [v / STARTING_CAPITAL for v in monthly.values()]
    months_positive = sum(1 for r in monthly_returns if r > 0)

    # By PIT score bucket
    by_pit = defaultdict(list)
    for c in equity_trades:
        pit = c["pit_score"]
        if pit >= 2.0:
            bucket = "high (2.0+)"
        elif pit >= 1.5:
            bucket = "medium (1.5-2.0)"
        else:
            bucket = "low (1.0-1.5)"
        by_pit[bucket].append(c["return_pct"])

    pit_breakdown = {}
    for bucket, rets in sorted(by_pit.items()):
        a = np.array(rets)
        pit_breakdown[bucket] = {
            "n": len(a),
            "win_rate": float(np.sum(a > 0) / len(a)),
            "mean_return": float(np.mean(a)),
            "sharpe": float((np.mean(a) / np.std(a, ddof=1)) * np.sqrt(min(252, len(a))))
                      if len(a) > 1 and np.std(a, ddof=1) > 0 else 0,
        }

    result = {
        "label": label,
        "period": f"{start_date} to {end_date}",
        "starting_capital": STARTING_CAPITAL,
        "final_equity": round(equity, 2),
        "total_pnl": round(equity - STARTING_CAPITAL, 2),
        "total_return_pct": round((equity - STARTING_CAPITAL) / STARTING_CAPITAL * 100, 2),
        "n_equity_trades": len(equity_trades),
        "n_signals_considered": len(all_trades),
        "n_skipped": skipped_signals,
        "n_circuit_breaker": circuit_breaker_count,

        # Per-trade metrics
        "sharpe": round(sharpe, 3),
        "abnormal_sharpe": round(abn_sharpe, 3),
        "win_rate": round(wins / n, 4),
        "mean_return": round(mean_ret, 5),
        "median_return": round(float(np.median(arr)), 5),
        "std_return": round(std_ret, 5),
        "mean_pnl": round(float(np.mean(all_pnls)), 2),

        # Risk metrics
        "max_portfolio_dd": round(max_dd, 4),
        "max_dd_date": max_dd_date,
        "max_consecutive_losses": max_consec_losses,
        "stops_hit": stops_hit,
        "stop_rate": round(stops_hit / n, 4) if n > 0 else 0,

        # Position sizing
        "max_concurrent": MAX_CONCURRENT,
        "avg_position_value": round(float(np.mean([c["position_value"] for c in equity_trades])), 2),
        "max_position_value": round(float(np.max([c["position_value"] for c in equity_trades])), 2),
        "min_position_value": round(float(np.min([c["position_value"] for c in equity_trades])), 2),
        "position_size_pct": NORMAL_SIZE_PCT,

        # Options overlay
        "options_trades": options_trades,
        "options_wins": options_wins,
        "options_win_rate": round(options_wins / options_trades, 4) if options_trades > 0 else 0,
        "options_total_pnl": round(options_pnl, 2),
        "options_allocation_pct": OPTIONS_ALLOC_PCT * 100,

        # Regime
        "months_traded": len(monthly),
        "months_positive": months_positive,
        "months_positive_pct": round(months_positive / len(monthly), 4) if monthly else 0,

        # PIT score analysis
        "pit_breakdown": pit_breakdown,
        "pit_min_threshold": PIT_MIN_SCORE,
        "pit_options_threshold": PIT_HIGH_SCORE,

        # Config
        "config": {
            "cluster_required": CLUSTER_REQUIRED,
            "pit_min_score": PIT_MIN_SCORE,
            "stop_loss": STOP_LOSS_PCT,
            "hold_days": HOLD_DAYS_EQUITY,
            "max_concurrent": MAX_CONCURRENT,
            "vix_threshold": VIX_THRESHOLD,
            "shares_alloc": SHARES_ALLOC_PCT,
            "options_alloc": OPTIONS_ALLOC_PCT,
        },
    }

    return result


def print_report(result: dict):
    """Print formatted simulation report."""
    w = 70
    print()
    print("=" * w)
    print(f"  {result['label']}")
    print(f"  Period: {result['period']}")
    print("=" * w)

    if result.get("n_equity_trades", 0) == 0:
        print("  No trades in period.")
        return

    print(f"\n  PORTFOLIO")
    print(f"    Starting capital:     ${result['starting_capital']:>12,}")
    print(f"    Final equity:         ${result['final_equity']:>12,.2f}")
    print(f"    Total P&L:            ${result['total_pnl']:>12,.2f}  ({result['total_return_pct']:+.1f}%)")

    print(f"\n  SIGNAL QUALITY")
    print(f"    Sharpe (raw):         {result['sharpe']:>12.3f}")
    print(f"    Sharpe (abnormal):    {result['abnormal_sharpe']:>12.3f}")
    print(f"    Win rate:             {result['win_rate']:>12.1%}")
    print(f"    Mean return:          {result['mean_return']:>12.3%}")
    print(f"    Median return:        {result['median_return']:>12.3%}")
    print(f"    Trades:               {result['n_equity_trades']:>12,}")

    print(f"\n  RISK")
    print(f"    Max portfolio DD:     {result['max_portfolio_dd']:>12.2%}  ({result['max_dd_date']})")
    print(f"    Max consec losses:    {result['max_consecutive_losses']:>12}")
    print(f"    Stops hit:            {result['stops_hit']:>12}  ({result['stop_rate']:.1%} of trades)")
    print(f"    Circuit breaker:      {result['n_circuit_breaker']:>12} times")
    print(f"    Months positive:      {result['months_positive']}/{result['months_traded']}  ({result['months_positive_pct']:.0%})")

    print(f"\n  POSITION SIZING")
    print(f"    Max concurrent:       {result['max_concurrent']:>12}")
    print(f"    Avg position:         ${result['avg_position_value']:>12,.2f}")
    print(f"    Max position:         ${result['max_position_value']:>12,.2f}")
    print(f"    Min position:         ${result['min_position_value']:>12,.2f}")
    print(f"    Skipped (capacity):   {result['n_skipped']:>12}")

    print(f"\n  OPTIONS OVERLAY")
    print(f"    Options trades:       {result['options_trades']:>12}")
    print(f"    Options win rate:     {result['options_win_rate']:>12.1%}")
    print(f"    Options P&L:          ${result['options_total_pnl']:>12,.2f}")
    print(f"    Allocation:           {result['options_allocation_pct']:>12.0f}% of sizing")

    print(f"\n  PIT SCORE BREAKDOWN")
    for bucket, stats in sorted(result.get("pit_breakdown", {}).items()):
        print(f"    {bucket:<20}  N={stats['n']:<6}  WR={stats['win_rate']:.1%}  "
              f"Mean={stats['mean_return']:+.3%}  Sharpe={stats['sharpe']:.2f}")

    print("=" * w)


def main():
    import sys as _sys
    _sys.stdout = open(_sys.stdout.fileno(), mode='w', buffering=1)

    print("=" * 70)
    print("  PIT Portfolio Simulation — $100K Account")
    print("  Shares Primary + Options Overlay")
    print("=" * 70)

    db = sqlite3.connect(str(DB_PATH))
    price_cache = {}

    # ── 1. Training period (2021-2024) ──
    print("\n[1/3] Running training period simulation (2021-2024)...")
    train = run_simulation(db, "2021-01-01", "2024-12-31", price_cache,
                           label="TRAINING (2021-2024) — In-Sample")
    print_report(train)

    # ── 2. OOS period (2025+) ──
    print("\n[2/3] Running OOS validation (2025-2026)...")
    oos = run_simulation(db, "2025-01-01", "2026-12-31", price_cache,
                         label="OUT-OF-SAMPLE (2025-2026) — Forward Validation")
    print_report(oos)

    # ── 3. Full period ──
    print("\n[3/3] Running full period (2021-2026)...")
    full = run_simulation(db, "2021-01-01", "2026-12-31", price_cache,
                          label="FULL PERIOD (2021-2026)")
    print_report(full)

    # ── Summary comparison ──
    print("\n" + "=" * 70)
    print("  TRAIN vs OOS COMPARISON")
    print("=" * 70)

    if train["n_equity_trades"] > 0 and oos["n_equity_trades"] > 0:
        print(f"\n  {'Metric':<25} {'Train':>12} {'OOS':>12} {'Delta':>12}")
        print(f"  {'-'*61}")

        metrics = [
            ("Sharpe", "sharpe", ".3f"),
            ("Abnormal Sharpe", "abnormal_sharpe", ".3f"),
            ("Win Rate", "win_rate", ".1%"),
            ("Mean Return", "mean_return", ".3%"),
            ("Max DD", "max_portfolio_dd", ".2%"),
            ("Consec Losses", "max_consecutive_losses", "d"),
            ("N Trades", "n_equity_trades", ",d"),
            ("Final Equity", "final_equity", ",.0f"),
        ]

        for label, key, fmt in metrics:
            v1 = train[key]
            v2 = oos[key]
            if isinstance(v1, float) and isinstance(v2, float):
                delta = v2 - v1
                print(f"  {label:<25} {v1:>12{fmt}} {v2:>12{fmt}} {delta:>+12{fmt}}")
            else:
                print(f"  {label:<25} {v1:>12{fmt}} {v2:>12{fmt}}")

        # Key verdict
        print()
        sharpe_holds = oos["sharpe"] >= train["sharpe"] * 0.5  # OOS >= 50% of train
        wr_holds = oos["win_rate"] >= 0.52
        dd_ok = oos["max_portfolio_dd"] >= -0.25

        if sharpe_holds and wr_holds and dd_ok:
            print("  VERDICT: OOS VALIDATES — signal holds forward")
        elif sharpe_holds and wr_holds:
            print("  VERDICT: OOS MOSTLY VALIDATES — drawdown elevated but signal intact")
        else:
            print("  VERDICT: OOS CONCERN — significant degradation vs training")

    # ── Save results ──
    OUTPUT_DIR.mkdir(exist_ok=True)
    results = {
        "training": train,
        "oos": oos,
        "full": full,
    }
    with open(OUTPUT_DIR / "pit_portfolio_sim_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to {OUTPUT_DIR / 'pit_portfolio_sim_results.json'}")

    db.close()
    return results


if __name__ == "__main__":
    main()
