#!/usr/bin/env python3
"""
Call Leg Grid Search — Buy-Side Options Analysis
-------------------------------------------------
Tests buying calls on high-confidence insider cluster buys.

Grid dimensions:
  - Signal filters: n_insiders (2, 3), min_value ($2M, $5M), min_quality (1.5, 2.14)
  - Strike: 5% ITM (0.95), ATM (1.00), 5% OTM (1.05)
  - Hold: 7d, 14d
  - DTE: tight, comfortable
  - Stop: -25%, -50%, none
  - Spread filter: 10%, 20%

Uses real Theta Data EOD options pricing (ask entry, bid exit).
Includes commission ($0.65/contract each way) and slippage stress tests.
Per-trade spot checks for stop validation.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), "pipelines", "insider_study")
sys.path.insert(0, PIPELINE_DIR)

from theta_client import get_eod_date, find_nearest_expiration, find_nearest_strike, add_trading_days

# Paths
BUY_EVENTS_CSV = os.path.join(PIPELINE_DIR, "data", "results_bulk_7d.csv")
THETA_DB = os.path.join(PIPELINE_DIR, "data", "theta_cache.db")
INSIDERS_DB = os.path.join(os.path.dirname(SCRIPT_DIR), "insider_catalog", "insiders.db")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), "reports", "insider_v2")

# Call strike mappings (note: for calls, ITM means strike < price)
STRIKES_CALL = {
    "5pct_itm": 0.95,    # 5% in-the-money
    "atm": 1.00,
    "5pct_otm": 1.05,    # 5% out-of-the-money
}

HOLD_DTE_MAP = {7: (14, 21), 14: (28, 45)}
NOTIONAL_PER_TRADE = 1000
COMMISSION_PER_CONTRACT = 0.65

# Walk-forward split
TRAIN_END = date(2022, 12, 31)

SLIPPAGE_SCENARIOS = {
    "conservative_base": 0.0,
    "25pct_worse": 0.25,
    "50pct_worse": 0.50,
}


def parse_list(raw, field, convert=str):
    if raw is None or raw == "__NONE__":
        return []
    result = []
    for r in raw:
        try:
            s = r.get(field, "").strip().strip('"') if isinstance(r, dict) else str(r).strip().strip('"')
            result.append(convert(s))
        except (ValueError, KeyError):
            continue
    return sorted(result)


def get_float(row, key):
    try:
        val = float(str(row.get(key, "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def resolve_call_contract(cache, ticker, entry_date, entry_price, hold_days, dte_type, strike_type):
    """Resolve a call option contract from the cache."""
    expirations = parse_list(cache.get(f"opt_exp|{ticker}"), "expiration",
                             lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    if not expirations:
        return None
    tight_dte, comf_dte = HOLD_DTE_MAP[hold_days]
    target_dte = tight_dte if dte_type == "tight" else comf_dte
    matched_exp = find_nearest_expiration(expirations, entry_date, target_dte)
    if matched_exp is None:
        return None
    exit_date = add_trading_days(entry_date, hold_days)
    if exit_date >= matched_exp:
        return None
    exp_str = matched_exp.strftime("%Y-%m-%d")
    strikes = parse_list(cache.get(f"opt_strikes|{ticker}|{exp_str}"), "strike", float)
    if not strikes:
        return None
    real_strike = find_nearest_strike(strikes, entry_price * STRIKES_CALL[strike_type])
    if real_strike is None:
        return None
    ps = (entry_date - timedelta(days=2)).strftime("%Y-%m-%d")
    pe = (exit_date + timedelta(days=5)).strftime("%Y-%m-%d")
    # Note: right=C for calls
    cache_key = f"opt_eod_daily|{ticker}|{exp_str}|{real_strike}|C|{ps}|{pe}"
    return {"exit_date": exit_date, "cache_key": cache_key, "strike": real_strike,
            "exp": exp_str, "matched_exp": matched_exp}


def parse_eod_rows(eod_rows):
    parsed = []
    if not eod_rows or not isinstance(eod_rows, list):
        return parsed
    for row in eod_rows:
        if not isinstance(row, dict):
            continue
        d = get_eod_date(row)
        if d is None:
            continue
        parsed.append((d, get_float(row, "bid"), get_float(row, "ask"),
                        get_float(row, "high"), get_float(row, "low"),
                        get_float(row, "close"),
                        int(row.get("volume", 0) or 0),
                        int(row.get("open_interest", 0) or 0)))
    parsed.sort(key=lambda x: x[0])
    return parsed


def simulate_call_trade(parsed_rows, entry_date, exit_date, stop_loss, extra_slippage_pct,
                        spread_filter=0.10):
    """
    Simulate a call trade with conservative pricing.

    For calls (bullish bet):
      - Entry: buy at ASK + extra slippage
      - Exit: sell at BID - extra slippage
      - Stop: if option price drops to stop_loss % of entry
      - Check HIGH for potential intraday stop recovery (conservative: use LOW for stops)
    """
    if not parsed_rows:
        return None

    # Find entry row
    entry_row = None
    for row in parsed_rows:
        if row[0] >= entry_date:
            entry_row = row
            break
    if entry_row is None:
        return None

    d, bid, ask, high, low, close, volume, oi = entry_row
    if bid is None or ask is None or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    spread = ask - bid
    spread_pct = spread / ask if ask > 0 else 1.0

    # Spread filter
    if spread_pct > spread_filter:
        return None

    # Entry at ask + extra slippage
    entry_px = ask + (spread * extra_slippage_pct)
    if entry_px <= 0:
        return None

    num_contracts = int(NOTIONAL_PER_TRADE // (entry_px * 100))
    if num_contracts <= 0:
        return None

    commission_total = num_contracts * COMMISSION_PER_CONTRACT * 2

    # Track daily prices for spot check detail
    daily_prices = []
    stop_hit = False
    stop_date = None
    final_px = None
    min_pnl_pct = 0.0  # Track closest to stop

    if stop_loss is not None:
        stop_level = entry_px * (1.0 + stop_loss)

    for row in parsed_rows:
        rd, rbid, rask, rhigh, rlow, rclose, rvol, roi = row
        if rd <= d:
            continue
        if rd > exit_date:
            break

        # Track daily bid (what we'd get if we sold)
        day_bid = rbid if rbid and rbid > 0 else (rclose if rclose and rclose > 0 else None)
        if day_bid is not None:
            day_pnl_pct = (day_bid - entry_px) / entry_px
            min_pnl_pct = min(min_pnl_pct, day_pnl_pct)
            daily_prices.append({
                "date": rd.isoformat(), "bid": rbid, "ask": rask,
                "high": rhigh, "low": rlow, "close": rclose,
                "volume": rvol, "oi": roi,
                "pnl_pct": round(day_pnl_pct, 4),
            })

        # Stop check using LOW (conservative — assumes we could have been stopped)
        if stop_loss is not None and not stop_hit:
            if rlow is not None and rlow <= stop_level:
                final_px = stop_level
                stop_hit = True
                stop_date = rd
                break

    if not stop_hit:
        # Exit at hold period end
        exit_row = None
        for row in parsed_rows:
            if row[0] >= exit_date:
                exit_row = row
                break
        if exit_row is None and parsed_rows:
            exit_row = parsed_rows[-1]
        if exit_row is None:
            return None
        exit_bid = exit_row[1]
        if exit_bid is None or exit_bid <= 0:
            exit_bid = exit_row[5]  # close fallback
            if exit_bid is None or exit_bid <= 0:
                return None
        final_px = exit_bid - (spread * extra_slippage_pct)
        if final_px <= 0:
            final_px = 0.01

    pct_ret = (final_px - entry_px) / entry_px
    dollar_pnl = (final_px - entry_px) * num_contracts * 100 - commission_total

    return {
        "pct_ret": pct_ret,
        "dollar_pnl": dollar_pnl,
        "spread_pct": spread_pct,
        "entry_date": d,
        "entry_px": entry_px,
        "exit_px": final_px,
        "num_contracts": num_contracts,
        "commission": commission_total,
        "spread_cost": spread * extra_slippage_pct * 2 * num_contracts * 100,
        "volume": volume,
        "oi": oi,
        "stop_hit": stop_hit,
        "stop_date": stop_date.isoformat() if stop_date else None,
        "min_pnl_pct": min_pnl_pct,  # Closest to stop
        "daily_prices": daily_prices,
    }


def calc_stats(trades):
    if not trades:
        return None
    rets = np.array([t["pct_ret"] for t in trades])
    pnls = np.array([t["dollar_pnl"] for t in trades])
    n = len(rets)
    mean_r = float(np.mean(rets))
    std_r = float(np.std(rets, ddof=1)) if n > 1 else 0.0
    wr = float(np.sum(rets > 0) / n)
    sharpe = (mean_r / std_r) * np.sqrt(min(252, n)) if std_r > 0 else 0.0
    tot_pnl = float(np.sum(pnls))
    med_r = float(np.median(rets))
    cum = np.cumsum(pnls)
    max_dd = float(np.min(cum - np.maximum.accumulate(cum)))
    total_commission = sum(t["commission"] for t in trades)
    total_spread_cost = sum(t["spread_cost"] for t in trades)
    avg_spread = np.mean([t["spread_pct"] for t in trades])
    avg_volume = np.mean([t["volume"] for t in trades])
    avg_oi = np.mean([t.get("oi", 0) for t in trades])
    stops = sum(1 for t in trades if t["stop_hit"])

    return {
        "n": n, "win_rate": round(wr, 4), "mean_return": round(mean_r, 6),
        "median_return": round(med_r, 6), "sharpe": round(float(sharpe), 4),
        "total_pnl": round(tot_pnl, 2), "max_drawdown": round(max_dd, 2),
        "total_commission": round(total_commission, 2),
        "total_spread_cost": round(total_spread_cost, 2),
        "avg_spread_pct": round(float(avg_spread), 4),
        "avg_volume": round(float(avg_volume), 1),
        "avg_oi": round(float(avg_oi), 1),
        "stops_hit": stops, "stops_pct": round(stops / n, 4) if n > 0 else 0,
    }


def load_buy_events_from_db():
    """Load cluster buy events directly from insiders.db (2020+)."""
    conn = sqlite3.connect(INSIDERS_DB)
    c = conn.cursor()

    c.execute("""
        SELECT t.ticker, t.filing_date,
               COUNT(DISTINCT i.name_normalized) as n_insiders,
               SUM(t.value) as total_value,
               AVG(t.price) as avg_price
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trade_type = 'buy'
          AND t.filing_date >= '2020-01-01' AND t.filing_date < '2026-01-01'
          AND t.price > 0
        GROUP BY t.ticker, t.filing_date
        ORDER BY t.filing_date
    """)

    events = []
    for ticker, filing_date, n_ins, total_val, avg_price in c.fetchall():
        # Use filing_date as entry_date (matches how options_pull.py cached the data)
        entry_date = datetime.strptime(filing_date, "%Y-%m-%d").date()
        events.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "entry_date": entry_date,
            "entry_price": avg_price,
            "n_insiders": n_ins,
            "total_value": total_val,
        })

    conn.close()
    return events


def find_spot_check_trades(trades, entry_px_key="entry_px"):
    """Find representative trades for each exit scenario."""
    spot_checks = {}

    # (1) Big winner — high positive return
    winners = [t for t in trades if t["pct_ret"] > 0.3 and not t["stop_hit"]]
    if winners:
        spot_checks["big_winner"] = max(winners, key=lambda t: t["pct_ret"])

    # (2) Stopped out
    stopped = [t for t in trades if t["stop_hit"]]
    if stopped:
        spot_checks["stopped_out"] = stopped[len(stopped) // 2]  # median

    # (3) Near-stop but survived (within 10% of stop threshold)
    near_stops = [t for t in trades if not t["stop_hit"]
                  and t.get("min_pnl_pct", 0) < -0.15  # got close to -25%
                  and t["pct_ret"] > -0.15]  # but recovered
    if near_stops:
        spot_checks["near_stop_survived"] = min(near_stops, key=lambda t: t["min_pnl_pct"])

    # (4) Hold expiry, no stop, no big win
    time_exits = [t for t in trades if not t["stop_hit"]
                  and -0.10 < t["pct_ret"] < 0.10]
    if time_exits:
        spot_checks["time_exit_flat"] = time_exits[len(time_exits) // 2]

    return spot_checks


def format_spot_check(label, trade, ticker_info=None):
    """Format a spot check trade for the summary."""
    lines = [f"\n  --- {label} ---"]
    if ticker_info:
        lines.append(f"  Ticker: {ticker_info['ticker']} | Filed: {ticker_info['filing_date']} "
                      f"| {ticker_info['n_insiders']} insiders | ${ticker_info['total_value']:,.0f}")
    lines.append(f"  Entry: ${trade['entry_px']:.2f} on {trade['entry_date']} | "
                 f"Exit: ${trade['exit_px']:.2f} | Return: {trade['pct_ret']*100:+.1f}%")
    lines.append(f"  Contracts: {trade['num_contracts']} | P&L: ${trade['dollar_pnl']:+.2f} | "
                 f"Commission: ${trade['commission']:.2f}")
    lines.append(f"  Spread at entry: {trade['spread_pct']*100:.1f}% | Volume: {trade['volume']} | "
                 f"OI: {trade.get('oi', '?')}")
    if trade["stop_hit"]:
        lines.append(f"  STOPPED OUT on {trade['stop_date']} | Min P&L: {trade['min_pnl_pct']*100:+.1f}%")
    else:
        lines.append(f"  Min P&L during hold: {trade['min_pnl_pct']*100:+.1f}%")

    # Daily price action
    if trade.get("daily_prices"):
        lines.append("  Daily prices:")
        for dp in trade["daily_prices"][:10]:  # cap at 10 days
            lines.append(f"    {dp['date']}: bid=${dp.get('bid') or 0:.2f} ask=${dp.get('ask') or 0:.2f} "
                         f"hi=${dp.get('high') or 0:.2f} lo=${dp.get('low') or 0:.2f} "
                         f"vol={dp.get('volume', 0)} P&L={dp['pnl_pct']*100:+.1f}%")

    return "\n".join(lines)


def main():
    print("=" * 90)
    print("CALL LEG GRID SEARCH — Buy-Side Options Analysis")
    print("=" * 90)

    # Load events
    print("\nLoading buy events from insiders.db (2020-2025)...")
    all_events = load_buy_events_from_db()
    print(f"  Total buy events: {len(all_events)}")

    # Connect to theta cache
    print("Loading theta cache...")
    conn = sqlite3.connect(THETA_DB)

    class CacheLookup:
        def __init__(self, conn):
            self._conn = conn
        def get(self, key):
            row = self._conn.execute(
                "SELECT response_json FROM cache WHERE cache_key = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            return json.loads(row[0])

    cache = CacheLookup(conn)

    # Grid search parameters
    grid = []
    for n_ins in [2, 3]:
        for min_val in [2_000_000, 5_000_000]:
            for min_qual in [0.0, 1.5, 2.14]:  # 0.0 = no quality filter
                for strike in ["5pct_itm", "atm", "5pct_otm"]:
                    for hold in [7, 14]:
                        for dte in ["tight", "comfortable"]:
                            for stop in [-0.25, -0.50, None]:
                                for spread_max in [0.10, 0.20]:
                                    grid.append({
                                        "n_ins": n_ins, "min_val": min_val,
                                        "min_qual": min_qual,
                                        "strike": strike, "hold": hold,
                                        "dte": dte, "stop": stop,
                                        "spread_max": spread_max,
                                    })

    print(f"Grid: {len(grid)} configurations")

    # Run grid search
    results = []
    best_sharpe = -999
    best_config = None
    event_trade_map = {}  # config_idx -> [(event, trade), ...]

    for gi, config in enumerate(grid):
        # Filter events
        filtered = [e for e in all_events
                    if e["n_insiders"] >= config["n_ins"]
                    and e["total_value"] >= config["min_val"]]

        # Quality filter (skip if 0.0 = no filter)
        # We don't have quality score pre-computed for individual events,
        # so we use n_insiders * log(value) as a proxy
        if config["min_qual"] > 0:
            import math
            filtered = [e for e in filtered
                        if (min(1.0, math.log2(max(1, e["n_insiders"])) / 3) * 0.5 +
                            min(1.0, math.log10(max(1, e["total_value"])) / 8) * 0.5) * 3.0
                        >= config["min_qual"]]

        trades = []
        trade_events = []

        for event in filtered:
            contract = resolve_call_contract(
                cache, event["ticker"], event["entry_date"],
                event["entry_price"], config["hold"], config["dte"],
                config["strike"],
            )
            if contract is None:
                continue

            eod_rows = cache.get(contract["cache_key"])
            if eod_rows is None:
                continue

            parsed = parse_eod_rows(eod_rows)
            if not parsed:
                continue

            result = simulate_call_trade(
                parsed, event["entry_date"], contract["exit_date"],
                config["stop"], 0.0, config["spread_max"],
            )
            if result is not None:
                trades.append(result)
                trade_events.append(event)

        if len(trades) >= 20:
            stats = calc_stats(trades)
            stats["config"] = config
            stats["config_idx"] = gi
            results.append(stats)
            event_trade_map[gi] = list(zip(trade_events, trades))

            if stats["sharpe"] > best_sharpe:
                best_sharpe = stats["sharpe"]
                best_config = stats

        if (gi + 1) % 100 == 0:
            print(f"  {gi+1}/{len(grid)} configs tested... (best Sharpe so far: {best_sharpe:.2f})")

    print(f"\nCompleted: {len(results)} configs with 20+ trades")

    # Sort by Sharpe
    results.sort(key=lambda x: x["sharpe"], reverse=True)

    # ═══ RESULTS ═══
    print("\n" + "=" * 90)
    print("TOP 20 CONFIGURATIONS BY SHARPE")
    print("=" * 90)
    print(f"{'#':>3} {'Sharpe':>7} {'N':>5} {'WR':>6} {'Mean':>8} {'Med':>8} "
          f"{'P&L':>9} {'MaxDD':>8} {'Stops':>6} "
          f"{'Ins':>3} {'Val':>5} {'Strike':>8} {'Hold':>4} {'DTE':>5} {'Stop':>5} {'Sprd':>5}")
    print("-" * 130)

    for i, r in enumerate(results[:20]):
        c = r["config"]
        val_str = f"{c['min_val']/1e6:.0f}M"
        stop_str = f"{c['stop']*100:.0f}%" if c['stop'] else "none"
        print(f"{i+1:>3} {r['sharpe']:>7.2f} {r['n']:>5} {r['win_rate']*100:>5.1f}% "
              f"{r['mean_return']*100:>7.2f}% {r['median_return']*100:>7.2f}% "
              f"${r['total_pnl']:>8,.0f} ${r['max_drawdown']:>7,.0f} "
              f"{r['stops_pct']*100:>5.1f}% "
              f"{c['n_ins']:>3} {val_str:>5} {c['strike']:>8} {c['hold']:>4}d "
              f"{c['dte']:>5} {stop_str:>5} {c['spread_max']*100:.0f}%")

    # Grid search distribution
    print("\n" + "=" * 90)
    print("GRID SEARCH DISTRIBUTION")
    print("=" * 90)
    sharpes = [r["sharpe"] for r in results]
    n_positive = sum(1 for s in sharpes if s > 0)
    n_gt1 = sum(1 for s in sharpes if s > 1.0)
    print(f"  Configs with 20+ trades: {len(results)}")
    if results:
        print(f"  Positive Sharpe: {n_positive} ({n_positive/len(results)*100:.0f}%)")
        print(f"  Sharpe > 1.0: {n_gt1} ({n_gt1/len(results)*100:.0f}%)")
        print(f"  Median Sharpe: {np.median(sharpes):.3f}")
        print(f"  Mean Sharpe: {np.mean(sharpes):.3f}")
    else:
        print("  No configs produced 20+ trades — check data availability")
        conn.close()
        return

    # ═══ WALK-FORWARD on top 5 ═══
    print("\n" + "=" * 90)
    print("WALK-FORWARD VALIDATION (train ≤2022, test ≥2023)")
    print("=" * 90)

    wf_results = []
    for i, r in enumerate(results[:5]):
        gi = r["config_idx"]
        if gi not in event_trade_map:
            continue
        pairs = event_trade_map[gi]
        train_trades = [t for e, t in pairs if t["entry_date"] <= TRAIN_END]
        test_trades = [t for e, t in pairs if t["entry_date"] > TRAIN_END]

        train_stats = calc_stats(train_trades) if len(train_trades) >= 10 else None
        test_stats = calc_stats(test_trades) if len(test_trades) >= 10 else None

        c = r["config"]
        val_str = f"{c['min_val']/1e6:.0f}M"
        stop_str = f"{c['stop']*100:.0f}%" if c['stop'] else "none"
        label = f"#{i+1}: {c['n_ins']}ins/{val_str} {c['strike']} {c['hold']}d {c['dte']} stop={stop_str}"

        print(f"\n  {label}")
        if train_stats:
            print(f"    TRAIN: N={train_stats['n']}, Sharpe={train_stats['sharpe']:.2f}, "
                  f"WR={train_stats['win_rate']*100:.1f}%, P&L=${train_stats['total_pnl']:+,.0f}")
        if test_stats:
            degradation = (1 - test_stats['sharpe'] / train_stats['sharpe']) * 100 if train_stats and train_stats['sharpe'] > 0 else 0
            print(f"    TEST:  N={test_stats['n']}, Sharpe={test_stats['sharpe']:.2f}, "
                  f"WR={test_stats['win_rate']*100:.1f}%, P&L=${test_stats['total_pnl']:+,.0f} "
                  f"(degradation: {degradation:.0f}%)")
        else:
            print(f"    TEST:  Insufficient trades")

        wf_results.append({
            "label": label, "config": c,
            "all": r,
            "train": train_stats,
            "test": test_stats,
        })

    # ═══ SLIPPAGE STRESS TEST on best config ═══
    if results:
        best = results[0]
        gi = best["config_idx"]
        if gi in event_trade_map:
            print("\n" + "=" * 90)
            print(f"SLIPPAGE STRESS TEST — Best Config")
            print("=" * 90)

            pairs = event_trade_map[gi]
            c = best["config"]

            for slip_name, slip_pct in SLIPPAGE_SCENARIOS.items():
                slip_trades = []
                for event, _ in pairs:
                    contract = resolve_call_contract(
                        cache, event["ticker"], event["entry_date"],
                        event["entry_price"], c["hold"], c["dte"], c["strike"],
                    )
                    if contract is None:
                        continue
                    eod_rows = cache.get(contract["cache_key"])
                    if eod_rows is None:
                        continue
                    parsed = parse_eod_rows(eod_rows)
                    result = simulate_call_trade(
                        parsed, event["entry_date"], contract["exit_date"],
                        c["stop"], slip_pct, c["spread_max"],
                    )
                    if result is not None:
                        slip_trades.append(result)

                if slip_trades:
                    stats = calc_stats(slip_trades)
                    print(f"  {slip_name}: N={stats['n']}, Sharpe={stats['sharpe']:.2f}, "
                          f"WR={stats['win_rate']*100:.1f}%, P&L=${stats['total_pnl']:+,.0f}")

    # ═══ SPOT CHECKS ═══
    if results and results[0]["config_idx"] in event_trade_map:
        print("\n" + "=" * 90)
        print("SPOT CHECKS — Trade Validation")
        print("=" * 90)

        gi = results[0]["config_idx"]
        pairs = event_trade_map[gi]
        all_trades = [t for _, t in pairs]
        all_events_matched = [e for e, _ in pairs]

        spot_checks = find_spot_check_trades(all_trades)

        for label, trade in spot_checks.items():
            # Find matching event
            idx = all_trades.index(trade)
            event = all_events_matched[idx]
            print(format_spot_check(label.upper().replace("_", " "), trade, event))

        # Validate stop integrity
        print("\n  --- STOP INTEGRITY CHECK ---")
        if results[0]["config"]["stop"] is not None:
            stop_level = results[0]["config"]["stop"]
            missed_stops = 0
            checked = 0
            for event, trade in pairs:
                if not trade.get("daily_prices"):
                    continue
                checked += 1
                for dp in trade["daily_prices"]:
                    if dp.get("low") and dp["low"] > 0:
                        day_low_pnl = (dp["low"] - trade["entry_px"]) / trade["entry_px"]
                        if day_low_pnl <= stop_level and not trade["stop_hit"]:
                            missed_stops += 1
                            print(f"  WARNING: Possible missed stop — {event['ticker']} {event['filing_date']}, "
                                  f"low=${dp['low']:.2f}, stop_level=${trade['entry_px'] * (1 + stop_level):.2f}")
                            break

            print(f"  Checked {checked} trades with daily data")
            print(f"  Missed stops: {missed_stops}")
            if missed_stops == 0:
                print(f"  PASS — All stops correctly triggered")
        else:
            print(f"  N/A — No stop loss on best config")

    # ═══ SAVE RESULTS ═══
    output = {
        "generated": datetime.now().isoformat(),
        "grid_size": len(grid),
        "configs_with_trades": len(results),
        "top_20": [],
        "walk_forward": wf_results,
        "grid_distribution": {
            "n_positive_sharpe": n_positive,
            "n_sharpe_gt_1": n_gt1,
            "median_sharpe": round(float(np.median(sharpes)), 3),
            "mean_sharpe": round(float(np.mean(sharpes)), 3),
        },
    }

    for r in results[:20]:
        entry = dict(r)
        entry.pop("config_idx", None)
        output["top_20"].append(entry)

    output_path = os.path.join(OUTPUT_DIR, "call_leg_analysis.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved to {output_path}")

    conn.close()


if __name__ == "__main__":
    main()
