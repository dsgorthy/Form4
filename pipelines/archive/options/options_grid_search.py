#!/usr/bin/env python3
"""
Insider Options Grid Search — Signal Quality × Strategy Parameters
------------------------------------------------------------------
Pre-computes all trade outcomes once, then slices by signal filters.

Grid:
  Signal: 3 n_insiders × 3 value × 3 quality × 2 spread = 54 combos
  Strategy: 4 holds × 2 DTE × 4 strikes × 4 stops × 1 pricing = 128 configs
  Total: 54 × 128 = 6,912 backtests

Usage:
    python options_grid_search.py [--side buy|sell|both]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from theta_client import get_eod_date, find_nearest_expiration, find_nearest_strike, add_trading_days

# ── Config ──

STRIKES_CALL = {"5pct_itm": 0.95, "atm": 1.00, "5pct_otm": 1.05, "10pct_otm": 1.10}
STRIKES_PUT  = {"5pct_itm": 1.05, "atm": 1.00, "5pct_otm": 0.95, "10pct_otm": 0.90}
STRIKES = STRIKES_CALL  # default, overridden per side in run_grid()
HOLD_DTE_MAP = {7: (14, 21), 14: (28, 45), 30: (50, 60), 60: (90, 120)}
HOLD_DAYS = [7, 14, 30, 60]
DTE_TYPES = ["tight", "comfortable"]
STRIKE_TYPES = list(STRIKES_CALL.keys())
STOP_LOSSES = [-0.25, -0.50, -0.75, None]
NOTIONAL_PER_TRADE = 1000

N_INSIDERS_FILTERS = [2, 3, 5]
VALUE_FILTERS = [500_000, 1_000_000, 5_000_000]
QUALITY_FILTERS = [0.0, 1.50, 2.14]
SPREAD_FILTERS = [0.10, 0.20]

BUY_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")
SELL_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_sells_7d.csv")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")


# ── Helpers ──

def get_float(row, key):
    try:
        val = float(str(row.get(key, "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


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


def resolve_contract(cache, ticker, entry_date, entry_price, hold_days, dte_type, strike_type,
                      option_type="C", strike_map=None):
    if strike_map is None:
        strike_map = STRIKES_CALL
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

    real_strike = find_nearest_strike(strikes, entry_price * strike_map[strike_type])
    if real_strike is None:
        return None

    ps = (entry_date - timedelta(days=2)).strftime("%Y-%m-%d")
    pe = (exit_date + timedelta(days=5)).strftime("%Y-%m-%d")
    cache_key = f"opt_eod_daily|{ticker}|{exp_str}|{real_strike}|{option_type}|{ps}|{pe}"

    return {"exit_date": exit_date, "cache_key": cache_key}


def parse_eod_rows(eod_rows):
    """Parse EOD rows into date-sorted list of (date, bid, ask, low, close)."""
    parsed = []
    for row in eod_rows:
        d = get_eod_date(row)
        if d is None:
            continue
        parsed.append((
            d,
            get_float(row, "bid"),
            get_float(row, "ask"),
            get_float(row, "low"),
            get_float(row, "close"),
        ))
    parsed.sort(key=lambda x: x[0])
    return parsed


def simulate_all_stops(parsed_rows, entry_date, exit_date):
    """
    Simulate a trade for ALL stop-loss levels at once (conservative pricing).
    Returns dict with entry info + per-stop results, or None if no valid entry.
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

    d, bid, ask, low, close = entry_row
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    spread_pct = (ask - bid) / mid

    # Conservative: entry at ask
    entry_px = ask
    if entry_px <= 0:
        return None

    num_contracts = int(NOTIONAL_PER_TRADE // (entry_px * 100))
    if num_contracts <= 0:
        return None

    actual_entry_date = d

    # Pre-compute stop levels
    stop_levels = {}
    for sp in STOP_LOSSES:
        if sp is not None:
            stop_levels[sp] = entry_px * (1.0 + sp)

    # Walk forward through hold period
    stop_triggered = {}  # stop_pct -> (date, exit_px)
    for row in parsed_rows:
        rd, rbid, rask, rlow, rclose = row
        if rd <= actual_entry_date or rd > exit_date:
            continue
        if rlow is None:
            continue
        for sp, level in stop_levels.items():
            if sp not in stop_triggered and rlow <= level:
                stop_triggered[sp] = level

    # Find time-based exit
    exit_row = None
    for row in parsed_rows:
        if row[0] >= exit_date:
            exit_row = row
            break
    if exit_row is None and parsed_rows:
        exit_row = parsed_rows[-1]
    if exit_row is None:
        return None

    # Conservative exit: bid
    time_exit_px = exit_row[1]  # bid
    if time_exit_px is None or time_exit_px <= 0:
        time_exit_px = exit_row[4]  # close fallback
        if time_exit_px is None or time_exit_px <= 0:
            return None

    # Build results for each stop level
    results = {}
    for sp in STOP_LOSSES:
        if sp is not None and sp in stop_triggered:
            final_px = stop_triggered[sp]
        else:
            final_px = time_exit_px

        pct_ret = (final_px - entry_px) / entry_px
        dollar_pnl = (final_px - entry_px) * num_contracts * 100
        results[sp] = (pct_ret, dollar_pnl)

    return {
        "spread_pct": spread_pct,
        "results": results,  # stop_pct -> (pct_return, dollar_pnl)
    }


# ── Core grid search ──

def run_grid(side, events_csv, output_path, top_n, cache_dict):
    """Run the full grid search for one side (buy/calls or sell/puts)."""
    option_type = "C" if side == "buy" else "P"
    strike_map = STRIKES_CALL if side == "buy" else STRIKES_PUT
    side_label = "BUY (calls)" if side == "buy" else "SELL (puts)"

    print("=" * 60)
    print(f"Insider Options Grid Search — {side_label}")
    print("=" * 60)

    n_signal = len(N_INSIDERS_FILTERS) * len(VALUE_FILTERS) * len(QUALITY_FILTERS) * len(SPREAD_FILTERS)
    n_strategy = len(HOLD_DAYS) * len(DTE_TYPES) * len(STRIKE_TYPES) * len(STOP_LOSSES)
    print(f"  {n_signal} signal combos × {n_strategy} strategy configs = {n_signal * n_strategy} total")
    print(f"  Pricing: conservative only")
    print()

    # ── Load events ──
    print(f"Loading cluster {side} events from {os.path.basename(events_csv)}...")
    with open(events_csv) as f:
        raw_events = list(csv.DictReader(f))
    events = [e for e in raw_events if e.get("is_cluster") == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_n_insiders"] = int(e["n_insiders"])
        e["_total_value"] = float(e["total_value"])
        e["_quality_score"] = float(e["quality_score"])
    print(f"  {len(events)} cluster events")

    # Wrap cache dict for resolve_contract
    class C:
        def get(self, k):
            return cache_dict.get(k)
    cache_obj = C()

    # ── Phase 1: Resolve contracts ──
    print("\nResolving contracts...")
    t0 = time.monotonic()
    contracts = {}
    eod_parsed = {}
    resolved = 0

    for i, ev in enumerate(events):
        for hd in HOLD_DAYS:
            for dt in DTE_TYPES:
                for st in STRIKE_TYPES:
                    c = resolve_contract(cache_obj, ev["_ticker"], ev["_entry_date"],
                                         ev["_entry_price"], hd, dt, st,
                                         option_type=option_type, strike_map=strike_map)
                    contracts[(i, hd, dt, st)] = c
                    if c is None:
                        continue
                    resolved += 1
                    ck = c["cache_key"]
                    if ck not in eod_parsed:
                        raw = cache_dict.get(ck)
                        if raw and raw != "__NONE__":
                            eod_parsed[ck] = parse_eod_rows(raw)
                        else:
                            eod_parsed[ck] = None

        if (i + 1) % 1000 == 0:
            print(f"  {i + 1}/{len(events)}...")

    print(f"  {resolved} contracts, {len(eod_parsed)} EOD series in {time.monotonic() - t0:.1f}s")

    # ── Phase 2: Pre-compute ALL trade outcomes ──
    print("\nPre-computing trade outcomes...")
    t0 = time.monotonic()
    trade_outcomes = {}
    computed = 0

    for i, ev in enumerate(events):
        for hd in HOLD_DAYS:
            for dt in DTE_TYPES:
                for st in STRIKE_TYPES:
                    c = contracts.get((i, hd, dt, st))
                    if c is None:
                        continue
                    rows = eod_parsed.get(c["cache_key"])
                    if rows is None:
                        continue

                    outcome = simulate_all_stops(rows, ev["_entry_date"], c["exit_date"])
                    if outcome is not None:
                        trade_outcomes[(i, hd, dt, st)] = outcome
                        computed += 1

        if (i + 1) % 1000 == 0:
            print(f"  {i + 1}/{len(events)}...")

    print(f"  {computed} valid trade outcomes in {time.monotonic() - t0:.1f}s")

    del eod_parsed
    del contracts

    # ── Phase 3: Aggregate by signal × strategy ──
    print("\nAggregating grid results...")
    t0 = time.monotonic()

    results = []
    config_num = 0

    for min_ins in N_INSIDERS_FILTERS:
        for min_val in VALUE_FILTERS:
            for min_q in QUALITY_FILTERS:
                mask = [
                    i for i, e in enumerate(events)
                    if e["_n_insiders"] >= min_ins
                    and e["_total_value"] >= min_val
                    and e["_quality_score"] >= min_q
                ]
                n_events = len(mask)

                for spread_f in SPREAD_FILTERS:
                    for hd in HOLD_DAYS:
                        for dt in DTE_TYPES:
                            for st in STRIKE_TYPES:
                                for stop in STOP_LOSSES:
                                    config_num += 1
                                    pct_rets = []
                                    dollar_pnls = []

                                    for i in mask:
                                        outcome = trade_outcomes.get((i, hd, dt, st))
                                        if outcome is None:
                                            continue
                                        if outcome["spread_pct"] > spread_f:
                                            continue
                                        r = outcome["results"].get(stop)
                                        if r is None:
                                            continue
                                        pct_rets.append(r[0])
                                        dollar_pnls.append(r[1])

                                    n_trades = len(pct_rets)
                                    if n_trades > 0:
                                        arr = np.array(pct_rets)
                                        mean_r = float(np.mean(arr))
                                        std_r = float(np.std(arr, ddof=1)) if n_trades > 1 else 0.0
                                        wr = float(np.sum(arr > 0) / n_trades)
                                        sharpe = (mean_r / std_r) * np.sqrt(min(252, n_trades)) if std_r > 0 else 0.0
                                        tot_pnl = float(np.sum(dollar_pnls))
                                        avg_pnl = float(np.mean(dollar_pnls))
                                        med_r = float(np.median(arr))
                                        max_dd = float(np.min(np.cumsum(dollar_pnls) - np.maximum.accumulate(np.cumsum(dollar_pnls))))
                                    else:
                                        mean_r = wr = sharpe = tot_pnl = avg_pnl = med_r = max_dd = 0.0

                                    results.append({
                                        "min_insiders": min_ins,
                                        "min_value": min_val,
                                        "min_quality": min_q,
                                        "spread_filter": spread_f,
                                        "hold_days": hd,
                                        "dte_type": dt,
                                        "strike_type": st,
                                        "stop_loss": stop if stop is not None else "none",
                                        "n_events_filtered": n_events,
                                        "n_trades": n_trades,
                                        "win_rate": round(wr, 4),
                                        "mean_return": round(mean_r, 6),
                                        "median_return": round(med_r, 6),
                                        "sharpe": round(float(sharpe), 4),
                                        "total_dollar_pnl": round(tot_pnl, 2),
                                        "avg_dollar_pnl": round(avg_pnl, 2),
                                        "max_drawdown": round(max_dd, 2),
                                    })

                    if config_num % 512 == 0:
                        elapsed = time.monotonic() - t0
                        print(f"  [{config_num}/{n_signal * n_strategy}] — {elapsed:.1f}s")

    results.sort(key=lambda r: -r["sharpe"])
    print(f"  Done: {len(results)} configs in {time.monotonic() - t0:.1f}s")

    # ── Output ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV: {output_path} ({len(results)} rows)")

    # ── Print top configs ──
    print()
    print("=" * 155)
    print(f"TOP CONFIGS BY SHARPE — {side_label} (conservative pricing)")
    print("=" * 155)
    hdr = (f"{'#':>3} | {'Ins':>3} | {'Val':>5} | {'Q':>4} | {'Sprd':>4} | "
           f"{'Hold':>4} | {'DTE':>11} | {'Strike':>10} | {'Stop':>5} | "
           f"{'Evts':>5} | {'N':>4} | {'WR':>5} | {'Mean':>8} | {'Sharpe':>7} | "
           f"{'Total$':>9} | {'MaxDD':>9}")
    print(hdr)
    print("-" * 155)

    for i, r in enumerate(results[:top_n]):
        vs = f"{r['min_value']/1e6:.1f}M"
        ss = "none" if r["stop_loss"] == "none" else f"{float(r['stop_loss']):.0%}"
        print(f"{i+1:>3} | {r['min_insiders']:>3} | {vs:>5} | {r['min_quality']:>4.1f} | "
              f"{r['spread_filter']:>3.0%} | {r['hold_days']:>4}d | {r['dte_type']:>11} | "
              f"{r['strike_type']:>10} | {ss:>5} | "
              f"{r['n_events_filtered']:>5} | {r['n_trades']:>4} | "
              f"{r['win_rate']:>4.1%} | {r['mean_return']:>+7.2%} | {r['sharpe']:>7.2f} | "
              f"${r['total_dollar_pnl']:>8,.0f} | ${r['max_drawdown']:>8,.0f}")

    # ── Signal combo rankings ──
    with_trades = [r for r in results if r["n_trades"] > 0]
    pos_sharpe = [r for r in with_trades if r["sharpe"] > 0]
    print(f"\nConfigs with trades: {len(with_trades)}/{len(results)} | "
          f"Positive Sharpe: {len(pos_sharpe)}/{len(results)}")

    signal_sharpes = defaultdict(list)
    for r in with_trades:
        sk = (r["min_insiders"], r["min_value"], r["min_quality"], r["spread_filter"])
        signal_sharpes[sk].append(r["sharpe"])

    print("\nTop 10 signal combos by avg Sharpe:")
    ranked = sorted(signal_sharpes.items(), key=lambda x: -np.mean(x[1]))
    for (mi, mv, mq, sf), sharpes in ranked[:10]:
        print(f"  ins>={mi} val>={mv/1e6:.1f}M q>={mq:.1f} sprd<={sf:.0%}: "
              f"avg={np.mean(sharpes):.3f} pos={sum(1 for s in sharpes if s > 0)}/{len(sharpes)}")

    # ── Hold period rankings ──
    hold_sharpes = defaultdict(list)
    for r in with_trades:
        hold_sharpes[r["hold_days"]].append(r["sharpe"])
    print("\nAvg Sharpe by hold period:")
    for hd in HOLD_DAYS:
        s = hold_sharpes.get(hd, [])
        print(f"  {hd}d: avg={np.mean(s):.3f} pos={sum(1 for x in s if x > 0)}/{len(s)}")

    # ── Strike rankings ──
    strike_sharpes = defaultdict(list)
    for r in with_trades:
        strike_sharpes[r["strike_type"]].append(r["sharpe"])
    print("\nAvg Sharpe by strike:")
    for st in STRIKE_TYPES:
        s = strike_sharpes.get(st, [])
        print(f"  {st}: avg={np.mean(s):.3f} pos={sum(1 for x in s if x > 0)}/{len(s)}")

    print("=" * 155)

    return results


# ── Main ──

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: auto per side)")
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--side", choices=["buy", "sell", "both"], default="buy",
                        help="Side: buy (calls), sell (puts), or both")
    args = parser.parse_args()

    sides = ["buy", "sell"] if args.side == "both" else [args.side]

    # ── Load cache into memory (shared across sides) ──
    db_path = os.path.join(SCRIPT_DIR, "data", "theta_cache.db")
    print(f"Loading theta_cache.db into memory...")
    t0 = time.monotonic()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT cache_key, response_json FROM cache")
    cache_dict = {}
    for key, val in cur:
        cache_dict[key] = json.loads(val) if val else None
    conn.close()
    print(f"  {len(cache_dict)} entries in {time.monotonic() - t0:.1f}s\n")

    for side in sides:
        if side == "buy":
            events_csv = BUY_EVENTS_CSV
            default_output = os.path.join(OUTPUT_DIR, "grid_search_results.csv")
        else:
            events_csv = SELL_EVENTS_CSV
            default_output = os.path.join(OUTPUT_DIR, "grid_search_results_sells.csv")

        output_path = args.output if (args.output and len(sides) == 1) else default_output
        run_grid(side, events_csv, output_path, args.top, cache_dict)
        print()


if __name__ == "__main__":
    main()
