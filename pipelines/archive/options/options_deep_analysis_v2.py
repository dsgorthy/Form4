#!/usr/bin/env python3
"""
Insider Options Deep Analysis — Next Steps 1-4
-----------------------------------------------
1. Time-period split (pre-COVID / COVID / post-COVID / yearly)
2. Ticker concentration (top PnL contributors, diversification)
3. Options vs shares comparison (apples-to-apples signal filters)
4. Cumulative PnL curves with drawdown stats
5. PnL stability (remove top contributors)

Uses pre-computed trade outcomes from options_grid_search.py infrastructure.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict, Counter
from datetime import date, datetime, timedelta

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from theta_client import get_eod_date, find_nearest_expiration, find_nearest_strike, add_trading_days

STRIKES = {"5pct_itm": 0.95, "atm": 1.00, "5pct_otm": 1.05, "10pct_otm": 1.10}
HOLD_DTE_MAP = {7: (14, 21), 14: (28, 45), 30: (50, 60), 60: (90, 120)}
NOTIONAL_PER_TRADE = 1000
BUY_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")

TOP_CONFIGS = [
    {"hold": 7, "dte": "tight", "strike": "atm", "stop": -0.25, "label": "7d/tight/ATM/-25%"},
    {"hold": 7, "dte": "tight", "strike": "5pct_otm", "stop": -0.25, "label": "7d/tight/5%OTM/-25%"},
    {"hold": 7, "dte": "comfortable", "strike": "atm", "stop": -0.25, "label": "7d/comf/ATM/-25%"},
    {"hold": 7, "dte": "tight", "strike": "atm", "stop": -0.50, "label": "7d/tight/ATM/-50%"},
    {"hold": 30, "dte": "tight", "strike": "atm", "stop": -0.25, "label": "30d/tight/ATM/-25%"},
]

SIGNAL_CONFIGS = [
    {"ins": 2, "val": 500_000, "qual": 0.0, "spread": 0.10, "label": "Loose (ins>=2, $500K+, 10%sprd)"},
    {"ins": 2, "val": 1_000_000, "qual": 1.5, "spread": 0.10, "label": "Medium (ins>=2, $1M+, q>=1.5, 10%sprd)"},
    {"ins": 2, "val": 500_000, "qual": 1.5, "spread": 0.20, "label": "Medium-wide (ins>=2, $500K+, q>=1.5, 20%sprd)"},
    {"ins": 2, "val": 5_000_000, "qual": 1.5, "spread": 0.10, "label": "Tight (ins>=2, $5M+, q>=1.5, 10%sprd)"},
    {"ins": 3, "val": 5_000_000, "qual": 1.5, "spread": 0.10, "label": "Very Tight (ins>=3, $5M+, q>=1.5, 10%sprd)"},
]

TIME_PERIODS = {
    "pre_covid": (date(2019, 1, 1), date(2020, 2, 29)),
    "covid": (date(2020, 3, 1), date(2021, 3, 31)),
    "post_covid": (date(2021, 4, 1), date(2022, 12, 31)),
    "2023+": (date(2023, 1, 1), date(2026, 12, 31)),
}


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

def resolve_contract(cache, ticker, entry_date, entry_price, hold_days, dte_type, strike_type):
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
    real_strike = find_nearest_strike(strikes, entry_price * STRIKES[strike_type])
    if real_strike is None:
        return None
    ps = (entry_date - timedelta(days=2)).strftime("%Y-%m-%d")
    pe = (exit_date + timedelta(days=5)).strftime("%Y-%m-%d")
    cache_key = f"opt_eod_daily|{ticker}|{exp_str}|{real_strike}|C|{ps}|{pe}"
    return {"exit_date": exit_date, "cache_key": cache_key}

def parse_eod_rows(eod_rows):
    parsed = []
    for row in eod_rows:
        d = get_eod_date(row)
        if d is None:
            continue
        parsed.append((d, get_float(row, "bid"), get_float(row, "ask"),
                        get_float(row, "low"), get_float(row, "close")))
    parsed.sort(key=lambda x: x[0])
    return parsed

def simulate_trade(parsed_rows, entry_date, exit_date, stop_loss):
    if not parsed_rows:
        return None
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
    entry_px = ask
    if entry_px <= 0:
        return None
    num_contracts = int(NOTIONAL_PER_TRADE // (entry_px * 100))
    if num_contracts <= 0:
        return None
    stop_hit = False
    final_px = None
    if stop_loss is not None:
        stop_level = entry_px * (1.0 + stop_loss)
        for row in parsed_rows:
            if row[0] <= d or row[0] > exit_date:
                continue
            if row[3] is not None and row[3] <= stop_level:
                final_px = stop_level
                stop_hit = True
                break
    if not stop_hit:
        exit_row = None
        for row in parsed_rows:
            if row[0] >= exit_date:
                exit_row = row
                break
        if exit_row is None and parsed_rows:
            exit_row = parsed_rows[-1]
        if exit_row is None:
            return None
        final_px = exit_row[1]
        if final_px is None or final_px <= 0:
            final_px = exit_row[4]
            if final_px is None or final_px <= 0:
                return None
    pct_ret = (final_px - entry_px) / entry_px
    dollar_pnl = (final_px - entry_px) * num_contracts * 100
    return (pct_ret, dollar_pnl, spread_pct, d)

def calc_stats(returns, pnls):
    if not returns:
        return None
    arr = np.array(returns)
    pnl_arr = np.array(pnls)
    n = len(arr)
    mean_r = float(np.mean(arr))
    std_r = float(np.std(arr, ddof=1)) if n > 1 else 0.0
    wr = float(np.sum(arr > 0) / n)
    sharpe = (mean_r / std_r) * np.sqrt(min(252, n)) if std_r > 0 else 0.0
    cum_pnl = np.cumsum(pnl_arr)
    max_dd = float(np.min(cum_pnl - np.maximum.accumulate(cum_pnl)))
    return {
        "n": n, "win_rate": round(wr, 4), "mean_return": round(mean_r, 6),
        "median_return": round(float(np.median(arr)), 6),
        "sharpe": round(float(sharpe), 4), "total_pnl": round(float(np.sum(pnl_arr)), 2),
        "avg_pnl": round(float(np.mean(pnl_arr)), 2), "max_drawdown": round(max_dd, 2),
    }

def filter_trades(trades, sig, events):
    out = {}
    for i, t in trades.items():
        ev = events[i]
        if (ev["_n_insiders"] >= sig["ins"] and ev["_total_value"] >= sig["val"]
                and ev["_quality_score"] >= sig["qual"] and t["spread_pct"] <= sig["spread"]):
            out[i] = t
    return out


def main():
    print("=" * 70)
    print("INSIDER OPTIONS DEEP ANALYSIS v2")
    print("=" * 70)

    print("\nLoading events...")
    with open(BUY_EVENTS_CSV) as f:
        raw_events = list(csv.DictReader(f))
    events = [e for e in raw_events if e.get("is_cluster") == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_n_insiders"] = int(e["n_insiders"])
        e["_total_value"] = float(e["total_value"])
        e["_quality_score"] = float(e["quality_score"])
        e["_7d_return"] = float(e.get("return_7d", 0) or 0)
    print(f"  {len(events)} cluster events")

    db_path = os.path.join(SCRIPT_DIR, "data", "theta_cache.db")
    print(f"\nLoading theta_cache.db...")
    t0 = time.monotonic()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT cache_key, response_json FROM cache")
    cache = {}
    for key, val in cur:
        cache[key] = json.loads(val) if val else None
    conn.close()
    print(f"  {len(cache)} entries in {time.monotonic() - t0:.1f}s")

    class C:
        def get(self, k):
            return cache.get(k)
    cache_obj = C()

    print("\nResolving contracts and simulating trades...")
    t0 = time.monotonic()
    all_trades = {}
    for cfg in TOP_CONFIGS:
        label = cfg["label"]
        trades = {}
        for i, ev in enumerate(events):
            c = resolve_contract(cache_obj, ev["_ticker"], ev["_entry_date"],
                                 ev["_entry_price"], cfg["hold"], cfg["dte"], cfg["strike"])
            if c is None:
                continue
            ck = c["cache_key"]
            raw = cache.get(ck)
            if raw is None or raw == "__NONE__":
                continue
            rows = parse_eod_rows(raw)
            result = simulate_trade(rows, ev["_entry_date"], c["exit_date"], cfg["stop"])
            if result is not None:
                pct_ret, dollar_pnl, spread_pct, actual_date = result
                trades[i] = {
                    "pct_ret": pct_ret, "dollar_pnl": dollar_pnl,
                    "spread_pct": spread_pct, "entry_date": actual_date,
                    "ticker": ev["_ticker"],
                }
        all_trades[label] = trades
        print(f"  {label}: {len(trades)} trades")
    print(f"  Done in {time.monotonic() - t0:.1f}s")
    del cache, cache_obj

    results = {"generated": datetime.now().isoformat()}

    # ═══ ANALYSIS 1: TIME-PERIOD SPLIT ═══
    print("\n" + "=" * 70)
    print("ANALYSIS 1: TIME-PERIOD SPLIT")
    print("=" * 70)

    time_results = {}
    for sig in SIGNAL_CONFIGS:
        sig_label = sig["label"]
        time_results[sig_label] = {}
        for cfg in TOP_CONFIGS:
            trades = filter_trades(all_trades[cfg["label"]], sig, events)
            period_stats = {}
            for pname, (start, end) in TIME_PERIODS.items():
                pt = {i: t for i, t in trades.items() if start <= t["entry_date"] <= end}
                period_stats[pname] = calc_stats(
                    [t["pct_ret"] for t in pt.values()],
                    [t["dollar_pnl"] for t in pt.values()])
            yearly = defaultdict(lambda: {"r": [], "p": []})
            for t in trades.values():
                yr = t["entry_date"].year
                yearly[yr]["r"].append(t["pct_ret"])
                yearly[yr]["p"].append(t["dollar_pnl"])
            yearly_stats = {str(yr): calc_stats(v["r"], v["p"]) for yr, v in sorted(yearly.items())}
            time_results[sig_label][cfg["label"]] = {
                "periods": period_stats, "yearly": yearly_stats, "total_trades": len(trades)}
    results["time_period_split"] = time_results

    # Print summary
    for sig in SIGNAL_CONFIGS[:3]:
        cfg = TOP_CONFIGS[0]
        tr = time_results[sig["label"]][cfg["label"]]
        print(f"\n{cfg['label']} | {sig['label']}")
        print(f"  {'Period':<15} {'N':>5} {'WR':>6} {'Mean%':>8} {'Sharpe':>7} {'PnL$':>9}")
        for p, s in tr["periods"].items():
            if s:
                print(f"  {p:<15} {s['n']:>5} {s['win_rate']:>5.1%} {s['mean_return']*100:>7.1f}% {s['sharpe']:>7.2f} {s['total_pnl']:>9.0f}")
        print(f"  Yearly:")
        for yr, s in tr["yearly"].items():
            if s:
                print(f"    {yr}: N={s['n']:>4}, WR={s['win_rate']:.1%}, Sharpe={s['sharpe']:.2f}, PnL=${s['total_pnl']:.0f}")

    # ═══ ANALYSIS 2: TICKER CONCENTRATION ═══
    print("\n" + "=" * 70)
    print("ANALYSIS 2: TICKER CONCENTRATION")
    print("=" * 70)

    ticker_results = {}
    for sig in SIGNAL_CONFIGS[:3]:
        cfg = TOP_CONFIGS[0]
        trades = filter_trades(all_trades[cfg["label"]], sig, events)
        ticker_pnl = defaultdict(lambda: {"pnl": 0, "n": 0, "rets": []})
        for t in trades.values():
            tk = t["ticker"]
            ticker_pnl[tk]["pnl"] += t["dollar_pnl"]
            ticker_pnl[tk]["n"] += 1
            ticker_pnl[tk]["rets"].append(t["pct_ret"])

        total_pnl = sum(v["pnl"] for v in ticker_pnl.values())
        sorted_tickers = sorted(ticker_pnl.items(), key=lambda x: x[1]["pnl"], reverse=True)
        n_tickers = len(ticker_pnl)
        pos_tickers = sum(1 for v in ticker_pnl.values() if v["pnl"] > 0)
        top5_pnl = sum(v["pnl"] for _, v in sorted_tickers[:5])
        top10_pnl = sum(v["pnl"] for _, v in sorted_tickers[:10])

        def tk_info(t, v):
            wr = sum(1 for r in v["rets"] if r > 0) / len(v["rets"]) if v["rets"] else 0
            return {"ticker": t, "pnl": round(v["pnl"], 2), "n": v["n"], "wr": round(wr, 2)}

        ticker_results[sig["label"]] = {
            "n_tickers": n_tickers, "n_positive": pos_tickers,
            "total_pnl": round(total_pnl, 2),
            "top5_pnl": round(top5_pnl, 2),
            "top5_pct": round(top5_pnl / total_pnl * 100, 1) if total_pnl > 0 else 0,
            "top10_pnl": round(top10_pnl, 2),
            "top10_pct": round(top10_pnl / total_pnl * 100, 1) if total_pnl > 0 else 0,
            "top_10_positive": [tk_info(t, v) for t, v in sorted_tickers[:10]],
            "top_10_negative": [tk_info(t, v) for t, v in sorted_tickers[-10:]],
        }

        print(f"\n{sig['label']} | {cfg['label']}")
        print(f"  {n_tickers} tickers | {pos_tickers} pos, {n_tickers - pos_tickers} neg")
        print(f"  Total PnL: ${total_pnl:,.0f}")
        if total_pnl > 0:
            print(f"  Top 5: ${top5_pnl:,.0f} ({top5_pnl/total_pnl*100:.0f}%) | Top 10: ${top10_pnl:,.0f} ({top10_pnl/total_pnl*100:.0f}%)")
        print(f"  Top 5 winners:")
        for t, v in sorted_tickers[:5]:
            wr = sum(1 for r in v["rets"] if r > 0) / len(v["rets"])
            print(f"    {t:<8} ${v['pnl']:>8,.0f}  N={v['n']:>3}  WR={wr:.0%}")
        print(f"  Top 5 losers:")
        for t, v in sorted_tickers[-5:]:
            wr = sum(1 for r in v["rets"] if r > 0) / len(v["rets"])
            print(f"    {t:<8} ${v['pnl']:>8,.0f}  N={v['n']:>3}  WR={wr:.0%}")
    results["ticker_concentration"] = ticker_results

    # ═══ ANALYSIS 3: OPTIONS VS SHARES ═══
    print("\n" + "=" * 70)
    print("ANALYSIS 3: OPTIONS VS SHARES COMPARISON")
    print("=" * 70)

    comparison = {}
    for sig in SIGNAL_CONFIGS:
        shares_rets = []
        for ev in events:
            if (ev["_n_insiders"] >= sig["ins"] and ev["_total_value"] >= sig["val"]
                    and ev["_quality_score"] >= sig["qual"]):
                ret = ev["_7d_return"]
                if ret != 0:
                    shares_rets.append(ret)
        shares_stats = calc_stats(shares_rets, [r * NOTIONAL_PER_TRADE for r in shares_rets]) if shares_rets else None

        cfg = TOP_CONFIGS[0]
        trades = filter_trades(all_trades[cfg["label"]], sig, events)
        opt_stats = calc_stats(
            [t["pct_ret"] for t in trades.values()],
            [t["dollar_pnl"] for t in trades.values()]) if trades else None

        comparison[sig["label"]] = {"shares": shares_stats, "options": opt_stats, "config": cfg["label"]}

        print(f"\n{sig['label']}")
        if shares_stats:
            print(f"  SHARES: N={shares_stats['n']}, WR={shares_stats['win_rate']:.1%}, "
                  f"mean={shares_stats['mean_return']*100:.2f}%, Sharpe={shares_stats['sharpe']:.2f}")
        if opt_stats:
            print(f"  OPTIONS: N={opt_stats['n']}, WR={opt_stats['win_rate']:.1%}, "
                  f"mean={opt_stats['mean_return']*100:.1f}%, Sharpe={opt_stats['sharpe']:.2f}, "
                  f"PnL=${opt_stats['total_pnl']:,.0f}")
        if shares_stats and opt_stats:
            print(f"  Delta:   Sharpe {opt_stats['sharpe'] - shares_stats['sharpe']:+.2f}, "
                  f"WR {(opt_stats['win_rate'] - shares_stats['win_rate'])*100:+.1f}pp")
    results["options_vs_shares"] = comparison

    # ═══ ANALYSIS 4: CUMULATIVE PnL & DRAWDOWN ═══
    print("\n" + "=" * 70)
    print("ANALYSIS 4: CUMULATIVE PnL & DRAWDOWN")
    print("=" * 70)

    pnl_curves = {}
    for sig in SIGNAL_CONFIGS[:3]:
        cfg = TOP_CONFIGS[0]
        trades = filter_trades(all_trades[cfg["label"]], sig, events)
        filtered = sorted(trades.values(), key=lambda t: t["entry_date"])
        if not filtered:
            continue
        pnls = [t["dollar_pnl"] for t in filtered]
        dates = [t["entry_date"].isoformat() for t in filtered]
        cum_pnl = np.cumsum(pnls)
        running_max = np.maximum.accumulate(cum_pnl)
        drawdown = cum_pnl - running_max
        max_dd_idx = int(np.argmin(drawdown))
        max_dd = float(drawdown[max_dd_idx])
        peak_idx = int(np.argmax(cum_pnl[:max_dd_idx + 1])) if max_dd_idx > 0 else 0
        recovery_idx = None
        peak_val = float(running_max[max_dd_idx])
        for j in range(max_dd_idx, len(cum_pnl)):
            if cum_pnl[j] >= peak_val:
                recovery_idx = j
                break
        years = set(t["entry_date"].year for t in filtered)
        trades_per_year = len(filtered) / max(len(years), 1)
        month_counts = Counter((t["entry_date"].year, t["entry_date"].month) for t in filtered)
        max_monthly = max(month_counts.values()) if month_counts else 0

        pnl_curves[sig["label"]] = {
            "total_trades": len(filtered), "trades_per_year": round(trades_per_year, 1),
            "total_pnl": round(float(cum_pnl[-1]), 2), "max_drawdown": round(max_dd, 2),
            "drawdown_start": dates[peak_idx], "drawdown_trough": dates[max_dd_idx],
            "drawdown_recovery": dates[recovery_idx] if recovery_idx else "NOT RECOVERED",
            "max_trades_per_month": max_monthly,
            "peak_capital_approx": max_monthly * NOTIONAL_PER_TRADE,
            "calmar_ratio": round(float(cum_pnl[-1]) / abs(max_dd), 2) if max_dd < 0 else float('inf'),
            "curve_dates": dates, "curve_pnl": [round(float(x), 2) for x in cum_pnl],
        }
        print(f"\n{sig['label']} | {cfg['label']}")
        print(f"  Trades: {len(filtered)} ({trades_per_year:.0f}/yr) | Total PnL: ${cum_pnl[-1]:,.0f}")
        print(f"  Max DD: ${max_dd:,.0f} ({dates[peak_idx]} -> {dates[max_dd_idx]})")
        if recovery_idx:
            print(f"  Recovery: {dates[recovery_idx]} ({recovery_idx - max_dd_idx} trades)")
        else:
            print(f"  Recovery: NOT RECOVERED")
        if max_dd < 0:
            print(f"  Calmar: {float(cum_pnl[-1]) / abs(max_dd):.2f}")
        print(f"  Max trades/month: {max_monthly} (~${max_monthly * NOTIONAL_PER_TRADE:,} peak capital)")
    results["cumulative_pnl"] = pnl_curves

    # ═══ ANALYSIS 5: PnL STABILITY ═══
    print("\n" + "=" * 70)
    print("ANALYSIS 5: PnL STABILITY (remove top contributors)")
    print("=" * 70)

    stability = {}
    for sig in SIGNAL_CONFIGS[:3]:
        cfg = TOP_CONFIGS[0]
        trades = filter_trades(all_trades[cfg["label"]], sig, events)
        ticker_pnl = defaultdict(float)
        for t in trades.values():
            ticker_pnl[t["ticker"]] += t["dollar_pnl"]
        sorted_tk = sorted(ticker_pnl.items(), key=lambda x: x[1], reverse=True)

        print(f"\n{sig['label']} | {cfg['label']}")
        print(f"  {'Excluded':<20} {'N':>5} {'WR':>6} {'Sharpe':>7} {'PnL$':>9}")

        excl_results = []
        for n_exclude in [0, 1, 3, 5, 10]:
            excluded = set(t for t, _ in sorted_tk[:n_exclude])
            rets = [t["pct_ret"] for t in trades.values() if t["ticker"] not in excluded]
            pnls = [t["dollar_pnl"] for t in trades.values() if t["ticker"] not in excluded]
            s = calc_stats(rets, pnls)
            if s:
                label = f"Top {n_exclude}" if n_exclude > 0 else "None"
                print(f"  {label:<20} {s['n']:>5} {s['win_rate']:>5.1%} {s['sharpe']:>7.2f} {s['total_pnl']:>9.0f}")
                excl_results.append({"excluded": n_exclude, **s})
        stability[sig["label"]] = excl_results
    results["stability"] = stability

    out_path = os.path.join(SCRIPT_DIR, "data", "options_deep_analysis_v2.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n\nResults saved to {out_path}")
    print("=" * 70)
    print("DEEP ANALYSIS COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    main()
