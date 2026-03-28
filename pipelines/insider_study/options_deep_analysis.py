#!/usr/bin/env python3
"""
Phase 4b: Options Backtest Deep Analysis
-----------------------------------------
Re-runs sweep with tighter spread filter, then performs:
1. Time-period split (2020-2022 vs 2023-2025)
2. Ticker concentration analysis
3. Options vs shares comparison
4. Cumulative PnL charts with drawdown visualization
5. Capital deployment and trade frequency stats

Usage:
    python options_deep_analysis.py --spread-filter 0.10
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from options_backtest import (
    HOLD_DAYS, DTE_TYPES, STRIKE_TYPES, STOP_LOSSES, STRIKES, HOLD_DTE_MAP,
    NOTIONAL_PER_TRADE,
    load_cluster_buy_events, resolve_contract, simulate_trade,
    compute_max_drawdown,
)
from theta_client import CacheDB

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")
CHARTS_DIR = os.path.join(SCRIPT_DIR, "data", "charts")
SHARES_CSV = os.path.join(SCRIPT_DIR, "data", "results_sweep.csv")


# ─────────────────────────────────────────────
# Full sweep with trade-level detail
# ─────────────────────────────────────────────

def run_full_sweep(events, cache, spread_filter):
    """Run sweep returning per-trade details for every config."""
    pricing_modes = ["conservative", "optimistic"]

    # Pre-resolve contracts
    print("Pre-resolving contracts...")
    contracts = {}
    eod_cache = {}
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
                    contracts[key] = contract
                    if contract is not None:
                        ck = contract["cache_key"]
                        if ck not in eod_cache:
                            raw = cache.get(ck)
                            eod_cache[ck] = raw if raw not in (None, "__NONE__") else None
        if (i + 1) % 1000 == 0:
            print(f"  Pre-resolved {i+1}/{len(events)} events...")
    print(f"  Done. {len(eod_cache)} unique EOD series.")

    # Sweep
    print("Running sweep...")
    all_results = {}  # config_key -> list of trade dicts (with event metadata)

    for hold_days in HOLD_DAYS:
        for dte_type in DTE_TYPES:
            for strike_type in STRIKE_TYPES:
                for stop_loss in STOP_LOSSES:
                    for pricing_mode in pricing_modes:
                        config_key = f"{hold_days}d|{dte_type}|{strike_type}|stop={stop_loss}|{pricing_mode}"
                        trades = []

                        for i, ev in enumerate(events):
                            ckey = (i, hold_days, dte_type, strike_type)
                            contract = contracts.get(ckey)
                            if contract is None:
                                continue
                            eod_rows = eod_cache.get(contract["cache_key"])
                            if eod_rows is None:
                                continue

                            result = simulate_trade(
                                eod_rows, ev["_entry_date"], contract["exit_date"],
                                stop_loss, pricing_mode, spread_filter,
                            )
                            if result is None or result.get("skip"):
                                continue

                            trades.append({
                                "ticker": ev["_ticker"],
                                "entry_date": ev["_entry_date"],
                                "exit_date": contract["exit_date"],
                                "entry_px": result["entry_px"],
                                "exit_px": result["exit_px"],
                                "num_contracts": result["num_contracts"],
                                "pct_return": result["pct_return"],
                                "dollar_pnl": result["dollar_pnl"],
                                "exit_reason": result["exit_reason"],
                                "spread_pct": result["spread_pct"],
                                "filing_date": ev.get("filing_date", ""),
                            })

                        all_results[config_key] = trades
    print(f"  Sweep complete: {len(all_results)} configs")
    return all_results


def summarize_config(trades):
    """Compute summary stats for a list of trades."""
    if not trades:
        return {"n_trades": 0, "sharpe": 0, "mean_return": 0, "win_rate": 0,
                "total_dollar_pnl": 0, "max_drawdown": 0, "median_return": 0,
                "avg_spread_pct": 0, "avg_dollar_pnl": 0}
    pct = np.array([t["pct_return"] for t in trades])
    dpnl = [t["dollar_pnl"] for t in trades]
    n = len(pct)
    mean_ret = float(np.mean(pct))
    std_ret = float(np.std(pct, ddof=1)) if n > 1 else 0.0
    return {
        "n_trades": n,
        "win_rate": float(np.sum(pct > 0) / n),
        "mean_return": mean_ret,
        "median_return": float(np.median(pct)),
        "sharpe": (mean_ret / std_ret) * np.sqrt(min(252, n)) if std_ret > 0 else 0.0,
        "total_dollar_pnl": float(np.sum(dpnl)),
        "avg_dollar_pnl": float(np.mean(dpnl)),
        "max_drawdown": compute_max_drawdown(dpnl),
        "avg_spread_pct": float(np.mean([t["spread_pct"] for t in trades])),
    }


# ─────────────────────────────────────────────
# Analysis 1: Time-Period Split
# ─────────────────────────────────────────────

def time_period_analysis(trades, config_key):
    """Split trades into 2020-2022 and 2023-2025."""
    early = [t for t in trades if t["entry_date"] < date(2023, 1, 1)]
    late = [t for t in trades if t["entry_date"] >= date(2023, 1, 1)]

    early_stats = summarize_config(early)
    late_stats = summarize_config(late)

    return {
        "2020-2022": early_stats,
        "2023-2025": late_stats,
        "n_early": len(early),
        "n_late": len(late),
    }


# ─────────────────────────────────────────────
# Analysis 2: Ticker Concentration
# ─────────────────────────────────────────────

def ticker_concentration(trades, config_key):
    """Analyze PnL contribution by ticker."""
    by_ticker = defaultdict(list)
    for t in trades:
        by_ticker[t["ticker"]].append(t)

    ticker_stats = []
    total_pnl = sum(t["dollar_pnl"] for t in trades)
    for ticker, ttrades in by_ticker.items():
        pnl = sum(t["dollar_pnl"] for t in ttrades)
        ticker_stats.append({
            "ticker": ticker,
            "n_trades": len(ttrades),
            "total_pnl": pnl,
            "pct_of_total": pnl / total_pnl if total_pnl != 0 else 0,
            "mean_return": float(np.mean([t["pct_return"] for t in ttrades])),
        })

    ticker_stats.sort(key=lambda x: -x["total_pnl"])
    n_tickers = len(ticker_stats)
    top5_pnl = sum(s["total_pnl"] for s in ticker_stats[:5])
    top10_pnl = sum(s["total_pnl"] for s in ticker_stats[:10])
    bottom5_pnl = sum(s["total_pnl"] for s in ticker_stats[-5:])

    return {
        "n_unique_tickers": n_tickers,
        "top5_pnl": top5_pnl,
        "top5_pct": top5_pnl / total_pnl if total_pnl else 0,
        "top10_pnl": top10_pnl,
        "top10_pct": top10_pnl / total_pnl if total_pnl else 0,
        "bottom5_pnl": bottom5_pnl,
        "top_tickers": ticker_stats[:15],
        "bottom_tickers": ticker_stats[-5:],
    }


# ─────────────────────────────────────────────
# Analysis 3: Options vs Shares
# ─────────────────────────────────────────────

def options_vs_shares(trades, config_key):
    """Compare options PnL to shares for the same events."""
    # Load shares results for matching hold period
    hold_str = config_key.split("|")[0]  # e.g. "7d"
    hold_days = int(hold_str.replace("d", ""))

    if not os.path.exists(SHARES_CSV):
        return {"error": "shares CSV not found"}

    # Load all shares results for matching hold period
    shares_by_key = {}  # (ticker, entry_date) -> shares return
    with open(SHARES_CSV) as f:
        for row in csv.DictReader(f):
            if int(row.get("hold_days", 0)) == hold_days:
                key = (row["ticker"], row["entry_date"])
                shares_by_key[key] = {
                    "return": float(row["trade_return"]) / 100.0,  # CSV is in pct
                    "pnl_per_1k": float(row["trade_return"]) / 100.0 * NOTIONAL_PER_TRADE,
                }

    matched = 0
    options_pnls = []
    shares_pnls = []
    for t in trades:
        key = (t["ticker"], str(t["entry_date"]))
        if key in shares_by_key:
            matched += 1
            options_pnls.append(t["dollar_pnl"])
            shares_pnls.append(shares_by_key[key]["pnl_per_1k"])

    if matched == 0:
        return {"matched": 0, "error": "no matching trades"}

    opt_arr = np.array(options_pnls)
    shr_arr = np.array(shares_pnls)

    opt_sharpe = (np.mean(opt_arr) / np.std(opt_arr, ddof=1)) * np.sqrt(min(252, matched)) if np.std(opt_arr, ddof=1) > 0 else 0
    shr_sharpe = (np.mean(shr_arr) / np.std(shr_arr, ddof=1)) * np.sqrt(min(252, matched)) if np.std(shr_arr, ddof=1) > 0 else 0

    return {
        "matched": matched,
        "options_total_pnl": float(np.sum(opt_arr)),
        "shares_total_pnl": float(np.sum(shr_arr)),
        "options_mean_pnl": float(np.mean(opt_arr)),
        "shares_mean_pnl": float(np.mean(shr_arr)),
        "options_sharpe": float(opt_sharpe),
        "shares_sharpe": float(shr_sharpe),
        "options_win_rate": float(np.sum(opt_arr > 0) / matched),
        "shares_win_rate": float(np.sum(shr_arr > 0) / matched),
        "options_max_dd": compute_max_drawdown(options_pnls),
        "shares_max_dd": compute_max_drawdown(shares_pnls),
    }


# ─────────────────────────────────────────────
# Analysis 4: Capital & Frequency Stats
# ─────────────────────────────────────────────

def capital_deployment_stats(trades):
    """Compute trades/year, max concurrent capital, etc."""
    if not trades:
        return {}

    # Sort by entry date
    sorted_trades = sorted(trades, key=lambda t: t["entry_date"])
    first_date = sorted_trades[0]["entry_date"]
    last_date = sorted_trades[-1]["entry_date"]
    span_days = (last_date - first_date).days
    span_years = span_days / 365.25 if span_days > 0 else 1.0

    trades_per_year = len(trades) / span_years

    # Max concurrent capital deployed
    # Track open positions by date
    events = []
    for t in sorted_trades:
        cost = t["num_contracts"] * t["entry_px"] * 100
        events.append((t["entry_date"], cost))
        events.append((t["exit_date"], -cost))

    events.sort(key=lambda x: x[0])
    running = 0.0
    max_capital = 0.0
    daily_capital = []
    for dt, amt in events:
        running += amt
        if running > max_capital:
            max_capital = running
        daily_capital.append((dt, running))

    # Avg capital deployed
    if daily_capital:
        avg_capital = np.mean([c for _, c in daily_capital])
    else:
        avg_capital = 0

    # Trades by year
    by_year = defaultdict(int)
    for t in sorted_trades:
        by_year[t["entry_date"].year] += 1

    return {
        "total_trades": len(trades),
        "span_years": round(span_years, 1),
        "trades_per_year": round(trades_per_year, 1),
        "max_concurrent_capital": round(max_capital, 2),
        "avg_capital_deployed": round(float(avg_capital), 2),
        "trades_by_year": dict(by_year),
        "first_date": str(first_date),
        "last_date": str(last_date),
    }


# ─────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────

def plot_cumulative_pnl(trades, config_key, pricing_mode_label, output_path):
    """Plot cumulative PnL with drawdown shading."""
    if not trades:
        return

    sorted_trades = sorted(trades, key=lambda t: t["entry_date"])
    dates = [t["entry_date"] for t in sorted_trades]
    pnls = [t["dollar_pnl"] for t in sorted_trades]
    cum_pnl = np.cumsum(pnls)
    peak = np.maximum.accumulate(cum_pnl)
    drawdown = cum_pnl - peak

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[3, 1],
                                     sharex=True, gridspec_kw={"hspace": 0.05})

    # Cumulative PnL
    ax1.plot(dates, cum_pnl, color="#2196F3", linewidth=1.5, label="Cumulative PnL")
    ax1.fill_between(dates, 0, cum_pnl, alpha=0.1, color="#2196F3")
    ax1.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")
    ax1.set_ylabel("Cumulative PnL ($)", fontsize=11)
    ax1.set_title(f"Options Backtest — {config_key}\n({pricing_mode_label} pricing, $1K notional/trade)",
                  fontsize=13, fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    # Add annotation for final PnL
    final_pnl = cum_pnl[-1]
    ax1.annotate(f"${final_pnl:,.0f}", xy=(dates[-1], final_pnl),
                 fontsize=10, fontweight="bold", color="#2196F3",
                 ha="right", va="bottom")

    # Drawdown
    ax2.fill_between(dates, 0, drawdown, color="#F44336", alpha=0.4, label="Drawdown")
    ax2.set_ylabel("Drawdown ($)", fontsize=11)
    ax2.set_xlabel("Date", fontsize=11)
    ax2.legend(loc="lower left")
    ax2.grid(True, alpha=0.3)

    # Format x-axis
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {output_path}")


def plot_conservative_vs_optimistic(cons_trades, opt_trades, config_base, output_path):
    """Overlay conservative and optimistic cumulative PnL."""
    fig, ax = plt.subplots(figsize=(14, 6))

    for trades, label, color in [
        (opt_trades, "Optimistic (midpoint)", "#4CAF50"),
        (cons_trades, "Conservative (ask/bid)", "#F44336"),
    ]:
        if not trades:
            continue
        sorted_t = sorted(trades, key=lambda t: t["entry_date"])
        dates = [t["entry_date"] for t in sorted_t]
        cum = np.cumsum([t["dollar_pnl"] for t in sorted_t])
        ax.plot(dates, cum, color=color, linewidth=1.5, label=f"{label} (${cum[-1]:,.0f})")

    ax.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")
    ax.set_ylabel("Cumulative PnL ($)", fontsize=11)
    ax.set_title(f"Conservative vs Optimistic — {config_base}\n($1K notional/trade)",
                 fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {output_path}")


def plot_top5_comparison(all_results, output_path):
    """Plot cumulative PnL for top 5 conservative configs overlaid."""
    # Rank by conservative Sharpe
    cons_configs = {k: v for k, v in all_results.items() if "conservative" in k and len(v) > 0}
    ranked = sorted(cons_configs.items(), key=lambda x: -summarize_config(x[1])["sharpe"])[:5]

    fig, ax = plt.subplots(figsize=(14, 7))
    colors = ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#F44336"]

    for idx, (config_key, trades) in enumerate(ranked):
        sorted_t = sorted(trades, key=lambda t: t["entry_date"])
        dates = [t["entry_date"] for t in sorted_t]
        cum = np.cumsum([t["dollar_pnl"] for t in sorted_t])
        stats = summarize_config(trades)
        short_key = config_key.replace("|conservative", "").replace("|", " / ")
        ax.plot(dates, cum, color=colors[idx], linewidth=1.5,
                label=f"{short_key} (Sharpe={stats['sharpe']:.2f}, ${cum[-1]:,.0f})")

    ax.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")
    ax.set_ylabel("Cumulative PnL ($)", fontsize=11)
    ax.set_title("Top 5 Conservative Configs — Cumulative PnL\n($1K notional/trade, 10% spread filter)",
                 fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved: {output_path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Phase 4b: Deep Options Analysis")
    parser.add_argument("--spread-filter", type=float, default=0.10,
                        help="Max spread %% (default: 10%%)")
    parser.add_argument("--top", type=int, default=10,
                        help="Analyze top N configs (default: 10)")
    args = parser.parse_args()

    os.makedirs(CHARTS_DIR, exist_ok=True)

    print("=" * 60)
    print("Phase 4b: Options Deep Analysis")
    print(f"  Spread filter: {args.spread_filter:.0%}")
    print("=" * 60)

    # Load events and cache
    events = load_cluster_buy_events()
    print(f"Loaded {len(events)} cluster buy events")
    cache = CacheDB()

    # Run full sweep
    all_results = run_full_sweep(events, cache, args.spread_filter)
    cache.close()

    # Build summary for all configs and write sweep CSV
    print("\nBuilding summaries...")
    summaries = []
    for config_key, trades in all_results.items():
        parts = config_key.split("|")
        stats = summarize_config(trades)
        stats["config_key"] = config_key
        stats["hold_days"] = int(parts[0].replace("d", ""))
        stats["dte_type"] = parts[1]
        stats["strike_type"] = parts[2]
        stats["stop_loss"] = parts[3].replace("stop=", "")
        stats["pricing_mode"] = parts[4]
        summaries.append(stats)

    summaries.sort(key=lambda s: -s["sharpe"])

    # Write sweep CSV
    sweep_path = os.path.join(OUTPUT_DIR, "sweep_options_buys_10pct.csv")
    fieldnames = ["config_key", "hold_days", "dte_type", "strike_type", "stop_loss",
                  "pricing_mode", "n_trades", "win_rate", "mean_return", "median_return",
                  "sharpe", "total_dollar_pnl", "avg_dollar_pnl", "avg_spread_pct", "max_drawdown"]
    with open(sweep_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summaries)
    print(f"Sweep CSV: {sweep_path}")

    # Identify top conservative and optimistic configs
    cons_ranked = [s for s in summaries if s["pricing_mode"] == "conservative" and s["n_trades"] >= 20]
    opt_ranked = [s for s in summaries if s["pricing_mode"] == "optimistic" and s["n_trades"] >= 20]
    cons_ranked.sort(key=lambda s: -s["sharpe"])
    opt_ranked.sort(key=lambda s: -s["sharpe"])

    print(f"\nTop conservative configs (>= 20 trades):")
    for i, s in enumerate(cons_ranked[:10]):
        print(f"  {i+1}. {s['config_key']} | N={s['n_trades']} WR={s['win_rate']:.1%} "
              f"Sharpe={s['sharpe']:.2f} PnL=${s['total_dollar_pnl']:,.0f}")

    # Deep analysis on top 5 conservative configs
    top5_cons = cons_ranked[:5]
    analysis_output = {
        "spread_filter": args.spread_filter,
        "total_configs": len(summaries),
        "configs_with_trades": sum(1 for s in summaries if s["n_trades"] > 0),
        "positive_sharpe": sum(1 for s in summaries if s["sharpe"] > 0),
    }

    # Pricing comparison
    cons_all = [s for s in summaries if s["pricing_mode"] == "conservative"]
    opt_all = [s for s in summaries if s["pricing_mode"] == "optimistic"]
    analysis_output["cons_avg_sharpe"] = float(np.mean([s["sharpe"] for s in cons_all])) if cons_all else 0
    analysis_output["opt_avg_sharpe"] = float(np.mean([s["sharpe"] for s in opt_all])) if opt_all else 0
    analysis_output["cons_positive"] = sum(1 for s in cons_all if s["sharpe"] > 0)
    analysis_output["opt_positive"] = sum(1 for s in opt_all if s["sharpe"] > 0)

    # Per hold period
    hold_stats = {}
    for hold in HOLD_DAYS:
        subset = [s for s in summaries if s["hold_days"] == hold]
        hold_stats[hold] = {
            "positive_sharpe": sum(1 for s in subset if s["sharpe"] > 0),
            "total": len(subset),
            "avg_sharpe": float(np.mean([s["sharpe"] for s in subset])),
            "avg_n": float(np.mean([s["n_trades"] for s in subset])),
            "best_sharpe": max(s["sharpe"] for s in subset) if subset else 0,
        }
    analysis_output["by_hold_period"] = hold_stats

    # Deep dive on each top config
    top_analyses = []
    for i, s in enumerate(top5_cons):
        config_key = s["config_key"]
        trades = all_results[config_key]
        print(f"\n--- Analyzing #{i+1}: {config_key} ---")

        # Time period split
        tp = time_period_analysis(trades, config_key)
        print(f"  Time split: 2020-22 N={tp['n_early']} Sharpe={tp['2020-2022']['sharpe']:.2f} | "
              f"2023-25 N={tp['n_late']} Sharpe={tp['2023-2025']['sharpe']:.2f}")

        # Ticker concentration
        tc = ticker_concentration(trades, config_key)
        print(f"  Tickers: {tc['n_unique_tickers']} unique, top5={tc['top5_pct']:.1%} of PnL")

        # Options vs shares
        ovs = options_vs_shares(trades, config_key)
        if ovs.get("matched", 0) > 0:
            print(f"  Options vs Shares ({ovs['matched']} matched): "
                  f"Opt Sharpe={ovs['options_sharpe']:.2f} Shr Sharpe={ovs['shares_sharpe']:.2f}")

        # Capital stats
        cap = capital_deployment_stats(trades)
        print(f"  Capital: {cap['trades_per_year']} trades/yr, "
              f"max deployed=${cap['max_concurrent_capital']:,.0f}")

        # Charts
        plot_cumulative_pnl(
            trades, config_key, "conservative",
            os.path.join(CHARTS_DIR, f"cum_pnl_cons_{i+1}.png"),
        )

        # Also get optimistic counterpart
        opt_key = config_key.replace("conservative", "optimistic")
        opt_trades = all_results.get(opt_key, [])
        if opt_trades:
            config_base = config_key.replace("|conservative", "")
            plot_conservative_vs_optimistic(
                trades, opt_trades, config_base,
                os.path.join(CHARTS_DIR, f"cons_vs_opt_{i+1}.png"),
            )

        top_analyses.append({
            "rank": i + 1,
            "config": s,
            "time_period": tp,
            "ticker_concentration": tc,
            "options_vs_shares": ovs,
            "capital_stats": cap,
        })

    analysis_output["top_analyses"] = top_analyses

    # Top 5 overlay chart
    plot_top5_comparison(all_results, os.path.join(CHARTS_DIR, "top5_conservative_overlay.png"))

    # Save full analysis JSON
    json_path = os.path.join(OUTPUT_DIR, "options_deep_analysis.json")
    with open(json_path, "w") as f:
        json.dump(analysis_output, f, indent=2, default=str)
    print(f"\nFull analysis JSON: {json_path}")
    print("Charts directory:", CHARTS_DIR)

    # Print summary table
    print("\n" + "=" * 100)
    print("TOP 5 CONSERVATIVE CONFIGS — DEEP ANALYSIS SUMMARY")
    print("=" * 100)
    for a in top_analyses:
        s = a["config"]
        tp = a["time_period"]
        tc = a["ticker_concentration"]
        cap = a["capital_stats"]
        ovs = a["options_vs_shares"]
        print(f"\n#{a['rank']}: {s['config_key']}")
        print(f"  Overall: N={s['n_trades']} WR={s['win_rate']:.1%} Sharpe={s['sharpe']:.2f} "
              f"PnL=${s['total_dollar_pnl']:,.0f} MaxDD=${s['max_drawdown']:,.0f}")
        print(f"  Period:  2020-22 Sharpe={tp['2020-2022']['sharpe']:.2f} (N={tp['n_early']}) | "
              f"2023-25 Sharpe={tp['2023-2025']['sharpe']:.2f} (N={tp['n_late']})")
        print(f"  Tickers: {tc['n_unique_tickers']} unique | Top5={tc['top5_pct']:.1%} | Top10={tc['top10_pct']:.1%}")
        print(f"  Capital: {cap['trades_per_year']} trades/yr | Max deployed=${cap['max_concurrent_capital']:,.0f}")
        if ovs.get("matched", 0) > 0:
            print(f"  Opt vs Shares: Opt Sharpe={ovs['options_sharpe']:.2f} vs Shr={ovs['shares_sharpe']:.2f} "
                  f"| Opt PnL=${ovs['options_total_pnl']:,.0f} vs Shr=${ovs['shares_total_pnl']:,.0f}")


if __name__ == "__main__":
    main()
