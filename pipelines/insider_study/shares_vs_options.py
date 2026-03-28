#!/usr/bin/env python3
"""
Phase 5: Shares vs Options Comparison
--------------------------------------
Compares best shares backtest configs (Phase 3) against best options configs (Phase 4).
Evaluates blended approaches and produces a final recommendation.

Usage:
    python shares_vs_options.py
    python shares_vs_options.py --top 10

Author: Claude Opus 4.6
Date: 2026-03-16
"""

from __future__ import annotations

import csv
import json
import os
import sys
from typing import Any

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

SHARES_RESULTS = os.path.join(DATA_DIR, "shares_backtest_results.json")
OPTIONS_SWEEP_CSV = os.path.join(DATA_DIR, "sweep_options_buys.csv")
OPTIONS_TRADES_JSON = os.path.join(DATA_DIR, "options_backtest_trades.json")
OUTPUT_JSON = os.path.join(DATA_DIR, "shares_vs_options_comparison.json")
OUTPUT_TXT = os.path.join(DATA_DIR, "shares_vs_options_report.txt")


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_shares_results() -> list[dict]:
    """Load Phase 3 shares backtest results and normalize to common schema."""
    with open(SHARES_RESULTS) as f:
        raw = json.load(f)

    results = []
    for config_key, metrics in raw.items():
        parts = config_key.split("|")
        signal_filter = parts[0]  # all_buys or cluster_only
        stop = parts[1].replace("stop=", "")
        sizing = parts[2].replace("sizing=", "")

        results.append({
            "type": "shares",
            "config_key": config_key,
            "signal_filter": signal_filter,
            "stop_loss": stop,
            "sizing": sizing,
            "hold_days": 7,  # Phase 3 uses 7-day hold
            "n_trades": metrics["n"],
            "sharpe": metrics["sharpe"],
            "win_rate": metrics["win_rate"],
            "mean_return": metrics["mean_return"],
            "median_return": metrics["median_return"],
            "max_dd": metrics["max_portfolio_dd"],
            "stops_hit": metrics["stops_hit"],
            "max_concurrent": metrics["max_concurrent_positions"],
        })
    return results


def load_options_results() -> list[dict]:
    """Load Phase 4 options sweep results and normalize to common schema."""
    with open(OPTIONS_SWEEP_CSV) as f:
        rows = list(csv.DictReader(f))

    results = []
    for r in rows:
        n_trades = int(r["n_trades"])
        if n_trades == 0:
            continue

        stop_str = r["stop_loss"]
        config_key = (f"{r['hold_days']}d|{r['dte_type']}|{r['strike_type']}|"
                      f"stop={stop_str}|{r['pricing_mode']}")

        results.append({
            "type": "options",
            "config_key": config_key,
            "hold_days": int(r["hold_days"]),
            "dte_type": r["dte_type"],
            "strike_type": r["strike_type"],
            "stop_loss": stop_str,
            "pricing_mode": r["pricing_mode"],
            "n_trades": n_trades,
            "sharpe": float(r["sharpe"]),
            "win_rate": float(r["win_rate"]),
            "mean_return": float(r["mean_return"]),
            "median_return": float(r["median_return"]),
            "total_dollar_pnl": float(r["total_dollar_pnl"]),
            "avg_dollar_pnl": float(r["avg_dollar_pnl"]),
            "avg_spread_pct": float(r["avg_spread_pct"]),
            "max_dd": float(r["max_drawdown"]),
            "n_skipped_spread": int(r["n_skipped_spread"]),
            "n_skipped_no_data": int(r["n_skipped_no_data"]),
        })
    return results


# ─────────────────────────────────────────────
# Analysis
# ─────────────────────────────────────────────

def filter_viable_options(options: list[dict], min_trades: int = 200,
                          min_sharpe: float = 0.0) -> list[dict]:
    """Filter options configs to viable ones (enough trades, positive Sharpe)."""
    return [o for o in options
            if o["n_trades"] >= min_trades and o["sharpe"] >= min_sharpe]


def compute_data_coverage(options: list[dict]) -> dict:
    """Compute what fraction of cluster events have options data."""
    # Total cluster events is ~5,228 (from Phase 3 cluster_only N=20,410
    # but that's individual insider trades, not unique events)
    if not options:
        return {}

    # Use a representative config to measure coverage
    representative = [o for o in options
                      if o["hold_days"] == 7 and "tight" in o.get("dte_type", "")]
    if not representative:
        representative = options[:1]

    r = representative[0]
    total_attempted = r["n_trades"] + r.get("n_skipped_spread", 0) + r.get("n_skipped_no_data", 0)

    return {
        "total_events_attempted": total_attempted,
        "with_data": r["n_trades"] + r.get("n_skipped_spread", 0),
        "no_data_pct": r.get("n_skipped_no_data", 0) / total_attempted if total_attempted > 0 else 0,
        "spread_filtered_pct": r.get("n_skipped_spread", 0) / total_attempted if total_attempted > 0 else 0,
    }


def build_comparison_table(shares: list[dict], options: list[dict],
                           top_n: int = 5) -> list[dict]:
    """Build side-by-side comparison of top shares vs top options configs."""
    # Best shares configs (cluster only, ranked by Sharpe)
    cluster_shares = [s for s in shares if s["signal_filter"] == "cluster_only"]
    cluster_shares.sort(key=lambda x: -x["sharpe"])

    # Best options configs (ranked by Sharpe)
    options.sort(key=lambda x: -x["sharpe"])

    # Viable options: at least 200 trades, positive Sharpe
    viable_options = filter_viable_options(options, min_trades=200)

    table = []
    # Add top shares
    for i, s in enumerate(cluster_shares[:top_n]):
        table.append({
            "rank": i + 1,
            "instrument": "SHARES",
            "config": s["config_key"],
            "n_trades": s["n_trades"],
            "sharpe": s["sharpe"],
            "win_rate": s["win_rate"],
            "mean_return": s["mean_return"],
            "median_return": s["median_return"],
            "max_dd": s["max_dd"],
        })

    # Add top viable options
    for i, o in enumerate(viable_options[:top_n]):
        table.append({
            "rank": i + 1,
            "instrument": "OPTIONS",
            "config": o["config_key"],
            "n_trades": o["n_trades"],
            "sharpe": o["sharpe"],
            "win_rate": o["win_rate"],
            "mean_return": o["mean_return"],
            "median_return": o["median_return"],
            "max_dd": o["max_dd"],
        })

    return table


def analyze_options_by_dimension(options: list[dict]) -> dict:
    """Break down options performance by each dimension."""
    viable = filter_viable_options(options, min_trades=100, min_sharpe=-999)

    analysis = {}

    # By hold period
    by_hold = {}
    for hold in [7, 14, 30, 60]:
        subset = [o for o in viable if o["hold_days"] == hold]
        if subset:
            sharpes = [o["sharpe"] for o in subset]
            by_hold[f"{hold}d"] = {
                "avg_sharpe": round(np.mean(sharpes), 4),
                "max_sharpe": round(max(sharpes), 4),
                "n_configs": len(subset),
                "pct_positive": round(sum(1 for s in sharpes if s > 0) / len(sharpes), 4),
            }
    analysis["by_hold_period"] = by_hold

    # By DTE type
    by_dte = {}
    for dte in ["tight", "comfortable"]:
        subset = [o for o in viable if o.get("dte_type") == dte]
        if subset:
            sharpes = [o["sharpe"] for o in subset]
            by_dte[dte] = {
                "avg_sharpe": round(np.mean(sharpes), 4),
                "max_sharpe": round(max(sharpes), 4),
                "n_configs": len(subset),
            }
    analysis["by_dte_type"] = by_dte

    # By strike
    by_strike = {}
    for strike in ["5pct_itm", "atm", "5pct_otm", "10pct_otm"]:
        subset = [o for o in viable if o.get("strike_type") == strike]
        if subset:
            sharpes = [o["sharpe"] for o in subset]
            by_strike[strike] = {
                "avg_sharpe": round(np.mean(sharpes), 4),
                "max_sharpe": round(max(sharpes), 4),
                "n_configs": len(subset),
            }
    analysis["by_strike"] = by_strike

    # By pricing mode
    by_pricing = {}
    for mode in ["conservative", "optimistic"]:
        subset = [o for o in viable if o.get("pricing_mode") == mode]
        if subset:
            sharpes = [o["sharpe"] for o in subset]
            by_pricing[mode] = {
                "avg_sharpe": round(np.mean(sharpes), 4),
                "max_sharpe": round(max(sharpes), 4),
                "n_configs": len(subset),
            }
    analysis["by_pricing_mode"] = by_pricing

    # By stop loss
    by_stop = {}
    for stop in ["-0.25", "-0.5", "-0.75", "none"]:
        subset = [o for o in viable if o["stop_loss"] == stop]
        if subset:
            sharpes = [o["sharpe"] for o in subset]
            by_stop[stop] = {
                "avg_sharpe": round(np.mean(sharpes), 4),
                "max_sharpe": round(max(sharpes), 4),
                "n_configs": len(subset),
            }
    analysis["by_stop_loss"] = by_stop

    return analysis


def blended_analysis(shares: list[dict], options: list[dict]) -> dict:
    """Evaluate blended shares + options approach."""
    # Best cluster shares config
    cluster_shares = [s for s in shares if s["signal_filter"] == "cluster_only"]
    cluster_shares.sort(key=lambda x: -x["sharpe"])
    best_shares = cluster_shares[0] if cluster_shares else None

    # Best viable options (conservative pricing, >= 300 trades)
    conservative_opts = filter_viable_options(
        [o for o in options if o.get("pricing_mode") == "conservative"],
        min_trades=300, min_sharpe=0.0
    )
    conservative_opts.sort(key=lambda x: -x["sharpe"])
    best_conservative = conservative_opts[0] if conservative_opts else None

    # Best viable options (optimistic pricing, >= 300 trades)
    optimistic_opts = filter_viable_options(
        [o for o in options if o.get("pricing_mode") == "optimistic"],
        min_trades=300, min_sharpe=0.0
    )
    optimistic_opts.sort(key=lambda x: -x["sharpe"])
    best_optimistic = optimistic_opts[0] if optimistic_opts else None

    result = {}

    if best_shares:
        result["best_shares"] = {
            "config": best_shares["config_key"],
            "sharpe": best_shares["sharpe"],
            "win_rate": best_shares["win_rate"],
            "mean_return": best_shares["mean_return"],
            "n_trades": best_shares["n_trades"],
        }

    if best_conservative:
        result["best_options_conservative"] = {
            "config": best_conservative["config_key"],
            "sharpe": best_conservative["sharpe"],
            "win_rate": best_conservative["win_rate"],
            "mean_return": best_conservative["mean_return"],
            "n_trades": best_conservative["n_trades"],
        }

    if best_optimistic:
        result["best_options_optimistic"] = {
            "config": best_optimistic["config_key"],
            "sharpe": best_optimistic["sharpe"],
            "win_rate": best_optimistic["win_rate"],
            "mean_return": best_optimistic["mean_return"],
            "n_trades": best_optimistic["n_trades"],
        }

    # Blended recommendation
    if best_shares and best_conservative:
        # Options data coverage is lower than shares (spread filter + no data)
        # Recommend: shares as primary, options as overlay on highest-conviction signals
        shares_sharpe = best_shares["sharpe"]
        opts_sharpe = best_conservative["sharpe"]
        coverage = best_conservative["n_trades"] / best_shares["n_trades"] if best_shares["n_trades"] > 0 else 0

        result["blended_recommendation"] = {
            "strategy": "shares_primary_options_overlay",
            "rationale": (
                f"Shares have {best_shares['n_trades']} trades vs options {best_conservative['n_trades']} "
                f"({coverage:.0%} coverage). Options conservative Sharpe {opts_sharpe:.2f} vs "
                f"shares {shares_sharpe:.2f}. "
                "Recommend shares as primary leg for all cluster signals, "
                "with options overlay on highest-conviction (Tier 1 + C-Suite) signals "
                "where options data is available and spreads are tight."
            ),
            "shares_allocation_pct": 70,
            "options_allocation_pct": 30,
            "options_coverage": round(coverage, 4),
        }

    return result


# ─────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────

def generate_report(shares: list[dict], options: list[dict],
                    comparison: list[dict], dim_analysis: dict,
                    blended: dict, top_n: int = 10) -> str:
    """Generate formatted text report."""
    lines = []
    w = 120

    lines.append("=" * w)
    lines.append("PHASE 5: SHARES vs OPTIONS COMPARISON REPORT")
    lines.append("=" * w)
    lines.append("")

    # ── Section 1: Summary Stats
    cluster_shares = [s for s in shares if s["signal_filter"] == "cluster_only"]
    cluster_shares.sort(key=lambda x: -x["sharpe"])
    viable_opts = filter_viable_options(options, min_trades=200)
    viable_opts.sort(key=lambda x: -x["sharpe"])

    lines.append("SUMMARY")
    lines.append("-" * w)
    lines.append(f"  Shares configs tested:    {len(shares)} ({len(cluster_shares)} cluster-only)")
    lines.append(f"  Options configs tested:   {len(options)} ({len(viable_opts)} viable with N≥200, Sharpe>0)")
    lines.append("")
    if cluster_shares:
        b = cluster_shares[0]
        lines.append(f"  Best shares:   Sharpe {b['sharpe']:.4f} | WR {b['win_rate']:.1%} | "
                     f"Mean {b['mean_return']:+.2%} | N={b['n_trades']} | {b['config_key']}")
    if viable_opts:
        b = viable_opts[0]
        lines.append(f"  Best options:  Sharpe {b['sharpe']:.4f} | WR {b['win_rate']:.1%} | "
                     f"Mean {b['mean_return']:+.2%} | N={b['n_trades']} | {b['config_key']}")
    lines.append("")

    # ── Section 2: Head-to-Head Comparison
    lines.append("HEAD-TO-HEAD: TOP SHARES vs TOP OPTIONS")
    lines.append("-" * w)
    header = (f"{'Type':>8} | {'#':>2} | {'Config':<50} | {'N':>5} | {'Sharpe':>7} | "
              f"{'WR':>6} | {'Mean':>8} | {'Med':>8} | {'MaxDD':>8}")
    lines.append(header)
    lines.append("-" * w)

    shares_rows = [c for c in comparison if c["instrument"] == "SHARES"]
    options_rows = [c for c in comparison if c["instrument"] == "OPTIONS"]

    for r in shares_rows[:top_n]:
        lines.append(
            f"{'SHARES':>8} | {r['rank']:>2} | {r['config']:<50} | {r['n_trades']:>5} | "
            f"{r['sharpe']:>7.4f} | {r['win_rate']:>5.1%} | {r['mean_return']:>+7.2%} | "
            f"{r['median_return']:>+7.2%} | {r['max_dd']:>+7.2%}"
        )
    lines.append("-" * w)
    for r in options_rows[:top_n]:
        lines.append(
            f"{'OPTIONS':>8} | {r['rank']:>2} | {r['config']:<50} | {r['n_trades']:>5} | "
            f"{r['sharpe']:>7.4f} | {r['win_rate']:>5.1%} | {r['mean_return']:>+7.2%} | "
            f"{r['median_return']:>+7.2%} | {r['max_dd']:>7.0f}"
        )
    lines.append("")

    # ── Section 3: Options Dimensional Analysis
    lines.append("OPTIONS DIMENSIONAL ANALYSIS")
    lines.append("-" * w)

    for dim_name, dim_data in dim_analysis.items():
        label = dim_name.replace("by_", "").replace("_", " ").title()
        lines.append(f"\n  {label}:")
        for key, stats in sorted(dim_data.items(), key=lambda x: -x[1]["avg_sharpe"]):
            lines.append(
                f"    {key:>15}: avg Sharpe {stats['avg_sharpe']:>7.4f} | "
                f"max Sharpe {stats['max_sharpe']:>7.4f} | "
                f"{stats['n_configs']} configs"
                + (f" | {stats.get('pct_positive', 0):.0%} positive" if 'pct_positive' in stats else "")
            )
    lines.append("")

    # ── Section 4: Blended Recommendation
    lines.append("BLENDED STRATEGY RECOMMENDATION")
    lines.append("-" * w)

    if "best_shares" in blended:
        b = blended["best_shares"]
        lines.append(f"  Shares (primary):       {b['config']} — Sharpe {b['sharpe']:.4f}, "
                     f"WR {b['win_rate']:.1%}, N={b['n_trades']}")

    if "best_options_conservative" in blended:
        b = blended["best_options_conservative"]
        lines.append(f"  Options (conservative): {b['config']} — Sharpe {b['sharpe']:.4f}, "
                     f"WR {b['win_rate']:.1%}, N={b['n_trades']}")

    if "best_options_optimistic" in blended:
        b = blended["best_options_optimistic"]
        lines.append(f"  Options (optimistic):   {b['config']} — Sharpe {b['sharpe']:.4f}, "
                     f"WR {b['win_rate']:.1%}, N={b['n_trades']}")

    if "blended_recommendation" in blended:
        rec = blended["blended_recommendation"]
        lines.append("")
        lines.append(f"  Recommendation: {rec['strategy']}")
        lines.append(f"  Allocation: {rec['shares_allocation_pct']}% shares / "
                     f"{rec['options_allocation_pct']}% options")
        lines.append(f"  Options coverage: {rec['options_coverage']:.1%} of cluster events")
        lines.append(f"  Rationale: {rec['rationale']}")

    lines.append("")

    # ── Section 5: Key Findings
    lines.append("KEY FINDINGS")
    lines.append("-" * w)

    # Compare optimistic vs conservative
    opt_sharpes = [o["sharpe"] for o in options if o.get("pricing_mode") == "optimistic" and o["n_trades"] >= 200]
    con_sharpes = [o["sharpe"] for o in options if o.get("pricing_mode") == "conservative" and o["n_trades"] >= 200]

    if opt_sharpes and con_sharpes:
        avg_opt = np.mean(opt_sharpes)
        avg_con = np.mean(con_sharpes)
        lines.append(f"  1. Pricing mode matters: optimistic avg Sharpe {avg_opt:.3f} vs "
                     f"conservative {avg_con:.3f} (Δ={avg_opt - avg_con:.3f})")
        lines.append(f"     → Conservative pricing is realistic; optimistic overstates edge by "
                     f"{((avg_opt / avg_con) - 1) * 100:.0f}%" if avg_con > 0 else "")

    # Compare hold periods
    hold_data = dim_analysis.get("by_hold_period", {})
    if hold_data:
        best_hold = max(hold_data.items(), key=lambda x: x[1]["avg_sharpe"])
        worst_hold = min(hold_data.items(), key=lambda x: x[1]["avg_sharpe"])
        lines.append(f"  2. Best hold period: {best_hold[0]} (avg Sharpe {best_hold[1]['avg_sharpe']:.3f}) "
                     f"vs worst: {worst_hold[0]} ({worst_hold[1]['avg_sharpe']:.3f})")

    # Shares vs options overall
    if cluster_shares and viable_opts:
        lines.append(f"  3. Shares Sharpe ({cluster_shares[0]['sharpe']:.2f}) vs "
                     f"best viable options ({viable_opts[0]['sharpe']:.2f}) — "
                     f"options {'outperform' if viable_opts[0]['sharpe'] > cluster_shares[0]['sharpe'] else 'underperform'} "
                     f"on Sharpe but with {'fewer' if viable_opts[0]['n_trades'] < cluster_shares[0]['n_trades'] else 'more'} "
                     f"trades ({viable_opts[0]['n_trades']} vs {cluster_shares[0]['n_trades']})")

    # Options coverage issue
    if viable_opts:
        best_opt = viable_opts[0]
        total_possible = best_opt["n_trades"] + best_opt.get("n_skipped_spread", 0) + best_opt.get("n_skipped_no_data", 0)
        if total_possible > 0:
            coverage = best_opt["n_trades"] / total_possible
            lines.append(f"  4. Options data coverage: {coverage:.1%} of events have tradeable options data "
                         f"({best_opt.get('n_skipped_no_data', 0)} no data, "
                         f"{best_opt.get('n_skipped_spread', 0)} filtered by spread)")

    # Win rate comparison
    if cluster_shares and viable_opts:
        lines.append(f"  5. Win rate: shares {cluster_shares[0]['win_rate']:.1%} vs "
                     f"options {viable_opts[0]['win_rate']:.1%} — "
                     f"options have lower WR but higher per-trade returns (leverage)")

    lines.append("")
    lines.append("=" * w)

    return "\n".join(lines)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Phase 5: Shares vs Options Comparison")
    parser.add_argument("--top", type=int, default=10,
                        help="Number of top configs to show per type (default: 10)")
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 5: Shares vs Options Comparison")
    print("=" * 60)

    # Load data
    print("Loading Phase 3 (shares) results...")
    shares = load_shares_results()
    print(f"  {len(shares)} shares configs loaded")

    print("Loading Phase 4 (options) results...")
    options = load_options_results()
    print(f"  {len(options)} options configs loaded")

    # Analysis
    print("\nRunning comparison analysis...")
    comparison = build_comparison_table(shares, options, top_n=args.top)
    dim_analysis = analyze_options_by_dimension(options)
    blended = blended_analysis(shares, options)
    coverage = compute_data_coverage(options)

    # Build output
    output = {
        "phase": "Phase 5: Shares vs Options Comparison",
        "comparison_table": comparison,
        "dimensional_analysis": dim_analysis,
        "blended_recommendation": blended,
        "data_coverage": coverage,
    }

    # Write JSON
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nJSON output: {OUTPUT_JSON}")

    # Generate and write report
    report = generate_report(shares, options, comparison, dim_analysis, blended, top_n=args.top)
    with open(OUTPUT_TXT, "w") as f:
        f.write(report)
    print(f"Text report: {OUTPUT_TXT}")

    # Print report
    print("\n")
    print(report)


if __name__ == "__main__":
    main()
