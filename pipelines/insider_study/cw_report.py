#!/usr/bin/env python3
"""
Generate a comparison report from CW simulation grid results.

Reads grid_results.csv produced by cw_simulation.py and generates a structured
Markdown report comparing strategies across instruments, thesis types, exit
strategies, hold times, and score filters.

Usage:
    python3 pipelines/insider_study/cw_report.py
    python3 pipelines/insider_study/cw_report.py --input reports/cw_simulation/grid_results.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports" / "cw_simulation"
DEFAULT_INPUT = REPORTS_DIR / "grid_results.csv"
DEFAULT_OUTPUT = REPORTS_DIR / "COMPARISON_REPORT.md"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_grid(path: Path) -> list[dict]:
    """Load grid_results.csv into list of dicts with proper types."""
    if not path.exists():
        logger.error("Grid results file not found: %s", path)
        sys.exit(1)

    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                if k in ("n_trades",):
                    parsed[k] = int(v) if v else 0
                elif k in ("name", "instrument", "exit_strategy", "thesis_filter", "min_grade"):
                    parsed[k] = v if v else ""
                elif k in ("hold_days",):
                    parsed[k] = int(v) if v else 0
                else:
                    try:
                        parsed[k] = float(v) if v else 0.0
                    except ValueError:
                        parsed[k] = v
                # Normalize thesis_filter
                if k == "thesis_filter" and v in ("", "None", "none"):
                    parsed[k] = "all"
                # Normalize min_grade
                if k == "min_grade" and v in ("", "None", "none"):
                    parsed[k] = "any"
            rows.append(parsed)

    logger.info("Loaded %d configs from %s", len(rows), path)
    return rows


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_pct(val: float) -> str:
    """Format as percentage string."""
    return f"{val:+.2%}" if isinstance(val, (int, float)) else str(val)


def fmt_f(val: float, decimals: int = 2) -> str:
    """Format float."""
    return f"{val:.{decimals}f}" if isinstance(val, (int, float)) else str(val)


def config_row(r: dict) -> str:
    """Format a single config as a Markdown table row."""
    pf = fmt_f(r["profit_factor"]) if r["profit_factor"] < 100 else "inf"
    return (
        f"| {r['name']:<45} | {r['n_trades']:>6} | {fmt_f(r['sharpe']):>7} | "
        f"{fmt_pct(r['win_rate']):>8} | {fmt_pct(r['avg_return']):>9} | "
        f"{fmt_pct(r['max_drawdown']):>9} | {pf:>6} |"
    )


def table_header() -> str:
    """Standard table header."""
    hdr = (
        f"| {'Config':<45} | {'Trades':>6} | {'Sharpe':>7} | "
        f"{'Win Rate':>8} | {'Avg Ret':>9} | "
        f"{'Max DD':>9} | {'PF':>6} |"
    )
    sep = "|" + "-" * 47 + "|" + "-" * 8 + "|" + "-" * 9 + "|" + "-" * 10 + "|" + "-" * 11 + "|" + "-" * 11 + "|" + "-" * 8 + "|"
    return f"{hdr}\n{sep}"


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def section_executive_summary(rows: list[dict]) -> str:
    """Top 5 configs by Sharpe, best stock config, best options config."""
    lines = ["## 1. Executive Summary\n"]

    valid = [r for r in rows if r["n_trades"] >= 10]
    by_sharpe = sorted(valid, key=lambda r: r["sharpe"], reverse=True)

    lines.append("### Top 5 Configurations by Sharpe\n")
    lines.append(table_header())
    for r in by_sharpe[:5]:
        lines.append(config_row(r))

    # Best stock config
    stocks = [r for r in valid if r["instrument"] == "shares"]
    if stocks:
        best_stock = max(stocks, key=lambda r: r["sharpe"])
        lines.append(f"\n**Best stock config:** {best_stock['name']} "
                     f"(Sharpe {fmt_f(best_stock['sharpe'])}, "
                     f"WR {fmt_pct(best_stock['win_rate'])}, "
                     f"N={best_stock['n_trades']})")

    # Best options config
    options = [r for r in valid if r["instrument"] in ("call", "put")]
    if options:
        best_opt = max(options, key=lambda r: r["sharpe"])
        lines.append(f"\n**Best options config:** {best_opt['name']} "
                     f"(Sharpe {fmt_f(best_opt['sharpe'])}, "
                     f"WR {fmt_pct(best_opt['win_rate'])}, "
                     f"N={best_opt['n_trades']})")

    lines.append("")
    return "\n".join(lines)


def section_stocks_vs_options(rows: list[dict]) -> str:
    """Compare shares/call/put for each thesis."""
    lines = ["## 2. Stocks vs Options\n"]

    # Group by thesis_filter
    by_thesis = defaultdict(list)
    for r in rows:
        by_thesis[r["thesis_filter"]].append(r)

    for thesis in sorted(by_thesis.keys()):
        configs = by_thesis[thesis]
        # Sub-group by instrument
        by_inst = defaultdict(list)
        for r in configs:
            by_inst[r["instrument"]].append(r)

        lines.append(f"### Thesis: {thesis}\n")
        lines.append(table_header())

        for inst in ["shares", "call", "put"]:
            inst_configs = by_inst.get(inst, [])
            if not inst_configs:
                continue
            # Show best config per instrument for this thesis
            valid = [r for r in inst_configs if r["n_trades"] >= 5]
            if not valid:
                continue
            best = max(valid, key=lambda r: r["sharpe"])
            lines.append(config_row(best))

        lines.append("")

    return "\n".join(lines)


def section_hold_time(rows: list[dict]) -> str:
    """How Sharpe varies by hold_days for each thesis."""
    lines = ["## 3. Hold Time Analysis\n"]

    # Only fixed-exit configs to isolate hold_days effect
    fixed = [r for r in rows if r["exit_strategy"] == "fixed" and r["n_trades"] >= 10]
    if not fixed:
        lines.append("_No fixed-exit configs with sufficient trades._\n")
        return "\n".join(lines)

    # Group by thesis_filter
    by_thesis = defaultdict(list)
    for r in fixed:
        by_thesis[r["thesis_filter"]].append(r)

    # Collect all hold_days values
    all_holds = sorted(set(r["hold_days"] for r in fixed))
    hold_cols = " | ".join(f"{h}d" for h in all_holds)

    lines.append(f"| {'Thesis':<12} | {'Inst':<6} | " + " | ".join(f"{h:>5}d" for h in all_holds) + " |")
    sep_parts = ["-" * 14, "-" * 8] + ["-" * 7 for _ in all_holds]
    lines.append("|" + "|".join(sep_parts) + "|")

    for thesis in sorted(by_thesis.keys()):
        configs = by_thesis[thesis]
        # Sub-group by instrument
        by_inst = defaultdict(list)
        for r in configs:
            by_inst[r["instrument"]].append(r)

        for inst in sorted(by_inst.keys()):
            sharpe_by_hold = {}
            for r in by_inst[inst]:
                sharpe_by_hold[r["hold_days"]] = r["sharpe"]
            cells = []
            for h in all_holds:
                s = sharpe_by_hold.get(h)
                cells.append(f"{s:>6.2f}" if s is not None else f"{'--':>6}")
            lines.append(f"| {thesis:<12} | {inst:<6} | " + " | ".join(cells) + " |")

    lines.append("")
    return "\n".join(lines)


def section_exit_strategy(rows: list[dict]) -> str:
    """Compare exit strategies for each thesis."""
    lines = ["## 4. Exit Strategy Comparison\n"]

    by_thesis = defaultdict(list)
    for r in rows:
        if r["n_trades"] >= 10:
            by_thesis[r["thesis_filter"]].append(r)

    for thesis in sorted(by_thesis.keys()):
        configs = by_thesis[thesis]
        # Group by exit_strategy, pick best instrument per exit
        by_exit = defaultdict(list)
        for r in configs:
            by_exit[r["exit_strategy"]].append(r)

        lines.append(f"### Thesis: {thesis}\n")
        lines.append(table_header())

        for exit_strat in sorted(by_exit.keys()):
            best = max(by_exit[exit_strat], key=lambda r: r["sharpe"])
            lines.append(config_row(best))

        lines.append("")

    return "\n".join(lines)


def section_thesis_performance(rows: list[dict]) -> str:
    """Which thesis types have best risk-adjusted returns."""
    lines = ["## 5. Thesis Performance\n"]

    # For each thesis, aggregate best-config metrics across instruments
    by_thesis = defaultdict(list)
    for r in rows:
        if r["n_trades"] >= 10:
            by_thesis[r["thesis_filter"]].append(r)

    lines.append(f"| {'Thesis':<12} | {'Best Sharpe':>11} | {'Best WR':>8} | "
                 f"{'Best Avg Ret':>12} | {'Configs':>7} | {'Best Config':<35} |")
    lines.append("|" + "-" * 14 + "|" + "-" * 13 + "|" + "-" * 10 + "|" + "-" * 14 + "|" + "-" * 9 + "|" + "-" * 37 + "|")

    thesis_order = sorted(by_thesis.keys(), key=lambda t: max(r["sharpe"] for r in by_thesis[t]), reverse=True)

    for thesis in thesis_order:
        configs = by_thesis[thesis]
        best_sharpe_cfg = max(configs, key=lambda r: r["sharpe"])
        best_wr_cfg = max(configs, key=lambda r: r["win_rate"])
        best_ret_cfg = max(configs, key=lambda r: r["avg_return"])

        lines.append(
            f"| {thesis:<12} | {fmt_f(best_sharpe_cfg['sharpe']):>11} | "
            f"{fmt_pct(best_wr_cfg['win_rate']):>8} | "
            f"{fmt_pct(best_ret_cfg['avg_return']):>12} | "
            f"{len(configs):>7} | {best_sharpe_cfg['name']:<35} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_score_filter(rows: list[dict]) -> str:
    """Impact of signal grade filtering on metrics."""
    lines = ["## 6. Score Filter Impact\n"]

    # Group by (thesis_filter, instrument, exit_strategy, hold_days) to isolate grade effect
    groups = defaultdict(list)
    for r in rows:
        if r["n_trades"] >= 5:
            key = (r["thesis_filter"], r["instrument"], r["exit_strategy"], r["hold_days"])
            groups[key].append(r)

    # Find groups that have multiple grade levels
    grade_comparisons = []
    for key, configs in groups.items():
        grades = set(r["min_grade"] for r in configs)
        if len(grades) >= 2:
            for r in configs:
                grade_comparisons.append(r)

    if not grade_comparisons:
        lines.append("_Insufficient data for score filter comparison (need same config at multiple grade levels)._\n")
        return "\n".join(lines)

    # Aggregate by grade level
    by_grade = defaultdict(list)
    for r in grade_comparisons:
        by_grade[r["min_grade"]].append(r)

    lines.append(f"| {'Grade Filter':<12} | {'Configs':>7} | {'Avg Sharpe':>10} | "
                 f"{'Avg WR':>8} | {'Avg Ret':>9} | {'Avg Trades':>10} |")
    lines.append("|" + "-" * 14 + "|" + "-" * 9 + "|" + "-" * 12 + "|" + "-" * 10 + "|" + "-" * 11 + "|" + "-" * 12 + "|")

    for grade in sorted(by_grade.keys()):
        configs = by_grade[grade]
        n = len(configs)
        avg_sharpe = sum(r["sharpe"] for r in configs) / n
        avg_wr = sum(r["win_rate"] for r in configs) / n
        avg_ret = sum(r["avg_return"] for r in configs) / n
        avg_trades = sum(r["n_trades"] for r in configs) / n

        lines.append(
            f"| {grade:<12} | {n:>7} | {fmt_f(avg_sharpe):>10} | "
            f"{fmt_pct(avg_wr):>8} | {fmt_pct(avg_ret):>9} | {avg_trades:>10.0f} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_statistical_notes(rows: list[dict]) -> str:
    """Sample sizes and caveats."""
    lines = ["## 7. Statistical Notes\n"]

    total_configs = len(rows)
    with_trades = [r for r in rows if r["n_trades"] > 0]
    no_trades = total_configs - len(with_trades)

    # Trade count stats
    if with_trades:
        trade_counts = [r["n_trades"] for r in with_trades]
        min_n = min(trade_counts)
        max_n = max(trade_counts)
        avg_n = sum(trade_counts) / len(trade_counts)
        median_n = sorted(trade_counts)[len(trade_counts) // 2]
    else:
        min_n = max_n = avg_n = median_n = 0

    # Instrument coverage
    instruments = set(r["instrument"] for r in rows)
    options_configs = [r for r in with_trades if r["instrument"] in ("call", "put")]

    lines.append(f"- **Total configurations evaluated:** {total_configs}")
    lines.append(f"- **Configs with trades:** {len(with_trades)} ({no_trades} had zero matches)")
    lines.append(f"- **Trade counts:** min={min_n}, max={max_n:,}, median={median_n}, mean={avg_n:.0f}")
    lines.append(f"- **Instruments tested:** {', '.join(sorted(instruments))}")
    lines.append(f"- **Options configs with trades:** {len(options_configs)}")
    lines.append("")
    lines.append("### Caveats\n")
    lines.append("- Options pricing uses ThetaData EOD data where available, Black-Scholes fallback otherwise.")
    lines.append("  Approximately 26% of events lack options data (OTC/micro-cap stocks).")
    lines.append("- Sharpe ratios assume 252 trading days/year and 0% risk-free rate.")
    lines.append("- Small sample sizes (N < 30) should be treated with caution.")
    lines.append("- Backtest does not account for slippage, commissions, or borrow costs on puts.")
    lines.append("- Point-in-time data used throughout: filing_date for signal timing, no look-ahead.")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def generate_report(rows: list[dict]) -> str:
    """Generate full Markdown comparison report."""
    sections = [
        f"# CW Simulation Comparison Report\n",
        f"_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n",
        f"_Source: grid_results.csv ({len(rows)} configurations)_\n",
        section_executive_summary(rows),
        section_stocks_vs_options(rows),
        section_hold_time(rows),
        section_exit_strategy(rows),
        section_thesis_performance(rows),
        section_score_filter(rows),
        section_statistical_notes(rows),
    ]
    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate CW simulation comparison report")
    parser.add_argument("--input", default=str(DEFAULT_INPUT),
                        help=f"Path to grid_results.csv (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help=f"Output path (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    rows = load_grid(input_path)
    if not rows:
        logger.error("No data loaded")
        sys.exit(1)

    report = generate_report(rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report)

    logger.info("Report written: %s (%d chars)", output_path, len(report))

    # Print summary to stdout
    valid = [r for r in rows if r["n_trades"] >= 10]
    print(f"\nReport: {output_path}")
    print(f"Configs: {len(rows)} total, {len(valid)} with 10+ trades")
    if valid:
        best = max(valid, key=lambda r: r["sharpe"])
        print(f"Best Sharpe: {best['name']} ({fmt_f(best['sharpe'])})")


if __name__ == "__main__":
    main()
