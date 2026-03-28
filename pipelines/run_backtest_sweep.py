"""
Comprehensive parameter sweep for spy_noon_break strategy.

Runs multiple config variants across multiple year ranges and prints a
consolidated comparison table. Useful for overnight batch analysis.

Usage:
    python pipelines/run_backtest_sweep.py
    python pipelines/run_backtest_sweep.py --years 2023 2024
    python pipelines/run_backtest_sweep.py --full  # 2020-2024 combined only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from framework.backtest.engine import BacktestEngine
from framework.data.storage import DataStorage
from strategies.archive.spy_noon_break.strategy import SpyNoonBreakStrategy


BASE_CONFIG_PATH = ROOT / "strategies" / "archive" / "spy_noon_break" / "config.yaml"


def load_base_config() -> dict:
    with open(BASE_CONFIG_PATH) as f:
        return yaml.safe_load(f)


# Config variants: (label, overrides_dict)
VARIANTS = [
    ("base",          {}),
    ("tgt0.30",       {"exit": {"target_pct": 0.30}}),
    ("tgt0.25",       {"exit": {"target_pct": 0.25}}),
    ("stp0.25",       {"exit": {"stop_pct": 0.25}}),
    ("rvol2.0",       {"filters": {"rvol_min": 2.0}}),
    ("rvol1.0",       {"filters": {"rvol_min": 1.0}}),
    ("no_rvol",       {"filters": {"rvol_min": 0.0}}),
    ("vwap_aligned",  {"entry": {"vwap_filter": "aligned"}}),
    ("brk0.20",       {"entry": {"min_break_pct": 0.20}}),
    ("brk0.10",       {"entry": {"min_break_pct": 0.10}}),
]

YEAR_RANGES = [
    ("2020", "2020-01-01", "2020-12-31"),
    ("2021", "2021-01-01", "2021-12-31"),
    ("2022", "2022-01-01", "2022-12-31"),
    ("2023", "2023-01-01", "2023-12-31"),
    ("2024", "2024-01-01", "2024-12-31"),
    ("2020-24", "2020-01-01", "2024-12-31"),
]


def deep_merge(base: dict, overrides: dict) -> dict:
    """Recursively merge overrides into base (non-destructive copy)."""
    import copy
    result = copy.deepcopy(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def run_variant(label: str, overrides: dict, start: str, end: str,
                storage: DataStorage) -> dict:
    """Run one backtest variant and return summary dict."""
    base = load_base_config()
    cfg = deep_merge(base, overrides)

    strategy = SpyNoonBreakStrategy(cfg, storage=storage)
    engine = BacktestEngine(
        strategy=strategy,
        config={
            "starting_capital": float(cfg["sizing"]["starting_capital"]),
            "position_size_pct": float(cfg["sizing"]["position_size_pct"]),
            "commission_per_contract": 0.65,
            "slippage_pct": 0.0002,
        },
        storage=storage,
    )

    result = engine.run(start, end)

    return {
        "variant":  label,
        "period":   f"{start[:4]}-{end[:4]}" if start[:4] != end[:4] else start[:4],
        "trades":   result.total_trades,
        "wr":       result.win_rate,
        "pf":       result.profit_factor,
        "sharpe":   result.sharpe_ratio,
        "net_pnl":  result.total_pnl,
        "max_dd":   result.max_drawdown_pct,
        "avg_win":  result.avg_win,
        "avg_loss": result.avg_loss,
    }


def print_table(rows: list[dict]):
    """Print results as a formatted table."""
    if not rows:
        print("No results.")
        return

    headers = ["Variant", "Period", "N", "WR%", "PF", "Sharpe", "Net$", "MaxDD%", "AvgW$", "AvgL$"]
    widths =   [14,        8,        5,   6,     5,   7,        9,      7,        7,       7]

    def fmt_row(r):
        return [
            r["variant"][:14],
            r["period"],
            str(r["trades"]),
            f"{r['wr']*100:.1f}",
            f"{r['pf']:.2f}",
            f"{r['sharpe']:.2f}",
            f"${r['net_pnl']:+.0f}",
            f"{r['max_dd']*100:.1f}",
            f"${r['avg_win']:+.0f}",
            f"${r['avg_loss']:+.0f}",
        ]

    sep = "  ".join("-" * w for w in widths)
    hdr = "  ".join(h.ljust(w) for h, w in zip(headers, widths))

    print(f"\n{'=' * len(sep)}")
    print(f"  spy_noon_break — Parameter Sweep Results")
    print(f"{'=' * len(sep)}")
    print(hdr)
    print(sep)

    last_period = None
    for r in rows:
        if r["period"] != last_period:
            if last_period is not None:
                print(sep)
            last_period = r["period"]
        cells = fmt_row(r)
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))

    print(f"{'=' * len(sep)}\n")


def main():
    parser = argparse.ArgumentParser(description="Comprehensive spy_noon_break parameter sweep")
    parser.add_argument("--years", nargs="+", help="Specific years to test (e.g. 2023 2024)")
    parser.add_argument("--full", action="store_true", help="Only run 2020-2024 combined")
    parser.add_argument("--variants", nargs="+", help="Specific variant labels to run")
    parser.add_argument(
        "--data-dir",
        default=str(ROOT.parent / "spy-0dte" / "data" / "raw"),
        help="Path to raw data directory",
    )
    args = parser.parse_args()

    storage = DataStorage(raw_dir=args.data_dir)

    # Select year ranges
    if args.full:
        year_ranges = [r for r in YEAR_RANGES if r[0] == "2020-24"]
    elif args.years:
        year_ranges = [r for r in YEAR_RANGES if r[0] in args.years]
        if not year_ranges:
            print(f"No matching years. Available: {[r[0] for r in YEAR_RANGES]}")
            sys.exit(1)
    else:
        year_ranges = YEAR_RANGES

    # Select variants
    variants = VARIANTS
    if args.variants:
        variants = [(l, o) for l, o in VARIANTS if l in args.variants]
        if not variants:
            print(f"No matching variants. Available: {[l for l, _ in VARIANTS]}")
            sys.exit(1)

    total = len(variants) * len(year_ranges)
    print(f"Running {total} backtests ({len(variants)} variants × {len(year_ranges)} periods)...")
    print(f"Data dir: {args.data_dir}\n")

    results = []
    n = 0
    for period_label, start, end in year_ranges:
        for var_label, overrides in variants:
            n += 1
            print(f"  [{n:3d}/{total}] {var_label:14s}  {period_label}", end="", flush=True)
            try:
                r = run_variant(var_label, overrides, start, end, storage)
                results.append(r)
                print(f"  → {r['trades']} trades  WR={r['wr']*100:.1f}%  PF={r['pf']:.2f}  Net=${r['net_pnl']:+.0f}")
            except Exception as e:
                print(f"  ERROR: {e}")

    print_table(results)

    # Save to CSV
    import csv
    out = ROOT / "reports" / "backtest_sweep.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    if results:
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=results[0].keys())
            w.writeheader()
            w.writerows(results)
        print(f"Results saved to: {out}")


if __name__ == "__main__":
    main()
