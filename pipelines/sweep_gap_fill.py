"""
Parameter sweep for spy_gap_fill strategy on any symbol.

Sweeps gap range, stop width, and F30 fade requirement.
Designed to diagnose why IWM/QQQ underperform SPY and find better settings.

Usage:
    python pipelines/sweep_gap_fill.py --symbol IWM
    python pipelines/sweep_gap_fill.py --symbol QQQ
    python pipelines/sweep_gap_fill.py --symbol IWM QQQ --start 2020-01-01 --end 2025-12-31
"""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from framework.backtest.engine import BacktestEngine
from framework.data.storage import DataStorage
from strategies.spy_gap_fill.strategy import SPYGapFillStrategy

BASE_CONFIG_PATH = ROOT / "strategies" / "spy_gap_fill" / "config_leveraged.yaml"

# ── Parameter grid ──────────────────────────────────────────────────────────
# Each variant: (label, override_dict)
# All variants run against the leveraged 3x config with the target symbol injected.

GAP_VARIANTS = [
    # (label, min_gap, max_gap)
    ("gap_0.05-0.15",  0.05, 0.15),   # small only — highest SPY fill rate tier
    ("gap_0.05-0.25",  0.05, 0.25),   # slightly tighter than baseline
    ("gap_0.05-0.30",  0.05, 0.30),   # baseline
    ("gap_0.10-0.30",  0.10, 0.30),   # skip noise floor
    ("gap_0.15-0.30",  0.15, 0.30),   # medium only
    ("gap_0.15-0.40",  0.15, 0.40),   # medium + wider
    ("gap_0.20-0.50",  0.20, 0.50),   # larger gaps only
]

STOP_VARIANTS = [
    # (label, stop_pct)
    ("stp_0.15",  0.15),
    ("stp_0.20",  0.20),
    ("stp_0.25",  0.25),   # baseline
    ("stp_0.35",  0.35),
    ("stp_0.50",  0.50),
]

F30_VARIANTS = [
    ("f30_on",   True),    # baseline
    ("f30_off",  False),
]

# ── Full cross-product: gap × stop × f30 ────────────────────────────────────
def build_variants() -> list[tuple[str, dict]]:
    variants = []
    for g_label, min_gap, max_gap in GAP_VARIANTS:
        for s_label, stop_pct in STOP_VARIANTS:
            for f_label, f30 in F30_VARIANTS:
                label = f"{g_label}__{s_label}__{f_label}"
                overrides = {
                    "entry": {
                        "min_gap_pct": min_gap,
                        "max_gap_pct": max_gap,
                        "require_f30_fade": f30,
                    },
                    "exit": {
                        "stop_pct": stop_pct,
                    },
                }
                variants.append((label, overrides))
    return variants


def deep_merge(base: dict, overrides: dict) -> dict:
    result = copy.deepcopy(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_base_config() -> dict:
    with open(BASE_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def run_variant(
    symbol: str,
    label: str,
    overrides: dict,
    start: str,
    end: str,
    storage: DataStorage,
    position_pct: float = 5.0,
) -> dict | None:
    base = load_base_config()
    cfg = deep_merge(base, overrides)
    # Inject target symbol
    cfg.setdefault("data", {})["primary_symbol"] = symbol
    cfg["data"]["symbols"] = [symbol, "VIXY"]

    try:
        strategy = SPYGapFillStrategy(cfg)
        engine = BacktestEngine(
            strategy=strategy,
            config={
                "starting_capital": 30_000.0,
                "position_size_pct": position_pct,
                "commission_per_contract": 0.0,
                "slippage_pct": 0.0,
            },
            storage=storage,
        )
        result = engine.run(start, end)
        return {
            "symbol":   symbol,
            "label":    label,
            "trades":   result.total_trades,
            "wr":       result.win_rate,
            "pf":       result.profit_factor,
            "sharpe":   result.sharpe_ratio,
            "net_pnl":  result.total_pnl,
            "max_dd":   result.max_drawdown_pct,
            "avg_win":  result.avg_win,
            "avg_loss": result.avg_loss,
            "expectancy": result.total_pnl / max(result.total_trades, 1),
        }
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def print_table(rows: list[dict], symbol: str, top_n: int = 20):
    """Print top_n rows sorted by Sharpe, then a bottom section for worst."""
    sym_rows = [r for r in rows if r["symbol"] == symbol and r["trades"] >= 10]
    if not sym_rows:
        print(f"  No results for {symbol}")
        return

    # Sort by Sharpe descending
    sym_rows.sort(key=lambda r: r["sharpe"], reverse=True)

    headers = ["Label", "N", "WR%", "PF", "Sharpe", "Net$", "AvgW$", "AvgL$", "BE_WR%"]
    widths   = [42,      5,   6,    5,    7,        9,      7,       7,       7]

    sep = "  ".join("-" * w for w in widths)
    hdr = "  ".join(h.ljust(w) for h, w in zip(headers, widths))

    def be_wr(r):
        """Break-even win rate = |avg_loss| / (avg_win + |avg_loss|)."""
        w, l = r["avg_win"], abs(r["avg_loss"])
        if w + l == 0:
            return float("nan")
        return l / (w + l) * 100

    def fmt_row(r):
        return [
            r["label"][:42],
            str(r["trades"]),
            f"{r['wr']*100:.1f}",
            f"{r['pf']:.2f}",
            f"{r['sharpe']:.2f}",
            f"${r['net_pnl']:+.0f}",
            f"${r['avg_win']:+.0f}",
            f"${r['avg_loss']:+.0f}",
            f"{be_wr(r):.1f}",
        ]

    print(f"\n{'=' * len(sep)}")
    print(f"  {symbol} — Gap Fill Parameter Sweep (top {top_n} by Sharpe, N≥10)")
    print(f"{'=' * len(sep)}")
    print(hdr)
    print(sep)

    for r in sym_rows[:top_n]:
        marker = " ◀" if r["sharpe"] > 0.5 else ""
        cells = fmt_row(r)
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)) + marker)

    print(f"{'=' * len(sep)}")
    print(f"  Baseline (gap_0.05-0.30__stp_0.25__f30_on):")
    base_rows = [r for r in sym_rows if "gap_0.05-0.30" in r["label"] and "stp_0.25" in r["label"] and "f30_on" in r["label"]]
    for r in base_rows:
        cells = fmt_row(r)
        print("  " + "  ".join(c.ljust(w) for c, w in zip(cells, widths)))
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Parameter sweep for gap fill strategy on IWM/QQQ."
    )
    parser.add_argument("--symbol", nargs="+", default=["IWM"],
                        help="Symbols to sweep (default: IWM)")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--position-pct", type=float, default=5.0)
    parser.add_argument("--top", type=int, default=20, help="Show top N variants per symbol")
    parser.add_argument("--data-dir", type=Path)
    args = parser.parse_args()

    # Storage setup (same multi-source logic as run_backtest.py)
    REPO_ROOT = ROOT.parent
    all_raw = [
        REPO_ROOT / "spy-0dte" / "data" / "raw",
        ROOT / "data" / "raw",
    ]
    primary = next((d for d in all_raw if d.exists()), ROOT / "data" / "raw")
    extra = [d for d in all_raw if d.exists() and d != primary]
    storage = DataStorage(raw_dir=args.data_dir or primary, extra_raw_dirs=extra or None)

    variants = build_variants()
    symbols = [s.upper() for s in args.symbol]
    total = len(variants) * len(symbols)

    print(f"Sweeping {len(variants)} variants × {len(symbols)} symbols = {total} backtests")
    print(f"Period: {args.start} → {args.end}  |  Position: {args.position_pct}%  |  Capital: $30K\n")

    all_results = []
    n = 0
    for symbol in symbols:
        print(f"--- {symbol} ---")
        for label, overrides in variants:
            n += 1
            # Print a compact progress indicator (no newline)
            entry = overrides["entry"]
            ex = overrides["exit"]
            desc = (f"  [{n:4d}/{total}] gap {entry['min_gap_pct']:.2f}-{entry['max_gap_pct']:.2f}"
                    f"  stp {ex['stop_pct']:.2f}"
                    f"  f30={'Y' if entry['require_f30_fade'] else 'N'}")
            print(desc, end="  ", flush=True)

            r = run_variant(symbol, label, overrides, args.start, args.end,
                            storage, args.position_pct)
            if r:
                all_results.append(r)
                print(f"→ {r['trades']:3d}T  WR={r['wr']*100:.1f}%  Sharpe={r['sharpe']:.2f}  Net=${r['net_pnl']:+.0f}")
            else:
                print("→ SKIP")

    for symbol in symbols:
        print_table(all_results, symbol, top_n=args.top)

    # Save CSV
    import csv
    out_path = ROOT / "reports" / "sweep_gap_fill.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if all_results:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_results[0].keys())
            w.writeheader()
            w.writerows(all_results)
        print(f"Full results saved to: {out_path}")


if __name__ == "__main__":
    main()
