"""
ETF Gap Fill — multi-symbol pipeline runner.

Discovers all config_*.yaml files in strategies/etf_gap_fill/, runs each symbol
through:
  1. Analysis gate  — empirical fill rate check (≥gate% required)
  2. Backtest       — BacktestEngine with the symbol's config
  3. Portfolio      — combined metrics, N×N correlations, co-occurrence

Usage:
    python pipelines/etf_gap_fill_runner.py
    python pipelines/etf_gap_fill_runner.py --symbols SPY QQQ DIA
    python pipelines/etf_gap_fill_runner.py --start 2020-01-01 --end 2025-12-31
    python pipelines/etf_gap_fill_runner.py --skip-gate        # skip analysis, run all
    python pipelines/etf_gap_fill_runner.py --gate 50          # lower gate threshold
    python pipelines/etf_gap_fill_runner.py --position-pct 10  # override position size
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from framework.backtest.engine import BacktestEngine
from framework.data.calendar import MarketCalendar
from framework.data.storage import DataStorage
from strategies.etf_gap_fill.strategy import ETFGapFillStrategy

# Import analysis logic directly (avoids subprocess overhead)
from pipelines.analyze_gap_fill import analyze_symbol, _fill_rate

ETF_STRATEGY_DIR = ROOT / "strategies" / "etf_gap_fill"


# ── Storage ──────────────────────────────────────────────────────────────────

def build_storage() -> DataStorage:
    REPO_ROOT = ROOT.parent
    all_raw = [
        REPO_ROOT / "spy-0dte" / "data" / "raw",
        ROOT / "data" / "raw",
    ]
    primary = next((d for d in all_raw if d.exists()), ROOT / "data" / "raw")
    extra = [d for d in all_raw if d.exists() and d != primary]
    return DataStorage(raw_dir=primary, extra_raw_dirs=extra or None)


# ── Config discovery ─────────────────────────────────────────────────────────

def discover_configs(symbols_filter: Optional[list[str]] = None) -> list[dict]:
    """
    Scan ETF_STRATEGY_DIR for config_*.yaml files.
    Returns list of dicts: {symbol, config_path, config}.
    Optionally filter to only the given symbols.
    """
    entries = []
    for path in sorted(ETF_STRATEGY_DIR.glob("config_*.yaml")):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        symbol = cfg.get("data", {}).get("primary_symbol", "").upper()
        if not symbol:
            continue
        if symbols_filter and symbol not in [s.upper() for s in symbols_filter]:
            continue
        entries.append({"symbol": symbol, "config_path": path, "config": cfg})
    return entries


# ── Analysis gate ─────────────────────────────────────────────────────────────

def passes_gate(symbol: str, storage: DataStorage, start: Optional[str],
                end: Optional[str], gate_pct: float, min_n: int = 10) -> tuple[bool, float, int]:
    """
    Run empirical fill rate analysis. Return (passes, fill_rate, n_qualifying).
    Gate: small gaps + F30 fade + not pre-filled, fill rate ≥ gate_pct.
    """
    df = analyze_symbol(symbol, storage, start, end)
    if df.empty:
        return False, float("nan"), 0

    clean_mask = (
        (df["bucket"] == "small") & df["f30_fades"] & ~df["gap_filled_in_f30"]
    )
    rate, n = _fill_rate(df, clean_mask)
    if n < min_n:
        return False, rate, n
    passes = (not math.isnan(rate)) and (rate >= gate_pct)
    return passes, rate, n


# ── Backtest runner ───────────────────────────────────────────────────────────

def run_backtest(entry: dict, start: str, end: str, storage: DataStorage,
                 capital: float, position_pct: float, no_fees: bool):
    cfg = entry["config"]
    strategy = ETFGapFillStrategy(cfg)
    engine = BacktestEngine(
        strategy=strategy,
        config={
            "starting_capital": capital,
            "position_size_pct": position_pct,
            "commission_per_contract": 0.0 if no_fees else 0.65,
            "slippage_pct": 0.0,
        },
        storage=storage,
    )
    return engine.run(start, end)


# ── Portfolio metrics ─────────────────────────────────────────────────────────

def compute_portfolio(results: list, labels: list[str],
                      trading_days: list[str], capital_per: float) -> dict:
    n_days = len(trading_days)
    total_cap = capital_per * len(results)

    series = []
    for res in results:
        dr = list(res.daily_returns)
        if len(dr) < n_days:
            dr += [0.0] * (n_days - len(dr))
        elif len(dr) > n_days:
            dr = dr[:n_days]
        series.append([r * capital_per for r in dr])

    combined_dollar = [sum(col) for col in zip(*series)]
    combined_daily_ret = [p / total_cap for p in combined_dollar]

    equity = total_cap
    equity_curve = [equity]
    for pnl in combined_dollar:
        equity += pnl
        equity_curve.append(equity)

    peak = equity_curve[0]
    max_dd = 0.0
    for e in equity_curve:
        peak = max(peak, e)
        dd = (peak - e) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    ret_arr = np.array(combined_daily_ret)
    std = np.std(ret_arr, ddof=1)
    sharpe = float(np.mean(ret_arr) / std * math.sqrt(252)) if std > 0 else 0.0

    # N×N correlation matrix
    corr_matrix = {}
    for i, li in enumerate(labels):
        for j, lj in enumerate(labels):
            if i >= j:
                continue
            dr_i = np.array(results[i].daily_returns[:n_days])
            dr_j = np.array(results[j].daily_returns[:n_days])
            if np.std(dr_i) > 0 and np.std(dr_j) > 0:
                corr_matrix[(li, lj)] = float(np.corrcoef(dr_i, dr_j)[0, 1])
            else:
                corr_matrix[(li, lj)] = float("nan")

    # Co-occurrence
    trade_dates = [{t.date for t in r.trades} for r in results]
    cooccur = {}
    for i, li in enumerate(labels):
        for j, lj in enumerate(labels):
            if i >= j:
                continue
            overlap = trade_dates[i] & trade_dates[j]
            cooccur[(li, lj)] = len(overlap)

    return {
        "total_cap": total_cap,
        "ending_cap": equity_curve[-1],
        "net_pnl": equity_curve[-1] - total_cap,
        "total_return_pct": (equity_curve[-1] - total_cap) / total_cap * 100,
        "sharpe": sharpe,
        "max_dd_pct": max_dd * 100,
        "corr_matrix": corr_matrix,
        "cooccur": cooccur,
        "trade_dates": trade_dates,
    }


# ── Printers ─────────────────────────────────────────────────────────────────

def print_individual(label: str, result, capital: float, position_pct: float,
                     gate_rate: float, gate_n: int) -> None:
    s = result.summary()
    years = result.total_trades / max(s["total_trades"] / 6, 0.01)  # rough
    print(f"\n  {label}  (pos {position_pct:.0f}%  capital ${capital:,.0f})")
    if not math.isnan(gate_rate):
        print(f"  Analysis gate: {gate_rate:.1f}% fill rate on {gate_n} qualifying setups")
    print(f"  {'─'*52}")
    print(f"    Trades:    {s['total_trades']:>5}  ({s['total_trades']/6:.1f}/yr)   WR: {s['win_rate']:.1%}")
    print(f"    Net P&L:   ${s['total_pnl']:>+9,.2f}   Return: {s['total_return_pct']:>+.2f}%")
    print(f"    Avg Win:   ${s['avg_win']:>+9,.2f}   Avg Loss: ${s['avg_loss']:>+9,.2f}")
    print(f"    PF:        {s['profit_factor']:>9.2f}   Sharpe:   {s['sharpe_ratio']:>9.2f}")
    print(f"    Max DD:    ${s['max_drawdown']:>9,.2f}   ({s['max_drawdown_pct']:.2%})")


def print_portfolio(labels: list[str], results: list, metrics: dict,
                    years: float = 6.0) -> None:
    W = 64
    n = len(labels)
    print(f"\n{'='*W}")
    print(f"  PORTFOLIO SUMMARY — {n} symbol{'s' if n != 1 else ''}: {', '.join(labels)}")
    print(f"{'='*W}")
    print(f"    Total Capital:    ${metrics['total_cap']:>10,.0f}  ({n} × pool)")
    print(f"    Ending Capital:   ${metrics['ending_cap']:>10,.2f}")
    print(f"    Net P&L ({years:.0f}yr):  ${metrics['net_pnl']:>+10,.2f}")
    print(f"    Annualized P&L:   ${metrics['net_pnl']/years:>+10,.2f}/yr")
    print(f"    Total Return:     {metrics['total_return_pct']:>+10.2f}%  ({metrics['total_return_pct']/years:>+.2f}%/yr)")
    print(f"    Portfolio Sharpe: {metrics['sharpe']:>10.2f}")
    print(f"    Max Drawdown:     {metrics['max_dd_pct']:>10.2f}%")

    # Total trades and trades/yr
    total_t = sum(r.total_trades for r in results)
    print(f"    Total Trades:     {total_t:>10d}  ({total_t/years:.1f}/yr)")

    print(f"\n  CO-OCCURRENCE (same-day triggers)")
    for (l1, l2), n_days in sorted(metrics["cooccur"].items()):
        t1 = results[labels.index(l1)].total_trades
        t2 = results[labels.index(l2)].total_trades
        p1 = n_days / t1 * 100 if t1 else 0
        p2 = n_days / t2 * 100 if t2 else 0
        print(f"    {l1} ∩ {l2}: {n_days:>3} days  ({p1:.1f}% of {l1}, {p2:.1f}% of {l2})")

    if metrics["corr_matrix"]:
        print(f"\n  DAILY RETURN CORRELATIONS")
        for (l1, l2), corr in sorted(metrics["corr_matrix"].items()):
            bar = "█" * int(abs(corr) * 20)
            sign = "+" if corr >= 0 else "-"
            print(f"    {l1:>4} vs {l2:<4}  {corr:>+.3f}  {sign}{bar}")

    # Individual P&L sum vs portfolio (shows diversification benefit)
    sum_pnl = sum(r.total_pnl for r in results)
    diff = metrics["net_pnl"] - sum_pnl
    print(f"\n  DIVERSIFICATION")
    print(f"    Sum of individual P&Ls:  ${sum_pnl:>+10,.2f}")
    print(f"    Portfolio P&L:           ${metrics['net_pnl']:>+10,.2f}  (diff ${diff:+.2f})")

    print(f"{'='*W}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="ETF gap fill multi-symbol runner."
    )
    parser.add_argument("--symbols", nargs="+",
                        help="Filter to specific symbols (default: all from config YAMLs)")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--capital", type=float, default=30_000.0,
                        help="Starting capital per symbol (default: $30,000)")
    parser.add_argument("--position-pct", type=float, default=5.0,
                        help="Position size %% (default: 5.0)")
    parser.add_argument("--gate", type=float, default=60.0,
                        help="Analysis gate: min fill %% required (default: 60)")
    parser.add_argument("--skip-gate", action="store_true",
                        help="Skip analysis gate, run all discovered symbols")
    parser.add_argument("--no-fees", action="store_true")
    parser.add_argument("--years", type=float, default=6.0,
                        help="Backtest period in years (for annualization)")
    args = parser.parse_args()

    storage = build_storage()
    trading_days = MarketCalendar().get_trading_days(args.start, args.end)
    args.years = len(trading_days) / 252  # actual trading years in range

    entries = discover_configs(args.symbols)
    if not entries:
        print("No config YAMLs found in strategies/etf_gap_fill/.")
        print("Run the setup steps in STRATEGY.md first.")
        sys.exit(1)

    print(f"\nETF Gap Fill Portfolio Runner")
    print(f"Discovered {len(entries)} symbol config(s): {', '.join(e['symbol'] for e in entries)}")
    print(f"Period: {args.start} → {args.end}  |  Capital: ${args.capital:,.0f}/symbol"
          f"  |  Position: {args.position_pct:.0f}%")
    print(f"Gate: {'SKIP' if args.skip_gate else f'≥{args.gate:.0f}% fill rate'}\n")

    qualified = []
    rejected = []
    gate_info = {}

    # ── Phase 1: Analysis gate ───────────────────────────────────────────────
    if not args.skip_gate:
        print("Phase 1: Analysis gate")
        print(f"{'─'*60}")
        for entry in entries:
            sym = entry["symbol"]
            passes, rate, n = passes_gate(sym, storage, args.start, args.end,
                                          args.gate, min_n=10)
            gate_info[sym] = (rate, n)
            rate_str = f"{rate:.1f}%" if not math.isnan(rate) else "n/a"
            verdict = "PASS" if passes else ("LOW-N" if n < 10 else "FAIL")
            print(f"  {sym:<6}  fill rate {rate_str:>7} ({n:>3} trades)  → {verdict}")
            if passes:
                qualified.append(entry)
            else:
                rejected.append(sym)

        if rejected:
            print(f"\n  Rejected: {', '.join(rejected)}")
        print()
    else:
        qualified = entries
        print("Phase 1: Gate skipped — running all symbols\n")

    if not qualified:
        print("No symbols passed the gate. Adjust --gate or collect more data.")
        sys.exit(0)

    # ── Phase 2: Backtests ───────────────────────────────────────────────────
    print(f"Phase 2: Backtests ({len(qualified)} symbol(s))")
    print(f"{'─'*60}")

    results = []
    labels = []
    for entry in qualified:
        sym = entry["symbol"]
        print(f"  Running {sym}...", end="  ", flush=True)
        try:
            r = run_backtest(entry, args.start, args.end, storage,
                             args.capital, args.position_pct, args.no_fees)
            results.append(r)
            labels.append(sym)
            print(f"{r.total_trades}T  WR={r.win_rate:.1%}  Sharpe={r.sharpe_ratio:.2f}"
                  f"  Net=${r.total_pnl:+.0f}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not results:
        print("All backtests failed.")
        sys.exit(1)

    # ── Phase 3: Individual results ──────────────────────────────────────────
    print(f"\n{'='*64}")
    print("  INDIVIDUAL RESULTS")
    print(f"{'='*64}")
    for entry, result, label in zip(qualified, results, labels):
        sym = entry["symbol"]
        rate, n = gate_info.get(sym, (float("nan"), 0))
        pct = entry["config"].get("risk", {}).get("position_size_pct", args.position_pct)
        print_individual(label, result, args.capital, args.position_pct, rate, n)

    # ── Phase 4: Portfolio summary ───────────────────────────────────────────
    if len(results) > 1:
        metrics = compute_portfolio(results, labels, trading_days, args.capital)
        print_portfolio(labels, results, metrics, years=args.years)
    else:
        print(f"\n(Only 1 symbol — portfolio metrics require ≥2 symbols)")


if __name__ == "__main__":
    main()
