"""
Portfolio backtest: run multiple gap fill configs and report combined metrics.

Each strategy runs with its own capital pool ($30K). Combined portfolio P&L
is the sum of all strategy P&Ls. Sharpe and drawdown are computed on the
combined daily return series (aligned by calendar date).

Usage:
    python pipelines/portfolio_backtest.py
    python pipelines/portfolio_backtest.py --start 2020-01-01 --end 2025-12-31
    python pipelines/portfolio_backtest.py --capital 30000 --no-fees
"""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import NamedTuple

import math
import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from framework.backtest.engine import BacktestEngine
from framework.data.calendar import MarketCalendar
from framework.data.storage import DataStorage
from strategies.spy_gap_fill.strategy import SPYGapFillStrategy


# ── Portfolio definition ─────────────────────────────────────────────────────

class StrategySpec(NamedTuple):
    label: str
    config_file: str           # relative to strategies/spy_gap_fill/
    position_pct: float


PORTFOLIO = [
    StrategySpec("SPY 3x",  "config_leveraged.yaml",     5.0),
    StrategySpec("QQQ 3x",  "config_qqq_leveraged.yaml", 5.0),
]

STRATEGY_DIR = ROOT / "strategies" / "spy_gap_fill"


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_config(filename: str) -> dict:
    with open(STRATEGY_DIR / filename) as f:
        return yaml.safe_load(f)


def build_storage() -> DataStorage:
    REPO_ROOT = ROOT.parent
    all_raw = [
        REPO_ROOT / "spy-0dte" / "data" / "raw",
        ROOT / "data" / "raw",
    ]
    primary = next((d for d in all_raw if d.exists()), ROOT / "data" / "raw")
    extra = [d for d in all_raw if d.exists() and d != primary]
    return DataStorage(raw_dir=primary, extra_raw_dirs=extra or None)


def run_strategy(spec: StrategySpec, start: str, end: str,
                 storage: DataStorage, capital: float, no_fees: bool):
    cfg = load_config(spec.config_file)
    strategy = SPYGapFillStrategy(cfg)
    engine = BacktestEngine(
        strategy=strategy,
        config={
            "starting_capital": capital,
            "position_size_pct": spec.position_pct,
            "commission_per_contract": 0.0 if no_fees else 0.65,
            "slippage_pct": 0.0,
        },
        storage=storage,
    )
    return engine.run(start, end)


def compute_portfolio_metrics(
    results: list,
    labels: list[str],
    trading_days: list[str],
    capital_per: float,
) -> dict:
    """
    Combine daily return series from multiple strategies.

    daily_returns in each result is P&L/capital per calendar day,
    parallel to trading_days. Combined portfolio:
        - Total capital = capital_per × N strategies
        - Combined daily $ P&L = sum of individual $ P&Ls
        - Combined daily return = combined $ P&L / total capital
    """
    n = len(trading_days)
    total_capital = capital_per * len(results)

    # Build per-strategy dollar P&L series aligned to trading_days
    series = []
    for res in results:
        dr = res.daily_returns
        # Pad / truncate to match calendar length (shouldn't differ, but safe)
        if len(dr) < n:
            dr = dr + [0.0] * (n - len(dr))
        elif len(dr) > n:
            dr = dr[:n]
        # Convert fraction → dollar P&L using starting capital
        dollar_pnl = [r * capital_per for r in dr]
        series.append(dollar_pnl)

    combined_dollar = [sum(col) for col in zip(*series)]
    combined_daily_ret = [p / total_capital for p in combined_dollar]

    # Equity curve
    equity = total_capital
    equity_curve = [equity]
    for pnl in combined_dollar:
        equity += pnl
        equity_curve.append(equity)

    # Max drawdown
    peak = equity_curve[0]
    max_dd = 0.0
    for e in equity_curve:
        peak = max(peak, e)
        dd = (peak - e) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    # Sharpe (annualized)
    ret_arr = np.array(combined_daily_ret)
    sharpe = (np.mean(ret_arr) / np.std(ret_arr, ddof=1) * math.sqrt(252)
              if np.std(ret_arr, ddof=1) > 0 else 0.0)

    # Days where multiple strategies triggered simultaneously
    trade_days_per = []
    for res in results:
        td = {t.date for t in res.trades}
        trade_days_per.append(td)

    overlap_2 = set()
    for i in range(len(trade_days_per)):
        for j in range(i + 1, len(trade_days_per)):
            overlap_2 |= trade_days_per[i] & trade_days_per[j]

    return {
        "total_capital": total_capital,
        "ending_capital": equity_curve[-1],
        "net_pnl": equity_curve[-1] - total_capital,
        "total_return_pct": (equity_curve[-1] - total_capital) / total_capital * 100,
        "sharpe": float(sharpe),
        "max_drawdown_pct": max_dd * 100,
        "combined_daily_ret": combined_daily_ret,
        "equity_curve": equity_curve,
        "same_day_triggers": len(overlap_2),
        "trade_days": [td for td in trade_days_per],
    }


def print_individual(label: str, result, capital: float, pos_pct: float) -> None:
    s = result.summary()
    print(f"\n  {label}  (position {pos_pct:.0f}%,  capital ${capital:,.0f})")
    print(f"  {'─'*50}")
    print(f"    Trades:      {s['total_trades']:>5}   WR: {s['win_rate']:.1%}")
    print(f"    Net P&L:     ${s['total_pnl']:>+8,.2f}   Return: {s['total_return_pct']:+.2f}%")
    print(f"    Avg Win:     ${s['avg_win']:>+8,.2f}   Avg Loss: ${s['avg_loss']:>+8,.2f}")
    print(f"    PF:          {s['profit_factor']:>8.2f}   Sharpe:   {s['sharpe_ratio']:>8.2f}")
    print(f"    Max DD:      ${s['max_drawdown']:>8,.2f}   ({s['max_drawdown_pct']:.2%})")


def print_portfolio(labels: list[str], results: list, metrics: dict, capital: float) -> None:
    width = 60
    print(f"\n{'='*width}")
    print(f"  PORTFOLIO SUMMARY ({' + '.join(labels)})")
    print(f"{'='*width}")
    print(f"    Total Capital:    ${metrics['total_capital']:>10,.0f}")
    print(f"    Ending Capital:   ${metrics['ending_capital']:>10,.2f}")
    print(f"    Net P&L:          ${metrics['net_pnl']:>+10,.2f}")
    print(f"    Total Return:     {metrics['total_return_pct']:>+10.2f}%")
    print(f"    Portfolio Sharpe: {metrics['sharpe']:>10.2f}")
    print(f"    Max Drawdown:     {metrics['max_drawdown_pct']:>10.2f}%")
    print()
    print(f"  CO-OCCURRENCE")
    for i, (li, ri) in enumerate(zip(labels, results)):
        for j, (lj, rj) in enumerate(zip(labels, results)):
            if j <= i:
                continue
            overlap = {t.date for t in ri.trades} & {t.date for t in rj.trades}
            ti, tj = ri.total_trades, rj.total_trades
            print(f"    {li} ∩ {lj}: {len(overlap)} days both trigger  "
                  f"({len(overlap)/max(ti,1):.1%} of {li}, "
                  f"{len(overlap)/max(tj,1):.1%} of {lj})")
    print()

    # Per-day returns correlation
    if len(results) == 2:
        dr0 = np.array(results[0].daily_returns)
        dr1 = np.array(results[1].daily_returns)
        if len(dr0) == len(dr1) and np.std(dr0) > 0 and np.std(dr1) > 0:
            corr = float(np.corrcoef(dr0, dr1)[0, 1])
            print(f"  DAILY RETURN CORRELATION")
            print(f"    {labels[0]} vs {labels[1]}: {corr:+.3f}")
            print()

    print(f"  VS INDIVIDUAL STRATEGIES")
    total_individual_pnl = sum(r.total_pnl for r in results)
    print(f"    Sum of individual P&Ls:  ${total_individual_pnl:>+8,.2f}")
    print(f"    Portfolio P&L:           ${metrics['net_pnl']:>+8,.2f}  "
          f"(diff ${metrics['net_pnl'] - total_individual_pnl:+.2f} from compounding)")
    print(f"{'='*width}")


def main():
    parser = argparse.ArgumentParser(description="Portfolio backtest for gap fill strategies.")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--capital", type=float, default=30_000.0,
                        help="Starting capital per strategy (default: $30,000)")
    parser.add_argument("--no-fees", action="store_true")
    args = parser.parse_args()

    storage = build_storage()
    trading_days = MarketCalendar().get_trading_days(args.start, args.end)

    print(f"\nPortfolio Backtest — Gap Fill (SPY + QQQ)")
    print(f"Period: {args.start} → {args.end}  |  Capital: ${args.capital:,.0f}/strategy")
    print(f"Fees: {'OFF (gross)' if args.no_fees else 'ON'}\n")

    results = []
    labels = []
    for spec in PORTFOLIO:
        print(f"Running {spec.label}...", flush=True)
        r = run_strategy(spec, args.start, args.end, storage, args.capital, args.no_fees)
        results.append(r)
        labels.append(spec.label)

    print("\n" + "="*60)
    print("  INDIVIDUAL RESULTS")
    print("="*60)
    for spec, result in zip(PORTFOLIO, results):
        print_individual(spec.label, result, args.capital, spec.position_pct)

    metrics = compute_portfolio_metrics(results, labels, trading_days, args.capital)
    print_portfolio(labels, results, metrics, args.capital)


if __name__ == "__main__":
    main()
