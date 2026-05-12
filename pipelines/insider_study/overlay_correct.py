#!/usr/bin/env python3
"""PIT-correct overlay calculator.

Replaces the broken math in api/routers/portfolio.py:307 (the /overlay endpoint).
The bug there: blended equity treats SPY as 100% allocated AT ALL TIMES, then
adds insider excess on top — which models >100% capital deployment.

This script computes the correct overlay:
  - At every moment, total_equity = insider_capital + idle_capital
  - insider_capital = sum of open-position capital (capped at total_equity)
  - idle_capital = total_equity − insider_capital
  - Only idle_capital earns SPY's daily return
  - Insider P&L recognized at exit
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from config.database import get_connection


@dataclass
class Position:
    trade_id: int
    entry_date: str
    exit_date: str
    capital_at_entry: float    # $ amount allocated at entry
    pnl_pct: float             # final realized P&L %


def run_overlay(strategy: str, idle_vehicle: str, starting_capital: float) -> Dict:
    conn = get_connection()

    # Load closed backtest trades. Filter to PRIMARY backtest only — exclude
    # backtest_v3 to avoid the 140% allocation overlap with original backtest.
    trades_raw = conn.execute(
        """
        SELECT id, ticker, entry_date::text AS entry_date,
               exit_date::text AS exit_date,
               position_size, pnl_pct, dollar_amount, portfolio_value
        FROM strategy_portfolio
        WHERE strategy = ?
          AND execution_source = 'backtest'
          AND status = 'closed'
          AND exit_date IS NOT NULL
          AND pnl_pct IS NOT NULL
        ORDER BY entry_date, id
        """,
        (strategy,),
    ).fetchall()

    # Load SPY/idle daily closes
    idle_ticker = idle_vehicle if idle_vehicle != "CASH" else "SPY"
    base_rows = conn.execute(
        """SELECT date::text, close FROM prices.daily_prices
           WHERE ticker = ? ORDER BY date""",
        (idle_ticker,),
    ).fetchall()
    base_close = {r[0]: float(r[1]) for r in base_rows if r[1]}

    # Trading calendar = days SPY traded
    cal = sorted(base_close.keys())

    if not trades_raw:
        return {"error": "no trades found"}
    first_date = min(t["entry_date"] for t in trades_raw)
    cal = [d for d in cal if d >= first_date]

    # Pre-compute next-day prev_close mapping for return
    spy_ret_by_day: Dict[str, float] = {}
    prev = None
    for d in cal:
        if prev is not None and prev > 0:
            spy_ret_by_day[d] = (base_close[d] - prev) / prev
        prev = base_close[d]

    # Track positions opening on each date and closing on each date
    opens_on: Dict[str, List[dict]] = {}
    closes_on: Dict[str, List[dict]] = {}
    for t in trades_raw:
        opens_on.setdefault(t["entry_date"], []).append(t)
        closes_on.setdefault(t["exit_date"], []).append(t)

    # ── Simulation ─────────────────────────────────────────────────────
    # State:
    #   equity       — total portfolio value (insider + idle)
    #   idle_cap     — $ in idle vehicle (SPY/BIL/cash)
    #   open_caps    — list of $ capital tied up in open insider positions
    #                  (insider capital at entry — return realized on exit)

    equity = starting_capital
    idle_cap = starting_capital
    open_caps: Dict[int, float] = {}    # trade_id → capital_at_entry

    daily_points: List[dict] = []
    pure_idle = starting_capital
    insider_only = starting_capital   # cash-drag version (no idle yield)

    SPY_FLAT_DAILY_RET = 0.0  # CASH variant

    for d in cal:
        # 1) Process exits (free up capital + realize P&L)
        for t in closes_on.get(d, []):
            tid = t["id"]
            if tid in open_caps:
                cap = open_caps[tid]
                exit_value = cap * (1 + (t["pnl_pct"] or 0))
                idle_cap += exit_value
                equity += exit_value - cap  # equity changes by P&L on this position
                insider_only += cap * (t["pnl_pct"] or 0)
                del open_caps[tid]

        # 2) Process opens (move capital from idle into insider)
        # IMPORTANT: cap total insider allocation at 100% of current equity.
        # If two entries try to open on the same day and both would push past
        # 100%, we admit them in order and let the cap allocation be the
        # remaining idle_cap (this mirrors what cw_runner would actually do
        # in live trading — you can't deploy more than your equity).
        for t in opens_on.get(d, []):
            # Target capital at entry = position_size_pct × current equity
            target = (t["position_size"] or 0.10) * equity
            available = max(0.0, idle_cap)
            allocated = min(target, available)
            if allocated > 0:
                open_caps[t["id"]] = allocated
                idle_cap -= allocated
            # else: skip — no capital available (mirrors live capacity)

        # 3) Idle cash earns SPY return today (only the idle portion)
        ret = spy_ret_by_day.get(d, 0.0) if idle_vehicle != "CASH" else SPY_FLAT_DAILY_RET
        if idle_cap > 0:
            idle_growth = idle_cap * ret
            idle_cap += idle_growth
            equity += idle_growth

        # Pure idle benchmark (100% in SPY)
        pure_idle *= (1 + (ret if idle_vehicle != "CASH" else 0))

        # Insider-only (cash-drag) — already updated above on exits

        daily_points.append({
            "date": d,
            "equity": round(equity, 0),
            "idle_cap": round(idle_cap, 0),
            "insider_cap_total": round(sum(open_caps.values()), 0),
            "n_open": len(open_caps),
            "alloc_pct": round((sum(open_caps.values()) / equity if equity > 0 else 0) * 100, 1),
            "pure_idle": round(pure_idle, 0),
            "insider_only_no_yield": round(insider_only, 0),
        })

    # Compute summary
    final = daily_points[-1]
    years = (datetime.strptime(final["date"], "%Y-%m-%d")
             - datetime.strptime(daily_points[0]["date"], "%Y-%m-%d")).days / 365.25

    def _cagr(start, end, yrs):
        if yrs <= 0 or start <= 0:
            return 0.0
        return (end / start) ** (1 / yrs) - 1

    return {
        "starting_capital": starting_capital,
        "idle_vehicle": idle_vehicle,
        "years": round(years, 2),
        "n_trades": len(trades_raw),
        "final_equity_corrected": final["equity"],
        "final_pure_idle": final["pure_idle"],
        "final_insider_only_no_yield": final["insider_only_no_yield"],
        "cagr_corrected": round(_cagr(starting_capital, final["equity"], years) * 100, 2),
        "cagr_pure_idle": round(_cagr(starting_capital, final["pure_idle"], years) * 100, 2),
        "cagr_insider_only": round(_cagr(starting_capital, final["insider_only_no_yield"], years) * 100, 2),
        "max_alloc_pct": max(p["alloc_pct"] for p in daily_points),
        "avg_alloc_pct": round(sum(p["alloc_pct"] for p in daily_points) / len(daily_points), 1),
        "daily_points_sample": daily_points[::100],  # every 100th for inspection
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="quality_momentum")
    p.add_argument("--idle", default="SPY", choices=["SPY", "CASH", "BIL", "SGOV", "QQQ", "TLT", "GLD"])
    p.add_argument("--starting", type=float, default=100_000.0)
    args = p.parse_args()

    print(f"\n{'='*80}")
    print(f"OVERLAY (CORRECTED) — strategy={args.strategy}, idle={args.idle}, start=${args.starting:,.0f}")
    print(f"{'='*80}\n")
    for vehicle in [args.idle, "CASH"]:
        result = run_overlay(args.strategy, vehicle, args.starting)
        print(f"--- Idle vehicle: {vehicle} ---")
        print(f"  Years simulated:           {result['years']}")
        print(f"  Trades:                    {result['n_trades']}")
        print(f"  Max insider alloc %:       {result['max_alloc_pct']:.1f}%")
        print(f"  Avg insider alloc %:       {result['avg_alloc_pct']:.1f}%")
        print(f"  Final equity (corrected):  ${result['final_equity_corrected']:>10,.0f}")
        print(f"  Final pure-{vehicle:<5}:       ${result['final_pure_idle']:>10,.0f}")
        print(f"  Final insider-only:        ${result['final_insider_only_no_yield']:>10,.0f}")
        print(f"  CAGR (corrected):          {result['cagr_corrected']:+.2f}%")
        print(f"  CAGR pure-{vehicle}:           {result['cagr_pure_idle']:+.2f}%")
        print(f"  CAGR insider-only:         {result['cagr_insider_only']:+.2f}%")
        print(f"  Alpha vs pure-{vehicle}:        "
              f"{result['cagr_corrected'] - result['cagr_pure_idle']:+.2f}pp")
        print()


if __name__ == "__main__":
    main()
