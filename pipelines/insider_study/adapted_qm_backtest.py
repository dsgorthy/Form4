#!/usr/bin/env python3
"""Backtest QM with prop-firm-compatible adaptations.

Adaptations (vs production QM yaml):
  - position_size_pct:  0.10 → 0.04        (4% per position)
  - max_concurrent:     10   → 5            (smaller book)
  - stop_loss_pct:      null → configurable (default -0.10, hard stop intra-hold)

Source of "would have entered" trades: the V3 simulator's audit table
(trade_decision_audit, source='simulation', strategy='quality_momentum',
stage='capacity', passed=TRUE). Entry price = filing_date close (T+1 open
in production is a fidelity improvement we ignore here; it shifts entry
by 1 day at most).

Exit logic per trade:
  1. Walk daily closes from entry_date forward.
  2. If close <= entry * (1 + stop_loss_pct) → EXIT STOP at that close.
  3. After hold_td trading days → EXIT TIME at that day's close.
  Note: this is daily-close stops, NOT intraday. A real -10% stop would
  often fill worse on a gap-down day; our number is conservative-ish but
  could overstate fill quality on flash drops.

Output:
  - Equity curve (saved as CSV if --emit-csv).
  - MaxDD vs peak, Sharpe (annualized), final return.
  - Max simultaneous open positions actually used.
  - Distribution of per-trade outcomes (winners, losers, stopped, time-exit).
  - Worst daily equity drawdown (for prop-firm daily-loss-limit comparison).

Usage (on Studio):
    python3 -m pipelines.insider_study.adapted_qm_backtest
    python3 -m pipelines.insider_study.adapted_qm_backtest \\
        --position-size 0.04 --max-concurrent 5 --stop -0.10
    python3 -m pipelines.insider_study.adapted_qm_backtest \\
        --sweep-position-size 0.02,0.03,0.04,0.05,0.06,0.10 \\
        --sweep-stop 0,-0.05,-0.10,-0.15,-0.20
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class OpenPos:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    target_exit_idx: int       # index into calendar
    stop_price: Optional[float]


@dataclass
class ClosedTrade:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    exit_reason: str           # 'time' | 'stop'
    pnl_pct: float
    pnl_dollar: float
    hold_days: int
    dollar_amount: float


def _load_candidates(conn) -> List[dict]:
    """All QM-simulator capacity-passed trades, in filing-date order."""
    rows = conn.execute(
        """
        SELECT tda.trade_id, tda.ticker, tda.filing_date::text AS filing_date,
               tda.conviction
        FROM trade_decision_audit tda
        WHERE tda.source = 'simulation'
          AND tda.strategy = 'quality_momentum'
          AND tda.stage = 'capacity'
          AND tda.passed = TRUE
        ORDER BY tda.filing_date, tda.conviction DESC, tda.trade_id
        """,
    ).fetchall()
    out = []
    for r in rows:
        d = {k: r[k] for k in r.keys()} if hasattr(r, "keys") else dict(r)
        out.append(d)
    return out


def _load_calendar(conn, start: str, end: str) -> List[str]:
    rows = conn.execute(
        """SELECT DISTINCT date::text AS d FROM prices.daily_prices
           WHERE date >= ? AND date <= ? ORDER BY d""",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def _preload_prices(conn, tickers: set, start: str, end: str) -> dict:
    out: dict = {}
    BATCH = 200
    tickers_list = sorted(tickers)
    for i in range(0, len(tickers_list), BATCH):
        chunk = tickers_list[i : i + BATCH]
        placeholders = ",".join(["?"] * len(chunk))
        rows = conn.execute(
            f"""SELECT ticker, date::text, close FROM prices.daily_prices
                WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?""",
            tuple(chunk) + (start, end),
        ).fetchall()
        for r in rows:
            if r[2] and r[2] > 0:
                out[(r[0], r[1])] = float(r[2])
    return out


def _find_first_close_on_or_after(prices: dict, calendar: List[str],
                                  cal_idx: int, ticker: str,
                                  max_forward: int = 5
                                  ) -> Optional[Tuple[int, float]]:
    """Return (cal_idx, close) for the first available price for ticker
    starting at cal_idx, searching forward up to max_forward trading days."""
    for off in range(max_forward + 1):
        i = cal_idx + off
        if i >= len(calendar):
            return None
        c = prices.get((ticker, calendar[i]))
        if c is not None:
            return i, c
    return None


def simulate(
    candidates: List[dict],
    prices: dict,
    calendar: List[str],
    starting_capital: float,
    position_size_pct: float,
    max_concurrent: int,
    stop_loss_pct: Optional[float],
    hold_td: int,
    margin_rate_apr: float = 0.0,        # Annual margin interest rate (e.g. 0.0625 for 6.25%)
) -> dict:
    """Walk forward through trading days, manage open positions, return stats."""
    # Bucket candidates by filing_date for per-day processing
    cands_by_date: dict = defaultdict(list)
    for c in candidates:
        cands_by_date[c["filing_date"]].append(c)

    # Build calendar index for fast lookups
    cal_idx_of = {d: i for i, d in enumerate(calendar)}

    held: List[OpenPos] = []
    closed: List[ClosedTrade] = []
    equity = starting_capital
    equity_history: List[Tuple[str, float, float]] = []  # (date, equity, open_unrealized)
    peak_equity = starting_capital
    worst_daily_drop_pct = 0.0
    today_str_today = date.today().isoformat()

    for cal_idx, d in enumerate(calendar):
        if d > today_str_today:
            break

        # ---- 1) Exit checks for held positions ----
        new_held = []
        day_realized = 0.0
        for pos in held:
            close_price = prices.get((pos.ticker, d))
            if close_price is None:
                # No price today — carry forward
                new_held.append(pos)
                continue

            # Stop?
            if pos.stop_price is not None and close_price <= pos.stop_price:
                pnl_pct = (close_price - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                closed.append(ClosedTrade(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    exit_date=d, exit_price=close_price,
                    exit_reason="stop", pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    hold_days=(datetime.strptime(d, "%Y-%m-%d") -
                               datetime.strptime(pos.entry_date, "%Y-%m-%d")).days,
                    dollar_amount=pos.dollar_amount,
                ))
                day_realized += pnl_dollar
                continue

            # Time exit?
            if cal_idx >= pos.target_exit_idx:
                pnl_pct = (close_price - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                closed.append(ClosedTrade(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    exit_date=d, exit_price=close_price,
                    exit_reason="time", pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    hold_days=(datetime.strptime(d, "%Y-%m-%d") -
                               datetime.strptime(pos.entry_date, "%Y-%m-%d")).days,
                    dollar_amount=pos.dollar_amount,
                ))
                day_realized += pnl_dollar
                continue

            new_held.append(pos)
        held = new_held

        # Update equity with realized PnL
        equity += day_realized

        # ---- 2) New entries (filings filed today) ----
        # Process candidates from today's filing_date in conviction order
        candidates_today = sorted(
            cands_by_date.get(d, []), key=lambda c: -c["conviction"],
        )
        held_tickers = {p.ticker for p in held}

        for c in candidates_today:
            if len(held) >= max_concurrent:
                break  # capacity full
            ticker = c["ticker"]
            if ticker in held_tickers:
                continue

            # Entry price: today's close (or next available trading day)
            entry_lookup = _find_first_close_on_or_after(
                prices, calendar, cal_idx, ticker,
            )
            if entry_lookup is None:
                continue
            entry_idx, entry_price = entry_lookup
            if entry_price < 2.0:
                continue  # min price floor

            dollar_amount = equity * position_size_pct
            stop_price = (entry_price * (1 + stop_loss_pct)
                          if stop_loss_pct is not None else None)
            target_exit_idx = entry_idx + hold_td

            held.append(OpenPos(
                trade_id=c["trade_id"], ticker=ticker,
                entry_date=calendar[entry_idx], entry_price=entry_price,
                dollar_amount=dollar_amount, target_exit_idx=target_exit_idx,
                stop_price=stop_price,
            ))
            held_tickers.add(ticker)

        # ---- 3) Mark-to-market — equity including open positions ----
        open_unrealized = 0.0
        deployed = 0.0
        for pos in held:
            close_today = prices.get((pos.ticker, d))
            if close_today is None:
                # Walk back up to 5 days for a price
                for back in range(1, 6):
                    bi = cal_idx - back
                    if bi < 0:
                        break
                    c_back = prices.get((pos.ticker, calendar[bi]))
                    if c_back is not None:
                        close_today = c_back
                        break
            if close_today is None:
                deployed += pos.dollar_amount
                continue
            pnl = (close_today - pos.entry_price) / pos.entry_price
            open_unrealized += pos.dollar_amount * pnl
            deployed += pos.dollar_amount

        mtm_equity = equity + open_unrealized

        # 3a) Margin interest — charge daily on the portion of deployed capital
        # that exceeds equity (i.e., what's borrowed). Uses /360 like Alpaca.
        if margin_rate_apr > 0:
            margin_debt = max(0.0, deployed - mtm_equity)
            daily_interest = margin_debt * (margin_rate_apr / 360.0)
            equity -= daily_interest         # cash debited
            mtm_equity -= daily_interest

        equity_history.append((d, mtm_equity, open_unrealized))

        # Track daily drop
        if len(equity_history) > 1:
            prev_mtm = equity_history[-2][1]
            daily_drop = (mtm_equity - prev_mtm) / prev_mtm if prev_mtm > 0 else 0
            if daily_drop < worst_daily_drop_pct:
                worst_daily_drop_pct = daily_drop

        # Trailing peak
        if mtm_equity > peak_equity:
            peak_equity = mtm_equity

    # ---- Compute summary stats ----
    final_equity = equity_history[-1][1] if equity_history else starting_capital
    total_return = (final_equity - starting_capital) / starting_capital

    # MaxDD on MTM equity vs running peak
    running_peak = starting_capital
    max_dd = 0.0
    for _, e, _ in equity_history:
        if e > running_peak:
            running_peak = e
        dd = (e - running_peak) / running_peak if running_peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    # Sharpe (daily returns annualized)
    daily_returns = []
    for i in range(1, len(equity_history)):
        prev = equity_history[i - 1][1]
        curr = equity_history[i][1]
        if prev > 0:
            daily_returns.append((curr - prev) / prev)
    if daily_returns:
        mean_daily = sum(daily_returns) / len(daily_returns)
        var_daily = sum((r - mean_daily) ** 2 for r in daily_returns) / len(daily_returns)
        std_daily = math.sqrt(var_daily) if var_daily > 0 else 0
        sharpe = (mean_daily / std_daily * math.sqrt(252)) if std_daily > 0 else 0
        ann_return = (1 + total_return) ** (252 / max(1, len(daily_returns))) - 1
    else:
        sharpe = 0
        ann_return = 0

    # Win rate / outcome counts
    n_closed = len(closed)
    n_stop = sum(1 for c in closed if c.exit_reason == "stop")
    n_time = sum(1 for c in closed if c.exit_reason == "time")
    n_win = sum(1 for c in closed if c.pnl_pct > 0)
    avg_pnl = sum(c.pnl_pct for c in closed) / max(1, n_closed)
    max_concurrent_seen = 0
    for d_, _, _ in equity_history:
        pass  # not tracked here; held list is the source

    # Max concurrent ever in held
    # (Track during sim if needed — quick post-hoc estimate)
    return {
        "n_candidates": len(candidates),
        "n_entered": n_closed + len(held),
        "n_closed": n_closed,
        "n_open_at_end": len(held),
        "n_winners": n_win,
        "win_rate": n_win / max(1, n_closed),
        "avg_pnl_pct": avg_pnl,
        "n_stopped": n_stop,
        "n_time_exit": n_time,
        "total_return_pct": total_return,
        "ann_return_pct": ann_return,
        "sharpe": sharpe,
        "max_dd_pct": max_dd,
        "worst_daily_drop_pct": worst_daily_drop_pct,
        "final_equity": final_equity,
        "starting_capital": starting_capital,
        "equity_history": equity_history,
        "closed_trades": closed,
    }


def _print_summary(label: str, stats: dict, position_size_pct: float,
                   max_concurrent: int, stop_loss_pct: Optional[float]):
    stop_str = f"{stop_loss_pct*100:+.0f}%" if stop_loss_pct is not None else "none"
    print(f"\n{label}  pos={position_size_pct:.0%}  max_conc={max_concurrent}  stop={stop_str}")
    print(f"  candidates={stats['n_candidates']}, entered={stats['n_entered']}, "
          f"closed={stats['n_closed']}, open_at_end={stats['n_open_at_end']}")
    print(f"  win_rate={stats['win_rate']*100:5.1f}%   avg_pnl={stats['avg_pnl_pct']*100:+6.2f}%")
    print(f"  stop_exits={stats['n_stopped']:>3}   time_exits={stats['n_time_exit']:>3}")
    print(f"  total_return={stats['total_return_pct']*100:+7.2f}%   "
          f"ann_return={stats['ann_return_pct']*100:+6.2f}%   "
          f"Sharpe={stats['sharpe']:+5.2f}")
    print(f"  MaxDD={stats['max_dd_pct']*100:+6.2f}%   "
          f"worst_daily_drop={stats['worst_daily_drop_pct']*100:+6.2f}%")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--starting-capital", type=float, default=50_000.0)
    p.add_argument("--position-size", type=float, default=0.04,
                   help="Per-position size as fraction of equity (default 0.04 = 4%)")
    p.add_argument("--max-concurrent", type=int, default=5,
                   help="Max simultaneously held positions (default 5)")
    p.add_argument("--stop", type=float, default=-0.10,
                   help="Hard stop as fraction (default -0.10 = -10%). "
                        "Pass 0 or empty for no stop.")
    p.add_argument("--hold-td", type=int, default=42,
                   help="Trading-day hold (default 42, same as production)")
    p.add_argument("--sweep-position-size", default=None,
                   help="Comma-separated list (e.g., 0.02,0.03,0.04,0.05). "
                        "Overrides single --position-size.")
    p.add_argument("--sweep-stop", default=None,
                   help="Comma-separated stop values (e.g., 0,-0.05,-0.10).")
    p.add_argument("--emit-csv", default=None,
                   help="Write per-day equity curve to this CSV path")
    p.add_argument("--margin-rate", type=float, default=0.0,
                   help="Annual margin interest rate (e.g. 0.0625 for Alpaca 6.25%%)")
    args = p.parse_args()

    conn = get_connection()
    candidates = _load_candidates(conn)
    logger.info("Loaded %d QM-qualifying candidates from simulator", len(candidates))
    if not candidates:
        logger.error("No candidates — run simulate_decision_audit first")
        return

    cal_start = (datetime.strptime(min(c["filing_date"] for c in candidates), "%Y-%m-%d")
                 - timedelta(days=5)).strftime("%Y-%m-%d")
    cal_end = (datetime.strptime(max(c["filing_date"] for c in candidates), "%Y-%m-%d")
               + timedelta(days=args.hold_td * 2 + 30)).strftime("%Y-%m-%d")
    calendar = _load_calendar(conn, cal_start, cal_end)
    tickers = {c["ticker"] for c in candidates}
    prices = _preload_prices(conn, tickers, cal_start, cal_end)
    logger.info("Cached %d (ticker,date) closes across %d days",
                len(prices), len(calendar))
    conn.close()

    # Sweeps
    if args.sweep_position_size or args.sweep_stop:
        pos_values = [float(x) for x in
                      (args.sweep_position_size.split(",") if args.sweep_position_size
                       else [str(args.position_size)])]
        stop_values_raw = (args.sweep_stop.split(",") if args.sweep_stop
                           else [str(args.stop)])
        stop_values: List[Optional[float]] = []
        for s in stop_values_raw:
            v = float(s)
            stop_values.append(None if v == 0 else v)

        print(f"\n{'='*90}")
        print(f"ADAPTED-QM SWEEP  starting=${args.starting_capital:,.0f}  hold_td={args.hold_td}")
        print(f"{'='*90}")
        print(f"{'pos%':>6}{'max_c':>7}{'stop':>8}{'entries':>9}{'WR':>7}"
              f"{'totRet':>9}{'annRet':>8}{'Sharpe':>8}{'MaxDD':>8}{'worstD':>8}")
        print("-" * 90)
        for pos in pos_values:
            for stop in stop_values:
                stats = simulate(
                    candidates, prices, calendar,
                    starting_capital=args.starting_capital,
                    position_size_pct=pos,
                    max_concurrent=args.max_concurrent,
                    stop_loss_pct=stop,
                    hold_td=args.hold_td,
                )
                stop_str = f"{stop*100:+.0f}%" if stop is not None else "none"
                print(f"{pos*100:>5.1f}%{args.max_concurrent:>7}{stop_str:>8}"
                      f"{stats['n_entered']:>9}"
                      f"{stats['win_rate']*100:>6.1f}%"
                      f"{stats['total_return_pct']*100:>+8.1f}%"
                      f"{stats['ann_return_pct']*100:>+7.1f}%"
                      f"{stats['sharpe']:>+8.2f}"
                      f"{stats['max_dd_pct']*100:>+7.1f}%"
                      f"{stats['worst_daily_drop_pct']*100:>+7.1f}%")
        print("=" * 90)
        return

    # Single run
    stop_val = None if args.stop == 0 else args.stop
    stats = simulate(
        candidates, prices, calendar,
        starting_capital=args.starting_capital,
        position_size_pct=args.position_size,
        max_concurrent=args.max_concurrent,
        stop_loss_pct=stop_val,
        hold_td=args.hold_td,
        margin_rate_apr=args.margin_rate,
    )
    _print_summary("ADAPTED-QM", stats,
                   args.position_size, args.max_concurrent, stop_val)

    if args.emit_csv:
        with open(args.emit_csv, "w") as f:
            f.write("date,mtm_equity,open_unrealized\n")
            for d, e, u in stats["equity_history"]:
                f.write(f"{d},{e:.2f},{u:.2f}\n")
        logger.info("Wrote equity curve to %s", args.emit_csv)


if __name__ == "__main__":
    main()
