#!/usr/bin/env python3
"""Counter-factual test: RD strategy with at_capacity=rotate vs skip.

Current RD config: at_capacity=skip — when 10 positions are open, new
high-conviction signals are dropped. The V3 audit data shows blocked RD
signals have AVG conviction 5.29 vs entered 4.34, suggesting we're
systematically rejecting higher-quality opportunities to hold lower-quality
ones.

This script replays the RD simulation chronologically TWO ways:
  A) SKIP   — baseline, matches current `at_capacity=skip` behavior
  B) ROTATE — when at max_concurrent AND new_conv > min(held_conv):
              close the min-conv held position at today's close, open new.

Both simulations use:
  - Same candidate set (RD-conviction-passing trades from trade_decision_audit)
  - Same entry rule (filing_date close, fall forward up to 5 days)
  - Same hold rule (21 trading days fixed_hold)
  - Same position size (10% of $100K = $10,000 per slot, fixed)
  - Same max_concurrent (10)

Outputs side-by-side stats:
  - Total entries
  - Total swaps (rotate only)
  - Total closed trades + open at end
  - Net PnL ($)
  - Win rate
  - Per-swap diagnostics: how long was the swapped-out position held,
    what return it had at swap, what return it would have had at full hold

Usage (on Studio):
    python3 -m pipelines.insider_study.rd_swap_test
    python3 -m pipelines.insider_study.rd_swap_test --strategy reversal_dip
    python3 -m pipelines.insider_study.rd_swap_test --emit-csv /tmp/rd_swaps.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


STARTING_CAPITAL = 100_000.0
POSITION_SIZE_PCT = 0.10
DOLLAR_AMOUNT = STARTING_CAPITAL * POSITION_SIZE_PCT  # $10,000
HOLD_TD_DEFAULT = {"reversal_dip": 21, "quality_momentum": 42, "tenb51_surprise": 60}
MAX_CONCURRENT = 10


@dataclass
class Position:
    trade_id: int
    ticker: str
    entry_date: str        # actual entry date (trading day)
    entry_price: float
    conviction: float
    target_exit_date: str  # entry + hold_td trading days (precomputed)
    days_held: int = 0


@dataclass
class ClosedTrade:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    conviction: float
    exit_date: str
    exit_price: float
    pnl_pct: float
    pnl_dollar: float
    exit_reason: str       # 'time_exit' | 'swapped_out'
    swapped_in_for: int | None = None  # trade_id that took its slot (rotate sim only)
    full_hold_exit_price: float | None = None
    full_hold_exit_date: str | None = None
    full_hold_pnl_pct: float | None = None


def _fetch_candidates(conn, strategy: str) -> list[dict]:
    """Conviction-passing trades from the V3 simulation audit. These are
    every signal the strategy generated, regardless of whether capacity
    accepted them. This is the universe both sims walk through."""
    rows = conn.execute(
        """
        SELECT
          tda.trade_id, tda.ticker, tda.filing_date::text AS filing_date,
          tda.conviction, tda.pit_grade,
          t.career_grade,
          COALESCE(i.display_name, i.name) AS insider_name
        FROM trade_decision_audit tda
        JOIN trades t ON t.trade_id = tda.trade_id
        LEFT JOIN insiders i ON t.insider_id = i.insider_id
        WHERE tda.source = 'simulation'
          AND tda.strategy = ?
          AND tda.stage = 'conviction'
          AND tda.passed = TRUE
        ORDER BY tda.filing_date, tda.conviction DESC, tda.trade_id
        """,
        (strategy,),
    ).fetchall()
    out = []
    for r in rows:
        if hasattr(r, "_asdict"):
            d = r._asdict()
        elif hasattr(r, "keys"):
            d = {k: r[k] for k in r.keys()}
        else:
            d = dict(r)
        out.append(d)
    return out


def _load_calendar(conn, start: str, end: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT date::text AS d FROM prices.daily_prices
        WHERE date >= ? AND date <= ?
        ORDER BY d
        """,
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def _shift_td(calendar: list[str], anchor: str, n: int) -> str | None:
    if not calendar:
        return None
    lo, hi = 0, len(calendar) - 1
    idx = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if calendar[mid] >= anchor:
            idx = mid
            hi = mid - 1
        else:
            lo = mid + 1
    if idx is None:
        return None
    target = idx + n
    if target >= len(calendar):
        return None
    return calendar[target]


def _preload_prices(conn, tickers: set[str], start: str, end: str) -> dict[tuple[str, str], float]:
    """Bulk-load close prices for all needed tickers/dates."""
    if not tickers:
        return {}
    logger.info("Preloading prices for %d tickers …", len(tickers))
    out: dict[tuple[str, str], float] = {}
    BATCH = 200
    tickers_list = sorted(tickers)
    for i in range(0, len(tickers_list), BATCH):
        chunk = tickers_list[i : i + BATCH]
        placeholders = ",".join(["?"] * len(chunk))
        rows = conn.execute(
            f"""
            SELECT ticker, date::text AS d, close
            FROM prices.daily_prices
            WHERE ticker IN ({placeholders})
              AND date >= ? AND date <= ?
            """,
            tuple(chunk) + (start, end),
        ).fetchall()
        for r in rows:
            tk = r[0]
            d = r[1]
            c = r[2]
            if c and c > 0:
                out[(tk, d)] = float(c)
    logger.info("  %d (ticker, date) close prices cached", len(out))
    return out


def _get_close(price_cache: dict, calendar: list[str], ticker: str,
               on_or_after: str, max_lookahead_td: int = 5) -> tuple[str, float] | None:
    """Find first available close on or after `on_or_after` (search next N
    trading days). Trading-day-aware, NOT calendar-day."""
    # Snap to first calendar date >= on_or_after
    lo, hi = 0, len(calendar) - 1
    idx = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if calendar[mid] >= on_or_after:
            idx = mid
            hi = mid - 1
        else:
            lo = mid + 1
    if idx is None:
        return None
    for offset in range(max_lookahead_td + 1):
        ti = idx + offset
        if ti >= len(calendar):
            return None
        d = calendar[ti]
        c = price_cache.get((ticker, d))
        if c is not None:
            return d, c
    return None


def _simulate(
    candidates: list[dict],
    price_cache: dict,
    calendar: list[str],
    hold_td: int,
    rotate: bool,
    advantage: float = 0.0,
) -> tuple[list[ClosedTrade], list[Position]]:
    """Run one sim mode. Returns (closed_trades, open_at_end)."""
    # Bucket candidates by filing_date for per-day processing
    by_date: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        by_date[c["filing_date"]].append(c)

    held: list[Position] = []
    closed: list[ClosedTrade] = []
    today_str = date.today().isoformat()
    swaps = 0

    # Process every trading day from min(candidate.filing_date) onward
    if not by_date:
        return closed, held
    sim_start = min(by_date.keys())
    start_idx = None
    for i, d in enumerate(calendar):
        if d >= sim_start:
            start_idx = i
            break
    if start_idx is None:
        return closed, held

    for d in calendar[start_idx:]:
        if d > today_str:
            break

        # ---- 1) Exits: close any position whose target_exit_date <= d ----
        kept = []
        for pos in held:
            if pos.target_exit_date <= d:
                exit_row = _get_close(price_cache, calendar, pos.ticker, pos.target_exit_date)
                if exit_row is None:
                    # No price near target — drop the position silently (rare)
                    continue
                exit_date, exit_price = exit_row
                pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
                pnl_dollar = DOLLAR_AMOUNT * pnl_pct
                closed.append(ClosedTrade(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    conviction=pos.conviction,
                    exit_date=exit_date, exit_price=exit_price,
                    pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    exit_reason="time_exit",
                ))
            else:
                kept.append(pos)
        held = kept

        # ---- 2) Entries for today, sorted by conviction DESC ----
        today_cands = sorted(by_date.get(d, []), key=lambda c: -c["conviction"])
        held_tickers = {p.ticker for p in held}

        for c in today_cands:
            ticker = c["ticker"]
            if ticker in held_tickers:
                continue  # dedup

            # Open entry — must have a valid price
            entry_row = _get_close(price_cache, calendar, ticker, d)
            if entry_row is None:
                continue
            entry_date, entry_price = entry_row
            if entry_price < 2.0:
                continue  # match grid_search min_price floor

            target_exit = _shift_td(calendar, entry_date, hold_td)
            if target_exit is None:
                # Cannot determine exit — skip
                continue

            new_pos = Position(
                trade_id=c["trade_id"], ticker=ticker,
                entry_date=entry_date, entry_price=entry_price,
                conviction=float(c["conviction"]),
                target_exit_date=target_exit,
            )

            if len(held) < MAX_CONCURRENT:
                held.append(new_pos)
                held_tickers.add(ticker)
                continue

            if not rotate:
                # Skip baseline: at capacity, drop the signal
                continue

            # ROTATE: find min-conviction held position; swap if new_conv strictly greater
            min_idx = min(range(len(held)), key=lambda i: held[i].conviction)
            victim = held[min_idx]
            if new_pos.conviction < victim.conviction + advantage:
                continue  # no improvement (or below advantage threshold)

            # Close victim at today's close (NOT its target_exit_date)
            victim_exit = _get_close(price_cache, calendar, victim.ticker, d)
            if victim_exit is None:
                continue
            v_exit_date, v_exit_price = victim_exit
            v_pnl_pct = (v_exit_price - victim.entry_price) / victim.entry_price
            v_pnl_dollar = DOLLAR_AMOUNT * v_pnl_pct
            # Counter-factual: what would victim have earned at full hold?
            full_hold = _get_close(price_cache, calendar, victim.ticker, victim.target_exit_date)
            if full_hold:
                fh_date, fh_price = full_hold
                fh_pnl = (fh_price - victim.entry_price) / victim.entry_price
            else:
                fh_date, fh_price, fh_pnl = None, None, None

            closed.append(ClosedTrade(
                trade_id=victim.trade_id, ticker=victim.ticker,
                entry_date=victim.entry_date, entry_price=victim.entry_price,
                conviction=victim.conviction,
                exit_date=v_exit_date, exit_price=v_exit_price,
                pnl_pct=v_pnl_pct, pnl_dollar=v_pnl_dollar,
                exit_reason="swapped_out",
                swapped_in_for=new_pos.trade_id,
                full_hold_exit_price=fh_price,
                full_hold_exit_date=fh_date,
                full_hold_pnl_pct=fh_pnl,
            ))
            held[min_idx] = new_pos
            held_tickers.discard(victim.ticker)
            held_tickers.add(ticker)
            swaps += 1

    # Tally swaps via the closed list (swapped_in_for is the marker)
    # so the caller doesn't need a separate counter argument.
    return closed, held


def _report(name: str, closed: list[ClosedTrade], open_at_end: list[Position]) -> dict:
    n_entries = len(closed) + len(open_at_end)
    n_closed = len(closed)
    n_swaps = sum(1 for c in closed if c.exit_reason == "swapped_out")
    pnl_total = sum(c.pnl_dollar for c in closed)
    win_count = sum(1 for c in closed if c.pnl_pct > 0)
    win_rate = win_count / n_closed if n_closed else 0.0
    avg_pnl_pct = (sum(c.pnl_pct for c in closed) / n_closed) if n_closed else 0.0

    # Swap diagnostics (rotate only)
    swap_diags = {}
    if n_swaps:
        swapped = [c for c in closed if c.exit_reason == "swapped_out"]
        held_days = []
        swap_pnls = []
        full_hold_pnls = []
        for c in swapped:
            try:
                days = (datetime.strptime(c.exit_date, "%Y-%m-%d") -
                        datetime.strptime(c.entry_date, "%Y-%m-%d")).days
                held_days.append(days)
            except Exception:
                pass
            swap_pnls.append(c.pnl_pct)
            if c.full_hold_pnl_pct is not None:
                full_hold_pnls.append(c.full_hold_pnl_pct)
        swap_diags = {
            "n_swaps": n_swaps,
            "avg_days_held_before_swap": (sum(held_days) / len(held_days)) if held_days else 0,
            "avg_pnl_pct_at_swap": (sum(swap_pnls) / len(swap_pnls)) if swap_pnls else 0,
            "avg_full_hold_pnl_pct_if_kept": (sum(full_hold_pnls) / len(full_hold_pnls)) if full_hold_pnls else 0,
        }

    out = {
        "mode": name,
        "n_entries": n_entries,
        "n_closed": n_closed,
        "n_open_at_end": len(open_at_end),
        "pnl_total_dollar": round(pnl_total, 0),
        "win_rate_pct": round(win_rate * 100, 1),
        "avg_pnl_pct": round(avg_pnl_pct * 100, 2),
        **swap_diags,
    }

    logger.info("[%s] %s", name, out)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="reversal_dip",
                   help="Strategy to test (default: reversal_dip)")
    p.add_argument("--emit-csv", default=None,
                   help="If set, write per-trade detail to this CSV path")
    p.add_argument("--advantage", type=float, default=0.0,
                   help="replacement_advantage for ROTATE: rotation only "
                        "fires when new_conv >= weakest_conv + advantage. "
                        "Higher = more conservative.")
    p.add_argument("--sweep", default=None,
                   help="Comma-separated advantage values; runs all and "
                        "prints a sweep table. Overrides --advantage.")
    args = p.parse_args()

    hold_td = HOLD_TD_DEFAULT.get(args.strategy, 21)
    logger.info("Strategy=%s hold_td=%d max_concurrent=%d position=$%d",
                args.strategy, hold_td, MAX_CONCURRENT, int(DOLLAR_AMOUNT))

    conn = get_connection()
    candidates = _fetch_candidates(conn, args.strategy)
    logger.info("%d candidate trades loaded", len(candidates))
    if not candidates:
        logger.error("No candidates — run simulate_decision_audit first.")
        conn.close()
        return

    tickers = {c["ticker"] for c in candidates}
    cal_start = (datetime.strptime(min(c["filing_date"] for c in candidates), "%Y-%m-%d")
                 - timedelta(days=10)).strftime("%Y-%m-%d")
    cal_end = (datetime.strptime(max(c["filing_date"] for c in candidates), "%Y-%m-%d")
               + timedelta(days=hold_td * 2 + 30)).strftime("%Y-%m-%d")
    calendar = _load_calendar(conn, cal_start, cal_end)
    price_cache = _preload_prices(conn, tickers, cal_start, cal_end)
    conn.close()

    # Run baseline SKIP once (advantage doesn't affect it)
    closed_skip, open_skip = _simulate(candidates, price_cache, calendar,
                                       hold_td=hold_td, rotate=False, advantage=0)
    skip_stats = _report("SKIP", closed_skip, open_skip)

    if args.sweep:
        # Multi-advantage sweep
        adv_values = [float(x.strip()) for x in args.sweep.split(",")]
        print("\n" + "=" * 90)
        print(f"ADVANTAGE SWEEP  strategy={args.strategy}  baseline SKIP pnl=${skip_stats['pnl_total_dollar']:,.0f}")
        print("=" * 90)
        print(f"{'advantage':>10}  {'n_entries':>10}  {'n_swaps':>8}  {'pnl_total':>12}  "
              f"{'win_rate':>9}  {'vs_skip':>10}")
        sweep_rows = []
        for adv in adv_values:
            closed_rot, open_rot = _simulate(candidates, price_cache, calendar,
                                              hold_td=hold_td, rotate=True,
                                              advantage=adv)
            s = _report(f"ROT_adv={adv}", closed_rot, open_rot)
            delta = s["pnl_total_dollar"] - skip_stats["pnl_total_dollar"]
            sweep_rows.append((adv, s, delta))
            print(f"{adv:>10.2f}  {s['n_entries']:>10}  {s.get('n_swaps', 0):>8}  "
                  f"${s['pnl_total_dollar']:>11,.0f}  {s['win_rate_pct']:>8.1f}%  "
                  f"${delta:>+9,.0f}")
        # Pick best
        best_adv, best_s, best_delta = max(sweep_rows, key=lambda x: x[2])
        print("-" * 90)
        print(f"BEST: advantage={best_adv}  Δ vs SKIP=${best_delta:+,.0f} "
              f"({best_s['n_swaps']} swaps, {best_s['win_rate_pct']}% WR)")
        print("=" * 90)
        return  # skip the standalone ROTATE report

    closed_rot, open_rot = _simulate(candidates, price_cache, calendar,
                                     hold_td=hold_td, rotate=True,
                                     advantage=args.advantage)
    rot_stats = _report(f"ROTATE(adv={args.advantage})", closed_rot, open_rot)

    # Side-by-side diff
    print("\n" + "=" * 70)
    print(f"RD swap-test summary  strategy={args.strategy}  hold_td={hold_td}")
    print("=" * 70)
    keys = ["n_entries", "n_closed", "n_open_at_end",
            "pnl_total_dollar", "win_rate_pct", "avg_pnl_pct"]
    print(f"{'metric':<28}{'SKIP':>15}{'ROTATE':>15}{'Δ (rot−skip)':>15}")
    for k in keys:
        a = skip_stats.get(k, 0)
        b = rot_stats.get(k, 0)
        diff = (b - a) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else "—"
        print(f"{k:<28}{a:>15}{b:>15}{diff:>15}")
    print("-" * 70)
    print("ROTATE-only diagnostics:")
    for k in ["n_swaps", "avg_days_held_before_swap",
              "avg_pnl_pct_at_swap", "avg_full_hold_pnl_pct_if_kept"]:
        print(f"  {k}: {rot_stats.get(k, 0)}")
    print("=" * 70)

    # CSV emit (rotate only — has the rich diag fields)
    if args.emit_csv:
        with open(args.emit_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["mode", "trade_id", "ticker", "entry_date", "entry_price",
                        "exit_date", "exit_price", "conviction", "exit_reason",
                        "pnl_pct", "pnl_dollar",
                        "swapped_in_for", "full_hold_exit_date",
                        "full_hold_exit_price", "full_hold_pnl_pct"])
            for c in closed_skip:
                w.writerow(["skip", c.trade_id, c.ticker, c.entry_date, c.entry_price,
                            c.exit_date, c.exit_price, c.conviction, c.exit_reason,
                            c.pnl_pct, c.pnl_dollar, "", "", "", ""])
            for c in closed_rot:
                w.writerow(["rotate", c.trade_id, c.ticker, c.entry_date, c.entry_price,
                            c.exit_date, c.exit_price, c.conviction, c.exit_reason,
                            c.pnl_pct, c.pnl_dollar, c.swapped_in_for or "",
                            c.full_hold_exit_date or "", c.full_hold_exit_price or "",
                            c.full_hold_pnl_pct if c.full_hold_pnl_pct is not None else ""])
        logger.info("Wrote per-trade detail to %s", args.emit_csv)


if __name__ == "__main__":
    main()
