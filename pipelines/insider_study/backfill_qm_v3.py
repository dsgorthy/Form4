#!/usr/bin/env python3
"""Backfill strategy_portfolio with QM V3-only-qualifying trades.

Context: QM yaml swapped pit_grade (V2) → career_grade (V3) on 2026-05-07.
The existing `strategy_portfolio` rows for `quality_momentum` were built
under V2 rules and never re-derived after the swap. A V3-PIT-correct
simulation (simulate_decision_audit.py + patched pit_scoring.py) shows
some trades that V3 considers entry-qualifying are missing from portfolio.

This script picks them up.

Source of truth for "should-have-entered":
    trade_decision_audit
    WHERE source = 'simulation'
      AND strategy = 'quality_momentum'
      AND stage = 'capacity'
      AND passed = TRUE
      AND trade_id NOT IN (SELECT trade_id FROM strategy_portfolio
                           WHERE strategy = 'quality_momentum')

Per-trade outputs:
  - entry_date = filing_date
  - entry_price = prices.daily_prices.close at filing_date (fallback: next td)
  - exit_date  = filing_date + 42 trading days
  - exit_price = close on exit_date
  - position_size_pct = 0.10  (10% of starting capital, matches yaml)
  - dollar_amount = $10,000 (10% × $100K starting capital, fixed sizing)
  - execution_source = 'backtest_v3'  (distinct from existing 'backtest' rows)

Trades with missing prices are skipped (logged). Trades whose exit date is
still in the future are written with status='open'.

Usage (on Studio):
    python3 -m pipelines.insider_study.backfill_qm_v3 --dry-run
    python3 -m pipelines.insider_study.backfill_qm_v3
    python3 -m pipelines.insider_study.backfill_qm_v3 --strategy quality_momentum --replace
"""
from __future__ import annotations

import argparse
import logging
import sys
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
DOLLAR_AMOUNT = STARTING_CAPITAL * POSITION_SIZE_PCT  # $10,000 per QM position
HOLD_TRADING_DAYS = 42  # QM yaml: hold_days: 42 (trading days)


def _fetch_missed(conn, strategy: str) -> list[dict]:
    """Return V3-simulator capacity-passed trades not in strategy_portfolio.

    Dedup is two-layered:
      1. NOT EXISTS by trade_id — same exact trade isn't already there.
      2. NOT EXISTS by (ticker, filing_date) for paper/live rows — avoids
         the BW-style case where two different insiders bought the same
         ticker on the same day and the live runner picked one; backfilling
         the other would falsely double-count.
    """
    rows = conn.execute(
        """
        SELECT
          tda.trade_id, tda.ticker, tda.filing_date::text AS filing_date,
          tda.conviction, tda.pit_grade,
          t.career_grade, t.insider_id,
          COALESCE(i.display_name, i.name) AS insider_name,
          t.title AS insider_title, t.is_csuite, t.is_rare_reversal,
          t.company, t.trade_date::text AS trade_date
        FROM trade_decision_audit tda
        JOIN trades t ON t.trade_id = tda.trade_id
        LEFT JOIN insiders i ON t.insider_id = i.insider_id
        WHERE tda.source = 'simulation'
          AND tda.strategy = ?
          AND tda.stage = 'capacity'
          AND tda.passed = TRUE
          AND NOT EXISTS (
            SELECT 1 FROM strategy_portfolio sp
            WHERE sp.strategy = ? AND sp.trade_id = tda.trade_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM strategy_portfolio sp
            WHERE sp.strategy = ?
              AND sp.execution_source IN ('paper', 'live')
              AND sp.ticker = tda.ticker
              AND sp.filing_date::date = tda.filing_date::date
          )
        ORDER BY tda.filing_date, tda.trade_id
        """,
        (strategy, strategy, strategy),
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


def _trading_calendar(conn, start: str, end: str) -> list[str]:
    """Dates that have ANY price row in prices.daily_prices. Used as a
    trading calendar proxy. Sorted ascending."""
    rows = conn.execute(
        """
        SELECT DISTINCT date::text AS d FROM prices.daily_prices
        WHERE date >= ? AND date <= ?
        ORDER BY d
        """,
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def _shift_trading_days(calendar: list[str], anchor: str, n_days: int) -> str | None:
    """Return the calendar date `n_days` trading days after `anchor`.
    If anchor is between trading days, snap forward to next trading day.
    Returns None if anchor is past calendar end or shift overshoots."""
    if not calendar:
        return None
    # Find the first calendar date >= anchor
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
    target = idx + n_days
    if target >= len(calendar):
        return None
    return calendar[target]


def _get_close(conn, ticker: str, on_or_after: str, max_lookahead_days: int = 5) -> tuple[str, float] | None:
    """Get the (date, close) for `ticker` on `on_or_after` or the next
    available trading day within `max_lookahead_days` calendar days.
    Returns None if no price found."""
    rows = conn.execute(
        """
        SELECT date::text, close FROM prices.daily_prices
        WHERE ticker = ? AND date >= ? AND date <= ?
        ORDER BY date LIMIT 1
        """,
        (
            ticker, on_or_after,
            (datetime.strptime(on_or_after, "%Y-%m-%d") + timedelta(days=max_lookahead_days)).strftime("%Y-%m-%d"),
        ),
    ).fetchone()
    if rows and rows[1] and rows[1] > 0:
        return rows[0], float(rows[1])
    return None


def _resolve_portfolio_id(conn, strategy: str) -> int | None:
    row = conn.execute("SELECT id FROM portfolios WHERE name = ?", (strategy,)).fetchone()
    return int(row[0]) if row else None


def _insert_row(conn, strategy: str, portfolio_id: int | None, t: dict,
                entry_date: str, entry_price: float,
                exit_date: str | None, exit_price: float | None,
                status: str, hold_days: int, exit_reason: str | None) -> None:
    """Insert one strategy_portfolio row."""
    if exit_price is not None:
        pnl_pct = (exit_price - entry_price) / entry_price
        pnl_dollar = DOLLAR_AMOUNT * pnl_pct
    else:
        pnl_pct = None
        pnl_dollar = None
    shares = int(DOLLAR_AMOUNT / entry_price) if entry_price else 0

    conn.execute(
        """
        INSERT INTO strategy_portfolio (
            strategy, portfolio_id, trade_id, ticker,
            trade_type, direction,
            entry_date, entry_price, exit_date, exit_price, hold_days,
            target_hold, stop_pct, stop_hit, pnl_pct, pnl_dollar,
            position_size, dollar_amount, shares,
            insider_name, insider_title, is_csuite,
            company, filing_date, trade_date,
            signal_grade, signal_quality, is_rare_reversal,
            exit_reason, status,
            execution_source, is_estimated, is_live,
            instrument
        ) VALUES (
            ?, ?, ?, ?,
            'buy_stock', 'long',
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            'backtest_v3', 1, FALSE,
            'stock'
        )
        """,
        (
            strategy, portfolio_id, t["trade_id"], t["ticker"],
            entry_date, entry_price, exit_date, exit_price, hold_days,
            HOLD_TRADING_DAYS, 0.0, 0, pnl_pct, pnl_dollar,
            POSITION_SIZE_PCT, DOLLAR_AMOUNT, shares,
            t.get("insider_name"), t.get("insider_title"), t.get("is_csuite"),
            t.get("company"), t.get("filing_date"), t.get("trade_date"),
            # Strategy currently filters on career_grade — record that
            t.get("career_grade"), t.get("conviction"), t.get("is_rare_reversal"),
            exit_reason, status,
        ),
    )


def backfill(strategy: str, dry_run: bool, replace: bool) -> dict:
    conn = get_connection()
    counts = {"missed": 0, "inserted": 0, "skipped_no_entry_price": 0,
              "skipped_no_exit_price": 0, "still_open": 0}

    missed = _fetch_missed(conn, strategy)
    counts["missed"] = len(missed)
    logger.info("[%s] %d V3-only missed trades to backfill", strategy, len(missed))
    if not missed:
        conn.close()
        return counts

    today = date.today().isoformat()
    cal_start = min(t["filing_date"] for t in missed)
    cal_end = (
        datetime.strptime(max(today, max(t["filing_date"] for t in missed)), "%Y-%m-%d")
        + timedelta(days=HOLD_TRADING_DAYS * 2 + 30)
    ).strftime("%Y-%m-%d")
    calendar = _trading_calendar(conn, cal_start, cal_end)
    logger.info("[%s] trading calendar: %d days (%s → %s)", strategy, len(calendar), cal_start, cal_end)

    portfolio_id = _resolve_portfolio_id(conn, strategy)

    if replace and not dry_run:
        n = conn.execute(
            "DELETE FROM strategy_portfolio WHERE strategy = ? AND execution_source = 'backtest_v3'",
            (strategy,),
        ).rowcount
        conn.commit()
        logger.info("[%s] cleared %d prior backtest_v3 rows", strategy, n or 0)

    for t in missed:
        ticker = t["ticker"]
        filing_date = t["filing_date"]

        entry = _get_close(conn, ticker, filing_date)
        if entry is None:
            counts["skipped_no_entry_price"] += 1
            logger.warning("  [%s/%s] no entry price near %s — skipped",
                           ticker, t["trade_id"], filing_date)
            continue
        entry_date, entry_price = entry

        target_exit = _shift_trading_days(calendar, entry_date, HOLD_TRADING_DAYS)
        if target_exit is None:
            # Hold still in-flight (target past calendar end) — write as open
            counts["still_open"] += 1
            if not dry_run:
                _insert_row(conn, strategy, portfolio_id, t,
                            entry_date, entry_price,
                            exit_date=None, exit_price=None,
                            status="open", hold_days=0, exit_reason=None)
                counts["inserted"] += 1
            continue

        if target_exit > today:
            # Future exit — write as open
            counts["still_open"] += 1
            if not dry_run:
                _insert_row(conn, strategy, portfolio_id, t,
                            entry_date, entry_price,
                            exit_date=None, exit_price=None,
                            status="open", hold_days=0, exit_reason=None)
                counts["inserted"] += 1
            continue

        exit_row = _get_close(conn, ticker, target_exit)
        if exit_row is None:
            counts["skipped_no_exit_price"] += 1
            logger.warning("  [%s/%s] no exit price near %s — skipped",
                           ticker, t["trade_id"], target_exit)
            continue
        exit_date, exit_price = exit_row
        hold_days_actual = (datetime.strptime(exit_date, "%Y-%m-%d") -
                            datetime.strptime(entry_date, "%Y-%m-%d")).days

        if not dry_run:
            _insert_row(conn, strategy, portfolio_id, t,
                        entry_date, entry_price,
                        exit_date=exit_date, exit_price=exit_price,
                        status="closed", hold_days=hold_days_actual,
                        exit_reason="time_exit")
            counts["inserted"] += 1

        if counts["inserted"] % 25 == 0:
            conn.commit()

    if not dry_run:
        conn.commit()

    # Aggregate stats
    if not dry_run:
        agg = conn.execute(
            """
            SELECT
              COUNT(*) AS n,
              COUNT(pnl_pct) AS n_closed,
              ROUND(AVG(pnl_pct)::numeric * 100, 2) AS avg_pnl_pct,
              ROUND(SUM(pnl_dollar)::numeric, 0) AS sum_pnl_dollar,
              ROUND(SUM(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0.0 END)
                    / NULLIF(COUNT(pnl_pct), 0) * 100, 1) AS win_rate
            FROM strategy_portfolio
            WHERE strategy = ? AND execution_source = 'backtest_v3'
            """,
            (strategy,),
        ).fetchone()
        if agg:
            logger.info("[%s] backtest_v3 totals: n=%s closed=%s "
                        "avg_pnl=%s%% sum_pnl=$%s win_rate=%s%%",
                        strategy, agg[0], agg[1], agg[2], agg[3], agg[4])

    conn.close()
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="quality_momentum",
                   help="Strategy name (default: quality_momentum)")
    p.add_argument("--dry-run", action="store_true",
                   help="Report what would be inserted, write nothing")
    p.add_argument("--replace", action="store_true",
                   help="Delete existing backtest_v3 rows first")
    args = p.parse_args()

    counts = backfill(args.strategy, args.dry_run, args.replace)
    logger.info("Done. counts=%s", counts)


if __name__ == "__main__":
    main()
