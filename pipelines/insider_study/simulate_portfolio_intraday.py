#!/usr/bin/env python3
"""Intraday strategy portfolio updater — keeps simulated portfolios current.

Designed to run every ~10 minutes during market hours plus once at market
open. Lightweight: only processes (a) exits on currently-open positions
and (b) new filings since the last entry. Does NOT re-walk 6 years of
history (that's `simulate_strategy_portfolio.py --extend`, the daily safety
net at 07:00 PT).

Single source of truth for the simulated $100K portfolio per strategy.
Writes to `strategy_portfolio` with execution_source='simulated'.

Mirrors cw_runner's filter+conviction logic, applied to the SIMULATED
portfolio (no Alpaca). Designed to behave like a real live trader that's
managing $100K, just without the broker.

Cadence:
  - Every 10 min during market hours (06:30 PT - 13:00 PT) via launchd
  - Once at 06:30 PT (market open) to process overnight backlog
  - 07:00 PT daily full rebuild remains as a safety net

Idle cash:
  - Held implicitly as "equity − sum(open position capital_at_entry)"
  - When a position opens, idle decreases by capital_at_entry
  - When it closes, idle increases by capital_at_entry × (1 + pnl_pct)
  - The /portfolio/overlay endpoint computes this view; this script
    doesn't materialize idle as a separate position type.

Usage:
    python3 -m pipelines.insider_study.simulate_portfolio_intraday --all
    python3 -m pipelines.insider_study.simulate_portfolio_intraday --strategy quality_momentum
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

import yaml

from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


STRATEGY_CONFIG = {
    "quality_momentum": {
        "yaml": REPO / "strategies/cw_strategies/configs/quality_momentum.yaml",
    },
    "reversal_dip": {
        "yaml": REPO / "strategies/cw_strategies/configs/reversal_dip.yaml",
    },
    "tenb51_surprise": {
        "yaml": REPO / "strategies/cw_strategies/configs/tenb51_surprise.yaml",
    },
}

STARTING_CAPITAL = 100_000.0
STOP_LOSS_PCT = -0.30
MIN_PRICE_FLOOR = 2.0

# Filter logic — share with the full simulator for byte-equivalence
from pipelines.insider_study.simulate_strategy_portfolio import (
    evaluate_filters, count_prior_10b5_1_sells,
)


@dataclass
class OpenPosition:
    row_id: int
    trade_id: int
    ticker: str
    insider_name: Optional[str]
    insider_title: Optional[str]
    company: Optional[str]
    entry_date: str
    entry_price: float
    capital_at_entry: float
    pit_grade: Optional[str]
    career_grade: Optional[str]
    conviction: float
    is_csuite: bool
    is_rare_reversal: bool


def load_open_positions(conn, strategy: str) -> List[OpenPosition]:
    rows = conn.execute(
        """SELECT sp.id, sp.trade_id, sp.ticker, sp.insider_name,
                  sp.insider_title, sp.company,
                  sp.entry_date::text, sp.entry_price, sp.dollar_amount,
                  sp.signal_grade, sp.signal_quality, sp.is_csuite,
                  sp.is_rare_reversal, t.pit_grade, t.career_grade
           FROM strategy_portfolio sp
           LEFT JOIN trades t ON t.trade_id = sp.trade_id
           WHERE sp.strategy = ? AND sp.execution_source = 'simulated'
             AND sp.status = 'open'
           ORDER BY sp.entry_date""",
        (strategy,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(OpenPosition(
            row_id=int(r["id"]),
            trade_id=int(r["trade_id"]),
            ticker=r["ticker"],
            insider_name=r["insider_name"],
            insider_title=r["insider_title"],
            company=r["company"],
            entry_date=r["entry_date"],
            entry_price=float(r["entry_price"] or 0),
            capital_at_entry=float(r["dollar_amount"] or 0),
            pit_grade=r["pit_grade"],
            career_grade=r["career_grade"],
            conviction=float(r["signal_quality"] or 0),
            is_csuite=bool(r["is_csuite"]),
            is_rare_reversal=bool(r["is_rare_reversal"]),
        ))
    return out


def get_realized_pnl_total(conn, strategy: str) -> float:
    row = conn.execute(
        """SELECT COALESCE(SUM(pnl_dollar), 0) AS s
           FROM strategy_portfolio
           WHERE strategy = ? AND execution_source = 'simulated'
             AND status = 'closed'""",
        (strategy,),
    ).fetchone()
    return float(row["s"] or 0)


def get_latest_close(conn, ticker: str) -> Optional[Tuple[str, float]]:
    row = conn.execute(
        """SELECT date::text, close FROM prices.daily_prices
           WHERE ticker = ? AND close > 0
           ORDER BY date DESC LIMIT 1""",
        (ticker,),
    ).fetchone()
    if row:
        return row[0], float(row[1])
    return None


def get_close_for(conn, ticker: str, date_str: str,
                  forward_days: int = 5) -> Optional[Tuple[str, float]]:
    forward_to = (datetime.strptime(date_str, "%Y-%m-%d") +
                  timedelta(days=forward_days)).strftime("%Y-%m-%d")
    row = conn.execute(
        """SELECT date::text, close FROM prices.daily_prices
           WHERE ticker = ? AND date >= ? AND date <= ? AND close > 0
           ORDER BY date LIMIT 1""",
        (ticker, date_str, forward_to),
    ).fetchone()
    if row:
        return row[0], float(row[1])
    return None


def trading_days_between(conn, start_date: str, end_date: str) -> int:
    row = conn.execute(
        """SELECT COUNT(DISTINCT date) AS n FROM prices.daily_prices
           WHERE ticker = 'SPY' AND date > ? AND date <= ?""",
        (start_date, end_date),
    ).fetchone()
    return int(row["n"] or 0)


def close_position(conn, pos: OpenPosition, exit_date: str,
                   exit_price: float, exit_reason: str,
                   equity_after: float):
    pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
    pnl_dollar = pos.capital_at_entry * pnl_pct
    hold_days = (datetime.strptime(exit_date, "%Y-%m-%d") -
                 datetime.strptime(pos.entry_date, "%Y-%m-%d")).days
    conn.execute(
        """UPDATE strategy_portfolio
           SET status = 'closed', exit_date = ?, exit_price = ?,
               hold_days = ?, pnl_pct = ?, pnl_dollar = ?,
               exit_reason = ?, stop_hit = ?, equity_after = ?
           WHERE id = ?""",
        (
            exit_date, round(exit_price, 4), hold_days,
            round(pnl_pct, 6), round(pnl_dollar, 2),
            exit_reason, 1 if exit_reason == "stop" else 0,
            round(equity_after, 2), pos.row_id,
        ),
    )


def open_position(conn, strategy: str, filing: dict, entry_date: str,
                  entry_price: float, capital: float, conviction: float,
                  current_equity: float):
    reasoning = json.dumps({
        "thesis": strategy,
        "filing_date": filing["filing_date"],
        "conviction": conviction,
        "career_grade": filing.get("career_grade"),
        "pit_grade": filing.get("pit_grade"),
        "is_csuite": bool(filing.get("is_csuite")),
        "is_rare_reversal": bool(filing.get("is_rare_reversal")),
    }, default=str)
    pf = conn.execute(
        "SELECT id FROM portfolios WHERE name = ?", (strategy,)
    ).fetchone()
    portfolio_id = int(pf["id"]) if pf else None

    conn.execute(
        """INSERT INTO strategy_portfolio (
              strategy, portfolio_id, trade_id, ticker, trade_type, direction,
              entry_date, entry_price, target_hold, stop_pct,
              position_size, dollar_amount, portfolio_value,
              insider_name, insider_title, is_csuite,
              company, filing_date, trade_date,
              signal_grade, signal_quality, is_rare_reversal,
              status, execution_source, is_estimated, is_live,
              entry_reasoning, instrument, shares
          ) VALUES (
              ?, ?, ?, ?, 'buy_stock', 'long',
              ?, ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              'open', 'simulated', 1, FALSE,
              ?, 'stock', ?
          )""",
        (
            strategy, portfolio_id, filing["trade_id"], filing["ticker"],
            entry_date, round(entry_price, 4),
            42, abs(STOP_LOSS_PCT),
            capital / max(STARTING_CAPITAL, current_equity), capital, current_equity,
            filing.get("insider_name"), filing.get("title"),
            int(bool(filing.get("is_csuite"))),
            filing.get("company"), filing.get("filing_date"), filing.get("trade_date"),
            filing.get("career_grade"), round(conviction, 4),
            1 if filing.get("is_rare_reversal") else 0,
            reasoning,
            int(capital / entry_price) if entry_price else 0,
        ),
    )


def run_one_strategy(strategy_name: str, conn) -> dict:
    cfg = yaml.safe_load(STRATEGY_CONFIG[strategy_name]["yaml"].read_text())
    thesis = (cfg.get("theses") or [{
        "name": strategy_name,
        "filters": cfg["filters"],
        "exit": cfg["exit"],
    }])[0]
    thesis_filters = thesis.get("filters", {})
    hold_td = int(thesis.get("exit", {}).get("hold_days", 30))
    position_size_pct = float(cfg["position_size_pct"])
    max_concurrent = int(cfg["max_concurrent"])
    min_conviction = float(cfg.get("min_conviction", 1.5))

    open_positions = load_open_positions(conn, strategy_name)
    realized = get_realized_pnl_total(conn, strategy_name)
    equity = STARTING_CAPITAL + realized

    n_closed = 0
    n_opened = 0

    # Exit checks
    still_open = []
    for pos in open_positions:
        latest = get_latest_close(conn, pos.ticker)
        if not latest:
            still_open.append(pos)
            continue
        latest_date, latest_close = latest

        if pos.entry_price > 0 and latest_close <= pos.entry_price * (1 + STOP_LOSS_PCT):
            close_position(conn, pos, latest_date, latest_close, "stop", equity)
            pnl_dollar = pos.capital_at_entry * ((latest_close - pos.entry_price) / pos.entry_price)
            equity += pnl_dollar
            n_closed += 1
            logger.info("[%s] STOP %s entry=%.2f latest=%.2f pnl=$%.0f",
                        strategy_name, pos.ticker, pos.entry_price, latest_close, pnl_dollar)
            continue

        td_held = trading_days_between(conn, pos.entry_date, latest_date)
        if td_held >= hold_td:
            close_position(conn, pos, latest_date, latest_close, "time", equity)
            pnl_dollar = pos.capital_at_entry * ((latest_close - pos.entry_price) / pos.entry_price)
            equity += pnl_dollar
            n_closed += 1
            logger.info("[%s] TIME %s entry=%.2f exit=%.2f td=%d pnl=$%.0f",
                        strategy_name, pos.ticker, pos.entry_price, latest_close,
                        td_held, pnl_dollar)
            continue

        still_open.append(pos)
    open_positions = still_open

    # New entries
    lookback_start = (date.today() - timedelta(days=10)).isoformat()
    rows = conn.execute(
        """SELECT t.trade_id, t.insider_id, t.ticker,
                  t.filing_date::text, t.trade_date::text,
                  t.title, COALESCE(i.display_name, i.name) AS insider_name,
                  t.company, t.is_csuite,
                  COALESCE(t.is_duplicate, 0) AS is_duplicate,
                  t.is_rare_reversal, t.consecutive_sells_before,
                  t.dip_1mo, t.dip_3mo,
                  t.above_sma50, t.above_sma200, t.is_largest_ever,
                  t.is_10b5_1, t.is_recurring, t.is_tax_sale, t.cohen_routine,
                  t.pit_grade, t.career_grade
           FROM trades t
           JOIN insiders i ON t.insider_id = i.insider_id
           WHERE t.trans_code = 'P'
             AND t.filing_date >= ?
             AND NOT EXISTS (
               SELECT 1 FROM strategy_portfolio sp
               WHERE sp.strategy = ? AND sp.execution_source = 'simulated'
                 AND sp.trade_id = t.trade_id
             )
           ORDER BY t.filing_date, t.trade_id""",
        (lookback_start, strategy_name),
    ).fetchall()

    held_tickers = {p.ticker for p in open_positions}

    from pipelines.insider_study.conviction_score import compute_conviction
    candidates = []
    for r in rows:
        t = {k: r[k] for k in r.keys()}
        ok, _ = evaluate_filters(thesis_filters, t)
        if not ok:
            continue
        min_10b5 = thesis_filters.get("min_prior_10b5_1_sells")
        if min_10b5:
            n = count_prior_10b5_1_sells(conn, t["insider_id"], t["ticker"], t["filing_date"])
            if n < int(min_10b5):
                continue
        conv = compute_conviction(
            thesis=strategy_name,
            signal_grade=t.get("pit_grade") or "C",
            consecutive_sells=t.get("consecutive_sells_before"),
            dip_1mo=t.get("dip_1mo"),
            is_largest_ever=bool(t.get("is_largest_ever")),
            above_sma50=bool(t.get("above_sma50")),
            above_sma200=bool(t.get("above_sma200")),
            insider_title=t.get("title"),
            is_csuite=bool(t.get("is_csuite")),
        )
        if conv < min_conviction:
            continue
        t["_conv"] = conv
        candidates.append(t)

    candidates.sort(key=lambda c: -c["_conv"])

    for t in candidates:
        if t["ticker"] in held_tickers:
            continue
        if len(open_positions) >= max_concurrent:
            break
        entry = get_close_for(conn, t["ticker"], t["filing_date"])
        if not entry:
            continue
        entry_date, entry_price = entry
        if entry_price < MIN_PRICE_FLOOR:
            continue

        capital = position_size_pct * equity
        open_position(conn, strategy_name, t, entry_date, entry_price, capital,
                      t["_conv"], equity)
        equity -= capital
        held_tickers.add(t["ticker"])
        open_positions.append(OpenPosition(
            row_id=0, trade_id=t["trade_id"], ticker=t["ticker"],
            insider_name=t.get("insider_name"), insider_title=t.get("title"),
            company=t.get("company"),
            entry_date=entry_date, entry_price=entry_price,
            capital_at_entry=capital,
            pit_grade=t.get("pit_grade"), career_grade=t.get("career_grade"),
            conviction=t["_conv"],
            is_csuite=bool(t.get("is_csuite")),
            is_rare_reversal=bool(t.get("is_rare_reversal")),
        ))
        n_opened += 1
        logger.info("[%s] OPEN %s @ %.2f $%.0f conv=%.2f",
                    strategy_name, t["ticker"], entry_price, capital, t["_conv"])

    conn.commit()
    return {
        "n_closed": n_closed,
        "n_opened": n_opened,
        "equity": round(equity, 0),
        "open_positions": len(open_positions),
    }


def is_market_open() -> bool:
    """Mon-Fri 06:30 PT - 13:00 PT (= 09:30 ET - 16:00 ET)."""
    from zoneinfo import ZoneInfo
    pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    if pt.weekday() >= 5:
        return False
    open_t = pt.replace(hour=6, minute=30, second=0, microsecond=0)
    close_t = pt.replace(hour=13, minute=0, second=0, microsecond=0)
    return open_t <= pt <= close_t


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=list(STRATEGY_CONFIG))
    p.add_argument("--all", action="store_true")
    p.add_argument("--force", action="store_true",
                   help="Skip the market-hours gate (use for manual runs / debugging)")
    args = p.parse_args()
    if not args.strategy and not args.all:
        p.error("specify --strategy or --all")

    if not args.force and not is_market_open():
        logger.info("Outside market hours — no-op (use --force to override).")
        return

    strategies = list(STRATEGY_CONFIG) if args.all else [args.strategy]
    conn = get_connection()
    t0 = time.monotonic()
    for s in strategies:
        result = run_one_strategy(s, conn)
        logger.info("[%s] cycle: closed=%d opened=%d equity=$%s open=%d",
                    s, result["n_closed"], result["n_opened"],
                    f"{result['equity']:,.0f}", result["open_positions"])
    conn.close()
    logger.info("Intraday cycle complete in %.1fs", time.monotonic() - t0)


if __name__ == "__main__":
    main()
