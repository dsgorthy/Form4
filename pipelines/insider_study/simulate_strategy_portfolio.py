#!/usr/bin/env python3
"""Unified strategy portfolio simulator — writes strategy_portfolio rows.

What this is:
  A single source of truth for each insider strategy's portfolio state,
  treating the strategy as a live $100K fund running since its start_date.
  Walks forward day-by-day applying current strategy logic (career_grade
  filter for QM, etc.), opening/closing positions with proper compounding
  sizing, marking positions still in their hold window as OPEN with no
  exit_date.

  Replaces the previous mess of:
    - Original backtest rows (variable sizing)
    - backfill_v3 rows (fixed $10K cap — the bug Derek caught)
    - Paper account rows scattered separately

  The dashboard reads from `strategy_portfolio` and sees ONE coherent
  track record per strategy. Open positions reflect what would currently
  be held under current strategy rules.

Modes:
  --rebuild    Wipe and re-simulate from scratch (one-shot, ~minutes per strategy)
  --extend     Walk forward from latest known state to today (incremental, daily job)

PIT compliance:
  - Uses `trades.career_grade` (rebuilt 2026-05-12 with patched scorer)
  - Uses pre-computed `trades.consecutive_sells_before / dip_3mo / above_sma50 /
    is_largest_ever / is_recurring / is_tax_sale / cohen_routine / is_10b5_1`
    (all PIT-clean per signal_registry memory)
  - Conviction calc receives `pit_grade` as signal_grade (mirrors live cw_runner)
  - Entry/exit prices use prices.daily_prices closes (no future leak)
  - -30% stop applied retroactively per Derek's directive

Usage:
    python3 -m pipelines.insider_study.simulate_strategy_portfolio \\
        --strategy quality_momentum --rebuild

    python3 -m pipelines.insider_study.simulate_strategy_portfolio --all --rebuild
    python3 -m pipelines.insider_study.simulate_strategy_portfolio --all --extend
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
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


# ── Strategy registry ───────────────────────────────────────────────────

STRATEGY_CONFIG = {
    "quality_momentum": {
        "yaml": REPO / "strategies/cw_strategies/configs/quality_momentum.yaml",
        "start_date": "2023-01-01",
    },
    "reversal_dip": {
        "yaml": REPO / "strategies/cw_strategies/configs/reversal_dip.yaml",
        "start_date": "2023-01-01",
    },
    "tenb51_surprise": {
        "yaml": REPO / "strategies/cw_strategies/configs/tenb51_surprise.yaml",
        "start_date": "2023-01-01",
    },
}

STARTING_CAPITAL = 100_000.0
STOP_LOSS_PCT = -0.30   # Derek's override 2026-05-12, applied to all strategies retroactively


# ── State types ─────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    """A currently-held position during the simulation."""
    trade_id: int
    ticker: str
    insider_id: int
    insider_name: Optional[str]
    insider_title: Optional[str]
    company: Optional[str]
    entry_date: str
    entry_price: float
    capital_at_entry: float       # $ allocated at entry (position_size_pct × equity_at_entry)
    target_exit_idx: int          # calendar index when hold_td expires
    stop_price: float             # entry_price × (1 + stop_loss_pct)
    pit_grade: Optional[str]
    career_grade: Optional[str]
    conviction: float
    is_csuite: bool
    is_rare_reversal: bool
    days_held: int = 0
    last_seen_close: Optional[float] = None  # for stale-exit fallback (set as we walk)
    last_seen_date: Optional[str] = None


@dataclass
class ClosedPosition:
    """A position that has exited."""
    trade_id: int
    ticker: str
    insider_id: int
    insider_name: Optional[str]
    insider_title: Optional[str]
    company: Optional[str]
    entry_date: str
    entry_price: float
    capital_at_entry: float
    exit_date: str
    exit_price: float
    exit_reason: str              # 'time' | 'stop'
    hold_days: int
    pnl_pct: float
    pnl_dollar: float
    pit_grade: Optional[str]
    career_grade: Optional[str]
    conviction: float
    is_csuite: bool
    is_rare_reversal: bool
    equity_after: float


# ── Filter evaluation ────────────────────────────────────────────────────
# Moved to framework.decision.filters as part of Stage 3 (shared engine).
# Re-exported here so callers that imported from this module keep working
# during the migration window.
from framework.decision.filters import evaluate_filters  # noqa: F401


def count_prior_10b5_1_sells(conn, insider_id, ticker, as_of):
    """PIT count of 10b5-1 sells filed BEFORE this trade's filing_date."""
    row = conn.execute(
        """SELECT COUNT(*) FROM trades
            WHERE insider_id = ? AND ticker = ?
              AND trans_code = 'S' AND is_10b5_1 = 1
              AND filing_date < ?""",
        (insider_id, ticker, as_of),
    ).fetchone()
    return int(row[0]) if row else 0


# ── Price helpers ───────────────────────────────────────────────────────

def load_trading_calendar(conn, start: str, end: str) -> List[str]:
    rows = conn.execute(
        """SELECT DISTINCT date::text FROM prices.daily_prices
           WHERE date >= ? AND date <= ? ORDER BY date""",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def preload_prices(conn, tickers: set, start: str, end: str) -> dict:
    """Bulk-load close prices into a dict[(ticker, date)] = close."""
    out = {}
    BATCH = 200
    tickers_list = sorted(tickers)
    for i in range(0, len(tickers_list), BATCH):
        chunk = tickers_list[i:i + BATCH]
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


def find_first_price_on_or_after(prices, calendar, ticker, cal_idx, max_forward=5):
    """First available close for ticker at or after calendar[cal_idx].
    Returns (cal_idx, close) or None."""
    for off in range(max_forward + 1):
        i = cal_idx + off
        if i >= len(calendar):
            return None
        c = prices.get((ticker, calendar[i]))
        if c is not None:
            return i, c
    return None


# ── Core simulation ─────────────────────────────────────────────────────

def simulate_one_strategy(
    conn,
    strategy_name: str,
    config: dict,
    start_date: str,
    end_date: str,
) -> Tuple[List[ClosedPosition], List[OpenPosition], float]:
    """Walk forward day-by-day. Returns (closed_positions, open_at_end, final_equity)."""
    thesis = (config.get("theses") or [{
        "name": strategy_name,
        "filters": config["filters"],
        "exit": config["exit"],
    }])[0]
    thesis_filters = thesis.get("filters", {})
    hold_td = int(thesis.get("exit", {}).get("hold_days", 30))
    position_size_pct = float(config["position_size_pct"])
    max_concurrent = int(config["max_concurrent"])
    min_conviction = float(config.get("min_conviction", 1.5))

    logger.info(
        "[%s] config: hold_td=%d, pos=%.0f%%, max=%d, min_conv=%.1f, stop=%.0f%%",
        strategy_name, hold_td, position_size_pct * 100, max_concurrent,
        min_conviction, STOP_LOSS_PCT * 100,
    )

    # Load every P-trade in the window with all features needed for filter+conviction
    t0 = time.monotonic()
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
                  t.pit_grade, t.career_grade,
                  t.net_buyer_flow_90d, t.industry_buy_pct_90d
           FROM trades t
           JOIN insiders i ON t.insider_id = i.insider_id
           WHERE t.trans_code = 'P'
             AND t.filing_date >= ? AND t.filing_date <= ?
           ORDER BY t.filing_date, t.trade_id""",
        (start_date, end_date),
    ).fetchall()
    logger.info("[%s] %d P-trades loaded in %.1fs",
                strategy_name, len(rows), time.monotonic() - t0)

    # Bucket by filing_date
    trades_by_date = defaultdict(list)
    for r in rows:
        d = {k: r[k] for k in r.keys()} if hasattr(r, "keys") else dict(r)
        trades_by_date[d["filing_date"]].append(d)

    # Pre-load prices (only for tickers that pass filter — most won't, but easier to load all)
    all_tickers = {r["ticker"] for r in rows if isinstance(r, dict) or r.get("ticker")}
    all_tickers = {(d if isinstance(d, dict) else dict(d))["ticker"]
                   for d in (trades_by_date[k] for k in trades_by_date) for d in d}
    if not all_tickers:
        return [], [], STARTING_CAPITAL
    cal = load_trading_calendar(conn,
                                (datetime.strptime(start_date, "%Y-%m-%d") -
                                 timedelta(days=10)).strftime("%Y-%m-%d"),
                                (datetime.strptime(end_date, "%Y-%m-%d") +
                                 timedelta(days=hold_td * 2 + 10)).strftime("%Y-%m-%d"))
    logger.info("[%s] calendar: %d days", strategy_name, len(cal))
    prices = preload_prices(conn, all_tickers, cal[0], cal[-1])
    logger.info("[%s] cached %d (ticker, date) prices", strategy_name, len(prices))

    cal_idx_of = {d: i for i, d in enumerate(cal)}

    # Find start index
    start_idx = None
    for i, d in enumerate(cal):
        if d >= start_date:
            start_idx = i
            break
    if start_idx is None:
        return [], [], STARTING_CAPITAL

    # State
    equity = STARTING_CAPITAL
    held: List[OpenPosition] = []
    closed: List[ClosedPosition] = []

    # Import conviction once
    from pipelines.insider_study.conviction_score import (
        compute_conviction, _categorize_insider,
    )

    today_str = date.today().isoformat()

    for cal_idx, d in enumerate(cal[start_idx:], start=start_idx):
        if d > today_str:
            break
        if d > end_date:
            break

        # ── 1) Exit checks for held positions ──────────────────────────
        kept = []
        for pos in held:
            close_today = prices.get((pos.ticker, d))
            exit_was_stale = False
            if close_today is not None:
                pos.last_seen_close = close_today
                pos.last_seen_date = d
            else:
                # Today's close is missing. Prefer the most recent close we
                # have seen since entry; only as a last resort fall back to
                # entry_price (which silently zeroes the trade). Flag stale
                # exits so the UI can mark them.
                if pos.last_seen_close is not None:
                    close_today = pos.last_seen_close
                    exit_was_stale = True
                else:
                    close_today = pos.entry_price
                    exit_was_stale = True

            # Stop hit?
            if close_today <= pos.stop_price:
                pnl_pct = (close_today - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.capital_at_entry * pnl_pct
                equity += pnl_dollar
                hold_days_actual = (datetime.strptime(d, "%Y-%m-%d") -
                                    datetime.strptime(pos.entry_date, "%Y-%m-%d")).days
                closed.append(ClosedPosition(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    insider_id=pos.insider_id, insider_name=pos.insider_name,
                    insider_title=pos.insider_title, company=pos.company,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    capital_at_entry=pos.capital_at_entry,
                    exit_date=d, exit_price=close_today,
                    exit_reason=("stop_stale" if exit_was_stale else "stop"),
                    hold_days=hold_days_actual,
                    pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    pit_grade=pos.pit_grade, career_grade=pos.career_grade,
                    conviction=pos.conviction,
                    is_csuite=pos.is_csuite, is_rare_reversal=pos.is_rare_reversal,
                    equity_after=equity,
                ))
                continue

            # Time exit?
            if cal_idx >= pos.target_exit_idx:
                pnl_pct = (close_today - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.capital_at_entry * pnl_pct
                equity += pnl_dollar
                hold_days_actual = (datetime.strptime(d, "%Y-%m-%d") -
                                    datetime.strptime(pos.entry_date, "%Y-%m-%d")).days
                closed.append(ClosedPosition(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    insider_id=pos.insider_id, insider_name=pos.insider_name,
                    insider_title=pos.insider_title, company=pos.company,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    capital_at_entry=pos.capital_at_entry,
                    exit_date=d, exit_price=close_today,
                    exit_reason=("time_stale" if exit_was_stale else "time"),
                    hold_days=hold_days_actual,
                    pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    pit_grade=pos.pit_grade, career_grade=pos.career_grade,
                    conviction=pos.conviction,
                    is_csuite=pos.is_csuite, is_rare_reversal=pos.is_rare_reversal,
                    equity_after=equity,
                ))
                if exit_was_stale:
                    logger.warning(
                        "[%s] STALE exit for %s on %s — used last-seen close %.4f from %s (data gap)",
                        strategy_name, pos.ticker, d, close_today,
                        pos.last_seen_date or "ENTRY",
                    )
                continue

            pos.days_held += 1
            kept.append(pos)
        held = kept

        # ── 2) Today's filings → filter + conviction + capacity ─────────
        candidates_today = trades_by_date.get(d, [])
        if not candidates_today:
            continue

        passing = []
        for t in candidates_today:
            ok, _ = evaluate_filters(thesis_filters, t)
            if not ok:
                continue

            # min_prior_10b5_1_sells (for tenb51_surprise)
            min_10b5 = thesis_filters.get("min_prior_10b5_1_sells")
            if min_10b5:
                n = count_prior_10b5_1_sells(conn, t["insider_id"], t["ticker"], d)
                if n < int(min_10b5):
                    continue

            # Conviction
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
            t["_conviction"] = conv
            passing.append(t)

        # Sort by conviction DESC for capacity allocation
        passing.sort(key=lambda x: -x["_conviction"])

        # ── 3) Capacity check + entry ───────────────────────────────────
        held_tickers = {p.ticker for p in held}
        entered_today = set()
        for t in passing:
            ticker = t["ticker"]
            if ticker in held_tickers or ticker in entered_today:
                continue
            if len(held) + len(entered_today) >= max_concurrent:
                # at_capacity rule: 'skip' for QM/10b5, 'replace_weakest' for RD
                # For the simulator we use 'skip' uniformly to keep it simple
                # and matching the historical default. Rotation logic can be
                # layered in later if backtests show it's meaningful.
                break

            # Entry price: today's close (or next available within 5td)
            entry_lookup = find_first_price_on_or_after(prices, cal, ticker, cal_idx)
            if entry_lookup is None:
                continue
            entry_idx, entry_price = entry_lookup
            if entry_price < 2.0:
                continue   # min price floor

            capital = position_size_pct * equity
            target_exit_idx = entry_idx + hold_td

            held.append(OpenPosition(
                trade_id=t["trade_id"], ticker=ticker,
                insider_id=t["insider_id"], insider_name=t.get("insider_name"),
                insider_title=t.get("title"), company=t.get("company"),
                entry_date=cal[entry_idx], entry_price=entry_price,
                capital_at_entry=capital,
                target_exit_idx=target_exit_idx,
                stop_price=entry_price * (1 + STOP_LOSS_PCT),
                pit_grade=t.get("pit_grade"), career_grade=t.get("career_grade"),
                conviction=t["_conviction"],
                is_csuite=bool(t.get("is_csuite")),
                is_rare_reversal=bool(t.get("is_rare_reversal")),
            ))
            entered_today.add(ticker)
            held_tickers.add(ticker)

    return closed, held, equity


# ── Persistence ─────────────────────────────────────────────────────────

def wipe_strategy(conn, strategy_name: str) -> int:
    n = conn.execute(
        "DELETE FROM strategy_portfolio WHERE strategy = ?",
        (strategy_name,),
    ).rowcount
    conn.commit()
    return n or 0


def ensure_portfolio_row(conn, strategy_name: str) -> int:
    """Get or create the portfolios row id."""
    row = conn.execute(
        "SELECT id FROM portfolios WHERE name = ?", (strategy_name,)
    ).fetchone()
    if row:
        return int(row["id"] if hasattr(row, "keys") else row[0])
    conn.execute(
        """INSERT INTO portfolios (name, display_name, description, starting_capital)
           VALUES (?, ?, ?, ?)""",
        (strategy_name, strategy_name, "", STARTING_CAPITAL),
    )
    conn.commit()
    return ensure_portfolio_row(conn, strategy_name)


def persist_positions(
    conn,
    strategy_name: str,
    closed: List[ClosedPosition],
    open_at_end: List[OpenPosition],
    final_equity: float,
):
    """Write all positions to strategy_portfolio."""
    portfolio_id = ensure_portfolio_row(conn, strategy_name)

    # Closed
    for c in closed:
        reasoning = json.dumps({
            "thesis": strategy_name,
            "filing_date": c.entry_date,
            "conviction": c.conviction,
            "career_grade": c.career_grade,
            "pit_grade": c.pit_grade,
            "is_csuite": c.is_csuite,
            "is_rare_reversal": c.is_rare_reversal,
        }, default=str)
        conn.execute(
            """INSERT INTO strategy_portfolio (
                  strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                  entry_date, entry_price, exit_date, exit_price,
                  hold_days, target_hold, stop_pct, stop_hit,
                  pnl_pct, pnl_dollar, position_size, dollar_amount,
                  portfolio_value, equity_after,
                  insider_name, insider_title, is_csuite,
                  company, filing_date, trade_date,
                  signal_grade, signal_quality, is_rare_reversal,
                  exit_reason, status,
                  execution_source, is_estimated, is_live,
                  entry_reasoning, instrument, shares
              ) VALUES (
                  ?, ?, ?, ?, 'buy_stock', 'long',
                  ?, ?, ?, ?,
                  ?, ?, ?, ?,
                  ?, ?, ?, ?,
                  ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, 'closed',
                  'simulated', 1, FALSE,
                  ?, 'stock', ?
              )""",
            (
                strategy_name, portfolio_id, c.trade_id, c.ticker,
                c.entry_date, c.entry_price, c.exit_date, c.exit_price,
                c.hold_days, c.hold_days,
                abs(STOP_LOSS_PCT), 1 if c.exit_reason == "stop" else 0,
                c.pnl_pct, c.pnl_dollar,
                c.capital_at_entry / max(STARTING_CAPITAL, c.equity_after - c.pnl_dollar),
                c.capital_at_entry,
                c.equity_after - c.pnl_dollar, c.equity_after,
                c.insider_name, c.insider_title, int(bool(c.is_csuite)),
                c.company, c.entry_date, c.entry_date,
                c.career_grade, c.conviction, 1 if c.is_rare_reversal else 0,
                c.exit_reason,
                reasoning,
                int(c.capital_at_entry / c.entry_price) if c.entry_price else 0,
            ),
        )

    # Open (still in flight)
    for o in open_at_end:
        reasoning = json.dumps({
            "thesis": strategy_name,
            "filing_date": o.entry_date,
            "conviction": o.conviction,
            "career_grade": o.career_grade,
            "pit_grade": o.pit_grade,
            "is_csuite": o.is_csuite,
            "is_rare_reversal": o.is_rare_reversal,
        }, default=str)
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
                strategy_name, portfolio_id, o.trade_id, o.ticker,
                o.entry_date, o.entry_price,
                (o.target_exit_idx - 0),  # rough — actual td count
                abs(STOP_LOSS_PCT),
                o.capital_at_entry / max(STARTING_CAPITAL, final_equity),
                o.capital_at_entry, final_equity,
                o.insider_name, o.insider_title, int(bool(o.is_csuite)),
                o.company, o.entry_date, o.entry_date,
                o.career_grade, o.conviction, 1 if o.is_rare_reversal else 0,
                reasoning,
                int(o.capital_at_entry / o.entry_price) if o.entry_price else 0,
            ),
        )

    conn.commit()


# ── Main ────────────────────────────────────────────────────────────────

def run(strategy_name: str, mode: str, end_date: str) -> Dict[str, int]:
    conn = get_connection()
    sc = STRATEGY_CONFIG[strategy_name]
    cfg = yaml.safe_load(sc["yaml"].read_text())

    if mode == "rebuild":
        n_deleted = wipe_strategy(conn, strategy_name)
        logger.info("[%s] wiped %d existing rows", strategy_name, n_deleted)
        start = sc["start_date"]
    elif mode == "extend":
        # Full wipe of every simulated row for this strategy, then re-run from
        # start_date. The previous 90d-window DELETE left pre-cutoff rows in
        # place and the re-sim re-inserted them, causing daily duplicate
        # accumulation (audited 2026-05-22). Full wipe costs ~30s/strategy
        # which is fine for a daily job.
        n_deleted = conn.execute(
            """DELETE FROM strategy_portfolio
               WHERE strategy = ? AND execution_source = 'simulated'""",
            (strategy_name,),
        ).rowcount
        conn.commit()
        logger.info("[%s] extend mode: wiped %d simulated rows",
                    strategy_name, n_deleted or 0)
        start = sc["start_date"]
    else:
        raise ValueError(mode)

    t0 = time.monotonic()
    closed, open_at_end, final_equity = simulate_one_strategy(
        conn, strategy_name, cfg, start, end_date,
    )
    elapsed = time.monotonic() - t0
    logger.info("[%s] sim done in %.1fs — closed=%d, open=%d, final_equity=$%.0f",
                strategy_name, elapsed, len(closed), len(open_at_end), final_equity)

    persist_positions(conn, strategy_name, closed, open_at_end, final_equity)
    logger.info("[%s] persisted %d closed + %d open positions",
                strategy_name, len(closed), len(open_at_end))

    conn.close()
    return {
        "n_closed": len(closed),
        "n_open": len(open_at_end),
        "final_equity": round(final_equity, 0),
        "total_return_pct": round((final_equity / STARTING_CAPITAL - 1) * 100, 1),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=list(STRATEGY_CONFIG),
                   help="Single strategy to run")
    p.add_argument("--all", action="store_true", help="Run all 3 strategies")
    p.add_argument("--rebuild", action="store_true",
                   help="Wipe + re-simulate from scratch (one-shot)")
    p.add_argument("--extend", action="store_true",
                   help="Daily incremental: wipe all simulated rows for the strategy and re-run from start_date")
    p.add_argument("--end", default=None,
                   help="End date (default: today)")
    args = p.parse_args()

    if not args.rebuild and not args.extend:
        p.error("specify --rebuild or --extend")
    if not args.strategy and not args.all:
        p.error("specify --strategy or --all")

    mode = "rebuild" if args.rebuild else "extend"
    end_date = args.end or date.today().isoformat()

    strategies = list(STRATEGY_CONFIG) if args.all else [args.strategy]

    from framework.observability import pipeline_run

    with pipeline_run(
        "strategy_simulator",
        log_path="/Users/derekg/trading-framework/logs/strategy-simulator.log",
    ) as prun:
        results = {}
        for s in strategies:
            logger.info("=" * 60)
            results[s] = run(s, mode, end_date)
        logger.info("=" * 60)
        logger.info("Summary:")
        for s, r in results.items():
            logger.info(
                "  %s: closed=%d, open=%d, final=$%s (%s%% total return)",
                s, r["n_closed"], r["n_open"], f"{r['final_equity']:,.0f}",
                f"{'+' if r['total_return_pct'] >= 0 else ''}{r['total_return_pct']:.1f}",
            )

        # Record telemetry: per-strategy closed/open counts + sum of rows touched.
        total_rows = sum(r["n_closed"] + r["n_open"] for r in results.values())
        prun.set_rows_written(total_rows)
        prun.set_metadata({
            "mode": mode,
            "end_date": end_date,
            "strategies": list(strategies),
            "results": results,
        })


if __name__ == "__main__":
    main()
