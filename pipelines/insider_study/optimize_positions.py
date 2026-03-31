#!/usr/bin/env python3
"""
Position configuration optimizer for CW insider strategies.

Day-by-day portfolio simulation that processes ALL open positions on each
calendar day. This avoids the bar-counting bugs that arise when processing
events in filing order and fast-forwarding positions individually.

Usage:
    python3 pipelines/insider_study/optimize_positions.py
    python3 pipelines/insider_study/optimize_positions.py --strategy reversal
    python3 pipelines/insider_study/optimize_positions.py --strategy composite
    python3 pipelines/insider_study/optimize_positions.py --verbose
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from pipelines.insider_study.conviction_score import (
        compute_conviction,
        MIN_CONVICTION,
        REPLACEMENT_ADVANTAGE,
    )
except ModuleNotFoundError:
    from conviction_score import (
        compute_conviction,
        MIN_CONVICTION,
        REPLACEMENT_ADVANTAGE,
    )

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"

# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SimConfig:
    """A single simulation configuration to test."""
    label: str
    strategy_type: str  # "reversal" or "composite"
    max_positions: int
    position_pct: float  # e.g. 0.05 for 5%
    hold_days: int
    stop_pct: float  # e.g. -0.15
    trailing_stop: bool
    replacement: str  # "skip", "weakest", "oldest"
    replacement_advantage: float  # min conviction advantage to replace
    conviction_boost_threshold: float | None  # conviction >= this gets 1.5x size
    conviction_boost_factor: float  # e.g. 1.5


@dataclass
class OpenPosition:
    """An open position in the portfolio."""
    trade_id: int
    ticker: str
    thesis: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    conviction: float
    hold_days_target: int
    stop_pct: float
    trailing_stop: bool
    peak_price: float
    days_held: int = 0
    exit_date: str | None = None
    exit_price: float | None = None
    exit_reason: str | None = None


@dataclass
class ClosedTrade:
    """A completed trade."""
    trade_id: int
    ticker: str
    thesis: str
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    dollar_amount: float
    pnl_pct: float
    pnl_dollar: float
    hold_days: int
    exit_reason: str
    conviction: float


@dataclass
class SimResult:
    """Results from a single simulation run."""
    label: str
    strategy_type: str
    trades: int
    wins: int
    win_rate: float
    final_equity: float
    cagr: float
    max_drawdown: float
    deployment_pct: float  # avg % of capital deployed
    replacements: int
    avg_hold_days: float
    profit_factor: float
    sharpe: float


# ---------------------------------------------------------------------------
# Event loading (same queries as production backfill)
# ---------------------------------------------------------------------------

def _load_reversal_events(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Load reversal-qualifying events."""
    query = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.company, t.title,
               t.trade_type, t.trade_date, t.filing_date, t.price, t.value,
               t.is_csuite, t.signal_grade, t.is_rare_reversal,
               t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
               t.above_sma50, t.above_sma200, t.is_largest_ever,
               t.is_recurring, t.is_tax_sale, t.cohen_routine,
               tr.entry_price
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code = 'P'
          AND t.is_rare_reversal = 1
          AND COALESCE(t.consecutive_sells_before, 0) >= 5
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price >= 2.0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    return [dict(zip(cols, r)) for r in rows]


def _load_dip_cluster_events(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Load dip_cluster-qualifying events (dip + top_trade signal)."""
    query = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.company, t.title,
               t.trade_type, t.trade_date, t.filing_date, t.price, t.value,
               t.is_csuite, t.signal_grade, t.is_rare_reversal,
               t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
               t.above_sma50, t.above_sma200, t.is_largest_ever,
               t.is_recurring, t.is_tax_sale, t.cohen_routine,
               tr.entry_price
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code = 'P'
          AND (t.dip_1mo <= -0.15 OR t.dip_3mo <= -0.25)
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price >= 2.0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    events = [dict(zip(cols, r)) for r in rows]

    if not events:
        return []

    # Filter to those with a top_trade signal
    trade_ids = [e["trade_id"] for e in events]
    top_ids = _get_top_trade_ids(conn, trade_ids)
    return [e for e in events if e["trade_id"] in top_ids]


def _load_momentum_largest_events(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Load momentum_largest-qualifying events."""
    query = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.company, t.title,
               t.trade_type, t.trade_date, t.filing_date, t.price, t.value,
               t.is_csuite, t.signal_grade, t.is_rare_reversal,
               t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
               t.above_sma50, t.above_sma200, t.is_largest_ever,
               t.is_recurring, t.is_tax_sale, t.cohen_routine,
               tr.entry_price
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code = 'P'
          AND t.above_sma50 = 1
          AND t.above_sma200 = 1
          AND t.is_largest_ever = 1
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price >= 2.0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    return [dict(zip(cols, r)) for r in rows]


def _get_top_trade_ids(conn: sqlite3.Connection, trade_ids: list[int]) -> set[int]:
    """Return trade_ids that have any top_trade signal."""
    if not trade_ids:
        return set()
    result: set[int] = set()
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i : i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(
            f"SELECT DISTINCT trade_id FROM trade_signals "
            f"WHERE trade_id IN ({placeholders}) AND signal_type = 'top_trade'",
            batch,
        ).fetchall()
        for (tid,) in rows:
            result.add(tid)
    return result


# ---------------------------------------------------------------------------
# Price cache — bulk-load all needed tickers into memory
# ---------------------------------------------------------------------------


class PriceCache:
    """In-memory price cache keyed by (ticker, date) -> (open, high, low, close).

    Bulk-loads all daily bars for the required tickers and date range once,
    then serves lookups from a dict. This is much faster than per-day SQL.
    """

    def __init__(self, prices_conn: sqlite3.Connection, tickers: set[str],
                 start: str, end: str):
        self._data: dict[tuple[str, str], tuple[float, float, float, float]] = {}
        self._dates_by_ticker: dict[str, list[str]] = {}

        # Bulk load
        t0 = time.time()
        ticker_list = sorted(tickers)
        batch_size = 200
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i : i + batch_size]
            ph = ",".join("?" * len(batch))
            rows = prices_conn.execute(
                f"SELECT ticker, date, open, high, low, close FROM daily_prices "
                f"WHERE ticker IN ({ph}) AND date BETWEEN ? AND ? ORDER BY ticker, date",
                batch + [start, end],
            ).fetchall()
            for ticker, date, o, h, l, c in rows:
                if c and c > 0:
                    self._data[(ticker, date)] = (o or c, h or c, l or c, c)
                    if ticker not in self._dates_by_ticker:
                        self._dates_by_ticker[ticker] = []
                    self._dates_by_ticker[ticker].append(date)

        elapsed = time.time() - t0
        print(f"  Price cache: {len(self._data):,} bars for {len(tickers)} tickers ({elapsed:.1f}s)")

    def get_close(self, ticker: str, date: str) -> float | None:
        """Get close price for a ticker on a specific date."""
        bar = self._data.get((ticker, date))
        return bar[3] if bar else None

    def get_open(self, ticker: str, date: str) -> float | None:
        """Get open price for a ticker on a specific date."""
        bar = self._data.get((ticker, date))
        return bar[0] if bar else None

    def get_bar(self, ticker: str, date: str) -> tuple[float, float, float, float] | None:
        """Get (open, high, low, close) for a ticker on a date."""
        return self._data.get((ticker, date))

    def get_close_on_or_before(self, ticker: str, date: str) -> float | None:
        """Get the most recent close price on or before a date."""
        dates = self._dates_by_ticker.get(ticker, [])
        if not dates:
            return None
        # Binary search for the latest date <= target
        lo, hi = 0, len(dates) - 1
        result = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if dates[mid] <= date:
                result = dates[mid]
                lo = mid + 1
            else:
                hi = mid - 1
        if result:
            return self._data[(ticker, result)][3]
        return None

    def get_first_trading_day_after(self, ticker: str, date: str) -> str | None:
        """Get first trading date strictly after the given date for this ticker."""
        dates = self._dates_by_ticker.get(ticker, [])
        if not dates:
            return None
        # Binary search for first date > target
        lo, hi = 0, len(dates) - 1
        result = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if dates[mid] > date:
                result = dates[mid]
                hi = mid - 1
            else:
                lo = mid + 1
        return result

    def has_data(self, ticker: str) -> bool:
        return ticker in self._dates_by_ticker


# ---------------------------------------------------------------------------
# Build trading calendar from prices
# ---------------------------------------------------------------------------

def _build_trading_calendar(prices_conn: sqlite3.Connection, start: str, end: str) -> list[str]:
    """Build a list of all trading dates in range (from SPY as reference)."""
    rows = prices_conn.execute(
        "SELECT DISTINCT date FROM daily_prices WHERE ticker = 'SPY' "
        "AND date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Core simulation: day-by-day
# ---------------------------------------------------------------------------

def run_simulation(
    insiders_conn: sqlite3.Connection,
    prices_conn: sqlite3.Connection,
    config: SimConfig,
    start: str = "2020-01-01",
    end: str = "2026-03-13",
    starting_capital: float = 100_000.0,
    verbose: bool = False,
) -> SimResult:
    """Run a day-by-day portfolio simulation for a given configuration.

    The key invariant: on each trading day we iterate ALL open positions to
    check exits, then process new entries. No per-trade bar walking.
    """

    # --- Load events ---
    events_by_thesis: dict[str, list[dict]] = {}

    if config.strategy_type == "reversal":
        evts = _load_reversal_events(insiders_conn, start, end)
        for e in evts:
            e["_thesis"] = "reversal"
        events_by_thesis["reversal"] = evts
    elif config.strategy_type == "composite":
        rev = _load_reversal_events(insiders_conn, start, end)
        for e in rev:
            e["_thesis"] = "reversal"
        events_by_thesis["reversal"] = rev

        dip = _load_dip_cluster_events(insiders_conn, start, end)
        for e in dip:
            e["_thesis"] = "dip_cluster"
        events_by_thesis["dip_cluster"] = dip

        mom = _load_momentum_largest_events(insiders_conn, start, end)
        for e in mom:
            e["_thesis"] = "momentum_largest"
        events_by_thesis["momentum_largest"] = mom

    # Merge, dedupe (same trade_id or same ticker+filing_date keeps first by
    # thesis priority: reversal > dip_cluster > momentum_largest)
    thesis_priority = {"reversal": 0, "dip_cluster": 1, "momentum_largest": 2}
    all_events: list[dict] = []
    for evts in events_by_thesis.values():
        all_events.extend(evts)
    all_events.sort(key=lambda e: (e["filing_date"], thesis_priority.get(e["_thesis"], 9), e["trade_id"]))

    seen_ids: set[int] = set()
    seen_ticker_dates: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for e in all_events:
        tid = e["trade_id"]
        td_key = (e["ticker"], e["filing_date"])
        if tid in seen_ids or td_key in seen_ticker_dates:
            continue
        seen_ids.add(tid)
        seen_ticker_dates.add(td_key)
        deduped.append(e)
    all_events = deduped

    # Compute conviction for all events upfront
    for e in all_events:
        e["_conviction"] = compute_conviction(
            thesis=e["_thesis"],
            signal_grade=e.get("signal_grade"),
            consecutive_sells=e.get("consecutive_sells_before"),
            dip_1mo=e.get("dip_1mo"),
            dip_3mo=e.get("dip_3mo"),
            is_largest_ever=bool(e.get("is_largest_ever")),
            above_sma50=bool(e.get("above_sma50")),
            above_sma200=bool(e.get("above_sma200")),
        )

    # Filter by minimum conviction
    all_events = [e for e in all_events if e["_conviction"] >= MIN_CONVICTION]

    # Index events by entry_date (filing_date + 1 calendar day)
    # We enter on the day AFTER filing — map that to the next trading day later
    events_by_filing: dict[str, list[dict]] = {}
    for e in all_events:
        fd = e["filing_date"]
        if fd not in events_by_filing:
            events_by_filing[fd] = []
        events_by_filing[fd].append(e)

    # Collect all tickers we need prices for
    all_tickers = {e["ticker"] for e in all_events}

    # Build price cache
    # Add buffer before start for price lookups
    cache_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    cache_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")
    prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)

    # Build trading calendar
    trading_days = _build_trading_calendar(prices_conn, start, cache_end)
    if not trading_days:
        return _empty_result(config)

    # --- Simulation state ---
    equity = starting_capital
    open_positions: list[OpenPosition] = []
    closed_trades: list[ClosedTrade] = []
    replacements = 0

    # Track equity curve for drawdown/Sharpe
    equity_curve: list[float] = [starting_capital]
    daily_returns: list[float] = []
    deployment_samples: list[float] = []

    # Track which filing dates have been processed for entries
    # (we enter on the first trading day strictly after filing_date)
    pending_entries: list[dict] = []  # events whose filing_date has passed but not yet entered
    last_filing_checked = ""

    total_events = len(all_events)

    for day_idx, today in enumerate(trading_days):
        if today < start:
            continue

        # --- Step 1: Gather any new signals filed on or before today that
        #     haven't been gathered yet. Entry is T+1 after filing, so
        #     events filed yesterday (or earlier weekend days) become
        #     eligible for entry today. ---
        # Collect events whose filing_date < today (so today is at least T+1)
        for fd in sorted(events_by_filing.keys()):
            if fd >= today:
                break
            if fd <= last_filing_checked:
                continue
            pending_entries.extend(events_by_filing[fd])
            last_filing_checked = fd

        # --- Step 2: Check all open positions for exits ---
        still_open: list[OpenPosition] = []
        for pos in open_positions:
            bar = prices.get_bar(pos.ticker, today)
            if bar is None:
                # No price data today — still open, don't increment days_held
                still_open.append(pos)
                continue

            o, h, l, c = bar
            pos.days_held += 1

            # Track peak for trailing stop
            if c > pos.peak_price:
                pos.peak_price = c

            current_return = (c - pos.entry_price) / pos.entry_price

            # Check hard stop: did low breach the stop level?
            stop_level = pos.entry_price * (1 + pos.stop_pct)
            if l <= stop_level:
                # Stop triggered — exit at stop price
                exit_price = stop_level
                pnl_pct = pos.stop_pct
                pnl_dollar = pos.dollar_amount * pnl_pct
                equity += pnl_dollar
                closed_trades.append(ClosedTrade(
                    trade_id=pos.trade_id, ticker=pos.ticker, thesis=pos.thesis,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    exit_date=today, exit_price=exit_price,
                    dollar_amount=pos.dollar_amount, pnl_pct=pnl_pct,
                    pnl_dollar=pnl_dollar, hold_days=pos.days_held,
                    exit_reason="stop_loss", conviction=pos.conviction,
                ))
                continue

            # Check trailing stop
            if pos.trailing_stop and pos.peak_price > pos.entry_price:
                trailing_level = pos.peak_price * (1 + pos.stop_pct)
                if l <= trailing_level:
                    exit_price = trailing_level
                    pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
                    pnl_dollar = pos.dollar_amount * pnl_pct
                    equity += pnl_dollar
                    closed_trades.append(ClosedTrade(
                        trade_id=pos.trade_id, ticker=pos.ticker, thesis=pos.thesis,
                        entry_date=pos.entry_date, entry_price=pos.entry_price,
                        exit_date=today, exit_price=exit_price,
                        dollar_amount=pos.dollar_amount, pnl_pct=pnl_pct,
                        pnl_dollar=pnl_dollar, hold_days=pos.days_held,
                        exit_reason="trailing_stop", conviction=pos.conviction,
                    ))
                    continue

            # Check time exit
            if pos.days_held >= pos.hold_days_target:
                exit_price = c
                pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                equity += pnl_dollar
                closed_trades.append(ClosedTrade(
                    trade_id=pos.trade_id, ticker=pos.ticker, thesis=pos.thesis,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    exit_date=today, exit_price=exit_price,
                    dollar_amount=pos.dollar_amount, pnl_pct=pnl_pct,
                    pnl_dollar=pnl_dollar, hold_days=pos.days_held,
                    exit_reason=f"time_exit_{pos.hold_days_target}d",
                    conviction=pos.conviction,
                ))
                continue

            # Still open
            still_open.append(pos)

        open_positions = still_open

        # --- Step 3: Process new entries from pending_entries ---
        # Sort pending by conviction descending so highest-conviction enters first
        pending_entries.sort(key=lambda e: -e["_conviction"])

        entered_tickers_today: set[str] = set()
        remaining_pending: list[dict] = []

        for event in pending_entries:
            ticker = event["ticker"]

            # Skip if already holding this ticker
            held_tickers = {p.ticker for p in open_positions}
            if ticker in held_tickers or ticker in entered_tickers_today:
                # Don't drop it — it might become relevant if position exits
                # But realistically this signal is stale, so drop it
                continue

            # Check if we have price data and can enter
            entry_price = prices.get_open(ticker, today)
            if entry_price is None or entry_price <= 0:
                # Try close as fallback
                entry_price = prices.get_close(ticker, today)
            if entry_price is None or entry_price <= 0:
                # No price data today — signal is stale, drop it
                continue

            conviction = event["_conviction"]
            thesis = event["_thesis"]

            # Position sizing
            size_pct = config.position_pct
            if (config.conviction_boost_threshold is not None
                    and conviction >= config.conviction_boost_threshold):
                size_pct *= config.conviction_boost_factor
            dollar_amount = equity * size_pct
            if dollar_amount <= 0:
                continue

            # --- Capacity check ---
            if len(open_positions) >= config.max_positions:
                if config.replacement == "skip":
                    # At capacity, skip this signal
                    continue

                elif config.replacement == "weakest":
                    weakest = min(open_positions, key=lambda p: p.conviction)
                    advantage = conviction - weakest.conviction
                    if advantage < config.replacement_advantage:
                        continue
                    # Replace: close weakest at today's close
                    rep_close = prices.get_close(weakest.ticker, today)
                    if rep_close is None or rep_close <= 0:
                        continue
                    rep_pnl_pct = (rep_close - weakest.entry_price) / weakest.entry_price
                    rep_pnl_dollar = weakest.dollar_amount * rep_pnl_pct
                    equity += rep_pnl_dollar
                    closed_trades.append(ClosedTrade(
                        trade_id=weakest.trade_id, ticker=weakest.ticker,
                        thesis=weakest.thesis,
                        entry_date=weakest.entry_date,
                        entry_price=weakest.entry_price,
                        exit_date=today, exit_price=rep_close,
                        dollar_amount=weakest.dollar_amount,
                        pnl_pct=rep_pnl_pct, pnl_dollar=rep_pnl_dollar,
                        hold_days=weakest.days_held,
                        exit_reason="replaced_weakest",
                        conviction=weakest.conviction,
                    ))
                    open_positions = [p for p in open_positions if p.trade_id != weakest.trade_id]
                    replacements += 1
                    # Recompute dollar_amount after equity change
                    dollar_amount = equity * size_pct
                    if dollar_amount <= 0:
                        continue

                elif config.replacement == "oldest":
                    oldest = min(open_positions, key=lambda p: p.entry_date)
                    # Replace: close oldest at today's close
                    rep_close = prices.get_close(oldest.ticker, today)
                    if rep_close is None or rep_close <= 0:
                        continue
                    rep_pnl_pct = (rep_close - oldest.entry_price) / oldest.entry_price
                    rep_pnl_dollar = oldest.dollar_amount * rep_pnl_pct
                    equity += rep_pnl_dollar
                    closed_trades.append(ClosedTrade(
                        trade_id=oldest.trade_id, ticker=oldest.ticker,
                        thesis=oldest.thesis,
                        entry_date=oldest.entry_date,
                        entry_price=oldest.entry_price,
                        exit_date=today, exit_price=rep_close,
                        dollar_amount=oldest.dollar_amount,
                        pnl_pct=rep_pnl_pct, pnl_dollar=rep_pnl_dollar,
                        hold_days=oldest.days_held,
                        exit_reason="replaced_oldest",
                        conviction=oldest.conviction,
                    ))
                    open_positions = [p for p in open_positions if p.trade_id != oldest.trade_id]
                    replacements += 1
                    dollar_amount = equity * size_pct
                    if dollar_amount <= 0:
                        continue
                else:
                    continue

            # --- Enter position ---
            pos = OpenPosition(
                trade_id=event["trade_id"],
                ticker=ticker,
                thesis=thesis,
                entry_date=today,
                entry_price=entry_price,
                dollar_amount=dollar_amount,
                conviction=conviction,
                hold_days_target=config.hold_days,
                stop_pct=config.stop_pct,
                trailing_stop=config.trailing_stop,
                peak_price=entry_price,
                days_held=0,
            )
            open_positions.append(pos)
            entered_tickers_today.add(ticker)

        # Whatever wasn't entered is now stale — drop it
        # (Signals are only actionable on T+1 after filing)
        pending_entries = []

        # --- Step 4: Record daily equity ---
        # Mark-to-market: equity includes cash + unrealized P&L of open positions
        unrealized = 0.0
        deployed = 0.0
        for pos in open_positions:
            c = prices.get_close(pos.ticker, today)
            if c and c > 0:
                unrealized += pos.dollar_amount * ((c - pos.entry_price) / pos.entry_price)
            deployed += pos.dollar_amount

        total_equity = equity + unrealized
        equity_curve.append(total_equity)
        if len(equity_curve) >= 2:
            prev = equity_curve[-2]
            if prev > 0:
                daily_returns.append((total_equity - prev) / prev)
            else:
                daily_returns.append(0.0)

        if total_equity > 0:
            deployment_samples.append(deployed / total_equity)
        else:
            deployment_samples.append(0.0)

    # --- Close any remaining open positions at final equity ---
    for pos in open_positions:
        last_close = prices.get_close_on_or_before(pos.ticker, trading_days[-1])
        if last_close and last_close > 0:
            pnl_pct = (last_close - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar
            closed_trades.append(ClosedTrade(
                trade_id=pos.trade_id, ticker=pos.ticker, thesis=pos.thesis,
                entry_date=pos.entry_date, entry_price=pos.entry_price,
                exit_date=trading_days[-1], exit_price=last_close,
                dollar_amount=pos.dollar_amount, pnl_pct=pnl_pct,
                pnl_dollar=pnl_dollar, hold_days=pos.days_held,
                exit_reason="sim_end", conviction=pos.conviction,
            ))

    # --- Compute metrics ---
    n_trades = len(closed_trades)
    wins = sum(1 for t in closed_trades if t.pnl_pct > 0)
    win_rate = wins / n_trades if n_trades > 0 else 0.0

    # CAGR
    years = len(trading_days) / 252.0
    final_equity = equity_curve[-1] if equity_curve else starting_capital
    if final_equity > 0 and starting_capital > 0 and years > 0:
        cagr = (final_equity / starting_capital) ** (1.0 / years) - 1.0
    else:
        cagr = -1.0

    # Max drawdown from equity curve
    peak_eq = starting_capital
    max_dd = 0.0
    for eq in equity_curve:
        if eq > peak_eq:
            peak_eq = eq
        dd = (peak_eq - eq) / peak_eq if peak_eq > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Avg deployment
    avg_deployment = sum(deployment_samples) / len(deployment_samples) if deployment_samples else 0.0

    # Avg hold days
    avg_hold = sum(t.hold_days for t in closed_trades) / n_trades if n_trades > 0 else 0.0

    # Profit factor
    gross_win = sum(t.pnl_dollar for t in closed_trades if t.pnl_dollar > 0)
    gross_loss = abs(sum(t.pnl_dollar for t in closed_trades if t.pnl_dollar < 0))
    profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Sharpe (annualized from daily returns)
    if daily_returns and len(daily_returns) > 1:
        import statistics
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.stdev(daily_returns)
        sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    return SimResult(
        label=config.label,
        strategy_type=config.strategy_type,
        trades=n_trades,
        wins=wins,
        win_rate=win_rate,
        final_equity=final_equity,
        cagr=cagr,
        max_drawdown=max_dd,
        deployment_pct=avg_deployment,
        replacements=replacements,
        avg_hold_days=avg_hold,
        profit_factor=profit_factor,
        sharpe=sharpe,
    )


def _empty_result(config: SimConfig) -> SimResult:
    return SimResult(
        label=config.label,
        strategy_type=config.strategy_type,
        trades=0, wins=0, win_rate=0.0,
        final_equity=100_000.0, cagr=0.0, max_drawdown=0.0,
        deployment_pct=0.0, replacements=0, avg_hold_days=0.0,
        profit_factor=0.0, sharpe=0.0,
    )


# ---------------------------------------------------------------------------
# Configuration grid
# ---------------------------------------------------------------------------

def build_reversal_configs() -> list[SimConfig]:
    """Build the reversal configuration grid."""
    configs = []

    # Skip configs
    configs.append(SimConfig(
        label="R:10pos/5%/30d/skip",
        strategy_type="reversal", max_positions=10, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="R:12pos/6%/30d/skip",
        strategy_type="reversal", max_positions=12, position_pct=0.06,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="R:15pos/5%/30d/skip",
        strategy_type="reversal", max_positions=15, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace weakest
    configs.append(SimConfig(
        label="R:10pos/5%/30d/weakest(1.5)",
        strategy_type="reversal", max_positions=10, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=1.5,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="R:12pos/6%/30d/weakest",
        strategy_type="reversal", max_positions=12, position_pct=0.06,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="R:15pos/5%/30d/weakest",
        strategy_type="reversal", max_positions=15, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace oldest
    configs.append(SimConfig(
        label="R:15pos/5%/30d/oldest",
        strategy_type="reversal", max_positions=15, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="oldest", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace weakest + conviction boost
    configs.append(SimConfig(
        label="R:10pos/5%/30d/weakest+boost7",
        strategy_type="reversal", max_positions=10, position_pct=0.05,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=1.5,
        conviction_boost_threshold=7.0, conviction_boost_factor=1.5,
    ))
    configs.append(SimConfig(
        label="R:12pos/6%/30d/weakest+boost",
        strategy_type="reversal", max_positions=12, position_pct=0.06,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=7.0, conviction_boost_factor=1.5,
    ))

    return configs


def build_composite_configs() -> list[SimConfig]:
    """Build the composite configuration grid."""
    configs = []

    # Skip configs
    configs.append(SimConfig(
        label="C:15pos/3.3%/30d/skip",
        strategy_type="composite", max_positions=15, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:20pos/3.3%/30d/skip",
        strategy_type="composite", max_positions=20, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:25pos/3.3%/14d/skip",
        strategy_type="composite", max_positions=25, position_pct=0.033,
        hold_days=14, stop_pct=-0.15, trailing_stop=True,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:20pos/5%/14d/skip",
        strategy_type="composite", max_positions=20, position_pct=0.05,
        hold_days=14, stop_pct=-0.15, trailing_stop=True,
        replacement="skip", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace weakest
    configs.append(SimConfig(
        label="C:15pos/3.3%/30d/weakest",
        strategy_type="composite", max_positions=15, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:20pos/3.3%/30d/weakest",
        strategy_type="composite", max_positions=20, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:25pos/3.3%/14d/weakest",
        strategy_type="composite", max_positions=25, position_pct=0.033,
        hold_days=14, stop_pct=-0.15, trailing_stop=True,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))
    configs.append(SimConfig(
        label="C:20pos/5%/14d/weakest",
        strategy_type="composite", max_positions=20, position_pct=0.05,
        hold_days=14, stop_pct=-0.15, trailing_stop=True,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace oldest
    configs.append(SimConfig(
        label="C:20pos/3.3%/30d/oldest",
        strategy_type="composite", max_positions=20, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="oldest", replacement_advantage=0,
        conviction_boost_threshold=None, conviction_boost_factor=1.0,
    ))

    # Replace weakest + conviction boost
    configs.append(SimConfig(
        label="C:15pos/3.3%/30d/weakest+boost",
        strategy_type="composite", max_positions=15, position_pct=0.033,
        hold_days=30, stop_pct=-0.15, trailing_stop=False,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=7.0, conviction_boost_factor=1.5,
    ))
    configs.append(SimConfig(
        label="C:20pos/5%/14d/weakest+boost",
        strategy_type="composite", max_positions=20, position_pct=0.05,
        hold_days=14, stop_pct=-0.15, trailing_stop=True,
        replacement="weakest", replacement_advantage=REPLACEMENT_ADVANTAGE,
        conviction_boost_threshold=7.0, conviction_boost_factor=1.5,
    ))

    return configs


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_results_table(results: list[SimResult], title: str) -> None:
    """Print a formatted comparison table sorted by CAGR descending."""
    results = sorted(results, key=lambda r: -r.cagr)

    print(f"\n{'=' * 120}")
    print(f"  {title}")
    print(f"{'=' * 120}")
    print(f"  {'Config':<38} {'Trades':>6} {'WR':>6} {'Final $':>12} {'CAGR':>8} "
          f"{'MaxDD':>7} {'Deploy':>7} {'Repl':>5} {'AvgHold':>7} {'PF':>6} {'Sharpe':>7}")
    print(f"  {'-' * 38} {'-' * 6} {'-' * 6} {'-' * 12} {'-' * 8} "
          f"{'-' * 7} {'-' * 7} {'-' * 5} {'-' * 7} {'-' * 6} {'-' * 7}")

    for r in results:
        pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 100 else "inf"
        print(f"  {r.label:<38} {r.trades:>6} {r.win_rate:>5.1%} "
              f"${r.final_equity:>10,.0f} {r.cagr:>7.1%} "
              f"{r.max_drawdown:>6.1%} {r.deployment_pct:>6.1%} "
              f"{r.replacements:>5} {r.avg_hold_days:>6.1f} "
              f"{pf_str:>6} {r.sharpe:>7.2f}")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CW position optimizer")
    parser.add_argument("--strategy", choices=["reversal", "composite", "all"],
                        default="all", help="Which strategy type to run")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2026-03-13")
    parser.add_argument("--capital", type=float, default=100_000.0)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print(f"CW Position Optimizer")
    print(f"Period: {args.start} to {args.end}")
    print(f"Starting capital: ${args.capital:,.0f}")
    print()

    # Open database connections
    insiders_conn = sqlite3.connect(str(DB_PATH))
    prices_conn = sqlite3.connect(str(PRICES_DB))

    # Attach prices.db to insiders.db so we can use prices data in queries if needed
    insiders_conn.execute(f"ATTACH DATABASE '{PRICES_DB}' AS prices")

    try:
        configs: list[SimConfig] = []
        if args.strategy in ("reversal", "all"):
            configs.extend(build_reversal_configs())
        if args.strategy in ("composite", "all"):
            configs.extend(build_composite_configs())

        print(f"Running {len(configs)} configurations...\n")

        reversal_results: list[SimResult] = []
        composite_results: list[SimResult] = []

        for i, cfg in enumerate(configs):
            t0 = time.time()
            print(f"[{i + 1}/{len(configs)}] {cfg.label}")
            result = run_simulation(
                insiders_conn, prices_conn, cfg,
                start=args.start, end=args.end,
                starting_capital=args.capital,
                verbose=args.verbose,
            )
            elapsed = time.time() - t0
            print(f"  => {result.trades} trades, {result.win_rate:.1%} WR, "
                  f"${result.final_equity:,.0f}, CAGR {result.cagr:.1%}, "
                  f"{result.replacements} replacements ({elapsed:.1f}s)\n")

            if cfg.strategy_type == "reversal":
                reversal_results.append(result)
            else:
                composite_results.append(result)

        # Print comparison tables
        if reversal_results:
            print_results_table(reversal_results, "REVERSAL STRATEGY CONFIGURATIONS")
        if composite_results:
            print_results_table(composite_results, "COMPOSITE STRATEGY CONFIGURATIONS")

        # Combined table
        if reversal_results and composite_results:
            print_results_table(
                reversal_results + composite_results,
                "ALL CONFIGURATIONS — SORTED BY CAGR",
            )

    finally:
        insiders_conn.close()
        prices_conn.close()


if __name__ == "__main__":
    main()
