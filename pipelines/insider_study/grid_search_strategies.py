#!/usr/bin/env python3
"""
Multi-strategy grid search over 3 validated insider trading theses.

Strategies:
  1. Quality + Momentum  — A+/A PIT grade + above SMA50/SMA200
  2. 10b5-1 Surprise Buy — scheduled seller breaks pattern and buys
  3. Deep Reversal + Dip  — 5+ consecutive sells + 3-month dip

Entry pricing:
  - Market-hours filings: next 5-min bar close from intraday.db
  - After-hours filings: T+1 open from prices.db
  Uses compute_entry_prices() from backfill_cw_portfolio (proven, PIT-safe).

Usage:
    python3 pipelines/insider_study/grid_search_strategies.py [--strategy quality|tenb|reversal|all]
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.insider_study.backfill_cw_portfolio import (
    PriceCache,
    _build_trading_calendar,
    _filed_during_market_hours,
    compute_entry_prices,
    DB_PATH,
    PRICES_DB,
    INTRADAY_DB,
)

START = "2020-01-01"
END = "2026-03-31"
STARTING_CAPITAL = 100_000.0

# Columns shared across all event loaders
_BASE_COLS = """
    t.trade_id, t.insider_id, t.ticker, t.company, t.title,
    t.trade_type, t.trade_date, t.filing_date, t.filed_at, t.price, t.value,
    t.is_csuite, t.signal_grade, t.is_rare_reversal,
    t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
    t.above_sma50, t.above_sma200, t.is_largest_ever,
    t.is_recurring, t.is_tax_sale, t.cohen_routine, t.is_10b5_1,
    t.shares_owned_after, t.qty, t.pit_cluster_size,
    t.pit_grade, t.pit_blended_score,
    tr.entry_price AS tr_entry_price
"""

_BASE_JOIN = """
    FROM trades t
    JOIN trade_returns tr ON t.trade_id = tr.trade_id
    WHERE t.trans_code = 'P'
      AND t.filing_date BETWEEN ? AND ?
      AND tr.entry_price IS NOT NULL
      AND tr.entry_price > 0
"""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GridConfig:
    """A single grid search configuration."""
    strategy: str
    position_size: float
    max_concurrent: int
    at_capacity: str            # skip, replace_oldest, replace_weakest
    hold_days: int
    stop_loss: float | None     # e.g. -0.15, or None for no stop
    exit_type: str              # fixed_hold, trailing_stop
    trailing_stop_pct: float    # only used when exit_type == trailing_stop
    circuit_breaker_dd: float   # max drawdown before halting entries
    # Strategy-specific filters (used to subset events before sim)
    filters: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        parts = [
            f"{self.position_size:.0%}/{self.max_concurrent}pos",
            f"{self.at_capacity}",
            f"{self.hold_days}d",
        ]
        if self.stop_loss is not None:
            parts.append(f"sl{self.stop_loss:.0%}")
        else:
            parts.append("no_sl")
        if self.exit_type == "trailing_stop":
            parts.append(f"trail{self.trailing_stop_pct:.0%}")
        filt_parts = []
        for k, v in sorted(self.filters.items()):
            filt_parts.append(f"{k}={v}")
        if filt_parts:
            parts.append("|".join(filt_parts))
        return "/".join(parts)


@dataclass
class OpenPos:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    conviction: float
    days_held: int = 0
    peak_price: float = 0.0


@dataclass
class SimResult:
    config: GridConfig
    trades: int = 0
    wins: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    total_pnl: float = 0.0
    final_equity: float = 0.0
    cagr: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    stops_hit: int = 0
    trailing_exits: int = 0
    time_exits: int = 0
    replacements: int = 0
    max_concurrent_seen: int = 0
    avg_concurrent: float = 0.0
    avg_deployment: float = 0.0
    n_intraday_entries: int = 0
    n_t1_entries: int = 0
    events_available: int = 0


# ---------------------------------------------------------------------------
# Event loaders
# ---------------------------------------------------------------------------

def load_quality_momentum_events(
    conn: sqlite3.Connection, start: str, end: str
) -> list[dict]:
    """Load buys with PIT grade A+/A and momentum data.

    Returns the WIDEST set — grid configs filter by grade_filter and
    require_momentum at sim time.
    """
    query = f"""
        SELECT {_BASE_COLS}
        {_BASE_JOIN}
          AND t.pit_grade IN ('A+', 'A', 'B')
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    events = [dict(zip(cols, r)) for r in rows]

    # Conviction for quality+momentum: grade-based ranking
    grade_conv = {"A+": 9.0, "A": 7.0, "B": 5.0}
    for e in events:
        base = grade_conv.get(e.get("pit_grade", ""), 3.0)
        # Momentum bonus
        if e.get("above_sma50") and e.get("above_sma200"):
            base += 1.0
        # Blended score bonus (continuous)
        bs = e.get("pit_blended_score") or 0
        if bs > 2.0:
            base += 0.5
        e["_conviction"] = min(base, 10.0)

    return events


def load_10b5_1_surprise_events(
    conn: sqlite3.Connection, start: str, end: str
) -> list[dict]:
    """Load buys by insiders who had prior 10b5-1 sells on the same ticker.

    PIT-safe: counts only 10b5-1 sells filed BEFORE each buy's filing_date.
    Returns widest set (any insider with 1+ prior 10b5-1 sell). Grid configs
    filter by min_10b5_1_sells threshold.
    """
    # Step 1: Load all 10b5-1 sells, sorted for efficient PIT counting
    sells = conn.execute("""
        SELECT insider_id, ticker, filing_date
        FROM trades
        WHERE trans_code = 'S' AND is_10b5_1 = 1
        ORDER BY insider_id, ticker, filing_date
    """).fetchall()

    # Index: (insider_id, ticker) -> sorted list of sell filing_dates
    sell_dates: dict[tuple[int, str], list[str]] = defaultdict(list)
    for insider_id, ticker, fd in sells:
        sell_dates[(insider_id, ticker)].append(fd)

    if not sell_dates:
        print("  WARNING: No 10b5-1 sells found in database")
        return []

    # Step 2: Load all buys by those insiders on those tickers
    insider_ticker_pairs = list(sell_dates.keys())

    # Build efficient query — batch by insider_id
    insider_ids = list({iid for iid, _ in insider_ticker_pairs})

    query = f"""
        SELECT {_BASE_COLS}
        {_BASE_JOIN}
          AND t.insider_id IN ({','.join('?' * len(insider_ids))})
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end, *insider_ids)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end, *insider_ids)).description]
    raw_events = [dict(zip(cols, r)) for r in rows]

    # Step 3: For each buy, count PIT-safe prior 10b5-1 sells (binary search)
    from bisect import bisect_left

    events = []
    for e in raw_events:
        key = (e["insider_id"], e["ticker"])
        dates = sell_dates.get(key)
        if not dates:
            continue
        # Count sells filed strictly before this buy's filing_date
        n_prior = bisect_left(dates, e["filing_date"])
        if n_prior == 0:
            continue
        e["_n_prior_10b5_sells"] = n_prior

        # Conviction: more prior sells = stronger signal (monotonic per validation)
        if n_prior >= 20:
            conv = 9.0
        elif n_prior >= 10:
            conv = 8.0
        elif n_prior >= 5:
            conv = 7.0
        elif n_prior >= 3:
            conv = 6.0
        else:
            conv = 5.0
        # Momentum bonus
        if e.get("above_sma50") and e.get("above_sma200"):
            conv += 1.0
        e["_conviction"] = min(conv, 10.0)
        events.append(e)

    return events


def load_reversal_dip_events(
    conn: sqlite3.Connection, start: str, end: str
) -> list[dict]:
    """Load rare reversal buys with dip context.

    Returns widest set (consecutive_sells >= 3, any dip).
    Grid configs filter by min_consecutive_sells and dip_threshold.
    """
    query = f"""
        SELECT {_BASE_COLS}
        {_BASE_JOIN}
          AND t.is_rare_reversal = 1
          AND COALESCE(t.consecutive_sells_before, 0) >= 3
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    events = [dict(zip(cols, r)) for r in rows]

    # Conviction: reversal depth + dip depth
    for e in events:
        csb = e.get("consecutive_sells_before") or 0
        dip = min(e.get("dip_3mo") or 0, e.get("dip_1mo") or 0)

        base = 4.0
        # Sell streak
        if csb >= 20:
            base += 3.0
        elif csb >= 10:
            base += 2.0
        elif csb >= 5:
            base += 1.0
        # Dip depth
        if dip <= -0.40:
            base += 2.0
        elif dip <= -0.25:
            base += 1.5
        elif dip <= -0.20:
            base += 1.0
        elif dip <= -0.15:
            base += 0.5
        # Grade bonus
        grade = e.get("pit_grade", "")
        if grade in ("A+", "A"):
            base += 1.0
        e["_conviction"] = min(base, 10.0)

    return events


def load_reversal_quality_events(
    conn: sqlite3.Connection, start: str, end: str
) -> list[dict]:
    """Load rare reversal buys by quality insiders (PIT grade A+/A/B).

    Returns widest set (is_rare_reversal + grade A-B).
    """
    query = f"""
        SELECT {_BASE_COLS}
        {_BASE_JOIN}
          AND t.is_rare_reversal = 1
          AND t.pit_grade IN ('A+', 'A', 'B')
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    events = [dict(zip(cols, r)) for r in rows]

    for e in events:
        grade = e.get("pit_grade", "C")
        base = 6.0 if grade in ("A+", "A") else 4.5
        csb = e.get("consecutive_sells_before") or 0
        if csb >= 10:
            base += 2.0
        elif csb >= 5:
            base += 1.0
        if e.get("is_largest_ever"):
            base += 0.5
        e["_conviction"] = min(base, 10.0)

    return events


# ---------------------------------------------------------------------------
# Event filtering for grid configs
# ---------------------------------------------------------------------------

def filter_events_for_config(
    events: list[dict], cfg: GridConfig
) -> list[dict]:
    """Apply strategy-specific grid filters to narrow the event set."""
    filtered = []
    for e in events:
        if cfg.strategy == "quality":
            # Grade filter
            grade_filter = cfg.filters.get("grade_filter", "A+/A")
            allowed = {"A+"} if grade_filter == "A+" else {"A+", "A"}
            if e.get("pit_grade") not in allowed:
                continue
            # Momentum always required for this strategy
            if not (e.get("above_sma50") and e.get("above_sma200")):
                continue
            # Min trade value
            min_val = cfg.filters.get("min_trade_value", 0)
            if min_val and (e.get("value") or 0) < min_val:
                continue

        elif cfg.strategy == "tenb":
            # Min prior 10b5-1 sells
            min_sells = cfg.filters.get("min_10b5_1_sells", 3)
            if (e.get("_n_prior_10b5_sells") or 0) < min_sells:
                continue
            # Momentum overlay
            if cfg.filters.get("require_momentum", False):
                if not (e.get("above_sma50") and e.get("above_sma200")):
                    continue

        elif cfg.strategy == "reversal":
            # Min consecutive sells
            min_csb = cfg.filters.get("min_consecutive_sells", 5)
            if (e.get("consecutive_sells_before") or 0) < min_csb:
                continue
            # 3-month dip threshold
            dip_thresh = cfg.filters.get("dip_threshold_3mo", -0.20)
            dip_3mo = e.get("dip_3mo") or 0
            if dip_3mo > dip_thresh:
                continue
            # Exclude 10b5-1
            if cfg.filters.get("exclude_10b5_1", True):
                if e.get("is_10b5_1"):
                    continue

        elif cfg.strategy == "reversal_quality":
            # Grade filter
            allowed = cfg.filters.get("grade_filter", {"A+", "A", "B"})
            if isinstance(allowed, str):
                allowed = set(allowed.split("/"))
            if e.get("pit_grade") not in allowed:
                continue

        filtered.append(e)
    return filtered


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------

def run_sim(
    events: list[dict],
    prices: PriceCache,
    calendar: list[str],
    cfg: GridConfig,
) -> SimResult:
    """Day-by-day portfolio simulation.

    Events must already have _entry_price and _entry_date set by
    compute_entry_prices(). Uses _entry_date for scheduling (NOT filing_date).
    """
    result = SimResult(config=cfg, events_available=len(events))
    if not events:
        result.final_equity = STARTING_CAPITAL
        return result

    equity = STARTING_CAPITAL
    open_positions: list[OpenPos] = []
    daily_returns: list[float] = []
    peak_equity = equity
    max_dd = 0.0
    completed_pnls: list[float] = []
    total_deployed_days = 0
    total_days = 0
    n_intraday = 0
    n_t1 = 0
    circuit_breaker_active = False

    # Index events by _entry_date
    events_by_entry: dict[str, list[dict]] = {}
    for e in events:
        ed = e.get("_entry_date")
        if ed and e.get("_entry_price") and e["_entry_price"] > 0:
            events_by_entry.setdefault(ed, []).append(e)

    for today in calendar:
        if today < START:
            continue

        # --- Exits ---
        still_open: list[OpenPos] = []
        day_pnl = 0.0

        for pos in open_positions:
            bar = prices.get_bar(pos.ticker, today)
            if bar is None:
                still_open.append(pos)
                continue

            o, h, l, c = bar
            pos.days_held += 1
            if c > pos.peak_price:
                pos.peak_price = c

            exited = False
            exit_reason = ""

            # Hard stop loss
            if cfg.stop_loss is not None and l and pos.entry_price > 0:
                dd = (l - pos.entry_price) / pos.entry_price
                if dd <= cfg.stop_loss:
                    exit_price = pos.entry_price * (1 + cfg.stop_loss)
                    pnl_pct = cfg.stop_loss
                    pnl_dollar = pos.dollar_amount * pnl_pct
                    equity += pnl_dollar
                    day_pnl += pnl_dollar
                    completed_pnls.append(pnl_pct)
                    result.stops_hit += 1
                    exited = True
                    exit_reason = "stop_loss"

            # Trailing stop
            if (not exited and cfg.exit_type == "trailing_stop"
                    and pos.peak_price > pos.entry_price):
                drawdown_from_peak = (c - pos.peak_price) / pos.peak_price
                if drawdown_from_peak <= -cfg.trailing_stop_pct:
                    pnl_pct = (c - pos.entry_price) / pos.entry_price
                    pnl_dollar = pos.dollar_amount * pnl_pct
                    equity += pnl_dollar
                    day_pnl += pnl_dollar
                    completed_pnls.append(pnl_pct)
                    result.trailing_exits += 1
                    exited = True
                    exit_reason = "trailing_stop"

            # Time exit
            if not exited and pos.days_held >= cfg.hold_days:
                pnl_pct = (c - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                equity += pnl_dollar
                day_pnl += pnl_dollar
                completed_pnls.append(pnl_pct)
                result.time_exits += 1
                exited = True
                exit_reason = "time_exit"

            if not exited:
                still_open.append(pos)

        open_positions = still_open

        # --- Circuit breaker ---
        if equity > peak_equity:
            peak_equity = equity
        current_dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        if current_dd > max_dd:
            max_dd = current_dd
        if current_dd >= cfg.circuit_breaker_dd:
            circuit_breaker_active = True
        elif current_dd < cfg.circuit_breaker_dd * 0.5:
            # Reset once drawdown recovers to half the threshold
            circuit_breaker_active = False

        # --- Entries ---
        todays_events = events_by_entry.get(today, [])
        todays_events.sort(key=lambda e: -e["_conviction"])

        held_tickers = {p.ticker for p in open_positions}
        entered_today: set[str] = set()
        replaced_today = False

        if not circuit_breaker_active:
            for event in todays_events:
                ticker = event["ticker"]
                if ticker in held_tickers or ticker in entered_today:
                    continue

                entry_price = event["_entry_price"]
                if entry_price <= 0:
                    continue

                # Skip penny stocks
                if entry_price < 2.0:
                    continue

                conv = event["_conviction"]

                # Capacity check
                if len(open_positions) + len(entered_today) >= cfg.max_concurrent:
                    if cfg.at_capacity == "skip" or replaced_today:
                        continue

                    # Find replacement candidate
                    candidate = None
                    if cfg.at_capacity == "replace_oldest":
                        candidates = [p for p in open_positions if p.days_held > 0]
                        if candidates:
                            candidate = max(candidates, key=lambda p: p.days_held)
                    elif cfg.at_capacity == "replace_weakest":
                        candidates = [p for p in open_positions if p.days_held > 0]
                        if candidates:
                            weakest = min(candidates, key=lambda p: p.conviction)
                            if conv > weakest.conviction + 1.5:
                                candidate = weakest

                    if candidate is None:
                        continue

                    # Close replacement
                    rep_close = prices.get_close(candidate.ticker, today)
                    if not rep_close or rep_close <= 0:
                        continue
                    rep_pnl_pct = (rep_close - candidate.entry_price) / candidate.entry_price
                    rep_pnl_dollar = candidate.dollar_amount * rep_pnl_pct
                    equity += rep_pnl_dollar
                    day_pnl += rep_pnl_dollar
                    completed_pnls.append(rep_pnl_pct)
                    open_positions = [p for p in open_positions if p.trade_id != candidate.trade_id]
                    held_tickers = {p.ticker for p in open_positions}
                    result.replacements += 1
                    replaced_today = True

                # Final capacity guard
                if len(open_positions) + len(entered_today) >= cfg.max_concurrent:
                    continue

                # Enter position
                dollar_amount = equity * cfg.position_size
                pos = OpenPos(
                    trade_id=event["trade_id"],
                    ticker=ticker,
                    entry_date=today,
                    entry_price=entry_price,
                    dollar_amount=dollar_amount,
                    conviction=conv,
                    peak_price=entry_price,
                )
                open_positions.append(pos)
                entered_today.add(ticker)
                held_tickers.add(ticker)

                # Track entry type
                if (event.get("filed_at")
                        and _filed_during_market_hours(event.get("filed_at"))
                        and event.get("_entry_date") == event.get("filing_date")):
                    n_intraday += 1
                else:
                    n_t1 += 1

        # Track stats
        if len(open_positions) > result.max_concurrent_seen:
            result.max_concurrent_seen = len(open_positions)
        total_deployed_days += len(open_positions)
        total_days += 1

        # Daily return for Sharpe
        prev_eq = equity - day_pnl
        if prev_eq > 0:
            daily_returns.append(day_pnl / prev_eq)

    # Close remaining positions at last available price
    for pos in open_positions:
        last_c = prices.get_close_on_or_before(pos.ticker, calendar[-1])
        if last_c and last_c > 0:
            pnl_pct = (last_c - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar
            completed_pnls.append(pnl_pct)

    # Compute metrics
    n = len(completed_pnls)
    result.trades = n
    result.wins = sum(1 for p in completed_pnls if p > 0)
    result.win_rate = result.wins / n * 100 if n else 0
    result.avg_return = sum(completed_pnls) / n * 100 if n else 0
    result.total_pnl = equity - STARTING_CAPITAL
    result.final_equity = equity

    years = max(
        (datetime.strptime(END, "%Y-%m-%d") - datetime.strptime(START, "%Y-%m-%d")).days / 365.25, 1
    )
    result.cagr = ((equity / STARTING_CAPITAL) ** (1 / years) - 1) * 100 if equity > 0 else 0

    if len(daily_returns) > 1:
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.stdev(daily_returns)
        result.sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0
    else:
        result.sharpe = 0

    result.max_drawdown = max_dd * 100
    result.avg_concurrent = total_deployed_days / total_days if total_days else 0
    result.avg_deployment = (result.avg_concurrent * cfg.position_size) * 100
    result.n_intraday_entries = n_intraday
    result.n_t1_entries = n_t1

    return result


# ---------------------------------------------------------------------------
# Grid builders
# ---------------------------------------------------------------------------

# Shared dimensions — kept tight to avoid combinatorial explosion
_POSITION_SIZES = [0.10, 0.20, 0.33]
_AT_CAPACITY = ["skip", "replace_oldest"]
_STOP_LOSSES = [-0.15, -0.20, None]


def build_quality_grid() -> list[GridConfig]:
    """Grid for Quality + Momentum strategy. ~216 configs."""
    configs = []
    for ps in _POSITION_SIZES:
        mc = max(1, int(1.0 / ps))
        for ac in _AT_CAPACITY:
            for hold in [30, 60, 90]:
                for sl in _STOP_LOSSES:
                    for gf in ["A+", "A+/A"]:
                        # Fixed hold
                        configs.append(GridConfig(
                            strategy="quality",
                            position_size=ps, max_concurrent=mc,
                            at_capacity=ac, hold_days=hold,
                            stop_loss=sl, exit_type="fixed_hold",
                            trailing_stop_pct=0.0,
                            circuit_breaker_dd=0.15,
                            filters={"grade_filter": gf},
                        ))
                        # Trailing stop (edge compounds — trailing may capture upside)
                        configs.append(GridConfig(
                            strategy="quality",
                            position_size=ps, max_concurrent=mc,
                            at_capacity=ac, hold_days=hold,
                            stop_loss=sl, exit_type="trailing_stop",
                            trailing_stop_pct=0.15,
                            circuit_breaker_dd=0.15,
                            filters={"grade_filter": gf},
                        ))
    return configs


def build_10b5_1_grid() -> list[GridConfig]:
    """Grid for 10b5-1 Surprise Buy strategy. ~432 configs."""
    configs = []
    for ps in _POSITION_SIZES:
        mc = max(1, int(1.0 / ps))
        for ac in _AT_CAPACITY:
            for hold in [60, 90, 120]:
                for sl in _STOP_LOSSES:
                    for min_sells in [3, 5, 10]:
                        for mom in [True, False]:
                            # Fixed hold
                            configs.append(GridConfig(
                                strategy="tenb",
                                position_size=ps, max_concurrent=mc,
                                at_capacity=ac, hold_days=hold,
                                stop_loss=sl, exit_type="fixed_hold",
                                trailing_stop_pct=0.0,
                                circuit_breaker_dd=0.15,
                                filters={
                                    "min_10b5_1_sells": min_sells,
                                    "require_momentum": mom,
                                },
                            ))
                            # Trailing stop (signal compounds — let winners run)
                            configs.append(GridConfig(
                                strategy="tenb",
                                position_size=ps, max_concurrent=mc,
                                at_capacity=ac, hold_days=hold,
                                stop_loss=sl, exit_type="trailing_stop",
                                trailing_stop_pct=0.15,
                                circuit_breaker_dd=0.15,
                                filters={
                                    "min_10b5_1_sells": min_sells,
                                    "require_momentum": mom,
                                },
                            ))
    return configs


def build_reversal_grid() -> list[GridConfig]:
    """Grid for Deep Reversal + Dip strategy. ~324 configs."""
    configs = []
    for ps in _POSITION_SIZES:
        mc = max(1, int(1.0 / ps))
        for ac in _AT_CAPACITY:
            for hold in [21, 30, 45]:
                for sl in _STOP_LOSSES:
                    # Mean reversion = fixed_hold only (trailing hurts per validation)
                    for min_csb in [5, 10, 20]:
                        for dip_thresh in [-0.15, -0.20, -0.25]:
                            configs.append(GridConfig(
                                strategy="reversal",
                                position_size=ps, max_concurrent=mc,
                                at_capacity=ac, hold_days=hold,
                                stop_loss=sl, exit_type="fixed_hold",
                                trailing_stop_pct=0.0,
                                circuit_breaker_dd=0.15,
                                filters={
                                    "min_consecutive_sells": min_csb,
                                    "dip_threshold_3mo": dip_thresh,
                                    "exclude_10b5_1": True,
                                },
                            ))
    return configs


# ---------------------------------------------------------------------------
# Dedup utility
# ---------------------------------------------------------------------------

def dedup_events(events: list[dict]) -> list[dict]:
    """Deduplicate by trade_id and (ticker, filing_date)."""
    seen_ids: set[int] = set()
    seen_td: set[tuple[str, str]] = set()
    result = []
    for e in events:
        tid = e["trade_id"]
        td = (e["ticker"], e["filing_date"])
        if tid in seen_ids or td in seen_td:
            continue
        seen_ids.add(tid)
        seen_td.add(td)
        result.append(e)
    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_HEADERS = [
    "strategy", "position_size", "max_concurrent", "at_capacity",
    "hold_days", "stop_loss", "exit_type", "trailing_stop_pct",
    "circuit_breaker_dd",
    # Strategy-specific filters
    "filter_grade", "filter_min_trade_value",
    "filter_min_10b5_1_sells", "filter_require_momentum",
    "filter_min_consecutive_sells", "filter_dip_threshold_3mo",
    "filter_exclude_10b5_1",
    # Results
    "events_available", "trades", "wins", "win_rate", "avg_return",
    "cagr", "sharpe", "max_drawdown", "final_equity", "total_pnl",
    "stops_hit", "trailing_exits", "time_exits", "replacements",
    "max_concurrent_seen", "avg_concurrent", "avg_deployment",
    "n_intraday_entries", "n_t1_entries",
]


def result_to_row(r: SimResult) -> list:
    c = r.config
    f = c.filters
    return [
        c.strategy, c.position_size, c.max_concurrent, c.at_capacity,
        c.hold_days, c.stop_loss or "", c.exit_type, c.trailing_stop_pct,
        c.circuit_breaker_dd,
        f.get("grade_filter", ""), f.get("min_trade_value", ""),
        f.get("min_10b5_1_sells", ""), f.get("require_momentum", ""),
        f.get("min_consecutive_sells", ""), f.get("dip_threshold_3mo", ""),
        f.get("exclude_10b5_1", ""),
        r.events_available, r.trades, r.wins, round(r.win_rate, 2),
        round(r.avg_return, 3), round(r.cagr, 2), round(r.sharpe, 3),
        round(r.max_drawdown, 2), round(r.final_equity, 2),
        round(r.total_pnl, 2), r.stops_hit, r.trailing_exits,
        r.time_exits, r.replacements, r.max_concurrent_seen,
        round(r.avg_concurrent, 2), round(r.avg_deployment, 2),
        r.n_intraday_entries, r.n_t1_entries,
    ]


def print_top_results(results: list[SimResult], strategy_name: str, n: int = 20):
    """Print top N results by Sharpe."""
    valid = [r for r in results if r.trades >= 10]
    if not valid:
        print(f"  No configs with >= 10 trades for {strategy_name}")
        return

    valid.sort(key=lambda r: -r.sharpe)

    print(f"\n{'=' * 140}")
    print(f"  {strategy_name}: TOP {n} by Sharpe (min 10 trades)")
    print(f"{'=' * 140}")
    hdr = f"{'#':>3} {'Config':55s} {'Evts':>5} {'Trd':>5} {'WR':>6} {'AvgR':>6} {'CAGR':>6} {'Sharpe':>7} {'MaxDD':>6} {'Equity':>10} {'5min':>4} {'T+1':>4}"
    print(hdr)
    print("-" * 140)
    for i, r in enumerate(valid[:n]):
        c = r.config
        # Build compact label
        filt = []
        for k, v in sorted(c.filters.items()):
            if v not in ("", 0, False):
                filt.append(f"{k}={v}")
        label = f"{c.position_size:.0%}/{c.max_concurrent}pos/{c.at_capacity}/{c.hold_days}d"
        if c.stop_loss is not None:
            label += f"/sl{c.stop_loss:.0%}"
        if c.exit_type == "trailing_stop":
            label += f"/tr{c.trailing_stop_pct:.0%}"
        if filt:
            label += "/" + ",".join(filt)

        print(
            f"{i+1:>3} {label:55s} {r.events_available:>5} {r.trades:>5} "
            f"{r.win_rate:>5.1f}% {r.avg_return:>5.2f}% {r.cagr:>5.1f}% "
            f"{r.sharpe:>7.2f} {r.max_drawdown:>5.1f}% "
            f"${r.final_equity:>9,.0f} {r.n_intraday_entries:>4} {r.n_t1_entries:>4}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_strategy_grid(
    strategy: str,
    conn: sqlite3.Connection,
    prices: PriceCache,
    intraday_conn: sqlite3.Connection | None,
    calendar: list[str],
) -> list[SimResult]:
    """Load events, compute entry prices, build grid, run all configs."""

    print(f"\n{'#' * 70}")
    print(f"  STRATEGY: {strategy.upper()}")
    print(f"{'#' * 70}")

    # Load events
    t0 = time.time()
    if strategy == "quality":
        raw_events = load_quality_momentum_events(conn, START, END)
        grid = build_quality_grid()
    elif strategy == "tenb":
        raw_events = load_10b5_1_surprise_events(conn, START, END)
        grid = build_10b5_1_grid()
    elif strategy == "reversal":
        raw_events = load_reversal_dip_events(conn, START, END)
        grid = build_reversal_grid()
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    events = dedup_events(raw_events)
    print(f"  {len(raw_events)} raw events -> {len(events)} after dedup ({time.time()-t0:.1f}s)")

    # Pre-compute entry prices (5-min intraday + T+1 open)
    # compute_entry_prices expects list[tuple[str, dict]]
    tagged = [(strategy, e) for e in events]
    compute_entry_prices(tagged, intraday_conn, prices, calendar)

    # Remove events with no entry price
    events = [e for e in events if e.get("_entry_price") and e["_entry_price"] > 0]
    print(f"  {len(events)} events with valid entry price")

    # Count entry types
    n_intra = sum(1 for e in events
                  if e.get("_entry_date") == e.get("filing_date")
                  and e.get("filed_at")
                  and _filed_during_market_hours(e.get("filed_at")))
    print(f"  Entry type split: {n_intra} intraday 5-min, {len(events)-n_intra} T+1 open")

    print(f"\n  Running {len(grid)} configurations...", flush=True)

    results: list[SimResult] = []
    t1 = time.time()
    for i, cfg in enumerate(grid):
        # Filter events for this config
        filtered = filter_events_for_config(events, cfg)
        r = run_sim(filtered, prices, calendar, cfg)
        results.append(r)
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t1
            rate = (i + 1) / elapsed
            eta = (len(grid) - i - 1) / rate if rate > 0 else 0
            print(f"    [{i+1}/{len(grid)}] {rate:.0f} configs/sec, ETA {eta:.0f}s", flush=True)

    elapsed = time.time() - t0
    print(f"\n  {strategy} grid complete: {len(grid)} configs in {elapsed:.1f}s")

    print_top_results(results, strategy)
    return results


def main():
    parser = argparse.ArgumentParser(description="Multi-strategy grid search")
    parser.add_argument("--strategy", default="all",
                        choices=["quality", "tenb", "reversal", "all"],
                        help="Which strategy to grid search")
    args = parser.parse_args()

    strategies = (
        ["quality", "tenb", "reversal"] if args.strategy == "all"
        else [args.strategy]
    )

    # Open DB connections
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    prices_conn = sqlite3.connect(str(PRICES_DB))
    intraday_conn = sqlite3.connect(str(INTRADAY_DB)) if INTRADAY_DB.exists() else None

    # Build price cache and trading calendar (shared across strategies)
    print("Loading price data...", flush=True)
    t0 = time.time()

    # Get all tickers that could be needed (broad query)
    all_tickers_row = conn.execute(
        "SELECT DISTINCT ticker FROM trades WHERE trans_code = 'P' AND filing_date >= ?",
        (START,),
    ).fetchall()
    all_tickers = {r[0] for r in all_tickers_row}

    cache_start = (datetime.strptime(START, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    cache_end = (datetime.strptime(END, "%Y-%m-%d") + timedelta(days=150)).strftime("%Y-%m-%d")

    prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)
    calendar = _build_trading_calendar(prices_conn, START, cache_end)
    prices_conn.close()

    print(f"  Price cache: {len(all_tickers)} tickers, {len(calendar)} trading days ({time.time()-t0:.1f}s)\n")

    # Run each strategy's grid
    all_results: list[SimResult] = []
    for strat in strategies:
        results = run_strategy_grid(strat, conn, prices, intraday_conn, calendar)
        all_results.extend(results)

    # Write combined CSV
    out_dir = Path(__file__).resolve().parents[2] / "reports" / "grid_search"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "strategy_grid.csv"

    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADERS)
        for r in all_results:
            w.writerow(result_to_row(r))

    print(f"\n{'=' * 70}")
    print(f"  Results written to {csv_path}")
    print(f"  Total configs: {len(all_results)}")
    print(f"{'=' * 70}")

    if intraday_conn:
        intraday_conn.close()
    conn.close()


if __name__ == "__main__":
    main()
