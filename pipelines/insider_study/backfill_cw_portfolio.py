#!/usr/bin/env python3
"""
Backfill CW strategy portfolios with historical trades.

Replays cw_reversal and cw_composite strategies on historical insider filings
from 2020-01-01 through today. Uses actual daily prices from prices.db for
exit simulation and tracks running equity with position limits.

Usage:
    python3 pipelines/insider_study/backfill_cw_portfolio.py
    python3 pipelines/insider_study/backfill_cw_portfolio.py --strategy cw_reversal
    python3 pipelines/insider_study/backfill_cw_portfolio.py --strategy cw_composite
    python3 pipelines/insider_study/backfill_cw_portfolio.py --start 2022-01-01
    python3 pipelines/insider_study/backfill_cw_portfolio.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from pipelines.insider_study.conviction_score import (
        compute_conviction, MIN_CONVICTION, REPLACEMENT_ADVANTAGE,
    )
except ModuleNotFoundError:
    from conviction_score import (
        compute_conviction, MIN_CONVICTION, REPLACEMENT_ADVANTAGE,
    )

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"

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
        self._adv_cache: dict[tuple[str, str], float | None] = {}

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

        # Precompute ADV for volume-aware sizing
        self._volume_data: dict[tuple[str, str], float] = {}
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i : i + batch_size]
            ph = ",".join("?" * len(batch))
            rows = prices_conn.execute(
                f"SELECT ticker, date, close * volume as dv FROM daily_prices "
                f"WHERE ticker IN ({ph}) AND date BETWEEN ? AND ? AND volume > 0",
                batch + [start, end],
            ).fetchall()
            for ticker, date, dv in rows:
                if dv and dv > 0:
                    self._volume_data[(ticker, date)] = dv

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

    def get_avg_daily_dollar_volume(self, ticker: str, date: str) -> float | None:
        """Get 10-day average daily dollar volume for a ticker as of a date."""
        key = (ticker, date[:7])
        if key in self._adv_cache:
            return self._adv_cache[key]
        dates = self._dates_by_ticker.get(ticker, [])
        if not dates:
            self._adv_cache[key] = None
            return None
        # Find the 10 trading days before this date
        dvs: list[float] = []
        for d in reversed(dates):
            if d >= date:
                continue
            dv = self._volume_data.get((ticker, d))
            if dv:
                dvs.append(dv)
            if len(dvs) >= 10:
                break
        val = sum(dvs) / len(dvs) if dvs else None
        self._adv_cache[key] = val
        return val

    def has_data(self, ticker: str) -> bool:
        return ticker in self._dates_by_ticker


# ---------------------------------------------------------------------------
# Trading calendar from SPY dates
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
# Grade mapping
# ---------------------------------------------------------------------------

GRADE_QUALITY = {"A": 10, "B": 8, "C": 6, "D": 4, "F": 2}

# ---------------------------------------------------------------------------
# Strategy configs
# ---------------------------------------------------------------------------


@dataclass
class ThesisConfig:
    """Configuration for a single thesis within a strategy."""
    name: str
    position_size: float          # fraction of equity per trade
    max_concurrent: int           # max open positions across strategy
    target_hold: int              # target calendar days
    stop_pct: float               # hard stop loss (negative, e.g., -0.15)
    trailing_stop: bool = False   # use trailing stop instead of hard stop
    exit_rule_name: str = ""      # label for exit_reason


@dataclass
class StrategyConfig:
    """Top-level strategy definition."""
    name: str
    starting_capital: float
    theses: list[ThesisConfig]
    max_concurrent: int


# cw_reversal: single thesis
# Reversal Sharpe peaks at 90d but 30d is better for turnover/deployment
CW_REVERSAL = StrategyConfig(
    name="cw_reversal",
    starting_capital=100_000.0,
    max_concurrent=6,  # soft 5 / hard 6 at 16.7% = 83% / 100%
    theses=[
        ThesisConfig(
            name="reversal",
            position_size=0.167,
            max_concurrent=6,
            target_hold=30,
            stop_pct=-0.15,
            trailing_stop=False,
            exit_rule_name="time_exit_30d",
        ),
    ],
)

# cw_composite: three theses sharing one equity pool
# Max deployment: 20 positions * 3.3% = 66% (leaves ~34% cash buffer)
CW_COMPOSITE = StrategyConfig(
    name="cw_composite",
    starting_capital=100_000.0,
    max_concurrent=20,  # 20 * 3.3% = 66% max deployment
    theses=[
        ThesisConfig(
            name="reversal",
            position_size=0.033,
            max_concurrent=20,
            target_hold=30,  # 30d (matches reversal-only strategy)
            stop_pct=-0.15,
            trailing_stop=False,
            exit_rule_name="time_exit_30d",
        ),
        ThesisConfig(
            name="dip_cluster",
            position_size=0.033,
            max_concurrent=20,
            target_hold=30,  # 30d — optimization showed 30d >> 14d
            stop_pct=-0.15,
            trailing_stop=True,
            exit_rule_name="time_exit_30d",
        ),
        ThesisConfig(
            name="momentum_largest",
            position_size=0.033,
            max_concurrent=20,
            target_hold=30,  # 30d — same finding
            stop_pct=-0.15,
            trailing_stop=True,
            exit_rule_name="time_exit_30d",
        ),
    ],
)

STRATEGIES = {
    "cw_reversal": CW_REVERSAL,
    "cw_composite": CW_COMPOSITE,
}


# ---------------------------------------------------------------------------
# Open position tracker
# ---------------------------------------------------------------------------

@dataclass
class OpenPosition:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    thesis: str
    target_hold: int
    stop_pct: float
    trailing_stop: bool
    event: dict         # full event row for reasoning
    conviction: float = 0.0  # conviction score for replacement logic
    peak_price: float = 0.0  # highest close seen (for trailing stop)
    days_held: int = 0       # trading days in position


# ---------------------------------------------------------------------------
# Event loading
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
               tr.entry_price, tr.exit_price_7d, tr.return_7d,
               tr.exit_price_30d, tr.return_30d,
               tr.exit_price_90d, tr.return_90d,
               i.display_name, i.name AS insider_name_raw
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        LEFT JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.is_rare_reversal = 1
          AND COALESCE(t.consecutive_sells_before, 0) >= 5
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price > 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    return [dict(zip(cols, r)) for r in rows]


def _load_dip_cluster_events(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Load dip_cluster-qualifying events (dip + cluster signal)."""
    query = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.company, t.title,
               t.trade_type, t.trade_date, t.filing_date, t.price, t.value,
               t.is_csuite, t.signal_grade, t.is_rare_reversal,
               t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
               t.above_sma50, t.above_sma200, t.is_largest_ever,
               t.is_recurring, t.is_tax_sale, t.cohen_routine,
               tr.entry_price, tr.exit_price_7d, tr.return_7d,
               tr.exit_price_30d, tr.return_30d,
               tr.exit_price_90d, tr.return_90d,
               i.display_name, i.name AS insider_name_raw
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        LEFT JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND (t.dip_1mo <= -0.15 OR t.dip_3mo <= -0.25)
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price > 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    events = [dict(zip(cols, r)) for r in rows]

    # Filter to those with a top_trade signal (cluster)
    if not events:
        return []
    trade_ids = [e["trade_id"] for e in events]
    cluster_ids = _get_cluster_trade_ids(conn, trade_ids)
    return [e for e in events if e["trade_id"] in cluster_ids]


def _load_momentum_largest_events(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Load momentum_largest-qualifying events."""
    query = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.company, t.title,
               t.trade_type, t.trade_date, t.filing_date, t.price, t.value,
               t.is_csuite, t.signal_grade, t.is_rare_reversal,
               t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
               t.above_sma50, t.above_sma200, t.is_largest_ever,
               t.is_recurring, t.is_tax_sale, t.cohen_routine,
               tr.entry_price, tr.exit_price_7d, tr.return_7d,
               tr.exit_price_30d, tr.return_30d,
               tr.exit_price_90d, tr.return_90d,
               i.display_name, i.name AS insider_name_raw
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        LEFT JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.above_sma50 = 1
          AND t.above_sma200 = 1
          AND t.is_largest_ever = 1
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.filing_date BETWEEN ? AND ?
          AND tr.entry_price IS NOT NULL
          AND tr.entry_price > 0
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(query, (start, end)).fetchall()
    cols = [d[0] for d in conn.execute(query, (start, end)).description]
    return [dict(zip(cols, r)) for r in rows]


def _get_cluster_trade_ids(conn: sqlite3.Connection, trade_ids: list[int]) -> set[int]:
    """Return trade_ids that have a top_trade signal with cluster reason."""
    if not trade_ids:
        return set()
    cluster_ids: set[int] = set()
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT trade_id, metadata FROM trade_signals
            WHERE trade_id IN ({placeholders})
              AND signal_type = 'top_trade'
        """, batch).fetchall()
        for tid, meta in rows:
            if meta:
                try:
                    m = json.loads(meta)
                    if "cluster" in m.get("reason", ""):
                        cluster_ids.add(tid)
                        continue
                except (json.JSONDecodeError, TypeError):
                    pass
            # Even without cluster reason, top_trade counts
            cluster_ids.add(tid)
    return cluster_ids


def _get_top_trade_ids(conn: sqlite3.Connection, trade_ids: list[int]) -> set[int]:
    """Return trade_ids that have any top_trade signal."""
    if not trade_ids:
        return set()
    result: set[int] = set()
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT DISTINCT trade_id FROM trade_signals
            WHERE trade_id IN ({placeholders})
              AND signal_type = 'top_trade'
        """, batch).fetchall()
        for (tid,) in rows:
            result.add(tid)
    return result


def _get_cluster_reason_ids(conn: sqlite3.Connection, trade_ids: list[int]) -> set[int]:
    """Return trade_ids that have a top_trade signal with '3plus_cluster' reason."""
    if not trade_ids:
        return set()
    cluster_ids: set[int] = set()
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT trade_id, metadata FROM trade_signals
            WHERE trade_id IN ({placeholders})
              AND signal_type = 'top_trade'
        """, batch).fetchall()
        for tid, meta in rows:
            if meta:
                try:
                    m = json.loads(meta)
                    if "cluster" in m.get("reason", ""):
                        cluster_ids.add(tid)
                except (json.JSONDecodeError, TypeError):
                    pass
    return cluster_ids


def _get_pit_score(conn: sqlite3.Connection, insider_id: int, ticker: str,
                   filing_date: str) -> float | None:
    """Get PIT blended_score: most recent as_of_date <= filing_date."""
    row = conn.execute("""
        SELECT blended_score FROM insider_ticker_scores
        WHERE insider_id = ? AND ticker = ?
          AND sufficient_data = 1
          AND as_of_date <= ?
        ORDER BY as_of_date DESC
        LIMIT 1
    """, (insider_id, ticker, filing_date)).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Portfolio trade record (for DB insertion)
# ---------------------------------------------------------------------------

@dataclass
class PortfolioTrade:
    """A completed portfolio trade ready for DB insertion."""
    strategy: str
    trade_id: int
    ticker: str
    company: str | None
    entry_date: str
    entry_price: float
    exit_date: str | None
    exit_price: float | None
    hold_days: int | None
    target_hold: int
    stop_pct: float
    stop_hit: int
    pnl_pct: float | None
    pnl_dollar: float | None
    position_size: float
    portfolio_value: float
    equity_after: float
    insider_name: str | None
    signal_quality: float | None
    exit_reason: str
    entry_reasoning: str    # JSON
    exit_reasoning: str     # JSON
    filing_date: str | None
    trade_date: str | None
    trade_value: float | None
    signal_grade: str | None
    is_csuite: int | None
    is_rare_reversal: int | None
    is_cluster: int
    insider_title: str | None
    peak_return: float | None
    dollar_amount: float
    thesis: str


def run_strategy(conn: sqlite3.Connection, strategy_cfg: StrategyConfig,
                 start: str, end: str, dry_run: bool = False) -> list[PortfolioTrade]:
    """
    Run a full historical replay of a CW strategy using day-by-day simulation.

    Walks through every trading day from start to end. On each day:
      1. Check all open positions for exit conditions (stop, trailing stop, time)
      2. Close any that trigger, update equity
      3. Gather new signals whose filing_date < today (T+1 entry)
      4. Sort new signals by conviction (highest first)
      5. Enter new positions up to capacity

    This prevents the capacity violation bug that occurs when pre-computing
    exit dates at entry time.
    """
    print(f"\n{'='*70}")
    print(f"  Strategy: {strategy_cfg.name}")
    print(f"  Period:   {start} to {end}")
    print(f"  Capital:  ${strategy_cfg.starting_capital:,.0f}")
    print(f"  Max concurrent: {strategy_cfg.max_concurrent}")
    print(f"{'='*70}\n")

    # Load events for each thesis
    thesis_events: dict[str, list[dict]] = {}
    for tc in strategy_cfg.theses:
        if tc.name == "reversal":
            evts = _load_reversal_events(conn, start, end)
        elif tc.name == "dip_cluster":
            evts = _load_dip_cluster_events(conn, start, end)
        elif tc.name == "momentum_largest":
            evts = _load_momentum_largest_events(conn, start, end)
        else:
            evts = []
        print(f"  {tc.name}: {len(evts)} qualifying events")
        thesis_events[tc.name] = evts

    # Build thesis config lookup
    thesis_cfg_map = {tc.name: tc for tc in strategy_cfg.theses}

    # Merge all events, tag with thesis, sort by filing_date
    thesis_order = [tc.name for tc in strategy_cfg.theses]
    all_events: list[tuple[str, dict]] = []
    for thesis_name, evts in thesis_events.items():
        for e in evts:
            all_events.append((thesis_name, e))

    # Deduplicate: a trade_id can only appear once per strategy. If it qualifies
    # for multiple theses, take the first thesis in config order.
    all_events.sort(key=lambda x: (x[1]["filing_date"], thesis_order.index(x[0]), x[1]["trade_id"]))

    seen_trade_ids: set[int] = set()
    seen_ticker_dates: set[tuple[str, str]] = set()
    deduped: list[tuple[str, dict]] = []
    for thesis_name, event in all_events:
        tid = event["trade_id"]
        ticker_date = (event["ticker"], event["filing_date"])
        if tid in seen_trade_ids:
            continue
        if ticker_date in seen_ticker_dates:
            continue
        seen_trade_ids.add(tid)
        seen_ticker_dates.add(ticker_date)
        deduped.append((thesis_name, event))
    all_events = deduped
    print(f"  Total unique events: {len(all_events)} (after ticker+date dedup)")

    # Preload cluster IDs for is_cluster flag
    all_trade_ids = [e["trade_id"] for _, e in all_events]
    cluster_reason_ids = _get_cluster_reason_ids(conn, all_trade_ids)

    # Compute conviction scores and filter by minimum
    for thesis_name, event in all_events:
        event["_thesis"] = thesis_name
        event["_conviction"] = compute_conviction(
            thesis=thesis_name,
            signal_grade=event.get("signal_grade"),
            consecutive_sells=event.get("consecutive_sells_before"),
            dip_1mo=event.get("dip_1mo"),
            dip_3mo=event.get("dip_3mo"),
            is_largest_ever=bool(event.get("is_largest_ever")),
            above_sma50=bool(event.get("above_sma50")),
            above_sma200=bool(event.get("above_sma200")),
        )

    all_events = [(t, e) for t, e in all_events if e["_conviction"] >= MIN_CONVICTION]

    # Skip penny stocks
    all_events = [(t, e) for t, e in all_events
                  if not (e.get("entry_price", 0) and e["entry_price"] < 2.0)]

    # Index events by filing_date (entry is T+1, i.e. first trading day after filing)
    events_by_filing: dict[str, list[tuple[str, dict]]] = {}
    for thesis_name, event in all_events:
        fd = event["filing_date"]
        if fd not in events_by_filing:
            events_by_filing[fd] = []
        events_by_filing[fd].append((thesis_name, event))

    # Collect all tickers for price cache
    all_tickers = {e["ticker"] for _, e in all_events}

    # Open prices.db connection for PriceCache and trading calendar
    prices_conn = sqlite3.connect(str(PRICES_DB))
    try:
        # Build price cache with buffer
        cache_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        cache_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")
        prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)

        # Build trading calendar
        trading_days = _build_trading_calendar(prices_conn, start, cache_end)
    finally:
        prices_conn.close()

    if not trading_days:
        print("  No trading days found in calendar. Aborting.")
        return []

    # PIT score cache
    pit_cache: dict[tuple[int, str, str], float | None] = {}

    # --- Simulation state ---
    equity = strategy_cfg.starting_capital
    open_positions: list[OpenPosition] = []
    completed_trades: list[PortfolioTrade] = []

    t0 = time.time()
    skipped_capacity = 0
    replacements = 0
    max_concurrent_seen = 0

    # Track which filing dates have been processed for entries
    pending_entries: list[tuple[str, dict]] = []
    last_filing_checked = ""

    for day_idx, today in enumerate(trading_days):
        if today < start:
            continue

        # --- Step 1: Gather new signals filed before today (T+1 entry) ---
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
                # No price data today -- still open, don't increment days_held
                still_open.append(pos)
                continue

            o, h, l, c = bar
            pos.days_held += 1

            # Track peak for trailing stop
            if c > pos.peak_price:
                pos.peak_price = c

            tc = thesis_cfg_map[pos.thesis]
            exit_triggered = False
            exit_price = None
            exit_reason = ""
            stop_hit = False

            # Hard stop: did close breach the stop level?
            current_return = (c - pos.entry_price) / pos.entry_price
            if current_return <= pos.stop_pct:
                exit_price = pos.entry_price * (1 + pos.stop_pct)
                exit_reason = "stop_loss"
                stop_hit = True
                exit_triggered = True

            # Trailing stop
            if not exit_triggered and pos.trailing_stop and pos.peak_price > pos.entry_price:
                drawdown_from_peak = (c - pos.peak_price) / pos.peak_price
                if drawdown_from_peak <= pos.stop_pct:
                    exit_price = c
                    exit_reason = "trailing_stop"
                    stop_hit = True
                    exit_triggered = True

            # Time exit
            if not exit_triggered and pos.days_held >= pos.target_hold:
                exit_price = c
                exit_reason = f"time_exit_{pos.target_hold}d"
                exit_triggered = True

            if exit_triggered and exit_price is not None:
                pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                equity += pnl_dollar

                peak_return = (pos.peak_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0.0

                # PIT score lookup
                ev = pos.event
                filing_date = ev.get("filing_date", "")
                cache_key = (ev["insider_id"], ev["ticker"], filing_date)
                if cache_key not in pit_cache:
                    pit_cache[cache_key] = _get_pit_score(
                        conn, ev["insider_id"], ev["ticker"], filing_date
                    )
                pit_score = pit_cache[cache_key]

                insider_name = ev.get("display_name") or ev.get("insider_name_raw") or ""
                signal_grade = ev.get("signal_grade")
                signal_quality = GRADE_QUALITY.get(signal_grade) if signal_grade else None
                is_cluster = 1 if ev["trade_id"] in cluster_reason_ids else 0

                entry_reasoning = json.dumps({
                    "thesis": pos.thesis,
                    "conviction": pos.conviction,
                    "consecutive_sells_before": ev.get("consecutive_sells_before"),
                    "is_rare_reversal": bool(ev.get("is_rare_reversal")),
                    "signal_grade": signal_grade,
                    "insider_name": insider_name,
                    "insider_title": ev.get("title"),
                    "trade_value": ev.get("value"),
                    "dip_1mo": ev.get("dip_1mo"),
                    "dip_3mo": ev.get("dip_3mo"),
                    "above_sma50": ev.get("above_sma50"),
                    "above_sma200": ev.get("above_sma200"),
                    "is_largest_ever": ev.get("is_largest_ever"),
                    "pit_score": round(pit_score, 4) if pit_score else None,
                    "why": _build_why_text(pos.thesis, ev, insider_name),
                })

                exit_reasoning_json = json.dumps({
                    "exit_rule": exit_reason,
                    "target_hold": pos.target_hold,
                    "actual_hold": pos.days_held,
                    "peak_return_pct": round(peak_return * 100, 2),
                    "exit_return_pct": round(pnl_pct * 100, 2),
                    "stop_pct": round(pos.stop_pct * 100, 1),
                    "stop_hit": stop_hit,
                })

                portfolio_value = equity - pnl_dollar
                equity_after = equity

                trade = PortfolioTrade(
                    strategy=strategy_cfg.name,
                    trade_id=pos.trade_id,
                    ticker=pos.ticker,
                    company=ev.get("company"),
                    entry_date=pos.entry_date,
                    entry_price=round(pos.entry_price, 4),
                    exit_date=today,
                    exit_price=round(exit_price, 4),
                    hold_days=pos.days_held,
                    target_hold=pos.target_hold,
                    stop_pct=pos.stop_pct,
                    stop_hit=1 if stop_hit else 0,
                    pnl_pct=round(pnl_pct, 6),
                    pnl_dollar=round(pnl_dollar, 2),
                    position_size=tc.position_size,
                    portfolio_value=round(portfolio_value, 2),
                    equity_after=round(equity_after, 2),
                    insider_name=insider_name,
                    signal_quality=signal_quality,
                    exit_reason=exit_reason,
                    entry_reasoning=entry_reasoning,
                    exit_reasoning=exit_reasoning_json,
                    filing_date=filing_date,
                    trade_date=ev.get("trade_date"),
                    trade_value=ev.get("value"),
                    signal_grade=signal_grade,
                    is_csuite=ev.get("is_csuite"),
                    is_rare_reversal=ev.get("is_rare_reversal"),
                    is_cluster=is_cluster,
                    insider_title=ev.get("title"),
                    peak_return=round(peak_return, 4),
                    dollar_amount=round(pos.dollar_amount, 2),
                    thesis=pos.thesis,
                )
                completed_trades.append(trade)
                continue  # Don't add to still_open

            # Still open
            still_open.append(pos)

        open_positions = still_open

        # --- Step 3: Process new entries from pending_entries ---
        # Sort pending by conviction descending so highest-conviction enters first
        pending_entries.sort(key=lambda x: -x[1]["_conviction"])

        entered_tickers_today: set[str] = set()
        held_tickers = {p.ticker for p in open_positions}

        for thesis_name, event in pending_entries:
            ticker = event["ticker"]

            # Skip if already holding this ticker
            if ticker in held_tickers or ticker in entered_tickers_today:
                continue

            # Check if we have price data for entry (use open price of entry day)
            entry_price = prices.get_open(ticker, today)
            if entry_price is None or entry_price <= 0:
                entry_price = prices.get_close(ticker, today)
            if entry_price is None or entry_price <= 0:
                continue

            conviction = event["_conviction"]
            tc = thesis_cfg_map[thesis_name]

            # --- Capacity check ---
            if len(open_positions) >= strategy_cfg.max_concurrent:
                if strategy_cfg.name == "cw_reversal":
                    # Reversal: replace weakest if incoming has REPLACEMENT_ADVANTAGE
                    weakest = min(open_positions, key=lambda p: p.conviction)
                    if conviction >= weakest.conviction + REPLACEMENT_ADVANTAGE:
                        # Replace: close weakest at today's close
                        rep_close = prices.get_close(weakest.ticker, today)
                        if rep_close is None or rep_close <= 0:
                            skipped_capacity += 1
                            continue
                        rep_pnl_pct = (rep_close - weakest.entry_price) / weakest.entry_price
                        rep_pnl_dollar = weakest.dollar_amount * rep_pnl_pct
                        equity += rep_pnl_dollar

                        rep_peak_ret = (weakest.peak_price - weakest.entry_price) / weakest.entry_price if weakest.entry_price > 0 else 0.0
                        rep_ev = weakest.event
                        rep_filing = rep_ev.get("filing_date", "")

                        # PIT score for replaced
                        rep_cache_key = (rep_ev["insider_id"], rep_ev["ticker"], rep_filing)
                        if rep_cache_key not in pit_cache:
                            pit_cache[rep_cache_key] = _get_pit_score(
                                conn, rep_ev["insider_id"], rep_ev["ticker"], rep_filing
                            )

                        completed_trades.append(PortfolioTrade(
                            strategy=strategy_cfg.name,
                            trade_id=weakest.trade_id,
                            ticker=weakest.ticker,
                            company=rep_ev.get("company"),
                            entry_date=weakest.entry_date,
                            entry_price=round(weakest.entry_price, 4),
                            exit_date=today,
                            exit_price=round(rep_close, 4),
                            hold_days=weakest.days_held,
                            target_hold=weakest.target_hold,
                            stop_pct=weakest.stop_pct,
                            stop_hit=0,
                            pnl_pct=round(rep_pnl_pct, 6),
                            pnl_dollar=round(rep_pnl_dollar, 2),
                            position_size=thesis_cfg_map[weakest.thesis].position_size,
                            portfolio_value=round(equity - rep_pnl_dollar, 2),
                            equity_after=round(equity, 2),
                            insider_name=rep_ev.get("display_name") or rep_ev.get("insider_name_raw") or "",
                            signal_quality=GRADE_QUALITY.get(rep_ev.get("signal_grade")) if rep_ev.get("signal_grade") else None,
                            exit_reason="replaced_by_higher_conviction",
                            entry_reasoning=json.dumps({"thesis": weakest.thesis, "conviction": weakest.conviction, "replaced": True}),
                            exit_reasoning=json.dumps({"replaced_by": ticker, "incoming_conviction": conviction}),
                            filing_date=rep_filing,
                            trade_date=rep_ev.get("trade_date"),
                            trade_value=rep_ev.get("value"),
                            signal_grade=rep_ev.get("signal_grade"),
                            is_csuite=rep_ev.get("is_csuite"),
                            is_rare_reversal=rep_ev.get("is_rare_reversal"),
                            is_cluster=1 if weakest.trade_id in cluster_reason_ids else 0,
                            insider_title=rep_ev.get("title"),
                            peak_return=round(rep_peak_ret, 4),
                            dollar_amount=round(weakest.dollar_amount, 2),
                            thesis=weakest.thesis,
                        ))
                        open_positions = [p for p in open_positions if p.trade_id != weakest.trade_id]
                        held_tickers = {p.ticker for p in open_positions}
                        replacements += 1
                    else:
                        skipped_capacity += 1
                        continue
                else:
                    # Composite: SKIP (no replacement)
                    skipped_capacity += 1
                    continue

            # --- Position sizing: min(equity * position_pct, 2% of ADV) ---
            target_amount = equity * tc.position_size
            adv = prices.get_avg_daily_dollar_volume(ticker, today)
            if adv and adv > 0:
                volume_cap = adv * 0.02
                dollar_amount = min(target_amount, volume_cap)
            else:
                dollar_amount = target_amount

            if dollar_amount <= 0:
                continue

            # --- Enter position ---
            pos = OpenPosition(
                trade_id=event["trade_id"],
                ticker=ticker,
                entry_date=today,
                entry_price=entry_price,
                dollar_amount=dollar_amount,
                thesis=thesis_name,
                target_hold=tc.target_hold,
                stop_pct=tc.stop_pct,
                trailing_stop=tc.trailing_stop,
                event=event,
                conviction=conviction,
                peak_price=entry_price,
                days_held=0,
            )
            open_positions.append(pos)
            entered_tickers_today.add(ticker)
            held_tickers.add(ticker)

        # Signals are only actionable on T+1 after filing — drop stale ones
        pending_entries = []

        # Track max concurrent for diagnostics
        if len(open_positions) > max_concurrent_seen:
            max_concurrent_seen = len(open_positions)

        # Progress
        if (day_idx + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"  [day {day_idx+1}/{len(trading_days)}] {today} equity=${equity:,.0f} "
                  f"open={len(open_positions)} trades={len(completed_trades)} "
                  f"max_concurrent={max_concurrent_seen} ({elapsed:.1f}s)")

    # --- Close any remaining open positions at simulation end ---
    for pos in open_positions:
        last_close = prices.get_close_on_or_before(pos.ticker, trading_days[-1])
        if last_close and last_close > 0:
            pnl_pct = (last_close - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar

            peak_return = (pos.peak_price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0.0
            ev = pos.event
            filing_date = ev.get("filing_date", "")
            tc = thesis_cfg_map[pos.thesis]

            cache_key = (ev["insider_id"], ev["ticker"], filing_date)
            if cache_key not in pit_cache:
                pit_cache[cache_key] = _get_pit_score(
                    conn, ev["insider_id"], ev["ticker"], filing_date
                )
            pit_score = pit_cache[cache_key]

            insider_name = ev.get("display_name") or ev.get("insider_name_raw") or ""
            signal_grade = ev.get("signal_grade")
            signal_quality = GRADE_QUALITY.get(signal_grade) if signal_grade else None
            is_cluster = 1 if ev["trade_id"] in cluster_reason_ids else 0

            entry_reasoning = json.dumps({
                "thesis": pos.thesis,
                "conviction": pos.conviction,
                "consecutive_sells_before": ev.get("consecutive_sells_before"),
                "is_rare_reversal": bool(ev.get("is_rare_reversal")),
                "signal_grade": signal_grade,
                "insider_name": insider_name,
                "insider_title": ev.get("title"),
                "trade_value": ev.get("value"),
                "dip_1mo": ev.get("dip_1mo"),
                "dip_3mo": ev.get("dip_3mo"),
                "above_sma50": ev.get("above_sma50"),
                "above_sma200": ev.get("above_sma200"),
                "is_largest_ever": ev.get("is_largest_ever"),
                "pit_score": round(pit_score, 4) if pit_score else None,
                "why": _build_why_text(pos.thesis, ev, insider_name),
            })

            exit_reasoning_json = json.dumps({
                "exit_rule": "sim_end",
                "target_hold": pos.target_hold,
                "actual_hold": pos.days_held,
                "peak_return_pct": round(peak_return * 100, 2),
                "exit_return_pct": round(pnl_pct * 100, 2),
                "stop_pct": round(pos.stop_pct * 100, 1),
                "stop_hit": False,
            })

            trade = PortfolioTrade(
                strategy=strategy_cfg.name,
                trade_id=pos.trade_id,
                ticker=pos.ticker,
                company=ev.get("company"),
                entry_date=pos.entry_date,
                entry_price=round(pos.entry_price, 4),
                exit_date=trading_days[-1],
                exit_price=round(last_close, 4),
                hold_days=pos.days_held,
                target_hold=pos.target_hold,
                stop_pct=pos.stop_pct,
                stop_hit=0,
                pnl_pct=round(pnl_pct, 6),
                pnl_dollar=round(pnl_dollar, 2),
                position_size=tc.position_size,
                portfolio_value=round(equity - pnl_dollar, 2),
                equity_after=round(equity, 2),
                insider_name=insider_name,
                signal_quality=signal_quality,
                exit_reason="sim_end",
                entry_reasoning=entry_reasoning,
                exit_reasoning=exit_reasoning_json,
                filing_date=filing_date,
                trade_date=ev.get("trade_date"),
                trade_value=ev.get("value"),
                signal_grade=signal_grade,
                is_csuite=ev.get("is_csuite"),
                is_rare_reversal=ev.get("is_rare_reversal"),
                is_cluster=is_cluster,
                insider_title=ev.get("title"),
                peak_return=round(peak_return, 4),
                dollar_amount=round(pos.dollar_amount, 2),
                thesis=pos.thesis,
            )
            completed_trades.append(trade)

    elapsed = time.time() - t0
    print(f"\n  Simulation complete in {elapsed:.1f}s")
    print(f"  Trades: {len(completed_trades)}")
    print(f"  Replacements: {replacements}")
    print(f"  Skipped (capacity): {skipped_capacity}")
    print(f"  Max concurrent positions: {max_concurrent_seen} (limit: {strategy_cfg.max_concurrent})")
    print(f"  Final equity: ${equity:,.2f}")

    if max_concurrent_seen > strategy_cfg.max_concurrent:
        print(f"  *** WARNING: Max concurrent ({max_concurrent_seen}) exceeded limit ({strategy_cfg.max_concurrent})! ***")

    return completed_trades


def _build_why_text(thesis: str, event: dict, insider_name: str) -> str:
    """Build human-readable why text for entry reasoning."""
    title = event.get("title") or "insider"
    price = event.get("entry_price") or event.get("price") or 0
    ticker = event.get("ticker", "???")

    if thesis == "reversal":
        n_sells = event.get("consecutive_sells_before") or 0
        return (f"First buy after {n_sells} consecutive sells. "
                f"{title} purchase of {ticker} at ${price:.2f}.")
    elif thesis == "dip_cluster":
        dip_1mo = event.get("dip_1mo")
        dip_3mo = event.get("dip_3mo")
        dip_str = ""
        if dip_1mo is not None:
            dip_str += f"1mo dip {dip_1mo:.1%}"
        if dip_3mo is not None:
            dip_str += f"{', ' if dip_str else ''}3mo dip {dip_3mo:.1%}"
        return (f"Cluster buy during dip ({dip_str}). "
                f"{title} buying {ticker} at ${price:.2f}.")
    elif thesis == "momentum_largest":
        return (f"Largest-ever purchase while above SMA50+SMA200. "
                f"{title} buying {ticker} at ${price:.2f}.")
    return f"{title} buying {ticker} at ${price:.2f}."


# ---------------------------------------------------------------------------
# Database write
# ---------------------------------------------------------------------------

def _ensure_portfolio(conn: sqlite3.Connection, strategy_name: str,
                      display_name: str, description: str,
                      config: dict, starting_capital: float) -> int:
    """Insert or get portfolio_id for a strategy."""
    conn.execute("""
        INSERT OR IGNORE INTO portfolios (name, display_name, description, config, starting_capital)
        VALUES (?, ?, ?, ?, ?)
    """, (strategy_name, display_name, description, json.dumps(config), starting_capital))
    conn.commit()
    row = conn.execute("SELECT id FROM portfolios WHERE name = ?", (strategy_name,)).fetchone()
    return row[0]


def write_trades(conn: sqlite3.Connection, trades: list[PortfolioTrade],
                 strategy_name: str, portfolio_id: int, dry_run: bool = False):
    """Write completed trades to strategy_portfolio table."""
    if dry_run:
        print(f"\n  [DRY RUN] Would write {len(trades)} trades for {strategy_name}")
        return

    # Clear existing backtest trades
    deleted = conn.execute(
        "DELETE FROM strategy_portfolio WHERE strategy = ? AND execution_source = 'backtest'",
        (strategy_name,)
    ).rowcount
    if deleted:
        print(f"  Cleared {deleted} existing backtest trades for {strategy_name}")

    # Insert
    insert_sql = """
        INSERT INTO strategy_portfolio (
            strategy, trade_id, ticker, company, trade_type, direction,
            entry_date, entry_price, exit_date, exit_price,
            hold_days, target_hold, stop_pct, stop_hit,
            pnl_pct, pnl_dollar, position_size,
            portfolio_value, equity_after,
            insider_name, signal_quality,
            exit_reason, status, execution_source, is_estimated,
            entry_reasoning, exit_reasoning,
            filing_date, trade_date, trade_value,
            signal_grade, is_csuite, is_rare_reversal, is_cluster,
            insider_title, portfolio_id, instrument,
            peak_return, dollar_amount
        ) VALUES (
            ?, ?, ?, ?, 'buy', 'long',
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, 'closed', 'backtest', 1,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, 'stock',
            ?, ?
        )
    """

    batch: list[tuple] = []
    for t in trades:
        batch.append((
            t.strategy, t.trade_id, t.ticker, t.company,
            t.entry_date, t.entry_price, t.exit_date, t.exit_price,
            t.hold_days, t.target_hold, t.stop_pct, t.stop_hit,
            t.pnl_pct, t.pnl_dollar, t.position_size,
            t.portfolio_value, t.equity_after,
            t.insider_name, t.signal_quality,
            t.exit_reason,
            t.entry_reasoning, t.exit_reasoning,
            t.filing_date, t.trade_date, t.trade_value,
            t.signal_grade, t.is_csuite, t.is_rare_reversal, t.is_cluster,
            t.insider_title, portfolio_id,
            t.peak_return, t.dollar_amount,
        ))

    conn.executemany(insert_sql, batch)
    conn.commit()
    print(f"  Wrote {len(batch)} trades for {strategy_name}")


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def print_summary(trades: list[PortfolioTrade], strategy_name: str, starting_capital: float):
    """Print strategy performance summary."""
    if not trades:
        print(f"\n  {strategy_name}: No trades to summarize")
        return

    returns = [t.pnl_pct for t in trades if t.pnl_pct is not None]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    total_pnl = sum(t.pnl_dollar for t in trades if t.pnl_dollar is not None)
    final_equity = trades[-1].equity_after if trades else starting_capital

    win_rate = len(wins) / len(returns) * 100 if returns else 0
    avg_return = sum(returns) / len(returns) * 100 if returns else 0
    avg_win = sum(wins) / len(wins) * 100 if wins else 0
    avg_loss = sum(losses) / len(losses) * 100 if losses else 0

    # Profit factor
    gross_profit = sum(r for r in returns if r > 0)
    gross_loss = abs(sum(r for r in returns if r < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Sharpe (annualized, assuming ~252 trading days)
    if len(returns) >= 2:
        import numpy as np
        ret_arr = np.array(returns)
        mean_ret = np.mean(ret_arr)
        std_ret = np.std(ret_arr, ddof=1)
        # Estimate trades per year
        try:
            first_date = datetime.strptime(trades[0].entry_date, "%Y-%m-%d")
            last_date = datetime.strptime(trades[-1].entry_date, "%Y-%m-%d")
            years = max((last_date - first_date).days / 365.25, 0.5)
            trades_per_year = len(returns) / years
        except (ValueError, TypeError):
            trades_per_year = 52  # fallback
        sharpe = (mean_ret / std_ret) * math.sqrt(trades_per_year) if std_ret > 0 else 0
    else:
        sharpe = 0

    # Max drawdown from equity curve
    equities = [starting_capital]
    for t in trades:
        if t.equity_after is not None:
            equities.append(t.equity_after)
    peak_eq = equities[0]
    max_dd = 0
    for eq in equities:
        peak_eq = max(peak_eq, eq)
        dd = (eq - peak_eq) / peak_eq
        max_dd = min(max_dd, dd)

    # Thesis breakdown
    thesis_counts: dict[str, int] = {}
    thesis_wins: dict[str, int] = {}
    for t in trades:
        thesis_counts[t.thesis] = thesis_counts.get(t.thesis, 0) + 1
        if t.pnl_pct is not None and t.pnl_pct > 0:
            thesis_wins[t.thesis] = thesis_wins.get(t.thesis, 0) + 1

    stop_hits = sum(1 for t in trades if t.stop_hit)

    print(f"\n{'='*70}")
    print(f"  {strategy_name} Summary")
    print(f"{'='*70}")
    print(f"  Total trades:      {len(trades)}")
    print(f"  Win rate:          {win_rate:.1f}%")
    print(f"  Avg return:        {avg_return:.2f}%")
    print(f"  Avg win:           {avg_win:.2f}%")
    print(f"  Avg loss:          {avg_loss:.2f}%")
    print(f"  Profit factor:     {profit_factor:.2f}")
    print(f"  Sharpe ratio:      {sharpe:.2f}")
    print(f"  Max drawdown:      {max_dd:.1%}")
    print(f"  Total P&L:         ${total_pnl:,.2f}")
    print(f"  Final equity:      ${final_equity:,.2f}")
    print(f"  Stop hits:         {stop_hits} ({stop_hits/len(trades)*100:.1f}%)")
    print()
    print(f"  Thesis breakdown:")
    for thesis_name in sorted(thesis_counts):
        cnt = thesis_counts[thesis_name]
        w = thesis_wins.get(thesis_name, 0)
        wr = w / cnt * 100 if cnt else 0
        print(f"    {thesis_name:<20} {cnt:>5} trades, {wr:.1f}% WR")
    print(f"{'='*70}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Backfill CW strategy portfolios with historical trades"
    )
    parser.add_argument("--strategy", choices=["cw_reversal", "cw_composite"],
                        help="Run only this strategy (default: both)")
    parser.add_argument("--start", default="2020-01-01",
                        help="Start date (default: 2020-01-01)")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"),
                        help="End date (default: today)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate only, don't write to DB")
    args = parser.parse_args()

    print(f"Database: {DB_PATH}")
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    if not PRICES_DB.exists():
        print(f"ERROR: Prices database not found at {PRICES_DB}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")

    strategies_to_run = [args.strategy] if args.strategy else ["cw_reversal", "cw_composite"]

    for strat_name in strategies_to_run:
        cfg = STRATEGIES[strat_name]

        # Ensure portfolio exists
        portfolio_id = _ensure_portfolio(
            conn, strat_name,
            display_name=strat_name.replace("_", " ").title(),
            description=f"CW {strat_name} backfill — historical replay using actual daily prices from prices.db",
            config={
                "theses": [tc.name for tc in cfg.theses],
                "max_concurrent": cfg.max_concurrent,
                "starting_capital": cfg.starting_capital,
            },
            starting_capital=cfg.starting_capital,
        )

        # Run simulation
        trades = run_strategy(conn, cfg, args.start, args.end, dry_run=args.dry_run)

        # Write to DB
        write_trades(conn, trades, strat_name, portfolio_id, dry_run=args.dry_run)

        # Print summary
        print_summary(trades, strat_name, cfg.starting_capital)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
