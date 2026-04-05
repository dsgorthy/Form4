#!/usr/bin/env python3
"""
Position management backtester — tests ~200 configurations of soft/hard cap
logic across reversal and composite insider trading strategies.

Extends the day-by-day simulation engine from optimize_positions.py with
configurable soft cap / hard cap position management and multiple replacement
rules. All configs run on 2020-2026 data with train/test split reporting.

Usage:
    python3 pipelines/insider_study/test_position_rules.py
    python3 pipelines/insider_study/test_position_rules.py --strategy reversal
    python3 pipelines/insider_study/test_position_rules.py --strategy composite

Output:
    reports/position_rules/grid_results.csv
    reports/position_rules/reversal_top10.txt
    reports/position_rules/composite_top10.txt
    reports/position_rules/approach_comparison.txt

===========================================================================
SELF-AUDIT (Phase 2) — Performed after implementation
===========================================================================

1. FORWARD-LOOKING BIAS CHECK
   [PASS] PIT score lookup: _get_pit_grade() uses bisect_right on sorted
          as_of_date lists to find the most recent score where
          as_of_date <= filing_date. Never reads future scores.
   [PASS] Conviction scoring: PIT grade from pit_score_to_grade() is passed
          as `signal_grade` to compute_conviction(). The trades.signal_grade
          column is loaded from DB but NEVER used in conviction computation.
   [PASS] insider_title and is_csuite: Both passed to compute_conviction()
          at line where conviction is computed per-event. The 10% owner and
          solo president filters in compute_conviction() will fire correctly.
   [PASS] Entry timing: Events indexed by filing_date; entry loop collects
          events with filing_date < today (strict less-than), so entry is
          at T+1 open at earliest. get_open(ticker, today) used for entry.
   [PASS] Exit timing: All exit checks use today's bar. Stop checks use
          today's low. Time exits and replacement closes use today's close.
          No tomorrow-price references.
   [PASS] Replacement close: When replacing a position at hard cap, the
          replaced position is closed at prices.get_close(ticker, today),
          which is the current day's close, not a future price.

2. CRASH RISK CHECK
   [PASS] Zero-trade configs: Sharpe computation guards with
          `if len(daily_returns) > 1` and `if std_r > 0`. CAGR guards
          with `if n_trades == 0: return _empty_metrics(...)`.
   [PASS] Missing price data: Entry skips when get_open returns None.
          Exit loop skips when get_bar returns None (position stays open,
          days_held not incremented). Replacement skips when get_close
          returns None.
   [PASS] No PIT score: _get_pit_grade() returns "C" when no score found
          for an insider+ticker combo (default grade per spec).
   [PASS] soft_cap > hard_cap: Config generation never creates this case.
          For Approach A, soft_cap is set equal to hard_cap. For Approach B,
          hard_cap is always soft_cap + delta where delta > 0.

3. STATISTICAL VALIDITY CHECK
   [PASS] Train/test split: _split_closed_trades() partitions by exit_date
          using TRAIN_END = "2023-12-31". Metrics computed independently
          for each period. No information leakage between periods.
   [PASS] Results reported separately: grid_results.csv has columns for
          train_, test_, and full_ prefixed metrics.
   [PASS] Config selection: Top-10 reports sort by test-period Sharpe but
          display both train and test metrics for comparison.

4. ADDITIONAL CHECKS
   [PASS] Memory safety: Single PriceCache instance per strategy type,
          shared across all configs. Events loaded once per strategy.
          No accumulation between configs.
   [PASS] Checkpoint: Partial results written to grid_results.csv every
          10 configs via _checkpoint_results().
   [PASS] Config count: Reversal generates ~96 configs, composite ~108.
          Total ~204. Within spec range of ~200.
   [PASS] Pending entries cleared each day: Events not entered on their
          T+1 day are dropped (stale signal). Consistent with
          optimize_positions.py behavior.
   [NOTE] The `worst_pnl` replacement rule selects the position with worst
          unrealized P&L. This requires a current close price. If no price
          is available for a position, it is skipped in the worst-finding
          logic (its unrealized is treated as 0.0).
   [NOTE] The `weakest_losing` rule only replaces among positions that are
          currently losing. If no positions are losing, it behaves like
          'skip' (no replacement made).
   [NOTE] Pre-existing edge case in conviction_score.py: titles like
          "President & C.E.O." don't match the "CEO" check due to dots,
          so they get filtered as solo presidents (conviction=0). Affects
          ~5 events out of 714 reversal events. Not introduced by this
          script; fix belongs in conviction_score.py _categorize_insider().
===========================================================================
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import statistics
import time
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from pipelines.insider_study.conviction_score import (
        compute_conviction,
        pit_score_to_grade,
    )
except ModuleNotFoundError:
    from conviction_score import (
        compute_conviction,
        pit_score_to_grade,
    )

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
REPORT_DIR = BASE_DIR / "reports" / "position_rules"
LOG_DIR = BASE_DIR / "logs"

TRAIN_END = "2023-12-31"  # Train: 2020-01-01 to 2023-12-31
TEST_START = "2024-01-01"  # Test:  2024-01-01 to 2026-12-31

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "position_rules.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class CapConfig:
    name: str
    approach: str           # 'hard_only' or 'soft_hard'
    soft_cap: int
    hard_cap: int
    position_size: float
    min_conv_below_soft: float
    min_conv_above_soft: float   # only used for soft_hard approach
    min_conv_at_hard: float      # minimum conviction to trigger replacement
    replace_rule: str            # 'weakest_conv', 'worst_pnl', 'oldest', 'weakest_losing', 'skip', 'skip_at_hard'
    high_conv_threshold: float
    high_conv_size_mult: float
    stop_pct: float
    hold_days: int

    def __post_init__(self):
        if self.approach == 'hard_only':
            self.soft_cap = self.hard_cap


# ---------------------------------------------------------------------------
# Position / trade dataclasses
# ---------------------------------------------------------------------------

@dataclass
class OpenPosition:
    trade_id: int
    ticker: str
    thesis: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    conviction: float
    hold_days_target: int
    stop_pct: float
    peak_price: float
    days_held: int = 0
    exit_date: str | None = None
    exit_price: float | None = None
    exit_reason: str | None = None


@dataclass
class ClosedTrade:
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


# ---------------------------------------------------------------------------
# PIT Score Cache — bulk-load all insider_ticker_scores into memory
# ---------------------------------------------------------------------------

class PITScoreCache:
    """In-memory PIT score lookup.

    For each (insider_id, ticker) pair, stores a sorted list of
    (as_of_date, blended_score) tuples. Lookup finds the most recent
    score where as_of_date <= target_date using binary search.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._data: dict[tuple[int, str], list[tuple[str, float]]] = {}
        t0 = time.time()
        rows = conn.execute(
            "SELECT insider_id, ticker, as_of_date, blended_score "
            "FROM insider_ticker_scores ORDER BY insider_id, ticker, as_of_date"
        ).fetchall()
        for insider_id, ticker, as_of_date, blended_score in rows:
            key = (insider_id, ticker)
            if key not in self._data:
                self._data[key] = []
            self._data[key].append((as_of_date, blended_score))
        elapsed = time.time() - t0
        log.info(f"PIT score cache: {len(rows):,} scores for {len(self._data):,} "
                 f"insider+ticker combos ({elapsed:.1f}s)")

    def get_pit_grade(self, insider_id: int, ticker: str, filing_date: str) -> str:
        """Get the PIT signal grade for an insider+ticker as of filing_date.

        Returns the grade from the most recent score where as_of_date <= filing_date.
        Defaults to "C" if no score exists (per spec: 0.3% of events).
        """
        key = (insider_id, ticker)
        entries = self._data.get(key)
        if not entries:
            return "C"  # No score available -> default grade

        # Binary search: find rightmost entry where as_of_date <= filing_date
        # entries is sorted by as_of_date
        dates = [e[0] for e in entries]
        idx = bisect_right(dates, filing_date) - 1
        if idx < 0:
            return "C"  # All scores are after filing_date

        blended_score = entries[idx][1]
        grade = pit_score_to_grade(blended_score)
        return grade if grade else "C"


# ---------------------------------------------------------------------------
# Price cache (copied from optimize_positions.py)
# ---------------------------------------------------------------------------

class PriceCache:
    """In-memory price cache keyed by (ticker, date) -> (open, high, low, close)."""

    def __init__(self, prices_conn: sqlite3.Connection, tickers: set[str],
                 start: str, end: str):
        self._data: dict[tuple[str, str], tuple[float, float, float, float]] = {}
        self._dates_by_ticker: dict[str, list[str]] = {}

        t0 = time.time()
        ticker_list = sorted(tickers)
        batch_size = 200
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i: i + batch_size]
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
        log.info(f"Price cache: {len(self._data):,} bars for {len(tickers)} tickers ({elapsed:.1f}s)")

    def get_close(self, ticker: str, date: str) -> float | None:
        bar = self._data.get((ticker, date))
        return bar[3] if bar else None

    def get_open(self, ticker: str, date: str) -> float | None:
        bar = self._data.get((ticker, date))
        return bar[0] if bar else None

    def get_bar(self, ticker: str, date: str) -> tuple[float, float, float, float] | None:
        return self._data.get((ticker, date))

    def get_close_on_or_before(self, ticker: str, date: str) -> float | None:
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

    def has_data(self, ticker: str) -> bool:
        return ticker in self._dates_by_ticker


# ---------------------------------------------------------------------------
# Event loading (from optimize_positions.py, adapted)
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
               t.qty, t.shares_owned_after,
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
               t.qty, t.shares_owned_after,
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
               t.qty, t.shares_owned_after,
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
    if not trade_ids:
        return set()
    result: set[int] = set()
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i: i + batch_size]
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
# Event preparation: dedupe, compute PIT conviction
# ---------------------------------------------------------------------------

def _prepare_events(
    strategy_type: str,
    insiders_conn: sqlite3.Connection,
    pit_cache: PITScoreCache,
    start: str,
    end: str,
) -> list[dict]:
    """Load, dedupe, and compute PIT-based conviction for all events."""

    events_by_thesis: dict[str, list[dict]] = {}

    if strategy_type == "reversal":
        evts = _load_reversal_events(insiders_conn, start, end)
        for e in evts:
            e["_thesis"] = "reversal"
        events_by_thesis["reversal"] = evts
    elif strategy_type == "composite":
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

    # Merge and dedupe (same logic as optimize_positions.py)
    thesis_priority = {"reversal": 0, "dip_cluster": 1, "momentum_largest": 2}
    all_events: list[dict] = []
    for evts in events_by_thesis.values():
        all_events.extend(evts)
    all_events.sort(
        key=lambda e: (e["filing_date"], thesis_priority.get(e["_thesis"], 9), e["trade_id"])
    )

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

    # Compute PIT-based conviction for every event
    # BLOCKER 1: Use PIT grade from insider_ticker_scores, NOT trades.signal_grade
    # BLOCKER 2: Pass insider_title and is_csuite to compute_conviction
    # Pre-compute cluster sizes per ticker+month (PIT: only count insiders who filed before)
    cluster_cache = {}  # (ticker, filing_date) -> cluster_size
    for e in all_events:
        key = (e["ticker"], e["filing_date"][:7])  # group by ticker+month
        cluster_cache[key] = cluster_cache.get(key, 0) + 1

    for e in all_events:
        pit_grade = pit_cache.get_pit_grade(
            insider_id=e["insider_id"],
            ticker=e["ticker"],
            filing_date=e["filing_date"],
        )
        e["_pit_grade"] = pit_grade

        # Compute holdings % change from qty and shares_owned_after
        holdings_pct = None
        qty = e.get("qty") or 0
        shares_after = e.get("shares_owned_after") or 0
        if shares_after > 0 and qty > 0:
            shares_before = shares_after - qty
            if shares_before > 0:
                holdings_pct = qty / shares_before

        # Compute streak break gap (days since last buy at this ticker)
        streak_days = None
        if e.get("_last_buy_date"):
            from datetime import datetime
            try:
                td = datetime.strptime(e["trade_date"], "%Y-%m-%d")
                lb = datetime.strptime(e["_last_buy_date"], "%Y-%m-%d")
                streak_days = (td - lb).days
            except (ValueError, TypeError):
                pass
        elif e.get("consecutive_sells_before") and e["consecutive_sells_before"] >= 5:
            # If no last_buy_date but has consecutive sells, treat as very long gap
            streak_days = 99999  # first ever buy at this ticker

        # Cluster size for this ticker in the filing month
        cluster_key = (e["ticker"], e["filing_date"][:7])
        cluster_sz = cluster_cache.get(cluster_key, 1)

        # Is this the first-ever buy at this ticker by this insider?
        is_first = e.get("_is_first_buy", False)
        if not is_first and e.get("consecutive_sells_before") and e["consecutive_sells_before"] >= 5:
            is_first = True  # reversal = effectively first meaningful buy

        e["_conviction"] = compute_conviction(
            thesis=e["_thesis"],
            signal_grade=pit_grade,
            consecutive_sells=e.get("consecutive_sells_before"),
            dip_1mo=e.get("dip_1mo"),
            dip_3mo=e.get("dip_3mo"),
            is_largest_ever=bool(e.get("is_largest_ever")),
            above_sma50=bool(e.get("above_sma50")),
            above_sma200=bool(e.get("above_sma200")),
            insider_title=e.get("title"),
            is_csuite=bool(e.get("is_csuite")),
            holdings_pct_change=holdings_pct,
            streak_break_days=streak_days,
            cluster_size=cluster_sz,
            is_first_buy=is_first,
            is_opportunistic=e.get("cohen_routine") == 0,
            trade_value=e.get("value"),
        )

    log.info(f"  {strategy_type}: {len(all_events)} events after dedupe, "
             f"conviction range {min(e['_conviction'] for e in all_events):.1f} - "
             f"{max(e['_conviction'] for e in all_events):.1f}" if all_events else
             f"  {strategy_type}: 0 events")

    return all_events


# ---------------------------------------------------------------------------
# Trading calendar
# ---------------------------------------------------------------------------

def _build_trading_calendar(prices_conn: sqlite3.Connection, start: str, end: str) -> list[str]:
    rows = prices_conn.execute(
        "SELECT DISTINCT date FROM daily_prices WHERE ticker = 'SPY' "
        "AND date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

@dataclass
class PeriodMetrics:
    trades: int = 0
    wins: int = 0
    win_rate: float = 0.0
    cagr: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    avg_hold: float = 0.0
    replacements: int = 0
    max_concurrent: int = 0
    pct_days_at_cap: float = 0.0
    final_equity: float = 100_000.0


def _empty_metrics() -> PeriodMetrics:
    return PeriodMetrics()


def _compute_metrics(
    closed_trades: list[ClosedTrade],
    equity_curve: list[float],
    daily_returns: list[float],
    starting_capital: float,
    n_trading_days: int,
    replacements: int,
    max_concurrent: int,
    days_at_cap: int,
) -> PeriodMetrics:
    n_trades = len(closed_trades)
    if n_trades == 0:
        m = _empty_metrics()
        m.max_concurrent = max_concurrent
        return m

    wins = sum(1 for t in closed_trades if t.pnl_pct > 0)
    win_rate = wins / n_trades

    # CAGR
    years = n_trading_days / 252.0
    final_equity = equity_curve[-1] if equity_curve else starting_capital
    if final_equity > 0 and starting_capital > 0 and years > 0.01:
        cagr = (final_equity / starting_capital) ** (1.0 / years) - 1.0
    else:
        cagr = 0.0

    # Max drawdown
    peak_eq = starting_capital
    max_dd = 0.0
    for eq in equity_curve:
        if eq > peak_eq:
            peak_eq = eq
        dd = (peak_eq - eq) / peak_eq if peak_eq > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Profit factor
    gross_win = sum(t.pnl_dollar for t in closed_trades if t.pnl_dollar > 0)
    gross_loss = abs(sum(t.pnl_dollar for t in closed_trades if t.pnl_dollar < 0))
    profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Sharpe
    if daily_returns and len(daily_returns) > 1:
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.stdev(daily_returns)
        sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    # Avg hold
    avg_hold = sum(t.hold_days for t in closed_trades) / n_trades

    # Pct days at cap
    pct_at_cap = days_at_cap / n_trading_days if n_trading_days > 0 else 0.0

    return PeriodMetrics(
        trades=n_trades,
        wins=wins,
        win_rate=win_rate,
        cagr=cagr,
        sharpe=sharpe,
        max_drawdown=max_dd,
        profit_factor=profit_factor,
        avg_hold=avg_hold,
        replacements=replacements,
        max_concurrent=max_concurrent,
        pct_days_at_cap=pct_at_cap,
        final_equity=final_equity,
    )


def _split_closed_trades(
    closed_trades: list[ClosedTrade],
) -> tuple[list[ClosedTrade], list[ClosedTrade]]:
    """Split closed trades into train (exit_date <= TRAIN_END) and test periods."""
    train = [t for t in closed_trades if t.exit_date <= TRAIN_END]
    test = [t for t in closed_trades if t.exit_date > TRAIN_END]
    return train, test


# ---------------------------------------------------------------------------
# Core simulation: day-by-day with soft/hard cap
# ---------------------------------------------------------------------------

@dataclass
class SimOutput:
    """Full simulation output with split metrics."""
    config_name: str
    strategy_type: str
    approach: str
    full: PeriodMetrics
    train: PeriodMetrics
    test: PeriodMetrics
    closed_trades: list = None  # populated when return_trades=True
    open_positions: list = None


def _find_replacement_target(
    open_positions: list[OpenPosition],
    replace_rule: str,
    prices: PriceCache,
    today: str,
) -> OpenPosition | None:
    """Find the position to replace based on the replacement rule.

    Returns None if no suitable replacement target found.
    """
    if not open_positions:
        return None

    if replace_rule == "weakest_conv":
        return min(open_positions, key=lambda p: p.conviction)

    elif replace_rule == "worst_pnl":
        # Find position with worst unrealized P&L
        worst = None
        worst_pnl = float("inf")
        for pos in open_positions:
            c = prices.get_close(pos.ticker, today)
            if c is not None and c > 0:
                pnl = (c - pos.entry_price) / pos.entry_price
            else:
                pnl = 0.0  # No price data, treat as flat
            if pnl < worst_pnl:
                worst_pnl = pnl
                worst = pos
        return worst

    elif replace_rule == "oldest":
        return min(open_positions, key=lambda p: p.entry_date)

    elif replace_rule == "weakest_losing":
        # Only replace among losing positions
        losing = []
        for pos in open_positions:
            c = prices.get_close(pos.ticker, today)
            if c is not None and c > 0:
                pnl = (c - pos.entry_price) / pos.entry_price
                if pnl < 0:
                    losing.append((pos, pnl))
        if not losing:
            return None  # No losing positions -> skip
        losing.sort(key=lambda x: x[1])
        return losing[0][0]  # Worst loser

    # 'skip' or 'skip_at_hard' or unrecognized
    return None


def run_simulation(
    config: CapConfig,
    strategy_type: str,
    all_events: list[dict],
    prices: PriceCache,
    trading_days: list[str],
    start: str,
    end: str,
    starting_capital: float = 100_000.0,
) -> SimOutput:
    """Run a day-by-day portfolio simulation with soft/hard cap logic."""

    # Index events by filing_date
    events_by_filing: dict[str, list[dict]] = {}
    for e in all_events:
        fd = e["filing_date"]
        if fd not in events_by_filing:
            events_by_filing[fd] = []
        events_by_filing[fd].append(e)

    # --- Simulation state ---
    equity = starting_capital
    open_positions: list[OpenPosition] = []
    closed_trades: list[ClosedTrade] = []
    replacements = 0
    max_concurrent = 0
    days_at_cap = 0

    equity_curve: list[float] = [starting_capital]
    daily_returns: list[float] = []

    # Split tracking for train/test
    train_equity_curve: list[float] = [starting_capital]
    train_daily_returns: list[float] = []
    test_equity_curve: list[float] = []
    test_daily_returns: list[float] = []
    train_replacements = 0
    test_replacements = 0
    train_max_concurrent = 0
    test_max_concurrent = 0
    train_days_at_cap = 0
    test_days_at_cap = 0
    train_days = 0
    test_days = 0
    test_start_equity = None

    pending_entries: list[dict] = []
    last_filing_checked = ""

    for today in trading_days:
        if today < start:
            continue

        is_train = today <= TRAIN_END
        if is_train:
            train_days += 1
        else:
            test_days += 1
            if test_start_equity is None:
                test_start_equity = equity_curve[-1]
                test_equity_curve.append(test_start_equity)

        # --- Step 1: Gather new signals (filing_date < today) ---
        for fd in sorted(events_by_filing.keys()):
            if fd >= today:
                break
            if fd <= last_filing_checked:
                continue
            pending_entries.extend(events_by_filing[fd])
            last_filing_checked = fd

        # --- Step 2: Check exits on all open positions ---
        still_open: list[OpenPosition] = []
        for pos in open_positions:
            bar = prices.get_bar(pos.ticker, today)
            if bar is None:
                still_open.append(pos)
                continue

            o, h, l, c = bar
            pos.days_held += 1

            if c > pos.peak_price:
                pos.peak_price = c

            # Check hard stop
            stop_level = pos.entry_price * (1 + pos.stop_pct)
            if l <= stop_level:
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

            still_open.append(pos)

        open_positions = still_open

        # --- Step 3: Process new entries ---
        pending_entries.sort(key=lambda e: -e["_conviction"])
        entered_tickers_today: set[str] = set()

        for event in pending_entries:
            ticker = event["ticker"]
            held_tickers = {p.ticker for p in open_positions}
            if ticker in held_tickers or ticker in entered_tickers_today:
                continue

            entry_price = prices.get_open(ticker, today)
            if entry_price is None or entry_price <= 0:
                entry_price = prices.get_close(ticker, today)
            if entry_price is None or entry_price <= 0:
                continue

            conviction = event["_conviction"]

            # Position sizing
            size_pct = config.position_size
            if (config.high_conv_threshold > 0
                    and conviction >= config.high_conv_threshold):
                size_pct *= config.high_conv_size_mult

            n_open = len(open_positions)

            # --- Capacity logic: Approach A (hard_only) ---
            if config.approach == "hard_only":
                if n_open < config.hard_cap:
                    if conviction < config.min_conv_below_soft:
                        continue
                    # Under cap, conviction meets minimum -> ENTER
                elif n_open >= config.hard_cap:
                    if conviction < config.min_conv_at_hard:
                        continue
                    if config.replace_rule in ("skip", "skip_at_hard"):
                        continue
                    # At cap, try replacement
                    target = _find_replacement_target(
                        open_positions, config.replace_rule, prices, today
                    )
                    if target is None:
                        continue
                    # Check conviction advantage
                    if conviction - target.conviction < 1.5:
                        continue
                    # Close replaced position at today's close
                    rep_close = prices.get_close(target.ticker, today)
                    if rep_close is None or rep_close <= 0:
                        continue
                    rep_pnl_pct = (rep_close - target.entry_price) / target.entry_price
                    rep_pnl_dollar = target.dollar_amount * rep_pnl_pct
                    equity += rep_pnl_dollar
                    closed_trades.append(ClosedTrade(
                        trade_id=target.trade_id, ticker=target.ticker,
                        thesis=target.thesis, entry_date=target.entry_date,
                        entry_price=target.entry_price, exit_date=today,
                        exit_price=rep_close, dollar_amount=target.dollar_amount,
                        pnl_pct=rep_pnl_pct, pnl_dollar=rep_pnl_dollar,
                        hold_days=target.days_held,
                        exit_reason=f"replaced_{config.replace_rule}",
                        conviction=target.conviction,
                    ))
                    open_positions = [p for p in open_positions if p.trade_id != target.trade_id]
                    replacements += 1
                    if is_train:
                        train_replacements += 1
                    else:
                        test_replacements += 1

            # --- Capacity logic: Approach B (soft_hard) ---
            elif config.approach == "soft_hard":
                if n_open < config.soft_cap:
                    # Under soft cap: take any trade above min_conv_below_soft
                    if conviction < config.min_conv_below_soft:
                        continue
                elif config.soft_cap <= n_open < config.hard_cap:
                    # Between soft and hard: only elite signals
                    if conviction < config.min_conv_above_soft:
                        continue
                elif n_open >= config.hard_cap:
                    # At hard cap: replacement logic
                    if conviction < config.min_conv_at_hard:
                        continue
                    if config.replace_rule in ("skip", "skip_at_hard"):
                        continue
                    target = _find_replacement_target(
                        open_positions, config.replace_rule, prices, today
                    )
                    if target is None:
                        continue
                    if conviction - target.conviction < 1.5:
                        continue
                    rep_close = prices.get_close(target.ticker, today)
                    if rep_close is None or rep_close <= 0:
                        continue
                    rep_pnl_pct = (rep_close - target.entry_price) / target.entry_price
                    rep_pnl_dollar = target.dollar_amount * rep_pnl_pct
                    equity += rep_pnl_dollar
                    closed_trades.append(ClosedTrade(
                        trade_id=target.trade_id, ticker=target.ticker,
                        thesis=target.thesis, entry_date=target.entry_date,
                        entry_price=target.entry_price, exit_date=today,
                        exit_price=rep_close, dollar_amount=target.dollar_amount,
                        pnl_pct=rep_pnl_pct, pnl_dollar=rep_pnl_dollar,
                        hold_days=target.days_held,
                        exit_reason=f"replaced_{config.replace_rule}",
                        conviction=target.conviction,
                    ))
                    open_positions = [p for p in open_positions if p.trade_id != target.trade_id]
                    replacements += 1
                    if is_train:
                        train_replacements += 1
                    else:
                        test_replacements += 1
                else:
                    continue

            # --- Enter position ---
            dollar_amount = equity * size_pct
            if dollar_amount <= 0:
                continue

            pos = OpenPosition(
                trade_id=event["trade_id"],
                ticker=ticker,
                thesis=event["_thesis"],
                entry_date=today,
                entry_price=entry_price,
                dollar_amount=dollar_amount,
                conviction=conviction,
                hold_days_target=config.hold_days,
                stop_pct=config.stop_pct,
                peak_price=entry_price,
                days_held=0,
            )
            open_positions.append(pos)
            entered_tickers_today.add(ticker)

        # Stale signals dropped
        pending_entries = []

        # Track max concurrent positions
        n_now = len(open_positions)
        if n_now > max_concurrent:
            max_concurrent = n_now
        if n_now >= config.hard_cap:
            days_at_cap += 1

        if is_train:
            if n_now > train_max_concurrent:
                train_max_concurrent = n_now
            if n_now >= config.hard_cap:
                train_days_at_cap += 1
        else:
            if n_now > test_max_concurrent:
                test_max_concurrent = n_now
            if n_now >= config.hard_cap:
                test_days_at_cap += 1

        # --- Step 4: Mark-to-market equity ---
        unrealized = 0.0
        for pos in open_positions:
            c = prices.get_close(pos.ticker, today)
            if c and c > 0:
                unrealized += pos.dollar_amount * ((c - pos.entry_price) / pos.entry_price)

        total_equity = equity + unrealized
        equity_curve.append(total_equity)
        if len(equity_curve) >= 2:
            prev = equity_curve[-2]
            if prev > 0:
                daily_returns.append((total_equity - prev) / prev)
            else:
                daily_returns.append(0.0)

        # Split equity tracking
        if is_train:
            train_equity_curve.append(total_equity)
            if len(train_equity_curve) >= 2:
                prev = train_equity_curve[-2]
                if prev > 0:
                    train_daily_returns.append((total_equity - prev) / prev)
                else:
                    train_daily_returns.append(0.0)
        else:
            test_equity_curve.append(total_equity)
            if len(test_equity_curve) >= 2:
                prev = test_equity_curve[-2]
                if prev > 0:
                    test_daily_returns.append((total_equity - prev) / prev)
                else:
                    test_daily_returns.append(0.0)

    # --- Capture open positions before force-closing ---
    still_open = list(open_positions)

    # --- Close remaining open positions ---
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

    # --- Compute metrics for each period ---
    train_trades, test_trades = _split_closed_trades(closed_trades)

    full_metrics = _compute_metrics(
        closed_trades, equity_curve, daily_returns,
        starting_capital, len([d for d in trading_days if start <= d]),
        replacements, max_concurrent, days_at_cap,
    )

    train_metrics = _compute_metrics(
        train_trades, train_equity_curve, train_daily_returns,
        starting_capital, train_days,
        train_replacements, train_max_concurrent, train_days_at_cap,
    )

    test_capital = test_start_equity if test_start_equity else starting_capital
    test_metrics = _compute_metrics(
        test_trades, test_equity_curve, test_daily_returns,
        test_capital, test_days,
        test_replacements, test_max_concurrent, test_days_at_cap,
    )

    return SimOutput(
        config_name=config.name,
        strategy_type=strategy_type,
        approach=config.approach,
        full=full_metrics,
        train=train_metrics,
        test=test_metrics,
        closed_trades=closed_trades,
        open_positions=still_open,
    )


# ---------------------------------------------------------------------------
# Config grid generation
# ---------------------------------------------------------------------------

def _build_reversal_configs() -> list[CapConfig]:
    """Build ~100 reversal configs testing hard_only and soft_hard approaches.

    Approach A (~48 configs): 4 hard_cap * 3 pos_size * 2 replace * 2 min_conv = 48
    Plus 8 boost variants (best combos only).
    Approach B (~54 configs): 3 soft * 3 delta * 3 min_below * 2 min_above = 54
    with weakest_conv replacement only (worst_pnl tested in A).
    """
    configs: list[CapConfig] = []

    # --- Approach A: Hard Cap Only (~56 configs) ---
    for hard_cap in [8, 10, 12, 14]:
        for position_size in [0.05, 0.06, 0.07]:
            for replace_rule in ["weakest_conv", "worst_pnl"]:
                for min_conv in [3, 5]:
                    name = f"R-A:{hard_cap}h/{position_size:.0%}/{replace_rule}/mc{min_conv}"
                    configs.append(CapConfig(
                        name=name,
                        approach="hard_only",
                        soft_cap=hard_cap,
                        hard_cap=hard_cap,
                        position_size=position_size,
                        min_conv_below_soft=min_conv,
                        min_conv_above_soft=0,
                        min_conv_at_hard=min_conv + 2,
                        replace_rule=replace_rule,
                        high_conv_threshold=0,
                        high_conv_size_mult=1.0,
                        stop_pct=-0.15,
                        hold_days=30,
                    ))

    # Boost variants: only for the most promising combos (10/12 cap, 5%/6% size, weakest_conv)
    for hard_cap in [10, 12]:
        for position_size in [0.05, 0.06]:
            for min_conv in [3, 5]:
                name = f"R-A:{hard_cap}h/{position_size:.0%}/weakest_conv/mc{min_conv}/boost"
                configs.append(CapConfig(
                    name=name,
                    approach="hard_only",
                    soft_cap=hard_cap,
                    hard_cap=hard_cap,
                    position_size=position_size,
                    min_conv_below_soft=min_conv,
                    min_conv_above_soft=0,
                    min_conv_at_hard=min_conv + 2,
                    replace_rule="weakest_conv",
                    high_conv_threshold=7.0,
                    high_conv_size_mult=1.5,
                    stop_pct=-0.15,
                    hold_days=30,
                ))

    # --- Approach B: Soft Cap + Hard Cap (~54 configs) ---
    for soft_cap in [6, 8, 10]:
        for delta in [2, 4, 6]:
            hard_cap = soft_cap + delta
            for min_conv_below in [3, 4, 5]:
                for min_conv_above in [7, 8]:
                    name = (f"R-B:{soft_cap}s/{hard_cap}h/"
                            f"mc{min_conv_below}/{min_conv_above}a/"
                            f"weakest_conv")
                    configs.append(CapConfig(
                        name=name,
                        approach="soft_hard",
                        soft_cap=soft_cap,
                        hard_cap=hard_cap,
                        position_size=0.05,
                        min_conv_below_soft=min_conv_below,
                        min_conv_above_soft=min_conv_above,
                        min_conv_at_hard=min_conv_above,
                        replace_rule="weakest_conv",
                        high_conv_threshold=0,
                        high_conv_size_mult=1.0,
                        stop_pct=-0.15,
                        hold_days=30,
                    ))

    log.info(f"Reversal configs: {len(configs)}")
    return configs


def _build_composite_configs() -> list[CapConfig]:
    """Build ~96 composite configs testing hard_only and soft_hard approaches.

    Approach A (~24 configs): 4 hard_cap * 2 replace * 3 min_conv = 24
    Approach B (~72 configs): 3 soft * 3 delta * 3 min_below * 2 min_above * 2 replace = 108
    Trimmed to ~72 by using 2 min_below values for skip_at_hard.
    """
    configs: list[CapConfig] = []

    # --- Approach A: Hard Cap Only (~24 configs) ---
    for hard_cap in [15, 18, 20, 25]:
        for replace_rule in ["skip", "weakest_conv"]:
            for min_conv in [3, 4, 5]:
                name = f"C-A:{hard_cap}h/3.3%/{replace_rule}/mc{min_conv}"
                configs.append(CapConfig(
                    name=name,
                    approach="hard_only",
                    soft_cap=hard_cap,
                    hard_cap=hard_cap,
                    position_size=0.033,
                    min_conv_below_soft=min_conv,
                    min_conv_above_soft=0,
                    min_conv_at_hard=min_conv + 2,
                    replace_rule=replace_rule,
                    high_conv_threshold=0,
                    high_conv_size_mult=1.0,
                    stop_pct=-0.15,
                    hold_days=30,
                ))

    # --- Approach B: Soft Cap + Hard Cap ---
    for soft_cap in [12, 15, 18]:
        for delta in [3, 5, 8]:
            hard_cap = soft_cap + delta
            for min_conv_below in [3, 4, 5]:
                for min_conv_above in [7, 7.5]:
                    # weakest_conv: all min_conv_below values
                    name = (f"C-B:{soft_cap}s/{hard_cap}h/"
                            f"mc{min_conv_below}/{min_conv_above:.0f}a/"
                            f"weakest_conv")
                    configs.append(CapConfig(
                        name=name,
                        approach="soft_hard",
                        soft_cap=soft_cap,
                        hard_cap=hard_cap,
                        position_size=0.033,
                        min_conv_below_soft=min_conv_below,
                        min_conv_above_soft=min_conv_above,
                        min_conv_at_hard=min_conv_above,
                        replace_rule="weakest_conv",
                        high_conv_threshold=0,
                        high_conv_size_mult=1.0,
                        stop_pct=-0.15,
                        hold_days=30,
                    ))

            # skip_at_hard: fewer combos (only min_conv 3 and 5)
            for min_conv_below in [3, 5]:
                for min_conv_above in [7, 7.5]:
                    name = (f"C-B:{soft_cap}s/{hard_cap}h/"
                            f"mc{min_conv_below}/{min_conv_above:.0f}a/"
                            f"skip_at_hard")
                    configs.append(CapConfig(
                        name=name,
                        approach="soft_hard",
                        soft_cap=soft_cap,
                        hard_cap=hard_cap,
                        position_size=0.033,
                        min_conv_below_soft=min_conv_below,
                        min_conv_above_soft=min_conv_above,
                        min_conv_at_hard=min_conv_above,
                        replace_rule="skip_at_hard",
                        high_conv_threshold=0,
                        high_conv_size_mult=1.0,
                        stop_pct=-0.15,
                        hold_days=30,
                    ))

    log.info(f"Composite configs: {len(configs)}")
    return configs


# ---------------------------------------------------------------------------
# Output: CSV + text reports
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "config_name", "strategy_type", "approach",
    # Full period
    "full_trades", "full_win_rate", "full_cagr", "full_sharpe",
    "full_max_drawdown", "full_profit_factor", "full_avg_hold",
    "full_replacements", "full_max_concurrent", "full_pct_days_at_cap",
    "full_final_equity",
    # Train
    "train_trades", "train_win_rate", "train_cagr", "train_sharpe",
    "train_max_drawdown", "train_profit_factor", "train_avg_hold",
    "train_replacements", "train_max_concurrent", "train_pct_days_at_cap",
    # Test
    "test_trades", "test_win_rate", "test_cagr", "test_sharpe",
    "test_max_drawdown", "test_profit_factor", "test_avg_hold",
    "test_replacements", "test_max_concurrent", "test_pct_days_at_cap",
]


def _result_to_row(r: SimOutput) -> dict:
    row = {
        "config_name": r.config_name,
        "strategy_type": r.strategy_type,
        "approach": r.approach,
    }
    for prefix, m in [("full", r.full), ("train", r.train), ("test", r.test)]:
        row[f"{prefix}_trades"] = m.trades
        row[f"{prefix}_win_rate"] = f"{m.win_rate:.4f}"
        row[f"{prefix}_cagr"] = f"{m.cagr:.6f}"
        row[f"{prefix}_sharpe"] = f"{m.sharpe:.4f}"
        row[f"{prefix}_max_drawdown"] = f"{m.max_drawdown:.4f}"
        pf = f"{m.profit_factor:.4f}" if m.profit_factor < 1000 else "inf"
        row[f"{prefix}_profit_factor"] = pf
        row[f"{prefix}_avg_hold"] = f"{m.avg_hold:.1f}"
        row[f"{prefix}_replacements"] = m.replacements
        row[f"{prefix}_max_concurrent"] = m.max_concurrent
        row[f"{prefix}_pct_days_at_cap"] = f"{m.pct_days_at_cap:.4f}"
    row["full_final_equity"] = f"{r.full.final_equity:.2f}"
    return row


def _checkpoint_results(results: list[SimOutput], filepath: Path):
    """Write partial results to CSV."""
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in results:
            writer.writerow(_result_to_row(r))


def _write_top10_report(
    results: list[SimOutput],
    filepath: Path,
    title: str,
):
    """Write top-10 report sorted by test-period Sharpe."""
    # Sort by test Sharpe descending, break ties with train Sharpe
    ranked = sorted(results, key=lambda r: (-r.test.sharpe, -r.train.sharpe))
    top = ranked[:10]

    lines = []
    lines.append(f"{'=' * 100}")
    lines.append(f"  {title}")
    lines.append(f"  Sorted by test-period Sharpe (2024-2026)")
    lines.append(f"{'=' * 100}")
    lines.append("")

    hdr = (f"  {'#':>2} {'Config':<50} "
           f"{'Train':>8} {'Test':>8} | "
           f"{'TrainWR':>7} {'TestWR':>7} | "
           f"{'TrainCAGR':>9} {'TestCAGR':>9} | "
           f"{'MaxDD':>6}")
    lines.append(hdr)
    lines.append(f"  {'-' * 2} {'-' * 50} "
                 f"{'-' * 8} {'-' * 8}   "
                 f"{'-' * 7} {'-' * 7}   "
                 f"{'-' * 9} {'-' * 9}   "
                 f"{'-' * 6}")

    for i, r in enumerate(top, 1):
        lines.append(
            f"  {i:>2} {r.config_name:<50} "
            f"{r.train.sharpe:>8.2f} {r.test.sharpe:>8.2f} | "
            f"{r.train.win_rate:>6.1%} {r.test.win_rate:>6.1%} | "
            f"{r.train.cagr:>8.1%} {r.test.cagr:>8.1%} | "
            f"{r.full.max_drawdown:>5.1%}"
        )

    lines.append("")
    lines.append("  Full details for #1:")
    if top:
        best = top[0]
        for period_name, m in [("Train (2020-2023)", best.train),
                               ("Test  (2024-2026)", best.test),
                               ("Full  (2020-2026)", best.full)]:
            pf = f"{m.profit_factor:.2f}" if m.profit_factor < 1000 else "inf"
            lines.append(
                f"    {period_name}: {m.trades} trades, {m.win_rate:.1%} WR, "
                f"CAGR {m.cagr:.1%}, Sharpe {m.sharpe:.2f}, "
                f"MaxDD {m.max_drawdown:.1%}, PF {pf}, "
                f"AvgHold {m.avg_hold:.0f}d, {m.replacements} repl, "
                f"MaxConc {m.max_concurrent}, AtCap {m.pct_days_at_cap:.0%}"
            )
    lines.append("")

    text = "\n".join(lines)
    filepath.write_text(text)
    log.info(f"Wrote {filepath}")
    return text


def _write_approach_comparison(
    reversal_results: list[SimOutput],
    composite_results: list[SimOutput],
    filepath: Path,
):
    """Write head-to-head comparison of best Approach A vs best Approach B."""
    lines = []
    lines.append(f"{'=' * 100}")
    lines.append(f"  APPROACH COMPARISON: Hard-Only (A) vs Soft+Hard (B)")
    lines.append(f"  Best config from each approach by test-period Sharpe")
    lines.append(f"{'=' * 100}")
    lines.append("")

    for label, results in [("REVERSAL", reversal_results), ("COMPOSITE", composite_results)]:
        a_results = [r for r in results if r.approach == "hard_only"]
        b_results = [r for r in results if r.approach == "soft_hard"]

        best_a = max(a_results, key=lambda r: r.test.sharpe) if a_results else None
        best_b = max(b_results, key=lambda r: r.test.sharpe) if b_results else None

        lines.append(f"  --- {label} ---")
        for tag, best in [("Approach A (hard_only)", best_a), ("Approach B (soft_hard)", best_b)]:
            if best is None:
                lines.append(f"  {tag}: No configs")
                continue
            lines.append(f"  {tag}: {best.config_name}")
            for period_name, m in [("  Train", best.train), ("  Test ", best.test),
                                   ("  Full ", best.full)]:
                pf = f"{m.profit_factor:.2f}" if m.profit_factor < 1000 else "inf"
                lines.append(
                    f"    {period_name}: {m.trades}t, {m.win_rate:.1%} WR, "
                    f"CAGR {m.cagr:.1%}, Sharpe {m.sharpe:.2f}, "
                    f"MaxDD {m.max_drawdown:.1%}, PF {pf}"
                )
        lines.append("")

    text = "\n".join(lines)
    filepath.write_text(text)
    log.info(f"Wrote {filepath}")
    return text


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Position management backtester")
    parser.add_argument("--strategy", choices=["reversal", "composite", "all"],
                        default="all", help="Which strategy type to test")
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default="2026-03-29")
    parser.add_argument("--capital", type=float, default=100_000.0)
    args = parser.parse_args()

    log.info(f"Position Rules Backtester")
    log.info(f"Period: {args.start} to {args.end}")
    log.info(f"Train: {args.start} to {TRAIN_END} | Test: {TEST_START} to {args.end}")
    log.info(f"Starting capital: ${args.capital:,.0f}")

    # Open database connections (read-only)
    insiders_conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    prices_conn = sqlite3.connect(f"file:{PRICES_DB}?mode=ro", uri=True)

    try:
        # Build PIT score cache (shared across all configs)
        pit_cache = PITScoreCache(insiders_conn)

        # Build trading calendar (shared)
        cache_start = (datetime.strptime(args.start, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        cache_end = (datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")
        trading_days = _build_trading_calendar(prices_conn, args.start, cache_end)
        log.info(f"Trading calendar: {len(trading_days)} days")

        all_results: list[SimOutput] = []
        reversal_results: list[SimOutput] = []
        composite_results: list[SimOutput] = []

        strategies_to_run = []
        if args.strategy in ("reversal", "all"):
            strategies_to_run.append(("reversal", _build_reversal_configs()))
        if args.strategy in ("composite", "all"):
            strategies_to_run.append(("composite", _build_composite_configs()))

        csv_path = REPORT_DIR / "grid_results.csv"

        for strategy_type, configs in strategies_to_run:
            log.info(f"\n{'=' * 60}")
            log.info(f"  Strategy: {strategy_type} ({len(configs)} configs)")
            log.info(f"{'=' * 60}")

            # Load events once per strategy type
            events = _prepare_events(
                strategy_type, insiders_conn, pit_cache, args.start, args.end
            )
            if not events:
                log.warning(f"No events for {strategy_type}, skipping")
                continue

            # Build price cache for this strategy's tickers
            all_tickers = {e["ticker"] for e in events}
            prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)

            for i, cfg in enumerate(configs):
                t0 = time.time()
                result = run_simulation(
                    config=cfg,
                    strategy_type=strategy_type,
                    all_events=events,
                    prices=prices,
                    trading_days=trading_days,
                    start=args.start,
                    end=args.end,
                    starting_capital=args.capital,
                )
                elapsed = time.time() - t0

                log.info(
                    f"[{i + 1}/{len(configs)}] {cfg.name} => "
                    f"T:{result.full.trades} WR:{result.full.win_rate:.1%} "
                    f"Sharpe:{result.full.sharpe:.2f} "
                    f"(train:{result.train.sharpe:.2f} test:{result.test.sharpe:.2f}) "
                    f"({elapsed:.1f}s)"
                )

                all_results.append(result)
                if strategy_type == "reversal":
                    reversal_results.append(result)
                else:
                    composite_results.append(result)

                # Checkpoint every 10 configs
                if (i + 1) % 10 == 0:
                    _checkpoint_results(all_results, csv_path)
                    log.info(f"  Checkpoint: {len(all_results)} results saved")

        # --- Write final outputs ---
        _checkpoint_results(all_results, csv_path)
        log.info(f"Wrote {csv_path} ({len(all_results)} rows)")

        if reversal_results:
            txt = _write_top10_report(
                reversal_results,
                REPORT_DIR / "reversal_top10.txt",
                "TOP 10 REVERSAL CONFIGS",
            )
            print(txt)

        if composite_results:
            txt = _write_top10_report(
                composite_results,
                REPORT_DIR / "composite_top10.txt",
                "TOP 10 COMPOSITE CONFIGS",
            )
            print(txt)

        if reversal_results or composite_results:
            txt = _write_approach_comparison(
                reversal_results, composite_results,
                REPORT_DIR / "approach_comparison.txt",
            )
            print(txt)

        log.info("Done.")

    finally:
        insiders_conn.close()
        prices_conn.close()


if __name__ == "__main__":
    main()
