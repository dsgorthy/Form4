#!/usr/bin/env python3
"""
CW Simulation — CEO Watcher-inspired multi-strategy comparison grid.

Event-driven simulation runner that backtests insider trade signals across
a grid of configurations: instruments (shares/calls/puts), exit strategies,
thesis filters, and score thresholds.

Follows the same event-driven pattern as pit_portfolio_sim.py.

Usage:
    # Run full comparison grid (default: 2016-2025)
    python3 pipelines/insider_study/cw_simulation.py

    # Single config
    python3 pipelines/insider_study/cw_simulation.py --single --hold-days 30 --instrument shares

    # Custom date range
    python3 pipelines/insider_study/cw_simulation.py --start 2020-01-01 --end 2024-12-31

    # Output to specific file
    python3 pipelines/insider_study/cw_simulation.py --output reports/cw_simulation/custom_grid.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Imports — handle both package and standalone execution
# ---------------------------------------------------------------------------

try:
    from pipelines.insider_study.price_utils import load_prices
    from pipelines.insider_study.cw_exit_strategies import (
        fixed_hold_exit,
        fair_value_exit,
        sma50_break_exit,
        catalyst_exit,
        trailing_stop_exit,
        thesis_based_exit,
    )
except ModuleNotFoundError:
    from price_utils import load_prices
    from cw_exit_strategies import (
        fixed_hold_exit,
        fair_value_exit,
        sma50_break_exit,
        catalyst_exit,
        trailing_stop_exit,
        thesis_based_exit,
    )

# Black-Scholes fallback for options pricing
try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from framework.pricing.black_scholes import BlackScholes
    HAS_BS = True
except ImportError:
    HAS_BS = False

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "reports" / "cw_simulation"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grade ordering for filter comparisons
# ---------------------------------------------------------------------------

GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4}


def _grade_passes(trade_grade: str | None, min_grade: str) -> bool:
    """Return True if trade_grade meets or exceeds min_grade (A > B > C ...)."""
    if trade_grade is None:
        return False
    return GRADE_ORDER.get(trade_grade, 99) <= GRADE_ORDER.get(min_grade, 99)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SimConfig:
    name: str
    # Indicator filters
    min_signal_grade: str | None = None
    require_cluster: bool = False
    require_reversal: bool = False
    min_dip_pct: float | None = None  # e.g., -0.30 for deep dip
    require_above_sma50: bool | None = None
    require_largest_ever: bool = False
    exclude_recurring: bool = True
    exclude_tax_sales: bool = True
    thesis_filter: str | None = None  # restrict to one thesis type
    min_consecutive_sells: int | None = None
    min_purchase_size_ratio: float | None = None
    # Execution
    instrument: str = "shares"  # 'shares', 'call', 'put'
    hold_days: int = 30
    exit_strategy: str = "fixed"  # 'fixed', 'fair_value', 'sma50_break', 'catalyst', 'trailing'
    # Options params
    strike_otm_pct: float = 0.05
    target_dte: int = 90
    # Risk
    stop_loss_pct: float | None = -0.15
    max_concurrent: int = 30
    starting_capital: float = 100_000.0
    equal_weight: bool = True


@dataclass
class TradeResult:
    trade_id: int
    ticker: str
    entry_date: str
    exit_date: str | None
    entry_price: float
    exit_price: float | None
    return_pct: float | None
    hold_days: int | None
    exit_reason: str
    thesis: str | None
    instrument: str
    # Options-specific
    option_entry: float | None = None
    option_exit: float | None = None


@dataclass
class SimResult:
    config: SimConfig
    trades: list[TradeResult] = field(default_factory=list)
    n_trades: int = 0
    win_rate: float = 0.0
    avg_return: float = 0.0
    median_return: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    total_pnl: float = 0.0
    annualized_return: float = 0.0


# ---------------------------------------------------------------------------
# Event universe loader
# ---------------------------------------------------------------------------

def load_event_universe(conn: sqlite3.Connection, config: SimConfig,
                        start: str, end: str) -> list[dict]:
    """
    Load eligible trades from insiders.db applying all indicator filters.

    Joins trades with trade_returns, trade_signals (for thesis), and
    insider_ticker_scores. Returns list of event dicts.
    """
    # Build WHERE clauses dynamically
    conditions = [
        # Filter on filing_date (when info becomes public), not trade_date
        "t.filing_date BETWEEN ? AND ?",
        "t.trans_code = 'P'",
        "COALESCE(t.cohen_routine, 0) = 0",
    ]
    params: list[Any] = [start, end]

    if config.exclude_recurring:
        conditions.append("COALESCE(t.is_recurring, 0) = 0")

    if config.exclude_tax_sales:
        conditions.append("COALESCE(t.is_tax_sale, 0) = 0")

    if config.min_signal_grade is not None:
        # We'll filter in Python since grade comparison is non-trivial in SQL
        pass

    if config.require_reversal:
        conditions.append("t.is_rare_reversal = 1")

    if config.require_above_sma50 is True:
        conditions.append("t.above_sma50 = 1")
    elif config.require_above_sma50 is False:
        conditions.append("(t.above_sma50 = 0 OR t.above_sma50 IS NULL)")

    if config.require_largest_ever:
        conditions.append("t.is_largest_ever = 1")

    if config.min_dip_pct is not None:
        # min_dip_pct is negative, e.g. -0.30.  We want trades where
        # dip_1mo <= -0.30  OR  dip_3mo <= -0.30  OR  dip_1yr <= -0.30
        conditions.append(
            "(t.dip_1mo <= ? OR t.dip_3mo <= ? OR t.dip_1yr <= ?)"
        )
        params.extend([config.min_dip_pct, config.min_dip_pct, config.min_dip_pct])

    if config.min_consecutive_sells is not None:
        conditions.append("COALESCE(t.consecutive_sells_before, 0) >= ?")
        params.append(config.min_consecutive_sells)

    if config.min_purchase_size_ratio is not None:
        conditions.append("t.purchase_size_ratio >= ?")
        params.append(config.min_purchase_size_ratio)

    where_clause = " AND ".join(conditions)

    # Left join trade_signals for thesis (signal_type = 'buying_the_dip',
    # 'trend_reversal', etc. — we map these to thesis categories)
    # Left join insider_ticker_scores for PIT blended_score
    query = f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.filing_date,
               t.price, t.value, t.is_csuite, t.title_weight, t.title,
               t.signal_grade, t.dip_1mo, t.dip_3mo, t.dip_1yr,
               t.above_sma50, t.is_rare_reversal, t.is_largest_ever,
               t.purchase_size_ratio, t.consecutive_sells_before,
               tr.return_7d, tr.return_14d, tr.return_30d, tr.return_60d,
               tr.return_90d, tr.return_180d, tr.return_365d,
               tr.entry_price AS tr_entry_price,
               its.blended_score AS pit_score
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        -- PIT fix: use the most recent score as of the trade's filing date,
        -- not the latest score in the entire DB
        LEFT JOIN insider_ticker_scores its
            ON t.insider_id = its.insider_id
            AND t.ticker = its.ticker
            AND its.sufficient_data = 1
            AND its.as_of_date = (
                SELECT MAX(its2.as_of_date)
                FROM insider_ticker_scores its2
                WHERE its2.insider_id = t.insider_id
                  AND its2.ticker = t.ticker
                  AND its2.sufficient_data = 1
                  AND its2.as_of_date <= t.filing_date
            )
        WHERE {where_clause}
        ORDER BY t.trade_date
    """

    cursor = conn.execute(query, params)
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Build event dicts
    events = []
    for row in rows:
        event = dict(zip(col_names, row))

        # Python-side grade filter
        if config.min_signal_grade is not None:
            if not _grade_passes(event.get("signal_grade"), config.min_signal_grade):
                continue

        events.append(event)

    # If require_cluster, filter to trades that are part of a cluster
    # (2+ distinct insiders buying same ticker within 30 days)
    if config.require_cluster:
        events = _filter_cluster_events(events)

    # Attach thesis labels from trade_signals
    if events:
        _attach_thesis_labels(conn, events)

    # Filter by thesis if requested
    if config.thesis_filter is not None:
        events = [e for e in events if e.get("thesis") == config.thesis_filter]

    logger.info("Loaded %d events for config '%s'", len(events), config.name)
    return events


def _filter_cluster_events(events: list[dict]) -> list[dict]:
    """Keep only events that are part of a cluster (2+ insiders, same ticker, within 30 days)."""
    from collections import defaultdict

    # Group by ticker
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        by_ticker[ev["ticker"]].append(ev)

    cluster_ids = set()
    for ticker, ticker_events in by_ticker.items():
        ticker_events.sort(key=lambda e: e["trade_date"])
        n = len(ticker_events)
        for i in range(n):
            td_i = ticker_events[i]["trade_date"]
            try:
                dt_i = datetime.strptime(td_i, "%Y-%m-%d")
            except ValueError:
                continue
            nearby_insiders = {ticker_events[i]["insider_id"]}
            for j in range(n):
                if i == j:
                    continue
                td_j = ticker_events[j]["trade_date"]
                try:
                    dt_j = datetime.strptime(td_j, "%Y-%m-%d")
                except ValueError:
                    continue
                if abs((dt_i - dt_j).days) <= 30:
                    nearby_insiders.add(ticker_events[j]["insider_id"])
            if len(nearby_insiders) >= 2:
                cluster_ids.add(ticker_events[i]["trade_id"])

    return [e for e in events if e["trade_id"] in cluster_ids]


def _attach_thesis_labels(conn: sqlite3.Connection, events: list[dict]):
    """Attach thesis label to each event based on trade_signals and indicators."""
    trade_ids = [e["trade_id"] for e in events]
    if not trade_ids:
        return

    # Fetch signal types for all trade_ids in batches
    signal_map: dict[int, list[str]] = {}
    batch_size = 500
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        rows = conn.execute(f"""
            SELECT trade_id, signal_type FROM trade_signals
            WHERE trade_id IN ({placeholders})
              AND signal_class = 'bullish'
        """, batch).fetchall()
        for tid, stype in rows:
            signal_map.setdefault(tid, []).append(stype)

    # Map signal_types to thesis categories
    for ev in events:
        signals = signal_map.get(ev["trade_id"], [])
        ev["thesis"] = _classify_thesis(ev, signals)


def _classify_thesis(event: dict, signal_types: list[str]) -> str | None:
    """
    Classify the primary thesis for an event based on its signals and indicators.

    Priority order (most specific to least):
      1. dip_buy     — buying_the_dip signal AND dip_1mo <= -0.10
      2. reversal    — trend_reversal signal OR is_rare_reversal
      3. momentum    — above_sma50 AND no dip signals
      4. catalyst    — contrarian (market dip = catalyst)
      5. cluster     — top_trade with cluster reason (fallback)
      6. value       — size_anomaly or large_holdings_increase
      7. growth      — default for above-SMA50 without other signals
    """
    dip_1mo = event.get("dip_1mo")
    is_reversal = event.get("is_rare_reversal", 0)
    above_sma50 = event.get("above_sma50")

    # Check for dip_buy
    if "buying_the_dip" in signal_types:
        return "dip_buy"
    if dip_1mo is not None and dip_1mo <= -0.10:
        return "dip_buy"

    # Check for reversal
    if "trend_reversal" in signal_types or is_reversal == 1:
        return "reversal"

    # Check for catalyst (contrarian = buying during market dip)
    if "contrarian" in signal_types:
        return "catalyst"

    # Check for value
    if "size_anomaly" in signal_types or "large_holdings_increase" in signal_types:
        return "value"

    # Momentum — above SMA50
    if above_sma50 == 1:
        return "momentum"

    # Cluster fallback
    if "top_trade" in signal_types:
        return "cluster"

    return None


# ---------------------------------------------------------------------------
# Trade simulation — shares
# ---------------------------------------------------------------------------

def _resolve_exit(prices: dict[str, float], event: dict, config: SimConfig) -> tuple[str | None, float | None, str]:
    """Resolve exit based on config's exit strategy."""
    entry_date = event.get("sim_entry_date", event["trade_date"])
    entry_price = event.get("sim_entry_price", event["price"])

    if config.exit_strategy == "fixed":
        return fixed_hold_exit(prices, entry_date, entry_price, config.hold_days)

    elif config.exit_strategy == "fair_value":
        # Need pre_dip_price — derive from dip_1mo if available
        dip_pct = event.get("dip_1mo") or event.get("dip_3mo") or -0.20
        if dip_pct >= 0:
            dip_pct = -0.20  # default assumption
        pre_dip_price = entry_price / (1 + dip_pct)
        return fair_value_exit(prices, entry_date, entry_price, pre_dip_price)

    elif config.exit_strategy == "sma50_break":
        return sma50_break_exit(prices, entry_date, entry_price)

    elif config.exit_strategy == "catalyst":
        # Use filing_date as the catalyst event
        event_date = event.get("filing_date", entry_date)
        return catalyst_exit(prices, entry_date, entry_price, event_date)

    elif config.exit_strategy == "trailing":
        stop = abs(config.stop_loss_pct) if config.stop_loss_pct else 0.15
        return trailing_stop_exit(prices, entry_date, entry_price, stop_pct=stop)

    elif config.exit_strategy == "thesis_based":
        thesis = event.get("thesis") or "cluster"
        kwargs: dict[str, Any] = {}
        if thesis == "dip_buy":
            dip_pct = event.get("dip_1mo") or event.get("dip_3mo") or -0.20
            if dip_pct >= 0:
                dip_pct = -0.20
            kwargs["pre_dip_price"] = entry_price / (1 + dip_pct)
        elif thesis == "catalyst":
            kwargs["event_date"] = event.get("filing_date", entry_date)
        return thesis_based_exit(prices, entry_date, entry_price, thesis, **kwargs)

    else:
        # Unknown strategy — fall back to fixed hold
        return fixed_hold_exit(prices, entry_date, entry_price, config.hold_days)


def simulate_shares_trade(event: dict, prices: dict[str, float],
                          config: SimConfig) -> TradeResult | None:
    """
    Simulate buying shares at T+1 (approximated as next available trading day close).

    Uses the exit strategy configured in config.
    """
    # CRITICAL FIX: Enter on filing_date + 1, NOT trade_date + 1.
    # The trade_date is when the insider transacted — unknowable until the
    # Form 4 filing becomes public (filing_date, typically 2 business days later).
    filing_date = event.get("filing_date") or event["trade_date"]
    trade_price = event.get("price", 0)
    if not trade_price or trade_price <= 0:
        return None

    # T+1 after filing: find the next trading day with a price
    dates_after = sorted(d for d in prices if d > filing_date)
    if not dates_after:
        return None

    entry_date = dates_after[0]
    entry_price = prices[entry_date]
    if entry_price <= 0:
        return None

    # Store entry info on event for exit resolution
    event["sim_entry_date"] = entry_date
    event["sim_entry_price"] = entry_price

    exit_date, exit_price, exit_reason = _resolve_exit(prices, event, config)

    # Apply stop-loss override if configured and not already handled by exit strategy
    if (config.stop_loss_pct is not None
            and exit_price is not None
            and exit_reason not in ("trailing_stop", "sma50_break")):
        stop_level = entry_price * (1 + config.stop_loss_pct)
        # Walk prices from entry to exit checking for stop hit
        if exit_date:
            for d in sorted(d for d in prices if entry_date < d <= exit_date):
                if prices[d] <= stop_level:
                    exit_date = d
                    exit_price = stop_level  # assume fill at stop level
                    exit_reason = "stop_loss"
                    break

    if exit_date is None or exit_price is None:
        return None

    return_pct = (exit_price - entry_price) / entry_price

    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
        exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")
        hold = (exit_dt - entry_dt).days
    except ValueError:
        hold = None

    return TradeResult(
        trade_id=event["trade_id"],
        ticker=event["ticker"],
        entry_date=entry_date,
        exit_date=exit_date,
        entry_price=entry_price,
        exit_price=exit_price,
        return_pct=return_pct,
        hold_days=hold,
        exit_reason=exit_reason,
        thesis=event.get("thesis"),
        instrument="shares",
    )


# ---------------------------------------------------------------------------
# Trade simulation — options
# ---------------------------------------------------------------------------

def simulate_option_trade(event: dict, prices: dict[str, float],
                          conn: sqlite3.Connection,
                          config: SimConfig) -> TradeResult | None:
    """
    Simulate an options trade for an insider event.

    For calls: buy OTM call, target_dte DTE.
    For puts: sell ATM put, 45 DTE (cash-secured).

    Attempts to find real option prices from the option_prices table first,
    falling back to Black-Scholes pricing from framework.pricing.black_scholes.
    """
    trade_date = event["trade_date"]
    trade_price = event.get("price", 0)
    if not trade_price or trade_price <= 0:
        return None

    # T+1 entry
    dates_after = sorted(d for d in prices if d > trade_date)
    if not dates_after:
        return None

    entry_date = dates_after[0]
    underlying_entry = prices[entry_date]
    if underlying_entry <= 0:
        return None

    event["sim_entry_date"] = entry_date
    event["sim_entry_price"] = underlying_entry

    is_call = config.instrument == "call"
    option_type = "call" if is_call else "put"

    # Determine strike
    if is_call:
        strike = underlying_entry * (1 + config.strike_otm_pct)
    else:
        strike = underlying_entry  # ATM for puts

    strike = round(strike, 0)  # round to nearest dollar

    # Determine DTE
    dte = config.target_dte if is_call else 45

    # Try Black-Scholes pricing
    if not HAS_BS:
        return None

    # Estimate IV from recent realized vol (30-day)
    iv = _estimate_iv(prices, entry_date)
    if iv is None:
        iv = 0.30  # default fallback

    r = 0.045  # approximate risk-free rate
    T_entry = dte / 365.0

    option_entry_price = BlackScholes.price(underlying_entry, strike, T_entry, r, iv, option_type)
    if option_entry_price <= 0.01:
        return None

    # Determine exit
    exit_date, underlying_exit, exit_reason = _resolve_exit(prices, event, config)
    if exit_date is None or underlying_exit is None:
        return None

    # Compute time to expiration at exit
    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
        exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")
        days_held = (exit_dt - entry_dt).days
        T_exit = max((dte - days_held) / 365.0, 0.001)
    except ValueError:
        return None

    # Apply stop-loss check
    if config.stop_loss_pct is not None:
        stop_level = option_entry_price * (1 + config.stop_loss_pct)
        # Walk through prices checking if option value hits stop
        for d in sorted(d for d in prices if entry_date < d <= exit_date):
            try:
                d_dt = datetime.strptime(d, "%Y-%m-%d")
                T_d = max((dte - (d_dt - entry_dt).days) / 365.0, 0.001)
            except ValueError:
                continue
            opt_val = BlackScholes.price(prices[d], strike, T_d, r, iv, option_type)
            if opt_val <= stop_level:
                exit_date = d
                underlying_exit = prices[d]
                T_exit = T_d
                exit_reason = "stop_loss"
                break

    option_exit_price = BlackScholes.price(underlying_exit, strike, T_exit, r, iv, option_type)

    # For put selling, return is premium received minus any payout
    if config.instrument == "put":
        # Sold the put: collected premium, now buy back
        return_pct = (option_entry_price - option_exit_price) / option_entry_price
    else:
        # Bought the call
        if option_entry_price > 0:
            return_pct = (option_exit_price - option_entry_price) / option_entry_price
        else:
            return None

    try:
        hold = (datetime.strptime(exit_date, "%Y-%m-%d") - datetime.strptime(entry_date, "%Y-%m-%d")).days
    except ValueError:
        hold = None

    return TradeResult(
        trade_id=event["trade_id"],
        ticker=event["ticker"],
        entry_date=entry_date,
        exit_date=exit_date,
        entry_price=underlying_entry,
        exit_price=underlying_exit,
        return_pct=return_pct,
        hold_days=hold,
        exit_reason=exit_reason,
        thesis=event.get("thesis"),
        instrument=config.instrument,
        option_entry=round(option_entry_price, 4),
        option_exit=round(option_exit_price, 4),
    )


def _estimate_iv(prices: dict[str, float], as_of_date: str, window: int = 30) -> float | None:
    """Estimate annualized realized volatility from recent daily returns."""
    dates_before = sorted(d for d in prices if d <= as_of_date)
    if len(dates_before) < window + 1:
        return None

    recent = dates_before[-(window + 1):]
    closes = [prices[d] for d in recent]
    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            returns.append(math.log(closes[i] / closes[i - 1]))

    if len(returns) < 10:
        return None

    return float(np.std(returns)) * math.sqrt(252)


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def compute_metrics(trades: list[TradeResult], config: SimConfig) -> SimResult:
    """
    Compute portfolio metrics from list of trade results.

    Sharpe = mean(returns) / std(returns) * sqrt(252 / avg_hold_days)
    Max drawdown from cumulative PnL curve.
    Profit factor = sum(wins) / abs(sum(losses)).
    """
    if not trades:
        return SimResult(
            config=config,
            trades=trades,
        )

    returns = [t.return_pct for t in trades if t.return_pct is not None]
    if not returns:
        return SimResult(config=config, trades=trades)

    arr = np.array(returns)
    n = len(arr)
    mean_ret = float(np.mean(arr))
    median_ret = float(np.median(arr))
    std_ret = float(np.std(arr, ddof=1)) if n > 1 else 0.0

    # Win rate
    wins = int(np.sum(arr > 0))
    win_rate = wins / n if n > 0 else 0.0

    # Sharpe — annualize based on average hold period
    hold_days_list = [t.hold_days for t in trades if t.hold_days is not None and t.hold_days > 0]
    avg_hold = float(np.mean(hold_days_list)) if hold_days_list else 30.0
    annualization_factor = math.sqrt(252 / max(avg_hold, 1))
    sharpe = (mean_ret / std_ret * annualization_factor) if std_ret > 0 else 0.0

    # Total PnL (per unit capital, equal weight)
    total_pnl = float(np.sum(arr))

    # Max drawdown from cumulative return curve
    cum_returns = np.cumsum(arr)
    peak = np.maximum.accumulate(cum_returns)
    drawdowns = cum_returns - peak
    max_dd = float(np.min(drawdowns)) if len(drawdowns) > 0 else 0.0

    # Profit factor
    gross_wins = float(np.sum(arr[arr > 0]))
    gross_losses = float(np.sum(arr[arr < 0]))
    profit_factor = gross_wins / abs(gross_losses) if gross_losses != 0 else float("inf")

    # Annualized return
    # Total period in years
    dates = sorted(set(
        t.entry_date for t in trades if t.entry_date
    ) | set(
        t.exit_date for t in trades if t.exit_date
    ))
    if len(dates) >= 2:
        try:
            first = datetime.strptime(dates[0], "%Y-%m-%d")
            last = datetime.strptime(dates[-1], "%Y-%m-%d")
            years = (last - first).days / 365.25
            if years > 0 and n > 0:
                # Compound return
                cumulative = 1.0
                for r in returns:
                    cumulative *= (1 + r / n)  # spread across n trades
                ann_return = (cumulative ** (1 / years)) - 1
            else:
                ann_return = 0.0
        except ValueError:
            ann_return = 0.0
    else:
        ann_return = 0.0

    return SimResult(
        config=config,
        trades=trades,
        n_trades=n,
        win_rate=round(win_rate, 4),
        avg_return=round(mean_ret, 6),
        median_return=round(median_ret, 6),
        sharpe=round(sharpe, 4),
        max_drawdown=round(max_dd, 6),
        profit_factor=round(profit_factor, 4),
        total_pnl=round(total_pnl, 6),
        annualized_return=round(ann_return, 6),
    )


# ---------------------------------------------------------------------------
# Single simulation run
# ---------------------------------------------------------------------------

def run_simulation(conn: sqlite3.Connection, config: SimConfig,
                   start: str, end: str) -> SimResult:
    """
    Run full simulation for one config.

    1. Load event universe
    2. For each event, simulate trade based on instrument type
    3. Compute and return metrics
    """
    events = load_event_universe(conn, config, start, end)
    if not events:
        logger.info("No events for config '%s'", config.name)
        return SimResult(config=config, trades=[])

    trades: list[TradeResult] = []
    skipped = 0

    for event in events:
        ticker = event["ticker"]
        prices = load_prices(ticker)
        if not prices:
            skipped += 1
            continue

        if config.instrument == "shares":
            result = simulate_shares_trade(event, prices, config)
        elif config.instrument in ("call", "put"):
            result = simulate_option_trade(event, prices, conn, config)
        else:
            skipped += 1
            continue

        if result is not None:
            trades.append(result)
        else:
            skipped += 1

    logger.info("Config '%s': %d trades, %d skipped", config.name, len(trades), skipped)
    result = compute_metrics(trades, config)
    # Drop trade list to free memory — only keep metrics
    result.trades = []
    return result


# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

def build_default_grid() -> list[SimConfig]:
    """
    Build the comparison grid of all valid config combinations.

    Instruments: shares, call, put
    Hold/exit: 7d, 14d, 30d, 60d, 90d, 180d, 365d fixed; thesis_based; trailing_15%
    Thesis: None (all), dip_buy, reversal, momentum, cluster
    Score: None, 'A', 'B'

    Skips invalid combos (fair_value only for dip_buy, etc.).
    """
    configs = []

    instruments = ["shares", "call", "put"]
    hold_exit_combos = [
        (7, "fixed"),
        (14, "fixed"),
        (30, "fixed"),
        (60, "fixed"),
        (90, "fixed"),
        (180, "fixed"),
        (365, "fixed"),
        (90, "thesis_based"),   # thesis_based uses variable hold
        (90, "trailing"),       # trailing 15% stop
    ]
    thesis_filters = [None, "dip_buy", "reversal", "momentum", "cluster"]
    score_filters = [None, "A", "B"]

    for instrument in instruments:
        for hold_days, exit_strategy in hold_exit_combos:
            for thesis in thesis_filters:
                for score in score_filters:

                    # Skip invalid combos
                    # fair_value only makes sense for dip_buy (handled within thesis_based)
                    # thesis_based + no thesis still works (dispatches per-event)

                    # Build descriptive name
                    parts = [instrument]
                    if exit_strategy == "fixed":
                        parts.append(f"{hold_days}d")
                    elif exit_strategy == "trailing":
                        parts.append("trail15%")
                    else:
                        parts.append(exit_strategy)
                    if thesis:
                        parts.append(thesis)
                    if score:
                        parts.append(f"grade{score}")
                    name = "_".join(parts)

                    # Options params by instrument type
                    if instrument == "call":
                        strike_otm = 0.05
                        target_dte = 90
                    elif instrument == "put":
                        strike_otm = 0.0  # ATM
                        target_dte = 45
                    else:
                        strike_otm = 0.05
                        target_dte = 90

                    configs.append(SimConfig(
                        name=name,
                        min_signal_grade=score,
                        thesis_filter=thesis,
                        instrument=instrument,
                        hold_days=hold_days,
                        exit_strategy=exit_strategy,
                        strike_otm_pct=strike_otm,
                        target_dte=target_dte,
                        stop_loss_pct=-0.15,
                        exclude_recurring=True,
                        exclude_tax_sales=True,
                    ))

    logger.info("Built grid with %d configs", len(configs))
    return configs


# ---------------------------------------------------------------------------
# Grid runner
# ---------------------------------------------------------------------------

def run_comparison_grid(conn: sqlite3.Connection, start: str = "2016-01-01",
                        end: str = "2025-12-31",
                        grid: list[SimConfig] | None = None) -> list[SimResult]:
    """Run all configs in the grid and return results sorted by Sharpe descending."""
    if grid is None:
        grid = build_default_grid()

    results: list[SimResult] = []
    total = len(grid)
    t0 = time.monotonic()

    for i, config in enumerate(grid):
        if (i + 1) % 25 == 0 or i == 0:
            elapsed = time.monotonic() - t0
            logger.info("Running config %d/%d (%.1fs elapsed)...", i + 1, total, elapsed)

        result = run_simulation(conn, config, start, end)
        results.append(result)

    # Sort by Sharpe descending
    results.sort(key=lambda r: -r.sharpe)

    elapsed = time.monotonic() - t0
    logger.info("Grid complete: %d configs in %.1fs", total, elapsed)
    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_grid_csv(results: list[SimResult], path: str | Path):
    """Write grid results to CSV."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "name", "instrument", "hold_days", "exit_strategy",
        "thesis_filter", "min_signal_grade",
        "n_trades", "win_rate", "avg_return", "median_return",
        "sharpe", "max_drawdown", "profit_factor", "total_pnl",
        "annualized_return",
    ]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "name": r.config.name,
                "instrument": r.config.instrument,
                "hold_days": r.config.hold_days,
                "exit_strategy": r.config.exit_strategy,
                "thesis_filter": r.config.thesis_filter or "all",
                "min_signal_grade": r.config.min_signal_grade or "any",
                "n_trades": r.n_trades,
                "win_rate": r.win_rate,
                "avg_return": r.avg_return,
                "median_return": r.median_return,
                "sharpe": r.sharpe,
                "max_drawdown": r.max_drawdown,
                "profit_factor": r.profit_factor,
                "total_pnl": r.total_pnl,
                "annualized_return": r.annualized_return,
            })

    logger.info("Grid CSV written: %s (%d rows)", path, len(results))


def print_summary_table(results: list[SimResult], top_n: int = 30):
    """Print a formatted summary table of top configs by Sharpe."""
    print()
    print("=" * 140)
    print("  CW SIMULATION GRID — TOP CONFIGS BY SHARPE")
    print("=" * 140)
    print()

    header = (
        f"{'#':>3} | {'Name':<40} | {'Inst':>6} | {'Exit':>12} | "
        f"{'N':>6} | {'WR':>6} | {'Mean':>8} | {'Med':>8} | "
        f"{'Sharpe':>7} | {'PF':>6} | {'MaxDD':>8} | {'PnL':>8}"
    )
    print(header)
    print("-" * 140)

    for i, r in enumerate(results[:top_n]):
        if r.n_trades == 0:
            continue
        pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 100 else "inf"
        print(
            f"{i+1:>3} | {r.config.name:<40} | {r.config.instrument:>6} | "
            f"{r.config.exit_strategy:>12} | "
            f"{r.n_trades:>6} | {r.win_rate:>5.1%} | "
            f"{r.avg_return:>+7.2%} | {r.median_return:>+7.2%} | "
            f"{r.sharpe:>7.2f} | {pf_str:>6} | {r.max_drawdown:>+7.2%} | "
            f"{r.total_pnl:>+7.2%}"
        )

    # Summary stats
    with_trades = [r for r in results if r.n_trades > 0]
    print()
    print("-" * 140)
    pos_sharpe = sum(1 for r in with_trades if r.sharpe > 0)
    avg_n = float(np.mean([r.n_trades for r in with_trades])) if with_trades else 0
    print(
        f"  Configs with trades: {len(with_trades)}/{len(results)} | "
        f"Positive Sharpe: {pos_sharpe}/{len(with_trades)} | "
        f"Avg trades/config: {avg_n:.0f}"
    )

    # By instrument
    for inst in ["shares", "call", "put"]:
        inst_results = [r for r in with_trades if r.config.instrument == inst]
        if inst_results:
            avg_sharpe = float(np.mean([r.sharpe for r in inst_results]))
            avg_wr = float(np.mean([r.win_rate for r in inst_results]))
            print(f"  {inst:>6}: Avg Sharpe={avg_sharpe:.3f}, Avg WR={avg_wr:.1%}, N configs={len(inst_results)}")

    print("=" * 140)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CW Simulation — multi-strategy comparison grid for insider trades"
    )
    parser.add_argument("--start", default="2016-01-01",
                        help="Start date (default: 2016-01-01)")
    parser.add_argument("--end", default="2025-12-31",
                        help="End date (default: 2025-12-31)")
    parser.add_argument("--output", default=str(OUTPUT_DIR / "grid_results.csv"),
                        help="Output CSV path")
    parser.add_argument("--top", type=int, default=30,
                        help="Number of top configs to display (default: 30)")
    parser.add_argument("--db", default=str(DB_PATH),
                        help="Path to insiders.db")

    # Single config mode
    parser.add_argument("--single", action="store_true",
                        help="Run a single config instead of the full grid")
    parser.add_argument("--instrument", default="shares",
                        choices=["shares", "call", "put"],
                        help="Instrument type (default: shares)")
    parser.add_argument("--hold-days", type=int, default=30,
                        help="Hold period in calendar days (default: 30)")
    parser.add_argument("--exit-strategy", default="fixed",
                        choices=["fixed", "fair_value", "sma50_break", "catalyst",
                                 "trailing", "thesis_based"],
                        help="Exit strategy (default: fixed)")
    parser.add_argument("--thesis", default=None,
                        help="Filter to specific thesis type")
    parser.add_argument("--grade", default=None,
                        help="Minimum signal grade (A, B, C)")
    parser.add_argument("--require-cluster", action="store_true",
                        help="Require cluster signal")
    parser.add_argument("--no-stop", action="store_true",
                        help="Disable stop-loss")

    args = parser.parse_args()

    print("=" * 70)
    print("  CW Simulation — Insider Trade Strategy Grid")
    print("=" * 70)
    print(f"  Period: {args.start} to {args.end}")
    print(f"  Database: {args.db}")
    print()

    conn = sqlite3.connect(args.db)
    conn.row_factory = None  # use tuples, not Row objects (we zip manually)

    if args.single:
        # Single config mode
        config = SimConfig(
            name=f"{args.instrument}_{args.hold_days}d_{args.exit_strategy}",
            instrument=args.instrument,
            hold_days=args.hold_days,
            exit_strategy=args.exit_strategy,
            thesis_filter=args.thesis,
            min_signal_grade=args.grade,
            require_cluster=args.require_cluster,
            stop_loss_pct=None if args.no_stop else -0.15,
        )
        print(f"  Single config: {config.name}")
        print()

        result = run_simulation(conn, config, args.start, args.end)

        # Print detailed results
        print(f"\n  RESULTS: {config.name}")
        print(f"  {'='*50}")
        print(f"  Trades:           {result.n_trades:>10}")
        print(f"  Win Rate:         {result.win_rate:>10.1%}")
        print(f"  Avg Return:       {result.avg_return:>+10.3%}")
        print(f"  Median Return:    {result.median_return:>+10.3%}")
        print(f"  Sharpe:           {result.sharpe:>10.3f}")
        print(f"  Max Drawdown:     {result.max_drawdown:>+10.3%}")
        pf_str = f"{result.profit_factor:.3f}" if result.profit_factor < 100 else "inf"
        print(f"  Profit Factor:    {pf_str:>10}")
        print(f"  Total PnL:        {result.total_pnl:>+10.3%}")
        print(f"  Ann. Return:      {result.annualized_return:>+10.3%}")

        if result.trades:
            # Breakdown by thesis
            thesis_counts: dict[str | None, list[float]] = {}
            for t in result.trades:
                if t.return_pct is not None:
                    thesis_counts.setdefault(t.thesis, []).append(t.return_pct)
            if thesis_counts:
                print(f"\n  BY THESIS:")
                for thesis, rets in sorted(thesis_counts.items(), key=lambda x: x[0] or "zzz"):
                    a = np.array(rets)
                    label = thesis or "none"
                    wr = float(np.sum(a > 0)) / len(a) if len(a) > 0 else 0
                    print(f"    {label:<15}  N={len(a):<6}  WR={wr:.1%}  Mean={float(np.mean(a)):+.3%}")

            # Breakdown by exit reason
            reason_counts: dict[str, int] = {}
            for t in result.trades:
                reason_counts[t.exit_reason] = reason_counts.get(t.exit_reason, 0) + 1
            if reason_counts:
                print(f"\n  BY EXIT REASON:")
                for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                    print(f"    {reason:<25}  {count}")

    else:
        # Grid mode
        grid = build_default_grid()
        print(f"  Grid size: {len(grid)} configs")
        print()

        results = run_comparison_grid(conn, args.start, args.end, grid)

        # Write CSV
        write_grid_csv(results, args.output)

        # Print summary
        print_summary_table(results, top_n=args.top)

        # Also save detailed JSON for top configs
        json_path = Path(args.output).with_suffix(".json")
        top_results = results[:min(50, len(results))]
        json_data = []
        for r in top_results:
            entry = {
                "config": asdict(r.config),
                "n_trades": r.n_trades,
                "win_rate": r.win_rate,
                "avg_return": r.avg_return,
                "median_return": r.median_return,
                "sharpe": r.sharpe,
                "max_drawdown": r.max_drawdown,
                "profit_factor": r.profit_factor,
                "total_pnl": r.total_pnl,
                "annualized_return": r.annualized_return,
            }
            json_data.append(entry)
        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=2, default=str)
        logger.info("Top %d configs saved to %s", len(json_data), json_path)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
