#!/usr/bin/env python3
"""
Exit Timing Research: Calendar vs Trading Days, Optimal Hold, Intraday Timing, Indicator Exits.

Analyzes insider buy events to determine:
  1. Calendar days vs trading days hold periods
  2. Optimal hold period per strategy thesis (7d/14d/30d/60d/90d)
  3. Intraday exit timing (open vs close vs VWAP proxy)
  4. Indicator-based exits (RSI>70, ATR-based) vs pure time exit

Usage:
    python3 research/exit_timing_study.py
    python3 research/exit_timing_study.py --study 1       # run specific study
    python3 research/exit_timing_study.py --start 2021-01-01 --end 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection
from framework.data.calendar import MarketCalendar
from framework.signals.indicators import rsi, atr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

CAL = MarketCalendar()

# ── Strategy thesis filters ──────────────────────────────────────────────────

STRATEGY_FILTERS = {
    "reversal_dip": """
        t.trans_code = 'P'
        AND t.is_rare_reversal = 1
        AND t.consecutive_sells_before >= 10
        AND t.dip_3mo <= -0.25
        AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        AND (t.is_recurring = 0 OR t.is_recurring IS NULL)
        AND (t.is_tax_sale = 0 OR t.is_tax_sale IS NULL)
        AND (t.cohen_routine = 0 OR t.cohen_routine IS NULL)
        AND (t.is_10b5_1 = 0 OR t.is_10b5_1 IS NULL)
    """,
    "quality_momentum": """
        t.trans_code = 'P'
        AND t.pit_grade IN ('A+', 'A')
        AND t.above_sma50 = 1
        AND t.above_sma200 = 1
        AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        AND (t.is_recurring = 0 OR t.is_recurring IS NULL)
        AND (t.is_tax_sale = 0 OR t.is_tax_sale IS NULL)
    """,
    "tenb51_surprise": """
        t.trans_code = 'P'
        AND t.is_10b5_1 = 0
        AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        AND (t.is_recurring = 0 OR t.is_recurring IS NULL)
        AND (t.is_tax_sale = 0 OR t.is_tax_sale IS NULL)
        AND t.filing_date >= '2023-01-01'
    """,
    "all_buys": """
        t.trans_code = 'P'
        AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
    """,
}


@dataclass
class Stats:
    n: int = 0
    mean_ret: float = 0.0
    median_ret: float = 0.0
    win_rate: float = 0.0
    sharpe: float = 0.0
    mean_abnormal: float = 0.0
    returns: list = field(default_factory=list, repr=False)

    @classmethod
    def from_returns(cls, rets: list[float], abnormals: list[float] | None = None) -> "Stats":
        if not rets:
            return cls()
        arr = np.array(rets)
        abn = np.array(abnormals) if abnormals else np.zeros_like(arr)
        return cls(
            n=len(arr),
            mean_ret=float(np.mean(arr)),
            median_ret=float(np.median(arr)),
            win_rate=float(np.mean(arr > 0)),
            sharpe=float(np.mean(arr) / np.std(arr)) if np.std(arr) > 0 else 0.0,
            mean_abnormal=float(np.mean(abn)),
            returns=rets,
        )

    def __str__(self):
        return (
            f"n={self.n:>5d}  mean={self.mean_ret:>+7.2%}  med={self.median_ret:>+7.2%}  "
            f"WR={self.win_rate:>5.1%}  Sharpe={self.sharpe:>5.2f}  alpha={self.mean_abnormal:>+7.2%}"
        )


# ── Price loading ────────────────────────────────────────────────────────────

def load_price_series(conn, ticker: str) -> dict[str, dict]:
    """Load daily OHLCV as {date_str: {open, high, low, close, volume}}."""
    rows = conn.execute(
        "SELECT date, open, high, low, close, volume FROM daily_prices WHERE ticker = ? ORDER BY date",
        (ticker,),
    ).fetchall()
    return {r["date"]: dict(r) for r in rows}


def load_close_map(conn, ticker: str) -> dict[str, float]:
    """Load {date_str: close} for a ticker."""
    rows = conn.execute(
        "SELECT date, close FROM daily_prices WHERE ticker = ? ORDER BY date",
        (ticker,),
    ).fetchall()
    return {r["date"]: r["close"] for r in rows}


# ── Study 1: Calendar vs Trading Days ────────────────────────────────────────

def study_calendar_vs_trading(conn, start: str, end: str):
    """Compare exit at N calendar days vs N trading days."""
    logger.info("=== Study 1: Calendar vs Trading Days ===")

    hold_configs = [
        (21, 15, "21cal/15trd"),  # reversal_dip equiv
        (30, 21, "30cal/21trd"),  # quality_momentum equiv
        (60, 42, "60cal/42trd"),  # tenb51_surprise equiv
    ]

    for strategy, sql_filter in STRATEGY_FILTERS.items():
        if strategy == "all_buys":
            continue
        logger.info("\n--- %s ---", strategy)

        events = conn.execute(f"""
            SELECT t.trade_id, t.ticker, t.filing_date, tr.entry_price
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {sql_filter}
              AND t.filing_date BETWEEN ? AND ?
              AND tr.entry_price IS NOT NULL AND tr.entry_price > 0
        """, (start, end)).fetchall()

        logger.info("Events: %d", len(events))

        for cal_days, trd_days, label in hold_configs:
            cal_rets, trd_rets = [], []

            for ev in events:
                ticker = ev["ticker"]
                entry_date = ev["filing_date"]
                entry_price = ev["entry_price"]

                closes = load_close_map(conn, ticker)
                if not closes:
                    continue

                # Calendar exit: entry + N calendar days, snap to trading day
                cal_exit_date = CAL.add_trading_days(
                    date.fromisoformat(entry_date) + timedelta(days=cal_days), 0
                ).isoformat()
                if cal_exit_date in closes:
                    cal_rets.append((closes[cal_exit_date] - entry_price) / entry_price)

                # Trading day exit: entry + N trading days
                trd_exit_date = CAL.add_trading_days(entry_date, trd_days).isoformat()
                if trd_exit_date in closes:
                    trd_rets.append((closes[trd_exit_date] - entry_price) / entry_price)

            cal_stats = Stats.from_returns(cal_rets)
            trd_stats = Stats.from_returns(trd_rets)
            print(f"  {label:15s}  Calendar: {cal_stats}")
            print(f"  {' ':15s}  Trading:  {trd_stats}")


# ── Study 2: Optimal Hold Period ─────────────────────────────────────────────

def study_optimal_hold(conn, start: str, end: str):
    """Compare 7d/14d/30d/60d/90d hold periods using pre-computed returns."""
    logger.info("\n=== Study 2: Optimal Hold Period ===")

    horizons = [
        (7,   "return_7d",  "abnormal_7d"),
        (14,  "return_14d", "abnormal_14d"),
        (30,  "return_30d", "abnormal_30d"),
        (60,  "return_60d", "abnormal_60d"),
        (90,  "return_90d", "abnormal_90d"),
    ]

    for strategy, sql_filter in STRATEGY_FILTERS.items():
        logger.info("\n--- %s ---", strategy)

        # Build column list for one query
        ret_cols = ", ".join(f"tr.{h[1]}, tr.{h[2]}" for h in horizons)
        rows = conn.execute(f"""
            SELECT {ret_cols}
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {sql_filter}
              AND t.filing_date BETWEEN ? AND ?
        """, (start, end)).fetchall()

        logger.info("Events: %d", len(rows))

        for days, ret_col, abn_col in horizons:
            rets = [r[ret_col] for r in rows if r[ret_col] is not None]
            abns = [r[abn_col] for r in rows if r[ret_col] is not None and r[abn_col] is not None]
            stats = Stats.from_returns(rets, abns)
            best = " ***" if stats.sharpe > 0.5 else ""
            print(f"  {days:>3d}d: {stats}{best}")


# ── Study 3: Intraday Exit Timing ────────────────────────────────────────────

def study_intraday_timing(conn, start: str, end: str):
    """Compare exit at open vs close vs VWAP-proxy on exit day."""
    logger.info("\n=== Study 3: Intraday Exit Timing (Open vs Close) ===")

    hold_days_map = {
        "reversal_dip": 21,
        "quality_momentum": 30,
        "tenb51_surprise": 60,
    }

    for strategy, sql_filter in STRATEGY_FILTERS.items():
        if strategy == "all_buys":
            continue
        hold = hold_days_map.get(strategy, 30)
        logger.info("\n--- %s (hold=%dd) ---", strategy, hold)

        events = conn.execute(f"""
            SELECT t.trade_id, t.ticker, t.filing_date, tr.entry_price
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {sql_filter}
              AND t.filing_date BETWEEN ? AND ?
              AND tr.entry_price IS NOT NULL AND tr.entry_price > 0
        """, (start, end)).fetchall()

        logger.info("Events: %d", len(events))

        open_rets, close_rets, vwap_rets = [], [], []
        open_vs_close = []  # positive = open is better

        for ev in events:
            ticker = ev["ticker"]
            entry_price = ev["entry_price"]

            # Compute exit date (N calendar days, snap to trading day)
            exit_date = CAL.add_trading_days(
                date.fromisoformat(ev["filing_date"]) + timedelta(days=hold), 0
            ).isoformat()

            prices = load_price_series(conn, ticker)
            if exit_date not in prices:
                continue

            bar = prices[exit_date]
            o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
            if not all(x and x > 0 for x in (o, h, l, c)):
                continue

            vwap_proxy = (o + h + l + c) / 4

            open_rets.append((o - entry_price) / entry_price)
            close_rets.append((c - entry_price) / entry_price)
            vwap_rets.append((vwap_proxy - entry_price) / entry_price)
            open_vs_close.append(((o - c) / entry_price) * 100)  # bps difference

        open_stats = Stats.from_returns(open_rets)
        close_stats = Stats.from_returns(close_rets)
        vwap_stats = Stats.from_returns(vwap_rets)

        print(f"  Open:  {open_stats}")
        print(f"  Close: {close_stats}")
        print(f"  VWAP:  {vwap_stats}")
        if open_vs_close:
            avg_diff = np.mean(open_vs_close)
            print(f"  Open vs Close avg diff: {avg_diff:+.2f}% of entry price")
            print(f"  Open better {np.mean(np.array(open_vs_close) > 0):.1%} of the time")


# ── Study 4: Indicator-Based Exits ───────────────────────────────────────────

def study_indicator_exits(conn, start: str, end: str):
    """Test RSI>70 and ATR-based early exits vs pure time exit."""
    logger.info("\n=== Study 4: Indicator-Based Exit Enhancement ===")

    import pandas as pd

    hold_days_map = {
        "reversal_dip": 21,
        "quality_momentum": 30,
        "tenb51_surprise": 60,
    }

    for strategy, sql_filter in STRATEGY_FILTERS.items():
        if strategy == "all_buys":
            continue
        max_hold = hold_days_map.get(strategy, 30)
        logger.info("\n--- %s (max_hold=%dd) ---", strategy, max_hold)

        events = conn.execute(f"""
            SELECT t.trade_id, t.ticker, t.filing_date, tr.entry_price
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {sql_filter}
              AND t.filing_date BETWEEN ? AND ?
              AND tr.entry_price IS NOT NULL AND tr.entry_price > 0
        """, (start, end)).fetchall()

        logger.info("Events: %d", len(events))

        time_rets, rsi_rets, atr_rets = [], [], []
        rsi_early_exits = 0
        atr_early_exits = 0
        total_valid = 0

        # Cache price series per ticker
        _cache: dict[str, dict] = {}

        for ev in events:
            ticker = ev["ticker"]
            entry_price = ev["entry_price"]
            entry_date_str = ev["filing_date"]
            entry_dt = date.fromisoformat(entry_date_str)

            # Load price series (cached)
            if ticker not in _cache:
                _cache[ticker] = load_price_series(conn, ticker)
            prices = _cache[ticker]

            if not prices:
                continue

            # Get sorted dates for this ticker
            sorted_dates = sorted(prices.keys())
            try:
                start_idx = next(i for i, d in enumerate(sorted_dates) if d >= entry_date_str)
            except StopIteration:
                continue

            # Need at least 20 bars before entry for RSI warm-up + hold period
            if start_idx < 20:
                continue

            # Build hold-period window
            exit_date = CAL.add_trading_days(entry_dt + timedelta(days=max_hold), 0).isoformat()

            # Extract close series for RSI (20 bars before entry + hold period)
            window_start = max(0, start_idx - 20)
            close_series = []
            date_series = []
            for i in range(window_start, len(sorted_dates)):
                d = sorted_dates[i]
                if d > exit_date:
                    break
                bar = prices[d]
                if bar["close"]:
                    close_series.append(bar["close"])
                    date_series.append(d)

            if len(close_series) < 25:
                continue

            # Compute RSI-14
            close_arr = pd.Series(close_series)
            rsi_arr = rsi(close_arr, period=14)

            # Compute ATR-14
            highs = pd.Series([prices[d]["high"] or prices[d]["close"] for d in date_series])
            lows = pd.Series([prices[d]["low"] or prices[d]["close"] for d in date_series])
            df_atr = pd.DataFrame({"high": highs, "low": lows, "close": close_arr})
            atr_arr = atr(df_atr, period=14)

            # Find entry index in the window
            try:
                entry_idx_in_window = date_series.index(entry_date_str)
            except ValueError:
                # Entry date not in series, find nearest
                entry_idx_in_window = next(
                    (i for i, d in enumerate(date_series) if d >= entry_date_str), None
                )
                if entry_idx_in_window is None:
                    continue

            entry_atr = atr_arr.iloc[entry_idx_in_window] if entry_idx_in_window < len(atr_arr) else None

            # Time exit: exit at hold day
            time_exit_price = None
            if exit_date in prices and prices[exit_date]["close"]:
                time_exit_price = prices[exit_date]["close"]

            if time_exit_price is None:
                continue

            total_valid += 1
            time_rets.append((time_exit_price - entry_price) / entry_price)

            # RSI exit: first day RSI > 70 during hold period
            rsi_exit_price = None
            for i in range(entry_idx_in_window + 1, len(date_series)):
                d = date_series[i]
                if d > exit_date:
                    break
                if i < len(rsi_arr) and rsi_arr.iloc[i] > 70:
                    rsi_exit_price = close_series[i]
                    rsi_early_exits += 1
                    break
            if rsi_exit_price is None:
                rsi_exit_price = time_exit_price  # Fallback to time exit
            rsi_rets.append((rsi_exit_price - entry_price) / entry_price)

            # ATR exit: first day price > entry + 2*ATR
            atr_exit_price = None
            if entry_atr and entry_atr > 0:
                target = entry_price + 2 * entry_atr
                for i in range(entry_idx_in_window + 1, len(date_series)):
                    d = date_series[i]
                    if d > exit_date:
                        break
                    if close_series[i] >= target:
                        atr_exit_price = close_series[i]
                        atr_early_exits += 1
                        break
            if atr_exit_price is None:
                atr_exit_price = time_exit_price
            atr_rets.append((atr_exit_price - entry_price) / entry_price)

        time_stats = Stats.from_returns(time_rets)
        rsi_stats = Stats.from_returns(rsi_rets)
        atr_stats = Stats.from_returns(atr_rets)

        print(f"  Time only: {time_stats}")
        print(f"  RSI > 70:  {rsi_stats}  (triggered early {rsi_early_exits}/{total_valid} = {rsi_early_exits/total_valid:.1%})" if total_valid else "  RSI > 70:  no data")
        print(f"  ATR 2x:    {atr_stats}  (triggered early {atr_early_exits}/{total_valid} = {atr_early_exits/total_valid:.1%})" if total_valid else "  ATR 2x:    no data")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Exit Timing Research")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2026-03-31")
    parser.add_argument("--study", type=int, help="Run specific study (1-4)")
    args = parser.parse_args()

    conn = get_connection(readonly=True)

    studies = {
        1: ("Calendar vs Trading Days", study_calendar_vs_trading),
        2: ("Optimal Hold Period", study_optimal_hold),
        3: ("Intraday Exit Timing", study_intraday_timing),
        4: ("Indicator-Based Exits", study_indicator_exits),
    }

    if args.study:
        name, fn = studies[args.study]
        fn(conn, args.start, args.end)
    else:
        for num, (name, fn) in studies.items():
            fn(conn, args.start, args.end)

    conn.close()


if __name__ == "__main__":
    main()
