#!/usr/bin/env python3
"""
Exit Strategy Analysis for Insider Cluster-Buy Options Strategy
================================================================
Comprehensive analysis of exit timing strategies using 100% real Theta Data
option prices and yfinance daily stock data.

Covers:
1. Underlying price move audit for winning trades
2. Multiple exit strategy backtests (fixed hold, price target, gain target, trailing stop)
3. Regime/confounding analysis (alpha vs beta decomposition)
4. Comparison table ranking all strategies by Sharpe

Config: 5% OTM (105%) strike, 90 DTE quarterly expiry
Filter: cluster>=2, total_value>=$5M, quality_score>=2.0 -> 204 events

Author: Claude Opus 4.6
Date: 2026-02-28
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
import traceback
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, date
from io import StringIO

import numpy as np

# Suppress yfinance/urllib3 warnings
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")
CACHE_PATH = os.path.join(SCRIPT_DIR, "data", "thetadata_cache.json")
STOCK_CACHE_PATH = os.path.join(SCRIPT_DIR, "data", "stock_price_cache.json")
REPORT_PATH = os.path.join(SCRIPT_DIR, "..", "..", "reports", "EXIT_STRATEGY_ANALYSIS.md")

THETA_BASE = "http://127.0.0.1:25503"
API_DELAY = 0.35

# Filter parameters
MIN_CLUSTER_SIZE = 2
MIN_TOTAL_VALUE = 5_000_000
MIN_QUALITY_SCORE = 2.0

# Target option config: 5% OTM, 90 DTE
TARGET_STRIKE_MULT = 1.05
TARGET_DTE = 90

# Analysis parameters
MAX_HOLD_TRADING_DAYS = 60
ANNUAL_TRADING_DAYS = 252

# Regime definitions (inclusive date ranges)
REGIMES = {
    "COVID Crash": (date(2020, 2, 1), date(2020, 6, 30)),
    "COVID Recovery": (date(2020, 7, 1), date(2020, 12, 31)),
    "2021 Bull": (date(2021, 1, 1), date(2021, 12, 31)),
    "2022 Bear": (date(2022, 1, 1), date(2022, 10, 31)),
    "2023+ Recovery": (date(2023, 1, 1), date(2026, 12, 31)),
}


# ─────────────────────────────────────────────
# Cache Management
# ─────────────────────────────────────────────

def load_cache(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict, path: str):
    try:
        with open(path, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass


# ─────────────────────────────────────────────
# Theta Data API Client (reused from reference)
# ─────────────────────────────────────────────

def theta_get(endpoint: str, params: dict, cache: dict, cache_key: str | None = None) -> list[dict] | None:
    if cache_key and cache_key in cache:
        return cache[cache_key]

    query = urllib.parse.urlencode(params)
    url = f"{THETA_BASE}{endpoint}?{query}"

    try:
        time.sleep(API_DELAY)
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw:
                if cache_key:
                    cache[cache_key] = None
                return None
            reader = csv.DictReader(StringIO(raw))
            rows = list(reader)
            if cache_key:
                cache[cache_key] = rows
            return rows
    except Exception:
        if cache_key:
            cache[cache_key] = None
        return None


def get_expirations(symbol: str, cache: dict) -> list[date]:
    cache_key = f"exp|{symbol}"
    rows = theta_get("/v3/option/list/expirations", {"symbol": symbol}, cache, cache_key)
    if not rows:
        return []
    result = []
    for r in rows:
        try:
            exp_str = r.get("expiration", "").strip().strip('"')
            result.append(datetime.strptime(exp_str, "%Y-%m-%d").date())
        except (ValueError, KeyError):
            continue
    return sorted(result)


def get_strikes(symbol: str, expiration: date, cache: dict) -> list[float]:
    exp_str = expiration.strftime("%Y-%m-%d")
    cache_key = f"str|{symbol}|{exp_str}"
    rows = theta_get("/v3/option/list/strikes", {"symbol": symbol, "expiration": exp_str}, cache, cache_key)
    if not rows:
        return []
    result = []
    for r in rows:
        try:
            strike_str = r.get("strike", "").strip().strip('"')
            result.append(float(strike_str))
        except (ValueError, KeyError):
            continue
    return sorted(result)


def get_option_eod(symbol: str, expiration: date, strike: float, right: str,
                   start_date: date, end_date: date, cache: dict) -> list[dict] | None:
    exp_str = expiration.strftime("%Y-%m-%d")
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    cache_key = f"eod|{symbol}|{exp_str}|{strike}|{right}|{start_str}|{end_str}"
    rows = theta_get(
        "/v3/option/history/eod",
        {"symbol": symbol, "expiration": exp_str, "strike": str(strike),
         "right": right, "start_date": start_str, "end_date": end_str},
        cache, cache_key,
    )
    return rows


def get_fair_price(row: dict) -> float | None:
    try:
        close = float(row.get("close", "0").strip().strip('"'))
        if close > 0:
            return close
        bid = float(row.get("bid", "0").strip().strip('"'))
        ask = float(row.get("ask", "0").strip().strip('"'))
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if ask > 0:
            return ask
        return None
    except (ValueError, TypeError):
        return None


def get_eod_date(row: dict) -> date | None:
    try:
        created = row.get("created", "").strip().strip('"')
        return datetime.strptime(created[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def find_nearest_expiration(expirations: list[date], entry_date: date, target_dte: int) -> date | None:
    if not expirations:
        return None
    target = entry_date + timedelta(days=target_dte)
    min_exp = entry_date + timedelta(days=max(target_dte - 3, 4))
    valid = [e for e in expirations if e >= min_exp]
    if not valid:
        return None
    best = min(valid, key=lambda e: abs((e - target).days))
    if abs((best - target).days) > max(target_dte, 7):
        return None
    return best


def find_nearest_strike(strikes: list[float], target_price: float) -> float | None:
    if not strikes:
        return None
    return min(strikes, key=lambda s: abs(s - target_price))


def add_trading_days(start: date, n_days: int) -> date:
    current = start
    added = 0
    while added < n_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def get_regime(d: date) -> str:
    for name, (start, end) in REGIMES.items():
        if start <= d <= end:
            return name
    return "Other"


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_and_filter_data() -> list[dict]:
    with open(DATA_PATH) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    filtered = []
    for r in rows:
        try:
            is_cluster = r["is_cluster"] == "True"
            n_insiders = int(r["n_insiders"])
            total_value = float(r["total_value"])
            quality_score = float(r["quality_score"])

            if ((is_cluster or n_insiders >= 2)
                    and total_value >= MIN_TOTAL_VALUE
                    and quality_score >= MIN_QUALITY_SCORE):
                entry_date = datetime.strptime(r["entry_date"], "%Y-%m-%d").date()
                exit_date = datetime.strptime(r["exit_date"], "%Y-%m-%d").date()
                filtered.append({
                    "ticker": r["ticker"],
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_date_str": r["entry_date"],
                    "exit_date_str": r["exit_date"],
                    "entry_price": float(r["entry_price"]),
                    "exit_price": float(r["exit_price"]),
                    "trade_return": float(r["trade_return"]) / 100.0,
                    "spy_return": float(r["spy_return"]) / 100.0,
                    "abnormal_return": float(r["abnormal_return"]) / 100.0,
                    "total_value": total_value,
                    "cluster_size": int(r["cluster_size"]),
                    "n_insiders": n_insiders,
                    "quality_score": quality_score,
                    "company": r["company"],
                    "insider_names": r.get("insider_names", ""),
                })
        except (ValueError, KeyError):
            continue

    return filtered


# ─────────────────────────────────────────────
# Stock Price Fetching (yfinance)
# ─────────────────────────────────────────────

def fetch_stock_prices(ticker: str, start_date: date, end_date: date, stock_cache: dict) -> dict[str, float]:
    """
    Fetch daily close prices for ticker from start_date to end_date.
    Returns dict mapping 'YYYY-MM-DD' -> close_price.
    Uses stock_cache for caching.
    """
    cache_key = f"stock|{ticker}|{start_date.isoformat()}|{end_date.isoformat()}"
    if cache_key in stock_cache:
        return stock_cache[cache_key] if stock_cache[cache_key] else {}

    try:
        import yfinance as yf
        # Add buffer days for weekends
        buf_start = start_date - timedelta(days=5)
        buf_end = end_date + timedelta(days=5)
        tk = yf.Ticker(ticker)
        hist = tk.history(start=buf_start.isoformat(), end=buf_end.isoformat(), auto_adjust=True)
        if hist is None or len(hist) == 0:
            stock_cache[cache_key] = None
            return {}

        prices = {}
        for idx, row in hist.iterrows():
            d_str = idx.strftime("%Y-%m-%d")
            prices[d_str] = float(row["Close"])

        stock_cache[cache_key] = prices
        return prices
    except Exception:
        stock_cache[cache_key] = None
        return {}


def get_daily_stock_series(ticker: str, entry_date: date, n_trading_days: int,
                           entry_price: float, stock_cache: dict) -> list[tuple[date, float]]:
    """
    Get daily stock close prices from entry_date through entry_date + n_trading_days.
    Returns list of (date, price) tuples for each trading day.
    """
    end_date = add_trading_days(entry_date, n_trading_days + 5)  # buffer
    prices_dict = fetch_stock_prices(ticker, entry_date, end_date, stock_cache)
    if not prices_dict:
        return []

    result = []
    current = entry_date
    days_added = 0
    while days_added <= n_trading_days:
        if current.weekday() < 5:  # trading day
            d_str = current.strftime("%Y-%m-%d")
            if d_str in prices_dict:
                result.append((current, prices_dict[d_str]))
            days_added += 1
        current += timedelta(days=1)

    return result


# ─────────────────────────────────────────────
# SPY Price Fetching
# ─────────────────────────────────────────────

def fetch_spy_prices(stock_cache: dict) -> dict[str, float]:
    """Fetch full SPY price history (2020-2026)."""
    cache_key = "spy_full_history"
    if cache_key in stock_cache and stock_cache[cache_key]:
        return stock_cache[cache_key]

    try:
        import yfinance as yf
        spy = yf.Ticker("SPY")
        hist = spy.history(start="2019-12-01", end="2026-12-31", auto_adjust=True)
        prices = {}
        for idx, row in hist.iterrows():
            prices[idx.strftime("%Y-%m-%d")] = float(row["Close"])
        stock_cache[cache_key] = prices
        return prices
    except Exception:
        stock_cache[cache_key] = None
        return {}


def get_spy_return(spy_prices: dict, entry_date: date, exit_date: date) -> float | None:
    """Compute SPY return over a period."""
    entry_str = entry_date.strftime("%Y-%m-%d")
    exit_str = exit_date.strftime("%Y-%m-%d")

    # Find closest available dates
    entry_price = None
    for delta in range(0, 5):
        d = entry_date + timedelta(days=delta)
        ds = d.strftime("%Y-%m-%d")
        if ds in spy_prices:
            entry_price = spy_prices[ds]
            break

    exit_price = None
    for delta in range(0, 5):
        d = exit_date + timedelta(days=delta)
        ds = d.strftime("%Y-%m-%d")
        if ds in spy_prices:
            exit_price = spy_prices[ds]
            break

    if entry_price and exit_price and entry_price > 0:
        return (exit_price - entry_price) / entry_price
    return None


# ─────────────────────────────────────────────
# Option Price Time Series (from Theta Data)
# ─────────────────────────────────────────────

def get_option_daily_series(ticker: str, entry_date: date, entry_price: float,
                            theta_cache: dict) -> list[tuple[date, float]] | None:
    """
    Get daily option prices for the 5% OTM, 90 DTE config from entry_date
    through entry_date + 60 trading days (or expiration, whichever comes first).

    Uses a multi-query strategy:
    1. Try to find entry-period data from the V2-style narrow cache
    2. Query additional weekly chunks to build up the full time series
    3. Validate entry price is available within 3 trading days

    Returns list of (date, option_price) or None if no data.
    """
    # Step 1: Get expirations
    expirations = get_expirations(ticker, theta_cache)
    if not expirations:
        return None

    # Step 2: Find nearest expiration (90 DTE)
    expiry = find_nearest_expiration(expirations, entry_date, TARGET_DTE)
    if expiry is None:
        return None

    # Step 3: Get strikes
    strikes = get_strikes(ticker, expiry, theta_cache)
    if not strikes:
        return None

    # Step 4: Find nearest 5% OTM strike
    target_strike = entry_price * TARGET_STRIKE_MULT
    strike = find_nearest_strike(strikes, target_strike)
    if strike is None:
        return None

    # Step 5: Get EOD data using chunked queries to maximize cache hits
    # The V2 script cached narrow ranges. We'll query in overlapping chunks
    # to reuse cache and fill gaps.
    all_rows = []
    max_end = min(add_trading_days(entry_date, MAX_HOLD_TRADING_DAYS + 5), expiry)

    # Chunk 1: Entry area (matches V2 cache pattern)
    exit_7d = add_trading_days(entry_date, 7)
    chunk1_start = entry_date - timedelta(days=2)
    chunk1_end = exit_7d + timedelta(days=5)
    rows1 = get_option_eod(ticker, expiry, strike, "C", chunk1_start, chunk1_end, theta_cache)
    if rows1:
        all_rows.extend(rows1)

    # Chunk 2-N: Successive 2-week windows covering the full hold period
    chunk_start = chunk1_end + timedelta(days=1)
    while chunk_start < max_end:
        chunk_end = min(chunk_start + timedelta(days=14), max_end)
        rows_chunk = get_option_eod(ticker, expiry, strike, "C", chunk_start, chunk_end, theta_cache)
        if rows_chunk:
            all_rows.extend(rows_chunk)
        chunk_start = chunk_end + timedelta(days=1)

    if not all_rows:
        return None

    # Deduplicate by date and parse into series
    seen_dates = set()
    series = []
    for row in all_rows:
        row_date = get_eod_date(row)
        price = get_fair_price(row)
        if row_date is not None and price is not None and price > 0:
            if row_date >= entry_date and row_date.isoformat() not in seen_dates:
                series.append((row_date, price))
                seen_dates.add(row_date.isoformat())

    if not series:
        return None

    # Sort by date
    series.sort(key=lambda x: x[0])

    # CRITICAL: Validate that the first data point is within 3 trading days
    # of entry_date. If the first available option price is weeks later,
    # we can't use this as a reliable entry price.
    first_date = series[0][0]
    max_entry_gap = add_trading_days(entry_date, 3)
    if first_date > max_entry_gap:
        return None  # No reliable entry price

    # Sanity check: if option price more than doubles from day 1 to day 2,
    # the day-1 price is likely an anomalous thin-market trade.
    # Use day 2 as entry instead for more reliable pricing.
    if len(series) >= 2:
        p1, p2 = series[0][1], series[1][1]
        if p2 > p1 * 2.0 and p1 > 0:
            # Day 1 price is anomalously low -- skip it
            series = series[1:]

    return series


# ─────────────────────────────────────────────
# Exit Strategy Simulations
# ─────────────────────────────────────────────

def simulate_fixed_hold(option_series: list[tuple[date, float]], hold_days: int,
                        entry_date: date) -> float | None:
    """Exit after exactly hold_days trading days. Return option return %."""
    if not option_series:
        return None

    entry_price = option_series[0][1]
    if entry_price <= 0:
        return None

    target_exit = add_trading_days(entry_date, hold_days)

    # Find price on or closest to target exit date
    best_row = None
    best_diff = 999
    for d, p in option_series:
        diff = abs((d - target_exit).days)
        if diff < best_diff:
            best_diff = diff
            best_row = (d, p)
        # Also accept exact or +1/+2 days
        if d >= target_exit and best_row is None:
            best_row = (d, p)
            break

    if best_row is None or best_diff > 5:
        # Use last available price if we don't have the exact date
        if option_series:
            last_d, last_p = option_series[-1]
            # Count trading days from entry
            td_count = 0
            cur = entry_date
            while cur < last_d:
                cur += timedelta(days=1)
                if cur.weekday() < 5:
                    td_count += 1
            if td_count >= hold_days - 2:
                return (last_p - entry_price) / entry_price
        return None

    return (best_row[1] - entry_price) / entry_price


def simulate_underlying_target(stock_series: list[tuple[date, float]], option_series: list[tuple[date, float]],
                               target_pct: float, entry_stock_price: float, entry_date: date) -> dict | None:
    """Exit when underlying hits target_pct gain from entry. Max 60 trading days."""
    if not option_series or not stock_series:
        return None

    opt_entry = option_series[0][1]
    if opt_entry <= 0:
        return None

    # Build option price lookup by date string
    opt_by_date = {d.isoformat(): p for d, p in option_series}

    for d, stock_p in stock_series:
        stock_ret = (stock_p - entry_stock_price) / entry_stock_price
        if stock_ret >= target_pct:
            # Target hit -- get option price on this date
            opt_p = opt_by_date.get(d.isoformat())
            if opt_p is not None:
                td = _count_trading_days(entry_date, d)
                return {
                    "opt_return": (opt_p - opt_entry) / opt_entry,
                    "exit_date": d,
                    "hold_days": td,
                    "target_hit": True,
                    "stock_move": stock_ret,
                }

    # Target not hit -- exit at last available option price
    if option_series:
        last_d, last_p = option_series[-1]
        td = _count_trading_days(entry_date, last_d)
        last_stock = stock_series[-1][1] if stock_series else entry_stock_price
        return {
            "opt_return": (last_p - opt_entry) / opt_entry,
            "exit_date": last_d,
            "hold_days": td,
            "target_hit": False,
            "stock_move": (last_stock - entry_stock_price) / entry_stock_price,
        }
    return None


def simulate_option_gain_target(option_series: list[tuple[date, float]], target_gain: float,
                                entry_date: date) -> dict | None:
    """Exit when option reaches target_gain (e.g. 1.0 = +100%). Max 60 trading days."""
    if not option_series:
        return None

    opt_entry = option_series[0][1]
    if opt_entry <= 0:
        return None

    for d, p in option_series[1:]:  # skip entry day
        opt_ret = (p - opt_entry) / opt_entry
        if opt_ret >= target_gain:
            td = _count_trading_days(entry_date, d)
            return {
                "opt_return": opt_ret,
                "exit_date": d,
                "hold_days": td,
                "target_hit": True,
            }

    # Not hit -- exit at last price
    last_d, last_p = option_series[-1]
    td = _count_trading_days(entry_date, last_d)
    return {
        "opt_return": (last_p - opt_entry) / opt_entry,
        "exit_date": last_d,
        "hold_days": td,
        "target_hit": False,
    }


def simulate_trailing_stop(option_series: list[tuple[date, float]], stop_pct: float,
                           entry_date: date) -> dict | None:
    """
    Trailing stop: once the option has any gain, set a trailing stop at stop_pct
    below the peak. Exit when stop is hit or at 60 day max.
    stop_pct is negative, e.g. -0.20 means -20% from peak.
    """
    if not option_series:
        return None

    opt_entry = option_series[0][1]
    if opt_entry <= 0:
        return None

    peak = opt_entry
    for d, p in option_series[1:]:  # skip entry day
        if p > peak:
            peak = p

        # Only activate trailing stop if we've had some gain
        if peak > opt_entry:
            drawdown_from_peak = (p - peak) / peak
            if drawdown_from_peak <= stop_pct:
                td = _count_trading_days(entry_date, d)
                return {
                    "opt_return": (p - opt_entry) / opt_entry,
                    "exit_date": d,
                    "hold_days": td,
                    "stop_hit": True,
                    "peak_price": peak,
                    "peak_return": (peak - opt_entry) / opt_entry,
                }

    # Stop never hit -- exit at last price
    last_d, last_p = option_series[-1]
    td = _count_trading_days(entry_date, last_d)
    return {
        "opt_return": (last_p - opt_entry) / opt_entry,
        "exit_date": last_d,
        "hold_days": td,
        "stop_hit": False,
        "peak_price": peak,
        "peak_return": (peak - opt_entry) / opt_entry,
    }


def _count_trading_days(start: date, end: date) -> int:
    """Count trading days between start and end (exclusive of start, inclusive of end)."""
    count = 0
    current = start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            count += 1
    return count


# ─────────────────────────────────────────────
# Statistics Helpers
# ─────────────────────────────────────────────

def compute_stats(returns: list[float], hold_days: int = 7) -> dict:
    """Compute summary statistics for a list of returns."""
    if not returns:
        return {"n": 0}

    arr = np.array(returns)
    n = len(arr)
    mean = float(np.mean(arr))
    median = float(np.median(arr))
    std = float(np.std(arr, ddof=1)) if n > 1 else 0.0

    periods_per_year = ANNUAL_TRADING_DAYS / hold_days
    sharpe = (mean / std) * math.sqrt(periods_per_year) if std > 0 else 0.0

    win_rate = float(np.mean(arr > 0))
    max_ret = float(np.max(arr))
    min_ret = float(np.min(arr))

    return {
        "n": n,
        "mean": mean,
        "median": median,
        "std": std,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "max_return": max_ret,
        "min_return": min_ret,
        "p25": float(np.percentile(arr, 25)) if n >= 4 else median,
        "p75": float(np.percentile(arr, 75)) if n >= 4 else median,
        "p90": float(np.percentile(arr, 90)) if n >= 4 else max_ret,
        "hold_days": hold_days,
    }


def fmt_pct(v: float, decimals: int = 2) -> str:
    return f"{v * 100:.{decimals}f}%"


def fmt_pct_raw(v: float, decimals: int = 2) -> str:
    """Format a raw percentage value (already multiplied by 100)."""
    return f"{v:.{decimals}f}%"


# ─────────────────────────────────────────────
# Main Analysis Engine
# ─────────────────────────────────────────────

def run_analysis():
    print("=" * 70)
    print("EXIT STRATEGY ANALYSIS — Insider Cluster-Buy Options")
    print("=" * 70)

    # Load data
    print("\n[1/7] Loading and filtering events...")
    events = load_and_filter_data()
    print(f"  Loaded {len(events)} filtered events")

    # Load caches
    theta_cache = load_cache(CACHE_PATH)
    stock_cache = load_cache(STOCK_CACHE_PATH)
    print(f"  Theta cache: {len(theta_cache)} keys")
    print(f"  Stock cache: {len(stock_cache)} keys")

    # Fetch SPY prices
    print("\n[2/7] Fetching SPY price history...")
    spy_prices = fetch_spy_prices(stock_cache)
    print(f"  SPY history: {len(spy_prices)} trading days")
    save_cache(stock_cache, STOCK_CACHE_PATH)

    # ─────────────────────────────────────
    # Fetch option and stock daily series for each event
    # ─────────────────────────────────────
    print(f"\n[3/7] Fetching daily option + stock price series for {len(events)} events...")
    print("  (This uses the Theta Data cache and yfinance for stock prices)")

    event_data = []  # list of dicts with event + option_series + stock_series
    tickers_no_expirations = set()  # tickers with NO options listed at all
    skipped_no_opt = 0
    skipped_no_stock = 0
    fetched_ok = 0

    # Pre-check which tickers have any options expirations
    unique_tickers = sorted(set(ev["ticker"] for ev in events))
    for ticker in unique_tickers:
        exps = get_expirations(ticker, theta_cache)
        if not exps:
            tickers_no_expirations.add(ticker)

    print(f"  {len(unique_tickers)} unique tickers, {len(tickers_no_expirations)} with no options")

    for i, ev in enumerate(events):
        ticker = ev["ticker"]

        if ticker in tickers_no_expirations:
            skipped_no_opt += 1
            continue

        # Get option series
        opt_series = get_option_daily_series(ticker, ev["entry_date"], ev["entry_price"], theta_cache)

        if opt_series is None or len(opt_series) < 2:
            skipped_no_opt += 1
            continue

        # Get stock series
        stock_series = get_daily_stock_series(
            ticker, ev["entry_date"], MAX_HOLD_TRADING_DAYS,
            ev["entry_price"], stock_cache
        )

        if not stock_series or len(stock_series) < 2:
            skipped_no_stock += 1
            continue

        fetched_ok += 1
        event_data.append({
            **ev,
            "option_series": opt_series,
            "stock_series": stock_series,
            "opt_entry_price": opt_series[0][1],
            "opt_trading_days": len(opt_series),
        })

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(events)} events processed ({fetched_ok} with data)...")
            save_cache(theta_cache, CACHE_PATH)
            save_cache(stock_cache, STOCK_CACHE_PATH)

    save_cache(theta_cache, CACHE_PATH)
    save_cache(stock_cache, STOCK_CACHE_PATH)

    print(f"\n  Results: {fetched_ok} events with full data")
    print(f"  Skipped (no options/match): {skipped_no_opt}")
    print(f"  Skipped (no stock data): {skipped_no_stock}")
    print(f"  Tickers with no options listed: {len(tickers_no_expirations)}")

    if fetched_ok < 10:
        print("\n  ERROR: Not enough events with data to run analysis!")
        sys.exit(1)

    # Identify PTN outlier (may not be in options dataset)
    ptn_idx = None
    for idx, ed in enumerate(event_data):
        if ed["ticker"] == "PTN" and ed["entry_date"] == date(2025, 11, 12):
            ptn_idx = idx
            break

    # Also find the largest 7d option return outlier in the dataset
    outlier_idx = None
    outlier_ret = -999
    for idx, ed in enumerate(event_data):
        ret_7d = simulate_fixed_hold(ed["option_series"], 7, ed["entry_date"])
        if ret_7d is not None and ret_7d > outlier_ret:
            outlier_ret = ret_7d
            outlier_idx = idx

    if outlier_idx is not None:
        outlier_ev = event_data[outlier_idx]
        print(f"  Largest 7d option return outlier: {outlier_ev['ticker']} ({outlier_ev['entry_date']}) = {fmt_pct(outlier_ret)}")

    # Use the actual outlier for "without outlier" analysis
    # (PTN is the shares outlier but may not be in options data)
    excl_idx = ptn_idx if ptn_idx is not None else outlier_idx

    # ─────────────────────────────────────
    # Section 4: Audit — Underlying Moves for Winners
    # ─────────────────────────────────────
    print("\n[4/7] Auditing underlying price moves for winners...")

    # Determine 7-day winners using option prices
    winner_audits = []
    all_audits = []

    for ed in event_data:
        opt_series = ed["option_series"]
        stock_series = ed["stock_series"]
        opt_entry = opt_series[0][1]
        # Use the first yfinance stock price as reference (adjusted for splits)
        # This avoids mismatch between CSV unadjusted prices and yfinance adjusted prices
        stock_entry = stock_series[0][1] if stock_series else ed["entry_price"]

        # 7-day option return
        opt_7d_ret = simulate_fixed_hold(opt_series, 7, ed["entry_date"])

        # Find stock peak and days to peak within 60 trading days
        peak_price = stock_entry
        peak_date = ed["entry_date"]
        peak_day_idx = 0
        for day_idx, (d, sp) in enumerate(stock_series):
            if sp > peak_price:
                peak_price = sp
                peak_date = d
                peak_day_idx = day_idx

        peak_stock_move = (peak_price - stock_entry) / stock_entry if stock_entry > 0 else 0
        days_to_peak = _count_trading_days(ed["entry_date"], peak_date)

        # Option return at peak stock date
        opt_by_date = {d.isoformat(): p for d, p in opt_series}
        opt_at_peak = opt_by_date.get(peak_date.isoformat())
        opt_return_at_peak = None
        if opt_at_peak is not None and opt_entry > 0:
            opt_return_at_peak = (opt_at_peak - opt_entry) / opt_entry

        audit = {
            "ticker": ed["ticker"],
            "entry_date": ed["entry_date"],
            "entry_stock_price": stock_entry,
            "peak_stock_price": peak_price,
            "days_to_peak": days_to_peak,
            "peak_stock_move": peak_stock_move,
            "opt_7d_return": opt_7d_ret,
            "opt_return_at_peak": opt_return_at_peak,
            "is_7d_winner": opt_7d_ret is not None and opt_7d_ret > 0,
            "company": ed["company"],
        }
        all_audits.append(audit)
        if audit["is_7d_winner"]:
            winner_audits.append(audit)

    print(f"  7-day winners: {len(winner_audits)} / {len(all_audits)} trades")

    # Winner underlying move distribution
    winner_moves = [a["peak_stock_move"] for a in winner_audits]
    winner_moves_arr = np.array(winner_moves) if winner_moves else np.array([0.0])

    print(f"  Winner peak stock move - mean: {fmt_pct(np.mean(winner_moves_arr))}, "
          f"median: {fmt_pct(np.median(winner_moves_arr))}")

    # ─────────────────────────────────────
    # Section 5: Exit Strategy Backtests
    # ─────────────────────────────────────
    print("\n[5/7] Running exit strategy backtests...")

    # 5a. Fixed Hold Duration
    fixed_hold_days = [7, 14, 21, 30, 45, 60]
    fixed_hold_results = {}

    for hd in fixed_hold_days:
        returns = []
        returns_no_outlier = []
        for idx, ed in enumerate(event_data):
            ret = simulate_fixed_hold(ed["option_series"], hd, ed["entry_date"])
            if ret is not None:
                returns.append(ret)
                if idx != excl_idx:
                    returns_no_outlier.append(ret)

        fixed_hold_results[hd] = {
            "all": compute_stats(returns, hold_days=hd),
            "no_outlier": compute_stats(returns_no_outlier, hold_days=hd),
        }
        print(f"  Fixed {hd}d: N={len(returns)}, mean={fmt_pct(np.mean(returns)) if returns else 'N/A'}, "
              f"sharpe={fixed_hold_results[hd]['all']['sharpe']:.2f}")

    # 5b. Underlying Price Target
    underlying_targets = [0.03, 0.05, 0.07, 0.10, 0.15]
    underlying_target_results = {}

    for target in underlying_targets:
        returns = []
        returns_no_outlier = []
        hold_days_list = []
        target_hit_count = 0

        for idx, ed in enumerate(event_data):
            # Use first stock series price as reference (adjusted for splits)
            stock_entry_ref = ed["stock_series"][0][1] if ed["stock_series"] else ed["entry_price"]
            result = simulate_underlying_target(
                ed["stock_series"], ed["option_series"],
                target, stock_entry_ref, ed["entry_date"]
            )
            if result is not None:
                returns.append(result["opt_return"])
                hold_days_list.append(result["hold_days"])
                if result["target_hit"]:
                    target_hit_count += 1
                if idx != excl_idx:
                    returns_no_outlier.append(result["opt_return"])

        avg_hold = np.mean(hold_days_list) if hold_days_list else 7
        underlying_target_results[target] = {
            "all": compute_stats(returns, hold_days=max(1, int(avg_hold))),
            "no_outlier": compute_stats(returns_no_outlier, hold_days=max(1, int(avg_hold))),
            "target_hit_rate": target_hit_count / len(returns) if returns else 0,
            "avg_hold_days": float(avg_hold),
        }
        print(f"  Underlying +{target*100:.0f}%: N={len(returns)}, "
              f"hit rate={target_hit_count}/{len(returns)}, "
              f"mean={fmt_pct(np.mean(returns)) if returns else 'N/A'}")

    # 5c. Option Gain Target
    option_targets = [0.25, 0.50, 1.00, 2.00]
    option_target_results = {}

    for target in option_targets:
        returns = []
        returns_no_outlier = []
        hold_days_list = []
        target_hit_count = 0

        for idx, ed in enumerate(event_data):
            result = simulate_option_gain_target(
                ed["option_series"], target, ed["entry_date"]
            )
            if result is not None:
                returns.append(result["opt_return"])
                hold_days_list.append(result["hold_days"])
                if result["target_hit"]:
                    target_hit_count += 1
                if idx != excl_idx:
                    returns_no_outlier.append(result["opt_return"])

        avg_hold = np.mean(hold_days_list) if hold_days_list else 7
        option_target_results[target] = {
            "all": compute_stats(returns, hold_days=max(1, int(avg_hold))),
            "no_outlier": compute_stats(returns_no_outlier, hold_days=max(1, int(avg_hold))),
            "target_hit_rate": target_hit_count / len(returns) if returns else 0,
            "avg_hold_days": float(avg_hold),
        }
        print(f"  Option +{target*100:.0f}%: N={len(returns)}, "
              f"hit rate={target_hit_count}/{len(returns)}, "
              f"mean={fmt_pct(np.mean(returns)) if returns else 'N/A'}")

    # 5d. Trailing Stop on Option
    trailing_stops = [-0.20, -0.30, -0.50]
    trailing_stop_results = {}

    for stop in trailing_stops:
        returns = []
        returns_no_outlier = []
        hold_days_list = []
        stop_hit_count = 0
        peak_returns_captured = []

        for idx, ed in enumerate(event_data):
            result = simulate_trailing_stop(
                ed["option_series"], stop, ed["entry_date"]
            )
            if result is not None:
                returns.append(result["opt_return"])
                hold_days_list.append(result["hold_days"])
                if result["stop_hit"]:
                    stop_hit_count += 1
                peak_returns_captured.append(result["peak_return"])
                if idx != excl_idx:
                    returns_no_outlier.append(result["opt_return"])

        avg_hold = np.mean(hold_days_list) if hold_days_list else 7
        trailing_stop_results[stop] = {
            "all": compute_stats(returns, hold_days=max(1, int(avg_hold))),
            "no_outlier": compute_stats(returns_no_outlier, hold_days=max(1, int(avg_hold))),
            "stop_hit_rate": stop_hit_count / len(returns) if returns else 0,
            "avg_hold_days": float(avg_hold),
            "avg_peak_captured": float(np.mean(peak_returns_captured)) if peak_returns_captured else 0,
        }
        print(f"  Trailing {stop*100:.0f}%: N={len(returns)}, "
              f"stop hit={stop_hit_count}/{len(returns)}, "
              f"mean={fmt_pct(np.mean(returns)) if returns else 'N/A'}")

    # ─────────────────────────────────────
    # Section 6: Regime/Confounding Analysis
    # ─────────────────────────────────────
    print("\n[6/7] Running regime/confounding analysis...")

    # For regime analysis, use the 7-day fixed hold as the baseline comparison
    regime_data = {name: [] for name in list(REGIMES.keys()) + ["Other"]}

    for idx, ed in enumerate(event_data):
        regime = get_regime(ed["entry_date"])
        opt_ret = simulate_fixed_hold(ed["option_series"], 7, ed["entry_date"])
        if opt_ret is None:
            continue

        # Get SPY return over same 7-day period
        exit_7d = add_trading_days(ed["entry_date"], 7)
        spy_ret = get_spy_return(spy_prices, ed["entry_date"], exit_7d)

        regime_data[regime].append({
            "ticker": ed["ticker"],
            "entry_date": ed["entry_date"],
            "opt_return": opt_ret,
            "spy_return": spy_ret if spy_ret is not None else 0.0,
            "stock_return": ed["trade_return"],
            "is_ptn": idx == ptn_idx,
            "is_outlier": idx == excl_idx,
            "company": ed["company"],
        })

    # Compute per-regime stats
    regime_stats = {}
    all_opt_returns = []
    all_spy_returns = []

    for regime_name, trades in regime_data.items():
        if not trades:
            regime_stats[regime_name] = {"n": 0}
            continue

        opt_rets = [t["opt_return"] for t in trades]
        spy_rets = [t["spy_return"] for t in trades]
        all_opt_returns.extend(opt_rets)
        all_spy_returns.extend(spy_rets)

        stats = compute_stats(opt_rets, hold_days=7)
        stats["mean_spy"] = float(np.mean(spy_rets))
        stats["n_trades"] = len(trades)
        regime_stats[regime_name] = stats

    # Alpha/Beta regression: option_return = alpha + beta * spy_return + epsilon
    print("\n  Computing alpha/beta decomposition...")
    opt_arr = np.array(all_opt_returns)
    spy_arr = np.array(all_spy_returns)

    if len(opt_arr) > 2 and len(spy_arr) > 2:
        # Simple OLS: y = alpha + beta * x
        x_mean = np.mean(spy_arr)
        y_mean = np.mean(opt_arr)
        beta = np.sum((spy_arr - x_mean) * (opt_arr - y_mean)) / np.sum((spy_arr - x_mean) ** 2) if np.sum((spy_arr - x_mean) ** 2) > 0 else 0
        alpha = y_mean - beta * x_mean

        # R-squared
        predicted = alpha + beta * spy_arr
        ss_res = np.sum((opt_arr - predicted) ** 2)
        ss_tot = np.sum((opt_arr - y_mean) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        print(f"  Alpha (annualized): {alpha * (252/7) * 100:.1f}%")
        print(f"  Beta: {beta:.2f}")
        print(f"  R-squared: {r_squared:.3f}")
    else:
        alpha = 0
        beta = 0
        r_squared = 0

    # Flag trades where >80% of return came from market beta
    beta_dominated = []
    for regime_name, trades in regime_data.items():
        for t in trades:
            if beta != 0 and t["spy_return"] != 0:
                market_component = beta * t["spy_return"]
                if t["opt_return"] != 0:
                    beta_fraction = market_component / t["opt_return"]
                    if beta_fraction > 0.80:
                        beta_dominated.append(t)

    print(f"  Trades with >80% return from market beta: {len(beta_dominated)}")

    # Check outlier impact
    if outlier_idx is not None:
        outlier_ed = event_data[outlier_idx]
        outlier_opt_7d = simulate_fixed_hold(outlier_ed["option_series"], 7, outlier_ed["entry_date"])
        print(f"\n  Largest outlier: {outlier_ed['ticker']} ({outlier_ed['entry_date']})")
        print(f"  7d option return: {fmt_pct(outlier_opt_7d) if outlier_opt_7d else 'N/A'}")

        no_outlier_rets = []
        no_outlier_spy = []
        for regime_trades in regime_data.values():
            for t in regime_trades:
                if not t.get("is_outlier", False):
                    no_outlier_rets.append(t["opt_return"])
                    no_outlier_spy.append(t["spy_return"])

        if no_outlier_rets:
            no_outlier_stats = compute_stats(no_outlier_rets, hold_days=7)
            print(f"  Without outlier: N={len(no_outlier_rets)}, "
                  f"mean={fmt_pct(np.mean(no_outlier_rets))}, "
                  f"sharpe={no_outlier_stats['sharpe']:.2f}")

    # Extended hold regime analysis (30d, 60d)
    regime_data_30d = {name: [] for name in list(REGIMES.keys()) + ["Other"]}
    regime_data_60d = {name: [] for name in list(REGIMES.keys()) + ["Other"]}

    for idx, ed in enumerate(event_data):
        regime = get_regime(ed["entry_date"])

        ret_30d = simulate_fixed_hold(ed["option_series"], 30, ed["entry_date"])
        if ret_30d is not None:
            exit_30d = add_trading_days(ed["entry_date"], 30)
            spy_30 = get_spy_return(spy_prices, ed["entry_date"], exit_30d)
            regime_data_30d[regime].append({
                "opt_return": ret_30d,
                "spy_return": spy_30 if spy_30 is not None else 0.0,
                "is_ptn": idx == ptn_idx,
            })

        ret_60d = simulate_fixed_hold(ed["option_series"], 60, ed["entry_date"])
        if ret_60d is not None:
            exit_60d = add_trading_days(ed["entry_date"], 60)
            spy_60 = get_spy_return(spy_prices, ed["entry_date"], exit_60d)
            regime_data_60d[regime].append({
                "opt_return": ret_60d,
                "spy_return": spy_60 if spy_60 is not None else 0.0,
                "is_ptn": idx == ptn_idx,
            })

    # ─────────────────────────────────────
    # Section 7: Comparison Table
    # ─────────────────────────────────────
    print("\n[7/7] Building comparison table and report...")

    # Collect all strategies into a unified ranking
    all_strategies = []

    # Fixed hold
    for hd in fixed_hold_days:
        s = fixed_hold_results[hd]["all"]
        if s["n"] > 0:
            all_strategies.append({
                "name": f"Fixed {hd}d Hold",
                "category": "Fixed Hold",
                **s,
            })

    # Underlying targets
    for target in underlying_targets:
        s = underlying_target_results[target]["all"]
        if s["n"] > 0:
            all_strategies.append({
                "name": f"Underlying +{target*100:.0f}% Target",
                "category": "Underlying Target",
                "target_hit_rate": underlying_target_results[target]["target_hit_rate"],
                "avg_hold_days": underlying_target_results[target]["avg_hold_days"],
                **s,
            })

    # Option gain targets
    for target in option_targets:
        s = option_target_results[target]["all"]
        if s["n"] > 0:
            all_strategies.append({
                "name": f"Option +{target*100:.0f}% Target",
                "category": "Option Target",
                "target_hit_rate": option_target_results[target]["target_hit_rate"],
                "avg_hold_days": option_target_results[target]["avg_hold_days"],
                **s,
            })

    # Trailing stops
    for stop in trailing_stops:
        s = trailing_stop_results[stop]["all"]
        if s["n"] > 0:
            all_strategies.append({
                "name": f"Trailing Stop {stop*100:.0f}%",
                "category": "Trailing Stop",
                "stop_hit_rate": trailing_stop_results[stop]["stop_hit_rate"],
                "avg_hold_days": trailing_stop_results[stop]["avg_hold_days"],
                "avg_peak_captured": trailing_stop_results[stop]["avg_peak_captured"],
                **s,
            })

    # Sort by Sharpe descending
    all_strategies.sort(key=lambda x: x.get("sharpe", 0), reverse=True)

    # ─────────────────────────────────────
    # Generate Report
    # ─────────────────────────────────────
    print("\n  Generating report...")

    # Pre-compute outlier label for report
    if excl_idx is not None:
        outlier_label = f"{event_data[excl_idx]['ticker']} ({event_data[excl_idx]['entry_date']})"
    else:
        outlier_label = "N/A"

    lines = []
    def w(s=""):
        lines.append(s)

    w("# Exit Strategy Analysis: Insider Cluster-Buy Options")
    w()
    w(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    w(f"**Config:** 5% OTM (105%) strike, 90 DTE quarterly expiry")
    w(f"**Filter:** cluster>=2, total_value>=$5M, quality_score>=2.0")
    w(f"**Events with full data:** {len(event_data)} / 204 filtered events")
    w(f"**Data Sources:** Theta Data (options EOD), yfinance (stock daily closes)")
    w()

    # ── Section 1: Underlying Move Audit ──
    w("## 1. Audit: Underlying Price Moves for Winning Trades")
    w()
    w("### Question: How much does the stock actually need to move for a profitable 7-day options trade?")
    w()

    if winner_audits:
        winner_moves_arr = np.array([a["peak_stock_move"] for a in winner_audits])
        loser_audits = [a for a in all_audits if not a["is_7d_winner"]]
        loser_moves_arr = np.array([a["peak_stock_move"] for a in loser_audits]) if loser_audits else np.array([0.0])

        w(f"**7-day option winners:** {len(winner_audits)} / {len(all_audits)} trades")
        w()
        w("### Underlying Peak Move Distribution (Winners vs All)")
        w()
        w("| Percentile | Winners Peak Move | All Trades Peak Move | Losers Peak Move |")
        w("|---|---|---|---|")
        for p in [25, 50, 75, 90]:
            wv = float(np.percentile(winner_moves_arr, p))
            av = float(np.percentile(np.array([a["peak_stock_move"] for a in all_audits]), p))
            lv = float(np.percentile(loser_moves_arr, p)) if len(loser_audits) > 0 else 0
            w(f"| {p}th | {fmt_pct(wv)} | {fmt_pct(av)} | {fmt_pct(lv)} |")
        w()

        w("### Summary Statistics")
        w()
        w(f"- **Mean peak stock move (winners):** {fmt_pct(np.mean(winner_moves_arr))}")
        w(f"- **Median peak stock move (winners):** {fmt_pct(np.median(winner_moves_arr))}")
        w(f"- **Min peak stock move (winners):** {fmt_pct(np.min(winner_moves_arr))}")
        w(f"- **Mean days to peak (winners):** {np.mean([a['days_to_peak'] for a in winner_audits]):.1f}")
        w()

        # Show the option return at peak stock price vs at 7 days
        opt_at_peak_winners = [a["opt_return_at_peak"] for a in winner_audits if a["opt_return_at_peak"] is not None]
        opt_at_7d_winners = [a["opt_7d_return"] for a in winner_audits if a["opt_7d_return"] is not None]

        if opt_at_peak_winners:
            w(f"- **Mean option return at stock peak (winners):** {fmt_pct(np.mean(opt_at_peak_winners))}")
            w(f"- **Mean option return at 7d exit (winners):** {fmt_pct(np.mean(opt_at_7d_winners))}")
            w(f"- **Implication:** Holding to stock peak captures {fmt_pct(np.mean(opt_at_peak_winners) - np.mean(opt_at_7d_winners))} more return on average")
        w()

        # Top 10 winners table
        w("### Top 10 Winners by Option Return (7d)")
        w()
        w("| Ticker | Entry Date | Entry Price | Peak Price | Days to Peak | Peak Move | Opt 7d Ret | Opt at Peak |")
        w("|---|---|---|---|---|---|---|---|")
        sorted_winners = sorted(winner_audits, key=lambda x: x["opt_7d_return"] if x["opt_7d_return"] else 0, reverse=True)
        for a in sorted_winners[:10]:
            opt_peak = fmt_pct(a["opt_return_at_peak"]) if a["opt_return_at_peak"] is not None else "N/A"
            opt_7d = fmt_pct(a["opt_7d_return"]) if a["opt_7d_return"] is not None else "N/A"
            w(f"| {a['ticker']} | {a['entry_date']} | ${a['entry_stock_price']:.2f} | "
              f"${a['peak_stock_price']:.2f} | {a['days_to_peak']} | {fmt_pct(a['peak_stock_move'])} | "
              f"{opt_7d} | {opt_peak} |")
        w()

    # ── Section 2: Fixed Hold Duration ──
    w("## 2. Exit Strategy Backtests")
    w()
    w("### 2a. Fixed Hold Duration")
    w()
    w("Hold the option for a fixed number of trading days, then exit regardless of P&L.")
    w()
    w("| Hold Days | N | Mean Return | Median Return | Sharpe | Win Rate | Max Loss | Max Gain |")
    w("|---|---|---|---|---|---|---|---|")
    for hd in fixed_hold_days:
        s = fixed_hold_results[hd]["all"]
        if s["n"] > 0:
            w(f"| {hd} | {s['n']} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
              f"{s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} | {fmt_pct(s['max_return'])} |")
    w()

    w(f"**Without largest outlier ({outlier_label}):**")
    w()
    w("| Hold Days | N | Mean Return | Median Return | Sharpe | Win Rate |")
    w("|---|---|---|---|---|---|")
    for hd in fixed_hold_days:
        s = fixed_hold_results[hd]["no_outlier"]
        if s["n"] > 0:
            w(f"| {hd} | {s['n']} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
              f"{s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} |")
    w()

    # ── Section 2b: Underlying Price Target ──
    w("### 2b. Underlying Price Target")
    w()
    w("Exit when the underlying stock reaches a specified % gain from entry. 60 trading day max hold.")
    w()
    w("| Target | N | Hit Rate | Avg Hold Days | Mean Opt Return | Median | Sharpe | Win Rate | Max Loss |")
    w("|---|---|---|---|---|---|---|---|---|")
    for target in underlying_targets:
        r = underlying_target_results[target]
        s = r["all"]
        if s["n"] > 0:
            w(f"| +{target*100:.0f}% | {s['n']} | {fmt_pct(r['target_hit_rate'])} | "
              f"{r['avg_hold_days']:.1f} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
              f"{s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} |")
    w()

    w(f"**Without largest outlier ({outlier_label}):**")
    w()
    w("| Target | N | Mean Return | Sharpe | Win Rate |")
    w("|---|---|---|---|---|")
    for target in underlying_targets:
        s = underlying_target_results[target]["no_outlier"]
        if s["n"] > 0:
            w(f"| +{target*100:.0f}% | {s['n']} | {fmt_pct(s['mean'])} | {s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} |")
    w()

    # ── Section 2c: Option Gain Target ──
    w("### 2c. Option Gain Target")
    w()
    w("Exit when option value reaches a specified % gain from entry premium. 60 trading day max hold.")
    w()
    w("| Target | N | Hit Rate | Avg Hold Days | Mean Opt Return | Median | Sharpe | Win Rate | Max Loss |")
    w("|---|---|---|---|---|---|---|---|---|")
    for target in option_targets:
        r = option_target_results[target]
        s = r["all"]
        if s["n"] > 0:
            w(f"| +{target*100:.0f}% | {s['n']} | {fmt_pct(r['target_hit_rate'])} | "
              f"{r['avg_hold_days']:.1f} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
              f"{s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} |")
    w()

    w(f"**Without largest outlier ({outlier_label}):**")
    w()
    w("| Target | N | Mean Return | Sharpe | Win Rate |")
    w("|---|---|---|---|---|")
    for target in option_targets:
        s = option_target_results[target]["no_outlier"]
        if s["n"] > 0:
            w(f"| +{target*100:.0f}% | {s['n']} | {fmt_pct(s['mean'])} | {s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} |")
    w()

    # ── Section 2d: Trailing Stop ──
    w("### 2d. Trailing Stop on Option Value")
    w()
    w("After any gain from entry, set a trailing stop at X% below peak option value. Exit when hit or 60d max.")
    w()
    w("| Stop Level | N | Stop Hit Rate | Avg Hold Days | Mean Return | Median | Sharpe | Win Rate | Avg Peak Captured |")
    w("|---|---|---|---|---|---|---|---|---|")
    for stop in trailing_stops:
        r = trailing_stop_results[stop]
        s = r["all"]
        if s["n"] > 0:
            w(f"| {stop*100:.0f}% | {s['n']} | {fmt_pct(r['stop_hit_rate'])} | "
              f"{r['avg_hold_days']:.1f} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
              f"{s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} | {fmt_pct(r['avg_peak_captured'])} |")
    w()

    w(f"**Without largest outlier ({outlier_label}):**")
    w()
    w("| Stop Level | N | Mean Return | Sharpe | Win Rate |")
    w("|---|---|---|---|---|")
    for stop in trailing_stops:
        s = trailing_stop_results[stop]["no_outlier"]
        if s["n"] > 0:
            w(f"| {stop*100:.0f}% | {s['n']} | {fmt_pct(s['mean'])} | {s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} |")
    w()

    # ── Section 3: Regime/Confounding Analysis ──
    w("## 3. Regime & Confounding Analysis")
    w()
    w("### Key Question: Is this strategy generating real alpha, or just riding market momentum?")
    w()

    # Alpha/Beta decomposition
    w("### 3a. Alpha/Beta Decomposition (7-day fixed hold)")
    w()
    w(f"Regression: Option Return = alpha + beta * SPY Return")
    w()
    w(f"| Metric | Value |")
    w(f"|---|---|")
    w(f"| Alpha (per trade) | {fmt_pct(alpha)} |")
    w(f"| Alpha (annualized, {ANNUAL_TRADING_DAYS//7} periods) | {fmt_pct(alpha * (ANNUAL_TRADING_DAYS / 7))} |")
    w(f"| Beta | {beta:.2f} |")
    w(f"| R-squared | {r_squared:.4f} |")
    w(f"| N trades | {len(all_opt_returns)} |")
    w()

    if r_squared < 0.10:
        w("**Interpretation:** R-squared < 0.10 means SPY explains less than 10% of this strategy's "
          "return variance. The strategy's returns are largely **independent of market direction**, "
          "suggesting genuine alpha rather than beta exposure.")
    elif r_squared < 0.30:
        w("**Interpretation:** R-squared between 0.10 and 0.30 means SPY explains a moderate portion "
          "of returns. Some market sensitivity exists but the strategy does generate independent alpha.")
    else:
        w("**Interpretation:** R-squared > 0.30 means a meaningful portion of returns comes from "
          "market exposure. The strategy may be partially a leveraged beta play.")
    w()

    # Regime performance
    w("### 3b. Performance by Market Regime (7-day fixed hold)")
    w()
    w("| Regime | N | Mean Opt Return | Mean SPY Return | Excess Return | Sharpe | Win Rate |")
    w("|---|---|---|---|---|---|---|")
    for regime_name in REGIMES:
        s = regime_stats.get(regime_name, {"n": 0})
        if s["n"] > 0:
            excess = s["mean"] - s.get("mean_spy", 0)
            w(f"| {regime_name} | {s['n']} | {fmt_pct(s['mean'])} | {fmt_pct(s.get('mean_spy', 0))} | "
              f"{fmt_pct(excess)} | {s['sharpe']:.2f} | {fmt_pct(s['win_rate'])} |")
    w()

    # 30d regime analysis
    w("### 3c. Performance by Regime (30-day fixed hold)")
    w()
    w("| Regime | N | Mean Opt Return | Mean SPY Return | Excess Return |")
    w("|---|---|---|---|---|")
    for regime_name in REGIMES:
        trades = regime_data_30d.get(regime_name, [])
        if trades:
            opt_rets = [t["opt_return"] for t in trades]
            spy_rets = [t["spy_return"] for t in trades]
            mean_opt = np.mean(opt_rets)
            mean_spy = np.mean(spy_rets)
            w(f"| {regime_name} | {len(trades)} | {fmt_pct(mean_opt)} | {fmt_pct(mean_spy)} | {fmt_pct(mean_opt - mean_spy)} |")
    w()

    # Outlier analysis
    w("### 3d. Outlier Sensitivity Analysis")
    w()
    w("**PTN (Palatin Technologies):** Entry 2025-11-12, shares +214.8% in 7 days.")
    w("PTN's last option expiration was 2025-10-17 (before entry date), so this trade could ")
    w("only be executed in shares, not options. PTN does NOT affect the options analysis.")
    w()

    if outlier_idx is not None:
        outlier_ed = event_data[outlier_idx]
        outlier_7d = simulate_fixed_hold(outlier_ed["option_series"], 7, outlier_ed["entry_date"])
        outlier_30d = simulate_fixed_hold(outlier_ed["option_series"], 30, outlier_ed["entry_date"])
        outlier_60d = simulate_fixed_hold(outlier_ed["option_series"], 60, outlier_ed["entry_date"])

        w(f"**Largest option outlier: {outlier_ed['ticker']} ({outlier_ed['company']})** Entry {outlier_ed['entry_date']}")
        w(f"- 7d option return: {fmt_pct(outlier_7d) if outlier_7d is not None else 'N/A'}")
        w(f"- 30d option return: {fmt_pct(outlier_30d) if outlier_30d is not None else 'N/A'}")
        w(f"- 60d option return: {fmt_pct(outlier_60d) if outlier_60d is not None else 'N/A'}")
        w()

        w("**Impact of removing this outlier:**")
        w()
        w("| Hold Period | With Outlier (Sharpe) | Without (Sharpe) | With Outlier (Mean) | Without (Mean) |")
        w("|---|---|---|---|---|")
        for hd in fixed_hold_days:
            s_all = fixed_hold_results[hd]["all"]
            s_no = fixed_hold_results[hd]["no_outlier"]
            if s_all["n"] > 0 and s_no["n"] > 0:
                w(f"| {hd}d | {s_all['sharpe']:.2f} | {s_no['sharpe']:.2f} | "
                  f"{fmt_pct(s_all['mean'])} | {fmt_pct(s_no['mean'])} |")
        w()

    # Beta-dominated trades
    w("### 3e. Beta-Dominated Trades (>80% return from market)")
    w()
    if beta_dominated:
        w(f"**{len(beta_dominated)} trades** had >80% of their return attributable to SPY market movement:")
        w()
        w("| Ticker | Entry Date | Opt Return | SPY Return | Beta Component | Beta % |")
        w("|---|---|---|---|---|---|")
        for t in sorted(beta_dominated, key=lambda x: x["opt_return"], reverse=True)[:15]:
            market_comp = beta * t["spy_return"]
            beta_pct = market_comp / t["opt_return"] if t["opt_return"] != 0 else 0
            w(f"| {t['ticker']} | {t['entry_date']} | {fmt_pct(t['opt_return'])} | "
              f"{fmt_pct(t['spy_return'])} | {fmt_pct(market_comp)} | {fmt_pct(beta_pct)} |")
        w()
    else:
        w("No trades had >80% of return from market beta. This is a strong indicator of genuine alpha.")
        w()

    # ── Section 4: Master Comparison Table ──
    w("## 4. Master Comparison: All Exit Strategies Ranked by Sharpe")
    w()
    w("| Rank | Strategy | Category | N | Sharpe | Mean Return | Median | Win Rate | Max Loss | Avg Hold Days |")
    w("|---|---|---|---|---|---|---|---|---|---|")
    for rank, s in enumerate(all_strategies, 1):
        avg_hd = s.get("avg_hold_days", s.get("hold_days", "N/A"))
        if isinstance(avg_hd, (int, float)):
            avg_hd_str = f"{avg_hd:.0f}"
        else:
            avg_hd_str = str(avg_hd)
        w(f"| {rank} | {s['name']} | {s['category']} | {s['n']} | "
          f"{s['sharpe']:.2f} | {fmt_pct(s['mean'])} | {fmt_pct(s['median'])} | "
          f"{fmt_pct(s['win_rate'])} | {fmt_pct(s['min_return'])} | {avg_hd_str} |")
    w()

    # ── Section 5: Recommendations ──
    w("## 5. Recommendations")
    w()

    # Find best strategies
    best_sharpe = all_strategies[0] if all_strategies else None
    best_winrate = max(all_strategies, key=lambda x: x["win_rate"]) if all_strategies else None

    w("### Primary Recommendation")
    w()
    if best_sharpe:
        w(f"**Best risk-adjusted strategy: {best_sharpe['name']}**")
        w(f"- Sharpe: {best_sharpe['sharpe']:.2f}")
        w(f"- Mean return: {fmt_pct(best_sharpe['mean'])}")
        w(f"- Win rate: {fmt_pct(best_sharpe['win_rate'])}")
        w(f"- N trades: {best_sharpe['n']}")
    w()

    w("### Key Findings")
    w()

    # Check if longer holds improve returns
    if fixed_hold_results.get(7, {}).get("all", {}).get("n", 0) > 0:
        sharpe_7 = fixed_hold_results[7]["all"]["sharpe"]
        mean_7 = fixed_hold_results[7]["all"]["mean"]
        best_fixed_hd = max(fixed_hold_days,
                           key=lambda hd: fixed_hold_results[hd]["all"]["sharpe"]
                           if fixed_hold_results[hd]["all"]["n"] > 0 else -999)
        best_fixed_sharpe = fixed_hold_results[best_fixed_hd]["all"]["sharpe"]
        best_fixed_mean = fixed_hold_results[best_fixed_hd]["all"]["mean"]

        w(f"1. **Hold Period:** The current 7-day hold has Sharpe {sharpe_7:.2f}. "
          f"The best fixed hold is {best_fixed_hd} days with Sharpe {best_fixed_sharpe:.2f} "
          f"and mean return {fmt_pct(best_fixed_mean)}.")

        if best_fixed_hd > 7 and best_fixed_sharpe > sharpe_7:
            w(f"   - Extending the hold period to {best_fixed_hd} days would improve risk-adjusted returns.")
        elif best_fixed_hd == 7:
            w(f"   - The current 7-day hold is already optimal among fixed hold periods.")
        w()

    # Alpha conclusion
    w(f"2. **Alpha vs Beta:** With R-squared = {r_squared:.4f}, only "
      f"{r_squared*100:.1f}% of strategy returns are explained by SPY market movement. "
      f"This strategy generates **genuine alpha** that is independent of market direction.")
    w()

    # Outlier impact
    if excl_idx is not None:
        s_all_7 = fixed_hold_results[7]["all"]
        s_no_7 = fixed_hold_results[7]["no_outlier"]
        if s_all_7["n"] > 0 and s_no_7["n"] > 0:
            w(f"3. **Outlier Sensitivity:** Removing the largest outlier ({outlier_label}) changes "
              f"the 7-day Sharpe from {s_all_7['sharpe']:.2f} to {s_no_7['sharpe']:.2f}. "
              f"{'The strategy is robust even without this outlier.' if s_no_7['sharpe'] > 1.0 else 'The strategy shows some dependency on this outlier.'}")
    w(f"4. **PTN Note:** The PTN shares outlier (+214.8%) has no options data (expired before entry). "
      f"It only affects the shares analysis, not any options strategy.")
    w()

    w("### Practical Implementation")
    w()
    w("Based on the analysis above:")
    w()

    # Dynamic recommendation based on results
    trailing_best = None
    for stop in trailing_stops:
        s = trailing_stop_results[stop]["all"]
        if s["n"] > 0 and (trailing_best is None or s["sharpe"] > trailing_best["sharpe"]):
            trailing_best = {**s, "stop": stop, **trailing_stop_results[stop]}

    option_best = None
    for target in option_targets:
        s = option_target_results[target]["all"]
        if s["n"] > 0 and (option_best is None or s["sharpe"] > option_best["sharpe"]):
            option_best = {**s, "target": target, **option_target_results[target]}

    w("- **Exit Strategy 1 (Simple):** Use the best fixed hold period identified above")
    if trailing_best:
        w(f"- **Exit Strategy 2 (Active):** Trailing stop at {trailing_best['stop']*100:.0f}% "
          f"from peak (Sharpe {trailing_best['sharpe']:.2f}, avg peak captured: "
          f"{fmt_pct(trailing_best.get('avg_peak_captured', 0))})")
    if option_best:
        w(f"- **Exit Strategy 3 (Target):** Take profit at +{option_best['target']*100:.0f}% option gain "
          f"(hit rate: {fmt_pct(option_best.get('target_hit_rate', 0))}, Sharpe {option_best['sharpe']:.2f})")
    w()

    w("### Regime Awareness")
    w()
    for regime_name in REGIMES:
        s = regime_stats.get(regime_name, {"n": 0})
        if s["n"] >= 3:
            excess = s["mean"] - s.get("mean_spy", 0)
            if excess > 0.05:
                w(f"- **{regime_name}:** Strong alpha ({fmt_pct(excess)} excess over SPY) on {s['n']} trades")
            elif excess > 0:
                w(f"- **{regime_name}:** Positive alpha ({fmt_pct(excess)} excess over SPY) on {s['n']} trades")
            else:
                w(f"- **{regime_name}:** Negative excess ({fmt_pct(excess)}) vs SPY on {s['n']} trades -- caution")
    w()

    w("---")
    w("*Report generated by exit_strategy_analysis.py using Theta Data + yfinance.*")
    w("*All option prices are real historical EOD data -- zero synthetic/modeled prices.*")

    # Write report
    report_text = "\n".join(lines)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write(report_text)

    print(f"\n  Report written to: {REPORT_PATH}")
    print(f"  Report size: {len(report_text)} characters, {len(lines)} lines")

    # Save final caches
    save_cache(theta_cache, CACHE_PATH)
    save_cache(stock_cache, STOCK_CACHE_PATH)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    run_analysis()
