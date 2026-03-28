#!/usr/bin/env python3
"""
Insider Trading Options vs Shares Analysis
-------------------------------------------
Models options returns on the best insider strategy (cluster>=2, $5M+, senior, 7d hold).

Uses REAL Alpaca historical options data for 2024-2025 events, with Black-Scholes
fallback for 2020-2023 events (where Alpaca has no expired option data).

Author: Claude Opus 4.6
Date: 2026-02-27
"""

import csv
import json
import math
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, date

import numpy as np
from scipy.stats import norm

# ─────────────────────────────────────────────
# Alpaca SDK imports
# ─────────────────────────────────────────────
try:
    from alpaca.data.historical.option import OptionHistoricalDataClient
    from alpaca.data.requests import OptionBarsRequest
    from alpaca.data.timeframe import TimeFrame
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    print("WARNING: alpaca-py not available. All events will use Black-Scholes.")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")
REPORT_PATH = os.path.join(SCRIPT_DIR, "..", "..", "reports", "INSIDER_OPTIONS_REPORT.md")
CACHE_PATH = os.path.join(SCRIPT_DIR, "data", "alpaca_options_cache.json")

# Load .env for Alpaca credentials
def load_env():
    """Load Alpaca credentials from .env files."""
    env_paths = [
        os.path.join(SCRIPT_DIR, ".env"),
        os.path.join(SCRIPT_DIR, "..", "spy-0dte", ".env"),
        os.path.join(SCRIPT_DIR, "..", "..", ".env"),
    ]
    for p in env_paths:
        if os.path.exists(p):
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, val = line.split("=", 1)
                        os.environ.setdefault(key.strip(), val.strip())

load_env()

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_API_SECRET = os.environ.get("ALPACA_API_SECRET", "")

# Filter parameters for best strategy
MIN_CLUSTER_SIZE = 2
MIN_TOTAL_VALUE = 5_000_000
MIN_QUALITY_SCORE = 2.0

# Black-Scholes parameters
IV = 0.40              # 40% implied volatility (typical small-cap)
RISK_FREE_RATE = 0.05  # 5% risk-free rate
HOLD_DAYS = 7          # 7-day hold period

# Strike prices relative to entry
STRIKES = {
    "ATM (100%)":        1.00,
    "5% ITM (95%)":      0.95,
    "5% OTM (105%)":     1.05,
    "10% OTM (110%)":    1.10,
}

# Expiry windows in days
EXPIRIES = {
    "7 DTE (Weekly)":      7,
    "14 DTE (2-Week)":    14,
    "30 DTE (Monthly)":   30,
    "45 DTE":             45,
    "90 DTE (Quarterly)": 90,
}

# Portfolio sizing
PORTFOLIO_VALUE = 30_000
TRADES_PER_YEAR = 40   # ~204 events / 5 years
POSITION_SIZE_PCT = 0.05  # 5% per trade for shares
ANNUAL_TRADING_DAYS = 252

# API rate limiting
API_DELAY = 0.35  # seconds between Alpaca calls (stay under 200/min)

# Alpaca data cutoff: only 2024+ has historical expired option data
ALPACA_DATA_CUTOFF = date(2024, 1, 1)


# ─────────────────────────────────────────────
# Black-Scholes Functions
# ─────────────────────────────────────────────

def bs_d1(S, K, T, r, sigma):
    """Calculate d1 in Black-Scholes formula."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def bs_d2(S, K, T, r, sigma):
    """Calculate d2 in Black-Scholes formula."""
    return bs_d1(S, K, T, r, sigma) - sigma * math.sqrt(max(T, 1e-10))


def bs_call_price(S, K, T, r, sigma):
    """
    Black-Scholes European call option price.
    S: spot price, K: strike, T: time to expiry (years), r: risk-free, sigma: IV
    """
    if T <= 0:
        return max(S - K, 0.0)
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = bs_d2(S, K, T, r, sigma)
    price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    return max(price, 0.0)


def bs_delta(S, K, T, r, sigma):
    """Black-Scholes call delta."""
    if T <= 0:
        return 1.0 if S > K else 0.0
    d1 = bs_d1(S, K, T, r, sigma)
    return norm.cdf(d1)


def bs_gamma(S, K, T, r, sigma):
    """Black-Scholes call gamma."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = bs_d1(S, K, T, r, sigma)
    return norm.pdf(d1) / (S * sigma * math.sqrt(T))


def bs_theta(S, K, T, r, sigma):
    """Black-Scholes call theta (per calendar day, negative = decay)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)
    theta_annual = (
        -S * norm.pdf(d1) * sigma / (2 * math.sqrt(T))
        - r * K * math.exp(-r * T) * norm.cdf(d2)
    )
    return theta_annual / 365.0


# ─────────────────────────────────────────────
# OCC Symbol Construction & Alpaca API
# ─────────────────────────────────────────────

def get_standard_strikes(price):
    """Get standard option strike intervals based on stock price."""
    if price < 5:
        return 0.5
    elif price < 25:
        return 1.0
    elif price < 200:
        return 2.5
    else:
        return 5.0


def nearest_strike(price, target_mult):
    """Find nearest standard strike to price * target_mult."""
    raw = price * target_mult
    interval = get_standard_strikes(price)
    return round(raw / interval) * interval


def find_expiry_date(entry_date, target_dte):
    """
    Find the nearest Friday (standard options expiry) that is >= target_dte days
    from entry_date. Options typically expire on Fridays.
    """
    d = entry_date + timedelta(days=target_dte)
    # Find the nearest Friday (weekday 4)
    days_until_friday = (4 - d.weekday()) % 7
    if days_until_friday == 0 and d.weekday() != 4:
        days_until_friday = 7
    if d.weekday() == 4:
        return d
    # Go to the nearest Friday
    next_friday = d + timedelta(days=days_until_friday)
    prev_friday = next_friday - timedelta(days=7)
    # Pick whichever is closer but >= entry + target_dte - 3
    min_date = entry_date + timedelta(days=max(target_dte - 3, 1))
    if prev_friday >= min_date:
        return prev_friday
    return next_friday


def build_occ_symbol(ticker, expiry_date, is_call, strike):
    """
    Build OCC option symbol.
    Format: {UNDERLYING}{YYMMDD}{C/P}{STRIKE*1000 zero-padded to 8}
    """
    exp_str = expiry_date.strftime("%y%m%d")
    cp = "C" if is_call else "P"
    strike_int = int(round(strike * 1000))
    strike_str = f"{strike_int:08d}"
    return f"{ticker}{exp_str}{cp}{strike_str}"


def init_alpaca_client():
    """Initialize Alpaca options data client."""
    if not ALPACA_AVAILABLE or not ALPACA_API_KEY or not ALPACA_API_SECRET:
        return None
    try:
        client = OptionHistoricalDataClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
        )
        return client
    except Exception as e:
        print(f"  WARNING: Could not init Alpaca client: {e}")
        return None


def query_alpaca_option_bars(client, symbol, start_date, end_date, max_retries=2):
    """
    Query Alpaca for daily option bars.
    Returns list of bar dicts with 'close', 'open', 'high', 'low', 'volume', 'timestamp'.
    Returns empty list on failure.
    """
    if client is None:
        return []

    for attempt in range(max_retries + 1):
        try:
            req = OptionBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=datetime.combine(start_date, datetime.min.time()),
                end=datetime.combine(end_date + timedelta(days=1), datetime.min.time()),
            )
            bars = client.get_option_bars(req)

            # bars is a dict of symbol -> list of bars
            if hasattr(bars, 'data'):
                bar_data = bars.data.get(symbol, []) if isinstance(bars.data, dict) else []
            elif isinstance(bars, dict):
                bar_data = bars.get(symbol, [])
            else:
                bar_data = []

            result = []
            for b in bar_data:
                result.append({
                    "open": float(b.open),
                    "high": float(b.high),
                    "low": float(b.low),
                    "close": float(b.close),
                    "volume": int(b.volume) if hasattr(b, 'volume') else 0,
                    "timestamp": str(b.timestamp),
                })
            return result

        except Exception as e:
            if attempt < max_retries:
                time.sleep(1.0)
            else:
                return []

    return []


# ─────────────────────────────────────────────
# Cache for Alpaca queries (to avoid re-running)
# ─────────────────────────────────────────────

def load_cache():
    """Load cached Alpaca query results."""
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH) as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_cache(cache):
    """Save Alpaca query results to cache."""
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except:
        pass


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_and_filter_data():
    """Load results CSV and filter to best strategy parameters."""
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
            cluster_size = int(r["cluster_size"])

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
                    "cluster_size": cluster_size,
                    "n_insiders": n_insiders,
                    "quality_score": quality_score,
                    "company": r["company"],
                    "is_alpaca_era": entry_date >= ALPACA_DATA_CUTOFF,
                })
        except (ValueError, KeyError):
            continue

    return filtered


# ─────────────────────────────────────────────
# Options Return Modeling (per-event)
# ─────────────────────────────────────────────

def model_option_return_bs(entry_price, exit_price, strike_mult, dte_days):
    """
    Model call option return using Black-Scholes for a single trade.
    Returns dict with pricing info, or None if option worthless at entry.
    """
    S_entry = entry_price
    S_exit = exit_price
    K = nearest_strike(entry_price, strike_mult)
    T_entry = dte_days / 365.0
    T_exit = max((dte_days - HOLD_DAYS) / 365.0, 0.0)

    opt_entry = bs_call_price(S_entry, K, T_entry, RISK_FREE_RATE, IV)
    if opt_entry < 0.01:
        return None

    opt_exit = bs_call_price(S_exit, K, T_exit, RISK_FREE_RATE, IV)
    opt_return = (opt_exit - opt_entry) / opt_entry

    delta_entry = bs_delta(S_entry, K, T_entry, RISK_FREE_RATE, IV)
    gamma_entry = bs_gamma(S_entry, K, T_entry, RISK_FREE_RATE, IV)
    theta_entry = bs_theta(S_entry, K, T_entry, RISK_FREE_RATE, IV)
    leverage = delta_entry * S_entry / opt_entry if opt_entry > 0 else 0
    stock_return = (S_exit - S_entry) / S_entry

    return {
        "strike": K,
        "strike_mult": strike_mult,
        "dte": dte_days,
        "opt_entry_price": opt_entry,
        "opt_exit_price": opt_exit,
        "opt_return": opt_return,
        "stock_return": stock_return,
        "delta": delta_entry,
        "gamma": gamma_entry,
        "theta_daily": theta_entry,
        "theta_7d": theta_entry * 7,
        "leverage": leverage,
        "total_loss": opt_return <= -0.95,
        "half_loss": opt_return <= -0.50,
        "data_source": "black_scholes",
    }


def model_option_return_alpaca(client, cache, ev, strike_mult, dte_days):
    """
    Try to get real option pricing from Alpaca for a 2024-2025 event.
    Falls back to BS if no data available.
    Returns (result_dict, data_source_str).
    """
    ticker = ev["ticker"]
    entry_date = ev["entry_date"]
    exit_date = ev["exit_date"]
    entry_price = ev["entry_price"]
    exit_price = ev["exit_price"]

    K = nearest_strike(entry_price, strike_mult)
    expiry_date = find_expiry_date(entry_date, dte_days)

    # Build OCC symbol
    occ_symbol = build_occ_symbol(ticker, expiry_date, True, K)

    # Check cache first
    cache_key = f"{occ_symbol}|{entry_date}|{exit_date}"
    if cache_key in cache:
        cached = cache[cache_key]
        if cached is None:
            # Previously failed, fall back to BS
            return model_option_return_bs(entry_price, exit_price, strike_mult, dte_days), "bs_fallback"
        # Use cached bars
        bars = cached
    else:
        # Query Alpaca with padding around entry/exit dates
        query_start = entry_date - timedelta(days=3)
        query_end = exit_date + timedelta(days=3)
        time.sleep(API_DELAY)
        bars = query_alpaca_option_bars(client, occ_symbol, query_start, query_end)
        cache[cache_key] = bars if bars else None

    if not bars or len(bars) < 2:
        # No data — try alternate strikes (shift by 1 interval in each direction)
        interval = get_standard_strikes(entry_price)
        for offset in [interval, -interval, 2 * interval, -2 * interval]:
            alt_K = K + offset
            if alt_K <= 0:
                continue
            alt_symbol = build_occ_symbol(ticker, expiry_date, True, alt_K)
            alt_cache_key = f"{alt_symbol}|{entry_date}|{exit_date}"
            if alt_cache_key in cache:
                if cache[alt_cache_key] is None:
                    continue
                bars = cache[alt_cache_key]
                K = alt_K
                occ_symbol = alt_symbol
                break
            else:
                time.sleep(API_DELAY)
                alt_bars = query_alpaca_option_bars(client, alt_symbol, entry_date - timedelta(days=3), exit_date + timedelta(days=3))
                cache[alt_cache_key] = alt_bars if alt_bars else None
                if alt_bars and len(alt_bars) >= 2:
                    bars = alt_bars
                    K = alt_K
                    occ_symbol = alt_symbol
                    break

    if not bars or len(bars) < 2:
        # Fall back to Black-Scholes
        result = model_option_return_bs(entry_price, exit_price, strike_mult, dte_days)
        if result:
            result["data_source"] = "bs_fallback"
        return result, "bs_fallback"

    # Extract entry and exit prices from bars
    # Entry: first bar on or after entry_date
    # Exit: last bar on or before exit_date
    entry_bar = None
    exit_bar = None
    for b in bars:
        bar_date_str = b["timestamp"][:10]
        bar_date = datetime.strptime(bar_date_str, "%Y-%m-%d").date()
        if bar_date >= entry_date and entry_bar is None:
            entry_bar = b
        if bar_date <= exit_date:
            exit_bar = b

    if entry_bar is None or exit_bar is None:
        result = model_option_return_bs(entry_price, exit_price, strike_mult, dte_days)
        if result:
            result["data_source"] = "bs_fallback"
        return result, "bs_fallback"

    # Use close prices for entry and exit
    opt_entry = entry_bar["close"]
    opt_exit = exit_bar["close"]

    if opt_entry < 0.01:
        return None, "alpaca_worthless"

    opt_return = (opt_exit - opt_entry) / opt_entry
    stock_return = (exit_price - entry_price) / entry_price

    # Compute BS greeks for reference (approximate, since we don't know real IV)
    actual_dte = (expiry_date - entry_date).days
    T_entry = actual_dte / 365.0
    delta_entry = bs_delta(entry_price, K, T_entry, RISK_FREE_RATE, IV)
    gamma_entry = bs_gamma(entry_price, K, T_entry, RISK_FREE_RATE, IV)
    theta_entry = bs_theta(entry_price, K, T_entry, RISK_FREE_RATE, IV)
    leverage = delta_entry * entry_price / opt_entry if opt_entry > 0 else 0

    return {
        "strike": K,
        "strike_mult": strike_mult,
        "dte": dte_days,
        "opt_entry_price": opt_entry,
        "opt_exit_price": opt_exit,
        "opt_return": opt_return,
        "stock_return": stock_return,
        "delta": delta_entry,
        "gamma": gamma_entry,
        "theta_daily": theta_entry,
        "theta_7d": theta_entry * 7,
        "leverage": leverage,
        "total_loss": opt_return <= -0.95,
        "half_loss": opt_return <= -0.50,
        "data_source": "alpaca_real",
        "occ_symbol": occ_symbol,
        "entry_bar_date": entry_bar["timestamp"][:10],
        "exit_bar_date": exit_bar["timestamp"][:10],
    }, "alpaca_real"


# ─────────────────────────────────────────────
# Main Analysis Engine
# ─────────────────────────────────────────────

def run_options_analysis(events, alpaca_client):
    """Run full options analysis across all strike x expiry combinations."""
    results = {}
    cache = load_cache()
    total_api_calls = 0
    source_counts = {"alpaca_real": 0, "bs_fallback": 0, "black_scholes": 0, "skipped": 0}

    alpaca_events = [e for e in events if e["is_alpaca_era"]]
    bs_events = [e for e in events if not e["is_alpaca_era"]]

    print(f"  Events: {len(alpaca_events)} Alpaca-era (2024+), {len(bs_events)} BS-era (2020-2023)")

    combo_count = 0
    total_combos = len(STRIKES) * len(EXPIRIES)

    for strike_name, strike_mult in STRIKES.items():
        for expiry_name, dte in EXPIRIES.items():
            combo_count += 1
            key = (strike_name, expiry_name)
            trades = []
            skipped = 0
            source_detail = {"alpaca_real": 0, "bs_fallback": 0, "black_scholes": 0}

            print(f"\r  [{combo_count}/{total_combos}] {strike_name} x {expiry_name}...", end="", flush=True)

            # Process BS-era events (2020-2023) — pure BS
            for ev in bs_events:
                result = model_option_return_bs(
                    ev["entry_price"], ev["exit_price"],
                    strike_mult, dte
                )
                if result is None:
                    skipped += 1
                    continue
                result["ticker"] = ev["ticker"]
                result["entry_date"] = ev["entry_date_str"]
                result["is_alpaca_era"] = False
                trades.append(result)
                source_detail["black_scholes"] += 1

            # Process Alpaca-era events (2024-2025) — try real data first
            for ev in alpaca_events:
                if alpaca_client is not None:
                    result, src = model_option_return_alpaca(
                        alpaca_client, cache, ev, strike_mult, dte
                    )
                else:
                    result = model_option_return_bs(
                        ev["entry_price"], ev["exit_price"],
                        strike_mult, dte
                    )
                    src = "black_scholes"

                if result is None:
                    skipped += 1
                    continue

                result["ticker"] = ev["ticker"]
                result["entry_date"] = ev["entry_date_str"]
                result["is_alpaca_era"] = True
                trades.append(result)
                source_detail[result.get("data_source", src)] += 1

            if not trades:
                results[key] = None
                continue

            # Save cache periodically
            save_cache(cache)

            # Compute statistics
            opt_returns = np.array([t["opt_return"] for t in trades])
            stock_returns = np.array([t["stock_return"] for t in trades])
            deltas = np.array([t["delta"] for t in trades])
            leverages = np.array([t["leverage"] for t in trades])
            total_losses = np.array([t["total_loss"] for t in trades])
            half_losses = np.array([t["half_loss"] for t in trades])

            periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS
            mean_ret = np.mean(opt_returns)
            std_ret = np.std(opt_returns, ddof=1) if len(opt_returns) > 1 else 1.0
            sharpe = (mean_ret / std_ret) * math.sqrt(periods_per_year) if std_ret > 0 else 0

            stock_mean = np.mean(stock_returns)
            stock_std = np.std(stock_returns, ddof=1) if len(stock_returns) > 1 else 1.0
            stock_sharpe = (stock_mean / stock_std) * math.sqrt(periods_per_year) if stock_std > 0 else 0

            # Separate Alpaca-only and BS-only trade lists
            alpaca_only_trades = [t for t in trades if t.get("data_source") == "alpaca_real"]
            bs_only_trades = [t for t in trades if t.get("data_source") in ("black_scholes", "bs_fallback")]

            results[key] = {
                "n_trades": len(trades),
                "n_skipped": skipped,
                "mean_return": mean_ret,
                "median_return": float(np.median(opt_returns)),
                "std_return": float(std_ret),
                "sharpe": sharpe,
                "win_rate": float(np.mean(opt_returns > 0)),
                "mean_leverage": float(np.mean(leverages)),
                "median_leverage": float(np.median(leverages)),
                "mean_delta": float(np.mean(deltas)),
                "pct_total_loss": float(np.mean(total_losses)),
                "pct_half_loss": float(np.mean(half_losses)),
                "max_return": float(np.max(opt_returns)),
                "min_return": float(np.min(opt_returns)),
                "p5_return": float(np.percentile(opt_returns, 5)),
                "p25_return": float(np.percentile(opt_returns, 25)),
                "p75_return": float(np.percentile(opt_returns, 75)),
                "p95_return": float(np.percentile(opt_returns, 95)),
                "stock_mean": stock_mean,
                "stock_median": float(np.median(stock_returns)),
                "stock_sharpe": stock_sharpe,
                "stock_win_rate": float(np.mean(stock_returns > 0)),
                "trades": trades,
                "source_detail": source_detail,
                # Subset stats
                "alpaca_only_trades": alpaca_only_trades,
                "bs_only_trades": bs_only_trades,
            }

    print()  # newline after progress
    save_cache(cache)

    # Count overall source usage
    for key, d in results.items():
        if d is not None:
            for src, cnt in d["source_detail"].items():
                source_counts[src] = source_counts.get(src, 0) + cnt
            source_counts["skipped"] += d["n_skipped"]

    return results, source_counts


def compute_subset_stats(trades_list):
    """Compute summary stats for a subset of trades."""
    if not trades_list:
        return None
    opt_returns = np.array([t["opt_return"] for t in trades_list])
    periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS
    mean_ret = np.mean(opt_returns)
    std_ret = np.std(opt_returns, ddof=1) if len(opt_returns) > 1 else 1.0
    sharpe = (mean_ret / std_ret) * math.sqrt(periods_per_year) if std_ret > 0 else 0
    return {
        "n": len(trades_list),
        "mean_return": mean_ret,
        "median_return": float(np.median(opt_returns)),
        "std_return": float(std_ret),
        "sharpe": sharpe,
        "win_rate": float(np.mean(opt_returns > 0)),
        "pct_half_loss": float(np.mean(np.array([t["half_loss"] for t in trades_list]))),
        "pct_total_loss": float(np.mean(np.array([t["total_loss"] for t in trades_list]))),
        "max_return": float(np.max(opt_returns)),
        "min_return": float(np.min(opt_returns)),
    }


def run_leveraged_analysis(events):
    """Analyze 2x and 3x leveraged returns on shares."""
    stock_returns = np.array([ev["trade_return"] for ev in events])
    periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS

    results = {}
    for multiplier in [1, 2, 3]:
        lev_returns = multiplier * stock_returns
        mean_ret = np.mean(lev_returns)
        std_ret = np.std(lev_returns, ddof=1)
        sharpe = (mean_ret / std_ret) * math.sqrt(periods_per_year) if std_ret > 0 else 0

        results[f"{multiplier}x"] = {
            "mean_return": float(mean_ret),
            "median_return": float(np.median(lev_returns)),
            "std_return": float(std_ret),
            "sharpe": sharpe,
            "win_rate": float(np.mean(lev_returns > 0)),
            "max_return": float(np.max(lev_returns)),
            "min_return": float(np.min(lev_returns)),
            "pct_loss_50": float(np.mean(lev_returns < -0.50)),
            "pct_loss_30": float(np.mean(lev_returns < -0.30)),
            "pct_loss_20": float(np.mean(lev_returns < -0.20)),
            "p5_return": float(np.percentile(lev_returns, 5)),
            "p25_return": float(np.percentile(lev_returns, 25)),
            "p75_return": float(np.percentile(lev_returns, 75)),
            "p95_return": float(np.percentile(lev_returns, 95)),
            "max_drawdown_single": float(np.min(lev_returns)),
        }

    return results


def run_position_sizing(events, options_results):
    """Compare position sizing and capital efficiency for $30K portfolio."""
    stock_returns = np.array([ev["trade_return"] for ev in events])
    capital_per_trade_shares = PORTFOLIO_VALUE * POSITION_SIZE_PCT

    shares_pnl = capital_per_trade_shares * stock_returns
    shares_total_pnl = float(np.sum(shares_pnl))
    shares_per_trade_pnl = float(np.mean(shares_pnl))

    sizing_results = {
        "shares": {
            "capital_per_trade": capital_per_trade_shares,
            "total_capital_deployed": capital_per_trade_shares * len(events),
            "total_pnl": shares_total_pnl,
            "mean_pnl_per_trade": shares_per_trade_pnl,
            "pnl_per_year": shares_total_pnl / (len(events) / TRADES_PER_YEAR),
        }
    }

    target_combos = [
        ("ATM (100%)", "30 DTE (Monthly)"),
        ("ATM (100%)", "45 DTE"),
        ("5% ITM (95%)", "30 DTE (Monthly)"),
        ("5% OTM (105%)", "30 DTE (Monthly)"),
        ("ATM (100%)", "14 DTE (2-Week)"),
        ("ATM (100%)", "90 DTE (Quarterly)"),
    ]

    for strike_name, expiry_name in target_combos:
        key = (strike_name, expiry_name)
        opt_data = options_results.get(key)
        if opt_data is None or opt_data["n_trades"] == 0:
            continue

        trades = opt_data["trades"]
        avg_opt_price = np.mean([t["opt_entry_price"] for t in trades])
        avg_delta = np.mean([t["delta"] for t in trades])
        avg_leverage = opt_data["mean_leverage"]
        capital_for_equiv_delta = capital_per_trade_shares / avg_leverage if avg_leverage > 0 else float("inf")

        opt_returns_arr = np.array([t["opt_return"] for t in trades])
        opt_pnl_equiv = capital_for_equiv_delta * opt_returns_arr
        opt_pnl_full = capital_per_trade_shares * opt_returns_arr

        sizing_results[f"opt_{strike_name}_{expiry_name}"] = {
            "avg_option_price": float(avg_opt_price),
            "avg_delta": float(avg_delta),
            "avg_leverage": float(avg_leverage),
            "capital_for_equiv_delta": float(capital_for_equiv_delta),
            "capital_freed": float(capital_per_trade_shares - capital_for_equiv_delta),
            "pct_capital_freed": float((1 - capital_for_equiv_delta / capital_per_trade_shares) * 100),
            "equiv_delta_total_pnl": float(np.sum(opt_pnl_equiv)),
            "equiv_delta_mean_pnl": float(np.mean(opt_pnl_equiv)),
            "full_capital_total_pnl": float(np.sum(opt_pnl_full)),
            "full_capital_mean_pnl": float(np.mean(opt_pnl_full)),
            "n_trades": len(trades),
        }

    return sizing_results


# ─────────────────────────────────────────────
# Report Generation
# ─────────────────────────────────────────────

def fmt_pct(v, decimals=2):
    return f"{v * 100:.{decimals}f}%"


def fmt_dollar(v):
    if abs(v) >= 1000:
        return f"${v:,.0f}"
    return f"${v:,.2f}"


def generate_report(events, options_results, leveraged_results, sizing_results, source_counts):
    """Generate the full markdown report."""
    lines = []
    L = lines.append

    n_alpaca_era = len([e for e in events if e["is_alpaca_era"]])
    n_bs_era = len([e for e in events if not e["is_alpaca_era"]])

    L("# Insider Trading: Options vs Shares Analysis")
    L("")
    L(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L(f"**Strategy:** Cluster >= {MIN_CLUSTER_SIZE}, Total Value >= ${MIN_TOTAL_VALUE/1e6:.0f}M, "
      f"Quality Score >= {MIN_QUALITY_SCORE}, Hold = {HOLD_DAYS} days")
    L(f"**Events analyzed:** {len(events)}")
    L("")

    # ── Section 1: Data Source Breakdown ──
    L("## 1. Data Source Breakdown")
    L("")
    L("This analysis uses **real Alpaca historical options data** for 2024-2025 events where available,")
    L("with **Black-Scholes synthetic pricing** as fallback for 2020-2023 events (Alpaca has no expired")
    L("options data for this period).")
    L("")
    L("| Source | Count (per strike x expiry combo) | Description |")
    L("|--------|------|-------------|")
    L(f"| Alpaca Real Data | {source_counts.get('alpaca_real', 0)} | Actual historical option bar prices from Alpaca API |")
    L(f"| BS Fallback (2024-2025) | {source_counts.get('bs_fallback', 0)} | 2024-2025 events where specific contract had no Alpaca data |")
    L(f"| BS Synthetic (2020-2023) | {source_counts.get('black_scholes', 0)} | Pre-2024 events, no Alpaca data available |")
    L(f"| Skipped (worthless) | {source_counts.get('skipped', 0)} | Option worth <$0.01 at entry |")
    L("")
    L(f"**Event breakdown by era:** {n_alpaca_era} events in 2024-2025 (Alpaca era), "
      f"{n_bs_era} events in 2020-2023 (BS era)")
    L("")
    L("**BS Assumptions:** IV = 40%, Risk-free = 5%, European-style, no dividends")
    L("")

    # ── Section 2: Baseline Shares Performance ──
    stock_returns = np.array([ev["trade_return"] for ev in events])
    ab_returns = np.array([ev["abnormal_return"] for ev in events])
    periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS
    stock_sharpe = (np.mean(stock_returns) / np.std(stock_returns, ddof=1)) * math.sqrt(periods_per_year)

    L("## 2. Baseline: Shares Performance")
    L("")
    L("| Metric | Value |")
    L("|--------|-------|")
    L(f"| N Events | {len(events)} |")
    L(f"| Mean Trade Return | {fmt_pct(np.mean(stock_returns))} |")
    L(f"| Median Trade Return | {fmt_pct(np.median(stock_returns))} |")
    L(f"| Std Dev | {fmt_pct(np.std(stock_returns, ddof=1))} |")
    L(f"| Win Rate | {fmt_pct(np.mean(stock_returns > 0))} |")
    L(f"| Mean Abnormal Return | {fmt_pct(np.mean(ab_returns))} |")
    L(f"| Sharpe Ratio (annualized) | {stock_sharpe:.2f} |")
    L(f"| Max Return | {fmt_pct(np.max(stock_returns))} |")
    L(f"| Max Loss | {fmt_pct(np.min(stock_returns))} |")
    L(f"| 5th Percentile | {fmt_pct(np.percentile(stock_returns, 5))} |")
    L(f"| 95th Percentile | {fmt_pct(np.percentile(stock_returns, 95))} |")
    L("")

    # ── Section 3: Full Options Grid ──
    L("## 3. Options Performance: Full Strike x Expiry Grid (All Events, Mixed Data Sources)")
    L("")
    L("### 3a. Mean Option Return")
    L("")

    header = "| Strike \\ Expiry | " + " | ".join(EXPIRIES.keys()) + " |"
    sep = "|" + "---|" * (len(EXPIRIES) + 1)
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                row += f" {fmt_pct(d['mean_return'])} |"
        L(row)
    L("")

    L("### 3b. Median Option Return")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {fmt_pct(d['median_return'])} |" if d else " N/A |"
        L(row)
    L("")

    L("### 3c. Win Rate")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {fmt_pct(d['win_rate'])} |" if d else " N/A |"
        L(row)
    L("")

    L("### 3d. Sharpe Ratio (Annualized)")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {d['sharpe']:.2f} |" if d else " N/A |"
        L(row)
    L("")
    L(f"*Shares baseline Sharpe: {stock_sharpe:.2f}*")
    L("")

    L("### 3e. Effective Leverage (Mean)")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {d['mean_leverage']:.1f}x |" if d else " N/A |"
        L(row)
    L("")

    L("### 3f. Risk: % of Trades Losing >50% of Premium")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {fmt_pct(d['pct_half_loss'])} |" if d else " N/A |"
        L(row)
    L("")

    L("### 3g. Risk: % of Trades with Near-Total Loss (>95%)")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            row += f" {fmt_pct(d['pct_total_loss'])} |" if d else " N/A |"
        L(row)
    L("")

    L("### 3h. N Trades (including data source breakdown)")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sd = d["source_detail"]
                ar = sd.get("alpaca_real", 0)
                bf = sd.get("bs_fallback", 0)
                bs = sd.get("black_scholes", 0)
                row += f" {d['n_trades']} (A:{ar} BF:{bf} BS:{bs}) |"
        L(row)
    L("")
    L("*A=Alpaca real, BF=BS fallback (2024-2025 no data), BS=Black-Scholes (2020-2023)*")
    L("")

    # ── Section 4: Alpaca-Only Subset Analysis ──
    L("## 4. Alpaca-Only Subset Analysis (2024-2025 Events with Real Data)")
    L("")
    L("This section reports results ONLY for events where we obtained actual historical option")
    L("prices from Alpaca. These are the most reliable results.")
    L("")

    L("### 4a. Alpaca-Real Mean Return")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sub = compute_subset_stats(d["alpaca_only_trades"])
                if sub and sub["n"] >= 5:
                    row += f" {fmt_pct(sub['mean_return'])} (N={sub['n']}) |"
                elif sub:
                    row += f" {fmt_pct(sub['mean_return'])} (N={sub['n']}*) |"
                else:
                    row += " -- |"
        L(row)
    L("")
    L("*\\* = fewer than 5 events, treat with caution*")
    L("")

    L("### 4b. Alpaca-Real Sharpe Ratio")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sub = compute_subset_stats(d["alpaca_only_trades"])
                if sub and sub["n"] >= 5:
                    row += f" {sub['sharpe']:.2f} (N={sub['n']}) |"
                elif sub:
                    row += f" {sub['sharpe']:.2f} (N={sub['n']}*) |"
                else:
                    row += " -- |"
        L(row)
    L("")

    L("### 4c. Alpaca-Real Win Rate")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sub = compute_subset_stats(d["alpaca_only_trades"])
                if sub:
                    row += f" {fmt_pct(sub['win_rate'])} (N={sub['n']}) |"
                else:
                    row += " -- |"
        L(row)
    L("")

    # ── Section 5: BS-Only Subset Analysis ──
    L("## 5. BS-Only Subset Analysis (2020-2023 Events)")
    L("")
    L("Results using ONLY Black-Scholes modeled events (pre-2024).")
    L("")

    L("### 5a. BS-Only Mean Return")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sub = compute_subset_stats(d["bs_only_trades"])
                if sub:
                    row += f" {fmt_pct(sub['mean_return'])} (N={sub['n']}) |"
                else:
                    row += " -- |"
        L(row)
    L("")

    L("### 5b. BS-Only Sharpe Ratio")
    L("")
    L(header)
    L(sep)
    for strike_name in STRIKES.keys():
        row = f"| **{strike_name}** |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row += " N/A |"
            else:
                sub = compute_subset_stats(d["bs_only_trades"])
                if sub:
                    row += f" {sub['sharpe']:.2f} (N={sub['n']}) |"
                else:
                    row += " -- |"
        L(row)
    L("")

    # ── Section 6: Detailed Comparison for Top Combos ──
    L("## 6. Detailed Comparison: Top Options Strategies vs Shares")
    L("")

    all_combos = []
    for key, d in options_results.items():
        if d is not None and d["n_trades"] >= 50:
            all_combos.append((key, d))
    all_combos.sort(key=lambda x: x[1]["sharpe"], reverse=True)

    L("### Ranked by Sharpe Ratio (min 50 trades)")
    L("")
    L("| Rank | Strike | Expiry | Sharpe | Mean Ret | Med Ret | Win Rate | Leverage | >50% Loss | Total Loss | N |")
    L("|------|--------|--------|--------|----------|---------|----------|----------|-----------|------------|---|")

    for i, (key, d) in enumerate(all_combos[:10], 1):
        strike_name, expiry_name = key
        L(f"| {i} | {strike_name} | {expiry_name} | {d['sharpe']:.2f} | "
          f"{fmt_pct(d['mean_return'])} | {fmt_pct(d['median_return'])} | "
          f"{fmt_pct(d['win_rate'])} | {d['mean_leverage']:.1f}x | "
          f"{fmt_pct(d['pct_half_loss'])} | {fmt_pct(d['pct_total_loss'])} | {d['n_trades']} |")
    L("")
    L(f"*Shares baseline: Sharpe {stock_sharpe:.2f}, Mean {fmt_pct(np.mean(stock_returns))}, "
      f"Win Rate {fmt_pct(np.mean(stock_returns > 0))}*")
    L("")

    # ── Section 7: Return Distribution ──
    L("## 7. Return Distribution Comparison")
    L("")
    L("### Percentile Analysis (Selected Strategies)")
    L("")
    L("| Percentile | Shares | ATM 30DTE | ATM 45DTE | 5% ITM 30DTE | 5% OTM 30DTE | ATM 90DTE |")
    L("|------------|--------|-----------|-----------|--------------|---------------|-----------|")

    compare_keys = [
        ("ATM (100%)", "30 DTE (Monthly)"),
        ("ATM (100%)", "45 DTE"),
        ("5% ITM (95%)", "30 DTE (Monthly)"),
        ("5% OTM (105%)", "30 DTE (Monthly)"),
        ("ATM (100%)", "90 DTE (Quarterly)"),
    ]

    for pct_label, pct_val in [("5th", 5), ("10th", 10), ("25th", 25), ("50th (median)", 50),
                                ("75th", 75), ("90th", 90), ("95th", 95)]:
        row = f"| {pct_label} | {fmt_pct(np.percentile(stock_returns, pct_val))} |"
        for ck in compare_keys:
            d = options_results.get(ck)
            if d is not None:
                opt_rets = np.array([t["opt_return"] for t in d["trades"]])
                row += f" {fmt_pct(np.percentile(opt_rets, pct_val))} |"
            else:
                row += " N/A |"
        L(row)
    L("")

    # ── Section 8: Leveraged Shares ──
    L("## 8. Leveraged Shares (2x, 3x) Comparison")
    L("")
    L("| Metric | 1x (Shares) | 2x Leveraged | 3x Leveraged |")
    L("|--------|-------------|--------------|--------------|")

    for metric, fmt_fn in [
        ("mean_return", fmt_pct),
        ("median_return", fmt_pct),
        ("std_return", fmt_pct),
        ("sharpe", lambda v: f"{v:.2f}"),
        ("win_rate", fmt_pct),
        ("max_return", fmt_pct),
        ("min_return", fmt_pct),
        ("pct_loss_20", fmt_pct),
        ("pct_loss_30", fmt_pct),
        ("pct_loss_50", fmt_pct),
        ("p5_return", fmt_pct),
        ("p95_return", fmt_pct),
    ]:
        label = metric.replace("_", " ").title()
        vals = [fmt_fn(leveraged_results[m][metric]) for m in ["1x", "2x", "3x"]]
        L(f"| {label} | {vals[0]} | {vals[1]} | {vals[2]} |")
    L("")

    # ── Section 9: Capital Efficiency ──
    L("## 9. Capital Efficiency & Position Sizing")
    L("")
    L(f"**Portfolio:** ${PORTFOLIO_VALUE:,} | **Position Size:** {POSITION_SIZE_PCT*100:.0f}% = "
      f"${PORTFOLIO_VALUE * POSITION_SIZE_PCT:,.0f} per trade | "
      f"**Events/Year:** ~{TRADES_PER_YEAR}")
    L("")

    L("### Shares Baseline")
    L("")
    sd = sizing_results["shares"]
    L(f"- Capital per trade: {fmt_dollar(sd['capital_per_trade'])}")
    L(f"- Total PnL ({len(events)} trades): {fmt_dollar(sd['total_pnl'])}")
    L(f"- Mean PnL per trade: {fmt_dollar(sd['mean_pnl_per_trade'])}")
    L(f"- Estimated annual PnL: {fmt_dollar(sd['pnl_per_year'])}")
    L("")

    L("### Options Strategies: Equivalent Delta Exposure")
    L("")
    L("| Strategy | Capital/Trade | Capital Freed | Equiv-Delta PnL/Trade | Full-Capital PnL/Trade |")
    L("|----------|---------------|---------------|----------------------|------------------------|")

    capital_per_trade_shares = sd["capital_per_trade"]
    for skey, sdata in sizing_results.items():
        if skey == "shares":
            continue
        label = skey.replace("opt_", "").replace("_", " ")
        L(f"| {label} | {fmt_dollar(sdata['capital_for_equiv_delta'])} | "
          f"{fmt_dollar(sdata['capital_freed'])} ({sdata['pct_capital_freed']:.0f}%) | "
          f"{fmt_dollar(sdata['equiv_delta_mean_pnl'])} | "
          f"{fmt_dollar(sdata['full_capital_mean_pnl'])} |")
    L("")

    L("### Interpretation")
    L("")
    L("- **Capital for Equiv Delta:** How much option premium gives the same dollar-delta exposure as $1,500 in shares")
    L("- **Capital Freed:** The difference -- money that stays in cash or earns risk-free return")
    L("- **Equiv-Delta PnL:** What you make deploying only the equivalent-delta capital in options")
    L("- **Full-Capital PnL:** What you make deploying the full $1,500 position in options (higher leverage)")
    L("")

    # Annual portfolio comparison
    L("### Annual Portfolio Comparison")
    L("")
    L("| Approach | Annual PnL Est | Capital at Risk/Trade | Max Single-Trade Loss |")
    L("|----------|---------------|----------------------|----------------------|")

    shares_annual = sd["pnl_per_year"]
    shares_max_loss = np.min(stock_returns) * capital_per_trade_shares
    L(f"| Shares (1x) | {fmt_dollar(shares_annual)} | {fmt_dollar(capital_per_trade_shares)} | "
      f"{fmt_dollar(shares_max_loss)} |")

    lev2 = leveraged_results["2x"]
    lev2_annual = lev2["mean_return"] * capital_per_trade_shares * TRADES_PER_YEAR
    lev2_max_loss = lev2["min_return"] * capital_per_trade_shares
    L(f"| Shares (2x) | {fmt_dollar(lev2_annual)} | {fmt_dollar(capital_per_trade_shares)} | "
      f"{fmt_dollar(lev2_max_loss)} |")

    lev3 = leveraged_results["3x"]
    lev3_annual = lev3["mean_return"] * capital_per_trade_shares * TRADES_PER_YEAR
    lev3_max_loss = lev3["min_return"] * capital_per_trade_shares
    L(f"| Shares (3x) | {fmt_dollar(lev3_annual)} | {fmt_dollar(capital_per_trade_shares)} | "
      f"{fmt_dollar(lev3_max_loss)} |")

    if all_combos:
        best_key, best_data = all_combos[0]
        bsk, bek = best_key
        opt_skey = f"opt_{bsk}_{bek}"
        if opt_skey in sizing_results:
            opt_sd = sizing_results[opt_skey]
            opt_annual = opt_sd["full_capital_mean_pnl"] * TRADES_PER_YEAR
            L(f"| Options ({bsk}, {bek}) | {fmt_dollar(opt_annual)} | "
              f"{fmt_dollar(capital_per_trade_shares)} (premium=max loss) | "
              f"{fmt_dollar(-capital_per_trade_shares)} |")
    L("")

    # ── Section 10: Theta Decay ──
    L("## 10. Theta Decay Impact")
    L("")
    L("Over the 7-day hold, theta (time decay) erodes option premium even if the stock stays flat.")
    L("")
    L("| Strike | Expiry | Mean Theta (7d) | Mean Entry Premium | Theta as % of Premium |")
    L("|--------|--------|-----------------|--------------------|-----------------------|")
    for strike_name in STRIKES.keys():
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                continue
            trades = d["trades"]
            avg_theta_7d = np.mean([t["theta_7d"] for t in trades])
            avg_entry_premium = np.mean([t["opt_entry_price"] for t in trades])
            theta_pct = (abs(avg_theta_7d) / avg_entry_premium * 100) if avg_entry_premium > 0 else 0
            L(f"| {strike_name} | {expiry_name} | {fmt_dollar(avg_theta_7d)} | "
              f"{fmt_dollar(avg_entry_premium)} | {theta_pct:.1f}% |")
    L("")

    # ── Section 11: Optimal Strike+Expiry ──
    L("## 11. Which Strike + Expiry Is Optimal?")
    L("")
    if all_combos:
        best_key, best_d = all_combos[0]
        L(f"**Best by Sharpe (all data):** {best_key[0]} + {best_key[1]}")
        L(f"- Sharpe: {best_d['sharpe']:.2f} vs shares {stock_sharpe:.2f}")
        L(f"- Mean return: {fmt_pct(best_d['mean_return'])} vs shares {fmt_pct(np.mean(stock_returns))}")
        L(f"- Win rate: {fmt_pct(best_d['win_rate'])}")
        L(f"- Leverage: {best_d['mean_leverage']:.1f}x")
        L("")

        # Find best with low tail risk
        practical_best = None
        for key, d in all_combos:
            if d["pct_total_loss"] < 0.10 and d["pct_half_loss"] < 0.30:
                practical_best = (key, d)
                break
        if practical_best:
            pk, pd = practical_best
            L(f"**Best with acceptable tail risk (<10% wipeout, <30% half-loss):** {pk[0]} + {pk[1]}")
            L(f"- Sharpe: {pd['sharpe']:.2f}")
            L(f"- >50% loss: {fmt_pct(pd['pct_half_loss'])}")
            L(f"- Total wipeout: {fmt_pct(pd['pct_total_loss'])}")
            L("")

        # Best among Alpaca-real only
        alpaca_combos = []
        for key, d in options_results.items():
            if d is not None:
                at = d.get("alpaca_only_trades", [])
                if len(at) >= 5:
                    sub = compute_subset_stats(at)
                    if sub:
                        alpaca_combos.append((key, sub))
        alpaca_combos.sort(key=lambda x: x[1]["sharpe"], reverse=True)
        if alpaca_combos:
            ak, ad = alpaca_combos[0]
            L(f"**Best among Alpaca-real data only (N>={5}):** {ak[0]} + {ak[1]}")
            L(f"- Sharpe: {ad['sharpe']:.2f} (N={ad['n']})")
            L(f"- Mean return: {fmt_pct(ad['mean_return'])}")
            L(f"- Win rate: {fmt_pct(ad['win_rate'])}")
            L("")
    L("")

    # ── Section 12: Risk Analysis ──
    L("## 12. Risk Analysis: How Often You Get Wiped")
    L("")
    L("### Worst-Case Scenarios by Strategy")
    L("")
    L("| Strategy | Worst Trade | 5th Percentile | % > 50% Loss | % Total Wipeout |")
    L("|----------|------------|----------------|--------------|-----------------|")
    L(f"| Shares (1x) | {fmt_pct(np.min(stock_returns))} | {fmt_pct(np.percentile(stock_returns, 5))} | "
      f"{fmt_pct(np.mean(stock_returns < -0.50))} | {fmt_pct(np.mean(stock_returns < -0.95))} |")

    for i, (key, d) in enumerate(all_combos[:5]):
        strike_name, expiry_name = key
        L(f"| {strike_name} + {expiry_name} | {fmt_pct(d['min_return'])} | "
          f"{fmt_pct(d['p5_return'])} | {fmt_pct(d['pct_half_loss'])} | "
          f"{fmt_pct(d['pct_total_loss'])} |")
    L("")

    L("### Tail Risk Summary")
    L("")
    L("Options have **asymmetric risk**: limited downside (max loss = premium) but the premium itself")
    L("can be a significant % of position. On a 7-day hold:")
    L("")
    for expiry_name, dte in EXPIRIES.items():
        atm_key = ("ATM (100%)", expiry_name)
        d = options_results.get(atm_key)
        if d is None:
            continue
        L(f"- **ATM {expiry_name}:** {fmt_pct(d['pct_half_loss'])} of trades lose >50% premium, "
          f"{fmt_pct(d['pct_total_loss'])} near-total loss")
    L("")

    # ── Section 13: Verdict ──
    L("## 13. Verdict: Options Better or Worse Than Shares?")
    L("")

    best_opt_sharpe = all_combos[0][1]["sharpe"] if all_combos else 0
    shares_sharpe_val = stock_sharpe

    if best_opt_sharpe > shares_sharpe_val:
        L(f"**Options CAN improve risk-adjusted returns.** Best option Sharpe: "
          f"{best_opt_sharpe:.2f} vs shares: {shares_sharpe_val:.2f}.")
        L("")
        best_key_name = all_combos[0][0]
        best_d = all_combos[0][1]
        L(f"The optimal configuration is **{best_key_name[0]} strike, {best_key_name[1]} expiry**:")
        L(f"- Mean return: {fmt_pct(best_d['mean_return'])} (vs {fmt_pct(np.mean(stock_returns))} shares)")
        L(f"- Win rate: {fmt_pct(best_d['win_rate'])} (vs {fmt_pct(np.mean(stock_returns > 0))} shares)")
        L(f"- Mean leverage: {best_d['mean_leverage']:.1f}x")
        L(f"- >50% loss rate: {fmt_pct(best_d['pct_half_loss'])}")
        L(f"- Total wipeout rate: {fmt_pct(best_d['pct_total_loss'])}")
    else:
        L(f"**Options do NOT improve risk-adjusted returns.** Best option Sharpe: {best_opt_sharpe:.2f} "
          f"vs shares: {shares_sharpe_val:.2f}.")
        L("")
        L("Leverage from options amplifies both winners AND losers. Theta decay over "
          "the 7-day hold significantly erodes returns on losing/flat trades.")

    L("")
    L("### Arguments FOR options:")
    L("1. Capped downside: max loss = premium paid (no unlimited gap-down risk)")
    L("2. Capital efficiency: free up cash for other trades or risk-free yield")
    L("3. Higher leverage on winners amplifies the mean abnormal return")
    L("")
    L("### Arguments AGAINST options:")
    L("1. Theta decay: 7-day hold loses significant time value, especially short-dated")
    L("2. ~43% of trades are losers -- options can lose 50-100% of premium while shares lose less")
    L("3. Median return often lower than mean due to asymmetric theta impact on losers")
    L("4. IV=40% assumption may be LOW for many small-cap insider stocks")
    L("5. Real-world bid-ask spreads reduce returns (not modeled)")
    L("6. Liquidity risk: small-cap single-stock options may have wide spreads or no market")
    L("")

    # ── Section 14: Final Recommendation ──
    L("## 14. Final Recommendation")
    L("")

    practical_best = None
    for key, d in all_combos:
        if d["pct_total_loss"] < 0.10 and d["pct_half_loss"] < 0.30:
            practical_best = (key, d)
            break

    if practical_best:
        pk, pd = practical_best
        better_sharpe = pd["sharpe"] > stock_sharpe

        if better_sharpe:
            L(f"### Use Options: {pk[0]} strike, {pk[1]} expiry")
            L("")
            L(f"This configuration achieves a Sharpe of **{pd['sharpe']:.2f}** vs **{stock_sharpe:.2f}** for shares,")
            L(f"with manageable tail risk ({fmt_pct(pd['pct_total_loss'])} wipeout, {fmt_pct(pd['pct_half_loss'])} half-loss).")
            L("")
            L("**Implementation:**")
            L(f"- Buy call options at **{pk[0]}** strike relative to entry price")
            L(f"- Target **{pk[1]}** expiry from entry date")
            L(f"- Allocate **2-3% of portfolio** per trade (lower than shares due to {pd['mean_leverage']:.0f}x leverage)")
            L(f"- Close position at T+7 trading days")
            L(f"- Expected ~{TRADES_PER_YEAR} trades/year")
        else:
            L("### Stick with Shares")
            L("")
            L(f"While options provide leverage and capital efficiency, the best risk-acceptable")
            L(f"option strategy (Sharpe {pd['sharpe']:.2f}) does not meaningfully beat shares (Sharpe {stock_sharpe:.2f}).")
            L(f"The added complexity, liquidity risk, and bid-ask costs make shares the better choice.")
    else:
        L("### Stick with Shares")
        L("")
        L("No options configuration provides sufficient risk-adjusted improvement with acceptable tail risk.")
        L("The high wipeout rates on short-dated options and theta drag make shares the safer choice.")

    L("")
    L("### Leveraged Shares Assessment")
    L("")
    lev2_sharpe = leveraged_results["2x"]["sharpe"]
    lev3_sharpe = leveraged_results["3x"]["sharpe"]
    L(f"- **2x leveraged:** Sharpe {lev2_sharpe:.2f} (mathematically identical to 1x {shares_sharpe_val:.2f}). "
      f"Doubles mean AND std. Worst trade: {fmt_pct(leveraged_results['2x']['min_return'])}.")
    L(f"- **3x leveraged:** Sharpe {lev3_sharpe:.2f}. "
      f"Worst trade: {fmt_pct(leveraged_results['3x']['min_return'])}. "
      f"{fmt_pct(leveraged_results['3x']['pct_loss_50'])} of trades lose >50%.")
    L("")
    L("Leveraged shares do NOT improve Sharpe. They only make sense if you want higher absolute "
      "returns and can absorb proportionally larger drawdowns.")
    L("")

    L("### Summary Table")
    L("")
    L("| Approach | Sharpe | Mean Return | Win Rate | Worst Case | Verdict |")
    L("|----------|--------|-------------|----------|------------|---------|")
    L(f"| Shares (1x) | {shares_sharpe_val:.2f} | {fmt_pct(np.mean(stock_returns))} | "
      f"{fmt_pct(np.mean(stock_returns > 0))} | {fmt_pct(np.min(stock_returns))} | Baseline |")
    L(f"| Shares (2x) | {lev2_sharpe:.2f} | {fmt_pct(leveraged_results['2x']['mean_return'])} | "
      f"{fmt_pct(leveraged_results['2x']['win_rate'])} | {fmt_pct(leveraged_results['2x']['min_return'])} | "
      f"Same Sharpe, more risk |")
    L(f"| Shares (3x) | {lev3_sharpe:.2f} | {fmt_pct(leveraged_results['3x']['mean_return'])} | "
      f"{fmt_pct(leveraged_results['3x']['win_rate'])} | {fmt_pct(leveraged_results['3x']['min_return'])} | "
      f"Same Sharpe, much more risk |")

    for i, (key, d) in enumerate(all_combos[:3], 1):
        strike_name, expiry_name = key
        verdict = "Best risk-adjusted" if i == 1 else ("Runner-up" if i == 2 else "Third")
        L(f"| Options ({strike_name}, {expiry_name}) | {d['sharpe']:.2f} | "
          f"{fmt_pct(d['mean_return'])} | {fmt_pct(d['win_rate'])} | -100% (premium) | {verdict} |")
    L("")

    L("---")
    L("")
    L("*Analysis uses real Alpaca historical option prices for 2024-2025 events where available, "
      "with Black-Scholes synthetic pricing (IV=40%, r=5%) for 2020-2023 events. "
      "Real-world results would vary due to bid-ask spreads, liquidity, actual IV levels, "
      "early exercise, and discrete strike availability.*")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 70)
    print("INSIDER TRADING: OPTIONS vs SHARES ANALYSIS")
    print("  Using Alpaca real data (2024-2025) + Black-Scholes (2020-2023)")
    print("=" * 70)
    print()

    # Load data
    print("[1/6] Loading and filtering data...")
    events = load_and_filter_data()
    n_alpaca = len([e for e in events if e["is_alpaca_era"]])
    n_bs = len([e for e in events if not e["is_alpaca_era"]])
    print(f"  Loaded {len(events)} events ({n_alpaca} Alpaca-era 2024+, {n_bs} BS-era 2020-2023)")
    print(f"  Mean stock return: {np.mean([e['trade_return'] for e in events])*100:.2f}%")
    print(f"  Median stock return: {np.median([e['trade_return'] for e in events])*100:.2f}%")

    # Init Alpaca client
    print("\n[2/6] Initializing Alpaca options data client...")
    alpaca_client = init_alpaca_client()
    if alpaca_client:
        print("  Alpaca client ready. Will query real option bars for 2024-2025 events.")
    else:
        print("  WARNING: No Alpaca client. All events will use Black-Scholes.")

    # Run options analysis
    print(f"\n[3/6] Running options analysis across {len(STRIKES)} strikes x {len(EXPIRIES)} expiries = {len(STRIKES) * len(EXPIRIES)} combinations...")
    print(f"  This will make Alpaca API calls for {n_alpaca} events per combo (with caching)...")
    options_results, source_counts = run_options_analysis(events, alpaca_client)

    # Print source summary
    print(f"\n  Data source summary:")
    for src, cnt in sorted(source_counts.items()):
        print(f"    {src}: {cnt}")

    # Quick Sharpe summary
    stock_returns = np.array([ev["trade_return"] for ev in events])
    periods_per_year = ANNUAL_TRADING_DAYS / HOLD_DAYS
    stock_sharpe = (np.mean(stock_returns) / np.std(stock_returns, ddof=1)) * math.sqrt(periods_per_year)
    print(f"\n  Sharpe grid (shares baseline: {stock_sharpe:.2f}):")
    header = f"  {'':20s} |"
    for en in EXPIRIES.keys():
        header += f" {en[:7]:>7s} |"
    print(header)
    for strike_name in STRIKES.keys():
        row_str = f"  {strike_name:20s} |"
        for expiry_name in EXPIRIES.keys():
            d = options_results.get((strike_name, expiry_name))
            if d is None:
                row_str += "     N/A |"
            else:
                row_str += f"   {d['sharpe']:5.2f} |"
        print(row_str)

    # Run leveraged analysis
    print(f"\n[4/6] Running leveraged shares analysis (1x, 2x, 3x)...")
    leveraged_results = run_leveraged_analysis(events)
    for mult in ["1x", "2x", "3x"]:
        d = leveraged_results[mult]
        print(f"  {mult}: Sharpe {d['sharpe']:.2f}, Mean {d['mean_return']*100:.2f}%, "
              f"Win Rate {d['win_rate']*100:.1f}%")

    # Run position sizing
    print(f"\n[5/6] Running position sizing analysis...")
    sizing_results = run_position_sizing(events, options_results)
    sd = sizing_results["shares"]
    print(f"  Shares: {fmt_dollar(sd['mean_pnl_per_trade'])}/trade, "
          f"{fmt_dollar(sd['pnl_per_year'])}/year est.")

    # Generate report
    print(f"\n[6/6] Generating report...")
    report = generate_report(events, options_results, leveraged_results, sizing_results, source_counts)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write(report)
    print(f"  Report written to: {REPORT_PATH}")

    # Print the verdict section
    print("\n" + "=" * 70)
    print("VERDICT (see report for full details)")
    print("=" * 70)

    all_combos = []
    for key, d in options_results.items():
        if d is not None and d["n_trades"] >= 50:
            all_combos.append((key, d))
    all_combos.sort(key=lambda x: x[1]["sharpe"], reverse=True)

    if all_combos:
        best_key, best_d = all_combos[0]
        print(f"\nBest options strategy: {best_key[0]} + {best_key[1]}")
        print(f"  Sharpe: {best_d['sharpe']:.2f} (shares: {stock_sharpe:.2f})")
        print(f"  Mean return: {best_d['mean_return']*100:.2f}% (shares: {np.mean(stock_returns)*100:.2f}%)")
        print(f"  Win rate: {best_d['win_rate']*100:.1f}% (shares: {(np.mean(stock_returns>0))*100:.1f}%)")
        print(f"  >50% loss risk: {best_d['pct_half_loss']*100:.1f}%")
        print(f"  Total wipeout risk: {best_d['pct_total_loss']*100:.1f}%")
        print(f"  Data sources: {best_d['source_detail']}")

        if best_d['sharpe'] > stock_sharpe:
            print(f"\n  >>> OPTIONS OUTPERFORM SHARES on risk-adjusted basis (+{best_d['sharpe']-stock_sharpe:.2f} Sharpe)")
        else:
            print(f"\n  >>> SHARES OUTPERFORM OPTIONS on risk-adjusted basis (delta: {stock_sharpe-best_d['sharpe']:.2f} Sharpe)")

    print("\nDone.")


if __name__ == "__main__":
    main()
