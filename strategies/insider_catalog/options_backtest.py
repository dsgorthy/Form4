#!/usr/bin/env python3
"""
Options backtest using REAL Theta Data EOD prices.

For each qualified insider's test-period trades, looks up actual option contracts
and computes real P&L. No Black-Scholes modeling.

Covers:
  - BUY side: 5% OTM calls at 90 DTE, hold 60 trading days or +100% profit target
  - SELL side: 5% OTM puts at 90 DTE, hold 60 trading days or +100% profit target

Uses Theta Data local terminal at http://127.0.0.1:25503.

Usage:
  python options_backtest.py                    # full backtest
  python options_backtest.py --side buy         # buy side only
  python options_backtest.py --side sell        # sell side only
  python options_backtest.py --max-trades 100   # limit for testing
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"
CACHE_PATH = Path(__file__).resolve().parent / "data" / "options_theta_cache.json"

THETA_BASE = "http://127.0.0.1:25503"
API_DELAY = 0.3  # Theta Data rate limit

# Options parameters
STRIKE_OTM_PCT = 0.05   # 5% OTM
TARGET_DTE = 90          # 90 DTE
MAX_HOLD_TRADING_DAYS = 60
PROFIT_TARGET = 1.0      # +100% on premium

WINDOWS = ["7d", "30d", "90d"]
ANNUALIZE = {"7d": (252 / 7) ** 0.5, "30d": (252 / 30) ** 0.5, "90d": (252 / 90) ** 0.5}


# ── Cache ────────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_cache(cache: dict):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        CACHE_PATH.write_text(json.dumps(cache))
    except Exception:
        pass


# ── Theta Data API ───────────────────────────────────────────────────────

def theta_get(endpoint: str, params: dict, cache: dict, cache_key: str = None):
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
                   start_date: date, end_date: date, cache: dict):
    exp_str = expiration.strftime("%Y-%m-%d")
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    cache_key = f"eod|{symbol}|{exp_str}|{strike}|{right}|{start_str}|{end_str}"
    return theta_get(
        "/v3/option/history/eod",
        {"symbol": symbol, "expiration": exp_str, "strike": str(strike),
         "right": right, "start_date": start_str, "end_date": end_str},
        cache, cache_key,
    )


def get_fair_price(row: dict) -> Optional[float]:
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


def get_eod_date(row: dict) -> Optional[date]:
    try:
        created = row.get("created", "").strip().strip('"')
        return datetime.strptime(created[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def add_trading_days(d: date, n: int) -> date:
    current = d
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# ── Find best option contract ────────────────────────────────────────────

def find_best_contract(symbol: str, stock_price: float, filing_date: date,
                       right: str, cache: dict) -> Optional[dict]:
    """
    Find the best option contract for entry:
      - right: 'C' (call) or 'P' (put)
      - Strike: nearest to 5% OTM
      - Expiry: nearest to 90 DTE from filing_date
    Returns dict with expiration, strike, right or None.
    """
    expirations = get_expirations(symbol, cache)
    if not expirations:
        return None

    # Target expiry: 90 calendar days out
    target_expiry = filing_date + timedelta(days=TARGET_DTE)

    # Find closest expiry between 75-120 DTE
    min_exp = filing_date + timedelta(days=75)
    max_exp = filing_date + timedelta(days=120)
    valid_exps = [e for e in expirations if min_exp <= e <= max_exp]

    if not valid_exps:
        # Fallback: closest to 90 DTE that's at least 60 days out
        min_exp_fallback = filing_date + timedelta(days=60)
        valid_exps = [e for e in expirations if e >= min_exp_fallback]
        if not valid_exps:
            return None

    best_exp = min(valid_exps, key=lambda e: abs((e - target_expiry).days))

    # Get strikes for that expiry
    strikes = get_strikes(symbol, best_exp, cache)
    if not strikes:
        return None

    # Target strike: 5% OTM
    if right == "C":
        target_strike = stock_price * (1 + STRIKE_OTM_PCT)
    else:  # put
        target_strike = stock_price * (1 - STRIKE_OTM_PCT)

    best_strike = min(strikes, key=lambda s: abs(s - target_strike))

    return {
        "expiration": best_exp,
        "strike": best_strike,
        "right": right,
        "symbol": symbol,
        "target_strike": target_strike,
        "actual_dte": (best_exp - filing_date).days,
    }


# ── Simulate one options trade ───────────────────────────────────────────

@dataclass
class OptionsTrade:
    ticker: str
    insider_name: str
    insider_id: int
    trade_type: str  # 'buy' -> call, 'sell' -> put
    filing_date: date
    stock_price: float
    option_right: str
    strike: float
    expiration: date
    dte: int
    entry_price: float
    entry_date: date
    exit_price: float
    exit_date: date
    exit_reason: str  # 'profit_target', 'time_exit', 'expiry', 'no_data'
    pnl_pct: float
    pnl_dollar: float  # per contract
    win: bool


def simulate_option_trade(
    ticker: str, insider_name: str, insider_id: int, trade_type: str,
    filing_date: date, stock_price: float, cache: dict,
) -> Optional[OptionsTrade]:
    """
    Simulate buying a call (buy signal) or put (sell signal) option.
    Entry at T+1 after filing, exit at min(+100% profit, 60 trading days, expiry-5d).
    """
    right = "C" if trade_type == "buy" else "P"

    contract = find_best_contract(ticker, stock_price, filing_date, right, cache)
    if not contract:
        return None

    expiration = contract["expiration"]
    strike = contract["strike"]
    dte = contract["actual_dte"]

    # Entry: T+1 after filing
    entry_date = add_trading_days(filing_date, 1)

    # Get EOD data for the contract from entry through hold period
    max_exit_date = min(
        add_trading_days(entry_date, MAX_HOLD_TRADING_DAYS),
        expiration - timedelta(days=5),  # exit at least 5 days before expiry
    )

    if max_exit_date <= entry_date:
        return None

    eod_rows = get_option_eod(ticker, expiration, strike, right,
                              entry_date, max_exit_date, cache)
    if not eod_rows:
        return None

    # Parse EOD into date -> price
    daily_prices = {}
    for row in eod_rows:
        d = get_eod_date(row)
        p = get_fair_price(row)
        if d and p and p > 0:
            daily_prices[d] = p

    if not daily_prices:
        return None

    # Entry price: first available date on or after entry_date
    sorted_dates = sorted(daily_prices.keys())
    entry_candidates = [d for d in sorted_dates if d >= entry_date]
    if not entry_candidates:
        return None

    actual_entry_date = entry_candidates[0]
    entry_price = daily_prices[actual_entry_date]

    if entry_price <= 0.01:  # skip penny options
        return None

    # Simulate hold: check each day for profit target
    exit_price = None
    exit_date = None
    exit_reason = "time_exit"

    trading_days_held = 0
    for d in sorted_dates:
        if d <= actual_entry_date:
            continue
        trading_days_held += 1
        price = daily_prices[d]

        # Check profit target
        if price >= entry_price * (1 + PROFIT_TARGET):
            exit_price = price
            exit_date = d
            exit_reason = "profit_target"
            break

        # Check time limit
        if trading_days_held >= MAX_HOLD_TRADING_DAYS or d >= max_exit_date:
            exit_price = price
            exit_date = d
            exit_reason = "time_exit"
            break

    if exit_price is None:
        # Use last available price
        if sorted_dates:
            last_date = sorted_dates[-1]
            if last_date > actual_entry_date:
                exit_price = daily_prices[last_date]
                exit_date = last_date
                exit_reason = "data_end"

    if exit_price is None or exit_date is None:
        return None

    pnl_pct = (exit_price - entry_price) / entry_price
    pnl_dollar = (exit_price - entry_price) * 100  # per contract

    return OptionsTrade(
        ticker=ticker,
        insider_name=insider_name,
        insider_id=insider_id,
        trade_type=trade_type,
        filing_date=filing_date,
        stock_price=stock_price,
        option_right=right,
        strike=strike,
        expiration=expiration,
        dte=dte,
        entry_price=entry_price,
        entry_date=actual_entry_date,
        exit_price=exit_price,
        exit_date=exit_date,
        exit_reason=exit_reason,
        pnl_pct=pnl_pct,
        pnl_dollar=pnl_dollar,
        win=pnl_pct > 0,
    )


# ── Walk-forward with options ────────────────────────────────────────────

def load_qualified_insiders(conn: sqlite3.Connection, trade_type: str,
                            min_trades: int = 8, train_frac: float = 0.75,
                            min_sharpe: float = 0.5):
    """
    Load qualified insiders using same walk-forward logic as walkforward_study.py.
    Returns list of (insider_id, name, best_window, test_trades).
    """
    negate = (trade_type == "sell")
    sign = -1.0 if negate else 1.0

    insiders = conn.execute("""
        SELECT t.insider_id, i.name,
               (SELECT ticker FROM trades WHERE insider_id = t.insider_id
                GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 1) as primary_ticker,
               COUNT(*) as n
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = ?
          AND (tr.abnormal_7d IS NOT NULL OR tr.abnormal_30d IS NOT NULL OR tr.abnormal_90d IS NOT NULL)
        GROUP BY t.insider_id
        HAVING n >= ?
    """, (trade_type, min_trades)).fetchall()

    results = []

    for insider_id, name, primary_ticker, n in insiders:
        trades = conn.execute("""
            SELECT t.trade_id, t.ticker, t.trade_date, t.filing_date, t.value,
                   tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d,
                   tr.entry_price
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = ?
            ORDER BY t.trade_date ASC
        """, (insider_id, trade_type)).fetchall()

        if len(trades) < min_trades:
            continue

        split = int(len(trades) * train_frac)
        train = trades[:split]
        test = trades[split:]

        # Compute train stats per window, find best
        best_window = None
        best_sharpe = 0

        for w in WINDOWS:
            abn_idx = {"7d": 6, "30d": 8, "90d": 10}[w]
            abns = [sign * t[abn_idx] for t in train if t[abn_idx] is not None]
            if len(abns) < 3:
                continue
            avg = statistics.mean(abns)
            std = statistics.stdev(abns) if len(abns) > 1 else 0
            if std <= 0 or avg <= 0:
                continue
            sharpe = (avg / std) * ANNUALIZE[w]
            n_conf = max(0, 1.0 - 2.0 / len(abns))
            sharpe_adj = sharpe * n_conf
            if sharpe_adj >= min_sharpe and sharpe_adj > best_sharpe:
                best_sharpe = sharpe_adj
                best_window = w

        if best_window is None:
            continue

        # Only include insiders whose best window is 30d or 90d (viable for options)
        # 7d is too short for options — theta decay kills it
        if best_window not in ("30d", "90d"):
            continue

        # Build test trade list with stock prices
        test_trades = []
        for t in test:
            entry_price = t[11]  # tr.entry_price
            if entry_price is None or entry_price <= 0:
                continue
            filing_date_str = t[3]
            try:
                fd = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            test_trades.append({
                "trade_id": t[0],
                "ticker": t[1],
                "filing_date": fd,
                "stock_price": entry_price,
                "value": t[4],
            })

        if test_trades:
            results.append((insider_id, name, best_window, best_sharpe, test_trades))

    return results


def run_backtest(conn: sqlite3.Connection, trade_type: str, cache: dict,
                 max_trades: int = 0) -> list[OptionsTrade]:
    """Run options backtest for one side."""
    side_label = "BUY→CALL" if trade_type == "buy" else "SELL→PUT"
    logger.info("Loading qualified %s insiders...", trade_type)

    qualified = load_qualified_insiders(conn, trade_type)
    total_test_trades = sum(len(tt) for _, _, _, _, tt in qualified)

    logger.info("Found %d qualified %s insiders with %d test trades",
                len(qualified), trade_type, total_test_trades)

    results = []
    no_contract = 0
    processed = 0
    cache_saves = 0

    for insider_id, name, best_window, train_sharpe, test_trades in qualified:
        for trade in test_trades:
            if max_trades > 0 and processed >= max_trades:
                break

            processed += 1
            if processed % 50 == 0:
                logger.info("  %s: %d/%d processed, %d results, %d no-contract",
                            side_label, processed, total_test_trades, len(results), no_contract)
                # Save cache periodically
                cache_saves += 1
                if cache_saves % 5 == 0:
                    save_cache(cache)

            result = simulate_option_trade(
                ticker=trade["ticker"],
                insider_name=name,
                insider_id=insider_id,
                trade_type=trade_type,
                filing_date=trade["filing_date"],
                stock_price=trade["stock_price"],
                cache=cache,
            )

            if result:
                results.append(result)
            else:
                no_contract += 1

        if max_trades > 0 and processed >= max_trades:
            break

    save_cache(cache)
    logger.info("  %s complete: %d options trades, %d no-contract (%d total)",
                side_label, len(results), no_contract, processed)

    return results


def print_results(trades: list[OptionsTrade], label: str):
    """Print options backtest results."""
    if not trades:
        print(f"\n  {label}: No trades")
        return

    n = len(trades)
    wins = sum(1 for t in trades if t.win)
    wr = wins / n
    pnls = [t.pnl_pct for t in trades]
    avg_pnl = statistics.mean(pnls)
    med_pnl = statistics.median(pnls)
    std_pnl = statistics.stdev(pnls) if n > 1 else 0

    # Dollar P&L per contract
    dollar_pnls = [t.pnl_dollar for t in trades]
    avg_dollar = statistics.mean(dollar_pnls)
    total_dollar = sum(dollar_pnls)

    # Win/loss analysis
    win_trades = [t for t in trades if t.win]
    loss_trades = [t for t in trades if not t.win]
    avg_win = statistics.mean([t.pnl_pct for t in win_trades]) if win_trades else 0
    avg_loss = statistics.mean([t.pnl_pct for t in loss_trades]) if loss_trades else 0

    # Exit reasons
    from collections import Counter
    reasons = Counter(t.exit_reason for t in trades)

    # Distribution
    pct_50plus = sum(1 for p in pnls if p > 0.5) / n
    pct_100plus = sum(1 for p in pnls if p > 1.0) / n
    pct_neg50 = sum(1 for p in pnls if p < -0.5) / n
    pct_neg80 = sum(1 for p in pnls if p < -0.8) / n
    total_loss = sum(1 for p in pnls if p <= -0.99) / n  # total wipeout

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(f"  Trades: {n}")
    print(f"  Win Rate: {wr*100:.1f}%")
    print(f"  Avg P&L: {avg_pnl*100:+.1f}%  |  Median: {med_pnl*100:+.1f}%")
    print(f"  Std Dev: {std_pnl*100:.1f}%")
    print(f"  Avg Win: {avg_win*100:+.1f}%  |  Avg Loss: {avg_loss*100:+.1f}%")
    print(f"  Avg $/contract: ${avg_dollar:+.0f}  |  Total: ${total_dollar:+,.0f}")
    print(f"\n  Exit Reasons:")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count} ({count/n*100:.1f}%)")
    print(f"\n  Return Distribution:")
    print(f"    >+100% (profit target hit): {pct_100plus*100:.1f}%")
    print(f"    >+50%: {pct_50plus*100:.1f}%")
    print(f"    <-50%: {pct_neg50*100:.1f}%")
    print(f"    <-80%: {pct_neg80*100:.1f}%")
    print(f"    Total wipeout (>-99%): {total_loss*100:.1f}%")

    # Sharpe on premium returns (approximate annualization for ~60d hold)
    if std_pnl > 0:
        sharpe = (avg_pnl / std_pnl) * (252 / 60) ** 0.5
        print(f"\n  Sharpe (annualized, ~60d hold): {sharpe:.2f}")

    # By window (best_window doesn't apply per trade, but we can show by DTE buckets)
    print(f"\n  By DTE bucket:")
    for dte_label, dte_min, dte_max in [("<80", 0, 80), ("80-100", 80, 100), (">100", 100, 999)]:
        bucket = [t for t in trades if dte_min <= t.dte < dte_max]
        if not bucket:
            continue
        bwr = sum(1 for t in bucket if t.win) / len(bucket)
        bavg = statistics.mean([t.pnl_pct for t in bucket])
        print(f"    DTE {dte_label}: N={len(bucket)}, WR={bwr*100:.1f}%, Avg={bavg*100:+.1f}%")

    # Top 10 and bottom 10
    sorted_trades = sorted(trades, key=lambda t: t.pnl_pct, reverse=True)
    print(f"\n  Top 10 trades:")
    for t in sorted_trades[:10]:
        print(f"    {t.ticker:>6} {t.option_right} ${t.strike:.0f} {t.expiration} "
              f"| ${t.entry_price:.2f}→${t.exit_price:.2f} ({t.pnl_pct*100:+.0f}%) "
              f"| {t.insider_name[:25]} | {t.exit_reason}")

    print(f"\n  Bottom 10 trades:")
    for t in sorted_trades[-10:]:
        print(f"    {t.ticker:>6} {t.option_right} ${t.strike:.0f} {t.expiration} "
              f"| ${t.entry_price:.2f}→${t.exit_price:.2f} ({t.pnl_pct*100:+.0f}%) "
              f"| {t.insider_name[:25]} | {t.exit_reason}")


def main():
    parser = argparse.ArgumentParser(description="Options backtest with real Theta Data")
    parser.add_argument("--side", choices=["buy", "sell", "both"], default="both")
    parser.add_argument("--max-trades", type=int, default=0,
                        help="Max trades to process per side (0=all)")
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--min-sharpe", type=float, default=0.5)
    args = parser.parse_args()

    # Check Theta Data is running
    try:
        urllib.request.urlopen(f"{THETA_BASE}/v3/option/list/expirations?symbol=SPY", timeout=5)
    except Exception:
        logger.error("Theta Data not running at %s — start the terminal first", THETA_BASE)
        return

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cache = load_cache()
    logger.info("Loaded cache with %d entries", len(cache))

    sides = ["buy", "sell"] if args.side == "both" else [args.side]
    all_results = {}

    for side in sides:
        results = run_backtest(conn, side, cache, max_trades=args.max_trades)
        all_results[side] = results
        label = "BUY SIDE → CALLS (5% OTM, ~90 DTE)" if side == "buy" else "SELL SIDE → PUTS (5% OTM, ~90 DTE)"
        print_results(results, label)

    # Head to head if both sides
    if "buy" in all_results and "sell" in all_results:
        buy = all_results["buy"]
        sell = all_results["sell"]
        if buy and sell:
            print(f"\n{'='*70}")
            print(f"  HEAD-TO-HEAD: CALLS vs PUTS (Real Options Data)")
            print(f"{'='*70}")
            for label, trades in [("Calls (buy signal)", buy), ("Puts (sell signal)", sell)]:
                n = len(trades)
                wr = sum(1 for t in trades if t.win) / n if n > 0 else 0
                avg = statistics.mean([t.pnl_pct for t in trades]) if trades else 0
                med = statistics.median([t.pnl_pct for t in trades]) if trades else 0
                std = statistics.stdev([t.pnl_pct for t in trades]) if n > 1 else 0
                sh = (avg / std) * (252/60)**0.5 if std > 0 else 0
                print(f"  {label:<25} N={n:>5} | WR={wr*100:>5.1f}% | Avg={avg*100:>+6.1f}% | Med={med*100:>+6.1f}% | Sharpe={sh:>.2f}")
            print(f"{'='*70}\n")

    conn.close()
    save_cache(cache)
    logger.info("Cache saved with %d entries", len(cache))


if __name__ == "__main__":
    main()
