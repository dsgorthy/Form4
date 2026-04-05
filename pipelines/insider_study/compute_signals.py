#!/usr/bin/env python3
"""
Compute trade signal tags for the insider catalog.

Each detector function identifies a pattern (e.g., buying_the_dip, first_time_buyer)
and returns a list of (trade_id, signal_type, signal_label, signal_class, confidence, metadata_json)
tuples for batch insertion into trade_signals.

Usage:
    python3 pipelines/insider_study/compute_signals.py
    python3 pipelines/insider_study/compute_signals.py --since 2026-03-01
    python3 pipelines/insider_study/compute_signals.py --signal-type first_time_buyer
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

try:
    from pipelines.insider_study.price_utils import (
        load_prices as _load_prices,
        find_price as _find_price,
        compute_30d_change as _compute_30d_change,
        available_tickers as _available_tickers,
        PRICES_DIR,
    )
except ModuleNotFoundError:
    from price_utils import (
        load_prices as _load_prices,
        find_price as _find_price,
        compute_30d_change as _compute_30d_change,
        available_tickers as _available_tickers,
        PRICES_DIR,
    )

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Signal registry ────────────────────────────────────────────────────────

SIGNAL_REGISTRY: dict[str, callable] = {}


def register_signal(fn):
    SIGNAL_REGISTRY[fn.__name__] = fn
    return fn


# ─── Detector: first_time_buyer ──────────────────────────────────────────────

@register_signal
def first_time_buyer(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Insider's first-ever P-code buy in a given ticker."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          {where_since}
          AND t.trade_id = (
              SELECT MIN(t2.trade_id)
              FROM trades t2
              WHERE t2.insider_id = t.insider_id
                AND t2.ticker = t.ticker
                AND t2.trans_code = 'P'
          )
    """).fetchall()

    results = []
    for r in rows:
        results.append((
            r["trade_id"],
            "first_time_buyer",
            "First-Time Buyer",
            "bullish",
            0.8,
            json.dumps({"insider_name": r["insider_name"], "ticker": r["ticker"]}),
        ))
    logger.info("first_time_buyer: %d signals", len(results))
    return results


# ─── Detector: insider_returns ───────────────────────────────────────────────

@register_signal
def insider_returns(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """PIT blended_score >= 2.0 (A-grade insider at time of filing)."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker,
               t.pit_blended_score, t.pit_grade,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.pit_blended_score >= 2.0
          AND t.pit_blended_score IS NOT NULL
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        results.append((
            r["trade_id"],
            "insider_returns",
            "Proven Insider (PIT)",
            "bullish",
            min(r["pit_blended_score"] / 3.0, 1.0),
            json.dumps({"pit_score": r["pit_blended_score"], "pit_grade": r["pit_grade"]}),
        ))
    logger.info("insider_returns: %d signals", len(results))
    return results


# ─── Detector: size_anomaly ──────────────────────────────────────────────────

@register_signal
def size_anomaly(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Trade value > 2x insider's PIT average at this ticker.
    PIT fix: only compares against trades with trade_date BEFORE the current trade."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_type, t.trade_date,
               t.value, COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code IN ('P', 'S')
          AND t.value > 0
          {where_since}
        ORDER BY t.insider_id, t.ticker, t.trade_date
    """).fetchall()

    # Build PIT averages: for each trade, only use prior trades by same insider+ticker
    from collections import defaultdict
    history = defaultdict(list)  # (insider_id, ticker) -> [values]
    results = []
    for r in rows:
        key = (r["insider_id"], r["ticker"])
        prior = history[key]
        if len(prior) >= 2:
            avg_prior = sum(prior) / len(prior)
            if avg_prior > 0 and r["value"] > 2.0 * avg_prior:
                ratio = round(r["value"] / avg_prior, 1)
                signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
                results.append((
                    r["trade_id"],
                    "size_anomaly",
                    "Unusually Large Trade",
                    signal_class,
                    min(ratio / 5.0, 1.0),
                    json.dumps({"ratio": ratio, "avg_value": round(avg_prior), "n_prior": len(prior)}),
                ))
        history[key].append(r["value"])

    logger.info("size_anomaly: %d signals", len(results))
    return results


# ─── Detector: high_signal ───────────────────────────────────────────────────

@register_signal
def high_signal(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """C-suite trades with PIT grade A or A+. Uses pit_grade (PIT-safe) instead of signal_quality."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_type, t.value, t.pit_grade,
               t.is_csuite,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.pit_grade IN ('A+', 'A')
          AND t.is_csuite = 1
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
        conf = 1.0 if r["pit_grade"] == "A+" else 0.9
        results.append((
            r["trade_id"],
            "high_signal",
            "High-Quality Signal",
            signal_class,
            conf,
            json.dumps({"is_csuite": True, "pit_grade": r["pit_grade"]}),
        ))
    logger.info("high_signal: %d signals", len(results))
    return results


# ─── Detector: top_trade ─────────────────────────────────────────────────────

@register_signal
def top_trade(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Top 1% by value, OR top 5% PIT score, OR 3+ insider cluster.
    PIT fix: value and score thresholds use a 3-year rolling window, not full history.
    This prevents 2020 mega-buys from suppressing all post-2020 signals and accounts
    for inflation in trade sizes over time."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""

    ROLLING_YEARS = 3  # 3-year lookback for percentile thresholds

    # Load all trades with value, ordered by date
    all_trades = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_type, t.trade_date, t.value,
               t.insider_id
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          AND t.value > 0
          {where_since}
        ORDER BY t.trade_date
    """).fetchall()

    # Load PIT blended_scores from insider_ticker_scores (has as_of_date).
    # For each trade, we look up the insider's score at that ticker as of the trade date.
    from collections import defaultdict
    pit_scores = defaultdict(list)  # (insider_id, ticker) -> [(as_of_date, blended_score)]
    score_rows = conn.execute("""
        SELECT insider_id, ticker, as_of_date, blended_score
        FROM insider_ticker_scores
        WHERE blended_score IS NOT NULL
        ORDER BY insider_id, ticker, as_of_date
    """).fetchall()
    for r in score_rows:
        pit_scores[(r["insider_id"], r["ticker"])].append((r["as_of_date"], r["blended_score"]))

    # Find 3+ insider clusters using monthly windows
    cluster_trade_ids = set()
    cluster_windows = conn.execute(f"""
        SELECT ticker, trade_type, MIN(trade_date) AS win_start, MAX(trade_date) AS win_end
        FROM trades
        WHERE trans_code IN ('P', 'S')
          {where_since.replace('t.', '')}
        GROUP BY ticker, trade_type, strftime('%Y-%m', trade_date)
        HAVING COUNT(DISTINCT insider_id) >= 3
    """).fetchall()
    for cw in cluster_windows:
        cw_rows = conn.execute("""
            SELECT trade_id FROM trades
            WHERE ticker = ? AND trade_type = ?
              AND trade_date BETWEEN ? AND ?
              AND trans_code IN ('P', 'S')
        """, (cw["ticker"], cw["trade_type"], cw["win_start"], cw["win_end"])).fetchall()
        for r in cw_rows:
            cluster_trade_ids.add(r["trade_id"])

    # Compute rolling 3-year PIT percentiles efficiently:
    # Pre-compute annual p99 thresholds, then for each trade use the
    # threshold from the 3-year window ending at its trade year.
    # This avoids O(n^2) per-trade percentile computation.
    from datetime import datetime, timedelta
    from bisect import insort, bisect_left

    # Group values by year for fast percentile computation
    year_values = {}  # year -> sorted list of values
    for r in all_trades:
        try:
            yr = int(r["trade_date"][:4])
        except (TypeError, ValueError):
            continue
        if yr not in year_values:
            year_values[yr] = []
        year_values[yr].append(r["value"])

    # Sort each year's values for percentile computation
    for yr in year_values:
        year_values[yr].sort()

    def get_rolling_p99(trade_year):
        """Get 99th percentile from 3-year rolling window ending at trade_year."""
        all_vals = []
        for yr in range(trade_year - ROLLING_YEARS, trade_year):
            if yr in year_values:
                all_vals.extend(year_values[yr])
        if len(all_vals) < 100:
            return None
        all_vals.sort()
        idx = int(len(all_vals) * 0.99)
        return all_vals[min(idx, len(all_vals) - 1)]

    # Cache thresholds per year
    p99_cache = {}
    for yr in sorted(year_values.keys()):
        p99_cache[yr] = get_rolling_p99(yr)

    seen = set()
    results = []

    for r in all_trades:
        tid = r["trade_id"]

        try:
            yr = int(r["trade_date"][:4])
        except (TypeError, ValueError):
            continue

        # Value check: rolling 3-year p99
        threshold = p99_cache.get(yr)
        if threshold and r["value"] >= threshold and tid not in seen:
            seen.add(tid)
            signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
            results.append((
                tid, "top_trade", "Top Trade", signal_class, 0.9,
                json.dumps({"reason": "top_1pct_value", "value": r["value"],
                            "threshold": round(threshold)}),
            ))

        # Score check: PIT blended_score >= 2.0 (A grade) as of trade date
        if tid not in seen:
            key = (r["insider_id"], r["ticker"])
            scores_list = pit_scores.get(key, [])
            # Binary search for latest score as_of_date <= trade_date
            pit_score = None
            for s_date, s_val in reversed(scores_list):
                if s_date <= r["trade_date"]:
                    pit_score = s_val
                    break
            if pit_score is not None and pit_score >= 2.0:
                seen.add(tid)
                signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
                results.append((
                    tid, "top_trade", "Top Trade", signal_class, 0.85,
                    json.dumps({"reason": "top_pit_score", "score": round(pit_score, 3)}),
                ))

        # Cluster (already PIT — monthly grouping is inherently bounded)
        if tid in cluster_trade_ids and tid not in seen:
            seen.add(tid)
            results.append((
                tid, "top_trade", "Top Trade",
                "bullish", 0.8,
                json.dumps({"reason": "3plus_cluster"}),
            ))

    logger.info("top_trade: %d signals (rolling %d-year PIT percentiles)", len(results), ROLLING_YEARS)
    return results


# ─── Detector: post_vest_dump ────────────────────────────────────────────────

@register_signal
def post_vest_dump(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """S-code sell within 30 days of A-code grant, same insider+ticker."""
    where_since = f"AND s.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT DISTINCT s.trade_id, s.ticker, s.value,
               COALESCE(i.display_name, i.name) AS insider_name,
               a.trade_date AS grant_date
        FROM trades s
        JOIN trades a ON s.insider_id = a.insider_id AND s.ticker = a.ticker
        JOIN insiders i ON s.insider_id = i.insider_id
        WHERE s.trans_code = 'S'
          AND a.trans_code = 'A'
          AND s.trade_date BETWEEN a.trade_date AND date(a.trade_date, '+30 days')
          AND s.trade_id != a.trade_id
          {where_since}
    """).fetchall()

    results = []
    seen = set()
    for r in rows:
        if r["trade_id"] not in seen:
            seen.add(r["trade_id"])
            results.append((
                r["trade_id"],
                "post_vest_dump",
                "Post-Vest Sell",
                "noise",
                0.9,
                json.dumps({"grant_date": r["grant_date"]}),
            ))
    logger.info("post_vest_dump: %d signals", len(results))
    return results


# ─── Detector: exercise_and_sell ─────────────────────────────────────────────

@register_signal
def exercise_and_sell(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """M-code exercise + S-code sale within 3 days, same insider+ticker."""
    where_since = f"AND s.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT DISTINCT s.trade_id, s.ticker, s.value,
               COALESCE(i.display_name, i.name) AS insider_name,
               m.trade_date AS exercise_date
        FROM trades s
        JOIN trades m ON s.insider_id = m.insider_id AND s.ticker = m.ticker
        JOIN insiders i ON s.insider_id = i.insider_id
        WHERE s.trans_code = 'S'
          AND m.trans_code = 'M'
          AND s.trade_date BETWEEN m.trade_date AND date(m.trade_date, '+3 days')
          AND s.trade_id != m.trade_id
          {where_since}
    """).fetchall()

    results = []
    seen = set()
    for r in rows:
        if r["trade_id"] not in seen:
            seen.add(r["trade_id"])
            results.append((
                r["trade_id"],
                "exercise_and_sell",
                "Exercise & Sell",
                "noise",
                0.95,
                json.dumps({"exercise_date": r["exercise_date"]}),
            ))
    logger.info("exercise_and_sell: %d signals", len(results))
    return results


# ─── Detector: trend_reversal ────────────────────────────────────────────────

@register_signal
def trend_reversal(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Insider who sold for 12+ months switches to buying (or vice versa)."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    # Find buys where the insider only sold in the prior 12 months
    buy_reversals = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          {where_since}
          AND EXISTS (
              SELECT 1 FROM trades t2
              WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
                AND t2.trade_type = 'sell'
                AND t2.trade_date BETWEEN date(t.trade_date, '-12 months') AND date(t.trade_date, '-1 day')
              GROUP BY t2.insider_id
              HAVING COUNT(*) >= 2
          )
          AND NOT EXISTS (
              SELECT 1 FROM trades t3
              WHERE t3.insider_id = t.insider_id AND t3.ticker = t.ticker
                AND t3.trade_type = 'buy'
                AND t3.trade_date BETWEEN date(t.trade_date, '-12 months') AND date(t.trade_date, '-1 day')
          )
    """).fetchall()

    results = []
    for r in buy_reversals:
        results.append((
            r["trade_id"],
            "trend_reversal",
            "Sell-to-Buy Reversal",
            "bullish",
            0.85,
            json.dumps({"direction": "sell_to_buy", "ticker": r["ticker"]}),
        ))

    # Find sells where the insider only bought in the prior 12 months
    sell_reversals = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'S'
          {where_since}
          AND EXISTS (
              SELECT 1 FROM trades t2
              WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
                AND t2.trade_type = 'buy'
                AND t2.trade_date BETWEEN date(t.trade_date, '-12 months') AND date(t.trade_date, '-1 day')
              GROUP BY t2.insider_id
              HAVING COUNT(*) >= 2
          )
          AND NOT EXISTS (
              SELECT 1 FROM trades t3
              WHERE t3.insider_id = t.insider_id AND t3.ticker = t.ticker
                AND t3.trade_type = 'sell'
                AND t3.trade_date BETWEEN date(t.trade_date, '-12 months') AND date(t.trade_date, '-1 day')
          )
    """).fetchall()

    for r in sell_reversals:
        results.append((
            r["trade_id"],
            "trend_reversal",
            "Buy-to-Sell Reversal",
            "bearish",
            0.85,
            json.dumps({"direction": "buy_to_sell", "ticker": r["ticker"]}),
        ))

    logger.info("trend_reversal: %d signals", len(results))
    return results


# ─── Detector: buying_the_dip ───────────────────────────────────────────────

@register_signal
def buying_the_dip(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy where stock is down >10% in prior 30 days."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    # Only fetch tickers that have price files
    avail_tickers = _available_tickers()
    if not avail_tickers:
        logger.info("buying_the_dip: 0 signals (no price files)")
        return []

    placeholders = ",".join("?" * len(avail_tickers))
    tickers = list(avail_tickers)
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.ticker IN ({placeholders})
          {where_since}
    """, tickers).fetchall()

    results = []
    for r in rows:
        prices = _load_prices(r["ticker"])
        change = _compute_30d_change(prices, r["trade_date"])
        if change is not None and change <= -0.10:
            results.append((
                r["trade_id"],
                "buying_the_dip",
                "Buying the Dip",
                "bullish",
                min(abs(change), 1.0),
                json.dumps({"drawdown_30d": round(change * 100, 1)}),
            ))

    logger.info("buying_the_dip: %d signals", len(results))
    return results


# ─── Detector: selling_the_rip ───────────────────────────────────────────────

@register_signal
def selling_the_rip(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """S-code non-10b5-1 sell where stock is up >15% in prior 30 days."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    avail_tickers = _available_tickers()
    if not avail_tickers:
        logger.info("selling_the_rip: 0 signals (no price files)")
        return []

    placeholders = ",".join("?" * len(avail_tickers))
    tickers = list(avail_tickers)
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date
        FROM trades t
        WHERE t.trans_code = 'S'
          AND (t.is_10b5_1 = 0 OR t.is_10b5_1 IS NULL)
          AND (t.is_routine = 0 OR t.is_routine IS NULL)
          AND t.ticker IN ({placeholders})
          {where_since}
    """, tickers).fetchall()

    results = []
    for r in rows:
        prices = _load_prices(r["ticker"])
        change = _compute_30d_change(prices, r["trade_date"])
        if change is not None and change >= 0.15:
            results.append((
                r["trade_id"],
                "selling_the_rip",
                "Selling the Rip",
                "bearish",
                min(change, 1.0),
                json.dumps({"rally_30d": round(change * 100, 1)}),
            ))

    logger.info("selling_the_rip: %d signals", len(results))
    return results


# ─── Detector: contrarian ────────────────────────────────────────────────────

@register_signal
def contrarian(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy while SPY (broad market) down >5% in 30 days."""
    spy_prices = _load_prices("SPY")
    if not spy_prices:
        logger.info("contrarian: 0 signals (no SPY price data)")
        return []

    # Precompute SPY 30d changes for all trade dates
    from datetime import datetime, timedelta
    spy_changes: dict[str, float] = {}
    for date_str in spy_prices:
        change = _compute_30d_change(spy_prices, date_str)
        if change is not None:
            spy_changes[date_str] = change

    # Only dates where SPY was down >5%
    dip_dates = {d for d, c in spy_changes.items() if c <= -0.05}
    if not dip_dates:
        logger.info("contrarian: 0 signals (no market dip periods)")
        return []

    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date
        FROM trades t
        WHERE t.trans_code = 'P'
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        td = r["trade_date"]
        # Check if trade date (or nearby) falls in a dip period
        change = spy_changes.get(td)
        if change is None:
            # Try nearby dates
            try:
                dt = datetime.strptime(td, "%Y-%m-%d")
                for off in range(1, 4):
                    nearby = (dt - timedelta(days=off)).strftime("%Y-%m-%d")
                    if nearby in spy_changes:
                        change = spy_changes[nearby]
                        break
            except ValueError:
                continue
        if change is not None and change <= -0.05:
            results.append((
                r["trade_id"],
                "contrarian",
                "Contrarian Buy",
                "bullish",
                min(abs(change) * 2, 1.0),
                json.dumps({"market_drawdown_30d": round(change * 100, 1), "benchmark": "SPY"}),
            ))

    logger.info("contrarian: %d signals", len(results))
    return results


# ─── Detector: large_holdings_increase ──────────────────────────────────────

@register_signal
def large_holdings_increase(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy that increases the insider's holdings by >=10%.
    Requires minimum $10K trade value and DIRECT ownership only.
    Indirect holdings (I) are excluded because shares_owned_after only
    reflects one entity, not the insider's total position — inflating the %."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.qty, t.shares_owned_after, t.trade_type, t.value
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.shares_owned_after IS NOT NULL
          AND t.shares_owned_after > 0
          AND t.qty > 0
          AND t.value >= 10000
          AND (t.direct_indirect = 'D' OR t.direct_indirect IS NULL)
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        shares_after = r["shares_owned_after"]
        qty = r["qty"]
        shares_before = shares_after - qty
        if shares_before <= 0:
            continue
        pct = (qty / shares_before) * 100
        if pct >= 100:
            results.append((
                r["trade_id"],
                "large_holdings_increase",
                "Massive Holdings Increase (>100%)",
                "bullish",
                1.0,
                json.dumps({"pct_increase": round(pct, 1)}),
            ))
        elif pct >= 50:
            results.append((
                r["trade_id"],
                "large_holdings_increase",
                "Major Holdings Increase (>50%)",
                "bullish",
                0.9,
                json.dumps({"pct_increase": round(pct, 1)}),
            ))
        elif pct >= 10:
            results.append((
                r["trade_id"],
                "large_holdings_increase",
                "Large Holdings Increase",
                "bullish",
                min(pct / 50, 0.8),
                json.dumps({"pct_increase": round(pct, 1)}),
            ))

    logger.info("large_holdings_increase: %d signals", len(results))
    return results


# ─── Detector: small_holdings_increase (noise) ─────────────────────────────

@register_signal
def small_holdings_increase(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy that increases holdings by <1%. Research shows significantly worse returns."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.qty, t.shares_owned_after
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.shares_owned_after IS NOT NULL
          AND t.shares_owned_after > 0
          AND t.qty > 0
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        shares_before = r["shares_owned_after"] - r["qty"]
        if shares_before <= 0:
            continue
        pct = (r["qty"] / shares_before) * 100
        if pct < 1:
            results.append((
                r["trade_id"],
                "small_holdings_increase",
                "Minimal Holdings Increase",
                "noise",
                0.7,
                json.dumps({"pct_increase": round(pct, 3)}),
            ))

    logger.info("small_holdings_increase: %d signals", len(results))
    return results


# ─── Detector: ten_pct_owner_buy ────────────────────────────────────────────

@register_signal
def ten_pct_owner_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Flag 10% Owner purchases. Pooled 10% Owner buys have no alpha (noise).
    Exception: activists with proven PIT track records (>=10 prior trades, avg abnormal > 2%).
    PIT fix: activist classification uses only returns from trades BEFORE the current trade."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""

    # Load all 10% owner P-code trades with returns, ordered by date
    all_rows = conn.execute("""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date,
               tr.abnormal_7d,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code = 'P'
          AND (t.title LIKE '%10%%' OR t.title LIKE '%TenPercent%')
          AND t.is_csuite = 0
        ORDER BY t.insider_id, t.trade_date
    """).fetchall()

    # Build PIT activist classification: for each trade, only use prior returns
    from collections import defaultdict
    insider_returns = defaultdict(list)  # insider_id -> [(abnormal_7d)]
    results = []

    for r in all_rows:
        iid = r["insider_id"]
        prior = insider_returns[iid]

        # Apply since filter for output
        if since and r["trade_date"] < since:
            # Still accumulate returns for PIT, but don't emit signal
            if r["abnormal_7d"] is not None:
                insider_returns[iid].append(r["abnormal_7d"])
            continue

        # PIT activist check: >= 10 prior trades with avg abnormal > 2%
        is_activist = False
        if len(prior) >= 10:
            avg_abn = sum(prior) / len(prior)
            is_activist = avg_abn > 0.02

        if is_activist:
            results.append((
                r["trade_id"],
                "ten_pct_owner_buy",
                "Activist Investor",
                "bullish",
                0.8,
                json.dumps({"insider_name": r["insider_name"], "is_activist": True,
                            "pit_n": len(prior), "pit_avg_abn": round(sum(prior)/len(prior)*100, 1)}),
            ))
        else:
            results.append((
                r["trade_id"],
                "ten_pct_owner_buy",
                "10% Owner Purchase",
                "noise",
                0.5,
                json.dumps({"insider_name": r["insider_name"], "is_activist": False}),
            ))

        # Accumulate this trade's return for future PIT lookups
        if r["abnormal_7d"] is not None:
            insider_returns[iid].append(r["abnormal_7d"])

    logger.info("ten_pct_owner_buy: %d signals", len(results))
    return results


# ─── Detector: opportunistic_trade ──────────────────────────────────────────

@register_signal
def opportunistic_trade(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Flag opportunistic trades using Cohen 3-year calendar-month classification.
    Opportunistic buys outperform routine by +1.8% at 30d (validated on our data)."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.trade_type, t.cohen_routine
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
          AND t.cohen_routine IS NOT NULL
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        if r["cohen_routine"] == 0:
            # Opportunistic — higher signal
            if r["trade_type"] == "buy":
                results.append((
                    r["trade_id"],
                    "opportunistic_trade",
                    "Opportunistic Buy",
                    "bullish",
                    0.75,
                    json.dumps({"cohen_routine": False}),
                ))
            else:
                results.append((
                    r["trade_id"],
                    "opportunistic_trade",
                    "Opportunistic Sell",
                    "bearish",
                    0.75,
                    json.dumps({"cohen_routine": False}),
                ))

    logger.info("opportunistic_trade: %d signals", len(results))
    return results


# ─── Detector: deep_dip_buy ──────────────────────────────────────────────────

@register_signal
def deep_dip_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy where stock is down >20% in 3 months or >30% in 1 year.
    CW's highest-conviction pattern. Tiered confidence by dip depth."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date, t.dip_1mo, t.dip_3mo, t.dip_1yr
        FROM trades t
        WHERE t.trans_code = 'P'
          AND (t.dip_3mo <= -0.20 OR t.dip_1yr <= -0.30)
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        depths = []
        if r["dip_1mo"] is not None and r["dip_1mo"] <= -0.15:
            depths.append(("1mo", r["dip_1mo"]))
        if r["dip_3mo"] is not None and r["dip_3mo"] <= -0.20:
            depths.append(("3mo", r["dip_3mo"]))
        if r["dip_1yr"] is not None and r["dip_1yr"] <= -0.30:
            depths.append(("1yr", r["dip_1yr"]))
        if not depths:
            continue
        worst = min(d[1] for d in depths)
        confidence = min(1.0, abs(worst))
        results.append((
            r["trade_id"], "deep_dip_buy", "Deep Dip Buy", "bullish", confidence,
            json.dumps({k: round(v * 100, 1) for k, v in depths}),
        ))

    logger.info("deep_dip_buy: %d signals", len(results))
    return results


# ─── Detector: reversal_buy ──────────────────────────────────────────────────

@register_signal
def reversal_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Insider with 5+ consecutive sells now buying. CW's highest signal type.
    'He sold 21 times in 13 years and now he's buying for the first time.'"""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date,
               t.consecutive_sells_before, t.insider_switch_rate,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.is_rare_reversal = 1
          AND t.consecutive_sells_before >= 5
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        sells = r["consecutive_sells_before"]
        # More consecutive sells = higher confidence
        confidence = min(1.0, 0.6 + (sells / 50.0))
        results.append((
            r["trade_id"], "reversal_buy", "Reversal Buy", "bullish", confidence,
            json.dumps({
                "consecutive_sells": sells,
                "switch_rate": round(r["insider_switch_rate"] or 0, 3),
                "insider": r["insider_name"],
            }),
        ))

    logger.info("reversal_buy: %d signals", len(results))
    return results


# ─── Detector: momentum_buy ──────────────────────────────────────────────────

@register_signal
def momentum_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """P-code buy while stock is above both SMA50 and SMA200 — momentum context."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.sma50_rel, t.sma200_rel
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.above_sma50 = 1
          AND t.above_sma200 = 1
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        confidence = 0.6
        results.append((
            r["trade_id"], "momentum_buy", "Momentum Buy", "bullish", confidence,
            json.dumps({
                "sma50_rel": round(r["sma50_rel"] or 0, 3),
                "sma200_rel": round(r["sma200_rel"] or 0, 3),
            }),
        ))

    logger.info("momentum_buy: %d signals", len(results))
    return results


# ─── Detector: largest_purchase_ever ──────────────────────────────────────────

@register_signal
def largest_purchase_ever(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Insider's largest purchase ever at this ticker. CW highlights this heavily."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.value, t.purchase_size_ratio,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.is_largest_ever = 1
          AND t.purchase_size_ratio > 1.5
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        ratio = r["purchase_size_ratio"] or 1.0
        confidence = min(1.0, 0.5 + (ratio / 10.0))
        results.append((
            r["trade_id"], "largest_purchase_ever", "Largest Purchase Ever", "bullish",
            confidence,
            json.dumps({
                "size_ratio": round(ratio, 2),
                "value": r["value"],
                "insider": r["insider_name"],
            }),
        ))

    logger.info("largest_purchase_ever: %d signals", len(results))
    return results


# ─── Detector: recurring_buyer_noise ──────────────────────────────────────────

@register_signal
def recurring_buyer_noise(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Insider buys on a detected schedule — low signal (noise).
    CW filters these out even without a 10b5-1 flag."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.recurring_period
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.is_recurring = 1
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        results.append((
            r["trade_id"], "recurring_buyer_noise", "Recurring Purchase", "noise", 0.3,
            json.dumps({"period": r["recurring_period"]}),
        ))

    logger.info("recurring_buyer_noise: %d signals", len(results))
    return results


# ─── Detector: tax_sale_noise ──────────────────────────────────────────────

@register_signal
def tax_sale_noise(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Tax-motivated sale (Nov/Dec, at a loss). CW's key differentiator:
    'Those seven sales were all tax sales. They have no signal at all.'"""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker
        FROM trades t
        WHERE t.trans_code = 'S'
          AND t.is_tax_sale = 1
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        results.append((
            r["trade_id"], "tax_sale_noise", "Tax Sale", "noise", 0.2,
            json.dumps({}),
        ))

    logger.info("tax_sale_noise: %d signals", len(results))
    return results


# ─── Composite: quality_momentum_buy ─────────────────────────────────────────

@register_signal
def quality_momentum_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """COMPOSITE: A+/A PIT grade insider buying in confirmed uptrend (above SMA50+SMA200).
    Validated post-2021: 57-74% WR, +2.4-5.5% abnormal at 30d. Signal compounds.
    ~50 events/year. ZERO overlap with reversal or 10b5-1 strategies."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.pit_grade, t.pit_blended_score,
               t.sma50_rel, t.sma200_rel,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.pit_grade IN ('A+', 'A')
          AND t.above_sma50 = 1
          AND t.above_sma200 = 1
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        conf = 0.95 if r["pit_grade"] == "A+" else 0.85
        results.append((
            r["trade_id"],
            "quality_momentum_buy",
            "Quality + Momentum",
            "bullish",
            conf,
            json.dumps({
                "pit_grade": r["pit_grade"],
                "pit_score": round(r["pit_blended_score"] or 0, 2),
                "sma50_rel": round(r["sma50_rel"] or 0, 3),
                "sma200_rel": round(r["sma200_rel"] or 0, 3),
                "insider": r["insider_name"],
                "thesis": "Proven insider buying in confirmed uptrend",
            }),
        ))

    logger.info("quality_momentum_buy: %d signals", len(results))
    return results


# ─── Composite: tenb51_surprise_buy ──────────────────────────────────────────

@register_signal
def tenb51_surprise_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """COMPOSITE: Insider with 5+ prior 10b5-1 plan sells on same ticker breaks pattern and buys.
    Validated post-2021: +2.89% at 30d, +4.48% at 90d. Signal COMPOUNDS — genuine rerating.
    ~40 events/year. Breaking a legal SEC commitment to buy is costly and deliberate.
    Only reliable post-Dec 2022 (10b5-1 disclosure reform)."""
    where_since = f"AND t.filing_date >= '{since}'" if since else ""

    # Load all 10b5-1 sells ordered by filing_date for PIT accumulation
    sells = conn.execute("""
        SELECT insider_id, ticker, filing_date
        FROM trades
        WHERE trans_code = 'S' AND is_10b5_1 = 1
        ORDER BY filing_date
    """).fetchall()

    # Build cumulative count: (insider_id, ticker) -> [(filing_date, cum_count)]
    from collections import defaultdict
    from bisect import bisect_left

    sell_counts: dict[tuple, list[tuple]] = defaultdict(list)
    running: dict[tuple, int] = defaultdict(int)
    for s in sells:
        key = (s["insider_id"], s["ticker"])
        running[key] += 1
        sell_counts[key].append((s["filing_date"], running[key]))

    # Load all P-code buys
    buys = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.filing_date,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          {where_since}
    """).fetchall()

    results = []
    for r in buys:
        key = (r["insider_id"], r["ticker"])
        history = sell_counts.get(key)
        if not history:
            continue

        # PIT: count 10b5-1 sells filed strictly BEFORE this buy's filing_date
        dates = [h[0] for h in history]
        idx = bisect_left(dates, r["filing_date"]) - 1
        if idx < 0:
            continue

        count = history[idx][1]
        if count >= 5:
            confidence = min(1.0, 0.7 + (count / 50.0))
            results.append((
                r["trade_id"],
                "tenb51_surprise_buy",
                "10b5-1 Surprise Buy",
                "bullish",
                confidence,
                json.dumps({
                    "prior_10b5_1_sells": count,
                    "insider": r["insider_name"],
                    "thesis": "Scheduled seller broke legal commitment to buy",
                }),
            ))

    logger.info("tenb51_surprise_buy: %d signals", len(results))
    return results


# ─── Composite: deep_reversal_dip_buy ────────────────────────────────────────

@register_signal
def deep_reversal_dip_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """COMPOSITE: 10+ consecutive sells then buy + 3-month dip ≤ -25%.
    Validated post-2021: Sharpe 1.08, +2.62% abnormal at 30d. Mean reversion — 30d hold ONLY.
    ~20 events/year. ZERO overlap with quality_momentum.
    CRITICAL: Do NOT add momentum/SMA filters — factor flips sign across regimes."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date,
               t.consecutive_sells_before, t.dip_3mo,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.is_rare_reversal = 1
          AND t.consecutive_sells_before >= 10
          AND t.dip_3mo <= -0.25
          AND (t.is_recurring = 0 OR t.is_recurring IS NULL)
          AND (t.is_tax_sale = 0 OR t.is_tax_sale IS NULL)
          AND (t.is_10b5_1 = 0 OR t.is_10b5_1 IS NULL)
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        sells = r["consecutive_sells_before"]
        dip = r["dip_3mo"]
        confidence = min(1.0, 0.7 + abs(dip) * 0.5 + (sells / 100.0))
        results.append((
            r["trade_id"],
            "deep_reversal_dip_buy",
            "Deep Reversal + Dip",
            "bullish",
            confidence,
            json.dumps({
                "consecutive_sells": sells,
                "dip_3mo": round(dip * 100, 1),
                "insider": r["insider_name"],
                "thesis": "Persistent seller reverses into depressed stock",
            }),
        ))

    logger.info("deep_reversal_dip_buy: %d signals", len(results))
    return results


# ─── Composite: reversal_quality_buy ──────────────────────────────────────────

@register_signal
def reversal_quality_buy(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """COMPOSITE: Rare reversal (80%+ sell history) by A+/A/B PIT-grade insider.
    Validated post-2021: +4.0% abnormal at 30d, 61.3% WR (N=354). Strongest single
    signal found. Robust across years, not driven by repeat insiders.
    ~70 events/year. Overlaps partially with deep_reversal_dip but distinct thesis."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_date,
               t.pit_grade, t.insider_switch_rate,
               t.consecutive_sells_before,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.is_rare_reversal = 1
          AND t.pit_grade IN ('A+', 'A', 'B')
          AND (t.is_recurring = 0 OR t.is_recurring IS NULL)
          AND (t.is_tax_sale = 0 OR t.is_tax_sale IS NULL)
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        grade = r["pit_grade"]
        conf = 0.95 if grade in ("A+", "A") else 0.80
        results.append((
            r["trade_id"],
            "reversal_quality_buy",
            "Rare Reversal + Quality",
            "bullish",
            conf,
            json.dumps({
                "pit_grade": grade,
                "consecutive_sells": r["consecutive_sells_before"],
                "switch_rate": round(r["insider_switch_rate"] or 0, 3),
                "insider": r["insider_name"],
                "thesis": "Proven insider breaks persistent sell pattern to buy",
            }),
        ))

    logger.info("reversal_quality_buy: %d signals", len(results))
    return results


# ─── Orchestrator ────────────────────────────────────────────────────────────

def run_detector(conn: sqlite3.Connection, name: str, fn, since: str | None) -> int:
    """Run one detector: clear old results, compute new, insert."""
    # Idempotent: delete existing signals of this type
    conn.execute("DELETE FROM trade_signals WHERE signal_type = ?", (name,))

    rows = fn(conn, since)
    if rows:
        conn.executemany(
            """INSERT OR IGNORE INTO trade_signals
               (trade_id, signal_type, signal_label, signal_class, confidence, metadata)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
    conn.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Compute trade signal tags")
    parser.add_argument("--since", help="Only process trades since this date (YYYY-MM-DD)")
    parser.add_argument("--signal-type", help="Run a single signal detector by name")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # Ensure trade_signals table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            signal_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id     INTEGER NOT NULL REFERENCES trades(trade_id),
            signal_type  TEXT    NOT NULL,
            signal_label TEXT    NOT NULL,
            signal_class TEXT    NOT NULL DEFAULT 'bullish',
            confidence   REAL    NOT NULL DEFAULT 1.0,
            metadata     TEXT,
            computed_at  TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(trade_id, signal_type)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_trade ON trade_signals(trade_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_type  ON trade_signals(signal_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_class ON trade_signals(signal_class)")
    conn.commit()

    if args.signal_type:
        if args.signal_type not in SIGNAL_REGISTRY:
            logger.error("Unknown signal type: %s. Available: %s",
                        args.signal_type, ", ".join(SIGNAL_REGISTRY))
            sys.exit(1)
        detectors = {args.signal_type: SIGNAL_REGISTRY[args.signal_type]}
    else:
        detectors = SIGNAL_REGISTRY

    total = 0
    for name, fn in detectors.items():
        logger.info("Running detector: %s", name)
        try:
            n = run_detector(conn, name, fn, args.since)
            total += n
        except Exception:
            logger.exception("Detector %s failed", name)

    # Summary
    summary = conn.execute("""
        SELECT signal_type, signal_class, COUNT(*) AS cnt
        FROM trade_signals
        GROUP BY signal_type, signal_class
        ORDER BY cnt DESC
    """).fetchall()

    logger.info("=== Signal Summary ===")
    for row in summary:
        logger.info("  %-25s %-10s %d", row["signal_type"], row["signal_class"], row["cnt"])
    logger.info("Total signals computed: %d", total)

    conn.close()


if __name__ == "__main__":
    main()
