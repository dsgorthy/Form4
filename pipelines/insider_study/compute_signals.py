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
    """PIT score >= 2.0 with win rate > 60%."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker,
               itr.score, itr.buy_win_rate_7d,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.trans_code = 'P'
          AND itr.score >= 2.0
          AND itr.buy_win_rate_7d > 0.6
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        results.append((
            r["trade_id"],
            "insider_returns",
            "Proven Insider",
            "bullish",
            min(r["score"] / 3.0, 1.0),
            json.dumps({"score": r["score"], "win_rate_7d": r["buy_win_rate_7d"]}),
        ))
    logger.info("insider_returns: %d signals", len(results))
    return results


# ─── Detector: size_anomaly ──────────────────────────────────────────────────

@register_signal
def size_anomaly(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Trade value > 2x insider's average at this ticker."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        WITH insider_avg AS (
            SELECT insider_id, ticker, AVG(value) AS avg_value, COUNT(*) AS n
            FROM trades
            WHERE trans_code IN ('P', 'S')
            GROUP BY insider_id, ticker
            HAVING COUNT(*) >= 2
        )
        SELECT t.trade_id, t.ticker, t.trade_type, t.value,
               ia.avg_value, ia.n,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insider_avg ia ON t.insider_id = ia.insider_id AND t.ticker = ia.ticker
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.value > 2.0 * ia.avg_value
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
        ratio = round(r["value"] / r["avg_value"], 1)
        results.append((
            r["trade_id"],
            "size_anomaly",
            "Unusually Large Trade",
            signal_class,
            min(ratio / 5.0, 1.0),
            json.dumps({"ratio": ratio, "avg_value": round(r["avg_value"]), "n_prior": r["n"]}),
        ))
    logger.info("size_anomaly: %d signals", len(results))
    return results


# ─── Detector: high_signal ───────────────────────────────────────────────────

@register_signal
def high_signal(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """signal_quality >= 0.8, C-suite, primary company, above-avg size."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""
    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_type, t.value, t.signal_quality,
               t.is_csuite,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.signal_quality >= 0.8
          AND t.is_csuite = 1
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
        results.append((
            r["trade_id"],
            "high_signal",
            "High-Quality Signal",
            signal_class,
            r["signal_quality"],
            json.dumps({"is_csuite": True, "signal_quality": r["signal_quality"]}),
        ))
    logger.info("high_signal: %d signals", len(results))
    return results


# ─── Detector: top_trade ─────────────────────────────────────────────────────

@register_signal
def top_trade(conn: sqlite3.Connection, since: str | None = None) -> list[tuple]:
    """Top 1% by value, OR top 5% PIT score, OR 3+ insider cluster."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""

    # Get value threshold (99th percentile)
    val_row = conn.execute("""
        SELECT value FROM trades
        WHERE trans_code IN ('P', 'S')
        ORDER BY value DESC
        LIMIT 1 OFFSET (SELECT COUNT(*) / 100 FROM trades WHERE trans_code IN ('P', 'S'))
    """).fetchone()
    val_threshold = val_row["value"] if val_row else 1e12

    # Get score threshold (95th percentile)
    score_row = conn.execute("""
        SELECT score FROM insider_track_records
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT 1 OFFSET (SELECT COUNT(*) / 20 FROM insider_track_records WHERE score IS NOT NULL)
    """).fetchone()
    score_threshold = score_row["score"] if score_row else 100

    # Find 3+ insider clusters using monthly windows for efficiency
    # Group trades by ticker, trade_type, and month — check if 3+ distinct insiders
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

    # Top value trades
    value_rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_type, t.value
        FROM trades t
        WHERE t.value >= ?
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """, (val_threshold,)).fetchall()

    # Top score trades
    score_rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.trade_type, itr.score
        FROM trades t
        JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE itr.score >= ?
          AND t.trans_code IN ('P', 'S')
          {where_since}
    """, (score_threshold,)).fetchall()

    seen = set()
    results = []

    for r in value_rows:
        if r["trade_id"] not in seen:
            seen.add(r["trade_id"])
            signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
            results.append((
                r["trade_id"], "top_trade", "Top Trade",
                signal_class, 0.9,
                json.dumps({"reason": "top_1pct_value", "value": r["value"]}),
            ))

    for r in score_rows:
        if r["trade_id"] not in seen:
            seen.add(r["trade_id"])
            signal_class = "bullish" if r["trade_type"] == "buy" else "bearish"
            results.append((
                r["trade_id"], "top_trade", "Top Trade",
                signal_class, 0.85,
                json.dumps({"reason": "top_5pct_score", "score": r["score"]}),
            ))

    for tid in cluster_trade_ids:
        if tid not in seen:
            seen.add(tid)
            results.append((
                tid, "top_trade", "Top Trade",
                "bullish", 0.8,
                json.dumps({"reason": "3plus_cluster"}),
            ))

    logger.info("top_trade: %d signals", len(results))
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
    Exception: specific activist investors with proven track records get neutral/bullish."""
    where_since = f"AND t.trade_date >= '{since}'" if since else ""

    # Get known high-alpha 10% owners (from our Phase 1 validation)
    # These are investors whose pooled 7d abnormal return is significantly positive
    activist_ids = set()
    activist_rows = conn.execute("""
        SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name,
               AVG(tr.abnormal_7d) AS avg_abn, COUNT(*) AS n
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code = 'P'
          AND (t.title LIKE '%10%%' OR t.title LIKE '%TenPercent%')
          AND t.is_csuite = 0
          AND tr.abnormal_7d IS NOT NULL
        GROUP BY i.insider_id
        HAVING n >= 10 AND avg_abn > 0.02
    """).fetchall()
    for r in activist_rows:
        activist_ids.add(r["insider_id"])

    rows = conn.execute(f"""
        SELECT t.trade_id, t.ticker, t.insider_id,
               COALESCE(i.display_name, i.name) AS insider_name
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND (t.title LIKE '%10%%' OR t.title LIKE '%TenPercent%')
          AND t.is_csuite = 0
          {where_since}
    """).fetchall()

    results = []
    for r in rows:
        if r["insider_id"] in activist_ids:
            results.append((
                r["trade_id"],
                "ten_pct_owner_buy",
                "Activist Investor",
                "bullish",
                0.8,
                json.dumps({"insider_name": r["insider_name"], "is_activist": True}),
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

    logger.info("ten_pct_owner_buy: %d signals (%d activists)", len(results), len(activist_ids))
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
