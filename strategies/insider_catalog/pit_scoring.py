#!/usr/bin/env python3
"""
Point-in-Time (PIT) scoring engine for insider-ticker pairs.

Phase C of the Data Quality & Scoring Redesign.

Computes per-insider-per-ticker scores using ONLY data available at the
as_of_date — no look-ahead bias. Blends ticker-specific and global
(cross-ticker) track records with adaptive weighting based on sample size.

Key properties:
  - Point-in-time: only uses trades + returns observable as of as_of_date
  - Per-ticker: separates insider's performance at each company
  - Blended: combines ticker-specific signal with global track record
  - Role-adjusted: CEO at their own company gets a boost

Usage:
    from pit_scoring import compute_insider_ticker_score

    score = compute_insider_ticker_score(conn, insider_id=42, ticker="AAPL",
                                         as_of_date="2024-06-15")
"""

from __future__ import annotations

import logging
import math
import sqlite3
import statistics
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _score_window(wr: float | None, avg_abn: float | None, n: int) -> float:
    """
    Compute a single-window quality score (0 to 1).
    Same logic as backfill._score_window but with explicit N-penalty.
    """
    if wr is None or avg_abn is None or n < 3:
        return 0.0
    wr_part = max(0, (wr - 0.4)) * 2.5
    ret_part = max(0, min(1.0, avg_abn * 10 + 0.5))
    n_confidence = max(0, 1.0 - 2.0 / n)
    return (wr_part * 0.5 + ret_part * 0.5) * n_confidence


def _compute_aggregate(returns: list[float]) -> dict:
    """Compute aggregate stats from a list of returns."""
    if not returns:
        return {"n": 0, "win_rate": None, "avg_return": None, "score": 0.0}

    n = len(returns)
    win_rate = sum(1 for r in returns if r > 0) / n
    avg_return = statistics.mean(returns)

    return {
        "n": n,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "score": _score_window(win_rate, avg_return, n),
    }


def compute_insider_ticker_score(
    conn: sqlite3.Connection,
    insider_id: int,
    ticker: str,
    as_of_date: str,
    min_ticker_trades: int = 2,
    min_global_trades: int = 3,
    return_window_days: int = 10,
) -> dict:
    """
    Compute point-in-time score for one insider at one ticker.

    Only uses trades where:
      - trade_date <= as_of_date
      - returns are observable (trade_date + return_window_days <= as_of_date)

    Args:
        conn: SQLite connection
        insider_id: insider to score
        ticker: ticker to score at
        as_of_date: point-in-time boundary (YYYY-MM-DD)
        min_ticker_trades: minimum trades at this ticker for ticker-specific score
        min_global_trades: minimum total trades for any scoring
        return_window_days: days after trade before return is considered observable

    Returns dict with all insider_ticker_scores columns.
    """
    # Observability boundary: returns must have had time to materialize
    # A trade on day T has a 7d return observable by ~T+10
    observable_cutoff = as_of_date  # trade_date must be this far back for return to be known

    # ── Ticker-specific aggregates ──
    ticker_returns = conn.execute("""
        SELECT tr.abnormal_7d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.insider_id = ? AND t.ticker = ? AND t.trade_type = 'buy'
          AND t.trade_date <= date(?, ?)
          AND tr.abnormal_7d IS NOT NULL
    """, (insider_id, ticker, as_of_date, f"-{return_window_days} days")).fetchall()
    ticker_abn = [r[0] for r in ticker_returns]
    ticker_stats = _compute_aggregate(ticker_abn)

    # ── Global aggregates (all tickers) ──
    global_returns = conn.execute("""
        SELECT tr.abnormal_7d
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.insider_id = ? AND t.trade_type = 'buy'
          AND t.trade_date <= date(?, ?)
          AND tr.abnormal_7d IS NOT NULL
    """, (insider_id, as_of_date, f"-{return_window_days} days")).fetchall()
    global_abn = [r[0] for r in global_returns]
    global_stats = _compute_aggregate(global_abn)

    # ── Total trade counts (including those without returns) ──
    ticker_trade_count = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE insider_id = ? AND ticker = ? AND trade_type = 'buy'
          AND trade_date <= ?
    """, (insider_id, ticker, as_of_date)).fetchone()[0]

    global_trade_count = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE insider_id = ? AND trade_type = 'buy'
          AND trade_date <= ?
    """, (insider_id, as_of_date)).fetchone()[0]

    # ── Sufficient data check ──
    sufficient_data = 1 if global_stats["n"] >= min_global_trades else 0

    # ── Scale scores to 0-3 ──
    ticker_score_raw = ticker_stats["score"] * 3.0
    global_score_raw = global_stats["score"] * 3.0

    # ── Blending weights based on ticker sample size ──
    ticker_n = ticker_stats["n"]
    if ticker_n < min_ticker_trades:
        # Not enough ticker-specific data — use global only
        global_weight = 1.0
        ticker_weight = 0.0
    elif ticker_n < 5:
        # 2-4 ticker trades → 70% global / 30% ticker
        global_weight = 0.70
        ticker_weight = 0.30
    elif ticker_n < 10:
        # 5-9 trades → 50/50
        global_weight = 0.50
        ticker_weight = 0.50
    else:
        # 10+ trades → 30% global / 70% ticker
        global_weight = 0.30
        ticker_weight = 0.70

    # ── Ticker outperformance adjustment ──
    # If insider has >10pp higher win rate at this ticker vs global, boost ticker weight
    if (ticker_stats["win_rate"] is not None
            and global_stats["win_rate"] is not None
            and ticker_stats["win_rate"] - global_stats["win_rate"] > 0.10
            and ticker_n >= min_ticker_trades):
        # Shift 10% from global to ticker
        shift = min(0.10, global_weight)
        global_weight -= shift
        ticker_weight += shift

    blended_score = (global_score_raw * global_weight + ticker_score_raw * ticker_weight)

    # ── Role adjustment ──
    # Check if this insider is a primary officer at this ticker
    role_row = conn.execute("""
        SELECT title FROM insider_companies
        WHERE insider_id = ? AND ticker = ?
    """, (insider_id, ticker)).fetchone()

    role_at_ticker = role_row[0] if role_row else None
    is_primary_company = 0
    role_weight = 1.0

    if role_at_ticker:
        role_lower = role_at_ticker.lower()
        # Check if this is the insider's primary company
        primary_row = conn.execute("""
            SELECT ticker FROM insider_companies
            WHERE insider_id = ?
            ORDER BY trade_count DESC LIMIT 1
        """, (insider_id,)).fetchone()
        if primary_row and primary_row[0] == ticker:
            is_primary_company = 1

        # Role weight boost for C-suite at their primary company
        if is_primary_company:
            if any(kw in role_lower for kw in ("ceo", "chief exec", "chairman")):
                role_weight = 1.15
            elif any(kw in role_lower for kw in ("cfo", "president")):
                role_weight = 1.10
            elif any(kw in role_lower for kw in ("coo", "evp", "svp")):
                role_weight = 1.05

        blended_score *= role_weight

    # Cap at 3.0
    blended_score = min(3.0, max(0.0, blended_score))

    return {
        "insider_id": insider_id,
        "ticker": ticker,
        "as_of_date": as_of_date,
        "ticker_trade_count": ticker_trade_count,
        "ticker_win_rate_7d": ticker_stats["win_rate"],
        "ticker_avg_abnormal_7d": ticker_stats["avg_return"],
        "ticker_score": round(ticker_score_raw, 4),
        "global_trade_count": global_trade_count,
        "global_win_rate_7d": global_stats["win_rate"],
        "global_avg_abnormal_7d": global_stats["avg_return"],
        "global_score": round(global_score_raw, 4),
        "blended_score": round(blended_score, 4),
        "role_at_ticker": role_at_ticker,
        "role_weight": round(role_weight, 4),
        "is_primary_company": is_primary_company,
        "sufficient_data": sufficient_data,
    }


def upsert_score(conn: sqlite3.Connection, score: dict, trigger_trade_id: int | None = None):
    """
    Insert or update a score in insider_ticker_scores, and append to score_history.
    """
    conn.execute("""
        INSERT OR REPLACE INTO insider_ticker_scores
            (insider_id, ticker, as_of_date,
             ticker_trade_count, ticker_win_rate_7d, ticker_avg_abnormal_7d, ticker_score,
             global_trade_count, global_win_rate_7d, global_avg_abnormal_7d, global_score,
             blended_score, role_at_ticker, role_weight,
             is_primary_company, sufficient_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        score["insider_id"], score["ticker"], score["as_of_date"],
        score["ticker_trade_count"], score["ticker_win_rate_7d"],
        score["ticker_avg_abnormal_7d"], score["ticker_score"],
        score["global_trade_count"], score["global_win_rate_7d"],
        score["global_avg_abnormal_7d"], score["global_score"],
        score["blended_score"], score["role_at_ticker"], score["role_weight"],
        score["is_primary_company"], score["sufficient_data"],
    ))

    conn.execute("""
        INSERT INTO score_history
            (insider_id, ticker, as_of_date, trigger_trade_id,
             blended_score, global_score, ticker_score, trade_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        score["insider_id"], score["ticker"], score["as_of_date"],
        trigger_trade_id,
        score["blended_score"], score["global_score"], score["ticker_score"],
        score["global_trade_count"],
    ))


def sync_to_track_records(conn: sqlite3.Connection):
    """
    Backward compatibility: sync latest PIT scores back to insider_track_records.

    Takes the most recent insider_ticker_scores snapshot per insider, aggregates
    to the global level (weighted by ticker trade count), and updates the
    existing insider_track_records.score and score_tier fields.

    This keeps the existing API endpoints and Form4.app UI working.
    """
    logger.info("Syncing PIT scores to insider_track_records...")

    # Get latest PIT score per insider (across all tickers, weighted by trade count)
    rows = conn.execute("""
        SELECT insider_id,
               SUM(blended_score * ticker_trade_count) / SUM(ticker_trade_count) as weighted_score,
               MAX(blended_score) as best_score,
               SUM(ticker_trade_count) as total_trades,
               COUNT(DISTINCT ticker) as n_tickers
        FROM insider_ticker_scores its
        WHERE as_of_date = (
            SELECT MAX(as_of_date) FROM insider_ticker_scores its2
            WHERE its2.insider_id = its.insider_id AND its2.ticker = its.ticker
        )
        AND sufficient_data = 1
        GROUP BY insider_id
    """).fetchall()

    if not rows:
        logger.info("No PIT scores to sync")
        return

    # Compute percentile ranks
    scores = [(r[0], r[1]) for r in rows]
    scores.sort(key=lambda x: x[1])
    n = len(scores)

    updated = 0
    for rank, (insider_id, weighted_score) in enumerate(scores):
        percentile = (rank + 1) / n * 100
        score = min(3.0, max(0.0, weighted_score))

        if percentile >= 93:
            tier = 3
        elif percentile >= 80:
            tier = 2
        elif percentile >= 67:
            tier = 1
        else:
            tier = 0

        conn.execute("""
            UPDATE insider_track_records
            SET score = ?, score_tier = ?, percentile = ?
            WHERE insider_id = ?
        """, (round(score, 4), tier, round(percentile, 2), insider_id))
        updated += 1

    conn.commit()
    logger.info("Synced %d insiders from PIT scores to track records", updated)


def get_pit_score(
    conn: sqlite3.Connection,
    insider_id: int,
    ticker: str,
    as_of_date: str,
) -> Optional[dict]:
    """
    Look up the most recent PIT score for an insider+ticker as of a date.

    Returns the score dict or None if no score exists.
    """
    row = conn.execute("""
        SELECT blended_score, global_score, ticker_score,
               ticker_trade_count, global_trade_count,
               ticker_win_rate_7d, global_win_rate_7d,
               sufficient_data, role_weight, is_primary_company
        FROM insider_ticker_scores
        WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC LIMIT 1
    """, (insider_id, ticker, as_of_date)).fetchone()

    if not row:
        return None

    return {
        "blended_score": row[0],
        "global_score": row[1],
        "ticker_score": row[2],
        "ticker_trade_count": row[3],
        "global_trade_count": row[4],
        "ticker_win_rate_7d": row[5],
        "global_win_rate_7d": row[6],
        "sufficient_data": row[7],
        "role_weight": row[8],
        "is_primary_company": row[9],
    }
