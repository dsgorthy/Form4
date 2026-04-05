"""PIT grade helpers for API endpoints.

Computes best PIT grade across tickers and per-ticker grade breakdowns.
"""
from __future__ import annotations

import sqlite3


# Grade thresholds — must match pit_scoring.py and scoring page
_GRADE_THRESHOLDS = [
    (2.5, "A+"),
    (2.0, "A"),
    (1.2, "B"),
    (0.6, "C"),
    (0.0, "D"),
]


def score_to_grade(score: float | None) -> str | None:
    """Convert blended_score to letter grade."""
    if score is None:
        return None
    for threshold, grade in _GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "D"


def get_best_pit_grade(conn: sqlite3.Connection, insider_id: int) -> dict:
    """Get the best PIT grade across all tickers for an insider.

    Returns dict with: best_pit_grade, best_pit_ticker, best_pit_score, n_scored_tickers.
    All None if no scores exist.
    """
    rows = conn.execute("""
        SELECT its.ticker, its.blended_score, its.ticker_trade_count
        FROM insider_ticker_scores its
        WHERE its.insider_id = ?
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
          AND its.blended_score IS NOT NULL
        ORDER BY its.blended_score DESC
    """, (insider_id,)).fetchall()

    if not rows:
        return {
            "best_pit_grade": None,
            "best_pit_ticker": None,
            "best_pit_score": None,
            "n_scored_tickers": 0,
        }

    best = rows[0]
    return {
        "best_pit_grade": score_to_grade(best["blended_score"]),
        "best_pit_ticker": best["ticker"],
        "best_pit_score": round(best["blended_score"], 2),
        "n_scored_tickers": len(rows),
    }


def get_ticker_grades(conn: sqlite3.Connection, insider_id: int) -> list[dict]:
    """Get all per-ticker grades for an insider, sorted by score DESC.

    Returns list of dicts: ticker, grade, score, trade_count.
    """
    rows = conn.execute("""
        SELECT its.ticker, its.blended_score, its.ticker_trade_count
        FROM insider_ticker_scores its
        WHERE its.insider_id = ?
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
          AND its.blended_score IS NOT NULL
        ORDER BY its.blended_score DESC
    """, (insider_id,)).fetchall()

    return [
        {
            "ticker": r["ticker"],
            "grade": score_to_grade(r["blended_score"]),
            "score": round(r["blended_score"], 2),
            "trade_count": r["ticker_trade_count"] or 0,
        }
        for r in rows
    ]


def get_ticker_pit_grade(conn: sqlite3.Connection, insider_id: int, ticker: str) -> str | None:
    """Get the PIT grade for a specific insider+ticker pair."""
    row = conn.execute("""
        SELECT blended_score
        FROM insider_ticker_scores
        WHERE insider_id = ? AND ticker = ?
        ORDER BY as_of_date DESC
        LIMIT 1
    """, (insider_id, ticker)).fetchone()

    if not row or row["blended_score"] is None:
        return None
    return score_to_grade(row["blended_score"])


def enrich_with_best_pit_grade(conn: sqlite3.Connection, items: list[dict]) -> None:
    """Bulk-enrich a list of items that have 'insider_id' with best PIT grade fields.

    Adds best_pit_grade, best_pit_ticker, n_scored_tickers to each item in-place.
    Efficient: single query for all insider_ids.
    """
    insider_ids = {item.get("insider_id") for item in items if item.get("insider_id")}
    if not insider_ids:
        return

    placeholders = ",".join("?" for _ in insider_ids)
    rows = conn.execute(f"""
        SELECT its.insider_id, its.ticker, its.blended_score,
               ROW_NUMBER() OVER (PARTITION BY its.insider_id ORDER BY its.blended_score DESC) AS rn,
               COUNT(*) OVER (PARTITION BY its.insider_id) AS n_tickers
        FROM insider_ticker_scores its
        WHERE its.insider_id IN ({placeholders})
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
          AND its.blended_score IS NOT NULL
    """, list(insider_ids)).fetchall()

    # Build lookup: insider_id -> best row
    best_by_insider: dict[int, dict] = {}
    for r in rows:
        if r["rn"] == 1:
            best_by_insider[r["insider_id"]] = {
                "best_pit_grade": score_to_grade(r["blended_score"]),
                "best_pit_ticker": r["ticker"],
                "n_scored_tickers": r["n_tickers"],
            }

    for item in items:
        iid = item.get("insider_id")
        if iid and iid in best_by_insider:
            item.update(best_by_insider[iid])
        else:
            item["best_pit_grade"] = None
            item["best_pit_ticker"] = None
            item["n_scored_tickers"] = 0
