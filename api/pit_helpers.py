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
    """Best Recent Form (V2) AND best Career Grade (V3) across tickers.

    Returns dict with:
      best_pit_grade, best_pit_ticker, best_pit_score, n_scored_tickers,
      best_career_grade, best_career_ticker, best_career_score.
    All None / 0 if no scores exist.
    """
    rows = conn.execute("""
        SELECT its.ticker, its.blended_score, its.career_blended_score,
               its.ticker_trade_count
        FROM insider_ticker_scores its
        WHERE its.insider_id = ?
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
        ORDER BY its.blended_score DESC NULLS LAST
    """, (insider_id,)).fetchall()

    out: dict = {
        "best_pit_grade": None, "best_pit_ticker": None, "best_pit_score": None,
        "n_scored_tickers": 0,
        "best_career_grade": None, "best_career_ticker": None, "best_career_score": None,
    }
    if not rows:
        return out

    # Recent Form best
    pit_rows = [r for r in rows if r["blended_score"] is not None]
    if pit_rows:
        best = pit_rows[0]
        out["best_pit_grade"] = score_to_grade(best["blended_score"])
        out["best_pit_ticker"] = best["ticker"]
        out["best_pit_score"] = round(best["blended_score"], 2)
        out["n_scored_tickers"] = len(pit_rows)

    # Career Grade best — re-sort by career score
    career_rows = [r for r in rows if r["career_blended_score"] is not None]
    if career_rows:
        career_sorted = sorted(career_rows, key=lambda r: -r["career_blended_score"])
        cbest = career_sorted[0]
        out["best_career_grade"] = score_to_grade(cbest["career_blended_score"])
        out["best_career_ticker"] = cbest["ticker"]
        out["best_career_score"] = round(cbest["career_blended_score"], 2)

    return out


def get_ticker_grades(conn: sqlite3.Connection, insider_id: int) -> list[dict]:
    """Per-ticker grades for an insider — both Recent Form and Career.

    Returns list of dicts: ticker, grade (Recent Form), score, career_grade,
    career_score, trade_count. Sorted by Recent Form score DESC.
    """
    rows = conn.execute("""
        SELECT its.ticker, its.blended_score, its.career_blended_score,
               its.ticker_trade_count
        FROM insider_ticker_scores its
        WHERE its.insider_id = ?
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
          AND (its.blended_score IS NOT NULL OR its.career_blended_score IS NOT NULL)
        ORDER BY its.blended_score DESC NULLS LAST
    """, (insider_id,)).fetchall()

    out = []
    for r in rows:
        out.append({
            "ticker": r["ticker"],
            "grade": score_to_grade(r["blended_score"]) if r["blended_score"] is not None else None,
            "score": round(r["blended_score"], 2) if r["blended_score"] is not None else None,
            "career_grade": score_to_grade(r["career_blended_score"]) if r["career_blended_score"] is not None else None,
            "career_score": round(r["career_blended_score"], 2) if r["career_blended_score"] is not None else None,
            "trade_count": r["ticker_trade_count"] or 0,
        })
    return out


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
    """Bulk-enrich items that have 'insider_id' with best Recent Form (V2) AND
    best Career Grade (V3) across tickers.

    Adds: best_pit_grade, best_pit_ticker, n_scored_tickers (Recent Form fields)
          best_career_grade, best_career_ticker (Career Grade fields)
    """
    insider_ids = {item.get("insider_id") for item in items if item.get("insider_id")}
    if not insider_ids:
        return

    placeholders = ",".join("?" for _ in insider_ids)
    # Recent Form (V2) — best by blended_score
    pit_rows = conn.execute(f"""
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

    # Career Grade (V3) — best by career_blended_score
    career_rows = conn.execute(f"""
        SELECT its.insider_id, its.ticker, its.career_blended_score,
               ROW_NUMBER() OVER (PARTITION BY its.insider_id ORDER BY its.career_blended_score DESC) AS rn
        FROM insider_ticker_scores its
        WHERE its.insider_id IN ({placeholders})
          AND its.as_of_date = (
              SELECT MAX(its2.as_of_date)
              FROM insider_ticker_scores its2
              WHERE its2.insider_id = its.insider_id
                AND its2.ticker = its.ticker
          )
          AND its.career_blended_score IS NOT NULL
    """, list(insider_ids)).fetchall()

    pit_best: dict[int, dict] = {}
    for r in pit_rows:
        if r["rn"] == 1:
            pit_best[r["insider_id"]] = {
                "best_pit_grade": score_to_grade(r["blended_score"]),
                "best_pit_ticker": r["ticker"],
                "n_scored_tickers": r["n_tickers"],
            }

    career_best: dict[int, dict] = {}
    for r in career_rows:
        if r["rn"] == 1:
            career_best[r["insider_id"]] = {
                "best_career_grade": score_to_grade(r["career_blended_score"]),
                "best_career_ticker": r["ticker"],
            }

    for item in items:
        iid = item.get("insider_id")
        if iid and iid in pit_best:
            item.update(pit_best[iid])
        else:
            item["best_pit_grade"] = None
            item["best_pit_ticker"] = None
            item["n_scored_tickers"] = 0
        if iid and iid in career_best:
            item.update(career_best[iid])
        else:
            item["best_career_grade"] = None
            item["best_career_ticker"] = None
