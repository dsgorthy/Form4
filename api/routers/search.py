from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import null_items_track_records
from api.id_encoding import encode_response_ids
from api.pit_helpers import enrich_with_best_pit_grade
from api.rate_limit import limiter

router = APIRouter(prefix="/api/v1/search", tags=["search"])


# Fetch more than we display so the client can give unused slots to the
# other type — "smith" matches 10 companies but 783 insiders.
RESULT_LIMIT = 8


@router.get("")
@limiter.limit("30/minute")
def search(q: str = Query(..., min_length=1, max_length=100), request: Request = None, user: UserContext = Depends(get_current_user)) -> dict:
    """Search for tickers and insiders. Returns top 5 of each."""
    query = q.strip()
    query_upper = query.upper()
    query_like = f"%{query}%"

    with get_db() as conn:
        # Ticker matches: search by ticker prefix and company name
        tickers = conn.execute(
            """
            SELECT ticker, MAX(company) AS company,
                   COUNT(*) AS trade_count,
                   SUM(value) AS total_value
            FROM trades
            WHERE ticker != 'NONE' AND (ticker LIKE ? OR company LIKE ?)
              AND trans_code IN ('P', 'S')
              AND is_derivative = 0
            GROUP BY ticker
            ORDER BY
                CASE WHEN ticker = ? THEN 0
                     WHEN ticker LIKE ? THEN 1
                     ELSE 2
                END,
                total_value DESC
            LIMIT ?
            """,
            (f"{query_upper}%", query_like, query_upper, f"{query_upper}%", RESULT_LIMIT),
        ).fetchall()

        # Insider matches: search by name
        # Relevance, then quality. The previous ORDER BY score DESC put NULLs
        # FIRST (Postgres default for DESC), so insiders with no track record
        # at all outranked scored ones — searching "penske" surfaced minor
        # namesakes above Roger S. Penske. Rank exact name, then prefix, then
        # substring, and only then by score with NULLS LAST.
        insiders = conn.execute(
            """
            SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                   i.slug,
                   itr.score, itr.score_tier, itr.primary_title, itr.primary_ticker
            FROM insiders i
            LEFT JOIN insider_track_records itr ON i.insider_id = itr.insider_id
            WHERE i.name LIKE ? OR i.name_normalized LIKE ? OR i.display_name LIKE ?
            ORDER BY
                CASE WHEN lower(COALESCE(i.display_name, i.name)) = lower(?) THEN 0
                     WHEN lower(COALESCE(i.display_name, i.name)) LIKE lower(?) THEN 1
                     ELSE 2
                END,
                itr.score DESC NULLS LAST,
                COALESCE(i.display_name, i.name)
            LIMIT ?
            """,
            (query_like, query_like, query_like, query, f"{query}%", RESULT_LIMIT),
        ).fetchall()

        # Totals so the UI can say "8 of 6,257" instead of implying the five
        # rows it shows are everything. A broad prefix like "mar" matches 169
        # companies and 6,257 insiders; silently truncating that reads as
        # "no more results".
        insider_total = conn.execute(
            """SELECT count(*) AS n FROM insiders i
                WHERE i.name LIKE ? OR i.name_normalized LIKE ? OR i.display_name LIKE ?""",
            (query_like, query_like, query_like),
        ).fetchone()["n"]

        ticker_total = conn.execute(
            """SELECT count(DISTINCT ticker) AS n FROM trades
                WHERE ticker != 'NONE' AND (ticker LIKE ? OR company LIKE ?)
                  AND trans_code IN ('P', 'S') AND is_derivative = 0""",
            (f"{query_upper}%", query_like),
        ).fetchone()["n"]

        insider_items = [dict(r) for r in insiders]
        enrich_with_best_pit_grade(conn, insider_items)

    if not user.is_pro:
        insider_items = null_items_track_records(insider_items)
    encode_response_ids(insider_items, trade=False, insider=True)

    return {
        "tickers": [dict(r) for r in tickers],
        "insiders": insider_items,
        # Totals drive the "see all N" affordance and let the client allocate
        # dropdown slots to whichever type actually matched.
        "ticker_total": ticker_total,
        "insider_total": insider_total,
    }
