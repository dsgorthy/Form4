from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import null_items_track_records
from api.id_encoding import encode_response_ids
from api.pit_helpers import enrich_with_best_pit_grade
from api.rate_limit import limiter

router = APIRouter(prefix="/api/v1/search", tags=["search"])


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
            SELECT ticker, company,
                   COUNT(*) AS trade_count,
                   SUM(value) AS total_value
            FROM trades
            WHERE ticker != 'NONE' AND (ticker LIKE ? OR company LIKE ?)
              AND trans_code IN ('P', 'S')
            GROUP BY ticker
            ORDER BY
                CASE WHEN ticker = ? THEN 0
                     WHEN ticker LIKE ? THEN 1
                     ELSE 2
                END,
                total_value DESC
            LIMIT 5
            """,
            (f"{query_upper}%", query_like, query_upper, f"{query_upper}%"),
        ).fetchall()

        # Insider matches: search by name
        insiders = conn.execute(
            """
            SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                   itr.score, itr.score_tier, itr.primary_title, itr.primary_ticker
            FROM insiders i
            LEFT JOIN insider_track_records itr ON i.insider_id = itr.insider_id
            WHERE i.name LIKE ? OR i.name_normalized LIKE ? OR i.display_name LIKE ?
            ORDER BY itr.score DESC
            LIMIT 5
            """,
            (query_like, query_like, query_like),
        ).fetchall()

        insider_items = [dict(r) for r in insiders]
        enrich_with_best_pit_grade(conn, insider_items)

    if not user.is_pro:
        insider_items = null_items_track_records(insider_items)
    encode_response_ids(insider_items, trade=False, insider=True)

    return {
        "tickers": [dict(r) for r in tickers],
        "insiders": insider_items,
    }
