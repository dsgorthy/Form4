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

# The dropdown wants 8; the /explore?q= results page wants a page's worth of
# each type. Capped because the ranking ORDER BY is the expensive part of the
# query and nobody browses past the first page of a name search.
MAX_RESULT_LIMIT = 50


@router.get("")
@limiter.limit("30/minute")
def search(
    q: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(RESULT_LIMIT, ge=1, le=MAX_RESULT_LIMIT),
    request: Request = None,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Search tickers and insiders.

    `limit` applies per type, not in total — the caller allocates between the
    two groups itself, since which one carries the match varies by query.
    Totals are returned unclamped so the caller can say "8 of 6,257".
    """
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
            WHERE ticker != 'NONE' AND (ticker ILIKE ? OR company ILIKE ?)
              AND trans_code IN ('P', 'S')
              AND is_derivative = 0
            GROUP BY ticker
            ORDER BY
                CASE WHEN ticker = ? THEN 0
                     WHEN ticker ILIKE ? THEN 1
                     ELSE 2
                END,
                total_value DESC
            LIMIT ?
            """,
            (f"{query_upper}%", query_like, query_upper, f"{query_upper}%", limit),
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
            WHERE i.name ILIKE ? OR i.name_normalized ILIKE ? OR i.display_name ILIKE ?
            ORDER BY
                -- Exact name always wins: typing "Penske Corp." in full is
                -- unambiguous intent.
                CASE WHEN lower(COALESCE(i.display_name, i.name)) = lower(?) THEN 0 ELSE 1 END,
                -- Then people before entities. The Companies group already
                -- covers the corporate angle (PAG surfaces there for
                -- "penske"), so an LLC or trust ranking above a named
                -- executive inside the INSIDERS list is nearly always the
                -- wrong answer. This deliberately outranks prefix matching:
                -- "penske" should lead with Roger S. Penske, not Penske Corp.
                COALESCE(i.is_entity, 0),
                CASE WHEN lower(COALESCE(i.display_name, i.name)) LIKE lower(?) THEN 0 ELSE 1 END,
                itr.score DESC NULLS LAST,
                COALESCE(i.display_name, i.name)
            LIMIT ?
            """,
            (query_like, query_like, query_like, query, f"{query}%", limit),
        ).fetchall()

        # Totals so the UI can say "8 of 6,257" instead of implying the five
        # rows it shows are everything. A broad prefix like "mar" matches 169
        # companies and 6,257 insiders; silently truncating that reads as
        # "no more results".
        insider_total = conn.execute(
            """SELECT count(*) AS n FROM insiders i
                WHERE i.name ILIKE ? OR i.name_normalized ILIKE ? OR i.display_name ILIKE ?""",
            (query_like, query_like, query_like),
        ).fetchone()["n"]

        ticker_total = conn.execute(
            """SELECT count(DISTINCT ticker) AS n FROM trades
                WHERE ticker != 'NONE' AND (ticker ILIKE ? OR company ILIKE ?)
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
