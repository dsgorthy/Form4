"""Lightweight sitemap data endpoint for Next.js sitemap.ts to consume.

Returns ticker lists and insider IDs for dynamic sitemap generation.
No auth required — this data is public (tickers and IDs only, no scores/PII).
"""
from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import get_db
from api.id_encoding import encode_insider_id

router = APIRouter(prefix="/api/v1/sitemap", tags=["sitemap"])


@router.get("/urls")
def sitemap_urls(
    limit_insiders: int = Query(default=10000, ge=100, le=50000),
    filing_days: int = Query(default=90, ge=7, le=365),
) -> dict:
    """Return tickers, insider IDs, and recent filing IDs for sitemap generation.

    Returns:
        tickers: list of all traded tickers
        insiders: list of {id, name} for slugged URLs (top N by track record)
        filings: list of encoded filing IDs (last N days)
    """
    with get_db() as conn:
        from api.id_encoding import encode_trade_id

        tickers: list[str] = []
        insiders: list[dict] = []
        filings: list[str] = []

        # All tickers — use insider_companies as a corruption-safe fallback
        try:
            ticker_rows = conn.execute("""
                SELECT DISTINCT ticker FROM trades
                WHERE ticker IS NOT NULL AND ticker != '' AND ticker != 'NONE'
                  AND trans_code IN ('P', 'S')
                ORDER BY ticker
            """).fetchall()
            tickers = [r["ticker"] for r in ticker_rows]
        except Exception:
            # Fallback: use insider_companies table (no btree corruption)
            ticker_rows = conn.execute("""
                SELECT DISTINCT ticker FROM insider_companies
                WHERE ticker IS NOT NULL AND ticker != '' AND ticker != 'NONE'
                ORDER BY ticker
            """).fetchall()
            tickers = [r["ticker"] for r in ticker_rows]

        # Top insiders by track record (avoids heavy trades GROUP BY)
        try:
            # Join the name in: insider URLs are /insider/{name-slug}-{id}
            # for SEO, and a sitemap of bare IDs would publish the one URL
            # shape search engines get nothing from.
            insider_rows = conn.execute("""
                SELECT tr.insider_id,
                       COALESCE(i.display_name, i.name) AS name,
                       i.slug
                  FROM insider_track_records tr
                  LEFT JOIN insiders i ON i.insider_id = tr.insider_id
                 WHERE tr.buy_count >= 2
                 -- Tiebreakers are load-bearing, not tidiness. 13,090 insiders
                 -- are eligible and 6,335 of them have a NULL score, so the
                 -- LIMIT cuts through the middle of one enormous tied block.
                 -- Ordering by score alone leaves Postgres free to return a
                 -- different subset every run: measured 2026-08-15, 1,347
                 -- insider URLs (13%) churned in and out of the sitemap
                 -- between two generations. A URL that appears and vanishes
                 -- between crawls is a stability signal we do not want to
                 -- send, and it left which insiders get indexed to chance.
                 --
                 -- buy_count before insider_id so the unscored insiders we do
                 -- include are the most active ones rather than the
                 -- lowest-numbered.
                 ORDER BY tr.score DESC NULLS LAST, tr.buy_count DESC, tr.insider_id
                 LIMIT ?
            """, (limit_insiders,)).fetchall()
            insiders = [
                {
                    "id": encode_insider_id(r["insider_id"]),
                    "name": r["name"] or "",
                    # Prefer the stored slug; the client only falls back to
                    # deriving one from the name when this is absent.
                    "slug": r["slug"] or "",
                }
                for r in insider_rows if r["insider_id"]
            ]
        except Exception:
            pass

        # Recent filings
        try:
            filing_rows = conn.execute(f"""
                SELECT trade_id FROM trades
                WHERE trans_code IN ('P', 'S')
                  AND filing_date >= date('now', '-{int(filing_days)} days')
                  AND superseded_by IS NULL
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                  -- Don't ask Google to index a page whose numbers we know are
                  -- wrong. Derivative rows carry notional value that reaches
                  -- $180 quadrillion, and value_suspect marks the rest of what
                  -- cannot be believed. 1,312 derivative filings sit above $1B.
                  AND is_derivative = 0
                  AND NOT COALESCE(value_suspect, FALSE)
                ORDER BY filing_date DESC
            """).fetchall()
            filings = [encode_trade_id(r["trade_id"]) for r in filing_rows if r["trade_id"]]
        except Exception:
            pass

    return {
        "tickers": tickers,
        "insiders": insiders,
        "filings": filings,
        "counts": {
            "tickers": len(tickers),
            "insiders": len(insiders),
            "filings": len(filings),
        },
    }
