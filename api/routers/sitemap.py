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
        insiders: list of encoded insider IDs (top N by trade count)
        filings: list of encoded filing IDs (last N days)
    """
    with get_db() as conn:
        from api.id_encoding import encode_trade_id

        tickers: list[str] = []
        insiders: list[str] = []
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
            insider_rows = conn.execute("""
                SELECT insider_id FROM insider_track_records
                WHERE buy_count >= 2
                ORDER BY score DESC NULLS LAST
                LIMIT ?
            """, (limit_insiders,)).fetchall()
            insiders = [encode_insider_id(r["insider_id"]) for r in insider_rows if r["insider_id"]]
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
