"""Sector hub pages — /insider-buying and /insider-buying/{sector}.

WHY THESE EXIST

Measured 2026-09-03: Googlebot crawls ~4,300 pages a week here and organic
search returns 62 unique visitors per 90 days. The pages being crawled are
leaves — one filing, one insider, one company — and they target queries with
almost no volume. "Erez Chimovits" is not a search anyone runs.

The 17 static hub URLs on the site are /pricing, /privacy, /terms and the
like. Not one targets an informational query. These pages do: "healthcare
insider buying", "energy insider buying", "stocks insiders are buying" are
real searches, and sector-scoped versions are winnable in a way the head term
"insider trading" is not against openinsider and finviz.

They are also link hubs. Each sector page points at ~40 leaf pages that
currently receive crawl but no internal links from anything topical.

EVERY SECTOR HAS ENOUGH TO SAY. Buy filings in the last 90 days, measured the
day this shipped: Financial Services 762, Healthcare 491, Technology 307,
Industrials 296, Consumer Cyclical 214, Energy 167, Real Estate 135, Basic
Materials 123, Consumer Defensive 101, Communication Services 87, Utilities
45. The thinnest is 45 filings across 18 companies, which is a page with
something on it rather than a doorway.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import get_db
from api.id_encoding import encode_insider_id, encode_trade_id

router = APIRouter(prefix="/api/v1/sectors", tags=["sectors"])

# The slug <-> name mapping is defined ONCE, here. The frontend reads it from
# /api/v1/sectors rather than keeping its own copy, so a sector renamed
# upstream cannot leave a route pointing at nothing.
def slugify(sector: str) -> str:
    return sector.lower().replace(" ", "-").replace("&", "and")


WINDOW_DAYS = 90

_SECTORS_SQL = """
SELECT m.sector,
       count(DISTINCT COALESCE(t.filing_key, t.accession)) AS buy_filings,
       count(DISTINCT t.ticker)     AS tickers,
       count(DISTINCT t.insider_id) AS insiders,
       SUM(t.value)                 AS total_value
  FROM trades t
  JOIN ticker_metadata m ON m.ticker = t.ticker
 WHERE t.signal_class = 'discretionary_buy'
   AND t.filing_date >= (CURRENT_DATE - {days})::text
   AND m.sector IS NOT NULL
   AND t.superseded_by IS NULL
   AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.is_derivative = 0
 GROUP BY 1
 ORDER BY buy_filings DESC
"""

# Largest single purchases. is_derivative and value_suspect are excluded for
# the same reason the sitemap excludes them: derivative rows carry notional
# value reaching $180 quadrillion, and a hub page ranked by value would be
# nothing but those.
_TOP_BUYS_SQL = """
SELECT t.trade_id, t.ticker, t.company, t.insider_id, t.title,
       t.value, t.qty, t.price, t.filing_date, t.trade_date,
       COALESCE(i.display_name, i.name) AS insider_name, i.slug AS insider_slug
  FROM trades t
  JOIN ticker_metadata m ON m.ticker = t.ticker
  LEFT JOIN insiders i ON i.insider_id = t.insider_id
 WHERE m.sector = ?
   AND t.signal_class = 'discretionary_buy'
   AND t.filing_date >= (CURRENT_DATE - {days})::text
   AND t.superseded_by IS NULL
   AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.is_derivative = 0
   AND t.value IS NOT NULL
 ORDER BY t.value DESC
 LIMIT ?
"""

_TOP_COMPANIES_SQL = """
SELECT t.ticker,
       MAX(t.company) AS company,
       count(DISTINCT COALESCE(t.filing_key, t.accession)) AS buy_filings,
       count(DISTINCT t.insider_id) AS insiders,
       SUM(t.value) AS total_value
  FROM trades t
  JOIN ticker_metadata m ON m.ticker = t.ticker
 WHERE m.sector = ?
   AND t.signal_class = 'discretionary_buy'
   AND t.filing_date >= (CURRENT_DATE - {days})::text
   AND t.superseded_by IS NULL
   AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.is_derivative = 0
 GROUP BY 1
 ORDER BY total_value DESC NULLS LAST
 LIMIT ?
"""

# Most active buyers in the sector. Deliberately ranked by ACTIVITY, not by
# track record: ranking people by past returns on a page headed "insider
# buying" would read as a tip sheet, and three experiments this month could
# not show our grades predict forward returns. The accuracy figures live on
# each insider's own page, where they carry their denominator and their
# floor -- this list is how a reader gets there.
_TOP_INSIDERS_SQL = """
SELECT t.insider_id,
       COALESCE(i.display_name, i.name) AS name,
       i.slug,
       COALESCE(i.is_entity, 0) AS is_entity,
       count(DISTINCT COALESCE(t.filing_key, t.accession)) AS buy_filings,
       count(DISTINCT t.ticker) AS tickers,
       SUM(t.value) AS total_value
  FROM trades t
  JOIN ticker_metadata m ON m.ticker = t.ticker
  LEFT JOIN insiders i ON i.insider_id = t.insider_id
 WHERE m.sector = ?
   AND t.signal_class = 'discretionary_buy'
   AND t.filing_date >= (CURRENT_DATE - {days})::text
   AND t.superseded_by IS NULL
   AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
   AND NOT COALESCE(t.value_suspect, FALSE)
   AND t.is_derivative = 0
   AND t.insider_id IS NOT NULL
 GROUP BY 1, 2, 3, 4
 ORDER BY total_value DESC NULLS LAST
 LIMIT ?
"""


def _sector_rows(conn):
    return conn.execute(_SECTORS_SQL.format(days=WINDOW_DAYS)).fetchall()


@router.get("")
def list_sectors() -> dict:
    """Every sector with insider buying in the window. Public."""
    with get_db() as conn:
        rows = _sector_rows(conn)
    return {
        "window_days": WINDOW_DAYS,
        "sectors": [
            {**dict(r), "slug": slugify(r["sector"])} for r in rows
        ],
    }


@router.get("/{slug}")
def get_sector(slug: str, limit: int = Query(20, ge=1, le=50)) -> dict:
    """One sector: largest buys, most-bought companies, most active buyers.

    Public and ungated. The whole point is to be a landing page.
    """
    with get_db() as conn:
        rows = _sector_rows(conn)
        match = next((r for r in rows if slugify(r["sector"]) == slug.lower()), None)
        if match is None:
            raise HTTPException(status_code=404, detail="Sector not found")
        sector = match["sector"]

        buys = conn.execute(
            _TOP_BUYS_SQL.format(days=WINDOW_DAYS), (sector, limit)
        ).fetchall()
        companies = conn.execute(
            _TOP_COMPANIES_SQL.format(days=WINDOW_DAYS), (sector, 12)
        ).fetchall()
        insiders = conn.execute(
            _TOP_INSIDERS_SQL.format(days=WINDOW_DAYS), (sector, 12)
        ).fetchall()

    return {
        "sector": sector,
        "slug": slugify(sector),
        "window_days": WINDOW_DAYS,
        "summary": {
            "buy_filings": match["buy_filings"],
            "tickers": match["tickers"],
            "insiders": match["insiders"],
            "total_value": match["total_value"],
        },
        "top_buys": [
            {**dict(r),
             "trade_id": encode_trade_id(r["trade_id"]),
             "insider_id": encode_insider_id(r["insider_id"]) if r["insider_id"] else None}
            for r in buys
        ],
        "top_companies": [dict(r) for r in companies],
        "top_insiders": [
            {**dict(r), "insider_id": encode_insider_id(r["insider_id"])}
            for r in insiders
        ],
        "all_sectors": [
            {"sector": r["sector"], "slug": slugify(r["sector"]),
             "buy_filings": r["buy_filings"]}
            for r in rows
        ],
    }
