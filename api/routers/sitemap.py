"""Lightweight sitemap data endpoint for Next.js sitemap.ts to consume.

Returns ticker lists and insider IDs for dynamic sitemap generation.
No auth required — this data is public (tickers and IDs only, no scores/PII).
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query

from api.db import get_db
from api.id_encoding import encode_insider_id
#: The suppression floor for the published track record. Imported, never
#: retyped: if it moves, the set of pages worth submitting moves with it.
from api.routers.insiders import MIN_SCORED_FILINGS

router = APIRouter(prefix="/api/v1/sitemap", tags=["sitemap"])

# ONE COMPUTATION AN HOUR, NOT ONE PER FILE. The response is ~5 MB (51,747
# insiders, every ticker, 90 days of filings) and Next.js refuses to cache a
# fetch over 2 MB, so every request for any of the seven sitemap files -- and
# every crawler's request for the index -- ran the three queries again.
# Measured 2026-09-20: AhrefsBot, Googlebot and Bingbot between them fetched
# sitemap files hundreds of times a day. Keyed on the two parameters; the
# data changes daily, so an hour is fine.
_CACHE_TTL_S = 3600
_cache: dict[tuple[int, int], tuple[float, dict]] = {}

# ── WHAT WE SUBMIT, AND WHY IT IS LESS THAN WHAT EXISTS ──────────────────────
#
# Page indexing on 2026-09-20: 44.6K indexed, 126K NOT — and 91% of the
# not-indexed total is "Discovered - currently not indexed" (59,022) plus
# "Crawled - currently not indexed" (56,197). Google is finding these pages and
# declining them. Crawl stats said it from the other side: 67% of budget went to
# Discovery, 33% to Refresh, so the pages that could rank were re-read least.
#
# We were submitting ~70,000 URLs on a rule that could not tell a page from a
# stub — `insider_track_records.buy_count >= 2`, where buy_count counts
# EXECUTION LOTS. One purchase filled in five tranches scored five, so ">= 2"
# admitted insiders who had made exactly one decision.
#
# The floor is now stated in terms of the thing that makes the page worth
# indexing at all. Below MIN_SCORED_FILINGS the track record is SUPPRESSED, so
# the page renders a name, a role and a filings table — byte-for-byte what
# secform4, openinsider and marketbeat already publish with more authority.
# Submitting it asks Google to rank a page with nothing of ours on it.
#
#   substantial      >= 10 decision filings, whatever the date
#   recent and real  filed in the last 12 months AND >= 5 decision filings
#
# Measured 2026-09-27: 30,793 insiders and 11,036 companies, against 51,797 and
# 18,267 before. Facts come from sitemap_quality_{insiders,companies}, rebuilt
# daily by pipelines/insider_study/refresh_sitemap_quality.py; the THRESHOLDS
# live here so the rule can move without a re-materialization.
SUBSTANTIAL_FILINGS = 10
#: Mirrors api.routers.insiders.MIN_SCORED_FILINGS — the count below which the
#: track record is suppressed. Imported rather than typed.
RECENT_MONTHS = 12

#: How stale the quality tables may be before we stop trusting them. The daily
#: refresh gives ~6 days of margin; past that we fall back to the old rule
#: rather than publish a sitemap shaped by a frozen snapshot.
QUALITY_MAX_AGE_DAYS = 7


def _as_insider_list(rows) -> list[dict]:
    """Shape rows for the client. Shared so the two query paths cannot drift."""
    return [
        {
            "id": encode_insider_id(r["insider_id"]),
            "name": r["name"] or "",
            # Prefer the stored slug; the client only falls back to deriving
            # one from the name when this is absent.
            "slug": r["slug"] or "",
        }
        for r in rows if r["insider_id"]
    ]


def _quality_is_usable(conn, table: str) -> bool:
    """Is `table` present and refreshed recently enough to shape the sitemap?

    The refresh stamps `refreshed_at=<iso>` into the table comment. A missing
    table, a missing stamp, or a stamp older than QUALITY_MAX_AGE_DAYS all read
    as unusable, and the caller falls back to submitting everything.

    Checked rather than assumed because the failure is invisible: a frozen
    quality table does not error, it just quietly stops admitting the pages
    that became eligible since it froze.
    """
    try:
        row = conn.execute(
            "SELECT obj_description(c.oid) AS c FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relname = ?",
            (table,),
        ).fetchone()
    except Exception:
        return False
    if not row or not row["c"]:
        return False
    m = re.search(r"refreshed_at=(\S+)", str(row["c"]))
    if not m:
        return False
    try:
        when = datetime.fromisoformat(m.group(1))
    except ValueError:
        return False
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - when
    return age <= timedelta(days=QUALITY_MAX_AGE_DAYS)


@router.get("/urls")
def sitemap_urls(
    limit_insiders: int = Query(default=45000, ge=100, le=200000),
    filing_days: int = Query(default=90, ge=7, le=365),
) -> dict:
    key = (int(limit_insiders), int(filing_days))
    hit = _cache.get(key)
    now = time.monotonic()
    if hit and hit[0] > now:
        return hit[1]
    result = _sitemap_urls_uncached(key[0], key[1])
    if result["counts"]["tickers"]:          # never cache an empty answer from a DB hiccup
        _cache[key] = (now + _CACHE_TTL_S, result)
    return result


def _sitemap_urls_uncached(
    # Ceiling raised 50,000 -> 200,000 on 2026-09-10. The old one was set to
    # the SITEMAP PROTOCOL cap, which only worked while the client emitted one
    # file; it now chunks insiders, so the protocol cap is a per-file property
    # and has no business bounding this query. The real bound is buy_count >= 2
    # below. Measured that day: 51,747 eligible, i.e. already past the old
    # ceiling, so this validator would have refused to serve them all.
    limit_insiders: int = Query(default=45000, ge=100, le=200000),
    filing_days: int = Query(default=90, ge=7, le=365),
) -> dict:
    """Return tickers, insider IDs, and recent filing IDs for sitemap generation.

    Returns:
        tickers: companies clearing the submission floor
        insiders: [{id, name, slug}] clearing the submission floor
        filings: encoded filing IDs (last N days) — computed but NOT published
                 since 2026-09-27; kept so flipping PUBLISH_FILINGS back on in
                 frontend/src/lib/sitemap-data.ts restores them without an API
                 change.

    See the SUBSTANTIAL_FILINGS block above for what the floor is and why.
    """
    with get_db() as conn:
        from api.id_encoding import encode_trade_id

        tickers: list[str] = []
        insiders: list[dict] = []
        filings: list[str] = []

        # Companies that clear the floor. FAIL OPEN: if the quality table is
        # missing, stale or empty, submit everything rather than nothing — a
        # sitemap that silently collapses is a worse failure than one that is
        # too generous, and this path has no other reader to notice.
        tickers = []
        if _quality_is_usable(conn, "sitemap_quality_companies"):
            try:
                ticker_rows = conn.execute(f"""
                    SELECT ticker FROM sitemap_quality_companies
                     WHERE decision_filings >= {SUBSTANTIAL_FILINGS}
                        OR (decision_filings >= {MIN_SCORED_FILINGS}
                            AND last_decision >=
                                (CURRENT_DATE - INTERVAL '{RECENT_MONTHS} months')::text)
                     ORDER BY ticker
                """).fetchall()
                tickers = [r["ticker"] for r in ticker_rows]
            except Exception:
                tickers = []
        if not tickers:
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

        # Insiders that clear the floor, newest activity first.
        #
        # The old query read insider_track_records.buy_count >= 2 and ordered by
        # tr.score. Two things were wrong with it and one was load-bearing:
        # buy_count counts EXECUTION LOTS (insider 14368: 1,167 decision filings
        # against a buy_count+sell_count of 24,994), and the ordering had to be
        # stabilised with explicit tiebreakers because 6,335 eligible insiders
        # shared a NULL score and the LIMIT cut through the tied block — 1,347
        # URLs (13%) churned in and out between two generations.
        #
        # Both go away here. The floor is a filing count, and the ORDER BY is
        # (last_decision, decision_filings, insider_id), which is total: no ties
        # to break and no dependence on a score column that is refreshed by a
        # different job. The LIMIT is now a backstop rather than the rule.
        insiders = []
        if _quality_is_usable(conn, "sitemap_quality_insiders"):
            try:
                insider_rows = conn.execute(f"""
                    SELECT q.insider_id,
                           COALESCE(i.display_name, i.name) AS name,
                           i.slug
                      FROM sitemap_quality_insiders q
                      JOIN insiders i ON i.insider_id = q.insider_id
                     WHERE q.decision_filings >= {SUBSTANTIAL_FILINGS}
                        OR (q.decision_filings >= {MIN_SCORED_FILINGS}
                            AND q.last_decision >=
                                (CURRENT_DATE - INTERVAL '{RECENT_MONTHS} months')::text)
                     ORDER BY q.last_decision DESC NULLS LAST,
                              q.decision_filings DESC,
                              q.insider_id
                     LIMIT ?
                """, (limit_insiders,)).fetchall()
                insiders = _as_insider_list(insider_rows)
            except Exception:
                insiders = []
        if not insiders:
            # FAIL OPEN to the previous rule. Same reasoning as the tickers
            # above: too many URLs is recoverable, an empty sitemap is not.
            try:
                insider_rows = conn.execute("""
                    SELECT tr.insider_id,
                           COALESCE(i.display_name, i.name) AS name,
                           i.slug
                      FROM insider_track_records tr
                      LEFT JOIN insiders i ON i.insider_id = tr.insider_id
                     WHERE tr.buy_count >= 2
                     ORDER BY tr.score DESC NULLS LAST, tr.buy_count DESC,
                              tr.insider_id
                     LIMIT ?
                """, (limit_insiders,)).fetchall()
                insiders = _as_insider_list(insider_rows)
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
                  AND price_quality IS DISTINCT FROM 'implausible'
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
