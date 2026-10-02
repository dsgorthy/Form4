"""Lightweight sitemap data endpoint for Next.js sitemap.ts to consume.

Returns ticker lists and insider IDs for dynamic sitemap generation.
No auth required — this data is public (tickers and IDs only, no scores/PII).
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query

from api.db import get_db
from api.id_encoding import encode_insider_id
#: The suppression floor for the published track record. Imported, never
#: retyped: if it moves, the set of pages worth submitting moves with it.
from api.routers.insiders import MIN_SCORED_FILINGS

logger = logging.getLogger(__name__)

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

#: Next.js refuses to write a fetch response larger than 2 MB into its data
#: cache. It does not raise — it logs one line ("items over 2MB can not be
#: cached") and serves the request anyway, so the sitemap keeps WORKING while
#: every request for every section re-fetches and re-parses the whole corpus.
#:
#: On 2026-10-01 that walked the frontend's Node heap to its 2,080 MB ceiling
#: and OOM-killed the container nine times. 32% of all origin requests returned
#: 502 and p95 on /filing/ reached 43 seconds. Nothing in the stack errored:
#: /api/v1/health answered 200 throughout.
#:
#: So keeping a response under this is a HARD INVARIANT of this endpoint, and
#: it is enforced at RUNTIME rather than only in a test — the payload crossed
#: the ceiling because the DATA grew (30,764 insiders and climbing), with no
#: code change for a test to catch.
NEXT_DATA_CACHE_MAX_BYTES = 2 * 1024 * 1024

#: Alarm below the real ceiling, so there is room to react before it bites. A
#: response over this logs at ERROR and reports `cacheable: false`.
#:
#: 1.8 MB is chosen so it cannot fire on a corpus that is actually fine. A
#: section holds at most CHUNK rows, and the largest a CHUNK=20,000 insider
#: section can be — every slug at the 60-character ceiling `slugifyName`
#: allows — is 1.64 MB. Measured against the real corpus on 2026-10-01 it is
#: 837 KB. So the gap between "legal worst case" and this alarm is deliberate:
#: anything above it is a new field or a raised CHUNK, not growth.
#:
#: That is the property that makes this testable at all. Before the sections
#: were sliced server-side the payload was bounded by the CORPUS, which grows
#: on its own and therefore could only ever be caught in production — which is
#: exactly how it was caught. It is now bounded by CHUNK, which is a constant a
#: test can reason about: see test_sitemap_payload_stays_cacheable.py.
CACHEABLE_WARN_BYTES = 1_800_000

#: What `section` values a caller may ask for. `all` is the legacy whole-corpus
#: response and is the one that cannot satisfy the invariant above — it is kept
#: only so a frontend from before this change keeps working through a rolling
#: deploy, and it is the only section exempt from the size check.
SECTIONS = ("all", "companies", "insiders", "filings")

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
    """Shape rows for the client. Shared so the two query paths cannot drift.

    `name` IS OMITTED WHEN A SLUG EXISTS, and that is not a micro-optimisation.
    `insiderPath` in frontend/src/lib/insider-url.ts returns `/insider/{slug}`
    the moment a slug is present and never looks at the name — so for 99.3% of
    rows the name was bytes nobody read. Sending it roughly doubled the insider
    payload, which is what pushed this response past the cache ceiling
    documented at NEXT_DATA_CACHE_MAX_BYTES.

    Rows with no slug keep their name, because that path still needs it to
    build `/insider/{derived-name}-{id}`.

    Rows with no insider_id are dropped HERE, before any caller slices. The
    client used to filter them after fetching and before chunking, in that
    order and deliberately (see the comment in sitemaps/[section]/route.ts): if
    a dropped row were removed after slicing, it would shrink one chunk and
    leave a gap no other chunk covers, and an insider would fall out of the
    sitemap entirely on a boundary. Now that the API does the slicing, the API
    has to own that ordering.
    """
    out: list[dict] = []
    for r in rows:
        if not r["insider_id"]:
            continue
        slug = r["slug"] or ""
        item = {"id": encode_insider_id(r["insider_id"]), "slug": slug}
        if not slug:
            item["name"] = r["name"] or ""
        out.append(item)
    return out


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


def _cached_full(limit_insiders: int, filing_days: int) -> dict:
    """The whole corpus, computed at most once an hour."""
    key = (int(limit_insiders), int(filing_days))
    hit = _cache.get(key)
    now = time.monotonic()
    if hit and hit[0] > now:
        return hit[1]
    result = _sitemap_urls_uncached(key[0], key[1])
    if result["counts"]["tickers"]:          # never cache an empty answer from a DB hiccup
        _cache[key] = (now + _CACHE_TTL_S, result)
    return result


def project_section(full: dict, section: str, chunk: int, chunk_size: int) -> dict:
    """Cut one sitemap file's worth of URLs out of the whole corpus.

    Pure, and separated from the request handler so a test can drive it with a
    synthetic corpus of any size — which is the only way to assert the size
    invariant for a population we do not have yet.

    `counts` stays the FULL corpus count on every section, so a monitor reading
    one section can still see the totals and notice a collapse. `returned` is
    what this particular response carries.
    """
    body: dict = {"tickers": [], "insiders": [], "filings": []}
    lo, hi = chunk * chunk_size, (chunk + 1) * chunk_size

    if section == "all":
        body = {k: full[k] for k in ("tickers", "insiders", "filings")}
    elif section == "companies":
        # Not chunked: one file holds every ticker, and 10,683 of them is an
        # order of magnitude under the protocol cap.
        body["tickers"] = full["tickers"]
    elif section == "insiders":
        body["insiders"] = full["insiders"][lo:hi]
    elif section == "filings":
        body["filings"] = full["filings"][lo:hi]

    out = dict(body)
    out["counts"] = full["counts"]
    out["returned"] = {k: len(body[k]) for k in ("tickers", "insiders", "filings")}
    return out


@router.get("/urls")
def sitemap_urls(
    limit_insiders: int = Query(default=45000, ge=100, le=200000),
    filing_days: int = Query(default=90, ge=7, le=365),
    # ── WHY THIS IS SLICED SERVER-SIDE ──────────────────────────────────────
    # Four sitemap files each used to fetch the ENTIRE corpus and slice their
    # own 20,000 URLs out of it in the client. Four full transfers and four
    # full JSON parses per crawl, none of them cacheable (see
    # NEXT_DATA_CACHE_MAX_BYTES), of a payload whose 30,764 insider objects
    # inflate about tenfold as live JS objects. That is what exhausted the
    # frontend heap on 2026-10-01.
    #
    # Slicing here costs nothing: the full corpus is already computed at most
    # once an hour by _cached_full, so a section request is a list slice.
    section: str = Query(default="all"),
    chunk: int = Query(default=0, ge=0),
    chunk_size: int = Query(default=20000, ge=1, le=50000),
) -> dict:
    if section not in SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown section {section!r}; expected one of {', '.join(SECTIONS)}",
        )

    full = _cached_full(int(limit_insiders), int(filing_days))
    out = project_section(full, section, int(chunk), int(chunk_size))

    # THE RUNTIME INVARIANT. Measured on what we are actually about to send,
    # every time, because the thing that broke was data growth and not a code
    # change. `all` is exempt: it is the legacy whole-corpus shape and cannot
    # satisfy the bound by construction.
    payload_bytes = len(json.dumps(out, separators=(",", ":")).encode())
    out["payload_bytes"] = payload_bytes
    out["cacheable"] = section == "all" or payload_bytes <= CACHEABLE_WARN_BYTES
    if section != "all" and payload_bytes > CACHEABLE_WARN_BYTES:
        logger.error(
            "sitemap section %s chunk %s is %d bytes, over the %d-byte "
            "cacheable budget (hard ceiling %d). Next will stop caching it and "
            "every crawl will re-parse it in the frontend heap. Lower CHUNK in "
            "frontend/src/lib/sitemap-data.ts or drop a field from the payload.",
            section, chunk, payload_bytes, CACHEABLE_WARN_BYTES,
            NEXT_DATA_CACHE_MAX_BYTES,
        )
    return out


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
