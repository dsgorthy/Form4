from __future__ import annotations

import time
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import add_signal_class_filter, add_trans_code_filter, filing_group_by
from api.titles import clean_title
from api.gating import get_free_cutoff_date, get_grace_cutoff_datetime, null_items_track_records, redact_gated_items
from api.id_encoding import decode_trade_id, encode_trade_id, encode_insider_id, encode_response_ids
from api.ownership import position_change
from api.signals_enrichment import enrich_items_with_signals
from api.context_enrichment import enrich_items_with_context
from api.price_dates import enrich_items_with_price_end
from api.classification import attach_classification
from api.ratings import attach_ratings
from api.trade_grade import enrich_items_with_trade_grade

router = APIRouter(prefix="/api/v1/filings", tags=["filings"])

# ── pagination total cache ───────────────────────────────────────────
#
# The COUNT is the expensive half of this endpoint. It groups the whole trades
# table — 1.74M rows down to 1.6M filings — and measured 1.8s of a 2.5s
# request on 2026-08-14, after a VACUUM had already taken the endpoint from
# 4.9s to 2.5s (the index-only scan was doing 154k heap fetches against a
# stale visibility map).
#
# That number is identical for every user on the same filter and only moves as
# new filings land, on a 5-minute ingest cadence. Recomputing it per request
# buys nothing: a total that is up to two minutes stale is invisible on a
# paginated feed, while a 1.8s wait is not.
#
# Deliberately in-process rather than Redis: there is no shared cache in this
# stack, each API worker warming its own copy is fine at this scale, and a
# cache that can fail is a new failure mode for the site's busiest route.
_COUNT_TTL_S = 120
_COUNT_CACHE_MAX = 256
_count_cache: dict[str, tuple[float, int]] = {}


def _cached_total(key: str, compute) -> int:
    """Memoize a pagination total for _COUNT_TTL_S seconds.

    Eviction is a full clear rather than LRU: filter combinations are a long
    tail, the entries are two ints, and the cost of being wrong is one
    recomputation. Bounded is the only property that matters here.
    """
    now = time.monotonic()
    hit = _count_cache.get(key)
    if hit is not None and now - hit[0] < _COUNT_TTL_S:
        return hit[1]
    total = compute()
    if len(_count_cache) >= _COUNT_CACHE_MAX:
        _count_cache.clear()
    _count_cache[key] = (now, total)
    return total




def _reconcile_positions(items: list[dict]) -> None:
    """Give list rows the same reconciled position the detail endpoint uses.

    The aggregate took MAX(shares_owned_after) across a filing's lots, which is
    "the biggest single balance" and not the holding. api/ownership documents
    what that costs: DST Global reports through seven partnerships, and the
    page and the Stocktwits post disagreed by 14x on one filing. The detail
    endpoint fixed it by reconciling through position_change; the list queries
    never did, so compute_trade_grade's Holdings factor scored the two
    endpoints differently — MED xm7gfj came out 61 "Notable" in the feed and
    59 "Modest" on its own page.

    Rather than reimplement the reconciliation in SQL, where it would drift
    from the Python, this refetches the lots for the page's filings in ONE
    query and calls the same function. When position_change declines to answer
    the balance is dropped rather than replaced with a guess, exactly as
    api/ownership instructs — the Holdings factor then contributes nothing,
    which is the correct outcome for a filing we cannot reconcile.
    """
    keyed = {
        (it["group_key"], it["insider_id"], it["ticker"], it["trade_type"]): it
        for it in items
        if it.get("group_key") and (it.get("lot_count") or 1) > 1
    }
    if not keyed:
        return                      # single-lot filings need no reconciling

    # Opens its own connection ON PURPOSE. The caller's `with get_db()` block
    # has already exited by the time enrichment runs, and borrowing that handle
    # raises "connection already closed" — the same way _blended_and_benchmark
    # broke /portfolio on 2026-08-19.
    # Split the keys by which column they came from, and compare each in its
    # OWN type. `COALESCE(txn_group_id::text, accession) IN (...)` reads
    # naturally and is a sequential scan of 1.65M rows — casting the column
    # defeats idx_trades_txn_group, and the COALESCE defeats both indexes. It
    # took the unfiltered feed from 0.36s to 2.5s. Written this way Postgres
    # bitmap-ORs the two existing indexes.
    #
    # txn_group_id is bigint and accession is text, so a key that parses as an
    # integer is a group id and anything else is an accession.
    group_ids, accessions = [], []
    for key, *_ in keyed:
        (group_ids if str(key).lstrip("-").isdigit() else accessions).append(key)

    clauses, params = [], []
    if group_ids:
        clauses.append(f"t.txn_group_id IN ({','.join('?' for _ in group_ids)})")
        params += [int(g) for g in group_ids]
    if accessions:
        clauses.append(f"t.accession IN ({','.join('?' for _ in accessions)})")
        params += accessions

    with get_db() as conn:
        rows = conn.execute(
            f"""SELECT COALESCE(t.txn_group_id::text, t.accession) AS group_key,
                       t.insider_id, t.ticker, t.trade_type,
                       t.qty, t.shares_owned_after, t.trade_date, t.trade_id,
                       t.direct_indirect, t.nature_of_ownership
                  FROM trades t
                 WHERE ({' OR '.join(clauses)})
                   AND t.superseded_by IS NULL
                   AND t.is_derivative = 0""",
            tuple(params),
        ).fetchall()

    grouped: dict[tuple, list[dict]] = {}
    for r in rows:
        k = (r["group_key"], r["insider_id"], r["ticker"], r["trade_type"])
        if k in keyed:
            grouped.setdefault(k, []).append(dict(r))

    for k, lots in grouped.items():
        item = keyed[k]
        pc = position_change(lots, is_buy=(k[3] == "buy"))
        if pc is not None:
            item["shares_owned_after"] = pc.after
            item["qty"] = pc.qty
        else:
            item["shares_owned_after"] = None


def _display_titles(items):
    """Replace the stored title with a renderable one, in place.

    The `title` column is typed by the filer and unvalidated, so it reaches us
    as "GroupPresident IntlVehiclePmts" or "Director,TenPercentOwner". Cleaning
    here rather than in each frontend means the feed, the filing page, the
    insider page and every OG card agree without any of them importing rules.
    `normalized_title` is left untouched — it is a classification used for
    filtering, not a label.
    """
    for it in items:
        if "title" in it:
            it["title"] = clean_title(it.get("title"))
    return items


@router.get("")
def list_filings(
    user: UserContext = Depends(get_current_user),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    min_value: Optional[float] = Query(default=None, ge=0),
    min_tier: Optional[int] = Query(default=None, ge=1, le=5),
    ticker: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    trans_codes: str = Query(default="P,S"),
    signal_class: Optional[str] = Query(default=None),
    hide_routine: bool = Query(default=False),
    hide_planned: bool = Query(default=False),
    include_private: bool = Query(default=False),
    min_grade: Optional[str] = Query(default=None, pattern="^[A-F]$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Paginated, filterable filings list with insider tier/score and returns."""
    conditions = ["t.superseded_by IS NULL", "t.is_derivative = 0"]
    if not include_private:
        conditions.append("t.ticker != 'NONE' AND t.ticker IS NOT NULL AND t.ticker != ''")
    params = []

    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None
    grace_cutoff = get_grace_cutoff_datetime() if user.is_grace else None

    add_trans_code_filter(conditions, params, trans_codes)
    add_signal_class_filter(conditions, params, signal_class)

    if trade_type is not None:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)
    if min_value is not None:
        conditions.append("t.value >= ?")
        params.append(min_value)
    # ── Quality filters are the product ──────────────────────────────────
    #
    # min_grade and min_tier both filter on t.pit_grade, which is our scoring
    # output, not a fact from the filing. Until 2026-08-18 either could be
    # passed by anyone, including an anonymous caller: `?min_grade=A` returned
    # a clean list of A-graded buys with no account at all. That is the whole
    # Pro proposition available over an unauthenticated GET.
    #
    # The rule Derek set is that we may tell you an insider traded a ticker,
    # but selecting on quality is Pro. A 403 rather than silently dropping the
    # filter: a caller that asks for A-grade buys and receives everything has
    # been given wrong data, which is worse than being told no.
    if (min_grade is not None or min_tier is not None) and not user.is_pro:
        raise HTTPException(
            status_code=403,
            detail=(
                "Filtering by insider grade is a Pro feature. "
                "Start a 7-day trial at /pricing — no card required."
            ),
        )

    if min_tier is not None:
        if min_tier >= 3:
            conditions.append("t.pit_grade = 'A'")
        elif min_tier >= 2:
            conditions.append("t.pit_grade IN ('A', 'B')")
        else:
            conditions.append("t.pit_grade IS NOT NULL")
    if ticker is not None:
        conditions.append("t.ticker = ?")
        params.append(ticker.upper())
    if date_from is not None:
        conditions.append("t.trade_date >= ?")
        params.append(date_from)
    if date_to is not None:
        conditions.append("t.trade_date <= ?")
        params.append(date_to)
    if hide_routine:
        conditions.append("(t.is_routine != 1 OR t.is_routine IS NULL)")
        conditions.append("(t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)")
    if hide_planned:
        conditions.append("(t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)")

    # Grace tier: 24h signal delay — hide filings filed in the last 24h
    if grace_cutoff:
        conditions.append("COALESCE(t.filed_at, t.filing_date) <= ?")
        params.append(grace_cutoff)

    # Grade filter: uses pre-computed signal_grade column on trades table
    grade_filter_active = min_grade is not None
    if grade_filter_active:
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3, "D": 4, "F": 5}
        min_idx = grade_order.get(min_grade, grade_order.get(min_grade.upper(), 5))
        allowed = [g for g, idx in grade_order.items() if idx <= min_idx and g != "F"]
        if allowed:
            placeholders = ",".join("?" * len(allowed))
            conditions.append(f"t.pit_grade IN ({placeholders})")
            params.extend(allowed)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    count_join = ""
    data_itr_join = ""

    _fgb = filing_group_by()

    fetch_limit = limit
    fetch_offset = offset

    # Date window optimization — skip when grade filtering (sparse results need wider scan)
    has_date_filter = date_from is not None or date_to is not None
    rows_needed = fetch_offset + fetch_limit
    if grade_filter_active:
        date_window = ""
    elif not has_date_filter and rows_needed <= 1000:
        days_needed = max((rows_needed // 4) + 7, 14)
        date_window = f"AND COALESCE(t.filed_at, t.filing_date) >= date('now', '-{days_needed} days')"
    else:
        date_window = ""

    with get_db() as conn:
        # For grade-filtered queries, skip expensive COUNT (full GROUP BY scan)
        # and use a fast estimate from the indexed signal_grade column instead.
        cache_key = repr((grade_filter_active, where_clause, count_join, params))
        if grade_filter_active:
            def _compute_total():
                row = conn.execute(
                    f"SELECT COUNT(*) AS cnt FROM trades t WHERE {where_clause}",
                    params,
                ).fetchone()
                # Rough estimate: each filing averages ~2.5 lots
                return row["cnt"] // 2

            total = _cached_total(cache_key, _compute_total)
        else:
            def _compute_total():
                row = conn.execute(
                    f"""
                    SELECT COUNT(*) AS cnt FROM (
                        SELECT 1
                        FROM trades t
                        {count_join}
                        WHERE {where_clause}
                        GROUP BY COALESCE(t.txn_group_id::text, t.accession), t.ticker, t.trade_type
                    )
                    """,
                    params,
                ).fetchone()
                return row["cnt"]

            total = _cached_total(cache_key, _compute_total)

        # Two-phase query: GROUP BY txn_group_id to collapse duplicate filers
        # reporting the same economic event. Picks the "best" insider (highest
        # track record score) as the representative filer.
        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.best_insider_id AS insider_id, agg.ticker, agg.company, agg.title, agg.normalized_title,
                agg.trade_type, agg.trade_date, agg.last_trade_date,
                agg.filing_date, agg.filed_at,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession, agg.trans_code,
                -- Already aliased by the aggregate. The reconciler keys on it to
                -- refetch exactly the lots this row collapsed.
                agg._group_key AS group_key,
                agg.signal_class, agg.is_10b5_1, agg.is_routine,
                agg.is_largest_ever, agg.dip_1mo, agg.dip_3mo, agg.cluster_size_pit,
                agg.cohen_routine, agg.shares_owned_after, agg.is_rare_reversal, agg.insider_switch_rate, agg.week52_proximity,
                agg.pit_grade, agg.pit_blended_score, agg.career_grade,
                agg.n_filers, agg.n_filings, agg.is_amendment, agg.document_type,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                -- Canonical insider URL, so every row on the feed and the
                -- landing page links to the slug form rather than the name+CIK
                -- shape. See insiderPath in lib/insider-url.ts. No braces in
                -- this comment: these queries are f-strings.
                i.slug AS insider_slug,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                    SELECT
                        MIN(t.trade_id) AS trade_id,
                        -- Pick the most relevant insider as representative
                        -- Prefer C-suite, then lowest insider_id for determinism
                        CASE WHEN MAX(t.is_csuite) = 1
                            THEN MIN(CASE WHEN t.is_csuite = 1 THEN t.insider_id END)
                            ELSE MIN(t.insider_id)
                        END AS best_insider_id,
                        t.ticker,
                        MAX(t.company) AS company,
                        MAX(t.title) AS title,
                        MAX(t.normalized_title) AS normalized_title,
                        t.trade_type,
                        MIN(t.trade_date) AS trade_date,
                        MAX(t.trade_date) AS last_trade_date,
                        MIN(t.filing_date) AS filing_date,
                        MAX(t.filed_at) AS filed_at,
                        ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                        SUM(t.qty) AS qty,
                        SUM(t.value) AS value,
                        COUNT(*) AS lot_count,
                        MAX(t.is_csuite) AS is_csuite,
                        MIN(t.accession) AS accession,
                        GROUP_CONCAT(DISTINCT t.trans_code) AS trans_code,
                        -- Scoring inputs. compute_trade_grade reads all of
                        -- these; when the list query omitted them the same
                        -- trade scored differently in the feed than on its own
                        -- page (MED xm7gfj: 58 vs 59 on 2026-08-21). The gap
                        -- reaches 13 points — Dip is worth up to 10 and
                        -- Largest Trade 3 — against bands only 10 wide, so two
                        -- surfaces could publish two different ratings for one
                        -- filing. MIN on the dips because more negative is a
                        -- deeper drawdown and the scorer takes the best one.
                        MAX(t.is_largest_ever) AS is_largest_ever,
                        MIN(t.dip_1mo) AS dip_1mo,
                        MIN(t.dip_3mo) AS dip_3mo,
                        MAX(t.pit_cluster_size) AS cluster_size_pit,
                        -- Constant within the group (derived from trans_code);
                        -- MAX is a picker, not a choice. api/classification
                        -- turns it into the published filing kind.
                        MAX(t.signal_class) AS signal_class,
                        MAX(t.is_10b5_1) AS is_10b5_1,
                        MAX(t.is_routine) AS is_routine,
                        MAX(t.cohen_routine) AS cohen_routine,
                        MAX(t.shares_owned_after) AS shares_owned_after,
                        MAX(t.is_rare_reversal) AS is_rare_reversal,
                        MAX(t.insider_switch_rate) AS insider_switch_rate,
                        MAX(t.week52_proximity) AS week52_proximity,
                        MAX(t.pit_grade) AS pit_grade,
                        MAX(t.pit_blended_score) AS pit_blended_score,
                        MAX(t.career_grade) AS career_grade,
                        COUNT(DISTINCT t.insider_id) AS n_filers,
                        COUNT(DISTINCT t.accession) AS n_filings,
                        MAX(t.is_amendment) AS is_amendment,
                        MAX(t.document_type) AS document_type,
                        COALESCE(t.txn_group_id::text, t.accession) AS _group_key
                    FROM trades t
                    {data_itr_join}
                    WHERE {where_clause}
                    {date_window}
                    GROUP BY COALESCE(t.txn_group_id::text, t.accession), t.ticker, t.trade_type
                    ORDER BY MAX(COALESCE(t.filed_at, t.filing_date)) DESC, SUM(t.value) DESC
                    LIMIT ? OFFSET ?
            ) agg
            LEFT JOIN insiders i ON agg.best_insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY COALESCE(agg.filed_at, agg.filing_date) DESC, agg.value DESC
            """,
            params + [fetch_limit, fetch_offset],
        ).fetchall()

    items = _display_titles([dict(r) for r in rows])

    # Enrich with signal tags, context facts, and price end dates
    with get_db() as sig_conn:
        enrich_items_with_signals(sig_conn, items)
        enrich_items_with_context(sig_conn, items)
    enrich_items_with_price_end(items)
    _reconcile_positions(items)
    enrich_items_with_trade_grade(None, items)
    attach_ratings(items)
    attach_classification(items)

    # The score, stars and label are the teaser and stay public. `factors` is
    # the model — named signals with their point contributions — and reading a
    # few hundred of those reconstructs the weighting. Pro only.
    if not user.is_pro:
        for item in items:
            grade = item.get("trade_grade")
            if isinstance(grade, dict):
                grade.pop("factors", None)

    if free_cutoff:
        items = null_items_track_records(items)
        for item in items:
            item["gated"] = item["trade_date"] < free_cutoff
        items = redact_gated_items(items)
    elif grace_cutoff:
        # Grace: track records nulled, but no gated/redacted items (just 24h delayed)
        items = null_items_track_records(items)
    encode_response_ids(items)

    resp: dict = {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }
    if free_cutoff:
        resp["free_cutoff"] = free_cutoff
    if grace_cutoff:
        resp["grace_delay"] = True
    return resp


@router.get("/{trade_id}/related")
def get_related_trades(trade_id: str, limit: int = Query(default=5, ge=1, le=20), user: UserContext = Depends(get_current_user)) -> List[dict]:
    """Up to N other filings by the same insider, aggregated by accession, ordered by filing_date DESC."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        base = conn.execute(
            "SELECT insider_id, accession FROM trades WHERE trade_id = ?",
            (raw_id,),
        ).fetchone()

    if base is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    insider_id = base["insider_id"]
    current_accession = base["accession"]
    _fgb = filing_group_by()

    with get_db() as conn:
        # Exclude the current filing (by accession if available, else by trade_id)
        if current_accession:
            exclude_clause = "AND NOT (t.accession = ?)"
            exclude_param = current_accession
        else:
            exclude_clause = "AND t.trade_id != ?"
            exclude_param = raw_id

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title, agg.normalized_title,
                agg.trade_type, agg.trade_date, agg.last_trade_date,
                agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession, agg.trans_code,
                agg.signal_class, agg.is_10b5_1, agg.is_routine,
                agg.is_largest_ever, agg.dip_1mo, agg.dip_3mo, agg.cluster_size_pit,
                agg.cohen_routine, agg.shares_owned_after, agg.is_rare_reversal, agg.insider_switch_rate, agg.week52_proximity,
                agg.pit_grade, agg.pit_blended_score, agg.career_grade,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                -- Canonical insider URL, so every row on the feed and the
                -- landing page links to the slug form rather than the name+CIK
                -- shape. See insiderPath in lib/insider-url.ts. No braces in
                -- this comment: these queries are f-strings.
                i.slug AS insider_slug,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, t.ticker, MAX(t.company) AS company, MAX(t.title) AS title, MAX(t.normalized_title) AS normalized_title,
                    t.trade_type,
                    MIN(t.trade_date) AS trade_date,
                    MAX(t.trade_date) AS last_trade_date,
                    MIN(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    COUNT(*) AS lot_count,
                    MAX(t.is_csuite) AS is_csuite, MIN(t.accession) AS accession,
                    GROUP_CONCAT(DISTINCT t.trans_code) AS trans_code,
                    -- See the sibling query above: these four are scoring
                    -- inputs and their absence made the feed and the filing
                    -- page disagree.
                    MAX(t.is_largest_ever) AS is_largest_ever,
                    MIN(t.dip_1mo) AS dip_1mo,
                    MIN(t.dip_3mo) AS dip_3mo,
                    MAX(t.pit_cluster_size) AS cluster_size_pit,
                    MAX(t.signal_class) AS signal_class,
                    MAX(t.is_10b5_1) AS is_10b5_1,
                    MAX(t.is_routine) AS is_routine,
                    MAX(t.cohen_routine) AS cohen_routine,
                    MAX(t.shares_owned_after) AS shares_owned_after,
                    MAX(t.is_rare_reversal) AS is_rare_reversal,
                        MAX(t.insider_switch_rate) AS insider_switch_rate,
                    MAX(t.week52_proximity) AS week52_proximity,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score,
                    MAX(t.career_grade) AS career_grade
                FROM trades t
                WHERE t.insider_id = ?
                  {exclude_clause}
                  AND t.trans_code IN ('P', 'S')
                  AND t.superseded_by IS NULL
                  AND t.is_derivative = 0
                GROUP BY t.insider_id, t.ticker, t.trade_type, {_fgb}
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.filing_date DESC
            LIMIT ?
            """,
            (insider_id, exclude_param, limit),
        ).fetchall()

    items = _display_titles([dict(r) for r in rows])
    if not user.is_pro:
        items = null_items_track_records(items)
    # Grace users: filter out filings from last 24h
    if user.is_grace:
        from api.gating import get_grace_cutoff_datetime as _grace_cutoff
        cutoff = _grace_cutoff()
        items = [i for i in items if (i.get("filing_date") or "") <= cutoff]
    encode_response_ids(items)
    return items


@router.get("/{trade_id}")
def get_filing(trade_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Single filing detail with lot breakdown."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT
                t.trade_id, t.insider_id, t.ticker, t.company, t.title, t.normalized_title,
                t.trade_type, t.trade_date, t.filing_date, t.filed_at,
                t.price, t.qty, t.value, t.is_csuite, t.title_weight,
                t.source, t.accession, t.trans_code,
                -- Both are lot-scoping keys; see the sibling-lot query below.
                t.is_derivative, t.security_title,
                -- Set only when price_validator.py judged the filing wrong and
                -- repaired it. Surfaced on the page: a number that disagrees
                -- with EDGAR has to say so itself.
                t.price_as_filed, t.value_as_filed, t.correction_method,
                t.value_suspect,
                t.signal_class,
                t.is_10b5_1, t.is_routine, t.cohen_routine, t.shares_owned_after, t.is_rare_reversal, t.insider_switch_rate, t.week52_proximity,
                -- Flags consumed by api/narrative.classify_tier
                COALESCE(t.is_tax_sale, 0) AS is_tax_sale,
                COALESCE(t.is_recurring, 0) AS is_recurring,
                COALESCE(t.is_largest_ever, 0) AS is_largest_ever,
                -- Aliased, not bare. compute_trade_grade reads `cluster_size`
                -- (falling back to `n_filers`); it has never read
                -- `pit_cluster_size`, so selecting it under that name meant the
                -- cluster factor — worth up to 12 points — silently scored 0
                -- on this endpoint.
                t.pit_cluster_size AS cluster_size_pit,
                -- Dip depth, worth up to 10. Absent here entirely until
                -- 2026-08-21 while the list queries had it, so the same filing
                -- graded differently depending on which page you were on.
                t.dip_1mo, t.dip_3mo,
                t.pit_grade, t.pit_blended_score, t.career_grade,
                t.is_amendment, t.document_type, t.date_of_orig_sub,
                COALESCE(i.is_entity, 0) as is_entity,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                -- The canonical insider URL. Without it the page falls back to
                -- name+CIK, which resolved for nobody: /insider/benjamin-wood-
                -- 0002123683 was a soft 404 on all 143,653 filing pages whose
                -- insider carries a CIK.
                i.slug AS insider_slug,
                tr.entry_price,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.spy_return_7d, tr.spy_return_30d, tr.spy_return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM trades t
            LEFT JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trade_id = ?
            """,
            (raw_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Filing not found")

        # Grace tier: block access to filings filed in the last 24h
        if user.is_grace:
            filed = row["filed_at"] or row["filing_date"] or ""
            if filed > get_grace_cutoff_datetime():
                raise HTTPException(status_code=403, detail="This filing is delayed 24h for your account. Upgrade for real-time access.")

        result = _display_titles([dict(row)])[0]

        # Add effective insider info if different from trade's insider
        try:
            if row["insider_id"]:
                eff = conn.execute("""
                    SELECT t.effective_insider_id FROM trades t WHERE t.trade_id = ?
                """, (raw_id,)).fetchone()
                if eff and eff["effective_insider_id"] and eff["effective_insider_id"] != row["insider_id"]:
                    eff_insider = conn.execute("""
                        SELECT insider_id, name, cik FROM insiders WHERE insider_id = ?
                    """, (eff["effective_insider_id"],)).fetchone()
                    if eff_insider:
                        result["effective_insider"] = dict(eff_insider)
                        result["effective_insider"]["insider_id"] = encode_insider_id(result["effective_insider"]["insider_id"])
        except Exception:
            pass

        # Sibling lots of the SAME security in the same filing.
        #
        # Four scoping rules, each of which was a wrong "Total Value" on this
        # page. is_derivative: 895 filings mix common and derivative rows, and
        # derivative notional runs to $180 quadrillion at the top end, so one
        # blended sum produced the trillion-dollar totals. security_title:
        # different share classes are different decisions — LILA summed Class
        # A, Class C and Series A Preference into a single $27.9M figure, and
        # a TSMC filing summed TWD-priced Taiwan shares with USD ADRs.
        # superseded_by: an amended row would otherwise be counted twice.
        # ticker: the accession branch never had one, so a filing covering two
        # issuers merged them.
        lot_cols = ("t.trade_id, t.trade_date, t.price, t.qty, t.value, t.accession, "
                    "t.shares_owned_after, t.cohen_routine, t.direct_indirect, "
                    "t.nature_of_ownership")
        lot_scope = ("t.is_derivative = ? AND t.superseded_by IS NULL "
                     "AND t.ticker = ? AND COALESCE(t.security_title,'') = ?")
        lot_common = (row["is_derivative"], row["ticker"], row["security_title"] or "")
        if row["accession"]:
            lots = conn.execute(
                f"""
                SELECT {lot_cols}
                FROM trades t
                WHERE t.accession = ? AND t.insider_id = ? AND t.trade_type = ?
                  AND {lot_scope}
                ORDER BY t.trade_date, t.price
                """,
                (row["accession"], row["insider_id"], row["trade_type"], *lot_common),
            ).fetchall()
        else:
            lots = conn.execute(
                f"""
                SELECT {lot_cols}
                FROM trades t
                WHERE t.insider_id = ? AND t.trade_date = ? AND t.trade_type = ?
                  AND {lot_scope}
                ORDER BY t.price
                """,
                (row["insider_id"], row["trade_date"], row["trade_type"], *lot_common),
            ).fetchall()

        if len(lots) > 1:
            lot_list = [dict(l) for l in lots]
            for lot in lot_list:
                if lot.get("trade_id") is not None:
                    lot["trade_id"] = encode_trade_id(lot["trade_id"])
            result["lots"] = lot_list
            result["total_qty"] = sum(l["qty"] for l in lots)
            result["total_value"] = sum(l["value"] for l in lots)
            # Use filing-level aggregated data for quality scoring consistency
            result["qty"] = result["total_qty"]
            result["value"] = result["total_value"]
            # Position after the filing, summed across ownership lines rather
            # than taken from the largest one. max() answers "the biggest
            # single balance", which is not the holding — DST Global's seven
            # partnerships made the page and the Stocktwits post disagree by
            # 14x on the same filing. api.ownership carries the reconciliation.
            pc = position_change(
                [dict(l) for l in lots], is_buy=(row["trade_type"] == "buy")
            )
            if pc is not None:
                result["shares_owned_after"] = pc.after
                result["position_before"] = pc.before
                result["position_change_pct"] = pc.fraction
                result["ownership_lines"] = pc.lines
            # cohen_routine: max (if any lot is routine, filing is routine)
            lot_cohen = [l["cohen_routine"] for l in lots if l["cohen_routine"] is not None]
            if lot_cohen:
                result["cohen_routine"] = max(lot_cohen)
        else:
            result["lots"] = []

    # Enrich with signal tags, context facts, and price end dates before encoding IDs
    with get_db() as sig_conn:
        enrich_items_with_signals(sig_conn, [result])
        enrich_items_with_context(sig_conn, [result])
    enrich_items_with_price_end([result])
    enrich_items_with_trade_grade(None, [result])
    attach_ratings([result])
    attach_classification([result])

    # "Why this matters" narrative — always present, with depth proportional to signal.
    #   high_signal → LLM-generated 4-field narrative (from trade_narrative)
    #   routine     → templated 1-sentence reason (scheduled / tax / recurring)
    #   low_signal  → templated 2-sentence summary (open-market, no special flags)
    # See api/narrative.py for the classifier + template logic.
    from api.narrative import build_narrative
    llm_narrative = None
    with get_db() as narr_conn:
        narr_row = narr_conn.execute(
            """SELECT summary, price_context, catalysts, risks,
                      generated_at::text AS generated_at, model_name
               FROM trade_narrative
               WHERE trade_id = ? AND summary IS NOT NULL""",
            (raw_id,),
        ).fetchone()
        if narr_row:
            llm_narrative = dict(narr_row)
    result["narrative"] = build_narrative(result, llm_narrative)

    if not user.is_pro:
        from api.gating import null_track_record_fields
        null_track_record_fields(result)
        # Same split as the list endpoint: the star rating is the teaser, the
        # factor breakdown is the model.
        grade = result.get("trade_grade")
        if isinstance(grade, dict):
            grade.pop("factors", None)

    # Encode top-level IDs
    if result.get("trade_id") is not None:
        result["trade_id"] = encode_trade_id(result["trade_id"])
    if result.get("insider_id") is not None:
        result["insider_id"] = encode_insider_id(result["insider_id"])

    return result


@router.get("/{trade_id}/what-if")
def what_if_simulation(trade_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Hypothetical performance at different time horizons and instruments.
    Shows what would have happened if you followed this insider's trade."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT t.trade_id, t.ticker, t.trade_type, t.trade_date, t.filing_date, t.price,
                   tr.return_7d, tr.return_30d, tr.return_90d, tr.return_180d, tr.return_365d,
                   tr.spy_return_7d, tr.spy_return_30d, tr.spy_return_90d,
                   tr.spy_return_180d, tr.spy_return_365d,
                   tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d,
                   tr.abnormal_180d, tr.abnormal_365d,
                   tr.entry_price
            FROM trades t
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trade_id = ?
            """,
            (raw_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Filing not found")

        result = _display_titles([dict(row)])[0]
        ticker = result["ticker"]
        filing_date = result["filing_date"]

        # Stock performance at each horizon
        horizons = []
        for window, days in [("7d", 7), ("30d", 30), ("90d", 90), ("180d", 180), ("365d", 365)]:
            ret = result.get(f"return_{window}")
            spy = result.get(f"spy_return_{window}")
            abn = result.get(f"abnormal_{window}")
            if ret is not None:
                entry = result.get("entry_price") or result.get("price") or 0
                horizons.append({
                    "window": window,
                    "days": days,
                    "stock_return": round(ret * 100, 2),
                    "spy_return": round(spy * 100, 2) if spy is not None else None,
                    "alpha": round(abn * 100, 2) if abn is not None else None,
                    "entry_price": round(entry, 2) if entry else None,
                    "exit_price": round(entry * (1 + ret), 2) if entry else None,
                    # Hypothetical P&L on $10K position
                    "pnl_10k": round(10000 * ret, 2),
                })

        # Options performance REMOVED 2026-08-13. It read prices.option_prices,
        # which is frozen at 2026-03-27 — the ThetaData subscription was
        # cancelled 2026-06-07 and there is no replacement feed. The block
        # silently returned nothing for any recent filing while looking like a
        # working feature, which is worse than not shipping it. The 23.5M
        # historical rows remain queryable in PG for backtests.

    return {
        "ticker": ticker,
        "trade_type": result["trade_type"],
        "filing_date": filing_date,
        "horizons": horizons,
    }
