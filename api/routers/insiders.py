from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import (
    MEANINGFUL_CLASSES,
    add_signal_class_filter,
    add_trans_code_filter,
    filing_group_by,
)
from api.gating import PUBLIC_FILING_STAT_FIELDS, PUBLIC_VOLUME_FIELDS, require_pro
from api.id_encoding import (
    decode_insider_id,
    encode_insider_id,
    encode_response_ids,
    identifier_from_slug,
)
from api.pit_helpers import get_best_pit_grade, get_ticker_grades
from api.signals_enrichment import enrich_items_with_signals
from api.context_enrichment import enrich_items_with_context
from api.price_dates import enrich_items_with_price_end

router = APIRouter(prefix="/api/v1/insiders", tags=["insiders"])

#: Minimum discretionary FILINGS before this product will publish an accuracy
#: percentage for an insider. Set to 5 on 2026-08-25.
#:
#: Filtering to discretionary filings shrinks the basis hard, and an accuracy
#: rendered to the nearest point over one or two filings can only ever read
#: 0%, 50% or 100%. Measured over insiders with >=5 sell lots, the corrected
#: basis lands: 2.1% at zero filings, 3.6% at one, 5.0% at two, 15.9% at 3-4,
#: 38.7% at 5-9, 26.6% at 10-24, 8.2% at 25+. A floor of 5 keeps the block on
#: 73.5% of sell records and 66.2% of buy records and drops the arithmetic
#: artifacts. Below it the API serves the filing count and nothing else.
MIN_SCORED_FILINGS = 5


def apply_scoring_floor(
    n: int,
    wins: Optional[int],
    avg_return: Optional[float],
    avg_abnormal: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Publish a window's figures only if it clears :data:`MIN_SCORED_FILINGS`.

    Returns ``(win_rate, avg_return, avg_abnormal)``, every element ``None``
    when the basis is too thin. Pure, so the floor is testable without a
    database -- it is the one rule standing between this product and an
    "accuracy" of 100% derived from a single filing.
    """
    if n is None or n < MIN_SCORED_FILINGS:
        return None, None, None
    return (
        round((wins or 0) / n, 4),
        None if avg_return is None else round(avg_return, 6),
        None if avg_abnormal is None else round(avg_abnormal, 6),
    )


def resolve_insider_id(conn, identifier: str) -> int | None:
    """Resolve a URL segment to an insider_id.

    Order matters. Clean slugs contain hyphens ("roger-s-penske"), so the
    legacy "strip everything after the last hyphen" rule would mangle them
    into "penske" and 404. The stored slug is therefore tried FIRST, as an
    exact match — which also covers disambiguated slugs, since those are
    stored whole ("john-smith-x7hq9r").

      1. exact slug           /insider/roger-s-penske        (current URLs)
      2. bare sqid            /insider/mgwdq7                (API clients)
      3. retired slug         /insider/jr-james-d-farley     (name since fixed)
      4. trailing-segment id  /insider/roger-s-penske-mgwdq7 (pre-slug URLs,
                                                              already indexed)
      5. CIK                  /insider/0001234567

    Every previously-published URL shape still resolves, so nothing that was
    linked or crawled breaks.
    """
    if not identifier:
        return None

    def _existing(insider_id: int | None) -> int | None:
        """A sqid only counts if it names a real row.

        Every character of "zzz" is in the insider alphabet, so it decodes
        cleanly to 624 — an insider who has nothing to do with the URL. Left
        unchecked, /insider/nobody-at-all-zzz serves that person's profile
        instead of a 404, and any mistyped or truncated URL can land on a
        stranger. Verifying here also lets the later rules run: without it a
        decodable trailing segment shadows the CIK lookup below.
        """
        if insider_id is None:
            return None
        hit = conn.execute(
            "SELECT insider_id FROM insiders WHERE insider_id = ?", (insider_id,)
        ).fetchone()
        return hit["insider_id"] if hit else None

    row = conn.execute(
        "SELECT insider_id FROM insiders WHERE slug = ?", (identifier,)
    ).fetchone()
    if row:
        return row["insider_id"]

    decoded = _existing(decode_insider_id(identifier))
    if decoded is not None:
        return decoded

    # A slug retired by a name correction. Checked before the trailing-segment
    # rule because a retired slug is a whole slug, and that rule would chop it.
    row = conn.execute(
        "SELECT insider_id FROM insider_slug_aliases WHERE old_slug = ?", (identifier,)
    ).fetchone()
    if row:
        return row["insider_id"]

    trailing = identifier_from_slug(identifier)
    if trailing != identifier:
        decoded = _existing(decode_insider_id(trailing))
        if decoded is not None:
            return decoded

    # CIK, bare or as the trailing segment of a slugged URL. Both shapes have
    # to be tried: the filing page builds /insider/{name}-{cik} whenever the
    # insider row carries a CIK, and matching only the whole identifier 404'd
    # every one of them — 10,279 insiders, reachable from 143,653 filing
    # pages. /insider/benjamin-wood-0002123683 was the reported case.
    candidates = [identifier] if trailing == identifier else [identifier, trailing]
    for candidate in candidates:
        row = conn.execute(
            "SELECT insider_id FROM insiders WHERE cik = ?", (candidate,)
        ).fetchone()
        if row:
            return row["insider_id"]
    return None


@router.get("/slug-aliases")
def get_slug_aliases() -> dict:
    """Retired slug -> current slug, for canonical redirects in middleware.

    MUST stay declared above /{identifier}: FastAPI matches in declaration
    order, so the dynamic route would otherwise swallow this path and treat
    "slug-aliases" as an insider identifier.

    Returned whole rather than queried per-request. The map is small (hundreds
    of entries, only growing when a name correction retires a slug) and the
    caller caches it, so canonical URLs — the overwhelming majority — cost no
    lookup at all. Unauthenticated: it is public URL structure, nothing more.
    """
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.old_slug, i.slug
                 FROM insider_slug_aliases a
                 JOIN insiders i ON i.insider_id = a.insider_id
                WHERE i.slug IS NOT NULL"""
        ).fetchall()
    return {r["old_slug"]: r["slug"] for r in rows}


@router.get("/{identifier}")
def get_insider(identifier: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Insider profile. Accepts a name slug, encoded sqids ID, or CIK.

    PUBLIC tier (anonymous, and therefore Googlebot): identity and activity —
    name, title, CIK, volume by type, ticker list, filing counts. Enough for a
    real <h1>, title tag and description, which is what makes the page worth
    indexing at all.

    PRO tier: the analytical layer — track record, scores, percentiles, win
    rates, sell pattern, PIT grades.

    This was require_pro until 2026-08-13, which 403'd every crawler. The
    result was a page whose title was the literal string "Insider Profile"
    with the person's name appearing nowhere in the server-rendered body —
    on what should be one of the strongest acquisition surfaces the product
    has.
    """
    with get_db() as conn:
        # resolve_insider_id already tries every published URL shape, CIK
        # included. A second cik lookup here could only ever repeat a query
        # that just failed, and having two copies of the rule is how one of
        # them came to be missing the trailing-segment case.
        insider = None
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.name_normalized, i.cik, i.slug, COALESCE(i.is_entity, 0) as is_entity FROM insiders i WHERE i.insider_id = ?",
                (decoded_id,),
            ).fetchone()

        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        track_record = conn.execute(
            "SELECT * FROM insider_track_records WHERE insider_id = ?",
            (insider_id,),
        ).fetchone()

        # insider_track_records counts EVERY row, derivatives included, and no
        # other surface does. Magnetar Financial showed "999 transactions
        # across 3 companies" because 833 real sales were padded with 166
        # derivative rows, and one of its three tickers (AJX) is derivative-
        # only — so the profile sentence said 2 companies while the social card
        # said 3, from the same profile payload.
        #
        # Recount the three display fields against signal_class, matching the
        # filter every feed, chart and post already uses. The rest of the
        # legacy row is left alone: it still carries columns the page reads,
        # and the registry has it flagged for removal separately.
        if track_record is not None:
            counts = conn.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE signal_class = 'discretionary_buy')  AS buys,
                  COUNT(*) FILTER (WHERE signal_class = 'discretionary_sell') AS sells,
                  COUNT(DISTINCT ticker) FILTER (
                      WHERE signal_class IN ('discretionary_buy', 'discretionary_sell')
                  ) AS n_tickers
                FROM trades
                WHERE insider_id = ?
                  AND superseded_by IS NULL
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                  AND NOT COALESCE(value_suspect, FALSE)
            """, (insider_id,)).fetchone()
            track_record = dict(track_record)
            track_record["buy_count"] = counts["buys"] or 0
            track_record["sell_count"] = counts["sells"] or 0
            track_record["n_tickers"] = counts["n_tickers"] or 0

        # Entity group info
        entity_group = None
        try:
            group_row = conn.execute("""
                SELECT ig.group_id, ig.group_name, ig.confidence, ig.method,
                       ig.primary_insider_id
                FROM insider_group_members igm
                JOIN insider_groups ig ON igm.group_id = ig.group_id
                WHERE igm.insider_id = ?
            """, (insider_id,)).fetchone()

            if group_row:
                members = conn.execute("""
                    SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.is_entity, igm.is_primary, igm.relationship
                    FROM insider_group_members igm
                    JOIN insiders i ON igm.insider_id = i.insider_id
                    WHERE igm.group_id = ?
                    ORDER BY igm.is_primary DESC
                """, (group_row["group_id"],)).fetchall()

                entity_group = {
                    "group_id": group_row["group_id"],
                    "group_name": group_row["group_name"],
                    "confidence": group_row["confidence"],
                    "method": group_row["method"],
                    "primary_insider_id": group_row["primary_insider_id"],
                    "members": [dict(m) for m in members],
                }
        except Exception:
            pass  # Tables may not exist yet

        # Volume breakdown by transaction type (filing-level: one filing = one trade).
        # Exclude derivative titles (options/warrants) — they came over from
        # research.derivative_trades with notional pricing that dwarfs real
        # stock-volume aggregates. NULL security_title is treated as common
        # stock for back-compat with older filings.
        volume_rows = conn.execute("""
            SELECT trans_code,
                   CASE WHEN trans_code IN ('P') THEN 'buy'
                        WHEN trans_code IN ('S') THEN 'sell'
                        ELSE MAX(trade_type) END AS trade_type,
                   COUNT(*) AS count, SUM(total_value) AS total_value
            FROM (
                SELECT trans_code, MAX(trade_type) AS trade_type, SUM(value) AS total_value
                FROM trades
                WHERE insider_id = ? AND trans_code IS NOT NULL
                  AND superseded_by IS NULL
                  AND is_derivative = 0
                  -- Match the /trades endpoint exactly. Without this, a row
                  -- stored twice inflated the volume stats, and the empty
                  -- state below it would offer to show records that the table
                  -- then filtered out. 20,299 rows across the corpus.
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                GROUP BY trans_code, COALESCE(filing_key, accession, trade_date)
            )
            GROUP BY trans_code
            ORDER BY total_value DESC
        """, (insider_id,)).fetchall()

        # Filing-level win rates, all three windows, discretionary only.
        #
        # Rebuilt 2026-08-25. This block used to serve only 7d and the page
        # took 30d/90d straight from `insider_track_records`, which counts
        # execution LOTS. One table row therefore carried two denominators:
        # Romano Gianluca (27782) rendered "Filings 19" beside an accuracy
        # computed over 154 lots. See `docs/insider_track_record.md`.
        #
        # Three rules, and all three windows obey them identically:
        #   1. one row per FILING, not per lot. A purchase filled in five
        #      tranches is one decision, not five.
        #   2. discretionary only  -- a 10b5-1 plan sale, a tax withholding
        #      and an option exercise are not timing decisions. Derived from
        #      MEANINGFUL_CLASSES; never type the class names here.
        #   3. same duplicate/derivative/superseded exclusions as
        #      `filing_counts` below, so the header count and the denominator
        #      are the same population.
        #
        # AVG over the lots in a filing, not MAX: MAX reports the best lot in
        # a multi-lot filing as if it were the filing's outcome.
        _cls = tuple(MEANINGFUL_CLASSES)
        _cls_ph = ", ".join("?" for _ in _cls)
        # `%` cannot appear literally: this SQL goes through the ? -> %s compat
        # layer, which would read it as a format character. Bind the patterns.
        _pat_10pct, _pat_owner = "%10%", "%owner%"
        filing_win_rates = conn.execute(f"""
            WITH filings AS (
                SELECT t.trade_type, t.ticker,
                       COALESCE(t.filing_key, t.accession, t.trade_date) AS fkey,
                       MIN(t.trade_date) AS d,
                       -- "speculative holder": an entity/fund or a 10% owner.
                       -- Verified 2026-08-25 that every title containing "10"
                       -- is the 10% ownership designation, so this cannot
                       -- catch an unrelated number.
                       BOOL_OR(i.is_entity = 1
                               OR t.title ILIKE ?
                               OR t.title ILIKE ?) AS speculative,
                       AVG(tr.return_7d)    AS ret7,
                       AVG(tr.return_30d)   AS ret30,
                       AVG(tr.return_90d)   AS ret90,
                       AVG(tr.abnormal_7d)  AS abn7,
                       AVG(tr.abnormal_30d) AS abn30,
                       AVG(tr.abnormal_90d) AS abn90
                FROM trades t
                JOIN insiders i ON i.insider_id = t.insider_id
                -- LEFT, deliberately: a filing with no returns yet still has
                -- to occupy its place in the sequence below, or a later sale
                -- looks like it followed fewer purchases than it did.
                LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
                WHERE t.insider_id = ? AND t.trans_code IN ('P', 'S')
                  AND t.superseded_by IS NULL
                  AND t.is_derivative = 0
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.signal_class IN ({_cls_ph})
                GROUP BY t.trade_type, t.ticker,
                         COALESCE(t.filing_key, t.accession, t.trade_date)
            ), seq AS (
                SELECT *,
                    COUNT(*) FILTER (WHERE trade_type='buy')  OVER w AS prior_buys,
                    COUNT(*) FILTER (WHERE trade_type='sell') OVER w AS prior_sells
                FROM filings
                WINDOW w AS (PARTITION BY ticker ORDER BY d
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
            ), scoreable AS (
                -- Buys are scored as they come: a discretionary purchase is a
                -- decision by construction, and it carries +1.67% median
                -- abnormal 30d against a -0.63% baseline.
                --
                -- Sells are not. Measured over 291,033 discretionary sell
                -- filings, the median is -0.58% against a -0.58% baseline --
                -- exactly nothing. Only two conditions separate a sale that
                -- predicts from one that does not, and a sell is scored here
                -- only if it meets at least one:
                --
                --   1. FIRST SELL AFTER BUYING this ticker. -2.03% median with
                --      3+ prior purchases, -1.54% with 1-2, against -0.52%
                --      for a sale that follows other sales. The accumulator
                --      who has started selling.
                --   2. SPECULATIVE HOLDER. Entity/fund -1.58%, 10% owner
                --      -0.77%; officers -0.57% and directors -0.55%, which is
                --      the baseline. Employees sell to diversify.
                --
                -- Both flags together give -2.83%. Deliberately NOT used:
                -- fraction of the stake sold (flat, even a >90% exit), sale
                -- size vs the insider's own history (non-monotonic), and gap
                -- regularity (flat). See docs/insider_track_record.md.
                SELECT * FROM seq
                 WHERE trade_type = 'buy'
                    OR (prior_sells = 0 AND prior_buys >= 1)
                    OR speculative
            )
            SELECT
                trade_type,
                COUNT(ret7)  AS n7,
                COUNT(ret30) AS n30,
                COUNT(ret90) AS n90,
                -- "win" is directional: a buy wins when the stock rose, a
                -- sell wins when it fell. Counted explicitly rather than as
                -- (total - wins), which scored a flat 0.00% as a good sell.
                SUM(CASE WHEN (trade_type = 'buy'  AND ret7  > 0)
                           OR (trade_type = 'sell' AND ret7  < 0)
                         THEN 1 ELSE 0 END) AS wins7,
                SUM(CASE WHEN (trade_type = 'buy'  AND ret30 > 0)
                           OR (trade_type = 'sell' AND ret30 < 0)
                         THEN 1 ELSE 0 END) AS wins30,
                SUM(CASE WHEN (trade_type = 'buy'  AND ret90 > 0)
                           OR (trade_type = 'sell' AND ret90 < 0)
                         THEN 1 ELSE 0 END) AS wins90,
                AVG(ret7) AS avg_ret7, AVG(ret30) AS avg_ret30, AVG(ret90) AS avg_ret90,
                AVG(abn7) AS avg_abn7, AVG(abn30) AS avg_abn30, AVG(abn90) AS avg_abn90
            FROM scoreable
            GROUP BY trade_type
        """, (_pat_10pct, _pat_owner, insider_id, *_cls)).fetchall()

        # Filing-level trade counts (consistent with feed display).
        filing_counts = conn.execute("""
            SELECT
                SUM(CASE WHEN trans_code = 'P' THEN 1 ELSE 0 END) AS buy_filings,
                SUM(CASE WHEN trans_code = 'S' THEN 1 ELSE 0 END) AS sell_filings
            FROM (
                SELECT trans_code
                FROM trades
                WHERE insider_id = ? AND trans_code IN ('P', 'S')
                  AND superseded_by IS NULL
                  AND is_derivative = 0
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                -- Grouped exactly as the /trades endpoint groups, ticker
                -- included, so the stat grid, the summary sentence and the
                -- table header cannot report three different totals for the
                -- same person the way they did for Sylebra Capital (15/14/15).
                GROUP BY ticker, filing_key, trans_code
            )
        """, (insider_id,)).fetchone()

        # Sell pattern breakdown (filing-level) — common stock only.
        sell_pattern = conn.execute("""
            SELECT
                COUNT(*) AS total_sells,
                SUM(CASE WHEN planned = 1 THEN 1 ELSE 0 END) AS planned_sells,
                SUM(CASE WHEN routine = 1 THEN 1 ELSE 0 END) AS routine_sells
            FROM (
                SELECT MAX(is_10b5_1) AS planned, MAX(is_routine) AS routine
                FROM trades
                WHERE insider_id = ? AND trans_code = 'S'
                  AND superseded_by IS NULL
                  AND is_derivative = 0
                GROUP BY COALESCE(filing_key, accession, trade_date)
            )
        """, (insider_id,)).fetchone()

        # PIT grade data (per-ticker)
        best_pit = get_best_pit_grade(conn, insider_id)
        ticker_grades = get_ticker_grades(conn, insider_id)

    TRANS_CODE_LABELS = {
        "P": "Open-Market Purchase",
        "S": "Open-Market Sale",
        "M": "Option Exercise",
        "F": "Tax Withholding",
        "A": "Award/Grant",
        "G": "Gift",
        "X": "RSU Exercise",
        "V": "Voluntary Report",
    }

    result = dict(insider)
    result["insider_id"] = encode_insider_id(result["insider_id"])
    result["volume_by_type"] = [
        {
            "trans_code": r["trans_code"],
            "label": TRANS_CODE_LABELS.get(r["trans_code"], r["trans_code"]),
            "trade_type": r["trade_type"],
            "count": r["count"],
            "total_value": r["total_value"],
        }
        for r in volume_rows
    ]
    result["track_record"] = dict(track_record) if track_record else None
    if result["track_record"] and result["track_record"].get("insider_id") is not None:
        result["track_record"]["insider_id"] = encode_insider_id(result["track_record"]["insider_id"])
    if entity_group:
        if entity_group.get("primary_insider_id") is not None:
            entity_group["primary_insider_id"] = encode_insider_id(entity_group["primary_insider_id"])
        for m in entity_group.get("members", []):
            if m.get("insider_id") is not None:
                m["insider_id"] = encode_insider_id(m["insider_id"])
    result["entity_group"] = entity_group

    # Filing-level win rates are the ONLY source for the track-record block.
    # There is deliberately no fallback to `insider_track_records` here: those
    # columns count lots, and no daily writer has refreshed them since
    # February 2026 (`sync_to_track_records` writes score/counts/dates and
    # never the win rates). Retired 2026-08-25 -- see the migration.
    filing_stats = {}
    for row in filing_win_rates:
        tt = row["trade_type"]
        if tt not in ("buy", "sell"):
            continue
        for window in ("7d", "30d", "90d"):
            w = window[:-1]  # "7d" -> "7"
            n = row[f"n{w}"] or 0
            # The count is published at every n, including below the floor and
            # at zero. The denominator being invisible is what let this block
            # show an accuracy over 154 lots under a header reading 19.
            filing_stats[f"{tt}_scored_filings_{window}"] = n
            rate, ret, abn = apply_scoring_floor(
                n, row[f"wins{w}"], row[f"avg_ret{w}"], row[f"avg_abn{w}"]
            )
            filing_stats[f"{tt}_win_rate_{window}"] = rate
            filing_stats[f"{tt}_avg_return_{window}"] = ret
            filing_stats[f"{tt}_avg_abnormal_{window}"] = abn
    result["filing_stats"] = filing_stats

    if filing_counts:
        result["filing_counts"] = {
            "buy": filing_counts["buy_filings"] or 0,
            "sell": filing_counts["sell_filings"] or 0,
        }
    if sell_pattern and sell_pattern["total_sells"] > 0:
        result["sell_pattern"] = {
            "total_sells": sell_pattern["total_sells"],
            "planned_sells": sell_pattern["planned_sells"],
            "routine_sells": sell_pattern["routine_sells"],
        }
    result.update(best_pit)
    result["ticker_grades"] = ticker_grades

    # ── holdings and trailing-twelve-months ─────────────────────────────
    #
    # OWN CONNECTION. The `with get_db()` block above has already exited by
    # this point, and reusing a returned-to-pool connection raises
    # InterfaceError: connection already closed. This is the third time that
    # mistake has been made in this codebase — _blended_and_benchmark carries
    # the same comment for the same reason.
    with get_db() as conn2:
        # `shares_owned_after` is a RUNNING BALANCE, not a delta — the latest
        # filing per (insider, ticker) is the position, and summing the column
        # double-counts. A filing split across five tranches carries the same
        # balance on every lot, so DISTINCT ON is doing real work.
        result["holdings"] = [
            {
                "ticker": h["ticker"],
                "shares": int(h["shares"]) if h["shares"] is not None else None,
                "last_close": float(h["close"]) if h["close"] is not None else None,
                "value": (round(float(h["shares"]) * float(h["close"]), 2)
                          if h["shares"] is not None and h["close"] is not None
                          else None),
                "as_of": h["filing_date"],
            }
            for h in conn2.execute("""
                WITH latest AS (
                  SELECT DISTINCT ON (t.ticker)
                         t.ticker, t.shares_owned_after AS shares, t.filing_date
                    FROM trades t
                   WHERE t.insider_id = ?
                     AND t.shares_owned_after IS NOT NULL
                     AND t.shares_owned_after > 0
                   ORDER BY t.ticker, t.filing_date DESC, t.trade_id DESC
                )
                SELECT l.ticker, l.shares, l.filing_date::text AS filing_date,
                       p.close
                  FROM latest l
                  LEFT JOIN LATERAL (
                        SELECT close FROM prices.daily_prices
                         WHERE ticker = l.ticker ORDER BY date DESC LIMIT 1
                  ) p ON true
                 ORDER BY (l.shares * COALESCE(p.close, 0)) DESC
            """, (insider_id,)).fetchall()
        ]

        # Counts FILINGS, not lots, and only discretionary classes: 184k
        # compensation grants and 221k option exercises carry trade_type='buy',
        # so an unfiltered count reports shares an insider was handed as shares
        # they bought.
        ttm = {}
        for r in conn2.execute("""
            SELECT trade_type,
                   count(DISTINCT COALESCE(filing_key, accession,
                                           CAST(trade_date AS TEXT))) AS filings,
                   COALESCE(sum(qty), 0) AS shares,   -- the column is qty
                   COALESCE(sum(value), 0)  AS value
              FROM trades
             WHERE insider_id = ?
               AND signal_class IN ('discretionary_buy', 'discretionary_sell')
               AND filing_date::date >= (CURRENT_DATE - INTERVAL '12 months')
             GROUP BY trade_type
        """, (insider_id,)).fetchall():
            ttm[r["trade_type"]] = {
                "filings": r["filings"],
                "shares": int(r["shares"] or 0),
                "value": float(r["value"] or 0),
            }
    result["ttm"] = {
        "buys": ttm.get("buy", {"filings": 0, "shares": 0, "value": 0.0}),
        "sells": ttm.get("sell", {"filings": 0, "shares": 0, "value": 0.0}),
    }

    # Non-Pro sees who this is and what they did; not how well it worked.
    if not user.is_pro:
        # Volume survives, outcomes do not. buy_count / sell_count / n_tickers
        # describe WHAT they did — the same figures every competitor publishes
        # freely and that Google lifts as the result snippet ("112 transactions
        # across 1 company"). Nulling the whole object took those with it and
        # left the summary sentence as a bare name, which is the one thing on
        # this page that has to earn the click.
        tr_full = result.get("track_record") or {}
        result["track_record"] = (
            {k: tr_full.get(k) for k in PUBLIC_VOLUME_FIELDS if k in tr_full}
            or None
        )
        # ONE WINDOW OF ANALYSIS SURVIVES, on the buy side. A visitor arriving
        # from search saw a name, a filing count and some dates -- what a free
        # SEC scraper gives them -- and nothing said we had done any work.
        stats_full = result.get("filing_stats") or {}
        result["filing_stats"] = {
            k: stats_full.get(k) for k in PUBLIC_FILING_STAT_FIELDS
            if k in stats_full
        }
        result["sell_pattern"] = None
        result["ticker_grades"] = []
        # THE RATING STAYS. It was popped here alongside the raw scores, but a
        # grade is not a score -- it is the conclusion, and it is the one glyph
        # that tells a stranger we have a view. The page renders
        # <InsiderGradeBadge grade={best_career_grade}>, so removing it left
        # the badge empty on precisely the pages we are trying to convert.
        #
        # The numeric scores behind it stay Pro: a grade is a claim, a
        # percentile invites arithmetic we do not want done on a free tier.
        for f in ("score", "score_tier", "percentile", "best_career_score",
                  "best_pit_score", "best_pit_grade"):
            result.pop(f, None)
        result["gated"] = True
    return result


@router.get("/{identifier}/score-history")
def get_insider_score_history(
    identifier: str,
    user: UserContext = Depends(require_pro),
) -> dict:
    """PIT score progression over time for an insider across all tickers."""
    with get_db() as conn:
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        rows = conn.execute("""
            SELECT sh.as_of_date, sh.ticker, sh.blended_score, sh.global_score,
                   sh.ticker_score, sh.trade_count
            FROM score_history sh
            WHERE sh.insider_id = ?
            ORDER BY sh.as_of_date
        """, (decoded_id,)).fetchall()

        # Build per-ticker series and a global (all-ticker) series
        by_ticker: dict[str, list] = {}
        global_series: list[dict] = []
        for r in rows:
            point = {
                "date": r["as_of_date"],
                "blended_score": round(r["blended_score"], 3) if r["blended_score"] is not None else None,
                "global_score": round(r["global_score"], 3) if r["global_score"] is not None else None,
                "ticker_score": round(r["ticker_score"], 3) if r["ticker_score"] is not None else None,
                "trade_count": r["trade_count"],
            }
            ticker = r["ticker"]
            by_ticker.setdefault(ticker, []).append(point)
            global_series.append({"date": r["as_of_date"], "score": point["blended_score"], "ticker": ticker})

        # Grade thresholds for chart reference lines
        grade_thresholds = [
            {"grade": "A", "score": 2.0},
            {"grade": "B", "score": 1.0},
            {"grade": "C", "score": 0.5},
        ]

        return {
            "by_ticker": by_ticker,
            "global_series": global_series,
            "grade_thresholds": grade_thresholds,
            "total_snapshots": len(rows),
        }


@router.get("/{identifier}/trades")
def get_insider_trades(
    identifier: str,
    trans_codes: str = Query(default="P,S"),
    signal_class: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Paginated trade history for an insider. Accepts encoded sqids ID or CIK."""
    with get_db() as conn:
        insider = None
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        tc_conditions: list = [
            "insider_id = ?",
            "superseded_by IS NULL",
            # Match the security_title filter used by the row query below so
            # the total count and the rendered list don't diverge.
            "is_derivative = 0",
            # The feed filters duplicates and this did not, so a row stored
            # twice — once with trans_code and trade_type disagreeing —
            # rendered as a phantom BUY and SELL of the same value on the
            # same day. See migrations/2026-08-18_trade_type_consistency.sql.
            "(is_duplicate = 0 OR is_duplicate IS NULL)",
        ]
        tc_params: list = [insider_id]
        add_trans_code_filter(tc_conditions, tc_params, trans_codes, alias="trades")
        tc_where = " AND ".join(tc_conditions)

        total = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT 1 FROM trades
                WHERE {tc_where}
                GROUP BY ticker, trade_type, {filing_group_by("trades")}
            )
            """,
            tc_params,
        ).fetchone()["cnt"]

        inner_conditions: list = [
            "t.insider_id = ?",
            "t.superseded_by IS NULL",
            # Exclude derivative titles — see comment in volume_by_type query.
            "t.is_derivative = 0",
            "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)",
        ]
        inner_params: list = [insider_id]
        add_trans_code_filter(inner_conditions, inner_params, trans_codes)
        add_signal_class_filter(inner_conditions, inner_params, signal_class)
        inner_where = " AND ".join(inner_conditions)

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.ticker, agg.company, agg.title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.trans_code,
                agg.is_10b5_1, agg.is_routine, agg.signal_class,
                agg.pit_grade, agg.pit_blended_score,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.ticker, MAX(t.company) AS company, MAX(t.title) AS title,
                    t.trade_type,
                    MIN(t.trade_date) AS trade_date,
                    MAX(t.trade_date) AS last_trade_date,
                    MIN(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    COUNT(*) AS lot_count,
                    MAX(t.is_csuite) AS is_csuite,
                    GROUP_CONCAT(DISTINCT t.trans_code) AS trans_code,
                    MAX(t.is_10b5_1) AS is_10b5_1,
                    MAX(t.is_routine) AS is_routine,
                    -- signal_class was NOT selected here, so every row reached
                    -- the client as null. insider-trades-table derives
                    -- `isRoutineSell = trade_type === "sell" &&
                    -- !isDiscretionary(signal_class)`, and isDiscretionary(null)
                    -- is false — so EVERY sell on EVERY insider page was
                    -- labelled "Routine", including plain discretionary sales.
                    -- The filing and explore pages had the field and disagreed,
                    -- which is how it was noticed.
                    --
                    -- AGGREGATION RULE: if ANY lot is discretionary, the
                    -- filing is discretionary.
                    --
                    -- Not MAX(). 723 filing groups in the last 90 days hold
                    -- more than one class even after grouping by trade_type —
                    -- `discretionary_sell + gift + tax_withholding` is a
                    -- common shape. MAX() sorts alphabetically and would
                    -- return `tax_withholding`, hiding a genuine open-market
                    -- sale behind the vesting paperwork filed beside it.
                    --
                    -- A filing containing a real decision IS a real decision,
                    -- whatever mechanical rows accompany it.
                    COALESCE(
                        MAX(t.signal_class) FILTER (
                            WHERE t.signal_class IN ('discretionary_buy',
                                                     'discretionary_sell')),
                        MAX(t.signal_class)
                    ) AS signal_class,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE {inner_where}
                GROUP BY t.ticker, t.trade_type, {filing_group_by()}
            ) agg
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.trade_date DESC
            LIMIT ? OFFSET ?
            """,
            inner_params + [limit, offset],
        ).fetchall()

    items = [dict(r) for r in rows]

    with get_db() as enrich_conn:
        enrich_items_with_signals(enrich_conn, items)
        enrich_items_with_context(enrich_conn, items)
    enrich_items_with_price_end(items)

    encode_response_ids(items, insider=False)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


@router.get("/{identifier}/companies")
def get_insider_companies(identifier: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Company history for an insider. Accepts encoded sqids ID or CIK."""
    with get_db() as conn:
        insider = None
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        rows = conn.execute(
            """
            SELECT ic.ticker, ic.company, ic.title, ic.trade_count, ic.total_value,
                   ic.first_trade, ic.last_trade,
                   (SELECT t.normalized_title FROM trades t
                    WHERE t.insider_id = ic.insider_id AND t.ticker = ic.ticker
                      AND t.normalized_title IS NOT NULL AND t.normalized_title != ''
                      AND t.normalized_title NOT IN ('Other', 'See Remarks', 'Unknown')
                    GROUP BY t.normalized_title
                    ORDER BY COUNT(*) DESC
                    LIMIT 1) AS normalized_title
            FROM insider_companies ic
            WHERE ic.insider_id = ?
            ORDER BY ic.total_value DESC
            """,
            (insider_id,),
        ).fetchall()

    return {"companies": [dict(r) for r in rows]}


RELATED_SQL = """
SELECT s.related_insider_id AS insider_id,
       s.rank, s.score, s.co_investment, s.sector_overlap, s.profile_sim,
       s.shared_tickers, s.shared_ticker_list,
       COALESCE(i.display_name, i.name) AS name,
       i.slug,
       COALESCE(i.is_entity, 0)         AS is_entity,
       -- NO GRADE ON THESE CARDS, deliberately. A rating rendered beside the
       -- word "related" reads as a ranking, which is the one thing this list
       -- is not: the clustering underneath does not predict returns. The card
       -- says who they are and why they are here, and the grade is one click
       -- away on their own page where it has its context.
       (SELECT count(DISTINCT COALESCE(t.filing_key, t.accession))
          FROM trades t
         WHERE t.insider_id = s.related_insider_id
           AND t.signal_class IN ('discretionary_buy','discretionary_sell')
       ) AS filing_count
  FROM insider_similarity s
  JOIN insiders i ON i.insider_id = s.related_insider_id
 WHERE s.insider_id = ?
 ORDER BY s.rank
 LIMIT ?
"""


@router.get("/{identifier}/related")
def get_related_insiders(
    identifier: str,
    limit: int = Query(8, ge=1, le=12),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Insiders similar to this one.

    DELIBERATELY UNGATED. It is a navigation aid on the surfaces that carry
    the most organic traffic, and the entire point of it is to give a visitor
    who arrived from search somewhere to go next. Gating a set of internal
    links would defeat both halves of that.

    THIS IS SIMILARITY AND NOT A RANKING. The behavioural clustering
    underneath was tested against forward returns and failed (permutation
    p=0.208), so nothing here may be rendered as "better" or "top" insiders.
    `reason` exists so the UI states the actual relation instead of implying
    one; see scripts/insider_similarity.py.
    """
    with get_db() as conn:
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is None:
            raise HTTPException(status_code=404, detail="Insider not found")
        rows = conn.execute(RELATED_SQL, (decoded_id, limit)).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        shared = d.get("shared_tickers") or 0
        tickers = [t for t in (d.get("shared_ticker_list") or "").split(",") if t]
        if shared:
            d["reason"] = "co_investment"
            d["reason_tickers"] = tickers
        else:
            d["reason"] = "similar_profile"
            d["reason_tickers"] = []
        # The score decomposition is for our own auditing, not for a card.
        for k in ("score", "co_investment", "sector_overlap", "profile_sim",
                  "shared_ticker_list"):
            d.pop(k, None)
        d["insider_id"] = encode_insider_id(d["insider_id"])
        out.append(d)

    return {"related": out}


# Bin edges in percent: ..., -20, -15, -10, -5, 0, 5, 10, 15, 20, ...
_BIN_EDGES = [-20, -15, -10, -5, 0, 5, 10, 15, 20]

_WINDOW_COL = {
    "7d": "return_7d",
    "30d": "return_30d",
    "90d": "return_90d",
}


def _build_bins(returns: list[float]) -> list[dict]:
    """Bucket a list of percent returns into histogram bins."""
    # Build bin boundaries: (-inf, -20], (-20, -15], ... (20, +inf)
    edges = _BIN_EDGES
    n_bins = len(edges) + 1
    counts = [0] * n_bins
    sums = [0.0] * n_bins

    for r in returns:
        placed = False
        for i, edge in enumerate(edges):
            if r <= edge:
                counts[i] += 1
                sums[i] += r
                placed = True
                break
        if not placed:
            counts[-1] += 1
            sums[-1] += r

    # Build labels
    labels: list[str] = []
    labels.append(f"<{edges[0]}%")
    for i in range(len(edges) - 1):
        labels.append(f"{edges[i]}% to {edges[i + 1]}%")
    labels.append(f">{edges[-1]}%")

    bins = []
    for i in range(n_bins):
        avg = round(sums[i] / counts[i], 2) if counts[i] > 0 else 0.0
        bins.append({"label": labels[i], "count": counts[i], "avg_return": avg})
    return bins


@router.get("/{identifier}/return-distribution")
def get_return_distribution(
    identifier: str,
    window: str = Query(default="7d", pattern="^(7d|30d|90d)$"),
    user: UserContext = Depends(require_pro),
) -> dict:
    """Binned return distribution for an insider's trades."""
    col = _WINDOW_COL[window]

    with get_db() as conn:
        insider = None
        decoded_id = resolve_insider_id(conn, identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        rows = conn.execute(
            f"""
            SELECT MAX(tr.{col}) AS ret, t.trade_type
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ?
              AND t.superseded_by IS NULL
              AND tr.{col} IS NOT NULL
            GROUP BY t.ticker, t.trade_type, {filing_group_by()}
            """,
            (insider_id,),
        ).fetchall()

        # Determine dominant trade type for this insider
        type_counts = conn.execute(
            """SELECT trade_type, COUNT(*) AS n FROM trades
               WHERE insider_id = ? AND trans_code IN ('P','S')
                 AND superseded_by IS NULL
               GROUP BY trade_type ORDER BY n DESC""",
            (insider_id,),
        ).fetchall()

        # Per-trade timeline data
        trade_rows = conn.execute(
            f"""
            SELECT MIN(t.trade_date) AS trade_date, t.ticker, t.trade_type,
                   SUM(t.value) AS value, MAX(tr.{col}) AS ret
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ?
              AND t.trans_code IN ('P','S')
              AND t.superseded_by IS NULL
              AND t.is_derivative = 0
              AND tr.{col} IS NOT NULL
            GROUP BY t.ticker, t.trade_type, {filing_group_by()}
            ORDER BY MIN(t.trade_date)
            """,
            (insider_id,),
        ).fetchall()

        # Global average for comparison
        global_avg = conn.execute(
            f"""
            SELECT
                AVG(CASE WHEN t.trade_type='buy' THEN tr.{col} END) AS avg_buy,
                AVG(CASE WHEN t.trade_type='sell' THEN tr.{col} END) AS avg_sell
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trans_code IN ('P','S') AND tr.{col} IS NOT NULL
            """,
        ).fetchone()

    dominant_type = type_counts[0]["trade_type"] if type_counts else "buy"

    returns_raw = [row["ret"] for row in rows]
    total_trades = len(returns_raw)

    # For sells, a "win" is when the stock declines (return < 0)
    if dominant_type == "sell":
        win_count = sum(1 for r in returns_raw if r < 0)
        loss_count = sum(1 for r in returns_raw if r >= 0)
    else:
        win_count = sum(1 for r in returns_raw if r > 0)
        loss_count = sum(1 for r in returns_raw if r <= 0)

    # Convert decimals (0.05) to percentage points (5.0) for binning
    returns_pct = [r * 100 for r in returns_raw]
    bins = _build_bins(returns_pct) if returns_pct else []

    # Per-trade timeline
    timeline = [
        {
            "date": r["trade_date"],
            "ticker": r["ticker"],
            "trade_type": r["trade_type"],
            "value": r["value"],
            "return_pct": round(r["ret"] * 100, 2),
        }
        for r in trade_rows
    ]

    avg_return = round(sum(returns_raw) / len(returns_raw) * 100, 2) if returns_raw else 0
    global_avg_pct = round(
        (global_avg["avg_sell" if dominant_type == "sell" else "avg_buy"] or 0) * 100, 2
    )

    return {
        "bins": bins,
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": loss_count,
        "dominant_type": dominant_type,
        "timeline": timeline,
        "avg_return_pct": avg_return,
        "global_avg_pct": global_avg_pct,
    }
