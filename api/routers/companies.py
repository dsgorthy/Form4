from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.routers.sectors import slugify as sector_slugify
from api.filters import (MEANINGFUL_CLASSES, add_signal_class_filter,
                         add_trans_code_filter, deduplicate_filers, filing_group_by)
from api.gating import get_free_cutoff_date, null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids
from api.pit_helpers import get_ticker_pit_grade

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/companies", tags=["companies"])


#: MEANINGFUL_CLASSES rendered as a SQL IN-list. Never type the class names —
#: they are derived from KIND_META and test_meaningful_is_one_definition fails
#: the build on drift. Queries below carry a {MEANINGFUL_IN} placeholder and
#: call _meaningful() on the way to execute().
_MEANINGFUL_IN = ", ".join(f"'{c}'" for c in MEANINGFUL_CLASSES)


def _meaningful(sql: str) -> str:
    return sql.replace("{MEANINGFUL_IN}", _MEANINGFUL_IN)


@router.get("/{ticker}")
def get_company(ticker: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Company overview with insider roster."""
    ticker = ticker.upper()
    if ticker == "NONE":
        raise HTTPException(status_code=404, detail="Company not found")

    from datetime import date, timedelta
    six_months_ago = (date.today() - timedelta(days=182)).isoformat()

    with get_db() as conn:
        # Get company name from most recent trade
        company_row = conn.execute(
            _meaningful("""
            -- ONE POPULATION, decided 2026-09-03: discretionary trades,
            -- counted per FILING. Previously this counted rows with
            -- trans_code IN ('P','S'), which is wider on both axes — it
            -- admits 10b5-1 planned trades (a scheduled sale is not a
            -- decision) and counts execution tranches rather than decisions.
            --
            -- The cost is real and was accepted deliberately: AAPL's headline
            -- goes from "2,381 transactions by 38 insiders" to "261
            -- open-market trades by 31 insiders". The old number is bigger in
            -- a search snippet; this one is the number the rest of the product
            -- means, and a page whose four counts disagree is worse than a
            -- page with a smaller true one.
            SELECT MAX(company) AS company, ticker,
                   COUNT(DISTINCT COALESCE(filing_key, accession)) AS total_trades,
                   SUM(value) AS total_value,
                   MIN(trade_date) AS first_trade,
                   MAX(trade_date) AS last_trade,
                   -- Aggregates for the summary sentence under the H1. Every
                   -- competitor that outranks us leads with figures like these
                   -- and Google lifts them verbatim as the result snippet
                   -- ("net sold $249.2M over the trailing 6 months").
                   -- Conditional SUMs over a scan we already perform, so no
                   -- extra query cost.
                   COUNT(DISTINCT insider_id) AS distinct_insiders,
                   SUM(CASE WHEN trade_type = 'buy'  THEN value ELSE 0 END) AS buy_value,
                   SUM(CASE WHEN trade_type = 'sell' THEN value ELSE 0 END) AS sell_value,
                   SUM(CASE WHEN trade_type = 'buy'
                             AND trade_date >= ? THEN value ELSE 0 END) AS buy_value_6mo,
                   SUM(CASE WHEN trade_type = 'sell'
                             AND trade_date >= ? THEN value ELSE 0 END) AS sell_value_6mo
            FROM trades
            WHERE ticker = ?
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
              AND superseded_by IS NULL
              AND signal_class IN ({MEANINGFUL_IN})
              AND is_derivative = 0
            GROUP BY ticker
            """),
            (six_months_ago, six_months_ago, ticker),
        ).fetchone()

        # The company's own sector, so the page can link to its sector hub.
        # That link is worth more than the nav entry: a Healthcare company
        # pointing at "Healthcare Insider Buying" is a topical signal from the
        # second-most-crawled surface on the site.
        _sector_row = conn.execute(
            "SELECT sector FROM ticker_metadata WHERE ticker = ?", (ticker,)
        ).fetchone()
        sector = (_sector_row and _sector_row["sector"]) or None

        if company_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        # Insider roster for this company
        roster = conn.execute(
            _meaningful("""
            SELECT
                ic.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                COALESCE(i.is_entity, 0) as is_entity,
                ic.title,
                (SELECT t.normalized_title FROM trades t
                 WHERE t.insider_id = ic.insider_id AND t.ticker = ic.ticker
                   AND t.normalized_title IS NOT NULL AND t.normalized_title != ''
                 ORDER BY t.trade_date DESC LIMIT 1) as normalized_title,
                ic.trade_count, ic.total_value,
                ic.first_trade, ic.last_trade
            FROM insider_companies ic
            JOIN insiders i ON ic.insider_id = i.insider_id
            WHERE ic.ticker = ?
              -- Discretionary filers only. Unfiltered this listed every
              -- officer who has ever received a grant, which is not "an
              -- insider who trades this stock" — and it read 48 under a
              -- sentence saying 38.
              AND EXISTS (
                    SELECT 1 FROM trades t
                     WHERE t.insider_id = ic.insider_id
                       AND t.ticker = ic.ticker
                       AND t.signal_class IN ({MEANINGFUL_IN})
              )
            ORDER BY ic.total_value DESC
            """),
            (ticker,),
        ).fetchall()

        roster_list = [dict(r) for r in roster]

        # Group entities under primary insiders
        try:
            for ins in roster_list:
                if ins.get("is_entity"):
                    group = conn.execute("""
                        SELECT ig.primary_insider_id, COALESCE(pi.display_name, pi.name) as primary_name
                        FROM insider_group_members igm
                        JOIN insider_groups ig ON igm.group_id = ig.group_id
                        JOIN insiders pi ON ig.primary_insider_id = pi.insider_id
                        WHERE igm.insider_id = ? AND igm.is_primary = 0
                    """, (ins["insider_id"],)).fetchone()
                    if group:
                        ins["controlled_by"] = {
                            "insider_id": group["primary_insider_id"],
                            "name": group["primary_name"],
                        }
        except Exception:
            pass

        # Enrich each insider with their PIT grade for THIS ticker
        for ins in roster_list:
            ins["pit_grade"] = get_ticker_pit_grade(conn, ins["insider_id"], ticker)

    result = dict(company_row)
    if not user.is_pro:
        roster_list = null_items_track_records(roster_list)
    encode_response_ids(roster_list, trade=False, insider=True)
    result["insiders"] = roster_list
    result["sector"] = sector
    # The SLUG comes from the same slugify the hub routes use. Deriving it in
    # the frontend is the drift test_sector_hubs.test_the_slug_is_defined_once
    # exists to prevent, and I wrote that test and then did it anyway.
    result["sector_slug"] = sector_slugify(sector) if sector else None
    return result


@router.get("/{ticker}/trades")
def get_company_trades(
    ticker: str,
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    trans_codes: str = Query(default="P,S"),
    signal_class: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Paginated trades for a company. Free users see all trades but gated ones are marked."""
    ticker = ticker.upper()
    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None

    conditions = [
        "t.ticker = ?",
        "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)",
        "t.superseded_by IS NULL",
        "t.is_derivative = 0",
    ]
    params: list = [ticker]

    add_trans_code_filter(conditions, params, trans_codes)
    add_signal_class_filter(conditions, params, signal_class)

    if trade_type is not None:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)

    where_clause = " AND ".join(conditions)

    with get_db() as conn:
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT 1 FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, {filing_group_by()}
            )
            """,
            params,
        ).fetchone()["cnt"]

        if total == 0 and trade_type is None:
            exists = conn.execute(
                "SELECT 1 FROM trades WHERE ticker = ? LIMIT 1", (ticker,)
            ).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Company not found")

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title,
                agg.normalized_title, agg.trans_code,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite,
                agg.is_10b5_1, agg.is_routine, agg.cohen_routine, agg.shares_owned_after, agg.is_rare_reversal, agg.week52_proximity,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                tr.return_7d, tr.return_30d, tr.return_90d, tr.return_180d, tr.return_365d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d, tr.abnormal_180d, tr.abnormal_365d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, MAX(t.ticker) AS ticker, MAX(t.company) AS company, MAX(t.title) AS title,
                    MAX(t.normalized_title) AS normalized_title,
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
                    MAX(t.cohen_routine) AS cohen_routine,
                    MAX(t.shares_owned_after) AS shares_owned_after,
                    MAX(t.is_rare_reversal) AS is_rare_reversal,
                    MAX(t.week52_proximity) AS week52_proximity,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, {filing_group_by()}
                ORDER BY MIN(t.trade_date) DESC
                LIMIT ? OFFSET ?
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.trade_date DESC
            """,
            params + [limit, offset],
        ).fetchall()

    raw_list = [dict(r) for r in rows]
    items = deduplicate_filers(
        raw_list,
        value_key="value",
        date_key="last_trade_date",
        identity_keys=("insider_id", "insider_name", "cik", "pit_blended_score", "pit_grade", "title"),
    )

    # Enrich with trade grade
    from api.trade_grade import enrich_items_with_trade_grade
    enrich_items_with_trade_grade(None, items)

    if free_cutoff:
        items = null_items_track_records(items)
        for item in items:
            item["gated"] = item["trade_date"] < free_cutoff
        # Deliberately NOT redact_gated_items() here.
        #
        # The company page is an indexed acquisition surface, and this table is
        # most of its substance. Redacting identity turned it into rows of
        # "Insider ••••" with no value — on AAPL, 14 of the 15 visible rows,
        # because everything outside the 90-day window is blanked. That is a
        # worse page than SEC.gov, which publishes the same filings in full and
        # for free, and it is what every competitor ranking above us shows.
        #
        # What the disclosure SAYS — who filed, what, when, how much — is public
        # record and stays public. What we COMPUTE about it — returns, alpha,
        # scores, grades — is the product and is still nulled by
        # null_items_track_records above. The `gated` flag is still set, so the
        # UI can mark these rows and upsell on the analysis.
    encode_response_ids(items)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
        **({"free_cutoff": free_cutoff} if free_cutoff else {}),
    }


@router.get("/{ticker}/price-history")
def get_company_price_history(ticker: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Insider trade markers over time for the price chart scatter plot."""
    ticker = ticker.upper()

    conditions = ["t.ticker = ?", "t.superseded_by IS NULL", "t.is_derivative = 0"]
    params_list = [ticker]

    add_trans_code_filter(conditions, params_list, "P,S")

    if not user.has_full_feed:
        cutoff = get_free_cutoff_date()
        conditions.append("t.trade_date >= ?")
        params_list.append(cutoff)

    where_clause = " AND ".join(conditions)

    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT
                agg.trade_date AS date,
                agg.price,
                agg.trade_type,
                agg.value,
                COALESCE(i.display_name, i.name) AS insider_name,
                agg.pit_grade
            FROM (
                SELECT
                    t.insider_id,
                    t.trade_type,
                    t.trade_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.value) AS value,
                    MAX(t.pit_grade) AS pit_grade
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            ) agg
            JOIN insiders i ON agg.insider_id = i.insider_id
            ORDER BY agg.trade_date ASC
            """,
            params_list,
        ).fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="No trades found for ticker")

    return {"trades": [dict(r) for r in rows]}


@router.get("/{ticker}/chart-data")
def get_chart_data(
    ticker: str,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    csuite: Optional[bool] = Query(default=None),
    director: Optional[bool] = Query(default=None),
    owner_10pct: Optional[bool] = Query(default=None),
    top_performer: Optional[bool] = Query(default=None),
    trans_codes: str = Query(default="P,S"),
    signal_class: Optional[str] = Query(default=None),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Daily OHLC candles + insider trade markers for the chart component."""
    ticker = ticker.upper()

    # Read OHLC from daily_prices table (pre-loaded, no external API call)
    candles = []
    with get_db() as conn:
        price_start = start or "2016-01-01"
        price_end = end or date.today().isoformat()
        rows = conn.execute(
            """SELECT date, open, high, low, close, volume
               FROM daily_prices
               WHERE ticker = ? AND date >= ? AND date <= ?
               ORDER BY date""",
            (ticker, price_start, price_end),
        ).fetchall()
        candles = [
            {"time": r["date"], "open": r["open"], "high": r["high"],
             "low": r["low"], "close": r["close"], "volume": r["volume"]}
            for r in rows
        ]

    # Compute date bounds matching the candle range
    if candles:
        trade_start = candles[0]["time"]
        trade_end = candles[-1]["time"]
    else:
        trade_start = "2016-01-01"
        trade_end = date.today().isoformat()

    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None

    # Build dynamic WHERE clause for trade filters
    conditions = [
        "t.ticker = ?",
        "t.trade_date >= ?",
        "t.trade_date <= ?",
        "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)",
        "t.superseded_by IS NULL",
        "t.is_derivative = 0",
    ]
    params: list = [ticker, trade_start, trade_end]

    add_trans_code_filter(conditions, params, trans_codes)
    add_signal_class_filter(conditions, params, signal_class)

    if trade_type is not None:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)
    if csuite is True:
        conditions.append("t.is_csuite = 1")
        conditions.append("t.insider_id NOT IN (SELECT insider_id FROM insiders WHERE is_entity = 1)")
    if director is True:
        conditions.append("t.normalized_title LIKE '%Director%'")
        conditions.append("t.normalized_title NOT LIKE '%10% Owner%'")
        conditions.append("t.insider_id NOT IN (SELECT insider_id FROM insiders WHERE is_entity = 1)")
    if owner_10pct is True:
        conditions.append("t.normalized_title LIKE '%10% Owner%'")

    where_clause = " AND ".join(conditions)

    # Fetch insider trade markers within the candle date range
    with get_db() as conn:
        query = f"""
            SELECT
                agg.trade_date AS date,
                agg.price,
                agg.trade_type,
                agg.value,
                COALESCE(i.display_name, i.name) AS insider_name,
                agg.pit_grade
            FROM (
                SELECT
                    t.insider_id,
                    t.trade_type,
                    t.trade_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.value) AS value,
                    MAX(t.pit_grade) AS pit_grade
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            ) agg
            JOIN insiders i ON agg.insider_id = i.insider_id
            {"WHERE agg.pit_grade IN ('A+','A','B')" if top_performer is True else ""}
            ORDER BY agg.trade_date ASC
        """
        rows = conn.execute(query, params).fetchall()

    trades = [dict(r) for r in rows]

    # Free users: all trades visible on chart, but mark gated ones
    if free_cutoff:
        for t in trades:
            t["gated"] = t["date"] < free_cutoff
        trades = redact_gated_items(trades)

    # Detect trading gaps > 30 calendar days (halts, delistings, etc.)
    gaps = []
    if len(candles) >= 2:
        from datetime import datetime as _dt
        for i in range(1, len(candles)):
            d_prev = _dt.strptime(candles[i - 1]["time"], "%Y-%m-%d")
            d_curr = _dt.strptime(candles[i]["time"], "%Y-%m-%d")
            gap_days = (d_curr - d_prev).days
            if gap_days > 30:
                gaps.append({
                    "start": candles[i - 1]["time"],
                    "end": candles[i]["time"],
                    "days": gap_days,
                    "price_before": candles[i - 1]["close"],
                    "price_after": candles[i]["close"],
                })

    result = {
        "candles": candles,
        "trades": trades,
        "gaps": gaps,
    }
    if free_cutoff:
        result["gated"] = True
        result["free_cutoff"] = free_cutoff
    return result


RELATED_COMPANIES_SQL = """
SELECT c.related_ticker AS ticker, c.rank, c.reason,
       c.shared_insiders, c.same_sector, c.recent_buys,
       m.sector,
       -- ticker_metadata carries no company name; insider_companies does, and
       -- a DISTINCT ON over six tickers is 3ms.
       (SELECT ic.company FROM insider_companies ic
         WHERE ic.ticker = c.related_ticker AND ic.company IS NOT NULL
         LIMIT 1) AS company
  FROM company_similarity c
  LEFT JOIN ticker_metadata m ON m.ticker = c.related_ticker
 WHERE c.ticker = ?
 ORDER BY c.rank
 LIMIT ?
"""


@router.get("/{ticker}/related")
def get_related_companies(ticker: str, limit: int = Query(6, ge=1, le=12)) -> dict:
    """Companies related to this one.

    UNGATED, like the insider equivalent. Company pages are the second-most
    crawled surface on the site -- 1,482 Googlebot requests in 7 days against
    508 for insider pages -- and until now they carried twenty outbound links
    to insiders and NOT ONE to another company. No topical navigation for a
    reader, no sector signal for a crawler.

    Two relations, and the card says which:
      shared_insiders  people file on both companies. Countable.
      sector_peer      same sector, ranked by recent insider buying.

    NOT a correlation, a comparison, or a trading signal. Two companies sharing
    a director tells you about a person's calendar, not about their stocks.
    """
    with get_db() as conn:
        rows = conn.execute(RELATED_COMPANIES_SQL, (ticker.upper(), limit)).fetchall()
    return {"related": [dict(r) for r in rows]}


@router.get("/{ticker}/insiders")
def company_insiders(ticker: str, limit: int = Query(default=50, le=200)):
    """Everyone who has filed on this ticker, most recent first.

    Cross-links the company and insider pages, and carries the career grade on
    each name — the thing a competitor roster cannot show.

    MEANINGFUL filings only. An unfiltered roster is dominated by people who
    have only ever received a compensation grant, which is not "an insider who
    trades this stock".
    """
    with get_db() as conn:
        rows = conn.execute(_meaningful("""
            SELECT DISTINCT ON (t.insider_id)
                   t.insider_id,
                   COALESCE(i.display_name, i.name) AS name,
                   i.slug,
                   t.title,
                   t.career_grade,
                   t.filing_date::text AS last_filed,
                   t.trade_type AS last_action
              FROM trades t
              JOIN insiders i ON i.insider_id = t.insider_id
             WHERE t.ticker = ?
               AND t.signal_class IN ({MEANINGFUL_IN})
             ORDER BY t.insider_id, t.filing_date DESC, t.trade_id DESC
        """), (ticker.upper(),)).fetchall()

    items = sorted(
        (dict(r) for r in rows),
        key=lambda r: r["last_filed"] or "",
        reverse=True,
    )[:limit]
    return {"ticker": ticker.upper(), "count": len(items), "items": items}
