from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import add_trans_code_filter, deduplicate_filers, filing_group_by
from api.gating import get_free_cutoff_date, null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/companies", tags=["companies"])


@router.get("/{ticker}")
def get_company(ticker: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Company overview with insider roster."""
    ticker = ticker.upper()
    if ticker == "NONE":
        raise HTTPException(status_code=404, detail="Company not found")

    with get_db() as conn:
        # Get company name from most recent trade
        company_row = conn.execute(
            """
            SELECT company, ticker,
                   COUNT(*) AS total_trades,
                   SUM(value) AS total_value,
                   MIN(trade_date) AS first_trade,
                   MAX(trade_date) AS last_trade
            FROM trades
            WHERE ticker = ?
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
              AND trans_code IN ('P', 'S')
            GROUP BY ticker
            """,
            (ticker,),
        ).fetchone()

        if company_row is None:
            raise HTTPException(status_code=404, detail="Company not found")

        # Insider roster for this company
        roster = conn.execute(
            """
            SELECT
                ic.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                COALESCE(i.is_entity, 0) as is_entity,
                ic.title,
                (SELECT t.normalized_title FROM trades t
                 WHERE t.insider_id = ic.insider_id AND t.ticker = ic.ticker
                   AND t.normalized_title IS NOT NULL AND t.normalized_title != ''
                 ORDER BY t.trade_date DESC LIMIT 1) as normalized_title,
                ic.trade_count, ic.total_value,
                ic.first_trade, ic.last_trade,
                itr.score, itr.score_tier, itr.percentile
            FROM insider_companies ic
            JOIN insiders i ON ic.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON ic.insider_id = itr.insider_id
            WHERE ic.ticker = ?
            ORDER BY itr.score DESC
            """,
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

    result = dict(company_row)
    if not user.is_pro:
        roster_list = null_items_track_records(roster_list)
    encode_response_ids(roster_list, trade=False, insider=True)
    result["insiders"] = roster_list
    return result


@router.get("/{ticker}/trades")
def get_company_trades(
    ticker: str,
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    trans_codes: str = Query(default="P,S"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Paginated trades for a company. Free users see all trades but gated ones are marked."""
    ticker = ticker.upper()
    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None

    conditions = ["t.ticker = ?", "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)"]
    params: list = [ticker]

    add_trans_code_filter(conditions, params, trans_codes)

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
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.sell_win_rate_7d,
                tr.return_7d, tr.return_30d, tr.return_90d, tr.return_180d, tr.return_365d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d, tr.abnormal_180d, tr.abnormal_365d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, t.ticker, t.company, t.title,
                    t.normalized_title,
                    t.trade_type,
                    MIN(t.trade_date) AS trade_date,
                    MAX(t.trade_date) AS last_trade_date,
                    MIN(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / SUM(t.qty), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    COUNT(*) AS lot_count,
                    t.is_csuite,
                    GROUP_CONCAT(DISTINCT t.trans_code) AS trans_code,
                    MAX(t.is_10b5_1) AS is_10b5_1,
                    MAX(t.is_routine) AS is_routine,
                    MAX(t.cohen_routine) AS cohen_routine,
                    MAX(t.shares_owned_after) AS shares_owned_after,
                    MAX(t.is_rare_reversal) AS is_rare_reversal,
                    MAX(t.week52_proximity) AS week52_proximity
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, {filing_group_by()}
                ORDER BY MIN(t.trade_date) DESC
                LIMIT ? OFFSET ?
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
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
        identity_keys=("insider_id", "insider_name", "cik", "score", "score_tier", "title"),
    )

    # Enrich with signal quality
    with get_db() as q_conn:
        from api.signal_quality import enrich_items_with_quality
        enrich_items_with_quality(q_conn, items)

    if free_cutoff:
        items = null_items_track_records(items)
        for item in items:
            item["gated"] = item["trade_date"] < free_cutoff
        items = redact_gated_items(items)
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

    conditions = ["t.ticker = ?"]
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
                itr.score_tier
            FROM (
                SELECT
                    t.insider_id,
                    t.trade_type,
                    t.trade_date,
                    ROUND(SUM(t.value) / SUM(t.qty), 2) AS price,
                    SUM(t.value) AS value
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            ) agg
            JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
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
    conditions = ["t.ticker = ?", "t.trade_date >= ?", "t.trade_date <= ?", "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)"]
    params: list = [ticker, trade_start, trade_end]

    add_trans_code_filter(conditions, params, trans_codes)

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
                itr.score_tier
            FROM (
                SELECT
                    t.insider_id,
                    t.trade_type,
                    t.trade_date,
                    ROUND(SUM(t.value) / SUM(t.qty), 2) AS price,
                    SUM(t.value) AS value
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            ) agg
            JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            {"WHERE itr.score_tier >= 2" if top_performer is True else ""}
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
