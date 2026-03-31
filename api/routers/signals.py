from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

import json

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import deduplicate_filers
from api.gating import get_free_cutoff_date, null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


@router.get("/sell-cessation")
def sell_cessation(
    user: UserContext = Depends(get_current_user),
    min_tier: Optional[int] = Query(default=None, ge=1, le=3),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    """Find insiders who regularly sold but have gone quiet — potential bullish signal.

    Criteria:
    - 2+ sell transactions in the 12 months before the 60-day quiet window
    - No sells in the last 60 days
    """
    with get_db() as conn:
        latest_row = conn.execute("SELECT MAX(filing_date) AS d FROM trades").fetchone()
        latest = latest_row["d"]
        if latest is None:
            return {"items": [], "total": 0}

        # Insiders who sold in the last 60 days — to exclude
        # Main query: insiders with 2+ sells between -14 months and -60 days,
        # but NOT in the exclusion set (sold in last 60 days)
        query = """
            SELECT
                t.insider_id,
                COALESCE(i.display_name, i.name) AS name,
                i.cik,
                COUNT(*) AS sell_count_12m,
                SUM(t.value) AS sell_value_12m,
                MAX(t.trade_date) AS last_sell_date,
                itr.score,
                itr.score_tier,
                GROUP_CONCAT(DISTINCT t.ticker) AS tickers
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            WHERE t.trade_type = 'sell'
              AND t.trans_code = 'S'
              AND t.trade_date BETWEEN date(?, '-14 months') AND date(?, '-60 days')
              AND t.insider_id NOT IN (
                  SELECT DISTINCT insider_id
                  FROM trades
                  WHERE trade_type = 'sell'
                    AND trans_code = 'S'
                    AND trade_date > date(?, '-60 days')
              )
            GROUP BY t.insider_id
            HAVING COUNT(*) >= 2
        """
        params: list = [latest, latest, latest]

        if min_tier is not None:
            query += " AND itr.score_tier >= ?"
            params.append(min_tier)

        query += " ORDER BY itr.score DESC NULLS LAST LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            # Calculate days_silent from last_sell_date to latest filing date
            days_row = conn.execute(
                "SELECT CAST(julianday(?) - julianday(?) AS INTEGER) AS days",
                (latest, item["last_sell_date"]),
            ).fetchone()
            item["days_silent"] = days_row["days"] if days_row else None
            items.append(item)

    # Deduplicate entities reporting the same economic event
    items = deduplicate_filers(
        items,
        value_key="sell_value_12m",
        date_key="last_sell_date",
        identity_keys=("insider_id", "name", "cik", "score", "score_tier"),
    )

    if not user.is_pro:
        items = null_items_track_records(items)
    if not user.has_full_feed:
        for item in items:
            item["gated"] = True
        items = redact_gated_items(items)
    encode_response_ids(items, trade=False, insider=True)

    return {"items": items, "total": len(items), "gated": not user.has_full_feed}


@router.get("/tagged")
def tagged_signals(
    user: UserContext = Depends(get_current_user),
    signal_type: Optional[str] = Query(default=None),
    signal_class: Optional[str] = Query(default=None, pattern="^(bullish|bearish|noise|neutral)$"),
    ticker: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None, ge=0.0, le=1.0),
    exclude_routine: bool = Query(default=True),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Paginated feed of tagged trade signals. Excludes routine/10b5-1 by default."""
    conditions = ["1=1"]
    params: list = []

    if signal_type is not None:
        conditions.append("ts.signal_type = ?")
        params.append(signal_type)
    if signal_class is not None:
        conditions.append("ts.signal_class = ?")
        params.append(signal_class)
    if ticker is not None:
        conditions.append("t.ticker = ?")
        params.append(ticker.upper())
    if date_from is not None:
        conditions.append("t.trade_date >= ?")
        params.append(date_from)
    if date_to is not None:
        conditions.append("t.trade_date <= ?")
        params.append(date_to)
    if min_confidence is not None:
        conditions.append("ts.confidence >= ?")
        params.append(min_confidence)
    if exclude_routine:
        conditions.append("(t.is_routine != 1 OR t.is_routine IS NULL)")

    where_clause = " AND ".join(conditions)
    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None

    with get_db() as conn:
        # Check if trade_signals table exists
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_signals'"
        ).fetchone()
        if not table_check:
            return {"total": 0, "limit": limit, "offset": offset, "items": []}

        count_row = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM trade_signals ts
            JOIN trades t ON ts.trade_id = t.trade_id
            WHERE {where_clause}
            """,
            params,
        ).fetchone()
        total = count_row["cnt"]

        rows = conn.execute(
            f"""
            SELECT
                ts.signal_id, ts.signal_type, ts.signal_label, ts.signal_class,
                ts.confidence, ts.metadata, ts.computed_at,
                t.trade_id, t.insider_id, t.ticker, t.company, t.title,
                t.trade_type, t.trade_date, t.filing_date, t.trans_code,
                t.price, t.qty, t.value, t.is_csuite,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM trade_signals ts
            JOIN trades t ON ts.trade_id = t.trade_id
            LEFT JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {where_clause}
            ORDER BY t.trade_date DESC, ts.confidence DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

    items = []
    for r in rows:
        item = dict(r)
        # Parse metadata JSON
        if item.get("metadata"):
            try:
                item["metadata"] = json.loads(item["metadata"])
            except (json.JSONDecodeError, TypeError):
                pass
        items.append(item)

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


@router.get("/types")
def signal_types(user: UserContext = Depends(get_current_user)) -> dict:
    """List available signal types with counts."""
    with get_db() as conn:
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_signals'"
        ).fetchone()
        if not table_check:
            return {"types": []}

        rows = conn.execute("""
            SELECT signal_type, signal_class, COUNT(*) AS count
            FROM trade_signals
            GROUP BY signal_type, signal_class
            ORDER BY count DESC
        """).fetchall()

    return {"types": [dict(r) for r in rows]}
