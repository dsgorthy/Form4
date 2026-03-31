from __future__ import annotations

from collections import defaultdict
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import null_items_track_records, redact_gated_items
from api.id_encoding import decode_insider_id, encode_response_ids

router = APIRouter(prefix="/api/v1/leaderboard", tags=["leaderboard"])

SORT_COLUMNS = {
    "score": "itr.score",
    "win_rate": "itr.buy_win_rate_7d",
    "alpha": "itr.buy_avg_abnormal_7d",
    "buy_count": "itr.buy_count",
    "percentile": "itr.percentile",
}


@router.get("")
def leaderboard(
    user: UserContext = Depends(get_current_user),
    sort_by: str = Query(default="score", pattern="^(score|win_rate|alpha|buy_count|percentile)$"),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
    min_trades: Optional[int] = Query(default=None, ge=1),
    min_tier: Optional[int] = Query(default=None, ge=1, le=5),
    title: Optional[str] = Query(default=None),
    tier: Optional[int] = Query(default=None, ge=1, le=5),
    hide_entities: bool = Query(default=False),
    active_since: Optional[str] = Query(default=None, description="Only insiders with trades since this date (YYYY-MM-DD)"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Ranked insiders, sortable and filterable."""
    conditions = ["itr.score IS NOT NULL"]
    params = []

    if min_trades is not None:
        conditions.append("itr.buy_count >= ?")
        params.append(min_trades)
    if min_tier is not None:
        conditions.append("itr.score_tier >= ?")
        params.append(min_tier)
    if tier is not None:
        conditions.append("itr.score_tier = ?")
        params.append(tier)
    if title is not None:
        conditions.append("itr.primary_title LIKE ?")
        params.append(f"%{title}%")
    if hide_entities:
        conditions.append("COALESCE(i.is_entity, 0) = 0")
    if active_since is not None:
        conditions.append("""i.insider_id IN (
            SELECT DISTINCT insider_id FROM trades
            WHERE trans_code IN ('P', 'S') AND filing_date >= ?
        )""")
        params.append(active_since)

    where_clause = " AND ".join(conditions)
    sort_col = SORT_COLUMNS[sort_by]
    order_dir = order.upper()

    with get_db() as conn:
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM insider_track_records itr
            JOIN insiders i ON itr.insider_id = i.insider_id
            WHERE {where_clause}
            """,
            params,
        ).fetchone()["cnt"]

        rows = conn.execute(
            f"""
            SELECT
                i.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                COALESCE(i.is_entity, 0) as is_entity,
                itr.score, itr.score_tier, itr.percentile,
                itr.buy_count, itr.buy_win_rate_7d,
                itr.buy_avg_return_7d, itr.buy_avg_abnormal_7d,
                itr.sell_count, itr.sell_win_rate_7d,
                itr.primary_title, itr.primary_ticker, itr.n_tickers,
                itr.score_recency_weighted, itr.tier_recency,
                itr.buy_last_date, itr.sell_last_date
            FROM insider_track_records itr
            JOIN insiders i ON itr.insider_id = i.insider_id
            WHERE {where_clause}
            ORDER BY {sort_col} {order_dir}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

    items = [dict(r) for r in rows]
    if not user.is_pro:
        items = null_items_track_records(items)
    if not user.has_full_feed:
        for item in items:
            item["gated"] = True
        items = redact_gated_items(items)
    encode_response_ids(items, trade=False, insider=True)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
        "gated": not user.has_full_feed,
    }


@router.get("/sparklines")
def sparklines(
    insider_ids: str = Query(..., description="Comma-separated insider IDs"),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Return last 10 trades' return_7d per insider for inline sparklines."""
    raw_tokens = [s.strip() for s in insider_ids.split(",") if s.strip()]
    if not raw_tokens:
        return {}

    # Decode sqids-encoded insider IDs to raw DB ints
    decoded_map: dict[int, str] = {}  # raw_id -> encoded token
    for token in raw_tokens:
        raw = decode_insider_id(token)
        if raw is not None:
            decoded_map[raw] = token

    if not decoded_map:
        return {}

    raw_ids = list(decoded_map.keys())
    placeholders = ",".join("?" for _ in raw_ids)

    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT t.insider_id, tr.return_7d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id IN ({placeholders})
              AND t.trade_type = 'buy'
              AND tr.return_7d IS NOT NULL
            ORDER BY t.trade_date DESC
            """,
            raw_ids,
        ).fetchall()

    # Group by insider, keep first 10 (most recent), then reverse to chronological
    grouped: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        iid = row["insider_id"]
        if len(grouped[iid]) < 10:
            grouped[iid].append(row["return_7d"])

    # Return with encoded insider_id keys
    return {decoded_map[iid]: list(reversed(returns)) for iid, returns in grouped.items()}
