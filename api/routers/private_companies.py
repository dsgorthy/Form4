from __future__ import annotations

import base64
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import get_free_cutoff_date, null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/private-companies", tags=["private-companies"])


# ---------------------------------------------------------------------------
# Slug helpers (base64url of the company name — reversible, no DB changes)
# ---------------------------------------------------------------------------

def company_to_slug(name: str) -> str:
    return base64.urlsafe_b64encode(name.encode()).rstrip(b"=").decode()


def slug_to_company(slug: str) -> str:
    # Add back padding
    padded = slug + "=" * (-len(slug) % 4)
    return base64.urlsafe_b64decode(padded).decode()


# ---------------------------------------------------------------------------
# List all private companies
# ---------------------------------------------------------------------------

@router.get("")
def list_private_companies(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """List all private companies (ticker = 'NONE') with trade counts."""
    with get_db() as conn:
        total = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM (
                SELECT 1 FROM trades
                WHERE ticker = 'NONE'
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                  AND superseded_by IS NULL
                  AND is_derivative = 0
                  AND trans_code IN ('P', 'S')
                GROUP BY company
            )
            """
        ).fetchone()["cnt"]

        rows = conn.execute(
            """
            SELECT
                company,
                COUNT(*) AS total_trades,
                SUM(value) AS total_value,
                MIN(trade_date) AS first_trade,
                MAX(trade_date) AS last_trade
            FROM trades
            WHERE ticker = 'NONE'
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
              AND superseded_by IS NULL
              AND is_derivative = 0
              AND trans_code IN ('P', 'S')
            GROUP BY company
            ORDER BY COUNT(*) DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()

    items = []
    for r in rows:
        item = dict(r)
        item["slug"] = company_to_slug(item["company"])
        item["is_private"] = True
        items.append(item)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


# ---------------------------------------------------------------------------
# Company profile
# ---------------------------------------------------------------------------

@router.get("/{slug}")
def get_private_company(slug: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Private company overview with insider roster."""
    try:
        company_name = slug_to_company(slug)
    except Exception:
        raise HTTPException(status_code=404, detail="Invalid company slug")

    with get_db() as conn:
        overview = conn.execute(
            """
            SELECT
                company,
                COUNT(*) AS total_trades,
                SUM(value) AS total_value,
                MIN(trade_date) AS first_trade,
                MAX(trade_date) AS last_trade
            FROM trades
            WHERE ticker = 'NONE'
              AND company = ?
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
              AND superseded_by IS NULL
              AND is_derivative = 0
              AND trans_code IN ('P', 'S')
            GROUP BY company
            """,
            (company_name,),
        ).fetchone()

        if overview is None:
            raise HTTPException(status_code=404, detail="Private company not found")

        # Insider roster — query trades directly since insider_companies
        # likely has no entries for NONE-ticker companies
        roster = conn.execute(
            """
            SELECT
                t.insider_id,
                MAX(COALESCE(i.display_name, i.name)) AS name,
                MAX(i.cik) AS cik,
                MAX(COALESCE(i.is_entity, 0)) AS is_entity,
                (SELECT t2.normalized_title FROM trades t2
                 WHERE t2.insider_id = t.insider_id
                   AND t2.ticker = 'NONE' AND t2.company = ?
                   AND t2.normalized_title IS NOT NULL AND t2.normalized_title != ''
                 ORDER BY t2.trade_date DESC LIMIT 1) AS normalized_title,
                COUNT(*) AS trade_count,
                SUM(t.value) AS total_value,
                MIN(t.trade_date) AS first_trade,
                MAX(t.trade_date) AS last_trade,
                MAX(itr.score) AS score, MAX(itr.score_tier) AS score_tier, MAX(itr.percentile) AS percentile
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            WHERE t.ticker = 'NONE'
              AND t.company = ?
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
              AND t.superseded_by IS NULL
              AND t.is_derivative = 0
              AND t.trans_code IN ('P', 'S')
            GROUP BY t.insider_id
            ORDER BY itr.score DESC
            """,
            (company_name, company_name),
        ).fetchall()

        roster_list = [dict(r) for r in roster]

        # Group entities under primary insiders
        try:
            for ins in roster_list:
                if ins.get("is_entity"):
                    group = conn.execute("""
                        SELECT ig.primary_insider_id, COALESCE(pi.display_name, pi.name) AS primary_name
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

    result = dict(overview)
    result["is_private"] = True
    result["slug"] = slug
    if not user.is_pro:
        roster_list = null_items_track_records(roster_list)
    encode_response_ids(roster_list, trade=False, insider=True)
    result["insiders"] = roster_list
    return result


# ---------------------------------------------------------------------------
# Company trades (paginated)
# ---------------------------------------------------------------------------

@router.get("/{slug}/trades")
def get_private_company_trades(
    slug: str,
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Paginated trades for a private company."""
    try:
        company_name = slug_to_company(slug)
    except Exception:
        raise HTTPException(status_code=404, detail="Invalid company slug")

    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None

    conditions = [
        "t.ticker = 'NONE'",
        "t.company = ?",
        "(t.is_duplicate = 0 OR t.is_duplicate IS NULL)",
        "t.superseded_by IS NULL",
        "t.is_derivative = 0",
        "t.trans_code IN ('P', 'S')",
    ]
    params: list = [company_name]

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
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            )
            """,
            params,
        ).fetchone()["cnt"]

        if total == 0 and trade_type is None:
            exists = conn.execute(
                "SELECT 1 FROM trades WHERE ticker = 'NONE' AND company = ? LIMIT 1",
                (company_name,),
            ).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Private company not found")

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title,
                agg.normalized_title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, MAX(t.ticker) AS ticker, MAX(t.company) AS company, MAX(t.title) AS title,
                    MAX(t.normalized_title) AS normalized_title,
                    t.trade_type, t.trade_date, MAX(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    COUNT(*) AS lot_count,
                    MAX(t.is_csuite) AS is_csuite,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE {where_clause}
                GROUP BY t.insider_id, t.trade_type, t.trade_date
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            ORDER BY agg.trade_date DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

    items = [dict(r) for r in rows]
    # No return columns for private companies — remove them if present
    for item in items:
        for key in ("return_7d", "return_30d", "return_90d",
                     "abnormal_7d", "abnormal_30d", "abnormal_90d"):
            item.pop(key, None)

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
