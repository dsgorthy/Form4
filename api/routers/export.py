from __future__ import annotations

import csv
import io
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from api.auth import UserContext
from api.db import get_db
from api.filters import add_trans_code_filter
from api.gating import require_pro

router = APIRouter(prefix="/api/v1/export", tags=["export"])


@router.get("/filings")
def export_filings(
    user: UserContext = Depends(require_pro),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    ticker: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    min_value: Optional[float] = Query(default=None, ge=0),
    trans_codes: str = Query(default="P,S"),
    limit: int = Query(default=10000, ge=1, le=50000),
) -> StreamingResponse:
    """Export filings as CSV. Pro only."""
    conditions = []
    params: list = []

    add_trans_code_filter(conditions, params, trans_codes, alias="t")

    if trade_type:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)
    if ticker:
        conditions.append("t.ticker = ?")
        params.append(ticker.upper())
    if date_from:
        conditions.append("t.trade_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("t.trade_date <= ?")
        params.append(date_to)
    if min_value:
        conditions.append("t.value >= ?")
        params.append(min_value)

    where = " AND ".join(conditions) if conditions else "1=1"

    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT
                t.ticker, t.company, t.trade_type, t.trans_code, t.trade_date, t.filing_date,
                t.price, t.qty, t.value,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM trades t
            LEFT JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {where}
            ORDER BY t.filing_date DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        headers = [
            "ticker", "company", "trade_type", "trans_code", "trade_date", "filing_date",
            "price", "qty", "value", "insider_name", "cik",
            "score", "score_tier", "percentile",
            "return_7d", "return_30d", "return_90d",
            "abnormal_7d", "abnormal_30d", "abnormal_90d",
        ]
        writer.writerow(headers)
        buf.seek(0)
        yield buf.read()

        for row in rows:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow([row[h] for h in headers])
            buf.seek(0)
            yield buf.read()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=form4_filings.csv"},
    )
