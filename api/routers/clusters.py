from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import add_trans_code_filter, deduplicate_filers
from api.gating import null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids

router = APIRouter(prefix="/api/v1/clusters", tags=["clusters"])


@router.get("")
def list_clusters(
    user: UserContext = Depends(get_current_user),
    days: int = Query(default=14, ge=1, le=90),
    min_insiders: int = Query(default=2, ge=2, le=20),
    min_value: Optional[float] = Query(default=None, ge=0),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    trans_codes: str = Query(default="P,S"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Detect clusters: tickers with multiple distinct insiders trading within a rolling window."""
    conditions = []
    params: list = []

    add_trans_code_filter(conditions, params, trans_codes)

    if trade_type is not None:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)

    extra_where = (" AND " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        latest = conn.execute("SELECT MAX(filing_date) AS d FROM trades").fetchone()["d"]
        if latest is None:
            return {"total": 0, "limit": limit, "offset": offset, "items": [], "window_start": None, "window_end": None}

        from datetime import datetime, timedelta
        latest_dt = datetime.strptime(latest, "%Y-%m-%d")
        window_start_date = (latest_dt - timedelta(days=days)).strftime("%Y-%m-%d")
        window_start = f"'{window_start_date}'"

        base_query = f"""
            SELECT
                ticker, trade_type, MAX(company) AS company,
                COUNT(DISTINCT representative_insider) AS insider_count,
                SUM(group_value) AS total_value,
                MIN(first_trade) AS first_trade,
                MAX(last_trade) AS last_trade,
                MAX(latest_filing) AS latest_filing,
                COUNT(*) AS trade_count,
                MAX(has_csuite) AS csuite_count,
                AVG(avg_score) AS avg_score
            FROM (
                SELECT
                    MAX(t.ticker) AS ticker, MAX(t.trade_type) AS trade_type,
                    MAX(t.company) AS company,
                    -- Pick one insider per txn_group (prefer C-suite)
                    CASE WHEN MAX(t.is_csuite) = 1
                        THEN MIN(CASE WHEN t.is_csuite = 1 THEN t.insider_id END)
                        ELSE MIN(t.insider_id)
                    END AS representative_insider,
                    SUM(t.value) / MAX(1, COUNT(*) * 1.0 / COUNT(DISTINCT t.insider_id)) AS group_value,
                    MIN(t.trade_date) AS first_trade,
                    MAX(t.trade_date) AS last_trade,
                    MAX(t.filing_date) AS latest_filing,
                    MAX(t.is_csuite) AS has_csuite,
                    AVG(itr.score) AS avg_score
                FROM trades t
                LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
                WHERE t.filing_date >= {window_start}
                  AND t.filing_date <= ?
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.superseded_by IS NULL
                  {extra_where}
                GROUP BY COALESCE(t.txn_group_id, t.accession)
            )
            GROUP BY ticker, trade_type
            HAVING COUNT(DISTINCT representative_insider) >= ?
        """

        having_params = params + [latest] + [min_insiders]

        if min_value is not None:
            # SUM(group_value) is the same outer aggregate that produces
            # total_value; t.value is not in scope outside the inner subquery.
            base_query += " AND SUM(group_value) >= ?"
            having_params.append(min_value)

        # Count total
        count_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM ({base_query})",
            having_params,
        ).fetchone()
        total = count_row["cnt"]

        # Fetch page
        rows = conn.execute(
            f"""
            {base_query}
            ORDER BY total_value DESC
            LIMIT ? OFFSET ?
            """,
            having_params + [limit, offset],
        ).fetchall()

        clusters = []
        for row in rows:
            cluster = dict(row)

            # Group by insider (bundles lots), then post-process to merge
            # entities reporting the same economic event (same value + date)
            insider_rows = conn.execute(
                f"""
                SELECT
                    t.insider_id, MAX(COALESCE(i.display_name, i.name)) AS name, MAX(i.cik) AS cik,
                    MAX(itr.score) AS score, MAX(itr.score_tier) AS score_tier,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score,
                    SUM(t.value) AS trade_value,
                    MAX(t.title) AS title,
                    MAX(t.is_csuite) AS is_csuite,
                    MAX(t.trade_date) AS last_trade_date,
                    COUNT(*) AS n_trades
                FROM trades t
                JOIN insiders i ON t.insider_id = i.insider_id
                LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
                WHERE t.ticker = ? AND t.trade_type = ?
                  AND t.filing_date >= {window_start}
                  AND t.filing_date <= ?
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.superseded_by IS NULL
                  {extra_where}
                GROUP BY t.insider_id
                ORDER BY SUM(t.value) DESC
                """,
                [row["ticker"], row["trade_type"]] + params + [latest],
            ).fetchall()
            # Post-process: merge insiders with identical trade_value + date
            # (e.g. SLTA IV + SLTA V both showing $74.6M on same date)
            raw_list = [dict(ir) for ir in insider_rows]
            insider_rows = deduplicate_filers(
                raw_list,
                value_key="trade_value",
                date_key="last_trade_date",
                identity_keys=("insider_id", "name", "cik", "score", "score_tier", "title"),
            )
            ins_list = [dict(ir) for ir in insider_rows]
            if not user.is_pro:
                ins_list = null_items_track_records(ins_list)
            if not user.has_full_feed:
                for ins in ins_list:
                    ins["gated"] = True
                ins_list = redact_gated_items(ins_list)
            encode_response_ids(ins_list, trade=False, insider=True)
            cluster["insiders"] = ins_list
            cluster["gated"] = not user.has_full_feed
            clusters.append(cluster)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": clusters,
        "window_start": latest.replace(latest, f"computed from {days}d window ending {latest}"),
        "window_end": latest,
    }


@router.get("/{ticker}/{trade_type}")
def get_cluster_detail(
    ticker: str,
    trade_type: str,
    days: int = Query(default=14, ge=1, le=90),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Detailed view of a single cluster: all trades and insiders for a ticker+type within window."""
    ticker = ticker.upper()
    _PS = "AND t.trans_code IN ('P', 'S')"

    with get_db() as conn:
        latest = conn.execute("SELECT MAX(filing_date) AS d FROM trades").fetchone()["d"]
        if latest is None:
            return {"ticker": ticker, "trade_type": trade_type, "insiders": [], "trades": []}

        from datetime import datetime, timedelta
        latest_dt = datetime.strptime(latest, "%Y-%m-%d")
        window_start_date = (latest_dt - timedelta(days=days)).strftime("%Y-%m-%d")
        window_start = f"'{window_start_date}'"

        # Summary
        summary = conn.execute(
            f"""
            SELECT
                t.ticker,
                t.trade_type,
                MAX(t.company) AS company,
                COUNT(DISTINCT t.txn_group_id) AS insider_count,
                SUM(t.value) / MAX(1, COUNT(*) * 1.0 / COUNT(DISTINCT t.txn_group_id)) AS total_value,
                MIN(t.trade_date) AS first_trade,
                MAX(t.trade_date) AS last_trade,
                COUNT(DISTINCT t.txn_group_id) AS trade_count,
                MAX(CASE WHEN t.is_csuite = 1 THEN 1 ELSE 0 END) AS csuite_count
            FROM trades t
            WHERE t.ticker = ? AND t.trade_type = ?
              AND t.filing_date >= {window_start}
              AND t.filing_date <= ?
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
              AND t.superseded_by IS NULL
              {_PS}
            GROUP BY t.ticker, t.trade_type
            """,
            (ticker, trade_type, latest),
        ).fetchone()

        if summary is None:
            return {"ticker": ticker, "trade_type": trade_type, "insiders": [], "trades": []}

        result = dict(summary)

        # Insiders — group by insider_id, then post-process to merge duplicate filers
        insider_rows = conn.execute(
            f"""
            SELECT
                t.insider_id, MAX(COALESCE(i.display_name, i.name)) AS name, MAX(i.cik) AS cik,
                MAX(itr.score) AS score, MAX(itr.score_tier) AS score_tier, MAX(itr.percentile) AS percentile,
                MAX(itr.buy_count) AS buy_count, MAX(itr.buy_win_rate_7d) AS buy_win_rate_7d, MAX(itr.buy_avg_return_7d) AS buy_avg_return_7d,
                MAX(t.pit_grade) AS pit_grade,
                MAX(t.pit_blended_score) AS pit_blended_score,
                SUM(t.value) AS trade_value, MAX(t.title) AS title,
                MAX(t.is_csuite) AS is_csuite,
                MAX(t.trade_date) AS last_trade_date, COUNT(*) AS n_trades, 1 AS n_filers
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            WHERE t.ticker = ? AND t.trade_type = ?
              AND t.filing_date >= {window_start}
              AND t.filing_date <= ?
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
              AND t.superseded_by IS NULL
              {_PS}
            GROUP BY t.insider_id
            ORDER BY SUM(t.value) DESC
            """,
            (ticker, trade_type, latest),
        ).fetchall()
        # Post-process: merge insiders with identical total value + date
        raw_list = [dict(ir) for ir in insider_rows]
        ins_list = deduplicate_filers(
            raw_list,
            value_key="trade_value",
            date_key="last_trade_date",
            identity_keys=("insider_id", "name", "cik", "score", "score_tier", "title"),
        )
        if not user.is_pro:
            ins_list = null_items_track_records(ins_list)
        if not user.has_full_feed:
            for ins in ins_list:
                ins["gated"] = True
            ins_list = redact_gated_items(ins_list)
        encode_response_ids(ins_list, trade=False, insider=True)
        result["insiders"] = ins_list

        # Individual trades (L2 aggregated)
        trade_rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value,
                agg.is_csuite,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, MAX(t.ticker) AS ticker, MAX(t.company) AS company, MAX(t.title) AS title,
                    t.trade_type, MIN(t.trade_date) AS trade_date, MIN(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    MAX(t.is_csuite) AS is_csuite,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE t.ticker = ? AND t.trade_type = ?
                  AND t.filing_date >= {window_start}
                  AND t.filing_date <= ?
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.superseded_by IS NULL
                  {_PS}
                GROUP BY t.insider_id, t.trade_type, CASE WHEN t.accession IS NOT NULL THEN t.accession ELSE t.trade_date END
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.trade_date DESC
            """,
            (ticker, trade_type, latest),
        ).fetchall()
        trades_list = [dict(tr) for tr in trade_rows]
        if not user.is_pro:
            trades_list = null_items_track_records(trades_list)
        if not user.has_full_feed:
            for t in trades_list:
                t["gated"] = True
            trades_list = redact_gated_items(trades_list)
        encode_response_ids(trades_list)
        result["trades"] = trades_list

    result["gated"] = not user.has_full_feed
    return result
