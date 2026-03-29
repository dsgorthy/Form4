from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import add_trans_code_filter
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

        window_start = f"date('{latest}', '-{days} days')"

        # Find clusters: ticker+trade_type combos with 2+ distinct insiders in the window
        # Count distinct insiders per ticker (after deduplicating filers
        # who report the same economic event via txn_group_id).
        # Step: for each txn_group, pick one insider_id, then count distinct.
        base_query = f"""
            SELECT
                ticker, trade_type, company,
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
                    t.ticker, t.trade_type,
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
                  {extra_where}
                GROUP BY t.txn_group_id
            )
            GROUP BY ticker, trade_type
            HAVING COUNT(DISTINCT representative_insider) >= ?
        """

        having_params = params + [latest] + [min_insiders]

        if min_value is not None:
            base_query += " AND SUM(t.value) >= ?"
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
                    t.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                    itr.score, itr.score_tier,
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
                  {extra_where}
                GROUP BY t.insider_id
                ORDER BY SUM(t.value) DESC
                """,
                [row["ticker"], row["trade_type"]] + params + [latest],
            ).fetchall()
            # Post-process: merge insiders with identical trade_value + date
            # (e.g. SLTA IV + SLTA V both showing $74.6M on same date)
            raw_list = [dict(ir) for ir in insider_rows]
            seen_signatures: dict[str, dict] = {}
            deduped_list = []
            for ins in raw_list:
                sig = f"{round(ins['trade_value'],0)}|{ins['last_trade_date']}"
                if sig in seen_signatures:
                    # Merge: keep the one with better score, note n_filers
                    existing = seen_signatures[sig]
                    existing["n_filers"] = existing.get("n_filers", 1) + 1
                    if (ins.get("score") or 0) > (existing.get("score") or 0):
                        existing.update({
                            "insider_id": ins["insider_id"],
                            "name": ins["name"],
                            "cik": ins["cik"],
                            "score": ins["score"],
                            "score_tier": ins["score_tier"],
                            "title": ins["title"],
                        })
                else:
                    ins["n_filers"] = 1
                    seen_signatures[sig] = ins
                    deduped_list.append(ins)
            insider_rows = deduped_list
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

        window_start = f"date('{latest}', '-{days} days')"

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
                t.insider_id, COALESCE(i.display_name, i.name) AS name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                itr.buy_count, itr.buy_win_rate_7d, itr.buy_avg_return_7d,
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
              {_PS}
            GROUP BY t.insider_id
            ORDER BY SUM(t.value) DESC
            """,
            (ticker, trade_type, latest),
        ).fetchall()
        # Post-process: merge insiders with identical total value + date
        raw_list = [dict(ir) for ir in insider_rows]
        seen_sigs: dict[str, dict] = {}
        deduped = []
        for ins in raw_list:
            sig = f"{round(ins['trade_value'], 0)}|{ins['last_trade_date']}"
            if sig in seen_sigs:
                seen_sigs[sig]["n_filers"] = seen_sigs[sig].get("n_filers", 1) + 1
                if (ins.get("score") or 0) > (seen_sigs[sig].get("score") or 0):
                    seen_sigs[sig].update({
                        "insider_id": ins["insider_id"], "name": ins["name"],
                        "cik": ins["cik"], "score": ins["score"],
                        "score_tier": ins["score_tier"], "title": ins["title"],
                    })
            else:
                ins["n_filers"] = 1
                seen_sigs[sig] = ins
                deduped.append(ins)
        ins_list = deduped
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
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, t.ticker, t.company, t.title,
                    t.trade_type, t.trade_date, t.filing_date,
                    ROUND(SUM(t.value) / SUM(t.qty), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    t.is_csuite
                FROM trades t
                WHERE t.ticker = ? AND t.trade_type = ?
                  AND t.filing_date >= {window_start}
                  AND t.filing_date <= ?
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
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
