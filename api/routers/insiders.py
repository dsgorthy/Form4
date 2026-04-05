from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext
from api.db import get_db
from api.filters import add_trans_code_filter, filing_group_by
from api.gating import require_pro
from api.id_encoding import decode_insider_id, encode_insider_id, encode_response_ids
from api.pit_helpers import get_best_pit_grade, get_ticker_grades
from api.signals_enrichment import enrich_items_with_signals
from api.context_enrichment import enrich_items_with_context
from api.price_dates import enrich_items_with_price_end

router = APIRouter(prefix="/api/v1/insiders", tags=["insiders"])


@router.get("/{identifier}")
def get_insider(identifier: str, user: UserContext = Depends(require_pro)) -> dict:
    """Insider profile with full track record. Accepts encoded sqids ID or CIK."""
    with get_db() as conn:
        # Try as sqids-encoded insider_id first, then as CIK
        insider = None
        decoded_id = decode_insider_id(identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.name_normalized, i.cik, COALESCE(i.is_entity, 0) as is_entity FROM insiders i WHERE i.insider_id = ?",
                (decoded_id,),
            ).fetchone()
        if insider is None:
            insider = conn.execute(
                "SELECT i.insider_id, COALESCE(i.display_name, i.name) AS name, i.name_normalized, i.cik, COALESCE(i.is_entity, 0) as is_entity FROM insiders i WHERE i.cik = ?",
                (identifier,),
            ).fetchone()

        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        track_record = conn.execute(
            "SELECT * FROM insider_track_records WHERE insider_id = ?",
            (insider_id,),
        ).fetchone()

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

        # Volume breakdown by transaction type (filing-level: one filing = one trade)
        volume_rows = conn.execute("""
            SELECT trans_code,
                   CASE WHEN trans_code IN ('P') THEN 'buy'
                        WHEN trans_code IN ('S') THEN 'sell'
                        ELSE trade_type END AS trade_type,
                   COUNT(*) AS count, SUM(total_value) AS total_value
            FROM (
                SELECT trans_code, trade_type, SUM(value) AS total_value
                FROM trades
                WHERE insider_id = ? AND trans_code IS NOT NULL
                  AND superseded_by IS NULL
                GROUP BY trans_code, filing_key
            )
            GROUP BY trans_code
            ORDER BY total_value DESC
        """, (insider_id,)).fetchall()

        # Filing-level win rates (consistent with filing-level counts)
        filing_win_rates = conn.execute(f"""
            SELECT
                trade_type,
                COUNT(*) AS total,
                SUM(CASE WHEN ret > 0 THEN 1 ELSE 0 END) AS wins,
                AVG(ret) AS avg_return,
                AVG(abn) AS avg_abnormal
            FROM (
                SELECT t.trade_type, tr.return_7d AS ret, tr.abnormal_7d AS abn
                FROM trades t
                JOIN trade_returns tr ON t.trade_id = tr.trade_id
                WHERE t.insider_id = ? AND t.trans_code IN ('P', 'S')
                  AND t.superseded_by IS NULL
                  AND tr.return_7d IS NOT NULL
                GROUP BY t.trade_type, {filing_group_by()}
            )
            GROUP BY trade_type
        """, (insider_id,)).fetchall()

        # Filing-level trade counts (consistent with feed display)
        filing_counts = conn.execute("""
            SELECT
                SUM(CASE WHEN trans_code = 'P' THEN 1 ELSE 0 END) AS buy_filings,
                SUM(CASE WHEN trans_code = 'S' THEN 1 ELSE 0 END) AS sell_filings
            FROM (
                SELECT trans_code
                FROM trades
                WHERE insider_id = ? AND trans_code IN ('P', 'S')
                  AND superseded_by IS NULL
                GROUP BY filing_key, trans_code
            )
        """, (insider_id,)).fetchone()

        # Sell pattern breakdown (filing-level)
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
                GROUP BY filing_key
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

    # Filing-level win rates override track record values
    filing_stats = {}
    for row in filing_win_rates:
        tt = row["trade_type"]
        total = row["total"]
        if tt == "buy":
            filing_stats["buy_win_rate_7d"] = round(row["wins"] / total, 4) if total else None
            filing_stats["buy_avg_return_7d"] = round(row["avg_return"], 6) if row["avg_return"] is not None else None
            filing_stats["buy_avg_abnormal_7d"] = round(row["avg_abnormal"], 6) if row["avg_abnormal"] is not None else None
        elif tt == "sell":
            # For sells, "win" = stock declined
            sell_wins = total - row["wins"]  # invert: wins counted as ret>0, but for sells ret<0 is good
            filing_stats["sell_win_rate_7d"] = round(sell_wins / total, 4) if total else None
            filing_stats["sell_avg_return_7d"] = round(row["avg_return"], 6) if row["avg_return"] is not None else None
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
    return result


@router.get("/{identifier}/score-history")
def get_insider_score_history(
    identifier: str,
    user: UserContext = Depends(require_pro),
) -> dict:
    """PIT score progression over time for an insider across all tickers."""
    with get_db() as conn:
        decoded_id = decode_insider_id(identifier)
        if decoded_id is None:
            row = conn.execute("SELECT insider_id FROM insiders WHERE cik = ?", (identifier,)).fetchone()
            decoded_id = row["insider_id"] if row else None
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
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(require_pro),
) -> dict:
    """Paginated trade history for an insider. Accepts encoded sqids ID or CIK."""
    with get_db() as conn:
        insider = None
        decoded_id = decode_insider_id(identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE cik = ?", (identifier,)
            ).fetchone()

        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        tc_conditions: list = ["insider_id = ?", "superseded_by IS NULL"]
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

        inner_conditions: list = ["t.insider_id = ?", "t.superseded_by IS NULL"]
        inner_params: list = [insider_id]
        add_trans_code_filter(inner_conditions, inner_params, trans_codes)
        inner_where = " AND ".join(inner_conditions)

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.ticker, agg.company, agg.title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.trans_code,
                agg.is_10b5_1, agg.is_routine,
                agg.pit_grade, agg.pit_blended_score,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.ticker, t.company, t.title,
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
def get_insider_companies(identifier: str, user: UserContext = Depends(require_pro)) -> dict:
    """Company history for an insider. Accepts encoded sqids ID or CIK."""
    with get_db() as conn:
        insider = None
        decoded_id = decode_insider_id(identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE cik = ?", (identifier,)
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
        decoded_id = decode_insider_id(identifier)
        if decoded_id is not None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE insider_id = ?", (decoded_id,)
            ).fetchone()
        if insider is None:
            insider = conn.execute(
                "SELECT insider_id FROM insiders WHERE cik = ?", (identifier,)
            ).fetchone()

        if insider is None:
            raise HTTPException(status_code=404, detail="Insider not found")

        insider_id = insider["insider_id"]

        rows = conn.execute(
            f"""
            SELECT tr.{col} AS ret, t.trade_type
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
                   SUM(t.value) AS value, tr.{col} AS ret
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ?
              AND t.trans_code IN ('P','S')
              AND t.superseded_by IS NULL
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
