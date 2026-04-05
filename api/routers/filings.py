from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import add_trans_code_filter, filing_group_by
from api.gating import get_free_cutoff_date, get_grace_cutoff_datetime, null_items_track_records, redact_gated_items
from api.id_encoding import decode_trade_id, encode_trade_id, encode_insider_id, encode_response_ids
from api.signals_enrichment import enrich_items_with_signals
from api.context_enrichment import enrich_items_with_context
from api.price_dates import enrich_items_with_price_end
from api.trade_grade import enrich_items_with_trade_grade

router = APIRouter(prefix="/api/v1/filings", tags=["filings"])


@router.get("")
def list_filings(
    user: UserContext = Depends(get_current_user),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell)$"),
    min_value: Optional[float] = Query(default=None, ge=0),
    min_tier: Optional[int] = Query(default=None, ge=1, le=5),
    ticker: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    trans_codes: str = Query(default="P,S"),
    hide_routine: bool = Query(default=False),
    hide_planned: bool = Query(default=False),
    include_private: bool = Query(default=False),
    min_grade: Optional[str] = Query(default=None, pattern="^[A-F]$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Paginated, filterable filings list with insider tier/score and returns."""
    conditions = ["t.superseded_by IS NULL"]
    if not include_private:
        conditions.append("t.ticker != 'NONE' AND t.ticker IS NOT NULL AND t.ticker != ''")
    params = []

    free_cutoff = get_free_cutoff_date() if not user.has_full_feed else None
    grace_cutoff = get_grace_cutoff_datetime() if user.is_grace else None

    add_trans_code_filter(conditions, params, trans_codes)

    if trade_type is not None:
        conditions.append("t.trade_type = ?")
        params.append(trade_type)
    if min_value is not None:
        conditions.append("t.value >= ?")
        params.append(min_value)
    if min_tier is not None:
        if min_tier >= 3:
            conditions.append("t.pit_grade = 'A'")
        elif min_tier >= 2:
            conditions.append("t.pit_grade IN ('A', 'B')")
        else:
            conditions.append("t.pit_grade IS NOT NULL")
    if ticker is not None:
        conditions.append("t.ticker = ?")
        params.append(ticker.upper())
    if date_from is not None:
        conditions.append("t.trade_date >= ?")
        params.append(date_from)
    if date_to is not None:
        conditions.append("t.trade_date <= ?")
        params.append(date_to)
    if hide_routine:
        conditions.append("(t.is_routine != 1 OR t.is_routine IS NULL)")
        conditions.append("(t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)")
    if hide_planned:
        conditions.append("(t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)")

    # Grace tier: 24h signal delay — hide filings filed in the last 24h
    if grace_cutoff:
        conditions.append("COALESCE(t.filed_at, t.filing_date) <= ?")
        params.append(grace_cutoff)

    # Grade filter: uses pre-computed signal_grade column on trades table
    grade_filter_active = min_grade is not None
    if grade_filter_active:
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3, "D": 4, "F": 5}
        min_idx = grade_order.get(min_grade, grade_order.get(min_grade.upper(), 5))
        allowed = [g for g, idx in grade_order.items() if idx <= min_idx and g != "F"]
        if allowed:
            placeholders = ",".join("?" * len(allowed))
            conditions.append(f"t.pit_grade IN ({placeholders})")
            params.extend(allowed)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    count_join = ""
    data_itr_join = ""

    _fgb = filing_group_by()

    fetch_limit = limit
    fetch_offset = offset

    # Date window optimization — skip when grade filtering (sparse results need wider scan)
    has_date_filter = date_from is not None or date_to is not None
    rows_needed = fetch_offset + fetch_limit
    if grade_filter_active:
        date_window = ""
    elif not has_date_filter and rows_needed <= 1000:
        days_needed = max((rows_needed // 4) + 7, 14)
        date_window = f"AND COALESCE(t.filed_at, t.filing_date) >= date('now', '-{days_needed} days')"
    else:
        date_window = ""

    with get_db() as conn:
        # For grade-filtered queries, skip expensive COUNT (full GROUP BY scan)
        # and use a fast estimate from the indexed signal_grade column instead.
        if grade_filter_active:
            count_row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM trades t WHERE {where_clause}",
                params,
            ).fetchone()
            # Rough estimate: each filing averages ~2.5 lots
            total = count_row["cnt"] // 2
        else:
            count_row = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt FROM (
                    SELECT 1
                    FROM trades t
                    {count_join}
                    WHERE {where_clause}
                    GROUP BY t.txn_group_id, t.ticker, t.trade_type
                )
                """,
                params,
            ).fetchone()
            total = count_row["cnt"]

        # Two-phase query: GROUP BY txn_group_id to collapse duplicate filers
        # reporting the same economic event. Picks the "best" insider (highest
        # track record score) as the representative filer.
        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.best_insider_id AS insider_id, agg.ticker, agg.company, agg.title, agg.normalized_title,
                agg.trade_type, agg.trade_date, agg.last_trade_date,
                agg.filing_date, agg.filed_at,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession, agg.trans_code,
                agg.is_10b5_1, agg.is_routine,
                agg.cohen_routine, agg.shares_owned_after, agg.is_rare_reversal, agg.week52_proximity,
                agg.pit_grade, agg.pit_blended_score,
                agg.n_filers, agg.n_filings, agg.is_amendment, agg.document_type,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile, itr.sell_win_rate_7d,
                itr.buy_win_rate_7d, itr.buy_avg_return_7d, itr.buy_avg_abnormal_7d,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                    SELECT
                        MIN(t.trade_id) AS trade_id,
                        -- Pick the most relevant insider as representative
                        -- Prefer C-suite, then lowest insider_id for determinism
                        CASE WHEN MAX(t.is_csuite) = 1
                            THEN MIN(CASE WHEN t.is_csuite = 1 THEN t.insider_id END)
                            ELSE MIN(t.insider_id)
                        END AS best_insider_id,
                        t.ticker,
                        MAX(t.company) AS company,
                        MAX(t.title) AS title,
                        MAX(t.normalized_title) AS normalized_title,
                        t.trade_type,
                        MIN(t.trade_date) AS trade_date,
                        MAX(t.trade_date) AS last_trade_date,
                        MIN(t.filing_date) AS filing_date,
                        MAX(t.filed_at) AS filed_at,
                        ROUND(SUM(t.value) / NULLIF(SUM(t.qty), 0), 2) AS price,
                        SUM(t.qty) AS qty,
                        SUM(t.value) AS value,
                        COUNT(*) AS lot_count,
                        MAX(t.is_csuite) AS is_csuite,
                        MIN(t.accession) AS accession,
                        GROUP_CONCAT(DISTINCT t.trans_code) AS trans_code,
                        MAX(t.is_10b5_1) AS is_10b5_1,
                        MAX(t.is_routine) AS is_routine,
                        MAX(t.cohen_routine) AS cohen_routine,
                        MAX(t.shares_owned_after) AS shares_owned_after,
                        MAX(t.is_rare_reversal) AS is_rare_reversal,
                        MAX(t.week52_proximity) AS week52_proximity,
                        MAX(t.pit_grade) AS pit_grade,
                        MAX(t.pit_blended_score) AS pit_blended_score,
                        COUNT(DISTINCT t.insider_id) AS n_filers,
                        COUNT(DISTINCT t.accession) AS n_filings,
                        MAX(t.is_amendment) AS is_amendment,
                        MAX(t.document_type) AS document_type,
                        t.txn_group_id
                    FROM trades t
                    {data_itr_join}
                    WHERE {where_clause}
                    {date_window}
                    GROUP BY t.txn_group_id, t.ticker, t.trade_type
                    ORDER BY COALESCE(t.filed_at, t.filing_date) DESC, SUM(t.value) DESC
                    LIMIT ? OFFSET ?
            ) agg
            LEFT JOIN insiders i ON agg.best_insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.best_insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY COALESCE(agg.filed_at, agg.filing_date) DESC, agg.value DESC
            """,
            params + [fetch_limit, fetch_offset],
        ).fetchall()

    items = [dict(r) for r in rows]

    # Enrich with signal tags, context facts, and price end dates
    with get_db() as sig_conn:
        enrich_items_with_signals(sig_conn, items)
        enrich_items_with_context(sig_conn, items)
    enrich_items_with_price_end(items)
    enrich_items_with_trade_grade(None, items)

    if free_cutoff:
        items = null_items_track_records(items)
        for item in items:
            item["gated"] = item["trade_date"] < free_cutoff
        items = redact_gated_items(items)
    elif grace_cutoff:
        # Grace: track records nulled, but no gated/redacted items (just 24h delayed)
        items = null_items_track_records(items)
    encode_response_ids(items)

    resp: dict = {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }
    if free_cutoff:
        resp["free_cutoff"] = free_cutoff
    if grace_cutoff:
        resp["grace_delay"] = True
    return resp


@router.get("/{trade_id}/related")
def get_related_trades(trade_id: str, limit: int = Query(default=5, ge=1, le=20), user: UserContext = Depends(get_current_user)) -> List[dict]:
    """Up to N other filings by the same insider, aggregated by accession, ordered by filing_date DESC."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        base = conn.execute(
            "SELECT insider_id, accession FROM trades WHERE trade_id = ?",
            (raw_id,),
        ).fetchone()

    if base is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    insider_id = base["insider_id"]
    current_accession = base["accession"]
    _fgb = filing_group_by()

    with get_db() as conn:
        # Exclude the current filing (by accession if available, else by trade_id)
        if current_accession:
            exclude_clause = "AND NOT (t.accession = ?)"
            exclude_param = current_accession
        else:
            exclude_clause = "AND t.trade_id != ?"
            exclude_param = raw_id

        rows = conn.execute(
            f"""
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title, agg.normalized_title,
                agg.trade_type, agg.trade_date, agg.last_trade_date,
                agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession, agg.trans_code,
                agg.is_10b5_1, agg.is_routine,
                agg.cohen_routine, agg.shares_owned_after, agg.is_rare_reversal, agg.week52_proximity,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile, itr.sell_win_rate_7d,
                itr.buy_win_rate_7d, itr.buy_avg_return_7d, itr.buy_avg_abnormal_7d,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM (
                SELECT
                    MIN(t.trade_id) AS trade_id,
                    t.insider_id, t.ticker, t.company, t.title, t.normalized_title,
                    t.trade_type,
                    MIN(t.trade_date) AS trade_date,
                    MAX(t.trade_date) AS last_trade_date,
                    MIN(t.filing_date) AS filing_date,
                    ROUND(SUM(t.value) / SUM(t.qty), 2) AS price,
                    SUM(t.qty) AS qty,
                    SUM(t.value) AS value,
                    COUNT(*) AS lot_count,
                    t.is_csuite, t.accession,
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
                WHERE t.insider_id = ?
                  {exclude_clause}
                  AND t.trans_code IN ('P', 'S')
                  AND t.superseded_by IS NULL
                GROUP BY t.insider_id, t.ticker, t.trade_type, {_fgb}
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.filing_date DESC
            LIMIT ?
            """,
            (insider_id, exclude_param, limit),
        ).fetchall()

    items = [dict(r) for r in rows]
    if not user.is_pro:
        items = null_items_track_records(items)
    # Grace users: filter out filings from last 24h
    if user.is_grace:
        from api.gating import get_grace_cutoff_datetime as _grace_cutoff
        cutoff = _grace_cutoff()
        items = [i for i in items if (i.get("filing_date") or "") <= cutoff]
    encode_response_ids(items)
    return items


@router.get("/{trade_id}")
def get_filing(trade_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Single filing detail with lot breakdown."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT
                t.trade_id, t.insider_id, t.ticker, t.company, t.title, t.normalized_title,
                t.trade_type, t.trade_date, t.filing_date, t.filed_at,
                t.price, t.qty, t.value, t.is_csuite, t.title_weight,
                t.source, t.accession, t.trans_code,
                t.is_10b5_1, t.is_routine, t.cohen_routine, t.shares_owned_after, t.is_rare_reversal, t.week52_proximity,
                t.pit_grade, t.pit_blended_score,
                t.is_amendment, t.document_type, t.date_of_orig_sub,
                COALESCE(i.is_entity, 0) as is_entity,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                itr.buy_count, itr.buy_win_rate_7d, itr.buy_avg_return_7d,
                itr.buy_avg_abnormal_7d, itr.sell_count, itr.sell_win_rate_7d,
                itr.primary_title, itr.primary_ticker,
                tr.entry_price,
                tr.return_7d, tr.return_30d, tr.return_90d,
                tr.spy_return_7d, tr.spy_return_30d, tr.spy_return_90d,
                tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
            FROM trades t
            LEFT JOIN insiders i ON t.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trade_id = ?
            """,
            (raw_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Filing not found")

        # Grace tier: block access to filings filed in the last 24h
        if user.is_grace:
            filed = row["filed_at"] or row["filing_date"] or ""
            if filed > get_grace_cutoff_datetime():
                raise HTTPException(status_code=403, detail="This filing is delayed 24h for your account. Upgrade for real-time access.")

        result = dict(row)

        # Add effective insider info if different from trade's insider
        try:
            if row["insider_id"]:
                eff = conn.execute("""
                    SELECT t.effective_insider_id FROM trades t WHERE t.trade_id = ?
                """, (raw_id,)).fetchone()
                if eff and eff["effective_insider_id"] and eff["effective_insider_id"] != row["insider_id"]:
                    eff_insider = conn.execute("""
                        SELECT insider_id, name, cik FROM insiders WHERE insider_id = ?
                    """, (eff["effective_insider_id"],)).fetchone()
                    if eff_insider:
                        result["effective_insider"] = dict(eff_insider)
                        result["effective_insider"]["insider_id"] = encode_insider_id(result["effective_insider"]["insider_id"])
        except Exception:
            pass

        # Find sibling lots: same filing (accession) or same insider+ticker+date+type
        if row["accession"]:
            lots = conn.execute(
                """
                SELECT t.trade_id, t.trade_date, t.price, t.qty, t.value, t.accession, t.shares_owned_after, t.cohen_routine
                FROM trades t
                WHERE t.accession = ? AND t.insider_id = ? AND t.trade_type = ?
                ORDER BY t.trade_date, t.price
                """,
                (row["accession"], row["insider_id"], row["trade_type"]),
            ).fetchall()
        else:
            lots = conn.execute(
                """
                SELECT t.trade_id, t.trade_date, t.price, t.qty, t.value, t.accession, t.shares_owned_after, t.cohen_routine
                FROM trades t
                WHERE t.insider_id = ? AND t.ticker = ? AND t.trade_date = ? AND t.trade_type = ?
                ORDER BY t.price
                """,
                (row["insider_id"], row["ticker"], row["trade_date"], row["trade_type"]),
            ).fetchall()

        if len(lots) > 1:
            lot_list = [dict(l) for l in lots]
            for lot in lot_list:
                if lot.get("trade_id") is not None:
                    lot["trade_id"] = encode_trade_id(lot["trade_id"])
            result["lots"] = lot_list
            result["total_qty"] = sum(l["qty"] for l in lots)
            result["total_value"] = sum(l["value"] for l in lots)
            # Use filing-level aggregated data for quality scoring consistency
            result["qty"] = result["total_qty"]
            result["value"] = result["total_value"]
            # shares_owned_after: max across lots (final holding after all lots)
            lot_soa = [l["shares_owned_after"] for l in lots if l["shares_owned_after"] is not None]
            if lot_soa:
                result["shares_owned_after"] = max(lot_soa)
            # cohen_routine: max (if any lot is routine, filing is routine)
            lot_cohen = [l["cohen_routine"] for l in lots if l["cohen_routine"] is not None]
            if lot_cohen:
                result["cohen_routine"] = max(lot_cohen)
        else:
            result["lots"] = []

    # Enrich with signal tags, context facts, and price end dates before encoding IDs
    with get_db() as sig_conn:
        enrich_items_with_signals(sig_conn, [result])
        enrich_items_with_context(sig_conn, [result])
    enrich_items_with_price_end([result])
    enrich_items_with_trade_grade(None, [result])

    if not user.is_pro:
        from api.gating import null_track_record_fields
        null_track_record_fields(result)

    # Encode top-level IDs
    if result.get("trade_id") is not None:
        result["trade_id"] = encode_trade_id(result["trade_id"])
    if result.get("insider_id") is not None:
        result["insider_id"] = encode_insider_id(result["insider_id"])

    return result


@router.get("/{trade_id}/what-if")
def what_if_simulation(trade_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Hypothetical performance at different time horizons and instruments.
    Shows what would have happened if you followed this insider's trade."""
    raw_id = decode_trade_id(trade_id)
    if raw_id is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT t.trade_id, t.ticker, t.trade_type, t.trade_date, t.filing_date, t.price,
                   tr.return_7d, tr.return_30d, tr.return_90d, tr.return_180d, tr.return_365d,
                   tr.spy_return_7d, tr.spy_return_30d, tr.spy_return_90d,
                   tr.spy_return_180d, tr.spy_return_365d,
                   tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d,
                   tr.abnormal_180d, tr.abnormal_365d,
                   tr.entry_price
            FROM trades t
            LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.trade_id = ?
            """,
            (raw_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Filing not found")

        result = dict(row)
        ticker = result["ticker"]
        filing_date = result["filing_date"]

        # Stock performance at each horizon
        horizons = []
        for window, days in [("7d", 7), ("30d", 30), ("90d", 90), ("180d", 180), ("365d", 365)]:
            ret = result.get(f"return_{window}")
            spy = result.get(f"spy_return_{window}")
            abn = result.get(f"abnormal_{window}")
            if ret is not None:
                entry = result.get("entry_price") or result.get("price") or 0
                horizons.append({
                    "window": window,
                    "days": days,
                    "stock_return": round(ret * 100, 2),
                    "spy_return": round(spy * 100, 2) if spy is not None else None,
                    "alpha": round(abn * 100, 2) if abn is not None else None,
                    "entry_price": round(entry, 2) if entry else None,
                    "exit_price": round(entry * (1 + ret), 2) if entry else None,
                    # Hypothetical P&L on $10K position
                    "pnl_10k": round(10000 * ret, 2),
                })

        # Options performance (if available)
        options = []
        if filing_date:
            # Use T+1 as entry date for options (same as stock entry)
            from datetime import datetime as _dt, timedelta as _td
            try:
                fd = _dt.strptime(filing_date[:10], "%Y-%m-%d").date()
                opt_entry_date = fd + _td(days=1)
                # Skip weekends
                while opt_entry_date.weekday() >= 5:
                    opt_entry_date += _td(days=1)
                opt_entry_str = opt_entry_date.isoformat()
            except Exception:
                opt_entry_str = filing_date

            entry_price = result.get("entry_price") or result.get("price") or 0
            opt_type = "P" if result["trade_type"] == "sell" else "C"

            for hold_label, hold_days in [("7d", 7), ("30d", 30), ("90d", 90)]:
                # For each hold period, find options with enough DTE to survive
                min_dte = hold_days + 7  # buffer so option doesn't expire during hold

                for strike_label, strike_mult in [("ITM (5%)", 1.05 if result["trade_type"] == "sell" else 0.95),
                                                   ("ATM", 1.00),
                                                   ("OTM (5%)", 0.95 if result["trade_type"] == "sell" else 1.05)]:
                    if entry_price <= 0:
                        continue
                    target_strike = round(entry_price * strike_mult, 2)

                    # Find contract with enough DTE for this hold period
                    opt = conn.execute("""
                        SELECT op.expiration, op.strike, op.ask, op.bid,
                               julianday(op.expiration) - julianday(op.trade_date) AS dte
                        FROM option_prices op
                        WHERE op.ticker = ? AND op.right = ? AND op.trade_date = ?
                          AND julianday(op.expiration) - julianday(op.trade_date) >= ?
                        ORDER BY ABS(op.strike - ?), op.expiration
                        LIMIT 1
                    """, (ticker, opt_type, opt_entry_str, min_dte, target_strike)).fetchone()

                    if not opt or not opt["ask"] or opt["ask"] <= 0:
                        continue

                    # Find exit price
                    exit_opt = conn.execute("""
                        SELECT bid, close FROM option_prices
                        WHERE ticker = ? AND right = ? AND expiration = ? AND strike = ?
                          AND trade_date BETWEEN date(?, '+' || ? || ' days') AND date(?, '+' || ? || ' days')
                        ORDER BY ABS(julianday(trade_date) - julianday(date(?, '+' || ? || ' days')))
                        LIMIT 1
                    """, (ticker, opt_type, opt["expiration"], opt["strike"],
                          opt_entry_str, hold_days - 2, opt_entry_str, hold_days + 3,
                          opt_entry_str, hold_days)).fetchone()

                    if not exit_opt:
                        continue

                    exit_px = exit_opt["bid"] if exit_opt["bid"] and exit_opt["bid"] > 0 else (exit_opt["close"] or 0) * 0.9
                    if exit_px <= 0:
                        continue

                    opt_return = (exit_px - opt["ask"]) / opt["ask"]
                    options.append({
                        "strike_label": strike_label,
                        "hold": hold_label,
                        "option_type": "Put" if opt_type == "P" else "Call",
                        "strike": opt["strike"],
                        "dte": int(opt["dte"]),
                        "entry_ask": round(opt["ask"], 2),
                        "exit_bid": round(exit_px, 2),
                        "return_pct": round(opt_return * 100, 1),
                        "pnl_1k": round(1000 * opt_return, 2),
                    })

    return {
        "ticker": ticker,
        "trade_type": result["trade_type"],
        "filing_date": filing_date,
        "horizons": horizons,
        "options": options,
    }
