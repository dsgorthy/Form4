from __future__ import annotations

import statistics
from typing import List

from fastapi import APIRouter, Depends, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.filters import deduplicate_filers
from api.gating import null_items_track_records, redact_gated_items
from api.id_encoding import encode_response_ids

# Dashboard always filters to open-market trades (P/S codes)
_PS_FILTER = "t.trans_code IN ('P', 'S')"
_PS_FILTER_BARE = "trans_code IN ('P', 'S')"

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


def _latest_filing_date(conn) -> str:
    """Get the most recent filing_date in the database.

    Allows the dashboard to show real data even when the DB hasn't been
    updated to today's date yet.
    """
    row = conn.execute("SELECT MAX(filing_date) AS d FROM trades").fetchone()
    return row["d"] if row["d"] else "2025-12-31"


@router.get("/stats")
def dashboard_stats(user: UserContext = Depends(get_current_user)) -> dict:
    """Key dashboard stats: signals today, active clusters, buy/sell ratio, top mover."""
    with get_db() as conn:
        latest = _latest_filing_date(conn)

        # Signals today: trades from PIT grade A/B insiders filed on the latest filing date
        signals_today = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM trades t
            WHERE t.filing_date = ?
              AND t.pit_grade IN ('A', 'B')
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
              AND t.superseded_by IS NULL
              AND """ + _PS_FILTER + """
            """,
            (latest,),
        ).fetchone()["cnt"]

        # Active clusters: distinct ticker+trade_type combos in last 7 days
        # where 2+ distinct insiders traded
        active_clusters = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM (
                SELECT ticker, trade_type
                FROM trades
                WHERE filing_date BETWEEN date(?, '-7 days') AND ?
                  AND (is_duplicate = 0 OR is_duplicate IS NULL)
                  AND superseded_by IS NULL
                  AND """ + _PS_FILTER_BARE + """
                Group BY ticker, trade_type
                HAVING COUNT(DISTINCT insider_id) >= 2
            )
            """,
            (latest, latest),
        ).fetchone()["cnt"]

        # Buy/sell dollar ratio trailing 5 days
        ratio_row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN value ELSE 0 END), 0) AS buy_val,
                COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN value ELSE 0 END), 0) AS sell_val
            FROM trades
            WHERE filing_date BETWEEN date(?, '-5 days') AND ?
              AND superseded_by IS NULL
              AND """ + _PS_FILTER_BARE + """
            """,
            (latest, latest),
        ).fetchone()
        buy_val = ratio_row["buy_val"]
        sell_val = ratio_row["sell_val"]
        if sell_val > 0:
            buy_sell_ratio = round(buy_val / sell_val, 3)
        else:
            buy_sell_ratio = 0.0

        # Top mover: highest value cluster ticker on the latest filing date
        top_mover_row = conn.execute(
            """
            SELECT ticker, SUM(value) AS total_value
            FROM trades
            WHERE filing_date = ?
              AND ticker != 'NONE'
              AND (is_duplicate = 0 OR is_duplicate IS NULL)
              AND superseded_by IS NULL
              AND """ + _PS_FILTER_BARE + """
            GROUP BY ticker
            HAVING COUNT(DISTINCT insider_id) >= 2
            ORDER BY total_value DESC
            LIMIT 1
            """,
            (latest,),
        ).fetchone()
        top_mover = None
        if top_mover_row:
            top_mover = {"ticker": top_mover_row["ticker"], "value": top_mover_row["total_value"]}

    return {
        "signals_today": signals_today,
        "active_clusters": active_clusters,
        "buy_sell_ratio": buy_sell_ratio,
        "top_mover": top_mover,
        "as_of": latest,
    }


@router.get("/sync-status")
def sync_status(user: UserContext = Depends(get_current_user)) -> dict:
    """Live sync status: last fetch time, filings today, freshness."""
    with get_db() as conn:
        # Last fetch run time (updated every run, even with 0 new filings)
        last_sync = conn.execute(
            "SELECT value AS ts FROM sync_meta WHERE key = 'last_fetch_at'"
        ).fetchone()
        if last_sync:
            last_sync_at = last_sync["ts"]
        else:
            # Fallback to old method if sync_meta doesn't exist yet
            last_sync = conn.execute(
                "SELECT MAX(processed_at) AS ts FROM processed_filings"
            ).fetchone()
            last_sync_at = last_sync["ts"] if last_sync else None

        # Most recent filed_at (SEC acceptance time)
        latest_filed = conn.execute(
            "SELECT MAX(filed_at) AS ts FROM trades WHERE filed_at IS NOT NULL"
        ).fetchone()
        latest_filed_at = latest_filed["ts"] if latest_filed else None

        # Filings ingested today (P/S only — open-market trades)
        today_count = conn.execute(
            """SELECT COUNT(*) AS cnt FROM trades
               WHERE filing_date = date('now')
               AND (is_duplicate = 0 OR is_duplicate IS NULL)
               AND superseded_by IS NULL
               AND """ + _PS_FILTER_BARE
        ).fetchone()["cnt"]

        # Latest filing date in DB
        latest_date = conn.execute(
            "SELECT MAX(filing_date) AS d FROM trades"
        ).fetchone()["d"]

        # Total trades (P/S only)
        total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE (is_duplicate = 0 OR is_duplicate IS NULL) AND superseded_by IS NULL AND " + _PS_FILTER_BARE
        ).fetchone()["cnt"]

    return {
        "last_sync_at": last_sync_at,
        "latest_filed_at": latest_filed_at,
        "latest_filing_date": latest_date,
        "filings_today": today_count,
        "total_trades": total,
    }


@router.get("/highlights")
def dashboard_highlights(user: UserContext = Depends(get_current_user)) -> dict:
    """Pre-built highlight views: top C-suite buys, large sells, and active clusters."""
    with get_db() as conn:
        latest = _latest_filing_date(conn)

        # Top 5 recent C-suite buys $100K+ (PIT grade A/B)
        csuite_buys = conn.execute(
            """
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                itr.buy_win_rate_7d, itr.buy_avg_return_7d, itr.buy_avg_abnormal_7d,
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
                    COUNT(*) AS lot_count,
                    t.is_csuite, t.accession,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE t.trade_type = 'buy'
                  AND t.is_csuite = 1
                  AND t.pit_grade IN ('A', 'B')
                  AND t.filing_date >= date(?, '-30 days')
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.superseded_by IS NULL
                  AND """ + _PS_FILTER + """
                GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date
                HAVING SUM(t.value) >= 100000
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.filing_date DESC, agg.value DESC
            LIMIT 5
            """,
            (latest,),
        ).fetchall()

        # Top 5 recent large sells $1M+
        large_sells = conn.execute(
            """
            SELECT
                agg.trade_id, agg.insider_id, agg.ticker, agg.company, agg.title,
                agg.trade_type, agg.trade_date, agg.filing_date,
                agg.price, agg.qty, agg.value, agg.lot_count,
                agg.is_csuite, agg.accession,
                agg.pit_grade, agg.pit_blended_score,
                COALESCE(i.display_name, i.name) AS insider_name, i.cik,
                itr.score, itr.score_tier, itr.percentile,
                itr.buy_win_rate_7d, itr.buy_avg_return_7d, itr.buy_avg_abnormal_7d,
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
                    COUNT(*) AS lot_count,
                    t.is_csuite, t.accession,
                    MAX(t.pit_grade) AS pit_grade,
                    MAX(t.pit_blended_score) AS pit_blended_score
                FROM trades t
                WHERE t.trade_type = 'sell'
                  AND t.filing_date >= date(?, '-30 days')
                  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
                  AND t.superseded_by IS NULL
                  AND """ + _PS_FILTER + """
                GROUP BY t.insider_id, t.ticker, t.trade_type, t.trade_date
                HAVING SUM(t.value) >= 1000000
            ) agg
            LEFT JOIN insiders i ON agg.insider_id = i.insider_id
            LEFT JOIN insider_track_records itr ON agg.insider_id = itr.insider_id
            LEFT JOIN trade_returns tr ON agg.trade_id = tr.trade_id
            ORDER BY agg.filing_date DESC, agg.value DESC
            LIMIT 5
            """,
            (latest,),
        ).fetchall()

        # Top 3 active clusters by total value (last 7 days, 2+ insiders)
        cluster_rows = conn.execute(
            """
            SELECT
                t.ticker,
                t.trade_type,
                MAX(t.company) AS company,
                COUNT(DISTINCT t.insider_id) AS insider_count,
                SUM(t.value) AS total_value,
                MIN(t.trade_date) AS first_trade,
                MAX(t.trade_date) AS last_trade,
                MAX(t.filing_date) AS latest_filing,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN t.is_csuite = 1 THEN 1 ELSE 0 END) AS csuite_count,
                AVG(itr.score) AS avg_score,
                MAX(t.pit_grade) AS pit_grade,
                AVG(t.pit_blended_score) AS avg_pit_blended_score
            FROM trades t
            LEFT JOIN insider_track_records itr ON t.insider_id = itr.insider_id
            WHERE t.filing_date >= date(?, '-7 days')
              AND t.filing_date <= ?
              AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
              AND t.superseded_by IS NULL
              AND """ + _PS_FILTER + """
            GROUP BY t.ticker, t.trade_type
            HAVING COUNT(DISTINCT t.insider_id) >= 2
            ORDER BY SUM(t.value) DESC
            LIMIT 3
            """,
            (latest, latest),
        ).fetchall()

    csuite_list = [dict(r) for r in csuite_buys]
    sells_list = [dict(r) for r in large_sells]
    clusters_list = [dict(r) for r in cluster_rows]

    # Deduplicate entities reporting the same economic event
    for _lst in (csuite_list, sells_list):
        _lst[:] = deduplicate_filers(
            _lst,
            value_key="value",
            date_key="trade_date",
            identity_keys=("insider_id", "insider_name", "cik", "score", "score_tier", "title"),
        )

    if not user.is_pro:
        csuite_list = null_items_track_records(csuite_list)
        sells_list = null_items_track_records(sells_list)
    if not user.has_full_feed:
        for item in csuite_list + sells_list:
            item["gated"] = True
        csuite_list = redact_gated_items(csuite_list)
        sells_list = redact_gated_items(sells_list)
    encode_response_ids(csuite_list)
    encode_response_ids(sells_list)

    return {
        "csuite_buys": csuite_list,
        "large_sells": sells_list,
        "top_clusters": clusters_list,
        "as_of": latest,
        "gated": not user.has_full_feed,
    }


@router.get("/sentiment")
def dashboard_sentiment(
    days: int = Query(default=90, ge=1, le=365),
    exclude_routine: bool = Query(default=True),
    user: UserContext = Depends(get_current_user),
) -> List[dict]:
    """Market-wide buy-sell dollar ratio, 5-day rolling average, for trailing N days.
    Excludes routine/10b5-1 trades by default for a cleaner signal."""
    routine_filter = "AND (is_routine != 1 OR is_routine IS NULL)" if exclude_routine else ""
    with get_db() as conn:
        latest = _latest_filing_date(conn)
        rows = conn.execute(
            f"""
            SELECT
                filing_date AS date,
                COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN value ELSE 0 END), 0) AS buy_value,
                COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN value ELSE 0 END), 0) AS sell_value
            FROM trades
            WHERE filing_date BETWEEN date(?, '-' || ? || ' days') AND ?
              AND superseded_by IS NULL
              AND {_PS_FILTER_BARE}
              {routine_filter}
            GROUP BY filing_date
            ORDER BY filing_date
            """,
            (latest, days, latest),
        ).fetchall()

    results = []
    for row in rows:
        buy_v = row["buy_value"]
        sell_v = row["sell_value"]
        ratio = round(buy_v / sell_v, 4) if sell_v > 0 else None
        results.append({
            "date": row["date"],
            "buy_value": buy_v,
            "sell_value": sell_v,
            "ratio": ratio,
        })

    # Compute 5-day rolling average ratio
    for i, item in enumerate(results):
        window = results[max(0, i - 4):i + 1]
        ratios = [w["ratio"] for w in window if w["ratio"] is not None]
        item["ratio_5d_avg"] = round(sum(ratios) / len(ratios), 4) if ratios else None

    return results


@router.get("/heatmap")
def dashboard_heatmap(days: int = Query(default=90, ge=1, le=365)) -> List[dict]:
    """Cluster events per day for last N days."""
    with get_db() as conn:
        latest = _latest_filing_date(conn)
        rows = conn.execute(
            """
            SELECT
                filing_date AS date,
                COUNT(*) AS count,
                SUM(value) AS total_value
            FROM trades
            WHERE filing_date BETWEEN date(?, '-' || ? || ' days') AND ?
              AND superseded_by IS NULL
              AND """ + _PS_FILTER_BARE + """
            GROUP BY filing_date
            ORDER BY filing_date
            """,
            (latest, days, latest),
        ).fetchall()

        # Get top ticker per day
        top_tickers = conn.execute(
            """
            SELECT filing_date AS date, ticker, SUM(value) AS tv
            FROM trades
            WHERE filing_date BETWEEN date(?, '-' || ? || ' days') AND ?
              AND superseded_by IS NULL
              AND """ + _PS_FILTER_BARE + """
            GROUP BY filing_date, ticker
            ORDER BY filing_date, tv DESC
            """,
            (latest, days, latest),
        ).fetchall()

    # Build lookup: date -> top ticker
    top_by_date = {}
    for r in top_tickers:
        d = r["date"]
        if d not in top_by_date:
            top_by_date[d] = r["ticker"]

    results = []
    for row in rows:
        results.append({
            "date": row["date"],
            "count": row["count"],
            "top_ticker": top_by_date.get(row["date"]),
            "total_value": row["total_value"],
        })

    return results


@router.get("/inflections")
def dashboard_inflections(
    exclude_routine: bool = Query(default=True),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Detect tickers where recent 7d activity spikes above the 90d rolling baseline.
    Excludes routine/10b5-1 trades by default for actionable signals only."""
    routine_filter = "AND (is_routine != 1 OR is_routine IS NULL)" if exclude_routine else ""
    with get_db() as conn:
        latest = _latest_filing_date(conn)

        # Recent 7d activity per ticker+trade_type
        recent_rows = conn.execute(
            f"""
            SELECT
                ticker,
                trade_type,
                MAX(company) AS company,
                SUM(value) AS recent_value,
                COUNT(DISTINCT insider_id) AS recent_insiders,
                MAX(filing_date) AS latest_filing
            FROM trades
            WHERE filing_date BETWEEN date(?, '-7 days') AND ?
              AND superseded_by IS NULL
              AND {_PS_FILTER_BARE}
              {routine_filter}
            GROUP BY ticker, trade_type
            """,
            (latest, latest),
        ).fetchall()

        # 90d baseline (days -90 to -7) per ticker+trade_type
        baseline_rows = conn.execute(
            f"""
            SELECT
                ticker,
                trade_type,
                SUM(value) / 90.0 AS daily_avg
            FROM trades
            WHERE filing_date BETWEEN date(?, '-90 days') AND date(?, '-8 days')
              AND superseded_by IS NULL
              AND {_PS_FILTER_BARE}
              {routine_filter}
            GROUP BY ticker, trade_type
            """,
            (latest, latest),
        ).fetchall()

    # Build baseline lookup
    baseline = {}
    for row in baseline_rows:
        baseline[(row["ticker"], row["trade_type"])] = row["daily_avg"]

    items = []
    for row in recent_rows:
        key = (row["ticker"], row["trade_type"])
        daily_avg = baseline.get(key, 0)
        baseline_weekly_avg = round(daily_avg * 7, 2)
        if baseline_weekly_avg <= 0:
            continue
        ratio = round(row["recent_value"] / baseline_weekly_avg, 2)
        if ratio < 2.0:
            continue
        items.append({
            "ticker": row["ticker"],
            "company": row["company"],
            "trade_type": row["trade_type"],
            "recent_value": row["recent_value"],
            "baseline_weekly_avg": baseline_weekly_avg,
            "ratio": ratio,
            "recent_insiders": row["recent_insiders"],
            "latest_filing": row["latest_filing"],
        })

    items.sort(key=lambda x: x["ratio"], reverse=True)
    items = items[:20]

    if not user.has_full_feed:
        for item in items:
            item["gated"] = True

    return {"items": items, "total": len(items), "gated": not user.has_full_feed}


@router.get("/filing-delays")
def filing_delays() -> dict:
    """Distribution of days between trade_date and filing_date."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT CAST(julianday(filing_date) - julianday(trade_date) AS INTEGER) AS delay_days
            FROM trades
            WHERE filing_date IS NOT NULL AND trade_date IS NOT NULL
              AND superseded_by IS NULL
              AND """ + _PS_FILTER_BARE + """
            """,
        ).fetchall()

    delays = [r["delay_days"] for r in rows]

    if not delays:
        return {
            "bins": [],
            "stats": {"avg_delay": 0, "median_delay": 0, "pct_within_2d": 0, "total": 0},
        }

    total = len(delays)

    # Bin definitions: (label, min_inclusive, max_inclusive)
    bin_defs = [
        ("0", 0, 0),
        ("1", 1, 1),
        ("2", 2, 2),
        ("3-5", 3, 5),
        ("6-10", 6, 10),
        ("11-30", 11, 30),
        ("30+", 31, None),
    ]

    bins = []
    for label, lo, hi in bin_defs:
        if hi is None:
            count = sum(1 for d in delays if d >= lo)
        else:
            count = sum(1 for d in delays if lo <= d <= hi)
        bins.append({
            "label": label,
            "count": count,
            "pct": round(count / total * 100, 1),
        })

    avg_delay = round(sum(delays) / total, 1)
    median_delay = round(statistics.median(delays), 1)
    within_2d = sum(1 for d in delays if d <= 2)
    pct_within_2d = round(within_2d / total * 100, 1)

    return {
        "bins": bins,
        "stats": {
            "avg_delay": avg_delay,
            "median_delay": median_delay,
            "pct_within_2d": pct_within_2d,
            "total": total,
        },
    }
