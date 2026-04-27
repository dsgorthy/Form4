from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.auth import UserContext, get_current_user
from api.db import get_db
from api.gating import require_pro_plus

router = APIRouter(prefix="/api/v1/congress", tags=["congress"])


def _tables_exist(conn: sqlite3.Connection) -> bool:
    """Check if congress_trades and politicians tables exist."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name IN ('congress_trades', 'politicians')
        """
    ).fetchone()
    return row["cnt"] == 2


@router.get("/trades")
def list_trades(
    ticker: Optional[str] = Query(default=None),
    trade_type: Optional[str] = Query(default=None, pattern="^(buy|sell|exchange)$"),
    politician_name: Optional[str] = Query(default=None),
    chamber: Optional[str] = Query(default=None, pattern="^(House|Senate)$"),
    min_value: Optional[int] = Query(default=None, ge=0),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(require_pro_plus),
) -> dict:
    """Paginated list of congress trades with politician details."""
    with get_db() as conn:
        if not _tables_exist(conn):
            return {"total": 0, "limit": limit, "offset": offset, "items": []}

        conditions = ["1=1"]
        params: list = []

        if ticker is not None:
            conditions.append("ct.ticker = ?")
            params.append(ticker.upper())
        if trade_type is not None:
            conditions.append("ct.trade_type = ?")
            params.append(trade_type)
        if politician_name is not None:
            conditions.append("p.name LIKE ?")
            params.append(f"%{politician_name}%")
        if chamber is not None:
            conditions.append("p.chamber = ?")
            params.append(chamber)
        if min_value is not None:
            conditions.append("ct.value_estimate >= ?")
            params.append(min_value)
        if date_from is not None:
            conditions.append("ct.trade_date >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append("ct.trade_date <= ?")
            params.append(date_to)

        where = " AND ".join(conditions)

        try:
            total = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM congress_trades ct
                JOIN politicians p ON ct.politician_id = p.politician_id
                WHERE {where}
                """,
                params,
            ).fetchone()["cnt"]

            rows = conn.execute(
                f"""
                SELECT
                    ct.congress_trade_id,
                    p.name AS politician_name,
                    p.chamber,
                    p.party,
                    p.state,
                    ct.ticker,
                    ct.company,
                    ct.trade_type,
                    ct.trade_date,
                    ct.value_low,
                    ct.value_high,
                    ct.value_estimate,
                    ct.filing_date,
                    ct.owner,
                    ct.asset_type
                FROM congress_trades ct
                JOIN politicians p ON ct.politician_id = p.politician_id
                WHERE {where}
                ORDER BY ct.trade_date DESC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
        except sqlite3.OperationalError:
            return {"total": 0, "limit": limit, "offset": offset, "items": []}

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [dict(r) for r in rows],
    }


@router.get("/politicians")
def list_politicians(user: UserContext = Depends(require_pro_plus)) -> dict:
    """List politicians with trade counts, ordered by total estimated value."""
    with get_db() as conn:
        if not _tables_exist(conn):
            return {"items": []}

        try:
            rows = conn.execute(
                """
                SELECT
                    p.politician_id,
                    MAX(p.name) AS name,
                    MAX(p.chamber) AS chamber,
                    MAX(p.party) AS party,
                    MAX(p.state) AS state,
                    COUNT(*) AS trade_count,
                    COALESCE(SUM(ct.value_estimate), 0) AS total_value_estimate,
                    MAX(ct.trade_date) AS last_trade_date
                FROM politicians p
                JOIN congress_trades ct ON p.politician_id = ct.politician_id
                GROUP BY p.politician_id
                ORDER BY total_value_estimate DESC
                """
            ).fetchall()
        except sqlite3.OperationalError:
            return {"items": []}

    return {"items": [dict(r) for r in rows]}


@router.get("/analytics")
def congress_analytics(days: int = Query(default=90, ge=7, le=365), user: UserContext = Depends(require_pro_plus)) -> dict:
    """Summary stats, daily heatmap, and top tickers for the congress page."""
    with get_db() as conn:
        if not _tables_exist(conn):
            return {"summary": {}, "heatmap": [], "top_tickers": [], "top_politicians": []}

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        try:
            # Summary stats
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_trades,
                    COALESCE(SUM(value_estimate), 0) AS total_value,
                    SUM(CASE WHEN trade_type = 'buy' THEN 1 ELSE 0 END) AS buys,
                    SUM(CASE WHEN trade_type = 'sell' THEN 1 ELSE 0 END) AS sells,
                    COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN value_estimate ELSE 0 END), 0) AS buy_value,
                    COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN value_estimate ELSE 0 END), 0) AS sell_value,
                    COUNT(DISTINCT politician_id) AS active_politicians,
                    COUNT(DISTINCT ticker) AS unique_tickers
                FROM congress_trades
                WHERE trade_date >= ?
                """,
                (cutoff,),
            ).fetchone()

            # Average filing delay
            delay = conn.execute(
                """
                SELECT ROUND(AVG(filing_delay_days)) AS avg_delay
                FROM congress_trades
                WHERE trade_date >= ? AND filing_delay_days IS NOT NULL AND filing_delay_days > 0
                """,
                (cutoff,),
            ).fetchone()

            # Daily heatmap
            heatmap_rows = conn.execute(
                """
                SELECT
                    trade_date AS date,
                    COUNT(*) AS count,
                    COALESCE(SUM(value_estimate), 0) AS total_value,
                    (SELECT ct2.ticker FROM congress_trades ct2
                     WHERE ct2.trade_date = ct.trade_date
                     GROUP BY ct2.ticker ORDER BY COUNT(*) DESC LIMIT 1) AS top_ticker
                FROM congress_trades ct
                WHERE trade_date >= ?
                GROUP BY trade_date
                ORDER BY trade_date
                """,
                (cutoff,),
            ).fetchall()

            # Top tickers
            top_tickers = conn.execute(
                """
                SELECT
                    ticker,
                    COUNT(*) AS trade_count,
                    COALESCE(SUM(value_estimate), 0) AS total_value,
                    SUM(CASE WHEN trade_type = 'buy' THEN 1 ELSE 0 END) AS buys,
                    SUM(CASE WHEN trade_type = 'sell' THEN 1 ELSE 0 END) AS sells,
                    COUNT(DISTINCT politician_id) AS politicians
                FROM congress_trades
                WHERE trade_date >= ?
                GROUP BY ticker
                ORDER BY COUNT(*) DESC
                LIMIT 10
                """,
                (cutoff,),
            ).fetchall()

            # Top politicians
            top_politicians = conn.execute(
                """
                SELECT
                    MAX(p.name) AS name,
                    MAX(p.party) AS party,
                    MAX(p.chamber) AS chamber,
                    COUNT(*) AS trade_count,
                    COALESCE(SUM(ct.value_estimate), 0) AS total_value,
                    SUM(CASE WHEN ct.trade_type = 'buy' THEN 1 ELSE 0 END) AS buys,
                    SUM(CASE WHEN ct.trade_type = 'sell' THEN 1 ELSE 0 END) AS sells
                FROM congress_trades ct
                JOIN politicians p ON ct.politician_id = p.politician_id
                WHERE ct.trade_date >= ?
                GROUP BY ct.politician_id
                ORDER BY COUNT(*) DESC
                LIMIT 5
                """,
                (cutoff,),
            ).fetchall()

        except sqlite3.OperationalError:
            return {"summary": {}, "heatmap": [], "top_tickers": [], "top_politicians": []}

    return {
        "summary": {
            **dict(summary),
            "avg_filing_delay": delay["avg_delay"] if delay["avg_delay"] else None,
        },
        "heatmap": [dict(r) for r in heatmap_rows],
        "top_tickers": [dict(r) for r in top_tickers],
        "top_politicians": [dict(r) for r in top_politicians],
    }


@router.get("/convergence")
def convergence(days: int = Query(default=90, ge=7, le=365), user: UserContext = Depends(require_pro_plus)) -> dict:
    """Detect tickers where both insiders and politicians bought within a 30-day window.

    Uses trailing N days from the latest data in each table.
    """
    with get_db() as conn:
        if not _tables_exist(conn):
            return {"items": []}

        try:
            # Use the latest actual dates from each table, not date('now')
            latest_insider = conn.execute(
                "SELECT MAX(trade_date) AS d FROM trades WHERE trade_date <= '2027-01-01'"
            ).fetchone()["d"] or "2025-12-31"
            latest_congress = conn.execute(
                "SELECT MAX(trade_date) AS d FROM congress_trades"
            ).fetchone()["d"] or "2025-12-31"

            # Compute cutoff dates in Python to avoid SQLite date() issues
            from datetime import datetime, timedelta
            insider_cutoff = (datetime.strptime(latest_insider, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
            congress_cutoff = (datetime.strptime(latest_congress, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")

            rows = conn.execute(
                """
                SELECT
                    ins.ticker,
                    ins.company,
                    ins.insider_buys,
                    ins.insider_total_value,
                    pol.politician_buys,
                    pol.politician_total_value_estimate,
                    MIN(ins.first_date, pol.first_date) AS first_date,
                    MAX(ins.last_date, pol.last_date) AS last_date
                FROM (
                    SELECT
                        ticker,
                        MAX(company) AS company,
                        COUNT(*) AS insider_buys,
                        SUM(value) AS insider_total_value,
                        MIN(trade_date) AS first_date,
                        MAX(trade_date) AS last_date
                    FROM trades
                    WHERE trade_type = 'buy'
                      AND trade_date >= ?
                      AND trade_date <= '2027-01-01'
                    GROUP BY ticker
                ) ins
                INNER JOIN (
                    SELECT
                        ticker,
                        COUNT(*) AS politician_buys,
                        COALESCE(SUM(value_estimate), 0) AS politician_total_value_estimate,
                        MIN(trade_date) AS first_date,
                        MAX(trade_date) AS last_date
                    FROM congress_trades
                    WHERE trade_type = 'buy'
                      AND trade_date >= ?
                    GROUP BY ticker
                ) pol ON ins.ticker = pol.ticker
                WHERE ABS(julianday(ins.last_date) - julianday(pol.last_date)) <= 30
                   OR ABS(julianday(ins.first_date) - julianday(pol.first_date)) <= 30
                ORDER BY (ins.insider_buys + pol.politician_buys) DESC,
                         (ins.insider_total_value + pol.politician_total_value_estimate) DESC
                """,
                (insider_cutoff, congress_cutoff),
            ).fetchall()
        except sqlite3.OperationalError:
            return {"items": []}

    return {"items": [dict(r) for r in rows]}


@router.get("/by-ticker/{ticker}")
def trades_by_ticker(
    ticker: str,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Paginated congress trades for a specific ticker, with politician details."""
    ticker = ticker.upper()

    with get_db() as conn:
        if not _tables_exist(conn):
            return {"ticker": ticker, "trades": [], "total": 0, "limit": limit, "offset": offset}

        try:
            total = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM congress_trades ct
                WHERE ct.ticker = ?
                """,
                (ticker,),
            ).fetchone()["cnt"]

            rows = conn.execute(
                """
                SELECT
                    ct.congress_trade_id,
                    p.name AS politician_name,
                    p.chamber,
                    p.party,
                    p.state,
                    ct.trade_type,
                    ct.trade_date,
                    ct.value_low,
                    ct.value_high,
                    ct.value_estimate,
                    ct.filing_date,
                    ct.owner
                FROM congress_trades ct
                JOIN politicians p ON ct.politician_id = p.politician_id
                WHERE ct.ticker = ?
                ORDER BY ct.trade_date DESC
                LIMIT ? OFFSET ?
                """,
                (ticker, limit, offset),
            ).fetchall()
        except sqlite3.OperationalError:
            return {"ticker": ticker, "trades": [], "total": 0, "limit": limit, "offset": offset}

    return {
        "ticker": ticker,
        "trades": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }
