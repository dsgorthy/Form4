"""Pyrrho dataplane MCP server — read-only tools over the signal store.

Pulled forward 2026-06-10 per Derek: scope is **data access only**.
Claude is the reasoning layer; this server is the typed retrieval surface.

No backfill / write / strategy authoring / persona logic in here — those
belong in the Desk workbench. This module's only job is to let an
operator query observations, the catalog, strategy evaluation tapes,
and ticker snapshots from Claude Desktop / Code via MCP.

Run via stdio (Claude Desktop config):

    "pyrrho": {
      "command": "/Users/derekg/dataplane_venv/bin/python",
      "args": ["-m", "dataplane.mcp_server"],
      "env": {
        "PYRRHO_DATAPLANE_DSN": "dbname=pyrrho_data_dev host=localhost"
      }
    }
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from mcp.server.fastmcp import FastMCP


mcp = FastMCP("pyrrho-dataplane")


@contextmanager
def _conn():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


def _rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        d = {}
        for k, v in zip(cols, row):
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif isinstance(v, (dict, list)):
                d[k] = v
            elif isinstance(v, str) and v.startswith("{") and k in ("value", "metadata", "upstream", "output_schema"):
                try:
                    d[k] = json.loads(v)
                except Exception:
                    d[k] = v
            else:
                d[k] = v
        out.append(d)
    return out


# ── Tools ──────────────────────────────────────────────────────────────

@mcp.tool()
def list_signals(include_strategies: bool = True) -> List[Dict[str, Any]]:
    """List every active signal in the catalog (raw signals + composite
    strategies). Returns id, version, class, description, owner, SLA,
    upstream declarations, output schema, and registration metadata.

    Use this first when exploring what data is available."""
    sql = """
        SELECT signal_id, version, signal_class::text AS signal_class,
               description, owner, sla_hours, business_hours_only,
               upstream, output_schema, registered_at, last_modified_at, status::text
          FROM signal_definitions
         WHERE status = 'active'
    """
    if not include_strategies:
        sql += " AND signal_class != 'composite'"
    sql += " ORDER BY signal_class, signal_id"
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return _rows(cur)
        finally:
            cur.close()


@mcp.tool()
def get_signal(signal_id: str) -> Dict[str, Any]:
    """Get full metadata for one signal (catalog row + observation rollup:
    total rows, latest ingest, latest as_of, earliest as_of, freshness
    status vs SLA, recent 24h / 7d row counts).

    Use when you want to know everything about a particular feed
    before drilling into observations."""
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT signal_id, version, signal_class::text AS signal_class,
                       description, owner, sla_hours, business_hours_only,
                       upstream, output_schema
                  FROM signal_definitions
                 WHERE signal_id = %s AND status = 'active'
                 LIMIT 1
                """,
                (signal_id,),
            )
            row = _rows(cur)
            if not row:
                return {"error": f"unknown signal {signal_id}"}
            meta = row[0]

            cur.execute(
                """
                SELECT COUNT(*)               AS row_count,
                       MAX(ingested_at)       AS latest_ingested_at,
                       MAX(as_of_date)        AS latest_as_of,
                       MIN(as_of_date)        AS earliest_as_of,
                       COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '24 hours') AS rows_24h,
                       COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '7 days')   AS rows_7d
                  FROM signal_observations
                 WHERE signal_id LIKE %s
                """,
                (f"{signal_id}%",),
            )
            stats = _rows(cur)[0]
            meta.update(stats)
            return meta
        finally:
            cur.close()


@mcp.tool()
def read_observations(
    signal_id: str,
    ticker: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return recent observations for a signal, newest first.

    Args:
        signal_id: 'insider.trades.raw' or 'insider.trades.raw.v1' — LIKE-matched
        ticker: optional ticker filter (case-insensitive)
        from_date / to_date: optional as_of_date window (YYYY-MM-DD inclusive)
        limit: cap rows; max 1000

    Each row carries the full value JSON plus as_of_date, ticker,
    confidence, ingested_at, metadata. The value shape varies per
    signal — call get_signal first to see the output_schema."""
    sql = """
        SELECT signal_id, ticker, as_of_date, value, confidence, ingested_at, metadata
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{signal_id}%"]
    if ticker:
        sql += " AND ticker = %s"
        params.append(ticker.upper())
    if from_date:
        sql += " AND as_of_date >= %s::date"
        params.append(from_date)
    if to_date:
        sql += " AND as_of_date < (%s::date + INTERVAL '1 day')"
        params.append(to_date)
    sql += " ORDER BY as_of_date DESC LIMIT %s"
    params.append(min(int(limit), 1000))

    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            return _rows(cur)
        finally:
            cur.close()


@mcp.tool()
def read_strategy_evaluations(
    strategy_id: str,
    ticker: Optional[str] = None,
    outcome: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Return strategy evaluation tape — every trigger this strategy
    considered, whether it triggered or was suppressed, and why.

    Args:
        strategy_id: 'strategy.<name>' (e.g. strategy.agrade_drawdown_buy)
        ticker: optional ticker filter
        outcome: 'triggered' | 'suppressed' | None (any)
        from_date / to_date: optional as_of window
        limit: cap rows; max 2000

    Returns each row's full evaluation payload: triggered flag, the
    trigger event itself, per-gate pass/fail with reasons, suppression
    fail_reason. Use this for backtest-style answers like 'what would
    this strategy have fired for NVDA in Q2?'"""
    sql = """
        SELECT ticker, as_of_date, value, ingested_at
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{strategy_id}%"]
    if ticker:
        sql += " AND ticker = %s"
        params.append(ticker.upper())
    if from_date:
        sql += " AND as_of_date >= %s::date"
        params.append(from_date)
    if to_date:
        sql += " AND as_of_date < (%s::date + INTERVAL '1 day')"
        params.append(to_date)
    if outcome == "triggered":
        sql += " AND (value->>'triggered')::boolean = true"
    elif outcome == "suppressed":
        sql += " AND (value->>'triggered')::boolean = false"
    sql += " ORDER BY as_of_date DESC LIMIT %s"
    params.append(min(int(limit), 2000))

    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            return _rows(cur)
        finally:
            cur.close()


@mcp.tool()
def ticker_snapshot(ticker: str) -> Dict[str, Any]:
    """Cross-signal view of one ticker: every signal that has any
    observation for this ticker, with its latest value.

    Pyrrho's 'what do we know about NVDA right now?' tool. Returns
    a list of {signal_id, latest_as_of, latest_value, row_count}."""
    sql = """
        WITH ranked AS (
            SELECT signal_id, as_of_date, value,
                   row_number() OVER (PARTITION BY signal_id ORDER BY as_of_date DESC) AS rk,
                   COUNT(*) OVER (PARTITION BY signal_id) AS row_count
              FROM signal_observations
             WHERE ticker = %s
        )
        SELECT signal_id, as_of_date, value, row_count
          FROM ranked
         WHERE rk = 1
         ORDER BY signal_id
    """
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            rows = _rows(cur)
        finally:
            cur.close()
    return {"ticker": ticker.upper(), "signals": rows}


@mcp.tool()
def fail_reason_distribution(
    strategy_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Histogram of why a strategy's evaluations did or didn't fire,
    grouped by outcome reason (or '(triggered)' for the fired ones).

    Useful for tuning a strategy: which gate is suppressing the most
    candidates?"""
    sql = """
        SELECT COALESCE(value->>'fail_reason', '(triggered)') AS reason,
               COUNT(*) AS count
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{strategy_id}%"]
    if from_date:
        sql += " AND as_of_date >= %s::date"
        params.append(from_date)
    if to_date:
        sql += " AND as_of_date < (%s::date + INTERVAL '1 day')"
        params.append(to_date)
    sql += " GROUP BY reason ORDER BY count DESC"
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            return _rows(cur)
        finally:
            cur.close()


@mcp.tool()
def health() -> Dict[str, Any]:
    """One-shot data-plane health: per-signal row count + latest ingest
    + freshness status vs SLA. Equivalent to the Pyrrho Desk Home page."""
    sql = """
        SELECT d.signal_id, d.version, d.signal_class::text,
               d.sla_hours,
               COALESCE(SUM(o.row_count), 0)::bigint AS row_count,
               MAX(o.latest_ingested_at)             AS latest_ingested_at,
               MAX(o.latest_as_of)                   AS latest_as_of
          FROM signal_definitions d
          LEFT JOIN (
            SELECT signal_id,
                   COUNT(*) AS row_count,
                   MAX(ingested_at) AS latest_ingested_at,
                   MAX(as_of_date) AS latest_as_of
              FROM signal_observations
             GROUP BY signal_id
          ) o ON o.signal_id LIKE d.signal_id || '%'
         WHERE d.status = 'active'
         GROUP BY d.signal_id, d.version, d.signal_class, d.sla_hours
         ORDER BY d.signal_class, d.signal_id
    """
    out = []
    now = datetime.now(timezone.utc)
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            for row in cur.fetchall():
                d = {
                    "signal_id":          row[0],
                    "version":            row[1],
                    "signal_class":       row[2],
                    "sla_hours":          float(row[3]),
                    "row_count":          int(row[4]),
                    "latest_ingested_at": row[5].isoformat() if row[5] else None,
                    "latest_as_of":       row[6].isoformat() if row[6] else None,
                }
                if row[5]:
                    latest = row[5] if row[5].tzinfo else row[5].replace(tzinfo=timezone.utc)
                    age_hours = (now - latest).total_seconds() / 3600.0
                    d["age_hours"] = round(age_hours, 1)
                    d["freshness_status"] = (
                        "GREEN" if age_hours < float(row[3])
                        else "YELLOW" if age_hours < 2 * float(row[3])
                        else "RED"
                    )
                else:
                    d["age_hours"] = None
                    d["freshness_status"] = "UNKNOWN"
                out.append(d)
        finally:
            cur.close()
    return {"as_of": now.isoformat(), "signals": out}


# ── Entry point ────────────────────────────────────────────────────────

def main():
    mcp.run()


if __name__ == "__main__":
    main()
