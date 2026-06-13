"""SQL helpers for the desk pages.

Reads pyrrho_data_dev (signal_definitions, signal_observations) +
dagster_runs (runs, event_logs). Read-only. All paginated queries cap at
sensible defaults so a misbehaving filter can't lock up the page.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2


# ── Connection helpers ─────────────────────────────────────────────────

@contextmanager
def dataplane_conn():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


@contextmanager
def dagster_conn():
    dsn = os.environ.get(
        "DAGSTER_RUNS_DSN", "dbname=dagster_runs host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


# ── Catalog ────────────────────────────────────────────────────────────

@dataclass
class SignalRow:
    signal_id: str
    version: str
    signal_class: str
    description: str
    owner: str
    sla_hours: float
    business_hours_only: bool
    upstream: List[dict]
    output_schema: dict
    registered_at: Optional[datetime]
    last_modified_at: Optional[datetime]
    # Rollup from observations:
    row_count: int = 0
    latest_ingested_at: Optional[datetime] = None
    latest_as_of: Optional[datetime] = None
    earliest_as_of: Optional[datetime] = None
    rows_24h: int = 0
    rows_7d: int = 0

    @property
    def is_strategy(self) -> bool:
        return self.signal_class == "composite"

    @property
    def age_hours(self) -> Optional[float]:
        if not self.latest_ingested_at:
            return None
        latest = self.latest_ingested_at
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0

    @property
    def freshness_status(self) -> str:
        age = self.age_hours
        if age is None:
            return "UNKNOWN"
        if age < self.sla_hours:
            return "GREEN"
        if age < 2 * self.sla_hours:
            return "YELLOW"
        return "RED"


_CATALOG_QUERY = """
WITH d AS (
    SELECT signal_id, version, signal_class::text AS signal_class,
           description, owner, sla_hours, business_hours_only,
           upstream, output_schema, registered_at, last_modified_at
      FROM signal_definitions
     WHERE status = 'active'
),
o AS (
    SELECT signal_id,
           COUNT(*)                                                       AS row_count,
           MAX(ingested_at)                                               AS latest_ingested_at,
           MAX(as_of_date)                                                AS latest_as_of,
           MIN(as_of_date)                                                AS earliest_as_of,
           COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '24 hours') AS rows_24h,
           COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '7 days')   AS rows_7d
      FROM signal_observations
     GROUP BY signal_id
)
SELECT d.signal_id, d.version, d.signal_class, d.description, d.owner,
       d.sla_hours, d.business_hours_only, d.upstream::text,
       d.output_schema::text, d.registered_at, d.last_modified_at,
       COALESCE(SUM(o.row_count), 0)::bigint  AS row_count,
       MAX(o.latest_ingested_at)              AS latest_ingested_at,
       MAX(o.latest_as_of)                    AS latest_as_of,
       MIN(o.earliest_as_of)                  AS earliest_as_of,
       COALESCE(SUM(o.rows_24h), 0)::bigint   AS rows_24h,
       COALESCE(SUM(o.rows_7d), 0)::bigint    AS rows_7d
  FROM d
  LEFT JOIN o ON o.signal_id LIKE d.signal_id || '%'
 GROUP BY d.signal_id, d.version, d.signal_class, d.description, d.owner,
          d.sla_hours, d.business_hours_only, d.upstream::text,
          d.output_schema::text, d.registered_at, d.last_modified_at
 ORDER BY d.signal_class, d.signal_id
"""


def all_signals() -> List[SignalRow]:
    out: List[SignalRow] = []
    with dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(_CATALOG_QUERY)
            for row in cur.fetchall():
                out.append(SignalRow(
                    signal_id=row[0], version=row[1], signal_class=row[2],
                    description=row[3] or "", owner=row[4],
                    sla_hours=float(row[5]),
                    business_hours_only=bool(row[6]),
                    upstream=json.loads(row[7] or "[]"),
                    output_schema=json.loads(row[8] or "{}"),
                    registered_at=row[9], last_modified_at=row[10],
                    row_count=int(row[11]),
                    latest_ingested_at=row[12],
                    latest_as_of=row[13], earliest_as_of=row[14],
                    rows_24h=int(row[15]), rows_7d=int(row[16]),
                ))
        finally:
            cur.close()
    return out


def get_signal(signal_id: str) -> Optional[SignalRow]:
    """Same shape as all_signals() but for one id."""
    for s in all_signals():
        if s.signal_id == signal_id:
            return s
    return None


# ── Observations ───────────────────────────────────────────────────────

@dataclass
class Observation:
    signal_id: str
    ticker: str
    as_of_date: datetime
    value: dict
    confidence: Optional[float]
    ingested_at: datetime
    metadata: dict


def recent_observations(
    signal_id: str,
    ticker: Optional[str] = None,
    limit: int = 50,
) -> List[Observation]:
    sql = """
        SELECT signal_id, ticker, as_of_date, value, confidence,
               ingested_at, metadata
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{signal_id}%"]
    if ticker:
        sql += " AND ticker = %s"
        params.append(ticker.upper())
    sql += " ORDER BY as_of_date DESC LIMIT %s"
    params.append(min(int(limit), 500))

    out: List[Observation] = []
    with dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            for row in cur.fetchall():
                out.append(Observation(
                    signal_id=row[0], ticker=row[1], as_of_date=row[2],
                    value=row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}"),
                    confidence=row[4],
                    ingested_at=row[5],
                    metadata=row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}"),
                ))
        finally:
            cur.close()
    return out


# ── Strategy eval tape ────────────────────────────────────────────────

@dataclass
class StratEval:
    ticker: str
    as_of_date: datetime
    triggered: bool
    fail_reason: Optional[str]
    trigger_value: dict
    gates: List[dict]
    ingested_at: datetime


def strategy_evaluations(
    signal_id: str,
    ticker: Optional[str] = None,
    outcome: Optional[str] = None,   # 'triggered' | 'suppressed'
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 100,
) -> List[StratEval]:
    sql = """
        SELECT ticker, as_of_date, value, ingested_at
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
    if outcome == "triggered":
        sql += " AND (value->>'triggered')::boolean = true"
    elif outcome == "suppressed":
        sql += " AND (value->>'triggered')::boolean = false"
    sql += " ORDER BY as_of_date DESC LIMIT %s"
    params.append(min(int(limit), 1000))

    out: List[StratEval] = []
    with dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            for row in cur.fetchall():
                v = row[2] if isinstance(row[2], dict) else json.loads(row[2] or "{}")
                out.append(StratEval(
                    ticker=row[0], as_of_date=row[1],
                    triggered=bool(v.get("triggered")),
                    fail_reason=v.get("fail_reason"),
                    trigger_value=v.get("trigger_value") or {},
                    gates=v.get("gates") or [],
                    ingested_at=row[3],
                ))
        finally:
            cur.close()
    return out


def fail_reason_distribution(
    signal_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> List[Tuple[str, int]]:
    sql = """
        SELECT COALESCE(value->>'fail_reason', '(triggered)') AS reason,
               COUNT(*)
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{signal_id}%"]
    if from_date:
        sql += " AND as_of_date >= %s::date"
        params.append(from_date)
    if to_date:
        sql += " AND as_of_date < (%s::date + INTERVAL '1 day')"
        params.append(to_date)
    sql += " GROUP BY reason ORDER BY 2 DESC LIMIT 20"
    out: List[Tuple[str, int]] = []
    with dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            for row in cur.fetchall():
                out.append((row[0], int(row[1])))
        finally:
            cur.close()
    return out


# ── Ticker view ───────────────────────────────────────────────────────

@dataclass
class TickerRow:
    signal_id: str
    latest_as_of: datetime
    latest_value: dict
    row_count: int


def ticker_summary(ticker: str) -> List[TickerRow]:
    """For one ticker, return the most recent value of every signal that
    has any observation for it."""
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
    out: List[TickerRow] = []
    with dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            for row in cur.fetchall():
                v = row[2] if isinstance(row[2], dict) else json.loads(row[2] or "{}")
                out.append(TickerRow(
                    signal_id=row[0], latest_as_of=row[1],
                    latest_value=v, row_count=int(row[3]),
                ))
        finally:
            cur.close()
    return out


# ── Pipelines (Dagster) ────────────────────────────────────────────────

@dataclass
class RunRow:
    run_id: str
    job_name: str
    partition: Optional[str]
    status: str
    started_at: Optional[datetime]
    duration_seconds: Optional[float]


def recent_runs(limit: int = 25) -> List[RunRow]:
    out: List[RunRow] = []
    try:
        with dagster_conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT run_id, pipeline_name, partition, status,
                           to_timestamp(start_time),
                           (end_time - start_time)::float
                      FROM runs
                     ORDER BY create_timestamp DESC
                     LIMIT %s
                    """,
                    (limit,),
                )
                for row in cur.fetchall():
                    out.append(RunRow(
                        run_id=row[0] or "?",
                        job_name=row[1] or "(unknown)",
                        partition=row[2],
                        status=(row[3] or "").upper(),
                        started_at=row[4],
                        duration_seconds=float(row[5]) if row[5] is not None else None,
                    ))
            finally:
                cur.close()
    except Exception:
        pass
    return out


def run_failure_message(run_id: str) -> Optional[str]:
    """Pull the most recent STEP_FAILURE event body for a run."""
    try:
        with dagster_conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT event
                      FROM event_logs
                     WHERE run_id = %s
                       AND dagster_event_type = 'STEP_FAILURE'
                     ORDER BY id DESC LIMIT 1
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                blob = row[0]
                # Best-effort extract: pull "message" from the JSON.
                try:
                    parsed = json.loads(blob)
                    err = (parsed.get("dagster_event") or {}).get("event_specific_data") or {}
                    e = err.get("error") or {}
                    cause = (e.get("cause") or {}).get("message") or ""
                    msg = e.get("message") or ""
                    return (cause + "\n" + msg).strip()
                except Exception:
                    return str(blob)[:400]
            finally:
                cur.close()
    except Exception:
        return None
