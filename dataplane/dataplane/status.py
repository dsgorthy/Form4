"""Pyrrho Dataplane Desk — operator health view.

Single-pane status: per-signal rows/freshness/SLA, per-strategy outcomes,
recent Dagster runs. Pure data layer. The CLI (`python3 -m dataplane
status`) and the upcoming browser dashboard both call gather_status()
and render the same StatusSnapshot.

Sources:
  - pyrrho_data_dev.signal_definitions    (catalog)
  - pyrrho_data_dev.signal_observations   (rows, ingest, as_of)
  - dagster_runs.runs                     (recent pipeline runs)
"""
from __future__ import annotations

import json
import os
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg2


# ── Connection helpers ─────────────────────────────────────────────────

@contextmanager
def _dataplane_conn():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


@contextmanager
def _dagster_conn():
    dsn = os.environ.get(
        "DAGSTER_RUNS_DSN", "dbname=dagster_runs host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


# ── Data shapes ────────────────────────────────────────────────────────

@dataclass
class SignalStatus:
    signal_id: str
    version: str
    signal_class: str
    owner: str
    sla_hours: float
    row_count: int = 0
    rows_24h: int = 0
    rows_7d: int = 0
    latest_ingested_at: Optional[datetime] = None
    latest_as_of: Optional[datetime] = None
    earliest_as_of: Optional[datetime] = None

    @property
    def is_strategy(self) -> bool:
        return self.signal_class == "composite"

    @property
    def freshness_status(self) -> str:
        """GREEN / YELLOW / RED / UNKNOWN based on last ingest vs SLA."""
        if not self.latest_ingested_at:
            return "UNKNOWN"
        now = datetime.now(timezone.utc)
        latest = self.latest_ingested_at
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age = (now - latest).total_seconds() / 3600.0  # hours
        sla = self.sla_hours
        if age < sla:
            return "GREEN"
        if age < 2 * sla:
            return "YELLOW"
        return "RED"

    @property
    def age_hours(self) -> Optional[float]:
        if not self.latest_ingested_at:
            return None
        latest = self.latest_ingested_at
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0


@dataclass
class StrategyOutcomes:
    """For composite signals: a roll-up of triggered vs suppressed."""
    evals_24h: int = 0
    evals_7d: int = 0
    triggered_24h: int = 0
    triggered_7d: int = 0
    top_fail_reason: Optional[str] = None
    top_fail_count: int = 0


@dataclass
class DagsterRun:
    job_name: str
    partition: Optional[str]
    status: str
    started_at: Optional[datetime]
    duration_seconds: Optional[float]


@dataclass
class StatusSnapshot:
    """Top-level snapshot returned by gather_status()."""
    as_of: datetime
    signals: List[SignalStatus] = field(default_factory=list)
    strategies: dict = field(default_factory=dict)  # signal_id → StrategyOutcomes
    recent_runs: List[DagsterRun] = field(default_factory=list)

    @property
    def healthy_pipelines(self) -> int:
        # Count of non-composite signals in GREEN status.
        return sum(
            1 for s in self.signals
            if not s.is_strategy and s.freshness_status == "GREEN"
        )

    @property
    def non_strategy_count(self) -> int:
        return sum(1 for s in self.signals if not s.is_strategy)

    @property
    def total_evals_24h(self) -> int:
        return sum(o.evals_24h for o in self.strategies.values())

    @property
    def total_triggered_24h(self) -> int:
        return sum(o.triggered_24h for o in self.strategies.values())


# ── Queries ────────────────────────────────────────────────────────────

_SIGNAL_QUERY = """
WITH d AS (
    SELECT signal_id, version, signal_class::text AS signal_class,
           owner, sla_hours
      FROM signal_definitions
     WHERE status = 'active'
),
o AS (
    SELECT  split_part(signal_id, '.v', 1) AS sid_base,
            signal_id,
            COUNT(*)                                                 AS row_count,
            COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '24 hours')  AS rows_24h,
            COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '7 days')    AS rows_7d,
            MAX(ingested_at)                                         AS latest_ingested_at,
            MAX(as_of_date)                                          AS latest_as_of,
            MIN(as_of_date)                                          AS earliest_as_of
      FROM signal_observations
     GROUP BY signal_id
)
SELECT d.signal_id, d.version, d.signal_class, d.owner, d.sla_hours,
       COALESCE(SUM(o.row_count), 0)::bigint AS row_count,
       COALESCE(SUM(o.rows_24h), 0)::bigint  AS rows_24h,
       COALESCE(SUM(o.rows_7d), 0)::bigint   AS rows_7d,
       MAX(o.latest_ingested_at)             AS latest_ingested_at,
       MAX(o.latest_as_of)                   AS latest_as_of,
       MIN(o.earliest_as_of)                 AS earliest_as_of
  FROM d
  LEFT JOIN o ON o.signal_id LIKE d.signal_id || '%'
 GROUP BY d.signal_id, d.version, d.signal_class, d.owner, d.sla_hours
 ORDER BY d.signal_class, d.signal_id
"""


_STRATEGY_OUTCOMES_QUERY = """
WITH x AS (
    SELECT signal_id,
           (value->>'triggered')::boolean      AS triggered,
           value->>'fail_reason'               AS fail_reason,
           ingested_at
      FROM signal_observations
     WHERE signal_id LIKE %s
       AND ingested_at > now() - INTERVAL '7 days'
)
SELECT
    COUNT(*) FILTER (WHERE ingested_at > now() - INTERVAL '24 hours') AS evals_24h,
    COUNT(*)                                                          AS evals_7d,
    COUNT(*) FILTER (WHERE triggered AND ingested_at > now() - INTERVAL '24 hours') AS triggered_24h,
    COUNT(*) FILTER (WHERE triggered)                                 AS triggered_7d
  FROM x
"""


_STRATEGY_TOP_FAIL_QUERY = """
SELECT value->>'fail_reason' AS reason, COUNT(*) AS n
  FROM signal_observations
 WHERE signal_id LIKE %s
   AND ingested_at > now() - INTERVAL '7 days'
   AND (value->>'triggered')::boolean = false
   AND value->>'fail_reason' IS NOT NULL
 GROUP BY 1
 ORDER BY 2 DESC
 LIMIT 1
"""


_RECENT_RUNS_QUERY = """
SELECT pipeline_name,
       partition,
       status,
       to_timestamp(start_time)            AS started_at,
       (end_time - start_time)::float      AS duration_seconds
  FROM runs
 ORDER BY create_timestamp DESC
 LIMIT %s
"""


# ── Top-level gather ───────────────────────────────────────────────────

def gather_status(recent_runs_limit: int = 8) -> StatusSnapshot:
    """One-shot query for the entire dashboard view."""
    snapshot = StatusSnapshot(as_of=datetime.now(timezone.utc))

    with _dataplane_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(_SIGNAL_QUERY)
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                s = SignalStatus(
                    signal_id=d["signal_id"],
                    version=d["version"],
                    signal_class=d["signal_class"],
                    owner=d["owner"],
                    sla_hours=float(d["sla_hours"]),
                    row_count=int(d["row_count"]),
                    rows_24h=int(d["rows_24h"]),
                    rows_7d=int(d["rows_7d"]),
                    latest_ingested_at=d["latest_ingested_at"],
                    latest_as_of=d["latest_as_of"],
                    earliest_as_of=d["earliest_as_of"],
                )
                snapshot.signals.append(s)
        finally:
            cur.close()

        # Per-strategy outcomes
        for s in snapshot.signals:
            if not s.is_strategy:
                continue
            outcomes = StrategyOutcomes()
            cur = conn.cursor()
            try:
                pattern = f"{s.signal_id}%"
                cur.execute(_STRATEGY_OUTCOMES_QUERY, (pattern,))
                row = cur.fetchone()
                if row:
                    outcomes.evals_24h = int(row[0] or 0)
                    outcomes.evals_7d = int(row[1] or 0)
                    outcomes.triggered_24h = int(row[2] or 0)
                    outcomes.triggered_7d = int(row[3] or 0)
                cur.execute(_STRATEGY_TOP_FAIL_QUERY, (pattern,))
                top = cur.fetchone()
                if top:
                    outcomes.top_fail_reason = top[0]
                    outcomes.top_fail_count = int(top[1] or 0)
            finally:
                cur.close()
            snapshot.strategies[s.signal_id] = outcomes

    # Recent Dagster runs — separate DB.
    try:
        with _dagster_conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(_RECENT_RUNS_QUERY, (recent_runs_limit,))
                for pipeline_name, partition, status, started_at, duration in cur.fetchall():
                    snapshot.recent_runs.append(DagsterRun(
                        job_name=pipeline_name or "(unknown)",
                        partition=partition,
                        status=(status or "").upper(),
                        started_at=started_at,
                        duration_seconds=float(duration) if duration is not None else None,
                    ))
            finally:
                cur.close()
    except Exception:
        # Dagster DB unreachable — render what we have.
        pass

    return snapshot


# ── Terminal renderer ──────────────────────────────────────────────────

def _badge(status: str) -> str:
    return {
        "GREEN": "  OK ",
        "YELLOW": " WARN",
        "RED": "STALE",
        "UNKNOWN": "  ?  ",
    }.get(status, "  ?  ")


def _age(td_hours: Optional[float]) -> str:
    if td_hours is None:
        return "—"
    if td_hours < 1:
        return f"{int(td_hours*60)}m"
    if td_hours < 48:
        return f"{td_hours:.1f}h"
    return f"{td_hours/24:.1f}d"


def _ts(d: Optional[datetime]) -> str:
    if d is None:
        return "—"
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone().strftime("%Y-%m-%d %H:%M")


def render_terminal(snap: StatusSnapshot) -> str:
    lines: List[str] = []
    bar = "─" * 78
    lines.append(bar)
    title = "Pyrrho · Dataplane Desk"
    ts = snap.as_of.astimezone().strftime("%a %Y-%m-%d %H:%M %Z")
    lines.append(f"  {title}{' ' * (78 - len(title) - len(ts) - 4)}{ts}")
    lines.append(bar)

    # Header
    healthy = snap.healthy_pipelines
    total = snap.non_strategy_count
    lines.append(
        f"  pipelines healthy:  {healthy}/{total}    "
        f"evals 24h: {snap.total_evals_24h:,}    "
        f"triggered 24h: {snap.total_triggered_24h:,}"
    )
    lines.append(bar)

    # Signals (non-strategy)
    raw_signals = [s for s in snap.signals if not s.is_strategy]
    if raw_signals:
        lines.append(
            f"  SIGNALS                            rows      24h     7d   age   SLA"
        )
        for s in raw_signals:
            badge = _badge(s.freshness_status)
            lines.append(
                f"  {badge}  {s.signal_id:<28} {s.row_count:>8,} {s.rows_24h:>6,} "
                f"{s.rows_7d:>6,} {_age(s.age_hours):>5} {int(s.sla_hours):>4}h"
            )
        lines.append(bar)

    # Strategies
    strats = [s for s in snap.signals if s.is_strategy]
    if strats:
        lines.append(
            f"  STRATEGIES                       evals     trig    age   top miss"
        )
        for s in strats:
            o = snap.strategies.get(s.signal_id, StrategyOutcomes())
            top = (o.top_fail_reason or "")[:30]
            badge = _badge(s.freshness_status)
            lines.append(
                f"  {badge}  {s.signal_id:<28} {o.evals_24h:>8,} {o.triggered_24h:>6,} "
                f"{_age(s.age_hours):>5}  {top}"
            )
        lines.append(bar)

    # Recent runs
    if snap.recent_runs:
        lines.append(
            f"  RECENT RUNS                            partition          dur"
        )
        for r in snap.recent_runs:
            status_mark = {
                "SUCCESS": " ✓", "STARTED": " ●", "FAILURE": " ✗",
                "CANCELED": " -", "QUEUED": " …",
            }.get(r.status, "  ")
            part = (r.partition or "—")[:18]
            dur = f"{int(r.duration_seconds)}s" if r.duration_seconds else "—"
            started = _ts(r.started_at)
            lines.append(
                f"  {status_mark}  {r.job_name:<22} {started:<17} {part:<18} {dur:>5}"
            )
        lines.append(bar)

    return "\n".join(lines)
