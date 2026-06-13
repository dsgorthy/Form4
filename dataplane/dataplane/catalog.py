"""Signal catalog — register signals, write observations, query the catalog.

A Signal subclass is metadata + a compute function. To actually run it, the
catalog needs to know about it (signal_definitions row) and writes go to
signal_observations. This module wraps both.

Typical lifecycle:

    from dataplane.catalog import register, write_observation
    from signals.insider.career_grade_v3 import CareerGradeV3

    # Once per deploy:
    with get_dev_conn() as conn:
        register(conn, CareerGradeV3)

    # Each compute:
    signal = CareerGradeV3(conn=conn)
    obs = signal.compute("AAPL", as_of)
    write_observation(conn, obs)
"""
from __future__ import annotations

import json
from typing import Type

from dataplane.observation import SignalObservation
from dataplane.signal import Signal


def register(conn, signal_cls: Type[Signal]) -> None:
    """Upsert a Signal class into signal_definitions.

    Idempotent — same (signal_id, version) is updated, not duplicated.
    Status defaults to 'active' on first registration; subsequent
    registrations preserve whatever status is already there (so a
    deprecated v2 stays deprecated when re-registered).
    """
    if not signal_cls.signal_id or not signal_cls.version:
        raise ValueError(
            f"{signal_cls.__name__} cannot register: missing signal_id/version"
        )

    # Either an explicit class attribute (strategies set 'composite') or the
    # first dot-segment of the signal_id ('insider.career_grade' → 'insider').
    signal_class_token = (
        getattr(signal_cls, "signal_class_override", None)
        or signal_cls.signal_id.split(".", 1)[0]
    )
    upstream_json = json.dumps([u.to_json() for u in signal_cls.upstream])
    output_schema_json = json.dumps(signal_cls.output_schema)

    sql = """
        INSERT INTO signal_definitions (
            signal_id, version, signal_class, description, owner,
            output_schema, upstream, sla_hours, business_hours_only
        ) VALUES (
            %s, %s, %s::signal_class, %s, %s,
            %s::jsonb, %s::jsonb, %s, %s
        )
        ON CONFLICT (signal_id, version) DO UPDATE SET
            description         = EXCLUDED.description,
            owner               = EXCLUDED.owner,
            output_schema       = EXCLUDED.output_schema,
            upstream            = EXCLUDED.upstream,
            sla_hours           = EXCLUDED.sla_hours,
            business_hours_only = EXCLUDED.business_hours_only,
            last_modified_at    = now()
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (
            signal_cls.signal_id,
            signal_cls.version,
            signal_class_token,
            signal_cls.description,
            signal_cls.owner,
            output_schema_json,
            upstream_json,
            float(signal_cls.sla_hours),
            bool(signal_cls.business_hours_only),
        ))
        conn.commit()
    finally:
        cur.close()


def write_observation(conn, obs: SignalObservation) -> None:
    """Upsert a SignalObservation into signal_observations.

    Idempotent on (signal_id, ticker, as_of_date). Re-running a backfill
    overwrites the previous value and bumps ingested_at, which is the
    correct behavior — the new computation is the truth.
    """
    sql = """
        INSERT INTO signal_observations (
            signal_id, ticker, as_of_date, value, confidence,
            source_run_id, metadata
        ) VALUES (
            %s, %s, %s, %s::jsonb, %s,
            %s, %s::jsonb
        )
        ON CONFLICT (signal_id, ticker, as_of_date) DO UPDATE SET
            value         = EXCLUDED.value,
            confidence    = EXCLUDED.confidence,
            source_run_id = EXCLUDED.source_run_id,
            ingested_at   = now(),
            metadata      = EXCLUDED.metadata
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (
            obs.signal_id,
            obs.ticker,
            obs.as_of_date,
            json.dumps(obs.value),
            obs.confidence,
            str(obs.source_run_id),
            json.dumps(obs.metadata),
        ))
        conn.commit()
    finally:
        cur.close()


def list_active(conn, signal_class: str | None = None) -> list[dict]:
    """Return active signal_definitions rows. Filter by class if given."""
    sql = """
        SELECT signal_id, version, signal_class::text, description, owner,
               sla_hours, business_hours_only, registered_at
          FROM signal_definitions
         WHERE status = 'active'
    """
    params: tuple = ()
    if signal_class:
        sql += " AND signal_class = %s::signal_class"
        params = (signal_class,)
    sql += " ORDER BY signal_class, signal_id, version"

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
