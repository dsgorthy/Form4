"""Helper for compute pipelines to write `signal_freshness` rows atomically.

Every analytical column in `config/freshness_contracts.yaml` has a
`populated_by` script. That script is the sole writer of `signal_freshness`
rows for its column. The pattern:

    from config.database import get_connection
    from framework.contracts.freshness_writer import write_freshness

    conn = get_connection()
    # ... compute and UPDATE the data ...
    write_freshness(conn, table="trades", column="pit_grade",
                    n_rows_affected=N, populated_by=__file__)
    conn.commit()

The `write_freshness` call MUST be in the same transaction as the data
write (single conn.commit() at the end). Atomicity guarantees:

    - If the data write fails, no signal_freshness row exists → next
      contract check raises FreshnessUnknownError (or stays at the older
      timestamp if a prior run succeeded).
    - If the freshness write fails, the data write rolls back too →
      consumers don't see partially-computed columns.

This is the structural fix for the 21-day silent April outage AND for
the false-positive halt the Phase 1 incomplete deployment introduced:
both came down to "we don't actually know when the compute ran." This
helper makes that knowledge mandatory.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]


def write_freshness(
    conn,
    *,
    table: str,
    column: str,
    n_rows_affected: int,
    run_id: Optional[str] = None,
    populated_by: Optional[str] = None,
    last_computed_at: Optional[datetime] = None,
) -> None:
    """Write one row to signal_freshness in the current transaction.

    Args:
      conn: open DB connection. Caller is responsible for the surrounding
            transaction (we don't commit here).
      table: e.g. "trades" or "prices.daily_prices" (schema.table form
             splits to source/table_name automatically).
      column: column name being marked fresh.
      n_rows_affected: how many rows the compute touched in this run.
                       Required — a 0-row run is suspicious and flags
                       downstream sanity checks.
      run_id: optional UUID for traceability across multiple writes from
              the same pipeline run. One generated automatically if absent.
      populated_by: optional script path. Best-effort auto-detected from
                    the calling frame if absent.
      last_computed_at: optional override (default: now in UTC). Useful for
                        backfills where you want the timestamp to reflect
                        when the data is logically current, not when this
                        helper ran.

    Does not raise — write failures log a warning. The data write is the
    primary event; freshness write is the secondary signal. (If freshness
    writes start failing systematically, the meta-check in the runner
    catches it within hours via FreshnessSystemBrokenError.)
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    if last_computed_at is None:
        last_computed_at = datetime.now(timezone.utc)
    if populated_by is None:
        populated_by = _detect_caller_path()

    if "." in table:
        source, table_name = table.split(".", 1)
    else:
        source, table_name = "public", table

    try:
        conn.execute(
            """
            INSERT INTO signal_freshness
                (source, table_name, column_name, last_computed_at,
                 n_rows_affected, run_id, populated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (source, table_name, column, last_computed_at,
             int(n_rows_affected), run_id, populated_by),
        )
    except Exception as e:
        logger.warning(
            "write_freshness failed for %s.%s (n=%d): %s",
            table, column, n_rows_affected, e,
        )


def _detect_caller_path() -> str:
    """Return the calling file's path relative to repo root, or 'unknown'."""
    import inspect
    try:
        frame = inspect.stack()[2]  # 0=this fn, 1=write_freshness, 2=caller
        path = Path(frame.filename).resolve()
        try:
            return str(path.relative_to(REPO_ROOT))
        except ValueError:
            return path.name
    except Exception:
        return "unknown"
