"""pipeline_run() context manager — records a row in pipeline_runs per invocation.

Convention mirrors framework.oms.audit: we manage our own connection here
(unlike the audit writers) because pipeline_runs is a system-of-record for
when things ran, decoupled from whatever DB connections the job itself uses.
A failure in the job should not prevent us from recording the failure.
"""
from __future__ import annotations

import json
import socket
import time
import traceback
import uuid
from contextlib import contextmanager
from typing import Any, Optional

from config.database import get_connection


class RunState:
    """Mutable handle passed to the with-block. Job code can attach metadata
    and row counts that will be persisted when the context manager exits."""

    def __init__(self):
        self.run_id: Optional[int] = None
        self.run_uuid: Optional[str] = None
        self.metadata: dict = {}
        self.rows_written: Optional[int] = None
        self.rows_deleted: Optional[int] = None

    def set_metadata(self, m: dict) -> None:
        self.metadata.update(m or {})

    def set_rows_written(self, n: int) -> None:
        self.rows_written = int(n) if n is not None else None

    def set_rows_deleted(self, n: int) -> None:
        self.rows_deleted = int(n) if n is not None else None


@contextmanager
def pipeline_run(
    service: str,
    log_path: Optional[str] = None,
    host: Optional[str] = None,
):
    """Records a pipeline run start/end with status, duration, and metadata.

    On exception: marks the run failed, captures the message + stack, then
    re-raises so the caller still sees the error. The pipeline_runs row
    itself is committed before re-raising so a crash doesn't lose telemetry.

    Args:
        service: short identifier for the job (matches launchd label suffix).
        log_path: where stdout/stderr is going (for /admin to deep-link to).
        host: defaults to socket.gethostname().

    Yields:
        RunState. Mutate `.metadata`, `.rows_written`, `.rows_deleted` from
        within the with-block; they get persisted on exit.
    """
    state = RunState()
    state.run_uuid = str(uuid.uuid4())  # caller-generated so we can avoid
                                        # multi-column RETURNING; the
                                        # compat-layer cursor auto-consumes
                                        # RETURNING into lastrowid (only one
                                        # column survives) — see backfill.py
                                        # for the established pattern.
    conn = get_connection()
    host = host or socket.gethostname()

    cur = conn.execute(
        """INSERT INTO pipeline_runs (service, host, log_path, status, run_uuid)
           VALUES (?, ?, ?, 'running', ?::uuid)
           RETURNING id""",
        (service, host, log_path, state.run_uuid),
    )
    state.run_id = cur.lastrowid
    conn.commit()

    t0 = time.monotonic()
    try:
        yield state
    except BaseException as exc:
        # Catch BaseException (not just Exception) so SystemExit / KeyboardInterrupt
        # also get recorded — important for the wrap.py launchd helper which
        # propagates a wrapped command's nonzero exit via SystemExit.
        duration_ms = int((time.monotonic() - t0) * 1000)
        # SystemExit's "code" is the exit code; preserve it where possible.
        exit_code = getattr(exc, "code", 1)
        if not isinstance(exit_code, int):
            exit_code = 1
        if exit_code == 0:
            # SystemExit(0) is a normal clean exit — record as ok.
            status = "ok"
            err_msg = None
        else:
            status = "failed"
            err_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        conn.execute(
            """UPDATE pipeline_runs
               SET ended_at = NOW(), status = ?, exit_code = ?,
                   duration_ms = ?, error_message = ?,
                   rows_written = ?, rows_deleted = ?, metadata = ?::jsonb
               WHERE id = ?""",
            (status, exit_code, duration_ms,
             err_msg[:4000] if err_msg else None,
             state.rows_written, state.rows_deleted,
             json.dumps(state.metadata or {}),
             state.run_id),
        )
        conn.commit()
        conn.close()
        raise

    duration_ms = int((time.monotonic() - t0) * 1000)
    conn.execute(
        """UPDATE pipeline_runs
           SET ended_at = NOW(), status = 'ok', exit_code = 0,
               duration_ms = ?, rows_written = ?, rows_deleted = ?,
               metadata = ?::jsonb
           WHERE id = ?""",
        (duration_ms, state.rows_written, state.rows_deleted,
         json.dumps(state.metadata or {}),
         state.run_id),
    )
    conn.commit()
    conn.close()
