"""Admin diagnostics router — private to ADMIN_USER_IDS only.

Provides full operational visibility into the 3 live paper trading strategies:
data freshness, every candidate decision (with reasons), recent alerts,
runner state. Intentionally distinct from /portfolio (public, shows live
trades only) and /paper-trading (any signed-in user, shows account summary).

Auth: every endpoint depends on `require_admin` — anyone outside the
ADMIN_USER_IDS env allowlist gets 403.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import UserContext, require_admin
from api.db import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/diagnostics", tags=["admin-diagnostics"])

REPO_ROOT = Path(__file__).resolve().parents[2]
ALERT_LOG_PATH = REPO_ROOT / "logs" / "alerts.ndjson"

# Inside the API container, logs/ is mounted at /data/logs (read-only).
# Outside the container (local dev) it resolves to <repo>/logs/.
LOGS_DIR = Path("/data/logs") if Path("/data/logs").exists() else (REPO_ROOT / "logs")
CW_DATA_DIR = (Path("/data/cw_strategies") if Path("/data/cw_strategies").exists()
               else REPO_ROOT / "strategies" / "cw_strategies" / "data")


# Catalog of monitored launchd / cron jobs.
#   `log`: log file in LOGS_DIR — used for tail display + (default) liveness mtime
#   `heartbeat`: optional path under CW_DATA_DIR — if present, use THIS file's
#                mtime for liveness instead of the log file. Critical for the
#                cw_runners, which only write to the log on real events (scans,
#                entries, exits) but write to the heartbeat file every cycle.
#   `cadence_s`: max age before status flips to lagging/stale
JOB_CATALOG = [
    # File ingestion + features
    {"name": "insider-fetch",      "log": "insider-fetch.log",      "cadence_s": 5 * 60,
     "label": "EDGAR Form 4 poll", "category": "ingestion"},
    {"name": "refresh-features",   "log": "refresh-features.log",   "cadence_s": 26 * 3600,
     "label": "Daily features refresh (06:00 PT)", "category": "ingestion"},
    {"name": "daily-prices",       "log": "daily-prices.log",       "cadence_s": 30 * 3600,
     "label": "Daily prices update (17:30 PT)", "category": "ingestion"},
    {"name": "backfill-returns",   "log": "backfill_returns.log",   "cadence_s": 26 * 3600,
     "label": "Forward returns backfill (05:00 PT)", "category": "ingestion"},
    # Live runners — heartbeat is the source of truth (log only updates on real events)
    {"name": "quality-momentum",   "log": "quality-momentum.log",
     "heartbeat": "quality_momentum_heartbeat.json", "cadence_s": 5 * 60,
     "label": "QM cw_runner (live paper)", "category": "live_runner"},
    {"name": "reversal-dip",       "log": "reversal-dip.log",
     "heartbeat": "reversal_dip_heartbeat.json", "cadence_s": 5 * 60,
     "label": "RD cw_runner (live paper)", "category": "live_runner"},
    {"name": "tenb51-surprise",    "log": "tenb51-surprise.log",
     "heartbeat": "tenb51_surprise_heartbeat.json", "cadence_s": 5 * 60,
     "label": "10b5 cw_runner (live paper)", "category": "live_runner"},
    # Simulated portfolio runners
    {"name": "strategy-intraday",  "log": "strategy-intraday.log",  "cadence_s": 15 * 60,
     "label": "Intraday simulated portfolio update", "category": "simulator"},
    {"name": "strategy-simulator", "log": "strategy-simulator.log", "cadence_s": 26 * 3600,
     "label": "Daily simulated portfolio rebuild (07:00 PT)", "category": "simulator"},
    # Order fill resolution (event-driven + periodic backstop)
    {"name": "alpaca-stream-listener", "log": "alpaca-stream-listener.log",
     "heartbeat": "alpaca_stream_heartbeat.json", "cadence_s": 5 * 60,
     "label": "Alpaca trade_updates WebSocket listener (continuous)", "category": "fills"},
    {"name": "alpaca-intraday-resolver", "log": "alpaca-intraday-resolver.log",
     "cadence_s": 10 * 60,
     "label": "Intraday Alpaca order resolver (every 5 min, market hours)", "category": "fills"},
    # Monitoring + alerts
    {"name": "freshness-probe",    "log": "freshness-probe.log",    "cadence_s": 45 * 60,
     "label": "Freshness contract probe (every 30 min)", "category": "monitoring"},
    {"name": "alpaca-reconcile",   "log": "alpaca-reconcile.log",   "cadence_s": 26 * 3600,
     "label": "Alpaca paper account reconcile (daily)", "category": "monitoring"},
]

# Source-of-truth strategy registry. Keep in sync with
# api/routers/paper_trading.py:STRATEGIES if/when adding new strategies.
STRATEGIES = [
    {"name": "quality_momentum", "label": "Quality + Momentum",
     "thesis": "A+/A PIT-graded insiders buying in uptrend"},
    {"name": "reversal_dip",     "label": "Deep Reversal",
     "thesis": "Persistent seller (10+ consecutive sells) reverses into a deep dip"},
    {"name": "tenb51_surprise",  "label": "10b5-1 Surprise",
     "thesis": "Insider with prior 10b5-1 plan sells breaks pattern and buys"},
]


# ── helpers ─────────────────────────────────────────────────────────────────

def _strategy_or_404(name: str) -> dict:
    for s in STRATEGIES:
        if s["name"] == name:
            return s
    raise HTTPException(status_code=404, detail=f"Unknown strategy: {name}")


def _decision_summary(conn, strategy: str) -> dict:
    """Roll up trade_decision_audit for the strategy: counts by stage/outcome."""
    rows = conn.execute(
        """
        SELECT stage, passed, COUNT(*) AS n
          FROM trade_decision_audit
         WHERE strategy = ?
         GROUP BY stage, passed
        """,
        (strategy,),
    ).fetchall()
    by_stage: dict[str, dict] = {}
    for r in rows:
        stage = r["stage"]
        passed = bool(r["passed"])
        n = int(r["n"])
        bucket = by_stage.setdefault(stage, {"passed": 0, "rejected": 0, "total": 0})
        bucket["passed" if passed else "rejected"] += n
        bucket["total"] += n
    # Totals
    total = sum(b["total"] for b in by_stage.values())
    rejected = sum(b["rejected"] for b in by_stage.values())
    return {
        "total_evaluations": total,
        "rejected": rejected,
        "by_stage": by_stage,
    }


def _rejection_histogram(conn, strategy: str, days: int = 30) -> list[dict]:
    """Top-N rejection reasons in the last N days."""
    rows = conn.execute(
        f"""
        SELECT stage, reason, COUNT(*) AS n
          FROM trade_decision_audit
         WHERE strategy = ? AND passed = false
           AND ts >= NOW() - INTERVAL '{int(days)} days'
         GROUP BY stage, reason
         ORDER BY n DESC
         LIMIT 25
        """,
        (strategy,),
    ).fetchall()
    return [{"stage": r["stage"], "reason": r["reason"], "count": int(r["n"])}
            for r in rows]


def _freshness_for_strategy(conn, strategy: str) -> list[dict]:
    """Per-contract freshness status for one strategy."""
    try:
        from framework.contracts.freshness import FreshnessRegistry, get_freshness
    except Exception as e:
        logger.warning("freshness module unavailable: %s", e)
        return []
    registry = FreshnessRegistry.get()
    out = []
    for c in registry.for_strategy(strategy):
        try:
            ts, age = get_freshness(conn, c.table, c.column)
        except Exception:
            ts, age = None, None
        out.append({
            "table": c.table,
            "column": c.column,
            "max_staleness_hours": c.max_staleness_hours,
            "observed_age_hours": round(age, 2) if age is not None else None,
            "last_observed_at": ts.isoformat() if ts else None,
            "stale": age is None or age > c.max_staleness_hours,
            "populated_by": c.populated_by,
        })
    return out


def _recent_alerts(limit: int, severity: Optional[str], component_filter: Optional[str]) -> list[dict]:
    """Read the tail of logs/alerts.ndjson, filter, return last N entries."""
    if not ALERT_LOG_PATH.exists():
        return []
    entries: list[dict] = []
    with ALERT_LOG_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if severity and entry.get("severity") != severity:
                continue
            if component_filter and component_filter not in entry.get("component", ""):
                continue
            entries.append(entry)
    return entries[-limit:]


# ── endpoints ───────────────────────────────────────────────────────────────

@router.get("/strategies")
def list_strategies(
    user: UserContext = Depends(require_admin),
):
    """One row per strategy with at-a-glance health."""
    out = []
    with get_db() as conn:
        for s in STRATEGIES:
            name = s["name"]
            summary = _decision_summary(conn, name)
            freshness = _freshness_for_strategy(conn, name)
            stale_count = sum(1 for f in freshness if f["stale"])
            # Latest live decision
            latest = conn.execute(
                """SELECT MAX(ts) AS last_ts FROM trade_decision_audit
                    WHERE strategy = ? AND source = 'live'""",
                (name,),
            ).fetchone()
            # Halt heuristic: any critical alert from this strategy in last 24h
            recent_alerts = _recent_alerts(limit=200, severity="critical",
                                           component_filter=f"cw_runner.{name}")
            out.append({
                "name": name,
                "label": s["label"],
                "thesis": s["thesis"],
                "decision_summary": summary,
                "freshness_stale_count": stale_count,
                "freshness_total": len(freshness),
                "latest_live_decision_at": str(latest["last_ts"]) if latest and latest["last_ts"] else None,
                "recent_critical_alerts": len(recent_alerts),
            })
    return {"strategies": out}


# Aggregated query: pivot per-stage rows into one row per evaluation
# (strategy, run_id, trade_id, source). Each evaluation = one trade getting
# scanned by the strategy on a given run. Per-stage outcomes become columns.
_EVAL_SQL_BASE = """
WITH evaluations AS (
    SELECT
        strategy,
        run_id,
        trade_id,
        ticker,
        filing_date,
        thesis,
        source,
        MAX(ts) AS ts,
        MAX(pit_grade) AS pit_grade,
        MAX(conviction) AS conviction,
        BOOL_OR(stage = 'dedup' AND passed)        AS dedup_passed,
        BOOL_OR(stage = 'filter' AND passed)       AS filter_passed,
        BOOL_OR(stage = 'pit_lookup' AND passed)   AS pit_passed,
        BOOL_OR(stage = 'min_10b5_1' AND passed)   AS tenb51_passed,
        BOOL_OR(stage = 'conviction' AND passed)   AS conviction_passed,
        BOOL_OR(stage = 'capacity' AND passed)     AS capacity_passed,
        BOOL_OR(stage = 'dedup')                   AS dedup_evaluated,
        BOOL_OR(stage = 'filter')                  AS filter_evaluated,
        BOOL_OR(stage = 'pit_lookup')              AS pit_evaluated,
        BOOL_OR(stage = 'min_10b5_1')              AS tenb51_evaluated,
        BOOL_OR(stage = 'conviction')              AS conviction_evaluated,
        BOOL_OR(stage = 'capacity')                AS capacity_evaluated,
        MAX(reason) FILTER (WHERE stage = 'dedup')      AS dedup_reason,
        MAX(reason) FILTER (WHERE stage = 'filter')     AS filter_reason,
        MAX(reason) FILTER (WHERE stage = 'pit_lookup') AS pit_reason,
        MAX(reason) FILTER (WHERE stage = 'min_10b5_1') AS tenb51_reason,
        MAX(reason) FILTER (WHERE stage = 'conviction') AS conviction_reason,
        MAX(reason) FILTER (WHERE stage = 'capacity')   AS capacity_reason,
        MAX(feature_snapshot::text) FILTER (WHERE stage = 'conviction') AS feature_snapshot_text
    FROM trade_decision_audit
    {where_clause}
    GROUP BY strategy, run_id, trade_id, ticker, filing_date, thesis, source
)
SELECT
    *,
    -- Final outcome. Two cases that count as "would-have-entered":
    --   1. Capacity stage was reached and passed (newer simulation runs +
    --      future live runs that emit the capacity stage).
    --   2. Capacity was not evaluated but conviction passed (legacy live rows
    --      and historical 'actual' imports that don't have a capacity row).
    COALESCE(
        capacity_passed,
        CASE WHEN capacity_evaluated THEN false ELSE conviction_passed END,
        false
    ) AS final_passed,
    -- The first stage that rejected this trade. NULL if every evaluated stage
    -- passed. Reads top-down through the pipeline; if conviction passed but
    -- capacity rejected, surfaces 'capacity' (e.g. max_concurrent skip,
    -- same-day same-ticker dedup).
    CASE
        WHEN dedup_passed = false      THEN 'dedup'
        WHEN filter_passed = false     THEN 'filter'
        WHEN pit_passed = false        THEN 'pit_lookup'
        WHEN tenb51_passed = false     THEN 'min_10b5_1'
        WHEN conviction_passed = false THEN 'conviction'
        WHEN capacity_passed = false   THEN 'capacity'
        ELSE NULL
    END AS rejected_at
FROM evaluations
"""


def _query_evaluations(conn, where_sql: str, params: list,
                       order_sql: str = "ORDER BY ts DESC",
                       limit_sql: str = "LIMIT 50") -> list[dict]:
    sql = _EVAL_SQL_BASE.format(where_clause=where_sql) + f" {order_sql} {limit_sql}"
    rows = conn.execute(sql, params).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        # Parse feature_snapshot if present
        snap_str = d.pop("feature_snapshot_text", None)
        if snap_str:
            try:
                d["feature_snapshot"] = json.loads(snap_str)
            except Exception:
                d["feature_snapshot"] = None
        else:
            d["feature_snapshot"] = None
        out.append(d)
    return out


def _reconciliation_state(conn, strategy: str) -> dict:
    """Active strategy↔Alpaca divergences + latest Alpaca snapshot. Returns
    {"divergences": [...], "alpaca_positions": [...], "captured_at": <iso>}.
    Empty if no snapshot has ever been captured."""
    divergences = []
    rows = conn.execute(
        """SELECT id, ticker, issue_type, severity, db_qty, alpaca_qty,
                  db_entry_price, alpaca_avg_cost, db_status, portfolio_id,
                  detail, detected_at
             FROM alpaca_reconciliation
            WHERE strategy = ? AND resolved_at IS NULL
            ORDER BY severity DESC, detected_at DESC""",
        (strategy,),
    ).fetchall()
    for r in rows:
        d = dict(r)
        if d.get("detected_at"):
            d["detected_at"] = str(d["detected_at"])
        divergences.append(d)

    # Latest snapshot (per ticker — pick the most recent row per ticker)
    snap_rows = conn.execute(
        """SELECT DISTINCT ON (ticker)
                  ticker, qty, avg_entry_price, market_value,
                  current_price, unrealized_pl, captured_at
             FROM alpaca_position_snapshots
            WHERE strategy = ?
            ORDER BY ticker, captured_at DESC""",
        (strategy,),
    ).fetchall()
    alpaca_positions = []
    latest_capture = None
    for r in snap_rows:
        d = dict(r)
        if d.get("captured_at"):
            ts = str(d["captured_at"])
            d["captured_at"] = ts
            if latest_capture is None or ts > latest_capture:
                latest_capture = ts
        alpaca_positions.append(d)

    return {
        "divergences": divergences,
        "alpaca_positions": alpaca_positions,
        "latest_capture_at": latest_capture,
    }


@router.get("/strategies/{name}")
def strategy_detail(
    name: str,
    user: UserContext = Depends(require_admin),
):
    """Full diagnostic for one strategy: freshness, decision summary, recent
    evaluations (one row per trade-evaluation, all stages summarized),
    rejection histogram, and strategy↔Alpaca divergence."""
    s = _strategy_or_404(name)
    with get_db() as conn:
        summary = _decision_summary(conn, name)
        freshness = _freshness_for_strategy(conn, name)
        rejections = _rejection_histogram(conn, name, days=30)
        recent_evaluations = _query_evaluations(
            conn,
            where_sql="WHERE strategy = ?",
            params=[name],
            order_sql="ORDER BY ts DESC",
            limit_sql="LIMIT 50",
        )
        reconciliation = _reconciliation_state(conn, name)
    alerts = _recent_alerts(limit=20, severity=None,
                            component_filter=f"cw_runner.{name}")
    return {
        "strategy": s,
        "decision_summary": summary,
        "freshness": freshness,
        "rejection_histogram_30d": rejections,
        "recent_evaluations": recent_evaluations,
        "recent_alerts": alerts,
        "reconciliation": reconciliation,
    }


@router.get("/strategies/{name}/evaluations")
def strategy_evaluations(
    name: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    final_passed: Optional[bool] = Query(None,
        description="true = only evaluations that PASSED all stages and would have entered; "
                    "false = only those that were rejected at some stage"),
    rejected_at: Optional[str] = Query(None,
        regex="^(dedup|filter|pit_lookup|min_10b5_1|conviction|capacity)$",
        description="Only show evaluations rejected at this specific stage"),
    source: Optional[str] = Query(None, regex="^(live|simulation|log_replay|actual)$"),
    since: Optional[str] = None,
    ticker: Optional[str] = None,
    user: UserContext = Depends(require_admin),
):
    """Paginated decisions, ONE ROW PER EVALUATION (= one row per trade-and-run).

    Each row summarizes all the stages this trade went through for this run:
    dedup → filter → pit_lookup → (min_10b5_1) → conviction → capacity.
    Columns indicate pass/fail for each stage and the human-readable reason.
    `final_passed` is the bottom-line outcome: did the strategy enter?

    Filter examples:
      ?final_passed=true                   → trades the strategy would have taken
      ?final_passed=false                  → trades it rejected (with reasons)
      ?rejected_at=conviction              → trades that nearly passed but conviction
                                             scored too low
      ?rejected_at=capacity                → blocked by max_concurrent or same-day
                                             same-ticker dedup (a higher-conviction
                                             variant won the slot)
      ?rejected_at=filter                  → trades that failed a hard filter clause
      ?source=simulation&final_passed=true → backfilled "would-have-entered" history
      ?source=actual                       → historical entries from strategy_portfolio
                                             (mixed backtest + live code versions)
      ?ticker=AAPL                         → all evaluations of AAPL
    """
    _strategy_or_404(name)
    clauses = ["strategy = ?"]
    params: list = [name]
    if source:
        clauses.append("source = ?")
        params.append(source)
    if ticker:
        clauses.append("ticker = ?")
        params.append(ticker.upper())
    if since:
        clauses.append("ts >= ?::timestamptz")
        params.append(since)

    base_where = " AND ".join(clauses)
    base_where_clause = f"WHERE {base_where}"
    offset = (page - 1) * per_page
    with get_db() as conn:
        # First, get total + apply final_passed/rejected_at filters at the
        # outer level (after the GROUP BY pivot).
        outer_clauses: list[str] = []
        if final_passed is not None:
            outer_clauses.append(f"final_passed = {bool(final_passed)}")
        if rejected_at:
            outer_clauses.append(f"rejected_at = '{rejected_at}'")
        outer_where = (" WHERE " + " AND ".join(outer_clauses)) if outer_clauses else ""

        # Total
        count_sql = (
            _EVAL_SQL_BASE.format(where_clause=base_where_clause)
            + outer_where
        )
        total_sql = f"SELECT COUNT(*) AS n FROM ({count_sql}) sub"
        total_row = conn.execute(total_sql, params).fetchone()
        total = int(total_row["n"]) if total_row else 0

        # Page rows
        sql = (
            _EVAL_SQL_BASE.format(where_clause=base_where_clause)
            + outer_where
            + f" ORDER BY ts DESC LIMIT {int(per_page)} OFFSET {int(offset)}"
        )
        rows = conn.execute(sql, params).fetchall()
        decisions = []
        for r in rows:
            d = dict(r)
            snap_str = d.pop("feature_snapshot_text", None)
            if snap_str:
                try:
                    d["feature_snapshot"] = json.loads(snap_str)
                except Exception:
                    d["feature_snapshot"] = None
            else:
                d["feature_snapshot"] = None
            decisions.append(d)
    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "evaluations": decisions,
    }


@router.get("/strategies/{name}/freshness")
def strategy_freshness(
    name: str,
    user: UserContext = Depends(require_admin),
):
    """Per-contract freshness state for one strategy."""
    _strategy_or_404(name)
    with get_db() as conn:
        return {"strategy": name, "freshness": _freshness_for_strategy(conn, name)}


@router.get("/alerts")
def alerts_feed(
    limit: int = Query(50, ge=1, le=500),
    severity: Optional[str] = Query(None, regex="^(info|warn|error|critical)$"),
    component: Optional[str] = None,
    user: UserContext = Depends(require_admin),
):
    """Read tail of logs/alerts.ndjson, filter, return last N entries (newest last)."""
    return {
        "alert_log_path": str(ALERT_LOG_PATH),
        "alerts": _recent_alerts(limit, severity, component),
    }


# ── System-wide jobs monitor ─────────────────────────────────────────────

def _tail_file(path: Path, n_lines: int = 5) -> list[str]:
    """Return last n_lines of a text file. Empty list if missing."""
    try:
        with path.open("rb") as f:
            # Seek backwards in chunks until we have enough newlines
            f.seek(0, 2)
            size = f.tell()
            block = 4096
            data = b""
            while size > 0 and data.count(b"\n") < n_lines + 1:
                read_size = min(block, size)
                size -= read_size
                f.seek(size)
                data = f.read(read_size) + data
            lines = data.decode("utf-8", errors="replace").splitlines()
            return [ln for ln in lines[-n_lines:] if ln.strip()]
    except Exception:
        return []


def _job_status(job: dict) -> dict:
    """Compute status for one job.

    Liveness signal:
      - If `heartbeat` is set: use heartbeat file mtime (and parse the JSON for
        status fields like 'detail' / 'open_positions'). This is the source of
        truth for cw_runners — they update the heartbeat every cycle even when
        idle, but the log file only sees writes on real events (entries, exits,
        scans), so log mtime gives false-stale readings.
      - Otherwise: use log file mtime as the liveness signal.

    Tail display always uses the log file (the heartbeat JSON has structured
    data, not human-readable history).
    """
    log_path = LOGS_DIR / job["log"]
    now = datetime.now(timezone.utc)
    cadence_s = job["cadence_s"]
    log_tail = _tail_file(log_path, n_lines=5) if log_path.exists() else []

    # Pick liveness file: heartbeat if specified, else log
    heartbeat_name = job.get("heartbeat")
    liveness_path = CW_DATA_DIR / heartbeat_name if heartbeat_name else log_path
    liveness_source = "heartbeat" if heartbeat_name else "log"

    if not liveness_path.exists():
        return {
            "name": job["name"],
            "label": job["label"],
            "category": job["category"],
            "log_file": str(log_path),
            "liveness_source": liveness_source,
            "liveness_file": str(liveness_path),
            "exists": False,
            "last_run_at": None,
            "age_seconds": None,
            "expected_cadence_seconds": cadence_s,
            "status": "missing",
            "tail": log_tail,
            "heartbeat": None,
        }

    mtime = datetime.fromtimestamp(liveness_path.stat().st_mtime, tz=timezone.utc)
    age_s = (now - mtime).total_seconds()
    if age_s <= cadence_s:
        status = "healthy"
    elif age_s <= cadence_s * 2:
        status = "lagging"
    else:
        status = "stale"

    # Parse heartbeat JSON for status detail if present
    heartbeat_data = None
    if heartbeat_name:
        try:
            heartbeat_data = json.loads(liveness_path.read_text())
        except Exception:
            heartbeat_data = None

    return {
        "name": job["name"],
        "label": job["label"],
        "category": job["category"],
        "log_file": str(log_path),
        "liveness_source": liveness_source,
        "liveness_file": str(liveness_path),
        "exists": True,
        "last_run_at": mtime.isoformat(),
        "age_seconds": int(age_s),
        "expected_cadence_seconds": cadence_s,
        "status": status,
        "tail": log_tail,
        "heartbeat": heartbeat_data,
    }


@router.get("/jobs")
def jobs_status(user: UserContext = Depends(require_admin)) -> dict:
    """Real-time status of every monitored launchd job.

    Status semantics:
      - healthy: log mtime within expected cadence
      - lagging: log mtime older than cadence but less than 2x cadence
      - stale:   log mtime older than 2x cadence (or log missing for too long)
      - missing: log file doesn't exist

    Returns one entry per job in JOB_CATALOG with last-run timestamp,
    age, expected cadence, and the last 5 lines of the log tail.
    """
    jobs = [_job_status(j) for j in JOB_CATALOG]
    summary = {
        "healthy": sum(1 for j in jobs if j["status"] == "healthy"),
        "lagging": sum(1 for j in jobs if j["status"] == "lagging"),
        "stale":   sum(1 for j in jobs if j["status"] == "stale"),
        "missing": sum(1 for j in jobs if j["status"] == "missing"),
        "total":   len(jobs),
    }
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "jobs": jobs,
    }


@router.get("/freshness")
def system_freshness(user: UserContext = Depends(require_admin)) -> dict:
    """All freshness contracts (table.column) and current state.

    Reads from signal_freshness table, which is populated by the upstream
    compute jobs. The probe at scripts/freshness_probe.py uses the same
    data; this endpoint surfaces it for the admin dashboard.
    """
    with get_db() as conn:
        rows = conn.execute(
            """SELECT source, table_name, column_name,
                      MAX(last_computed_at) AS last_computed,
                      MAX(n_rows_affected) AS last_n_rows,
                      MAX(populated_by) AS last_populated_by
               FROM signal_freshness
               GROUP BY source, table_name, column_name
               ORDER BY 1, 2, 3"""
        ).fetchall()
    now = datetime.now(timezone.utc)
    contracts = []
    for r in rows:
        d = dict(r)
        last = d.get("last_computed")
        if last:
            if isinstance(last, str):
                try:
                    last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                except Exception:
                    last_dt = None
            else:
                last_dt = last if hasattr(last, "tzinfo") else None
            age_s = int((now - last_dt).total_seconds()) if last_dt else None
        else:
            age_s = None
        contracts.append({
            "source": d.get("source"),
            "table": d.get("table_name"),
            "column": d.get("column_name"),
            "last_computed_at": last.isoformat() if hasattr(last, "isoformat") else last,
            "age_seconds": age_s,
            "last_n_rows_affected": int(d.get("last_n_rows") or 0),
            "populated_by": d.get("last_populated_by"),
        })
    return {
        "checked_at": now.isoformat(),
        "n_contracts": len(contracts),
        "contracts": contracts,
    }
