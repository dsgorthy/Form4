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
        BOOL_OR(stage = 'dedup')                   AS dedup_evaluated,
        BOOL_OR(stage = 'filter')                  AS filter_evaluated,
        BOOL_OR(stage = 'pit_lookup')              AS pit_evaluated,
        BOOL_OR(stage = 'min_10b5_1')              AS tenb51_evaluated,
        BOOL_OR(stage = 'conviction')              AS conviction_evaluated,
        MAX(reason) FILTER (WHERE stage = 'dedup')      AS dedup_reason,
        MAX(reason) FILTER (WHERE stage = 'filter')     AS filter_reason,
        MAX(reason) FILTER (WHERE stage = 'pit_lookup') AS pit_reason,
        MAX(reason) FILTER (WHERE stage = 'min_10b5_1') AS tenb51_reason,
        MAX(reason) FILTER (WHERE stage = 'conviction') AS conviction_reason,
        MAX(feature_snapshot::text) FILTER (WHERE stage = 'conviction') AS feature_snapshot_text
    FROM trade_decision_audit
    {where_clause}
    GROUP BY strategy, run_id, trade_id, ticker, filing_date, thesis, source
)
SELECT
    *,
    -- Final outcome. true iff this evaluation reached AND passed conviction.
    -- If conviction was not reached (rejected earlier), final_passed = false.
    COALESCE(conviction_passed, false) AS final_passed,
    -- The first stage that rejected this trade. NULL if it passed every stage.
    CASE
        WHEN dedup_passed = false      THEN 'dedup'
        WHEN filter_passed = false     THEN 'filter'
        WHEN pit_passed = false        THEN 'pit_lookup'
        WHEN tenb51_passed = false     THEN 'min_10b5_1'
        WHEN conviction_passed = false THEN 'conviction'
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


@router.get("/strategies/{name}")
def strategy_detail(
    name: str,
    user: UserContext = Depends(require_admin),
):
    """Full diagnostic for one strategy: freshness, decision summary, recent
    evaluations (one row per trade-evaluation, all stages summarized), and
    rejection histogram."""
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
    alerts = _recent_alerts(limit=20, severity=None,
                            component_filter=f"cw_runner.{name}")
    return {
        "strategy": s,
        "decision_summary": summary,
        "freshness": freshness,
        "rejection_histogram_30d": rejections,
        "recent_evaluations": recent_evaluations,
        "recent_alerts": alerts,
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
        regex="^(dedup|filter|pit_lookup|min_10b5_1|conviction)$",
        description="Only show evaluations rejected at this specific stage"),
    source: Optional[str] = Query(None, regex="^(live|simulation|log_replay)$"),
    since: Optional[str] = None,
    ticker: Optional[str] = None,
    user: UserContext = Depends(require_admin),
):
    """Paginated decisions, ONE ROW PER EVALUATION (= one row per trade-and-run).

    Each row summarizes all the stages this trade went through for this run:
    dedup → filter → pit_lookup → (min_10b5_1) → conviction. Columns indicate
    pass/fail for each stage and the human-readable reason. `final_passed`
    is the bottom-line outcome: did the strategy enter the trade?

    Filter examples:
      ?final_passed=true                   → trades the strategy would have taken
      ?final_passed=false                  → trades it rejected (with reasons)
      ?rejected_at=conviction              → trades that nearly passed but conviction
                                             scored too low
      ?rejected_at=filter                  → trades that failed a hard filter clause
      ?source=simulation&final_passed=true → backfilled "would-have-entered" history
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
