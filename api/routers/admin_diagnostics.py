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


@router.get("/strategies/{name}")
def strategy_detail(
    name: str,
    user: UserContext = Depends(require_admin),
):
    """Full diagnostic for one strategy: freshness, decision summary, recent decisions, rejection histogram."""
    s = _strategy_or_404(name)
    with get_db() as conn:
        summary = _decision_summary(conn, name)
        freshness = _freshness_for_strategy(conn, name)
        rejections = _rejection_histogram(conn, name, days=30)
        recent_decisions = conn.execute(
            """SELECT ts, ticker, trade_id, filing_date, thesis, stage, passed,
                      reason, pit_grade, conviction, source
                 FROM trade_decision_audit
                WHERE strategy = ?
                ORDER BY ts DESC
                LIMIT 50""",
            (name,),
        ).fetchall()
        decisions_out = [dict(r) for r in recent_decisions]
    alerts = _recent_alerts(limit=20, severity=None,
                            component_filter=f"cw_runner.{name}")
    return {
        "strategy": s,
        "decision_summary": summary,
        "freshness": freshness,
        "rejection_histogram_30d": rejections,
        "recent_decisions": decisions_out,
        "recent_alerts": alerts,
    }


@router.get("/strategies/{name}/decisions")
def strategy_decisions(
    name: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    stage: Optional[str] = None,
    passed: Optional[bool] = None,
    source: Optional[str] = Query(None, regex="^(live|simulation|log_replay)$"),
    since: Optional[str] = None,
    ticker: Optional[str] = None,
    user: UserContext = Depends(require_admin),
):
    """Paginated decision audit for one strategy, with filters."""
    _strategy_or_404(name)
    clauses = ["strategy = ?"]
    params: list = [name]
    if stage:
        clauses.append("stage = ?")
        params.append(stage)
    if passed is not None:
        clauses.append("passed = ?")
        params.append(passed)
    if source:
        clauses.append("source = ?")
        params.append(source)
    if ticker:
        clauses.append("ticker = ?")
        params.append(ticker.upper())
    if since:
        clauses.append("ts >= ?::timestamptz")
        params.append(since)

    where = " AND ".join(clauses)
    offset = (page - 1) * per_page
    with get_db() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(*) AS n FROM trade_decision_audit WHERE {where}",
            params,
        ).fetchone()
        total = int(total_row["n"]) if total_row else 0
        rows = conn.execute(
            f"""SELECT id, ts, run_id, strategy, ticker, trade_id, filing_date,
                       thesis, stage, passed, reason, pit_grade, conviction,
                       feature_snapshot, source
                  FROM trade_decision_audit
                 WHERE {where}
                 ORDER BY ts DESC
                 LIMIT {int(per_page)} OFFSET {int(offset)}""",
            params,
        ).fetchall()
        decisions = []
        for r in rows:
            d = dict(r)
            # feature_snapshot is jsonb; psycopg2 returns it parsed already
            decisions.append(d)
    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "decisions": decisions,
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
