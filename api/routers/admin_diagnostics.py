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
from datetime import datetime, timedelta, timezone, date as _date
from pathlib import Path
from typing import Optional

import requests
import yaml
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
    {"name": "candidate-count-probe", "log": "candidate-count-probe.log", "cadence_s": 26 * 3600,
     "label": "Per-strategy candidate-count probe (18:00 ET daily)", "category": "monitoring"},
    {"name": "post-deploy-audit",  "log": "post-deploy-audit.log",  "cadence_s": 48 * 3600,
     "label": "Post-deploy audit (alerts since deploy, plist coverage)", "category": "monitoring"},
    {"name": "heartbeat-probe",    "log": "heartbeat-probe.log",    "cadence_s": 30 * 60,
     "label": "cw_runner heartbeat freshness probe", "category": "monitoring"},
    {"name": "strategy-health",    "log": "strategy-health.log",    "cadence_s": 6 * 3600,
     "label": "Strategy health (P&L drift, position aging)", "category": "monitoring"},
    {"name": "thesis-monitor",     "log": "thesis-monitor.log",     "cadence_s": 26 * 3600,
     "label": "Thesis-level position tracker (daily)", "category": "monitoring"},
    {"name": "compute-signals",    "log": "compute-signals.log",    "cadence_s": 26 * 3600,
     "label": "Insider signal compute (21 detectors, daily)", "category": "ingestion"},
    {"name": "daily-summary",      "log": "daily-summary.log",      "cadence_s": 26 * 3600,
     "label": "Daily summary email (17:30 ET, includes alert aggregate)", "category": "monitoring"},
    {"name": "pit-shadow",         "log": "pit_shadow.log",         "cadence_s": 7 * 24 * 3600,
     "label": "PIT engine shadow validator (one-shot windows)", "category": "monitoring"},
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


# Strategy yaml location (same on Studio + Mini).
_STRAT_YAML_DIR = REPO_ROOT / "strategies" / "cw_strategies" / "configs"


def _read_strategy_rules(name: str) -> dict:
    """Return the operationally-relevant subset of the strategy's yaml.

    Used by the /positions endpoint to render a "Strategy rules" header
    band (target_hold, stop_pct, exit_strategy, capacity caps) and by the
    strategy_detail endpoint to expose execution_mode for the frontend's
    alert_only conditional rendering.
    """
    yaml_path = _STRAT_YAML_DIR / f"{name}.yaml"
    try:
        cfg = yaml.safe_load(yaml_path.read_text()) or {}
    except Exception as e:
        logger.warning("strategy yaml read failed for %s: %s", name, e)
        return {}
    exit_cfg = cfg.get("exit", {}) or {}
    return {
        "execution_mode": cfg.get("execution_mode", "paper"),
        "exit_strategy": exit_cfg.get("strategy"),
        "hold_days": exit_cfg.get("hold_days"),
        "stop_loss_pct": exit_cfg.get("stop_loss_pct"),
        "max_concurrent": cfg.get("max_concurrent"),
        "soft_cap": cfg.get("soft_cap"),
        "min_conviction": cfg.get("min_conviction"),
        "min_conviction_above_soft": cfg.get("min_conviction_above_soft"),
        "position_size_pct": cfg.get("position_size_pct"),
    }


# Per-process 60s price cache. Open positions are bounded (~10 per
# strategy), so a single dashboard refresh fetches at most ~10 quotes
# from Alpaca. 60s is the same TTL the paper_trading.py /dashboard uses.
_PRICE_CACHE: dict[str, tuple[datetime, dict]] = {}
_PRICE_TTL_SECONDS = 60


def _fetch_current_prices(strategy: str, tickers: list[str], conn) -> dict[str, dict]:
    """Return {ticker: {"price": float|None, "at": iso|None, "source": str}}.

    source ∈ {"alpaca", "eod_fallback", "unavailable"}.

    Uses the strategy's per-account trading credentials against
    data.alpaca.markets — the same path the runner uses since the
    2026-05-30 price-fetch fix. On failure, falls back to the most
    recent close in prices.daily_prices. Both routes contribute to a
    shared 60s cache.
    """
    if not tickers:
        return {}
    now = datetime.now(timezone.utc)
    out: dict[str, dict] = {}
    misses: list[str] = []
    for t in tickers:
        cached = _PRICE_CACHE.get(t)
        if cached and (now - cached[0]).total_seconds() < _PRICE_TTL_SECONDS:
            out[t] = cached[1]
        else:
            misses.append(t)
    if not misses:
        return out

    prefix = strategy.upper()
    api_key = os.getenv(f"ALPACA_API_KEY_{prefix}", "")
    api_secret = os.getenv(f"ALPACA_API_SECRET_{prefix}", "")
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret} if api_key else {}

    for t in misses:
        d = {"price": None, "at": None, "source": "unavailable"}
        if headers:
            try:
                r = requests.get(
                    f"https://data.alpaca.markets/v2/stocks/{t}/trades/latest",
                    headers=headers,
                    timeout=5,
                )
                if r.status_code == 200:
                    trade = (r.json() or {}).get("trade") or {}
                    p = trade.get("p")
                    if p:
                        d = {"price": float(p), "at": trade.get("t"), "source": "alpaca"}
            except Exception as e:
                logger.warning("Alpaca quote failed for %s: %s", t, e)

        if d["price"] is None and conn is not None:
            try:
                row = conn.execute(
                    """SELECT date, close FROM prices.daily_prices
                        WHERE ticker = ? ORDER BY date DESC LIMIT 1""",
                    (t,),
                ).fetchone()
                if row and row["close"]:
                    d = {"price": float(row["close"]),
                         "at": str(row["date"]),
                         "source": "eod_fallback"}
            except Exception as e:
                logger.warning("daily_prices lookup failed for %s: %s", t, e)

        _PRICE_CACHE[t] = (now, d)
        out[t] = d

    return out


def _trading_days_between(d1: _date, d2: _date) -> int:
    """Trading days from d1 to d2 (exclusive of d1, inclusive of d2).
    Negative if d2 < d1. Used for hold time + time remaining display."""
    if d1 == d2:
        return 0
    try:
        from framework.data.calendar import MarketCalendar
    except Exception:
        # Fallback to calendar days if MarketCalendar unavailable.
        return (d2 - d1).days
    cal = MarketCalendar()
    sign = 1 if d2 > d1 else -1
    start, end = (d1, d2) if sign > 0 else (d2, d1)
    n = 0
    cursor = start + timedelta(days=1)
    while cursor <= end:
        if cal.is_trading_day(cursor):
            n += 1
        cursor += timedelta(days=1)
    return sign * n


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
    """Per-contract freshness status for one strategy.

    Returns two staleness numbers per row:
      observed_age_hours — raw clock hours since last update.
      business_age_hours — hours excluding weekends + US market holidays
                           (only when the contract's business_hours_only=true,
                            which is the default).
    `effective_stale` is the business-aware verdict — what the UI should
    treat as authoritative. `stale` is kept as a back-compat flag set on
    the raw observed_age for any older consumers.
    """
    try:
        from framework.contracts.freshness import (
            FreshnessRegistry, get_freshness, business_age_hours,
        )
    except Exception as e:
        logger.warning("freshness module unavailable: %s", e)
        return []
    registry = FreshnessRegistry.get()
    now = datetime.now(timezone.utc)
    out = []
    for c in registry.for_strategy(strategy):
        try:
            ts, age = get_freshness(conn, c.table, c.column)
        except Exception:
            ts, age = None, None

        # Business-hours-adjusted age. Falls back to raw age if the
        # contract doesn't have business_hours_only set or if ts missing.
        if ts is not None and c.business_hours_only:
            biz_age = business_age_hours(ts, now)
        elif age is not None:
            biz_age = float(age)
        else:
            biz_age = None

        observed_stale = age is None or age > c.max_staleness_hours
        effective_stale = biz_age is None or biz_age > c.max_staleness_hours

        # Human-friendly status label.
        # - unknown:        no signal_freshness row found
        # - fresh:          inside SLA on business-hours basis
        # - weekend_ok:     past raw SLA but inside business-hours SLA
        # - stale:          past business-hours SLA
        if age is None:
            status_label = "unknown"
        elif effective_stale:
            status_label = "stale"
        elif observed_stale:
            status_label = "weekend_ok"
        else:
            status_label = "fresh"

        out.append({
            "table": c.table,
            "column": c.column,
            "max_staleness_hours": c.max_staleness_hours,
            "observed_age_hours": round(age, 2) if age is not None else None,
            "business_age_hours": round(biz_age, 2) if biz_age is not None else None,
            "last_observed_at": ts.isoformat() if ts else None,
            "stale": observed_stale,
            "effective_stale": effective_stale,
            "status_label": status_label,
            "business_hours_only": c.business_hours_only,
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
                       limit_sql: str = "LIMIT 50",
                       *,
                       dedup: bool = True,
                       strategy_for_prices: Optional[str] = None) -> list[dict]:
    """Pull evaluation rows enriched with accession/insider/outcome.

    `dedup=True` (default) collapses multi-lot Form 4 filings to one row
    per SEC accession (highest-conviction lot). Set False to surface
    every individual evaluation row.

    `strategy_for_prices` enables live-quote lookup for rows whose
    entered position is still open — produces `outcome.kind='open'` with
    fresh unrealized P&L.
    """
    extra_where = " WHERE dedup_rn = 1" if dedup else ""
    sql = (_EVAL_DEDUP_SQL.format(where_clause=where_sql)
           + extra_where + f" {order_sql} {limit_sql}")
    rows = conn.execute(sql, params).fetchall()
    open_tickers = [
        r["ticker"] for r in rows
        if r.get("alert_status") == "open" and r.get("ticker")
    ]
    open_tickers = list(set(open_tickers))
    price_map = (
        _fetch_current_prices(strategy_for_prices, open_tickers, conn)
        if (strategy_for_prices and open_tickers) else {}
    )
    out: list[dict] = []
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
        d["outcome"] = _build_eval_outcome(d, price_map)
        d.pop("dedup_rn", None)
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
            dedup=True,
            strategy_for_prices=name,
        )
        reconciliation = _reconciliation_state(conn, name)
    alerts = _recent_alerts(limit=20, severity=None,
                            component_filter=f"cw_runner.{name}")
    rules = _read_strategy_rules(name)
    return {
        "strategy": {**s, "execution_mode": rules.get("execution_mode")},
        "decision_summary": summary,
        "freshness": freshness,
        "rejection_histogram_30d": rejections,
        "recent_evaluations": recent_evaluations,
        "recent_alerts": alerts,
        # Reconciliation block is only meaningful when the runner actually
        # touches Alpaca. For alert_only strategies (QM, RD) there are no
        # Alpaca orders to reconcile, so the frontend hides the panel.
        "reconciliation": reconciliation if rules.get("execution_mode") != "alert_only" else None,
        "rules": rules,
    }


_EVAL_DEDUP_SQL = """
WITH evaluations AS (
    SELECT
        strategy, run_id, trade_id, ticker, filing_date, thesis, source,
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
),
enriched AS (
    SELECT
        ev.*,
        COALESCE(
            capacity_passed,
            CASE WHEN capacity_evaluated THEN false ELSE conviction_passed END,
            false
        ) AS final_passed,
        CASE
            WHEN dedup_passed = false      THEN 'dedup'
            WHEN filter_passed = false     THEN 'filter'
            WHEN pit_passed = false        THEN 'pit_lookup'
            WHEN tenb51_passed = false     THEN 'min_10b5_1'
            WHEN conviction_passed = false THEN 'conviction'
            WHEN capacity_passed = false   THEN 'capacity'
            ELSE NULL
        END AS rejected_at,
        t.accession                                AS accession,
        t.value                                    AS trade_value,
        i.name                                     AS insider_name,
        t.title                                    AS insider_title,
        t.career_grade                             AS trade_career_grade,
        sp.status                                  AS alert_status,
        sp.entry_price                             AS alert_entry_price,
        sp.exit_price                              AS alert_exit_price,
        sp.pnl_pct                                 AS alert_pnl_pct,
        sp.pnl_dollar                              AS alert_pnl_dollar,
        sp.shares                                  AS alert_shares,
        sp.entry_date                              AS alert_entry_date,
        sp.exit_reason                             AS alert_exit_reason,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(t.accession, 'eval_' || ev.trade_id::text)
            ORDER BY ev.conviction DESC NULLS LAST, ev.ts DESC
        ) AS dedup_rn,
        COUNT(*) OVER (
            PARTITION BY COALESCE(t.accession, 'eval_' || ev.trade_id::text)
        ) AS lots_in_filing
    FROM evaluations ev
    LEFT JOIN trades t       ON t.trade_id = ev.trade_id
    LEFT JOIN insiders i     ON i.insider_id = t.effective_insider_id
    LEFT JOIN LATERAL (
        SELECT * FROM strategy_portfolio sp_inner
         WHERE sp_inner.strategy = ev.strategy
           AND sp_inner.trade_id = ev.trade_id
           AND COALESCE(sp_inner.is_live, false) = false
         ORDER BY (sp_inner.status = 'open') DESC, sp_inner.entry_date DESC
         LIMIT 1
    ) sp ON true
)
SELECT * FROM enriched
"""


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
    dedup: bool = Query(True,
        description="When true (default), collapse multi-lot Form 4 filings to one "
                    "row per SEC accession. The representative row is the lot with "
                    "the highest conviction (ties broken by latest ts)."),
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
        # Outer filters applied after the per-stage pivot. Dedup adds
        # "dedup_rn = 1" so we keep one representative per accession.
        outer_clauses: list[str] = []
        if final_passed is not None:
            outer_clauses.append(f"final_passed = {bool(final_passed)}")
        if rejected_at:
            outer_clauses.append(f"rejected_at = '{rejected_at}'")
        if dedup:
            outer_clauses.append("dedup_rn = 1")
        outer_where = (" WHERE " + " AND ".join(outer_clauses)) if outer_clauses else ""

        # Total
        count_sql = (
            _EVAL_DEDUP_SQL.format(where_clause=base_where_clause)
            + outer_where
        )
        total_sql = f"SELECT COUNT(*) AS n FROM ({count_sql}) sub"
        total_row = conn.execute(total_sql, params).fetchone()
        total = int(total_row["n"]) if total_row else 0

        # Page rows
        sql = (
            _EVAL_DEDUP_SQL.format(where_clause=base_where_clause)
            + outer_where
            + f" ORDER BY ts DESC LIMIT {int(per_page)} OFFSET {int(offset)}"
        )
        rows = conn.execute(sql, params).fetchall()

        # Resolve current prices for entered-and-still-open rows so we
        # can show live unrealized P&L in the Filings table's Outcome
        # column. Bounded to (per_page) lookups, hits the shared cache.
        open_tickers = list({
            r["ticker"] for r in rows
            if r.get("alert_status") == "open" and r.get("ticker")
        })
        price_map = _fetch_current_prices(name, open_tickers, conn) if open_tickers else {}

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
            # Outcome shape — single dict consumed by the Filings table's
            # Outcome column. open: unrealized P&L vs entry; closed:
            # stored realized P&L; not_entered: terminal stage + reason.
            d["outcome"] = _build_eval_outcome(d, price_map)
            # dedup_rn is an implementation detail; the count is what's useful.
            d.pop("dedup_rn", None)
            decisions.append(d)
    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "dedup": dedup,
        "evaluations": decisions,
    }


def _build_eval_outcome(d: dict, price_map: dict[str, dict]) -> dict:
    """Compress an enriched evaluation row into a single Outcome shape.

    Three branches:
      not_entered → strategy rejected at some stage; surface terminal stage
                    + reason from the corresponding *_reason column.
      open        → entered and currently held; compute unrealized P&L
                    using a live quote (fall through to None if missing).
      closed      → entered and exited; use stored pnl_pct / pnl_dollar.
    """
    if not d.get("final_passed"):
        stage = d.get("rejected_at")
        reason_field_map = {
            "dedup": "dedup_reason",
            "filter": "filter_reason",
            "pit_lookup": "pit_reason",
            "min_10b5_1": "tenb51_reason",
            "conviction": "conviction_reason",
            "capacity": "capacity_reason",
        }
        reason = d.get(reason_field_map.get(stage or "", ""), "")
        return {
            "kind": "not_entered",
            "rejected_at": stage,
            "reason": reason,
        }
    status = d.get("alert_status")
    if status == "open":
        ticker = d.get("ticker")
        quote = price_map.get(ticker) or {}
        current_price = quote.get("price")
        entry_price = float(d["alert_entry_price"]) if d.get("alert_entry_price") is not None else None
        shares = int(d["alert_shares"] or 0) if d.get("alert_shares") is not None else 0
        if current_price is not None and entry_price is not None:
            pnl_pct = (current_price - entry_price) / entry_price
            pnl_dollar = (current_price - entry_price) * shares
        else:
            pnl_pct = None
            pnl_dollar = None
        return {
            "kind": "open",
            "entry_price": entry_price,
            "current_price": current_price,
            "price_source": quote.get("source", "unavailable"),
            "shares": shares,
            "pnl_pct": pnl_pct,
            "pnl_dollar": pnl_dollar,
            "entry_date": d.get("alert_entry_date"),
        }
    if status == "closed":
        return {
            "kind": "closed",
            "entry_price": float(d["alert_entry_price"]) if d.get("alert_entry_price") is not None else None,
            "exit_price": float(d["alert_exit_price"]) if d.get("alert_exit_price") is not None else None,
            "pnl_pct": float(d["alert_pnl_pct"]) if d.get("alert_pnl_pct") is not None else None,
            "pnl_dollar": float(d["alert_pnl_dollar"]) if d.get("alert_pnl_dollar") is not None else None,
            "exit_reason": d.get("alert_exit_reason"),
        }
    # final_passed=true but no matching strategy_portfolio row — historical
    # rows from old code versions, or out-of-band cleanup. Mark as entered-
    # but-untracked so the UI shows "✓ entered" without a P&L number.
    return {"kind": "entered_untracked"}


@router.get("/strategies/{name}/freshness")
def strategy_freshness(
    name: str,
    user: UserContext = Depends(require_admin),
):
    """Per-contract freshness state for one strategy."""
    _strategy_or_404(name)
    with get_db() as conn:
        return {"strategy": name, "freshness": _freshness_for_strategy(conn, name)}


def _build_position_rows(
    rows: list,
    strategy: str,
    price_map: dict[str, dict],
    today: _date,
    is_closed: bool,
    rules: Optional[dict] = None,
) -> list[dict]:
    """Shared shape for open + closed position rows. Includes P&L
    (unrealized for open, realized for closed), days_held /
    trading_days_remaining (open only), and a portfolio-relative size
    fraction so the UI can show "$18.6k · 10.0%" per row.

    If the row's stored planned_exit_date is missing (the simulator
    doesn't write it), we synthesize it from entry_date + rules.hold_days
    via MarketCalendar — same formula cw_runner uses, so sim + alert rows
    end up directionally consistent. trading_days_remaining is always
    computed against this resolved planned exit.
    """
    hold_days = (rules or {}).get("hold_days")
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        ticker = d.get("ticker")
        entry_price = float(d["entry_price"]) if d.get("entry_price") is not None else None
        shares = int(d["shares"] or 0)
        entry_date_str = d.get("entry_date")
        try:
            entry_d = _date.fromisoformat(entry_date_str) if entry_date_str else None
        except Exception:
            entry_d = None

        dollar_amount = float(d["dollar_amount"]) if d.get("dollar_amount") is not None else None
        portfolio_value = float(d["portfolio_value"]) if d.get("portfolio_value") is not None else None
        # % of portfolio at the time the position was sized. For sim rows
        # portfolio_value happens to hold final equity (legacy simulator
        # quirk) so this comes in slightly under 10% — the actual sizing
        # at entry was 10% of entry-time equity. Either way, the relative
        # ratio per row matches the strategy's position_size_pct target.
        size_pct = (
            dollar_amount / portfolio_value
            if dollar_amount is not None and portfolio_value not in (None, 0)
            else None
        )
        row: dict = {
            "id": d.get("id"),
            "trade_id": d.get("trade_id"),
            "ticker": ticker,
            "execution_source": d.get("execution_source"),
            "insider_name": d.get("insider_name"),
            "insider_title": d.get("insider_title"),
            "signal_grade": d.get("signal_grade"),
            "entry_date": entry_date_str,
            "entry_price": entry_price,
            "shares": shares,
            "dollar_amount": dollar_amount,
            "portfolio_value": portfolio_value,
            "position_size_pct": size_pct,
        }

        if is_closed:
            exit_price = float(d["exit_price"]) if d.get("exit_price") is not None else None
            row["exit_date"] = d.get("exit_date")
            row["exit_price"] = exit_price
            row["exit_reason"] = d.get("exit_reason")
            row["hold_days"] = int(d["hold_days"]) if d.get("hold_days") is not None else None
            row["pnl_pct"] = float(d["pnl_pct"]) if d.get("pnl_pct") is not None else None
            row["pnl_dollar"] = float(d["pnl_dollar"]) if d.get("pnl_dollar") is not None else None
        else:
            quote = price_map.get(ticker) or {}
            current_price = quote.get("price")
            row["current_price"] = current_price
            row["current_price_at"] = quote.get("at")
            row["price_source"] = quote.get("source", "unavailable")
            if current_price is not None and entry_price is not None:
                row["unrealized_pnl_pct"] = (current_price - entry_price) / entry_price
                row["unrealized_pnl_dollar"] = (current_price - entry_price) * shares
            else:
                row["unrealized_pnl_pct"] = None
                row["unrealized_pnl_dollar"] = None
            row["days_held"] = (today - entry_d).days if entry_d else None
            # Resolve planned_exit_date: prefer the stored value (cw_runner
            # writes it correctly); fall back to entry_date + rules.hold_days
            # trading days for sim rows where the simulator skips the column.
            stored_planned = d.get("planned_exit_date")
            try:
                planned_d = _date.fromisoformat(stored_planned) if stored_planned else None
            except Exception:
                planned_d = None
            if planned_d is None and entry_d is not None and hold_days:
                try:
                    from framework.data.calendar import MarketCalendar
                    planned_d = MarketCalendar().add_trading_days(entry_d, int(hold_days))
                except Exception as exc:
                    logger.debug("planned_exit synthesis failed for %s: %s", ticker, exc)
            row["planned_exit_date"] = planned_d.isoformat() if planned_d else None
            row["trading_days_remaining"] = (
                _trading_days_between(today, planned_d) if planned_d else None
            )

        out.append(row)
    return out


@router.get("/strategies/{name}/positions")
def strategy_positions(
    name: str,
    page: int = Query(1, ge=1, description="Closed positions page"),
    per_page: int = Query(25, ge=1, le=200, description="Closed positions per page"),
    user: UserContext = Depends(require_admin),
):
    """Open + paginated closed alert positions with P&L.

    Open positions get a live current_price (Alpaca data API → daily_prices
    fallback). Closed positions get stored realized P&L from
    strategy_portfolio.

    The `rules` block surfaces the strategy yaml's hold_days, stop_pct,
    exit_strategy, etc. so the UI can render a "Strategy rules" header
    band without re-deriving anything client-side.
    """
    _strategy_or_404(name)
    rules = _read_strategy_rules(name)
    today = datetime.now(timezone(timedelta(hours=-4))).date()
    offset = (page - 1) * per_page

    with get_db() as conn:
        # Match /portfolio's canonical filter: every non-live row regardless
        # of execution_source. Simulated rows (from strategy_simulator) and
        # alert rows (from cw_runner alert_only) are both "what the strategy
        # thinks it has" — the operator needs to see both. Each row carries
        # execution_source so the UI can label its provenance.
        open_rows = conn.execute(
            """SELECT id, ticker, entry_date, entry_price, shares, dollar_amount,
                      insider_name, insider_title, signal_grade, trade_id,
                      planned_exit_date, execution_source, portfolio_value
                 FROM strategy_portfolio
                WHERE strategy = ?
                  AND COALESCE(is_live, false) = false
                  AND status = 'open'
                ORDER BY entry_date DESC, id DESC""",
            (name,),
        ).fetchall()

        total_closed_row = conn.execute(
            """SELECT COUNT(*) AS n
                 FROM strategy_portfolio
                WHERE strategy = ?
                  AND COALESCE(is_live, false) = false
                  AND status = 'closed'""",
            (name,),
        ).fetchone()
        total_closed = int(total_closed_row["n"]) if total_closed_row else 0

        closed_rows = conn.execute(
            f"""SELECT id, ticker, entry_date, entry_price, exit_date, exit_price,
                       shares, dollar_amount, insider_name, insider_title,
                       signal_grade, trade_id, pnl_pct, pnl_dollar, exit_reason,
                       hold_days, execution_source, portfolio_value
                  FROM strategy_portfolio
                 WHERE strategy = ?
                   AND COALESCE(is_live, false) = false
                   AND status = 'closed'
                 ORDER BY exit_date DESC NULLS LAST, entry_date DESC, id DESC
                 LIMIT {int(per_page)} OFFSET {int(offset)}""",
            (name,),
        ).fetchall()

        # Fetch live prices for open positions
        open_tickers = list({r["ticker"] for r in open_rows if r["ticker"]})
        price_map = _fetch_current_prices(name, open_tickers, conn) if open_tickers else {}

        open_positions = _build_position_rows(open_rows, name, price_map, today, is_closed=False, rules=rules)
        closed_positions = _build_position_rows(closed_rows, name, {}, today, is_closed=True, rules=rules)

    # Roll-ups
    open_total_pnl = sum((p["unrealized_pnl_dollar"] or 0.0) for p in open_positions)
    open_total_cost = sum((p["dollar_amount"] or 0.0) for p in open_positions)
    return {
        "strategy": name,
        "rules": rules,
        "open": {
            "rows": open_positions,
            "count": len(open_positions),
            "total_cost": round(open_total_cost, 2),
            "total_unrealized_pnl_dollar": round(open_total_pnl, 2),
            "total_unrealized_pnl_pct": (open_total_pnl / open_total_cost) if open_total_cost > 0 else None,
        },
        "closed": {
            "rows": closed_positions,
            "page": page,
            "per_page": per_page,
            "total": total_closed,
        },
    }


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


# ── Pipeline runs (Stage 2.5 — structured batch-job telemetry) ──────────────


@router.get("/pipelines")
def pipelines_status(
    limit: int = Query(default=200, ge=1, le=1000),
    service: Optional[str] = Query(default=None),
    user: UserContext = Depends(require_admin),
) -> dict:
    """Structured pipeline run history from the pipeline_runs table.

    Distinct from /jobs (which infers status from launchd log mtimes —
    fragile). A service starts logging here as soon as it adopts
    `framework.observability.pipeline_run()`; until then, its history is
    empty here and /jobs is still the source of truth.

    Returns:
        - per-service summary: last run, last success, recent failure count,
          24h run count, last duration
        - recent runs (most recent N across all services)
    """
    where = "WHERE service = ?" if service else ""
    params = (service,) if service else ()

    with get_db() as conn:
        # Per-service summary
        summary_rows = conn.execute(
            f"""WITH ranked AS (
                    SELECT service, started_at, ended_at, status, duration_ms,
                           rows_written, error_message,
                           ROW_NUMBER() OVER (PARTITION BY service ORDER BY started_at DESC) AS rn
                    FROM pipeline_runs
                    {where}
                )
                SELECT
                    service,
                    MAX(started_at)                                    AS last_run,
                    MAX(started_at) FILTER (WHERE status = 'ok')       AS last_success,
                    MAX(started_at) FILTER (WHERE status = 'failed')   AS last_failure,
                    COUNT(*) FILTER (WHERE started_at > NOW() - INTERVAL '24 hours')                       AS runs_24h,
                    COUNT(*) FILTER (WHERE started_at > NOW() - INTERVAL '24 hours' AND status = 'failed') AS failures_24h,
                    MAX(duration_ms)  FILTER (WHERE rn = 1)            AS last_duration_ms,
                    MAX(status)       FILTER (WHERE rn = 1)            AS last_status,
                    MAX(rows_written) FILTER (WHERE rn = 1)            AS last_rows_written,
                    MAX(error_message) FILTER (WHERE rn = 1)           AS last_error
                FROM ranked
                GROUP BY service
                ORDER BY MAX(started_at) DESC""",
            params,
        ).fetchall()

        recent_rows = conn.execute(
            f"""SELECT id, service, started_at, ended_at, duration_ms, status,
                       exit_code, rows_written, rows_deleted, error_message,
                       metadata, host, log_path, run_uuid::text AS run_uuid
                FROM pipeline_runs
                {where}
                ORDER BY started_at DESC
                LIMIT ?""",
            params + (limit,),
        ).fetchall()

    services = [dict(r) for r in summary_rows]
    runs = [dict(r) for r in recent_rows]
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "n_services": len(services),
        "services": services,
        "n_runs": len(runs),
        "runs": runs,
    }


# Drift audit endpoint removed 2026-06-07. It read from strategy_drift_audit
# which was populated by scripts/drift_detector.py comparing sim_portfolio
# vs paper_trades — two tables that never received post-backfill writes.
# Every drift event was a false positive against a stale snapshot.
# Consolidated to strategy_portfolio as the single source; see migration
# 2026-06-07_consolidate_to_strategy_portfolio.sql.
