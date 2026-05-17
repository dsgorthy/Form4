#!/usr/bin/env python3
"""Monday-morning paper-trading health monitor.

Fires once on Monday 07:30 PT (about 1 hour after the 06:25 PT cw_runner
pre-market wake + 06:00 PT refresh-features chain). Runs 5 verification
checks that confirm the weekend's pipelines fired cleanly and the live
paper runners are ready for the trading day. Writes structured alerts to
`logs/alerts.ndjson` (component=`monday_paper_monitor`) and a standalone
report file at `logs/monday_paper_monitor_YYYY-MM-DD.log`.

The 14:30 PT daily-summary plist picks up these alerts and surfaces them
in the daily summary email's "Monday paper monitor" section.

Checks (each ALERT-routed; severity in parens):
  1. Heartbeats fresh                  (warn on stale)
  2. Refresh-features chain ran clean  (critical on missing column today)
  3. Writer-registry runtime preflight (critical on mismatch)
  4. No unexpected critical alerts     (warn on any new critical)
  5. QM scan produced audit rows       (warn on zero rows)

Exit code 0 if all checks PASS, 1 if any FAIL. Runs on Studio only (needs
psql access to form4 and read access to /Users/derekg/trading-framework/).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection
from framework.alerts.log import info as alert_info, warn as alert_warn, critical as alert_critical

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

STRATEGIES = ["quality_momentum", "reversal_dip", "tenb51_surprise"]
HEARTBEAT_MAX_AGE_MIN = 30   # cw_runner writes heartbeat every cycle
ALERT_LOG = REPO / "logs" / "alerts.ndjson"
DEPLOY_COMMIT_UTC = "2026-05-17T07:00:00+00:00"   # Phase 2 deploy reference
# Components whose critical alerts during the deploy window are expected
# (transient docker restart blips, etc.) and shouldn't be flagged.
DEPLOY_NOISE_COMPONENTS = {"uptime_monitor"}


@dataclass
class CheckResult:
    name: str
    ok: bool
    severity: str  # "info" | "warn" | "critical"
    detail: str
    extra: dict = field(default_factory=dict)


# ── Check 1: heartbeats fresh ────────────────────────────────────────────


def check_heartbeats() -> CheckResult:
    data_dir = REPO / "strategies" / "cw_strategies" / "data"
    now = datetime.now(timezone.utc)
    stale: list[str] = []
    parsed: dict[str, dict] = {}
    for strategy in STRATEGIES:
        hb_path = data_dir / f"{strategy}_heartbeat.json"
        if not hb_path.exists():
            stale.append(f"{strategy}: heartbeat file missing")
            continue
        try:
            hb = json.loads(hb_path.read_text())
        except Exception as exc:
            stale.append(f"{strategy}: heartbeat unreadable: {exc}")
            continue
        try:
            ts = datetime.fromisoformat(hb["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            stale.append(f"{strategy}: heartbeat timestamp unparseable: {hb.get('timestamp')!r}")
            continue
        age_min = (now - ts).total_seconds() / 60
        parsed[strategy] = {
            "age_min": round(age_min, 1),
            "status": hb.get("status"),
            "pid": hb.get("pid"),
        }
        if age_min > HEARTBEAT_MAX_AGE_MIN:
            stale.append(f"{strategy}: heartbeat {age_min:.1f}min stale (status={hb.get('status')})")
    if stale:
        return CheckResult(
            name="heartbeats_fresh", ok=False, severity="warn",
            detail="; ".join(stale),
            extra={"heartbeats": parsed},
        )
    return CheckResult(
        name="heartbeats_fresh", ok=True, severity="info",
        detail=f"all 3 fresh (max age {max(p['age_min'] for p in parsed.values()):.1f}min)",
        extra={"heartbeats": parsed},
    )


# ── Check 2: refresh-features chain ran clean today ──────────────────────


REFRESH_COLUMNS = [
    # (table, column, populated_by — for diagnostic)
    ("trades", "career_grade", "compute_career_grades.py"),
    ("trades", "is_rare_reversal", "compute_switch_rate.py"),
    ("trades", "week52_proximity", "compute_week52_proximity.py"),
    ("insider_track_records", "score", "pit_scoring.py"),
    ("trades", "pit_grade", "backfill_pit_grades.py"),
    ("trades", "pit_cluster_size", "compute_pit_clusters.py"),
    ("trades", "above_sma50", "compute_cw_indicators.py"),
]


def check_refresh_features_chain() -> CheckResult:
    today_pt = (datetime.now(timezone.utc) - timedelta(hours=8)).date().isoformat()
    conn = get_connection()
    missing: list[str] = []
    fresh: list[str] = []
    try:
        for table, column, _writer in REFRESH_COLUMNS:
            if "." in table:
                schema, table_name = table.split(".", 1)
            else:
                schema, table_name = "public", table
            row = conn.execute(
                """SELECT last_computed_at::text, populated_by
                     FROM signal_freshness
                    WHERE source = ? AND table_name = ? AND column_name = ?
                    ORDER BY last_computed_at DESC LIMIT 1""",
                (schema, table_name, column),
            ).fetchone()
            if not row:
                missing.append(f"{table}.{column}: no signal_freshness row")
                continue
            last_at = row[0]
            populated_by = row[1]
            try:
                ts = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:
                missing.append(f"{table}.{column}: unparseable last_computed_at={last_at!r}")
                continue
            age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age_h > 8:
                missing.append(f"{table}.{column}: {age_h:.1f}h stale (populated_by={populated_by!r})")
            else:
                fresh.append(f"{table}.{column}={age_h:.1f}h ago")
    finally:
        conn.close()
    if missing:
        return CheckResult(
            name="refresh_features_chain", ok=False, severity="critical",
            detail=f"{len(missing)} contracted column(s) NOT fresh today: " + "; ".join(missing[:5]),
            extra={"missing": missing, "fresh": fresh, "today_pt": today_pt},
        )
    return CheckResult(
        name="refresh_features_chain", ok=True, severity="info",
        detail=f"all {len(REFRESH_COLUMNS)} contracted columns fresh today (PT {today_pt})",
        extra={"fresh": fresh},
    )


# ── Check 3: writer-registry runtime preflight ───────────────────────────


def check_writer_registry_runtime() -> CheckResult:
    from framework.contracts.freshness import assert_all_writers_wired_for_strategy
    from framework.contracts.exceptions import WriterMismatchError
    conn = get_connection()
    failures: list[str] = []
    try:
        for s in STRATEGIES:
            try:
                assert_all_writers_wired_for_strategy(conn, s)
            except WriterMismatchError as e:
                failures.append(
                    f"{s}: {e.table}.{e.column} registry={e.registered_script} "
                    f"observed={e.observed_populated_by}"
                )
    finally:
        conn.close()
    if failures:
        return CheckResult(
            name="writer_registry_runtime", ok=False, severity="critical",
            detail="; ".join(failures),
            extra={"failures": failures},
        )
    return CheckResult(
        name="writer_registry_runtime", ok=True, severity="info",
        detail=f"all {len(STRATEGIES)} strategies pass assert_all_writers_wired",
    )


# ── Check 4: no unexpected critical alerts since deploy ──────────────────


def check_unexpected_critical_alerts() -> CheckResult:
    if not ALERT_LOG.exists():
        return CheckResult(
            name="unexpected_criticals", ok=True, severity="info",
            detail="alerts.ndjson empty/missing",
        )
    cutoff = datetime.fromisoformat(DEPLOY_COMMIT_UTC)
    unexpected: list[dict] = []
    own_criticals: list[dict] = []
    with open(ALERT_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("severity") != "critical":
                continue
            try:
                ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            except Exception:
                continue
            if ts < cutoff:
                continue
            component = e.get("component", "")
            if component == "monday_paper_monitor":
                own_criticals.append(e)
                continue
            if component in DEPLOY_NOISE_COMPONENTS:
                continue
            unexpected.append(e)
    if unexpected:
        # Surface the first few as detail; remaining counts only.
        head = "; ".join(
            f"{e.get('component')}: {(e.get('message') or '')[:80]}"
            for e in unexpected[:3]
        )
        return CheckResult(
            name="unexpected_criticals", ok=False, severity="warn",
            detail=f"{len(unexpected)} critical alert(s) since deploy {DEPLOY_COMMIT_UTC} — {head}",
            extra={"unexpected_components": sorted({e.get("component", "") for e in unexpected})},
        )
    return CheckResult(
        name="unexpected_criticals", ok=True, severity="info",
        detail=f"0 unexpected critical alerts since deploy {DEPLOY_COMMIT_UTC}",
    )


# ── Check 5: QM scan produced audit rows today ───────────────────────────


def check_qm_scan_today() -> CheckResult:
    """Today's QM scan should produce >0 rows in trade_decision_audit.
    Even if no candidate passes the filter, the dedup/pit_lookup audit
    rows confirm the scan ran. Zero rows = freshness halt or runner down."""
    today_pt = (datetime.now(timezone.utc) - timedelta(hours=8)).date().isoformat()
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT COUNT(*) AS n, COUNT(DISTINCT stage) AS n_stages,
                      COUNT(*) FILTER (WHERE passed AND stage = 'conviction') AS n_passed_conviction
                 FROM trade_decision_audit
                WHERE strategy = ?
                  AND ts::date = CURRENT_DATE
                  AND source IN ('live', 'simulation')""",
            ("quality_momentum",),
        ).fetchone()
        if row is None:
            n_audit = 0
            n_stages = 0
            n_passed = 0
        else:
            n_audit = int(row[0] or 0)
            n_stages = int(row[1] or 0)
            n_passed = int(row[2] or 0)
    finally:
        conn.close()
    if n_audit == 0:
        return CheckResult(
            name="qm_scan_today", ok=False, severity="warn",
            detail=f"trade_decision_audit has 0 rows for quality_momentum today (PT {today_pt}) "
                   f"— preflight likely halted; check refresh_features_chain finding",
        )
    return CheckResult(
        name="qm_scan_today", ok=True, severity="info",
        detail=f"QM produced {n_audit} audit row(s) across {n_stages} stage(s); "
               f"{n_passed} cleared conviction (PT {today_pt})",
        extra={"n_audit_rows": n_audit, "n_stages": n_stages, "n_passed_conviction": n_passed},
    )


# ── Orchestration ────────────────────────────────────────────────────────


CHECKS = [
    check_heartbeats,
    check_refresh_features_chain,
    check_writer_registry_runtime,
    check_unexpected_critical_alerts,
    check_qm_scan_today,
]


def _emit_alert(r: CheckResult) -> None:
    """Route the CheckResult to the alerts.ndjson log."""
    if r.severity == "critical":
        alert_critical("monday_paper_monitor", f"FAIL {r.name}: {r.detail}", check=r.name)
    elif r.severity == "warn":
        alert_warn("monday_paper_monitor", f"FAIL {r.name}: {r.detail}", check=r.name)
    else:
        alert_info("monday_paper_monitor", f"PASS {r.name}: {r.detail}", check=r.name)


def _write_report(results: list[CheckResult]) -> Path:
    today = date.today().isoformat()
    path = REPO / "logs" / f"monday_paper_monitor_{today}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"Monday paper-trading monitor — {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "=" * 72,
        "",
    ]
    for r in results:
        marker = "PASS" if r.ok else "FAIL"
        lines.append(f"[{marker}/{r.severity}] {r.name}")
        lines.append(f"  {r.detail}")
        if r.extra:
            lines.append(f"  extra: {json.dumps(r.extra, separators=(',', ':'))}")
        lines.append("")
    n_pass = sum(1 for r in results if r.ok)
    n_fail = len(results) - n_pass
    lines.append(f"Result: {n_pass}/{len(results)} pass, {n_fail} fail")
    path.write_text("\n".join(lines))
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-alert", action="store_true",
                        help="Skip writing alerts.ndjson (still writes the report file)")
    parser.add_argument("--json", action="store_true",
                        help="Emit JSON to stdout for CI / smoke tests")
    args = parser.parse_args()

    logger.info("Monday paper monitor — %d checks", len(CHECKS))
    results: list[CheckResult] = []
    for fn in CHECKS:
        try:
            r = fn()
        except Exception as exc:
            logger.exception("check %s failed", fn.__name__)
            r = CheckResult(
                name=fn.__name__, ok=False, severity="critical",
                detail=f"check raised: {exc!r}",
            )
        results.append(r)
        if not args.no_alert:
            _emit_alert(r)
        logger.info("[%s] %s — %s",
                    "PASS" if r.ok else "FAIL", r.name, r.detail[:120])

    report_path = _write_report(results)
    logger.info("report written: %s", report_path)

    if args.json:
        print(json.dumps(
            [{"name": r.name, "ok": r.ok, "severity": r.severity,
              "detail": r.detail, "extra": r.extra}
             for r in results], indent=2,
        ))

    n_fail = sum(1 for r in results if not r.ok)
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
