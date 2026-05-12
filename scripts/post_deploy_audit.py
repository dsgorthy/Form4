#!/usr/bin/env python3
"""Post-deploy audit (one-shot, scheduled to run Monday 2026-05-11 14:00 PT).

Verifies that the Phase 0 + Phase 1 + Phase 2 (days 3a/3b/3c) reliability
work deployed Friday 2026-05-08 is functioning on the first market day.

Sections in the report:
  1. Top-level verdict (PASS / FAIL / DEGRADED)
  2. Strategy heartbeats — all three runners alive + sleeping correctly
  3. Freshness contracts — all 15 GREEN, signal_freshness writes flowing
  4. Today's trading activity — candidates evaluated, orders submitted, fills
  5. Capacity audit (3a) — execute_entries audit rows landed?
  6. OMS V2 status (3b/3c) — flag state + V2 row counts if enabled
  7. Critical alerts since deploy
  8. Probe health — alpaca-reconcile, freshness-probe, heartbeat-probe,
     candidate-count-probe
  9. Open issues / next steps

Output:
  - HTML email to derek.gorthy@gmail.com via Resend (api.email.send_email)
  - HTML + JSON dump to logs/post-deploy-audit-{date}.{html,json}

Designed to run on Studio (has DB + logs + launchd state). Reads only;
makes no state changes.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection
from api.email import send_email

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STRATEGIES = ["quality_momentum", "reversal_dip", "tenb51_surprise"]
DEPLOY_DATE = "2026-05-08"  # Friday's deploy date — used for "since deploy" filters
TO_EMAIL = os.getenv("POST_DEPLOY_AUDIT_TO", "derek.gorthy@gmail.com")
TODAY_PT = datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()


# ── Section dataclasses ────────────────────────────────────────────────────


@dataclass
class Finding:
    """A single audit finding — passes or has details to surface."""
    name: str
    status: str  # 'pass' | 'warn' | 'fail' | 'info'
    detail: str = ""
    items: list[str] = field(default_factory=list)


@dataclass
class Section:
    title: str
    findings: list[Finding] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(f.status == "fail" for f in self.findings):
            return "fail"
        if any(f.status == "warn" for f in self.findings):
            return "warn"
        return "pass"


# ── Section 1: Heartbeats ───────────────────────────────────────────────────


def audit_heartbeats(conn) -> Section:
    s = Section("1. Strategy heartbeats")
    now = datetime.now(timezone.utc)
    for strat in STRATEGIES:
        path = REPO / "strategies" / "cw_strategies" / "data" / f"{strat}_heartbeat.json"
        if not path.exists():
            s.findings.append(Finding(strat, "fail", "heartbeat file missing"))
            continue
        try:
            d = json.loads(path.read_text())
            ts_str = d.get("timestamp", "")
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = (now - ts).total_seconds() / 60
            status_str = d.get("status", "?")
            mode = d.get("mode", "?")
            if age_min > 120:  # >2h is concerning during market hours
                s.findings.append(Finding(
                    strat, "warn",
                    f"heartbeat age={age_min:.0f}min status={status_str} mode={mode}"
                ))
            else:
                s.findings.append(Finding(
                    strat, "pass",
                    f"age={age_min:.0f}min status={status_str} mode={mode}"
                ))
        except Exception as e:
            s.findings.append(Finding(strat, "fail", f"heartbeat parse error: {e}"))
    return s


# ── Section 2: Freshness contracts ──────────────────────────────────────────


def audit_freshness(conn) -> Section:
    s = Section("2. Freshness contracts")
    try:
        from framework.contracts.freshness import FreshnessRegistry, get_freshness
        registry = FreshnessRegistry.get()
        n_stale = 0
        n_unknown = 0
        for c in registry.all():
            ts, age_h = get_freshness(conn, c.table, c.column)
            label = f"{c.table}.{c.column}"
            if ts is None:
                s.findings.append(Finding(label, "fail", "no signal_freshness row"))
                n_unknown += 1
            elif age_h > c.max_staleness_hours:
                s.findings.append(Finding(
                    label, "fail",
                    f"age={age_h:.1f}h > SLA={c.max_staleness_hours:.1f}h"
                ))
                n_stale += 1
            else:
                s.findings.append(Finding(
                    label, "pass",
                    f"age={age_h:.1f}h / SLA={c.max_staleness_hours:.1f}h"
                ))
    except Exception as e:
        s.findings.append(Finding("freshness_check", "fail", f"error: {e}"))

    # Did refresh-features run today at the new 06:00 PT time?
    #
    # NOTE: cannot use MAX(last_computed_at) on signal_freshness here —
    # compute_cw_indicators.py is also called by fetch_latest._run_indicators
    # every 5 min on insider-fetch cycles. Both writers populate the SAME
    # signal_freshness rows, so MAX returns the LATER (fetch-driven) write
    # and falsely reports "refresh-features ran at HH:MM" when it was
    # actually fetch_latest. (This bug produced a misleading WARN in the
    # 2026-05-11 audit.)
    #
    # Reliable approach: parse refresh-features.log for the start/done
    # banner lines, which only refresh_features_daily.sh writes.
    try:
        log_path = REPO / "logs" / "refresh-features.log"
        if not log_path.exists():
            s.findings.append(Finding(
                "refresh-features schedule", "fail",
                "refresh-features.log not found",
            ))
        else:
            text = log_path.read_text()
            # Grab the most recent "===== refresh-features done at <ts> ====="
            import re
            matches = re.findall(
                r"===== refresh-features done at (.+?) =====", text,
            )
            if not matches:
                s.findings.append(Finding(
                    "refresh-features schedule", "fail",
                    "no completion banners in refresh-features.log",
                ))
            else:
                last_done_str = matches[-1].strip()
                # Format example: "Mon May 11 06:03:24 PDT 2026"
                try:
                    last_done = datetime.strptime(
                        last_done_str, "%a %b %d %H:%M:%S %Z %Y",
                    )
                    # %Z may not parse PDT correctly on Linux; manually set if PT
                    if "PDT" in last_done_str or "PST" in last_done_str:
                        last_done = last_done.replace(
                            tzinfo=ZoneInfo("America/Los_Angeles"),
                        )
                except Exception:
                    last_done = None
                if last_done is None:
                    s.findings.append(Finding(
                        "refresh-features schedule", "warn",
                        f"parse failed for: {last_done_str!r}",
                    ))
                else:
                    last_done_pt = last_done.astimezone(
                        ZoneInfo("America/Los_Angeles"),
                    )
                    ran_today = last_done_pt.date().isoformat() == TODAY_PT
                    target_hour = 6  # 06:00 PT scheduled
                    if ran_today and last_done_pt.hour < 8:
                        s.findings.append(Finding(
                            "refresh-features schedule", "pass",
                            f"completed today at {last_done_pt.strftime('%H:%M PT')} "
                            f"(target: ~06:00 PT)",
                        ))
                    elif ran_today:
                        s.findings.append(Finding(
                            "refresh-features schedule", "warn",
                            f"completed today at {last_done_pt.strftime('%H:%M PT')} "
                            f"— expected ~06:00 PT",
                        ))
                    else:
                        s.findings.append(Finding(
                            "refresh-features schedule", "fail",
                            f"did NOT complete today; last completion "
                            f"{last_done_pt.isoformat()}",
                        ))
    except Exception as e:
        s.findings.append(Finding("refresh_features_schedule", "fail", str(e)))

    return s


# ── Section 3: Today's trading activity ─────────────────────────────────────


def audit_trading_activity(conn) -> Section:
    s = Section("3. Today's trading activity")
    for strat in STRATEGIES:
        # Decisions today
        try:
            cur = conn.execute("""
                SELECT stage, passed, COUNT(*) AS n
                FROM trade_decision_audit
                WHERE strategy = ?
                  AND ts >= NOW() - INTERVAL '24 hours'
                GROUP BY 1, 2 ORDER BY 1, 2
            """, (strat,))
            stages = cur.fetchall()
        except Exception as e:
            s.findings.append(Finding(strat, "fail", f"decision_audit query error: {e}"))
            continue

        if not stages:
            s.findings.append(Finding(
                strat, "warn",
                "no trade_decision_audit rows in last 24h — strategy may not have scanned"
            ))
            continue

        breakdown = " | ".join(
            f"{r['stage']}({'pass' if r['passed'] else 'rej'})={r['n']}"
            for r in stages
        )
        # Did any candidate make it to 'final' (entered) or 'capacity' rejection?
        final_passed = sum(r["n"] for r in stages
                           if r["stage"] == "final" and r["passed"])
        capacity_rejected = sum(r["n"] for r in stages
                                if r["stage"] == "capacity" and not r["passed"])
        s.findings.append(Finding(
            strat,
            "pass" if final_passed > 0 else "info",
            f"{final_passed} entered, {capacity_rejected} capacity-rejected | {breakdown}"
        ))

        # Orders submitted today
        try:
            cur = conn.execute("""
                SELECT fill_status, COUNT(*) AS n
                FROM order_audit
                WHERE strategy = ?
                  AND decided_at >= NOW() - INTERVAL '24 hours'
                GROUP BY 1 ORDER BY 1
            """, (strat,))
            orders = cur.fetchall()
            if orders:
                summary = " | ".join(f"{r['fill_status']}={r['n']}" for r in orders)
                s.findings.append(Finding(
                    f"  {strat} orders", "pass", summary
                ))
        except Exception:
            pass

    return s


# ── Section 4: Capacity audit (P2 day 3a) ───────────────────────────────────


def audit_capacity_stage(conn) -> Section:
    s = Section("4. Capacity-stage audit (day 3a)")
    try:
        cur = conn.execute("""
            SELECT strategy, stage, passed, reason, COUNT(*) AS n
            FROM trade_decision_audit
            WHERE ts >= NOW() - INTERVAL '24 hours'
              AND stage IN ('capacity', 'risk', 'final')
            GROUP BY 1, 2, 3, 4
            ORDER BY 1, 2, 3
        """)
        rows = cur.fetchall()
        if not rows:
            s.findings.append(Finding(
                "capacity_audit_rows",
                "warn",
                "0 capacity/risk/final rows in last 24h — execute_entries may not have run"
            ))
        else:
            for r in rows:
                pf = "pass" if r["passed"] else "rej"
                s.findings.append(Finding(
                    f"{r['strategy']} {r['stage']}({pf})",
                    "pass",
                    f"{r['n']}× {r['reason'] or '(no reason)'}"
                ))
    except Exception as e:
        s.findings.append(Finding("capacity_audit_query", "fail", str(e)))
    return s


# ── Section 5: OMS V2 (P2 day 3b/3c) ────────────────────────────────────────


def audit_oms_v2(conn) -> Section:
    s = Section("5. OMS V2 (day 3b/3c)")
    enabled = os.getenv("OMS_V2", "").lower() in ("1", "true", "yes", "on")
    s.findings.append(Finding(
        "OMS_V2 env var",
        "info" if enabled else "info",
        "ENABLED" if enabled else "disabled (V1 path active by default)"
    ))

    # Count V1 vs V2 order rows in last 24h
    try:
        cur = conn.execute("""
            SELECT
                COUNT(*) FILTER (WHERE order_id LIKE 'f4_%') AS v2_count,
                COUNT(*) FILTER (WHERE order_id NOT LIKE 'f4_%') AS v1_count
            FROM order_audit
            WHERE decided_at >= NOW() - INTERVAL '24 hours'
        """)
        row = cur.fetchone()
        if row:
            v2, v1 = row["v2_count"] or 0, row["v1_count"] or 0
            s.findings.append(Finding(
                "order_audit row split",
                "pass",
                f"V1 (uuid)={v1}, V2 (f4_*)={v2}"
            ))
            if enabled and v2 == 0 and v1 > 0:
                s.findings.append(Finding(
                    "v2_active_but_no_rows",
                    "warn",
                    "OMS_V2 enabled but no V2 rows — candidates may lack decision_id"
                ))
    except Exception as e:
        s.findings.append(Finding("oms_v2_query", "fail", str(e)))

    return s


# ── Section 6: Critical alerts since deploy ─────────────────────────────────


def audit_alerts() -> Section:
    s = Section("6. Critical alerts since deploy (2026-05-08 21:00 PT)")
    log = REPO / "logs" / "alerts.ndjson"
    if not log.exists():
        s.findings.append(Finding("alerts.ndjson", "fail", "log file not found"))
        return s

    deploy_threshold = datetime(2026, 5, 9, 4, 0, tzinfo=timezone.utc)
    by_component: dict[str, int] = {}
    by_message: dict[str, int] = {}
    n_critical = 0

    for line in log.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("severity") != "critical":
            continue
        ts_str = e.get("ts", "").replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            continue
        if ts < deploy_threshold:
            continue
        n_critical += 1
        by_component[e.get("component", "?")] = by_component.get(
            e.get("component", "?"), 0) + 1
        msg_head = (e.get("message", "") or "")[:60]
        by_message[msg_head] = by_message.get(msg_head, 0) + 1

    if n_critical == 0:
        s.findings.append(Finding(
            "critical alerts",
            "pass",
            "0 critical alerts since deploy — clean weekend"
        ))
    else:
        s.findings.append(Finding(
            "critical alerts",
            "warn" if n_critical < 10 else "fail",
            f"{n_critical} critical alerts since deploy"
        ))
        for comp, count in sorted(by_component.items(), key=lambda x: -x[1])[:10]:
            s.findings.append(Finding(
                f"  {comp}", "info", f"{count}×"
            ))
    return s


# ── Section 7: Probe health ─────────────────────────────────────────────────


def audit_probes() -> Section:
    s = Section("7. Probe / monitor health")
    probes = [
        ("alpaca-reconcile",  "com.openclaw.alpaca-reconcile"),
        ("freshness-probe",   "com.openclaw.freshness-probe"),
        ("heartbeat-probe",   "com.openclaw.heartbeat-probe"),
        ("candidate-count",   "com.openclaw.candidate-count-probe"),
        ("daily-summary",     "com.openclaw.daily-summary"),
        ("insider-fetch",     "com.openclaw.insider-fetch"),
        ("refresh-features",  "com.openclaw.refresh-features"),
    ]
    for name, label in probes:
        try:
            uid = subprocess.run(["id", "-u"], capture_output=True, text=True).stdout.strip()
            out = subprocess.run(
                ["launchctl", "print", f"gui/{uid}/{label}"],
                capture_output=True, text=True, timeout=5,
            ).stdout
        except Exception as e:
            s.findings.append(Finding(name, "fail", f"launchctl error: {e}"))
            continue

        state = "?"
        last_exit = "?"
        runs = "?"
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("state ="):
                state = line.split("=", 1)[1].strip()
            elif line.startswith("last exit code ="):
                last_exit = line.split("=", 1)[1].strip()
            elif line.startswith("runs ="):
                runs = line.split("=", 1)[1].strip()

        status = "pass"
        if last_exit not in ("?", "0") and last_exit != "(never run)":
            status = "warn"
        s.findings.append(Finding(
            name, status,
            f"state={state} runs={runs} last_exit={last_exit}"
        ))
    return s


# ── HTML rendering ──────────────────────────────────────────────────────────


COLORS = {
    "pass": "#22C55E",
    "warn": "#F59E0B",
    "fail": "#EF4444",
    "info": "#8888A0",
}


def render_section_html(s: Section) -> str:
    rows = []
    for f in s.findings:
        color = COLORS.get(f.status, "#8888A0")
        items_html = ""
        if f.items:
            items_html = "<ul style='margin:4px 0 0 16px;font-size:12px;color:#8888A0;'>"
            for item in f.items[:10]:
                items_html += f"<li>{item}</li>"
            items_html += "</ul>"
        rows.append(f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #2A2A3A;
                     font-size:13px;color:{color};font-weight:600;width:80px;">
            {f.status.upper()}
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #2A2A3A;
                     font-size:13px;color:#E8E8ED;">
            <strong>{f.name}</strong>
            <div style="font-size:12px;color:#8888A0;margin-top:2px;">{f.detail}</div>
            {items_html}
          </td>
        </tr>
        """)
    section_color = COLORS.get(s.status, "#8888A0")
    return f"""
    <div style="margin:24px 0;background:#12121A;border:1px solid #2A2A3A;
                border-radius:8px;overflow:hidden;">
      <div style="padding:12px 16px;background:#1A1A26;border-bottom:1px solid #2A2A3A;">
        <span style="font-size:14px;font-weight:600;color:#E8E8ED;">{s.title}</span>
        <span style="float:right;font-size:12px;color:{section_color};font-weight:600;">
          {s.status.upper()}
        </span>
      </div>
      <table style="width:100%;border-collapse:collapse;">
        {''.join(rows)}
      </table>
    </div>
    """


def render_html(sections: list[Section], overall_status: str) -> str:
    overall_color = COLORS.get(overall_status, "#8888A0")
    body_html = "\n".join(render_section_html(s) for s in sections)
    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
                max-width:780px;margin:0 auto;padding:24px;background:#0A0A0F;color:#E8E8ED;">
      <div style="margin-bottom:16px;">
        <span style="font-size:22px;font-weight:bold;color:#E8E8ED;">
          Form<span style="color:#3B82F6;">4</span>
        </span>
        <span style="margin-left:12px;font-size:14px;color:#8888A0;">
          Post-deploy audit · {TODAY_PT}
        </span>
      </div>
      <div style="padding:16px 20px;background:#12121A;border:2px solid {overall_color};
                  border-radius:8px;margin-bottom:8px;">
        <div style="font-size:12px;color:#8888A0;margin-bottom:4px;">
          Overall verdict
        </div>
        <div style="font-size:20px;font-weight:700;color:{overall_color};">
          {overall_status.upper()}
        </div>
        <div style="font-size:13px;color:#8888A0;margin-top:8px;line-height:1.5;">
          Verifying P0 (freshness contracts) + P1 (pre-deploy gate) + P2 days 3a-3c
          (audit + OMS V2) deployed Friday 2026-05-08. Generated automatically by
          scripts/post_deploy_audit.py running on Studio.
        </div>
      </div>
      {body_html}
      <div style="margin-top:24px;padding-top:16px;border-top:1px solid #2A2A3A;
                  font-size:11px;color:#55556A;text-align:center;">
        Form4 / scripts/post_deploy_audit.py · one-shot scheduled run
      </div>
    </div>
    """


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Render to stdout, no email")
    p.add_argument("--to", default=TO_EMAIL)
    args = p.parse_args()

    logger.info("Post-deploy audit starting (today_pt=%s)", TODAY_PT)
    conn = get_connection(readonly=True)

    sections = [
        audit_heartbeats(conn),
        audit_freshness(conn),
        audit_trading_activity(conn),
        audit_capacity_stage(conn),
        audit_oms_v2(conn),
        audit_alerts(),
        audit_probes(),
    ]
    conn.close()

    # Compute overall verdict
    section_statuses = [s.status for s in sections]
    if "fail" in section_statuses:
        overall = "fail"
    elif "warn" in section_statuses:
        overall = "warn"
    else:
        overall = "pass"

    html = render_html(sections, overall)

    # Persist locally
    out_dir = REPO / "logs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / f"post-deploy-audit-{TODAY_PT}.html"
    out_html.write_text(html)
    out_json = out_dir / f"post-deploy-audit-{TODAY_PT}.json"
    out_json.write_text(json.dumps({
        "today_pt": TODAY_PT,
        "overall": overall,
        "sections": [
            {
                "title": s.title,
                "status": s.status,
                "findings": [
                    {"name": f.name, "status": f.status, "detail": f.detail,
                     "items": f.items}
                    for f in s.findings
                ],
            }
            for s in sections
        ],
    }, indent=2, default=str))
    logger.info("Wrote %s and %s", out_html, out_json)

    if args.dry_run:
        print(html)
        return

    subject = f"[Form4] Post-deploy audit · {TODAY_PT} · {overall.upper()}"
    ok = send_email(args.to, subject, html)
    if ok:
        logger.info("Sent post-deploy audit to %s (verdict: %s)", args.to, overall)
    else:
        logger.error("Email send failed (HTML still saved at %s)", out_html)


if __name__ == "__main__":
    main()
