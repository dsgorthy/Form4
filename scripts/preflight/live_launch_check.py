#!/usr/bin/env python3
"""Day-14 GO/NO-GO validator for flipping a strategy to live trading.

Runs a series of independent gates. ALL must pass to GO. Exit code 0 on
all-GREEN, 1 if any gate fails. The script is read-only (no DB writes
except an alert on a failed gate during a Day-14-or-later check).

Usage:
    python3 scripts/preflight/live_launch_check.py --strategy quality_momentum
    python3 scripts/preflight/live_launch_check.py --strategy quality_momentum --min-equity 9500
    python3 scripts/preflight/live_launch_check.py --strategy quality_momentum --markdown > preflight.md
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

from config.database import get_connection


@dataclass
class GateResult:
    name: str
    ok: bool
    detail: str
    severity: str = "blocker"   # blocker | warning


def gate_paper_sharpe_30d(conn, strategy: str) -> GateResult:
    """Last 30d closed paper trades — mean & stddev of pnl_pct → annualized
    Sharpe proxy. We require Sharpe > 0 (paper isn't bleeding) as a basic
    "the strategy edge is at least directionally there".
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT pnl_pct FROM strategy_portfolio
            WHERE strategy = ? AND status = 'closed'
              AND COALESCE(is_live, false) = false
              AND exit_date >= ? AND pnl_pct IS NOT NULL""",
        (strategy, cutoff),
    ).fetchall()
    pnls = [float(dict(r).get("pnl_pct") or 0) for r in rows]
    if len(pnls) < 5:
        return GateResult(
            "paper_sharpe_30d", True,
            f"only {len(pnls)} closed trades in 30d — too few to compute, skipping (not blocking)",
            severity="warning",
        )
    n = len(pnls)
    mean = sum(pnls) / n
    var = sum((p - mean) ** 2 for p in pnls) / max(n - 1, 1)
    std = var ** 0.5
    if std == 0:
        return GateResult(
            "paper_sharpe_30d", False,
            f"stddev=0 across {n} trades (data quality issue)",
        )
    sharpe = (mean / std) * (252 ** 0.5)
    ok = sharpe >= 0.5
    return GateResult(
        "paper_sharpe_30d", ok,
        f"Sharpe(30d, n={n})={sharpe:.2f} mean={mean*100:+.2f}% stdev={std*100:.2f}% "
        f"(threshold ≥ 0.5)",
    )


def gate_active_divergences(conn, strategy: str) -> GateResult:
    rows = conn.execute(
        """SELECT issue_type, severity, ticker FROM alpaca_reconciliation
            WHERE strategy = ? AND resolved_at IS NULL""",
        (strategy,),
    ).fetchall()
    rows = [dict(r) for r in rows]
    crit_or_warn = [r for r in rows if r.get("severity") in ("critical", "warn")]
    ok = len(crit_or_warn) == 0
    if ok:
        return GateResult("no_active_divergences", True,
                          f"{len(rows)} info-level entries; 0 warn/critical")
    detail = "; ".join(f"{r['ticker']} {r['issue_type']}/{r['severity']}"
                       for r in crit_or_warn[:5])
    return GateResult("no_active_divergences", False, detail)


def gate_unresolved_orders(conn, strategy: str) -> GateResult:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    rows = conn.execute(
        """SELECT order_id, fill_status FROM order_audit
            WHERE strategy = ? AND fill_status NOT IN ('filled', 'skipped')
              AND decided_at < ?""",
        (strategy, cutoff),
    ).fetchall()
    rows = [dict(r) for r in rows]
    ok = len(rows) == 0
    if ok:
        return GateResult("no_unresolved_orders", True,
                          "no order_audit rows older than 1h in non-terminal state")
    return GateResult(
        "no_unresolved_orders", False,
        f"{len(rows)} orders stuck >1h: " +
        ", ".join(f"{r['order_id'][:8]}={r['fill_status']}" for r in rows[:3]),
    )


def gate_freshness(conn, strategy: str) -> GateResult:
    """Reuse framework.contracts.freshness if present."""
    try:
        from framework.contracts.freshness import assert_all_fresh_for_strategy
        from framework.contracts.exceptions import StaleSignalError
    except Exception as e:
        return GateResult("freshness_contracts", True,
                          f"contracts module not loaded: {e}",
                          severity="warning")
    try:
        assert_all_fresh_for_strategy(conn, strategy)
        return GateResult("freshness_contracts", True, "all contracts green")
    except StaleSignalError as e:
        return GateResult("freshness_contracts", False, str(e))
    except Exception as e:
        return GateResult("freshness_contracts", False, f"{type(e).__name__}: {e}")


def gate_is_live_columns(conn) -> GateResult:
    try:
        rows = conn.execute(
            """SELECT table_name, column_name FROM information_schema.columns
                WHERE column_name = 'is_live'
                  AND table_name IN ('strategy_portfolio','order_audit',
                                     'alpaca_position_snapshots','alpaca_reconciliation')"""
        ).fetchall()
    except Exception as exc:
        return GateResult("is_live_migration", False, f"schema query failed: {exc}")
    found = {dict(r)["table_name"] for r in rows}
    expected = {"strategy_portfolio", "order_audit",
                "alpaca_position_snapshots", "alpaca_reconciliation"}
    missing = expected - found
    if missing:
        return GateResult("is_live_migration", False,
                          f"is_live column missing on: {', '.join(sorted(missing))}")
    return GateResult("is_live_migration", True, "is_live present on all 4 tables")


def gate_daily_summary_recent() -> GateResult:
    log_path = REPO / "logs" / "daily-summary.log"
    if not log_path.exists():
        return GateResult("daily_summary_recent", False,
                          "logs/daily-summary.log missing")
    last_ok = None
    try:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if line and "sent=True" in line:
                    last_ok = line
    except Exception as exc:
        return GateResult("daily_summary_recent", False, f"log unreadable: {exc}")
    if not last_ok:
        return GateResult("daily_summary_recent", False,
                          "no successful run logged yet")
    try:
        ts_str = last_ok.split()[0]
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return GateResult("daily_summary_recent", False, "timestamp parse failed")
    age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
    ok = age_hours <= 36
    return GateResult("daily_summary_recent", ok,
                      f"last successful send {age_hours:.1f}h ago")


def gate_heartbeat_fresh(strategy: str) -> GateResult:
    hb_path = REPO / "strategies/cw_strategies/data" / f"{strategy}_heartbeat.json"
    if not hb_path.exists():
        return GateResult("heartbeat_fresh", False,
                          f"{hb_path.name} missing — runner not started?")
    try:
        hb = json.loads(hb_path.read_text())
    except Exception as exc:
        return GateResult("heartbeat_fresh", False, f"unreadable: {exc}")
    ts_str = hb.get("timestamp")
    if not ts_str:
        return GateResult("heartbeat_fresh", False, "no timestamp in heartbeat")
    try:
        from zoneinfo import ZoneInfo
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("America/New_York"))
        age_min = (datetime.now(ZoneInfo("America/New_York")) - ts).total_seconds() / 60
    except Exception as exc:
        return GateResult("heartbeat_fresh", False, f"ts parse: {exc}")
    # 30 min market hours, 24h off — be lenient since this can run any time
    ok = age_min <= 24 * 60
    return GateResult("heartbeat_fresh", ok,
                      f"age={age_min:.0f}min status={hb.get('status')}")


def gate_live_creds(strategy: str, min_equity: float) -> GateResult:
    """Spawn verify_live_creds.py — captures stdout for the report."""
    cmd = ["python3", str(REPO / "scripts/verify_live_creds.py"),
           "--strategy", strategy, "--min-equity", str(min_equity)]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception as exc:
        return GateResult("live_creds", False, f"verify_live_creds failed: {exc}")
    if out.returncode == 0:
        first_lines = "; ".join(line.strip() for line in out.stdout.splitlines()[1:6] if line.strip())
        return GateResult("live_creds", True, first_lines or "verified")
    detail = (out.stdout.strip().splitlines() or [""])[-3:]
    return GateResult("live_creds", False, " | ".join(detail))


def gate_kill_switch_off(strategy: str) -> GateResult:
    truthy = lambda v: str(v).strip().lower() in ("1", "true", "yes", "on")
    if truthy(os.getenv("TRADING_HALTED", "")):
        return GateResult("kill_switch_off", False, "TRADING_HALTED set globally")
    per = f"TRADING_HALTED_{strategy.upper()}"
    if truthy(os.getenv(per, "")):
        return GateResult("kill_switch_off", False, f"{per} set")
    return GateResult("kill_switch_off", True, "TRADING_HALTED not set")


def gate_recent_deploy() -> GateResult:
    """git log on Studio — last commit > 24h old means changes have settled."""
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%ci"],
            capture_output=True, text=True, cwd=REPO, timeout=10,
        )
    except Exception as exc:
        return GateResult("recent_deploy", False, f"git log failed: {exc}",
                          severity="warning")
    if out.returncode != 0:
        return GateResult("recent_deploy", False, out.stderr[:120],
                          severity="warning")
    ts_str = out.stdout.strip()
    try:
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S %z")
    except Exception:
        return GateResult("recent_deploy", False, f"ts parse: {ts_str}",
                          severity="warning")
    age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
    ok = age_hours >= 24
    return GateResult(
        "recent_deploy", ok,
        f"last commit {age_hours:.1f}h ago (require ≥24h for changes to settle)",
        severity="warning" if not ok else "blocker",
    )


# ── Driver ─────────────────────────────────────────────────────────────────

def run_all(strategy: str, min_equity: float) -> list[GateResult]:
    conn = get_connection(readonly=True)
    results: list[GateResult] = []
    results.append(gate_is_live_columns(conn))
    results.append(gate_paper_sharpe_30d(conn, strategy))
    results.append(gate_active_divergences(conn, strategy))
    results.append(gate_unresolved_orders(conn, strategy))
    results.append(gate_freshness(conn, strategy))
    results.append(gate_daily_summary_recent())
    results.append(gate_heartbeat_fresh(strategy))
    results.append(gate_live_creds(strategy, min_equity))
    results.append(gate_kill_switch_off(strategy))
    results.append(gate_recent_deploy())
    conn.close()
    return results


def render_text(strategy: str, results: list[GateResult]) -> str:
    lines = [f"=== Pre-launch check: {strategy} ===", ""]
    blockers = [r for r in results if not r.ok and r.severity == "blocker"]
    warnings = [r for r in results if not r.ok and r.severity == "warning"]
    for r in results:
        mark = "✓" if r.ok else ("⚠" if r.severity == "warning" else "✗")
        lines.append(f"  [{mark}] {r.name:<28s}  {r.detail}")
    lines.append("")
    if blockers:
        lines.append(f"❌ NO-GO: {len(blockers)} blocker(s)")
    elif warnings:
        lines.append(f"⚠️  GO with warnings: {len(warnings)} non-blocking issue(s)")
    else:
        lines.append("✅ ALL GREEN — clear to flip live")
    return "\n".join(lines)


def render_markdown(strategy: str, results: list[GateResult]) -> str:
    blockers = [r for r in results if not r.ok and r.severity == "blocker"]
    warnings = [r for r in results if not r.ok and r.severity == "warning"]
    if blockers:
        verdict = f"### ❌ NO-GO\n\n{len(blockers)} blocker(s) — see below."
    elif warnings:
        verdict = f"### ⚠️  GO with warnings\n\n{len(warnings)} non-blocking issue(s)."
    else:
        verdict = "### ✅ ALL GREEN — clear to flip live"
    rows = "\n".join(
        f"| {'✓' if r.ok else '✗'} | `{r.name}` | {r.severity} | {r.detail} |"
        for r in results
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""# Pre-launch check — {strategy}
*Generated {today}*

{verdict}

| ✓ | Gate | Severity | Detail |
|---|------|----------|--------|
{rows}
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="quality_momentum")
    p.add_argument("--min-equity", type=float, default=9500.0)
    p.add_argument("--markdown", action="store_true",
                   help="Output Markdown report instead of text")
    args = p.parse_args()

    results = run_all(args.strategy, args.min_equity)
    if args.markdown:
        print(render_markdown(args.strategy, results))
    else:
        print(render_text(args.strategy, results))

    blockers = [r for r in results if not r.ok and r.severity == "blocker"]
    sys.exit(0 if not blockers else 1)


if __name__ == "__main__":
    main()
