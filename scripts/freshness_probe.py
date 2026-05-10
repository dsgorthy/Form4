#!/usr/bin/env python3
"""Daily freshness probe.

Iterates every contract in `config/freshness_contracts.yaml` and queries the
live PG `form4` database for current freshness. Compares against the
contract's `max_staleness_hours`. Alerts on transitions only (ok → stale).

State file: `logs/freshness_state.json` — records last-known status per
contract so we don't spam Telegram.

Designed to run every 30 minutes via launchd. The single probe alone
would have caught the April 2026 silent outage on Day 1 instead of Day 21.

Usage (Studio):
    python3 scripts/freshness_probe.py
    python3 scripts/freshness_probe.py --json    # machine output
    python3 scripts/freshness_probe.py --check   # exit non-zero if any stale
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.contracts.freshness import FreshnessRegistry, get_freshness
from framework.alerts.log import alert

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9 fallback (Studio is on 3.12 — this won't trip)
    ZoneInfo = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STATE_FILE = REPO / "logs" / "freshness_state.json"


def _in_quiet_window(now: datetime) -> bool:
    """Return True if alerts should be suppressed (structurally-expected stale).

    refresh-features.plist runs Mon-Fri only at 06:00 PT. SLAs in the
    26-30h band naturally breach Saturday onwards because writers haven't
    fired since Friday morning. These breaches are EXPECTED, not
    actionable — alerting on them produces pager noise that operators
    learn to ignore (which is itself a reliability failure).

    Quiet window:
      Saturday — all day in Pacific Time
      Sunday — all day in Pacific Time
      Monday — 00:00 PT through 07:00 PT (refresh-features runs at 06:00,
               give it 60 min of completion + propagation buffer)

    During the quiet window:
      - Alert dispatch is skipped
      - State file IS still updated (so transitions track correctly across
        the weekend; a column that was OK Friday and is OK Monday morning
        won't fire any alert at all)

    The cw_runner's preflight check is the primary safety net — if Monday
    morning refresh actually fails, the strategy runner halts with R-002
    StaleSignalError immediately at 06:25 ET wake, BEFORE this probe's
    quiet window even ends. So suppressing weekend probe alerts doesn't
    create a real-failure blind spot.
    """
    if ZoneInfo is None:
        return False  # don't suppress if tz support is missing
    pt = now.astimezone(ZoneInfo("America/Los_Angeles"))
    weekday = pt.weekday()  # 0=Mon, 5=Sat, 6=Sun
    if weekday in (5, 6):
        return True
    if weekday == 0 and pt.hour < 7:
        return True
    return False


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(STATE_FILE)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true",
                   help="Emit JSON output instead of human format")
    p.add_argument("--check", action="store_true",
                   help="Exit non-zero if any contract is stale")
    p.add_argument("--no-alert", action="store_true",
                   help="Skip alerting (useful for ad-hoc inspection)")
    p.add_argument("--no-quiet-window", action="store_true",
                   help="Disable weekend-quiet-window suppression (force alerts "
                        "through for testing or manual diagnostics)")
    args = p.parse_args()

    registry = FreshnessRegistry.get()
    contracts = registry.all()
    state = _load_state()
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    in_quiet = _in_quiet_window(now_dt) and not args.no_quiet_window

    conn = get_connection(readonly=True)
    results: list[dict] = []

    for c in contracts:
        try:
            ts, age = get_freshness(conn, c.table, c.column)
        except Exception as e:
            logger.warning("freshness lookup failed for %s.%s: %s",
                           c.table, c.column, e)
            ts, age = None, None

        is_stale = age is None or age > c.max_staleness_hours
        key = f"{c.table}.{c.column}"
        prev_status = state.get(key, {}).get("status", "unknown")
        new_status = "stale" if is_stale else "ok"

        results.append({
            "table": c.table,
            "column": c.column,
            "max_staleness_hours": c.max_staleness_hours,
            "observed_age_hours": round(age, 2) if age is not None else None,
            "last_observed_at": ts.isoformat() if ts else None,
            "status": new_status,
            "prev_status": prev_status,
            "transitioned": prev_status != new_status,
            "required_for": list(c.required_for),
        })
        state[key] = {"status": new_status, "checked_at": now,
                      "age_h": round(age, 2) if age is not None else None}

    conn.close()
    _save_state(state)

    # Alert only on transitions ok → stale, or on first run (prev=unknown) if stale.
    transitioned_to_stale = [
        r for r in results
        if r["status"] == "stale" and r["prev_status"] in ("ok", "unknown")
    ]
    transitioned_to_ok = [
        r for r in results
        if r["status"] == "ok" and r["prev_status"] == "stale"
    ]

    if transitioned_to_stale and not args.no_alert and not in_quiet:
        body = "\n".join(
            f"  • {r['table']}.{r['column']}: "
            f"age={r['observed_age_hours']}h > contract={r['max_staleness_hours']}h"
            for r in transitioned_to_stale
        )
        alert.critical(
            "freshness_probe",
            f"{len(transitioned_to_stale)} contract(s) breached:\n{body}\n\nRunbook: R-001",
            breached=[f"{r['table']}.{r['column']}" for r in transitioned_to_stale],
        )
    elif transitioned_to_stale and in_quiet:
        # Log the suppression so it's visible in the probe's launchd log
        # — operators can grep for "quiet_window" to confirm "yes, we
        # noticed, we just didn't page anyone."
        logger.info(
            "quiet_window: suppressing %d stale-transition alert(s) "
            "(weekend / pre-Monday-refresh window)",
            len(transitioned_to_stale),
        )

    if transitioned_to_ok and not args.no_alert and not in_quiet:
        body = "\n".join(f"  • {r['table']}.{r['column']} recovered"
                         for r in transitioned_to_ok)
        alert.info("freshness_probe",
                   f"{len(transitioned_to_ok)} contract(s) recovered:\n{body}")
    elif transitioned_to_ok and in_quiet:
        logger.info(
            "quiet_window: suppressing %d recovery notification(s)",
            len(transitioned_to_ok),
        )

    # Output
    n_stale = sum(1 for r in results if r["status"] == "stale")
    if args.json:
        print(json.dumps({
            "checked_at": now,
            "n_total": len(results),
            "n_stale": n_stale,
            "results": results,
        }, indent=2))
    else:
        print(f"{'table.column':<48} {'max_h':>7} {'age_h':>9} {'status':>8}")
        print("─" * 80)
        for r in sorted(results, key=lambda x: (x["status"], x["table"], x["column"])):
            age_str = f"{r['observed_age_hours']:.1f}" if r["observed_age_hours"] is not None else "—"
            mark = "STALE" if r["status"] == "stale" else "ok"
            transition = " ←TRANSITION" if r["transitioned"] else ""
            print(f"{r['table']+'.'+r['column']:<48} {r['max_staleness_hours']:>7.1f} "
                  f"{age_str:>9} {mark:>8}{transition}")
        print(f"\n{n_stale} stale / {len(results)} total at {now}")
        if transitioned_to_stale:
            print(f"\n🚨 {len(transitioned_to_stale)} new staleness alert(s) appended to logs/alerts.ndjson")
        if transitioned_to_ok:
            print(f"\n✅ {len(transitioned_to_ok)} recovery notification(s) appended to logs/alerts.ndjson")

    if args.check and n_stale > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
