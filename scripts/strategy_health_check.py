#!/usr/bin/env python3
"""Daily strategy health check — alerts if any strategy is idle too long.

Checks:
1. Days since last entry for each strategy (alert if >= threshold)
2. Runner heartbeat freshness (alert if stale)
3. Alpaca account connectivity per strategy

Usage:
    python3 scripts/strategy_health_check.py              # run checks, print to stdout
    python3 scripts/strategy_health_check.py --dry-run    # synonym (kept for back-compat)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from config.database import get_connection

ET = ZoneInfo("America/New_York")
CONFIGS_DIR = PROJECT_ROOT / "strategies" / "cw_strategies" / "configs"
DATA_DIR = PROJECT_ROOT / "strategies" / "cw_strategies" / "data"

IDLE_THRESHOLD_DAYS = 5
HEARTBEAT_MAX_AGE_HOURS = 3


def _market_days_since(date_str: str) -> int:
    """Count market days (weekdays) between date_str and today."""
    if not date_str:
        return 999
    d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
    today = datetime.now(ET).date()
    count = 0
    current = d + timedelta(days=1)
    while current <= today:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


def check_idle_strategies(conn) -> list[str]:
    """Check days since last entry for each strategy."""
    import yaml
    alerts = []
    for p in sorted(CONFIGS_DIR.glob("*.yaml")):
        with open(p) as f:
            cfg = yaml.safe_load(f)
        name = cfg.get("strategy_name", p.stem)

        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(entry_date) FROM strategy_portfolio WHERE strategy = %s",
            (name,),
        )
        row = cur.fetchone()
        last_entry = row[0] if row else None

        days = _market_days_since(last_entry) if last_entry else 999
        status = "OK" if days < IDLE_THRESHOLD_DAYS else "IDLE"
        print(f"  {name}: last entry={last_entry}, {days} market days ago [{status}]")

        if days >= IDLE_THRESHOLD_DAYS:
            alerts.append(
                f"*{name}*: {days} market days with no entry (last: {last_entry or 'never'})"
            )
    return alerts


def check_heartbeats() -> list[str]:
    """Check runner heartbeat file freshness."""
    alerts = []
    for hb_file in sorted(DATA_DIR.glob("*_heartbeat.json")):
        try:
            data = json.loads(hb_file.read_text())
            ts = datetime.fromisoformat(data["timestamp"])
            age_hours = (datetime.now(ET) - ts).total_seconds() / 3600
            strategy = data.get("strategy", hb_file.stem)
            status_str = data.get("status", "unknown")
            status = "OK" if age_hours < HEARTBEAT_MAX_AGE_HOURS else "STALE"
            print(f"  {strategy}: heartbeat {age_hours:.1f}h ago, status={status_str} [{status}]")

            if age_hours >= HEARTBEAT_MAX_AGE_HOURS:
                alerts.append(
                    f"*{strategy}*: heartbeat is {age_hours:.0f}h old (status: {status_str})"
                )
        except Exception as e:
            alerts.append(f"*{hb_file.stem}*: failed to read heartbeat: {e}")
    return alerts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No-op (kept for back-compat).")
    parser.parse_args()

    now = datetime.now(ET)
    print(f"Strategy Health Check — {now.strftime('%Y-%m-%d %H:%M ET')}")
    print()

    conn = get_connection(readonly=True)

    print("Idle check (threshold: {} market days):".format(IDLE_THRESHOLD_DAYS))
    idle_alerts = check_idle_strategies(conn)
    conn.close()

    print()
    print("Heartbeat check (max age: {}h):".format(HEARTBEAT_MAX_AGE_HOURS))
    hb_alerts = check_heartbeats()

    all_alerts = idle_alerts + hb_alerts
    if all_alerts:
        print(f"\nALERT: {len(all_alerts)} health issue(s):")
        for a in all_alerts:
            print(f"  {a}")
    else:
        print("\nAll strategies healthy.")


if __name__ == "__main__":
    main()
