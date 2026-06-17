"""One-off recovery: push triggered strategy observations that the
Dagster asset wrapper failed to push due to the emit_alerts cooldown
self-dedup bug (observed 2026-06-16, fixed in commit 87c3074).

For each (strategy, ticker) with a triggered observation in the given
window where no prior ntfy push has happened, send the alert. Idempotent
within a run because we dedup by ticker.

Usage:
    python scripts/push_missed_alerts.py \\
        --strategy strategy.agrade_drawdown_buy \\
        --from 2026-06-16 --to 2026-06-16

Run on Studio with NTFY_ALERT_TOPIC + PYRRHO_DATAPLANE_DSN sourced.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import requests

_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parents[1]))  # dataplane/
from dataplane.emit import _format_alert  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", required=True,
                   help="signal_id prefix, e.g. strategy.agrade_drawdown_buy")
    p.add_argument("--from", dest="from_date", required=True)
    p.add_argument("--to", dest="to_date", required=True)
    p.add_argument("--dry-run", action="store_true",
                   help="print what would be pushed; no HTTP calls")
    args = p.parse_args()

    topic = os.environ.get("NTFY_ALERT_TOPIC")
    if not topic and not args.dry_run:
        sys.exit("FATAL: NTFY_ALERT_TOPIC not set")

    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (ticker) ticker, as_of_date, value
          FROM signal_observations
         WHERE signal_id LIKE %s
           AND (value->>'triggered')::boolean = true
           AND as_of_date >= %s::date
           AND as_of_date <  (%s::date + INTERVAL '1 day')
         ORDER BY ticker, as_of_date ASC
        """,
        (f"{args.strategy}%", args.from_date, args.to_date),
    )
    rows = cur.fetchall()
    print(f"found {len(rows)} distinct ticker(s) with triggered obs in window")

    name = args.strategy.removeprefix("strategy.")
    pushed = 0
    for ticker, as_of, value in rows:
        body = _format_alert(f"{name} (recovery push)", value, ticker)
        dollars = float((value.get("trigger_value") or {}).get("value", 0) or 0)
        if args.dry_run:
            print(f"  [dry-run] {ticker} ${dollars:,.0f}:")
            for line in body.split("\n"):
                print(f"    {line}")
            continue
        try:
            resp = requests.post(
                f"https://ntfy.sh/{topic}",
                data=body,
                headers={
                    "Title": f"Pyrrho · {name} (recovery)",
                    "Priority": "high",
                    "Tags": "bell",
                },
                timeout=10,
            )
            print(f"  {ticker} ${dollars:,.0f}: HTTP {resp.status_code}")
            if resp.ok:
                pushed += 1
        except Exception as exc:
            print(f"  {ticker}: ERROR {exc}")
    print(f"DONE. {pushed} pushed")
    conn.close()


if __name__ == "__main__":
    main()
