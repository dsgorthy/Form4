#!/usr/bin/env python3
"""Run HogQL against PostHog. Read-only.

Credentials come from .env: POSTHOG_PERSONAL_API_KEY (a phx_ personal key, NOT
the phc_ ingest key the frontend uses) and POSTHOG_PROJECT_ID. The key is never
printed, logged, or passed on a command line.

Usage:
    python3 scripts/posthog_query.py --sql "SELECT count() FROM events"
    python3 scripts/posthog_query.py --preset top-pages
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

PRESETS = {
    "top-pages": """
        SELECT properties.$pathname AS path,
               count() AS views,
               count(DISTINCT person_id) AS visitors
          FROM events
         WHERE event = '$pageview' AND timestamp > now() - INTERVAL 30 DAY
         GROUP BY path ORDER BY views DESC LIMIT 20
    """,
    "funnel": """
        SELECT event, count() AS n, count(DISTINCT person_id) AS people
          FROM events
         WHERE timestamp > now() - INTERVAL 30 DAY
           AND event IN ('$pageview','signed_up','onboarding_complete',
                         'checkout_started','upgrade_complete')
         GROUP BY event ORDER BY n DESC
    """,
    "visitors": """
        SELECT toDate(timestamp) AS day,
               count(DISTINCT person_id) AS visitors,
               count() AS events
          FROM events
         WHERE timestamp > now() - INTERVAL 30 DAY
         GROUP BY day ORDER BY day DESC LIMIT 30
    """,
    "entry-pages": """
        SELECT properties.$pathname AS landing, count(DISTINCT person_id) AS visitors
          FROM events
         WHERE event = '$pageview' AND timestamp > now() - INTERVAL 30 DAY
         GROUP BY landing ORDER BY visitors DESC LIMIT 15
    """,
}


def env(name: str) -> str | None:
    for line in (REPO / ".env").read_text().splitlines():
        line = line.strip()
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    return os.environ.get(name)


def run(sql: str) -> dict:
    key, proj = env("POSTHOG_PERSONAL_API_KEY"), env("POSTHOG_PROJECT_ID")
    host = (env("NEXT_PUBLIC_POSTHOG_HOST") or "https://us.i.posthog.com")
    # The ingest host (us.i.posthog.com) is not the API host.
    host = host.replace("us.i.posthog.com", "us.posthog.com")
    if not key or not proj:
        raise SystemExit("POSTHOG_PERSONAL_API_KEY / POSTHOG_PROJECT_ID missing from .env")
    body = json.dumps({"query": {"kind": "HogQLQuery", "query": sql}}).encode()
    req = urllib.request.Request(
        f"{host}/api/projects/{proj}/query/", data=body,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:400]
        raise SystemExit(f"HTTP {e.code}: {detail}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql")
    ap.add_argument("--preset", choices=sorted(PRESETS))
    args = ap.parse_args()
    sql = args.sql or (PRESETS[args.preset] if args.preset else None)
    if not sql:
        raise SystemExit("give --sql or --preset")
    d = run(sql)
    cols = d.get("columns") or []
    rows = d.get("results") or []
    if cols:
        print(" | ".join(str(c) for c in cols))
        print("-" * 70)
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))
    print(f"\n{len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
