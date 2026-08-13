#!/usr/bin/env python3
"""Off-box health + freshness watchdog. RUNS ON THE MINI, watches Studio.

Why off-box: on 2026-07-28 Studio ran out of TCP ephemeral ports. form4.app
and trytailorly.com served 502 for 14 days and nobody knew. Every monitor
that should have caught it — form4-uptime, freshness-probe, heartbeat-probe —
runs ON Studio and alerts via a network path that was itself broken. A
watcher cannot watch the box it lives on, and an alert channel that shares
the failure domain is not an alert channel.

This deliberately depends on Studio for nothing except the checks themselves.
If Studio is unreachable, that IS the alert.

Checks:
  1. Public endpoints respond 200 (the thing users actually see)
  2. Studio reachable over Tailscale at all
  3. Data freshness: trades / daily_prices / insider_ticker_scores /
     congress_trades, each against a staleness budget in days
  4. Dataplane freshness: newest signal_observations row

Exit 0 = all good, 1 = at least one problem (and an ntfy push was attempted).

Usage (on the Mini):
    python3 scripts/offbox_watchdog.py            # check + alert on problems
    python3 scripts/offbox_watchdog.py --dry-run  # report, never alert
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import urllib.request
from datetime import date, datetime
from pathlib import Path

STUDIO = "100.78.9.66"
SSH_TARGET = f"derekg@{STUDIO}"

ENDPOINTS = {
    "form4.app": "https://form4.app/",
    "trytailorly.com": "https://trytailorly.com/",
}

# (label, database, SQL returning one date/text, max age in days)
# Budgets are generous enough not to fire on a normal weekend but tight
# enough that a 14-day silence is impossible.
FRESHNESS = [
    ("insider filings", "form4",
     "SELECT max(filing_date) FROM trades", 4),
    ("daily prices", "form4",
     "SELECT max(date) FROM prices.daily_prices", 4),
    ("PIT scores", "form4",
     "SELECT max(as_of_date)::text FROM insider_ticker_scores", 4),
    ("congress", "form4",
     "SELECT max(filing_date) FROM congress_trades", 10),
    ("dataplane observations", "pyrrho_data_dev",
     "SELECT max(as_of_date)::date::text FROM signal_observations", 3),
]


def notify(title: str, message: str, topic: str) -> None:
    """Push to ntfy. Topic is treated as a secret (it is the auth)."""
    if not topic:
        print("  [no NTFY_ALERT_TOPIC — cannot alert]", file=sys.stderr)
        return
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}",
        data=message.encode("utf-8"),
        headers={"Title": title, "Priority": "high", "Tags": "rotating_light"},
    )
    try:
        urllib.request.urlopen(req, timeout=15)
    except Exception as exc:  # noqa: BLE001
        print(f"  [ntfy push failed: {exc}]", file=sys.stderr)


def ssh_psql(db: str, sql: str) -> str | None:
    """Run one query on Studio. None means unreachable or query failed."""
    cmd = [
        "ssh", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", SSH_TARGET,
        f"/opt/homebrew/bin/psql -d {db} -tAc \"{sql}\"",
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    except subprocess.TimeoutExpired:
        return None
    if out.returncode != 0:
        return None
    return (out.stdout or "").strip() or None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="never send alerts")
    args = ap.parse_args()

    # Topic lives in the repo .env, same convention as framework/alerts/ntfy.py.
    topic = os.environ.get("NTFY_ALERT_TOPIC", "")
    if not topic:
        env = Path(__file__).resolve().parents[1] / ".env"
        if env.exists():
            for line in env.read_text().splitlines():
                if line.startswith("NTFY_ALERT_TOPIC="):
                    topic = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    problems: list[str] = []
    today = date.today()

    print(f"=== off-box watchdog {datetime.now():%Y-%m-%d %H:%M:%S} ===")

    for name, url in ENDPOINTS.items():
        # Cloudflare 403s urllib's default User-Agent ("Python-urllib/3.x")
        # while serving 200 to a browser or curl. Without this the watchdog
        # reports both sites down permanently — a monitor that cries wolf
        # gets muted, which is worse than having no monitor.
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                code = r.status
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "code", 0) or 0
        ok = code == 200
        print(f"  {'OK  ' if ok else 'FAIL'} {name}: HTTP {code}")
        if not ok:
            problems.append(f"{name} returned HTTP {code}")

    reachable = ssh_psql("postgres", "SELECT 1") == "1"
    print(f"  {'OK  ' if reachable else 'FAIL'} studio reachable: {reachable}")
    if not reachable:
        problems.append("Studio unreachable over Tailscale (ssh+psql failed)")
        # Everything below needs Studio; report what we have and alert now.
        _finish(problems, topic, args.dry_run)
        return 1

    for label, db, sql, budget in FRESHNESS:
        val = ssh_psql(db, sql)
        if not val:
            problems.append(f"{label}: query returned nothing")
            print(f"  FAIL {label}: no value")
            continue
        try:
            d = datetime.strptime(val[:10], "%Y-%m-%d").date()
        except ValueError:
            problems.append(f"{label}: unparseable date {val!r}")
            print(f"  FAIL {label}: unparseable {val!r}")
            continue
        age = (today - d).days
        ok = age <= budget
        print(f"  {'OK  ' if ok else 'FAIL'} {label}: {d} ({age}d old, budget {budget}d)")
        if not ok:
            problems.append(f"{label} is {age}d stale (budget {budget}d, latest {d})")

    _finish(problems, topic, args.dry_run)
    return 1 if problems else 0


def _finish(problems: list[str], topic: str, dry_run: bool) -> None:
    if not problems:
        print("=== all checks passed ===")
        return
    body = "\n".join(f"• {p}" for p in problems)
    print(f"=== {len(problems)} PROBLEM(S) ===\n{body}")
    if dry_run:
        print("(dry run — no alert sent)")
        return
    notify(f"Studio watchdog: {len(problems)} problem(s)", body, topic)


if __name__ == "__main__":
    sys.exit(main())
