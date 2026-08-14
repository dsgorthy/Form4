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
import re
import subprocess
import sys
import urllib.request
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
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

# (label, database, SQL returning one timestamp, max age in HOURS)
#
# The day-granularity checks above cannot see an intraday outage: they read
# filing_date, which only advances once a day, so a feed that dies at 09:00
# still looks current until tomorrow. With ingest on a 5-minute cadence, a
# 4-day budget means a total stall could run most of a week while the site
# serves stale data as if it were live. These read the INGEST timestamp
# instead — when a row was last written, not what date it describes.
#
# Budget is measured, not guessed. Over 21 days of EDGAR-hours ingests
# (2026-08-13, n=9,765): p99 gap 6.6 min, p99.9 gap 42 min. The only larger
# gap in the window was the known 18-day outage. 3 hours is ~4x p99.9 — quiet
# in normal operation, and it catches a real stall the same morning.
FRESHNESS_HOURLY = [
    ("insider trades ingest", "form4",
     "SELECT max(created_at) FROM trades", 3),
]

# EDGAR accepts Form 4 filings 06:00-22:00 ET on weekdays. Outside that window
# there is nothing to ingest, so silence is correct rather than a fault, and
# alerting on it would train us to ignore the pager.
EDGAR_OPEN_ET = (6, 22)


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

    now_et = datetime.now(ZoneInfo("America/New_York"))
    edgar_open = (
        now_et.weekday() < 5 and EDGAR_OPEN_ET[0] <= now_et.hour < EDGAR_OPEN_ET[1]
    )
    for label, db, sql, budget_h in FRESHNESS_HOURLY:
        if not edgar_open:
            print(f"  SKIP {label}: {now_et:%a %H:%M} ET, EDGAR closed")
            continue
        val = ssh_psql(db, sql)
        if not val:
            problems.append(f"{label}: query returned nothing")
            print(f"  FAIL {label}: no value")
            continue
        ts = _parse_ts(val)
        if ts is None:
            problems.append(f"{label}: unparseable timestamp {val!r}")
            print(f"  FAIL {label}: unparseable {val!r}")
            continue
        # Normalize before printing: created_at often carries a -07 offset,
        # so formatting the raw value with a "Z" suffix labels Pacific time as
        # UTC and makes a healthy feed look 7 hours stale to anyone reading it.
        ts_utc = ts.astimezone(timezone.utc)
        age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
        ok = age_h <= budget_h
        print(f"  {'OK  ' if ok else 'FAIL'} {label}: {ts_utc:%Y-%m-%d %H:%M}Z "
              f"({age_h:.1f}h old, budget {budget_h}h)")
        if not ok:
            problems.append(
                f"{label} has not ingested for {age_h:.1f}h "
                f"(budget {budget_h}h, latest {ts_utc:%Y-%m-%d %H:%M}Z)"
            )

    _finish(problems, topic, args.dry_run)
    return 1 if problems else 0


# Postgres renders a whole-hour UTC offset as "-07", not "-07:00". Python's
# fromisoformat only learned to accept that in 3.11, and this script runs on
# /usr/bin/python3 — Apple's system Python, 3.9.6.
_BARE_HOUR_OFFSET = re.compile(r"(T\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2})$")

# What a naive timestamp means. trades.created_at defaults to now(), which on
# Studio renders LOCAL time — so assuming UTC for a naive value backdates it by
# the offset and invents staleness that isn't there.
_DB_TZ = ZoneInfo("America/Los_Angeles")


def _parse_ts(val: str) -> "datetime | None":
    """Parse a Postgres timestamp written into a TEXT column.

    created_at is TEXT with a now() default, so the format varies with whatever
    wrote the row: with or without microseconds, with or without an offset, and
    with the offset in either "-07" or "-07:00" form.

    This got it wrong once and paged about a 7-hour ingest stall while the feed
    was six minutes old. Two compounding mistakes: fromisoformat rejected "-07"
    on 3.9, and the strptime fallback sliced the string to 26 characters, which
    silently discarded the offset it was falling back to handle. The naive
    result was then stamped UTC — turning Pacific into a 7-hour delay, which is
    exactly the offset.
    """
    raw = val.strip().replace(" ", "T", 1)
    raw = _BARE_HOUR_OFFSET.sub(r"\1\2:00", raw)

    try:
        ts = datetime.fromisoformat(raw)
    except ValueError:
        # %z consumes the offset when present; the naive formats come after so
        # an offset is never dropped by falling through to them.
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
        ):
            try:
                ts = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    return ts if ts.tzinfo else ts.replace(tzinfo=_DB_TZ)


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
