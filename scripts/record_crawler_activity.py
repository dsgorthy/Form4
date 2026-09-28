#!/usr/bin/env python3
"""Persist per-hour crawler counts, because the access log holds about one day.

WHY THIS EXISTS

Google traffic went to zero on 2026-09-17 and the cause was not found until
2026-09-27. Ten days. The single most useful number during that period — how many
requests Googlebot was actually making — did not exist anywhere.

**MEASURED 2026-09-28: the Caddy access log retains NINETEEN MINUTES.** Not a
day. json-file with `max-size 10m` and `max-file 3` is 30 MB, and Caddy logs
every request header, so a line runs about 1.6 KB and current bot volume is
~29,000 requests an hour. 9,176 lines spanned 16:03 to 16:22. A first version of
this script recorded per-HOUR counts on an hourly schedule, which would have seen
roughly a third of each hour and — because the upsert REPLACES the count — would
have overwritten a fuller count with a partial one. Both wrong, and wrong
quietly.

So the unit is a TEN-MINUTE SLOT, written every ten minutes. A slot is always
fully inside the retention window, so each is counted exactly once and daily
totals are an honest SUM. `docker-compose.prod.yml` also now keeps more log, but
this design does not depend on that: it is correct at nineteen minutes.

Search Console reports crawl stats, but on a two-to-three day lag and only in a
chart you cannot difference. This table answers "is Google crawling us today, and
how does that compare to last Tuesday" in one query.

WHAT IT MEASURES, AND THE ONE SUBTLETY

Search engines are counted BY IP, not by user agent. A `Googlebot/2.1` user agent
is trivially spoofed, and during the 2026-09-27 investigation this session
repeatedly fetched pages with exactly that string while checking the fix — so a
UA-based count would have recorded a crawl recovery that was partly its own
traffic. Both are stored: `googlebot` is IP-verified, `googlebot_ua_only` is the
remainder claiming the identity without the address. A growing gap is spoofing,
and it is worth knowing about rather than silently folding into the real number.

Everything else is counted by user agent, because Applebot and GPTBot do not
matter enough to verify and their address ranges move.

IDEMPOTENT. Each run re-counts the last 20 minutes and upserts on (slot,
crawler), so running twice changes nothing and one missed run is recovered by the
next. Two consecutive misses lose those slots, which Dagster will have recorded
as failures.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

CONTAINER = "trading-framework-caddy-1"
DOCKER = "/opt/homebrew/bin/docker"

#: Googlebot's published ranges, and the other Google properties that crawl.
#: Verified by address because the user agent is a free-text header.
GOOGLE_PREFIXES = ("66.249.", "64.233.", "72.14.", "74.125.", "209.85.",
                   "216.239.", "35.191.", "130.211.")
#: bingbot's long-standing ranges. Microsoft publishes more; these cover the
#: crawler in practice. A miss here undercounts Bing, which is the safe
#: direction for a number used to detect a COLLAPSE.
BING_PREFIXES = ("40.77.", "157.55.", "207.46.", "13.66.", "199.30.")

#: Everything else, matched on the user agent. Order matters only in that the
#: first match wins.
UA_AGENTS = (
    ("applebot", "Applebot"),
    ("gptbot", "GPTBot"),
    ("oai_searchbot", "OAI-SearchBot"),
    ("claudebot", "ClaudeBot"),
    ("perplexitybot", "PerplexityBot"),
    ("meta_externalagent", "meta-externalagent"),
    ("amazonbot", "Amazonbot"),
    ("bytespider", "Bytespider"),
    ("yandexbot", "YandexBot"),
    ("ahrefsbot", "AhrefsBot"),
    ("semrushbot", "SemrushBot"),
    ("mj12bot", "MJ12bot"),
    ("dotbot", "DotBot"),
)

#: Ten minutes. Small enough to sit inside a nineteen-minute retention window,
#: big enough that a day is 144 rows per crawler rather than 1,440.
SLOT_MINUTES = 10

DDL = """
CREATE TABLE IF NOT EXISTS crawler_activity (
    slot       timestamptz NOT NULL,
    crawler    text        NOT NULL,
    requests   int         NOT NULL,
    errors     int         NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (slot, crawler)
)
"""
DDL_INDEX = ("CREATE INDEX IF NOT EXISTS crawler_activity_slot_idx "
             "ON crawler_activity (slot DESC)")


def classify(ua: str, client_ip: str) -> str | None:
    """Which crawler is this, if any? None means a human or an unknown bot."""
    if client_ip.startswith(GOOGLE_PREFIXES):
        return "googlebot"
    if client_ip.startswith(BING_PREFIXES):
        return "bingbot"
    low = ua.lower()
    # Claims Google or Bing without the address. Recorded separately so a
    # spoofed UA can never be mistaken for a crawl recovery.
    if "googlebot" in low or "google-inspectiontool" in low:
        return "googlebot_ua_only"
    if "bingbot" in low or "msnbot" in low:
        return "bingbot_ua_only"
    for key, needle in UA_AGENTS:
        if needle.lower() in low:
            return key
    return None


def read_log(minutes: int) -> list[dict]:
    """The access log, as far back as `minutes`. Empty on any docker failure —
    this is observability, and it must never be the reason a job fails."""
    try:
        proc = subprocess.run(
            [DOCKER, "logs", "--since", f"{minutes}m", CONTAINER],
            capture_output=True, text=True, timeout=600,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        print(f"could not read the container log: {e}", file=sys.stderr)
        return []
    out = []
    # Caddy writes to stderr; take both and let the JSON filter decide.
    for line in (proc.stdout + proc.stderr).splitlines():
        if '"handled request"' not in line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def tally(entries: list[dict]) -> dict[tuple[datetime, str], tuple[int, int]]:
    counts: Counter = Counter()
    errors: Counter = Counter()
    for d in entries:
        req = d.get("request") or {}
        headers = req.get("headers") or {}
        ua = (headers.get("User-Agent") or [""])[0]
        # Cloudflare fronts the origin, so remote_ip is Cloudflare's. The real
        # client is in X-Forwarded-For, first hop.
        xff = (headers.get("X-Forwarded-For") or [""])[0]
        client = xff.split(",")[0].strip()
        who = classify(ua, client)
        if who is None:
            continue
        ts = d.get("ts")
        if ts is None:
            continue
        when = datetime.fromtimestamp(ts)
        slot = when.replace(minute=(when.minute // SLOT_MINUTES) * SLOT_MINUTES,
                            second=0, microsecond=0)
        key = (slot, who)
        counts[key] += 1
        status = d.get("status") or 0
        if status >= 400:
            errors[key] += 1
    return {k: (counts[k], errors.get(k, 0)) for k in counts}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--minutes", type=int, default=20,
                    help="how far back to re-count; upserts, so overlap is free")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = tally(read_log(args.minutes))
    if not rows:
        # Not an error. A quiet hour on a container that just restarted is
        # indistinguishable from this, and neither should page anyone.
        print("no crawler requests found in the window")
        return 0

    if args.dry_run:
        for (slot, who), (n, err) in sorted(rows.items()):
            print(f"{slot:%Y-%m-%d %H:%M}  {who:22} {n:>6}  errors {err}")
        return 0

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SET lock_timeout = '10s'")
        cur.execute(DDL)
        cur.execute(DDL_INDEX)
        for (slot, who), (n, err) in rows.items():
            # GREATEST, not assignment. The newest slot is still filling when a
            # run lands mid-slot, so a later partial read must never lower a
            # count an earlier fuller read already established.
            cur.execute(
                """INSERT INTO crawler_activity (slot, crawler, requests, errors)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (slot, crawler) DO UPDATE
                     SET requests = GREATEST(crawler_activity.requests,
                                             EXCLUDED.requests),
                         errors = GREATEST(crawler_activity.errors,
                                           EXCLUDED.errors),
                         updated_at = now()""",
                (slot, who, n, err),
            )
        conn.commit()
    finally:
        conn.close()

    total = sum(n for n, _ in rows.values())
    goog = sum(n for (_, w), (n, _) in rows.items() if w == "googlebot")
    print(f"recorded {len(rows)} (slot, crawler) rows, {total} requests; "
          f"googlebot (IP-verified) {goog}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
