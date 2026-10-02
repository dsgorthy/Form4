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
  5. Intraday ingest: hours since the last row was WRITTEN, during EDGAR hours
  6. Nightly jobs actually SUCCEEDED — a failing job is invisible to (3),
     which measures data age and cannot tell a stalled feed from a slow one
  7. High-frequency services (5-30 min cadence) are still ticking AND their
     latest completed run succeeded. Recency alone passed notification_scanner
     for 17 days while 3,811 of its 3,912 runs were status='failed'

Exit 0 = all good, 1 = at least one problem (and an ntfy push was attempted).

Usage (on the Mini):
    python3 scripts/offbox_watchdog.py            # check + alert on problems
    python3 scripts/offbox_watchdog.py --dry-run  # report, never alert
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

STUDIO = "100.78.9.66"
SSH_TARGET = f"derekg@{STUDIO}"

# RETIRED 2026-09-30: trytailorly.com. The service is wound down, so a page
# probe against it is pure noise — and noise in a pager is worse than nothing,
# because it trains the reader to swipe the whole topic away. It was pushing on
# every cycle while the Studio was unreachable.
#
# NOTE FOR WHOEVER WINDS IT DOWN PROPERLY: as of 2026-09-30 the site still
# answered 200 and job-search-project-{api,frontend,caddy} were all running on
# Studio, so the containers and the tunnel are still live. Removing the monitor
# does not stop the service; it only stops the alerts.
ENDPOINTS = {
    "form4.app": "https://form4.app/",
}

# The pages Google sends people to, fetched the way Googlebot fetches them.
# Between 2026-09-16 and 09-20 the edge dropped connections in bursts that
# the home-page probe above never hit, and Google visitors went to zero; the
# home page was 200 every time it was asked. These ask for what Google asks
# for, and check that the page still says what Google needs to hear.
GOOGLEBOT_UA = ("Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; "
                "Googlebot/2.1; +http://www.google.com/bot.html) Chrome/126.0 Safari/537.36")
PAGE_PROBES = {
    "insider page (as Googlebot)": "https://form4.app/insider/gianluca-romano",
    "company page (as Googlebot)": "https://form4.app/company/NVDA",
    "robots.txt (as Googlebot)": "https://form4.app/robots.txt",
    "sitemap index (as Googlebot)": "https://form4.app/sitemap.xml",
}


def check_page(url: str, code: int, body: str) -> list[str]:
    """What is wrong with this response, for Google. Empty means nothing. Pure."""
    problems = []
    if code != 200:
        return [f"HTTP {code}"]
    if url.endswith("robots.txt"):
        if "Sitemap:" not in body:
            problems.append("robots.txt has no Sitemap line")
        if "Disallow: /\n" in body or body.rstrip().endswith("Disallow: /"):
            problems.append("robots.txt disallows everything")
        return problems
    if url.endswith("sitemap.xml"):
        if "<sitemapindex" not in body or "<loc>" not in body:
            problems.append("sitemap index is not a sitemapindex with locs")
        return problems
    low = body.lower()
    if "noindex" in low:
        problems.append("page carries noindex")
    if 'rel="canonical"' not in low:
        problems.append("page has no canonical")
    if len(body) < 20_000:
        problems.append(f"page is only {len(body)} bytes; the record did not render")
    return problems


# ── THE FRONTEND HEAP, AND WHY THIS IS NOT A SITEMAP CHECK ──────────────────
#
# On 2026-10-01 the frontend container was OOM-killed nine times. 32% of origin
# requests returned 502 and p95 on /filing/ reached 43 seconds. The cause was a
# 2.33 MB sitemap payload that Next declined to cache — it logs that it declined
# and serves the request, so every crawl re-parsed the whole corpus into a
# 2,080 MB heap.
#
# The sitemap is now sliced server-side and a test bounds every section
# (tests/unit/test_sitemap_payload_stays_cacheable.py). But the thing that made
# this expensive was not the sitemap: it was that NOTHING WATCHED THE HEAP.
# /api/v1/health answered 200 the whole time, Postgres answered in 0.01s,
# Dagster was green and every launchd agent was up. The first signal was a user
# saying the site was throwing errors.
#
# So these two checks are deliberately about the SYMPTOM CLASS rather than this
# cause. Any oversized fetch, from any route, added at any time, trips the
# first. Any path to heap exhaustion trips the second.

#: Next.js refuses to cache a fetch response over 2 MB. Mirrors
#: NEXT_DATA_CACHE_MAX_BYTES in api/routers/sitemap.py; it is Next's constant,
#: not ours, which is why both places name it rather than importing one from
#: the other across a language boundary.
UNCACHEABLE_MARKER = "items over 2MB can not be cached"

#: Node's default old-space ceiling for this container, in MB, as its own GC
#: reports it: "Mark-Compact 1967.1 (2080.6) -> ...". Past this fraction of the
#: ceiling the process is spending its time in GC rather than serving.
HEAP_ALARM_FRACTION = 0.85


def evaluate_frontend_heap(log_tail: str) -> list[str]:
    """Problems visible in the frontend container's recent log. Pure.

    Two independent signals, because they fail at different times: the
    uncacheable-fetch line appears as soon as a payload crosses the ceiling,
    hours or days before the heap actually runs out, while the GC lines appear
    once it is already too late to be graceful.

    A tail with no PROBE_OK sentinel means the probe itself did not run, which
    is a problem in its own right and NOT the same as a quiet frontend. See
    PROBE_OK.
    """
    problems: list[str] = []

    if PROBE_OK not in log_tail:
        return [
            "frontend heap: the probe did not run (no sentinel) — docker logs "
            "failed on Studio, so nothing is watching the heap right now"
        ]
    log_tail = log_tail.replace(PROBE_OK, "")

    if UNCACHEABLE_MARKER in log_tail:
        # Name the URLs, because the fix is always "make that response smaller"
        # and the reader needs to know which one.
        urls = sorted({
            m.group(1)
            for m in re.finditer(r"Failed to set Next\.js data cache for (\S+)", log_tail)
        })
        shown = ", ".join(u.split("?")[0] for u in urls[:3]) or "unknown route"
        problems.append(
            f"frontend has an UNCACHEABLE fetch ({shown}): over Next's 2 MB "
            f"data-cache ceiling, so every request re-fetches and re-parses it "
            f"into the Node heap. This is what OOM-killed the container on "
            f"2026-10-01."
        )

    if "JavaScript heap out of memory" in log_tail:
        problems.append(
            "frontend hit 'JavaScript heap out of memory' — the container has "
            "been OOM-killed; expect 502s and multi-second p95 until it settles"
        )

    # "Mark-Compact 1967.1 (2080.6) -> 1945.8 (2076.1) MB"
    peaks = [
        (float(m.group(1)), float(m.group(2)))
        for m in re.finditer(r"Mark-Compact ([\d.]+) \(([\d.]+)\)", log_tail)
    ]
    near = [(used, cap) for used, cap in peaks if cap and used / cap >= HEAP_ALARM_FRACTION]
    if near:
        used, cap = max(near)
        problems.append(
            f"frontend heap reached {used:.0f} MB of a {cap:.0f} MB ceiling "
            f"({100 * used / cap:.0f}%) in {len(near)} GC cycles — it is in a "
            f"GC spiral, which shows up as slow pages before it shows up as 502s"
        )
    return problems


#: Printed last when the probe genuinely ran. Without it a HEALTHY frontend is
#: indistinguishable from a broken probe: `grep` exits 1 when it matches
#: nothing, `ssh_run` returns None on any non-zero exit, and the watchdog read
#: that as "could not read the container log" — pushing a false alarm every
#: thirty minutes for a frontend that was perfectly well. Caught on the first
#: dry run after writing it, 2026-10-01.
#:
#: `|| true` alone would be worse than the bug: it makes a real docker failure
#: look like silence, which is the direction that hides outages.
PROBE_OK = "__PROBE_OK__"

#: Enough log to see a GC spiral building without shipping megabytes over ssh.
#: The container is restarted on deploy, so this is minutes-to-hours of history.
FRONTEND_LOG_CMD = (
    "/opt/homebrew/bin/docker logs --tail 4000 trading-framework-frontend-1 "
    "> /tmp/watchdog_frontend.log 2>&1 && { "
    "/usr/bin/grep -E 'can not be cached|heap out of memory|Mark-Compact' "
    "/tmp/watchdog_frontend.log | /usr/bin/tail -60; "
    f"/bin/echo {PROBE_OK}; }}"
)

#: The sections the sitemap index actually names, asked for the way the
#: frontend asks. insiders-0 is the biggest and the first to cross a budget;
#: companies is unchunked, so it is the one that grows without a bound of its
#: own and is worth asking about every cycle.
SITEMAP_SECTIONS = {
    "companies": "section=companies&chunk=0&chunk_size=20000&limit_insiders=60000",
    "insiders-0": "section=insiders&chunk=0&chunk_size=20000&limit_insiders=60000",
}


def evaluate_sitemap_sections(results: "dict[str, dict]") -> list[str]:
    """Problems across the sitemap section responses. Pure.

    `results` maps a label to the parsed JSON of one
    /api/v1/sitemap/urls?section=... response, or {} where the fetch failed.

    Checks the API's own self-report rather than re-deriving the budget here:
    the endpoint measures the bytes it is about to send and says whether they
    fit. One definition, on the side that knows.
    """
    problems: list[str] = []
    for label, body in sorted(results.items()):
        if not body:
            problems.append(f"sitemap section {label}: fetch failed")
            continue
        if body.get("cacheable") is False:
            problems.append(
                f"sitemap section {label} is {body.get('payload_bytes', 0):,} "
                f"bytes and reports itself NOT cacheable — lower CHUNK in "
                f"frontend/src/lib/sitemap-data.ts before the frontend heap "
                f"pays for it"
            )
        counts = body.get("counts") or {}
        # An empty corpus is the silent-shrink failure: a valid, empty sitemap
        # is indistinguishable from a broken one without the totals.
        if not counts.get("tickers") or not counts.get("insiders"):
            problems.append(
                f"sitemap section {label}: corpus reports "
                f"{counts.get('tickers', 0)} tickers / "
                f"{counts.get('insiders', 0)} insiders — the sitemap would "
                f"publish an empty urlset"
            )
    return problems

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
    # 10 days was set when congress was a manual scrape. It is a daily
    # dataplane feed now, and that budget is what let a 2-day stall on
    # 2026-08-12/13 pass unnoticed. 5 still survives a long weekend —
    # disclosures are sporadic and a quiet Friday-to-Monday is normal — while
    # putting a real stall inside the same week. The job-success check below
    # is the primary signal; this is defence in depth.
    ("congress", "form4",
     "SELECT max(filing_date) FROM congress_trades", 5),
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

# (job name, max hours since its last SUCCESS)
#
# Data-age checks cannot see a job that fails. daily_signals failed on
# 2026-08-12 and 08-13 and every freshness budget stayed green throughout,
# because congress carries a 10-day budget and the other signals in that job
# have a legitimate 1-day as_of lag that makes "yesterday" look normal.
#
# This asks a different question: did the job SUCCEED. Keyed on run status
# rather than observation recency precisely because of that lag — a signal can
# be a day behind by design and still be perfectly healthy.
#
# A FLAT HOUR BUDGET CANNOT DESCRIBE A WEEKDAY-ONLY JOB.
#
# This was "hours since last success <= 30" for both jobs. form4_pipeline runs
# Mon-Fri at 17:30 PT, so from midnight on Monday the gap back to Friday's run
# is 30.5 hours and climbing, against a 30-hour budget — it failed every
# Monday from 00:00 until the 17:30 run cleared it. The watchdog fires every
# 30 minutes and does not deduplicate, so that is ~35 push notifications a
# week for a pipeline that was doing exactly what it was told. Logged 18 on
# Monday 2026-08-17 and 19 on Monday 2026-08-24, and on no other day.
#
# The `weekday() < 5` guard below suppresses the check on Saturday and Sunday,
# which is why this looked handled. It is not: Monday is precisely when the
# gap is at its maximum.
#
# So ask the question we actually mean. Not "how long since it last ran?" but
# "has it run since the last time it was SUPPOSED to?" — which needs the
# schedule, not a duration.
#
#   days   : weekday numbers it fires on, Monday=0 (as date.weekday()).
#   hour/minute + tz : when, in the schedule's own timezone.
#   grace_h: how long after a scheduled fire before a missing success counts.
#            One slow run plus a retry. form4_pipeline takes ~6 minutes.
#
# Keep in step with dataplane/dagster_project/definitions.py —
# tests/unit/test_watchdog_schedules.py fails the build if these drift.
WEEKDAYS = (0, 1, 2, 3, 4)
EVERY_DAY = (0, 1, 2, 3, 4, 5, 6)

JOB_SUCCESS = [
    # build_schedule_from_partitioned_job(hour_of_day=4, minute_of_hour=30) UTC
    {"job": "daily_signals", "days": EVERY_DAY, "hour": 4, "minute": 30,
     "tz": "UTC", "grace_h": 6},
    # cron_schedule="30 17 * * 1-5", execution_timezone America/Los_Angeles
    {"job": "form4_pipeline", "days": WEEKDAYS, "hour": 17, "minute": 30,
     "tz": "America/Los_Angeles", "grace_h": 6},
]

# Return the last success as an ABSOLUTE instant, not an age.
#
# Asking for an age and subtracting it from a locally-captured `now` mixes two
# clocks: `now` is read before the SSH round-trip and the age is measured from
# Postgres's clock after it, so the reconstructed timestamp lands ~1s early.
# Both jobs then failed a boundary comparison by under a second, because a
# Dagster run created at 17:30:00.34 was being judged against a 17:30:00 due
# time and losing.
#
# `create_timestamp` is `timestamp without time zone` holding LOCAL Pacific —
# a run scheduled 17:30 America/Los_Angeles stores 17:30. AT TIME ZONE reads it
# back as the instant it actually was. Same assumption as _DB_TZ below.
JOB_SUCCESS_SQL = (
    "SELECT max(create_timestamp) AT TIME ZONE 'America/Los_Angeles' "
    "FROM runs WHERE pipeline_name = '{job}' AND status = 'SUCCESS'"
)


# ── high-frequency services: is the clock still turning? ───────────────────
#
# JOB_SUCCESS above covers two DAILY Dagster jobs out of dagster_runs.runs.
# Nothing covered the minute-to-minute services, and on 2026-09-08 that gap
# cost 57 hours: between 00:57 and 01:20 SIX of them stopped firing within 23
# minutes of each other and stayed stopped, while this watchdog printed "all
# checks passed" on every run in between. Every data-freshness check here kept
# passing because the DATA was fine — the jobs that maintain it had simply
# stopped, and no check asked that question.
#
# The cause was launchd: a census that day found six jobs on StartInterval, all
# six dead, and fourteen on StartCalendarInterval or KeepAlive, all fourteen
# alive. macOS defers interval timers indefinitely on a long-uptime box. But
# the reason it went unseen for 57 hours is this list not existing, and that
# would be just as true of a Dagster schedule silently unregistering.
#
# budget_m is derived from MEASURED inter-run gaps over the 30 days before the
# stall, not from the nominal cadence — see
# feedback_monitor_budgets_follow_schedules. Each budget is at least twice the
# largest gap actually observed, so a missed tick or a slow run is not a page:
#
#   service                       median   p95    max seen   budget
#   form4_uptime                    1.1     1.1      14.4       30
#   notification_scanner            5.0     5.1      26.7       45
#   insider_fetch                   5.2     6.0      20.1       45
#   strategy_intraday              10.0    10.0      30.0       60
#   heartbeat_probe                15.0    15.0      30.0       60
#   refresh-open-position-prices   15.0    15.0      30.0       60
#   freshness_probe                30.0    30.0      60.0      120
#
# All seven run continuously — no market-hours or weekday gating — which is why
# a flat budget is honest here. Do not copy that assumption to a job with a
# calendar schedule; use JOB_SUCCESS and last_expected_fire() for those.
#
# RECENCY IS NOT HEALTH. Each service gets a second, independent verdict.
#
# As first written on 2026-09-10 this asked only when a service last STARTED.
# From 2026-08-25 13:14 to 2026-09-10 23:10 notification_scanner started every
# five minutes and wrote status='failed' on 3,811 of its 3,912 runs — the same
# TypeError each time; the 101 'ok' rows all fall on 09-01. At 22:52 on 09-10
# this watchdog printed "OK notification_scanner last run: 2m ago" and then
# "all checks passed"; the row it had just read, started 22:50:07, was
# 'failed'. A loop that turns and fails on every turn has the same heartbeat
# as a healthy one. Same lesson as feedback_liveness_is_not_health, learnt
# again on a different service.
#
# So: stale (started_at older than budget_m) and failing (latest COMPLETED run
# is not 'ok') are judged separately and both reported. 'running' rows are
# excluded from the status verdict because this watchdog regularly lands
# mid-run — insider_fetch has taken 904s and notification_scanner 1,300s on
# 5-minute cadences — and a run in progress has no verdict yet. They still
# count for recency, so a row that hangs in 'running' forever ages out like
# any other silence rather than pinning the service at "fresh".
SERVICE_HEARTBEAT = [
    {"service": "form4_uptime", "budget_m": 30},
    {"service": "insider_fetch", "budget_m": 45},
    {"service": "notification_scanner", "budget_m": 45},
    {"service": "strategy_intraday", "budget_m": 60},
    {"service": "heartbeat_probe", "budget_m": 60},
    {"service": "refresh-open-position-prices", "budget_m": 60},
    {"service": "freshness_probe", "budget_m": 120},
]

# One query for all of them: this runs from the Mini over SSH every 30 minutes,
# and seven round-trips to answer one question is six too many.
#
# started_at is `timestamp with time zone`, so AT TIME ZONE renders it as local
# Pacific and _parse_ts stamps _DB_TZ back on. Note this is the opposite
# direction to JOB_SUCCESS_SQL, where create_timestamp is naive and AT TIME
# ZONE attaches an offset instead.
#
# Four columns, and the order is load-bearing. max(started_at) is over ALL
# rows, 'running' included, so recency is never masked by a hung run. Status
# and error come from the newest row that is NOT 'running' (see above). The
# error is the LAST column and only its first line: error_message holds a
# full traceback, and psql -A would emit every line of it as if it were a
# row, while a '|' inside a Python exception text — repr of a dict, an SQL
# fragment — would shift columns if anything followed it. The consumer splits
# on at most three '|' for the same reason.
#
# No double quote and no '$' anywhere in this string: ssh_psql hands it to a
# remote shell inside double quotes. chr(10) instead of E'\n' for that reason.
# Proven through that exact path on 2026-09-11 (132ms over 264k rows, on the
# (service, started_at DESC) index); with `AND started_at < '2026-09-10
# 23:12-07'` appended it returns the incident row:
#   notification_scanner|2026-09-10 23:10:05.606411|failed|TypeError: '<' ...
SERVICE_HEARTBEAT_SQL = (
    "SELECT service, "
    "max(started_at) AT TIME ZONE 'America/Los_Angeles', "
    "coalesce((array_agg(status ORDER BY started_at DESC) "
    "FILTER (WHERE status <> 'running'))[1], ''), "
    "coalesce((array_agg(split_part(error_message, chr(10), 1) "
    "ORDER BY started_at DESC) FILTER (WHERE status <> 'running'))[1], '') "
    "FROM pipeline_runs WHERE service IN ({names}) GROUP BY service"
)


# Long-running agents on the Studio that must ALWAYS have a pid. `launchctl
# list` prints "-" in the PID column for a job that is loaded but not running,
# and reports the last exit code -- 0 -- as if that were fine. A KeepAlive=true
# job stays that way once launchd itself has stopped it. On 2026-09-15
# 23:25:37 a Colima restart left six of these dead at once -- both non-form4
# cloudflared tunnels, the GitHub deploy runner (so pushes to main queued
# instead of deploying), dagster-daemon (so every Dagster schedule stopped),
# dagster-webserver and Ollama -- for 12 to 19 hours. The service-heartbeat
# check below caught the Dagster symptom; nothing named the cause. This does.
MUST_RUN_AGENTS = [
    ("com.cloudflare.cloudflared", "form4.app tunnel"),
    # com.openclaw.tailorly-tunnel removed 2026-09-30 — see ENDPOINTS above.
    # It was one of the six agents that died in the 2026-09-15 Colima incident,
    # which is why it was listed; the service it fronts is now wound down.
    ("com.cloudflare.cloudflared.designquiz", "interiordesignfordummies.com tunnel"),
    ("homebrew.mxcl.colima", "the Docker VM"),
    ("com.derekg.lima-master-keepalive", "the VM's port forwards"),
    ("com.openclaw.dagster-daemon", "every Dagster schedule"),
    ("com.openclaw.dagster-webserver", "Dagster UI"),
    ("com.openclaw.pyrrho-desk", "dataplane desk"),
    ("actions.runner.dsgorthy-Form4.dereks-mac-studio", "push-to-main deploys"),
    ("com.ollama.server", "Ollama"),
]

LAUNCHCTL_LIST_CMD = "/bin/launchctl list"


def evaluate_must_run_agents(launchctl_list: str) -> "tuple[list[str], list[str]]":
    """`launchctl list` output -> (problems, report lines). Pure.

    Columns are PID, last exit status, label; PID is "-" when nothing is
    running. Both "not loaded" and "loaded but not running" are problems: the
    remedy for the second is `launchctl kickstart -k gui/$(id -u)/<label>`,
    which the message says, because at 4 am nobody should have to work it out.
    """
    pids: dict[str, str] = {}
    for line in launchctl_list.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2] != "Label":
            pids[parts[2]] = parts[0]
    problems: list[str] = []
    report: list[str] = []
    for label, role in MUST_RUN_AGENTS:
        pid = pids.get(label)
        if pid is None:
            report.append(f"  FAIL {label}: not loaded")
            problems.append(f"{label} ({role}) is not loaded in launchd on Studio")
        elif pid == "-":
            report.append(f"  FAIL {label}: loaded, not running")
            problems.append(
                f"{label} ({role}) is loaded but NOT RUNNING on Studio -- "
                f"launchctl kickstart -k gui/$(id -u)/{label}"
            )
        else:
            report.append(f"  OK   {label}: pid {pid}")
    return problems, report


def last_expected_fire(spec: dict, now: "datetime | None" = None) -> datetime:
    """The most recent scheduled fire that has had its grace period elapse.

    Walks back day by day in the schedule's own timezone, so DST is handled by
    zoneinfo rather than by arithmetic, and a weekday-only job simply skips the
    weekend instead of accumulating hours across it.
    """
    tz = ZoneInfo(spec["tz"])
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    # 10 days back covers a Friday job seen the following Monday with room to
    # spare; nothing here fires less often than weekly.
    for back in range(0, 11):
        day = (now - timedelta(days=back)).date()
        if day.weekday() not in spec["days"]:
            continue
        fire = datetime(day.year, day.month, day.day,
                        spec["hour"], spec["minute"], tzinfo=tz)
        if fire + timedelta(hours=spec["grace_h"]) <= now:
            return fire.astimezone(timezone.utc)
    # No scheduled fire in the window has come due yet.
    return (now - timedelta(days=11)).astimezone(timezone.utc)


def evaluate_service_heartbeats(rows: str, now: datetime) -> "tuple[list[str], list[str]]":
    """Judge every SERVICE_HEARTBEAT entry from one SERVICE_HEARTBEAT_SQL result.

    Pure so the 2026-09-10 row can be replayed in a test without Studio:
    `rows` is the psql -tA text, `now` the instant to age against. Returns
    (problems, report_lines) — one report line per service, and for each
    service up to TWO problems, judged independently:

      stale   — newest started_at older than budget_m
      failing — newest COMPLETED run is not 'ok'

    They are not folded into one verdict because they are different faults
    with different fixes: a stale service has a scheduler problem, a failing
    one has a code problem, and 3h of failures is both. An empty status means
    every row so far is still 'running' — no verdict yet, which is not a
    failure; the recency verdict still applies to it.
    """
    problems: list[str] = []
    report: list[str] = []

    seen: dict = {}
    for line in rows.splitlines():
        # The error is the last column, and this cap is what keeps a '|' inside
        # an exception message inside its column.
        parts = line.split("|", 3)
        if len(parts) < 4 or not parts[0].strip():
            continue
        svc, ts, status, err = (p.strip() for p in parts)
        seen[svc] = (ts, status, err)

    for spec in SERVICE_HEARTBEAT:
        svc, budget = spec["service"], spec["budget_m"]
        row = seen.get(svc)
        if row is None:
            # A service with no row at all is not "fine by default" — it is
            # the same silence, one step earlier.
            problems.append(f"{svc}: no run on record in pipeline_runs")
            report.append(f"  FAIL {svc}: never ran")
            continue
        ts, status, err = row
        last = _parse_ts(ts)
        if last is None:
            problems.append(f"{svc}: unparseable last-run time {ts!r}")
            report.append(f"  FAIL {svc}: unparseable last run {ts!r}")
            continue

        age_m = (now - last).total_seconds() / 60.0
        fresh = age_m <= budget
        healthy = status in ("ok", "")

        shown = status or "running (no completed run yet)"
        if not healthy and err:
            # First line of the traceback only, and not all of that: this ends
            # up in a push notification, not a log.
            shown += f": {err[:200]}"
        report.append(
            f"  {'OK  ' if fresh and healthy else 'FAIL'} {svc} last run: "
            f"{age_m:.0f}m ago (budget {budget}m); status {shown}"
        )
        if not fresh:
            problems.append(
                f"{svc} has not run for {age_m:.0f} minutes "
                f"(budget {budget}m); last run {last:%Y-%m-%d %H:%M %Z}"
            )
        if not healthy:
            problems.append(f"{svc} last completed run {shown}")

    return problems, report


def _header_safe(text: str) -> str:
    """Make a string safe to put in an HTTP header.

    Headers are latin-1; urllib raises on anything outside it. The body is
    sent as explicit UTF-8 and is unaffected — this is only for Title.

    Found 2026-08-14 by sending a test push whose title contained an em-dash:
    the request threw, the exception was swallowed by the handler below, and
    the alert vanished with only a line on stderr that nothing reads. An alert
    channel that drops messages on a character class is worse than no channel,
    because it looks healthy right up until the message that mattered.
    """
    return text.encode("latin-1", "replace").decode("latin-1")


def notify(title: str, message: str, topic: str) -> None:
    """Push to ntfy. Topic is treated as a secret (it is the auth)."""
    if not topic:
        print("  [no NTFY_ALERT_TOPIC — cannot alert]", file=sys.stderr)
        return
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}",
        data=message.encode("utf-8"),
        headers={
            "Title": _header_safe(title),
            "Priority": "high",
            "Tags": "rotating_light",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=15)
    except Exception as exc:  # noqa: BLE001
        # Last resort: retry with a title that cannot possibly offend, so a
        # formatting problem downgrades the alert instead of deleting it.
        print(f"  [ntfy push failed: {exc}; retrying with plain title]", file=sys.stderr)
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    f"https://ntfy.sh/{topic}",
                    data=message.encode("utf-8"),
                    headers={"Title": "Studio watchdog alert", "Priority": "high"},
                ),
                timeout=15,
            )
        except Exception as exc2:  # noqa: BLE001
            print(f"  [ntfy retry also failed: {exc2}]", file=sys.stderr)


def ssh_run(remote_cmd: str) -> str | None:
    """Run one command on Studio. None means unreachable or it failed."""
    cmd = ["ssh", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", SSH_TARGET, remote_cmd]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    except subprocess.TimeoutExpired:
        return None
    if out.returncode != 0:
        return None
    return out.stdout or None


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

    for name, url in PAGE_PROBES.items():
        req = urllib.request.Request(url, headers={"User-Agent": GOOGLEBOT_UA, "Accept": "text/html,application/xml"})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                code, body = r.status, r.read(600_000).decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            code, body = (getattr(exc, "code", 0) or 0), ""
        found = check_page(url, code, body)
        print(f"  {'OK  ' if not found else 'FAIL'} {name}: HTTP {code}" + (f" -- {'; '.join(found)}" if found else ""))
        problems.extend(f"{name}: {f}" for f in found)

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

    # Did the nightly jobs actually SUCCEED since they were last due?
    #
    # No weekday guard here any more. It was doing the wrong job: it silenced
    # Saturday and Sunday, when a weekday-only pipeline is correctly idle, and
    # left Monday — when the gap back to Friday is at its widest — fully
    # armed against a flat 30-hour budget. Each job now carries its own
    # schedule, so a weekend is skipped by construction on every day of the
    # week, and a genuinely missed Saturday run of a 7-day job is still caught.
    now = datetime.now(timezone.utc)
    for spec in JOB_SUCCESS:
        job = spec["job"]
        due = last_expected_fire(spec, now)
        val = ssh_psql("dagster_runs", JOB_SUCCESS_SQL.format(job=job))
        if not val:
            problems.append(f"{job}: no successful run on record")
            print(f"  FAIL {job}: never succeeded")
            continue
        last_success = _parse_ts(val)
        if last_success is None:
            print(f"  FAIL {job}: unparseable last success {val!r}")
            problems.append(f"{job}: unparseable last-success time {val!r}")
            continue
        age_h = (now - last_success).total_seconds() / 3600.0
        ok = last_success >= due
        print(f"  {'OK  ' if ok else 'FAIL'} {job} last success: "
              f"{age_h:.1f}h ago; last due {due:%Y-%m-%d %H:%M}Z "
              f"(+{spec['grace_h']}h grace)")
        if not ok:
            problems.append(
                f"{job} has not succeeded since it was last due "
                f"({due:%a %Y-%m-%d %H:%M}Z); last success {age_h:.1f}h ago"
            )

    # Is the clock still turning on the high-frequency services — and did the
    # last turn actually succeed?
    names = ", ".join("'" + s["service"] + "'" for s in SERVICE_HEARTBEAT)
    rows = ssh_psql("form4", SERVICE_HEARTBEAT_SQL.format(names=names))
    if rows is None:
        problems.append("service heartbeats: could not query pipeline_runs")
        print("  FAIL service heartbeats: query failed")
    else:
        svc_problems, svc_report = evaluate_service_heartbeats(rows, now)
        print("\n".join(svc_report))
        problems.extend(svc_problems)

    # Are the daemons that everything above depends on actually running?
    listing = ssh_run(LAUNCHCTL_LIST_CMD)
    if listing is None:
        problems.append("must-run agents: could not read launchctl list on Studio")
        print("  FAIL must-run agents: launchctl list failed")
    else:
        agent_problems, agent_report = evaluate_must_run_agents(listing)
        print("\n".join(agent_report))
        problems.extend(agent_problems)

    # Is the frontend about to run out of heap? Nothing watched this before
    # 2026-10-01 and it cost 40 minutes of 502s. See evaluate_frontend_heap.
    heap_log = ssh_run(FRONTEND_LOG_CMD)
    if heap_log is None:
        problems.append("frontend heap: could not read the container log on Studio")
        print("  FAIL frontend heap: docker logs failed")
    else:
        heap_problems = evaluate_frontend_heap(heap_log)
        print("\n".join(f"  {'FAIL' if heap_problems else 'OK  '} frontend heap"
                        f"{': ' + p if p else ''}" for p in (heap_problems or [""])))
        problems.extend(heap_problems)

    # Does every sitemap section still fit the cache it depends on?
    sections = {}
    for label, qs in SITEMAP_SECTIONS.items():
        raw = ssh_run(
            "/usr/bin/curl -s --max-time 30 "
            f"'http://127.0.0.1/api/v1/sitemap/urls?{qs}'"
        )
        try:
            body = json.loads(raw) if raw else {}
            # Keep the envelope, drop the URL lists — this runs every cycle and
            # there is no reason to move a megabyte through ssh to count it.
            sections[label] = {k: body.get(k) for k in
                               ("counts", "returned", "payload_bytes", "cacheable")}
        except (ValueError, TypeError):
            sections[label] = {}
    sitemap_problems = evaluate_sitemap_sections(sections)
    for label in sorted(sections):
        b = sections[label]
        print(f"  {'OK  ' if b.get('cacheable') else 'FAIL'} sitemap {label}: "
              f"{b.get('payload_bytes') or 0:,} bytes, cacheable={b.get('cacheable')}")
    problems.extend(sitemap_problems)

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


# ── NOTIFY ON CHANGE, NOT ON STATE ──────────────────────────────────────────
#
# This used to push a high-priority ntfy on EVERY run that had any problem, and
# it runs every thirty minutes. On 2026-10-01/02 five to eight problems sat
# unresolved for about eighteen hours — three hung launchd jobs, a stale
# congress feed, a pipeline that had missed two ticks — and each one was
# re-pushed 36 times. Derek's words: "ive gotten a ton of push notifications".
#
# None of those pushes carried new information, and that is the damage: a pager
# that repeats itself teaches the reader to swipe the topic away, which is how
# the one that matters gets missed. Same reasoning that retired the Tailorly
# probe on 09-30 and that fixed the heap probe's false alarm earlier today.
#
# So: push when the SET of problems changes, or when a long-running problem
# needs re-asserting, and stay quiet otherwise. The printed output is
# unchanged — the log always lists everything, every run.

#: Where the last-seen problem set lives, so change can be detected across
#: runs. On the Mini, next to the watchdog's own log.
STATE_PATH = Path(__file__).resolve().parents[1] / "logs" / "offbox_watchdog_state.json"

#: Re-assert an unchanged, unresolved problem set this often, so something
#: broken for days does not go completely silent.
REASSERT_HOURS = 12


def problem_key(problem: str) -> str:
    """Identity of a problem, with the parts that move every cycle removed.

    THIS IS THE LOAD-BEARING PART. "heartbeat_probe has not run for 375
    minutes" becomes a different string every single cycle, so naive
    change-detection would fire every time and change nothing. Collapsing the
    numbers makes one defect one key for as long as it lasts.
    """
    k = re.sub(r"\d+\.\d+", "N", problem)
    k = re.sub(r"\b\d+\b", "N", k)
    return re.sub(r"\s+", " ", k).strip()


def decide_notification(
    problems: list[str],
    state: dict,
    now: datetime,
) -> "tuple[bool, str, dict]":
    """Should we push, what should the title say, and what state do we keep?

    Pure, so the policy is testable without a box or an ntfy topic.

    Pushes when a problem is NEW, when one has RESOLVED since the last push,
    or when REASSERT_HOURS have passed with problems still outstanding.
    """
    # A corrupt or hand-edited state file must never stop the watchdog from
    # watching. Anything unreadable reads as "nothing seen before", which
    # errs toward pushing — the safe direction.
    raw_seen = state.get("problems")
    seen: dict = raw_seen if isinstance(raw_seen, dict) else {}
    keys = {problem_key(p): p for p in problems}

    new_keys = [k for k in keys if k not in seen]
    gone_keys = [k for k in seen if k not in keys]

    last_push_raw = state.get("last_push")
    last_push = None
    if last_push_raw:
        try:
            last_push = datetime.fromisoformat(last_push_raw)
            if last_push.tzinfo is None:
                last_push = last_push.replace(tzinfo=timezone.utc)
        except ValueError:
            last_push = None

    due_reassert = bool(keys) and (
        last_push is None or (now - last_push) >= timedelta(hours=REASSERT_HOURS)
    )

    should = bool(new_keys) or bool(gone_keys) or due_reassert

    if new_keys and gone_keys:
        title = f"Studio watchdog: {len(new_keys)} new, {len(gone_keys)} resolved"
    elif new_keys:
        title = f"Studio watchdog: {len(new_keys)} NEW problem(s)"
    elif gone_keys and not keys:
        title = "Studio watchdog: all clear"
    elif gone_keys:
        title = f"Studio watchdog: {len(gone_keys)} resolved, {len(keys)} remain"
    elif due_reassert:
        oldest = min(seen.values()) if seen else None
        age = ""
        if oldest:
            try:
                t = datetime.fromisoformat(oldest)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                age = f", oldest {int((now - t).total_seconds() // 3600)}h"
            except ValueError:
                pass
        title = f"Studio watchdog: still {len(keys)} problem(s){age}"
    else:
        title = f"Studio watchdog: {len(keys)} problem(s)"

    next_state = {
        # Keep the first-seen time for a problem that persists, so the
        # re-assert can say how old it is.
        "problems": {k: seen.get(k, now.isoformat()) for k in keys},
        "last_push": now.isoformat() if should else state.get("last_push"),
    }
    return should, title, next_state


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:  # noqa: BLE001  — missing or corrupt reads as empty
        return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))
    except Exception as exc:  # noqa: BLE001
        print(f"  [watchdog state write failed: {exc}]", file=sys.stderr)


def _finish(problems: list[str], topic: str, dry_run: bool) -> None:
    now = datetime.now(timezone.utc)
    state = _load_state()
    should, title, next_state = decide_notification(problems, state, now)

    if problems:
        body = "\n".join(f"• {p}" for p in problems)
        print(f"=== {len(problems)} PROBLEM(S) ===\n{body}")
    else:
        print("=== all checks passed ===")
        body = "every check passed"

    if dry_run:
        print(f"(dry run — would {'PUSH: ' + title if should else 'stay quiet'})")
        return

    if should:
        notify(title, body, topic)
    else:
        print(f"  (unchanged since the last push — not re-pushing; "
              f"re-assert in {REASSERT_HOURS}h)")
    _save_state(next_state)


if __name__ == "__main__":
    sys.exit(main())
