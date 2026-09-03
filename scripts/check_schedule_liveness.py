#!/usr/bin/env python3
"""Has every scheduled unit actually RUN inside its own cadence?

WHY THIS EXISTS

On 2026-08-26 the three strategy runners were unloaded for the SEC reload and
nobody noticed until 2026-09-02 -- SEVEN DAYS, spanning five trading days of
missed alerts. Nothing was broken and nothing alarmed. The site's own
runner-status endpoint reported:

    "status": "sleeping", "detail": "After hours", "age_seconds": 624717

A daemon that is GONE looks exactly like a daemon that is IDLE. The heartbeat
aged past a week and the only thing that caught it was Derek looking at the
page.

`freshness_contracts.yaml` answers "is this COLUMN stale". This answers a
different question: "did this JOB run". A job can be absent for a week while
every column it does not write stays perfectly fresh.

THE BUDGET COMES FROM THE CRON, NEVER A FLAT DURATION

A weekday 06:30 job is not late at 09:00 on a Monday just because 50 hours have
passed -- it is not due until Monday morning. Judging every unit against one
duration pages every Monday for jobs that are working, which is how a monitor
teaches people to ignore it. So the allowance is derived from each unit's own
schedule: expected interval, plus a grace multiple.

Usage:
    python3 scripts/check_schedule_liveness.py
    python3 scripts/check_schedule_liveness.py --notify   # ntfy on failure
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

REGISTRY = REPO / "dataplane/deploy/scheduled_work.yaml"

#: Registry name -> the substring that appears in Dagster's pipeline_name.
#:
#: The registry is named after the launchd unit each entry replaced, which is
#: the right name for a migration ledger and the wrong one for matching runs:
#: `quality-notrend` became the Dagster job `job_ops_runner_notrend`. Without
#: this the three newly-migrated runners reported NEVER RUN while working.
ALIASES = {
    "quality-notrend":  "runner_notrend",
    "quality-momentum": "runner_momentum",
    "reversal-dip":     "runner_dip",
}

#: Entries that describe a GROUP of schedules rather than one job, so no single
#: cadence applies. form4_ops is "10 schedules, 12 assets" -- its members are
#: each checked by their own Dagster schedule, and asking when "form4_ops" last
#: ran is not a well-formed question.
CONTAINERS = {"form4_ops"}

#: How many expected intervals may pass before a unit is called stale. Two
#: means one missed firing is tolerated and two is not -- transient failures
#: retry, systematic ones do not.
GRACE = 2.5

#: Floor and ceiling on the allowance. A */5 job should not page for a
#: 12-minute blip; a monthly one should not go unchecked for months.
#:
#: THE CEILING MUST NEVER FALL BELOW THE CADENCE ITSELF. A flat 3-day cap
#: applied to `0 9 * * 0` guaranteed a false alarm: a Sunday job is
#: legitimately 96 hours old on a Wednesday, and capping its allowance at 72
#: made form4_weekly page for working exactly as designed. The cap is now a
#: floor of its own -- see allowance_for().
MIN_ALLOWANCE = timedelta(minutes=30)
MAX_ALLOWANCE = timedelta(days=3)


def allowance_for(interval: timedelta) -> timedelta:
    """How stale a unit may get before it counts as an outage.

    GRACE multiples of its own cadence, bounded -- but the upper bound can
    never squeeze the allowance below the cadence, or a slow job is stale the
    moment it finishes.
    """
    ceiling = max(MAX_ALLOWANCE, interval * 1.25)
    return max(MIN_ALLOWANCE, min(ceiling, interval * GRACE))


def expected_interval(cron: str) -> timedelta | None:
    """Rough firing interval from a cron string.

    Deliberately approximate. The question is "roughly how often should this
    appear", not "exactly when next" -- and being approximate keeps this
    readable, which matters more for a monitor than precision does.
    """
    cron = (cron or "").strip()
    if not cron or cron.startswith("long-lived"):
        return None
    parts = cron.split()
    if len(parts) < 5:
        # bare "06:30" style entries in the registry
        if ":" in cron:
            return timedelta(days=1)
        return None
    minute, hour, dom, month, dow = parts[:5]

    if minute.startswith("*/"):
        try:
            base = timedelta(minutes=int(minute[2:]))
        except ValueError:
            return None
        # An hour-restricted schedule (6-13) is idle overnight, so the gap
        # across the closed window is what actually matters.
        if hour != "*":
            return timedelta(hours=18)
        return base
    if hour.startswith("*/"):
        try:
            return timedelta(hours=int(hour[2:]))
        except ValueError:
            return None
    # A specific hour: daily, unless the day fields narrow it.
    #
    # A SINGLE day-of-week IS NOT THE SAME AS A WEEKDAY RANGE, and treating
    # them alike is a false alarm generator. `0 9 * * 0` fires on Sundays --
    # 168 hours apart -- so on a Wednesday its last run is legitimately ~96h
    # old. The first version returned 3 days for anything with a dow field and
    # duly paged for form4_weekly working exactly as designed.
    if dow not in ("*", "?"):
        n_days = len({d for part in dow.split(",") for d in _expand_dow(part)})
        if n_days <= 1:
            return timedelta(days=7)      # weekly
        if n_days <= 3:
            return timedelta(days=4)      # a few days a week
        return timedelta(days=3)          # weekday range: the weekend gap
    if dom not in ("*", "?"):
        return timedelta(days=31)
    return timedelta(days=1)


def _expand_dow(part: str) -> set[int]:
    """Days named by one cron day-of-week token ('0', '1-5', '*/2')."""
    part = part.strip()
    if part in ("*", "?", ""):
        return set(range(7))
    if "/" in part:
        base, _, step = part.partition("/")
        try:
            st = int(step)
        except ValueError:
            return set(range(7))
        rng = _expand_dow(base) if base not in ("*", "") else set(range(7))
        return {d for d in rng if d % st == 0}
    if "-" in part:
        a, _, b = part.partition("-")
        try:
            return set(range(int(a), int(b) + 1))
        except ValueError:
            return set(range(7))
    try:
        return {int(part)}
    except ValueError:
        return set(range(7))


def load_units() -> list[dict]:
    d = yaml.safe_load(REGISTRY.read_text()) or {}
    out = []
    for status, items in d.items():
        if status != "dagster" or not isinstance(items, list):
            continue
        for it in items:
            if isinstance(it, dict) and it.get("name"):
                out.append({"name": it["name"],
                            "schedule": it.get("schedule", ""),
                            "match": ALIASES.get(it["name"], it["name"])})
    return out


def last_runs(units: list[dict]) -> dict[str, datetime]:
    """Most recent Dagster run per unit, matched loosely on pipeline name.

    CONNECTS TO dagster_runs EXPLICITLY, and raises rather than degrading.

    The first version used config.database (which points at `form4`) and hoped
    an env var would redirect it, then swallowed the resulting error with
    `except Exception: return {}`. The check duly reported all ten units as
    "NEVER RUN" -- a false alarm on everything, including jobs I had watched
    succeed minutes earlier.

    A monitor that reports an outage when it cannot reach its own data source
    is worse than no monitor: the first response to ten simultaneous alarms is
    to disbelieve the monitor, which is the correct response and also the end
    of its usefulness. It now fails loudly instead.
    """
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(host="/tmp", dbname="dagster_runs")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT pipeline_name, MAX(start_time) AS last_start
              FROM runs WHERE start_time IS NOT NULL
             GROUP BY pipeline_name
        """)
        rows = cur.fetchall()
    finally:
        conn.close()
    seen: dict[str, datetime] = {}
    for r in rows:
        pn = (r["pipeline_name"] or "").lower()
        ts = datetime.fromtimestamp(float(r["last_start"]), tz=timezone.utc)
        for u in units:
            key = u["match"].replace("-", "_").lower()
            if key in pn or pn.endswith(key):
                n = u["name"]
                if n not in seen or ts > seen[n]:
                    seen[n] = ts
    return seen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--notify", action="store_true")
    args = ap.parse_args()

    units = load_units()
    # Deliberately unguarded. If the run history is unreachable this must
    # crash, not report every unit as dead.
    seen = last_runs(units)

    now = datetime.now(timezone.utc)
    stale, unknown, ok = [], [], []
    for u in units:
        if u["name"] in CONTAINERS:
            unknown.append((u["name"], u["schedule"],
                            "container entry: members checked individually"))
            continue
        iv = expected_interval(u["schedule"])
        if iv is None:
            unknown.append((u["name"], u["schedule"], "no parseable cadence"))
            continue
        allowance = allowance_for(iv)
        last = seen.get(u["name"])
        if last is None:
            stale.append((u["name"], u["schedule"], "NEVER RUN", allowance))
            continue
        age = now - last
        if age > allowance:
            stale.append((u["name"], u["schedule"],
                          f"last run {age.total_seconds()/3600:.1f}h ago",
                          allowance))
        else:
            ok.append((u["name"], age, allowance))

    print(f"{len(units)} scheduled unit(s) on Dagster: "
          f"{len(ok)} fresh, {len(stale)} STALE, {len(unknown)} unknown cadence\n")
    for n, age, al in sorted(ok, key=lambda x: -x[1].total_seconds()):
        print(f"  ok    {n:<34} {age.total_seconds()/3600:6.1f}h ago "
              f"(allowance {al.total_seconds()/3600:.1f}h)")
    for n, sched, why, al in stale:
        print(f"  STALE {n:<34} {why}  "
              f"(cron {sched!r}, allowance {al.total_seconds()/3600:.1f}h)")
    for n, sched, why in unknown:
        print(f"  ?     {n:<34} {why} (cron {sched!r})")

    if stale and args.notify:
        try:
            from framework.alerts.ntfy import send_ntfy
            send_ntfy(
                title=f"{len(stale)} scheduled job(s) not running",
                message="\n".join(f"{n}: {why}" for n, _, why, _ in stale),
                priority="high", tags="warning",
            )
        except Exception as exc:      # never let alerting fail the check
            print(f"\n(ntfy failed: {exc})")

    if stale:
        print("\nA job absent for longer than its own cadence is an outage, "
              "whatever its heartbeat says.")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
