#!/usr/bin/env python3
"""Rewrite a launchd agent from StartInterval to StartCalendarInterval.

WHY THIS EXISTS. On 2026-09-08 between 00:57 and 01:20, six com.openclaw
services stopped firing within 23 minutes of each other and stayed stopped for
57 hours. Every one reported `last exit code = 0` and wrote no log line,
because a job that never runs writes nothing.

A census of every loaded agent that day makes the cause exact:

    StartInterval          6 agents    6 dead
    StartCalendarInterval  ) 14 agents  0 dead
    KeepAlive              )

macOS coalesces and defers interval timers for power management and, on a
long-uptime machine (29 days at the time), can defer them indefinitely. A
calendar schedule is a wall-clock trigger and is not subject to the same
coalescing. insider-fetch was fixed this way on 2026-09-08 and has fired every
five minutes since.

An interval of N seconds becomes the equivalent set of minute-of-hour entries,
so behaviour is preserved rather than reinterpreted: 900s -> :00 :15 :30 :45.
Only divisors of 3600 can be expressed this way, which is every interval in use
here. Anything else should move to Dagster instead -- see
feedback_dagster_owns_scheduling; this script exists for the watchdogs that are
deliberately exempt from Dagster, because a watchdog inside the thing it
watches goes quiet exactly when it is needed.

Usage:
    launchd_interval_to_calendar.py <plist> [<plist> ...]   # rewrite in place
    launchd_interval_to_calendar.py --check <plist> ...     # report only
"""
import argparse
import plistlib
import sys
from pathlib import Path

HOUR = 3600


def minutes_for(interval: int) -> list[int]:
    """The minute-of-hour entries equivalent to a StartInterval of `interval`."""
    if interval <= 0 or HOUR % interval:
        raise ValueError(
            f"StartInterval={interval} does not divide {HOUR}; it cannot be "
            "expressed as minute-of-hour entries. Move this job to Dagster."
        )
    return list(range(0, 60, interval // 60)) if interval >= 60 else list(range(60))


def convert(path: Path, check_only: bool) -> int:
    with path.open("rb") as fh:
        data = plistlib.load(fh)

    label = data.get("Label", path.stem)

    if "StartCalendarInterval" in data and "StartInterval" not in data:
        print(f"  ok      {label}: already on a calendar schedule")
        return 0
    if "StartInterval" not in data:
        print(f"  skip    {label}: no StartInterval (KeepAlive or manual)")
        return 0

    interval = int(data["StartInterval"])
    mins = minutes_for(interval)

    if check_only:
        print(f"  WOULD   {label}: StartInterval={interval} -> "
              f"{len(mins)} calendar entries")
        return 1

    del data["StartInterval"]
    data["StartCalendarInterval"] = [{"Minute": m} for m in mins]

    # Write via a temp file in the same directory, then replace. A partial
    # write here leaves launchd with an unparseable plist for a job whose
    # entire purpose is noticing that something else broke.
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as fh:
        plistlib.dump(data, fh)
    tmp.replace(path)
    print(f"  rewrote {label}: StartInterval={interval} -> "
          f"{len(mins)} calendar entries (every {interval // 60 or 1}m)")
    return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("plists", nargs="+", type=Path)
    ap.add_argument("--check", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args()

    changed = 0
    for p in args.plists:
        if not p.exists():
            print(f"  MISSING {p}", file=sys.stderr)
            return 2
        changed += convert(p, args.check)
    print(f"\n{changed} plist(s) {'would be ' if args.check else ''}changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
