#!/usr/bin/env python3
"""Rewrite launchd plists to wrap each command in framework.observability.wrap.

After this script, every targeted service records a row in pipeline_runs on
every invocation — start, end, exit code, duration. Rich metadata (rows_written,
per-service detail) requires per-script instrumentation; this wrapper gives
the minimum viable observability for ALL services in one shot.

Safety:
- Backs up each plist to <path>.bak.<unix_ts> before editing.
- Validates the new plist parses before writing.
- Reloads via launchctl bootout + bootstrap; verifies presence in launchctl list.
- If anything fails for a service, restores from backup and skips reload.

Usage:
    python3 scripts/instrument_plists.py            # process all targets
    python3 scripts/instrument_plists.py SERVICE…   # only the given services

Idempotent: re-running detects already-wrapped plists and skips them.
"""
from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
import time
from pathlib import Path

LAUNCH_DIR = Path.home() / "Library" / "LaunchAgents"
WRAPPER_MODULE = "framework.observability.wrap"
WRAPPER_PYTHON = "/opt/homebrew/bin/python3"
REPO = "/Users/derekg/trading-framework"

# Services to wrap. Excludes:
#   - already-instrumented (strategy-simulator, drift-detector, insider-fetch,
#     daily-prices, backfill-returns) — those have rich Python pipeline_run().
#   - KeepAlive daemons (alpaca-stream-listener, cw_runner instances, tunnels)
#     — pipeline_run as outer-wrap doesn't fit; would never end.
#   - pm-* (separate codebase / project)
#   - one-shots (paper-reset-phase2 in /tmp)
TARGETS = [
    "alpaca-intraday-resolver",
    "alpaca-reconcile",
    "breaking-signal",
    "candidate-count-probe",
    "ceowatcher-reader",
    "compute-signals",
    "daily-content",
    "daily-summary",
    "form4-seed-positions",
    "form4-uptime",
    "freshness-probe",
    "heartbeat-probe",
    "monday-paper-monitor",
    "pit-shadow",
    "post-deploy-audit",
    "refresh-features",
    "strategy-health",
    "strategy-intraday",
    "thesis-monitor",
    "trial-emails",
]


def plist_path(service: str) -> Path:
    return LAUNCH_DIR / f"com.openclaw.{service}.plist"


def is_already_wrapped(plist: dict) -> bool:
    args = plist.get("ProgramArguments", [])
    return WRAPPER_MODULE in args


def wrap_args(orig: list[str], service: str) -> list[str]:
    """Build the new ProgramArguments list."""
    return [WRAPPER_PYTHON, "-m", WRAPPER_MODULE, service, "--"] + orig


def ensure_pythonpath(plist: dict) -> None:
    env = plist.get("EnvironmentVariables", {})
    env["PYTHONPATH"] = REPO
    # Make sure PATH includes homebrew so /opt/homebrew/bin/python3 works
    if "PATH" not in env:
        env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
    plist["EnvironmentVariables"] = env


def reload_service(service: str) -> tuple[bool, str]:
    """bootout + bootstrap the service. Returns (ok, message)."""
    label = f"com.openclaw.{service}"
    uid = os.getuid()
    domain = f"gui/{uid}"
    plist = str(plist_path(service))
    # bootout — ignore exit code; service may already be unloaded
    subprocess.run(
        ["launchctl", "bootout", f"{domain}/{label}"],
        capture_output=True, text=True,
    )
    res = subprocess.run(
        ["launchctl", "bootstrap", domain, plist],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        return False, f"bootstrap failed: {res.stderr.strip() or res.stdout.strip()}"
    # Verify
    res2 = subprocess.run(
        ["launchctl", "list", label],
        capture_output=True, text=True,
    )
    if res2.returncode != 0:
        return False, "service not in launchctl list after bootstrap"
    return True, "loaded"


def _load_plist(path: Path) -> tuple[dict, bool]:
    """Load a plist. Returns (data, lost_comments).

    Tries plistlib first (preserves the file structure / comments on rewrite).
    Falls back to `plutil -convert xml1` which is more lenient about
    XML-illegal sequences like `--` inside comments — but strips comments.
    """
    try:
        with path.open("rb") as f:
            return plistlib.load(f), False
    except Exception:
        # plutil tolerates `--` in comments by stripping them on canonicalization
        res = subprocess.run(
            ["plutil", "-convert", "xml1", "-o", "-", str(path)],
            capture_output=True,
        )
        if res.returncode != 0:
            raise RuntimeError(
                f"plutil also failed: {res.stderr.decode(errors='replace')}"
            )
        return plistlib.loads(res.stdout), True


def process_one(service: str) -> tuple[str, str]:
    """Returns (status, detail). status ∈ {wrapped, skipped, error, restored}."""
    path = plist_path(service)
    if not path.exists():
        return "skipped", "plist not found"

    try:
        plist, lost_comments = _load_plist(path)
    except Exception as exc:
        return "error", f"parse failed: {exc}"

    if is_already_wrapped(plist):
        return "skipped", "already wrapped"

    if "ProgramArguments" not in plist or not plist["ProgramArguments"]:
        return "skipped", "no ProgramArguments"

    orig_args = list(plist["ProgramArguments"])
    backup = path.with_suffix(f".plist.bak.{int(time.time())}")
    shutil.copy2(path, backup)

    plist["ProgramArguments"] = wrap_args(orig_args, service.replace("-", "_"))
    ensure_pythonpath(plist)

    # Validate by round-tripping in memory
    try:
        buf = plistlib.dumps(plist)
        plistlib.loads(buf)
    except Exception as exc:
        return "error", f"new plist invalid: {exc} (no changes written)"

    with path.open("wb") as f:
        f.write(buf)

    ok, msg = reload_service(service)
    if not ok:
        # Restore
        shutil.copy2(backup, path)
        reload_service(service)
        return "restored", f"reload failed → restored backup: {msg}"

    note = f"backup={backup.name}"
    if lost_comments:
        note += " (comments stripped by plutil canonicalization)"
    return "wrapped", note


def main():
    targets = sys.argv[1:] or TARGETS
    print(f"Processing {len(targets)} services...\n")
    summary = {"wrapped": 0, "skipped": 0, "error": 0, "restored": 0}
    for s in targets:
        status, detail = process_one(s)
        summary[status] = summary.get(status, 0) + 1
        marker = {
            "wrapped":  "OK ",
            "skipped":  ".. ",
            "error":    "!! ",
            "restored": "!! ",
        }.get(status, "?? ")
        print(f"  {marker} {s:30s} {status:10s} {detail}")
    print(f"\nSummary: {summary}")


if __name__ == "__main__":
    main()
