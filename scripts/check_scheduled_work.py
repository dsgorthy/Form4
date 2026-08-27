#!/usr/bin/env python3
"""Does reality match dataplane/deploy/scheduled_work.yaml?

Dagster owns scheduling. This is the check that says so out loud, because on
2026-08-26 nobody could answer "what is still on cron?" without SSHing in and
reading plists out of four different directories — repo root, scripts/launchd/,
dataplane/deploy/ and assorted package dirs — and because the migration stalled
for nine days with nothing anywhere reflecting that.

Three failures, in order of how much they cost:

  DOUBLE-RUN   a unit marked `dagster` still has a loaded plist. The work runs
               twice. This is the one that corrupts data rather than merely
               annoying you, and it is exactly what the migration notes warn
               about: "disable the cron in the same change that adds the
               schedule."
  UNREGISTERED a loaded plist nobody declared. Someone added scheduled work
               outside Dagster and outside this file.
  UNJUSTIFIED  an exemption with no reason. "Not in Dagster" is a position that
               has to be argued, not asserted.

Run on Studio (reads launchctl), or anywhere with --plists to lint the file
alone.

    python3 scripts/check_scheduled_work.py
    python3 scripts/check_scheduled_work.py --registry-only
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / "dataplane" / "deploy" / "scheduled_work.yaml"

#: Services belonging to other products on the same box. Not our plane, not
#: our debt, and not evidence of drift.
FOREIGN_PREFIXES = ("com.derekg.",)


def load_registry() -> dict:
    data = yaml.safe_load(REGISTRY.read_text())
    for key in ("dagster", "pending", "exempt"):
        data.setdefault(key, []) or data.get(key)
    return data


def loaded_services() -> list[str]:
    """com.openclaw.* units launchd currently has loaded."""
    out = subprocess.run(["launchctl", "list"], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError("launchctl list failed — are you on Studio?")
    names = []
    for line in out.stdout.splitlines():
        parts = line.split("\t")
        label = parts[-1].strip() if parts else ""
        if label.startswith("com.openclaw.") and not label.startswith(FOREIGN_PREFIXES):
            names.append(label.replace("com.openclaw.", ""))
    return sorted(set(names))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry-only", action="store_true",
                    help="lint the file without consulting launchctl")
    args = ap.parse_args()

    reg = load_registry()
    dagster = {e["name"] for e in reg["dagster"]}
    pending = {e["name"] for e in reg["pending"]}
    exempt = {e["name"]: (e.get("reason") or "").strip() for e in reg["exempt"]}

    problems: list[str] = []

    unjustified = [n for n, r in exempt.items() if not r]
    for n in unjustified:
        problems.append(f"UNJUSTIFIED  {n} is exempt with no reason")

    overlap = (pending & set(exempt)) | (dagster & pending) | (dagster & set(exempt))
    for n in sorted(overlap):
        problems.append(f"AMBIGUOUS    {n} appears in more than one section")

    print(f"registry: {len(dagster)} on Dagster, {len(pending)} pending, "
          f"{len(exempt)} exempt")

    if not args.registry_only:
        try:
            live = loaded_services()
        except Exception as exc:
            print(f"\n(skipping launchctl checks: {exc})")
            live = None
        if live is not None:
            known = dagster | pending | set(exempt)
            for name in live:
                if name in dagster:
                    problems.append(
                        f"DOUBLE-RUN   {name} is marked `dagster` but its plist is "
                        f"LOADED — the work is running twice")
                elif name not in known:
                    problems.append(
                        f"UNREGISTERED {name} is loaded but declared nowhere. New "
                        f"scheduled work belongs in Dagster, and in this file.")
            still_on_cron = sorted(n for n in live if n in pending)
            print(f"launchd:  {len(live)} loaded, {len(still_on_cron)} of them "
                  f"pending migration")
            if still_on_cron:
                print("\nstill on cron, owed to Dagster:")
                for n in still_on_cron:
                    print(f"   - {n}")

    if problems:
        print("\n" + "\n".join(sorted(problems)))
        print(f"\nFAIL: {len(problems)} problem(s)")
        return 1
    print("\nOK: reality matches the registry")
    return 0


if __name__ == "__main__":
    sys.exit(main())
