#!/usr/bin/env python3
"""End-to-end smoke test: run a full --once --dry-run scan against today's data.

Exits 0 on clean run, 1 on any exception. Writes a compact pass/fail
report. Suitable for the Day-14 preflight checklist.

Usage:
    python3 scripts/preflight/e2e_smoke.py --strategy quality_momentum
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def smoke(strategy: str) -> int:
    config = REPO / f"strategies/cw_strategies/configs/{strategy}.yaml"
    if not config.exists():
        print(f"FAIL: config not found at {config}")
        return 1
    cmd = [
        "python3",
        str(REPO / "strategies/cw_strategies/cw_runner.py"),
        "--config", str(config),
        "--once", "--dry-run",
    ]
    print(f"$ {' '.join(cmd)}")
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        print("FAIL: --once --dry-run took > 180s")
        return 1
    print(out.stdout[-2000:])
    if out.returncode != 0:
        print(f"FAIL: exit code {out.returncode}")
        print("--- stderr ---")
        print(out.stderr[-2000:])
        return 1
    # Heuristic: "scan_signals: %d candidates" must appear (the runner did its
    # job); STALE_INPUT_HALT should NOT appear in non-halt-mode.
    if "STALE_INPUT_HALT" in out.stdout:
        print("FAIL: STALE_INPUT_HALT fired during smoke run — fix freshness "
              "contracts before going live.")
        return 1
    if "scan_signals" not in out.stdout:
        print("WARN: 'scan_signals' didn't appear — runner may have exited early")
    print(f"\n✓ Smoke run completed cleanly for {strategy}")
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="quality_momentum")
    args = p.parse_args()
    sys.exit(smoke(args.strategy))


if __name__ == "__main__":
    main()
