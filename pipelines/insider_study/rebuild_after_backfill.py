#!/usr/bin/env python3
"""Recompute everything that was derived from an incomplete trades table.

The SEC bulk load roughly doubles `trades`. Every derived column, score,
grade, signal and simulated book downstream of it was computed over the old
half and is now stale. This runs them in dependency order, records which
steps finished, and can be re-run to resume.

ORDER IS NOT ARBITRARY. Each step reads what the ones above it wrote:

  cw_indicators      dips, SMA flags, is_largest_ever, consecutive_sells_before,
                     purchase_size_ratio  -- reads trades + prices only
  week52_proximity   needs prices; independent of the rest
  pit_clusters       pit_cluster_size -- reads trades only, but MUST run after
                     the load or clusters are counted against half a market
  switch_rate        insider_switch_rate -- reads trades only
  cohen_pit          cohen_routine -- needs 3 prior years PER INSIDER, so it
                     is the first step that genuinely benefits from the
                     pre-2016 history the load brought in
  returns            trade_returns -- needs prices, which start 2016-01-04, so
                     nothing before that gets a return and nothing before that
                     can be graded
  pit_grades         pit_grade -- needs returns
  career_grades      career_grade -- needs returns
  signals            trade_signals -- needs everything above
  books              the three published books -- needs everything above

compute_signals runs FULL, never --since: its detectors filter with NOT EXISTS
against existing tags, so an incremental run lets untagged new rows pass guards
they should fail. See the derived-tag-guards-fail-open note.

Usage:
    python3 pipelines/insider_study/rebuild_after_backfill.py
    python3 pipelines/insider_study/rebuild_after_backfill.py --only cohen_pit
    python3 pipelines/insider_study/rebuild_after_backfill.py --from returns
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
STUDY = REPO / "pipelines" / "insider_study"
sys.path.insert(0, str(REPO))
from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

PY = sys.executable

# EVERY wrapped script gets PYTHONPATH and PATH explicitly.
#
# compute_signals.py does `from config.database import get_connection` at
# import time and has no sys.path bootstrap of its own. Run from a plist it
# inherits PYTHONPATH from the plist; run from here with only cwd set, it dies
# in 0s on ModuleNotFoundError. That is exactly the failure the dataplane notes
# record against backfill_returns.py, and this driver walked into it on
# 2026-08-27: `signals` failed instantly and `books` then rebuilt the three
# published books against STALE trade_signals.
#
# cwd alone is not enough. CPython only puts the SCRIPT's directory on
# sys.path, not the working directory.
SCRIPT_ENV = {
    **os.environ,
    "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": str(REPO),
}
# Prices begin 2016-01-04; anything asking for a return or a dip is bounded
# by that, not by how far the filing history now goes back.
PRICE_START = "2016-01-01"

STEPS = [
    ("cw_indicators",    [PY, str(STUDY / "compute_cw_indicators.py")]),
    ("week52_proximity", [PY, str(STUDY / "compute_week52_proximity.py"),
                          "--since", PRICE_START, "--rebuild"]),
    ("pit_clusters",     [PY, str(STUDY / "compute_pit_clusters.py")]),
    ("switch_rate",      [PY, str(STUDY / "compute_switch_rate.py"),
                          "--since", "2006-01-01"]),
    ("cohen_pit",        [PY, str(STUDY / "compute_cohen_pit.py")]),
    ("returns",          [PY, str(STUDY / "backfill_returns.py"), "--skip-download"]),
    ("pit_grades",       [PY, str(STUDY / "backfill_pit_grades.py")]),
    ("career_grades",    [PY, str(STUDY / "compute_career_grades.py"),
                          "--since", PRICE_START, "--rebuild"]),
    ("signals",          [PY, str(STUDY / "compute_signals.py")]),
    ("books",            [PY, str(STUDY / "simulate_strategy_portfolio.py"),
                          "--all", "--rebuild"]),
]


def ensure_table(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS rebuild_progress (
        step TEXT PRIMARY KEY, status TEXT, seconds REAL,
        finished_at TEXT, detail TEXT)""")
    conn.commit()


def record(conn, step, status, seconds, detail=""):
    conn.execute(
        """INSERT INTO rebuild_progress (step, status, seconds, finished_at, detail)
           VALUES (?, ?, ?, datetime('now'), ?)
           ON CONFLICT (step) DO UPDATE SET
             status=excluded.status, seconds=excluded.seconds,
             finished_at=excluded.finished_at, detail=excluded.detail""",
        (step, status, seconds, detail[:500]))
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="run a single step by name")
    ap.add_argument("--from", dest="start_at", help="start at this step")
    ap.add_argument("--force", action="store_true",
                    help="re-run steps already marked done")
    args = ap.parse_args()

    conn = get_connection()
    ensure_table(conn)
    done = {r[0] for r in conn.execute(
        "SELECT step FROM rebuild_progress WHERE status = 'ok'").fetchall()}

    steps = STEPS
    if args.only:
        steps = [s for s in STEPS if s[0] == args.only]
        if not steps:
            logger.error("no step named %r; have %s",
                         args.only, ", ".join(s[0] for s in STEPS))
            return 2
    elif args.start_at:
        names = [s[0] for s in STEPS]
        if args.start_at not in names:
            logger.error("no step named %r", args.start_at)
            return 2
        steps = STEPS[names.index(args.start_at):]

    failed = []
    for name, cmd in steps:
        if name in done and not args.force and not args.only:
            logger.info("%-18s already done, skipping", name)
            continue
        logger.info("%-18s starting: %s", name, " ".join(cmd[1:]))
        t0 = time.monotonic()
        proc = subprocess.run(cmd, cwd=str(REPO), env=SCRIPT_ENV,
                              capture_output=True, text=True)
        secs = time.monotonic() - t0
        if proc.returncode == 0:
            logger.info("%-18s OK in %.0fs", name, secs)
            record(conn, name, "ok", secs, (proc.stdout or "")[-400:])
        else:
            tail = ((proc.stderr or "") + (proc.stdout or ""))[-600:]
            logger.error("%-18s FAILED rc=%d in %.0fs\n%s",
                         name, proc.returncode, secs, tail)
            record(conn, name, "failed", secs, tail)
            failed.append(name)
            # Keep going. A later step failing is informative, and stopping
            # dead means one broken legacy script blocks the whole rebuild.

    if failed:
        logger.error("FAILED steps: %s", ", ".join(failed))
        logger.error("re-run one with:  --only <step>")
        return 1
    logger.info("Rebuild complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
