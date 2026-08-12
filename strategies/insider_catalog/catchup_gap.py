#!/usr/bin/env python3
"""Chunked EDGAR catch-up for the 2026-07-24 -> 2026-08-11 outage gap.

EFTS 500s on wide date ranges (result cap), so this walks the gap in
windows the size production already handles (fetch_latest --days 2), reusing
_run_fetch_inner so the ingest path is identical to the live 5-minute job.

_run_indicators() is deliberately called ONCE at the end rather than per
chunk -- it shells out to the indicator/PIT subprocesses and is expensive.

Usage (on Studio):
    python3.12 strategies/insider_catalog/catchup_gap.py \
        [--start YYYY-MM-DD] [--end YYYY-MM-DD] \
        [--step-days N] [--dry-run] [--skip-indicators]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "strategies" / "insider_catalog"))

from strategies.insider_catalog.fetch_latest import (  # noqa: E402
    _run_fetch_inner,
    _run_indicators,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("catchup")


def daterange_chunks(start: date, end: date, step: int):
    """Yield (chunk_start, chunk_end) ISO pairs covering [start, end]."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=step), end)
        yield cur.isoformat(), chunk_end.isoformat()
        if chunk_end == end:
            break
        # +1 day so windows don't overlap; dedup is by accession anyway.
        cur = chunk_end + timedelta(days=1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default="2026-07-23")
    p.add_argument("--end", default=date.today().isoformat())
    p.add_argument("--step-days", type=int, default=2,
                   help="Window width in days (default 2, matching prod --days 2)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-indicators", action="store_true",
                   help="Do not run the indicator/PIT chain at the end")
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    chunks = list(daterange_chunks(start, end, args.step_days))

    log.info("=" * 68)
    log.info("CATCH-UP %s -> %s in %d chunk(s) of %dd (dry_run=%s)",
             args.start, args.end, len(chunks), args.step_days, args.dry_run)
    log.info("=" * 68)

    totals = {"new": 0, "inserted": 0, "failed_chunks": 0}
    t0 = time.monotonic()

    for i, (cs, ce) in enumerate(chunks, 1):
        log.info("--- chunk %d/%d: %s .. %s ---", i, len(chunks), cs, ce)
        try:
            stats = _run_fetch_inner(cs, ce, args.dry_run)
        except Exception as exc:  # keep going; one bad window shouldn't kill the run
            totals["failed_chunks"] += 1
            log.error("chunk %s..%s FAILED: %s", cs, ce, exc)
            continue

        new = stats.get("new", 0)
        ins = stats.get("inserted", 0)
        totals["new"] += new
        totals["inserted"] += ins
        log.info("chunk %d/%d done: new=%d inserted=%d (running total inserted=%d)",
                 i, len(chunks), new, ins, totals["inserted"])

    elapsed = time.monotonic() - t0
    log.info("=" * 68)
    log.info("FETCH COMPLETE in %.1fs: new=%d inserted=%d failed_chunks=%d",
             elapsed, totals["new"], totals["inserted"], totals["failed_chunks"])

    if totals["inserted"] > 0 and not args.dry_run and not args.skip_indicators:
        log.info("Running indicator/PIT chain once for the whole catch-up...")
        try:
            _run_indicators()
            log.info("Indicator/PIT chain OK")
        except Exception as exc:
            log.error("Indicator/PIT chain FAILED: %s", exc)
            return 1
    else:
        log.info("Skipping indicator/PIT chain (inserted=%d skip=%s dry_run=%s)",
                 totals["inserted"], args.skip_indicators, args.dry_run)

    log.info("DONE")
    return 0 if totals["failed_chunks"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
