#!/usr/bin/env python3
"""Backfill trade_decision_audit from historical cw_runner log files.

Each strategy's launchd log (`logs/{strategy}.log`) has one INFO line per
candidate evaluation with a PASS or SKIP outcome. This script parses those
lines and writes one audit row per occurrence with source='log_replay',
so the admin diagnostics view has historical coverage going back to
strategy start.

This is INTENTIONALLY lossy (only post-conviction PASS/SKIP — not per-stage
filter detail). For richer per-stage history, run the simulation replay
(Phase 2 work). Live decisions from today forward are richer (per-stage
filter outcomes via the new instrumentation in cw_runner.scan_signals).

Format examples (from real logs):
    2026-04-15 09:31:00,123 [INFO]   PASS PANW 2026-04-15 conv=6.0 >= 1.5 grade=A role=CEO Arora Nikesh
    2026-04-16 09:30:55,001 [INFO]   SKIP CRM 2026-04-15 conv=4.0 < 5.0 grade=B role=Dir Doe Jane

Idempotent: uses ON CONFLICT DO NOTHING on a synthetic (source, ts, strategy,
trade_id, ticker, filing_date) constraint we don't actually have, so we
de-dupe in-Python before insert.

Usage:
    python3 scripts/backfill_decision_audit_from_logs.py
    python3 scripts/backfill_decision_audit_from_logs.py --strategy quality_momentum
    python3 scripts/backfill_decision_audit_from_logs.py --since 2026-04-01
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Strategy log filename → strategy name in the audit table.
STRATEGIES = {
    "quality-momentum.log":  "quality_momentum",
    "reversal-dip.log":      "reversal_dip",
    "tenb51-surprise.log":   "tenb51_surprise",
}

# Pattern matches:
#   2026-04-15 09:31:00,123 [INFO]   PASS PANW 2026-04-15 conv=6.0 >= 1.5 grade=A role=CEO Arora Nikesh
#   2026-04-16 09:30:55,001 [INFO]   SKIP CRM 2026-04-15 conv=4.0 < 5.0 grade=B role=Dir Doe Jane
LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),\d{3}\s+\[INFO\]\s+"
    r"(?P<outcome>PASS|SKIP)\s+(?P<ticker>[A-Z][A-Z0-9._-]{0,9})\s+"
    r"(?P<filing_date>\d{4}-\d{2}-\d{2})\s+conv=(?P<conv>[0-9.-]+)\s+"
    r"(?P<op>[<>=]+)\s+(?P<thresh>[0-9.-]+)\s+grade=(?P<grade>[A-Z+]+)\s+"
    r"role=(?P<role>\S+)\s+(?P<insider>.+?)\s*$"
)


def parse_log(path: Path) -> Iterator[dict]:
    """Yield one parsed dict per matched PASS/SKIP line."""
    if not path.exists():
        logger.warning("log file missing: %s", path)
        return
    with path.open() as f:
        for line in f:
            m = LINE_RE.match(line)
            if not m:
                continue
            d = m.groupdict()
            try:
                ts = datetime.strptime(d["ts"], "%Y-%m-%d %H:%M:%S")
                ts = ts.replace(tzinfo=timezone.utc)  # log timestamps are UTC-ish; close enough for backfill
            except ValueError:
                continue
            yield {
                "ts": ts,
                "outcome": d["outcome"],
                "ticker": d["ticker"],
                "filing_date": d["filing_date"],
                "conv": float(d["conv"]),
                "thresh": float(d["thresh"]),
                "grade": d["grade"],
                "role": d["role"],
                "insider": d["insider"].strip(),
            }


def backfill(strategy: str, log_path: Path, since: str | None) -> int:
    """Insert audit rows for one strategy. Returns row count."""
    rows = list(parse_log(log_path))
    if since:
        cutoff = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        rows = [r for r in rows if r["ts"] >= cutoff]

    if not rows:
        logger.info("[%s] 0 PASS/SKIP entries to backfill", strategy)
        return 0

    # Group by ts so each scan_signals invocation gets one shared run_id.
    # The runner logs all candidates from one scan within a few ms of each
    # other, so grouping by ts-rounded-to-minute is a good proxy.
    by_minute: dict[str, list[dict]] = {}
    for r in rows:
        key = r["ts"].strftime("%Y-%m-%dT%H:%M")
        by_minute.setdefault(key, []).append(r)

    conn = get_connection()
    inserted = 0
    skipped_existing = 0

    # Pre-load existing log_replay rows so we can dedupe without unique-constraint
    # tricks. Cheap because scope is small.
    existing = set()
    cur = conn.execute(
        "SELECT ts, ticker, filing_date FROM trade_decision_audit "
        "WHERE strategy = ? AND source = 'log_replay'",
        (strategy,),
    )
    for row in cur.fetchall():
        ts_val = row["ts"] if hasattr(row, "__getitem__") else row[0]
        ticker_val = row["ticker"] if hasattr(row, "__getitem__") else row[1]
        fd_val = row["filing_date"] if hasattr(row, "__getitem__") else row[2]
        existing.add((str(ts_val)[:19], ticker_val, str(fd_val)[:10]))

    batch = []
    for minute_key, group in sorted(by_minute.items()):
        run_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                f"log_replay/{strategy}/{minute_key}"))
        for r in group:
            ts_str = r["ts"].strftime("%Y-%m-%d %H:%M:%S")
            dedup_key = (ts_str, r["ticker"], r["filing_date"])
            if dedup_key in existing:
                skipped_existing += 1
                continue
            existing.add(dedup_key)
            passed = (r["outcome"] == "PASS")
            reason = f"conv={r['conv']:.1f} {r['op']} {r['thresh']:.1f}"
            snapshot = {
                "grade": r["grade"], "role": r["role"], "insider": r["insider"],
                "log_replay": True,
            }
            batch.append((
                run_id, strategy, r["ticker"], None, r["filing_date"],
                strategy, "conviction", passed, reason,
                r["grade"], r["conv"], json.dumps(snapshot), "log_replay",
                r["ts"],
            ))

    if not batch:
        conn.close()
        logger.info("[%s] %d entries already backfilled (no-op)", strategy, skipped_existing)
        return 0

    try:
        conn.executemany(
            """INSERT INTO trade_decision_audit
               (run_id, strategy, ticker, trade_id, filing_date, thesis,
                stage, passed, reason, pit_grade, conviction, feature_snapshot,
                source, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)""",
            batch,
        )
        conn.commit()
        inserted = len(batch)
    except Exception as e:
        conn.rollback()
        logger.exception("[%s] insert failed: %s", strategy, e)
    finally:
        conn.close()

    logger.info("[%s] inserted %d log_replay rows (%d already present)",
                strategy, inserted, skipped_existing)
    return inserted


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=list(STRATEGIES.values()),
                   help="Only backfill this strategy (default: all 3)")
    p.add_argument("--since", help="Only entries with ts >= this YYYY-MM-DD")
    p.add_argument("--log-dir", default=str(REPO / "logs"),
                   help="Directory containing the strategy log files")
    args = p.parse_args()

    log_dir = Path(args.log_dir)
    total = 0
    for log_name, strategy in STRATEGIES.items():
        if args.strategy and strategy != args.strategy:
            continue
        path = log_dir / log_name
        n = backfill(strategy, path, args.since)
        total += n
    logger.info("Total log_replay rows inserted: %d", total)


if __name__ == "__main__":
    main()
