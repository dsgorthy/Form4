#!/usr/bin/env python3
"""Keep Silver current with Bronze, and Gold current with Silver.

Three steps, each already its own script and each self-healing by
construction (their work lists are "rows the previous layer has that this
one lacks"), run in order:

    1. build.py     parse every Bronze submission with no Silver receipt
    2. assess.py    judge the price on every new line (a bounded number of batches)
    3. backfill_10b51.py --pending   flag 10b5-1 on every line with no flag yet

Hourly under Dagster (ops_silver_keep_up, :40, after the :20 Bronze top-up).
`--refresh-gold` instead rebuilds gold.form4_line (REFRESH MATERIALIZED
VIEW, ~2.5 min) -- nightly, since the parity report and any future reader
of Gold see a snapshot.

Until 2026-09-18 none of this ran on a schedule: Silver stopped at what
Bronze held on 09-14, which the Gold parity report showed as a 2026 tail
1,600 filings short.
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STEPS = [
    ("build", ["pipelines/silver/build.py"]),
    ("assess", ["pipelines/silver/assess.py", "--max-batches", "3"]),
    ("10b5-1", ["pipelines/silver/backfill_10b51.py", "--pending"]),
]


def run_steps() -> int:
    for name, args in STEPS:
        cmd = [sys.executable, str(ROOT / args[0]), *args[1:]]
        logger.info("%s: %s", name, " ".join(cmd))
        rc = subprocess.call(cmd, cwd=str(ROOT))
        if rc != 0:
            logger.error("%s exited %d; stopping (the later steps read this one's output)", name, rc)
            return rc
    return 0


def refresh_gold() -> int:
    from config.database import get_connection
    conn = get_connection()
    conn.execute("SET statement_timeout = '3600s'")
    conn.execute("REFRESH MATERIALIZED VIEW gold.form4_line")
    conn.commit()
    n = conn.execute("SELECT count(*) AS n FROM gold.form4_line").fetchone()["n"]
    logger.info("gold.form4_line refreshed: %d lines", n)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh-gold", action="store_true", help="rebuild gold.form4_line instead of the three steps")
    args = ap.parse_args()
    return refresh_gold() if args.refresh_gold else run_steps()


if __name__ == "__main__":
    sys.exit(main())
