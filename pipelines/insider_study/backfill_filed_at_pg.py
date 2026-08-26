#!/usr/bin/env python3
"""Fill trades.filed_at from EDGAR's acceptance timestamps.

The bulk SEC datasets carry FILING_DATE but no acceptance TIME, so every row
loaded by backfill_from_sec_datasets.py lands with filed_at NULL.

That is safe but pessimistic. framework.decision.entry_timing treats a missing
filed_at as "filed after the bell" — filed_before_close(None) is False — so
those rows fill at the NEXT session's open. It can never manufacture
look-ahead, but it does understate a filing that genuinely beat the bell, and
76% of filings arrive after the bell so the ones that didn't are exactly the
ones worth getting right.

data.sec.gov/submissions/CIK##########.json lists acceptanceDateTime per
accession for everything an entity filed. A Form 4's ISSUER is a filer on it,
so iterating issuers covers the whole set in far fewer requests than iterating
reporting owners.

REPLACES pipelines/insider_study/backfill_filed_at.py, which opens
`strategies/insider_catalog/insiders.db` with sqlite3 — the archived file, not
the live Postgres. It cannot have worked since the PG migration.

Usage:
    python3 pipelines/insider_study/backfill_filed_at_pg.py --since 2016-01-01
    python3 pipelines/insider_study/backfill_filed_at_pg.py --since 2016-01-01 --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

UA = "Form4 filed_at backfill (derek.gorthy@gmail.com)"
DELAY = 0.13          # SEC allows 10/s; stay under


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    finally:
        time.sleep(DELAY)


def acceptance_map(cik: str) -> dict:
    """accession -> 'YYYY-MM-DD HH:MM:SS' for everything this entity filed.

    The `recent` block holds ~1000 filings; anything older is paginated into
    files listed under filings.files, which must be followed or the oldest
    rows silently keep their NULL.
    """
    out = {}
    base = _get(f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json")
    if not base:
        return out

    def absorb(block):
        accs = block.get("accessionNumber") or []
        dts = block.get("acceptanceDateTime") or []
        for a, dt in zip(accs, dts):
            if a and dt:
                out[a] = dt.replace("T", " ").replace("Z", "")[:19]

    absorb(base.get("filings", {}).get("recent", {}))
    for extra in base.get("filings", {}).get("files", []) or []:
        name = extra.get("name")
        if not name:
            continue
        page = _get(f"https://data.sec.gov/submissions/{name}")
        if page:
            absorb(page)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01",
                    help="only fill rows with filing_date >= this")
    ap.add_argument("--limit", type=int, default=0, help="stop after N issuers")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    rows = conn.execute(
        """SELECT issuer_cik, COUNT(*) AS n FROM trades
            WHERE filed_at IS NULL AND issuer_cik IS NOT NULL
              AND filing_date >= ?
            GROUP BY issuer_cik ORDER BY COUNT(*) DESC""",
        (args.since,)).fetchall()
    if args.limit:
        rows = rows[:args.limit]
    total_rows = sum(int(r[1]) for r in rows)
    logger.info("%d issuer(s), %d row(s) missing filed_at since %s",
                len(rows), total_rows, args.since)

    filled = seen = 0
    for i, r in enumerate(rows, 1):
        cik = str(r[0]).strip()
        if not cik.isdigit():
            continue
        try:
            amap = acceptance_map(cik)
        except Exception as exc:
            logger.warning("CIK %s: %s", cik, exc)
            continue
        seen += 1
        if not amap or args.dry_run:
            continue
        # One UPDATE per accession is far fewer statements than one per row.
        accs = conn.execute(
            """SELECT DISTINCT accession FROM trades
                WHERE issuer_cik = ? AND filed_at IS NULL
                  AND filing_date >= ? AND accession IS NOT NULL""",
            (cik, args.since)).fetchall()
        for (acc,) in [(a[0],) for a in accs]:
            dt = amap.get(acc)
            if not dt:
                continue
            cur = conn.execute(
                "UPDATE trades SET filed_at = ? WHERE accession = ? AND filed_at IS NULL",
                (dt, acc))
            filled += getattr(cur, "rowcount", 0) or 0
        conn.commit()
        if i % 100 == 0:
            logger.info("  %d/%d issuers, %d rows filled", i, len(rows), filled)

    logger.info("Done: %d issuers queried, %d rows filled", seen, filled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
