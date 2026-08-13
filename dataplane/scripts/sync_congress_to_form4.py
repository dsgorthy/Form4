#!/usr/bin/env python3
"""Sync congress.trades.raw.v1 observations into form4.congress_trades.

The dataplane is the system of record for this feed; form4.congress_trades
is a read model that the existing API (/congress, /congress/by-ticker) and
the company/screener pages already query. Syncing rather than repointing the
API keeps the product unchanged while the dataplane takes over ingestion —
this is the same "bridge" shape used for insider filings, just in the
opposite direction.

Idempotent: relies on the congress_trades_natural_key unique index
(migrations/2026-08-12_congress_trades_natural_key.sql) and ON CONFLICT DO
NOTHING, so overlapping re-runs are free.

Politicians are resolved by normalized name against form4.politicians and
created when missing, mirroring the legacy scraper's get_or_create.

Usage (on Studio):
    python sync_congress_to_form4.py                     # dry run, all
    python sync_congress_to_form4.py --since 2026-03-31 --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

_DATAPLANE = Path(__file__).resolve().parents[1]
_REPO = _DATAPLANE.parent
for p in (str(_DATAPLANE), str(_REPO)):
    if p not in sys.path:
        sys.path.insert(0, p)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("sync_congress")

SIGNAL_PREFIX = "congress.trades.raw"


def normalize_name(name: str) -> str:
    """Match the legacy scraper's normalization so we reuse its politicians."""
    return re.sub(r"[^a-z ]", "", (name or "").lower()).strip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", default=None,
                    help="Only sync disclosures filed on/after this date")
    ap.add_argument("--apply", action="store_true", help="Write (default: dry run)")
    ap.add_argument("--dataplane-dsn", default=os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"))
    ap.add_argument("--form4-dsn", default="dbname=form4 host=localhost")
    args = ap.parse_args()

    dp = psycopg2.connect(args.dataplane_dsn)
    cur = dp.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql = """
        SELECT ticker, as_of_date, value
          FROM signal_observations
         WHERE signal_id LIKE %s
    """
    params: list = [f"{SIGNAL_PREFIX}%"]
    if args.since:
        sql += " AND value->>'filing_date' >= %s"
        params.append(args.since)
    sql += " ORDER BY as_of_date"
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    dp.close()
    log.info("read %d observation(s) from dataplane", len(rows))
    if not rows:
        return 0

    f4 = psycopg2.connect(args.form4_dsn)
    fcur = f4.cursor()

    # Existing politicians, keyed by normalized name.
    fcur.execute("SELECT politician_id, name_normalized FROM politicians")
    politicians = {n: pid for pid, n in fcur.fetchall() if n}
    log.info("known politicians: %d", len(politicians))

    inserted = conflicted = skipped = created_pols = 0

    for r in rows:
        v = r["value"] or {}
        name = v.get("politician")
        if not name or not r["ticker"]:
            skipped += 1
            continue

        norm = normalize_name(name)
        pid = politicians.get(norm)
        if pid is None:
            if not args.apply:
                created_pols += 1
                politicians[norm] = -1        # placeholder for dry-run accounting
                pid = -1
            else:
                fcur.execute(
                    """INSERT INTO politicians (name, name_normalized, chamber, state, party)
                       VALUES (%s, %s, %s, %s, %s)
                       RETURNING politician_id""",
                    (name, norm, v.get("chamber"), v.get("state"), v.get("party")),
                )
                pid = fcur.fetchone()[0]
                politicians[norm] = pid
                created_pols += 1

        if not args.apply:
            inserted += 1
            continue

        fcur.execute(
            """INSERT INTO congress_trades
                   (politician_id, ticker, company, trade_type, trade_date,
                    filing_date, value_low, value_high, value_estimate,
                    owner, report_url, source)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                pid, r["ticker"], v.get("company"), v.get("trade_type"),
                v.get("trade_date"), v.get("filing_date"),
                v.get("value_low"), v.get("value_high"), v.get("value_estimate"),
                v.get("owner"), v.get("report_url"), "dataplane:congress.trades.raw.v1",
            ),
        )
        if fcur.rowcount:
            inserted += 1
        else:
            conflicted += 1

    if args.apply:
        f4.commit()
    fcur.close()
    f4.close()

    verb = "would insert" if not args.apply else "inserted"
    log.info("%s=%d already_present=%d skipped=%d new_politicians=%d",
             verb, inserted, conflicted, skipped, created_pols)
    if not args.apply:
        log.info("DRY RUN — re-run with --apply to write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
