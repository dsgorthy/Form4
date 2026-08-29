#!/usr/bin/env python3
"""Short interest into short_metrics, from FINRA's public API.

SOURCE

    https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest

Tested live: HTTP 200 with NO authentication. FINRA Rule 4560 requires member
firms to report short positions twice a month, so this is bi-monthly, not daily,
with archives back to roughly 2014.

Fields used: symbolCode, settlementDate, currentShortPositionQuantity,
daysToCoverQuantity, averageDailyVolumeQuantity.

THE PIT RULE

Settlement dates are bi-monthly and the figure is published some days after
settlement. Readers must join `date <= filing_date` and forward-fill, never
interpolate forward from the next observation. short_metrics stores the
settlement date as `date`; a reader taking the NEXT settlement after a filing
is looking ahead.

WHY short_pct_float IS LEFT NULL HERE

It needs shares outstanding, which arrives separately from EDGAR XBRL
(backfill_shares_outstanding.py). Computing it from a stale or missing share
count would be worse than leaving it empty -- the column is read by nothing yet,
and a wrong number is harder to notice than a NULL.

Usage:
    python3 pipelines/insider_study/backfill_short_interest.py --dry-run
    python3 pipelines/insider_study/backfill_short_interest.py --since 2016-01-01
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

UA = "SidequestGroup Form4 Research derek@sidequestgroup.com"
BASE = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
PAGE = 5000
SLEEP = 0.25

# A short position larger than this, or a negative one, is a feed error.
MAX_SHORT = 50_000_000_000


def fetch_page(offset: int, limit: int, since: str | None) -> list[dict]:
    params = {"limit": limit, "offset": offset}
    if since:
        # FINRA's filter syntax; if it is rejected we fall back to client-side.
        params["compareFilters"] = json.dumps(
            [{"fieldName": "settlementDate", "fieldValue": since,
              "compareType": "GTE"}])
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept": "application/json",
                                               "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    d = json.loads(raw)
    return d if isinstance(d, list) else d.get("data", []) or []


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-pages", type=int, default=2000)
    args = ap.parse_args()

    conn = get_connection()

    # Only tickers we hold trades for. The feed covers the whole market and we
    # have no use for the rest.
    ours = {r["ticker"] for r in conn.execute("""
        SELECT DISTINCT ticker FROM trades
         WHERE ticker IS NOT NULL AND ticker <> 'NONE'
           AND signal_class IN ('discretionary_buy','discretionary_sell')
    """).fetchall()}
    # RELEASE THE READ LOCK BEFORE ANY NETWORK I/O.
    #
    # config/database.py opens connections in a transaction, so the seeding
    # SELECT above holds AccessShareLock on `trades` until something commits.
    # Without this the lock is held across hundreds of HTTP round trips --
    # minutes -- and any ALTER TABLE on `trades` queues behind it, which then
    # blocks every later read on the table because Postgres grants lock
    # requests in order. That is precisely the shape of the 2026-08-27 outage,
    # and it is what refused the derived-features migration today.
    conn.commit()
    logger.info("%d tickers of interest", len(ours))

    offset, kept, seen, pages = 0, 0, 0, 0
    server_filter = True
    t0 = time.time()
    while pages < args.max_pages:
        try:
            rows = fetch_page(offset, PAGE,
                              args.since if server_filter else None)
        except urllib.error.HTTPError as e:
            # The server ACCEPTS compareFilters and then ignores it -- verified
            # live: a GTE 2024-01-01 filter returns rows from 2020-04-15,
            # byte-identical to unfiltered. So this branch is unreachable in
            # practice and `--since` only ever filters client-side.
            #
            # It is kept for the day FINRA starts validating, but the original
            # `continue` skipped both `pages += 1` and the sleep while leaving
            # `pages == 0` true: a tight, un-backed-off retry loop hammering
            # FINRA forever. One attempt only, and it sleeps.
            if pages == 0 and server_filter and e.code in (400, 403):
                logger.warning("server rejected the date filter (HTTP %s); "
                               "falling back to client-side filtering", e.code)
                server_filter = False
                time.sleep(SLEEP)
                continue
            raise
        if not rows:
            break
        pages += 1
        seen += len(rows)

        for r in rows:
            sym = (r.get("symbolCode") or "").strip().upper()
            if not sym or sym not in ours:
                continue
            settle = (r.get("settlementDate") or "")[:10]
            si = _int(r.get("currentShortPositionQuantity"))
            if not settle or si is None or si < 0 or si > MAX_SHORT:
                continue
            if args.since and settle < args.since:
                continue
            dtc = r.get("daysToCoverQuantity")
            adv = _int(r.get("averageDailyVolumeQuantity"))
            kept += 1
            if args.dry_run:
                continue
            conn.execute("""
                INSERT INTO short_metrics
                    (ticker, date, short_interest, days_to_cover,
                     avg_daily_volume, source)
                VALUES (?, ?, ?, ?, ?, 'finra')
                ON CONFLICT (ticker, date) DO UPDATE
                   SET short_interest   = EXCLUDED.short_interest,
                       days_to_cover    = EXCLUDED.days_to_cover,
                       avg_daily_volume = EXCLUDED.avg_daily_volume
            """, (sym, settle, si,
                  float(dtc) if dtc not in (None, "") else None, adv))

        if not args.dry_run and pages % 5 == 0:
            conn.commit()
        logger.info("  page %d  offset=%d  seen=%d  kept=%d  (%.0fs)",
                    pages, offset, seen, kept, time.time() - t0)
        if len(rows) < PAGE:
            break
        offset += PAGE
        time.sleep(SLEEP)

    if not args.dry_run:
        conn.commit()
    if pages >= args.max_pages:
        logger.warning("STOPPED AT THE PAGE CAP (%d pages x %d). The feed is "
                       "larger than this run covered, so coverage is TRUNCATED, "
                       "not complete. Re-run with a higher --max-pages.",
                       args.max_pages, PAGE)
    logger.info("Done in %.0fs: %d feed rows scanned, %d kept for our tickers",
                time.time() - t0, seen, kept)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
