#!/usr/bin/env python3
"""Earnings announcement dates per issuer, from EDGAR submissions.

SOURCE

    https://data.sec.gov/submissions/CIK{cik:010d}.json

An 8-K carrying Item 2.02 -- "Results of Operations and Financial Condition" --
IS the earnings release. Verified on AAPL: 46 of them across the recent 1,001
filings, one per quarter, with acceptanceDateTime to the second. Free, keyless,
the same host as the SIC and shares pulls.

Full history lives in two places: `filings.recent` (the last ~1,000) and the
paginated files listed under `filings.files`. Both are read; taking only
`recent` silently truncates older issuers, which is the same shape of defect as
the EFTS 10,000 cap that cost this project half its filings.

10-Q and 10-K dates are recorded too, under their own `source`, as a fallback
for issuers that never file a 2.02. They are the periodic REPORT rather than
the announcement, so they are not mixed with 8-K dates silently.

Usage:
    python3 pipelines/insider_study/backfill_earnings_dates.py --dry-run --limit 5
    python3 pipelines/insider_study/backfill_earnings_dates.py
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

UA = "SidequestGroup Form4 Research derek@sidequestgroup.com"
SLEEP = 0.11                      # ~9 req/s, inside SEC's 10/s ceiling
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
PAGE = "https://data.sec.gov/submissions/{name}"

EARNINGS_ITEM = "2.02"
PERIODIC = ("10-Q", "10-K")


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept-Encoding": "gzip, deflate"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    return json.loads(raw)


def _rows_from(block: dict) -> list[dict]:
    """Pull earnings-bearing filings out of one submissions block."""
    forms = block.get("form") or []
    n = len(forms)
    if not n:
        return []
    dates = block.get("filingDate") or [""] * n
    accept = block.get("acceptanceDateTime") or [""] * n
    items = block.get("items") or [""] * n
    accn = block.get("accessionNumber") or [""] * n

    out = []
    for i in range(n):
        form = forms[i]
        itm = items[i] or ""
        if form == "8-K" and EARNINGS_ITEM in itm:
            src = "edgar_8k_202"
        elif form in PERIODIC:
            src = "edgar_periodic"
        else:
            continue
        if not dates[i]:
            continue
        out.append({"date": dates[i], "accept": accept[i] or None,
                    "form": form, "items": itm or None,
                    "accn": accn[i] or None, "source": src})
    return out


def fetch(cik: int) -> list[dict] | None:
    try:
        d = _get(SUBMISSIONS.format(cik=cik))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    rows = _rows_from(d.get("filings", {}).get("recent", {}))
    # Older filings are paginated into separate files. Skipping them truncates
    # long-lived issuers exactly where the history is most useful.
    for f in d.get("filings", {}).get("files", []) or []:
        name = f.get("name")
        if not name:
            continue
        time.sleep(SLEEP)
        try:
            rows.extend(_rows_from(_get(PAGE.format(name=name))))
        except Exception as exc:
            logger.debug("cik %s page %s: %s", cik, name, exc)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--retry-errors", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    skip = "('ok','empty')" if not args.retry_errors else "('ok')"
    rows = conn.execute(f"""
        SELECT DISTINCT t.issuer_cik AS cik
          FROM trades t
         WHERE t.issuer_cik IS NOT NULL AND t.issuer_cik <> ''
           AND t.signal_class IN ('discretionary_buy','discretionary_sell')
           AND NOT EXISTS (SELECT 1 FROM issuer_earnings_status s
                            WHERE s.cik = t.issuer_cik AND s.status IN {skip})
         ORDER BY 1
    """).fetchall()
    # Release the read lock before any network I/O: this transaction would
    # otherwise hold AccessShareLock on `trades` across thousands of HTTP round
    # trips, which is the pattern behind the 2026-08-27 outage.
    conn.commit()

    if args.limit:
        rows = rows[:args.limit]
    logger.info("%d issuer CIK(s) to fetch", len(rows))

    ok = empty = err = total = 0
    t0 = time.time()
    for i, r in enumerate(rows, 1):
        raw_cik = str(r["cik"]).strip()
        try:
            cik_int = int(raw_cik)
        except ValueError:
            continue
        try:
            got = fetch(cik_int)
            status, msg = ("ok" if got else "empty"), None
        except Exception as exc:
            got, status, msg = None, "error", f"{type(exc).__name__}: {exc}"[:280]
            err += 1
        time.sleep(SLEEP)

        if args.dry_run:
            n8k = sum(1 for g in (got or []) if g["source"] == "edgar_8k_202")
            logger.info("  %s -> %s, %d rows (%d are 8-K 2.02)",
                        raw_cik, status, len(got or []), n8k)
            continue

        if got:
            for g in got:
                conn.execute("""
                    INSERT INTO issuer_earnings
                        (cik, announce_date, acceptance_datetime, form, items,
                         accession, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (cik, announce_date, form) DO NOTHING
                """, (raw_cik, g["date"], g["accept"], g["form"], g["items"],
                      g["accn"], g["source"]))
            total += len(got)
            ok += 1
        elif status == "empty":
            empty += 1

        conn.execute("""
            INSERT INTO issuer_earnings_status (cik, status, n_rows, attempts, last_error)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT (cik) DO UPDATE
               SET status = EXCLUDED.status, n_rows = EXCLUDED.n_rows,
                   attempts = issuer_earnings_status.attempts + 1,
                   last_error = EXCLUDED.last_error, last_attempt = NOW()
        """, (raw_cik, status, len(got or []), msg))

        if i % 100 == 0:
            conn.commit()
            logger.info("  %d/%d ok=%d empty=%d err=%d rows=%d (%.0fs)",
                        i, len(rows), ok, empty, err, total, time.time() - t0)

    if not args.dry_run:
        conn.commit()
    logger.info("Done in %.0fs: ok=%d empty=%d err=%d, %d rows",
                time.time() - t0, ok, empty, err, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
