#!/usr/bin/env python3
"""Shares outstanding per issuer, point-in-time, from EDGAR XBRL.

SOURCE

    https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/dei/EntityCommonStockSharesOutstanding.json

dei:EntityCommonStockSharesOutstanding is reported on the cover page of every
10-Q and 10-K, so it is quarterly and continuous for any filer. Free, keyless,
and the same host the SIC backfill already uses. Verified: AAPL returns 70
observations back to 2009-06-27.

THE PIT RULE, WHICH IS THE WHOLE POINT

Each observation carries `end` (what the count is a count of) and `filed` (when
the filing carrying it appeared). They differ, often by months -- TSLA's
2026-01-23 count was not filed until 2026-04-30.

    JOIN ON filed_date <= filing_date.  NEVER ON as_of_date.

Joining on as_of_date would let a trade in February use a share count nobody
could read until April. That is the same class of look-ahead as reading
filed_at as UTC, which put 37 entries a session early into the books.

Both dates are stored so the mistake is at least visible.

Usage:
    python3 pipelines/insider_study/backfill_shares_outstanding.py --dry-run --limit 5
    python3 pipelines/insider_study/backfill_shares_outstanding.py
    python3 pipelines/insider_study/backfill_shares_outstanding.py --retry-errors
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
RATE_LIMIT_SLEEP = 0.11          # ~9 req/s, inside SEC's published 10/s ceiling
# companyFACTS, not companyCONCEPT.
#
# The obvious endpoint is companyconcept/.../dei/EntityCommonStockSharesOutstanding,
# and it is 376 bytes instead of 250KB. It is also WRONG for a large share of
# filers: for Abbott (CIK 1800) it returns HTTP 200 with units.shares = [], while
# companyfacts for the same CIK carries 68 observations of the same tag. Four of
# the first five CIKs tried came back falsely empty. A source that reports
# "no data" when data exists would have silently produced a market-cap column
# populated for a fifth of the corpus and nobody would have known why.
#
# companyfacts costs ~253KB on the wire per issuer, so ~2.1GB and ~16 minutes of
# requests for the whole universe. That is the right trade.
URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"

#: Preferred first. dei is the cover-page count -- the actual shares outstanding
#: as of the filing. CommonStockSharesIssued is a balance-sheet figure that
#: includes treasury stock, so it OVERSTATES the float; it is a fallback only,
#: and is recorded under its own source so the two are never silently mixed.
CONCEPTS = (
    ("dei",     "EntityCommonStockSharesOutstanding", "edgar_xbrl_dei"),
    ("us-gaap", "CommonStockSharesIssued",            "edgar_xbrl_issued"),
)

# A share count outside this range is a units error or a corrupt filing, not a
# company. Recording it would poison every market cap derived from it.
MIN_SHARES = 1_000
MAX_SHARES = 500_000_000_000


def fetch(cik: int) -> list[dict] | None:
    """Share-count observations for one CIK, or None if the filer is unknown."""
    req = urllib.request.Request(URL.format(cik=cik),
                                 headers={"User-Agent": UA,
                                          "Accept-Encoding": "gzip, deflate"})
    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
        d = json.loads(raw)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None          # no XBRL facts for this filer at all
        raise

    facts = d.get("facts", {})
    for taxonomy, tag, source in CONCEPTS:
        entries = (facts.get(taxonomy, {}).get(tag, {})
                        .get("units", {}).get("shares", []))
        out = []
        for u in entries:
            end, filed, val = u.get("end"), u.get("filed"), u.get("val")
            if not (end and filed and isinstance(val, (int, float))):
                continue
            if not (MIN_SHARES <= val <= MAX_SHARES):
                continue
            out.append({"as_of": end, "filed": filed, "shares": int(val),
                        "form": u.get("form"), "accn": u.get("accn"),
                        "source": source})
        if out:
            return out           # first concept that yields anything wins
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--retry-errors", action="store_true",
                    help="re-attempt CIKs previously recorded as error")
    args = ap.parse_args()

    conn = get_connection()

    # Only issuers we actually hold trades for, and only those not already done.
    skip = "('ok','empty')" if not args.retry_errors else "('ok')"
    rows = conn.execute(f"""
        SELECT DISTINCT t.issuer_cik AS cik
          FROM trades t
         WHERE t.issuer_cik IS NOT NULL AND t.issuer_cik <> ''
           AND t.signal_class IN ('discretionary_buy','discretionary_sell')
           AND NOT EXISTS (SELECT 1 FROM issuer_shares_status s
                            WHERE s.cik = t.issuer_cik
                              AND s.status IN {skip})
         ORDER BY 1
    """).fetchall()
    if args.limit:
        rows = rows[:args.limit]
    logger.info("%d issuer CIK(s) to fetch", len(rows))

    ok = empty = err = total_rows = 0
    t0 = time.time()
    for i, r in enumerate(rows, 1):
        raw_cik = str(r["cik"]).strip()
        try:
            cik_int = int(raw_cik)
        except ValueError:
            continue

        try:
            obs = fetch(cik_int)
            status, msg = ("ok" if obs else "empty"), None
        except Exception as exc:                      # network / parse
            obs, status, msg = None, "error", f"{type(exc).__name__}: {exc}"[:280]
            err += 1
        time.sleep(RATE_LIMIT_SLEEP)

        if args.dry_run:
            logger.info("  %s -> %s, %d obs", raw_cik, status, len(obs or []))
            continue

        if obs:
            for o in obs:
                conn.execute("""
                    INSERT INTO issuer_shares_outstanding
                        (cik, as_of_date, filed_date, shares, form, accession, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (cik, as_of_date, filed_date) DO UPDATE
                       SET shares = EXCLUDED.shares, form = EXCLUDED.form,
                           source = EXCLUDED.source
                """, (raw_cik, o["as_of"], o["filed"], o["shares"],
                      o["form"], o["accn"], o["source"]))
            total_rows += len(obs)
            ok += 1
        elif status == "empty":
            empty += 1

        conn.execute("""
            INSERT INTO issuer_shares_status (cik, status, n_rows, attempts, last_error)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT (cik) DO UPDATE
               SET status = EXCLUDED.status, n_rows = EXCLUDED.n_rows,
                   attempts = issuer_shares_status.attempts + 1,
                   last_error = EXCLUDED.last_error, last_attempt = NOW()
        """, (raw_cik, status, len(obs or []), msg))

        if i % 200 == 0:
            conn.commit()
            logger.info("  %d/%d  ok=%d empty=%d err=%d rows=%d (%.0fs)",
                        i, len(rows), ok, empty, err, total_rows, time.time() - t0)

    if not args.dry_run:
        conn.commit()
    logger.info("Done in %.0fs: ok=%d empty=%d err=%d, %d observation rows",
                time.time() - t0, ok, empty, err, total_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
