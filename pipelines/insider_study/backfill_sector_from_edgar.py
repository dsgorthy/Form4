#!/usr/bin/env python3
"""Fill ticker_metadata.sector from EDGAR SIC codes, for names yfinance cannot see.

WHY

industry_buy_pct_90d is NULL on 73% of eligible discretionary buys, and the
binding constraint is sector coverage: only 4,664 of 8,500 traded tickers carry
one. Every existing row came from yfinance, and the gap is not transient --
2,500 tickers return quoteType=NONE after NINE attempts, because yfinance only
knows currently-listed symbols and a large share of insider filings are on
delisted, OTC, or renamed issuers.

EDGAR does not have that problem. The issuer files the Form 4 itself, so every
ticker we hold has an issuer_cik (3,757 of the 3,836 missing ones), and
data.sec.gov publishes an SIC code per filer. It is free, keyless, and already
a dependency of this project.

ONE VOCABULARY

The existing 9,392 rows use yfinance's eleven sectors. Emitting raw SIC
divisions alongside them would silently break industry_buy_pct_90d, which
compares a ticker against its sector peers -- half the peer group would be
labelled in a different taxonomy. So SIC is MAPPED onto those same eleven
names, and rows are stamped source='edgar_sic' so provenance stays visible.

WHAT IS DELIBERATELY LEFT NULL

Mutual funds (884) and ETFs (127). A sector for a fund is not missing data, it
is a category error, and inventing one would put them in peer groups they do
not belong to.

Usage:
    python3 pipelines/insider_study/backfill_sector_from_edgar.py --dry-run
    python3 pipelines/insider_study/backfill_sector_from_edgar.py
    python3 pipelines/insider_study/backfill_sector_from_edgar.py --limit 50
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# SEC asks for a descriptive UA with contact info, and rate-limits at 10 req/s.
UA = "SidequestGroup Form4 Research derek@sidequestgroup.com"
RATE_LIMIT_SLEEP = 0.11          # ~9 req/s, inside SEC's published ceiling
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

# Exact 4-digit SIC codes whose 2-digit group would send them to the wrong
# sector. Biotech (8731) is the important one: a large share of small-cap
# insider buying sits there, and its major group 87 is otherwise Industrials.
SIC4 = {
    "2833": "Healthcare", "2834": "Healthcare", "2835": "Healthcare",
    "2836": "Healthcare", "8731": "Healthcare", "3841": "Healthcare",
    "3842": "Healthcare", "3843": "Healthcare", "3844": "Healthcare",
    "3845": "Healthcare", "3826": "Healthcare",
    "6798": "Real Estate", "6500": "Real Estate", "6512": "Real Estate",
    "6531": "Real Estate", "6552": "Real Estate",
    "7372": "Technology", "7370": "Technology", "7371": "Technology",
    "7373": "Technology", "7374": "Technology", "7389": "Industrials",
    "3570": "Technology", "3571": "Technology", "3572": "Technology",
    "3576": "Technology", "3577": "Technology", "3674": "Technology",
    "1311": "Energy", "1381": "Energy", "1389": "Energy", "2911": "Energy",
    "4911": "Utilities", "4931": "Utilities", "4924": "Utilities",
    "5812": "Consumer Cyclical", "5912": "Consumer Defensive",
    "5411": "Consumer Defensive", "2011": "Consumer Defensive",
    "3711": "Consumer Cyclical", "3721": "Industrials", "3728": "Industrials",
}

# 2-digit SIC major group -> sector. Fallback when no 4-digit rule applies.
SIC2 = {
    "01": "Consumer Defensive", "02": "Consumer Defensive",
    "07": "Consumer Defensive", "08": "Basic Materials", "09": "Consumer Defensive",
    "10": "Basic Materials", "12": "Energy", "13": "Energy", "14": "Basic Materials",
    "15": "Industrials", "16": "Industrials", "17": "Industrials",
    "20": "Consumer Defensive", "21": "Consumer Defensive",
    "22": "Consumer Cyclical", "23": "Consumer Cyclical",
    "24": "Basic Materials", "25": "Consumer Cyclical", "26": "Basic Materials",
    "27": "Communication Services", "28": "Basic Materials", "29": "Energy",
    "30": "Basic Materials", "31": "Consumer Cyclical", "32": "Basic Materials",
    "33": "Basic Materials", "34": "Industrials", "35": "Industrials",
    "36": "Technology", "37": "Industrials", "38": "Healthcare",
    "39": "Consumer Cyclical",
    "40": "Industrials", "41": "Industrials", "42": "Industrials",
    "44": "Industrials", "45": "Industrials", "46": "Energy",
    "47": "Industrials", "48": "Communication Services", "49": "Utilities",
    "50": "Industrials", "51": "Consumer Defensive",
    "52": "Consumer Cyclical", "53": "Consumer Cyclical", "54": "Consumer Defensive",
    "55": "Consumer Cyclical", "56": "Consumer Cyclical", "57": "Consumer Cyclical",
    "58": "Consumer Cyclical", "59": "Consumer Cyclical",
    "60": "Financial Services", "61": "Financial Services",
    "62": "Financial Services", "63": "Financial Services",
    "64": "Financial Services", "65": "Real Estate", "67": "Financial Services",
    "70": "Consumer Cyclical", "72": "Consumer Cyclical", "73": "Technology",
    "75": "Consumer Cyclical", "76": "Industrials", "78": "Communication Services",
    "79": "Communication Services", "80": "Healthcare", "81": "Industrials",
    "82": "Consumer Defensive", "83": "Healthcare", "86": "Industrials",
    "87": "Industrials", "89": "Industrials",
}

VALID = {"Financial Services", "Healthcare", "Technology", "Industrials",
         "Consumer Cyclical", "Real Estate", "Consumer Defensive",
         "Communication Services", "Energy", "Basic Materials", "Utilities"}


def sic_to_sector(sic: str | None) -> str | None:
    if not sic:
        return None
    sic = str(sic).strip().zfill(4)
    if sic in SIC4:
        return SIC4[sic]
    return SIC2.get(sic[:2])


def fetch_sic(cik: int) -> tuple[str | None, str | None]:
    """(sic, sicDescription) for a CIK, or (None, None)."""
    req = urllib.request.Request(SUBMISSIONS.format(cik=cik),
                                 headers={"User-Agent": UA,
                                          "Accept-Encoding": "gzip, deflate"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                import gzip
                raw = gzip.decompress(raw)
            d = json.loads(raw)
            return d.get("sic") or None, d.get("sicDescription") or None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None
        raise
    except Exception as exc:
        logger.debug("cik %s: %s", cik, exc)
        return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    conn = get_connection()

    # Tickers we actually trade on, with no sector, that are not funds. The
    # quoteType filter is what keeps mutual funds and ETFs correctly NULL.
    rows = conn.execute("""
        SELECT DISTINCT t.ticker, MAX(t.issuer_cik) AS cik
          FROM trades t
          LEFT JOIN ticker_metadata m ON m.ticker = t.ticker
         WHERE t.signal_class IN ('discretionary_buy','discretionary_sell')
           AND t.ticker IS NOT NULL AND t.ticker <> 'NONE'
           AND t.issuer_cik IS NOT NULL
           AND (m.sector IS NULL)
           AND COALESCE(m.last_error, '') NOT LIKE '%MUTUALFUND%'
           AND COALESCE(m.last_error, '') NOT LIKE '%ETF%'
         GROUP BY t.ticker
         ORDER BY t.ticker
    """).fetchall()

    if args.limit:
        rows = rows[:args.limit]
    logger.info("%d tickers to resolve via EDGAR SIC", len(rows))

    resolved = unmapped = notfound = 0
    t0 = time.time()
    for i, r in enumerate(rows, 1):
        ticker, cik = r["ticker"], r["cik"]
        try:
            cik_int = int(str(cik).strip())
        except (TypeError, ValueError):
            continue

        sic, sic_desc = fetch_sic(cik_int)
        time.sleep(RATE_LIMIT_SLEEP)

        if not sic:
            notfound += 1
            continue
        sector = sic_to_sector(sic)
        if not sector:
            unmapped += 1
            logger.info("  %s: SIC %s (%s) has no sector mapping", ticker, sic, sic_desc)
            continue
        assert sector in VALID, f"{sector!r} is not one of the eleven yfinance sectors"
        resolved += 1

        if not args.dry_run:
            conn.execute("""
                INSERT INTO ticker_metadata (ticker, sector, industry, source,
                                             last_refreshed, refresh_attempts)
                VALUES (?, ?, ?, 'edgar_sic', NOW(), 1)
                ON CONFLICT (ticker) DO UPDATE
                   SET sector = EXCLUDED.sector,
                       industry = COALESCE(ticker_metadata.industry, EXCLUDED.industry),
                       source = 'edgar_sic',
                       last_refreshed = EXCLUDED.last_refreshed
                 WHERE ticker_metadata.sector IS NULL
            """, (ticker, sector, sic_desc))
            if i % 100 == 0:
                conn.commit()

        if i % 200 == 0:
            logger.info("  %d/%d  resolved=%d unmapped=%d notfound=%d (%.0fs)",
                        i, len(rows), resolved, unmapped, notfound, time.time() - t0)

    if not args.dry_run:
        conn.commit()
    logger.info("Done in %.0fs: resolved=%d unmapped=%d notfound=%d",
                time.time() - t0, resolved, unmapped, notfound)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
