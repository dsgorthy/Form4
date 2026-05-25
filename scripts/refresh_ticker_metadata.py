#!/usr/bin/env python3
"""Refresh ticker_metadata from yfinance.

Populates sector + industry for every ticker that has any P/S insider trade in
the trades table. Initial backfill is ~9k tickers; weekly delta is small.

Why yfinance: free, no API key, generous rate limit, covers ~80% of micro-caps.
Known limitations: occasional NULLs on very recently listed names, occasional
stale sector for companies that pivoted. Acceptable for our use case (industry
net-flow signals are robust to a few % misclassification).

PIT notes: ticker_metadata is a SNAPSHOT, not a temporal table. We use today's
sector classification for all historical trades. Companies switching sectors
mid-history are rare (<5% per decade for S&P names). DO NOT use ticker_metadata
in any scoring path that needs historical sector — it would silently apply
today's value to old trades. tests/unit/test_pit_validation.py enforces this
via TestNoFutureDataInTickerMetadata.

Usage:
    python3 scripts/refresh_ticker_metadata.py            # delta refresh
    python3 scripts/refresh_ticker_metadata.py --full     # re-fetch ALL tickers
    python3 scripts/refresh_ticker_metadata.py --tickers AAPL,MSFT
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.observability import pipeline_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# yfinance is generous but courtesy throttle. 2 req/sec keeps us off the radar.
SLEEP_BETWEEN_REQUESTS_S = 0.5
# How long sector data is considered fresh before we re-fetch.
STALENESS_DAYS = 7


def get_tickers_needing_refresh(conn, full: bool, explicit: list[str] | None) -> list[str]:
    if explicit:
        return sorted({t.strip().upper() for t in explicit if t.strip()})

    if full:
        rows = conn.execute(
            """SELECT DISTINCT ticker FROM trades
               WHERE trans_code IN ('P','S') AND ticker IS NOT NULL AND ticker != 'NONE'
               ORDER BY ticker"""
        ).fetchall()
        return [r[0] for r in rows]

    # Delta: tickers in trades NOT in metadata, OR stale
    rows = conn.execute(
        f"""SELECT DISTINCT t.ticker
            FROM trades t
            LEFT JOIN ticker_metadata m ON m.ticker = t.ticker
            WHERE t.trans_code IN ('P','S')
              AND t.ticker IS NOT NULL AND t.ticker != 'NONE'
              AND (m.ticker IS NULL
                   OR m.last_refreshed < NOW() - INTERVAL '{STALENESS_DAYS} days')
            ORDER BY t.ticker"""
    ).fetchall()
    return [r[0] for r in rows]


def fetch_one(ticker: str) -> tuple[str | None, str | None, str | None]:
    """Returns (sector, industry, error_message). Imports yfinance lazily so
    --help doesn't pay the import cost."""
    import yfinance as yf
    try:
        info = yf.Ticker(ticker).info or {}
        # yfinance returns "industry" + "sector" for stocks; ETFs have neither.
        # quoteType helps us identify the case for the error message.
        sector = info.get("sector") or None
        industry = info.get("industry") or None
        if not sector and not industry:
            qt = info.get("quoteType", "?")
            return None, None, f"no sector/industry (quoteType={qt})"
        return sector, industry, None
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"


def upsert_ticker(conn, ticker: str, sector: str | None, industry: str | None,
                  error: str | None) -> None:
    if error and not sector and not industry:
        # Record the failure attempt — don't overwrite an existing valid record
        conn.execute(
            """INSERT INTO ticker_metadata
                 (ticker, sector, industry, source, last_refreshed,
                  refresh_attempts, last_error)
               VALUES (?, NULL, NULL, 'yfinance', NOW(), 1, ?)
               ON CONFLICT (ticker) DO UPDATE SET
                 last_refreshed = NOW(),
                 refresh_attempts = ticker_metadata.refresh_attempts + 1,
                 last_error = EXCLUDED.last_error""",
            (ticker, error),
        )
        return

    conn.execute(
        """INSERT INTO ticker_metadata
             (ticker, sector, industry, source, last_refreshed,
              refresh_attempts, last_error)
           VALUES (?, ?, ?, 'yfinance', NOW(), 0, NULL)
           ON CONFLICT (ticker) DO UPDATE SET
             sector = EXCLUDED.sector,
             industry = EXCLUDED.industry,
             source = EXCLUDED.source,
             last_refreshed = NOW(),
             refresh_attempts = 0,
             last_error = NULL""",
        (ticker, sector, industry),
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--full", action="store_true",
                   help="Re-fetch EVERY ticker, not just stale/missing")
    p.add_argument("--tickers", default=None,
                   help="Comma-separated explicit tickers to refresh")
    p.add_argument("--limit", type=int, default=None,
                   help="Stop after this many tickers (smoke testing)")
    args = p.parse_args()

    with pipeline_run(
        "refresh_ticker_metadata",
        log_path="/Users/derekg/trading-framework/logs/refresh-ticker-metadata.log",
    ) as prun:
        conn = get_connection()
        explicit = args.tickers.split(",") if args.tickers else None
        tickers = get_tickers_needing_refresh(conn, args.full, explicit)
        if args.limit:
            tickers = tickers[: args.limit]

        logger.info("Tickers to refresh: %d", len(tickers))

        ok = 0
        empty = 0
        errored = 0
        t0 = time.monotonic()

        for i, ticker in enumerate(tickers, 1):
            sector, industry, err = fetch_one(ticker)
            upsert_ticker(conn, ticker, sector, industry, err)
            if sector or industry:
                ok += 1
            elif err and "no sector" in err:
                empty += 1
            else:
                errored += 1

            # Commit every 50 to bound the transaction
            if i % 50 == 0:
                conn.commit()
                elapsed = time.monotonic() - t0
                eta = elapsed / i * (len(tickers) - i)
                logger.info(
                    "  %d/%d  ok=%d empty=%d errored=%d  elapsed=%.0fs  eta=%.0fs",
                    i, len(tickers), ok, empty, errored, elapsed, eta,
                )

            time.sleep(SLEEP_BETWEEN_REQUESTS_S)

        conn.commit()
        conn.close()

        logger.info("Done. ok=%d empty=%d errored=%d total=%d in %.1fs",
                    ok, empty, errored, len(tickers), time.monotonic() - t0)

        prun.set_rows_written(ok + empty + errored)
        prun.set_metadata({
            "ok": ok,
            "empty": empty,
            "errored": errored,
            "total": len(tickers),
            "full": args.full,
        })


if __name__ == "__main__":
    main()
