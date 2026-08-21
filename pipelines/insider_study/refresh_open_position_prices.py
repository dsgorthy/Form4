#!/usr/bin/env python3
"""Keep marks on OPEN positions current during the trading session.

THE PROBLEM

`prices.daily_prices` is refreshed once a day by the evening feature chain, so
every unrealised P&L on the site was as of the previous close. `/portfolio`
marks open positions to "the latest available close", and until this job
existed that meant yesterday's.

The 10-minute `strategy-intraday` job did not help: it calls `get_latest_close()`,
which reads `prices.daily_prices`, so it re-read the same stale row every ten
minutes. It was intraday for ENTRIES and not for PRICES.

THE FIX, AND WHY IT IS SHAPED THIS WAY

Alpaca's 1Day bar updates through the session — its close is the last trade so
far — so upserting today's bar mid-session gives a current mark. Refreshing the
DB rather than calling Alpaca from the API means:

  * every reader benefits at once — /portfolio, the overlay endpoint, the
    intraday simulator — with no code change in any of them,
  * no third-party call sits in a web request path, where its latency and
    rate limits would become the page's,
  * the API container has no Alpaca credentials, and does not need any.

SCOPE IS DELIBERATELY SMALL. Only tickers with an open simulated position —
about twenty. This is not a general price refresh; the evening chain still owns
the full universe and the historical record.

WHAT THIS DOES NOT DO

It does not make stops intraday. The simulator still compares the stop level to
a close, and that is on purpose: modelling the -30% stop against the daily LOW
instead was measured on 2026-08-20 and would have converted four of 255
positions into -30% losses, two of which actually finished POSITIVE (CATX
+8.2%, ENVX +0.4%). All four touched the level intraday and recovered. The
close-only check is acting as a whipsaw filter and is worth keeping.

Usage:
    python3 -m pipelines.insider_study.refresh_open_position_prices
    python3 -m pipelines.insider_study.refresh_open_position_prices --force
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection
from framework.observability.pipeline_runner import pipeline_run

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("refresh_open_position_prices")

_ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parents[2]

#: Published books only. quality_momentum_2x and retired strategies still have
#: rows; nobody is looking at their marks.
STRATEGIES = ("quality_notrend", "quality_momentum", "reversal_dip")


def market_is_open(now: datetime | None = None) -> bool:
    """Regular session, Eastern. Deliberately ignores holidays — the cost of
    running on Thanksgiving is one wasted Alpaca call."""
    now = now or datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    return (9, 30) <= (now.hour, now.minute) <= (16, 5)


def open_position_tickers() -> list[str]:
    conn = get_connection()
    placeholders = ",".join("?" for _ in STRATEGIES)
    rows = conn.execute(
        f"""SELECT DISTINCT ticker FROM strategy_portfolio
             WHERE status = 'open' AND execution_source = 'simulated'
               AND strategy IN ({placeholders})
               AND ticker IS NOT NULL AND ticker <> 'NONE'
             ORDER BY ticker""",
        tuple(STRATEGIES),
    ).fetchall()
    return [r["ticker"] for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--force", action="store_true",
                    help="run even when the market is closed")
    args = ap.parse_args()

    with pipeline_run("refresh-open-position-prices") as run:
        if not args.force and not market_is_open():
            logger.info("market closed — nothing to do")
            run.metadata.update({"skipped": "market_closed"})
            return 0

        tickers = open_position_tickers()
        if not tickers:
            logger.info("no open positions")
            run.metadata.update({"tickers": 0})
            return 0

        logger.info("refreshing %d tickers: %s", len(tickers), ",".join(tickers))
        result = subprocess.run(
            [sys.executable, "-m", "pipelines.insider_study.update_daily_prices",
             "--tickers", ",".join(tickers), "--lookback-days", "5"],
            cwd=str(REPO), capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            tail = (result.stderr or result.stdout or "")[-400:]
            logger.error("update_daily_prices failed: %s", tail)
            raise RuntimeError(f"price refresh failed: {tail}")

        run.metadata.update({"tickers": len(tickers)})
        run.rows_written = len(tickers)
        logger.info("done — %d tickers refreshed", len(tickers))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
