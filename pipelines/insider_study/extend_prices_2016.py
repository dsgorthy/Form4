"""
Extend daily price CSVs back to 2016-01-01 for the EDGAR 2016-2019 backfill.

Existing CSVs in data/prices/ start around 2019-01-01. This script:
1. Queries the trades DB for tickers with trade_date < 2019-01-01
2. For each ticker, downloads 2016-01-01 to 2018-12-31 from Alpaca
3. Prepends the new data to the existing CSV (if any), or creates a new one
4. Also handles brand-new tickers that have no existing CSV

Usage:
    python pipelines/insider_study/extend_prices_2016.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
if str(_FRAMEWORK_ROOT) not in sys.path:
    sys.path.insert(0, str(_FRAMEWORK_ROOT))

from framework.data.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

DB_PATH = _FRAMEWORK_ROOT / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DIR = Path(__file__).parent / "data" / "prices"
EXTEND_START = "2016-01-01"
EXTEND_END = "2018-12-31"


def _load_credentials() -> tuple[str, str]:
    key = os.environ.get("ALPACA_DATA_API_KEY", "")
    secret = os.environ.get("ALPACA_DATA_API_SECRET", "")
    if not key or not secret:
        env_path = _FRAMEWORK_ROOT / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("ALPACA_DATA_API_KEY=") and not key:
                    key = line.split("=", 1)[1].strip()
                elif line.startswith("ALPACA_DATA_API_SECRET=") and not secret:
                    secret = line.split("=", 1)[1].strip()
    if not key or not secret:
        logger.error("Alpaca data credentials not found (ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET)")
        sys.exit(1)
    return key, secret


def get_tickers_needing_extension() -> list[str]:
    """Get tickers that have trades before 2019 but price data starts at 2019+."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")

    # Tickers with pre-2019 trades
    rows = conn.execute("""
        SELECT DISTINCT ticker FROM trades
        WHERE trade_date < '2019-01-01'
        ORDER BY ticker
    """).fetchall()
    conn.close()

    tickers = [r[0] for r in rows if r[0]]

    # Filter to those whose CSV either doesn't exist or starts after 2016
    need_extension = []
    for t in tickers:
        csv_path = PRICES_DIR / f"{t.upper()}.csv"
        if not csv_path.exists():
            need_extension.append(t)
            continue
        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True, nrows=1)
            first_date = df.index[0]
            if hasattr(first_date, 'year') and first_date.year > 2016:
                need_extension.append(t)
        except Exception:
            need_extension.append(t)

    # Always include SPY
    if "SPY" not in need_extension:
        need_extension.insert(0, "SPY")

    return need_extension


def extend_ticker(client: AlpacaClient, ticker: str) -> bool:
    """Download 2016-2018 data and prepend to existing CSV."""
    try:
        df_new = client.get_daily_bars(ticker, EXTEND_START, EXTEND_END)
    except Exception as e:
        logger.warning("Alpaca failed for %s: %s", ticker, e)
        return False

    if df_new.empty:
        logger.warning("No data for %s in 2016-2018", ticker)
        return False

    if df_new.index.tz is not None:
        df_new.index = df_new.index.tz_convert("UTC").tz_localize(None)

    csv_path = PRICES_DIR / f"{ticker.upper()}.csv"

    if csv_path.exists():
        # Load existing and prepend
        df_existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        if df_existing.index.tz is not None:
            df_existing.index = df_existing.index.tz_convert("UTC").tz_localize(None)
        # Combine, dedup by date, sort
        df_combined = pd.concat([df_new, df_existing])
        df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
        df_combined.sort_index(inplace=True)
        df_combined.to_csv(csv_path)
        logger.info("%s: prepended %d bars (total %d)", ticker, len(df_new), len(df_combined))
    else:
        df_new.to_csv(csv_path)
        logger.info("%s: created new CSV with %d bars", ticker, len(df_new))

    return True


def main():
    parser = argparse.ArgumentParser(description="Extend price CSVs back to 2016")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Finding tickers that need 2016-2018 price data...")
    tickers = get_tickers_needing_extension()
    logger.info("Found %d tickers needing extension", len(tickers))

    if args.dry_run:
        for t in tickers:
            csv_path = PRICES_DIR / f"{t.upper()}.csv"
            status = "extend" if csv_path.exists() else "new"
            print(f"  {t}: {status}")
        print(f"\nTotal: {len(tickers)} tickers")
        return

    key, secret = _load_credentials()
    client = AlpacaClient(api_key=key, api_secret=secret)

    success = 0
    failed = []

    for i, ticker in enumerate(tickers):
        if i % 50 == 0 and i > 0:
            logger.info("  Progress: %d/%d (%.1f%%)", i, len(tickers), 100 * i / len(tickers))

        if extend_ticker(client, ticker):
            success += 1
        else:
            failed.append(ticker)

    logger.info("Done: %d/%d extended, %d failed", success, len(tickers), len(failed))
    if failed:
        logger.info("Failed: %s", ", ".join(failed[:30]))


if __name__ == "__main__":
    main()
