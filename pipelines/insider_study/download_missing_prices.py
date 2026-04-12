#!/usr/bin/env python3
"""
Download daily price data for tickers missing from the price cache.
Uses Alpaca Data API. Sends Telegram progress updates.
"""

import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
if str(_FRAMEWORK_ROOT) not in sys.path:
    sys.path.insert(0, str(_FRAMEWORK_ROOT))

from framework.data.alpaca_client import AlpacaClient

sys.path.insert(0, str(Path(__file__).parent))
import notify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

PRICES_DIR = Path(__file__).parent / "data" / "prices"
MISSING_FILE = Path(__file__).parent / "data" / "missing_tickers.txt"


def load_credentials():
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
    return key, secret


def main():
    tickers = MISSING_FILE.read_text().strip().split("\n")
    tickers = [t.strip().upper() for t in tickers if t.strip()]

    # Filter out already-downloaded
    already = {f.stem for f in PRICES_DIR.glob("*.csv")}
    tickers = [t for t in tickers if t not in already]

    logger.info("Downloading %d missing tickers", len(tickers))
    notify.phase_start("Price Download", f"Downloading daily bars for {len(tickers)} missing tickers via Alpaca")

    key, secret = load_credentials()
    if not key or not secret:
        notify.error("Price Download", "Alpaca credentials not found")
        return

    client = AlpacaClient(api_key=key, api_secret=secret)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    success = 0
    failed = []
    start_time = time.time()
    last_notify = time.time()

    for i, ticker in enumerate(tickers):
        out_path = PRICES_DIR / f"{ticker}.csv"
        if out_path.exists():
            success += 1
            continue

        try:
            df = client.get_daily_bars(ticker, "2019-01-01", "2026-04-01")
            if df is not None and not df.empty:
                if df.index.tz is not None:
                    df.index = df.index.tz_convert("UTC").tz_localize(None)
                df.to_csv(out_path)
                success += 1
            else:
                failed.append(ticker)
        except Exception as e:
            failed.append(ticker)
            if i < 5:
                logger.warning("Failed %s: %s", ticker, e)

        # Progress update every 30 min
        elapsed = time.time() - start_time
        if time.time() - last_notify > 1800 or (i + 1) == len(tickers):
            pct = (i + 1) / len(tickers) * 100
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_min = (len(tickers) - i - 1) / rate / 60 if rate > 0 else 0
            notify.progress(
                "Price Download",
                pct,
                f"{success} downloaded, {len(failed)} failed of {len(tickers)}\n"
                f"Rate: {rate:.1f}/sec | ETA: {eta_min:.0f}min"
            )
            last_notify = time.time()

        if (i + 1) % 200 == 0:
            logger.info("  %d/%d (%.1f%%) — %d ok, %d failed",
                        i + 1, len(tickers), pct, success, len(failed))

    elapsed = time.time() - start_time
    notify.phase_end("Price Download",
                     f"Downloaded {success}/{len(tickers)} tickers in {elapsed/60:.1f}min\n"
                     f"Failed: {len(failed)}")

    if failed:
        fail_path = Path(__file__).parent / "data" / "failed_tickers.txt"
        fail_path.write_text("\n".join(failed))
        logger.info("Failed tickers written to %s", fail_path)


if __name__ == "__main__":
    main()
