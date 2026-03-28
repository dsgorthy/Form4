"""
Historical data collection — pulls SPY and VIXY 1-minute bars from Alpaca.

Reads credentials from spy-0dte/.env (or explicit flags).
Saves parquets to spy-0dte/data/raw/{SYMBOL}/{YYYY-MM-DD}.parquet to reuse existing storage.

Usage:
    python3 pipelines/collect_historical.py --start 2020-01-01 --end 2023-12-31
    python3 pipelines/collect_historical.py --start 2020-01-01 --end 2023-12-31 --symbols SPY,VIXY
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

PROJECT_ROOT = Path(__file__).parent.parent
REPO_ROOT = PROJECT_ROOT.parent


def load_credentials(env_path: Path) -> dict:
    """Parse a .env file for Alpaca credentials."""
    creds = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                creds[k.strip()] = v.strip()
    return creds


def collect_symbol(client, symbol: str, start: str, end: str, output_dir: Path):
    """Pull 1-min bars for a symbol over a date range, saving one parquet per day."""
    import pandas as pd
    from framework.data.calendar import MarketCalendar

    cal = MarketCalendar()
    trading_days = cal.get_trading_days(start, end)

    skipped = 0
    fetched = 0
    errors = 0

    for date_str in trading_days:
        out_path = output_dir / symbol / f"{date_str}.parquet"
        if out_path.exists():
            skipped += 1
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            bars = client.get_bars(
                symbol=symbol,
                timeframe="1Min",
                start=f"{date_str}T04:00:00-05:00",
                end=f"{date_str}T20:00:00-05:00",
            )
            if bars.empty:
                logger.debug("No bars for %s on %s", symbol, date_str)
                skipped += 1
                continue

            # Filter to market hours (9:00 AM – 4:30 PM ET)
            bars = bars[
                (bars.index.hour >= 9) & (bars.index.hour <= 16)
            ]
            if bars.empty:
                skipped += 1
                continue

            bars.to_parquet(out_path)
            fetched += 1
            if fetched % 50 == 0:
                logger.info("  %s: %d fetched, %d skipped so far", symbol, fetched, skipped)

            # Rate-limit courtesy
            time.sleep(0.15)

        except Exception as e:
            logger.error("Error fetching %s on %s: %s", symbol, date_str, e)
            errors += 1
            time.sleep(1.0)

    logger.info("%s done: %d fetched, %d skipped, %d errors", symbol, fetched, skipped, errors)


def main():
    parser = argparse.ArgumentParser(description="Pull historical 1-min bars from Alpaca")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--symbols", default="SPY,VIXY", help="Comma-separated symbols")
    parser.add_argument("--output-dir", type=Path, help="Output directory (default: spy-0dte/data/raw)")
    parser.add_argument("--env-file", type=Path, help="Path to .env with Alpaca credentials")
    args = parser.parse_args()

    # Resolve credentials
    env_paths = [
        args.env_file,
        REPO_ROOT / "spy-0dte" / ".env",
        PROJECT_ROOT / ".env",
    ]
    creds = {}
    for p in env_paths:
        if p and p.exists():
            creds = load_credentials(p)
            if creds.get("ALPACA_API_KEY") and creds.get("ALPACA_API_SECRET"):
                logger.info("Using credentials from %s", p)
                break

    api_key = creds.get("ALPACA_API_KEY", "")
    api_secret = creds.get("ALPACA_API_SECRET", "")
    if not api_key or not api_secret:
        logger.error("No Alpaca credentials found. Set ALPACA_API_KEY and ALPACA_API_SECRET in .env")
        sys.exit(1)

    # Resolve output directory
    output_dir = args.output_dir or (REPO_ROOT / "spy-0dte" / "data" / "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", output_dir)

    # Initialize client
    from framework.data.alpaca_client import AlpacaClient
    client = AlpacaClient(
        api_key=api_key,
        api_secret=api_secret,
        base_url=creds.get("ALPACA_DATA_URL", "https://data.alpaca.markets/v2"),
    )

    symbols = [s.strip() for s in args.symbols.split(",")]
    logger.info("Collecting %s from %s to %s", symbols, args.start, args.end)

    for symbol in symbols:
        logger.info("=== %s ===", symbol)
        collect_symbol(client, symbol, args.start, args.end, output_dir)

    logger.info("Collection complete.")


if __name__ == "__main__":
    main()
