"""
Download daily OHLCV price data for insider event tickers via Alpaca Data API v2.

For each ticker in the event calendar, downloads daily bars covering the full
study window. Saves one CSV per ticker to data/prices/{TICKER}.csv.
Also always downloads SPY as the benchmark.

Usage:
    python collect_prices.py \
        --events data/events.csv \
        --output-dir data/prices/ \
        --start 2019-01-01 \
        --end 2026-01-01

Credentials are read from .env (ALPACA_API_KEY / ALPACA_API_SECRET) two
directories up from this file, or from environment variables.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# Allow running as a standalone script from any directory
_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
if str(_FRAMEWORK_ROOT) not in sys.path:
    sys.path.insert(0, str(_FRAMEWORK_ROOT))

from framework.data.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")


def _load_credentials() -> tuple[str, str]:
    """Load Alpaca credentials from .env file or environment variables."""
    # Try environment variables first
    key = os.environ.get("ALPACA_API_KEY", "")
    secret = os.environ.get("ALPACA_API_SECRET", "")

    if not key or not secret:
        # Try .env file in framework root
        env_path = _FRAMEWORK_ROOT / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("ALPACA_API_KEY=") and not key:
                    key = line.split("=", 1)[1].strip()
                elif line.startswith("ALPACA_API_SECRET=") and not secret:
                    secret = line.split("=", 1)[1].strip()

    if not key or not secret:
        logger.error(
            "Alpaca credentials not found. Set ALPACA_API_KEY and ALPACA_API_SECRET "
            "in environment or in %s/.env", _FRAMEWORK_ROOT
        )
        sys.exit(1)

    return key, secret


def download_ticker(
    client: AlpacaClient,
    ticker: str,
    start: str,
    end: str,
    output_dir: Path,
    overwrite: bool = False,
) -> bool:
    """
    Download daily bars for a ticker via Alpaca and save to CSV.
    Returns True on success, False on failure.

    Output columns: open, high, low, close, volume, vwap, trade_count
    Index: timestamp (datetime, timezone-naive for compatibility with run_event_study.py)
    """
    out_path = output_dir / f"{ticker.upper()}.csv"
    if out_path.exists() and not overwrite:
        return True

    try:
        df = client.get_daily_bars(ticker, start, end)
    except Exception as e:
        logger.warning("Alpaca failed for %s: %s", ticker, e)
        return False

    if df.empty:
        logger.warning("No daily bar data for %s (%s to %s)", ticker, start, end)
        return False

    # Normalize to timezone-naive UTC for consistent CSV storage and downstream reads
    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)

    df.to_csv(out_path)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Download daily price data for insider event tickers via Alpaca"
    )
    parser.add_argument(
        "--events",
        type=Path,
        required=True,
        help="Event calendar CSV (output of build_event_calendar.py)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "data" / "prices",
    )
    parser.add_argument(
        "--start",
        default="2019-01-01",
        help="Start date for price history (go earlier than first event to allow T+1 entry)",
    )
    parser.add_argument("--end", default="2026-01-01", help="End date")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download even if CSV already exists",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    events = pd.read_csv(args.events)
    tickers = list(events["ticker"].dropna().astype(str).str.strip().str.upper().unique())

    # Always include SPY as the benchmark
    if "SPY" not in tickers:
        tickers = ["SPY"] + tickers

    logger.info(
        "Downloading daily bars for %d tickers (%s to %s) via Alpaca...",
        len(tickers), args.start, args.end,
    )

    key, secret = _load_credentials()
    client = AlpacaClient(api_key=key, api_secret=secret)

    success = 0
    failed = []

    for i, ticker in enumerate(tickers):
        if i % 100 == 0 and i > 0:
            logger.info("  %d/%d (%.1f%%)", i, len(tickers), 100 * i / len(tickers))

        ok = download_ticker(client, ticker, args.start, args.end, args.output_dir, args.overwrite)
        if ok:
            success += 1
        else:
            failed.append(ticker)

    logger.info("Done: %d/%d downloaded, %d failed", success, len(tickers), len(failed))
    if failed:
        # Alpaca won't have OTC, foreign-listed, or very small tickers — expected
        logger.info(
            "Failed tickers (%d) — likely OTC, delisted, or not in Alpaca SIP feed: %s%s",
            len(failed),
            ", ".join(failed[:30]),
            " ..." if len(failed) > 30 else "",
        )

    print(f"\nPrice data saved to: {args.output_dir}")
    print(f"Downloaded: {success}/{len(tickers)}")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}")


if __name__ == "__main__":
    main()
