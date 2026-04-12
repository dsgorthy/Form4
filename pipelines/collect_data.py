"""
Generic multi-symbol 1-min bar collector.

Reads Alpaca credentials from environment variables (via .env file in the
trading-framework root, spy-0dte sibling dir, or os.environ).

Saves one parquet per symbol/day to:
    {FRAMEWORK_ROOT}/data/raw/{SYMBOL}/{YYYY-MM-DD}.parquet

Resume-safe: skips any date that already has a file on disk.

Usage:
    python pipelines/collect_data.py --symbols QQQ IWM --start 2020-01-01 --end 2025-12-31
    python pipelines/collect_data.py --symbols SPY VIXY QQQ IWM --start 2020-01-01 --end 2025-12-31
    python pipelines/collect_data.py --symbols QQQ --start 2020-01-01 --end 2025-12-31 --output-dir /path/to/raw

Runtime estimate at Alpaca's 200 req/min limit:
    ~1545 trading days × N symbols ÷ 200 = ~8 min per symbol
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

FRAMEWORK_ROOT = Path(__file__).parent.parent
REPO_ROOT = FRAMEWORK_ROOT.parent


def load_env_file(path: Path) -> dict:
    """Parse a .env file into a dict."""
    result = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
    return result


def resolve_credentials(env_file: Path = None) -> tuple[str, str, str]:
    """
    Resolve shared read-only Alpaca data credentials in priority order:
      1. Explicit --env-file argument
      2. .env in trading-framework root
      3. .env in spy-0dte sibling dir
      4. os.environ (ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET)
    Returns (api_key, api_secret, data_url).
    """
    candidates = [env_file] if env_file else []
    candidates += [
        FRAMEWORK_ROOT / ".env",
        REPO_ROOT / "spy-0dte" / ".env",
    ]

    creds: dict = {}
    for p in candidates:
        if p and p.exists():
            loaded = load_env_file(p)
            if loaded.get("ALPACA_DATA_API_KEY") and loaded.get("ALPACA_DATA_API_SECRET"):
                creds = loaded
                logger.info("Using credentials from %s", p)
                break

    api_key = creds.get("ALPACA_DATA_API_KEY") or os.getenv("ALPACA_DATA_API_KEY", "")
    api_secret = creds.get("ALPACA_DATA_API_SECRET") or os.getenv("ALPACA_DATA_API_SECRET", "")
    data_url = creds.get("ALPACA_DATA_URL") or os.getenv(
        "ALPACA_DATA_URL", "https://data.alpaca.markets/v2"
    )
    return api_key, api_secret, data_url


def collect_symbol(client, symbol: str, start: str, end: str, output_dir: Path) -> None:
    """
    Pull 1-min bars for one symbol over a date range.
    Saves one parquet per trading day. Skips dates already on disk.
    """
    from framework.data.calendar import MarketCalendar

    cal = MarketCalendar()
    trading_days = cal.get_trading_days(start, end)
    total = len(trading_days)
    logger.info("%s — %d trading days in range [%s, %s]", symbol, total, start, end)

    fetched = skipped = errors = 0

    for i, date_str in enumerate(trading_days, 1):
        out_path = output_dir / symbol.upper() / f"{date_str}.parquet"
        if out_path.exists():
            skipped += 1
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Request full extended-hours window; we filter to regular session
            df = client.get_bars_df(
                symbol=symbol,
                start=f"{date_str}T04:00:00-05:00",
                end=f"{date_str}T20:00:00-05:00",
                timeframe="1Min",
            )

            if df.empty:
                logger.debug("No bars for %s on %s", symbol, date_str)
                skipped += 1
                continue

            # Filter to 9:00 AM – 4:30 PM ET (covers regular + brief AH overlap)
            df = df[(df.index.hour >= 9) & (df.index.hour <= 16)]
            if df.empty:
                skipped += 1
                continue

            df.to_parquet(out_path, engine="pyarrow", index=True)
            fetched += 1

            if fetched % 50 == 0:
                logger.info(
                    "  %s: %d/%d days — %d fetched, %d skipped, %d errors",
                    symbol, i, total, fetched, skipped, errors,
                )

        except Exception as exc:
            logger.error("Error fetching %s on %s: %s", symbol, date_str, exc)
            errors += 1
            time.sleep(2.0)

    logger.info(
        "%s complete: %d fetched, %d skipped, %d errors (total %d days)",
        symbol, fetched, skipped, errors, total,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Collect 1-min bars from Alpaca for one or more symbols."
    )
    parser.add_argument(
        "--symbols", nargs="+", required=True,
        help="Symbols to collect (space-separated), e.g. QQQ IWM"
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--output-dir", type=Path,
        help="Output raw data directory (default: trading-framework/data/raw)"
    )
    parser.add_argument("--env-file", type=Path, help="Path to .env with Alpaca credentials")
    args = parser.parse_args()

    api_key, api_secret, data_url = resolve_credentials(args.env_file)
    if not api_key or not api_secret:
        logger.error(
            "No Alpaca data credentials found. Set ALPACA_DATA_API_KEY and "
            "ALPACA_DATA_API_SECRET in .env or environment variables — these are "
            "the shared read-only data credentials, never used for order execution."
        )
        sys.exit(1)

    output_dir = args.output_dir or (FRAMEWORK_ROOT / "data" / "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", output_dir)

    from framework.data.alpaca_client import AlpacaClient
    client = AlpacaClient(api_key=api_key, api_secret=api_secret, base_url=data_url)

    symbols = [s.upper() for s in args.symbols]
    logger.info("Collecting %s from %s to %s", symbols, args.start, args.end)

    for symbol in symbols:
        logger.info("=== %s ===", symbol)
        collect_symbol(client, symbol, args.start, args.end, output_dir)

    logger.info("All done.")


if __name__ == "__main__":
    main()
