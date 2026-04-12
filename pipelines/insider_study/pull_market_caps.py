#!/usr/bin/env python3
"""Pull market cap data for all tickers in the insider trades database.

Uses yfinance to fetch current market cap. Saves to data/market_caps.json.
Supports incremental pulls — only fetches tickers not already in the cache.

Usage:
    python3 pipelines/insider_study/pull_market_caps.py             # pull missing only
    python3 pipelines/insider_study/pull_market_caps.py --full      # re-pull everything
    python3 pipelines/insider_study/pull_market_caps.py --stats     # show coverage
    python3 pipelines/insider_study/pull_market_caps.py --top 50    # pull top 50 by trade count
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
MCAP_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "market_caps.json"

# Tickers that we know won't resolve (OTC, foreign, etc.)
# Populated as we discover failures to avoid re-trying
FAILURES_PATH = MCAP_PATH.parent / "market_caps_failures.json"


def load_mcaps() -> dict[str, int | float]:
    if MCAP_PATH.exists():
        return json.loads(MCAP_PATH.read_text())
    return {}


def save_mcaps(mcaps: dict) -> None:
    MCAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    MCAP_PATH.write_text(json.dumps(mcaps, indent=None, separators=(",", ":")))


def load_failures() -> set[str]:
    if FAILURES_PATH.exists():
        return set(json.loads(FAILURES_PATH.read_text()))
    return set()


def save_failures(failures: set[str]) -> None:
    FAILURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAILURES_PATH.write_text(json.dumps(sorted(failures)))


def get_all_tickers(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Get all unique tickers with trade count, ordered by most traded."""
    rows = conn.execute("""
        SELECT ticker, COUNT(*) as cnt
        FROM trades
        WHERE ticker IS NOT NULL AND ticker != ''
        GROUP BY ticker
        ORDER BY cnt DESC
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def fetch_market_cap_yfinance(ticker: str) -> int | None:
    """Fetch market cap from yfinance. Returns None on failure."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.fast_info
        mc = getattr(info, "market_cap", None)
        if mc and mc > 0:
            return int(mc)
    except Exception:
        pass
    return None


def fetch_market_cap_alpaca(ticker: str) -> int | None:
    """Fetch market cap from Alpaca snapshot as fallback."""
    import os
    try:
        import requests
        api_key = os.getenv("ALPACA_DATA_API_KEY", "")
        api_secret = os.getenv("ALPACA_DATA_API_SECRET", "")
        if not api_key:
            return None
        resp = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{ticker}/snapshot",
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            # Alpaca snapshots don't include market cap directly
            # but we can use latest trade price * shares outstanding from another source
            pass
    except Exception:
        pass
    return None


def pull_market_caps(
    full: bool = False,
    top_n: int | None = None,
) -> dict:
    """Pull market caps for tickers, returns stats."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    all_tickers = get_all_tickers(conn)
    conn.close()

    mcaps = {} if full else load_mcaps()
    failures = set() if full else load_failures()

    # Determine which tickers to pull
    to_pull = []
    for ticker, cnt in all_tickers:
        if ticker in mcaps:
            continue
        if ticker in failures:
            continue
        to_pull.append((ticker, cnt))

    if top_n:
        to_pull = to_pull[:top_n]

    total = len(to_pull)
    if total == 0:
        logger.info("All tickers already have market cap data (or are known failures)")
        return {"pulled": 0, "success": 0, "failed": 0, "total_coverage": len(mcaps)}

    logger.info("Pulling market cap for %d tickers (%d already cached, %d known failures)",
                total, len(mcaps), len(failures))

    success = 0
    failed = 0
    batch_save_interval = 100

    for i, (ticker, cnt) in enumerate(to_pull):
        mc = fetch_market_cap_yfinance(ticker)

        if mc is not None:
            mcaps[ticker] = mc
            success += 1
            if mc >= 1e12:
                tier = "mega"
            elif mc >= 1e11:
                tier = "large"
            elif mc >= 1e10:
                tier = "mid"
            elif mc >= 2e9:
                tier = "small"
            else:
                tier = "micro"
            logger.info("[%d/%d] %s: $%s (%s) — %d trades",
                        i + 1, total, ticker,
                        f"{mc/1e9:.1f}B" if mc >= 1e9 else f"{mc/1e6:.0f}M",
                        tier, cnt)
        else:
            failures.add(ticker)
            failed += 1
            if (i + 1) % 50 == 0:
                logger.info("[%d/%d] %s: FAILED — %d trades", i + 1, total, ticker, cnt)

        # Save periodically
        if (i + 1) % batch_save_interval == 0:
            save_mcaps(mcaps)
            save_failures(failures)
            logger.info("  checkpoint: %d success, %d failed, %d total coverage",
                        success, failed, len(mcaps))

        # Rate limit — yfinance can get throttled
        time.sleep(0.3)

    # Final save
    save_mcaps(mcaps)
    save_failures(failures)

    logger.info(
        "Done: %d pulled (%d success, %d failed). Total coverage: %d tickers.",
        total, success, failed, len(mcaps),
    )
    return {
        "pulled": total,
        "success": success,
        "failed": failed,
        "total_coverage": len(mcaps),
    }


def show_stats() -> None:
    """Show market cap coverage stats."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    all_tickers = get_all_tickers(conn)
    conn.close()

    mcaps = load_mcaps()
    failures = load_failures()

    total_tickers = len(all_tickers)
    covered = sum(1 for t, _ in all_tickers if t in mcaps)
    failed = sum(1 for t, _ in all_tickers if t in failures)
    missing = total_tickers - covered - failed

    # Trade-weighted coverage
    total_trades = sum(c for _, c in all_tickers)
    covered_trades = sum(c for t, c in all_tickers if t in mcaps)

    print(f"Total tickers in DB:   {total_tickers:,}")
    print(f"  With market cap:     {covered:,} ({covered/total_tickers*100:.1f}%)")
    print(f"  Known failures:      {failed:,} ({failed/total_tickers*100:.1f}%)")
    print(f"  Missing:             {missing:,} ({missing/total_tickers*100:.1f}%)")
    print()
    print(f"Trade-weighted coverage: {covered_trades:,}/{total_trades:,} ({covered_trades/total_trades*100:.1f}%)")
    print()

    # Tier distribution
    tiers = {"mega": 0, "large": 0, "mid": 0, "small": 0, "micro": 0}
    for mc in mcaps.values():
        if mc >= 1e12:
            tiers["mega"] += 1
        elif mc >= 1e11:
            tiers["large"] += 1
        elif mc >= 1e10:
            tiers["mid"] += 1
        elif mc >= 2e9:
            tiers["small"] += 1
        else:
            tiers["micro"] += 1

    print("Market cap tiers:")
    for tier, count in tiers.items():
        print(f"  {tier:8s}: {count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull market cap data for insider tickers")
    parser.add_argument("--full", action="store_true", help="Re-pull all tickers (ignore cache)")
    parser.add_argument("--top", type=int, help="Only pull top N tickers by trade count")
    parser.add_argument("--stats", action="store_true", help="Show coverage stats and exit")
    args = parser.parse_args()

    if args.stats:
        show_stats()
    else:
        result = pull_market_caps(full=args.full, top_n=args.top)
        print(json.dumps(result, indent=2))
        print()
        show_stats()


if __name__ == "__main__":
    main()
