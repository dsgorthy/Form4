#!/usr/bin/env python3
"""Pull market cap data from SEC EDGAR + our price database.

Two-step approach:
  1. Map tickers → company CIKs via SEC's company_tickers.json
  2. For each CIK, fetch shares outstanding from EDGAR company facts API
  3. Multiply by latest available price from our price DB to get market cap

Covers delisted/acquired companies since their SEC filings persist.

Usage:
    python3 pipelines/insider_study/pull_market_caps_sec.py           # pull missing
    python3 pipelines/insider_study/pull_market_caps_sec.py --full    # re-pull all
    python3 pipelines/insider_study/pull_market_caps_sec.py --stats   # coverage stats
"""
from __future__ import annotations

import argparse
import json
import logging
import os
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
PRICE_DIR = Path(__file__).resolve().parent / "data" / "prices"
SEC_CACHE_PATH = Path(__file__).resolve().parent / "data" / "sec_shares_outstanding.json"

USER_AGENT = "Form4App derek@sidequestgroup.com"
SEC_RATE_LIMIT = 0.12  # SEC asks for max 10 req/sec, we'll do ~8


def load_mcaps() -> dict[str, int | float]:
    if MCAP_PATH.exists():
        return json.loads(MCAP_PATH.read_text())
    return {}


def save_mcaps(mcaps: dict) -> None:
    MCAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    MCAP_PATH.write_text(json.dumps(mcaps, indent=None, separators=(",", ":")))


def load_shares_cache() -> dict[str, dict]:
    """Cache of {ticker: {shares: int, date: str, cik: str}}"""
    if SEC_CACHE_PATH.exists():
        try:
            return json.loads(SEC_CACHE_PATH.read_text())
        except Exception:
            pass
    return {}


def save_shares_cache(cache: dict) -> None:
    SEC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SEC_CACHE_PATH.write_text(json.dumps(cache, indent=None, separators=(",", ":")))


# ---------------------------------------------------------------------------
# Step 1: Ticker → CIK mapping
# ---------------------------------------------------------------------------

def fetch_ticker_cik_map() -> dict[str, int]:
    """Fetch SEC's ticker→CIK mapping. Returns {TICKER: CIK}."""
    import requests
    resp = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # {ticker: cik} — take first match per ticker (most traded)
    mapping: dict[str, int] = {}
    for entry in data.values():
        ticker = entry["ticker"].upper()
        cik = int(entry["cik_str"])
        if ticker not in mapping:
            mapping[ticker] = cik

    logger.info("SEC ticker map: %d tickers", len(mapping))
    return mapping


# ---------------------------------------------------------------------------
# Step 2: Fetch shares outstanding from EDGAR company facts
# ---------------------------------------------------------------------------

def fetch_shares_outstanding(cik: int) -> tuple[int | None, str | None]:
    """Fetch latest shares outstanding from SEC EDGAR company facts.

    Returns (shares, end_date) or (None, None) on failure.
    """
    import requests

    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        if resp.status_code == 404:
            return None, None
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None, None

    # Try multiple fields — different companies use different ones
    facts = data.get("facts", {})
    for namespace in ["dei", "us-gaap"]:
        ns_facts = facts.get(namespace, {})
        for key in [
            "EntityCommonStockSharesOutstanding",
            "CommonStockSharesOutstanding",
            "SharesOutstanding",
            "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
            "CommonStockSharesIssued",
        ]:
            if key not in ns_facts:
                continue
            units = ns_facts[key].get("units", {}).get("shares", [])
            if not units:
                continue
            # Get most recent entry
            latest = max(units, key=lambda x: x.get("end", ""))
            shares = int(latest["val"])
            end_date = latest.get("end", "")
            if shares > 0:
                return shares, end_date

    return None, None


# ---------------------------------------------------------------------------
# Step 3: Get price from our price DB
# ---------------------------------------------------------------------------

def get_latest_price(ticker: str) -> float | None:
    """Get latest available price for a ticker from our price database."""
    price_file = PRICE_DIR / f"{ticker}.csv"
    if not price_file.exists():
        return None

    try:
        # Read last few lines to get latest price
        with open(price_file, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return None

        # CSV format: date,open,high,low,close,volume
        last_line = lines[-1].strip()
        if not last_line:
            last_line = lines[-2].strip()
        parts = last_line.split(",")
        close = float(parts[4])  # close price
        return close if close > 0 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main pull logic
# ---------------------------------------------------------------------------

def pull_market_caps(full: bool = False) -> dict:
    """Pull market caps via SEC EDGAR shares outstanding × price."""
    import requests

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    # Get all tickers with trade counts
    all_tickers = conn.execute("""
        SELECT ticker, COUNT(*) as cnt
        FROM trades WHERE ticker IS NOT NULL AND ticker != ''
        GROUP BY ticker ORDER BY cnt DESC
    """).fetchall()
    ticker_counts = {r["ticker"]: r["cnt"] for r in all_tickers}
    conn.close()

    mcaps = {} if full else load_mcaps()
    shares_cache = {} if full else load_shares_cache()

    # Step 1: Get ticker → CIK mapping
    logger.info("Fetching SEC ticker → CIK mapping...")
    ticker_cik = fetch_ticker_cik_map()

    # Determine which tickers need work
    to_pull = []
    no_cik = 0
    already_cached = 0
    for ticker, cnt in ticker_counts.items():
        if ticker in mcaps and not full:
            already_cached += 1
            continue
        if ticker not in ticker_cik:
            # Try common transformations
            alt = ticker.replace(".", "-")  # BRK.B → BRK-B
            if alt in ticker_cik:
                ticker_cik[ticker] = ticker_cik[alt]
            else:
                no_cik += 1
                continue
        to_pull.append((ticker, cnt, ticker_cik[ticker]))

    logger.info(
        "To pull: %d tickers (%d already cached, %d no CIK mapping)",
        len(to_pull), already_cached, no_cik,
    )

    if not to_pull:
        return {"pulled": 0, "success": 0, "total_coverage": len(mcaps)}

    success = 0
    failed = 0
    no_price = 0
    checkpoint_interval = 200

    for i, (ticker, cnt, cik) in enumerate(to_pull):
        # Check shares cache first
        if ticker in shares_cache and not full:
            shares = shares_cache[ticker].get("shares")
            if shares:
                price = get_latest_price(ticker)
                if price:
                    mc = int(shares * price)
                    mcaps[ticker] = mc
                    success += 1
                    continue

        # Fetch from SEC
        shares, end_date = fetch_shares_outstanding(cik)
        time.sleep(SEC_RATE_LIMIT)

        if shares is None:
            failed += 1
            if (i + 1) % 500 == 0:
                logger.info("[%d/%d] %s: no shares data (CIK=%d)", i + 1, len(to_pull), ticker, cik)
            continue

        # Cache shares
        shares_cache[ticker] = {"shares": shares, "date": end_date, "cik": str(cik)}

        # Get price
        price = get_latest_price(ticker)

        # If no local price, try yfinance as fallback
        if price is None:
            try:
                import yfinance as yf
                t = yf.Ticker(ticker)
                hist = t.history(period="5d")
                if len(hist) > 0:
                    price = float(hist["Close"].iloc[-1])
            except Exception:
                pass

        # If still no price, try Alpaca
        if price is None:
            try:
                api_key = os.getenv("ALPACA_API_KEY", "")
                api_secret = os.getenv("ALPACA_API_SECRET", "")
                if api_key:
                    resp = requests.get(
                        f"https://data.alpaca.markets/v2/stocks/{ticker}/trades/latest",
                        headers={
                            "APCA-API-KEY-ID": api_key,
                            "APCA-API-SECRET-KEY": api_secret,
                        },
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        price = resp.json().get("trade", {}).get("p")
            except Exception:
                pass

        if price is None or price <= 0:
            no_price += 1
            if (i + 1) % 500 == 0:
                logger.info("[%d/%d] %s: have shares (%s) but no price", i + 1, len(to_pull), ticker, f"{shares:,}")
            continue

        mc = int(shares * price)
        mcaps[ticker] = mc
        success += 1

        tier = (
            "mega" if mc >= 1e12 else
            "large" if mc >= 1e11 else
            "mid" if mc >= 1e10 else
            "small" if mc >= 2e9 else
            "micro"
        )

        if (i + 1) % 100 == 0 or mc >= 1e11:
            mc_str = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
            logger.info("[%d/%d] %s: %s (%s) — %d trades",
                        i + 1, len(to_pull), ticker, mc_str, tier, cnt)

        # Checkpoint
        if (i + 1) % checkpoint_interval == 0:
            save_mcaps(mcaps)
            save_shares_cache(shares_cache)
            logger.info("  checkpoint: %d success, %d failed, %d no price, %d total",
                        success, failed, no_price, len(mcaps))

    # Final save
    save_mcaps(mcaps)
    save_shares_cache(shares_cache)

    logger.info(
        "Done: %d attempted, %d success, %d failed (no shares), %d no price. Coverage: %d tickers.",
        len(to_pull), success, failed, no_price, len(mcaps),
    )

    return {
        "attempted": len(to_pull),
        "success": success,
        "failed_no_shares": failed,
        "failed_no_price": no_price,
        "no_cik_mapping": no_cik,
        "total_coverage": len(mcaps),
    }


def show_stats() -> None:
    """Show coverage stats."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    all_tickers = conn.execute("""
        SELECT ticker, COUNT(*) as cnt FROM trades
        WHERE ticker IS NOT NULL GROUP BY ticker ORDER BY cnt DESC
    """).fetchall()
    conn.close()

    mcaps = load_mcaps()
    total_tickers = len(all_tickers)
    total_trades = sum(r["cnt"] for r in all_tickers)
    covered = sum(1 for r in all_tickers if r["ticker"] in mcaps)
    covered_trades = sum(r["cnt"] for r in all_tickers if r["ticker"] in mcaps)

    print(f"Total tickers in DB:   {total_tickers:,}")
    print(f"  With market cap:     {covered:,} ({covered/total_tickers*100:.1f}%)")
    print(f"  Missing:             {total_tickers - covered:,}")
    print()
    print(f"Trade-weighted:        {covered_trades:,}/{total_trades:,} ({covered_trades/total_trades*100:.1f}%)")
    print()

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

    print("Tiers:")
    for tier, count in tiers.items():
        print(f"  {tier:8s}: {count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull market caps from SEC EDGAR")
    parser.add_argument("--full", action="store_true", help="Re-pull all (ignore cache)")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    args = parser.parse_args()

    if args.stats:
        show_stats()
    else:
        result = pull_market_caps(full=args.full)
        print(json.dumps(result, indent=2))
        print()
        show_stats()


if __name__ == "__main__":
    main()
