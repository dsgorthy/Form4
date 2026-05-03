#!/usr/bin/env python3
"""Pull 1-second bars for index ETFs from Alpaca trades endpoint.

Aggregates individual trades into 1-second OHLCV bars. Resume-safe —
skips days that already have CSV files on disk.

Usage:
    python3 pipelines/pull_1sec_bars.py
    python3 pipelines/pull_1sec_bars.py --days 30 --symbols SPY IWM
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "1sec"
BASE = "https://data.alpaca.markets/v2"
DEFAULT_SYMBOLS = ["SPY", "QQQ", "IWM", "DIA"]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "APCA-API-KEY-ID": os.getenv("ALPACA_DATA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_DATA_API_SECRET", ""),
    })
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=1))
    return s


def trading_days(n: int) -> list[date]:
    days = []
    d = date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days)


def pull_trades_for_day(session: requests.Session, symbol: str, day: date) -> list[dict]:
    start = f"{day}T09:30:00Z"
    end = f"{day}T16:00:00Z"
    all_trades = []
    page_token = None
    pages = 0

    while True:
        params = {"start": start, "end": end, "limit": 10000, "feed": "sip"}
        if page_token:
            params["page_token"] = page_token

        try:
            resp = session.get(f"{BASE}/stocks/{symbol}/trades", params=params, timeout=30)
        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection error on %s %s (page %d), retrying in 5s: %s", symbol, day, pages, e)
            time.sleep(5)
            session.close()
            session = make_session()
            continue

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            logger.warning("Rate limited, waiting %ds", wait)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            logger.error("HTTP %d on %s %s: %s", resp.status_code, symbol, day, resp.text[:200])
            break

        data = resp.json()
        trades = data.get("trades") or []
        all_trades.extend(trades)
        pages += 1

        page_token = data.get("next_page_token")
        if not page_token or not trades:
            break

        if pages % 50 == 0:
            time.sleep(0.5)

    return all_trades


def aggregate_to_1sec(trades: list[dict]) -> list[tuple[str, dict]]:
    bars: dict[str, dict] = {}
    for t in trades:
        ts = t["t"][:19]
        price = float(t["p"])
        size = int(t["s"])

        if ts not in bars:
            bars[ts] = {"open": price, "high": price, "low": price, "close": price, "volume": 0, "trades": 0}

        b = bars[ts]
        b["high"] = max(b["high"], price)
        b["low"] = min(b["low"], price)
        b["close"] = price
        b["volume"] += size
        b["trades"] += 1

    return sorted(bars.items())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    days = trading_days(args.days)
    session = make_session()

    logger.info("Pulling 1-sec bars for %s over %d days (%s to %s)", args.symbols, len(days), days[0], days[-1])

    for symbol in args.symbols:
        symbol_dir = DATA_DIR / symbol
        symbol_dir.mkdir(exist_ok=True)

        total_bars = 0
        skipped = 0

        for i, day in enumerate(days):
            outfile = symbol_dir / f"{day}.csv"
            if outfile.exists() and outfile.stat().st_size > 100:
                skipped += 1
                continue

            trades = pull_trades_for_day(session, symbol, day)
            if not trades:
                logger.info("  %s [%d/%d] %s — no trades (holiday?)", symbol, i + 1, len(days), day)
                continue

            bars = aggregate_to_1sec(trades)

            with open(outfile, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["timestamp", "open", "high", "low", "close", "volume", "trades"])
                for ts, b in bars:
                    w.writerow([ts, f"{b['open']:.4f}", f"{b['high']:.4f}", f"{b['low']:.4f}",
                               f"{b['close']:.4f}", b["volume"], b["trades"]])

            total_bars += len(bars)

            if (i + 1) % 5 == 0 or i == 0:
                logger.info("  %s [%d/%d] %s — %s trades → %s bars",
                           symbol, i + 1, len(days), day, f"{len(trades):,}", f"{len(bars):,}")

            time.sleep(0.5)

        size_mb = sum(f.stat().st_size for f in symbol_dir.glob("*.csv")) / 1024 / 1024
        logger.info("  %s DONE: %s new bars, %d skipped, %.1f MB on disk\n",
                    symbol, f"{total_bars:,}", skipped, size_mb)

    total_size = sum(f.stat().st_size for f in DATA_DIR.rglob("*.csv")) / 1024 / 1024
    logger.info("Total: %.1f MB across %d symbols", total_size, len(args.symbols))


if __name__ == "__main__":
    main()
