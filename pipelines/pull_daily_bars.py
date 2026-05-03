#!/usr/bin/env python3
"""Pull daily equity bars from Alpaca. One Parquet per symbol covering the
requested date range — daily volumes are small enough that per-symbol files
(not per-day) are the right granularity.

Usage:
    # VIXY 2024-01-01 → today (closes the VIX regime-filter gap)
    python3 pipelines/pull_daily_bars.py --symbols VIXY --start 2024-01-01

    # Bulk universe from a file
    python3 pipelines/pull_daily_bars.py --symbol-file data/manifest/universe.txt \\
        --start 2016-01-01 --end 2026-04-30

Output:
    {paths.equity_daily}/{SYMBOL}.parquet  with columns
    (date, open, high, low, close, volume, trade_count, vwap)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from config.storage_paths import paths
from pipelines._lib.resumable_puller import ResumablePuller

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

ALPACA_BASE = "https://data.alpaca.markets/v2"
DEFAULT_FEED = "sip"

DAILY_SCHEMA = pa.schema([
    ("date",        pa.string()),       # YYYY-MM-DD
    ("open",        pa.float64()),
    ("high",        pa.float64()),
    ("low",         pa.float64()),
    ("close",       pa.float64()),
    ("volume",      pa.int64()),
    ("trade_count", pa.int64()),
    ("vwap",        pa.float64()),
])


def alpaca_headers() -> dict:
    key = os.getenv("ALPACA_DATA_API_KEY")
    sec = os.getenv("ALPACA_DATA_API_SECRET")
    if not key or not sec:
        raise RuntimeError("ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET missing from .env")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}


def output_path(symbol: str) -> Path:
    return paths.equity_daily / f"{symbol}.parquet"


class DailyBarPuller(ResumablePuller):
    dataset = "equity_daily"
    description = "Daily OHLCV bars (Alpaca SIP)"
    storage_root = paths.equity_daily

    def __init__(self, items, *, start: str, end: str, feed: str = DEFAULT_FEED, **kwargs):
        super().__init__(items, session_headers=alpaca_headers(), **kwargs)
        self.start = start
        self.end = end
        self.feed = feed

    def item_key(self, item: dict) -> str:
        return f"{item['symbol']}|{self.start}|{self.end}"

    def fetch_item(self, session: requests.Session, item: dict) -> Optional[list[dict]]:
        symbol = item["symbol"]
        start = f"{self.start}T00:00:00Z"
        end_dt = datetime.strptime(self.end, "%Y-%m-%d") + timedelta(days=1)
        end = f"{end_dt.strftime('%Y-%m-%d')}T00:00:00Z"

        all_bars: list[dict] = []
        page_token = None
        while True:
            params = {"timeframe": "1Day", "start": start, "end": end,
                      "limit": 10000, "feed": self.feed, "adjustment": "raw"}
            if page_token:
                params["page_token"] = page_token
            r = session.get(f"{ALPACA_BASE}/stocks/{symbol}/bars", params=params, timeout=30)
            if r.status_code == 422:
                logger.warning("422 on %s — symbol probably not in feed", symbol)
                return None
            r.raise_for_status()
            payload = r.json()
            bars = payload.get("bars") or []
            all_bars.extend(bars)
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return all_bars or None

    def write_item(self, item: dict, fetched: list[dict]) -> tuple[int, int]:
        symbol = item["symbol"]
        outfile = output_path(symbol)
        outfile.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(fetched)
        df["date"] = pd.to_datetime(df["t"], utc=True).dt.strftime("%Y-%m-%d")
        df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                                "c": "close", "v": "volume", "n": "trade_count", "vw": "vwap"})
        df = df[["date", "open", "high", "low", "close", "volume", "trade_count", "vwap"]]
        df = df.astype({"open": "float64", "high": "float64", "low": "float64",
                        "close": "float64", "volume": "int64", "trade_count": "int64",
                        "vwap": "float64"})

        table = pa.Table.from_pandas(df, schema=DAILY_SCHEMA, preserve_index=False)
        tmp = outfile.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        os.replace(tmp, outfile)
        return len(fetched), outfile.stat().st_size


def load_symbols(symbol_file: Optional[str], symbols: Optional[list[str]]) -> list[str]:
    if symbol_file:
        out = []
        for line in Path(symbol_file).read_text().splitlines():
            sym = line.strip().split("|")[0]
            if sym and not sym.startswith("#"):
                out.append(sym)
        return out
    if symbols:
        return symbols
    raise SystemExit("supply --symbols or --symbol-file")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+")
    p.add_argument("--symbol-file")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD (default: yesterday)")
    p.add_argument("--rate-limit", type=float, default=4)
    p.add_argument("--feed", default=DEFAULT_FEED)
    args = p.parse_args()

    end = args.end or (date.today() - timedelta(days=1)).isoformat()
    symbols = load_symbols(args.symbol_file, args.symbols)
    items = [{"symbol": s} for s in symbols]
    logger.info("daily-bar pull: %d symbols, %s → %s", len(symbols), args.start, end)
    logger.info("output root: %s", paths.equity_daily)

    DailyBarPuller(
        items,
        start=args.start,
        end=end,
        feed=args.feed,
        rate_limit_per_sec=args.rate_limit,
        progress_every_n=10,
        manifest_every_n=50,
        completion_strategy="disk",
        disk_marker=lambda it: output_path(it["symbol"]),
    ).run()


if __name__ == "__main__":
    main()
