#!/usr/bin/env python3
"""Pull 1-minute equity bars from Alpaca and write per-day Parquet files.

Generalizes the proven pattern in `pull_1sec_bars.py`:
  * One file per (symbol, trading_day) under storage_paths.equity_1min
  * Resume via disk-marker (skip days already on disk)
  * Built on the resumable_puller framework

Designed to land into:
  data/raw/equity/1min/{SYMBOL}/{YYYY-MM-DD}.parquet  — today (Mini, repo-relative)
  /Volumes/data/form4/equity/1min/{SYMBOL}/{YYYY-MM-DD}.parquet  — post-array

Set FORM4_DATA_ROOT in env to override the root.

Usage:
    # Smoke test — 5 symbols × 3 days
    python3 pipelines/pull_1min_bars.py --symbols SPY QQQ IWM AAPL MSFT --days 3

    # Full universe from a precomputed list
    python3 pipelines/pull_1min_bars.py \\
        --symbol-file data/manifest/universe_top_liquid_1000.txt \\
        --start 2016-01-01 --end 2026-04-30
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from config.storage_paths import paths
from pipelines._lib.resumable_puller import ResumablePuller, make_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

ALPACA_BASE = "https://data.alpaca.markets/v2"
DEFAULT_FEED = "sip"

BAR_SCHEMA = pa.schema([
    ("ts",          pa.timestamp("ns", tz="UTC")),
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
        raise RuntimeError(
            "ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET missing from env. "
            "Add to .env (shared read-only credentials)."
        )
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}


def trading_days(start: date, end: date) -> list[date]:
    """Inclusive list of US weekdays. Doesn't filter holidays — Alpaca returns
    empty for those and the puller marks them done with 0 rows."""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def output_path(symbol: str, day: date) -> Path:
    return paths.equity_1min / symbol / f"{day.isoformat()}.parquet"


class OneMinBarPuller(ResumablePuller):
    """One pull-item = one (symbol, day) pair."""
    dataset = "equity_1min"
    description = "Phase 1 #1 — 1-min OHLCV bars (Alpaca SIP feed)"
    storage_root = paths.equity_1min

    def __init__(self, items: list[dict], *, feed: str = DEFAULT_FEED, **kwargs):
        super().__init__(items, session_headers=alpaca_headers(), **kwargs)
        self.feed = feed

    def item_key(self, item: dict) -> str:
        return f"{item['symbol']}|{item['day'].isoformat()}"

    def fetch_item(self, session: requests.Session, item: dict) -> Optional[list[dict]]:
        symbol = item["symbol"]
        day: date = item["day"]
        start = f"{day.isoformat()}T00:00:00Z"
        end_dt = day + timedelta(days=1)
        end = f"{end_dt.isoformat()}T00:00:00Z"

        all_bars: list[dict] = []
        page_token = None
        pages = 0
        while True:
            params = {
                "timeframe": "1Min",
                "start": start,
                "end": end,
                "limit": 10000,
                "feed": self.feed,
                "adjustment": "raw",
            }
            if page_token:
                params["page_token"] = page_token
            r = session.get(f"{ALPACA_BASE}/stocks/{symbol}/bars", params=params, timeout=30)
            if r.status_code == 422:
                logger.warning("422 on %s %s — symbol probably not in feed", symbol, day)
                return None
            r.raise_for_status()
            payload = r.json()
            bars = payload.get("bars") or []
            all_bars.extend(bars)
            page_token = payload.get("next_page_token")
            pages += 1
            if not page_token:
                break
            if pages > 50:
                logger.warning("Pagination cap hit on %s %s", symbol, day)
                break
        return all_bars or None

    def write_item(self, item: dict, fetched: list[dict]) -> tuple[int, int]:
        if not fetched:
            return 0, 0
        outfile = output_path(item["symbol"], item["day"])
        outfile.parent.mkdir(parents=True, exist_ok=True)

        # Alpaca returns ts as ISO-8601 strings — parse to pandas datetime then to arrow.
        import pandas as pd
        df = pd.DataFrame(fetched)
        df["ts"] = pd.to_datetime(df["t"], utc=True)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                                "c": "close", "v": "volume", "n": "trade_count", "vw": "vwap"})
        df = df[["ts", "open", "high", "low", "close", "volume", "trade_count", "vwap"]]
        df = df.astype({"open": "float64", "high": "float64", "low": "float64",
                        "close": "float64", "volume": "int64", "trade_count": "int64",
                        "vwap": "float64"})

        table = pa.Table.from_pandas(df, schema=BAR_SCHEMA, preserve_index=False)
        tmp = outfile.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        os.replace(tmp, outfile)
        return len(fetched), outfile.stat().st_size


def load_symbols(symbol_file: Optional[str], symbols: Optional[list[str]]) -> list[str]:
    if symbol_file:
        path = Path(symbol_file)
        out = []
        for line in path.read_text().splitlines():
            sym = line.strip().split("|")[0]
            if sym and not sym.startswith("#"):
                out.append(sym)
        return out
    if symbols:
        return symbols
    return ["SPY", "QQQ", "IWM"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", help="Tickers to pull (overrides --symbol-file)")
    p.add_argument("--symbol-file", help="One ticker per line (also accepts pipe-delimited from psql)")
    p.add_argument("--days", type=int, help="Last N trading days; mutually exclusive with --start/--end")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD (inclusive)")
    p.add_argument("--rate-limit", type=float, default=8,
                   help="Max requests/sec (Alpaca SIP allows 200 rpm = ~3/s; ramp slowly)")
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--feed", default=DEFAULT_FEED, choices=["sip", "iex", "otc"])
    args = p.parse_args()

    symbols = load_symbols(args.symbol_file, args.symbols)

    if args.days:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days * 2)  # generous buffer for weekends
        days = [d for d in trading_days(start, end)][-args.days:]
    elif args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()
        days = trading_days(start, end)
    else:
        # default: last 5 trading days
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=14)
        days = trading_days(start, end)[-5:]

    items = [{"symbol": s, "day": d} for s in symbols for d in days]
    logger.info("1-min bar pull: %d symbols × %d days = %d items, feed=%s",
                len(symbols), len(days), len(items), args.feed)
    logger.info("Output root: %s", paths.equity_1min)

    OneMinBarPuller(
        items,
        feed=args.feed,
        rate_limit_per_sec=args.rate_limit,
        progress_every_n=args.progress_every,
        manifest_every_n=max(args.progress_every * 4, 200),
        completion_strategy="disk",
        disk_marker=lambda it: output_path(it["symbol"], it["day"]),
    ).run()


if __name__ == "__main__":
    main()
