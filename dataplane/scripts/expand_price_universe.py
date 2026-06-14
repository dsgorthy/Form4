"""Expand the price.daily.close universe via batched Alpaca calls.

Step 1: Pull the insider-active universe from form4 (tickers with at
least N open-market purchases [trans_code='P'] in the last M days).

Step 2: Batch-fetch daily bars from Alpaca (up to 100 symbols per call,
many days per call). Massive speedup vs the per-ticker compute path.

Step 3: Write SignalObservation rows directly via the catalog upsert,
mirroring what price.daily.close.v1.compute() emits. The signal class
isn't called — this script bypasses the per-ticker path for bulk seed.

Idempotent: same (signal_id, ticker, as_of_date) primary key, ON CONFLICT
DO UPDATE — re-running overwrites.

Usage:
    python scripts/expand_price_universe.py \\
        --from 2025-12-15 --to 2026-06-13 \\
        --min-trades 3 --max-tickers 300

Run on Studio (form4 + pyrrho_data_dev only live there).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from uuid import uuid4

import psycopg2
import requests

# Make `dataplane.*` importable when this is run as a script from the
# dataplane/ folder.
_HERE = Path(__file__).resolve()
_REPO = _HERE.parents[2]  # …/trading-framework
sys.path.insert(0, str(_REPO / "dataplane"))

from dataplane.catalog import register, write_observation  # noqa: E402
from dataplane.observation import SignalObservation  # noqa: E402
from signals.price.daily_close_v1 import PricesDailyCloseV1 as PriceDailyCloseV1  # noqa: E402


ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
BATCH_SIZE = 100  # Alpaca max symbols per call


def _active_universe(form4_dsn: str, min_trades: int, days_back: int, max_tickers: int) -> List[str]:
    """Tickers with at least min_trades P-trans-code insider buys in the
    last days_back days. Newest-active first.

    Filters to plausibly Alpaca-tradeable symbols: <= 5 alpha chars,
    no warrants/preferred share suffixes that the bars endpoint chokes on.
    """
    with psycopg2.connect(form4_dsn) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT ticker, COUNT(*) AS n
                  FROM trades
                 WHERE trans_code = 'P'
                   AND filing_date >= (CURRENT_DATE - %s::int)::text
                   AND COALESCE(is_duplicate, 0) = 0
                   AND ticker IS NOT NULL AND ticker != ''
                 GROUP BY ticker
                HAVING COUNT(*) >= %s
                 ORDER BY 2 DESC
                """,
                (days_back, min_trades),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    out: List[str] = []
    # Tickers that confuse Alpaca's symbol parser (NONE → null) or are
    # otherwise unreliable. Add to taste.
    _SKIP = {"NONE", "NULL", "NAN"}
    for ticker, _ in rows:
        t = (ticker or "").strip().upper()
        # Skip warrants / units / preferred / multi-class with suffix bias
        # toward common stock. BRK.B etc. pass via the dot allowance.
        if not t or len(t) > 5:
            continue
        if any(c in t for c in ("$", "/")) or t.endswith("W"):
            continue
        if t in _SKIP:
            continue
        out.append(t)
        if len(out) >= max_tickers:
            break
    return out


def _alpaca_headers() -> Dict[str, str]:
    # Same convention as price.daily.close.v1 — deliberately uses the
    # quality_momentum credentials since ALPACA_DATA_API_KEY is revoked.
    key = os.environ.get("ALPACA_API_KEY_QUALITY_MOMENTUM")
    secret = os.environ.get("ALPACA_API_SECRET_QUALITY_MOMENTUM")
    if not (key and secret):
        raise SystemExit(
            "ALPACA_API_KEY_QUALITY_MOMENTUM / ALPACA_API_SECRET_QUALITY_MOMENTUM not set"
        )
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def _fetch_batch(symbols: List[str], start: str, end: str) -> Dict[str, list]:
    """Pull all bars for `symbols` over [start, end], paginating through
    Alpaca's `next_page_token`. Returns dict: symbol → [bar, ...]."""
    aggregated: Dict[str, list] = {}
    next_token: str = ""
    while True:
        params = {
            "symbols": ",".join(symbols),
            "timeframe": "1Day",
            "start": start,
            "end": end,
            "feed": "iex",
            "adjustment": "raw",
            "limit": 10000,
        }
        if next_token:
            params["page_token"] = next_token
        resp = requests.get(
            ALPACA_BARS_URL, params=params, headers=_alpaca_headers(), timeout=60
        )
        resp.raise_for_status()
        data = resp.json()
        bars_by_symbol = data.get("bars") or {}
        for sym, bars in bars_by_symbol.items():
            aggregated.setdefault(sym, []).extend(bars or [])
        next_token = data.get("next_page_token") or ""
        if not next_token:
            break
    return aggregated


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", required=True)
    p.add_argument("--to", dest="to_date", required=True)
    p.add_argument("--min-trades", type=int, default=3)
    p.add_argument("--days-back", type=int, default=90)
    p.add_argument("--max-tickers", type=int, default=300)
    p.add_argument("--dataplane-dsn", default=os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"))
    p.add_argument("--form4-dsn", default="dbname=form4 host=localhost")
    args = p.parse_args()

    tickers = _active_universe(
        args.form4_dsn, args.min_trades, args.days_back, args.max_tickers,
    )
    print(f"active universe: {len(tickers)} tickers (min P-trades={args.min_trades}, last {args.days_back}d)")
    print(f"  first 20: {tickers[:20]}")

    # Register the signal once so signal_definitions has the row.
    with psycopg2.connect(args.dataplane_dsn) as conn:
        register(conn, PriceDailyCloseV1)
        conn.commit()

    total_rows = 0
    started = time.time()

    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start:batch_start + BATCH_SIZE]
        print(f"batch {batch_start // BATCH_SIZE + 1}: {len(batch)} symbols …", end="", flush=True)
        try:
            bars_by_symbol = _fetch_batch(batch, args.from_date, args.to_date)
        except requests.exceptions.HTTPError as exc:
            # Bad batch — usually one bad symbol poisoning the whole call.
            # Bisect: split and retry, drop any sub-batch that still 400s.
            print(f" 400 on whole batch — bisecting")
            bars_by_symbol = {}
            stack: list = [batch]
            while stack:
                sub = stack.pop()
                if len(sub) == 1:
                    continue  # drop singletons that 400
                try:
                    bars_by_symbol.update(_fetch_batch(sub, args.from_date, args.to_date))
                except requests.exceptions.HTTPError:
                    mid = len(sub) // 2
                    stack.append(sub[:mid])
                    stack.append(sub[mid:])
        except Exception as exc:
            print(f" FAILED: {exc}")
            continue

        rows_this_batch = 0
        with psycopg2.connect(args.dataplane_dsn) as conn:
            for symbol, bars in bars_by_symbol.items():
                for bar in bars or []:
                    # bar = {"t": "2024-01-02T05:00:00Z", "o": ..., "h": ..., "l": ..., "c": ..., "v": ..., "n": ..., "vw": ...}
                    try:
                        ts = datetime.fromisoformat(bar["t"].replace("Z", "+00:00"))
                    except Exception:
                        continue
                    # Mirror price.daily.close.v1's observation shape.
                    obs = SignalObservation(
                        signal_id=f"{PriceDailyCloseV1.signal_id}.{PriceDailyCloseV1.version}",
                        ticker=symbol,
                        as_of_date=ts.replace(hour=21, minute=0, second=0, microsecond=0),
                        value={
                            "open":   bar.get("o"),
                            "high":   bar.get("h"),
                            "low":    bar.get("l"),
                            "close":  bar.get("c"),
                            "volume": bar.get("v"),
                            "vwap":   bar.get("vw"),
                            "trades": bar.get("n"),
                            "feed":   "iex",
                            "source": "expand_price_universe",
                            "status": "ok",
                        },
                        source_run_id=uuid4(),
                        metadata={"loader": "expand_price_universe"},
                    )
                    try:
                        write_observation(conn, obs)
                        rows_this_batch += 1
                    except Exception:
                        continue
        total_rows += rows_this_batch
        print(f" wrote {rows_this_batch} rows ({len(bars_by_symbol)} symbols had data)")

    elapsed = int(time.time() - started)
    print(f"\nDONE. {total_rows:,} rows in {elapsed}s across {len(tickers)} tickers")


if __name__ == "__main__":
    main()
