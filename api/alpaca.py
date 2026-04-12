"""
Singleton Alpaca client for the API process.

Provides get_daily_ohlc(ticker) with daily lru_cache so Alpaca is called
at most once per ticker per day.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# Load .env from project root
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if _ENV_PATH.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV_PATH)
    except ImportError:
        # Manual fallback: parse KEY=VALUE lines
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

# Lazy singleton
_client = None


def _get_client():
    global _client
    if _client is None:
        from framework.data.alpaca_client import AlpacaClient

        api_key = os.environ.get("ALPACA_DATA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_DATA_API_SECRET", "")
        if not api_key or not api_secret:
            raise RuntimeError("ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET not set — see .env for the read-only credential convention")
        _client = AlpacaClient(api_key=api_key, api_secret=api_secret)
    return _client


@lru_cache(maxsize=256)
def get_daily_ohlc(
    ticker: str,
    cache_date: date,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[dict]:
    """
    Fetch daily OHLC bars from Alpaca for a ticker.

    Returns list of {time, open, high, low, close, volume} dicts
    in the format expected by lightweight-charts.

    cache_date param ensures the cache invalidates daily.
    """
    if end is None:
        end = cache_date.isoformat()
    if start is None:
        start = "2016-01-01"

    try:
        client = _get_client()
        bars = client.get_bars(ticker, start, end, timeframe="1Day", adjustment="split")
    except Exception as e:
        logger.warning("Alpaca fetch failed for %s: %s", ticker, e)
        return []

    # Convert to lightweight-charts format
    candles = []
    for bar in bars:
        # bar["timestamp"] is like "2024-03-15 09:30:00-04:00"
        ts = bar["timestamp"]
        day = ts[:10]  # extract YYYY-MM-DD
        candles.append({
            "time": day,
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
        })

    return candles
