"""Shared price loading and computation utilities for insider study pipelines."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

PRICES_DIR = Path(__file__).resolve().parent / "data" / "prices"

_price_cache: dict[str, dict[str, float]] = {}


def load_prices(ticker: str) -> dict[str, float]:
    """Load {date_str: close_price} from pipelines/insider_study/data/prices/{TICKER}.csv."""
    if ticker in _price_cache:
        return _price_cache[ticker]

    csv_path = PRICES_DIR / f"{ticker}.csv"
    prices = {}
    if csv_path.exists():
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    close = float(row["close"])
                    date_val = row.get("date") or row.get("timestamp", "")
                    date_key = date_val[:10]
                    if len(date_key) == 10:
                        prices[date_key] = close
                except (KeyError, ValueError):
                    continue
    _price_cache[ticker] = prices
    return prices


def find_price(prices: dict[str, float], date_str: str, offsets: range) -> float | None:
    """Find closest available price near a date."""
    try:
        td = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    for off in offsets:
        check = (td - timedelta(days=off)).strftime("%Y-%m-%d")
        if check in prices:
            return prices[check]
        check = (td + timedelta(days=off)).strftime("%Y-%m-%d")
        if check in prices:
            return prices[check]
    return None


def compute_30d_change(prices: dict[str, float], trade_date: str) -> float | None:
    """Compute 30-day price change ending at trade_date."""
    try:
        td = datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        return None
    current = find_price(prices, trade_date, range(4))
    ago = find_price(prices, (td - timedelta(days=30)).strftime("%Y-%m-%d"), range(6))
    if current and ago and ago > 0:
        return (current - ago) / ago
    return None


def compute_period_change(prices: dict[str, float], trade_date: str, days: int) -> float | None:
    """Compute price change over a given number of calendar days ending at trade_date."""
    try:
        td = datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        return None
    current = find_price(prices, trade_date, range(4))
    ago = find_price(prices, (td - timedelta(days=days)).strftime("%Y-%m-%d"), range(6))
    if current and ago and ago > 0:
        return (current - ago) / ago
    return None


def available_tickers() -> set[str]:
    """Return set of tickers that have price CSV files."""
    return {p.stem for p in PRICES_DIR.glob("*.csv")}
