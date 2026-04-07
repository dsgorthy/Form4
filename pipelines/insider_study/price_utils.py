"""Shared price loading and computation utilities for insider study pipelines.

Primary source: prices.db (SQLite, daily_prices table — 7,500+ tickers, 12M+ rows).
Fallback: CSV files in pipelines/insider_study/data/prices/ (legacy, avoid for new code).

All lookups are PIT-safe: find_price() only searches backward, never forward.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PRICES_DB = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "prices.db"
PRICES_DIR = Path(__file__).resolve().parent / "data" / "prices"  # legacy CSV fallback

# ---------------------------------------------------------------------------
# DB connection (lazy singleton)
# ---------------------------------------------------------------------------

_db_conn: sqlite3.Connection | None = None


def _get_db() -> sqlite3.Connection | None:
    """Get a read-only connection to prices.db. Lazy init, reused across calls."""
    global _db_conn
    if _db_conn is not None:
        return _db_conn
    if PRICES_DB.exists():
        _db_conn = sqlite3.connect(f"file:{PRICES_DB}?mode=ro", uri=True)
        _db_conn.execute("PRAGMA query_only=ON")
        _db_conn.execute("PRAGMA wal_autocheckpoint=0")
        return _db_conn
    return None


# ---------------------------------------------------------------------------
# Per-ticker price cache (populated from DB, not CSVs)
# ---------------------------------------------------------------------------

_price_cache: dict[str, dict[str, float]] = {}


def load_prices(ticker: str) -> dict[str, float]:
    """Load {date_str: close_price} for a ticker.

    Primary: query prices.db (fast, indexed).
    Fallback: CSV file if ticker not in DB.
    Results are cached in memory per ticker.
    """
    if ticker in _price_cache:
        return _price_cache[ticker]

    prices: dict[str, float] = {}

    # Try prices.db first
    db = _get_db()
    if db is not None:
        try:
            rows = db.execute(
                "SELECT date, close FROM daily_prices WHERE ticker = ? ORDER BY date",
                (ticker,),
            ).fetchall()
            for date_str, close in rows:
                if close and date_str:
                    prices[date_str[:10]] = close
        except Exception:
            pass

    # Fallback to CSV if DB had no data for this ticker
    if not prices:
        csv_path = PRICES_DIR / f"{ticker}.csv"
        if csv_path.exists():
            import csv
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


def clear_cache() -> None:
    """Clear the in-memory price cache to free memory."""
    _price_cache.clear()


def get_close(ticker: str, date: str) -> float | None:
    """Get closing price for a ticker on or just before a date.

    Single-row DB query — does NOT load full ticker history into memory.
    Use this when you only need one price, not a full series.
    """
    db = _get_db()
    if db is not None:
        try:
            r = db.execute(
                "SELECT close FROM daily_prices WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
                (ticker, date),
            ).fetchone()
            if r and r[0]:
                return r[0]
        except Exception:
            pass
    # Fallback to cached full series
    prices = load_prices(ticker)
    return find_price(prices, date, range(6))


def find_price(prices: dict[str, float], date_str: str, offsets: range) -> float | None:
    """Find closest available price at or BEFORE date_str. Never looks forward (PIT-safe)."""
    try:
        td = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    for off in offsets:
        check = (td - timedelta(days=off)).strftime("%Y-%m-%d")
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
    """Return set of tickers that have price data (DB or CSV)."""
    tickers: set[str] = set()
    db = _get_db()
    if db is not None:
        try:
            rows = db.execute("SELECT DISTINCT ticker FROM daily_prices").fetchall()
            tickers = {r[0] for r in rows}
        except Exception:
            pass
    if not tickers:
        tickers = {p.stem for p in PRICES_DIR.glob("*.csv")}
    return tickers
