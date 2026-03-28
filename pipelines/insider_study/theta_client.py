#!/usr/bin/env python3
"""
Concurrent Theta Data API Client
---------------------------------
Semaphore-based concurrent client for Theta Data's local REST API.
Designed for the Professional plan (8 concurrent outstanding requests).

Usage:
    from theta_client import ThetaClient

    client = ThetaClient(max_concurrent=8)
    expirations = await client.get_expirations("AAPL")
    eod_data = await client.get_option_eod("AAPL", exp_date, 150.0, "C", start, end)
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import sqlite3
import time
import urllib.parse
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

THETA_BASE = "http://127.0.0.1:25503"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DB_PATH = os.path.join(SCRIPT_DIR, "data", "theta_cache.db")


# ─────────────────────────────────────────────
# SQLite Cache
# ─────────────────────────────────────────────

def init_cache_db(db_path: str = CACHE_DB_PATH) -> sqlite3.Connection:
    """Initialize the persistent SQLite cache."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            cache_key TEXT PRIMARY KEY,
            response_json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pull_progress (
            ticker TEXT PRIMARY KEY,
            status TEXT DEFAULT 'pending',
            events_total INTEGER DEFAULT 0,
            events_completed INTEGER DEFAULT 0,
            started_at TEXT,
            completed_at TEXT,
            error TEXT
        )
    """)
    conn.commit()
    return conn


class CacheDB:
    """Thread-safe SQLite cache wrapper."""

    def __init__(self, db_path: str = CACHE_DB_PATH):
        self.db_path = db_path
        self._conn = init_cache_db(db_path)
        self._lock = asyncio.Lock()

    def get(self, key: str) -> list[dict] | None:
        """Synchronous cache lookup (called from within async context)."""
        row = self._conn.execute(
            "SELECT response_json FROM cache WHERE cache_key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def put(self, key: str, value: Any):
        """Synchronous cache write."""
        self._conn.execute(
            "INSERT OR REPLACE INTO cache (cache_key, response_json) VALUES (?, ?)",
            (key, json.dumps(value)),
        )
        self._conn.commit()

    def has(self, key: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM cache WHERE cache_key = ? LIMIT 1", (key,)
        ).fetchone()
        return row is not None

    def count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM cache").fetchone()
        return row[0]

    # Progress tracking
    def set_ticker_status(self, ticker: str, status: str, events_total: int = 0,
                          events_completed: int = 0, error: str = None):
        now = datetime.utcnow().isoformat()
        started = now if status == "in_progress" else None
        completed = now if status in ("completed", "failed") else None
        self._conn.execute("""
            INSERT INTO pull_progress (ticker, status, events_total, events_completed, started_at, completed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                status = excluded.status,
                events_total = CASE WHEN excluded.events_total > 0 THEN excluded.events_total ELSE events_total END,
                events_completed = CASE WHEN excluded.events_completed > 0 THEN excluded.events_completed ELSE events_completed END,
                started_at = COALESCE(excluded.started_at, started_at),
                completed_at = excluded.completed_at,
                error = excluded.error
        """, (ticker, status, events_total, events_completed, started, completed, error))
        self._conn.commit()

    def update_ticker_progress(self, ticker: str, events_completed: int):
        self._conn.execute(
            "UPDATE pull_progress SET events_completed = ? WHERE ticker = ?",
            (events_completed, ticker),
        )
        self._conn.commit()

    def get_completed_tickers(self) -> set[str]:
        rows = self._conn.execute(
            "SELECT ticker FROM pull_progress WHERE status = 'completed'"
        ).fetchall()
        return {r[0] for r in rows}

    def get_progress_summary(self) -> dict:
        rows = self._conn.execute("""
            SELECT status, COUNT(*), SUM(events_total), SUM(events_completed)
            FROM pull_progress GROUP BY status
        """).fetchall()
        summary = {}
        for status, count, total, completed in rows:
            summary[status] = {"tickers": count, "events_total": total or 0, "events_completed": completed or 0}
        return summary

    def close(self):
        self._conn.close()


# ─────────────────────────────────────────────
# Async Theta Data Client
# ─────────────────────────────────────────────

class ThetaClient:
    """
    Async Theta Data client with semaphore-based concurrency control.

    The Professional plan allows 8 concurrent outstanding requests.
    We use a semaphore to enforce this limit, with automatic retry
    on 429 (queue overflow) responses.
    """

    def __init__(self, max_concurrent: int = 8, cache_db_path: str = CACHE_DB_PATH):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.cache = CacheDB(cache_db_path)
        self.session: aiohttp.ClientSession | None = None

        # Stats
        self.requests_made = 0
        self.requests_cached = 0
        self.requests_failed = 0
        self.start_time = time.monotonic()
        self._stats_lock = asyncio.Lock()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        )
        self.start_time = time.monotonic()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
        self.cache.close()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def throughput(self) -> float:
        elapsed = self.elapsed
        if elapsed == 0:
            return 0
        return self.requests_made / elapsed

    async def _fetch(self, endpoint: str, params: dict, cache_key: str | None = None,
                     max_retries: int = 5) -> list[dict] | None:
        """
        Core fetch with semaphore, caching, and retry logic.
        """
        # Check cache first (no semaphore needed)
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                async with self._stats_lock:
                    self.requests_cached += 1
                return cached if cached != "__NONE__" else None

        query = urllib.parse.urlencode(params)
        url = f"{THETA_BASE}{endpoint}?{query}"

        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 429:
                            # Queue overflow — back off and retry
                            wait = min(2 ** attempt * 0.5, 10)
                            logger.warning(f"429 on {endpoint} — retry {attempt+1}/{max_retries} in {wait:.1f}s")
                            await asyncio.sleep(wait)
                            continue

                        if resp.status != 200:
                            logger.warning(f"HTTP {resp.status} on {url}")
                            if cache_key:
                                self.cache.put(cache_key, "__NONE__")
                            async with self._stats_lock:
                                self.requests_failed += 1
                            return None

                        raw = await resp.text()
                        raw = raw.strip()

                        async with self._stats_lock:
                            self.requests_made += 1

                        if not raw:
                            if cache_key:
                                self.cache.put(cache_key, "__NONE__")
                            return None

                        reader = csv.DictReader(StringIO(raw))
                        rows = list(reader)

                        if cache_key:
                            self.cache.put(cache_key, rows)

                        return rows

                except asyncio.TimeoutError:
                    wait = min(2 ** attempt * 0.5, 10)
                    logger.warning(f"Timeout on {endpoint} — retry {attempt+1}/{max_retries} in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                except Exception as e:
                    logger.error(f"Error on {endpoint}: {e}")
                    async with self._stats_lock:
                        self.requests_failed += 1
                    if cache_key:
                        self.cache.put(cache_key, "__NONE__")
                    return None

        # All retries exhausted
        logger.error(f"All {max_retries} retries failed for {endpoint}")
        async with self._stats_lock:
            self.requests_failed += 1
        if cache_key:
            self.cache.put(cache_key, "__NONE__")
        return None

    # ─── Public API Methods ────────────────────

    async def get_expirations(self, symbol: str) -> list[date]:
        """Get all available option expirations for a symbol."""
        cache_key = f"opt_exp|{symbol}"
        rows = await self._fetch(
            "/v3/option/list/expirations",
            {"symbol": symbol},
            cache_key,
        )
        if not rows:
            return []
        result = []
        for r in rows:
            try:
                exp_str = r.get("expiration", "").strip().strip('"')
                result.append(datetime.strptime(exp_str, "%Y-%m-%d").date())
            except (ValueError, KeyError):
                continue
        return sorted(result)

    async def get_strikes(self, symbol: str, expiration: date) -> list[float]:
        """Get all available strikes for a symbol + expiration."""
        exp_str = expiration.strftime("%Y-%m-%d")
        cache_key = f"opt_strikes|{symbol}|{exp_str}"
        rows = await self._fetch(
            "/v3/option/list/strikes",
            {"symbol": symbol, "expiration": exp_str},
            cache_key,
        )
        if not rows:
            return []
        result = []
        for r in rows:
            try:
                strike_str = r.get("strike", "").strip().strip('"')
                result.append(float(strike_str))
            except (ValueError, KeyError):
                continue
        return sorted(result)

    async def get_option_eod(self, symbol: str, expiration: date, strike: float,
                             right: str, start_date: date, end_date: date) -> list[dict] | None:
        """Get EOD option data for a specific contract."""
        exp_str = expiration.strftime("%Y-%m-%d")
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        cache_key = f"opt_eod_daily|{symbol}|{exp_str}|{strike}|{right}|{start_str}|{end_str}"

        return await self._fetch(
            "/v3/option/history/eod",
            {
                "symbol": symbol,
                "expiration": exp_str,
                "strike": str(strike),
                "right": right,
                "start_date": start_str,
                "end_date": end_str,
            },
            cache_key,
        )

    # ─── Batch helpers ─────────────────────────

    async def get_option_eod_batch(self, requests: list[dict]) -> list[list[dict] | None]:
        """
        Fetch multiple EOD option data concurrently.

        Each request dict should have: symbol, expiration, strike, right, start_date, end_date
        The semaphore ensures we never exceed max_concurrent outstanding requests.
        """
        tasks = [
            self.get_option_eod(
                r["symbol"], r["expiration"], r["strike"], r["right"],
                r["start_date"], r["end_date"]
            )
            for r in requests
        ]
        return await asyncio.gather(*tasks)

    def stats_summary(self) -> str:
        """Return a formatted stats string."""
        elapsed = self.elapsed
        total = self.requests_made + self.requests_cached
        return (
            f"Requests: {self.requests_made} API + {self.requests_cached} cached "
            f"({self.requests_failed} failed) | "
            f"Throughput: {self.throughput:.1f} req/sec | "
            f"Elapsed: {elapsed/60:.1f}min"
        )


# ─── Helpers (shared with old code) ──────────

def get_fair_price(row: dict) -> float | None:
    """Extract fair price from an EOD row (close or bid/ask midpoint)."""
    try:
        close = float(row.get("close", "0").strip().strip('"'))
        if close > 0:
            return close
        bid = float(row.get("bid", "0").strip().strip('"'))
        ask = float(row.get("ask", "0").strip().strip('"'))
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if ask > 0:
            return ask
        return None
    except (ValueError, TypeError):
        return None


def get_eod_date(row: dict) -> date | None:
    """Extract date from EOD row."""
    try:
        created = row.get("created", "").strip().strip('"')
        return datetime.strptime(created[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def find_nearest_expiration(expirations: list[date], entry_date: date, target_dte: int) -> date | None:
    """Find the nearest available expiration to entry_date + target_dte."""
    if not expirations:
        return None
    target = entry_date + timedelta(days=target_dte)
    min_exp = entry_date + timedelta(days=max(target_dte - 3, 4))
    valid = [e for e in expirations if e >= min_exp]
    if not valid:
        return None
    best = min(valid, key=lambda e: abs((e - target).days))
    if abs((best - target).days) > max(target_dte, 7):
        return None
    return best


def find_nearest_strike(strikes: list[float], target_price: float) -> float | None:
    """Find the nearest available strike to the target price."""
    if not strikes:
        return None
    return min(strikes, key=lambda s: abs(s - target_price))


def add_trading_days(start: date, n_days: int) -> date:
    """Add n trading days (skip weekends) to start date."""
    current = start
    added = 0
    while added < n_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# ─── Quick benchmark ──────────────────────────

async def benchmark():
    """Quick benchmark to test throughput with the Pro plan."""
    print(f"Theta Data Concurrent Client — Benchmark")
    print(f"Max concurrent: 8 (Professional plan)")
    print(f"Target: {THETA_BASE}")
    print()

    async with ThetaClient(max_concurrent=8) as client:
        # Test: fetch expirations for 20 symbols concurrently
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
                    "META", "NVDA", "JPM", "V", "JNJ",
                    "WMT", "PG", "UNH", "HD", "BAC",
                    "XOM", "PFE", "ABBV", "KO", "PEP"]

        print(f"Fetching expirations for {len(symbols)} symbols...")
        start = time.monotonic()

        tasks = [client.get_expirations(s) for s in symbols]
        results = await asyncio.gather(*tasks)

        elapsed = time.monotonic() - start
        success = sum(1 for r in results if r)

        print(f"  Completed: {success}/{len(symbols)} in {elapsed:.2f}s")
        print(f"  {client.stats_summary()}")
        print()

        # Test: batch EOD fetch for 20 contracts
        if results[0]:  # AAPL expirations
            exp = results[0][-1]  # latest expiration
            print(f"Fetching 20 EOD requests for AAPL {exp}...")
            start = time.monotonic()

            batch = [
                {"symbol": "AAPL", "expiration": exp, "strike": 150.0 + i * 5,
                 "right": "C", "start_date": date(2025, 1, 1), "end_date": date(2025, 3, 1)}
                for i in range(20)
            ]
            await client.get_option_eod_batch(batch)

            elapsed = time.monotonic() - start
            print(f"  Completed in {elapsed:.2f}s")
            print(f"  {client.stats_summary()}")

    print("\nCache entries:", CacheDB().count())


if __name__ == "__main__":
    asyncio.run(benchmark())
