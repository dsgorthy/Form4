#!/usr/bin/env python3
"""CIK ↔ ticker mapping cache (SEC's company_tickers.json).

SEC publishes a daily-updated JSON of every CIK + primary ticker:
    https://www.sec.gov/files/company_tickers.json

We cache it under paths.edgar / 'company_tickers.json' and refresh on demand.
Used by event_8k loader, 13F resolver, etc. to map cik → ticker.

Note: the file maps CIK → primary ticker. Multi-class shares
(Berkshire BRK.A/BRK.B, Alphabet GOOG/GOOGL) collapse to one row.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.storage_paths import paths
from pipelines.edgar_archive.fetch_form_index import USER_AGENT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

CACHE_PATH = paths.edgar / "company_tickers.json"
SOURCE_URL = "https://www.sec.gov/files/company_tickers.json"
MAX_AGE_SECONDS = 7 * 24 * 3600   # refresh weekly


def _stale(path: Path) -> bool:
    if not path.exists():
        return True
    return (time.time() - path.stat().st_mtime) > MAX_AGE_SECONDS


def fetch_or_cache(force: bool = False) -> Path:
    """Download company_tickers.json if missing or older than MAX_AGE_SECONDS."""
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not force and not _stale(CACHE_PATH):
        return CACHE_PATH
    logger.info("fetching %s", SOURCE_URL)
    r = requests.get(SOURCE_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    tmp = CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_bytes(r.content)
    os.replace(tmp, CACHE_PATH)
    return CACHE_PATH


_cache: dict[str, str] | None = None  # cik(no leading zeros) → ticker


def load() -> dict[str, str]:
    """Return a {cik_int_string: ticker} dict. Cached after first call."""
    global _cache
    if _cache is not None:
        return _cache
    fetch_or_cache()
    raw = json.loads(CACHE_PATH.read_text())
    out: dict[str, str] = {}
    # Format: {"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}, ...}
    for v in raw.values():
        cik = str(v.get("cik_str", "")).lstrip("0") or "0"
        ticker = (v.get("ticker") or "").upper()
        if cik and ticker:
            out[cik] = ticker
    _cache = out
    logger.info("loaded %d CIK→ticker mappings", len(out))
    return out


def cik_to_ticker(cik: str) -> str | None:
    """Resolve a CIK to its primary ticker. Returns None if not found."""
    if not cik:
        return None
    return load().get(str(cik).lstrip("0") or "0")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cik", nargs="*", help="One or more CIKs to resolve")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    if args.refresh:
        fetch_or_cache(force=True)

    if args.stats:
        m = load()
        print(f"mappings: {len(m):,}")
        print(f"cache:    {CACHE_PATH}")
        return

    if not args.cik:
        p.error("supply one or more CIK arguments, or use --stats")

    for c in args.cik:
        print(f"  {c:>12s} → {cik_to_ticker(c) or '(none)'}")


if __name__ == "__main__":
    main()
