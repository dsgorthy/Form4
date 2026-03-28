"""Cache of last available price date per ticker, for N/A return explanations."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# In Docker, pipelines/ isn't available — use the DB mount directory which has a copy
_DB_DIR = Path(os.environ.get("INSIDERS_DB_PATH", "")).parent if os.environ.get("INSIDERS_DB_PATH") else None
_LAST_DATES_PATH = (
    (_DB_DIR / "last_dates.json") if _DB_DIR and (_DB_DIR / "last_dates.json").exists()
    else Path(__file__).resolve().parent.parent / "pipelines" / "insider_study" / "data" / "prices" / "last_dates.json"
)
_cache: dict[str, str] | None = None


def _load() -> dict[str, str]:
    global _cache
    if _cache is not None:
        return _cache
    try:
        _cache = json.loads(_LAST_DATES_PATH.read_text())
    except Exception:
        _cache = {}
    return _cache


def get_last_price_date(ticker: str) -> str | None:
    """Return the last available price date for a ticker, or None if no data."""
    dates = _load()
    return dates.get(ticker.upper())


def enrich_items_with_price_end(items: list[dict], trade_id_key: str = "trade_id") -> None:
    """Add price_data_end to items where returns are missing on old trades."""
    dates = _load()
    if not dates:
        return

    from datetime import datetime, timedelta
    cutoff_7d = (datetime.now() - timedelta(days=21)).strftime("%Y-%m-%d")

    for item in items:
        trade_date = item.get("trade_date")
        if not trade_date or trade_date > cutoff_7d:
            continue

        # Only annotate if at least one return window is missing
        has_gap = (
            item.get("return_7d") is None
            or item.get("return_30d") is None
            or item.get("return_90d") is None
        )
        if not has_gap:
            continue

        ticker = item.get("ticker", "").upper()
        last_date = dates.get(ticker)
        if last_date:
            item["price_data_end"] = last_date
        elif ticker and ticker != "NONE":
            item["price_data_end"] = "none"  # no price file at all
