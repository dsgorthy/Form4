"""Macro signal fetch via FRED. API key from FRED_API_KEY env."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx

log = logging.getLogger(__name__)

FRED_API = "https://api.stlouisfed.org/fred/series/observations"

# FRED series ids
SERIES = {
    "brent": "DCOILBRENTEU",  # Brent spot, EU
    "wti": "DCOILWTICO",       # WTI spot
    "ovx": "OVXCLS",           # CBOE oil VIX
}


@dataclass
class MacroPoint:
    name: str
    value: float
    date: str
    prev_value: Optional[float]
    pct_change: Optional[float]


def _fetch_series(series_id: str, *, days: int = 14) -> Optional[MacroPoint]:
    key = os.getenv("FRED_API_KEY", "")
    if not key:
        log.warning("FRED_API_KEY not set; skipping %s", series_id)
        return None
    params = {
        "series_id": series_id,
        "api_key": key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": days,
    }
    try:
        r = httpx.get(FRED_API, params=params, timeout=15)
        r.raise_for_status()
        obs = r.json().get("observations", [])
    except httpx.HTTPError as exc:
        log.error("FRED error %s: %s", series_id, exc)
        return None
    # Filter to numeric observations
    pts = [(o["date"], float(o["value"])) for o in obs if o.get("value") not in (".", "", None)]
    if not pts:
        return None
    date, value = pts[0]
    prev_value = pts[1][1] if len(pts) > 1 else None
    pct = ((value - prev_value) / prev_value * 100) if prev_value else None
    return MacroPoint(name=series_id, value=value, date=date, prev_value=prev_value, pct_change=pct)


def fetch_macro() -> dict[str, MacroPoint]:
    out: dict[str, MacroPoint] = {}
    for key, series_id in SERIES.items():
        p = _fetch_series(series_id)
        if p:
            out[key] = p
    return out
