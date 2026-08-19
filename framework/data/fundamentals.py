"""Lightweight fundamentals fetch — shares-outstanding (float PROXY) for the scanner.

The scanner needs a float filter. True public float is paid data; as a free proxy we
use Finnhub /stock/profile2 `shareOutstanding` (total shares, in millions). For
micro-caps this OVER-estimates tradable float (insiders/restricted shares included),
so a `float<15M` style filter using this proxy is conservative (lets some names through
that a true-float filter would cut). Upgrade path: FMP /v4/shares_float (free tier has
real float) or a paid provider — swap behind `shares_outstanding_m`.

Reuses the existing FINNHUB_API_KEY convention (see pipelines/thesis_monitor/news.py).
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests

_FINNHUB_PROFILE = "https://finnhub.io/api/v1/stock/profile2"
_TIMEOUT = 15


def parse_shares_outstanding_m(payload: Dict[str, Any]) -> Optional[float]:
    """Pure: Finnhub profile2 reports `shareOutstanding` already in millions."""
    v = (payload or {}).get("shareOutstanding")
    try:
        return float(v) if v not in (None, "", 0) else None
    except (TypeError, ValueError):
        return None


def finnhub_profile(ticker: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Fetch Finnhub company profile. Returns {} on missing key / error (fail-soft)."""
    key = api_key or os.getenv("FINNHUB_API_KEY", "")
    if not key:
        return {}
    try:
        r = requests.get(_FINNHUB_PROFILE, params={"symbol": ticker, "token": key}, timeout=_TIMEOUT)
        if r.status_code == 200:
            return r.json() or {}
    except requests.RequestException:
        return {}
    return {}


def shares_outstanding_m(ticker: str, api_key: Optional[str] = None) -> Optional[float]:
    """Shares outstanding in millions (float proxy), or None if unavailable."""
    return parse_shares_outstanding_m(finnhub_profile(ticker, api_key))
