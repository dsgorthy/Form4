"""Alpaca market-data SCREENER helpers — candidate discovery for the momentum scanner.

Kept SEPARATE from framework/data/alpaca_client.py on purpose: that module is on
the live paper-trading path, and the screener endpoints live under a different API
version (/v1beta1) than the bars/snapshot API (/v2). These are thin, additive,
read-only helpers — no shared state with the trading client.

Endpoints (https://data.alpaca.markets):
  GET /v1beta1/screener/stocks/movers        -> {"gainers":[...], "losers":[...]}
  GET /v1beta1/screener/stocks/most-actives  -> {"most_actives":[...]}

NOTE: Alpaca's movers endpoint historically restricts to price >= $1 and a minimum
volume, so deep sub-$1 names (e.g. OBAI in the $0.40s) can be MISSED here. For full
sub-$1 coverage, supplement with a price-filtered universe scan (a later stage). The
parse_* functions are pure and unit-tested; the get_* wrappers do the network I/O.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

_BASE = "https://data.alpaca.markets"
_TIMEOUT = 20


def _headers(api_key: str, api_secret: str) -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Accept": "application/json",
    }


def _get(api_key: str, api_secret: str, path: str, params: Dict[str, Any], retries: int = 2) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(_BASE + path, headers=_headers(api_key, api_secret), params=params, timeout=_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429,) or r.status_code >= 500:
                last_exc = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(1.0 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network
            last_exc = exc
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Alpaca screener {path} failed: {last_exc}")


def parse_gainers(payload: Dict[str, Any], min_pct: float = 0.0) -> List[Dict[str, Any]]:
    """Pure: extract gainers [{symbol, percent_change, price}] sorted desc by % change."""
    out: List[Dict[str, Any]] = []
    for g in (payload or {}).get("gainers", []) or []:
        sym = g.get("symbol")
        pct = g.get("percent_change")
        if not sym or pct is None:
            continue
        if float(pct) < min_pct:
            continue
        out.append({"symbol": sym, "percent_change": float(pct), "price": g.get("price")})
    out.sort(key=lambda d: d["percent_change"], reverse=True)
    return out


def parse_most_actives(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pure: extract most-actives [{symbol, volume, trade_count}]."""
    out: List[Dict[str, Any]] = []
    for a in (payload or {}).get("most_actives", []) or []:
        sym = a.get("symbol")
        if not sym:
            continue
        out.append({"symbol": sym, "volume": a.get("volume"), "trade_count": a.get("trade_count")})
    return out


def get_movers(api_key: str, api_secret: str, top: int = 50) -> Dict[str, Any]:
    """Top gainers/losers by % change. Returns the raw payload."""
    return _get(api_key, api_secret, "/v1beta1/screener/stocks/movers", {"top": top})


def get_most_actives(api_key: str, api_secret: str, top: int = 50, by: str = "volume") -> Dict[str, Any]:
    """Most active stocks (by volume or trades). Returns the raw payload."""
    return _get(api_key, api_secret, "/v1beta1/screener/stocks/most-actives", {"top": top, "by": by})


def candidate_symbols(
    api_key: str,
    api_secret: str,
    *,
    top: int = 50,
    min_gainer_pct: float = 10.0,
    include_most_actives: bool = True,
) -> List[str]:
    """De-duped candidate symbol list: gainers above `min_gainer_pct`, optionally
    unioned with most-actives (which surfaces high-RVOL names that aren't top % yet)."""
    symbols: List[str] = []
    seen = set()

    movers = get_movers(api_key, api_secret, top=top)
    for g in parse_gainers(movers, min_pct=min_gainer_pct):
        if g["symbol"] not in seen:
            seen.add(g["symbol"])
            symbols.append(g["symbol"])

    if include_most_actives:
        actives = get_most_actives(api_key, api_secret, top=top)
        for a in parse_most_actives(actives):
            if a["symbol"] not in seen:
                seen.add(a["symbol"])
                symbols.append(a["symbol"])

    return symbols
