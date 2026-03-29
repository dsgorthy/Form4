"""
Alpaca v1beta1 options bars collector — constructor-param credentials.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import pytz
import requests

logger = logging.getLogger(__name__)

_OPTIONS_BASE = "https://data.alpaca.markets/v1beta1"
_EASTERN = pytz.timezone("US/Eastern")
_MAX_SYMBOLS = 100
_RATE_LIMIT = 200
_MIN_INTERVAL = 60.0 / _RATE_LIMIT
_MAX_RETRIES = 3
_STRIKE_RADIUS = 5
_DTE_LIST = [0, 1, 2, 3]
_WINDOW_START = "15:00"
_WINDOW_END = "16:05"


def build_occ_symbol(underlying: str, expiry: date, option_type: str, strike: float) -> str:
    yy = expiry.strftime("%y")
    mm = expiry.strftime("%m")
    dd = expiry.strftime("%d")
    cp = "C" if option_type.lower().startswith("c") else "P"
    strike_int = round(strike * 1000)
    return f"{underlying.upper()}{yy}{mm}{dd}{cp}{strike_int:08d}"


def parse_occ_symbol(symbol: str) -> Dict:
    underlying = symbol[:3]
    date_str = symbol[3:9]
    cp = symbol[9]
    strike_raw = symbol[10:]
    expiry = datetime.strptime(date_str, "%y%m%d").date()
    strike = int(strike_raw) / 1000.0
    option_type = "call" if cp == "C" else "put"
    return {"underlying": underlying, "expiry": expiry, "option_type": option_type, "strike": strike}


def _next_trading_days(from_date: date, n: int) -> List[date]:
    days = []
    current = from_date
    while len(days) < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days.append(current)
    return days


def expiry_for_dte(trade_date: date, dte: int) -> date:
    if dte == 0:
        return trade_date
    return _next_trading_days(trade_date, dte)[-1]


class OptionsClient:
    """Thin wrapper around Alpaca v1beta1/options/bars."""

    def __init__(self, api_key: str, api_secret: str):
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Accept": "application/json",
        })
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)

    def _request(self, path: str, params: dict) -> dict:
        url = f"{_OPTIONS_BASE}{path}"
        for attempt in range(1, _MAX_RETRIES + 1):
            self._throttle()
            self._last_request_time = time.monotonic()
            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                logger.warning("Request error attempt %d/%d: %s", attempt, _MAX_RETRIES, exc)
                if attempt == _MAX_RETRIES:
                    raise
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 200:
                return resp.json()
            logger.warning("HTTP %d on %s (attempt %d): %s", resp.status_code, path, attempt, resp.text[:200])
            if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES:
                wait = float(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(wait)
                continue
            resp.raise_for_status()
        raise RuntimeError("Exhausted retries")

    def get_bars(self, symbols: List[str], start: str, end: str, timeframe: str = "1Min") -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        for i in range(0, len(symbols), _MAX_SYMBOLS):
            batch = symbols[i: i + _MAX_SYMBOLS]
            all_raw: Dict[str, list] = {}
            page_token = None
            while True:
                params: dict = {
                    "symbols": ",".join(batch), "timeframe": timeframe,
                    "start": start, "end": end, "limit": 10_000, "sort": "asc",
                }
                if page_token:
                    params["page_token"] = page_token
                data = self._request("/options/bars", params)
                for sym, bars in (data.get("bars") or {}).items():
                    all_raw.setdefault(sym, []).extend(bars)
                page_token = data.get("next_page_token")
                if not page_token:
                    break
            for sym, bars in all_raw.items():
                if not bars:
                    continue
                rows = []
                for b in bars:
                    ts = pd.Timestamp(b["t"]).tz_convert(_EASTERN)
                    rows.append({
                        "timestamp": ts, "open": float(b["o"]), "high": float(b["h"]),
                        "low": float(b["l"]), "close": float(b["c"]), "volume": int(b["v"]),
                        "vwap": float(b.get("vw", b["c"])),
                    })
                df = pd.DataFrame(rows).set_index("timestamp")
                df.index.name = "timestamp"
                result[sym] = df
        return result


def symbols_for_day(trade_date: date, atm_strike: float, strike_radius: int = _STRIKE_RADIUS, dte_list: List[int] = _DTE_LIST) -> List[str]:
    atm_rounded = round(atm_strike)
    strikes = [atm_rounded + offset for offset in range(-strike_radius, strike_radius + 1)]
    symbols = []
    for dte in dte_list:
        expiry = expiry_for_dte(trade_date, dte)
        for strike in strikes:
            for option_type in ("call", "put"):
                symbols.append(build_occ_symbol("SPY", expiry, option_type, strike))
    return list(dict.fromkeys(symbols))


def collect_day(client: OptionsClient, trade_date: date, atm_strike: float,
                strike_radius: int = _STRIKE_RADIUS, dte_list: List[int] = _DTE_LIST) -> Optional[pd.DataFrame]:
    symbols = symbols_for_day(trade_date, atm_strike, strike_radius, dte_list)
    if not symbols:
        return None
    start_dt = _EASTERN.localize(datetime.combine(trade_date, datetime.strptime(_WINDOW_START, "%H:%M").time()))
    end_dt = _EASTERN.localize(datetime.combine(trade_date, datetime.strptime(_WINDOW_END, "%H:%M").time()))
    bars_by_sym = client.get_bars(symbols, start_dt.isoformat(), end_dt.isoformat())
    if not bars_by_sym:
        logger.warning("No option bars returned for %s", trade_date)
        return None
    close_frames = {sym: df["close"].rename(sym) for sym, df in bars_by_sym.items()}
    wide = pd.concat(close_frames.values(), axis=1)
    wide.sort_index(inplace=True)
    return wide
