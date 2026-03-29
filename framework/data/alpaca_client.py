"""
Alpaca Data API v2 client — constructor-param credentials.
No settings import. Pass credentials explicitly.
"""

import sys
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pytz
import requests

_EASTERN = pytz.timezone("US/Eastern")
_MAX_BARS_PER_PAGE = 10_000
_RATE_LIMIT_PER_MIN = 200
_MIN_REQUEST_INTERVAL = 60.0 / _RATE_LIMIT_PER_MIN
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0


def _normalize_date(value: Union[str, datetime, date]) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = _EASTERN.localize(value)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Expected str, datetime, or date; got {type(value).__name__}")


def _localize_timestamp(ts_str: str) -> str:
    dt = pd.Timestamp(ts_str)
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return str(dt.tz_convert(_EASTERN))


def _bar_to_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": _localize_timestamp(raw["t"]),
        "open": float(raw["o"]),
        "high": float(raw["h"]),
        "low": float(raw["l"]),
        "close": float(raw["c"]),
        "volume": int(raw["v"]),
        "vwap": float(raw["vw"]),
        "trade_count": int(raw["n"]),
    }


class AlpacaClient:
    """Thin wrapper around Alpaca Data API v2 (stocks/bars, snapshots)."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://data.alpaca.markets/v2",
    ):
        if not api_key or not api_secret:
            raise ValueError(
                "Alpaca credentials missing. Pass api_key and api_secret."
            )
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Accept": "application/json",
        })
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        for attempt in range(1, _MAX_RETRIES + 1):
            self._throttle()
            self._last_request_time = time.monotonic()
            try:
                resp = self._session.request(method, url, params=params, timeout=30)
            except requests.RequestException as exc:
                print(f"[AlpacaClient] request error (attempt {attempt}/{_MAX_RETRIES}): {exc}", file=sys.stderr)
                if attempt == _MAX_RETRIES:
                    raise
                time.sleep(_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue

            if resp.status_code == 200:
                return resp.json()

            retryable = resp.status_code == 429 or resp.status_code >= 500
            print(f"[AlpacaClient] HTTP {resp.status_code} on {path} (attempt {attempt}/{_MAX_RETRIES}): {resp.text[:300]}", file=sys.stderr)
            if retryable and attempt < _MAX_RETRIES:
                backoff = _BACKOFF_BASE * (2 ** (attempt - 1))
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        backoff = max(backoff, float(retry_after))
                    except ValueError:
                        pass
                time.sleep(backoff)
                continue
            resp.raise_for_status()
        raise RuntimeError("Exhausted retries without a response")

    def _fetch_all_bars(self, symbol: str, start: str, end: str, timeframe: str, adjustment: str = "raw") -> List[Dict[str, Any]]:
        all_bars: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "start": start, "end": end, "timeframe": timeframe,
                "limit": _MAX_BARS_PER_PAGE, "adjustment": adjustment,
                "feed": "sip", "sort": "asc",
            }
            if page_token is not None:
                params["page_token"] = page_token
            data = self._request("GET", f"/stocks/{symbol}/bars", params=params)
            bars = data.get("bars") or []
            all_bars.extend(bars)
            page_token = data.get("next_page_token")
            if not page_token:
                break
        return all_bars

    def get_bars(self, symbol: str, start: Union[str, datetime, date], end: Union[str, datetime, date], timeframe: str = "1Min", adjustment: str = "raw") -> List[Dict[str, Any]]:
        start_str = _normalize_date(start)
        end_str = _normalize_date(end)
        raw_bars = self._fetch_all_bars(symbol, start_str, end_str, timeframe, adjustment=adjustment)
        return [_bar_to_dict(b) for b in raw_bars]

    def get_bars_df(self, symbol: str, start: Union[str, datetime, date], end: Union[str, datetime, date], timeframe: str = "1Min", adjustment: str = "raw") -> pd.DataFrame:
        bars = self.get_bars(symbol, start, end, timeframe, adjustment=adjustment)
        if not bars:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "vwap", "trade_count"])
        df = pd.DataFrame(bars)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(_EASTERN)
        df.set_index("timestamp", inplace=True)
        df.index.name = "timestamp"
        return df

    def get_snapshot(self, symbol: str) -> Dict[str, Any]:
        data = self._request("GET", f"/stocks/{symbol}/snapshot")
        for section in ["latestTrade", "latestQuote", "minuteBar", "dailyBar", "prevDailyBar"]:
            if section in data and "t" in data[section]:
                data[section]["t"] = _localize_timestamp(data[section]["t"])
        return data

    def get_daily_bars(self, symbol: str, start: Union[str, datetime, date], end: Union[str, datetime, date], adjustment: str = "split") -> pd.DataFrame:
        return self.get_bars_df(symbol, start, end, timeframe="1Day", adjustment=adjustment)
