"""Daily price snapshots via yfinance. End-of-day reliable, no API key."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import yfinance as yf

log = logging.getLogger(__name__)


@dataclass
class Quote:
    ticker: str
    last: float
    prev_close: float
    day_pct: float
    volume: int

    def fmt(self) -> str:
        sign = "+" if self.day_pct >= 0 else ""
        return f"${self.last:,.2f} ({sign}{self.day_pct:.2f}%)"


def get_quote(ticker: str) -> Optional[Quote]:
    try:
        t = yf.Ticker(ticker)
        # 2-day history so we have prev close even mid-day
        hist = t.history(period="5d", auto_adjust=False)
        if hist.empty:
            log.warning("No history for %s", ticker)
            return None
        last_row = hist.iloc[-1]
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else float(last_row["Open"])
        last = float(last_row["Close"])
        day_pct = (last - prev_close) / prev_close * 100 if prev_close else 0.0
        return Quote(
            ticker=ticker,
            last=last,
            prev_close=prev_close,
            day_pct=day_pct,
            volume=int(last_row["Volume"]),
        )
    except Exception as exc:
        log.error("yfinance error for %s: %s", ticker, exc)
        return None


def get_quotes(tickers: list[str]) -> dict[str, Quote]:
    out: dict[str, Quote] = {}
    for t in tickers:
        q = get_quote(t)
        if q:
            out[t] = q
    return out
