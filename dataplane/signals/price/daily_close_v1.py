"""prices.daily.close.v1 — daily bar close from Alpaca data API.

Cross-domain proof: not insider data, fits the same Signal contract, lands
as one row per (ticker, day) in signal_observations.

Per the operator's data-source preference (Alpaca-first, free-before-paid),
this uses the strategy's trading credentials against data.alpaca.markets —
the same auth path Form4's cw_runner uses since the 2026-05-30 fix.

Output value shape:
    {
      "close":  float,
      "open":   float,
      "high":   float,
      "low":    float,
      "volume": int,
      "date":   "YYYY-MM-DD",
      "source": "alpaca"
    }

Confidence is left null — raw price observations don't carry a Bayesian
confidence. Composite signals derived from this will compute their own.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from dataplane import PIT, PITTestCase, Signal, SignalObservation


class PricesDailyCloseV1(Signal):
    """End-of-day OHLCV bar from Alpaca."""

    signal_id = "price.daily.close"
    version = "v1.0.0"
    owner = "derek"
    sla_hours = 18.0   # EOD bar lands ~17:30 PT; 18h gives margin to next-day open
    business_hours_only = True
    description = "Daily OHLCV bar from Alpaca, EOD."
    upstream = []   # no upstream signals — this IS a raw ingestion
    output_schema = {
        "close":  "real",
        "open":   "real",
        "high":   "real",
        "low":    "real",
        "volume": "bigint",
        "date":   "text",
        "source": "text",
    }

    _ALPACA_BASE = "https://data.alpaca.markets"

    @PIT.strict
    def compute(self, ticker: str, as_of: datetime) -> SignalObservation:
        # Alpaca EOD bars are available a few hours after market close.
        # Request the bar dated as_of's calendar date (not yesterday) — if
        # we're computing for today, we get today's bar if available.
        target_date = as_of.strftime("%Y-%m-%d")

        headers = self._headers()
        params = {
            "timeframe": "1Day",
            "start": target_date,
            "end":   target_date,
            "limit": 1,
            "adjustment": "raw",
            "feed": "iex",   # IEX = free tier; SIP requires paid sub
        }
        url = f"{self._ALPACA_BASE}/v2/stocks/{ticker}/bars"
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
        except Exception as exc:
            return self.observation(
                ticker, as_of,
                value={"source": "alpaca", "error": str(exc), "date": target_date},
                confidence=None,
                metadata={"http_status": None},
            )

        if r.status_code != 200:
            return self.observation(
                ticker, as_of,
                value={
                    "source": "alpaca",
                    "error": f"HTTP {r.status_code}",
                    "date": target_date,
                    "detail": r.text[:200],
                },
                confidence=None,
                metadata={"http_status": r.status_code},
            )

        bars = (r.json() or {}).get("bars") or []
        if not bars:
            return self.observation(
                ticker, as_of,
                value={"source": "alpaca", "no_data": True, "date": target_date},
                confidence=None,
                metadata={"http_status": r.status_code},
            )

        bar = bars[0]
        return self.observation(
            ticker, as_of,
            value={
                "close":  float(bar["c"]),
                "open":   float(bar["o"]),
                "high":   float(bar["h"]),
                "low":    float(bar["l"]),
                "volume": int(bar["v"]),
                "date":   target_date,
                "source": "alpaca",
            },
            confidence=None,
            metadata={"http_status": 200, "bar_t": bar.get("t")},
        )

    def _headers(self) -> dict:
        # Use trading credentials against data.alpaca.markets — same auth path
        # cw_runner uses since the 2026-05-30 fix. ALPACA_DATA_API_KEY was
        # revoked and returns 401 even when set; we skip it deliberately.
        key = (
            os.environ.get("ALPACA_API_KEY_QUALITY_MOMENTUM")
            or os.environ.get("ALPACA_API_KEY")
        )
        secret = (
            os.environ.get("ALPACA_API_SECRET_QUALITY_MOMENTUM")
            or os.environ.get("ALPACA_API_SECRET")
        )
        if not key or not secret:
            raise RuntimeError(
                "Alpaca credentials missing — set ALPACA_API_KEY_QUALITY_MOMENTUM "
                "+ ALPACA_API_SECRET_QUALITY_MOMENTUM in env."
            )
        return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}

    @classmethod
    def test_cases(cls):
        # Known stable historical dates. Specific values aren't asserted
        # because Alpaca's IEX feed returns minor adjustments over time;
        # the PIT validator catches structural diffs.
        return [
            PITTestCase(
                ticker="SPY",
                as_of=datetime(2025, 1, 6, tzinfo=timezone.utc),
                description="Monday after holiday — high-volume reference bar",
            ),
            PITTestCase(
                ticker="AAPL",
                as_of=datetime(2025, 6, 2, tzinfo=timezone.utc),
                description="Mid-year recent close",
            ),
            PITTestCase(
                ticker="ZZZZZ_NOT_A_TICKER",
                as_of=datetime(2025, 6, 2, tzinfo=timezone.utc),
                description="Unknown ticker → no_data response",
            ),
        ]


if __name__ == "__main__":
    import argparse
    import json as _json

    p = argparse.ArgumentParser()
    p.add_argument("--ticker", required=True)
    p.add_argument("--as-of", required=True)
    args = p.parse_args()

    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    obs = PricesDailyCloseV1().compute(args.ticker, as_of)
    print(_json.dumps({
        "signal_id": obs.signal_id,
        "ticker": obs.ticker,
        "as_of_date": obs.as_of_date.isoformat(),
        "value": obs.value,
        "confidence": obs.confidence,
        "metadata": obs.metadata,
    }, indent=2))
