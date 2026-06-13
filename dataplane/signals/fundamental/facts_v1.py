"""fundamentals.facts.v1 — SEC XBRL companyfacts as a per_ticker_per_day signal.

For each (ticker, as_of_date), emit the most recently filed financial
snapshot — Revenues / NetIncomeLoss / Assets / StockholdersEquity /
CommonStockSharesOutstanding / LongTermDebtNoncurrent /
CashAndCashEquivalentsAtCarryingValue / OperatingIncomeLoss — restricted
to facts with filed <= as_of_date so PIT honesty holds.

Source: data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json (free, no
auth). SEC asks for an identifying User-Agent and ≤10 req/sec; we obey.
Per-CIK responses are cached in-process so backfills only hit the API
once per ticker per run.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

from dataplane import PIT, Signal, SignalObservation, Upstream


# ── Tracked US-GAAP concepts ───────────────────────────────────────────
# Each entry: pyrrho field → list of US-GAAP concept names, in preference
# order (newer accounting standards first). For each concept, we pull
# the most-recent fact with filed <= as_of_date.

_CONCEPTS: Dict[str, List[str]] = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ],
    "net_income": ["NetIncomeLoss"],
    "assets": ["Assets"],
    "stockholders_equity": ["StockholdersEquity"],
    "shares_outstanding": [
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
    ],
    "long_term_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue"],
    "operating_income": ["OperatingIncomeLoss"],
}


# ── Ticker → CIK map (subset for our 31-ticker default universe) ───────
# CIKs are zero-padded to 10 digits for the URL. Looked up from
# https://www.sec.gov/files/company_tickers.json. Extend as universe grows.

_TICKER_TO_CIK: Dict[str, str] = {
    "AAPL":  "0000320193",
    "MSFT":  "0000789019",
    "NVDA":  "0001045810",
    "AMZN":  "0001018724",
    "META":  "0001326801",
    "GOOGL": "0001652044",
    "TSLA":  "0001318605",
    "AMD":   "0000002488",
    "AVGO":  "0001730168",
    "ADBE":  "0000796343",
    "NFLX":  "0001065280",
    "INTC":  "0000050863",
    "JPM":   "0000019617",
    "BAC":   "0000070858",
    "WFC":   "0000072971",
    "GS":    "0000886982",
    "JNJ":   "0000200406",
    "PFE":   "0000078003",
    "MRK":   "0000310158",
    "LLY":   "0000059478",
    "XOM":   "0000034088",
    "CVX":   "0000093410",
    "BRK.B": "0001067983",
    "V":     "0001403161",
    "MA":    "0001141391",
    "HD":    "0000354950",
    "WMT":   "0000104169",
    "PG":    "0000080424",
    # SPY/QQQ/IWM are ETFs — skip; we'd return a no_data observation.
}


_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "Pyrrho Dataplane derek@example.com",
)
_REQUEST_INTERVAL = 0.12  # ≥10 req/sec, with some margin

# Process-level cache: cik → (fetched_at, facts_dict)
_FACTS_CACHE: Dict[str, dict] = {}
_FACTS_LOCK = threading.Lock()
_LAST_REQUEST_TIME = [0.0]


def _fetch_companyfacts(cik: str) -> Optional[dict]:
    """Memoized fetch — one HTTP per CIK per process."""
    with _FACTS_LOCK:
        if cik in _FACTS_CACHE:
            return _FACTS_CACHE[cik]
        # Pace the request
        elapsed = time.time() - _LAST_REQUEST_TIME[0]
        if elapsed < _REQUEST_INTERVAL:
            time.sleep(_REQUEST_INTERVAL - elapsed)
        try:
            resp = requests.get(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                timeout=20,
            )
            _LAST_REQUEST_TIME[0] = time.time()
            if resp.status_code != 200:
                _FACTS_CACHE[cik] = None
                return None
            data = resp.json()
            _FACTS_CACHE[cik] = data
            return data
        except Exception:
            _FACTS_CACHE[cik] = None
            return None


def _latest_fact_before(
    facts: dict, concept_candidates: List[str], as_of: datetime
) -> Optional[dict]:
    """Find the most recently filed fact for any of the candidate concepts
    with filed <= as_of (YYYY-MM-DD). Returns None if nothing knowable."""
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    cutoff = as_of.date().isoformat()

    best = None
    best_filed = None
    for concept in concept_candidates:
        node = gaap.get(concept)
        if not node:
            continue
        for unit_rows in (node.get("units") or {}).values():
            for r in unit_rows:
                filed = r.get("filed", "")
                if not filed or filed > cutoff:
                    continue
                if best_filed is None or filed > best_filed:
                    best = r
                    best_filed = filed
                    best["_concept"] = concept
    return best


class FundamentalsFactsV1(Signal):
    """Per-ticker daily snapshot of SEC XBRL facts known as-of the date."""

    signal_id = "fundamental.facts"
    version = "v1.0.0"
    owner = "derek"
    sla_hours = 96.0  # filings land sporadically, so freshness window is wide
    business_hours_only = False
    description = (
        "Most-recently-reported SEC XBRL facts for the ticker, "
        "filtered to filings public as of as_of_date."
    )
    upstream = [
        # External source — no PIT lag (the API tells us when each fact was filed).
        Upstream("external.sec.xbrl.companyfacts", pit_lag=timedelta(0)),
    ]
    output_schema = {
        "cik":                  "text",
        "entity_name":          "text",
        "latest_filing":        "jsonb",  # {form, filed, period_end, fy, fp, accn}
        "facts":                "jsonb",  # field → numeric value
        "status":               "text",   # "ok" | "no_data" | "no_cik"
    }

    @PIT.strict
    def compute(self, ticker: str, as_of: datetime) -> SignalObservation:
        cik = _TICKER_TO_CIK.get(ticker)
        if not cik:
            return self.observation(ticker, as_of, value={
                "status": "no_cik", "note": "ticker not in CIK map",
            })
        data = _fetch_companyfacts(cik)
        if not data:
            return self.observation(ticker, as_of, value={
                "status": "no_data", "cik": cik,
            })

        facts: Dict[str, float] = {}
        latest_filing: Optional[dict] = None
        latest_filed = ""
        for field, concepts in _CONCEPTS.items():
            r = _latest_fact_before(data, concepts, as_of)
            if r is None:
                facts[field] = None
                continue
            facts[field] = r.get("val")
            # Track the absolute newest filed across all extracted facts as
            # this snapshot's "latest filing" tag.
            filed = r.get("filed", "")
            if filed > latest_filed:
                latest_filed = filed
                latest_filing = {
                    "form":        r.get("form"),
                    "filed":       r.get("filed"),
                    "period_end":  r.get("end"),
                    "fy":          r.get("fy"),
                    "fp":          r.get("fp"),
                    "accn":        r.get("accn"),
                    "concept_used": r.get("_concept"),
                }

        return self.observation(ticker, as_of, value={
            "status":         "ok" if latest_filing else "no_data",
            "cik":            cik,
            "entity_name":    data.get("entityName"),
            "latest_filing":  latest_filing,
            "facts":          facts,
        })
