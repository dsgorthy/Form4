"""insider.filings.raw.v1 — direct EDGAR Form 4 ingestion (no form4 bridge).

Materialization mode: per_partition_events. For each day partition, queries
EDGAR's EFTS full-text search for Form 4 filings whose filing_date equals
the partition, pulls each filing's XML, parses it, and emits one
SignalObservation per non-derivative + derivative trade.

This is the dataplane-native sibling of insider.trades.raw.v1 (the form4
bridge). Both should converge — a parity comparator (see tools/parity_check
or `python3 -m dataplane parity`) lets us confirm row-by-row agreement
before retiring the bridge.

Field set follows what form4's existing parser emits. Acceptance timestamp
becomes as_of_date; sub-second collisions (multiple trades per filing) are
disambiguated by a deterministic md5 of (accession, trade_index) modulo 1e6.
"""
from __future__ import annotations

import hashlib
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, List, Optional

import requests

# Reuse form4's tested EDGAR fetcher + parser. The script lives at
# strategies/insider_catalog/backfill_live.py — make it importable.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# Plus the package dir, since fetch_latest also does this:
_INSIDER_CATALOG = _REPO_ROOT / "strategies" / "insider_catalog"
if str(_INSIDER_CATALOG) not in sys.path:
    sys.path.insert(0, str(_INSIDER_CATALOG))

from strategies.insider_catalog.backfill_live import (  # noqa: E402
    fetch_all_form4_filings,
    fetch_form4_xml,
    parse_form4_xml,
)

from dataplane import Signal, SignalObservation, Upstream  # noqa: E402


def _retry_on_5xx(
    fn: Callable[..., Any],
    *args,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    **kwargs,
):
    """Retry EDGAR calls on transient 5xx errors. EFTS occasionally
    returns 500 mid-pagination (observed 2026-06-11 and 2026-06-12);
    one such hit nuking the whole nightly job is not acceptable.

    Retries only on HTTPError where status >= 500. 4xx and non-HTTP errors
    re-raise immediately — those mean we asked the wrong thing.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.HTTPError as exc:
            status = getattr(exc.response, "status_code", 0)
            if status < 500 or attempt == max_attempts - 1:
                raise
            last_exc = exc
            time.sleep(base_delay * (2 ** attempt))
    if last_exc:
        raise last_exc


class InsiderFilingsRawV1(Signal):
    """Direct EDGAR ingestion of Form 4 filings — no form4.trades dependency.

    Designed to replace insider.trades.raw.v1 (the form4 bridge) once
    parity is confirmed. Same per_partition_events shape so consumers
    don't change.
    """

    signal_id = "insider.filings.raw"
    version = "v1.0.0"
    owner = "derek"
    sla_hours = 6.0
    business_hours_only = False
    description = "Raw Form 4 filings ingested directly from EDGAR EFTS."
    materialization_mode = "per_partition_events"
    # Off the nightly schedule until parity vs insider.trades.raw holds ≥99.5%
    # for 30 days. CLI backfills + Dagster UI manual triggers still work.
    auto_schedule = False
    upstream = [
        Upstream("external.edgar.efts", pit_lag=timedelta(0)),
    ]
    output_schema = {
        "ticker":         "text",
        "insider_name":   "text",
        "title":          "text",
        "trade_type":     "text",
        "trans_code":     "text",
        "trade_date":     "text",
        "filing_date":    "text",
        "accepted_at":    "text",
        "price":          "real",
        "qty":            "bigint",
        "value":          "real",
        "cik":            "text",
        "company":        "text",
        "is_csuite":      "boolean",
        "is_10b5_1":      "boolean",
        "accession":      "text",
        "security_title": "text",
        "direct_indirect": "text",
        "trans_acquired_disp": "text",
    }

    def materialize_partition(
        self, partition_date: datetime
    ) -> List[SignalObservation]:
        """Fetch all Form 4 filings filed on partition_date, return one
        observation per parsed trade."""
        day = partition_date.strftime("%Y-%m-%d")
        observations: List[SignalObservation] = []

        # Stage 1: get filing metadata from EFTS (retry on 5xx)
        try:
            filings = _retry_on_5xx(fetch_all_form4_filings, day, day)
        except Exception:
            return observations  # EDGAR down — emit nothing for the partition

        # Stage 2: fetch + parse each filing
        for filing in filings:
            cik = filing["cik"]
            accession = filing["accession"]
            company = filing.get("company", "")
            filing_date_str = filing.get("filing_date", day) or day

            try:
                xml_text, accepted_at = _retry_on_5xx(
                    fetch_form4_xml, cik, accession
                )
            except Exception:
                continue
            if not xml_text:
                continue

            try:
                trades = parse_form4_xml(xml_text, cik, filing_date_str, company)
            except Exception:
                continue
            if not trades:
                continue

            base_ts = _resolve_accepted_ts(accepted_at, filing_date_str)
            if base_ts is None:
                continue

            for idx, trade in enumerate(trades):
                ticker = trade.get("ticker")
                if not ticker:
                    continue

                event_ts = _disambiguate(base_ts, accession, idx)

                payload = _serializable(trade)
                payload["accession"] = accession
                payload["accepted_at"] = accepted_at

                observations.append(SignalObservation(
                    signal_id=f"{self.signal_id}.{self.version}",
                    ticker=ticker,
                    as_of_date=event_ts,
                    value=payload,
                    source_run_id=self._run_id,
                    metadata={"source": "edgar.efts.direct"},
                ))

        return observations


def _resolve_accepted_ts(
    accepted_at: Optional[str], filing_date: str
) -> Optional[datetime]:
    """Parse the SEC acceptance timestamp; fall back to end of filing_date."""
    if accepted_at:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                base = datetime.strptime(accepted_at, fmt)
                return base.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    if filing_date:
        try:
            d = datetime.fromisoformat(filing_date)
            return d.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _disambiguate(base: datetime, accession: str, idx: int) -> datetime:
    """Stable microsecond offset so multi-trade filings have unique as_of."""
    key = f"{accession}:{idx}".encode()
    offset = int(hashlib.md5(key).hexdigest()[:8], 16) % 1_000_000
    return base.replace(microsecond=offset)


def _serializable(trade: dict) -> dict:
    """Coerce parser output into JSON-clean primitives."""
    out = {}
    for k, v in trade.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out
