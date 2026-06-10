"""insider.trades.raw.v1 — every SEC Form 4 filing as a per-event row.

Materialization mode: per_partition_events. For each day partition, the
asset materializer queries form4.trades for filings whose filing_date
equals the partition and emits one SignalObservation per filing event
with as_of_date set to the filing's precise timestamp (filed_at when
present, else filing_date midnight UTC).

This is the operator-chosen "Option 1" shape: many rows per (signal_id,
ticker) but unique on (signal_id, ticker, as_of_date) because each event
gets its own timestamp.

Value payload (intentionally a minimal raw subset — derived features stay
in form4.trades for now):
    {
      "trade_id":        int,
      "insider_id":      int,
      "insider_name":    str,
      "insider_title":   str | null,
      "trade_type":      "buy" | "sell" | other SEC code,
      "trans_code":      str | null,    # SEC transaction code (P, S, M, A, F, …)
      "trade_date":      "YYYY-MM-DD",
      "filing_date":     "YYYY-MM-DD",
      "filed_at":        "YYYY-MM-DDTHH:MM:SS" | null,
      "price":           float,
      "qty":             int,
      "value":           float,
      "is_csuite":       bool,
      "is_10b5_1":       bool,
      "accession":       str,
      "security_title":  str | null
    }

This signal is the foundation under future insider-derived signals
(`insider.career_grade.v3` would read this once it drops the form4
bridge; `insider.recent_form.v2` similar). It's also directly query-
able by Claude: "show all insider trades for NVDA last week" =
SELECT FROM signal_observations WHERE signal_id LIKE 'insider.trades.raw%'.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg2

from dataplane import PIT, PITTestCase, Signal, SignalObservation, Upstream


class InsiderTradesRawV1(Signal):
    """Per-event ingestion of Form 4 filings from the form4 bridge.

    Phase 1: reads form4.trades directly (the bridge). When form4 itself
    becomes a dataplane-only ingestion target, the source flips to an
    EDGAR-direct fetcher and form4.trades is deprecated. The schema this
    signal emits doesn't change.
    """

    signal_id = "insider.trades.raw"
    version = "v1.0.0"
    owner = "derek"
    sla_hours = 6.0   # SEC filings appear in EDGAR within minutes of receipt
    business_hours_only = False  # EDGAR publishes during market hours; but
                                 # filings can land late evening too
    description = "Raw Form 4 filing rows from EDGAR (via form4 bridge)."
    materialization_mode = "per_partition_events"
    upstream = [
        # External source — same pattern as career_grade.v3's phase-1 bridge.
        Upstream("external.form4.trades", pit_lag=timedelta(0)),
    ]
    output_schema = {
        "trade_id":       "bigint",
        "insider_id":     "bigint",
        "insider_name":   "text",
        "insider_title":  "text",
        "trade_type":     "text",
        "trans_code":     "text",
        "trade_date":     "text",
        "filing_date":    "text",
        "filed_at":       "text",
        "price":          "real",
        "qty":            "bigint",
        "value":          "real",
        "is_csuite":      "boolean",
        "is_10b5_1":      "boolean",
        "accession":      "text",
        "security_title": "text",
    }

    _FORM4_DSN = "dbname=form4 host=localhost"

    def materialize_partition(self, partition_date: datetime) -> List[SignalObservation]:
        """Return all Form 4 filings whose filing_date matches the partition.

        Each filing emerges as its own SignalObservation with as_of_date set
        to filed_at (the precise SEC receipt timestamp). When filed_at is
        null (older rows), we fall back to filing_date at the END of the day
        UTC — a defensible best-guess that keeps PIT semantics meaningful.
        """
        day = partition_date.strftime("%Y-%m-%d")
        observations: List[SignalObservation] = []

        with psycopg2.connect(self._FORM4_DSN) as form4_conn:
            cur = form4_conn.cursor()
            cur.execute(
                """
                SELECT t.trade_id,
                       t.effective_insider_id,
                       i.name,
                       t.title,
                       t.trade_type,
                       t.trans_code,
                       t.trade_date,
                       t.filing_date,
                       t.filed_at,
                       t.price,
                       t.qty,
                       t.value,
                       t.is_csuite,
                       t.is_10b5_1,
                       t.accession,
                       t.security_title,
                       t.ticker
                  FROM trades t
                  LEFT JOIN insiders i ON i.insider_id = t.effective_insider_id
                 WHERE t.filing_date = %s
                   AND COALESCE(t.is_duplicate, 0) = 0
                """,
                (day,),
            )
            rows = cur.fetchall()
            cur.close()

        for r in rows:
            (trade_id, insider_id, insider_name, insider_title, trade_type,
             trans_code, trade_date, filing_date, filed_at, price, qty,
             value, is_csuite, is_10b5_1, accession, security_title,
             ticker) = r

            event_ts = _parse_event_timestamp(filed_at, filing_date, trade_id)
            if event_ts is None or not ticker:
                continue   # skip undated or untickered rows

            observations.append(SignalObservation(
                signal_id=f"{self.signal_id}.{self.version}",
                ticker=ticker,
                as_of_date=event_ts,
                value={
                    "trade_id":       int(trade_id) if trade_id is not None else None,
                    "insider_id":     int(insider_id) if insider_id is not None else None,
                    "insider_name":   insider_name,
                    "insider_title":  insider_title,
                    "trade_type":     trade_type,
                    "trans_code":     trans_code,
                    "trade_date":     trade_date,
                    "filing_date":    filing_date,
                    "filed_at":       filed_at,
                    "price":          float(price) if price is not None else None,
                    "qty":            int(qty) if qty is not None else None,
                    "value":          float(value) if value is not None else None,
                    "is_csuite":      bool(is_csuite) if is_csuite is not None else None,
                    "is_10b5_1":      bool(is_10b5_1) if is_10b5_1 is not None else None,
                    "accession":      accession,
                    "security_title": security_title,
                },
                source_run_id=self._run_id,
                metadata={"source": "form4.bridge"},
            ))

        return observations


def _parse_event_timestamp(
    filed_at: Optional[str],
    filing_date: Optional[str],
    trade_id: Optional[int],
) -> Optional[datetime]:
    """Resolve the most precise PIT timestamp we have for an event.

    Order of preference:
      1. filed_at (precise SEC receipt timestamp) if present
      2. filing_date at 23:59:59 UTC (end-of-day fallback)
      3. None (skip the row)

    EDGAR sometimes timestamps multiple filings to the same second; the
    same-day fallback also collapses every untimed row onto one second.
    Both collide on the primary key (signal_id, ticker, as_of_date) and
    lose data on upsert. To disambiguate, we offset the microsecond
    component by `trade_id % 1_000_000`. This is deterministic, idempotent
    across re-runs, and within the same second so PIT semantics are
    preserved (the offset is sub-second granularity).
    """
    base: Optional[datetime] = None
    if filed_at:
        try:
            base = datetime.fromisoformat(filed_at.replace("Z", "+00:00"))
            if base.tzinfo is None:
                base = base.replace(tzinfo=timezone.utc)
        except Exception:
            base = None
    if base is None and filing_date:
        try:
            d = datetime.fromisoformat(filing_date)
            base = d.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except Exception:
            base = None
    if base is None:
        return None
    offset_us = (int(trade_id) % 1_000_000) if trade_id is not None else 0
    return base.replace(microsecond=offset_us)
