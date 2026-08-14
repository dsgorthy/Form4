"""congress.trades.raw.v1 — STOCK Act congressional trade disclosures.

Materialization mode: per_partition_events. For each day partition the
materializer pages Capitol Trades newest-first and emits one
SignalObservation per disclosure whose *published* (filing) date matches
the partition.

Why this signal exists
----------------------
The legacy path (`pipelines/congress_scraper/`) wrote to the archived
SQLite `insiders.db`, while the product reads Postgres
`form4.congress_trades`. Those are different databases, so the scraper
could never have refreshed the site — congress data has been frozen at
2026-03-31 since the May 2026 Postgres cutover copied a stale snapshot
across. It was also never installed as a launchd job.

Rather than port a cron that fails silently, this lands the feed in the
dataplane where scheduling, retries, backfill, freshness SLA and lineage
come from the Signal contract instead of being hand-rolled per feed.

The HTTP fetch + HTML parse is reused verbatim from
`pipelines.congress_scraper.scrape_capitol_trades.scrape_page`, which has
no storage coupling — only the *sink* changes. That code ran clean in
production for months (final log 2026-04-02: 0 errors); it was the write
path and the scheduling that were wrong, not the scraping.

PIT semantics
-------------
`as_of_date` is the **disclosure (published) date**, not the trade date.
That is when the world could first have known. Under the STOCK Act a
member has up to 45 days to disclose, so trade_date routinely precedes
as_of_date by weeks — reading this signal at time T correctly returns
only what was public at T. trade_date travels in the payload for
analysis but must never be used as the observation timestamp.

Value payload:
    {
      "politician":     str,
      "party":          "R" | "D" | "I" | null,
      "chamber":        "House" | "Senate" | null,
      "state":          str | null,
      "trade_type":     "buy" | "sell" | "exchange" | ...,
      "trade_date":     "YYYY-MM-DD",       # when they traded
      "filing_date":    "YYYY-MM-DD",       # when it became public
      "value_low":      int | null,          # STOCK Act band floor
      "value_high":     int | null,          # band ceiling
      "value_estimate": int | null,          # band midpoint
      "owner":          str,                 # Self / Spouse / Child / Joint
      "company":        str | null,
      "report_url":     str | null
    }
"""
from __future__ import annotations

import hashlib
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from dataplane import Signal, SignalObservation

logger = logging.getLogger("dataplane.congress.trades_raw")

# The scraper lives in the trading-framework repo, outside the dataplane
# package. Import the fetch+parse function only — never its SQLite sink.
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _ensure_repo_on_path() -> None:
    """Put the repo root on sys.path, idempotently.

    Called again immediately before the deferred scraper import rather than
    relying solely on the module-level call. Dagster executes each op in a
    subprocess, and doing this once at module scope was not enough: the
    nightly daily_signals job failed with "No module named 'pipelines'" on
    both 2026-08-12 and 2026-08-13 while the same import succeeded from the
    CLI and from a plain interpreter. Cheap to repeat, and it removes the
    dependency on how the module happened to get loaded.
    """
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))


_ensure_repo_on_path()

# Capitol Trades pages newest-first, so a day partition is reached by
# walking pages until the page's newest row predates the target day.
MAX_PAGES = 40
PAGE_PAUSE_SECONDS = 1.0


class CongressTradesRawV1(Signal):
    """Per-event ingestion of congressional trade disclosures."""

    signal_id = "congress.trades.raw"
    version = "v1.0.0"
    owner = "derek"
    # Disclosures trickle in daily but are inherently bursty — a member can
    # dump 40 trades in one filing, then nothing for a week. 48h keeps the
    # freshness check meaningful without firing on a normal quiet weekend.
    sla_hours = 48.0
    business_hours_only = False
    description = "Congressional STOCK Act trade disclosures (Capitol Trades)."
    materialization_mode = "per_partition_events"
    # True external ingestion: no upstream dataplane signal.
    upstream = []
    output_schema = {
        "politician":     "text",
        "party":          "text",
        "chamber":        "text",
        "state":          "text",
        "trade_type":     "text",
        "trade_date":     "text",
        "filing_date":    "text",
        "value_low":      "bigint",
        "value_high":     "bigint",
        "value_estimate": "bigint",
        "owner":          "text",
        "company":        "text",
        "report_url":     "text",
    }

    def materialize_partition(self, partition_date: datetime) -> List[SignalObservation]:
        """Emit every disclosure published on the partition date."""
        _ensure_repo_on_path()
        from pipelines.congress_scraper.scrape_capitol_trades import scrape_page

        day = partition_date.strftime("%Y-%m-%d")
        observations: List[SignalObservation] = []
        seen: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            try:
                rows = scrape_page(page)
            except Exception as exc:
                # One bad page shouldn't void the partition. Log and stop —
                # a partial partition is re-materializable (upsert is safe).
                logger.warning("congress page %d failed for %s: %s", page, day, exc)
                break

            if not rows:
                break

            page_filing_dates = [r.get("filing_date") for r in rows if r.get("filing_date")]

            for r in rows:
                if r.get("filing_date") != day:
                    continue
                ticker = (r.get("ticker") or "").strip().upper()
                if not ticker:
                    # Bonds, funds and non-equity assets carry no ticker.
                    # SignalObservation is keyed on ticker, so they can't be
                    # represented here; they stay out of this signal.
                    continue

                fingerprint = _fingerprint(r)
                if fingerprint in seen:
                    continue          # same disclosure repeated across pages
                seen.add(fingerprint)

                obs = self.observation_from_row(r, page=page)
                if obs is not None:
                    observations.append(obs)

            # Pages run newest-first. Once the whole page predates the target
            # day there is nothing older to find for this partition.
            if page_filing_dates and max(page_filing_dates) < day:
                break

            time.sleep(PAGE_PAUSE_SECONDS)   # be a polite scraper

        logger.info("congress %s: %d disclosure(s)", day, len(observations))
        return observations

    def observation_from_row(self, row: dict, page: Optional[int] = None
                             ) -> Optional[SignalObservation]:
        """Convert one scraped disclosure into a SignalObservation.

        Shared by the daily partition materializer and the bulk backfill
        (`dataplane/scripts/backfill_congress.py`) so both paths emit
        byte-identical rows. If they diverged, a backfilled partition and a
        re-materialized one would disagree and the upsert would churn.
        """
        ticker = (row.get("ticker") or "").strip().upper()
        filing_date = row.get("filing_date")
        if not ticker or not filing_date:
            return None

        fingerprint = _fingerprint(row)
        return SignalObservation(
            signal_id=f"{self.signal_id}.{self.version}",
            ticker=ticker,
            as_of_date=_disclosure_timestamp(filing_date, fingerprint),
            value={
                "politician":     row.get("name"),
                "party":          row.get("party"),
                "chamber":        row.get("chamber"),
                "state":          row.get("state"),
                "trade_type":     row.get("trade_type"),
                "trade_date":     row.get("trade_date"),
                "filing_date":    filing_date,
                "value_low":      row.get("value_low"),
                "value_high":     row.get("value_high"),
                "value_estimate": row.get("value_estimate"),
                "owner":          row.get("owner"),
                "company":        row.get("company"),
                "report_url":     row.get("report_url"),
            },
            source_run_id=self._run_id,
            metadata={"source": "capitoltrades", **({"page": page} if page else {})},
        )


def _fingerprint(row: dict) -> str:
    """Stable identity for one disclosure.

    Capitol Trades exposes no per-trade ID, and one politician can file
    several trades in the same ticker on the same day (different lots or
    owners). Hash the fields that together identify a disclosure so re-runs
    are idempotent and same-day siblings stay distinct.
    """
    parts = [
        str(row.get("name") or ""),
        str(row.get("ticker") or ""),
        str(row.get("trade_type") or ""),
        str(row.get("trade_date") or ""),
        str(row.get("filing_date") or ""),
        str(row.get("value_low") or ""),
        str(row.get("value_high") or ""),
        str(row.get("owner") or ""),
    ]
    return hashlib.sha1("|".join(parts).encode()).hexdigest()


def _disclosure_timestamp(filing_date: str, fingerprint: str) -> datetime:
    """Disclosure date at end-of-day UTC, disambiguated sub-second.

    Capitol Trades gives a date with no clock time, so every disclosure on
    a given day would collapse onto one timestamp and collide on the
    (signal_id, ticker, as_of_date) primary key — silently losing rows on
    upsert. Offset the microsecond component by a deterministic hash of the
    row so siblings stay distinct. Same trick as insider.trades.raw.v1 uses
    for untimed filings: idempotent across re-runs, and sub-second so PIT
    semantics are unaffected.
    """
    base = datetime.strptime(filing_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    micros = int(fingerprint[:8], 16) % 1_000_000
    return base.replace(microsecond=micros)
