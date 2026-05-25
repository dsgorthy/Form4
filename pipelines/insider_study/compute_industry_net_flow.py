#!/usr/bin/env python3
"""Backfill trades.industry_buy_pct_90d (PIT).

For each P-trade T (ticker K, filing_date F):
  Look up K's sector S from ticker_metadata.
  industry_tickers = { all tickers in sector S, per ticker_metadata }

  current_pct = (distinct tickers in industry_tickers with ≥1 UNSCHEDULED P-trade
                 filed in [F-90d, F)) / |industry_tickers|

  For 12 quarter-end anchors Q in [F - 3y, F):
      pct_Q = same calc but ending at Q (strict < Q)
  baseline_median = median(pct_Q)

  industry_buy_pct_90d = current_pct - baseline_median

"Unscheduled" excludes is_10b5_1=1, is_recurring=1, is_tax_sale=1, cohen_routine=1
to match CEO Watcher's filtering.

CRITICAL PIT contract:
  - filing_date STRICTLY < anchor for every window
  - Anchors STRICTLY < F
  - Tickers with no sector classification → NULL output (we don't know the
    industry, can't compute the signal)
  - Sector membership uses today's ticker_metadata snapshot. This is a
    documented PIT approximation: companies that changed sectors mid-period
    are classified by today's sector. <5% of names per decade do this.

Test coverage: tests/unit/test_pit_validation.py::TestIndustryNetFlowPIT.

Usage:
  python3 -m pipelines.insider_study.compute_industry_net_flow
"""
from __future__ import annotations

import argparse
import bisect
import logging
import statistics
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.contracts.freshness_writer import write_freshness
from framework.observability import pipeline_run

# Shared helpers with the company-level backfill
from pipelines.insider_study.compute_company_net_flow import (
    _anchor_dates, WINDOW_DAYS, ANCHOR_QUARTERS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000


def _industry_pct_at(
    sector_events: list[tuple[date, str]],
    industry_size: int,
    anchor: date,
) -> float | None:
    """Compute (distinct tickers with ≥1 unscheduled buy in [anchor-90d, anchor))
    / industry_size. STRICT < anchor.

    sector_events: sorted list of (filing_date, ticker) for unscheduled P-trades
    in this sector across all time.

    Returns None if industry_size is 0 (defensive).
    """
    if industry_size <= 0:
        return None
    window_start = anchor - timedelta(days=WINDOW_DAYS)
    lo = bisect.bisect_left(sector_events, (window_start, ""))
    hi = bisect.bisect_left(sector_events, (anchor, ""))
    if lo >= hi:
        return 0.0
    distinct_tickers = {t for _, t in sector_events[lo:hi]}
    return len(distinct_tickers) / industry_size


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default=None,
                   help="Only UPDATE trades with filing_date >= this date")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    with pipeline_run(
        "compute_industry_net_flow",
        log_path="/Users/derekg/trading-framework/logs/compute-industry-net-flow.log",
    ) as prun:
        conn = get_connection()

        # Load sector mapping
        t0 = time.monotonic()
        sector_by_ticker: dict[str, str] = {}
        sector_members: dict[str, set[str]] = defaultdict(set)
        for r in conn.execute(
            "SELECT ticker, sector FROM ticker_metadata WHERE sector IS NOT NULL"
        ):
            sector_by_ticker[r["ticker"]] = r["sector"]
            sector_members[r["sector"]].add(r["ticker"])
        logger.info("Loaded %d ticker→sector mappings across %d sectors (%.1fs)",
                    len(sector_by_ticker), len(sector_members),
                    time.monotonic() - t0)

        if not sector_by_ticker:
            logger.error("ticker_metadata is empty — run refresh_ticker_metadata.py first")
            sys.exit(1)

        # Load all unscheduled P-trades (those are the events that feed the
        # numerator). We don't care about S-trades for the industry-buying
        # percentage signal.
        t1 = time.monotonic()
        all_events = conn.execute("""
            SELECT ticker, filing_date::text
            FROM trades
            WHERE trans_code = 'P'
              AND ticker IS NOT NULL AND ticker != 'NONE'
              AND filing_date IS NOT NULL
              AND COALESCE(is_10b5_1, 0) = 0
              AND COALESCE(is_recurring, 0) = 0
              AND COALESCE(is_tax_sale, 0) = 0
              AND COALESCE(cohen_routine, 0) = 0
            ORDER BY filing_date
        """).fetchall()
        logger.info("Loaded %d unscheduled P-trades in %.1fs",
                    len(all_events), time.monotonic() - t1)

        # Bucket by sector — each value is a sorted list of (date, ticker)
        events_by_sector: dict[str, list[tuple[date, str]]] = defaultdict(list)
        for r in all_events:
            ticker = r["ticker"]
            sector = sector_by_ticker.get(ticker)
            if not sector:
                continue
            try:
                fd = datetime.strptime(r["filing_date"][:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            events_by_sector[sector].append((fd, ticker))
        for s in events_by_sector:
            events_by_sector[s].sort()

        # Compute industry size once per sector
        industry_size = {s: len(members) for s, members in sector_members.items()}

        # Identify target trades — P-trades that need the column populated
        update_where = "AND filing_date >= %s" if args.since else ""
        params: tuple = (args.since,) if args.since else ()
        targets = conn.execute(
            f"""SELECT trade_id, ticker, filing_date::text
                FROM trades
                WHERE trans_code = 'P'
                  AND ticker IS NOT NULL AND ticker != 'NONE'
                  AND filing_date IS NOT NULL
                  {update_where.replace('%s', '?')}
                ORDER BY filing_date""",
            params,
        ).fetchall()
        if args.limit:
            targets = targets[: args.limit]
        logger.info("Trades to UPDATE: %d", len(targets))

        updates: list[tuple[float | None, int]] = []
        n_matched = 0
        n_no_sector = 0
        n_null_baseline = 0
        t2 = time.monotonic()

        for i, r in enumerate(targets, 1):
            ticker = r["ticker"]
            sector = sector_by_ticker.get(ticker)
            if not sector:
                n_no_sector += 1
                updates.append((None, r["trade_id"]))
                if len(updates) >= BATCH_SIZE:
                    conn.executemany(
                        "UPDATE trades SET industry_buy_pct_90d = ? WHERE trade_id = ?",
                        updates,
                    )
                    conn.commit()
                    updates = []
                continue

            try:
                F = datetime.strptime(r["filing_date"][:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                updates.append((None, r["trade_id"]))
                continue

            sec_events = events_by_sector.get(sector, [])
            isize = industry_size.get(sector, 0)

            current_pct = _industry_pct_at(sec_events, isize, F)
            if current_pct is None:
                updates.append((None, r["trade_id"]))
                continue

            anchor_pcts = []
            for anchor in _anchor_dates(F, n=ANCHOR_QUARTERS):
                val = _industry_pct_at(sec_events, isize, anchor)
                if val is not None:
                    anchor_pcts.append(val)

            if len(anchor_pcts) < ANCHOR_QUARTERS // 2:
                n_null_baseline += 1
                updates.append((None, r["trade_id"]))
                continue

            baseline_median = statistics.median(anchor_pcts)
            signal = current_pct - baseline_median
            updates.append((float(signal), r["trade_id"]))
            n_matched += 1

            if len(updates) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE trades SET industry_buy_pct_90d = ? WHERE trade_id = ?",
                    updates,
                )
                conn.commit()
                updates = []
                elapsed = time.monotonic() - t2
                logger.info(
                    "  %d/%d  matched=%d no_sector=%d null_baseline=%d  elapsed=%.0fs",
                    i, len(targets), n_matched, n_no_sector, n_null_baseline, elapsed,
                )

        if updates:
            conn.executemany(
                "UPDATE trades SET industry_buy_pct_90d = ? WHERE trade_id = ?",
                updates,
            )
            conn.commit()

        if n_matched > 0:
            write_freshness(
                conn,
                table="trades",
                column="industry_buy_pct_90d",
                n_rows_affected=n_matched,
                populated_by="pipelines/insider_study/compute_industry_net_flow.py",
            )
            conn.commit()

        conn.close()
        elapsed_total = time.monotonic() - t0
        logger.info("Done in %.1fs. matched=%d no_sector=%d null_baseline=%d total=%d",
                    elapsed_total, n_matched, n_no_sector, n_null_baseline, len(targets))

        prun.set_rows_written(n_matched)
        prun.set_metadata({
            "matched": n_matched,
            "no_sector": n_no_sector,
            "null_baseline": n_null_baseline,
            "total": len(targets),
            "since": args.since,
            "sectors_loaded": len(industry_size),
        })


if __name__ == "__main__":
    main()
