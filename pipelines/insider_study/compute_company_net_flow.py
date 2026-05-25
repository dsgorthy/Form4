#!/usr/bin/env python3
"""Backfill trades.net_buyer_flow_90d (PIT).

For each P-trade T (ticker K, filing_date F):
  current_net_90d  = (distinct buyers in [F-90d, F)) - (distinct sellers in [F-90d, F))
  For 12 quarter-end anchors Q in [F - 3y, F):
      net_Q        = same window calc ending at Q (strict <)
  baseline_median  = median(net_Q over the 12 anchors)
  net_buyer_flow_90d = current_net_90d - baseline_median

CRITICAL PIT contract:
  - All windows use filing_date STRICTLY LESS THAN the anchor date.
    Excludes the current trade and any same-day filings from its own signal.
  - Anchors are quarter-ends prior to F. No future data.
  - Tickers with <3y of activity → NULL output.

Algorithm: O(events + queries) per ticker via sweep-line.
  Sort all events and all query points by date. Walk events forward through
  a 90-day sliding window. Per-insider counters track distinct buyers/sellers
  in O(1) per admission/expiration. Queries are answered in O(1) at each
  query date. Total work is linear in (events + queries) per ticker.

Test coverage: tests/unit/test_pit_validation.py::TestNetFlowSignalsPIT.

Usage:
  python3 -m pipelines.insider_study.compute_company_net_flow
  python3 -m pipelines.insider_study.compute_company_net_flow --since 2023-01-01
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WINDOW_DAYS = 90
ANCHOR_QUARTERS = 12
BATCH_SIZE = 5_000


_QEND_CANDIDATES_IN_YEAR = ((3, 31), (6, 30), (9, 30), (12, 31))


def _last_quarter_end_before(d: date) -> date:
    """Return the LARGEST quarter-end strictly less than d."""
    valid = [date(d.year, m, day) for (m, day) in _QEND_CANDIDATES_IN_YEAR
             if date(d.year, m, day) < d]
    if valid:
        return max(valid)
    return date(d.year - 1, 12, 31)


def _previous_quarter_end(qe: date) -> date:
    """Given a quarter-end date, return the immediately-preceding quarter-end."""
    if qe.month == 3:
        return date(qe.year - 1, 12, 31)
    if qe.month == 6:
        return date(qe.year, 3, 31)
    if qe.month == 9:
        return date(qe.year, 6, 30)
    if qe.month == 12:
        return date(qe.year, 9, 30)
    raise ValueError(f"not a quarter-end: {qe}")


def _anchor_dates(filing_date: date, n: int = ANCHOR_QUARTERS) -> list[date]:
    """Return n quarter-end anchors STRICTLY BEFORE filing_date, walking
    backward one quarter at a time. PIT guarantee: every returned date
    satisfies date < filing_date.

    Bug fix 2026-05-25: original loop used a fuzzy "step back 60 days then
    snap to quarter-end" which non-monotonically converged when filing_date
    was itself a quarter-end (infinite loop) or in the first few weeks of
    a quarter (anchors got stuck on the same prior quarter-end). Now uses
    explicit enumeration which is guaranteed monotonic.
    """
    cur = _last_quarter_end_before(filing_date)
    anchors = [cur]
    for _ in range(n - 1):
        cur = _previous_quarter_end(cur)
        anchors.append(cur)
    return anchors


def _sweep_compute(
    events: list[tuple[date, int, str]],
    queries: list[tuple[date, int, int]],
) -> dict[tuple[int, int], int]:
    """Compute net flow at each query date for one ticker, in linear time.

    events: sorted by date. Each is (filing_date, insider_id, trans_code).
    queries: list of (query_date, trade_id, anchor_index). Will be sorted here.

    Returns: dict mapping (trade_id, anchor_index) → net_flow_int.

    A query is answered at the moment the sweep pointer first reaches the
    query date. The window is [query_date - 90d, query_date) STRICTLY, so:
      - On entry: an event at fd is admitted just before any query at fd
        (we want the event to be in the window when query_date > fd; the
        loop admits events with fd < q before answering query q).
      - On expiry: an event at fd expires when query_date - 90d > fd
        (i.e., fd < query_date - 90d). Since the window is [q-90, q)
        half-open at the bottom — events at fd == q-90 are STILL in window.
    """
    queries_sorted = sorted(queries)

    buyer_counts: dict[int, int] = defaultdict(int)
    seller_counts: dict[int, int] = defaultdict(int)
    distinct_buyers = 0
    distinct_sellers = 0

    right_ptr = 0  # index of next event to admit
    left_ptr = 0   # index of oldest event still in window

    out: dict[tuple[int, int], int] = {}

    for q_date, trade_id, anchor_idx in queries_sorted:
        window_start_excl = q_date - timedelta(days=WINDOW_DAYS)

        # Admit events with fd < q_date — they BELONG in the [q-90, q) window
        # if their fd is also >= q-90d.
        while right_ptr < len(events) and events[right_ptr][0] < q_date:
            fd, iid, tc = events[right_ptr]
            if fd >= window_start_excl:   # admit only if in window
                if tc == "P":
                    buyer_counts[iid] += 1
                    if buyer_counts[iid] == 1:
                        distinct_buyers += 1
                elif tc == "S":
                    seller_counts[iid] += 1
                    if seller_counts[iid] == 1:
                        distinct_sellers += 1
            right_ptr += 1

        # Expire events whose fd is no longer in [q-90, q) — i.e., fd < q-90d.
        while left_ptr < right_ptr and events[left_ptr][0] < window_start_excl:
            fd, iid, tc = events[left_ptr]
            if tc == "P":
                buyer_counts[iid] -= 1
                if buyer_counts[iid] == 0:
                    distinct_buyers -= 1
            elif tc == "S":
                seller_counts[iid] -= 1
                if seller_counts[iid] == 0:
                    distinct_sellers -= 1
            left_ptr += 1

        out[(trade_id, anchor_idx)] = distinct_buyers - distinct_sellers

    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default=None,
                   help="Only UPDATE trades with filing_date >= this date")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    with pipeline_run(
        "compute_company_net_flow",
        log_path="/Users/derekg/trading-framework/logs/compute-company-net-flow.log",
    ) as prun:
        conn = get_connection()

        # Load every P/S trade (window context needed regardless of --since)
        t0 = time.monotonic()
        rows = conn.execute("""
            SELECT trade_id, ticker, filing_date::text AS fd, insider_id, trans_code
            FROM trades
            WHERE trans_code IN ('P','S')
              AND ticker IS NOT NULL AND ticker != 'NONE'
              AND filing_date IS NOT NULL
            ORDER BY ticker, filing_date
        """).fetchall()
        logger.info("Loaded %d P/S trades in %.1fs", len(rows), time.monotonic() - t0)

        events_by_ticker: dict[str, list[tuple[date, int, str]]] = defaultdict(list)
        for r in rows:
            try:
                fd = datetime.strptime(r["fd"][:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            events_by_ticker[r["ticker"]].append((fd, r["insider_id"], r["trans_code"]))
        for t in events_by_ticker:
            events_by_ticker[t].sort()

        # Identify UPDATE targets — P-trades only
        update_where = "AND filing_date >= %s" if args.since else ""
        params: tuple = (args.since,) if args.since else ()
        targets = conn.execute(
            f"""SELECT trade_id, ticker, filing_date::text AS fd
                FROM trades
                WHERE trans_code = 'P'
                  AND ticker IS NOT NULL AND ticker != 'NONE'
                  AND filing_date IS NOT NULL
                  {update_where.replace('%s', '?')}
                ORDER BY ticker, filing_date""",
            params,
        ).fetchall()
        targets_list = []
        for r in targets:
            try:
                fd = datetime.strptime(r["fd"][:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            targets_list.append((r["ticker"], r["trade_id"], fd))
        if args.limit:
            targets_list = targets_list[: args.limit]
        logger.info("Trades to UPDATE: %d (across %d tickers)",
                    len(targets_list),
                    len({t[0] for t in targets_list}))

        # Group targets by ticker
        targets_by_ticker: dict[str, list[tuple[int, date]]] = defaultdict(list)
        for ticker, trade_id, fd in targets_list:
            targets_by_ticker[ticker].append((trade_id, fd))

        # Process each ticker — sweep-line per ticker, results merged
        updates: list[tuple[float | None, int]] = []
        n_matched = 0
        n_null_baseline = 0
        n_no_events = 0
        t1 = time.monotonic()
        n_tickers_done = 0
        logger.info("Starting per-ticker sweep over %d tickers...", len(targets_by_ticker))

        for ticker, ticker_targets in targets_by_ticker.items():
            events = events_by_ticker.get(ticker, [])
            if not events:
                for trade_id, _ in ticker_targets:
                    updates.append((None, trade_id))
                    n_no_events += 1
                n_tickers_done += 1
                continue

            # Build query list: per trade, 1 "current" + 12 anchor queries
            queries: list[tuple[date, int, int]] = []
            for trade_id, fd in ticker_targets:
                queries.append((fd, trade_id, -1))   # -1 = current
                for i, anchor in enumerate(_anchor_dates(fd)):
                    queries.append((anchor, trade_id, i))

            # Sweep
            results = _sweep_compute(events, queries)

            # Compose per-trade signal
            for trade_id, fd in ticker_targets:
                current_net = results.get((trade_id, -1))
                if current_net is None:
                    updates.append((None, trade_id))
                    continue
                anchor_nets = [
                    results[(trade_id, i)]
                    for i in range(ANCHOR_QUARTERS)
                    if (trade_id, i) in results
                ]
                if len(anchor_nets) < ANCHOR_QUARTERS // 2:
                    n_null_baseline += 1
                    updates.append((None, trade_id))
                    continue
                baseline = statistics.median(anchor_nets)
                updates.append((float(current_net - baseline), trade_id))
                n_matched += 1

            n_tickers_done += 1
            # Per-ticker progress log every 100 tickers
            if n_tickers_done % 100 == 0:
                elapsed = time.monotonic() - t1
                logger.info("  ticker progress: %d/%d  matched=%d  elapsed=%.0fs",
                            n_tickers_done, len(targets_by_ticker), n_matched, elapsed)

            # Batch commit
            if len(updates) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE trades SET net_buyer_flow_90d = ? WHERE trade_id = ?",
                    updates,
                )
                conn.commit()
                updates = []
                elapsed = time.monotonic() - t1
                logger.info(
                    "  tickers %d/%d  matched=%d null_baseline=%d no_events=%d  elapsed=%.0fs",
                    n_tickers_done, len(targets_by_ticker),
                    n_matched, n_null_baseline, n_no_events, elapsed,
                )

        if updates:
            conn.executemany(
                "UPDATE trades SET net_buyer_flow_90d = ? WHERE trade_id = ?",
                updates,
            )
            conn.commit()

        if n_matched > 0:
            write_freshness(
                conn,
                table="trades",
                column="net_buyer_flow_90d",
                n_rows_affected=n_matched,
                populated_by="pipelines/insider_study/compute_company_net_flow.py",
            )
            conn.commit()

        conn.close()
        elapsed_total = time.monotonic() - t0
        logger.info("Done in %.1fs. matched=%d null_baseline=%d no_events=%d total=%d",
                    elapsed_total, n_matched, n_null_baseline, n_no_events, len(targets_list))

        prun.set_rows_written(n_matched)
        prun.set_metadata({
            "matched": n_matched,
            "null_baseline": n_null_baseline,
            "no_events": n_no_events,
            "total": len(targets_list),
            "since": args.since,
        })


if __name__ == "__main__":
    main()
