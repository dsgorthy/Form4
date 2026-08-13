#!/usr/bin/env python3
"""Bulk backfill for congress.trades.raw.v1.

Why this exists instead of a Dagster partition backfill
-------------------------------------------------------
Capitol Trades paginates newest-first with no date filter, so
`materialize_partition(day)` has to walk pages from the top until it passes
the target day. That is fine for the daily run (the target is always page
1-2) but quadratic for a backfill: 134 partitions x up to 40 pages each is
thousands of requests, most of them re-fetching the same pages, and rude to
a site we do not pay for.

This walks the pagination ONCE, buckets every disclosure by its published
date, and writes them all. ~2.9 pages/day of history at 12 rows/page, so
the 2026-03-31 -> today gap is roughly 380 requests.

Rows are converted via CongressTradesRawV1.observation_from_row(), the same
method the Dagster asset uses, so a backfilled partition and a later
re-materialized one produce identical rows and the upsert is a no-op.

Usage (on Studio):
    python backfill_congress.py --start 2026-03-31            # dry run
    python backfill_congress.py --start 2026-03-31 --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

_DATAPLANE = Path(__file__).resolve().parents[1]
_REPO = _DATAPLANE.parent
for p in (str(_DATAPLANE), str(_REPO)):
    if p not in sys.path:
        sys.path.insert(0, p)

import os  # noqa: E402
import psycopg2  # noqa: E402

from dataplane.catalog import register, write_observation  # noqa: E402
from signals.congress.trades_raw_v1 import CongressTradesRawV1  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("backfill_congress")

# Capitol Trades soft-rate-limits sustained pagination: a fast walk starts
# returning empty pages around page ~37 while the SAME page served 12 rows
# moments earlier in isolation. Empty therefore means "slow down", not "end
# of data" — hence the patient default pause and generous retries.
PAGE_PAUSE_SECONDS = 4.0
HARD_PAGE_CAP = 1200          # backstop; ~4 years of history
EMPTY_PAGE_RETRIES = 5        # an empty page is throttling, not the end
DRY_PAGES_BEFORE_STOP = 8     # consecutive pages with nothing in-window


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True,
                    help="Earliest published date to keep (YYYY-MM-DD)")
    ap.add_argument("--end", default=None,
                    help="Latest published date to keep (default: today UTC)")
    ap.add_argument("--apply", action="store_true",
                    help="Write observations (default: dry run)")
    ap.add_argument("--max-pages", type=int, default=HARD_PAGE_CAP)
    ap.add_argument("--dsn", default=None, help="override PYRRHO_DATAPLANE_DSN")
    ap.add_argument("--pause", type=float, default=PAGE_PAUSE_SECONDS,
                    help="seconds between page fetches")
    ap.add_argument("--start-page", type=int, default=1,
                    help="resume the walk at this page")
    ap.add_argument("--cooldown-every", type=int, default=30,
                    help="pages per burst before a cooldown (0 disables)")
    ap.add_argument("--cooldown-seconds", type=float, default=150.0,
                    help="idle seconds between bursts; the throttle is a "
                         "rolling request budget that resets on idle")
    args = ap.parse_args()

    from pipelines.congress_scraper.scrape_capitol_trades import scrape_page

    start = args.start
    end = args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    sig = CongressTradesRawV1()
    by_day: dict[str, list[dict]] = defaultdict(list)
    seen_fingerprints: set[str] = set()
    pages_walked = 0
    dry_streak = 0

    log.info("walking Capitol Trades until published < %s", start)
    for page in range(args.start_page, args.start_page + args.max_pages):
        # An empty page is usually a transient hiccup or soft rate-limit, NOT
        # the end of pagination — capitoltrades serves data well past the
        # point where a single empty response shows up. Treating the first
        # empty page as the end silently truncates the backfill and looks
        # like success, so retry before believing it.
        rows = []
        for attempt in range(1, EMPTY_PAGE_RETRIES + 1):
            try:
                rows = scrape_page(page)
            except Exception as exc:  # noqa: BLE001
                log.warning("page %d attempt %d failed: %s", page, attempt, exc)
                rows = []
            if rows:
                break
            if attempt < EMPTY_PAGE_RETRIES:
                # Empty == throttled. Verified: a page that returns empty
                # mid-walk serves 12 rows moments later once the burst stops.
                # So wait out the rolling window rather than giving up.
                backoff = max(args.cooldown_seconds, args.pause * 10) * attempt
                log.info("  page %d empty (attempt %d) — cooling %.0fs",
                         page, attempt, backoff)
                time.sleep(backoff)
        pages_walked = page
        if not rows:
            log.info("page %d empty after %d attempts — treating as end of pagination",
                     page, EMPTY_PAGE_RETRIES)
            break

        published = [r.get("filing_date") for r in rows if r.get("filing_date")]
        added_this_page = 0
        for r in rows:
            fd = r.get("filing_date")
            if not fd or fd < start or fd > end:
                continue
            key = f"{fd}|{r.get('name')}|{r.get('ticker')}|{r.get('trade_date')}|{r.get('value_low')}|{r.get('owner')}|{r.get('trade_type')}"
            if key in seen_fingerprints:
                continue
            seen_fingerprints.add(key)
            by_day[fd].append(r)
            added_this_page += 1

        if page % 25 == 0:
            log.info("  page %d — %d day(s), %d disclosure(s) so far",
                     page, len(by_day), sum(len(v) for v in by_day.values()))

        # Deep pages are not strictly monotonic (page 40 and page 60 both
        # surfaced 2026-07-22), so a single out-of-window page is not proof
        # we are done. Require a run of pages with nothing new in-window.
        if added_this_page == 0 and published and max(published) < start:
            dry_streak += 1
            if dry_streak >= DRY_PAGES_BEFORE_STOP:
                log.info("page %d — %d consecutive pages past %s, walk complete",
                         page, dry_streak, start)
                break
        elif added_this_page:
            dry_streak = 0
        if args.cooldown_every and (page - args.start_page + 1) % args.cooldown_every == 0:
            log.info("  burst of %d pages done — cooling %.0fs",
                     args.cooldown_every, args.cooldown_seconds)
            time.sleep(args.cooldown_seconds)
        else:
            time.sleep(args.pause)

    total_rows = sum(len(v) for v in by_day.values())
    log.info("walked %d page(s): %d disclosure(s) across %d day(s), %s..%s",
             pages_walked, total_rows, len(by_day),
             min(by_day) if by_day else "-", max(by_day) if by_day else "-")

    if not args.apply:
        top = Counter({d: len(v) for d, v in by_day.items()}).most_common(8)
        log.info("DRY RUN — busiest days: %s", top)
        log.info("re-run with --apply to write")
        return 0

    # Same DSN convention as dataplane.status / expand_price_universe.
    dsn = args.dsn or os.environ.get("PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost")
    conn = psycopg2.connect(dsn)
    register(conn, CongressTradesRawV1)
    written = skipped = errors = 0
    for day in sorted(by_day):
        for r in by_day[day]:
            obs = sig.observation_from_row(r)
            if obs is None:
                skipped += 1
                continue
            try:
                write_observation(conn, obs)
                written += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if errors <= 5:
                    log.warning("write failed %s/%s: %s", r.get("ticker"), day, exc)
    conn.commit()
    conn.close()
    log.info("DONE — written=%d skipped=%d errors=%d", written, skipped, errors)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
