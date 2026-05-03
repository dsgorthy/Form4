#!/usr/bin/env python3
"""Pull FINRA Reg SHO daily short-volume files.

FINRA publishes one consolidated file per trading day at:
    https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt

Format (pipe-delimited, no quoting):
    Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

This is *daily short volume* (a flow metric), not the bi-monthly
*short interest* report (a stock metric). Short interest comes from a
different source (finra.org/data/short-interest) and is also pulled
here when available.

Output:
    {paths.short_metrics}/regsho_daily/{YYYY-MM-DD}.parquet
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.storage_paths import paths
from pipelines._lib.resumable_puller import ResumablePuller

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

FINRA_BASE = "https://cdn.finra.org/equity/regsho/daily"

# FINRA publishes daily short-volume files keyed by venue prefix:
#   CNMS — Consolidated NMS (NYSE/Nasdaq/AMEX/ARCA listed)        ← default
#   FORF — OTC Reporting Facility (legitimate OTC equity volume)  ← OTC tier
#   FNRA — OTC ATS (current files often empty; deprecated tier)
#   FNQC / FNSQ / FNYX — Nasdaq/NYSE TRF tier files (per-venue rollups
#                        already aggregated into CNMS)
# CNMS is what 99% of Form4 work needs. FORF adds OTC equities — useful for
# squeeze setups in micro-caps. The tier files (FNQC etc.) are redundant
# with CNMS for analytical purposes.
FEED_PREFIXES = {
    "CNMS": "regsho_daily",        # default NMS-listed; existing path
    "FORF": "regsho_daily_otc",    # OTC equity, separate stream
}

REGSHO_SCHEMA = pa.schema([
    ("date",                 pa.string()),       # YYYY-MM-DD
    ("symbol",               pa.string()),
    # FINRA publishes fractional shares (some venues report aggregated
    # algo executions as decimals). int64 truncates ~3K shares/day silently.
    ("short_volume",         pa.float64()),
    ("short_exempt_volume",  pa.float64()),
    ("total_volume",         pa.float64()),
    ("market",               pa.string()),
])


def trading_days(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def output_path(day: date, feed: str = "CNMS") -> Path:
    subdir = FEED_PREFIXES[feed]
    return paths.short_metrics / subdir / f"{day.isoformat()}.parquet"


class RegShoDailyPuller(ResumablePuller):
    """One pull-item = one (date, feed) tuple. CNMS is the default (NMS-listed
    consolidated volume); pass feed='FORF' for OTC equities."""
    description = "Phase 1 #4 — FINRA Reg SHO daily short volume (per-feed)"

    def __init__(self, items, *, feed: str = "CNMS", **kwargs):
        if feed not in FEED_PREFIXES:
            raise ValueError(f"unknown FINRA feed {feed!r}; expected one of {list(FEED_PREFIXES)}")
        self.feed = feed
        self.dataset = f"finra_regsho_{feed.lower()}"
        self.storage_root = paths.short_metrics / FEED_PREFIXES[feed]
        super().__init__(items, **kwargs)

    def item_key(self, item: dict) -> str:
        return item["day"].isoformat()

    def fetch_item(self, session: requests.Session, item: dict) -> Optional[bytes]:
        day: date = item["day"]
        url = f"{FINRA_BASE}/{self.feed}shvol{day.strftime('%Y%m%d')}.txt"
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            logger.info("%s [%s]: not posted (holiday/weekend or feed inactive)",
                        day, self.feed)
            return None
        r.raise_for_status()
        # Empty FINRA file (header + "0" trailer only): treat as no data.
        if len(r.content) < 100:
            return None
        return r.content

    def write_item(self, item: dict, fetched: bytes) -> tuple[int, int]:
        day: date = item["day"]
        outfile = output_path(day, feed=self.feed)
        outfile.parent.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(
            io.BytesIO(fetched),
            sep="|",
            header=0,
            names=["date_raw", "symbol", "short_volume", "short_exempt_volume",
                   "total_volume", "market"],
            dtype={"date_raw": "string", "symbol": "string", "market": "string"},
            # CRITICAL: keep_default_na=False so the symbol "NA" (Nano Labs)
            # parses as the string "NA" not NaN, which silently dropped one
            # ticker per day in earlier pulls.
            keep_default_na=False,
            na_values=[""],
            on_bad_lines="warn",
        )
        # Footer line in some FINRA files: starts with "File Trailer"
        df = df[df["date_raw"].str.match(r"^\d{8}$", na=False)]

        df["date"] = (
            df["date_raw"].str.slice(0, 4) + "-"
            + df["date_raw"].str.slice(4, 6) + "-"
            + df["date_raw"].str.slice(6, 8)
        )
        df = df[["date", "symbol", "short_volume", "short_exempt_volume",
                 "total_volume", "market"]]
        # Preserve fractional shares — schema is float64 because FINRA
        # publishes decimals from venue-aggregated reporting.
        df = df.astype({"short_volume": "float64", "short_exempt_volume": "float64",
                        "total_volume": "float64"})

        table = pa.Table.from_pandas(df, schema=REGSHO_SCHEMA, preserve_index=False)
        tmp = outfile.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        os.replace(tmp, outfile)
        return len(df), outfile.stat().st_size


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, help="Last N trading days from yesterday")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD (inclusive)")
    p.add_argument("--rate-limit", type=float, default=2)
    p.add_argument("--feeds", nargs="+", default=["CNMS"],
                   choices=list(FEED_PREFIXES),
                   help="FINRA feeds to pull. CNMS (NMS-listed) is the default; "
                        "add FORF to also capture OTC short volume.")
    args = p.parse_args()

    if args.days:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=args.days * 2)
        days = trading_days(start, end)[-args.days:]
    elif args.start and args.end:
        days = trading_days(
            datetime.strptime(args.start, "%Y-%m-%d").date(),
            datetime.strptime(args.end, "%Y-%m-%d").date(),
        )
    else:
        end = date.today() - timedelta(days=1)
        days = trading_days(end - timedelta(days=10), end)[-3:]

    items = [{"day": d} for d in days]

    for feed in args.feeds:
        logger.info("FINRA Reg SHO [%s] daily pull: %d days (%s → %s)",
                    feed, len(items), days[0], days[-1])
        RegShoDailyPuller(
            items,
            feed=feed,
            rate_limit_per_sec=args.rate_limit,
            progress_every_n=5,
            manifest_every_n=20,
            completion_strategy="disk",
            disk_marker=lambda it, f=feed: output_path(it["day"], feed=f),
        ).run()


if __name__ == "__main__":
    main()
