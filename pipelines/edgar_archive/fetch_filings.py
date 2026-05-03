#!/usr/bin/env python3
"""Download SEC EDGAR filings to disk using the resumable-puller framework.

Input is a list of FilingRow dicts (from `fetch_form_index.py`). Each filing's
all-in-one .txt submission is fetched once, then stored under:

    {paths.edgar_filings}/{form_type_safe}/{cik}/{accession}.txt

Resume is by file presence on disk. Failed fetches are recorded in pull_status
with status='failed' so they can be retried separately.

EDGAR fair-use:
  * <= 10 RPS (we cap at 8 to stay safe)
  * User-Agent must include name + email

Smoke test:
    python3 pipelines/edgar_archive/fetch_filings.py --year 2026 --quarter 1 \\
        --forms 8-K --limit 5 --rate 4
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.storage_paths import paths
from pipelines._lib.resumable_puller import ResumablePuller
from pipelines.edgar_archive.fetch_form_index import (
    DEFAULT_FORMS, USER_AGENT, FilingRow, fetch_quarters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def _safe_form(form_type: str) -> str:
    """Filesystem-safe form type. '8-K/A' → '8-K_A', 'SC 13D' → 'SC_13D'."""
    return re.sub(r'[^A-Za-z0-9._-]+', '_', form_type)


def filing_output_path(row: FilingRow) -> Path:
    return (
        paths.edgar_filings
        / _safe_form(row.form_type)
        / row.cik
        / f"{row.accession_number}.txt"
    )


class EdgarFilingPuller(ResumablePuller):
    dataset = "edgar_filings"
    description = "Phase 1 #2 — SEC EDGAR full archive (8-K, 10-K, 10-Q, 13F, 13D/G, 5, 144)"
    storage_root = paths.edgar_filings

    def item_key(self, item: dict) -> str:
        row: FilingRow = item["row"]
        return row.accession_number

    def fetch_item(self, session: requests.Session, item: dict) -> Optional[bytes]:
        row: FilingRow = item["row"]
        url = row.submission_txt_url
        r = session.get(url, timeout=60)
        if r.status_code == 404:
            logger.warning("404 (filing missing): %s", url)
            return None
        r.raise_for_status()
        return r.content

    def write_item(self, item: dict, fetched: bytes) -> tuple[int, int]:
        row: FilingRow = item["row"]
        outfile = filing_output_path(row)
        outfile.parent.mkdir(parents=True, exist_ok=True)
        tmp = outfile.with_suffix(".txt.tmp")
        tmp.write_bytes(fetched)
        os.replace(tmp, outfile)
        return 1, len(fetched)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, nargs="+", required=True)
    p.add_argument("--quarter", type=int, nargs="+", default=[1, 2, 3, 4])
    p.add_argument("--forms", nargs="+", default=DEFAULT_FORMS)
    p.add_argument("--limit", type=int, help="Max filings to fetch (smoke test)")
    p.add_argument("--rate", type=float, default=8,
                   help="Max requests/sec (EDGAR cap is 10; default 8)")
    p.add_argument("--cik", help="Filter to one CIK (smoke test)")
    args = p.parse_args()

    rows = fetch_quarters(args.year, args.quarter, only_forms=args.forms)
    if args.cik:
        rows = [r for r in rows if r.cik == args.cik.lstrip("0")
                or r.cik.lstrip("0") == args.cik.lstrip("0")]
    if args.limit:
        rows = rows[:args.limit]

    logger.info("preparing to fetch %d filings → %s", len(rows), paths.edgar_filings)
    logger.info("rate cap: %.1f rps  ·  user-agent: %s", args.rate, USER_AGENT)

    items = [{"row": row} for row in rows]
    EdgarFilingPuller(
        items,
        session_headers={"User-Agent": USER_AGENT},
        rate_limit_per_sec=args.rate,
        progress_every_n=25,
        manifest_every_n=200,
        completion_strategy="disk",
        disk_marker=lambda it: filing_output_path(it["row"]),
    ).run()


if __name__ == "__main__":
    main()
