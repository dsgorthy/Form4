#!/usr/bin/env python3
"""Download SEC EDGAR quarterly form indexes and parse them to filing rows.

EDGAR publishes one master/form/company index per quarter:
  https://www.sec.gov/Archives/edgar/full-index/{YYYY}/QTR{1-4}/form.idx

Each line is fixed-width:
  Form Type  Company Name                                  CIK         Date Filed  File Name

This script downloads the form.idx files we don't have and parses them into
a Python list of dicts. Used as the seed list for `fetch_filings.py`.

EDGAR fair-use rules:
  * 10 requests/second max per IP
  * User-Agent must include name + contact email
  * https://www.sec.gov/os/accessing-edgar-data
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.storage_paths import paths
from pipelines._lib.resumable_puller import make_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

EDGAR_BASE = "https://www.sec.gov/Archives/edgar/full-index"
USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT",
    "Form4 Trading Framework derek@sidequestgroup.com",
)


@dataclass(frozen=True)
class FilingRow:
    form_type: str
    company_name: str
    cik: str
    filed_date: str          # YYYY-MM-DD
    file_path: str           # e.g. edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm

    @property
    def accession_number(self) -> str:
        # Last segment of file_path before -index.htm; format 0000123456-YY-NNNNNN
        m = re.search(r'(\d{10}-\d{2}-\d{6})', self.file_path)
        return m.group(1) if m else self.file_path

    @property
    def primary_doc_url(self) -> str:
        return f"https://www.sec.gov/Archives/{self.file_path}"

    @property
    def submission_txt_url(self) -> str:
        """Concatenated all-in-one submission file. One HTTP fetch covers
        the entire filing including exhibits."""
        accession = self.accession_number
        accession_nodash = accession.replace("-", "")
        return (
            f"https://www.sec.gov/Archives/edgar/data/{int(self.cik)}/"
            f"{accession_nodash}/{accession}.txt"
        )


def quarter_index_path(year: int, qtr: int) -> Path:
    return paths.edgar_indexes / f"{year}_QTR{qtr}_form.idx"


def fetch_quarter_index(session: requests.Session, year: int, qtr: int,
                        force: bool = False) -> Path:
    """Download form.idx for one quarter into the indexes dir. Idempotent."""
    out = quarter_index_path(year, qtr)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and not force and out.stat().st_size > 1000:
        logger.info("cached: %s (%d bytes)", out, out.stat().st_size)
        return out
    url = f"{EDGAR_BASE}/{year}/QTR{qtr}/form.idx"
    logger.info("fetching %s", url)
    r = session.get(url, timeout=60)
    r.raise_for_status()
    tmp = out.with_suffix(".idx.tmp")
    tmp.write_bytes(r.content)
    os.replace(tmp, out)
    return out


# form.idx is fixed-width. SEC docs claim Form Type is positions 1-12, but
# observed files use 17-char form-type column (e.g. "1-A POS          " is
# 17 chars). Slicing at 12 still works for short common forms but truncates
# longer ones. We slice generously and let the row-builder decide.
#
# Header (skipped): 11 lines
#
# Canonical form-type names per recent EDGAR are:
#   SCHEDULE 13D  / SCHEDULE 13D/A
#   SCHEDULE 13G  / SCHEDULE 13G/A
# Older filings or amendments occasionally appear as `SC 13D` / `SC 13D/A`.
# The alias map below normalizes the user-supplied filter so any of these
# variants resolve to the same canonical bucket.

# user_input → set of form-types to keep (each lowercased upper-trimmed)
FORM_ALIASES: dict[str, set[str]] = {
    "SC 13D":     {"SC 13D", "SCHEDULE 13D"},
    "SC 13G":     {"SC 13G", "SCHEDULE 13G"},
    "SCHEDULE 13D": {"SC 13D", "SCHEDULE 13D"},
    "SCHEDULE 13G": {"SC 13G", "SCHEDULE 13G"},
    # Late-filing notifications: same base form, "NT" prefix variant.
    # Per SEC, these are filed when an issuer needs more time and often
    # signal accounting / audit issues — material on their own.
    "NT 10-K":    {"NT 10-K"},
    "NT 10-Q":    {"NT 10-Q"},
}

# Default keep set covers the Form4 plan (Phase 1 #2) plus several closely
# related filings the audit surfaced as legitimate but missing:
#   * Form 3      — initial statement of beneficial ownership (paired with
#                    Form 4 in clustering analyses, ~21K/quarter)
#   * 6-K          — foreign-issuer current report (the ~7K/quarter
#                    equivalent of an 8-K for cross-listed names)
#   * NT 10-K / NT 10-Q — late-filing notifications (often signal financial
#                    or audit problems)
#   * DEF 14A      — proxy statements (executive comp, board changes;
#                    relevant context for tenb51_surprise + insider work)
#
# Deliberately NOT in the default set:
#   * Form 4 / 4/A — already ingested via insider_catalog pipeline
#   * NPORT-P, D, 424B*, FWP, 497*, etc. — out of scope for Form4
#   * 8-K12B, 8-K12G3 — narrow registration variants, low-value
#   * 20-F / 40-F  — foreign annual filings, low Form4-universe overlap
DEFAULT_FORMS = [
    "8-K", "10-K", "10-Q", "13F-HR",
    "SCHEDULE 13D", "SCHEDULE 13G",
    "5", "144",
    # Audit additions (2026-05-01):
    "3", "6-K", "NT 10-K", "NT 10-Q", "DEF 14A",
]


def _expand_keep(only_forms: Iterable[str]) -> set[str]:
    """Resolve the user's filter list into the actual set of base form-type
    strings to match in the index. Amendments (`/A`) are added by parser."""
    keep: set[str] = set()
    for f in only_forms:
        normed = f.upper().strip()
        keep.update(FORM_ALIASES.get(normed, {normed}))
    return keep


def parse_form_idx(path: Path, *, only_forms: Iterable[str] | None = None) -> list[FilingRow]:
    """Parse a form.idx file into FilingRow records.

    only_forms: optional set/list of form-type names to keep. The filter
                accepts user-friendly aliases (e.g. 'SC 13D' resolves to
                both 'SC 13D' and 'SCHEDULE 13D'). Amendments (`/A`
                suffix) are kept whenever their base form matches.
    """
    rows: list[FilingRow] = []
    keep = _expand_keep(only_forms) if only_forms else None

    with path.open(encoding="latin-1") as f:
        # Skip the 11-line header
        for _ in range(11):
            f.readline()
        for line in f:
            if len(line) < 99 or not line.strip():
                continue
            form_type = line[0:17].strip()           # widened from 12 → 17
            company   = line[17:74].strip()
            cik       = line[74:86].strip()
            filed     = line[86:98].strip()
            filepath  = line[98:].strip()

            if not (form_type and cik and filed and filepath):
                continue
            if keep:
                # Strip trailing /A (amendment marker) only — not embedded slashes.
                ftu = form_type.upper()
                base_form = ftu[:-2].strip() if ftu.endswith("/A") else ftu
                if base_form not in keep:
                    continue
            rows.append(FilingRow(form_type, company, cik, filed, filepath))
    return rows


def fetch_quarters(years: Iterable[int], quarters: Iterable[int] = range(1, 5),
                   only_forms: Iterable[str] | None = None) -> list[FilingRow]:
    """Convenience: fetch + parse a range of quarters, return concatenated rows."""
    session = make_session(headers={"User-Agent": USER_AGENT})
    all_rows: list[FilingRow] = []
    for year in years:
        for qtr in quarters:
            try:
                path = fetch_quarter_index(session, year, qtr)
            except requests.HTTPError as e:
                logger.warning("skip %s Q%d: %s", year, qtr, e)
                continue
            rows = parse_form_idx(path, only_forms=only_forms)
            logger.info("  %sQ%d: %d filings (filtered to %s)",
                        year, qtr, len(rows), only_forms or "ALL")
            all_rows.extend(rows)
    return all_rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, nargs="+",
                   default=[date.today().year - 1, date.today().year])
    p.add_argument("--quarter", type=int, nargs="+", default=[1, 2, 3, 4])
    p.add_argument("--forms", nargs="+", default=DEFAULT_FORMS)
    p.add_argument("--summary", action="store_true",
                   help="Print per-form-type counts after fetching")
    args = p.parse_args()

    rows = fetch_quarters(args.year, args.quarter, only_forms=args.forms)
    logger.info("total filings: %d", len(rows))

    if args.summary:
        from collections import Counter
        c = Counter(r.form_type for r in rows)
        print("\nForm-type counts:")
        for ft, n in c.most_common():
            print(f"  {ft:14s} {n:>8,}")


if __name__ == "__main__":
    main()
