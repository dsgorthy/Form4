#!/usr/bin/env python3
"""Parse 8-K filings from disk into structured event_8k rows.

Input: a downloaded EDGAR submission .txt file (concatenated SGML + HTML).
Output: a dict matching the `event_8k` table schema:
    accession_number, cik, ticker, filing_date, event_date, item_codes, summary

This is a deliberately simple parser — regex-based, no full HTML parse. 8-K
structure is regular enough that this works for >99% of filings; the rest can
be flagged for manual review via parse_status='failed' on edgar_filings.

Usage:
    python3 pipelines/edgar_archive/parse_8k.py path/to/filing.txt
    python3 pipelines/edgar_archive/parse_8k.py --batch data/edgar/filings/8-K
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


# EDGAR submission .txt files use a colon-delimited header inside <SEC-HEADER>:
#     ACCESSION NUMBER:        0001084869-26-000004
#     FILED AS OF DATE:        20260129
# Match the field name (case-insensitive, multi-space tolerant), capture
# everything to end of line.
def _header_pat(field: str) -> re.Pattern:
    return re.compile(
        rf"^\s*{re.escape(field)}\s*:\s*(.+?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )

HEADER_FIELDS = {
    "accession":         "ACCESSION NUMBER",
    "form_type":         "CONFORMED SUBMISSION TYPE",
    "filed_date":        "FILED AS OF DATE",
    "period_of_report":  "CONFORMED PERIOD OF REPORT",
    "cik":               "CENTRAL INDEX KEY",
    "company":           "COMPANY CONFORMED NAME",
}

# Item heading inside body. Tolerates html: <span> tags, &nbsp;, varying whitespace.
RE_ITEM = re.compile(
    r"Item\s*[\s&nbsp;]*?(\d{1,2})\.(\d{2})",
    re.IGNORECASE,
)

# Strip HTML for summary extraction
RE_TAG = re.compile(r"<[^>]+>")
RE_NBSP = re.compile(r"&nbsp;|&#160;|&#xa0;", re.IGNORECASE)
RE_WS = re.compile(r"\s+")


@dataclass
class Parsed8K:
    accession_number: str
    cik: str
    ticker: Optional[str]
    filing_date: str
    event_date: Optional[str]
    item_codes: list[str]
    summary: str
    parse_warnings: list[str]


_HEADER_CACHE: dict[str, re.Pattern] = {}

def _extract_header(text: str, key: str) -> Optional[str]:
    field = HEADER_FIELDS[key]
    pat = _HEADER_CACHE.setdefault(field, _header_pat(field))
    m = pat.search(text)
    if m:
        return m.group(1).strip()
    # Fallback: try the older <TAG>value form some old filings still use
    fallback = re.search(rf"<{re.escape(field).replace(' ', '-')}>\s*([^<\n]+)",
                         text, re.IGNORECASE)
    return fallback.group(1).strip() if fallback else None


def _format_filed_date(yyyymmdd: Optional[str]) -> Optional[str]:
    if not yyyymmdd or len(yyyymmdd) != 8:
        return None
    return f"{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def _strip_html(html: str) -> str:
    text = RE_TAG.sub(" ", html)
    text = RE_NBSP.sub(" ", text)
    text = RE_WS.sub(" ", text)
    return text.strip()


def parse_8k_file(path: Path) -> Parsed8K:
    """Parse one 8-K submission .txt file. Raises ValueError on malformed input."""
    raw = path.read_text(encoding="latin-1", errors="replace")
    if "<TYPE>" not in raw and "<TYPE>".lower() not in raw.lower():
        raise ValueError(f"no SGML header found in {path}")

    accession = _extract_header(raw, "accession") or path.stem
    cik = _extract_header(raw, "cik") or ""
    if cik:
        cik = cik.lstrip("0") or "0"
    form_type = _extract_header(raw, "form_type") or ""
    filed_date = _format_filed_date(_extract_header(raw, "filed_date")) or ""
    event_date = _format_filed_date(_extract_header(raw, "period_of_report"))
    ticker = None  # Resolved separately via cik→ticker mapping; not in 8-K headers

    warnings = []
    if not form_type.upper().startswith("8-K"):
        warnings.append(f"form_type is {form_type!r}, not 8-K")

    # Body = everything between the first <DOCUMENT> ... </DOCUMENT> after headers
    body_match = re.search(r"<DOCUMENT>(.*?)</DOCUMENT>", raw, re.IGNORECASE | re.DOTALL)
    body = body_match.group(1) if body_match else raw
    text = _strip_html(body)

    # Pull item codes (deduped, ordered)
    items: list[str] = []
    for m in RE_ITEM.finditer(text):
        code = f"{m.group(1)}.{m.group(2)}"
        if code not in items:
            items.append(code)
    if not items:
        warnings.append("no Item codes found in body")

    # Summary: first 400 chars of body text after first item heading
    summary = ""
    first_item = RE_ITEM.search(text)
    if first_item:
        tail = text[first_item.start():first_item.start() + 600]
        summary = tail.strip()
    elif text:
        summary = text[:400].strip()

    return Parsed8K(
        accession_number=accession,
        cik=cik,
        ticker=ticker,
        filing_date=filed_date,
        event_date=event_date,
        item_codes=items,
        summary=summary,
        parse_warnings=warnings,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("paths", nargs="*", help="One or more .txt filings to parse")
    p.add_argument("--batch", help="Walk a directory and parse every .txt under it")
    p.add_argument("--limit", type=int, help="Stop after N (with --batch)")
    p.add_argument("--json", action="store_true", help="Emit JSON lines instead of summary text")
    args = p.parse_args()

    targets: list[Path] = []
    if args.batch:
        targets.extend(sorted(Path(args.batch).rglob("*.txt")))
        if args.limit:
            targets = targets[:args.limit]
    targets.extend(Path(p_) for p_ in args.paths)

    if not targets:
        p.error("no input files (use positional args or --batch)")

    n_ok = n_fail = 0
    for path in targets:
        try:
            parsed = parse_8k_file(path)
            n_ok += 1
            if args.json:
                print(json.dumps(asdict(parsed), default=str))
            else:
                print(f"\n── {path.name} ──")
                print(f"  cik:        {parsed.cik}")
                print(f"  ticker:     {parsed.ticker or '(none)'}")
                print(f"  filed:      {parsed.filing_date}")
                print(f"  event:      {parsed.event_date or '(none)'}")
                print(f"  items:      {parsed.item_codes}")
                if parsed.parse_warnings:
                    print(f"  warnings:   {parsed.parse_warnings}")
                print(f"  summary[:200]: {parsed.summary[:200]!r}")
        except Exception as e:
            n_fail += 1
            logger.error("parse failed: %s — %s", path, e)

    print(f"\n{n_ok} parsed, {n_fail} failed", file=sys.stderr)


if __name__ == "__main__":
    main()
