"""
Download and parse SEC EDGAR Form 4 insider transaction data.

SEC EDGAR publishes quarterly index files listing every Form 4 filing. This script:
  1. Downloads quarterly index files (company.idx.gz) from SEC EDGAR
  2. Extracts all Form 4 filing URLs
  3. Downloads and parses each Form 4 XML file
  4. Saves a clean CSV matching the OpenInsider format expected by build_event_calendar.py

The resulting CSV can be passed to build_event_calendar.py:
    python build_event_calendar.py --input data/edgar_form4.csv --format openinsider

Why EDGAR vs OpenInsider:
  - OpenInsider exports large-dollar transactions (biased toward hedge fund 10% owners)
  - EDGAR has EVERY Form 4, including $25K C-suite officer personal buys
  - EDGAR is free and complete; OpenInsider is curated and size-filtered

Rate limiting: SEC allows 10 requests/second. We use 0.12s delays = ~8 req/sec.

Usage:
    python download_edgar_data.py --start 2022-Q1 --end 2024-Q4
    python download_edgar_data.py --start 2020-Q1 --end 2025-Q4 --output-dir data/edgar_raw/
    python download_edgar_data.py --start 2024-Q1 --end 2024-Q4 --max-filings 500 --sample
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator, Optional

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# SEC EDGAR base URLs
EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.gz"
EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}-index.htm"
EDGAR_XML_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}.xml"

# SEC requires User-Agent with contact info
HEADERS = {"User-Agent": "TradeResearch/1.0 derek.gorthy@gmail.com"}

# Rate limiting: SEC allows 10 req/sec
REQUEST_DELAY = 0.12


def quarter_range(start: str, end: str) -> Iterator[tuple[int, int]]:
    """Generate (year, quarter) tuples from 'YYYY-QN' to 'YYYY-QN' inclusive."""
    sy, sq = int(start[:4]), int(start[-1])
    ey, eq = int(end[:4]), int(end[-1])
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        yield y, q
        q += 1
        if q > 4:
            q = 1
            y += 1


def download_quarterly_index(year: int, quarter: int, cache_dir: Path) -> list[dict]:
    """
    Download and parse the EDGAR quarterly company index.
    Returns list of dicts: {cik, company, form_type, date_filed, filename}
    Only returns Form 4 entries.
    """
    cache_path = cache_dir / f"index-{year}-Q{quarter}.csv"
    if cache_path.exists():
        logger.info("Using cached index: %s", cache_path.name)
        rows = []
        with open(cache_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    url = EDGAR_INDEX_URL.format(year=year, quarter=quarter)
    logger.info("Downloading EDGAR index %d Q%d from %s", year, quarter, url)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to download index %d Q%d: %s", year, quarter, e)
        return []

    time.sleep(REQUEST_DELAY)

    # The company.gz file is a fixed-width text file
    # Format: Company Name | Form Type | CIK | Date Filed | Filename
    # Header lines start with "Company Name"
    text = gzip.decompress(resp.content).decode("utf-8", errors="replace")
    rows = []
    in_data = False
    for line in text.splitlines():
        if line.startswith("Company Name"):
            in_data = True
            continue
        if line.startswith("---"):
            continue
        if not in_data or not line.strip():
            continue

        # Fixed-width (measured from actual EDGAR data, NOT the column header positions):
        #   Company    [0:62]   (62 chars, left-padded)
        #   Form Type  [62:79]  (17 chars — form type + right-padding; header label at 62 is misleading)
        #   CIK        [79:91]  (12 chars, right-aligned)
        #   Date Filed [91:103] (12 chars, YYYY-MM-DD + trailing spaces)
        #   File Name  [103:]   (rest)
        try:
            company = line[:62].strip()
            form_type = line[62:79].strip()
            cik = line[79:91].strip()
            date_filed = line[91:103].strip()
            filename = line[103:].strip()
        except IndexError:
            continue

        if form_type != "4":
            continue

        rows.append({
            "company": company,
            "form_type": form_type,
            "cik": cik,
            "date_filed": date_filed,
            "filename": filename,
        })

    logger.info("Found %d Form 4 filings in %d Q%d index", len(rows), year, quarter)

    # Cache it
    cache_dir.mkdir(parents=True, exist_ok=True)
    if rows:
        with open(cache_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)

    return rows


def download_form4_xml(sgml_filename: str) -> Optional[str]:
    """
    Download and extract the Form 4 XML from the EDGAR SGML full-submission file.

    The EDGAR quarterly index points to a .txt SGML file that contains the entire
    filing, including the XML Form 4 document embedded within <TEXT>...</TEXT> tags.
    This is more reliable than guessing the XML filename in the filing subdirectory,
    because the XML filename is arbitrary (wf-form4_TIMESTAMP.xml, form4.xml, etc.).

    Args:
        sgml_filename: The filename field from the EDGAR index, e.g.
                       'edgar/data/12345/0001234567-24-000001.txt'

    Returns:
        XML string extracted from the SGML, or None on failure.
    """
    sgml_url = f"https://www.sec.gov/Archives/{sgml_filename}"

    try:
        resp = requests.get(sgml_url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
        text = resp.text
    except requests.RequestException:
        return None

    # The SGML format embeds documents like:
    #   <DOCUMENT>
    #   <TYPE>4
    #   <SEQUENCE>1
    #   <FILENAME>form4.xml
    #   <TEXT>
    #   <?xml version="1.0"?>
    #   <ownershipDocument>...</ownershipDocument>
    #   </TEXT>
    #   </DOCUMENT>
    #
    # We find the <TYPE>4 document block and extract the XML from <TEXT>.

    doc_match = re.search(r"<DOCUMENT>\s*<TYPE>4\b", text)
    if not doc_match:
        return None

    doc_section = text[doc_match.start():]
    text_match = re.search(r"<TEXT>\s*(.*?)\s*</TEXT>", doc_section, re.DOTALL)
    if not text_match:
        return None

    xml_content = text_match.group(1).strip()
    if "<" not in xml_content:
        return None

    # EDGAR SGML wraps the XML in <XML>...</XML> and includes an inner <?xml?> declaration.
    # Structure: <XML>\n<?xml version="1.0"?>\n<ownershipDocument>...</ownershipDocument>\n</XML>
    # ET.fromstring() can't handle <?xml?> at line 2 (not start of entity).
    # Extract the ownershipDocument element directly.
    od_match = re.search(r"(<ownershipDocument\b.*?</ownershipDocument>)", xml_content, re.DOTALL)
    if od_match:
        return od_match.group(1)

    # Fallback: strip XML declaration (if <?xml?> is at start before root element)
    xml_content = re.sub(r"^\s*<\?xml[^?]*\?>\s*", "", xml_content)
    return xml_content


def parse_form4_xml(xml_text: str, cik: str, date_filed: str, company: str) -> list[dict]:
    """
    Parse a Form 4 XML and extract non-derivative purchase transactions.

    Returns list of trade dicts matching OpenInsider CSV format:
    {Filing Date, Trade Date, Ticker, Company Name, Insider Name, Title, Trade Type, Price, Qty, Value}
    """
    trades = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return trades

    # XML namespace
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def find(element, path):
        return element.find(f"{ns}{path}" if ns else path)

    def findtext(element, path, default=""):
        el = find(element, path)
        return el.text.strip() if el is not None and el.text else default

    # Issuer
    issuer = find(root, "issuer")
    ticker = findtext(issuer, "issuerTradingSymbol") if issuer is not None else ""
    issuer_name = findtext(issuer, "issuerName") if issuer is not None else company

    # Reporting person
    person = find(root, "reportingOwner")
    insider_name = ""
    title = ""
    if person is not None:
        person_id = find(person, "reportingOwnerId")
        if person_id is not None:
            insider_name = findtext(person_id, "rptOwnerName")
        relationship = find(person, "reportingOwnerRelationship")
        if relationship is not None:
            parts = []
            is_officer = findtext(relationship, "isOfficer")
            officer_title = findtext(relationship, "officerTitle")
            is_director = findtext(relationship, "isDirector")
            is_ten_pct = findtext(relationship, "isTenPercentOwner")
            # XML uses "1"/"0" in older filings and "true"/"false" in newer ones
            def is_true(val):
                return val in ("1", "true", "True", "TRUE")

            if is_true(is_officer) and officer_title:
                parts.append(officer_title)
            elif is_true(is_director):
                parts.append("Dir")
            if is_true(is_ten_pct):
                parts.append("10%")
            title = ", ".join(parts) if parts else "Unknown"

    if not ticker:
        return trades

    # Non-derivative transactions (stock purchases)
    nd_table = find(root, "nonDerivativeTable")
    if nd_table is None:
        return trades

    for txn in nd_table.findall(f"{ns}nonDerivativeTransaction" if ns else "nonDerivativeTransaction"):
        txn_code = findtext(txn, "transactionCoding/transactionCode")
        if txn_code != "P":
            continue

        # Transaction date
        trade_date_raw = findtext(txn, "transactionDate/value", date_filed)
        trade_date = trade_date_raw[:10] if trade_date_raw else date_filed

        # Price and quantity
        price_str = findtext(txn, "transactionAmounts/transactionPricePerShare/value", "0")
        qty_str = findtext(txn, "transactionAmounts/transactionShares/value", "0")

        try:
            price = float(price_str) if price_str else 0.0
            qty = float(qty_str) if qty_str else 0.0
        except ValueError:
            continue

        if price <= 0 or qty <= 0:
            continue

        value = price * qty

        trades.append({
            "Filing Date": date_filed,
            "Trade Date": trade_date,
            "Ticker": ticker.upper(),
            "Company Name": issuer_name,
            "Insider Name": insider_name,
            "Title": title,
            "Trade Type": "P - Purchase",
            "Price": f"${price:.2f}",
            "Qty": f"+{int(qty):,}",
            "Owned": "",
            "DeltaOwn": "",
            "Value": f"+${value:,.0f}",
            "1d": "",
            "1w": "",
            "1m": "",
            "6m": "",
        })

    return trades


def main():
    parser = argparse.ArgumentParser(description="Download SEC EDGAR Form 4 insider transaction data")
    parser.add_argument("--start", default="2022-Q1", help="Start quarter YYYY-QN (e.g. 2022-Q1)")
    parser.add_argument("--end", default="2024-Q4", help="End quarter YYYY-QN")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "data" / "edgar_raw",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "data" / "edgar_form4.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--max-filings",
        type=int,
        default=0,
        help="Limit TOTAL filings processed across all quarters (0 = no limit).",
    )
    parser.add_argument(
        "--max-per-quarter",
        type=int,
        default=0,
        help="Limit filings processed PER QUARTER (0 = no limit). Use with --shuffle "
             "for a representative random sample from each quarter.",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Process only first 100 filings per quarter (for testing)",
    )
    parser.add_argument(
        "--min-value",
        type=float,
        default=10_000,
        help="Minimum transaction value to include (default $10K)",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle filing order per quarter before applying max-filings. Gives a "
             "representative random sample instead of just alphabetically-first filers.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    quarters = list(quarter_range(args.start, args.end))
    logger.info(
        "Processing %d quarters (%s to %s)",
        len(quarters), args.start, args.end,
    )

    all_trades = []
    total_filings_processed = 0
    skipped = 0

    for year, quarter in quarters:
        filings = download_quarterly_index(year, quarter, args.output_dir)

        if args.sample:
            filings = filings[:100]

        if args.shuffle:
            import random
            random.shuffle(filings)

        if args.max_per_quarter:
            filings = filings[:args.max_per_quarter]

        logger.info("Processing %d Form 4 filings for %d Q%d...", len(filings), year, quarter)
        q_trades = 0

        for i, filing in enumerate(filings):
            if args.max_filings and total_filings_processed >= args.max_filings:
                logger.info("Reached max_filings=%d, stopping.", args.max_filings)
                break

            cik = filing["cik"]
            date_filed = filing["date_filed"]
            company = filing["company"]
            filename = filing["filename"]
            # filename is the SGML path: edgar/data/12345/0001234500-24-000123.txt
            # Pass directly — download_form4_xml fetches and extracts embedded XML.

            xml_text = download_form4_xml(filename)
            time.sleep(REQUEST_DELAY)

            if not xml_text:
                skipped += 1
                continue

            trades = parse_form4_xml(xml_text, cik, date_filed, company)

            # Apply minimum value filter
            filtered = []
            for t in trades:
                try:
                    val = float(t["Value"].replace("$", "").replace(",", "").replace("+", ""))
                    if val >= args.min_value:
                        filtered.append(t)
                except (ValueError, KeyError):
                    pass
            trades = filtered

            all_trades.extend(trades)
            q_trades += len(trades)
            total_filings_processed += 1

            if i % 200 == 0 and i > 0:
                logger.info(
                    "  Q%d: %d/%d filings, %d trades so far",
                    quarter, i, len(filings), q_trades,
                )

        logger.info(
            "%d Q%d: processed %d/%d filings, found %d purchase trades",
            year, quarter, min(len(filings), total_filings_processed), len(filings), q_trades,
        )

    # Write output CSV
    logger.info(
        "Writing %d total trades to %s (skipped %d failed XML fetches)",
        len(all_trades), args.output, skipped,
    )

    if all_trades:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_trades[0].keys())
            w.writeheader()
            w.writerows(all_trades)
        logger.info("Done. Output: %s", args.output)
        print(f"\nExtracted {len(all_trades):,} purchase transactions from {total_filings_processed:,} Form 4 filings.")
        print(f"Output: {args.output}")
    else:
        logger.warning("No trades found. Try --sample to debug, or check SEC EDGAR for index availability.")


if __name__ == "__main__":
    main()
