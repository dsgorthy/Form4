"""
House Financial Disclosure PTR importer.

Downloads annual FD ZIP indexes from disclosures-clerk.house.gov,
fetches PTR PDFs, extracts transaction data, and loads into the
congress_trades / politicians tables.

Usage:
    python3 pipelines/insider_study/import_house.py [--years 2024,2025] [--limit 0]
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen

# ── paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
SCHEMA_PATH = ROOT / "strategies" / "insider_catalog" / "congress_schema.sql"
CACHE_DIR = ROOT / "data" / "congress_raw" / "house"

# ── constants ──────────────────────────────────────────────────────────
FD_ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
USER_AGENT = "Form4/1.0 (research; contact: admin@form4.app)"

# STOCK Act value bands
AMOUNT_BANDS = {
    "$1,001 - $15,000": (1001, 15000),
    "$15,001 - $50,000": (15001, 50000),
    "$50,001 - $100,000": (50001, 100000),
    "$100,001 - $250,000": (100001, 250000),
    "$250,001 - $500,000": (250001, 500000),
    "$500,001 - $1,000,000": (500001, 1000000),
    "$1,000,001 - $5,000,000": (1000001, 5000000),
    "$5,000,001 - $25,000,000": (5000001, 25000000),
    "$25,000,001 - $50,000,000": (25000001, 50000000),
    "$50,000,001 - ": (50000001, 100000000),
}

TRADE_TYPE_MAP = {"P": "buy", "S": "sell", "E": "exchange"}

# Known party affiliations for House members (major traders)
# This is a bootstrap — can be enriched from a separate source later
PARTY_MAP: Dict[str, Tuple[str, str]] = {}  # populated below


def _fetch(url: str, retries: int = 2) -> bytes:
    """Fetch URL with retries and polite delay."""
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            return urlopen(req, timeout=30).read()
        except HTTPError as e:
            if e.code == 404:
                raise
            if attempt < retries:
                time.sleep(1)
            else:
                raise
        except Exception:
            if attempt < retries:
                time.sleep(1)
            else:
                raise
    return b""


def download_fd_index(year: int) -> List[dict]:
    """Download the annual FD ZIP and parse the XML index for PTR records."""
    url = FD_ZIP_URL.format(year=year)
    print(f"  Downloading {url}...")
    data = _fetch(url)
    z = zipfile.ZipFile(io.BytesIO(data))

    xml_name = next(n for n in z.namelist() if n.endswith(".xml"))
    tree = ET.parse(z.open(xml_name))
    root = tree.getroot()

    records = []
    for m in root.findall(".//Member"):
        ft = m.find("FilingType")
        if ft is None or ft.text != "P":
            continue
        doc_id = m.find("DocID")
        if doc_id is None:
            continue

        first = (m.find("First").text or "").strip()
        last = (m.find("Last").text or "").strip()
        prefix = (m.find("Prefix").text or "").strip()
        suffix_el = m.find("Suffix")
        suffix = (suffix_el.text or "").strip() if suffix_el is not None else ""
        state_dst = (m.find("StateDst").text or "").strip()
        filing_date = (m.find("FilingDate").text or "").strip()

        # Parse state and district from StateDst (e.g., "GA12", "CA33")
        sd_match = re.match(r"([A-Z]{2})(\d+)?", state_dst)
        state = sd_match.group(1) if sd_match else None
        district = sd_match.group(2) if sd_match else None

        name = f"{first} {last}".strip()
        if suffix:
            name = f"{name}, {suffix}"

        records.append({
            "doc_id": doc_id.text.strip(),
            "name": name,
            "prefix": prefix,
            "state": state,
            "district": district,
            "state_dst": state_dst,
            "filing_date": filing_date,
            "year": year,
        })

    return records


def parse_amount(amount_str: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Parse a STOCK Act amount string into (low, high, estimate)."""
    amount_str = re.sub(r"\s+", " ", amount_str.strip())

    for band_str, (lo, hi) in AMOUNT_BANDS.items():
        if band_str.replace(" ", "") in amount_str.replace(" ", "").replace("\n", ""):
            return lo, hi, (lo + hi) // 2

    # Try regex: $X - $Y or $X -$Y
    m = re.search(r"\$?([\d,]+)\s*-\s*\$?([\d,]+)", amount_str)
    if m:
        lo = int(m.group(1).replace(",", ""))
        hi = int(m.group(2).replace(",", ""))
        return lo, hi, (lo + hi) // 2

    return None, None, None


def parse_date(date_str: str) -> Optional[str]:
    """Convert MM/DD/YYYY to YYYY-MM-DD."""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return None


def extract_ticker(asset_str: str) -> Optional[str]:
    """Extract ticker symbol from asset description like 'Apple Inc. (AAPL) [ST]'."""
    m = re.search(r"\(([A-Z]{1,5})\)", asset_str)
    if m:
        return m.group(1)
    return None


def parse_ptr_pdf(pdf_data: bytes) -> List[dict]:
    """Parse a House PTR PDF and extract transactions."""
    try:
        import pdfplumber
    except ImportError:
        print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
        sys.exit(1)

    pdf = pdfplumber.open(io.BytesIO(pdf_data))
    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    # Extract filer info
    name_match = re.search(r"Name:\s*(.+?)(?:\n|$)", full_text)
    state_match = re.search(r"State/District:\s*(\w+)", full_text)

    filer_name = name_match.group(1).strip() if name_match else None
    filer_state_dst = state_match.group(1).strip() if state_match else None

    transactions = []

    # Strategy: find lines matching the transaction pattern
    # Format: [Owner] Asset description (TICKER) [type] P|S|E MM/DD/YYYY MM/DD/YYYY $amount
    # The amount may wrap to next line
    lines = full_text.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for transaction pattern: ... P|S|E MM/DD/YYYY MM/DD/YYYY ...
        tx_match = re.search(
            r"^(.*?)\s+([PSE])\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(.*?)$",
            line,
        )
        if tx_match:
            asset_part = tx_match.group(1).strip()
            tx_type = tx_match.group(2)
            trade_date_raw = tx_match.group(3)
            filing_date_raw = tx_match.group(4)
            amount_part = tx_match.group(5).strip()

            # Amount might continue on next line
            if amount_part and not re.search(r"\d{3,}", amount_part.replace(",", "").replace("$", "")):
                # Incomplete amount, check next line
                if i + 1 < len(lines):
                    amount_part += " " + lines[i + 1].strip()
                    i += 1

            # If amount starts with $ but no complete band yet, grab next line
            if amount_part.startswith("$") and "-" in amount_part:
                # Check if next line completes the band
                if not re.search(r"\$[\d,]+\s*$", amount_part):
                    if i + 1 < len(lines) and lines[i + 1].strip().startswith("$"):
                        amount_part += " " + lines[i + 1].strip()
                        i += 1

            # Parse owner from asset part
            owner = "Self"
            owner_match = re.match(r"^(SP|JT|DC)\s+", asset_part)
            if owner_match:
                owner_code = owner_match.group(1)
                asset_part = asset_part[len(owner_code):].strip()
                owner = {"SP": "Spouse", "JT": "Joint", "DC": "Child"}.get(owner_code, owner_code)

            ticker = extract_ticker(asset_part)
            trade_date = parse_date(trade_date_raw)
            notification_date = parse_date(filing_date_raw)
            value_low, value_high, value_estimate = parse_amount(amount_part)

            # Extract company name (text before ticker parens)
            company = None
            if ticker:
                cm = re.match(r"^(.+?)\s*\(" + re.escape(ticker) + r"\)", asset_part)
                if cm:
                    company = cm.group(1).strip().rstrip("-").strip()

            trade_type = TRADE_TYPE_MAP.get(tx_type)
            if trade_type and trade_date:
                transactions.append({
                    "ticker": ticker,
                    "company": company,
                    "asset_type": "stock" if ticker else "other",
                    "trade_type": trade_type,
                    "trade_date": trade_date,
                    "filing_date": notification_date,
                    "value_low": value_low,
                    "value_high": value_high,
                    "value_estimate": value_estimate,
                    "owner": owner,
                })

        i += 1

    return transactions


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create congress tables if they don't exist."""
    if SCHEMA_PATH.exists():
        conn.executescript(SCHEMA_PATH.read_text())
    else:
        print(f"WARNING: Schema file not found at {SCHEMA_PATH}")


def get_or_create_politician(
    conn: sqlite3.Connection,
    name: str,
    state: Optional[str],
    district: Optional[str],
    cache: Dict[str, int],
) -> int:
    """Get or create a politician record, returns politician_id."""
    norm = name.lower().strip()
    if norm in cache:
        return cache[norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'House'",
        (norm,),
    ).fetchone()

    if row:
        cache[norm] = row[0]
        return row[0]

    # Look up party if we have it
    party = None
    if name in PARTY_MAP:
        party, _ = PARTY_MAP[name]

    conn.execute(
        """INSERT INTO politicians (name, name_normalized, chamber, state, party, district)
           VALUES (?, ?, 'House', ?, ?, ?)""",
        (name, norm, state, party, district),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    cache[norm] = pid
    return pid


def main():
    parser = argparse.ArgumentParser(description="Import House PTR filings")
    parser.add_argument("--years", default="2024,2025", help="Comma-separated years to import")
    parser.add_argument("--limit", type=int, default=0, help="Max PTRs to process per year (0=all)")
    parser.add_argument("--delay", type=float, default=0.3, help="Seconds between PDF downloads")
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",")]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    politician_cache: Dict[str, int] = {}
    total_ptrs = 0
    total_txns = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0

    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing year {year}")
        print(f"{'='*60}")

        try:
            ptrs = download_fd_index(year)
        except Exception as e:
            print(f"  ERROR downloading index: {e}")
            continue

        print(f"  Found {len(ptrs)} PTR filings")

        if args.limit > 0:
            ptrs = ptrs[:args.limit]
            print(f"  Limiting to {args.limit}")

        for idx, ptr in enumerate(ptrs):
            doc_id = ptr["doc_id"]
            name = ptr["name"]
            state = ptr["state"]
            district = ptr["district"]
            total_ptrs += 1

            # Check cache
            cache_path = CACHE_DIR / f"{year}_{doc_id}.pdf"

            try:
                if cache_path.exists():
                    pdf_data = cache_path.read_bytes()
                else:
                    url = PTR_PDF_URL.format(year=year, doc_id=doc_id)
                    pdf_data = _fetch(url)
                    cache_path.write_bytes(pdf_data)
                    time.sleep(args.delay)

                transactions = parse_ptr_pdf(pdf_data)
                total_txns += len(transactions)

                politician_id = get_or_create_politician(conn, name, state, district, politician_cache)

                for tx in transactions:
                    if not tx["ticker"]:
                        total_skipped += 1
                        continue

                    # COALESCE-safe NULL dedup check
                    existing = conn.execute(
                        """SELECT 1 FROM congress_trades
                           WHERE politician_id = ? AND ticker = ? AND trade_type = ?
                             AND trade_date = ? AND COALESCE(value_low, -1) = COALESCE(?, -1)""",
                        (politician_id, tx["ticker"], tx["trade_type"],
                         tx["trade_date"], tx["value_low"]),
                    ).fetchone()
                    if existing:
                        total_skipped += 1
                        continue

                    try:
                        conn.execute(
                            """INSERT OR IGNORE INTO congress_trades
                               (politician_id, ticker, company, asset_type, trade_type,
                                trade_date, value_low, value_high, value_estimate,
                                filing_date, owner, source)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'house_ptr')""",
                            (
                                politician_id,
                                tx["ticker"],
                                tx["company"],
                                tx["asset_type"],
                                tx["trade_type"],
                                tx["trade_date"],
                                tx["value_low"],
                                tx["value_high"],
                                tx["value_estimate"],
                                tx["filing_date"],
                                tx["owner"],
                            ),
                        )
                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            total_inserted += 1
                        else:
                            total_skipped += 1
                    except sqlite3.IntegrityError:
                        total_skipped += 1

                if (idx + 1) % 25 == 0 or idx == len(ptrs) - 1:
                    conn.commit()
                    print(f"  [{idx+1}/{len(ptrs)}] {name} ({state or '??'}) — {len(transactions)} txns from {doc_id}")

            except HTTPError as e:
                if e.code == 404:
                    total_errors += 1
                    if (idx + 1) % 50 == 0:
                        print(f"  [{idx+1}/{len(ptrs)}] {doc_id} — 404 Not Found")
                else:
                    total_errors += 1
                    print(f"  [{idx+1}/{len(ptrs)}] {doc_id} — HTTP {e.code}")
            except Exception as e:
                total_errors += 1
                if (idx + 1) % 50 == 0:
                    print(f"  [{idx+1}/{len(ptrs)}] {doc_id} — ERROR: {e}")

        conn.commit()

    # Final stats
    house_count = conn.execute(
        "SELECT COUNT(*) FROM congress_trades WHERE source = 'house_ptr'"
    ).fetchone()[0]
    politician_count = conn.execute(
        "SELECT COUNT(*) FROM politicians WHERE chamber = 'House'"
    ).fetchone()[0]

    conn.close()

    print(f"\n{'='*60}")
    print(f"IMPORT COMPLETE")
    print(f"{'='*60}")
    print(f"  PTRs processed:   {total_ptrs}")
    print(f"  Transactions found: {total_txns}")
    print(f"  Inserted:         {total_inserted}")
    print(f"  Skipped (dupes/no ticker): {total_skipped}")
    print(f"  Errors (404/parse): {total_errors}")
    print(f"  House politicians: {politician_count}")
    print(f"  House trades in DB: {house_count}")


if __name__ == "__main__":
    main()
