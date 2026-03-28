"""House Financial Disclosure scraper — polls disclosures-clerk.house.gov for new PTR filings.

The House Clerk site provides:
1. An annual XML index ZIP with all filing metadata
2. Individual PTR PDFs at predictable URLs

For real-time polling, we check the search page which has an AJAX endpoint
returning recent filings. We also periodically re-fetch the XML index to
catch anything the search page might miss.

PDF parsing is delegated to parse_ptr.parse_house_ptr_pdf().
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
import zipfile
import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError

import requests

from pipelines.congress_scraper.parse_ptr import parse_house_ptr_pdf

logger = logging.getLogger("congress_scraper.house")

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
SCHEMA_PATH = ROOT / "strategies" / "insider_catalog" / "congress_schema.sql"
CACHE_DIR = ROOT / "data" / "congress_raw" / "house"

# House disclosure URLs
FD_ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
SEARCH_URL = "https://disclosures-clerk.house.gov/FinancialDisclosure/ViewMemberSearchResult"

USER_AGENT = "Form4/1.0 (research; contact: admin@form4.app)"


def normalize_name(name: str) -> str:
    """Normalize a politician name for dedup matching."""
    name = re.sub(r"\b(Hon\.?|Representative|Rep\.?|Jr\.?|Sr\.?|III|II|IV)\b", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", name.strip().lower())


def get_or_create_politician(
    conn: sqlite3.Connection,
    name: str,
    state: Optional[str],
    district: Optional[str],
    party: Optional[str],
    cache: dict[str, int],
) -> int:
    """Get or create a politician record, returns politician_id."""
    norm = normalize_name(name)
    if norm in cache:
        return cache[norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'House'",
        (norm,),
    ).fetchone()

    if row:
        pid = row[0]
        # Update metadata if we have new info
        if state or party or district:
            conn.execute(
                """UPDATE politicians SET
                     state = COALESCE(state, ?),
                     party = COALESCE(party, ?),
                     district = COALESCE(district, ?)
                   WHERE politician_id = ?""",
                (state, party, district, pid),
            )
        cache[norm] = pid
        return pid

    conn.execute(
        """INSERT INTO politicians (name, name_normalized, chamber, state, party, district)
           VALUES (?, ?, 'House', ?, ?, ?)""",
        (name.strip(), norm, state, party, district),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    cache[norm] = pid
    return pid


def fetch_xml_index(year: int) -> list[dict]:
    """Download the annual FD ZIP and parse the XML index for PTR records."""
    url = FD_ZIP_URL.format(year=year)
    logger.info(f"Fetching House FD index: {url}")

    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    xml_name = next(n for n in z.namelist() if n.endswith(".xml"))
    tree = ET.parse(z.open(xml_name))
    root = tree.getroot()

    records = []
    for m in root.findall(".//Member"):
        ft = m.find("FilingType")
        if ft is None or ft.text != "P":
            continue
        doc_id_el = m.find("DocID")
        if doc_id_el is None:
            continue

        first = (m.find("First").text or "").strip() if m.find("First") is not None else ""
        last = (m.find("Last").text or "").strip() if m.find("Last") is not None else ""
        suffix_el = m.find("Suffix")
        suffix = (suffix_el.text or "").strip() if suffix_el is not None else ""
        state_dst = (m.find("StateDst").text or "").strip() if m.find("StateDst") is not None else ""
        filing_date = (m.find("FilingDate").text or "").strip() if m.find("FilingDate") is not None else ""

        # Parse state and district
        sd_match = re.match(r"([A-Z]{2})(\d+)?", state_dst)
        state = sd_match.group(1) if sd_match else None
        district = sd_match.group(2) if sd_match else None

        name = f"{first} {last}".strip()
        if suffix:
            name = f"{name}, {suffix}"

        records.append({
            "doc_id": doc_id_el.text.strip(),
            "name": name,
            "state": state,
            "district": district,
            "filing_date": filing_date,
            "year": year,
        })

    logger.info(f"House FD index {year}: {len(records)} PTR filings")
    return records


def fetch_recent_search() -> list[dict]:
    """Poll the House disclosure search page for recent PTR filings.

    The House Clerk site uses a POST-based search form. This supplements
    the XML index for catching very recent filings that may not be in
    the ZIP yet.
    """
    from bs4 import BeautifulSoup

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    })

    # The search form requires a POST with form data
    try:
        resp = session.post(
            SEARCH_URL,
            data={
                "FilingYear": str(datetime.now().year),
                "State": "",
                "District": "",
                "LastName": "",
                "FilingType": "P",  # PTR
            },
            headers={"Referer": "https://disclosures-clerk.house.gov/FinancialDisclosure"},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"House search page fetch failed: {e}")
        return []

    # Parse the HTML response for filing links
    records = []
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        logger.debug("No results table found on House search page")
        return records

    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        name = cells[0].get_text(strip=True)
        # Try to find state/district in subsequent cells
        state = None
        district = None
        filing_date = None

        for cell in cells[1:]:
            text = cell.get_text(strip=True)
            # State is 2-letter code
            if re.match(r"^[A-Z]{2}$", text) and not state:
                state = text
            # District is a number
            elif re.match(r"^\d{1,2}$", text) and not district:
                district = text
            # Filing date is MM/DD/YYYY
            elif re.match(r"\d{1,2}/\d{1,2}/\d{4}", text) and not filing_date:
                filing_date = text

        # Find PDF link in any cell
        link = None
        for cell in cells:
            a = cell.find("a", href=True)
            if a and ("ptr-pdfs" in a["href"] or "pdf" in a["href"].lower()):
                link = a
                break

        if not link:
            continue

        href = link["href"]
        doc_match = re.search(r"(\d{8,})", href)
        if not doc_match:
            continue

        records.append({
            "doc_id": doc_match.group(1),
            "name": name,
            "state": state,
            "district": district,
            "filing_date": filing_date,
            "year": datetime.now().year,
            "pdf_url": href if href.startswith("http") else f"https://disclosures-clerk.house.gov{href}",
        })

    logger.info(f"House search returned {len(records)} recent PTRs")
    return records


def fetch_pdf(doc_id: str, year: int) -> Optional[bytes]:
    """Download a PTR PDF, using local cache if available."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{year}_{doc_id}.pdf"

    if cache_path.exists():
        return cache_path.read_bytes()

    url = PTR_PDF_URL.format(year=year, doc_id=doc_id)
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        resp.raise_for_status()
        pdf_data = resp.content
        cache_path.write_bytes(pdf_data)
        return pdf_data
    except requests.HTTPError as e:
        if e.response and e.response.status_code == 404:
            logger.debug(f"PDF not found: {doc_id}")
        else:
            logger.warning(f"PDF fetch failed for {doc_id}: {e}")
        return None
    except Exception as e:
        logger.warning(f"PDF fetch error for {doc_id}: {e}")
        return None


def scrape_house(
    state: dict,
    db_path: Path = DB_PATH,
) -> dict:
    """Run one scrape cycle for House PTRs.

    Uses two strategies:
    1. Poll the search page for recent filings (fast, covers last few days)
    2. Periodically re-fetch the XML index to catch anything missed

    Args:
        state: Mutable state dict with watermarks.
        db_path: Path to insiders.db.

    Returns:
        Stats dict with counts.
    """
    stats = {"ptrs_found": 0, "pdfs_fetched": 0, "transactions": 0, "inserted": 0, "errors": 0}

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    if SCHEMA_PATH.exists():
        conn.executescript(SCHEMA_PATH.read_text())

    politician_cache: dict[str, int] = {}
    processed_doc_ids: set[str] = set()

    # Load already-processed doc IDs from state
    known_ids = set(state.get("house_known_doc_ids", []))

    # Strategy 1: Search page for recent filings
    try:
        recent = fetch_recent_search()
    except Exception as e:
        logger.error(f"House recent search failed: {e}")
        recent = []

    # Strategy 2: XML index (refresh every 6 hours)
    xml_records = []
    last_xml_fetch = state.get("house_last_xml_fetch", "")
    now_iso = datetime.now().isoformat()
    if not last_xml_fetch or (datetime.now() - datetime.fromisoformat(last_xml_fetch)).total_seconds() > 21600:
        try:
            current_year = datetime.now().year
            xml_records = fetch_xml_index(current_year)
            state["house_last_xml_fetch"] = now_iso
        except Exception as e:
            logger.error(f"House XML index fetch failed: {e}")

    # Merge both sources, dedup by doc_id
    all_ptrs: dict[str, dict] = {}
    for r in recent:
        all_ptrs[r["doc_id"]] = r
    for r in xml_records:
        if r["doc_id"] not in all_ptrs:
            all_ptrs[r["doc_id"]] = r

    # Filter to only new filings
    new_ptrs = {did: r for did, r in all_ptrs.items() if did not in known_ids}
    stats["ptrs_found"] = len(new_ptrs)

    if not new_ptrs:
        conn.close()
        return stats

    logger.info(f"Processing {len(new_ptrs)} new House PTRs")

    for doc_id, ptr in new_ptrs.items():
        try:
            pdf_data = fetch_pdf(doc_id, ptr.get("year", datetime.now().year))
            if not pdf_data:
                stats["errors"] += 1
                continue

            stats["pdfs_fetched"] += 1
            time.sleep(0.3)  # polite delay

            transactions = parse_house_ptr_pdf(pdf_data)
            stats["transactions"] += len(transactions)

            if not transactions:
                processed_doc_ids.add(doc_id)
                continue

            politician_id = get_or_create_politician(
                conn,
                ptr["name"],
                ptr.get("state"),
                ptr.get("district"),
                None,  # party not available from index
                politician_cache,
            )

            # Parse filing date
            filing_date = None
            raw_fd = ptr.get("filing_date", "")
            if raw_fd:
                m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_fd)
                if m:
                    filing_date = f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"

            for tx in transactions:
                if not tx.get("ticker"):
                    continue

                if not tx.get("filing_date") and filing_date:
                    tx["filing_date"] = filing_date

                filing_delay = None
                if tx.get("filing_date") and tx.get("trade_date"):
                    try:
                        td = datetime.strptime(tx["trade_date"], "%Y-%m-%d")
                        fd = datetime.strptime(tx["filing_date"], "%Y-%m-%d")
                        filing_delay = (fd - td).days
                    except ValueError:
                        pass

                report_url = PTR_PDF_URL.format(year=ptr.get("year", datetime.now().year), doc_id=doc_id)

                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO congress_trades
                           (politician_id, ticker, company, asset_type, trade_type,
                            trade_date, value_low, value_high, value_estimate,
                            filing_date, filing_delay_days, owner, report_url, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'house_scraper')""",
                        (
                            politician_id,
                            tx["ticker"],
                            tx.get("company"),
                            tx.get("asset_type", "stock"),
                            tx["trade_type"],
                            tx["trade_date"],
                            tx.get("value_low"),
                            tx.get("value_high"),
                            tx.get("value_estimate"),
                            tx.get("filing_date"),
                            filing_delay,
                            tx.get("owner", "Self"),
                            report_url,
                        ),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        stats["inserted"] += 1
                except sqlite3.Error as e:
                    logger.warning(f"DB insert error for {ptr['name']}/{tx.get('ticker')}: {e}")
                    stats["errors"] += 1

            processed_doc_ids.add(doc_id)
            conn.commit()

        except Exception as e:
            logger.error(f"Error processing House PTR {doc_id} ({ptr['name']}): {e}")
            stats["errors"] += 1

    conn.close()

    # Update state with newly processed IDs
    # Keep only last 5000 IDs to prevent unbounded growth
    all_known = known_ids | processed_doc_ids
    if len(all_known) > 5000:
        all_known = set(sorted(all_known)[-5000:])
    state["house_known_doc_ids"] = list(all_known)

    logger.info(
        f"House scrape complete: {stats['ptrs_found']} PTRs, "
        f"{stats['pdfs_fetched']} PDFs, {stats['transactions']} txns, "
        f"{stats['inserted']} inserted, {stats['errors']} errors"
    )
    return stats
