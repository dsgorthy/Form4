"""Senate eFD scraper — polls efdsearch.senate.gov for new PTR filings.

The Senate Electronic Financial Disclosures site requires:
1. GET the search page to obtain a CSRF token + session cookie
2. POST to the search API with the CSRF token
3. Parse the HTML response for PTR links
4. Fetch each PTR detail page and parse transactions

This module handles the session management and search polling.
Individual PTR HTML parsing is delegated to parse_ptr.parse_senate_ptr_html().
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from pipelines.congress_scraper.parse_ptr import (
    parse_senate_ptr_html,
    normalize_owner,
)

logger = logging.getLogger("congress_scraper.senate")

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
SCHEMA_PATH = ROOT / "strategies" / "insider_catalog" / "congress_schema.sql"

SEARCH_URL = "https://efdsearch.senate.gov/search/"
SEARCH_API_URL = "https://efdsearch.senate.gov/search/report/data/"
REPORT_BASE = "https://efdsearch.senate.gov"
USER_AGENT = "Form4/1.0 (research; contact: admin@form4.app)"

# Map Senate party labels
PARTY_MAP = {
    "D": "D",
    "R": "R",
    "I": "I",
    "Democrat": "D",
    "Republican": "R",
    "Independent": "I",
}

# State name to abbreviation (common ones — full map below)
STATE_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC",
}


class SenateSession:
    """Manages a session with the Senate eFD search site, handling CSRF tokens."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        self.csrf_token: Optional[str] = None
        self._last_init = 0.0

    def _init_session(self) -> None:
        """Fetch the search page, accept terms agreement, and obtain CSRF token."""
        now = time.time()
        if self.csrf_token and (now - self._last_init) < 300:
            return  # reuse for 5 minutes

        # Step 1: GET the landing page (shows prohibition agreement form)
        resp = self.session.get(SEARCH_URL, timeout=30)
        resp.raise_for_status()

        # Extract CSRF token from cookie (set by Django)
        csrf = self.session.cookies.get("csrftoken", "")
        if not csrf:
            # Try hidden input
            m = re.search(
                r'name=["\']csrfmiddlewaretoken["\']\s+value=["\'](.*?)["\']',
                resp.text,
            )
            if m:
                csrf = m.group(1)

        if not csrf:
            logger.warning("Could not extract CSRF token from Senate eFD page")
            self.csrf_token = ""
            self._last_init = now
            return

        # Step 2: Accept the prohibition agreement (required before search works)
        if "prohibition_agreement" in resp.text:
            agree_resp = self.session.post(
                SEARCH_URL,
                data={
                    "prohibition_agreement": "1",
                    "csrfmiddlewaretoken": csrf,
                },
                headers={
                    "Referer": SEARCH_URL,
                    "X-CSRFToken": csrf,
                },
                timeout=30,
            )
            agree_resp.raise_for_status()
            # CSRF token may have rotated after POST
            csrf = self.session.cookies.get("csrftoken", csrf)

        self.csrf_token = csrf
        self._last_init = now
        logger.info("Senate eFD session initialized (CSRF obtained, terms accepted)")

    def search_ptrs(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        """Search for Periodic Transaction Reports.

        Args:
            start_date: MM/DD/YYYY format
            end_date: MM/DD/YYYY format

        Returns:
            List of dicts with keys: name, office, report_url, filing_date
        """
        self._init_session()

        if not end_date:
            end_date = datetime.now().strftime("%m/%d/%Y")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%m/%d/%Y")

        # The Senate eFD search API accepts POST with form data
        # Report type "11" = Periodic Transaction Report
        payload = {
            "start": "0",
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": start_date,
            "submitted_end_date": end_date,
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
        }

        headers = {
            "Referer": SEARCH_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token

        resp = self.session.post(
            SEARCH_API_URL,
            data=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.error("Failed to parse Senate search response as JSON")
            return []

        results = []
        # Senate eFD returns DataTables-format JSON: {"data": [[...], ...]}
        for row in data.get("data", []):
            if not isinstance(row, list) or len(row) < 5:
                continue

            # Row format: [name_html, office, report_type_html, filing_date, ...]
            # Parse name and URL from the HTML cell
            name_html = str(row[0])
            name_match = re.search(r'href="(/search/view/[^"]+)"[^>]*>([^<]+)', name_html)
            if not name_match:
                continue

            report_path = name_match.group(1)
            name = name_match.group(2).strip()

            # Parse report type — we only want PTRs
            report_type_html = str(row[2]) if len(row) > 2 else ""
            if "periodic" not in report_type_html.lower() and "ptr" not in report_type_html.lower():
                # Could be an Annual report or other type that slipped through
                if "transaction" not in report_type_html.lower():
                    continue

            office = str(row[1]).strip() if len(row) > 1 else ""
            filing_date_raw = str(row[3]).strip() if len(row) > 3 else ""

            # Parse state/party from office field
            # Format is typically "Senator from State" or includes party info
            state = None
            party = None
            if office:
                # Try to extract state
                for state_name, abbrev in STATE_ABBREV.items():
                    if state_name.lower() in office.lower():
                        state = abbrev
                        break
                # Try to extract party from name or separate field
                for pkey, pval in PARTY_MAP.items():
                    if f"({pkey})" in name or f"[{pkey}]" in name:
                        party = pval
                        name = re.sub(r"\s*[\(\[]\s*[DRI]\s*[\)\]]", "", name).strip()
                        break

            results.append({
                "name": name,
                "office": office,
                "state": state,
                "party": party,
                "report_url": REPORT_BASE + report_path,
                "filing_date": filing_date_raw,
            })

        logger.info(f"Senate search returned {len(results)} PTR results")
        return results

    def fetch_ptr_page(self, url: str) -> str:
        """Fetch a PTR detail page HTML."""
        self._init_session()
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text


def normalize_name(name: str) -> str:
    """Normalize a politician name for dedup matching."""
    # Remove honorifics, Jr/Sr/III, extra whitespace
    name = re.sub(r"\b(Hon\.?|Senator|Sen\.?|Jr\.?|Sr\.?|III|II|IV)\b", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", name.strip().lower())


def get_or_create_politician(
    conn: sqlite3.Connection,
    name: str,
    state: Optional[str],
    party: Optional[str],
    cache: dict[str, int],
) -> int:
    """Get or create a politician record, returns politician_id."""
    norm = normalize_name(name)
    if norm in cache:
        return cache[norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'Senate'",
        (norm,),
    ).fetchone()

    if row:
        # Update state/party if we now have them and they were missing
        pid = row[0]
        if state or party:
            conn.execute(
                """UPDATE politicians SET
                     state = COALESCE(state, ?),
                     party = COALESCE(party, ?)
                   WHERE politician_id = ?""",
                (state, party, pid),
            )
        cache[norm] = pid
        return pid

    conn.execute(
        """INSERT INTO politicians (name, name_normalized, chamber, state, party)
           VALUES (?, ?, 'Senate', ?, ?)""",
        (name.strip(), norm, state, party),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    cache[norm] = pid
    return pid


def scrape_senate(
    state: dict,
    db_path: Path = DB_PATH,
    lookback_days: int = 7,
) -> dict:
    """Run one scrape cycle for Senate PTRs.

    Args:
        state: Mutable state dict with 'senate_last_filing_date' watermark.
        db_path: Path to insiders.db.
        lookback_days: How far back to search from today.

    Returns:
        Stats dict with counts.
    """
    stats = {"searched": 0, "ptrs_found": 0, "transactions": 0, "inserted": 0, "errors": 0}

    session = SenateSession()

    # Determine search window
    end_date = datetime.now().strftime("%m/%d/%Y")
    watermark = state.get("senate_last_filing_date")
    if watermark:
        # Parse YYYY-MM-DD watermark, search from that date
        try:
            wm_dt = datetime.strptime(watermark, "%Y-%m-%d")
            start_date = wm_dt.strftime("%m/%d/%Y")
        except ValueError:
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
    else:
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%m/%d/%Y")

    try:
        results = session.search_ptrs(start_date=start_date, end_date=end_date)
    except Exception as e:
        logger.error(f"Senate search failed: {e}")
        stats["errors"] += 1
        return stats

    stats["searched"] = 1
    stats["ptrs_found"] = len(results)

    if not results:
        return stats

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Ensure schema exists
    if SCHEMA_PATH.exists():
        conn.executescript(SCHEMA_PATH.read_text())

    politician_cache: dict[str, int] = {}
    max_filing_date = watermark or ""

    for result in results:
        try:
            # Fetch the PTR detail page
            html = session.fetch_ptr_page(result["report_url"])
            time.sleep(0.5)  # polite delay

            # Parse transactions from the HTML
            transactions = parse_senate_ptr_html(html)
            stats["transactions"] += len(transactions)

            if not transactions:
                continue

            politician_id = get_or_create_politician(
                conn,
                result["name"],
                result["state"],
                result["party"],
                politician_cache,
            )

            # Parse filing date for watermark tracking
            filing_date = None
            raw_fd = result.get("filing_date", "")
            if raw_fd:
                # Try MM/DD/YYYY format
                m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_fd)
                if m:
                    filing_date = f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"

            for tx in transactions:
                if not tx.get("ticker"):
                    continue

                # Set filing_date from report metadata if not in transaction
                if not tx.get("filing_date") and filing_date:
                    tx["filing_date"] = filing_date

                # Calculate filing delay
                filing_delay = None
                if tx.get("filing_date") and tx.get("trade_date"):
                    try:
                        td = datetime.strptime(tx["trade_date"], "%Y-%m-%d")
                        fd = datetime.strptime(tx["filing_date"], "%Y-%m-%d")
                        filing_delay = (fd - td).days
                    except ValueError:
                        pass

                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO congress_trades
                           (politician_id, ticker, company, asset_type, trade_type,
                            trade_date, value_low, value_high, value_estimate,
                            filing_date, filing_delay_days, owner, report_url, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'senate_efd')""",
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
                            result["report_url"],
                        ),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        stats["inserted"] += 1
                except sqlite3.Error as e:
                    logger.warning(f"DB insert error for {result['name']}/{tx.get('ticker')}: {e}")
                    stats["errors"] += 1

            # Track max filing date for watermark
            if filing_date and filing_date > max_filing_date:
                max_filing_date = filing_date

            conn.commit()

        except Exception as e:
            logger.error(f"Error processing Senate PTR for {result['name']}: {e}")
            stats["errors"] += 1

    conn.close()

    # Update watermark
    if max_filing_date:
        state["senate_last_filing_date"] = max_filing_date

    logger.info(
        f"Senate scrape complete: {stats['ptrs_found']} PTRs, "
        f"{stats['transactions']} txns, {stats['inserted']} inserted, "
        f"{stats['errors']} errors"
    )
    return stats
