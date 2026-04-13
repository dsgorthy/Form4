#!/usr/bin/env python3
"""
Live EDGAR backfill: fetch 2026 Form 4 filings via EFTS full-text search API,
parse XML, and insert into insiders.db using the same schema as bulk backfill.

Unlike download_sec_bulk.py which uses quarterly ZIP archives (published with lag),
this script fetches individual filings in near-real-time via the EDGAR search API.

Steps:
  1. Query EDGAR EFTS for Form 4 filings in a date range
  2. For each filing, fetch and parse the Form 4 XML
  3. Insert trades into insiders.db (same schema as backfill.py)
  4. Optionally recompute track records

Validation:
  --validate flag compares 2025 Q4 EFTS results against existing DB to confirm parity.

Usage:
  # Backfill 2026 filings
  python backfill_live.py --start 2026-01-01 --end 2026-03-11

  # Validate against existing 2025 Q4 data first
  python backfill_live.py --validate --start 2025-10-01 --end 2025-12-31

  # Backfill + recompute scores
  python backfill_live.py --start 2026-01-01 --end 2026-03-11 --refresh-scores

  # Dry run (parse and report, don't insert)
  python backfill_live.py --start 2026-01-01 --end 2026-03-11 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# Import shared functions from backfill.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.database import get_connection
from backfill import (
    DB_PATH,
    get_or_create_insider,
    is_csuite,
    get_title_weight,
    normalize_name,
    normalize_ticker,
    compute_track_records,
    print_summary,
    validate_trade_date,
    migrate_schema,
)

RESEARCH_DB = DB_PATH.parent / "research.db"  # derivative_trades, filing_footnotes, nonderiv_holdings

try:
    from entity_resolution import is_entity_name, ensure_schema as ensure_entity_schema
except ImportError:
    is_entity_name = None
    ensure_entity_schema = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── EDGAR API Config ─────────────────────────────────────────────────────

EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
USER_AGENT = "Form4/1.0 dsgorthy@hotmail.com"
REQUEST_DELAY = 0.12  # SEC allows 10 req/sec, stay under

# ── EFTS Search ──────────────────────────────────────────────────────────


def search_form4_filings(
    start_date: str,
    end_date: str,
    start_from: int = 0,
    page_size: int = 100,
) -> Tuple[List[dict], int]:
    """
    Search EDGAR EFTS for Form 4 filings in a date range.

    Returns (filings_list, total_hits).
    Each filing dict has: accession, cik, company, filing_date.
    """
    params = {
        "q": '"4"',
        "forms": "4",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "from": str(start_from),
        "size": str(page_size),
    }
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}

    resp = requests.get(EFTS_URL, params=params, headers=headers, timeout=30)
    time.sleep(REQUEST_DELAY)
    resp.raise_for_status()
    data = resp.json()

    total = data.get("hits", {}).get("total", {})
    if isinstance(total, dict):
        total_hits = total.get("value", 0)
    else:
        total_hits = int(total)

    hits = data.get("hits", {}).get("hits", [])
    filings = []
    seen = set()

    for hit in hits:
        source = hit.get("_source", {})
        raw_id = hit.get("_id", "")
        accession = raw_id.split(":")[0] if ":" in raw_id else raw_id
        if not accession or accession in seen:
            continue
        seen.add(accession)

        ciks = source.get("ciks", [])
        display_names = source.get("display_names", [])
        cik = str(ciks[0]) if ciks else ""
        company = display_names[-1] if display_names else ""

        if not cik:
            continue

        filings.append({
            "accession": accession,
            "cik": cik,
            "company": company,
            "filing_date": source.get("file_date", ""),
        })

    return filings, total_hits


def fetch_all_form4_filings(start_date: str, end_date: str) -> List[dict]:
    """Paginate through all Form 4 filings in a date range."""
    all_filings = []
    offset = 0
    page_size = 100

    filings, total = search_form4_filings(start_date, end_date, 0, page_size)
    all_filings.extend(filings)
    logger.info("EFTS: %d total Form 4 filings in %s to %s", total, start_date, end_date)

    while offset + page_size < total:
        offset += page_size
        filings, _ = search_form4_filings(start_date, end_date, offset, page_size)
        all_filings.extend(filings)
        if len(filings) == 0:
            break
        if len(all_filings) % 500 == 0:
            logger.info("  Fetched %d/%d filing metadata...", len(all_filings), total)

    # Deduplicate by accession
    seen = set()
    deduped = []
    for f in all_filings:
        if f["accession"] not in seen:
            seen.add(f["accession"])
            deduped.append(f)

    logger.info("Fetched %d unique filing metadata entries", len(deduped))
    return deduped


# ── Form 4 XML Fetch + Parse ─────────────────────────────────────────────


def fetch_form4_xml(cik: str, accession: str) -> Tuple[Optional[str], Optional[str]]:
    """Download Form 4 XML from EDGAR. Returns (xml_string, accepted_at) or (None, None)."""
    acc_clean = accession.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession}-index.htm"
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(index_url, headers=headers, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            return None, None
    except requests.RequestException:
        return None, None

    index_html = resp.text

    # Extract acceptance timestamp (e.g. "2026-03-13 17:25:29")
    accepted_at = None
    dt_matches = re.findall(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})', index_html)
    if dt_matches:
        accepted_at = dt_matches[0]

    # Find XML link
    xml_links = re.findall(r'href="([^"]*\.xml)"', index_html, re.IGNORECASE)
    raw_xml_links = [l for l in xml_links if "/xsl" not in l.lower()]
    if not raw_xml_links:
        return None, accepted_at

    xml_file = raw_xml_links[0]
    for link in raw_xml_links:
        name = link.lower()
        if "form4" in name or "ownership" in name or "edgardoc" in name:
            xml_file = link
            break

    if xml_file.startswith("/"):
        xml_url = f"https://www.sec.gov{xml_file}"
    elif xml_file.startswith("http"):
        xml_url = xml_file
    else:
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{xml_file}"

    try:
        resp = requests.get(xml_url, headers=headers, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            return None, accepted_at
        return resp.text, accepted_at
    except requests.RequestException:
        return None, accepted_at


def parse_form4_xml(
    xml_text: str, cik: str, filing_date: str, company: str
) -> List[dict]:
    """
    Parse Form 4 XML and extract ALL non-derivative transactions (all codes).

    Returns list of trade dicts matching the insiders.db schema with new fields:
      ticker, insider_name, title, trade_type, trade_date, filing_date,
      price, qty, value, cik, company, is_csuite, title_weight,
      trans_code, trans_acquired_disp, direct_indirect, shares_owned_after,
      value_owned_after, nature_of_ownership, equity_swap, is_10b5_1,
      security_title, deemed_execution_date, trans_form_type, rptowner_cik,
      derivative_trades (list of derivative transaction dicts)
    """
    result = {"trades": [], "derivative_trades": []}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    # Handle XML namespaces
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def find(element, path):
        return element.find(f"{ns}{path}" if ns else path)

    def findall(element, path):
        return element.findall(f"{ns}{path}" if ns else path)

    def findtext(element, path, default=""):
        el = find(element, path)
        return el.text.strip() if el is not None and el.text else default

    def is_true(val):
        return val in ("1", "true", "True", "TRUE")

    # Issuer info
    issuer = find(root, "issuer")
    ticker = findtext(issuer, "issuerTradingSymbol") if issuer is not None else ""
    issuer_name = findtext(issuer, "issuerName") if issuer is not None else company

    ticker = normalize_ticker(ticker)
    if not ticker:
        return []

    # Reporting owner info (first owner)
    person = find(root, "reportingOwner")
    insider_name = ""
    title = ""
    rptowner_cik = ""
    if person is not None:
        person_id = find(person, "reportingOwnerId")
        if person_id is not None:
            insider_name = findtext(person_id, "rptOwnerName")
            rptowner_cik = findtext(person_id, "rptOwnerCik")

        relationship = find(person, "reportingOwnerRelationship")
        if relationship is not None:
            parts = []
            is_officer = findtext(relationship, "isOfficer")
            officer_title = findtext(relationship, "officerTitle")
            is_director = findtext(relationship, "isDirector")
            is_ten_pct = findtext(relationship, "isTenPercentOwner")

            if is_true(is_officer) and officer_title:
                parts.append(officer_title)
            elif is_true(is_director):
                parts.append("Dir")
            if is_true(is_ten_pct):
                parts.append("10%")
            title = ", ".join(parts) if parts else "Unknown"

    # Detect 10b5-1 plan from remarks/footnotes
    is_10b5_1 = 0
    remarks = findtext(root, "remarks")
    if remarks and ("10b5-1" in remarks.lower() or "10b5" in remarks.lower()):
        is_10b5_1 = 1

    # Also check footnotes in XML
    footnotes_el = find(root, "footnotes")
    if footnotes_el is not None:
        for fn in findall(footnotes_el, "footnote"):
            fn_text = fn.text or ""
            if fn.tail:
                fn_text += fn.tail
            if "10b5-1" in fn_text.lower() or "10b5" in fn_text.lower():
                is_10b5_1 = 1
                break

    trades = []

    # ── Parse non-derivative transactions — ALL codes ──
    nd_table = find(root, "nonDerivativeTable")
    if nd_table is not None:
        txn_tag = f"{ns}nonDerivativeTransaction" if ns else "nonDerivativeTransaction"
        for txn in nd_table.findall(txn_tag):
            txn_code = findtext(txn, "transactionCoding/transactionCode")

            # Map transaction code to trade_type
            if txn_code == "P":
                trade_type = "buy"
            elif txn_code == "S":
                trade_type = "sell"
            elif txn_code in ("F", "M", "A", "G", "V", "X"):
                # Non-P/S codes: still store as trades with their trans_code
                # trade_type reflects the acquisition/disposition direction
                acq_disp = findtext(txn, "transactionCoding/transactionAcquiredDisposedCode")
                if not acq_disp:
                    acq_disp = findtext(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")
                trade_type = "buy" if acq_disp == "A" else "sell"
            else:
                continue

            trade_date_raw = findtext(txn, "transactionDate/value", filing_date)
            trade_date = trade_date_raw[:10] if trade_date_raw else filing_date
            trade_date = validate_trade_date(trade_date, filing_date)
            if not trade_date:
                continue

            price_str = findtext(txn, "transactionAmounts/transactionPricePerShare/value", "0")
            qty_str = findtext(txn, "transactionAmounts/transactionShares/value", "0")

            try:
                price = float(price_str) if price_str else 0.0
                qty = float(qty_str) if qty_str else 0.0
            except ValueError:
                continue

            # For P/S codes, require valid price+qty; for other codes, allow zero price
            if txn_code in ("P", "S") and (price <= 0 or qty <= 0):
                continue
            if qty <= 0:
                continue

            value = price * abs(qty)

            # Post-transaction ownership
            post_el = find(txn, "postTransactionAmounts")
            shares_owned_after = 0.0
            value_owned_after = 0.0
            if post_el is not None:
                try:
                    shares_owned_after = float(findtext(post_el, "sharesOwnedFollowingTransaction/value", "0"))
                except ValueError:
                    pass
                try:
                    value_owned_after = float(findtext(post_el, "valueOwnedFollowingTransaction/value", "0"))
                except ValueError:
                    pass

            # Ownership nature
            ownership_el = find(txn, "ownershipNature")
            direct_indirect = ""
            nature_of_ownership = ""
            if ownership_el is not None:
                direct_indirect = findtext(ownership_el, "directOrIndirectOwnership/value")
                nature_of_ownership = findtext(ownership_el, "natureOfOwnership/value")

            # Equity swap
            equity_swap_str = findtext(txn, "transactionCoding/equitySwapInvolved")
            equity_swap = 1 if is_true(equity_swap_str) else 0

            # Acquisition/disposition code
            trans_acquired_disp = findtext(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")

            # Security title
            security_title = findtext(txn, "securityTitle/value")

            # Deemed execution date
            deemed_execution_date = findtext(txn, "deemedExecutionDate/value")

            # Transaction form type
            trans_form_type = findtext(txn, "transactionCoding/transactionFormType")

            trades.append({
                "ticker": ticker,
                "insider_name": insider_name,
                "title": title,
                "trade_type": trade_type,
                "trade_date": trade_date,
                "filing_date": filing_date,
                "price": price,
                "qty": int(abs(qty)),
                "value": value,
                "cik": cik,
                "company": issuer_name,
                "is_csuite": 1 if is_csuite(title) else 0,
                "title_weight": get_title_weight(title),
                # New fields
                "trans_code": txn_code,
                "trans_acquired_disp": trans_acquired_disp,
                "direct_indirect": direct_indirect,
                "shares_owned_after": shares_owned_after,
                "value_owned_after": value_owned_after,
                "nature_of_ownership": nature_of_ownership,
                "equity_swap": equity_swap,
                "is_10b5_1": is_10b5_1,
                "security_title": security_title,
                "deemed_execution_date": deemed_execution_date,
                "trans_form_type": trans_form_type,
                "rptowner_cik": rptowner_cik,
            })

    # ── Parse derivative transactions ──
    dt_table = find(root, "derivativeTable")
    if dt_table is not None:
        dtxn_tag = f"{ns}derivativeTransaction" if ns else "derivativeTransaction"
        for dtxn in dt_table.findall(dtxn_tag):
            txn_code = findtext(dtxn, "transactionCoding/transactionCode")
            if not txn_code:
                continue

            trade_date_raw = findtext(dtxn, "transactionDate/value", filing_date)
            trade_date = trade_date_raw[:10] if trade_date_raw else filing_date
            trade_date = validate_trade_date(trade_date, filing_date)
            if not trade_date:
                continue

            def safe_float(el, path, default=0.0):
                try:
                    return float(findtext(el, path, str(default)))
                except ValueError:
                    return default

            trans_acquired_disp = findtext(dtxn, "transactionAmounts/transactionAcquiredDisposedCode/value")

            # Post-transaction
            post_el = find(dtxn, "postTransactionAmounts")
            shares_after = 0.0
            value_after = 0.0
            if post_el is not None:
                shares_after = safe_float(post_el, "sharesOwnedFollowingTransaction/value")
                value_after = safe_float(post_el, "valueOwnedFollowingTransaction/value")

            # Ownership
            ownership_el = find(dtxn, "ownershipNature")
            direct_indirect = ""
            nature_of_ownership = ""
            if ownership_el is not None:
                direct_indirect = findtext(ownership_el, "directOrIndirectOwnership/value")
                nature_of_ownership = findtext(ownership_el, "natureOfOwnership/value")

            # Underlying security
            underlying_el = find(dtxn, "underlyingSecurity")
            underlying_title = ""
            underlying_shares = 0.0
            underlying_value = 0.0
            if underlying_el is not None:
                underlying_title = findtext(underlying_el, "underlyingSecurityTitle/value")
                underlying_shares = safe_float(underlying_el, "underlyingSecurityShares/value")
                underlying_value = safe_float(underlying_el, "underlyingSecurityValue/value")

            deriv_dict = {
                "ticker": ticker,
                "insider_name": insider_name,
                "title": title,
                "cik": cik,
                "company": issuer_name,
                "trans_code": txn_code,
                "trans_acquired_disp": trans_acquired_disp,
                "trade_date": trade_date,
                "filing_date": filing_date,
                "security_title": findtext(dtxn, "securityTitle/value"),
                "exercise_price": safe_float(dtxn, "conversionOrExercisePrice/value"),
                "expiration_date": findtext(dtxn, "expirationDate/value"),
                "trans_shares": safe_float(dtxn, "transactionAmounts/transactionShares/value"),
                "trans_price_per_share": safe_float(dtxn, "transactionAmounts/transactionPricePerShare/value"),
                "trans_total_value": safe_float(dtxn, "transactionAmounts/transactionTotalValue/value"),
                "underlying_title": underlying_title,
                "underlying_shares": underlying_shares,
                "underlying_value": underlying_value,
                "shares_owned_after": shares_after,
                "value_owned_after": value_after,
                "direct_indirect": direct_indirect,
                "nature_of_ownership": nature_of_ownership,
                "equity_swap": 1 if is_true(findtext(dtxn, "transactionCoding/equitySwapInvolved")) else 0,
                "is_10b5_1": is_10b5_1,
                "deemed_execution_date": findtext(dtxn, "deemedExecutionDate/value"),
                "trans_form_type": findtext(dtxn, "transactionCoding/transactionFormType"),
                "rptowner_cik": rptowner_cik,
                "is_csuite": 1 if is_csuite(title) else 0,
                "title_weight": get_title_weight(title),
            }
            result["derivative_trades"].append(deriv_dict)

            # Promote P/S derivative transactions on common stock underlying
            # to the main trades list. This captures economic exposure from
            # swaps, options exercises, and other derivative instruments that
            # reference the issuer's equity (e.g., total return swaps by 10%
            # owners). Without this, filings like DART KENNETH BRYAN's $90M+
            # FLUT swap purchases are invisible in the feed.
            if txn_code in ("P", "S") and underlying_title and "common" in underlying_title.lower():
                deriv_price = deriv_dict["trans_price_per_share"]
                deriv_qty = underlying_shares or deriv_dict["trans_shares"]
                deriv_value = deriv_price * deriv_qty if deriv_price and deriv_qty else deriv_dict["trans_total_value"]

                trades.append({
                    "ticker": ticker,
                    "insider_name": insider_name,
                    "insider_id": None,
                    "title": title,
                    "trade_type": "buy" if txn_code == "P" else "sell",
                    "trade_date": trade_date,
                    "filing_date": filing_date,
                    "price": deriv_price or 0,
                    "qty": int(abs(deriv_qty)) if deriv_qty else 0,
                    "value": abs(deriv_value) if deriv_value else 0,
                    "cik": cik,
                    "company": issuer_name,
                    "is_csuite": 1 if is_csuite(title) else 0,
                    "title_weight": get_title_weight(title),
                    "trans_code": txn_code,
                    "trans_acquired_disp": trans_acquired_disp,
                    "direct_indirect": direct_indirect,
                    "shares_owned_after": shares_after,
                    "value_owned_after": value_after,
                    "nature_of_ownership": nature_of_ownership,
                    "equity_swap": 1 if is_true(findtext(dtxn, "transactionCoding/equitySwapInvolved")) else 0,
                    "is_10b5_1": is_10b5_1,
                    "security_title": deriv_dict["security_title"],
                    "deemed_execution_date": deriv_dict["deemed_execution_date"],
                    "trans_form_type": deriv_dict["trans_form_type"],
                    "rptowner_cik": rptowner_cik,
                })

    return trades


def parse_form4_xml_full(
    xml_text: str, cik: str, filing_date: str, company: str
) -> dict:
    """
    Like parse_form4_xml() but returns a dict with both trades and derivative_trades.

    Returns: {"trades": [...], "derivative_trades": [...]}
    """
    # parse_form4_xml builds result["derivative_trades"] internally;
    # we need to capture it. Use a wrapper approach.
    trades = parse_form4_xml(xml_text, cik, filing_date, company)

    # Re-parse just for derivatives (parse_form4_xml already populates them
    # in the local `result` dict, but it's not exposed). For efficiency,
    # re-extract derivative trades from the XML.
    derivative_trades = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {"trades": trades, "derivative_trades": []}

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def find(element, path):
        return element.find(f"{ns}{path}" if ns else path)

    def findtext(element, path, default=""):
        el = find(element, path)
        return el.text.strip() if el is not None and el.text else default

    def is_true(val):
        return val in ("1", "true", "True", "TRUE")

    def safe_float(el, path, default=0.0):
        try:
            return float(findtext(el, path, str(default)))
        except ValueError:
            return default

    # Re-extract common fields
    issuer = find(root, "issuer")
    ticker = findtext(issuer, "issuerTradingSymbol") if issuer is not None else ""
    issuer_name = findtext(issuer, "issuerName") if issuer is not None else company
    ticker = normalize_ticker(ticker)

    person = find(root, "reportingOwner")
    insider_name = ""
    title = ""
    rptowner_cik = ""
    if person is not None:
        person_id = find(person, "reportingOwnerId")
        if person_id is not None:
            insider_name = findtext(person_id, "rptOwnerName")
            rptowner_cik = findtext(person_id, "rptOwnerCik")
        relationship = find(person, "reportingOwnerRelationship")
        if relationship is not None:
            parts = []
            if is_true(findtext(relationship, "isOfficer")):
                ot = findtext(relationship, "officerTitle")
                if ot:
                    parts.append(ot)
            elif is_true(findtext(relationship, "isDirector")):
                parts.append("Dir")
            if is_true(findtext(relationship, "isTenPercentOwner")):
                parts.append("10%")
            title = ", ".join(parts) if parts else "Unknown"

    # 10b5-1 detection
    is_10b5_1 = 0
    remarks = findtext(root, "remarks")
    if remarks and "10b5" in remarks.lower():
        is_10b5_1 = 1
    footnotes_el = find(root, "footnotes")
    if footnotes_el is not None:
        fn_tag = f"{ns}footnote" if ns else "footnote"
        for fn in footnotes_el.findall(fn_tag):
            fn_text = (fn.text or "") + (fn.tail or "")
            if "10b5" in fn_text.lower():
                is_10b5_1 = 1
                break

    if ticker:
        dt_table = find(root, "derivativeTable")
        if dt_table is not None:
            dtxn_tag = f"{ns}derivativeTransaction" if ns else "derivativeTransaction"
            for dtxn in dt_table.findall(dtxn_tag):
                txn_code = findtext(dtxn, "transactionCoding/transactionCode")
                if not txn_code:
                    continue
                trade_date_raw = findtext(dtxn, "transactionDate/value", filing_date)
                trade_date = trade_date_raw[:10] if trade_date_raw else filing_date
                trade_date = validate_trade_date(trade_date, filing_date)
                if not trade_date:
                    continue

                trans_acquired_disp = findtext(dtxn, "transactionAmounts/transactionAcquiredDisposedCode/value")

                post_el = find(dtxn, "postTransactionAmounts")
                shares_after = safe_float(post_el, "sharesOwnedFollowingTransaction/value") if post_el else 0.0
                value_after = safe_float(post_el, "valueOwnedFollowingTransaction/value") if post_el else 0.0

                ownership_el = find(dtxn, "ownershipNature")
                direct_indirect = findtext(ownership_el, "directOrIndirectOwnership/value") if ownership_el else ""
                nature_of_ownership = findtext(ownership_el, "natureOfOwnership/value") if ownership_el else ""

                underlying_el = find(dtxn, "underlyingSecurity")
                underlying_title = findtext(underlying_el, "underlyingSecurityTitle/value") if underlying_el else ""
                underlying_shares = safe_float(underlying_el, "underlyingSecurityShares/value") if underlying_el else 0.0
                underlying_value = safe_float(underlying_el, "underlyingSecurityValue/value") if underlying_el else 0.0

                derivative_trades.append({
                    "ticker": ticker,
                    "insider_name": insider_name,
                    "title": title,
                    "cik": cik,
                    "company": issuer_name,
                    "trans_code": txn_code,
                    "trans_acquired_disp": trans_acquired_disp,
                    "trade_date": trade_date,
                    "filing_date": filing_date,
                    "security_title": findtext(dtxn, "securityTitle/value"),
                    "exercise_price": safe_float(dtxn, "conversionOrExercisePrice/value"),
                    "expiration_date": findtext(dtxn, "expirationDate/value"),
                    "trans_shares": safe_float(dtxn, "transactionAmounts/transactionShares/value"),
                    "trans_price_per_share": safe_float(dtxn, "transactionAmounts/transactionPricePerShare/value"),
                    "trans_total_value": safe_float(dtxn, "transactionAmounts/transactionTotalValue/value"),
                    "underlying_title": underlying_title,
                    "underlying_shares": underlying_shares,
                    "underlying_value": underlying_value,
                    "shares_owned_after": shares_after,
                    "value_owned_after": value_after,
                    "direct_indirect": direct_indirect,
                    "nature_of_ownership": nature_of_ownership,
                    "equity_swap": 1 if is_true(findtext(dtxn, "transactionCoding/equitySwapInvolved")) else 0,
                    "is_10b5_1": is_10b5_1,
                    "deemed_execution_date": findtext(dtxn, "deemedExecutionDate/value"),
                    "trans_form_type": findtext(dtxn, "transactionCoding/transactionFormType"),
                    "rptowner_cik": rptowner_cik,
                    "is_csuite": 1 if is_csuite(title) else 0,
                    "title_weight": get_title_weight(title),
                })

    return {"trades": trades, "derivative_trades": derivative_trades}


# ── DB Insertion ─────────────────────────────────────────────────────────


def insert_trades(conn, trades: List[dict], accession: str, filed_at: Optional[str] = None) -> int:
    """Insert parsed trades into insiders.db. Returns count of new rows."""
    inserted = 0
    for t in trades:
        insider_id = get_or_create_insider(conn, t["insider_name"], t["cik"])

        # Flag entity insiders on insert
        if is_entity_name and is_entity_name(normalize_name(t["insider_name"])):
            try:
                conn.execute("UPDATE insiders SET is_entity = 1 WHERE insider_id = ? AND is_entity = 0", (insider_id,))
            except Exception:
                pass  # Column may not exist yet

        # Compute normalized title
        norm_title = ""
        try:
            from strategies.insider_catalog.normalize_titles import normalize_title
            norm_title = normalize_title(t["title"])
        except ImportError:
            pass

        try:
            conn.execute("""
                INSERT OR IGNORE INTO trades
                    (insider_id, ticker, company, title, trade_type, trade_date,
                     filing_date, price, qty, value, is_csuite, title_weight,
                     source, accession, normalized_title, filed_at,
                     trans_code, trans_acquired_disp, direct_indirect,
                     shares_owned_after, value_owned_after, nature_of_ownership,
                     equity_swap, is_10b5_1, security_title,
                     deemed_execution_date, trans_form_type, rptowner_cik)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'edgar_live', ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                insider_id, t["ticker"], t["company"], t["title"],
                t["trade_type"], t["trade_date"], t["filing_date"],
                t["price"], t["qty"], t["value"],
                t["is_csuite"], t["title_weight"], accession, norm_title,
                filed_at,
                t.get("trans_code"),
                t.get("trans_acquired_disp"),
                t.get("direct_indirect") or None,
                t.get("shares_owned_after") if t.get("shares_owned_after") else None,
                t.get("value_owned_after") if t.get("value_owned_after") else None,
                t.get("nature_of_ownership") or None,
                t.get("equity_swap"),
                t.get("is_10b5_1"),
                t.get("security_title") or None,
                t.get("deemed_execution_date") or None,
                t.get("trans_form_type") or None,
                t.get("rptowner_cik") or None,
            ))
            inserted += 1
        except Exception:
            pass  # duplicate
    return inserted


def insert_derivative_trades(conn, deriv_trades: List[dict], accession: str) -> int:
    """Insert derivative transactions into derivative_trades table."""
    inserted = 0
    for t in deriv_trades:
        insider_id = get_or_create_insider(conn, t["insider_name"], t.get("cik", ""))

        try:
            conn.execute("""
                INSERT OR IGNORE INTO derivative_trades
                    (insider_id, ticker, company, title, trans_code, trans_acquired_disp,
                     trade_date, filing_date, security_title, exercise_price, expiration_date,
                     trans_shares, trans_price_per_share, trans_total_value,
                     underlying_title, underlying_shares, underlying_value,
                     shares_owned_after, value_owned_after, direct_indirect,
                     nature_of_ownership, equity_swap, is_10b5_1,
                     deemed_execution_date, trans_form_type, rptowner_cik,
                     accession, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'edgar_live')
            """, (
                insider_id, t["ticker"], t.get("company"), t.get("title"),
                t["trans_code"], t.get("trans_acquired_disp"),
                t["trade_date"], t["filing_date"],
                t.get("security_title") or None,
                t.get("exercise_price") if t.get("exercise_price") else None,
                t.get("expiration_date") or None,
                t.get("trans_shares") if t.get("trans_shares") else None,
                t.get("trans_price_per_share") if t.get("trans_price_per_share") else None,
                t.get("trans_total_value") if t.get("trans_total_value") else None,
                t.get("underlying_title") or None,
                t.get("underlying_shares") if t.get("underlying_shares") else None,
                t.get("underlying_value") if t.get("underlying_value") else None,
                t.get("shares_owned_after") if t.get("shares_owned_after") else None,
                t.get("value_owned_after") if t.get("value_owned_after") else None,
                t.get("direct_indirect") or None,
                t.get("nature_of_ownership") or None,
                t.get("equity_swap"),
                t.get("is_10b5_1"),
                t.get("deemed_execution_date") or None,
                t.get("trans_form_type") or None,
                t.get("rptowner_cik") or None,
                accession,
            ))
            inserted += 1
        except Exception:
            pass
    return inserted


# ── Validation ───────────────────────────────────────────────────────────


def validate_against_existing(
    conn, start_date: str, end_date: str
) -> None:
    """
    Compare EFTS-parsed trades against existing bulk-imported trades for a
    date range to confirm format parity.
    """
    logger.info("=" * 60)
    logger.info("VALIDATION MODE: comparing EFTS vs existing DB for %s to %s", start_date, end_date)
    logger.info("=" * 60)

    # Get existing trades from DB in this date range
    existing = conn.execute("""
        SELECT t.ticker, t.trade_date, t.trade_type, t.insider_id, t.value,
               i.name, i.name_normalized, t.title, t.price, t.qty
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.filing_date BETWEEN ? AND ?
        ORDER BY t.filing_date, t.ticker
    """, (start_date, end_date)).fetchall()

    logger.info("Existing DB trades in range: %d", len(existing))

    # Build lookup: (ticker, trade_date, name_normalized, trade_type) → values
    existing_lookup = {}  # type: Dict[tuple, list]
    for row in existing:
        key = (row[0], row[1], row[2], row[6])  # ticker, trade_date, trade_type, name_norm
        if key not in existing_lookup:
            existing_lookup[key] = []
        existing_lookup[key].append({
            "value": row[4], "name": row[5], "title": row[7],
            "price": row[8], "qty": row[9],
        })

    # Fetch a sample of filings from EFTS and parse
    # Use a 7-day window within the range for manageable sample size
    sample_end = end_date
    sample_start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)
    sample_start = sample_start_dt.strftime("%Y-%m-%d")

    filings = fetch_all_form4_filings(sample_start, sample_end)
    logger.info("EFTS filings in sample window (%s to %s): %d", sample_start, sample_end, len(filings))

    # Parse a sample (first 200 filings)
    sample = filings[:200]
    efts_trades = []
    parsed_count = 0
    xml_fail_count = 0

    for i, filing in enumerate(sample):
        xml, _filed_at = fetch_form4_xml(filing["cik"], filing["accession"])
        if xml is None:
            xml_fail_count += 1
            continue
        trades = parse_form4_xml(xml, filing["cik"], filing["filing_date"], filing["company"])
        efts_trades.extend(trades)
        parsed_count += 1
        if (i + 1) % 50 == 0:
            logger.info("  Parsed %d/%d filings (%d trades so far)...", i + 1, len(sample), len(efts_trades))

    logger.info("Parsed %d filings (%d failed XML fetch), got %d trades",
                parsed_count, xml_fail_count, len(efts_trades))

    # Compare: for each EFTS trade, check if a matching trade exists in DB
    matched = 0
    unmatched = 0
    value_mismatches = 0
    title_diffs = 0

    for t in efts_trades:
        name_norm = normalize_name(t["insider_name"])
        key = (t["ticker"], t["trade_date"], t["trade_type"], name_norm)

        if key in existing_lookup:
            matched += 1
            # Check value parity
            db_trades = existing_lookup[key]
            value_match = any(abs(db["value"] - t["value"]) / max(t["value"], 1) < 0.01 for db in db_trades)
            if not value_match:
                value_mismatches += 1
                if value_mismatches <= 5:
                    logger.warning(
                        "  Value mismatch: %s %s %s — EFTS: $%.0f, DB: %s",
                        t["ticker"], t["trade_date"], t["insider_name"],
                        t["value"],
                        [f"${d['value']:.0f}" for d in db_trades],
                    )
            # Check title parity (informational, not a failure)
            title_match = any(db["title"] == t["title"] for db in db_trades)
            if not title_match:
                title_diffs += 1
        else:
            unmatched += 1
            if unmatched <= 10:
                logger.info(
                    "  No DB match: %s %s %s %s $%.0f",
                    t["ticker"], t["trade_date"], t["trade_type"],
                    t["insider_name"], t["value"],
                )

    logger.info("")
    logger.info("=" * 60)
    logger.info("VALIDATION RESULTS")
    logger.info("=" * 60)
    logger.info("  EFTS trades parsed:  %d", len(efts_trades))
    logger.info("  Matched in DB:       %d (%.1f%%)",
                matched, matched / max(len(efts_trades), 1) * 100)
    logger.info("  Unmatched:           %d (%.1f%%)",
                unmatched, unmatched / max(len(efts_trades), 1) * 100)
    logger.info("  Value mismatches:    %d", value_mismatches)
    logger.info("  Title differences:   %d (expected — XML vs bulk title formats differ)", title_diffs)
    logger.info("=" * 60)

    if matched / max(len(efts_trades), 1) > 0.80:
        logger.info("PASS — >80%% match rate. EFTS parsing is compatible with bulk data.")
    else:
        logger.warning("LOW MATCH RATE — review unmatched trades above for format issues.")


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Live EDGAR backfill into insiders.db")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--validate", action="store_true",
                        help="Validate EFTS parsing against existing DB data (no writes)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and report without inserting into DB")
    parser.add_argument("--refresh-scores", action="store_true",
                        help="Recompute track records after inserting trades")
    parser.add_argument("--min-value", type=float, default=0,
                        help="Minimum trade value to insert (default: 0 = all)")
    args = parser.parse_args()

    conn = get_connection()

    # Ensure schema has new columns/tables
    migrate_schema(conn)

    if args.validate:
        validate_against_existing(conn, args.start, args.end)
        conn.close()
        return

    # Count existing trades in range
    pre_count = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE filing_date BETWEEN ? AND ?",
        (args.start, args.end),
    ).fetchone()[0]
    logger.info("Existing trades in %s to %s: %d", args.start, args.end, pre_count)

    # Fetch all filing metadata from EFTS
    filings = fetch_all_form4_filings(args.start, args.end)

    # Process filings: fetch XML, parse, insert
    total_inserted = 0
    total_trades_parsed = 0
    total_skipped = 0
    xml_failures = 0
    buy_count = 0
    sell_count = 0

    start_time = time.monotonic()

    for i, filing in enumerate(filings):
        xml, filed_at = fetch_form4_xml(filing["cik"], filing["accession"])
        if xml is None:
            xml_failures += 1
            continue

        parsed = parse_form4_xml_full(
            xml, filing["cik"], filing["filing_date"], filing["company"]
        )
        trades = parsed["trades"]
        deriv_trades = parsed["derivative_trades"]
        total_trades_parsed += len(trades)

        # Filter by min value if specified
        if args.min_value > 0:
            before = len(trades)
            trades = [t for t in trades if t["value"] >= args.min_value]
            total_skipped += before - len(trades)

        for t in trades:
            if t["trade_type"] == "buy":
                buy_count += 1
            else:
                sell_count += 1

        if not args.dry_run and trades:
            inserted = insert_trades(conn, trades, filing["accession"], filed_at=filed_at)
            total_inserted += inserted
        if not args.dry_run and deriv_trades:
            insert_derivative_trades(conn, deriv_trades, filing["accession"])
            if (i + 1) % 100 == 0:
                conn.commit()

        # Progress logging
        if (i + 1) % 100 == 0:
            elapsed = time.monotonic() - start_time
            rate = (i + 1) / elapsed
            eta = (len(filings) - i - 1) / rate if rate > 0 else 0
            logger.info(
                "Progress: %d/%d filings (%.1f/sec) | %d trades parsed | "
                "%d inserted | %d buys, %d sells | ETA: %.0f min",
                i + 1, len(filings), rate, total_trades_parsed,
                total_inserted, buy_count, sell_count, eta / 60,
            )

    if not args.dry_run:
        conn.commit()

    elapsed = time.monotonic() - start_time

    # Post count
    post_count = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE filing_date BETWEEN ? AND ?",
        (args.start, args.end),
    ).fetchone()[0]

    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info("  Date range:         %s to %s", args.start, args.end)
    logger.info("  Filings processed:  %d", len(filings))
    logger.info("  XML fetch failures: %d", xml_failures)
    logger.info("  Trades parsed:      %d (%d buys, %d sells)", total_trades_parsed, buy_count, sell_count)
    if args.min_value > 0:
        logger.info("  Trades below min value ($%.0f): %d skipped", args.min_value, total_skipped)
    if args.dry_run:
        logger.info("  DRY RUN — no trades inserted")
    else:
        logger.info("  New trades inserted: %d", total_inserted)
        logger.info("  Trades in range:     %d → %d", pre_count, post_count)
    logger.info("  Elapsed:            %.1f min (%.1f filings/sec)", elapsed / 60, len(filings) / max(elapsed, 1))
    logger.info("=" * 60)

    # Validate and fix suspect trade prices
    if not args.dry_run and total_inserted > 0:
        try:
            from strategies.insider_catalog.price_validator import run_validation
        except ImportError:
            from price_validator import run_validation
        logger.info("Running price validation on new trades...")
        run_validation(conn)

        # Clean display names for any new insiders
        try:
            from strategies.insider_catalog.name_cleaner import clean_name, ensure_column
        except ImportError:
            from name_cleaner import clean_name, ensure_column
        ensure_column(conn)
        new_insiders = conn.execute(
            "SELECT insider_id, name, COALESCE(is_entity, 0) FROM insiders WHERE display_name IS NULL"
        ).fetchall()
        if new_insiders:
            for insider_id, name, is_entity in new_insiders:
                display = clean_name(name, bool(is_entity))
                conn.execute(
                    "UPDATE insiders SET display_name = ? WHERE insider_id = ?",
                    (display, insider_id),
                )
            conn.commit()
            logger.info("Cleaned display names for %d new insiders", len(new_insiders))

    # Update PIT scores for newly inserted trades
    if not args.dry_run and total_inserted > 0:
        try:
            from pit_scoring import compute_insider_ticker_score, upsert_score
            logger.info("Updating PIT scores for new trades...")
            # Get the newly inserted trades
            new_trades = conn.execute("""
                SELECT trade_id, insider_id, ticker, filing_date
                FROM trades
                WHERE filing_date BETWEEN ? AND ?
                  AND source = 'edgar_live'
                  AND trade_type = 'buy'
                ORDER BY filing_date ASC
            """, (args.start, args.end)).fetchall()
            pit_count = 0
            for trade_id, insider_id, ticker, filing_date in new_trades:
                score = compute_insider_ticker_score(conn, insider_id, ticker, filing_date)
                upsert_score(conn, score, trigger_trade_id=trade_id)
                pit_count += 1
            conn.commit()
            logger.info("Updated %d PIT scores", pit_count)
        except ImportError:
            logger.debug("pit_scoring not available, skipping PIT score update")
        except Exception as e:
            logger.warning("PIT score update failed: %s", e)

    if args.refresh_scores and not args.dry_run:
        logger.info("Recomputing track records...")
        compute_track_records(conn)
        print_summary(conn)

    conn.close()


if __name__ == "__main__":
    main()
