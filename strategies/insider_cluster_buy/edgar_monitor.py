"""
EDGAR RSS monitor for Form 4 insider filings.

Polls SEC EDGAR full-text search for new Form 4 filings, parses the XML,
maintains a 30-day rolling window per ticker, and checks for qualifying
cluster buy signals.
"""

from __future__ import annotations

import logging
import math
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, date
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# SEC EDGAR endpoints
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}"

# Rate limiting: SEC allows 10 req/sec
REQUEST_DELAY = 0.15

# Cluster signal thresholds (from strategy spec)
MIN_CLUSTER_INSIDERS = 2
MIN_CLUSTER_VALUE = 5_000_000
MIN_QUALITY_SCORE = 2.0

# ── Title weights (mirrored from build_event_calendar.py) ────────────────
TITLE_WEIGHT_RULES = [
    (["ceo", "chief exec"],                           3.0),
    (["chairman", "exec chair", "executive chair"],   3.0),
    (["cfo", "chief financial"],                      2.5),
    (["president"],                                   2.5),
    (["10% owner", "10 percent owner", "10pct"],      2.5),
    (["coo", "chief operating"],                      2.0),
    (["svp", "evp", "senior vp", "senior vice",
      "exec vp", "executive vp", "executive vice president"],  1.8),
    (["vp", "vice president"],                        1.5),
    (["director", "board"],                           1.5),
    (["treasurer", "secretary"],                      1.2),
]
DEFAULT_TITLE_WEIGHT = 1.0

CSUITE_KEYWORDS = [
    "ceo", "chief exec", "chief executive", "co-ceo",
    "cfo", "chief financial", "chief fin",
    "coo", "chief operating",
    "president", "pres",
    "chairman", "chairwoman", "chair", "cob",
    "evp", "executive vp", "executive vice president",
    "svp", "senior vp", "senior vice president",
]

CONF_NORMALIZER = 112.5


def get_title_weight(title: str) -> float:
    if not isinstance(title, str) or not title.strip():
        return DEFAULT_TITLE_WEIGHT
    t = title.lower()
    for keywords, weight in TITLE_WEIGHT_RULES:
        for kw in keywords:
            if kw in t:
                return weight
    return DEFAULT_TITLE_WEIGHT


def is_csuite(title: str) -> bool:
    if not isinstance(title, str) or not title.strip():
        return False
    t = title.lower()
    return any(kw in t for kw in CSUITE_KEYWORDS)


def compute_confidence_score(
    total_value: float,
    n_distinct_insiders: int,
    title_weights: list,
    max_single_value: float,
) -> dict:
    """Compute multi-factor confidence score for one event group."""
    value_score = min(5.0, math.log10(max(1.0, total_value) / 25_000) + 1.0)
    breadth_score = min(5.0, 1.0 + math.log2(max(1, n_distinct_insiders)))
    quality_score = sum(title_weights) / len(title_weights) if title_weights else DEFAULT_TITLE_WEIGHT
    concentration = max_single_value / total_value if total_value > 0 else 1.0
    concentration_bonus = concentration * 0.5
    raw = value_score * breadth_score * quality_score * (1.0 + concentration_bonus)
    confidence_score = min(100.0, raw / CONF_NORMALIZER * 100.0)
    return {
        "value_score": round(value_score, 4),
        "breadth_score": round(breadth_score, 4),
        "quality_score": round(quality_score, 4),
        "concentration": round(concentration, 4),
        "confidence_score": round(confidence_score, 4),
    }


# ── EDGAR RSS Polling ────────────────────────────────────────────────────


def poll_edgar_rss(
    last_seen_accession: Optional[str],
    user_agent: str,
    lookback_days: int = 2,
) -> tuple[list[dict], Optional[str]]:
    """
    Poll SEC EDGAR full-text search for new Form 4 filings.

    Returns:
        (new_filings, latest_accession) where new_filings is a list of dicts
        with keys: accession, cik, company, filing_date, url
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)

    params = {
        "q": '"4"',
        "forms": "4",
        "dateRange": "custom",
        "startdt": start_date.isoformat(),
        "enddt": end_date.isoformat(),
    }
    headers = {"User-Agent": user_agent, "Accept": "application/atom+xml"}

    try:
        resp = requests.get(
            EDGAR_SEARCH_URL, params=params, headers=headers, timeout=30
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("EDGAR RSS poll failed: %s", e)
        return [], last_seen_accession

    # Parse the EDGAR EFTS response (JSON format)
    try:
        data = resp.json()
    except ValueError:
        logger.error("EDGAR response not valid JSON, length=%d", len(resp.text))
        return [], last_seen_accession

    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        logger.debug("No Form 4 filings found in EDGAR search")
        return [], last_seen_accession

    new_filings = []
    latest_accession = last_seen_accession
    seen_accessions = set()

    for hit in hits:
        source = hit.get("_source", {})
        if not source:
            continue

        # _id format: "accession_number:document_filename"
        raw_id = hit.get("_id", "")
        if ":" in raw_id:
            accession = raw_id.split(":")[0]
        else:
            accession = raw_id
        if not accession:
            continue

        # Deduplicate by accession (same filing may appear multiple times)
        if accession in seen_accessions:
            continue
        seen_accessions.add(accession)

        # Skip already-seen filings
        if last_seen_accession and accession <= last_seen_accession:
            continue

        # CIKs list: first is typically the reporting person (insider)
        ciks = source.get("ciks", [])
        display_names = source.get("display_names", [])
        cik = ciks[0] if ciks else ""
        company = display_names[-1] if display_names else ""  # Last is usually the issuer

        # Clean CIK (remove leading zeros for some uses, keep for URLs)
        filing = {
            "accession": accession,
            "cik": str(cik),
            "company": company,
            "filing_date": source.get("file_date", ""),
            "form_type": "4",
        }

        if not filing["cik"]:
            continue

        new_filings.append(filing)

        if latest_accession is None or accession > latest_accession:
            latest_accession = accession

    logger.info("EDGAR poll: %d new filings found (deduplicated)", len(new_filings))
    return new_filings, latest_accession


# ── Form 4 XML Parsing ──────────────────────────────────────────────────


def fetch_form4_xml(cik: str, accession: str, user_agent: str) -> Optional[str]:
    """
    Download the Form 4 filing index page, find the XML document, and fetch it.
    Returns XML string or None.
    """
    # Clean accession for URL: remove dashes for directory, keep for filename
    acc_clean = accession.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession}-index.htm"

    headers = {"User-Agent": user_agent}

    try:
        resp = requests.get(index_url, headers=headers, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            logger.debug("Filing index 404 for %s/%s", cik, accession)
            return None
    except requests.RequestException as e:
        logger.debug("Failed to fetch filing index: %s", e)
        return None

    # Find the XML file link in the index page
    # Skip xsl-transformed links (those produce HTML, not raw XML)
    xml_links = re.findall(
        r'href="([^"]*\.xml)"', resp.text, re.IGNORECASE
    )
    # Filter out XSLT-transformed versions
    raw_xml_links = [l for l in xml_links if "/xsl" not in l.lower()]
    if not raw_xml_links:
        logger.debug("No raw XML links found in filing index for %s", accession)
        return None

    # Prefer links with "form4", "ownership", or "edgardoc" in name
    xml_file = raw_xml_links[0]
    for link in raw_xml_links:
        name = link.lower()
        if "form4" in name or "ownership" in name or "edgardoc" in name:
            xml_file = link
            break

    # Build absolute URL
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
            return None
        return resp.text
    except requests.RequestException:
        return None


def parse_form4_xml(xml_text: str, cik: str, filing_date: str, company: str) -> list[dict]:
    """
    Parse Form 4 XML and extract non-derivative purchase transactions.
    Returns list of trade dicts with: ticker, insider_name, title, trade_date,
    price, qty, value, filing_date, cik, company.
    """
    trades = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return trades

    # Handle XML namespaces
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def find(element, path):
        return element.find(f"{ns}{path}" if ns else path)

    def findtext(element, path, default=""):
        el = find(element, path)
        return el.text.strip() if el is not None and el.text else default

    # Issuer info
    issuer = find(root, "issuer")
    ticker = findtext(issuer, "issuerTradingSymbol") if issuer is not None else ""
    issuer_name = findtext(issuer, "issuerName") if issuer is not None else company

    if not ticker:
        return trades

    ticker = ticker.upper().strip()

    # Reporting owner info
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

            def is_true(val):
                return val in ("1", "true", "True", "TRUE")

            if is_true(is_officer) and officer_title:
                parts.append(officer_title)
            elif is_true(is_director):
                parts.append("Dir")
            if is_true(is_ten_pct):
                parts.append("10%")
            title = ", ".join(parts) if parts else "Unknown"

    # Parse non-derivative transactions (purchase code = "P")
    nd_table = find(root, "nonDerivativeTable")
    if nd_table is None:
        return trades

    tag = f"{ns}nonDerivativeTransaction" if ns else "nonDerivativeTransaction"
    for txn in nd_table.findall(tag):
        txn_code = findtext(txn, "transactionCoding/transactionCode")
        if txn_code != "P":
            continue

        trade_date_raw = findtext(txn, "transactionDate/value", filing_date)
        trade_date = trade_date_raw[:10] if trade_date_raw else filing_date

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
            "ticker": ticker,
            "insider_name": insider_name,
            "title": title,
            "trade_date": trade_date,
            "filing_date": filing_date,
            "price": price,
            "qty": int(qty),
            "value": value,
            "cik": cik,
            "company": issuer_name,
            "is_csuite": is_csuite(title),
            "title_weight": get_title_weight(title),
        })

    return trades


# ── Rolling Window + Cluster Detection ───────────────────────────────────


def update_rolling_window(
    trades: list[dict],
    rolling_window: dict,
    window_days: int = 30,
) -> None:
    """
    Add parsed trades to the 30-day rolling window (keyed by ticker).
    Prunes entries older than window_days.
    """
    cutoff = (date.today() - timedelta(days=window_days)).isoformat()

    for trade in trades:
        ticker = trade["ticker"]
        if ticker not in rolling_window:
            rolling_window[ticker] = []
        rolling_window[ticker].append(trade)

    # Prune old entries across all tickers
    for ticker in list(rolling_window.keys()):
        rolling_window[ticker] = [
            t for t in rolling_window[ticker]
            if t.get("filing_date", "") >= cutoff
        ]
        if not rolling_window[ticker]:
            del rolling_window[ticker]


def check_cluster_trigger(ticker: str, rolling_window: dict) -> Optional[dict]:
    """
    Check if the filings for a ticker form a qualifying cluster signal.

    Qualifying cluster (from strategy spec):
      - 2+ distinct insiders
      - $5M+ total purchase value
      - Quality score >= 2.0 (mean title weight)
      - At least one C-suite insider (not just routine 10% owners)

    Returns signal dict or None.
    """
    filings = rolling_window.get(ticker, [])
    if not filings:
        return None

    # Count distinct insiders
    distinct_insiders = set()
    total_value = 0.0
    title_weights = []
    max_single_value = 0.0
    insider_values = defaultdict(float)
    has_csuite = False

    for f in filings:
        name = f.get("insider_name", "Unknown")
        distinct_insiders.add(name)
        val = f.get("value", 0)
        total_value += val
        insider_values[name] += val
        title_weights.append(f.get("title_weight", DEFAULT_TITLE_WEIGHT))
        if f.get("is_csuite", False):
            has_csuite = True

    n_insiders = len(distinct_insiders)
    max_single_value = max(insider_values.values()) if insider_values else 0

    # Apply cluster filters
    if n_insiders < MIN_CLUSTER_INSIDERS:
        return None
    if total_value < MIN_CLUSTER_VALUE:
        return None
    if not has_csuite:
        return None

    # Compute confidence
    conf = compute_confidence_score(
        total_value=total_value,
        n_distinct_insiders=n_insiders,
        title_weights=title_weights,
        max_single_value=max_single_value,
    )

    quality = conf["quality_score"]
    if quality < MIN_QUALITY_SCORE:
        return None

    # Build signal
    latest_filing = max(filings, key=lambda f: f.get("filing_date", ""))
    company = latest_filing.get("company", ticker)

    signal = {
        "ticker": ticker,
        "company": company,
        "n_insiders": n_insiders,
        "insiders": list(distinct_insiders),
        "total_value": total_value,
        "confidence": conf["confidence_score"],
        "quality_score": quality,
        "trigger_date": latest_filing["filing_date"],
        "entry_date": date.today().isoformat(),  # Queue for next market open
        "filings": filings,
    }

    logger.info(
        "CLUSTER SIGNAL: %s — %d insiders, $%.0f, confidence=%.1f, quality=%.2f",
        ticker, n_insiders, total_value, conf["confidence_score"], quality,
    )
    return signal
