"""Shared PTR parsing for House PDFs and Senate HTML pages.

Reuses and improves the parsing logic from the batch importers.
Two strategies for House PDFs (table extraction first, regex fallback).
Separate parser for Senate eFD HTML PTR pages.
"""
from __future__ import annotations

import io
import re
from typing import Optional

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
    "Over $50,000,000": (50000001, 100000000),
}

TRADE_TYPE_MAP = {"P": "buy", "S": "sell", "E": "exchange"}

OWNER_MAP = {
    "self": "Self",
    "sp": "Spouse",
    "spouse": "Spouse",
    "jt": "Joint",
    "joint": "Joint",
    "dc": "Child",
    "child": "Child",
    "dependent": "Child",
}


def parse_amount(amount_str: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Parse a STOCK Act amount string into (low, high, estimate)."""
    if not amount_str or amount_str.strip() in ("--", "N/A", ""):
        return None, None, None

    amount_str = re.sub(r"\s+", " ", amount_str.strip())

    # Try known bands first
    normalized = amount_str.replace(" ", "").replace("\n", "")
    for band_str, (lo, hi) in AMOUNT_BANDS.items():
        if band_str.replace(" ", "") in normalized:
            return lo, hi, (lo + hi) // 2

    # Try regex: $X - $Y
    m = re.search(r"\$?([\d,]+)\s*-\s*\$?([\d,]+)", amount_str)
    if m:
        lo = int(m.group(1).replace(",", ""))
        hi = int(m.group(2).replace(",", ""))
        return lo, hi, (lo + hi) // 2

    return None, None, None


def parse_date(date_str: str) -> Optional[str]:
    """Convert MM/DD/YYYY to YYYY-MM-DD. Passes through YYYY-MM-DD. Returns None on failure."""
    if not date_str or date_str.strip() in ("--", "", "N/A"):
        return None
    date_str = date_str.strip()
    if re.match(r"\d{4}-\d{2}-\d{2}$", date_str):
        return date_str
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    return None


def extract_ticker(asset_str: str) -> Optional[str]:
    """Extract ticker symbol from asset description like 'Apple Inc. (AAPL) [ST]'."""
    m = re.search(r"\(([A-Z]{1,5})\)", asset_str)
    if m:
        ticker = m.group(1)
        # Filter false positives
        if ticker in ("N", "A", "I", "II", "III", "IV", "V", "VI", "DC", "SP", "JT", "NA"):
            return None
        return ticker
    return None


def extract_company(asset_str: str, ticker: Optional[str]) -> Optional[str]:
    """Extract company name from asset description."""
    if not ticker:
        return None
    m = re.match(r"^(.+?)\s*\(" + re.escape(ticker) + r"\)", asset_str)
    if m:
        return m.group(1).strip().rstrip("-").strip()
    return None


def normalize_owner(owner_str: str) -> Optional[str]:
    """Map owner field to normalized value."""
    if not owner_str or owner_str.strip() in ("--", ""):
        return None
    return OWNER_MAP.get(owner_str.strip().lower(), owner_str.strip())


# ---------------------------------------------------------------------------
# House PTR PDF parsing
# ---------------------------------------------------------------------------


def parse_house_ptr_pdf(pdf_data: bytes) -> list[dict]:
    """Parse a House PTR PDF using pdfplumber table extraction with regex fallback."""
    import pdfplumber

    pdf = pdfplumber.open(io.BytesIO(pdf_data))
    transactions: list[dict] = []

    # Strategy A: table extraction
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            if not table or len(table) < 2:
                continue

            # Find header row
            header_idx = -1
            for i, row in enumerate(table):
                if row and any("transaction" in str(c).lower() for c in row if c):
                    header_idx = i
                    break

            if header_idx < 0:
                continue

            headers = [str(c).strip().lower() if c else "" for c in table[header_idx]]

            # Map column indices
            col_map = {}
            for idx, h in enumerate(headers):
                if "owner" in h:
                    col_map["owner"] = idx
                elif "asset" in h:
                    col_map["asset"] = idx
                elif "type" in h and "asset" not in h:
                    col_map["type"] = idx
                elif "transaction" in h and "date" in h:
                    col_map["tx_date"] = idx
                elif "notification" in h or "filing" in h:
                    col_map["filing_date"] = idx
                elif "amount" in h:
                    col_map["amount"] = idx
                elif "date" in h and "tx_date" not in col_map:
                    col_map["tx_date"] = idx

            if "asset" not in col_map:
                continue

            # Parse data rows
            for row in table[header_idx + 1:]:
                if not row or all(not c for c in row):
                    continue

                def cell(key: str) -> str:
                    idx = col_map.get(key)
                    if idx is not None and idx < len(row) and row[idx]:
                        return str(row[idx]).strip()
                    return ""

                asset = cell("asset")
                if not asset:
                    continue

                ticker = extract_ticker(asset)
                if not ticker:
                    continue

                tx_type_raw = cell("type").strip()
                trade_type = TRADE_TYPE_MAP.get(tx_type_raw[:1].upper()) if tx_type_raw else None
                if not trade_type:
                    # Try full text matching
                    lower = tx_type_raw.lower()
                    if "purchase" in lower:
                        trade_type = "buy"
                    elif "sale" in lower:
                        trade_type = "sell"
                    elif "exchange" in lower:
                        trade_type = "exchange"
                if not trade_type:
                    continue

                trade_date = parse_date(cell("tx_date"))
                if not trade_date:
                    continue

                filing_date = parse_date(cell("filing_date"))
                value_low, value_high, value_estimate = parse_amount(cell("amount"))
                owner = normalize_owner(cell("owner")) or "Self"
                company = extract_company(asset, ticker)

                transactions.append({
                    "ticker": ticker,
                    "company": company,
                    "asset_type": "stock",
                    "trade_type": trade_type,
                    "trade_date": trade_date,
                    "filing_date": filing_date,
                    "value_low": value_low,
                    "value_high": value_high,
                    "value_estimate": value_estimate,
                    "owner": owner,
                })

    # Strategy B: regex fallback if table extraction found nothing
    if not transactions:
        transactions = _parse_house_ptr_regex(pdf)

    pdf.close()
    return transactions


def _parse_house_ptr_regex(pdf) -> list[dict]:
    """Fallback regex parser for House PTR PDFs."""
    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    transactions = []
    lines = full_text.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i].strip()

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
                if i + 1 < len(lines):
                    amount_part += " " + lines[i + 1].strip()
                    i += 1

            # Parse owner from asset part
            owner = "Self"
            owner_match = re.match(r"^(SP|JT|DC)\s+", asset_part)
            if owner_match:
                owner_code = owner_match.group(1)
                asset_part = asset_part[len(owner_code):].strip()
                owner = OWNER_MAP.get(owner_code.lower(), owner_code)

            ticker = extract_ticker(asset_part)
            trade_date = parse_date(trade_date_raw)
            filing_date = parse_date(filing_date_raw)
            value_low, value_high, value_estimate = parse_amount(amount_part)
            company = extract_company(asset_part, ticker)

            trade_type = TRADE_TYPE_MAP.get(tx_type)
            if trade_type and trade_date and ticker:
                transactions.append({
                    "ticker": ticker,
                    "company": company,
                    "asset_type": "stock",
                    "trade_type": trade_type,
                    "trade_date": trade_date,
                    "filing_date": filing_date,
                    "value_low": value_low,
                    "value_high": value_high,
                    "value_estimate": value_estimate,
                    "owner": owner,
                })

        i += 1

    return transactions


# ---------------------------------------------------------------------------
# Senate eFD HTML PTR parsing
# ---------------------------------------------------------------------------


def parse_senate_ptr_html(html: str) -> list[dict]:
    """Parse a Senate eFD PTR HTML page and extract transactions."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    transactions = []

    # Find the transaction table
    table = soup.find("table", class_="table")
    if not table:
        # Try finding any table with transaction-like headers
        for t in soup.find_all("table"):
            headers_text = " ".join(th.get_text() for th in t.find_all("th"))
            if "transaction" in headers_text.lower() or "asset" in headers_text.lower():
                table = t
                break

    if not table:
        return transactions

    # Parse headers
    header_row = table.find("thead")
    if header_row:
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
    else:
        first_row = table.find("tr")
        headers = [td.get_text(strip=True).lower() for td in first_row.find_all(["th", "td"])]

    # Map columns
    col_map: dict[str, int] = {}
    for idx, h in enumerate(headers):
        if "owner" in h:
            col_map["owner"] = idx
        elif "asset" in h and "type" not in h:
            col_map["asset"] = idx
        elif "asset" in h and "type" in h:
            col_map["asset_type"] = idx
        elif "transaction type" in h:
            col_map["tx_type"] = idx
        elif "type" in h and "transaction" not in h and "asset" not in h:
            col_map["tx_type"] = idx
        elif "transaction" in h and "date" in h:
            col_map["tx_date"] = idx
        elif "date" in h and "tx_date" not in col_map:
            col_map["tx_date"] = idx
        elif "amount" in h:
            col_map["amount"] = idx
        elif "comment" in h:
            col_map["comment"] = idx

    tbody = table.find("tbody") or table
    rows = tbody.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        def cell(key: str) -> str:
            idx = col_map.get(key)
            if idx is not None and idx < len(cells):
                return cells[idx].get_text(strip=True)
            return ""

        asset_text = cell("asset")
        if not asset_text:
            continue

        ticker = extract_ticker(asset_text)
        if not ticker:
            # Senate sometimes puts ticker in a separate span
            spans = cells[col_map.get("asset", 0)].find_all("span") if "asset" in col_map else []
            for span in spans:
                text = span.get_text(strip=True)
                if re.match(r"^[A-Z]{1,5}$", text):
                    ticker = text
                    break
        if not ticker:
            continue

        # Parse trade type
        tx_type_text = cell("tx_type").lower()
        if "purchase" in tx_type_text:
            trade_type = "buy"
        elif "sale" in tx_type_text:
            trade_type = "sell"
        elif "exchange" in tx_type_text:
            trade_type = "exchange"
        else:
            continue

        trade_date = parse_date(cell("tx_date"))
        if not trade_date:
            continue

        value_low, value_high, value_estimate = parse_amount(cell("amount"))
        owner = normalize_owner(cell("owner")) or "Self"
        company = extract_company(asset_text, ticker)

        transactions.append({
            "ticker": ticker,
            "company": company,
            "asset_type": "stock",
            "trade_type": trade_type,
            "trade_date": trade_date,
            "filing_date": None,  # Set by the scraper from the report metadata
            "value_low": value_low,
            "value_high": value_high,
            "value_estimate": value_estimate,
            "owner": owner,
        })

    return transactions
