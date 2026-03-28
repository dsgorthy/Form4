"""
Download SEC EDGAR Insider Transactions bulk data (Form 3/4/5).

Uses the SEC's pre-parsed quarterly ZIP files — far faster than downloading
individual Form 4 XML files.  One ~15 MB ZIP per quarter contains all filings
pre-split into relational TSVs:

    SUBMISSION.tsv       — accession, filing_date, ticker, issuer
    NONDERIV_TRANS.tsv   — accession, trans_code, date, shares, price
    REPORTINGOWNER.tsv   — accession, name, title

We join the three tables on ACCESSION_NUMBER, filter by transaction type and
min value, and write a CSV matching the OpenInsider format expected by
build_event_calendar.py.

Supports:
    --trade-type buy    → P-code purchases (default, original behavior)
    --trade-type sell   → S-code sales (for short/put signal research)
    --trade-type both   → both buy and sell transactions

URL pattern:
    https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/
    {year}q{quarter}_form345.zip

Coverage: 2004 Q1 – present (quarterly, ~1-2 week lag).

Usage:
    # Download purchases (default)
    python pipelines/insider_study/download_sec_bulk.py

    # Download sales for short/put analysis
    python pipelines/insider_study/download_sec_bulk.py \\
        --trade-type sell \\
        --output pipelines/insider_study/data/edgar_bulk_form4_sells.csv

    # Download both
    python pipelines/insider_study/download_sec_bulk.py \\
        --trade-type both \\
        --output pipelines/insider_study/data/edgar_bulk_form4_all.csv

Then feed into the existing event calendar builder:
    python pipelines/insider_study/build_event_calendar.py \\
        --input pipelines/insider_study/data/edgar_bulk_form4.csv \\
        --format openinsider \\
        --output pipelines/insider_study/data/events_bulk.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import time
import urllib.request
import urllib.error
import zipfile
from pathlib import Path
from typing import Iterator

_TICKER_RE = re.compile(r'^[A-Z0-9\.]{1,10}$')

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

SEC_BASE = (
    "https://www.sec.gov/files/structureddata/data/"
    "insider-transactions-data-sets/{year}q{quarter}_form345.zip"
)
HEADERS = {"User-Agent": "TradeResearch/1.0 derek.gorthy@gmail.com"}

# OpenInsider-compatible output columns (what build_event_calendar.py expects)
OUTPUT_COLS = [
    "Filing Date", "Trade Date", "Ticker", "Company Name",
    "Insider Name", "Title", "Trade Type", "Price", "Qty",
    "Owned", "DeltaOwn", "Value", "1d", "1w", "1m", "6m",
]


def quarter_range(start: str, end: str) -> Iterator[tuple[int, int]]:
    """Yield (year, quarter) from 'YYYY-QN' to 'YYYY-QN' inclusive."""
    sy, sq = int(start[:4]), int(start[-1])
    ey, eq = int(end[:4]), int(end[-1])
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        yield y, q
        q += 1
        if q > 4:
            q = 1
            y += 1


def download_zip(year: int, quarter: int, cache_dir: Path) -> bytes | None:
    """
    Download the quarterly ZIP file and return its raw bytes.
    Caches to disk — subsequent calls read from cache.
    """
    cache_path = cache_dir / f"{year}q{quarter}_form345.zip"
    if cache_path.exists():
        logger.info("Using cached ZIP: %s", cache_path.name)
        return cache_path.read_bytes()

    url = SEC_BASE.format(year=year, quarter=quarter)
    logger.info("Downloading %dQ%d from %s", year, quarter, url)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning("Not found: %dQ%d (may not be published yet)", year, quarter)
        else:
            logger.error("HTTP %d downloading %dQ%d", e.code, year, quarter)
        return None
    except Exception as e:
        logger.error("Error downloading %dQ%d: %s", year, quarter, e)
        return None

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(data)
    logger.info("Saved %dQ%d (%d MB)", year, quarter, len(data) // 1_048_576)
    return data


def parse_quarter(zip_bytes: bytes, min_value: float, trade_type: str = "buy") -> list[dict]:
    """
    Parse one quarterly ZIP.

    trade_type: "buy" (P-code), "sell" (S-code), or "both"
    Returns list of OpenInsider-format dicts for qualifying transactions ≥ min_value.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        logger.error("Bad ZIP file")
        return []

    def read_tsv(name: str) -> dict[str, dict]:
        """Read a TSV from the ZIP keyed on ACCESSION_NUMBER."""
        out = {}
        with zf.open(name) as fb:
            f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                acc = row.get("ACCESSION_NUMBER", "").strip()
                if acc:
                    # REPORTINGOWNER can have multiple owners per accession;
                    # keep a list for that table
                    if name == "REPORTINGOWNER.tsv":
                        out.setdefault(acc, []).append(row)
                    else:
                        out[acc] = row
        return out

    # Load lookup tables (all fit in memory: ~30 MB uncompressed per quarter)
    logger.debug("Loading SUBMISSION.tsv")
    submissions = read_tsv("SUBMISSION.tsv")

    logger.debug("Loading REPORTINGOWNER.tsv")
    owners_by_acc: dict[str, list] = {}
    with zf.open("REPORTINGOWNER.tsv") as fb:
        f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            acc = row.get("ACCESSION_NUMBER", "").strip()
            if acc:
                owners_by_acc.setdefault(acc, []).append(row)

    # Determine which transaction codes to include
    if trade_type == "buy":
        allowed_codes = {"P"}  # Purchase
    elif trade_type == "sell":
        allowed_codes = {"S"}  # Sale
    else:  # "both"
        allowed_codes = {"P", "S"}

    logger.debug("Scanning NONDERIV_TRANS.tsv for %s transactions", trade_type)
    results = []
    with zf.open("NONDERIV_TRANS.tsv") as fb:
        f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            trans_code = row.get("TRANS_CODE", "").strip()
            if trans_code not in allowed_codes:
                continue
            acq_disp = row.get("TRANS_ACQUIRED_DISP_CD", "").strip()
            # P-code should be Acquisition, S-code should be Disposition
            if trans_code == "P" and acq_disp != "A":
                continue
            if trans_code == "S" and acq_disp != "D":
                continue

            acc = row.get("ACCESSION_NUMBER", "").strip()
            sub = submissions.get(acc)
            if not sub:
                continue

            ticker_raw = sub.get("ISSUERTRADINGSYMBOL", "").strip().upper()
            # Clean common malformed forms: (CALX), "OMEX", NASDAQ:DHC, UHAL,UHALB
            ticker = ticker_raw.strip('"\'()').split(",")[0]
            if ":" in ticker:
                ticker = ticker.split(":")[1]
            ticker = ticker.strip()
            if not ticker or not _TICKER_RE.match(ticker):
                continue

            company = sub.get("ISSUERNAME", "").strip()
            filing_date = _reformat_date(sub.get("FILING_DATE", ""))

            # Transaction details
            trade_date = _reformat_date(row.get("TRANS_DATE", "") or filing_date)
            try:
                price = float(row.get("TRANS_PRICEPERSHARE", 0) or 0)
                shares = float(row.get("TRANS_SHARES", 0) or 0)
            except (ValueError, TypeError):
                continue

            if price <= 0 or shares <= 0:
                continue

            value = price * shares
            if value < min_value:
                continue

            # Insider name + title (first owner listed)
            owners = owners_by_acc.get(acc, [])
            insider_name = ""
            title_parts = []
            for owner in owners:
                if not insider_name:
                    insider_name = owner.get("RPTOWNERNAME", "").strip()
                rel = owner.get("RPTOWNER_RELATIONSHIP", "").strip()
                title_raw = owner.get("RPTOWNER_TITLE", "").strip()
                if title_raw:
                    title_parts.append(title_raw)
                elif rel:
                    title_parts.append(rel)

            title = "; ".join(dict.fromkeys(title_parts)) if title_parts else "Unknown"

            is_sale = trans_code == "S"
            results.append({
                "Filing Date": filing_date,
                "Trade Date": trade_date,
                "Ticker": ticker,
                "Company Name": company,
                "Insider Name": insider_name,
                "Title": title,
                "Trade Type": "S - Sale" if is_sale else "P - Purchase",
                "Price": f"${price:.2f}",
                "Qty": f"-{int(shares):,}" if is_sale else f"+{int(shares):,}",
                "Owned": "",
                "DeltaOwn": "",
                "Value": f"-${value:,.0f}" if is_sale else f"+${value:,.0f}",
                "1d": "", "1w": "", "1m": "", "6m": "",
            })

    return results


def parse_quarter_full(zip_bytes: bytes) -> dict:
    """
    Parse one quarterly ZIP extracting ALL fields from ALL TSVs.

    Unlike parse_quarter() which filters to P/S and returns OpenInsider-format
    CSV strings, this returns structured dicts with all EDGAR fields for:
      - nonderiv_trans: ALL transaction codes (P/S/F/M/A/G/V/X)
      - deriv_trans: derivative transactions
      - footnotes: filing footnotes
      - nonderiv_holdings: end-of-period holdings
      - submissions: filing-level metadata (AFF10B5ONE, etc.)
      - owners: reporting owner details (CIK, relationships)

    Returns dict with keys: nonderiv_trans, deriv_trans, footnotes,
                            nonderiv_holdings, submissions, owners
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        logger.error("Bad ZIP file")
        return {
            "nonderiv_trans": [], "deriv_trans": [], "footnotes": [],
            "nonderiv_holdings": [], "submissions": {}, "owners": {},
        }

    def read_tsv_rows(name: str) -> list[dict]:
        """Read all rows from a TSV as list of dicts."""
        try:
            with zf.open(name) as fb:
                f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
                return list(csv.DictReader(f, delimiter="\t"))
        except KeyError:
            logger.warning("TSV not found in ZIP: %s", name)
            return []

    # ── Load submissions keyed by accession ──
    sub_rows = read_tsv_rows("SUBMISSION.tsv")
    submissions = {}
    for row in sub_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        if acc:
            submissions[acc] = row

    # ── Load owners keyed by accession (multiple per filing) ──
    owner_rows = read_tsv_rows("REPORTINGOWNER.tsv")
    owners_by_acc: dict[str, list[dict]] = {}
    for row in owner_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        if acc:
            owners_by_acc.setdefault(acc, []).append(row)

    # ── Non-derivative transactions (ALL codes) ──
    nd_rows = read_tsv_rows("NONDERIV_TRANS.tsv")
    nonderiv_trans = []
    for row in nd_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        sub = submissions.get(acc)
        if not sub:
            continue

        ticker_raw = sub.get("ISSUERTRADINGSYMBOL", "").strip().upper()
        ticker = ticker_raw.strip("\"'()").split(",")[0]
        if ":" in ticker:
            ticker = ticker.split(":")[1]
        ticker = ticker.strip()
        if not ticker or not _TICKER_RE.match(ticker):
            continue

        trans_code = row.get("TRANS_CODE", "").strip()
        if not trans_code:
            continue

        filing_date = _reformat_date(sub.get("FILING_DATE", ""))
        trade_date = _reformat_date(row.get("TRANS_DATE", "") or filing_date)

        try:
            price = float(row.get("TRANS_PRICEPERSHARE", 0) or 0)
            shares = float(row.get("TRANS_SHARES", 0) or 0)
        except (ValueError, TypeError):
            price, shares = 0.0, 0.0

        try:
            shares_owned_after = float(row.get("SHRS_OWND_FOLWNG_TRANS", 0) or 0)
        except (ValueError, TypeError):
            shares_owned_after = 0.0

        try:
            value_owned_after = float(row.get("VALU_OWND_FOLWNG_TRANS", 0) or 0)
        except (ValueError, TypeError):
            value_owned_after = 0.0

        try:
            equity_swap = int(row.get("EQUITY_SWAP_INVOLVED", 0) or 0)
        except (ValueError, TypeError):
            equity_swap = 0

        # Get owner info
        owners = owners_by_acc.get(acc, [])
        insider_name = ""
        rptowner_cik = ""
        title_parts = []
        for owner in owners:
            if not insider_name:
                insider_name = owner.get("RPTOWNERNAME", "").strip()
                rptowner_cik = owner.get("RPTOWNERCIK", "").strip()
            title_raw = owner.get("RPTOWNER_TITLE", "").strip()
            rel = owner.get("RPTOWNER_RELATIONSHIP", "").strip()
            if title_raw:
                title_parts.append(title_raw)
            elif rel:
                title_parts.append(rel)
        title = "; ".join(dict.fromkeys(title_parts)) if title_parts else ""

        # 10b5-1 flag from submission
        aff10b5 = sub.get("AFF10B5ONE", "").strip()
        is_10b5_1 = 1 if aff10b5 in ("1", "true", "True", "TRUE") else 0

        nonderiv_trans.append({
            "accession": acc,
            "ticker": ticker,
            "company": sub.get("ISSUERNAME", "").strip(),
            "filing_date": filing_date,
            "trade_date": trade_date,
            "insider_name": insider_name,
            "title": title,
            "rptowner_cik": rptowner_cik,
            "trans_code": trans_code,
            "trans_acquired_disp": row.get("TRANS_ACQUIRED_DISP_CD", "").strip(),
            "price": price,
            "shares": shares,
            "value": price * abs(shares) if price > 0 and shares > 0 else 0.0,
            "security_title": row.get("SECURITY_TITLE", "").strip(),
            "shares_owned_after": shares_owned_after,
            "value_owned_after": value_owned_after,
            "direct_indirect": row.get("DIRECT_INDIRECT_OWNERSHIP", "").strip(),
            "nature_of_ownership": row.get("NATURE_OF_OWNERSHIP", "").strip(),
            "equity_swap": equity_swap,
            "is_10b5_1": is_10b5_1,
            "deemed_execution_date": _reformat_date(row.get("DEEMED_EXECUTION_DATE", "")),
            "trans_form_type": row.get("TRANS_FORM_TYPE", "").strip(),
        })

    # ── Derivative transactions ──
    dt_rows = read_tsv_rows("DERIV_TRANS.tsv")
    deriv_trans = []
    for row in dt_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        sub = submissions.get(acc)
        if not sub:
            continue

        ticker_raw = sub.get("ISSUERTRADINGSYMBOL", "").strip().upper()
        ticker = ticker_raw.strip("\"'()").split(",")[0]
        if ":" in ticker:
            ticker = ticker.split(":")[1]
        ticker = ticker.strip()
        if not ticker or not _TICKER_RE.match(ticker):
            continue

        trans_code = row.get("TRANS_CODE", "").strip()
        if not trans_code:
            continue

        filing_date = _reformat_date(sub.get("FILING_DATE", ""))
        trade_date = _reformat_date(row.get("TRANS_DATE", "") or filing_date)

        def _safe_float(val):
            try:
                return float(val) if val else 0.0
            except (ValueError, TypeError):
                return 0.0

        owners = owners_by_acc.get(acc, [])
        insider_name = ""
        rptowner_cik = ""
        title_parts = []
        for owner in owners:
            if not insider_name:
                insider_name = owner.get("RPTOWNERNAME", "").strip()
                rptowner_cik = owner.get("RPTOWNERCIK", "").strip()
            title_raw = owner.get("RPTOWNER_TITLE", "").strip()
            rel = owner.get("RPTOWNER_RELATIONSHIP", "").strip()
            if title_raw:
                title_parts.append(title_raw)
            elif rel:
                title_parts.append(rel)
        title = "; ".join(dict.fromkeys(title_parts)) if title_parts else ""

        aff10b5 = sub.get("AFF10B5ONE", "").strip()
        is_10b5_1 = 1 if aff10b5 in ("1", "true", "True", "TRUE") else 0

        deriv_trans.append({
            "accession": acc,
            "ticker": ticker,
            "company": sub.get("ISSUERNAME", "").strip(),
            "filing_date": filing_date,
            "trade_date": trade_date,
            "insider_name": insider_name,
            "title": title,
            "rptowner_cik": rptowner_cik,
            "trans_code": trans_code,
            "trans_acquired_disp": row.get("TRANS_ACQUIRED_DISP_CD", "").strip(),
            "security_title": row.get("SECURITY_TITLE", "").strip(),
            "exercise_price": _safe_float(row.get("CONV_EXERCISE_PRICE")),
            "expiration_date": _reformat_date(row.get("EXPIRATION_DATE", "")),
            "trans_shares": _safe_float(row.get("TRANS_SHARES")),
            "trans_price_per_share": _safe_float(row.get("TRANS_PRICEPERSHARE")),
            "trans_total_value": _safe_float(row.get("TRANS_TOTAL_VALUE")),
            "underlying_title": row.get("UNDLYNG_SEC_TITLE", "").strip(),
            "underlying_shares": _safe_float(row.get("UNDLYNG_SEC_SHARES")),
            "underlying_value": _safe_float(row.get("UNDLYNG_SEC_VALUE")),
            "shares_owned_after": _safe_float(row.get("SHRS_OWND_FOLWNG_TRANS")),
            "value_owned_after": _safe_float(row.get("VALU_OWND_FOLWNG_TRANS")),
            "direct_indirect": row.get("DIRECT_INDIRECT_OWNERSHIP", "").strip(),
            "nature_of_ownership": row.get("NATURE_OF_OWNERSHIP", "").strip(),
            "equity_swap": 1 if row.get("EQUITY_SWAP_INVOLVED", "").strip() in ("1", "true") else 0,
            "is_10b5_1": is_10b5_1,
            "deemed_execution_date": _reformat_date(row.get("DEEMED_EXECUTION_DATE", "")),
            "trans_form_type": row.get("TRANS_FORM_TYPE", "").strip(),
        })

    # ── Footnotes ──
    fn_rows = read_tsv_rows("FOOTNOTES.tsv")
    footnotes = []
    for row in fn_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        fn_id = row.get("FOOTNOTE_ID", "").strip()
        fn_text = row.get("FOOTNOTE_TXT", "").strip()
        if acc and fn_id:
            footnotes.append({
                "accession": acc,
                "footnote_id": fn_id,
                "footnote_text": fn_text,
            })

    # ── Non-derivative holdings ──
    nh_rows = read_tsv_rows("NONDERIV_HOLDING.tsv")
    nonderiv_holdings = []
    for row in nh_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        sub = submissions.get(acc)
        if not sub:
            continue

        ticker_raw = sub.get("ISSUERTRADINGSYMBOL", "").strip().upper()
        ticker = ticker_raw.strip("\"'()").split(",")[0]
        if ":" in ticker:
            ticker = ticker.split(":")[1]
        ticker = ticker.strip()

        try:
            shares = float(row.get("SHRS_OWND_FOLWNG_TRANS", 0) or 0)
        except (ValueError, TypeError):
            shares = 0.0
        try:
            value = float(row.get("VALU_OWND_FOLWNG_TRANS", 0) or 0)
        except (ValueError, TypeError):
            value = 0.0

        # Get owner for this filing
        owners = owners_by_acc.get(acc, [])
        insider_name = owners[0].get("RPTOWNERNAME", "").strip() if owners else ""
        rptowner_cik = owners[0].get("RPTOWNERCIK", "").strip() if owners else ""

        nonderiv_holdings.append({
            "accession": acc,
            "ticker": ticker if ticker and _TICKER_RE.match(ticker) else "",
            "insider_name": insider_name,
            "rptowner_cik": rptowner_cik,
            "security_title": row.get("SECURITY_TITLE", "").strip(),
            "shares_owned": shares,
            "value_owned": value,
            "direct_indirect": row.get("DIRECT_INDIRECT_OWNERSHIP", "").strip(),
            "nature_of_ownership": row.get("NATURE_OF_OWNERSHIP", "").strip(),
            "trans_form_type": row.get("TRANS_FORM_TYPE", "").strip(),
        })

    return {
        "nonderiv_trans": nonderiv_trans,
        "deriv_trans": deriv_trans,
        "footnotes": footnotes,
        "nonderiv_holdings": nonderiv_holdings,
        "submissions": submissions,
        "owners": owners_by_acc,
    }


def _reformat_date(raw: str) -> str:
    """
    Convert SEC date formats to YYYY-MM-DD.
    SEC uses DD-MON-YYYY (e.g. '28-FEB-2024') in bulk data.
    Also handles YYYY-MM-DD and MM/DD/YYYY passthrough.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    # DD-MON-YYYY
    months = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }
    if len(raw) == 11 and raw[2] == "-" and raw[6] == "-":
        day, mon, year = raw.split("-")
        return f"{year}-{months.get(mon.upper(), '00')}-{day.zfill(2)}"
    # YYYY-MM-DD passthrough
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return raw


def main():
    parser = argparse.ArgumentParser(
        description="Download SEC bulk insider transactions (Form 4)."
    )
    parser.add_argument("--start", default="2020-Q1",
                        help="Start quarter YYYY-QN (default: 2020-Q1)")
    parser.add_argument("--end", default="2025-Q4",
                        help="End quarter YYYY-QN (default: 2025-Q4)")
    parser.add_argument("--min-value", type=float, default=50_000,
                        help="Minimum transaction value in USD (default: $50,000)")
    parser.add_argument("--trade-type", choices=["buy", "sell", "both"], default="buy",
                        help="Transaction type: buy (P-code), sell (S-code), or both")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(__file__).parent / "data" / "sec_bulk_cache",
        help="Directory to cache downloaded ZIPs",
    )
    args = parser.parse_args()

    # Default output file based on trade type
    if args.output is None:
        suffix = {"buy": "", "sell": "_sells", "both": "_all"}[args.trade_type]
        args.output = Path(__file__).parent / "data" / f"edgar_bulk_form4{suffix}.csv"

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    quarters = list(quarter_range(args.start, args.end))
    logger.info(
        "Processing %d quarters (%s → %s), min value $%.0f",
        len(quarters), args.start, args.end, args.min_value,
    )

    all_rows: list[dict] = []

    for year, quarter in quarters:
        zip_bytes = download_zip(year, quarter, args.cache_dir)
        if zip_bytes is None:
            continue

        rows = parse_quarter(zip_bytes, args.min_value, args.trade_type)
        logger.info("  %dQ%d: %d qualifying %s transactions", year, quarter, len(rows), args.trade_type)
        all_rows.extend(rows)
        time.sleep(0.5)  # be polite between quarters

    logger.info("Total rows: %d", len(all_rows))

    if not all_rows:
        logger.warning("No rows found. Check date range and min_value.")
        return

    # Deduplicate by (ticker, filing_date, insider_name, value) — same transaction
    # can appear if filing is amended
    seen: set[tuple] = set()
    deduped = []
    for r in all_rows:
        key = (r["Ticker"], r["Filing Date"], r["Insider Name"], r["Value"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    logger.info("After dedup: %d rows", len(deduped))

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLS)
        writer.writeheader()
        writer.writerows(deduped)

    type_label = {"buy": "purchases", "sell": "sales", "both": "transactions"}[args.trade_type]
    print(f"\nDone. {len(deduped):,} {type_label} written to {args.output}")
    print(f"  Date range: {args.start} → {args.end}")
    print(f"  Min value:  ${args.min_value:,.0f}")
    print(f"  Trade type: {args.trade_type}")
    print(f"\nNext step:")
    print(f"  python pipelines/insider_study/build_event_calendar.py \\")
    print(f"    --input {args.output} --format openinsider \\")
    print(f"    --output pipelines/insider_study/data/events_bulk.csv")


if __name__ == "__main__":
    main()
