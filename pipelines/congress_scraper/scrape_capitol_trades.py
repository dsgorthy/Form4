"""Capitol Trades scraper — primary source for congress stock trading data.

Scrapes capitoltrades.com HTML tables for both Senate and House trades.
No API key or signup required. Data updates daily, ~1 day after filing.

Usage:
    # As module
    from pipelines.congress_scraper.scrape_capitol_trades import scrape_capitol_trades
    stats = scrape_capitol_trades(state)

    # Standalone test
    python3 pipelines/congress_scraper/scrape_capitol_trades.py [--pages 5] [--backfill 50]
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
SCHEMA_PATH = ROOT / "strategies" / "insider_catalog" / "congress_schema.sql"

logger = logging.getLogger("congress_scraper.capitol_trades")

BASE_URL = "https://www.capitoltrades.com/trades"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# Size labels to STOCK Act value bands
SIZE_MAP = {
    "1K–15K":    (1001, 15000),
    "15K–50K":   (15001, 50000),
    "50K–100K":  (50001, 100000),
    "100K–250K": (100001, 250000),
    "250K–500K": (250001, 500000),
    "500K–1M":   (500001, 1000000),
    "1M–5M":     (1000001, 5000000),
    "5M–25M":    (5000001, 25000000),
    "25M–50M":   (25000001, 50000000),
    "50M+":      (50000001, 100000000),
}

OWNER_MAP = {
    "self": "Self",
    "spouse": "Spouse",
    "joint": "Joint",
    "child": "Child",
    "dependent": "Child",
    "undisclosed": None,
    "n/a": None,
}


def parse_ct_date(text: str) -> Optional[str]:
    """Parse Capitol Trades date format like '13 Mar2026' or '2 Feb2026' to YYYY-MM-DD."""
    text = text.strip()
    # Insert space before year if missing: '13 Mar2026' -> '13 Mar 2026'
    text = re.sub(r"([A-Za-z])(\d{4})", r"\1 \2", text)
    # Capitol Trades uses "Sept" instead of standard "Sep"
    text = text.replace("Sept ", "Sep ")
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_size(text: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Parse size like '1K–15K' or '50K–100K' into (low, high, estimate)."""
    text = text.strip()
    if text in SIZE_MAP:
        lo, hi = SIZE_MAP[text]
        return lo, hi, (lo + hi) // 2
    # Try alternate dash chars
    normalized = text.replace("—", "–").replace("-", "–")
    if normalized in SIZE_MAP:
        lo, hi = SIZE_MAP[normalized]
        return lo, hi, (lo + hi) // 2
    return None, None, None


def normalize_owner(text: str) -> Optional[str]:
    """Normalize owner field."""
    if not text:
        return None
    return OWNER_MAP.get(text.strip().lower(), text.strip())


def scrape_page(page: int) -> list[dict]:
    """Scrape a single page of trades from Capitol Trades.

    Returns list of parsed trade dicts.
    """
    resp = requests.get(
        BASE_URL,
        params={"page": str(page)},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    trades = []
    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 9:
            continue

        try:
            # Cell 0: Politician — name, party, chamber, state
            pol_link = cells[0].find("a")
            name = pol_link.get_text(strip=True) if pol_link else ""
            pol_id = pol_link["href"].split("/")[-1] if pol_link and pol_link.get("href") else None

            party_span = cells[0].find("span", class_=re.compile("party--"))
            party = None
            if party_span:
                cls = " ".join(party_span.get("class", []))
                if "republican" in cls:
                    party = "R"
                elif "democrat" in cls:
                    party = "D"
                elif "independent" in cls:
                    party = "I"

            chamber_span = cells[0].find("span", class_=re.compile("chamber"))
            chamber = chamber_span.get_text(strip=True) if chamber_span else None

            state_span = cells[0].find("span", class_=re.compile("us-state"))
            state = state_span.get_text(strip=True).upper() if state_span else None

            # Cell 1: Issuer — company name, ticker
            issuer_link = cells[1].find("a")
            company = issuer_link.get_text(strip=True) if issuer_link else None
            ticker_span = cells[1].find("span", class_=re.compile("ticker"))
            ticker_raw = ticker_span.get_text(strip=True) if ticker_span else ""
            # Remove exchange suffix like ":US"
            ticker = ticker_raw.split(":")[0].strip() if ticker_raw else None

            if not ticker or not re.match(r"^[A-Z]{1,5}$", ticker):
                continue

            # Cell 2: Published date
            published = parse_ct_date(cells[2].get_text(strip=True))

            # Cell 3: Traded date
            traded = parse_ct_date(cells[3].get_text(strip=True))
            if not traded:
                continue

            # Cell 5: Owner
            owner = normalize_owner(cells[5].get_text(strip=True))

            # Cell 6: Type (buy/sell)
            trade_type = cells[6].get_text(strip=True).lower()
            if trade_type not in ("buy", "sell", "exchange"):
                # Try partial match
                if "buy" in trade_type or "purchase" in trade_type:
                    trade_type = "buy"
                elif "sell" in trade_type or "sale" in trade_type:
                    trade_type = "sell"
                elif "exchange" in trade_type:
                    trade_type = "exchange"
                else:
                    continue

            # Cell 7: Size
            size_span = cells[7].find("span", class_="q-field")
            size_text = size_span.get_text(strip=True) if size_span else cells[7].get_text(strip=True)
            value_low, value_high, value_estimate = parse_size(size_text)

            # Cell 9: Trade detail link (for report_url)
            detail_link = cells[9].find("a") if len(cells) > 9 else None
            report_url = None
            if detail_link and detail_link.get("href"):
                report_url = "https://www.capitoltrades.com" + detail_link["href"]

            trades.append({
                "name": name,
                "party": party,
                "chamber": chamber,
                "state": state,
                "pol_id": pol_id,
                "ticker": ticker,
                "company": company,
                "trade_type": trade_type,
                "trade_date": traded,
                "filing_date": published,
                "value_low": value_low,
                "value_high": value_high,
                "value_estimate": value_estimate,
                "owner": owner or "Self",
                "report_url": report_url,
            })

        except Exception as e:
            logger.warning(f"Error parsing row on page {page}: {e}")
            continue

    return trades


def normalize_name(name: str) -> str:
    """Normalize politician name for DB matching."""
    name = re.sub(r"\b(Hon\.?|Senator|Sen\.?|Representative|Rep\.?|Jr\.?|Sr\.?|III|II|IV)\b", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", name.strip().lower())


def get_or_create_politician(
    conn: sqlite3.Connection,
    name: str,
    chamber: Optional[str],
    state: Optional[str],
    party: Optional[str],
    cache: dict[str, int],
) -> int:
    """Get or create a politician record."""
    norm = normalize_name(name)
    chamber_db = chamber if chamber in ("House", "Senate") else "House"

    if norm in cache:
        return cache[norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = ?",
        (norm, chamber_db),
    ).fetchone()

    if row:
        pid = row[0]
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
           VALUES (?, ?, ?, ?, ?)""",
        (name.strip(), norm, chamber_db, state, party),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    cache[norm] = pid
    return pid


def scrape_capitol_trades(
    state: dict,
    db_path: Path = DB_PATH,
    max_pages: int = 10,
    start_page: int = 1,
    consecutive_empty_limit: int = 10,
) -> dict:
    """Run one scrape cycle. Fetches pages until all trades are older than the watermark.

    Args:
        state: Mutable state dict with 'ct_last_published' watermark (YYYY-MM-DD).
        db_path: Path to insiders.db.
        max_pages: Safety cap on pages per cycle.
        start_page: First page to scrape (for resuming backfills).
        consecutive_empty_limit: Stop after this many consecutive pages with no stock trades.

    Returns:
        Stats dict.
    """
    stats = {"pages": 0, "trades_found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    watermark = state.get("ct_last_published", "")
    new_watermark = watermark

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    if SCHEMA_PATH.exists():
        conn.executescript(SCHEMA_PATH.read_text())

    politician_cache: dict[str, int] = {}

    consecutive_empty = 0
    end_page = start_page + max_pages

    for page in range(start_page, end_page):
        try:
            trades = scrape_page(page)
        except Exception as e:
            logger.error(f"Failed to scrape page {page}: {e}")
            stats["errors"] += 1
            break

        stats["pages"] += 1
        stats["trades_found"] += len(trades)

        if not trades:
            consecutive_empty += 1
            # Pages with all N/A tickers return empty — skip but don't stop
            if consecutive_empty >= consecutive_empty_limit:
                logger.info(f"{consecutive_empty_limit} consecutive empty pages at page {page}, stopping")
                break
            time.sleep(0.5)
            continue
        consecutive_empty = 0

        all_older = True
        for trade in trades:
            filing_date = trade.get("filing_date", "")

            # Track newest published date for watermark
            if filing_date and filing_date > new_watermark:
                new_watermark = filing_date

            # Stop pagination if all trades on this page are older than watermark
            if filing_date and watermark and filing_date > watermark:
                all_older = False

            # Skip N/A tickers
            if trade["ticker"] in ("N/A", "NA", "--"):
                stats["skipped"] += 1
                continue

            politician_id = get_or_create_politician(
                conn,
                trade["name"],
                trade["chamber"],
                trade["state"],
                trade["party"],
                politician_cache,
            )

            # Calculate filing delay
            filing_delay = None
            if trade.get("filing_date") and trade.get("trade_date"):
                try:
                    td = datetime.strptime(trade["trade_date"], "%Y-%m-%d")
                    fd = datetime.strptime(trade["filing_date"], "%Y-%m-%d")
                    filing_delay = (fd - td).days
                except ValueError:
                    pass

            try:
                conn.execute(
                    """INSERT OR IGNORE INTO congress_trades
                       (politician_id, ticker, company, asset_type, trade_type,
                        trade_date, value_low, value_high, value_estimate,
                        filing_date, filing_delay_days, owner, report_url, source)
                       VALUES (?, ?, ?, 'stock', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'capitol_trades')""",
                    (
                        politician_id,
                        trade["ticker"],
                        trade["company"],
                        trade["trade_type"],
                        trade["trade_date"],
                        trade["value_low"],
                        trade["value_high"],
                        trade["value_estimate"],
                        trade["filing_date"],
                        filing_delay,
                        trade["owner"],
                        trade["report_url"],
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    stats["inserted"] += 1
                else:
                    stats["skipped"] += 1
            except sqlite3.Error as e:
                logger.warning(f"DB insert error: {e}")
                stats["errors"] += 1

        conn.commit()

        # If all trades on this page predate our watermark, we're caught up
        if all_older and watermark:
            logger.info(f"Page {page}: all trades at or before watermark {watermark}, stopping")
            break

        time.sleep(0.5)  # polite delay

    conn.close()

    # Update watermark
    if new_watermark:
        state["ct_last_published"] = new_watermark

    logger.info(
        f"Capitol Trades scrape: {stats['pages']} pages, {stats['trades_found']} found, "
        f"{stats['inserted']} inserted, {stats['skipped']} skipped, {stats['errors']} errors"
    )
    return stats


def main():
    """Standalone test / backfill."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Capitol Trades scraper")
    parser.add_argument("--pages", type=int, default=5, help="Max pages to scrape")
    parser.add_argument("--backfill", type=int, default=0, help="Backfill N pages from the beginning (ignores watermark)")
    parser.add_argument("--start-page", type=int, default=1, help="Start page for backfill (resume from where you left off)")
    args = parser.parse_args()

    state: dict = {}

    if args.backfill > 0:
        stats = scrape_capitol_trades(
            state,
            max_pages=args.backfill,
            start_page=args.start_page,
            consecutive_empty_limit=50,  # backfill needs high tolerance
        )
    else:
        stats = scrape_capitol_trades(state, max_pages=args.pages)

    print(f"\nDone: {stats}")


if __name__ == "__main__":
    main()
