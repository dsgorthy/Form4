#!/usr/bin/env python3
"""Load Form 4 history from the SEC's own quarterly datasets.

WHY NOT SCRAPE

The historical record was assembled through EDGAR full-text search, which
hard-caps at 10,000 hits and reports the cap as the total. For 2021Q1 it
answers `total = 10000` where EDGAR published 64,665 filings, and past the cap
it returns HTTP 200 with zero hits — so the loader exited cleanly and logged
success. Measured 2026-08-26, that left us holding 939,453 of 1,933,494 Form 4
filings since 2016: 48.6%, uniformly 45-53% in every single year. Zillow has
none at all for 2020, 2021, 2022, 2023 or 2025.

Re-fetching a million filings one at a time would be ~2M HTTP requests against
a 10/s limit. The SEC already publishes the same data, parsed, as one zip per
quarter:

    https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{Y}q{Q}_form345.zip

~16 MB each, complete back to 2006Q1. Verified against EDGAR's own index:
2021Q1's SUBMISSION.tsv holds exactly 64,665 Form 4 rows, matching
`full-index/2021/QTR1/form.idx` to the row.

The trailing two quarters are NOT published yet (2026Q2/Q3 as of writing);
those stay with the live fetcher, which now discovers from the daily index.

IDEMPOTENCE

`trades` is unique on (insider_id, ticker, trade_date, trade_type, value,
trans_code), not on accession, so re-running is safe and a filing we already
hold simply contributes nothing. That also means this pass TOPS UP filings we
already have with transaction codes the XML parser used to discard.

WHAT IS DELIBERATELY NOT HERE

Derivative transactions. `trades` is the common-stock table; the XML path
promotes a narrow class of derivative P/S rows into it, and reproducing that
selectively from the TSVs would be a second, differently-shaped decision.
Non-derivative transactions are what every grade, signal and strategy reads.

Usage:
    python3 pipelines/insider_study/backfill_from_sec_datasets.py --start 2006q1 --end 2026q1
    python3 pipelines/insider_study/backfill_from_sec_datasets.py --start 2021q1 --end 2021q1 --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import time
import urllib.request
import zipfile
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "strategies" / "insider_catalog"))

from config.database import get_connection  # noqa: E402
from backfill import (  # noqa: E402
    get_title_weight,
    is_csuite,
    normalize_name,
    normalize_ticker,
)
from backfill_live import _classify_is_derivative  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

BASE = ("https://www.sec.gov/files/structureddata/data/"
        "insider-transactions-data-sets/{y}q{q}_form345.zip")
USER_AGENT = "Form4 historical backfill (derek.gorthy@gmail.com)"
CACHE = REPO / "data" / "sec_form345"
EARLIEST = (2006, 1)

# Every code the SEC publishes. The XML parser accepted only
# P S F M A G V X and dropped C J D L I U W O with a bare `continue` —
# 6.29% of non-derivative transactions in 2021Q1, and 2.79% of filings had
# nothing else, so those filings looked empty. Direction for anything that is
# not an outright purchase or sale comes from the acquired/disposed flag.
BUY, SELL = "buy", "sell"

csv.field_size_limit(10_000_000)


# ── SEC dataset plumbing ───────────────────────────────────────────────────

def quarter_path(y: int, q: int) -> Path:
    return CACHE / f"{y}q{q}_form345.zip"


def download_quarter(y: int, q: int) -> Path | None:
    """Fetch one quarterly zip, cached on disk. None if SEC has not published it."""
    dest = quarter_path(y, q)
    if dest.exists() and dest.stat().st_size > 1_000_000:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = BASE.format(y=y, q=q)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            body = resp.read()
    except Exception as exc:
        logger.warning("%dq%d unavailable: %s", y, q, exc)
        return None
    if len(body) < 1_000_000:
        logger.warning("%dq%d returned %d bytes — not published yet", y, q, len(body))
        return None
    dest.write_bytes(body)
    time.sleep(0.15)
    logger.info("%dq%d downloaded (%.1f MB)", y, q, len(body) / 1e6)
    return dest


def _rows(zf: zipfile.ZipFile, name: str):
    """Stream one TSV out of the zip."""
    try:
        raw = zf.read(name)
    except KeyError:
        return
    yield from csv.DictReader(
        io.StringIO(raw.decode("latin-1")), delimiter="\t")


def _d(s: str) -> str | None:
    """'26-MAR-2021' -> '2021-03-26'. The datasets use no other format."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:11], "%d-%b-%Y").date().isoformat()
    except ValueError:
        return None


def _f(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _title(relationship: str, officer_title: str) -> str:
    """Mirror parse_form4_xml exactly, so a row cannot be told apart by title."""
    rel = (relationship or "")
    parts = []
    if "Officer" in rel and (officer_title or "").strip():
        parts.append(officer_title.strip())
    elif "Director" in rel:
        parts.append("Dir")
    if "TenPercent" in rel:
        parts.append("10%")
    return ", ".join(parts) if parts else "Unknown"


# ── insider identity ───────────────────────────────────────────────────────

class InsiderCache:
    """name_normalized -> insider_id, held in memory.

    get_or_create_insider does a SELECT and possibly an INSERT per trade. At
    ~10M transactions that is 10M round trips. The whole table is 129k rows.

    Matching on name_normalized ALONE is deliberate: the live path passes the
    FILER's cik into get_or_create_insider, whose predicate is
    `name_normalized = ? AND (cik = ? OR cik IS NULL OR ? IS NULL)`. Passing a
    different cik for the same person would miss the existing row and mint a
    duplicate insider — across a million filings that would fragment identity
    far more damagingly than a missing cik.
    """

    def __init__(self, conn):
        self.conn = conn
        rows = conn.execute(
            "SELECT name_normalized, insider_id FROM insiders "
            "WHERE name_normalized IS NOT NULL").fetchall()
        self.map = {r[0]: r[1] for r in rows}
        self.created = 0
        logger.info("Insider cache: %d names", len(self.map))

    def resolve(self, name: str, cik: str | None) -> int | None:
        norm = normalize_name(name) or "unknown"
        hit = self.map.get(norm)
        if hit is not None:
            return hit
        cur = self.conn.execute(
            "INSERT INTO insiders (name, name_normalized, cik) VALUES (?, ?, ?) "
            "RETURNING insider_id", (name, norm, cik or None))
        row = cur.fetchone()
        new_id = row[0] if row else None
        if new_id is None:
            row = self.conn.execute(
                "SELECT insider_id FROM insiders WHERE name_normalized = ?",
                (norm,)).fetchone()
            new_id = row[0] if row else None
        if new_id is not None:
            self.map[norm] = new_id
            self.created += 1
        return new_id


# ── the load ───────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO trades
    (insider_id, ticker, company, title, trade_type, trade_date, filing_date,
     price, qty, value, is_csuite, title_weight, source, accession,
     trans_code, trans_acquired_disp, direct_indirect, shares_owned_after,
     value_owned_after, nature_of_ownership, equity_swap, is_10b5_1,
     security_title, deemed_execution_date, trans_form_type, rptowner_cik,
     is_derivative, issuer_cik, period_of_report, date_of_orig_sub,
     document_type, remarks)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sec_form345', ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""


def load_quarter(conn, cache: InsiderCache, y: int, q: int, dry_run: bool) -> dict:
    path = download_quarter(y, q)
    if path is None:
        return {"skipped": True}
    today = date.today().isoformat()

    with zipfile.ZipFile(path) as zf:
        subs = {}
        for r in _rows(zf, "SUBMISSION.tsv"):
            if not (r.get("DOCUMENT_TYPE") or "").startswith("4"):
                continue
            subs[r["ACCESSION_NUMBER"]] = r
        owners = {}
        for r in _rows(zf, "REPORTINGOWNER.tsv"):
            owners.setdefault(r["ACCESSION_NUMBER"], r)   # first owner, as the XML path does
        # 10b5-1 is not a column anywhere in these datasets. The XML parser
        # infers it from remarks and footnote text; do the same or the flag
        # would read 0 on a million filings that carry a plan.
        planned = set()
        for r in _rows(zf, "FOOTNOTES.tsv"):
            if "10b5" in (r.get("FOOTNOTE_TXT") or "").lower():
                planned.add(r["ACCESSION_NUMBER"])
        for acc, r in subs.items():
            if "10b5" in (r.get("REMARKS") or "").lower():
                planned.add(acc)

        stats = {"filings": 0, "rows": 0, "inserted": 0, "no_direction": 0,
                 "no_ticker": 0, "future": 0}
        batch = []
        for r in _rows(zf, "NONDERIV_TRANS.tsv"):
            acc = r["ACCESSION_NUMBER"]
            sub = subs.get(acc)
            if sub is None:
                continue                       # a Form 3/5 transaction
            stats["rows"] += 1

            ticker = normalize_ticker(sub.get("ISSUERTRADINGSYMBOL") or "")
            if not ticker:
                stats["no_ticker"] += 1
                continue

            code = (r.get("TRANS_CODE") or "").strip().upper()
            acq = (r.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper()
            if code == "P":
                ttype = BUY
            elif code == "S":
                ttype = SELL
            elif acq in ("A", "D"):
                ttype = BUY if acq == "A" else SELL
            else:
                stats["no_direction"] += 1     # no code, no direction, no row
                continue

            qty = _f(r.get("TRANS_SHARES"))
            price = _f(r.get("TRANS_PRICEPERSHARE")) or 0.0
            if not qty or qty <= 0:
                continue
            # Same guard the XML path applies: an open-market trade with no
            # price or size is not a trade.
            if code in ("P", "S") and (price <= 0 or qty <= 0):
                continue

            tdate = _d(r.get("TRANS_DATE")) or _d(sub.get("FILING_DATE"))
            fdate = _d(sub.get("FILING_DATE"))
            if not tdate or not fdate:
                continue
            if tdate > today:
                stats["future"] += 1           # issuer year-typo, as P1.12
                continue

            owner = owners.get(acc, {})
            name = (owner.get("RPTOWNERNAME") or "").strip()
            if not name:
                continue
            insider_id = cache.resolve(name, (owner.get("RPTOWNERCIK") or "").strip())
            if insider_id is None:
                continue

            title = _title(owner.get("RPTOWNER_RELATIONSHIP"),
                           owner.get("RPTOWNER_TITLE"))
            sec_title = (r.get("SECURITY_TITLE") or "").strip() or None
            value = price * qty
            batch.append((
                insider_id, ticker, (sub.get("ISSUERNAME") or "").strip(), title,
                ttype, tdate, fdate, price, int(abs(qty)), value,
                1 if is_csuite(title) else 0, get_title_weight(title), acc,
                code, acq or None,
                (r.get("DIRECT_INDIRECT_OWNERSHIP") or "").strip() or None,
                _f(r.get("SHRS_OWND_FOLWNG_TRANS")),
                _f(r.get("VALU_OWND_FOLWNG_TRANS")),
                (r.get("NATURE_OF_OWNERSHIP") or "").strip() or None,
                1 if (r.get("EQUITY_SWAP_INVOLVED") or "").strip() in ("1", "true", "TRUE") else 0,
                1 if acc in planned else 0,
                sec_title,
                _d(r.get("DEEMED_EXECUTION_DATE")),
                (r.get("TRANS_FORM_TYPE") or "").strip() or None,
                (owner.get("RPTOWNERCIK") or "").strip() or None,
                _classify_is_derivative(sec_title, price, ticker, value),
                (sub.get("ISSUERCIK") or "").strip() or None,
                _d(sub.get("PERIOD_OF_REPORT")),
                _d(sub.get("DATE_OF_ORIG_SUB")),
                (sub.get("DOCUMENT_TYPE") or "").strip() or None,
                (sub.get("REMARKS") or "").strip() or None,
            ))
            if len(batch) >= 5000 and not dry_run:
                stats["inserted"] += _flush(conn, batch)
                batch.clear()

        if batch and not dry_run:
            stats["inserted"] += _flush(conn, batch)
        elif dry_run:
            stats["inserted"] = len(batch)
        stats["filings"] = len(subs)
    return stats


def _flush(conn, batch) -> int:
    n = 0
    for row in batch:
        try:
            conn.execute(INSERT_SQL, row)
            n += 1
        except Exception:
            pass          # unique index rejected a row we already hold
    conn.commit()
    return n


def quarters(start: tuple, end: tuple):
    y, q = start
    while (y, q) <= end:
        yield y, q
        q += 1
        if q > 4:
            y, q = y + 1, 1


def _parse_q(s: str) -> tuple:
    m = re.fullmatch(r"(\d{4})[qQ]([1-4])", s.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"expected e.g. 2021q1, got {s!r}")
    return int(m.group(1)), int(m.group(2))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=_parse_q, default=EARLIEST)
    ap.add_argument("--end", type=_parse_q, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    conn.execute("""CREATE TABLE IF NOT EXISTS sec_dataset_progress (
        quarter TEXT PRIMARY KEY, filings INTEGER, rows_seen INTEGER,
        inserted INTEGER, loaded_at TEXT)""")
    conn.commit()
    done = {r[0] for r in conn.execute(
        "SELECT quarter FROM sec_dataset_progress").fetchall()}

    cache = InsiderCache(conn)
    grand = 0
    for y, q in quarters(args.start, args.end):
        key = f"{y}q{q}"
        if key in done and not args.dry_run:
            logger.info("%s already loaded, skipping", key)
            continue
        t0 = time.monotonic()
        s = load_quarter(conn, cache, y, q, args.dry_run)
        if s.get("skipped"):
            continue
        grand += s["inserted"]
        logger.info("%s: %d filings, %d transactions, %d inserted "
                    "(no-ticker %d, no-direction %d, future %d) in %.0fs",
                    key, s["filings"], s["rows"], s["inserted"],
                    s["no_ticker"], s["no_direction"], s["future"],
                    time.monotonic() - t0)
        if not args.dry_run:
            conn.execute(
                """INSERT INTO sec_dataset_progress
                       (quarter, filings, rows_seen, inserted, loaded_at)
                   VALUES (?, ?, ?, ?, datetime('now'))
                   ON CONFLICT (quarter) DO UPDATE SET
                       filings=excluded.filings, rows_seen=excluded.rows_seen,
                       inserted=excluded.inserted, loaded_at=excluded.loaded_at""",
                (key, s["filings"], s["rows"], s["inserted"]))
            conn.commit()
    logger.info("TOTAL inserted: %d rows (%d new insiders)", grand, cache.created)
    return 0


if __name__ == "__main__":
    sys.exit(main())
