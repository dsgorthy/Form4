#!/usr/bin/env python3
"""
Automated backfill of missing trade returns.

1. Finds tickers with P/S trades that have no trade_returns row and no price CSV
2. Downloads price data for those tickers via Alpaca
3. Runs compute_returns.py to fill in the missing rows

Designed to run on a schedule (e.g., daily via launchd).
Skips tickers that previously failed (OTC, delisted, foreign).

Usage:
    python3 pipelines/insider_study/backfill_returns.py
    python3 pipelines/insider_study/backfill_returns.py --max-download 500
    python3 pipelines/insider_study/backfill_returns.py --skip-download  # just run compute_returns
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection

_SCRIPT_DIR = Path(__file__).resolve().parent
_FRAMEWORK_ROOT = _SCRIPT_DIR.parents[1]

if str(_FRAMEWORK_ROOT) not in sys.path:
    sys.path.insert(0, str(_FRAMEWORK_ROOT))

DB_PATH = _FRAMEWORK_ROOT / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"  # daily_prices, option_prices
PRICES_DIR = _SCRIPT_DIR / "data" / "prices"
FAILED_TICKERS_PATH = _SCRIPT_DIR / "data" / "failed_tickers.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_failed_tickers() -> set[str]:
    """Load set of tickers that previously failed download (OTC, delisted, etc.)."""
    if FAILED_TICKERS_PATH.exists():
        try:
            return set(json.loads(FAILED_TICKERS_PATH.read_text()))
        except (json.JSONDecodeError, TypeError):
            pass
    return set()


def save_failed_tickers(failed: set[str]):
    """Persist failed tickers to avoid re-attempting."""
    FAILED_TICKERS_PATH.write_text(json.dumps(sorted(failed)))


def is_valid_ticker(ticker: str) -> bool:
    """Filter out junk tickers: parenthesized, dots, numeric, too long."""
    if not ticker or ticker == "NONE":
        return False
    if re.search(r"[().:]", ticker):
        return False
    if ticker.isdigit():
        return False
    if len(ticker) > 5:
        return False
    return True


def find_missing_tickers(conn) -> list[str]:
    """Find tickers with P/S trades that have no trade_returns row and no price CSV."""
    rows = conn.execute("""
        SELECT DISTINCT t.ticker, COUNT(*) AS n
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code IN ('P', 'S')
          AND t.ticker != 'NONE'
          AND tr.trade_id IS NULL
          AND t.trade_date < date('now', '-14 days')
        GROUP BY t.ticker
        ORDER BY n DESC
    """).fetchall()

    failed = load_failed_tickers()
    missing = []
    for r in rows:
        ticker = r["ticker"]
        if not is_valid_ticker(ticker):
            continue
        if ticker in failed:
            continue
        if (PRICES_DIR / f"{ticker}.csv").exists():
            # Has price file but missing returns — compute_returns will handle it
            continue
        missing.append(ticker)

    return missing


def download_prices(tickers: list[str], max_download: int) -> tuple[int, list[str]]:
    """Download price data for tickers via collect_prices.py. Returns (success_count, new_failures)."""
    if not tickers:
        return 0, []

    to_download = tickers[:max_download]
    logger.info("Downloading prices for %d tickers (capped at %d)", len(tickers), max_download)

    # Write temp events CSV
    tmp_csv = Path("/tmp/backfill_tickers.csv")
    with open(tmp_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker"])
        for t in to_download:
            w.writerow([t])

    today = date.today().isoformat()
    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT_DIR / "collect_prices.py"),
            "--events", str(tmp_csv),
            "--start", "2016-01-01",
            "--end", today,
        ],
        capture_output=True,
        text=True,
        timeout=3600,
    )

    if result.returncode != 0:
        logger.error("collect_prices.py failed: %s", result.stderr[-500:] if result.stderr else "unknown")
        return 0, []

    # Check which ones succeeded
    success = 0
    new_failures = []
    for t in to_download:
        if (PRICES_DIR / f"{t}.csv").exists():
            success += 1
        else:
            new_failures.append(t)

    logger.info("Downloaded: %d/%d, Failed: %d", success, len(to_download), len(new_failures))
    return success, new_failures


def run_compute_returns():
    """Run compute_returns.py to fill missing trade_returns rows."""
    logger.info("Running compute_returns.py...")
    result = subprocess.run(
        [
            sys.executable,
            str(_FRAMEWORK_ROOT / "strategies" / "insider_catalog" / "compute_returns.py"),
            "--trade-type", "both",
        ],
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if result.returncode != 0:
        logger.error("compute_returns.py failed: %s", result.stderr[-500:] if result.stderr else "unknown")
    else:
        # Log the last few lines of output for summary
        lines = result.stdout.strip().split("\n")
        for line in lines[-10:]:
            logger.info("  %s", line)


def _regenerate_last_dates():
    """Rebuild last_dates.json from price CSVs for API N/A tooltips."""
    import json as _json
    last_dates = {}
    for p in PRICES_DIR.glob("*.csv"):
        try:
            with open(p, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 200))
                last_line = f.readlines()[-1].decode("utf-8", errors="ignore")
                date_str = last_line.split(",")[0][:10]
                if len(date_str) == 10:
                    last_dates[p.stem] = date_str
        except Exception:
            continue

    # Write to both locations (pipelines data dir + DB dir for Docker)
    for out in [
        PRICES_DIR / "last_dates.json",
        _FRAMEWORK_ROOT / "strategies" / "insider_catalog" / "last_dates.json",
    ]:
        out.write_text(_json.dumps(last_dates))
    logger.info("Regenerated last_dates.json: %d tickers", len(last_dates))


def main():
    parser = argparse.ArgumentParser(description="Backfill missing trade returns")
    parser.add_argument("--max-download", type=int, default=500,
                        help="Max tickers to download per run (default: 500)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip price download, just run compute_returns")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = get_connection()

    # Step 1: Find tickers needing price data
    missing = find_missing_tickers(conn)
    logger.info("Tickers missing price data: %d", len(missing))

    # Step 2: Download prices
    if not args.skip_download and missing:
        success, new_failures = download_prices(missing, args.max_download)

        # Update failed tickers list
        if new_failures:
            failed = load_failed_tickers()
            failed.update(new_failures)
            save_failed_tickers(failed)
            logger.info("Total known-failed tickers: %d", len(failed))

    # Step 3: Regenerate last_dates.json for API tooltips
    _regenerate_last_dates()

    # Step 4: Compute returns for all trades with price data but missing returns
    run_compute_returns()

    # Step 5: Sync daily_prices table from CSV files
    _sync_daily_prices()

    # Step 6: Update cohen_routine + signal_grade for recent trades
    _update_recent_signals()

    # Summary
    remaining = conn.execute("""
        SELECT COUNT(*) AS cnt FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trans_code IN ('P', 'S') AND tr.trade_id IS NULL
          AND t.trade_date < date('now', '-14 days')
    """).fetchone()
    logger.info("Remaining trades without returns: %d", remaining["cnt"])

    conn.close()


def _sync_daily_prices():
    """Load any new price CSV data into the daily_prices table."""
    import csv as _csv

    conn = get_connection()

    # Get latest date per ticker in DB
    latest_map: dict[str, str] = {}
    for r in conn.execute("SELECT ticker, MAX(date) AS d FROM daily_prices GROUP BY ticker").fetchall():
        latest_map[r[0]] = r[1]

    total = 0
    for f in sorted(PRICES_DIR.glob("*.csv")):
        ticker = f.stem
        cutoff = latest_map.get(ticker, "2000-01-01")
        batch = []
        with open(f, "r") as fh:
            reader = _csv.reader(fh)
            next(reader, None)
            for row in reader:
                if len(row) < 5:
                    continue
                try:
                    d = row[0][:10]
                    if d <= cutoff:
                        continue
                    o = float(row[1]) if row[1] else None
                    h = float(row[2]) if row[2] else None
                    l = float(row[3]) if row[3] else None
                    c = float(row[4])
                    v = int(float(row[5])) if len(row) > 5 and row[5] else None
                    batch.append((ticker, d, o, h, l, c, v))
                except (ValueError, IndexError):
                    continue
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO daily_prices (ticker, date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                batch,
            )
            total += len(batch)

    if total:
        conn.commit()
    conn.close()
    logger.info("Synced %d new price rows to daily_prices", total)


def _update_recent_signals():
    """Run cohen_routine PIT + signal_grade for trades filed in last 7 days."""
    since = (date.today().isoformat()[:10])
    # Only process trades from last 7 days to catch any gaps
    since_7d = str(date.today().replace(day=max(1, date.today().day - 7)))

    logger.info("Updating cohen_routine + signal_grade for trades since %s", since_7d)

    # Cohen PIT
    try:
        result = subprocess.run(
            [sys.executable, str(_SCRIPT_DIR / "compute_cohen_pit.py"), "--since", since_7d],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            logger.info("Cohen PIT updated")
        else:
            logger.warning("Cohen PIT failed: %s", result.stderr[:500])
    except Exception as exc:
        logger.warning("Cohen PIT error: %s", exc)

    # Signal grade — now computed via trade_grade (PIT-safe)
    try:
        conn = get_connection()

        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from api.trade_grade import compute_trade_grade

        rows = conn.execute(
            "SELECT trade_id, trade_type, cohen_routine, shares_owned_after, "
            "qty, title, is_csuite, is_10b5_1, is_routine, ticker, pit_grade, "
            "value, price, is_rare_reversal, dip_1mo, dip_3mo, "
            "week52_proximity, is_largest_ever, pit_cluster_size "
            "FROM trades WHERE filing_date >= ?", (since_7d,)
        ).fetchall()

        updates = []
        for r in rows:
            item = dict(r)
            tg = compute_trade_grade(item)
            grade_letter = tg.get("label", "Average")[0] if tg else None  # E/S/A/W/P → first letter
            updates.append((grade_letter, r["trade_id"]))

        conn.executemany("UPDATE trades SET signal_grade = ? WHERE trade_id = ?", updates)
        conn.commit()
        conn.close()
        logger.info("Updated signal_grade for %d recent trades", len(updates))
    except Exception as exc:
        logger.warning("Signal grade update error: %s", exc)


if __name__ == "__main__":
    main()
