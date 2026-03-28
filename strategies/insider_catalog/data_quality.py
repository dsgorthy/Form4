#!/usr/bin/env python3
"""
Automated data quality report for the insider catalog.

Checks:
  - Entity duplicate trades (linked vs unlinked)
  - Bad dates (out of range)
  - Missing CIK values
  - Congress trade duplicates
  - Missing politician party affiliations
  - Coverage statistics

Outputs: reports/data_quality.json

Usage:
    python3 strategies/insider_catalog/data_quality.py
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"
REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "reports"


def run_quality_checks(conn: sqlite3.Connection) -> dict:
    """Run all quality checks and return results dict."""
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "db_path": str(DB_PATH),
    }

    # ── Basic counts ──
    logger.info("Gathering basic counts...")
    report["counts"] = {
        "insiders": conn.execute("SELECT COUNT(*) FROM insiders").fetchone()[0],
        "trades": conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0],
        "buy_trades": conn.execute("SELECT COUNT(*) FROM trades WHERE trade_type = 'buy'").fetchone()[0],
        "sell_trades": conn.execute("SELECT COUNT(*) FROM trades WHERE trade_type = 'sell'").fetchone()[0],
        "trade_returns": conn.execute("SELECT COUNT(*) FROM trade_returns").fetchone()[0],
        "scored_insiders": conn.execute("SELECT COUNT(*) FROM insider_track_records WHERE score IS NOT NULL").fetchone()[0],
    }

    # Congress counts (may not exist)
    try:
        report["counts"]["politicians"] = conn.execute("SELECT COUNT(*) FROM politicians").fetchone()[0]
        report["counts"]["congress_trades"] = conn.execute("SELECT COUNT(*) FROM congress_trades").fetchone()[0]
    except sqlite3.OperationalError:
        report["counts"]["politicians"] = 0
        report["counts"]["congress_trades"] = 0

    # ── Entity insiders ──
    logger.info("Checking entity insiders...")
    try:
        entity_count = conn.execute("SELECT COUNT(*) FROM insiders WHERE is_entity = 1").fetchone()[0]
        non_entity = conn.execute("SELECT COUNT(*) FROM insiders WHERE is_entity = 0").fetchone()[0]
    except sqlite3.OperationalError:
        entity_count = 0
        non_entity = report["counts"]["insiders"]

    report["entities"] = {
        "entity_insiders": entity_count,
        "individual_insiders": non_entity,
    }

    # Entity groups
    try:
        group_count = conn.execute("SELECT COUNT(*) FROM insider_groups").fetchone()[0]
        trades_with_effective = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE effective_insider_id IS NOT NULL AND effective_insider_id != insider_id"
        ).fetchone()[0]
        report["entities"]["groups"] = group_count
        report["entities"]["trades_with_effective_id"] = trades_with_effective

        # Groups by method
        methods = conn.execute(
            "SELECT method, COUNT(*) as n FROM insider_groups GROUP BY method"
        ).fetchall()
        report["entities"]["groups_by_method"] = {m[0]: m[1] for m in methods}
    except sqlite3.OperationalError:
        report["entities"]["groups"] = 0
        report["entities"]["trades_with_effective_id"] = 0
        report["entities"]["groups_by_method"] = {}

    # ── Cross-insider duplicates (entity/individual on same trade) ──
    logger.info("Checking cross-insider duplicates...")
    try:
        # Count trades that share (ticker, trade_date, trade_type, value) with a different insider
        cross_dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT t1.trade_id
                FROM trades t1
                JOIN trades t2 ON t1.ticker = t2.ticker
                    AND t1.trade_date = t2.trade_date
                    AND t1.trade_type = t2.trade_type
                    AND t1.value = t2.value
                    AND t1.insider_id != t2.insider_id
                    AND t1.trade_id < t2.trade_id
            )
        """).fetchone()[0]
        report["entities"]["cross_insider_duplicate_pairs"] = cross_dupes
    except Exception:
        report["entities"]["cross_insider_duplicate_pairs"] = -1

    # ── Bad dates ──
    logger.info("Checking bad dates...")
    bad_dates = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE trade_date < '2000-01-01' OR trade_date > '2030-12-31'
    """).fetchone()[0]
    report["bad_dates"] = {
        "out_of_range_trades": bad_dates,
    }

    if bad_dates > 0:
        examples = conn.execute("""
            SELECT trade_id, ticker, trade_date, filing_date
            FROM trades
            WHERE trade_date < '2000-01-01' OR trade_date > '2030-12-31'
            LIMIT 10
        """).fetchall()
        report["bad_dates"]["examples"] = [
            {"trade_id": r[0], "ticker": r[1], "trade_date": r[2], "filing_date": r[3]}
            for r in examples
        ]

    # ── Missing CIK ──
    logger.info("Checking missing CIK...")
    total_insiders = report["counts"]["insiders"]
    missing_cik = conn.execute("SELECT COUNT(*) FROM insiders WHERE cik IS NULL OR cik = ''").fetchone()[0]
    has_cik = total_insiders - missing_cik
    report["cik"] = {
        "missing": missing_cik,
        "present": has_cik,
        "coverage_pct": round(has_cik / max(total_insiders, 1) * 100, 1),
    }

    # ── Congress duplicates ──
    logger.info("Checking congress duplicates...")
    try:
        congress_dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT politician_id, ticker, trade_type, trade_date, COALESCE(value_low, -1) as vl,
                       COUNT(*) as n
                FROM congress_trades
                GROUP BY politician_id, ticker, trade_type, trade_date, vl
                HAVING n > 1
            )
        """).fetchone()[0]

        # Top offenders
        top_dupes = conn.execute("""
            SELECT p.name, ct.ticker, ct.trade_date, COUNT(*) as n
            FROM congress_trades ct
            JOIN politicians p ON ct.politician_id = p.politician_id
            GROUP BY ct.politician_id, ct.ticker, ct.trade_type, ct.trade_date, COALESCE(ct.value_low, -1)
            HAVING n > 3
            ORDER BY n DESC
            LIMIT 5
        """).fetchall()

        report["congress_duplicates"] = {
            "duplicate_groups": congress_dupes,
            "top_offenders": [
                {"politician": r[0], "ticker": r[1], "date": r[2], "count": r[3]}
                for r in top_dupes
            ],
        }
    except sqlite3.OperationalError:
        report["congress_duplicates"] = {"duplicate_groups": 0, "top_offenders": []}

    # ── Missing party ──
    logger.info("Checking missing party...")
    try:
        missing_party = conn.execute(
            "SELECT COUNT(*) FROM politicians WHERE party IS NULL"
        ).fetchone()[0]
        missing_by_chamber = conn.execute("""
            SELECT chamber, COUNT(*) as n
            FROM politicians WHERE party IS NULL
            GROUP BY chamber
        """).fetchall()
        report["missing_party"] = {
            "total": missing_party,
            "by_chamber": {r[0]: r[1] for r in missing_by_chamber},
        }
    except sqlite3.OperationalError:
        report["missing_party"] = {"total": 0, "by_chamber": {}}

    # ── Coverage stats ──
    logger.info("Gathering coverage stats...")
    date_range = conn.execute(
        "SELECT MIN(trade_date), MAX(trade_date) FROM trades"
    ).fetchone()

    sources = conn.execute(
        "SELECT source, COUNT(*) FROM trades GROUP BY source"
    ).fetchall()

    tier_dist = conn.execute("""
        SELECT score_tier, COUNT(*)
        FROM insider_track_records
        WHERE score_tier IS NOT NULL
        GROUP BY score_tier
    """).fetchall()

    report["coverage"] = {
        "trade_date_range": {"min": date_range[0], "max": date_range[1]},
        "sources": {r[0]: r[1] for r in sources},
        "tier_distribution": {f"tier_{r[0]}": r[1] for r in tier_dist},
    }

    return report


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    report = run_quality_checks(conn)
    conn.close()

    # Write report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "data_quality.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info("Report written to %s", output_path)

    # Print summary
    print(f"\n{'='*60}")
    print("DATA QUALITY REPORT")
    print(f"{'='*60}")
    print(f"Insiders:          {report['counts']['insiders']:,}")
    print(f"Trades:            {report['counts']['trades']:,}")
    print(f"  Entity insiders: {report['entities']['entity_insiders']:,}")
    print(f"  Entity groups:   {report['entities']['groups']}")
    print(f"  Deduped trades:  {report['entities']['trades_with_effective_id']:,}")
    print(f"Bad dates:         {report['bad_dates']['out_of_range_trades']}")
    print(f"CIK coverage:      {report['cik']['coverage_pct']}%")
    print(f"Congress dupes:    {report['congress_duplicates']['duplicate_groups']}")
    print(f"Missing party:     {report['missing_party']['total']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
