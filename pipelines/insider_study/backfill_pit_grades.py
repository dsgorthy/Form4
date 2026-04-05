"""
Backfill pit_grade and pit_blended_score on trades table from insider_ticker_scores.

For each trade, looks up the most recent PIT score where as_of_date <= filing_date.
Trades without a PIT score get NULL (not a default grade).

Usage:
    python3 pipelines/insider_study/backfill_pit_grades.py
    python3 pipelines/insider_study/backfill_pit_grades.py --since 2025-01-01
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.insider_study.db_lock import db_write_lock
from pipelines.insider_study.conviction_score import pit_score_to_grade

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
BATCH_SIZE = 5000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Only backfill trades with filing_date >= this date")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA journal_mode=WAL")

    # Build PIT score lookup: (insider_id, ticker) -> sorted list of (as_of_date, blended_score)
    print("Loading insider_ticker_scores...", flush=True)
    t0 = time.time()
    rows = db.execute("""
        SELECT insider_id, ticker, as_of_date, blended_score
        FROM insider_ticker_scores
        ORDER BY insider_id, ticker, as_of_date
    """).fetchall()

    pit_data: dict[tuple[int, str], list[tuple[str, float]]] = {}
    for iid, ticker, aod, score in rows:
        key = (iid, ticker)
        if key not in pit_data:
            pit_data[key] = []
        pit_data[key].append((aod, score))

    print(f"  {len(rows):,} PIT scores for {len(pit_data):,} insider+ticker combos ({time.time()-t0:.1f}s)", flush=True)

    # Load trades needing pit_grade
    where = "WHERE pit_grade IS NULL AND pit_blended_score IS NULL"
    if args.since:
        where += f" AND filing_date >= '{args.since}'"

    total = db.execute(f"SELECT COUNT(*) FROM trades {where}").fetchone()[0]
    print(f"Trades to process: {total:,}", flush=True)

    if total == 0:
        print("Nothing to do.")
        db.close()
        return

    cursor = db.execute(f"""
        SELECT trade_id, insider_id, ticker, filing_date
        FROM trades {where}
        ORDER BY filing_date
    """)

    updates = []
    processed = 0
    matched = 0

    for trade_id, insider_id, ticker, filing_date in cursor:
        score = None
        grade = None

        # Binary search for most recent PIT score <= filing_date
        entries = pit_data.get((insider_id, ticker))
        if entries:
            lo, hi = 0, len(entries) - 1
            best = None
            while lo <= hi:
                mid = (lo + hi) // 2
                if entries[mid][0] <= filing_date:
                    best = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            if best is not None:
                score = entries[best][1]
                grade = pit_score_to_grade(score)

        updates.append((grade, score, trade_id))
        if grade is not None:
            matched += 1
        processed += 1

        if len(updates) >= BATCH_SIZE:
            with db_write_lock():
                db.executemany(
                    "UPDATE trades SET pit_grade = ?, pit_blended_score = ? WHERE trade_id = ?",
                    updates,
                )
                db.commit()
            updates = []
            if processed % 50000 == 0:
                print(f"  {processed:,}/{total:,} processed, {matched:,} matched ({100*matched/processed:.1f}%)", flush=True)

    # Final batch
    if updates:
        with db_write_lock():
            db.executemany(
                "UPDATE trades SET pit_grade = ?, pit_blended_score = ? WHERE trade_id = ?",
                updates,
            )
            db.commit()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s", flush=True)
    print(f"  Processed: {processed:,}", flush=True)
    print(f"  Matched PIT score: {matched:,} ({100*matched/processed:.1f}%)", flush=True)
    print(f"  No PIT score (NULL): {processed - matched:,}", flush=True)

    # Verify
    dist = db.execute("SELECT pit_grade, COUNT(*) FROM trades GROUP BY pit_grade ORDER BY pit_grade").fetchall()
    print("\nGrade distribution:", flush=True)
    for grade, count in dist:
        print(f"  {grade or 'NULL':5s}: {count:>10,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
