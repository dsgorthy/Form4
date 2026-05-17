"""Recurring writer for trades.career_grade (V3) and insider_ticker_scores
career_blended_score / career_grade columns.

Mirrors the V3 block in `strategies/insider_catalog/backfill_live.py:1152-1197`
but standalone, so refresh_features_daily.sh can run it as its own step.

For each trade in the --since window without a career_grade:
  1. Compute V3 ITS score (SCORER_V3 — 5-year half-life, soft total_weight floor).
  2. Update insider_ticker_scores.{career_blended_score, career_grade}.
  3. Update trades.career_grade.

Idempotent: only writes when trades.career_grade IS NULL by default.
Pass --rebuild to overwrite within the window.

Usage:
    python3 pipelines/insider_study/compute_career_grades.py --since 2026-04-01
    python3 pipelines/insider_study/compute_career_grades.py --since 2026-04-01 --rebuild
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_LOG_EVERY = 1000


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", required=True, help="Only process trades with filing_date >= this date (YYYY-MM-DD)")
    parser.add_argument("--rebuild", action="store_true",
                        help="Overwrite existing career_grade in the window (default: only fill NULL)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Commit every N trades")
    args = parser.parse_args()

    # Import here so the module loads even when the score lib path is unset
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog"))
    from pit_scoring import (  # noqa: E402
        compute_insider_ticker_score,
        pit_score_to_grade,
        SCORER_V3,
    )

    conn = get_connection()

    where_clauses = [
        "trans_code = 'P'",
        "filing_date >= ?",
    ]
    params = [args.since]
    if not args.rebuild:
        where_clauses.append("career_grade IS NULL")
    where_sql = " AND ".join(where_clauses)

    total = conn.execute(
        f"SELECT COUNT(*) FROM trades WHERE {where_sql}", tuple(params)
    ).fetchone()[0]
    logger.info("Trades to compute career_grade for: %d (since=%s, rebuild=%s)",
                total, args.since, args.rebuild)

    if total == 0:
        logger.info("Nothing to do.")
        return 0

    cursor = conn.execute(
        f"""SELECT trade_id, insider_id, ticker, filing_date
              FROM trades WHERE {where_sql}
          ORDER BY filing_date ASC""",
        tuple(params),
    )
    rows = cursor.fetchall()

    t0 = time.time()
    processed = 0
    matched = 0  # got a non-NULL career_grade

    for trade_id, insider_id, ticker, filing_date in rows:
        try:
            v3 = compute_insider_ticker_score(
                conn, insider_id, ticker, filing_date, scorer=SCORER_V3
            )
        except Exception as e:
            logger.warning("V3 score failed for trade %s (%s/%s @ %s): %s",
                           trade_id, insider_id, ticker, filing_date, e)
            processed += 1
            continue

        career_grade = (
            pit_score_to_grade(v3.blended_score)
            if v3.sufficient_data else None
        )
        career_blended = v3.blended_score if v3.sufficient_data else None

        conn.execute("""
            UPDATE insider_ticker_scores
               SET career_blended_score = ?, career_grade = ?
             WHERE insider_id = ? AND ticker = ? AND as_of_date = ?
        """, (career_blended, career_grade, insider_id, ticker, filing_date))
        conn.execute(
            "UPDATE trades SET career_grade = ? WHERE trade_id = ?",
            (career_grade, trade_id),
        )

        if career_grade is not None:
            matched += 1
        processed += 1

        if processed % args.batch_size == 0:
            conn.commit()
        if processed % BATCH_LOG_EVERY == 0:
            elapsed = time.time() - t0
            logger.info("%d/%d processed (%.0f/s, matched=%d)",
                        processed, total, processed / max(elapsed, 0.01), matched)

    conn.commit()

    elapsed = time.time() - t0
    logger.info("Done in %.1fs: processed=%d, matched=%d (%.1f%%), NULL=%d",
                elapsed, processed, matched,
                100 * matched / max(processed, 1), processed - matched)

    try:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn,
            table="trades",
            column="career_grade",
            n_rows_affected=matched,
            populated_by="pipelines/insider_study/compute_career_grades.py",
        )
        conn.commit()
    except Exception as e:
        logger.warning("freshness write failed: %s", e)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
