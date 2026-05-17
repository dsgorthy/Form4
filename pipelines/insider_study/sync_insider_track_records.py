#!/usr/bin/env python3
"""Recurring writer for insider_track_records (P1.6 step 1, 2026-05-17).

Thin CLI wrapper around `pit_scoring.sync_to_track_records`. Wired into
`refresh_features_daily.sh` as step 7 — closes the 53-day-stale gap that
14 API routers + 5 pipelines were silently reading.

Per-trade PIT-sensitive reads (signals, dashboard, filings, clusters,
companies, etc.) are being migrated off this table in P1.6 steps 2-6;
the table remains for entity-level display surfaces (insider profile,
leaderboard, search, sitemap) where cached career aggregates beat
on-the-fly recomputation.

Usage:
    python3 pipelines/insider_study/sync_insider_track_records.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog"))

from config.database import get_connection  # noqa: E402
from pit_scoring import sync_to_track_records  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> int:
    conn = get_connection()
    n = sync_to_track_records(conn)
    conn.close()
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
