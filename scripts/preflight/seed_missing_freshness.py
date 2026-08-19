#!/usr/bin/env python3
"""Seed bootstrap rows in signal_freshness for any contracted column that has zero rows.

Closes the deploy-ordering gap that produced the 2026-05-18 outage: a new
gating contract (`assert_freshness_system_healthy`) can land in the same
release as its writer, and if the writer's plist runs less often than the
strategy scan (e.g. compute_signals runs Mon-Fri 17:45 PT but cw_runner
scans at 06:25 PT), there's a window where the contract is enforced but
the writer hasn't run yet — fail-closed, all strategies produce 0
candidates.

This script is the antidote: at deploy time, seed at least one
signal_freshness row per contracted column with `n_rows_affected=0` and
`populated_by="bootstrap"`. The next real writer run overwrites these with
real values; until then the gating check passes.

Usage:
    # Dry run — list columns needing seed:
    python3 scripts/preflight/seed_missing_freshness.py

    # Apply seeds:
    python3 scripts/preflight/seed_missing_freshness.py --apply

Exit codes:
    0 — nothing to seed (clean) or applied successfully
    1 — seeds needed and not applied (dry run with non-empty diff)
    2 — error
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

import yaml

from config.database import get_connection

CONTRACTS_PATH = REPO / "config/freshness_contracts.yaml"


def _split_table_column(key: str) -> tuple[str, str]:
    """`schema.table.column` → (table, column) (drops schema for query)."""
    parts = key.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        # schema.table.column — store as (table, column); signal_freshness has source col
        return parts[1], parts[2]
    raise ValueError(f"unexpected contract key: {key}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true",
                   help="actually INSERT seed rows (default: dry run)")
    args = p.parse_args()

    contracts = yaml.safe_load(CONTRACTS_PATH.read_text())
    conn = get_connection()
    missing: list[tuple[str, str]] = []
    for key, spec in contracts.items():
        if not isinstance(spec, dict):
            continue
        table, column = _split_table_column(key)
        cur = conn.execute(
            "SELECT COUNT(*) AS n FROM signal_freshness WHERE table_name=? AND column_name=?",
            (table, column),
        )
        n = cur.fetchone()["n"]
        if n == 0:
            missing.append((table, column))

    if not missing:
        print("OK — every contracted column has signal_freshness rows; nothing to seed.")
        conn.close()
        return 0

    print(f"Found {len(missing)} contracted column(s) with NO signal_freshness rows:")
    for t, c in missing:
        print(f"  · {t}.{c}")

    if not args.apply:
        print("\nDry run — re-run with --apply to insert bootstrap rows.")
        conn.close()
        return 1

    # Apply
    for t, c in missing:
        run_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO signal_freshness
                  (source, table_name, column_name, last_computed_at, n_rows_affected,
                   run_id, populated_by)
               VALUES
                  ('public', ?, ?, NOW(), 0, ?, 'bootstrap')""",
            (t, c, run_id),
        )
    conn.commit()
    conn.close()
    print(f"\nApplied: seeded {len(missing)} bootstrap rows in signal_freshness.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
