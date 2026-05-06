#!/usr/bin/env python3
"""order_audit ↔ strategy_portfolio completeness check.

For every entry / exit recorded in strategy_portfolio since order_audit
went live (2026-05-04), there should be a matching order_audit row tied
by client_order_id. Missing order_audit rows = audit gap; the trade
happened but we have no provenance for it.

Run before flipping live so the audit baseline is clean. Then run
periodically (e.g., weekly Friday) so any future drift surfaces fast.

Usage:
    python3 scripts/audit/order_completeness.py
    python3 scripts/audit/order_completeness.py --since 2026-05-01
    python3 scripts/audit/order_completeness.py --strategy quality_momentum
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from config.database import get_connection

# order_audit went live on 2026-05-04. Trades before that have no
# expected order_audit row.
ORDER_AUDIT_GO_LIVE = "2026-05-04"


def find_orphan_entries(conn, since: str, strategy: str | None) -> list[dict]:
    where_clauses = ["sp.entry_date >= ?", "sp.execution_source != 'backtest'"]
    params = [since]
    if strategy:
        where_clauses.append("sp.strategy = ?")
        params.append(strategy)

    sql = f"""
        SELECT sp.id, sp.strategy, sp.ticker, sp.entry_date, sp.shares,
               sp.entry_price, sp.is_live
          FROM strategy_portfolio sp
          LEFT JOIN order_audit oa
            ON oa.strategy = sp.strategy
           AND oa.ticker   = sp.ticker
           AND oa.side     = 'buy'
           AND oa.decided_at::date = sp.entry_date::date
         WHERE {' AND '.join(where_clauses)}
           AND oa.order_id IS NULL
         ORDER BY sp.entry_date DESC, sp.strategy
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def find_orphan_exits(conn, since: str, strategy: str | None) -> list[dict]:
    where_clauses = ["sp.exit_date >= ?", "sp.status = 'closed'",
                     "sp.execution_source != 'backtest'"]
    params = [since]
    if strategy:
        where_clauses.append("sp.strategy = ?")
        params.append(strategy)

    sql = f"""
        SELECT sp.id, sp.strategy, sp.ticker, sp.exit_date, sp.shares,
               sp.exit_price, sp.exit_reason, sp.is_live
          FROM strategy_portfolio sp
          LEFT JOIN order_audit oa
            ON oa.strategy = sp.strategy
           AND oa.ticker   = sp.ticker
           AND oa.side     = 'sell'
           AND oa.decided_at::date = sp.exit_date::date
         WHERE {' AND '.join(where_clauses)}
           AND oa.order_id IS NULL
         ORDER BY sp.exit_date DESC, sp.strategy
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default=ORDER_AUDIT_GO_LIVE)
    p.add_argument("--strategy", default=None)
    args = p.parse_args()

    conn = get_connection(readonly=True)
    orphan_entries = find_orphan_entries(conn, args.since, args.strategy)
    orphan_exits = find_orphan_exits(conn, args.since, args.strategy)
    conn.close()

    print(f"=== order_audit completeness check (since {args.since}) ===")
    print(f"Orphan entries (strategy_portfolio buy w/o order_audit row): {len(orphan_entries)}")
    for r in orphan_entries[:20]:
        print(f"  {r['strategy']:<18s} {r['ticker']:<6s} entry={r['entry_date']} "
              f"shares={r.get('shares', 0)} live={r.get('is_live', False)}")
    if len(orphan_entries) > 20:
        print(f"  ... +{len(orphan_entries) - 20} more")

    print(f"\nOrphan exits (strategy_portfolio sell w/o order_audit row): {len(orphan_exits)}")
    for r in orphan_exits[:20]:
        print(f"  {r['strategy']:<18s} {r['ticker']:<6s} exit={r['exit_date']} "
              f"reason={r.get('exit_reason')} live={r.get('is_live', False)}")
    if len(orphan_exits) > 20:
        print(f"  ... +{len(orphan_exits) - 20} more")

    if not orphan_entries and not orphan_exits:
        print("\n✓ All entries + exits since cutoff have order_audit provenance.")
        return 0

    print("\n⚠️  Audit gap. Investigate before flipping live: every live order "
          "MUST have a matching order_audit row, period.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
