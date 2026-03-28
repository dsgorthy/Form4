#!/usr/bin/env python3
"""
Fix 27 malformed tickers affecting 583 trades in insiders.db.

Handles:
  - Exchange prefix stripping (NYSE:, OTCQX:, ASX:, etc.)
  - Bloomberg suffix stripping (US)
  - Dual-class merging (Z AND ZG → Z, CBS, CBS.A → CBS, etc.)
  - Invalid/non-stock deletions (N/A, DMA; MSFIX, DIVALL 2)
  - Misc fixes (LEE ENT → LEE, OV6:GR → OSG)

Uses UPDATE OR IGNORE + DELETE orphans to handle potential unique constraint
conflicts when renaming creates duplicates.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

# Mapping: malformed ticker → corrected ticker (None = DELETE)
TICKER_FIXES = {
    "Z AND ZG":     "Z",
    "NYSE: MEG":    "MEG",
    "NYSE:EVC":     "EVC",
    "OV6:GR":       "OSG",
    "N/A":          None,
    "AROC US":      "AROC",
    "CBS, CBS.A":   "CBS",
    "PAYD:OTC":     "PAYD",
    "BIO, BIOB":    "BIO",
    "NRCIA/B":      "NRCI",
    "FCEA/FCEB":    "FCE.A",
    "LLYVA/K":      "LLYVA",
    "QADA, QADB":   "QADA",
    "GEF, GEF-B":   "GEF",
    "OTCQX:HDYN":   "HDYN",
    "DMA; MSFIX":   None,
    "GGO/GGO.A":    "GGO",
    "VIA, VIAB":    "VIA",
    "NYSE: KRC":    "KRC",
    "BFA, BFB":     "BFA",
    "LEE ENT":      "LEE",
    "ROIA/ROIAK":   "ROIA",
    "ASX:LNW":      "LNW",
    "DIVALL 2":     None,
    "NYSE:FLG":     "FLG",
    "(NYSE:FBC)":   "FBC",
    "MOGA/MOGB":    "MOG.A",
}


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    # Preview
    print("=" * 70)
    print("MALFORMED TICKER FIX — PREVIEW")
    print("=" * 70)

    total_update = 0
    total_delete = 0

    for bad, good in TICKER_FIXES.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE ticker = ?", (bad,)
        ).fetchone()[0]

        if count == 0:
            continue

        if good is None:
            print(f"  DELETE  {bad:20s} → (remove)     {count:4d} trades")
            total_delete += count
        else:
            print(f"  RENAME  {bad:20s} → {good:10s}  {count:4d} trades")
            total_update += count

    print(f"\nTotal: {total_update} trades to rename, {total_delete} trades to delete")
    print(f"       {total_update + total_delete} trades affected\n")

    # Execute in a transaction
    print("Applying fixes...")
    cur = conn.cursor()
    cur.execute("BEGIN")

    try:
        renamed = 0
        deleted = 0

        for bad, good in TICKER_FIXES.items():
            if good is None:
                # Delete trades with invalid tickers
                # First delete related trade_returns
                trade_ids = [r[0] for r in cur.execute(
                    "SELECT trade_id FROM trades WHERE ticker = ?", (bad,)
                ).fetchall()]

                if trade_ids:
                    placeholders = ",".join("?" * len(trade_ids))
                    cur.execute(
                        f"DELETE FROM trade_returns WHERE trade_id IN ({placeholders})",
                        trade_ids,
                    )
                    # Delete from insider_companies too
                    cur.execute(
                        "DELETE FROM insider_companies WHERE ticker = ?", (bad,)
                    )
                    n = cur.execute(
                        "DELETE FROM trades WHERE ticker = ?", (bad,)
                    ).rowcount
                    deleted += n
                    print(f"  Deleted {n} trades with ticker '{bad}'")
            else:
                # Rename: use UPDATE OR IGNORE to skip rows that would violate
                # the UNIQUE constraint (insider_id, ticker, trade_date, trade_type, value)
                n = cur.execute(
                    "UPDATE OR IGNORE trades SET ticker = ? WHERE ticker = ?",
                    (good, bad),
                ).rowcount
                renamed += n

                # Delete any orphans that couldn't be updated (duplicates)
                orphans = cur.execute(
                    "SELECT COUNT(*) FROM trades WHERE ticker = ?", (bad,)
                ).fetchone()[0]
                if orphans > 0:
                    # These are genuine duplicates — delete their returns first
                    orphan_ids = [r[0] for r in cur.execute(
                        "SELECT trade_id FROM trades WHERE ticker = ?", (bad,)
                    ).fetchall()]
                    if orphan_ids:
                        placeholders = ",".join("?" * len(orphan_ids))
                        cur.execute(
                            f"DELETE FROM trade_returns WHERE trade_id IN ({placeholders})",
                            orphan_ids,
                        )
                    cur.execute("DELETE FROM trades WHERE ticker = ?", (bad,))
                    print(f"  Renamed {n} trades '{bad}' → '{good}' ({orphans} duplicate orphans removed)")
                else:
                    print(f"  Renamed {n} trades '{bad}' → '{good}'")

                # Update insider_companies too
                cur.execute(
                    "UPDATE OR IGNORE insider_companies SET ticker = ? WHERE ticker = ?",
                    (good, bad),
                )
                cur.execute(
                    "DELETE FROM insider_companies WHERE ticker = ?", (bad,)
                )

        cur.execute("COMMIT")
        print(f"\nDone. Renamed {renamed} trades, deleted {deleted} trades.")

    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"\nERROR — rolled back: {e}")
        raise

    # Verify no malformed tickers remain
    print("\nVerification:")
    remaining = 0
    for bad in TICKER_FIXES:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE ticker = ?", (bad,)
        ).fetchone()[0]
        if count > 0:
            print(f"  WARNING: {count} trades still have ticker '{bad}'")
            remaining += count

    if remaining == 0:
        print("  All malformed tickers fixed successfully.")

    total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    print(f"  Total trades in DB: {total:,}")

    conn.close()


if __name__ == "__main__":
    main()
