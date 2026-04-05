"""
Rebuild insiders.db from corrupt copy by exporting all tables via SELECT
and importing into a fresh database. Handles index corruption where data
pages are intact but btree/index pages have invalid references.

Usage:
    python3 pipelines/insider_study/rebuild_db.py
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
NEW_DB_PATH = DB_PATH.parent / "insiders_rebuilt.db"


def main():
    if NEW_DB_PATH.exists():
        print(f"Removing existing {NEW_DB_PATH.name}")
        os.remove(str(NEW_DB_PATH))

    old = sqlite3.connect(str(DB_PATH))
    old.execute("PRAGMA busy_timeout=30000")

    new = sqlite3.connect(str(NEW_DB_PATH))
    new.execute("PRAGMA journal_mode=WAL")
    new.execute("PRAGMA synchronous=NORMAL")

    cur_old = old.cursor()

    # Get all table schemas from sqlite_master (original CREATE TABLE)
    cur_old.execute("""
        SELECT name, sql FROM sqlite_master
        WHERE type='table' AND name != 'sqlite_sequence'
        ORDER BY name
    """)
    table_schemas = {name: sql for name, sql in cur_old.fetchall() if sql}
    table_names = sorted(table_schemas.keys())
    print(f"Tables to rebuild: {len(table_names)}")

    total_start = time.time()
    for name in table_names:
        # Get actual columns (includes ALTER TABLE additions)
        cur_old.execute(f"PRAGMA table_info([{name}])")
        cols_info = cur_old.fetchall()
        col_names = [c[1] for c in cols_info]
        data_col_count = len(col_names)

        cur_old.execute(f"SELECT COUNT(*) FROM [{name}]")
        count = cur_old.fetchone()[0]
        print(f"  {name}: {count:,} rows ({data_col_count} cols)...", end="", flush=True)

        # Create table using original schema
        schema_sql = table_schemas[name]
        new.execute(schema_sql)

        # Check if ALTER TABLE added columns not in original schema
        cur_new = new.cursor()
        cur_new.execute(f"PRAGMA table_info([{name}])")
        new_col_count = len(cur_new.fetchall())

        if new_col_count < data_col_count:
            # Add missing columns via ALTER TABLE
            for col in cols_info[new_col_count:]:
                cid, cname, ctype, notnull, default, pk = col
                alter = f"ALTER TABLE [{name}] ADD COLUMN [{cname}]"
                if ctype:
                    alter += f" {ctype}"
                new.execute(alter)
                print(f"+{cname}", end=" ", flush=True)

        new.commit()

        if count == 0:
            print(" skip")
            continue

        t0 = time.time()
        col_list = ",".join(f"[{c}]" for c in col_names)
        placeholders = ",".join("?" * data_col_count)

        # Stream in batches
        cur_old.execute(f"SELECT {col_list} FROM [{name}]")
        batch = []
        copied = 0
        for row in cur_old:
            batch.append(row)
            if len(batch) >= 10000:
                new.executemany(
                    f"INSERT INTO [{name}] ({col_list}) VALUES ({placeholders})", batch
                )
                copied += len(batch)
                batch = []
        if batch:
            new.executemany(
                f"INSERT INTO [{name}] ({col_list}) VALUES ({placeholders})", batch
            )
            copied += len(batch)
        new.commit()

        elapsed = time.time() - t0
        rate = copied / elapsed if elapsed > 0 else 0
        print(f" {copied:,} ({elapsed:.1f}s, {rate:.0f}/s)")

    # Recreate indexes
    cur_old.execute("""
        SELECT sql FROM sqlite_master
        WHERE type='index' AND sql IS NOT NULL
        ORDER BY name
    """)
    indexes = [r[0] for r in cur_old.fetchall()]
    print(f"\nRecreating {len(indexes)} indexes...")
    for idx_sql in indexes:
        new.execute(idx_sql)
    new.commit()
    print("Indexes done.")

    # Recreate triggers
    cur_old.execute("""
        SELECT sql FROM sqlite_master
        WHERE type='trigger' AND sql IS NOT NULL
    """)
    triggers = [r[0] for r in cur_old.fetchall()]
    if triggers:
        print(f"Recreating {len(triggers)} triggers...")
        for t in triggers:
            new.execute(t)
        new.commit()

    # Verify
    print("\nVerification:")
    cur_new = new.cursor()
    all_ok = True
    for name in table_names:
        cur_old.execute(f"SELECT COUNT(*) FROM [{name}]")
        old_count = cur_old.fetchone()[0]
        cur_new.execute(f"SELECT COUNT(*) FROM [{name}]")
        new_count = cur_new.fetchone()[0]
        ok = old_count == new_count
        if not ok:
            all_ok = False
        status = "OK" if ok else f"MISMATCH ({old_count} vs {new_count})"
        print(f"  {name}: {new_count:,} {status}")

    # Integrity check
    print("\nIntegrity check...")
    cur_new.execute("PRAGMA integrity_check")
    result = cur_new.fetchone()[0]
    print(f"  {result}")

    old.close()
    new.close()

    old_size = DB_PATH.stat().st_size / (1024**3)
    new_size = NEW_DB_PATH.stat().st_size / (1024**3)
    total_elapsed = time.time() - total_start
    print(f"\nOld: {old_size:.2f} GB, New: {new_size:.2f} GB")
    print(f"Total time: {total_elapsed/60:.1f} min")

    if all_ok:
        print(f"\nAll tables match. To replace:")
        print(f"  mv {DB_PATH} {DB_PATH}.corrupt")
        print(f"  mv {NEW_DB_PATH} {DB_PATH}")
    else:
        print("\nWARNING: Some tables have mismatches. Review before replacing.")


if __name__ == "__main__":
    main()
