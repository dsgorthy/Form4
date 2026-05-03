"""Per-dataset progress manifest.

Each puller calls `manifest.update(dataset, ...)` after a batch. The manifest
records item counts, bytes on disk, last item pulled, and last-update time.

Storage: a single JSON file at <FORM4_DATA_ROOT>/manifest/datasets.json.
Once the dataset_manifest PG table is applied, this module also writes there
as a secondary index — JSON remains authoritative so an offline array can be
inspected without DB access.
"""
from __future__ import annotations

import json
import os
import sys
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.storage_paths import paths

MANIFEST_FILE = paths.manifest / "datasets.json"


def _load() -> dict:
    if not MANIFEST_FILE.exists():
        return {}
    with open(MANIFEST_FILE) as f:
        return json.load(f)


def _save(data: dict) -> None:
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_FILE.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, MANIFEST_FILE)


def _maybe_write_pg(dataset: str, entry: dict) -> None:
    """Best-effort write to dataset_manifest PG table. Silently no-ops if
    the DB is unreachable or the table doesn't exist yet."""
    if os.getenv("FORM4_MANIFEST_DISABLE_PG"):
        return
    with suppress(Exception):
        from config.database import get_connection
        conn = get_connection()
        try:
            conn.execute(
                """INSERT INTO dataset_manifest
                   (dataset, storage_root, description, item_count, bytes_on_disk,
                    last_item_pulled, last_updated_at, notes)
                   VALUES (?, ?, ?, ?, ?, ?, NOW(), ?)
                   ON CONFLICT (dataset) DO UPDATE SET
                       storage_root = EXCLUDED.storage_root,
                       description  = COALESCE(EXCLUDED.description, dataset_manifest.description),
                       item_count   = EXCLUDED.item_count,
                       bytes_on_disk = EXCLUDED.bytes_on_disk,
                       last_item_pulled = EXCLUDED.last_item_pulled,
                       last_updated_at = NOW(),
                       notes = COALESCE(EXCLUDED.notes, dataset_manifest.notes)""",
                (dataset, entry["storage_root"], entry.get("description"),
                 entry["item_count"], entry["bytes_on_disk"],
                 entry.get("last_item_pulled"), entry.get("notes")),
            )
            conn.commit()
        finally:
            conn.close()


def update(
    dataset: str,
    *,
    storage_root: str,
    item_count: int,
    bytes_on_disk: int,
    last_item_pulled: Optional[str] = None,
    description: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """Record dataset progress. Idempotent and atomic on the JSON side."""
    data = _load()
    entry = {
        "storage_root": storage_root,
        "item_count": item_count,
        "bytes_on_disk": bytes_on_disk,
        "last_item_pulled": last_item_pulled,
        "description": description,
        "notes": notes,
        "last_updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    data[dataset] = entry
    _save(data)
    _maybe_write_pg(dataset, entry)


def get(dataset: str) -> Optional[dict]:
    return _load().get(dataset)


def list_all() -> dict:
    return _load()


def scan_dir_size(path: Path) -> tuple[int, int]:
    """Walk a directory and return (file_count, total_bytes). Cheap enough
    for periodic manifest refresh; for very large trees consider caching."""
    if not path.exists():
        return 0, 0
    files = 0
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            files += 1
            total += entry.stat().st_size
    return files, total


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Inspect or refresh the dataset manifest")
    p.add_argument("--refresh", action="store_true",
                   help="Re-scan known dataset directories and update sizes")
    p.add_argument("--dataset", help="Show only this dataset")
    args = p.parse_args()

    if args.refresh:
        scans = [
            ("equity_1min",     paths.equity_1min,     "Phase 1 #1 — 1-min OHLCV bars"),
            ("equity_daily",    paths.equity_daily,    "Daily OHLCV"),
            ("equity_taq",      paths.equity_taq,      "Phase 2 #7 — TAQ event windows"),
            ("options_eod",     paths.options_eod,     "Phase 1 #3 — ThetaData EOD options"),
            ("options_1min",    paths.options_1min,    "Phase 2 #6 — ThetaData 1-min options"),
            ("edgar_filings",   paths.edgar_filings,   "Phase 1 #2 — SEC EDGAR archive"),
            ("fundamentals",    paths.fundamentals,    "Phase 3 #11 — XBRL parsed"),
            ("news",            paths.news,            "Phase 3 #8 — news corpus"),
            ("transcripts",     paths.transcripts,     "Phase 3 #9 — earnings call transcripts"),
            ("short_metrics",   paths.short_metrics,   "Phase 1 #4 — short interest + borrow"),
            ("etf_data",        paths.etf_data,        "Phase 3 #10 — ETF holdings + flows"),
        ]
        for name, dir_path, desc in scans:
            files, bytes_ = scan_dir_size(dir_path)
            update(name, storage_root=str(dir_path), item_count=files,
                   bytes_on_disk=bytes_, description=desc)
            print(f"  {name:16s} {files:>10,} files  {bytes_/1e9:>8.2f} GB")
        print(f"\nManifest: {MANIFEST_FILE}")
    else:
        data = _load()
        if not data:
            print(f"Manifest empty. Run with --refresh to populate.")
            print(f"Path: {MANIFEST_FILE}")
        elif args.dataset:
            entry = data.get(args.dataset)
            if not entry:
                print(f"No entry for {args.dataset!r}")
                sys.exit(1)
            print(json.dumps(entry, indent=2))
        else:
            print(f"{'dataset':18s} {'items':>10s} {'GB':>8s}  updated")
            print("─" * 70)
            for name in sorted(data):
                e = data[name]
                gb = e["bytes_on_disk"] / 1e9
                ts = e["last_updated_at"][:19]
                print(f"{name:18s} {e['item_count']:>10,} {gb:>8.2f}  {ts}")
