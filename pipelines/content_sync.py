#!/usr/bin/env python3
"""Content sync — mirror between GDrive and local backup.

GDrive is the hot store (accessible from phone). After 7 days, content
moves to local backup to save Drive storage.

Folder structure (identical on both):
  form4-content/
    2026-03-25/
      storyboard/
      carousels/
        top_buys/
        top_sells/
        weekly_top_buys/
      x_post.txt
      captions.txt
      carousel_caption.txt

Usage:
    python3 pipelines/content_sync.py --backup       # GDrive → local (>7 days old)
    python3 pipelines/content_sync.py --restore 2026-03-25  # local → GDrive
    python3 pipelines/content_sync.py --status        # show what's where
"""
from __future__ import annotations

import argparse
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

GDRIVE_BASE = Path.home() / "Library/CloudStorage/GoogleDrive-derek@sidequestgroup.com/My Drive/personal-share/form4-content"
LOCAL_BASE = Path(__file__).resolve().parent.parent / "data" / "content-archive"
BACKUP_AFTER_DAYS = 7


def backup(dry_run: bool = False):
    """Move content older than BACKUP_AFTER_DAYS from GDrive to local."""
    if not GDRIVE_BASE.exists():
        # Fallback path
        alt = Path.home() / "Google Drive/My Drive/personal-share/form4-content"
        if alt.exists():
            gdrive = alt
        else:
            logger.error(f"GDrive not found at {GDRIVE_BASE}")
            return
    else:
        gdrive = GDRIVE_BASE

    LOCAL_BASE.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now() - timedelta(days=BACKUP_AFTER_DAYS)

    moved = 0
    for day_dir in sorted(gdrive.iterdir()):
        if not day_dir.is_dir():
            continue
        name = day_dir.name
        # Parse date from folder name (YYYY-MM-DD)
        try:
            folder_date = datetime.strptime(name[:10], "%Y-%m-%d")
        except (ValueError, IndexError):
            continue

        if folder_date >= cutoff:
            continue  # too recent, keep on GDrive

        local_dest = LOCAL_BASE / name
        if local_dest.exists():
            logger.info(f"  {name}: already backed up locally, removing from GDrive")
            if not dry_run:
                shutil.rmtree(day_dir)
            moved += 1
            continue

        logger.info(f"  {name}: moving to local backup")
        if not dry_run:
            shutil.copytree(day_dir, local_dest)
            shutil.rmtree(day_dir)
        moved += 1

    logger.info(f"Backup complete: {moved} folders {'would be ' if dry_run else ''}moved")


def restore(date_str: str):
    """Copy a specific date from local backup back to GDrive."""
    gdrive = GDRIVE_BASE if GDRIVE_BASE.exists() else Path.home() / "Google Drive/My Drive/personal-share/form4-content"

    local_src = LOCAL_BASE / date_str
    if not local_src.exists():
        logger.error(f"Not found in local backup: {local_src}")
        return

    gdrive_dest = gdrive / date_str
    if gdrive_dest.exists():
        logger.info(f"{date_str} already on GDrive")
        return

    shutil.copytree(local_src, gdrive_dest)
    logger.info(f"Restored {date_str} to GDrive")


def status():
    """Show what's on GDrive vs local backup."""
    gdrive = GDRIVE_BASE if GDRIVE_BASE.exists() else Path.home() / "Google Drive/My Drive/personal-share/form4-content"

    gdrive_dates = set()
    if gdrive.exists():
        for d in gdrive.iterdir():
            if d.is_dir() and len(d.name) >= 10:
                gdrive_dates.add(d.name[:10])

    local_dates = set()
    if LOCAL_BASE.exists():
        for d in LOCAL_BASE.iterdir():
            if d.is_dir() and len(d.name) >= 10:
                local_dates.add(d.name[:10])

    all_dates = sorted(gdrive_dates | local_dates)
    cutoff = (datetime.now() - timedelta(days=BACKUP_AFTER_DAYS)).strftime("%Y-%m-%d")

    print(f"{'Date':>12} | {'GDrive':>6} | {'Local':>5} | Status")
    print("-" * 45)
    for d in all_dates:
        on_gdrive = "YES" if d in gdrive_dates else ""
        on_local = "YES" if d in local_dates else ""
        if d in gdrive_dates and d < cutoff:
            note = "← should backup"
        elif d in local_dates and d not in gdrive_dates:
            note = "backed up"
        elif d in gdrive_dates and d in local_dates:
            note = "both (GDrive copy can be removed)"
        else:
            note = "active"
        print(f"{d:>12} | {on_gdrive:>6} | {on_local:>5} | {note}")

    print(f"\nGDrive: {len(gdrive_dates)} dates | Local: {len(local_dates)} dates")
    print(f"Backup threshold: {BACKUP_AFTER_DAYS} days (cutoff: {cutoff})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", action="store_true", help="Move old content from GDrive to local")
    parser.add_argument("--restore", type=str, help="Restore a date from local to GDrive (YYYY-MM-DD)")
    parser.add_argument("--status", action="store_true", help="Show what's where")
    parser.add_argument("--dry-run", action="store_true", help="Preview backup without moving")
    args = parser.parse_args()

    if args.status:
        status()
    elif args.backup:
        backup(dry_run=args.dry_run)
    elif args.restore:
        restore(args.restore)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
