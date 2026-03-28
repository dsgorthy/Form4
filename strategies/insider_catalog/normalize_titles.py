"""
Title normalization for insider trades.

Extracts canonical role tags from raw SEC Form 4 title strings.
Stores semicolon-separated sorted tags in trades.normalized_title.

Usage:
    python3 -m strategies.insider_catalog.normalize_titles [--db PATH]
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Canonical tag extraction rules ──────────────────────────────────────────
# Order matters: more specific patterns first to avoid false matches.
# Each rule: (tag, compiled regex). Matched against the uppercased title.

TAG_RULES: list[tuple[str, re.Pattern]] = [
    ("CEO",       re.compile(r"\bCO-?CEO\b|CO-?CHIEF EXECUTIVE|CHIEF EXECUT(?:IVE|VE) OFFICER|PRINCIPAL EXECUTIVE OFFICER|\bCEO\b|CHIEF EXECUTIVE")),
    ("CFO",       re.compile(r"\bCO-?CFO\b|CO-?CHIEF FINANCIAL|CHIEF FINANCIAL OFFICER|PRINCIPAL FINANCIAL OFFICER|\bC\.?F\.?O\.?\b")),
    ("COO",       re.compile(r"\bCO-?COO\b|CO-?CHIEF OPERAT|CHIEF OPERAT(?:ING|IONS) OFFICER|CHIEF BUS(?:INESS|\.?) OPERAT(?:ING|IONS) OFFICER|\bCOO\b")),
    ("CTO",       re.compile(r"\bCO-?CTO\b|CO-?CHIEF TECH|CHIEF TECH(?:NOLOGY|NICAL|NOLOG|\.?)? OFF?IC?(?:ER|IER)|\bCTO\b")),
    ("CLO",       re.compile(r"\bCO-?CLO\b|CHIEF LEGAL(?:\b|.*?OFFICER)|GENERAL COUNSEL|\bCLO\b")),
    ("CMO",       re.compile(r"\bCMO\b|CHIEF MARKETING OFFICER")),
    ("CIO",       re.compile(r"\bCIO\b|CHIEF INFORMATION OFFICER|CHIEF INVESTMENT OFFICER")),
    ("CAO",       re.compile(r"\bCAO\b|CHIEF ACCOUNT(?:ING|ABILITY) OFFICER|CHIEF ADMIN(?:ISTRATIVE)? OFFICER|PRINCIPAL ACCOUNTING OFFICER")),
    ("CSO",       re.compile(r"\bCSO\b|CHIEF STRATEGY OFFICER|CHIEF SCIEN(?:CE|TIFIC) OFFICER|CHIEF SECURITY OFFICER")),
    ("CPO",       re.compile(r"\bCPO\b|CHIEF PRODUCT OFFICER|CHIEF PEOPLE OFFICER")),
    ("CRO",       re.compile(r"\bCRO\b|CHIEF REVENUE OFFICER|CHIEF RISK OFFICER")),
    ("CHRO",      re.compile(r"\bCHRO\b|CHIEF HUMAN RESOURCES? OFFICER|CHIEF HR OFFICER|CHIEF PEOPLE & CULTURE|CHIEF LEADERSHIP & HR|CHIEF HR & CORP")),
    ("CCO",       re.compile(r"\bCCO\b|CHIEF COMMERCIAL OFFICER|CHIEF COMPLIANCE OFFICER|CHIEF COMMUNICATIONS? OFFICER")),
    ("Chairman",  re.compile(r"\bCHAIR(?:MAN|WOMAN|PERSON)?\b|\bCOB\b|CHAIR OF THE BOARD")),
    ("President", re.compile(r"\bPRESIDENT\b|\bPRES\b")),
    ("Director",  re.compile(r"\bDIRECTOR\b|(?:^|[;,\s])DIR(?:$|[;,\s])|DIRECTOROTHER|DIRECTOR,?OTHER")),
    ("10% Owner", re.compile(r"TENPERCENTOWNER|10\s*%|TEN PERCENT")),
    ("VP",        re.compile(r"\bS?E?\.?V\.?P\.?\b|EXECUTIVE VICE PRESIDENT|SENIOR VICE PRESIDENT|\bVICE PRESIDENT\b|\bVP\b|\bV\.P\.")),
    ("Secretary", re.compile(r"\bSECRETARY\b|\bSECY?\b")),
    ("Treasurer", re.compile(r"\bTREASURER\b")),
    ("Founder",   re.compile(r"\bFOUNDER\b|CO-?FOUNDER")),
    ("Controller",re.compile(r"\bCONTROLLER\b")),
]


def normalize_title(raw_title: str | None) -> str:
    """
    Extract canonical role tags from a raw title string.

    Returns semicolon-separated sorted tags, e.g. "CEO;Chairman;Director".
    Returns "Other" if no rules match, or empty string if title is None/empty.
    """
    if not raw_title or not raw_title.strip():
        return ""

    upper = raw_title.upper()
    tags: list[str] = []

    for tag, pattern in TAG_RULES:
        if pattern.search(upper):
            tags.append(tag)

    if not tags:
        return "Other"

    # Sort for consistent ordering
    tags.sort()
    return ";".join(tags)


def ensure_column(conn: sqlite3.Connection) -> None:
    """Add normalized_title column if it doesn't exist."""
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN normalized_title TEXT DEFAULT ''")
        conn.commit()
        logger.info("Added normalized_title column to trades")
    except sqlite3.OperationalError:
        pass  # column already exists


def backfill_normalized_titles(conn: sqlite3.Connection, batch_size: int = 10000) -> dict:
    """
    Backfill normalized_title for all trades.

    Returns stats dict with counts.
    """
    # Get all distinct raw titles
    raw_titles = conn.execute(
        "SELECT DISTINCT title FROM trades WHERE title IS NOT NULL"
    ).fetchall()

    # Build mapping: raw -> normalized
    title_map: dict[str, str] = {}
    for (raw,) in raw_titles:
        title_map[raw] = normalize_title(raw)

    # Count what mapped to "Other" for audit
    other_titles: dict[str, int] = {}
    for (raw,) in raw_titles:
        if title_map[raw] == "Other":
            count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE title = ?", (raw,)
            ).fetchone()[0]
            other_titles[raw] = count

    # Batch update by distinct title (much faster than per-row)
    updated = 0
    for raw, normalized in title_map.items():
        conn.execute(
            "UPDATE trades SET normalized_title = ? WHERE title = ?",
            (normalized, raw),
        )
        updated += 1
        if updated % 500 == 0:
            conn.commit()
            logger.info("Updated %d / %d distinct titles", updated, len(title_map))

    # Handle NULL titles
    conn.execute("UPDATE trades SET normalized_title = '' WHERE title IS NULL")
    conn.commit()

    stats = {
        "distinct_titles": len(title_map),
        "mapped_to_tags": sum(1 for v in title_map.values() if v and v != "Other"),
        "mapped_to_other": sum(1 for v in title_map.values() if v == "Other"),
        "other_titles": dict(sorted(other_titles.items(), key=lambda x: -x[1])),
    }

    return stats


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Normalize insider trade titles")
    parser.add_argument(
        "--db",
        default=str(Path(__file__).resolve().parent / "insiders.db"),
        help="Path to insiders.db",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    db_path = args.db
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    logger.info("Ensuring normalized_title column exists...")
    ensure_column(conn)

    logger.info("Backfilling normalized titles...")
    stats = backfill_normalized_titles(conn)

    conn.close()

    logger.info("Done. %d distinct titles processed.", stats["distinct_titles"])
    logger.info("  Mapped to tags: %d", stats["mapped_to_tags"])
    logger.info("  Mapped to 'Other': %d", stats["mapped_to_other"])

    if stats["other_titles"]:
        logger.info("")
        logger.info("=== UNMATCHED TITLES (mapped to 'Other') ===")
        for title, count in stats["other_titles"].items():
            logger.info("  [%d trades] %s", count, title)

    # Write audit report
    report_dir = Path(__file__).resolve().parent.parent.parent / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / "title_normalization_audit.json"
    with open(report_path, "w") as f:
        json.dump(stats, f, indent=2)
    logger.info("\nAudit report written to %s", report_path)


if __name__ == "__main__":
    main()
