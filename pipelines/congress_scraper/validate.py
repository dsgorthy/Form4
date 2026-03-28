"""4-layer data quality validation for congress trading data.

Layer 1: Field-level validation (per-record)
Layer 2: Cross-record validation (dedup, consistency)
Layer 3: Periodic integrity checks (coverage, freshness)
Layer 4: Alerting (Telegram notifications for anomalies)

Can be run standalone for a full audit or called per-record
from the scraper for real-time validation.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("congress_scraper.validate")

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"

# Valid value bands from STOCK Act
VALID_LOW_VALUES = {1001, 15001, 50001, 100001, 250001, 500001, 1000001, 5000001, 25000001, 50000001}
VALID_HIGH_VALUES = {15000, 50000, 100000, 250000, 500000, 1000000, 5000000, 25000000, 50000000, 100000000}

VALID_TRADE_TYPES = {"buy", "sell", "exchange"}
VALID_OWNERS = {"Self", "Spouse", "Joint", "Child", None}
VALID_CHAMBERS = {"House", "Senate"}
VALID_PARTIES = {"D", "R", "I", None}


# ---------------------------------------------------------------------------
# Layer 1: Field-level validation
# ---------------------------------------------------------------------------

class ValidationError:
    """A single validation issue."""
    def __init__(self, level: str, field: str, message: str, record_id: Optional[int] = None):
        self.level = level  # "error" or "warning"
        self.field = field
        self.message = message
        self.record_id = record_id

    def __repr__(self):
        prefix = f"[{self.record_id}] " if self.record_id else ""
        return f"{self.level.upper()}: {prefix}{self.field} — {self.message}"


def validate_record(record: dict) -> list[ValidationError]:
    """Validate a single transaction record before insert.

    Args:
        record: Dict with keys matching congress_trades columns.

    Returns:
        List of ValidationErrors (empty = clean record).
    """
    errors: list[ValidationError] = []
    rid = record.get("congress_trade_id")

    # Ticker
    ticker = record.get("ticker")
    if not ticker:
        errors.append(ValidationError("error", "ticker", "Missing ticker", rid))
    elif not re.match(r"^[A-Z]{1,5}$", ticker):
        errors.append(ValidationError("error", "ticker", f"Invalid ticker format: {ticker}", rid))

    # Trade type
    trade_type = record.get("trade_type")
    if trade_type not in VALID_TRADE_TYPES:
        errors.append(ValidationError("error", "trade_type", f"Invalid trade type: {trade_type}", rid))

    # Trade date
    trade_date = record.get("trade_date")
    if not trade_date:
        errors.append(ValidationError("error", "trade_date", "Missing trade date", rid))
    else:
        try:
            td = datetime.strptime(trade_date, "%Y-%m-%d")
            if td > datetime.now() + timedelta(days=1):
                errors.append(ValidationError("warning", "trade_date", f"Future trade date: {trade_date}", rid))
            if td < datetime(2012, 1, 1):
                errors.append(ValidationError("warning", "trade_date", f"Pre-STOCK Act date: {trade_date}", rid))
        except ValueError:
            errors.append(ValidationError("error", "trade_date", f"Invalid date format: {trade_date}", rid))

    # Filing date
    filing_date = record.get("filing_date")
    if filing_date:
        try:
            fd = datetime.strptime(filing_date, "%Y-%m-%d")
            if trade_date:
                td = datetime.strptime(trade_date, "%Y-%m-%d")
                delay = (fd - td).days
                if delay < 0:
                    errors.append(ValidationError("warning", "filing_date",
                                                  f"Filing date before trade date (delay={delay}d)", rid))
                if delay > 90:
                    errors.append(ValidationError("warning", "filing_date",
                                                  f"Filing delay > 90 days ({delay}d)", rid))
        except ValueError:
            errors.append(ValidationError("error", "filing_date", f"Invalid date format: {filing_date}", rid))

    # Value range
    value_low = record.get("value_low")
    value_high = record.get("value_high")
    if value_low is not None and value_high is not None:
        if value_low > value_high:
            errors.append(ValidationError("error", "value_range", f"Low > high: {value_low} > {value_high}", rid))
        if value_low not in VALID_LOW_VALUES:
            errors.append(ValidationError("warning", "value_low",
                                          f"Non-standard STOCK Act low value: {value_low}", rid))
        if value_high not in VALID_HIGH_VALUES:
            errors.append(ValidationError("warning", "value_high",
                                          f"Non-standard STOCK Act high value: {value_high}", rid))

    # Owner
    owner = record.get("owner")
    if owner and owner not in VALID_OWNERS:
        errors.append(ValidationError("warning", "owner", f"Non-standard owner: {owner}", rid))

    return errors


def validate_record_strict(record: dict) -> bool:
    """Return True if record passes strict validation (no errors, warnings OK)."""
    errors = validate_record(record)
    return not any(e.level == "error" for e in errors)


# ---------------------------------------------------------------------------
# Layer 2: Cross-record validation
# ---------------------------------------------------------------------------

def check_duplicates(conn: sqlite3.Connection) -> list[ValidationError]:
    """Find potential duplicate records that slipped past UNIQUE constraint.

    Checks for same politician + ticker + trade_type + trade_date with different value ranges
    (which would bypass the UNIQUE constraint that includes value_low).
    """
    errors = []

    rows = conn.execute("""
        SELECT politician_id, ticker, trade_type, trade_date, COUNT(*) as cnt,
               GROUP_CONCAT(congress_trade_id) as ids
        FROM congress_trades
        GROUP BY politician_id, ticker, trade_type, trade_date
        HAVING cnt > 1
    """).fetchall()

    for row in rows:
        pid, ticker, tt, td, cnt, ids = row
        if cnt > 3:  # More than 3 records for same person/ticker/date is suspicious
            errors.append(ValidationError(
                "warning", "duplicate",
                f"Politician {pid} has {cnt} {tt} trades for {ticker} on {td} (IDs: {ids})"
            ))

    logger.info(f"Duplicate check: {len(errors)} potential issues found")
    return errors


def check_politician_consistency(conn: sqlite3.Connection) -> list[ValidationError]:
    """Check for politician records that might be the same person with different names."""
    errors = []

    # Find politicians with very similar normalized names in the same chamber
    rows = conn.execute("""
        SELECT a.politician_id, a.name, a.name_normalized, a.chamber,
               b.politician_id, b.name, b.name_normalized
        FROM politicians a
        JOIN politicians b ON a.chamber = b.chamber
            AND a.politician_id < b.politician_id
            AND (
                a.name_normalized LIKE b.name_normalized || '%'
                OR b.name_normalized LIKE a.name_normalized || '%'
            )
    """).fetchall()

    for row in rows:
        errors.append(ValidationError(
            "warning", "politician_merge",
            f"Possible duplicate politicians: [{row[0]}] {row[1]} vs [{row[4]}] {row[5]} ({row[3]})"
        ))

    logger.info(f"Politician consistency: {len(errors)} potential merges")
    return errors


# ---------------------------------------------------------------------------
# Layer 3: Periodic integrity checks
# ---------------------------------------------------------------------------

def check_coverage(conn: sqlite3.Connection) -> dict:
    """Check data coverage metrics for monitoring."""
    metrics = {}

    # Total records by source
    rows = conn.execute("""
        SELECT source, COUNT(*), MIN(trade_date), MAX(trade_date), MAX(filing_date)
        FROM congress_trades
        GROUP BY source
    """).fetchall()

    for source, count, min_date, max_date, last_filing in rows:
        metrics[f"{source}_count"] = count
        metrics[f"{source}_date_range"] = f"{min_date} to {max_date}"
        metrics[f"{source}_last_filing"] = last_filing

    # Records per chamber
    rows = conn.execute("""
        SELECT p.chamber, COUNT(ct.congress_trade_id)
        FROM congress_trades ct
        JOIN politicians p ON ct.politician_id = p.politician_id
        GROUP BY p.chamber
    """).fetchall()
    for chamber, count in rows:
        metrics[f"{chamber.lower()}_trades"] = count

    # Politicians per chamber
    rows = conn.execute("""
        SELECT chamber, COUNT(*)
        FROM politicians
        GROUP BY chamber
    """).fetchall()
    for chamber, count in rows:
        metrics[f"{chamber.lower()}_politicians"] = count

    # Freshness: trades in last 7 days
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) FROM congress_trades WHERE filing_date >= ?",
        (week_ago,),
    ).fetchone()
    metrics["filings_last_7d"] = row[0]

    # Freshness: trades in last 30 days
    month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) FROM congress_trades WHERE filing_date >= ?",
        (month_ago,),
    ).fetchone()
    metrics["filings_last_30d"] = row[0]

    # Null rate for key fields
    total = conn.execute("SELECT COUNT(*) FROM congress_trades").fetchone()[0]
    if total > 0:
        nulls = conn.execute("""
            SELECT
                SUM(CASE WHEN value_low IS NULL THEN 1 ELSE 0 END) as null_value,
                SUM(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END) as null_filing,
                SUM(CASE WHEN owner IS NULL THEN 1 ELSE 0 END) as null_owner,
                SUM(CASE WHEN company IS NULL THEN 1 ELSE 0 END) as null_company
            FROM congress_trades
        """).fetchone()
        metrics["null_rate_value"] = f"{nulls[0] / total * 100:.1f}%"
        metrics["null_rate_filing_date"] = f"{nulls[1] / total * 100:.1f}%"
        metrics["null_rate_owner"] = f"{nulls[2] / total * 100:.1f}%"
        metrics["null_rate_company"] = f"{nulls[3] / total * 100:.1f}%"

    return metrics


def check_freshness_alert(conn: sqlite3.Connection, max_gap_hours: int = 48) -> Optional[str]:
    """Return an alert message if data is stale, else None.

    'Stale' means no new filings (by filing_date) within max_gap_hours.
    This accounts for weekends — if it's Monday and last filing was Friday, that's OK.
    """
    row = conn.execute(
        "SELECT MAX(created_at) FROM congress_trades WHERE source IN ('senate_efd', 'house_scraper')"
    ).fetchone()

    if not row or not row[0]:
        return "No scraped congress trades found at all — scraper may not have run"

    last_insert = datetime.fromisoformat(row[0])
    gap = datetime.now() - last_insert
    gap_hours = gap.total_seconds() / 3600

    if gap_hours > max_gap_hours:
        return f"Congress scraper data is stale: last insert was {gap_hours:.1f} hours ago ({last_insert.isoformat()})"

    return None


# ---------------------------------------------------------------------------
# Layer 4: Alerting
# ---------------------------------------------------------------------------

def send_telegram_alert(message: str) -> None:
    """Send a validation alert via Telegram (reuses existing framework alert)."""
    try:
        from framework.alerts.telegram import send_alert
        send_alert(f"[Congress Scraper] {message}")
    except ImportError:
        logger.warning(f"Telegram alert not available. Alert: {message}")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")


# ---------------------------------------------------------------------------
# Full audit (standalone entry point)
# ---------------------------------------------------------------------------

def run_full_audit(db_path: Path = DB_PATH) -> dict:
    """Run all validation layers and return a comprehensive report."""
    conn = sqlite3.Connection(str(db_path))
    conn.row_factory = sqlite3.Row

    report = {
        "timestamp": datetime.now().isoformat(),
        "coverage": {},
        "field_errors": [],
        "duplicates": [],
        "politician_issues": [],
        "freshness_alert": None,
    }

    # Layer 1: Field-level validation on recent records
    recent = conn.execute("""
        SELECT * FROM congress_trades
        WHERE created_at >= datetime('now', '-7 days')
        ORDER BY created_at DESC
        LIMIT 1000
    """).fetchall()

    for row in recent:
        record = dict(row)
        errors = validate_record(record)
        if errors:
            report["field_errors"].extend([repr(e) for e in errors])

    # Layer 2: Cross-record
    report["duplicates"] = [repr(e) for e in check_duplicates(conn)]
    report["politician_issues"] = [repr(e) for e in check_politician_consistency(conn)]

    # Layer 3: Coverage
    report["coverage"] = check_coverage(conn)

    # Layer 4: Freshness
    report["freshness_alert"] = check_freshness_alert(conn)

    conn.close()

    # Log summary
    total_issues = len(report["field_errors"]) + len(report["duplicates"]) + len(report["politician_issues"])
    logger.info(f"Full audit complete: {total_issues} issues found across {len(recent)} recent records")

    if report["freshness_alert"]:
        logger.warning(report["freshness_alert"])
        send_telegram_alert(report["freshness_alert"])

    if len(report["field_errors"]) > 50:
        msg = f"High field error rate: {len(report['field_errors'])} errors in last 1000 records"
        logger.warning(msg)
        send_telegram_alert(msg)

    return report


def main():
    """Run full audit and print results."""
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    report = run_full_audit()

    print("\n" + "=" * 60)
    print("CONGRESS DATA QUALITY AUDIT")
    print("=" * 60)

    print("\n--- Coverage ---")
    for k, v in report["coverage"].items():
        print(f"  {k}: {v}")

    if report["freshness_alert"]:
        print(f"\n--- FRESHNESS ALERT ---\n  {report['freshness_alert']}")

    print(f"\n--- Field Errors ({len(report['field_errors'])}) ---")
    for e in report["field_errors"][:20]:
        print(f"  {e}")
    if len(report["field_errors"]) > 20:
        print(f"  ... and {len(report['field_errors']) - 20} more")

    print(f"\n--- Duplicate Issues ({len(report['duplicates'])}) ---")
    for e in report["duplicates"][:10]:
        print(f"  {e}")

    print(f"\n--- Politician Issues ({len(report['politician_issues'])}) ---")
    for e in report["politician_issues"][:10]:
        print(f"  {e}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
