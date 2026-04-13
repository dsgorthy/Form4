#!/usr/bin/env python3
"""
CEO Watcher Signal Comparison
==============================

Fetches CEO Watcher daily emails, parses their trade picks, and compares
against Form4's own signals for the same filing date. Produces a report
showing:
  - Trades they flagged that we also flagged (overlap)
  - Trades they flagged that we missed or graded low (their edge)
  - Trades we flagged that they missed (our edge)

Usage:
    python3 pipelines/ceowatcher_compare.py                 # Latest unread
    python3 pipelines/ceowatcher_compare.py --all            # All emails
    python3 pipelines/ceowatcher_compare.py --date 2026-04-10  # Specific date
    python3 pipelines/ceowatcher_compare.py --summary        # Aggregate stats across all
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from config.database import get_connection
from pipelines.ceowatcher_reader import connect, fetch_emails

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "ceowatcher"


# ---------------------------------------------------------------------------
# Email parsing — extract structured trades from CEO Watcher format
# ---------------------------------------------------------------------------

# Pattern: * Title (Name) at **Company $TICKER** purchased/sold $VALUE ...
TRADE_PATTERN = re.compile(
    r"^\*\s+"                                     # bullet
    r"(?P<title>[^(]+?)\s*"                        # title
    r"\((?P<name>[^)]+)\)\s+"                      # (name)
    r"at\s+\*?\*?(?P<company>[^$*]+?)\s*"          # company
    r"\$(?P<ticker>[A-Z]{1,5})\*?\*?\s+"           # $TICKER
    r"(?P<action>purchased|sold)\s+"               # action
    r"\$(?P<value>[\d,.]+[KMB]?)",                 # $value
    re.IGNORECASE
)

# Context patterns within the same bullet
CONTEXT_PATTERNS = {
    "dip_buy": re.compile(r"dip buy.*?down\s+[-]?([\d.]+)%", re.IGNORECASE),
    "rip_buy": re.compile(r"rip buy.*?up\s+([\d.]+)%", re.IGNORECASE),
    "largest": re.compile(r"(\d+)(?:st|nd|rd|th) largest (?:purchase|sale)", re.IGNORECASE),
    "holdings_change": re.compile(r"increased.*?holdings by ([\d.]+)%", re.IGNORECASE),
    "first_purchase": re.compile(r"first purchase", re.IGNORECASE),
    "cluster": re.compile(r"(\d+) other insiders also (?:purchased|sold)", re.IGNORECASE),
    "repeat": re.compile(r"purchased.*?(\d+) times? in the last", re.IGNORECASE),
    "high_signal": re.compile(r"high signal", re.IGNORECASE),
}


def parse_value(val_str: str) -> float:
    """Parse '$44.30M' or '$164.43K' or '$5,000' into a float."""
    val_str = val_str.replace(",", "")
    multiplier = 1
    if val_str.upper().endswith("B"):
        multiplier = 1_000_000_000
        val_str = val_str[:-1]
    elif val_str.upper().endswith("M"):
        multiplier = 1_000_000
        val_str = val_str[:-1]
    elif val_str.upper().endswith("K"):
        multiplier = 1_000
        val_str = val_str[:-1]
    try:
        return float(val_str) * multiplier
    except ValueError:
        return 0.0


def parse_email_trades(body: str) -> list[dict]:
    """Extract structured trades from CEO Watcher email body."""
    # Strip HTML
    if "<html" in body.lower():
        text = re.sub(r"<style[^>]*>.*?</style>", "", body, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "\n", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&#\d+;", "", text)
    else:
        text = body

    trades = []
    # Split into bullet-point blocks (each starts with *)
    bullets = re.split(r"\n\*\s+", text)

    for bullet in bullets:
        bullet = "* " + bullet.strip() if not bullet.startswith("*") else bullet.strip()
        m = TRADE_PATTERN.match(bullet)
        if not m:
            continue

        trade = {
            "title": m.group("title").strip(),
            "name": m.group("name").strip(),
            "company": m.group("company").strip().rstrip("*").strip(),
            "ticker": m.group("ticker").upper(),
            "trade_type": "buy" if "purchased" in m.group("action").lower() else "sell",
            "value": parse_value(m.group("value")),
            "context": {},
            "raw": bullet[:300],
        }

        # Extract context signals
        for key, pattern in CONTEXT_PATTERNS.items():
            cm = pattern.search(bullet)
            if cm:
                trade["context"][key] = cm.group(1) if cm.lastindex else True

        trades.append(trade)

    return trades


def extract_date_from_subject(subject: str) -> str | None:
    """Extract date from subject like 'Top insider trades (Fri, Apr 10)'."""
    m = re.search(r"\((?:\w+,\s+)?(\w+ \d+)\)", subject)
    if not m:
        return None
    try:
        dt = datetime.strptime(f"{m.group(1)} {datetime.now().year}", "%b %d %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Compare against our database
# ---------------------------------------------------------------------------

def get_our_signals(conn, filing_date: str) -> dict[str, dict]:
    """Get our trades around a filing date, keyed by ticker.

    Uses a 3-day window because CEO Watcher emails often report trades
    1-2 days after the SEC filing date.
    """
    rows = conn.execute("""
        SELECT
            t.ticker,
            t.trade_type,
            MAX(COALESCE(i.display_name, i.name)) AS insider_name,
            MAX(t.title) AS title,
            SUM(t.value) AS total_value,
            MAX(t.signal_grade) AS signal_grade,
            MAX(t.pit_grade) AS pit_grade,
            MAX(t.pit_blended_score) AS pit_blended_score,
            MAX(t.is_rare_reversal) AS is_rare_reversal,
            MAX(t.is_csuite) AS is_csuite,
            MAX(t.is_10b5_1) AS is_10b5_1,
            MAX(t.cohen_routine) AS cohen_routine,
            COUNT(DISTINCT t.insider_id) AS n_insiders
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.filing_date BETWEEN
              (DATE %s - INTERVAL '3 days')::text
              AND (DATE %s + INTERVAL '1 day')::text
          AND t.trans_code IN ('P', 'S')
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        GROUP BY t.ticker, t.trade_type
        ORDER BY SUM(t.value) DESC
    """, (filing_date, filing_date)).fetchall()

    result = {}
    for r in rows:
        key = f"{r['ticker']}:{r['trade_type']}"
        result[key] = dict(r)
    return result


def compare_signals(cw_trades: list[dict], our_signals: dict[str, dict]) -> dict:
    """Compare CEO Watcher trades against our signals."""
    overlap = []
    their_edge = []
    our_edge = []
    matched_keys = set()

    for cw in cw_trades:
        key = f"{cw['ticker']}:{cw['trade_type']}"
        ours = our_signals.get(key)

        if ours:
            matched_keys.add(key)
            overlap.append({
                "ticker": cw["ticker"],
                "trade_type": cw["trade_type"],
                "cw_value": cw["value"],
                "cw_name": cw["name"],
                "cw_context": cw["context"],
                "our_value": ours["total_value"],
                "our_grade": ours["signal_grade"],
                "our_pit_grade": ours["pit_grade"],
                "our_score": ours["pit_blended_score"],
                "our_n_insiders": ours["n_insiders"],
                "our_is_rare_reversal": bool(ours["is_rare_reversal"]),
                "our_is_routine": bool(ours["cohen_routine"]),
            })
        else:
            their_edge.append({
                "ticker": cw["ticker"],
                "trade_type": cw["trade_type"],
                "cw_value": cw["value"],
                "cw_name": cw["name"],
                "cw_title": cw["title"],
                "cw_context": cw["context"],
                "reason": "not in our database for this filing date",
            })

    # Our signals they missed (top by value, exclude matched)
    for key, ours in our_signals.items():
        if key not in matched_keys and ours["total_value"] and ours["total_value"] >= 50_000:
            grade = ours.get("signal_grade") or ""
            our_edge.append({
                "ticker": ours["ticker"],
                "trade_type": ours["trade_type"],
                "our_value": ours["total_value"],
                "our_name": ours["insider_name"],
                "our_grade": grade,
                "our_pit_grade": ours["pit_grade"],
                "our_score": ours["pit_blended_score"],
                "our_is_rare_reversal": bool(ours["is_rare_reversal"]),
                "our_n_insiders": ours["n_insiders"],
            })

    # Sort our edge by value descending
    our_edge.sort(key=lambda x: -(x.get("our_value") or 0))

    return {
        "overlap": overlap,
        "their_edge": their_edge,
        "our_edge": our_edge[:20],  # Top 20 by value
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_comparison(filing_date: str, cw_trades: list[dict], comparison: dict):
    """Print a human-readable comparison report."""
    n_cw = len(cw_trades)
    n_overlap = len(comparison["overlap"])
    n_their = len(comparison["their_edge"])
    n_ours = len(comparison["our_edge"])

    print(f"\n{'='*70}")
    print(f"  CEO WATCHER vs FORM4 — {filing_date}")
    print(f"{'='*70}")
    print(f"  CW flagged: {n_cw} trades")
    print(f"  Overlap:    {n_overlap} ({n_overlap/n_cw*100:.0f}% of theirs)" if n_cw else "")
    print(f"  Their edge: {n_their} (they flagged, we didn't)")
    print(f"  Our edge:   {n_ours} (we have, they missed)")
    print()

    if comparison["overlap"]:
        print("  OVERLAP (both flagged):")
        for t in comparison["overlap"]:
            grade = t["our_grade"] or t["our_pit_grade"] or "—"
            routine = " [routine]" if t["our_is_routine"] else ""
            reversal = " [RARE REVERSAL]" if t["our_is_rare_reversal"] else ""
            print(f"    {t['ticker']:6s} {t['trade_type']:4s}  CW: ${t['cw_value']:>12,.0f}  Ours: ${t['our_value']:>12,.0f}  Grade: {grade}{routine}{reversal}")
        print()

    if comparison["their_edge"]:
        print("  THEIR EDGE (CW flagged, we didn't):")
        for t in comparison["their_edge"][:10]:
            ctx = ", ".join(f"{k}={v}" for k, v in t["cw_context"].items()) if t["cw_context"] else ""
            print(f"    {t['ticker']:6s} {t['trade_type']:4s}  ${t['cw_value']:>12,.0f}  {t['cw_name'][:30]}  {ctx}")
        print()

    if comparison["our_edge"]:
        print("  OUR EDGE (we have, CW missed):")
        for t in comparison["our_edge"][:10]:
            grade = t["our_grade"] or t["our_pit_grade"] or "—"
            reversal = " [RARE REVERSAL]" if t["our_is_rare_reversal"] else ""
            print(f"    {t['ticker']:6s} {t['trade_type']:4s}  ${t['our_value']:>12,.0f}  Grade: {grade}  {t['our_name'][:30]}{reversal}")
        print()


def save_comparison(filing_date: str, cw_trades: list[dict], comparison: dict):
    """Save comparison results to JSON."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{filing_date}_comparison.json"
    data = {
        "filing_date": filing_date,
        "cw_trade_count": len(cw_trades),
        "cw_trades": cw_trades,
        **comparison,
        "stats": {
            "overlap": len(comparison["overlap"]),
            "their_edge": len(comparison["their_edge"]),
            "our_edge": len(comparison["our_edge"]),
            "overlap_pct": len(comparison["overlap"]) / len(cw_trades) * 100 if cw_trades else 0,
        },
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("Saved comparison: %s", path)


def print_summary(comparisons: list[dict]):
    """Print aggregate stats across all comparisons."""
    if not comparisons:
        print("No comparisons to summarize.")
        return

    total_cw = sum(c["stats"]["cw_trade_count"] for c in comparisons)
    total_overlap = sum(c["stats"]["overlap"] for c in comparisons)
    total_their = sum(c["stats"]["their_edge"] for c in comparisons)
    total_ours = sum(c["stats"]["our_edge"] for c in comparisons)

    print(f"\n{'='*70}")
    print(f"  AGGREGATE SUMMARY — {len(comparisons)} emails")
    print(f"{'='*70}")
    print(f"  CW total trades flagged:  {total_cw}")
    print(f"  Overlap:                  {total_overlap} ({total_overlap/total_cw*100:.0f}%)" if total_cw else "")
    print(f"  Their edge:               {total_their}")
    print(f"  Our edge (>$50K):         {total_ours}")
    print(f"  Avg overlap per email:    {total_overlap/len(comparisons):.1f}")
    print()

    # Most common tickers in their edge (stuff they catch that we don't)
    from collections import Counter
    their_tickers = Counter()
    for c in comparisons:
        for t in c.get("their_edge", []):
            their_tickers[t["ticker"]] += 1
    if their_tickers:
        print("  Most common tickers in THEIR EDGE:")
        for ticker, count in their_tickers.most_common(10):
            print(f"    {ticker}: {count}x")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compare CEO Watcher signals against Form4")
    parser.add_argument("--all", action="store_true", help="Process all emails")
    parser.add_argument("--date", help="Compare specific date (YYYY-MM-DD)")
    parser.add_argument("--summary", action="store_true", help="Show aggregate summary")
    args = parser.parse_args()

    conn = get_connection(readonly=True)

    if args.date:
        # Compare against our DB for a specific date
        our = get_our_signals(conn, args.date)
        # Check if we have a saved CW file for this date
        cw_path = OUTPUT_DIR / f"{args.date}_parsed.json"
        if cw_path.exists():
            with open(cw_path) as f:
                parsed = json.load(f)
            cw_trades = parse_email_trades(parsed.get("raw_text", ""))
        else:
            print(f"No saved CW email for {args.date}. Fetching from IMAP...")
            imap = connect()
            emails = fetch_emails(imap, all_emails=True)
            imap.logout()
            cw_trades = []
            for em in emails:
                d = extract_date_from_subject(em["subject"])
                if d == args.date:
                    cw_trades = parse_email_trades(em["body"])
                    break
            if not cw_trades:
                print(f"No CW email found for {args.date}")
                return

        comparison = compare_signals(cw_trades, our)
        print_comparison(args.date, cw_trades, comparison)
        save_comparison(args.date, cw_trades, comparison)
        conn.close()
        return

    # Fetch emails and compare each
    from pipelines.ceowatcher_reader import save_email, parse_ceowatcher_email

    imap = connect()
    logger.info("Connected to IMAP")
    emails = fetch_emails(imap, all_emails=args.all)
    imap.logout()

    if not emails:
        print("No new CEO Watcher emails.")
        return

    all_comparisons = []

    for em in emails:
        # Save raw email
        raw_parsed = parse_ceowatcher_email(em["body"])
        date_str = extract_date_from_subject(em["subject"])
        if not date_str:
            logger.warning("Could not extract date from subject: %s", em["subject"])
            continue

        save_email(em, raw_parsed)

        # Parse structured trades
        cw_trades = parse_email_trades(em["body"])
        if not cw_trades:
            logger.warning("No trades parsed from: %s", em["subject"])
            continue

        logger.info("Parsed %d trades from %s", len(cw_trades), em["subject"])

        # Compare against our signals
        our = get_our_signals(conn, date_str)
        comparison = compare_signals(cw_trades, our)
        print_comparison(date_str, cw_trades, comparison)
        save_comparison(date_str, cw_trades, comparison)

        all_comparisons.append({
            "filing_date": date_str,
            "stats": {
                "cw_trade_count": len(cw_trades),
                "overlap": len(comparison["overlap"]),
                "their_edge": len(comparison["their_edge"]),
                "our_edge": len(comparison["our_edge"]),
            },
            "their_edge": comparison["their_edge"],
        })

    conn.close()

    if args.summary or len(all_comparisons) > 1:
        print_summary(all_comparisons)


if __name__ == "__main__":
    main()
