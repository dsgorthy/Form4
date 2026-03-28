#!/usr/bin/env python3
"""
CEO Watcher Email Reader — Competitive Intelligence
====================================================

Reads daily CEO Watcher emails via IMAP, parses signals and context,
saves to data/ceowatcher/ for comparison against our Form4 signals.

Usage:
    python3 pipelines/ceowatcher_reader.py              # Fetch latest unread
    python3 pipelines/ceowatcher_reader.py --all         # Fetch all
    python3 pipelines/ceowatcher_reader.py --compare     # Fetch + compare against our data
"""

from __future__ import annotations

import argparse
import email
import imaplib
import json
import logging
import os
import re
from datetime import datetime
from email.header import decode_header
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

IMAP_SERVER = "imap.gmail.com"
IMAP_EMAIL = os.environ.get("IMAP_EMAIL", "")
IMAP_PASSWORD = os.environ.get("IMAP_APP_PASSWORD", "")
ALIAS_FILTER = os.environ.get("IMAP_ALIAS_FILTER", "ceo_watcher")
OUTPUT_DIR = ROOT_DIR / "data" / "ceowatcher"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def connect() -> imaplib.IMAP4_SSL:
    imap = imaplib.IMAP4_SSL(IMAP_SERVER)
    imap.login(IMAP_EMAIL, IMAP_PASSWORD)
    return imap


def fetch_emails(imap: imaplib.IMAP4_SSL, all_emails: bool = False) -> list[dict]:
    """Fetch CEO Watcher emails from inbox."""
    imap.select("INBOX")

    # Search for emails to the +alias address
    search_query = f'(TO "{IMAP_EMAIL.replace("@", f"+{ALIAS_FILTER}@")}")'
    if not all_emails:
        search_query = f'(UNSEEN TO "{IMAP_EMAIL.replace("@", f"+{ALIAS_FILTER}@")}")'

    status, msg_ids = imap.search(None, search_query)
    if not msg_ids[0]:
        logger.info("No new emails found")
        return []

    ids = msg_ids[0].split()
    logger.info("Found %d emails", len(ids))

    emails = []
    for msg_id in ids:
        status, data = imap.fetch(msg_id, "(RFC822)")
        if status != "OK":
            continue

        raw = data[0][1]
        msg = email.message_from_bytes(raw)

        subject = ""
        for part, encoding in decode_header(msg["Subject"] or ""):
            if isinstance(part, bytes):
                subject += part.decode(encoding or "utf-8", errors="replace")
            else:
                subject += part

        date_str = msg["Date"] or ""
        from_addr = msg["From"] or ""

        # Extract body
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                ct = part.get_content_type()
                if ct == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
                        break
                elif ct == "text/html" and not body:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode("utf-8", errors="replace")

        emails.append({
            "subject": subject,
            "date": date_str,
            "from": from_addr,
            "body": body,
            "msg_id": msg_id.decode(),
        })

    return emails


def parse_ceowatcher_email(body: str) -> dict:
    """Parse CEO Watcher email body and extract structured signals.

    Returns dict with:
      - trades: list of {ticker, insider, title, trade_type, value, context_strings}
      - summary_stats: any aggregate stats they report
      - raw_body: the full text for manual review
    """
    trades = []
    summary = {}

    # Strip HTML if present
    if "<html" in body.lower():
        # Basic HTML tag stripping
        text = re.sub(r"<style[^>]*>.*?</style>", "", body, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "\n", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"&#\d+;", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
    else:
        text = body

    # The actual parsing patterns will be refined once we see real emails.
    # For now, capture the raw text and look for common patterns:
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Try to find ticker mentions (all-caps 1-5 letter words)
    ticker_pattern = re.compile(r'\b([A-Z]{1,5})\b')
    # Dollar amounts
    dollar_pattern = re.compile(r'\$[\d,]+(?:\.\d+)?[KMB]?|\$[\d.]+\s*(?:million|billion)', re.IGNORECASE)
    # Percentage patterns
    pct_pattern = re.compile(r'[-+]?\d+(?:\.\d+)?%')

    # Extract context strings (lines that contain insider-like information)
    context_lines = []
    for line in lines:
        if any(kw in line.lower() for kw in [
            "purchase", "sale", "bought", "sold", "increased", "decreased",
            "holdings", "largest", "first time", "last purchase", "last sale",
            "days ago", "trading plan", "10b5", "cluster", "insider",
        ]):
            context_lines.append(line)

    return {
        "trades": trades,
        "context_lines": context_lines,
        "tickers_mentioned": list(set(ticker_pattern.findall(text))),
        "dollar_amounts": dollar_pattern.findall(text),
        "percentages": pct_pattern.findall(text),
        "raw_text": text,
        "n_lines": len(lines),
    }


def save_email(email_data: dict, parsed: dict):
    """Save raw and parsed email to disk."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate filename from date
    try:
        dt = email.utils.parsedate_to_datetime(email_data["date"])
        date_str = dt.strftime("%Y-%m-%d")
    except Exception:
        date_str = datetime.now().strftime("%Y-%m-%d")

    # Save raw
    raw_path = OUTPUT_DIR / f"{date_str}_raw.txt"
    raw_path.write_text(email_data["body"])

    # Save parsed
    parsed_path = OUTPUT_DIR / f"{date_str}_parsed.json"
    parsed["subject"] = email_data["subject"]
    parsed["email_date"] = email_data["date"]
    with open(parsed_path, "w") as f:
        json.dump(parsed, f, indent=2, default=str)

    logger.info("Saved: %s (%d context lines, %d tickers)",
                date_str, len(parsed["context_lines"]), len(parsed["tickers_mentioned"]))
    return date_str


def main():
    parser = argparse.ArgumentParser(description="Read CEO Watcher emails")
    parser.add_argument("--all", action="store_true", help="Fetch all emails (not just unread)")
    parser.add_argument("--compare", action="store_true", help="Compare against our signals")
    args = parser.parse_args()

    if not IMAP_EMAIL or not IMAP_PASSWORD:
        logger.error("IMAP credentials not configured in .env")
        return

    imap = connect()
    logger.info("Connected to IMAP as %s", IMAP_EMAIL)

    emails = fetch_emails(imap, all_emails=args.all)

    dates_saved = []
    for em in emails:
        parsed = parse_ceowatcher_email(em["body"])
        date_str = save_email(em, parsed)
        dates_saved.append(date_str)

        # Print summary
        print(f"\n{'='*60}")
        print(f"  {em['subject']}")
        print(f"  {em['date']}")
        print(f"{'='*60}")
        print(f"  Tickers: {', '.join(parsed['tickers_mentioned'][:20])}")
        print(f"  Context lines: {len(parsed['context_lines'])}")
        if parsed["context_lines"]:
            print("  Sample context:")
            for cl in parsed["context_lines"][:5]:
                print(f"    - {cl[:100]}")

    imap.logout()

    if not emails:
        print("\nNo new CEO Watcher emails. Sign up at ceowatcher.com with:")
        print(f"  {IMAP_EMAIL.replace('@', f'+{ALIAS_FILTER}@')}")

    logger.info("Done. %d emails processed.", len(emails))


if __name__ == "__main__":
    main()
