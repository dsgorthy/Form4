#!/usr/bin/env python3
"""
Convert edgar_bulk_form4_2016_2019_buys.csv to the schema expected by options_pull.py.

Input columns:  Filing Date, Trade Date, Ticker, Company Name, Insider Name, Title, Trade Type, Price, Qty, Owned, DeltaOwn, Value, 1d, 1w, 1m, 6m
Output columns: ticker, filing_date, entry_date, entry_price, is_cluster, total_value, insider_names, company
"""

import csv
import os
import re
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(SCRIPT_DIR, "data", "edgar_bulk_form4_2016_2019_buys.csv")
OUTPUT = os.path.join(SCRIPT_DIR, "data", "buys_2016_2019_converted.csv")


def clean_price(raw: str):
    """'$16.38' -> 16.38"""
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace(",", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_value(raw: str) -> float:
    """'+$323,538' -> 323538.0"""
    if not raw:
        return 0.0
    cleaned = raw.replace("$", "").replace(",", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def convert():
    skipped = 0
    written = 0

    with open(INPUT) as fin, open(OUTPUT, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=[
            "ticker", "filing_date", "entry_date", "entry_price",
            "is_cluster", "total_value", "insider_names", "company",
        ])
        writer.writeheader()

        for row in reader:
            price = clean_price(row.get("Price", ""))
            if price is None or price <= 0:
                skipped += 1
                continue

            trade_date = row.get("Trade Date", "").strip()
            if not trade_date:
                skipped += 1
                continue

            # Normalize date format to YYYY-MM-DD
            try:
                dt = datetime.strptime(trade_date, "%Y-%m-%d")
            except ValueError:
                try:
                    dt = datetime.strptime(trade_date, "%m/%d/%Y")
                except ValueError:
                    skipped += 1
                    continue

            ticker = row.get("Ticker", "").strip()
            if not ticker:
                skipped += 1
                continue

            writer.writerow({
                "ticker": ticker,
                "filing_date": row.get("Filing Date", "").strip(),
                "entry_date": dt.strftime("%Y-%m-%d"),
                "entry_price": f"{price:.2f}",
                "is_cluster": "False",  # no cluster info — use --all flag
                "total_value": clean_value(row.get("Value", "")),
                "insider_names": row.get("Insider Name", "").strip(),
                "company": row.get("Company Name", "").strip(),
            })
            written += 1

    print(f"Converted: {written} events written to {OUTPUT}")
    print(f"Skipped: {skipped} (bad price or date)")


if __name__ == "__main__":
    convert()
