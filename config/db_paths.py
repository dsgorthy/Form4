"""Central database path configuration.

insiders.db was split on 2026-03-27 to fix SIGBUS crashes caused by
concurrent access to an 11GB file.

  insiders.db  — core tables: trades, insiders, trade_returns, trade_signals, etc. (~2GB)
  prices.db    — daily_prices, option_prices, option_pull_status (~5.6GB)
  research.db  — filing_footnotes, derivative_trades, nonderiv_holdings (~2.2GB)

Usage:
    from config.db_paths import INSIDERS_DB, PRICES_DB, RESEARCH_DB
"""
from __future__ import annotations

import os
from pathlib import Path

_CATALOG_DIR = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog"

INSIDERS_DB = Path(os.getenv("INSIDERS_DB_PATH", str(_CATALOG_DIR / "insiders.db")))
PRICES_DB = Path(os.getenv("PRICES_DB_PATH", str(_CATALOG_DIR / "prices.db")))
RESEARCH_DB = Path(os.getenv("RESEARCH_DB_PATH", str(_CATALOG_DIR / "research.db")))


def connect_ro(attach_prices: bool = False, attach_research: bool = False):
    """Open a read-only connection to insiders.db with optional ATTACHes.

    ATTACH makes tables from prices.db/research.db queryable as if they
    were in insiders.db — no SQL changes needed in calling code.
    """
    import sqlite3
    conn = sqlite3.connect(f"file:{INSIDERS_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if attach_prices and PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    if attach_research and RESEARCH_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{RESEARCH_DB}?mode=ro' AS research")
    return conn
