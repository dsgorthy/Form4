"""Central database path configuration.

Legacy paths kept for scripts that check file existence.
All connections now go through config.database (PostgreSQL).

Usage:
    from config.db_paths import INSIDERS_DB, PRICES_DB, RESEARCH_DB
    from config.db_paths import connect_ro
"""
from __future__ import annotations

import os
from pathlib import Path

_CATALOG_DIR = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog"

# Legacy paths — kept for scripts that check .exists()
INSIDERS_DB = Path(os.getenv("INSIDERS_DB_PATH", str(_CATALOG_DIR / "insiders.db")))
PRICES_DB = Path(os.getenv("PRICES_DB_PATH", str(_CATALOG_DIR / "prices.db")))
RESEARCH_DB = Path(os.getenv("RESEARCH_DB_PATH", str(_CATALOG_DIR / "research.db")))


def connect_ro(attach_prices: bool = False, attach_research: bool = False):
    """Open a read-only PostgreSQL connection.

    attach_prices/attach_research params are ignored — all schemas
    are accessible via search_path in the PG connection.
    """
    from config.database import get_connection
    return get_connection(readonly=True)
