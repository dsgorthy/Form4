"""Congress stock trading scraper daemon.

Primary source: Capitol Trades (capitoltrades.com)
  - Both Senate and House, ~1 day delay, no signup required
  - Polls every 30 minutes (data updates daily)

Usage:
    python3 pipelines/congress_scraper/run_scraper.py [--once] [--audit] [--backfill N]

Options:
    --once          Run one cycle and exit (useful for testing)
    --audit         Run validation audit and exit
    --backfill N    Scrape N pages for historical data (default: incremental only)
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path for imports
ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.congress_scraper.scrape_capitol_trades import scrape_capitol_trades
from pipelines.congress_scraper.validate import run_full_audit

STATE_FILE = Path(__file__).resolve().parent / "state.json"
LOG_FILE = ROOT / "logs" / "congress_scraper.log"
SCRAPE_INTERVAL = 1800  # 30 minutes — Capitol Trades updates daily, no need for 5 min

# Graceful shutdown
_running = True


def _signal_handler(signum, frame):
    global _running
    _running = False
    logging.getLogger("congress_scraper").info(f"Received signal {signum}, shutting down...")


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def setup_logging() -> None:
    """Configure logging to file and stdout."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger("congress_scraper")
    root_logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on restart
    if root_logger.handlers:
        return

    fh = logging.FileHandler(str(LOG_FILE))
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger.addHandler(fh)
    root_logger.addHandler(ch)


def load_state() -> dict:
    """Load scraper state from disk."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_state(state: dict) -> None:
    """Persist scraper state to disk."""
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


def run_cycle(state: dict, max_pages: int = 10) -> dict:
    """Run one scrape cycle.

    Returns stats dict.
    """
    logger = logging.getLogger("congress_scraper")
    logger.info("--- Capitol Trades scrape starting ---")

    try:
        stats = scrape_capitol_trades(state, max_pages=max_pages)
    except Exception as e:
        logger.error(f"Capitol Trades scrape failed: {e}", exc_info=True)
        stats = {"error": str(e)}

    state["last_run"] = datetime.now().isoformat()
    save_state(state)

    logger.info(
        f"Cycle complete — {stats.get('inserted', 0)} new / "
        f"{stats.get('skipped', 0)} skipped / {stats.get('errors', 0)} errors"
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Congress stock trading scraper")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--audit", action="store_true", help="Run validation audit and exit")
    parser.add_argument("--backfill", type=int, default=0, help="Scrape N pages for historical backfill")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger("congress_scraper")

    if args.audit:
        run_full_audit()
        return

    state = load_state()

    if args.backfill > 0:
        logger.info(f"Backfill mode: scraping {args.backfill} pages")
        # Clear watermark for backfill
        backfill_state: dict = {}
        scrape_capitol_trades(backfill_state, max_pages=args.backfill)
        return

    logger.info(f"Congress scraper starting (interval={SCRAPE_INTERVAL}s)")
    logger.info(f"State file: {STATE_FILE}")

    if args.once:
        run_cycle(state)
        return

    # Daemon loop
    while _running:
        try:
            run_cycle(state)
        except Exception as e:
            logger.error(f"Unhandled error in scrape cycle: {e}", exc_info=True)

        # Sleep in small increments for responsive shutdown
        for _ in range(SCRAPE_INTERVAL):
            if not _running:
                break
            time.sleep(1)

    logger.info("Congress scraper shut down cleanly")


if __name__ == "__main__":
    main()
