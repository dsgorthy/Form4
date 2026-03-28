"""
Master runner for the insider event study pipeline.

Steps:
1. Check for input CSV (OpenInsider export or EDGAR downloader output)
2. Build event calendar: group trades into events, compute confidence scores
3. Download price data via Alpaca Data API (collect_prices.py)
4. Run event study with sweep of holding periods and confidence analysis

Usage:
    # OpenInsider (manual download):
    python run_all.py --start 2020-01-01 --end 2025-12-31

    # EDGAR (automated, use download_edgar_data.py first):
    python run_all.py \\
        --format edgar \\
        --input data/edgar_form4_2024.csv \\
        --exclude-dates 2020-01-01:2021-12-31

Data acquisition:
    Option A (OpenInsider, manual):
        1. Go to http://openinsider.com/
        2. Set filters: Trade type P, Min value $25K, date range
        3. Export CSV → save as data/openinsider_raw.csv

    Option B (EDGAR, automated):
        python download_edgar_data.py \\
            --start 2024-Q1 --end 2024-Q4 \\
            --max-per-quarter 5000 --shuffle \\
            --output data/edgar_form4_2024.csv
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

STUDY_DIR = Path(__file__).parent
DATA_DIR = STUDY_DIR / "data"


def check_dependencies():
    """Check required Python packages."""
    missing = []
    for pkg in ["pandas", "numpy", "requests"]:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        logger.error(
            "Missing packages: %s. Install: pip install %s", missing, " ".join(missing)
        )
        sys.exit(1)

    # Inform about optional scipy
    try:
        import scipy  # noqa: F401
    except ImportError:
        logger.info(
            "scipy not installed — correlation p-values will be skipped. "
            "Install with: pip install scipy"
        )


def run_step(script: Path, args: list, description: str):
    """Run a pipeline step as a subprocess, exit on failure."""
    cmd = [sys.executable, str(script)] + args
    logger.info("=== %s ===", description)
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("Step failed: %s (exit code %d)", description, result.returncode)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(
        description="Run full insider event study pipeline"
    )
    parser.add_argument("--start", default="2020-01-01", help="Start date for price data")
    parser.add_argument("--end",   default="2025-12-31", help="End date for price data")
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_DIR / "openinsider_raw.csv",
        help="OpenInsider CSV export path",
    )
    parser.add_argument(
        "--no-market-cap",
        action="store_true",
        default=True,
        help="Skip market cap filter (much faster; default True)",
    )
    parser.add_argument(
        "--min-value",
        type=float,
        default=50_000,
        help="Minimum individual trade value to include ($)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum event confidence score to include in study (0-100)",
    )
    parser.add_argument(
        "--csuite-only",
        action="store_true",
        help="Filter to C-suite/senior officers only (exclude 10%% owners and board directors)",
    )
    parser.add_argument(
        "--format",
        default="openinsider",
        choices=["openinsider", "edgar"],
        help="Input CSV format: 'openinsider' (default) or 'edgar' (download_edgar_data.py output)",
    )
    parser.add_argument(
        "--exclude-dates",
        default="",
        metavar="START:END[,START:END,...]",
        help=(
            "Exclude events whose filing_date falls within these ranges. "
            "Comma-separated list of YYYY-MM-DD:YYYY-MM-DD pairs. "
            "Example: --exclude-dates 2020-01-01:2021-12-31"
        ),
    )
    args = parser.parse_args()

    # Default input path depends on format when not explicitly provided
    if args.input == DATA_DIR / "openinsider_raw.csv" and args.format == "edgar":
        args.input = DATA_DIR / "edgar_form4_2024.csv"

    check_dependencies()

    # Check for input data
    if not args.input.exists():
        if args.format == "edgar":
            print(f"""
+--------------------------------------------------------------+
|  DATA ACQUISITION REQUIRED                                   |
+--------------------------------------------------------------+
|                                                              |
|  Run the EDGAR downloader first:                             |
|    python download_edgar_data.py \\                          |
|        --start 2024-Q1 --end 2024-Q4 \\                     |
|        --max-per-quarter 5000 --shuffle                      |
|                                                              |
|  File path expected:                                         |
|     {str(args.input)[:60]:<60}|
|                                                              |
+--------------------------------------------------------------+
            """)
        else:
            print(f"""
+--------------------------------------------------------------+
|  DATA ACQUISITION REQUIRED                                   |
+--------------------------------------------------------------+
|                                                              |
|  1. Go to http://openinsider.com/                            |
|                                                              |
|  2. Set these filters:                                       |
|     - Insider trade type: P (open-market purchase)           |
|     - Min value: $25,000                                     |
|     - Date range: {args.start} to {args.end:<20}|
|                                                              |
|  3. Click "Export" -> save as CSV                            |
|     File path expected:                                      |
|     {str(args.input)[:60]:<60}|
|                                                              |
|  Alternative (automated, paid):                              |
|     - sec-api.io    : $49/month for Form 4 API               |
|     - Quiver Quant  : $30/month                              |
|                                                              |
+--------------------------------------------------------------+
            """)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        sys.exit(0)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    events_path = DATA_DIR / "events.csv"
    prices_dir = DATA_DIR / "prices"

    # Step 1: Build event calendar (all substantial buys, scored)
    # Note: EDGAR downloader output uses the same column layout as OpenInsider,
    # so both formats use "--format openinsider" here.
    step1_args = [
        "--input",  str(args.input),
        "--format", "openinsider",
        "--output", str(events_path),
        "--min-value", str(int(args.min_value)),
    ]
    if args.no_market_cap:
        step1_args.append("--no-market-cap")
    if args.csuite_only:
        step1_args.append("--csuite-only")

    # Note: --cluster-only is intentionally NOT passed here.
    # All events are included and scored. Filtering by confidence is done
    # at the study level, or by using --cluster-only in run_event_study.py.
    run_step(STUDY_DIR / "build_event_calendar.py", step1_args, "Build event calendar")

    # Step 2: Download price data for all tickers in the event calendar
    run_step(
        STUDY_DIR / "collect_prices.py",
        [
            "--events",     str(events_path),
            "--output-dir", str(prices_dir),
            "--start",      args.start,
            "--end",        args.end,
        ],
        "Download price data",
    )

    # Step 3: Run event study — sweep 7, 21, 63 day holding periods
    step3_args = [
        "--events",     str(events_path),
        "--prices-dir", str(prices_dir),
        "--sweep",
    ]
    if args.min_confidence > 0:
        step3_args += ["--min-confidence", str(args.min_confidence)]
    if args.exclude_dates:
        step3_args += ["--exclude-dates", args.exclude_dates]

    run_step(
        STUDY_DIR / "run_event_study.py",
        step3_args,
        "Run event study (sweep 7/21/63 day holding periods)",
    )

    logger.info("Pipeline complete. Results in %s", DATA_DIR)
    print(f"\nPipeline complete. Results saved to: {DATA_DIR}")


if __name__ == "__main__":
    main()
