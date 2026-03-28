"""
Standalone backtest pipeline.

Usage:
    python pipelines/run_backtest.py --strategy spy_0dte_reversal
    python pipelines/run_backtest.py --strategy spy_0dte_reversal --start 2024-01-01 --end 2025-12-31
"""

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.backtest.engine import BacktestEngine
from framework.data.storage import DataStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Default date ranges if not specified
DEFAULT_START = "2024-01-01"
DEFAULT_END = "2025-12-31"


def load_strategy(strategy_name: str, config: dict):
    """Dynamically load a strategy class from strategies/{name}/strategy.py."""
    module_path = f"strategies.{strategy_name}.strategy"
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        logger.error("Cannot import strategy module %s: %s", module_path, exc)
        sys.exit(1)

    # Find the strategy class (look for class ending in "Strategy")
    strategy_class = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (
            isinstance(attr, type)
            and attr_name != "BaseStrategy"
            and hasattr(attr, "generate_signal")
        ):
            strategy_class = attr
            break

    if strategy_class is None:
        logger.error("No strategy class found in %s", module_path)
        sys.exit(1)

    logger.info("Loaded strategy: %s", strategy_class.__name__)
    return strategy_class(config)


def load_config(strategy_name: str, config_file: str = "config.yaml") -> dict:
    """Load config YAML for a strategy. Defaults to config.yaml."""
    config_path = Path(__file__).parent.parent / "strategies" / strategy_name / config_file
    if not config_path.exists():
        logger.error("No config found at %s", config_path)
        sys.exit(1)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Run strategy backtest")
    parser.add_argument("--strategy", required=True, help="Strategy directory name")
    parser.add_argument("--config", default="config.yaml", help="Config filename within strategy dir")
    parser.add_argument("--start", default=DEFAULT_START, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=DEFAULT_END, help="End date YYYY-MM-DD")
    parser.add_argument("--capital", type=float, default=30_000.0, help="Starting capital")
    parser.add_argument("--position-pct", type=float, default=3.0, help="Position size %")
    parser.add_argument("--data-dir", type=Path, help="Override data directory")
    parser.add_argument("--save-json", action="store_true", help="Save backtest results to JSON")
    parser.add_argument("--spy-data", type=Path, help="Path to existing spy-0dte data dir (raw/)")
    parser.add_argument("--options-dir", type=Path, help="Path to options parquet dir")
    parser.add_argument("--commission", type=float, default=0.65, help="Commission per contract/share (each leg)")
    parser.add_argument("--slippage", type=float, default=0.01, help="Slippage as fraction of premium (e.g. 0.01 = 1%%)")
    parser.add_argument("--no-fees", action="store_true", help="Disable fee modeling (gross P&L)")
    args = parser.parse_args()

    commission = 0.0 if args.no_fees else args.commission
    slippage = 0.0 if args.no_fees else args.slippage
    logger.info("=== Backtest: %s ===", args.strategy)
    logger.info("Period: %s to %s | Capital: $%.0f | Size: %.1f%%",
                args.start, args.end, args.capital, args.position_pct)
    if args.no_fees:
        logger.info("Fees: DISABLED (gross P&L mode)")
    else:
        logger.info("Fees: commission=$%.2f/contract, slippage=%.1f%% of premium", commission, slippage * 100)

    # Load config and strategy
    config = load_config(args.strategy, args.config)

    # Allow capital override from CLI
    if "sizing" not in config:
        config["sizing"] = {}
    config["sizing"]["starting_capital"] = args.capital

    strategy = load_strategy(args.strategy, config)

    # Set up storage
    # Resolution order:
    #   1. Explicit --spy-data / --data-dir / --options-dir CLI flags
    #   2. Sibling spy-0dte/ directory (legacy convenience)
    #   3. Framework's own data/ directory
    FRAMEWORK_ROOT = Path(__file__).parent.parent

    def _find_dir(explicit: Path, candidates: list) -> Path:
        if explicit:
            return explicit
        for c in candidates:
            if c.exists():
                logger.info("Auto-detected data at %s", c)
                return c
        return None

    raw_dir = _find_dir(
        args.spy_data or args.data_dir,
        [
            FRAMEWORK_ROOT.parent / "spy-0dte" / "data" / "raw",
            FRAMEWORK_ROOT / "data" / "raw",
        ]
    )
    options_dir = _find_dir(
        args.options_dir,
        [
            FRAMEWORK_ROOT.parent / "spy-0dte" / "data" / "options",
            FRAMEWORK_ROOT / "data" / "options",
        ]
    )

    # Build list of all existing raw data dirs (for multi-source data spanning multiple years)
    all_raw_candidates = [
        FRAMEWORK_ROOT.parent / "spy-0dte" / "data" / "raw",
        FRAMEWORK_ROOT / "data" / "raw",
    ]
    if args.spy_data or args.data_dir:
        primary = args.spy_data or args.data_dir
        extra_raw = [d for d in all_raw_candidates if d.exists() and d != primary]
    else:
        extra_raw = [d for d in all_raw_candidates[1:] if d.exists() and d != raw_dir]

    storage = DataStorage(
        raw_dir=raw_dir,
        options_dir=options_dir,
        extra_raw_dirs=extra_raw if extra_raw else None,
    ) if raw_dir else DataStorage(options_dir=options_dir)

    # Override storage in strategy (not all strategies have these attributes)
    if hasattr(strategy, "_storage"):
        strategy._storage = storage
    if hasattr(strategy, "_feature_engine") and strategy._feature_engine is not None:
        strategy._feature_engine.storage = storage

    # Run backtest
    engine = BacktestEngine(
        strategy=strategy,
        config={
            "starting_capital": args.capital,
            "position_size_pct": args.position_pct,
            "commission_per_contract": commission,
            "slippage_pct": slippage,
        },
        storage=storage,
    )

    result = engine.run(args.start, args.end)
    result.print_summary()

    # Save JSON for board evaluation
    if args.save_json:
        output_dir = Path(__file__).parent.parent / "reports" / args.strategy
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "backtest_latest.json"
        result.save_json(json_path)
        logger.info("Saved backtest JSON to %s", json_path)


if __name__ == "__main__":
    main()
