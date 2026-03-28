"""
Research pipeline — scaffolds a new strategy from hypothesis to data collection.

Usage:
    python research/pipeline.py --slug spy_orb --name "SPY Opening Range Breakout"
    python research/pipeline.py --slug my_strat --name "My Strategy" --symbols SPY,QQQ
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
STRATEGIES_DIR = PROJECT_ROOT / "strategies"
TEMPLATES_DIR = Path(__file__).parent / "templates"


def scaffold_strategy(slug: str, name: str, symbols: list, author: str = "unknown") -> Path:
    """
    Create a new strategy directory with boilerplate files.

    Parameters
    ----------
    slug : str
        Snake-case strategy identifier (e.g. "spy_orb").
    name : str
        Human-readable name (e.g. "SPY Opening Range Breakout").
    symbols : list
        List of symbols required.
    author : str
        Strategy author.

    Returns
    -------
    Path
        The new strategy directory.
    """
    strategy_dir = STRATEGIES_DIR / slug
    if strategy_dir.exists():
        logger.warning("Strategy directory already exists: %s", strategy_dir)
        return strategy_dir

    strategy_dir.mkdir(parents=True, exist_ok=True)

    # Write __init__.py
    (strategy_dir / "__init__.py").write_text("")

    # Write strategy_spec.md from template
    spec_template = TEMPLATES_DIR / "strategy_spec.md"
    if spec_template.exists():
        spec_text = spec_template.read_text(encoding="utf-8")
        spec_text = spec_text.replace("[strategy_slug]", slug)
        spec_text = spec_text.replace("[YYYY-MM-DD]", datetime.now().strftime("%Y-%m-%d"))
        spec_text = spec_text.replace("[author]", author)
        (strategy_dir / "strategy_spec.md").write_text(spec_text, encoding="utf-8")

    # Write minimal config.yaml
    config = {
        "strategy": {
            "name": slug,
            "description": name,
            "asset_type": "options",
            "version": "0.1.0",
        },
        "data": {
            "symbols": symbols,
            "timeframes": ["1Min", "5Min"],
            "lookback_days": 30,
            "requires_options": True,
        },
        "entry": {"time": "TBD"},
        "exit": {"time_stop": "TBD"},
        "sizing": {"starting_capital": 30000.0, "position_size_pct": 3.0},
        "filters": {},
    }
    with open(strategy_dir / "config.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    # Write strategy.py stub
    stub = f'''"""
{name} Strategy

TODO: Implement the strategy logic by subclassing BaseStrategy.
"""

from __future__ import annotations
from typing import Dict, Optional

import pandas as pd

from framework.strategy import BaseStrategy, Signal, DataRequirements


class {_to_class_name(slug)}(BaseStrategy):
    """
    {name}

    TODO: Fill in strategy logic.
    """

    def strategy_name(self) -> str:
        return "{slug}"

    def data_requirements(self) -> DataRequirements:
        data_cfg = self.config.get("data", {{}})
        return DataRequirements(
            symbols=data_cfg.get("symbols", {symbols}),
            timeframes=data_cfg.get("timeframes", ["1Min", "5Min"]),
            lookback_days=data_cfg.get("lookback_days", 30),
            requires_options=data_cfg.get("requires_options", True),
        )

    def generate_signal(self, bars: Dict[str, pd.DataFrame], date: str) -> Signal:
        # TODO: Implement signal logic
        return Signal(direction=None, confidence=0.0, instrument=None)

    def select_instrument(self, signal: Signal, bars: Dict[str, pd.DataFrame], date: str) -> dict:
        # TODO: Implement instrument selection
        return {{}}

    def should_exit(self, position: dict, bars: Dict[str, pd.DataFrame]) -> Optional[str]:
        # TODO: Implement exit logic
        return None
'''
    (strategy_dir / "strategy.py").write_text(stub, encoding="utf-8")
    (strategy_dir / "features.py").write_text("# Feature engineering for this strategy\n")

    logger.info("Scaffolded strategy at %s", strategy_dir)
    return strategy_dir


def _to_class_name(slug: str) -> str:
    """Convert snake_case to PascalCase + 'Strategy'."""
    return "".join(word.title() for word in slug.split("_")) + "Strategy"


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Scaffold a new trading strategy")
    parser.add_argument("--slug", required=True, help="Snake-case strategy identifier")
    parser.add_argument("--name", required=True, help="Human-readable strategy name")
    parser.add_argument("--symbols", default="SPY,VIXY", help="Comma-separated symbols")
    parser.add_argument("--author", default="unknown", help="Author name")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",")]
    strategy_dir = scaffold_strategy(args.slug, args.name, symbols, args.author)
    print(f"Strategy scaffolded at: {strategy_dir}")
    print(f"Next steps:")
    print(f"  1. Fill in {strategy_dir}/strategy_spec.md with your hypothesis")
    print(f"  2. Update {strategy_dir}/config.yaml with parameters")
    print(f"  3. Implement {strategy_dir}/strategy.py")
    print(f"  4. Run backtest: python pipelines/run_backtest.py --strategy {args.slug}")
    print(f"  5. Run board: python pipelines/run_board.py --strategy {args.slug}")


if __name__ == "__main__":
    main()
