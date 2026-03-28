"""
Master orchestration pipeline: research → collect → backtest → board → paper.

Usage:
    python pipelines/run_pipeline.py --strategy spy_0dte_reversal --stage all
    python pipelines/run_pipeline.py --strategy spy_orb --stage backtest
    python pipelines/run_pipeline.py --strategy spy_orb --stage board
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

STAGES = ["scaffold", "backtest", "board", "paper", "all"]

PIPELINES_DIR = Path(__file__).parent


def run_stage(stage: str, strategy: str, extra_args: list = None) -> int:
    """Run a pipeline stage as a subprocess. Returns exit code."""
    script_map = {
        "backtest": PIPELINES_DIR / "run_backtest.py",
        "board": PIPELINES_DIR / "run_board.py",
        "paper": PIPELINES_DIR / "run_paper.py",
    }

    if stage == "scaffold":
        from research.pipeline import scaffold_strategy
        strategy_dir = scaffold_strategy(strategy, strategy.replace("_", " ").title(), ["SPY"])
        logger.info("Scaffold complete: %s", strategy_dir)
        return 0

    script = script_map.get(stage)
    if script is None:
        logger.error("Unknown stage: %s", stage)
        return 1

    cmd = [sys.executable, str(script), "--strategy", strategy]
    if extra_args:
        cmd.extend(extra_args)

    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Master trading framework pipeline")
    parser.add_argument("--strategy", required=True, help="Strategy name")
    parser.add_argument("--stage", choices=STAGES, default="all", help="Which stage to run")
    parser.add_argument("--start", default="2024-01-01", help="Backtest start date")
    parser.add_argument("--end", default="2025-12-31", help="Backtest end date")
    parser.add_argument("--skip-board", action="store_true", help="Skip board evaluation")
    parser.add_argument("--dry-run", action="store_true", help="Dry run paper stage")
    args = parser.parse_args()

    logger.info("=== Pipeline: %s | Stage: %s ===", args.strategy, args.stage)

    if args.stage == "all":
        stages_to_run = ["backtest", "board", "paper"]
        if args.skip_board:
            stages_to_run = ["backtest", "paper"]
    else:
        stages_to_run = [args.stage]

    for stage in stages_to_run:
        logger.info("--- Stage: %s ---", stage.upper())
        extra = []
        if stage == "backtest":
            extra = ["--start", args.start, "--end", args.end, "--save-json"]
        elif stage == "paper" and args.dry_run:
            extra = ["--dry-run"]

        rc = run_stage(stage, args.strategy, extra)
        if rc != 0:
            if stage == "board":
                logger.warning("Board returned non-zero (strategy not approved for paper)")
                if args.stage == "all":
                    logger.info("Stopping pipeline after board rejection.")
                    sys.exit(rc)
            else:
                logger.error("Stage %s failed with exit code %d", stage, rc)
                sys.exit(rc)

    logger.info("Pipeline complete for %s", args.strategy)


if __name__ == "__main__":
    main()
