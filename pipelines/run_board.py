"""
Standalone board evaluation pipeline.

Usage:
    python pipelines/run_board.py --strategy spy_0dte_reversal
    python pipelines/run_board.py --strategy spy_0dte_reversal --backtest-file reports/spy_0dte_reversal/backtest_latest.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from board.runner import BoardRunner
from board.report import generate_board_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def load_backtest_summary(strategy_name: str, backtest_file: Path = None) -> dict:
    """Load backtest summary from file or return a placeholder."""
    if backtest_file and backtest_file.exists():
        with open(backtest_file, "r") as f:
            data = json.load(f)
        # Handle both raw summary dict and wrapped format
        if "summary" in data:
            return data["summary"]
        return data

    # Try default location
    default = Path(__file__).parent.parent / "reports" / strategy_name / "backtest_latest.json"
    if default.exists():
        with open(default, "r") as f:
            data = json.load(f)
        if "summary" in data:
            return data["summary"]
        return data

    logger.warning("No backtest file found for %s — using placeholder metrics", strategy_name)
    return {
        "total_trades": 0,
        "win_rate": 0.0,
        "total_pnl": 0.0,
        "sharpe_ratio": 0.0,
        "max_drawdown_pct": 0.0,
        "note": "No backtest data — board will evaluate based on strategy spec only",
    }


def load_strategy_spec(strategy_name: str) -> str:
    """Load strategy spec from config.yaml or strategy_spec.md."""
    strategy_dir = Path(__file__).parent.parent / "strategies" / strategy_name

    # Try spec file first
    spec_file = strategy_dir / "strategy_spec.md"
    if spec_file.exists():
        return spec_file.read_text(encoding="utf-8")

    # Fall back to config.yaml
    config_file = strategy_dir / "config.yaml"
    if config_file.exists():
        return config_file.read_text(encoding="utf-8")

    return f"Strategy: {strategy_name}\nNo specification file found."


def main():
    parser = argparse.ArgumentParser(description="Run Board of Personas evaluation")
    parser.add_argument("--strategy", required=True, help="Strategy name (directory under strategies/)")
    parser.add_argument("--backtest-file", type=Path, help="Path to backtest summary JSON file")
    parser.add_argument("--timeout", type=int, default=120, help="Per-persona timeout in seconds")
    parser.add_argument("--output-dir", type=Path, help="Output directory for report")
    args = parser.parse_args()

    logger.info("=== Board of Personas: %s ===", args.strategy)

    # Load inputs
    backtest_summary = load_backtest_summary(args.strategy, args.backtest_file)
    strategy_spec = load_strategy_spec(args.strategy)

    logger.info("Backtest summary: %d trades, WR=%.1f%%, Sharpe=%.2f",
                backtest_summary.get("total_trades", 0),
                backtest_summary.get("win_rate", 0) * 100,
                backtest_summary.get("sharpe_ratio", 0))

    # Run board
    runner = BoardRunner(strategy_name=args.strategy)
    logger.info("Running 5 persona evaluations in parallel...")
    verdicts = runner.run(
        backtest_summary=backtest_summary,
        strategy_spec=strategy_spec,
        timeout=args.timeout,
    )

    # Generate report
    report_path = generate_board_report(
        strategy_name=args.strategy,
        verdicts=verdicts,
        backtest_summary=backtest_summary,
        output_dir=args.output_dir,
    )

    logger.info("Board report saved to %s", report_path)
    print(f"\nBoard report: {report_path}")

    # Print verdict summary
    from board.report import _determine_recommendation, _count_verdicts
    recommendation, explanation = _determine_recommendation(verdicts)
    approvals, conditionals, rejections, errors = _count_verdicts(verdicts)

    print(f"\n{'='*60}")
    print(f"  BOARD VERDICT: {recommendation.replace('_', ' ').upper()}")
    print(f"  {explanation}")
    print(f"  Approve: {approvals} | Conditional: {conditionals} | Reject: {rejections}")
    print(f"{'='*60}\n")

    # Exit code based on recommendation
    if recommendation == "return_to_research":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
