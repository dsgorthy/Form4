"""
Full strategy development pipeline: ideate → research → draft → test → revise → test → deploy

Usage:
    # Run full pipeline
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --stage all

    # Run individual stages
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --stage ideate
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --stage backtest
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --stage board
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --stage deploy

    # Resume from a stage (skips completed stages)
    python3 pipelines/research_pipeline.py --strategy spy_noon_break --from-stage revise

Stages:
    ideate    — Scaffold strategy directory, create spec template
    research  — (Manual step) Fill in research_notes.md with hypothesis evidence
    draft     — Verify strategy.py and config.yaml are implemented
    backtest  — Run initial backtest, save results
    board     — Run Board of Personas evaluation
    revise    — Print board recommendations; bump config version for next iteration
    backtest2 — Run revised backtest
    deploy    — Launch paper trading daemon

Artifacts produced per stage:
    ideate    → strategies/{slug}/strategy_spec.md, config.yaml, strategy.py stub
    research  → strategies/{slug}/research_notes.md  (human/AI authored)
    draft     → strategies/{slug}/strategy.py (implemented)
    backtest  → reports/{slug}/backtest_latest.json
    board     → reports/{slug}/board_report_{date}.md
    revise    → reports/{slug}/revision_notes_{date}.md
    backtest2 → reports/{slug}/backtest_revised.json
    deploy    → paper trading daemon launched
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

PROJECT_ROOT = Path(__file__).parent.parent
STRATEGIES_DIR = PROJECT_ROOT / "strategies"
REPORTS_DIR = PROJECT_ROOT / "reports"
RESEARCH_DIR = PROJECT_ROOT / "research"

STAGES = ["ideate", "research", "draft", "backtest", "board", "revise", "backtest2", "deploy"]

STAGE_DESCRIPTIONS = {
    "ideate":    "Scaffold strategy directory and spec template",
    "research":  "Document hypothesis evidence in research_notes.md",
    "draft":     "Verify strategy implementation is complete",
    "backtest":  "Run initial backtest",
    "board":     "Run Board of Personas evaluation",
    "revise":    "Review board feedback and update config",
    "backtest2": "Run revised backtest with updated parameters",
    "deploy":    "Launch paper trading daemon",
}


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------

def stage_ideate(slug: str, name: str = None, symbols: str = "SPY,VIXY") -> bool:
    """Scaffold strategy directory via research/pipeline.py."""
    strategy_dir = STRATEGIES_DIR / slug
    if strategy_dir.exists() and (strategy_dir / "strategy.py").stat().st_size > 200:
        logger.info("[ideate] Strategy directory already exists and has implementation: %s", slug)
        return True

    name = name or slug.replace("_", " ").title()
    cmd = [
        sys.executable, str(RESEARCH_DIR / "pipeline.py"),
        "--slug", slug,
        "--name", name,
        "--symbols", symbols,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error("[ideate] Scaffolding failed:\n%s", result.stderr)
        return False
    logger.info("[ideate] %s", result.stdout.strip())
    return True


def stage_research(slug: str) -> bool:
    """Check that research_notes.md exists and is non-empty."""
    notes_path = STRATEGIES_DIR / slug / "research_notes.md"
    if not notes_path.exists() or notes_path.stat().st_size < 50:
        logger.warning(
            "[research] research_notes.md missing or empty at %s.\n"
            "  Create it with your hypothesis evidence, then re-run with --from-stage draft",
            notes_path,
        )
        # Create a template if it doesn't exist
        if not notes_path.exists():
            notes_path.write_text(
                f"# Research Notes — {slug}\n\n"
                "## Hypothesis\n\nTODO\n\n"
                "## Academic / Practitioner Evidence\n\nTODO\n\n"
                "## Key Parameters\n\nTODO\n\n"
                "## Risks / Failure Modes\n\nTODO\n",
                encoding="utf-8",
            )
            logger.info("[research] Created template at %s", notes_path)
        return False
    logger.info("[research] research_notes.md found (%d bytes)", notes_path.stat().st_size)
    return True


def stage_draft(slug: str) -> bool:
    """Verify strategy.py has a real implementation (not a stub)."""
    strategy_py = STRATEGIES_DIR / slug / "strategy.py"
    if not strategy_py.exists():
        logger.error("[draft] strategy.py not found at %s", strategy_py)
        return False
    text = strategy_py.read_text()
    if "TODO: Implement" in text:
        logger.warning("[draft] strategy.py still has TODO stubs — implement before proceeding")
        return False
    logger.info("[draft] strategy.py looks implemented (%d chars)", len(text))
    return True


def stage_backtest(
    slug: str,
    start: str = "2024-01-01",
    end: str = "2025-12-31",
    commission: float = None,
    slippage: float = None,
    no_fees: bool = False,
    output_key: str = "backtest_latest",
) -> bool:
    """Run backtest via run_backtest.py."""
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipelines" / "run_backtest.py"),
        "--strategy", slug,
        "--start", start,
        "--end", end,
        "--save-json",
    ]
    if commission is not None:
        cmd += ["--commission", str(commission)]
    if slippage is not None:
        cmd += ["--slippage", str(slippage)]
    if no_fees:
        cmd.append("--no-fees")

    logger.info("[backtest] Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        logger.error("[backtest] Backtest failed with code %d", result.returncode)
        return False

    json_path = REPORTS_DIR / slug / "backtest_latest.json"
    if output_key != "backtest_latest" and json_path.exists():
        dest = REPORTS_DIR / slug / f"{output_key}.json"
        import shutil
        shutil.copy(json_path, dest)
        logger.info("[backtest] Saved to %s", dest)

    logger.info("[backtest] Completed")
    return True


def stage_board(slug: str) -> bool:
    """Run Board of Personas via run_board.py."""
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipelines" / "run_board.py"),
        "--strategy", slug,
    ]
    logger.info("[board] Running Board of Personas for %s", slug)
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        logger.error("[board] Board run failed with code %d", result.returncode)
        return False
    logger.info("[board] Board evaluation complete")
    return True


def stage_revise(slug: str) -> bool:
    """Print latest board report and create revision notes template."""
    reports_dir = REPORTS_DIR / slug
    board_reports = sorted(reports_dir.glob("board_report_*.md"), reverse=True)
    if not board_reports:
        logger.warning("[revise] No board report found — run board stage first")
        return False

    latest = board_reports[0]
    print("\n" + "=" * 70)
    print(latest.read_text(encoding="utf-8"))
    print("=" * 70 + "\n")

    date_str = datetime.now().strftime("%Y-%m-%d")
    notes_path = reports_dir / f"revision_notes_{date_str}.md"
    if not notes_path.exists():
        notes_path.write_text(
            f"# Revision Notes — {slug} — {date_str}\n\n"
            "## Board Feedback Summary\n\nTODO\n\n"
            "## Parameter Changes\n\nTODO\n\n"
            "## Rationale\n\nTODO\n",
            encoding="utf-8",
        )
        logger.info("[revise] Created revision notes template at %s", notes_path)

    # Check board verdict from the JSON in backtest report
    json_path = reports_dir / "backtest_latest.json"
    if json_path.exists():
        with open(json_path) as f:
            data = json.load(f)
        print("Latest backtest summary:")
        for k, v in data.items():
            if not isinstance(v, (dict, list)):
                print(f"  {k}: {v}")

    logger.info(
        "[revise] Review board report and update strategies/%s/config.yaml, "
        "then re-run with --from-stage backtest2",
        slug,
    )
    return True


def stage_backtest2(slug: str, start: str = "2024-01-01", end: str = "2025-12-31") -> bool:
    """Run revised backtest."""
    return stage_backtest(slug, start, end, output_key="backtest_revised")


def stage_deploy(slug: str) -> bool:
    """Launch paper trading daemon (check for .env credentials first)."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        logger.error("[deploy] .env file not found — add Alpaca credentials before deploying")
        return False

    env_text = env_file.read_text()
    if "ALPACA_DATA_API_KEY" not in env_text or "ALPACA_DATA_API_SECRET" not in env_text:
        logger.error("[deploy] ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET not found in .env")
        return False

    # NOTE: run_paper.py was retired 2026-04-11. This research_pipeline auto-deploy
    # path will need updating to use cw_runner.py with a strategy yaml before use.
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "strategies" / "cw_strategies" / "cw_runner.py"),
        "--config", slug,
    ]
    logger.info("[deploy] Launching paper trading daemon: %s", " ".join(cmd))
    # Launch in background (non-blocking)
    subprocess.Popen(cmd)
    logger.info("[deploy] Paper trading daemon launched (check logs for status)")
    return True


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    slug: str,
    name: str = None,
    symbols: str = "SPY,VIXY",
    start_stage: str = "ideate",
    end_stage: str = "deploy",
    start: str = "2024-01-01",
    end: str = "2025-12-31",
) -> dict:
    """
    Run the strategy development pipeline from start_stage to end_stage.

    Returns a dict mapping stage_name → True/False (success/skip).
    """
    stage_start_idx = STAGES.index(start_stage)
    stage_end_idx = STAGES.index(end_stage)
    stages_to_run = STAGES[stage_start_idx:stage_end_idx + 1]

    results = {}
    for stage in stages_to_run:
        logger.info("\n%s  STAGE: %s — %s  %s", "=" * 20, stage.upper(), STAGE_DESCRIPTIONS[stage], "=" * 20)

        if stage == "ideate":
            ok = stage_ideate(slug, name, symbols)
        elif stage == "research":
            ok = stage_research(slug)
        elif stage == "draft":
            ok = stage_draft(slug)
        elif stage == "backtest":
            ok = stage_backtest(slug, start=start, end=end)
        elif stage == "board":
            ok = stage_board(slug)
        elif stage == "revise":
            ok = stage_revise(slug)
        elif stage == "backtest2":
            ok = stage_backtest2(slug, start=start, end=end)
        elif stage == "deploy":
            ok = stage_deploy(slug)
        else:
            logger.warning("Unknown stage: %s", stage)
            ok = False

        results[stage] = ok

        if not ok and stage not in ("research", "revise"):
            logger.error("Stage '%s' failed — halting pipeline. Fix and re-run with --from-stage %s", stage, stage)
            break

    print("\n" + "=" * 60)
    print(f"Pipeline results for '{slug}':")
    for stage, ok in results.items():
        status = "✓" if ok else "✗"
        print(f"  {status}  {stage}")
    print("=" * 60 + "\n")

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="Strategy development pipeline")
    parser.add_argument("--strategy", required=True, help="Strategy slug (e.g. spy_noon_break)")
    parser.add_argument("--name", default=None, help="Human-readable name")
    parser.add_argument("--symbols", default="SPY,VIXY", help="Comma-separated symbols")
    parser.add_argument(
        "--stage",
        default="all",
        choices=STAGES + ["all"],
        help="Single stage to run, or 'all'",
    )
    parser.add_argument(
        "--from-stage",
        default=None,
        choices=STAGES,
        dest="from_stage",
        help="Resume pipeline from this stage (runs through all remaining stages)",
    )
    parser.add_argument("--start", default="2024-01-01", help="Backtest start date")
    parser.add_argument("--end", default="2025-12-31", help="Backtest end date")
    return parser.parse_args()


def main():
    args = _parse_args()

    if args.from_stage:
        start_stage = args.from_stage
        end_stage = "deploy"
    elif args.stage == "all":
        start_stage = "ideate"
        end_stage = "deploy"
    else:
        start_stage = args.stage
        end_stage = args.stage

    run_pipeline(
        slug=args.strategy,
        name=args.name,
        symbols=args.symbols,
        start_stage=start_stage,
        end_stage=end_stage,
        start=args.start,
        end=args.end,
    )


if __name__ == "__main__":
    main()
