"""
Board report aggregator — combines 5 persona verdicts into a final recommendation.

Advance rules:
    - 5 approve → advance to paper
    - 4 approve + 1 conditional → advance to paper
    - 3 approve + 2 conditional → advance with tracked conditions
    - Any reject (from non-error persona) → return to research
    Note: Skeptic alone cannot block (requires 2 total rejects from any personas)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).parent.parent / "reports"


def _count_verdicts(verdicts: List[Dict]) -> Tuple[int, int, int, int]:
    """Return (approvals, conditionals, rejections, errors)."""
    approvals = sum(1 for v in verdicts if v.get("verdict") == "approve")
    conditionals = sum(1 for v in verdicts if v.get("verdict") == "conditional")
    rejections = sum(1 for v in verdicts if v.get("verdict") == "reject")
    errors = sum(1 for v in verdicts if v.get("verdict") == "error")
    return approvals, conditionals, rejections, errors


def _determine_recommendation(verdicts: List[Dict]) -> Tuple[str, str]:
    """
    Apply advance rules and return (recommendation, explanation).

    Recommendations: "advance_to_paper" | "advance_with_conditions" | "return_to_research"
    """
    approvals, conditionals, rejections, errors = _count_verdicts(verdicts)

    # Handle errors — treat as no-vote for rule purposes
    valid_count = approvals + conditionals + rejections

    # Apply advance rules
    if rejections == 0 and approvals >= 5:
        return "advance_to_paper", "All 5 personas approve."
    elif rejections == 0 and approvals >= 4 and conditionals >= 1:
        return "advance_to_paper", f"{approvals} approve + {conditionals} conditional — advance to paper."
    elif rejections == 0 and approvals >= 3 and conditionals >= 2:
        conditions = []
        for v in verdicts:
            if v.get("verdict") == "conditional":
                conditions.extend(v.get("conditions", []))
        return "advance_with_conditions", f"{approvals} approve + {conditionals} conditional — advance with tracked conditions: {'; '.join(conditions)}"
    elif rejections >= 1:
        # Check if only the Skeptic rejected
        skeptic = next((v for v in verdicts if v.get("persona") == "skeptic"), None)
        non_skeptic_rejections = sum(
            1 for v in verdicts
            if v.get("verdict") == "reject" and v.get("persona") != "skeptic"
        )
        if rejections == 1 and skeptic and skeptic.get("verdict") == "reject":
            # Only the skeptic rejected — do not auto-block (but still flag)
            if approvals >= 3:
                return "advance_with_conditions", "Skeptic rejected but cannot veto alone. Other concerns noted."
            else:
                return "return_to_research", "Skeptic rejected and insufficient approvals."
        else:
            return "return_to_research", f"{rejections} non-skeptic rejection(s). Address concerns before re-evaluation."
    else:
        return "return_to_research", f"Insufficient approvals (approve={approvals}, conditional={conditionals}, reject={rejections}, error={errors})."


def generate_board_report(
    strategy_name: str,
    verdicts: List[Dict],
    backtest_summary: dict,
    output_dir: Path = None,
) -> Path:
    """
    Generate a markdown board report from persona verdicts.

    Parameters
    ----------
    strategy_name : str
        Name of the strategy being evaluated.
    verdicts : list of dicts
        Output from BoardRunner.run().
    backtest_summary : dict
        Backtest metrics for reference.
    output_dir : Path, optional
        Where to save the report. Defaults to reports/{strategy_name}/.

    Returns
    -------
    Path
        Path to the generated report file.
    """
    if output_dir is None:
        output_dir = REPORTS_DIR / strategy_name
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"board_report_{date_str}.md"

    recommendation, explanation = _determine_recommendation(verdicts)
    approvals, conditionals, rejections, errors = _count_verdicts(verdicts)

    avg_score = 0.0
    valid_scores = [v.get("score", 0) for v in verdicts if isinstance(v.get("score"), (int, float)) and v.get("verdict") != "error"]
    if valid_scores:
        avg_score = sum(valid_scores) / len(valid_scores)

    # Recommendation emoji
    rec_emoji = {
        "advance_to_paper": "✅",
        "advance_with_conditions": "⚠️",
        "return_to_research": "❌",
    }.get(recommendation, "❓")

    lines = [
        f"# Board of Personas — {strategy_name}",
        f"**Date:** {date_str}",
        f"**Strategy:** {strategy_name}",
        "",
        "---",
        "",
        "## Aggregate Verdict",
        "",
        f"**Recommendation:** {rec_emoji} {recommendation.replace('_', ' ').title()}",
        "",
        f"**Explanation:** {explanation}",
        "",
        f"**Score Summary:** {avg_score:.1f}/10 average | {approvals} approve | {conditionals} conditional | {rejections} reject | {errors} error",
        "",
        "---",
        "",
        "## Backtest Summary",
        "",
        "```",
    ]

    # Format backtest metrics
    metrics_to_show = [
        ("total_trades", "Total Trades"),
        ("win_rate", "Win Rate"),
        ("total_pnl", "Total P&L"),
        ("total_return_pct", "Total Return %"),
        ("sharpe_ratio", "Sharpe Ratio"),
        ("max_drawdown_pct", "Max Drawdown %"),
        ("max_consecutive_losses", "Max Consecutive Losses"),
        ("profit_factor", "Profit Factor"),
        ("expectancy", "Expectancy / Trade"),
        ("avg_win", "Avg Win"),
        ("avg_loss", "Avg Loss"),
    ]

    for key, label in metrics_to_show:
        if key in backtest_summary:
            val = backtest_summary[key]
            if isinstance(val, float):
                lines.append(f"  {label:<30} {val:.4f}")
            else:
                lines.append(f"  {label:<30} {val}")

    lines.extend([
        "```",
        "",
        "---",
        "",
        "## Individual Persona Verdicts",
        "",
    ])

    persona_display_names = {
        "quant_analyst": "Quant Analyst",
        "risk_manager": "Risk Manager",
        "head_trader": "Head Trader",
        "portfolio_manager": "Portfolio Manager",
        "skeptic": "Skeptic",
    }

    verdict_emoji = {
        "approve": "✅",
        "conditional": "⚠️",
        "reject": "❌",
        "error": "💥",
    }

    for v in verdicts:
        persona = v.get("persona", "unknown")
        display_name = persona_display_names.get(persona, persona.replace("_", " ").title())
        verdict = v.get("verdict", "error")
        score = v.get("score", 0)
        emoji = verdict_emoji.get(verdict, "❓")

        lines.append(f"### {emoji} {display_name} — {verdict.upper()} ({score}/10)")
        lines.append("")

        reasoning = v.get("reasoning", "No reasoning provided.")
        lines.append(f"**Reasoning:** {reasoning}")
        lines.append("")

        strengths = v.get("key_strengths", [])
        if strengths:
            lines.append("**Key Strengths:**")
            for s in strengths:
                lines.append(f"- {s}")
            lines.append("")

        concerns = v.get("key_concerns", [])
        if concerns:
            lines.append("**Key Concerns:**")
            for c in concerns:
                lines.append(f"- {c}")
            lines.append("")

        conditions = v.get("conditions", [])
        if conditions and verdict == "conditional":
            lines.append("**Conditions for Advance:**")
            for cond in conditions:
                lines.append(f"- {cond}")
            lines.append("")

        if verdict == "error":
            error_msg = v.get("error", "Unknown error")
            lines.append(f"**Error:** {error_msg}")
            lines.append("")

        lines.append("---")
        lines.append("")

    report_text = "\n".join(lines)
    report_path.write_text(report_text, encoding="utf-8")
    logger.info("Board report written to %s", report_path)

    return report_path
