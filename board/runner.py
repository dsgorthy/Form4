"""
Board of Personas runner — evaluates a strategy by running 5 parallel Claude subprocess calls.

Usage:
    from board.runner import BoardRunner
    runner = BoardRunner(strategy_name="spy_0dte_reversal")
    verdicts = runner.run(backtest_summary, strategy_spec)
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

PERSONAS = ["quant_analyst", "risk_manager", "head_trader", "portfolio_manager", "skeptic"]
PERSONAS_DIR = Path(__file__).parent / "personas"
MAX_RETRIES = 2


def _run_persona(
    persona_name: str,
    context_json: str,
    timeout: int = 120,
) -> Dict:
    """
    Run a single persona evaluation via claude CLI subprocess.

    Returns a dict with verdict, score, conditions, key_concerns, key_strengths, reasoning.
    On failure, returns a dict with verdict="error" and an error message.
    """
    persona_file = PERSONAS_DIR / f"{persona_name}.md"
    if not persona_file.exists():
        return {"verdict": "error", "score": 0, "error": f"Persona file not found: {persona_file}"}

    system_prompt = persona_file.read_text(encoding="utf-8")

    # Strip CLAUDECODE so nested claude processes are not blocked
    import os
    clean_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = subprocess.run(
                [
                    "claude",
                    "--print",
                    "--output-format", "json",
                    "--system-prompt", system_prompt,
                    context_json,
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=clean_env,
            )

            if result.returncode != 0:
                logger.warning(
                    "Persona %s (attempt %d/%d): claude exited with code %d: %s",
                    persona_name, attempt, MAX_RETRIES, result.returncode, result.stderr[:500]
                )
                continue

            # claude --output-format json wraps the response in {"result": "...", ...}
            outer = json.loads(result.stdout)
            inner_text = outer.get("result") or outer.get("content") or result.stdout

            # Try to parse the inner JSON (the persona's actual JSON response)
            parsed = _extract_json(inner_text)
            if parsed is None:
                logger.warning("Persona %s (attempt %d): could not parse JSON from response", persona_name, attempt)
                if attempt == MAX_RETRIES:
                    return {
                        "verdict": "error",
                        "score": 0,
                        "error": f"JSON parse failed after {MAX_RETRIES} attempts",
                        "raw_response": inner_text[:500],
                    }
                continue

            # Normalize verdict
            verdict = parsed.get("verdict", "").lower()
            if verdict not in ("approve", "reject", "conditional"):
                parsed["verdict"] = "conditional"

            parsed["persona"] = persona_name
            return parsed

        except subprocess.TimeoutExpired:
            logger.warning("Persona %s (attempt %d): timeout after %ds", persona_name, attempt, timeout)
            if attempt == MAX_RETRIES:
                return {"verdict": "error", "score": 0, "persona": persona_name, "error": "timeout"}
        except json.JSONDecodeError as exc:
            logger.warning("Persona %s (attempt %d): outer JSON parse error: %s", persona_name, attempt, exc)
            if attempt == MAX_RETRIES:
                return {"verdict": "error", "score": 0, "persona": persona_name, "error": str(exc)}
        except Exception as exc:
            logger.error("Persona %s (attempt %d): unexpected error: %s", persona_name, attempt, exc)
            return {"verdict": "error", "score": 0, "persona": persona_name, "error": str(exc)}

    return {"verdict": "error", "score": 0, "persona": persona_name, "error": "exhausted retries"}


def _extract_json(text: str) -> Optional[Dict]:
    """Extract a JSON object from text that might have surrounding prose."""
    # Try direct parse first
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Look for ```json...``` blocks
    match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Look for first { ... } block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


class BoardRunner:
    """
    Runs 5 persona evaluations in parallel and returns all verdicts.
    """

    def __init__(self, strategy_name: str, max_workers: int = 5):
        self.strategy_name = strategy_name
        self.max_workers = max_workers

    def build_context(self, backtest_summary: dict, strategy_spec: str, research_notes: str = "") -> str:
        """
        Build the context JSON string sent to each persona.

        Parameters
        ----------
        backtest_summary : dict
            Output of BacktestResult.summary() — metrics dict.
        strategy_spec : str
            Strategy specification text (from strategy_spec.md or config.yaml).
        research_notes : str
            Any additional research notes or hypothesis documentation.
        """
        context = {
            "strategy_name": self.strategy_name,
            "backtest_summary": backtest_summary,
            "strategy_spec": strategy_spec,
            "research_notes": research_notes,
            "instructions": (
                "Evaluate this trading strategy based on the backtest results and strategy specification. "
                "Respond ONLY with valid JSON matching the schema in your system prompt. "
                "Do not include any text outside the JSON object."
            ),
        }
        return json.dumps(context, indent=2, default=str)

    def run(
        self,
        backtest_summary: dict,
        strategy_spec: str,
        research_notes: str = "",
        timeout: int = 120,
    ) -> List[Dict]:
        """
        Run all 5 persona evaluations in parallel.

        Parameters
        ----------
        backtest_summary : dict
            Backtest metrics dict.
        strategy_spec : str
            Strategy specification text.
        research_notes : str
            Additional context.
        timeout : int
            Per-persona timeout in seconds.

        Returns
        -------
        list of dicts
            One dict per persona with verdict, score, concerns, strengths, reasoning.
        """
        context_json = self.build_context(backtest_summary, strategy_spec, research_notes)

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_persona = {
                executor.submit(_run_persona, persona, context_json, timeout): persona
                for persona in PERSONAS
            }

            for future in as_completed(future_to_persona):
                persona = future_to_persona[future]
                try:
                    result = future.result()
                    results[persona] = result
                    logger.info(
                        "Persona %s: verdict=%s score=%s",
                        persona, result.get("verdict"), result.get("score")
                    )
                except Exception as exc:
                    logger.error("Persona %s raised an exception: %s", persona, exc)
                    results[persona] = {"verdict": "error", "score": 0, "persona": persona, "error": str(exc)}

        # Return in consistent order
        return [results.get(p, {"verdict": "error", "score": 0, "persona": p}) for p in PERSONAS]
