"""The stop lives in the strategy yaml, and both surfaces must read it there.

WHAT WENT WRONG

`simulate_strategy_portfolio.py` carried a module-level constant:

    STOP_LOSS_PCT = -0.30   # Derek's override 2026-05-12, applied to all
                            # strategies retroactively

All three strategy yamls said `stop_loss_pct: null`. `cw_runner.py` — the live
alert runner — reads the yaml and treats null as no stop. So from 2026-05-12 to
2026-08-20 the published book simulated a -30% stop that the alerts a subscriber
actually received never applied. Neither surface was wrong on its own terms;
they simply read different sources for the same rule.

The cost was not small. Removing the override moved quality_notrend +5.3 CAGR
points and quality_momentum +3.2 over the published window, with max drawdown
unchanged to a tenth of a point on all three.

WHAT THESE TESTS PIN

1. No module-level stop constant reappears in the simulator.
2. The simulator and the live runner resolve the SAME stop from the SAME yaml.
3. Normalisation matches cw_runner's: null/0/positive all mean "no stop", and a
   positive magnitude is coerced negative rather than silently inverting the
   rule into a take-profit.

Test 2 is the one that matters. A future stop change is fine — a stop change
that lands on only one of the two surfaces is not.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from pipelines.insider_study.simulate_strategy_portfolio import resolve_stop_pct

REPO = Path(__file__).resolve().parents[2]
CONFIGS = REPO / "strategies" / "cw_strategies" / "configs"
SIMULATOR = REPO / "pipelines" / "insider_study" / "simulate_strategy_portfolio.py"

#: The three published books. Deliberately literal rather than imported from
#: ACTIVE_STRATEGIES — if a strategy is added, this test should fail until
#: somebody confirms its stop is config-driven too.
PUBLISHED = ("quality_notrend", "quality_momentum", "reversal_dip")


def _load(name: str) -> dict:
    return yaml.safe_load((CONFIGS / f"{name}.yaml").read_text())


def _cw_runner_stop(config: dict) -> float | None:
    """Reimplementation of cw_runner.py's normalisation, kept deliberately dumb.

    Mirrors the two places the runner resolves a stop (the exit-config read
    around cw_runner.py:1487 and the exit check at :2125). If the runner's
    behaviour changes, this diverges and test 2 fails — which is the point.
    """
    exit_cfg = (config.get("theses") or [{}])[0].get("exit") or config.get("exit") or {}
    stop = exit_cfg.get("stop_loss_pct")
    if stop is None:
        stop = exit_cfg.get("stop_pct")
    if stop is None:
        return None
    stop = float(stop)
    if stop > 0:
        stop = -stop
    # The runner's exit check: "A stop must be negative; 0 / None / positive
    # all mean 'no stop'."
    return stop if stop < 0 else None


def test_no_module_level_stop_constant_in_the_simulator():
    """The override must not come back as a global."""
    src = SIMULATOR.read_text()
    # Match an assignment, not the docstring that explains why it was removed.
    offenders = [
        line for line in src.splitlines()
        if re.match(r"^\s*(STOP_LOSS_PCT|DEFAULT_STOP|HARD_STOP)\s*=", line)
    ]
    assert not offenders, (
        "A module-level stop constant is back in simulate_strategy_portfolio.py: "
        f"{offenders}. The stop belongs in the strategy yaml, where cw_runner "
        "also reads it. A constant here applies to every strategy and is "
        "invisible to the live runner — that divergence cost 5.3 CAGR points "
        "on quality_notrend before it was caught on 2026-08-20."
    )


@pytest.mark.parametrize("strategy", PUBLISHED)
def test_simulator_and_live_runner_resolve_the_same_stop(strategy):
    """The whole point. One yaml, one answer, both surfaces."""
    config = _load(strategy)
    assert resolve_stop_pct(config) == _cw_runner_stop(config), (
        f"{strategy}: the backtest and the live alert runner disagree about the "
        f"stop — simulator says {resolve_stop_pct(config)}, cw_runner would "
        f"apply {_cw_runner_stop(config)}. Published performance would be "
        "modelling a rule subscribers never receive."
    )


@pytest.mark.parametrize("strategy", PUBLISHED)
def test_published_strategies_declare_a_stop_explicitly(strategy):
    """null is a legitimate answer, but it has to be written down.

    A missing key and a deliberate `null` read identically to the code and very
    differently to a human deciding whether the stop was considered.
    """
    exit_cfg = _load(strategy).get("exit") or {}
    assert "stop_loss_pct" in exit_cfg, (
        f"{strategy}.yaml has no stop_loss_pct key in its exit block. Write it "
        "down even when the answer is null."
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, None),      # not set
        (0, None),         # zero is not a stop at the entry price
        (0.0, None),
        (-0.50, -0.50),    # the normal case
        (0.50, -0.50),     # a positive magnitude means the same stop, not a target
        (-0.30, -0.30),
    ],
)
def test_stop_normalisation_matches_the_runner(raw, expected):
    assert resolve_stop_pct({"exit": {"stop_loss_pct": raw}}) == expected


def test_a_thesis_exit_block_wins_over_a_top_level_one():
    """Multi-thesis configs keep their per-thesis exit rules."""
    config = {
        "exit": {"stop_loss_pct": -0.30},
        "theses": [{"exit": {"stop_loss_pct": -0.50}}],
    }
    assert resolve_stop_pct(config) == -0.50
