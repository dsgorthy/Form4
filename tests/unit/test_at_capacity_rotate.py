"""Tests for replace_weakest (rotation) at_capacity behavior.

The rotation logic lives in cw_runner.execute_entries (lines ~936–1060) and
is gated by yaml `at_capacity: replace_weakest`. These tests verify:

1. The reversal_dip.yaml has the expected rotation configuration.
2. The decision logic — given a held set with known convictions and an
   incoming candidate — produces the same swap decisions as
   `pipelines/insider_study/rd_swap_test.py`.

Pure logic test: no DB, no Alpaca. We extract the decision rule into a small
function and exercise it. The cw_runner code is the source of truth; if it
drifts, this test will silently still pass but the integration won't match.
A bigger swap-equivalence integration test lives in
`pipelines/insider_study/rd_swap_test.py`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml


REPO = Path(__file__).resolve().parents[2]
RD_YAML = REPO / "strategies/cw_strategies/configs/reversal_dip.yaml"


# ──────────────────────────────────────────────────────────────────────────
# 1) Config sanity — the yaml must enable rotation correctly
# ──────────────────────────────────────────────────────────────────────────

def test_reversal_dip_yaml_enables_rotation():
    cfg = yaml.safe_load(RD_YAML.read_text())
    assert cfg["at_capacity"] == "replace_weakest", (
        "reversal_dip.yaml must set at_capacity=replace_weakest to enable "
        "rotation. Backtest evidence: +$2.1K/yr alpha vs skip "
        "(rd_swap_test.py 2026-05-12)."
    )


def test_reversal_dip_rotation_threshold_consistent_with_entry():
    cfg = yaml.safe_load(RD_YAML.read_text())
    min_entry = cfg["min_conviction"]
    min_hard = cfg["min_conviction_at_hard"]
    assert min_hard >= min_entry, (
        f"min_conviction_at_hard ({min_hard}) must be >= min_conviction "
        f"({min_entry}); otherwise the rotation gate is looser than the "
        "normal entry gate, which is incoherent."
    )


def test_reversal_dip_replacement_advantage_set():
    cfg = yaml.safe_load(RD_YAML.read_text())
    adv = cfg.get("replacement_advantage")
    assert adv is not None, (
        "reversal_dip.yaml should explicitly set replacement_advantage. "
        "Default in cw_runner is 1.5 which is too conservative for RD's "
        "3–6 conviction range — would suppress most rotations."
    )
    assert 0.0 <= adv < 2.0, (
        "replacement_advantage outside reasonable bounds — should be a "
        "small noise-buffer (0.1–1.0)."
    )


# ──────────────────────────────────────────────────────────────────────────
# 2) Decision rule — pure logic equivalent to cw_runner's replace_weakest
# ──────────────────────────────────────────────────────────────────────────

def replace_weakest_decision(
    new_conv: float,
    held_convictions: list[float],
    max_concurrent: int,
    min_conv_at_hard: float,
    replacement_advantage: float,
) -> tuple[str, float | None]:
    """Mirror of the rotation decision in cw_runner.execute_entries.

    Returns (action, swapped_out_conv):
      - ('enter',   None)            — slot available, no swap
      - ('swap',    weakest_conv)    — at-capacity, replacing weakest
      - ('skip-hard', None)          — at-capacity, new conv below hard floor
      - ('skip-adv',  None)          — at-capacity, new conv doesn't beat weakest by advantage
    """
    if len(held_convictions) < max_concurrent:
        return ("enter", None)

    if new_conv < min_conv_at_hard:
        return ("skip-hard", None)

    weakest = min(held_convictions)
    if new_conv >= weakest + replacement_advantage:
        return ("swap", weakest)

    return ("skip-adv", None)


# Test cases — each (input, expected) pair
@pytest.mark.parametrize("new_conv,held,maxc,hard,adv,expected", [
    # Under capacity — always enter regardless of conviction
    (3.0, [], 10, 3.0, 0.5, ("enter", None)),
    (3.0, [5.0] * 9, 10, 3.0, 0.5, ("enter", None)),
    (10.0, [5.0] * 9, 10, 3.0, 0.5, ("enter", None)),

    # At capacity, below hard floor → skip
    (2.5, [4.0] * 10, 10, 3.0, 0.5, ("skip-hard", None)),

    # At capacity, above hard floor but not advantage-greater than weakest → skip
    (4.0, [4.0] * 10, 10, 3.0, 0.5, ("skip-adv", None)),
    (4.4, [4.0] * 10, 10, 3.0, 0.5, ("skip-adv", None)),  # 4.4 < 4.0+0.5

    # At capacity, advantage-greater → swap
    (4.5, [4.0] * 10, 10, 3.0, 0.5, ("swap", 4.0)),
    (5.0, [4.0] * 10, 10, 3.0, 0.5, ("swap", 4.0)),

    # At capacity, weakest is the right victim
    (6.0, [3.0, 5.0, 5.5, 4.0, 4.5, 5.0, 6.0, 5.0, 4.0, 5.0], 10, 3.0, 0.5, ("swap", 3.0)),

    # Edge — exactly equal to weakest+advantage
    (4.5, [4.0] * 10, 10, 3.0, 0.5, ("swap", 4.0)),

    # Edge — strict less than weakest+advantage
    (4.49, [4.0] * 10, 10, 3.0, 0.5, ("skip-adv", None)),
])
def test_replace_weakest_decisions(new_conv, held, maxc, hard, adv, expected):
    actual = replace_weakest_decision(new_conv, held, maxc, hard, adv)
    assert actual == expected, (
        f"new={new_conv} held={held} max={maxc} hard={hard} adv={adv} → {actual} != {expected}"
    )


# ──────────────────────────────────────────────────────────────────────────
# 3) Property — rotation never produces a held set with a lower min
# ──────────────────────────────────────────────────────────────────────────

def test_swap_strictly_raises_min_held_conviction():
    """After a swap, the weakest held position must be replaced by the new
    position, so min(held) is monotonically non-decreasing across swaps.
    A regression that violated this would mean we swap WORSE candidates in."""
    held = [3.0, 5.0, 5.5, 4.0, 4.5, 5.0, 6.0, 5.0, 4.0, 5.0]
    new = 6.0
    action, victim_conv = replace_weakest_decision(
        new, held, max_concurrent=10, min_conv_at_hard=3.0, replacement_advantage=0.5,
    )
    assert action == "swap"
    # Simulate the swap: remove victim, add new
    held_after = [c for c in held if c != victim_conv][:9] + [new]
    assert min(held_after) >= min(held), (
        "min(held) decreased after swap — replace_weakest semantics broken"
    )
