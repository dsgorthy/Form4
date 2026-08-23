"""What the site says about stops must match what the yamls say.

WHY

On 2026-08-23 the public methodology page was still telling subscribers the
stop was a flat −30% across all three strategies. It had been −50% since
2026-08-20 and Insider Breakout had moved to −20% that morning. The page was
wrong in two directions at once, and nothing failed.

The specific trap is that "the stop" stopped being a single number. Two books
carry a −50% backstop that has never fired; one carries a −20% working stop
that has closed 19 of its 85 positions. Any copy that names one global figure
is now false for at least one book, so this pins the yaml as the only source
and fails the build if a literal reappears that no yaml supports.

Related: tests/unit/test_stop_is_config_driven.py pins sim-vs-live agreement.
This one pins what we PUBLISH against the same configs.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CONFIGS = REPO / "strategies/cw_strategies/configs"
METHODOLOGY_PAGE = REPO / "frontend/src/app/research/methodology/page.tsx"
METHODOLOGY_DOC = REPO / "docs/published_returns_methodology.md"
WHITEPAPERS = {
    "quality_momentum": REPO / "frontend/content/research/whitepapers/quality-momentum.md",
    "reversal_dip": REPO / "frontend/content/research/whitepapers/reversal-dip.md",
}

ACTIVE = ["quality_notrend", "quality_momentum", "reversal_dip"]


def _stop(key: str) -> float:
    cfg = yaml.safe_load((CONFIGS / f"{key}.yaml").read_text())
    return float(cfg["exit"]["stop_loss_pct"])


def test_the_three_books_do_not_all_share_one_stop():
    """If they ever do again, the copy below may be simplified — but it has to
    be a deliberate edit, not an assumption that silently goes stale."""
    stops = {k: _stop(k) for k in ACTIVE}
    assert len(set(stops.values())) > 1, (
        f"all three stops are equal again ({stops}); the public copy names them "
        "separately and should be revisited"
    )


@pytest.mark.parametrize("key", ACTIVE)
def test_every_active_strategy_declares_a_stop_in_yaml(key):
    assert _stop(key) < 0, f"{key} has no negative stop_loss_pct in its yaml"


def test_public_page_states_no_stop_percentage_the_yamls_do_not_support():
    """The page may name any stop a config actually declares, and no other."""
    src = METHODOLOGY_PAGE.read_text()
    allowed = {abs(round(_stop(k) * 100)) for k in ACTIVE}
    # Percentages written as "&minus;NN%" — the entity form the page uses for
    # a signed figure. A bare "20%" elsewhere is prose about something else.
    cited = {int(m) for m in re.findall(r"&minus;(\d{2})%", src)}
    unsupported = cited - allowed
    assert not unsupported, (
        f"the public methodology page cites stop levels {sorted(unsupported)}% "
        f"that no strategy yaml declares (declared: {sorted(allowed)}%). "
        "This is exactly how the page came to advertise -30% for three days."
    )


def test_public_page_names_the_breakout_stop_as_a_working_stop():
    """A -20% stop that closes 19 of 85 positions cannot be described the way a
    never-triggered backstop is."""
    src = METHODOLOGY_PAGE.read_text()
    assert "&minus;20%" in src, "the page no longer names Insider Breakout's stop"
    assert "Why No Tight Stops" not in src, (
        "the page reverted to claiming there are no tight stops — Insider "
        "Breakout carries a working one"
    )


def test_docs_do_not_claim_a_single_stop_that_never_fired():
    """CLAUDE.md and the methodology doc both carried this sentence, and it
    stopped being true the morning Breakout moved to -20%."""
    for path in (REPO / "CLAUDE.md", METHODOLOGY_DOC):
        body = path.read_text()
        assert "The stop is −50%, declared in each strategy yaml, and it has never fired" not in body, (
            f"{path.name} claims one global -50% stop that never fired; "
            "Insider Breakout is -20% and has fired 19 times"
        )


def test_methodology_doc_records_the_per_trade_audit():
    """The audit is the substantiation behind every figure on the page. If the
    record goes, the figures are unbacked again."""
    body = METHODOLOGY_DOC.read_text()
    assert "per-trade audit" in body.lower()
    for required in ("2,040", "zero exceptions", "entry_timing", "evaluate_filters"):
        assert required in body, f"the audit record no longer states: {required}"


def test_methodology_doc_publishes_both_drawdown_figures():
    """Publishing only the trade-row figure is how Dip Buys advertised 11.3%
    against a lived 21.5%."""
    body = METHODOLOGY_DOC.read_text()
    assert "max DD, trade-row" in body and "max DD, daily" in body, (
        "the headline table no longer carries both drawdown columns"
    )


# ── the whitepapers describe the same books and drifted the same way ────────


@pytest.mark.parametrize("key,path", sorted(WHITEPAPERS.items()))
def test_whitepaper_does_not_deny_a_stop_its_yaml_declares(key, path):
    """Both published whitepapers said "There is no stop-loss" — four times
    between them — while every yaml declared one. For Insider Breakout that had
    become badly wrong: a -20% working stop closes 19 of its 85 positions.

    A paper may say there is no TRADING stop, which is a claim about how the
    stop is used. It may not say there is no stop."""
    assert path.exists(), f"{path.name} moved — update this map"
    body = path.read_text()
    assert _stop(key) < 0, f"{key} declares no stop; this test's premise is gone"
    for denial in ("There is no stop-loss", "there is no stop-loss",
                   "no stop-loss and no trailing stop"):
        assert denial not in body, (
            f"{path.name} says {denial!r} while {key}.yaml declares "
            f"{_stop(key):.0%}. Describe how the stop is USED, not that it is absent."
        )


@pytest.mark.parametrize("key,path", sorted(WHITEPAPERS.items()))
def test_whitepaper_names_its_own_stop_level(key, path):
    """The level has to appear, so a change to the yaml forces a look here."""
    body = path.read_text()
    level = f"{abs(round(_stop(key) * 100))}%"
    assert level in body, (
        f"{path.name} never names its own stop level ({level}); it cannot be "
        "checked against the yaml and will drift again"
    )
