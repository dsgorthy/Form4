"""The feature framework's invariants. These are the ones that bit.

FOUR LOOK-AHEADS IN ONE WEEK, each because a script derived timing for itself:

  1. filed_at read as UTC when it holds naive Eastern -> 37 positions entered a
     session early.
  2. (filed_at::timestamptz AT TIME ZONE ...) in the label generator -> result
     depended on the SESSION timezone; on a UTC connection it handed 71.3% of
     filings a close they did not exist for.
  3. Derived features anchored on filing_DATE -> the 27.3% of buys accepted
     intraday read a close up to six hours in the future.
  4. The grade's knowledge guard used <= -> a trade filed long after execution
     entered its OWN track record (43.53pp gap vs -0.96pp on clean rows).

So: one anchor module, every feature declares which anchor it uses, and
percentiles must declare a trailing window.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from framework.features import anchor as A            # noqa: E402
from framework.features import registry as R          # noqa: E402


# ── the anchor ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ts,expected", [
    ("2026-01-02 09:45:00", False),   # intraday
    ("2026-01-02 15:59:59", False),   # one second before the bell
    ("2026-01-02 16:00:00", True),    # the boundary is inclusive
    ("2026-01-02 18:59:00", True),    # after hours
    ("2026-01-02 06:00:00", False),   # pre-open
])
def test_after_bell_boundary(ts, expected):
    assert A.after_bell(ts) is expected


@pytest.mark.parametrize("bad", [None, "", "2026-01-02", "garbage"])
def test_unparseable_timestamps_are_conservative(bad):
    """Unknown must resolve to AFTER the bell. That pushes observation later
    and execution later -- conservative on both sides. Resolving to 'intraday'
    would move the observation anchor back a day for no reason."""
    assert A.after_bell(bad) is True


def test_no_timezone_conversion_anywhere_in_the_anchor():
    """CODE only, not prose.

    The module docstring names `timestamptz` and `AT TIME ZONE` in order to
    explain the bug it exists to prevent, so a raw substring search over the
    file fails on its own explanation -- the same grep-fragility that made an
    earlier test in this repo pass for the wrong reason. Strings and comments
    are stripped before searching.
    """
    import io, tokenize
    src = (REPO / "framework/features/anchor.py").read_text()
    code = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type in (tokenize.STRING, tokenize.COMMENT):
            continue
        code.append(tok.string)
    body = " ".join(code)
    assert "timestamptz" not in body, (
        "the anchor casts filed_at. It is naive EASTERN text; casting makes "
        "the result depend on whoever is connected."
    )
    assert "AT TIME ZONE" not in body


def test_observation_and_execution_are_mirror_images():
    """A feature using the execution anchor reads a price that had not
    printed; a label using the observation anchor claims a fill nobody could
    get. They must not collapse into each other."""
    obs, exe = A.observation_session_sql(), A.execution_session_sql()
    assert "MAX(c.d)" in obs and "MIN(c.d)" in exe, (
        "observation looks BACKWARD to the last closed session, execution "
        "looks FORWARD to the first tradeable one"
    )
    assert obs != exe


# ── the registry ───────────────────────────────────────────────────────────

def test_every_feature_declares_a_known_anchor():
    for f in R.all_features():
        assert f.anchor in ("observation", "none"), f"{f.name}: {f.anchor!r}"


def test_percentiles_cannot_be_declared_without_a_trailing_window():
    """A percentile over the whole table ranks the past using the future."""
    bad = R.Feature("x", "t.value", "d", rankable_by=("sector",),
                    window_days=None)
    with pytest.raises(ValueError, match="trailing window"):
        R.percentile_variants(bad)


def test_unknown_groupings_are_rejected():
    bad = R.Feature("x", "t.value", "d", rankable_by=("astrology",))
    with pytest.raises(ValueError, match="unknown grouping"):
        R.percentile_variants(bad)


def test_ratio_features_carry_their_guards_in_the_registry():
    """The first build read an unguarded expression and immediately reproduced
    max=54,339 on value_pct_of_adv. Guards belong to the feature, not to
    whichever pipeline happens to compute it."""
    by = {f.name: f for f in R.RAW}
    assert "LEAST" in by["value_pct_of_adv"].expr
    assert str(int(R.MIN_ADV_DOLLARS)) in by["value_pct_of_adv"].expr
    assert "LEAST" in by["pct_of_prior_holding"].expr
    assert str(R.MIN_PRIOR_SHARES) in by["pct_of_prior_holding"].expr


def test_no_expression_has_an_unsubstituted_placeholder():
    """f-string expressions spanning two literals silently keep their braces."""
    for f in R.RAW:
        assert "{" not in f.expr, f"{f.name} has an unsubstituted brace: {f.expr}"


def test_generated_features_are_marked_as_generated():
    """A reader must be able to tell a declared feature from an exploded one."""
    gen = [f for f in R.all_features() if f.generated_from]
    assert gen, "the percentile generator produced nothing"
    for f in gen:
        assert f.generated_from in {r.name for r in R.RAW}
        assert "_pctile_by_" in f.name
