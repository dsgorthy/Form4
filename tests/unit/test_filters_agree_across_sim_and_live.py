"""Every filter a shipped yaml declares must be honoured by ALL THREE paths.

A strategy's admission rule is evaluated in three places:

  1. the simulator, which produces the published book
       -> framework.decision.evaluate_filters
  2. the live runner's PIT engine, which produces subscribers' alerts
       -> framework/pit/strategies/*.py
  3. the live runner's legacy SQL fallback, used when the engine raises
       -> cw_runner._build_thesis_query

If a filter is missing from any one of them, that path silently admits
candidates the others reject. This has happened twice and cost real numbers
both times:

  - `STOP_LOSS_PCT = -0.30` lived in the simulator and not in the runner, so
    for three months the published book simulated a stop the alerts never
    applied. Removing it was worth +5.3 CAGR points on A-List.
  - `quality_notrend` was unregistered in `_get_pit_strategy_class`, so
    cw_runner fell back to path 3, which admitted grade B and C candidates the
    yaml excluded.

And it was about to happen a third time. `min_value_pct_of_adv`,
`max_pct_off_52w_high` and `min_filing_lag_days` were added to path 1 on
2026-08-29 and existed in NEITHER live path until 2026-09-25, the day A-List
adopted the first of them.

Paths 2 and 3 are covered structurally rather than key-by-key: path 2 must
delegate to the same evaluator as path 1, and path 3 must name every key.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
CONFIGS = REPO / "strategies" / "cw_strategies" / "configs"
FILTERS = REPO / "framework" / "decision" / "filters.py"
RUNNER = REPO / "strategies" / "cw_strategies" / "cw_runner.py"
PIT_STRATEGIES = REPO / "framework" / "pit" / "strategies"

#: Keys that are not admission filters and are handled elsewhere.
NOT_A_FILTER = {
    # tenb51_surprise counts prior 10b5-1 sales with its own query in both the
    # simulator and the runner; it is not part of evaluate_filters.
    "min_prior_10b5_1_sells",
    # require_cluster selects a JOIN against trade_signals rather than a
    # column predicate, in both paths.
    "require_cluster",
}


def _yaml_filter_keys() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for path in sorted(CONFIGS.glob("*.yaml")):
        cfg = yaml.safe_load(path.read_text()) or {}
        keys: set[str] = set()
        blocks = []
        if isinstance(cfg.get("filters"), dict):
            blocks.append(cfg["filters"])
        for thesis in (cfg.get("theses") or []):
            if isinstance(thesis, dict) and isinstance(thesis.get("filters"), dict):
                blocks.append(thesis["filters"])
        for b in blocks:
            keys |= set(b)
        out[path.name] = keys - NOT_A_FILTER
    return out


def _keys_handled(src: str, var: str) -> set[str]:
    """Filter keys a source file reads off its filter dict."""
    found = set(re.findall(rf'{var}\.get\("([a-z0-9_]+)"', src))
    found |= set(re.findall(rf'"([a-z0-9_]+)" in {var}', src))
    found |= set(re.findall(rf'{var}\["([a-z0-9_]+)"\]', src))
    # The grade filters are read from a tuple loop, not by literal subscript.
    for m in re.finditer(r'for \w+ in \(([^)]*)\)', src):
        for g in re.findall(r'"([a-z0-9_]+)"', m.group(1)):
            if g.endswith("_grade"):
                found.add(g)
    return found


def _build_thesis_query_src() -> str:
    src = RUNNER.read_text()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_build_thesis_query")
    return ast.get_source_segment(src, fn)


def test_simulator_honours_every_filter_a_shipped_yaml_declares():
    handled = _keys_handled(FILTERS.read_text(), "thesis_filters")
    missing = {name: sorted(keys - handled)
               for name, keys in _yaml_filter_keys().items()
               if keys - handled}
    assert not missing, (
        "these yaml filters are not evaluated by framework.decision."
        f"evaluate_filters, so the published book ignores them: {missing}"
    )


def test_legacy_sql_path_honours_every_filter_a_shipped_yaml_declares():
    handled = _keys_handled(_build_thesis_query_src(), "filters")
    missing = {name: sorted(keys - handled)
               for name, keys in _yaml_filter_keys().items()
               if keys - handled}
    assert not missing, (
        "these yaml filters have no clause in cw_runner._build_thesis_query. "
        "That builder is the fallback whenever the PIT engine raises, so on any "
        "engine error the live runner would admit candidates the published book "
        f"rejects: {missing}"
    )


def test_every_live_strategy_class_delegates_to_the_shared_evaluator():
    """Path 2 is covered by construction, not key-by-key.

    A strategy class that hand-codes its filter conditions honours exactly the
    ones someone remembered; delegating means every key the yaml may legally
    declare is applied.
    """
    offenders = []
    for path in sorted(PIT_STRATEGIES.glob("*.py")):
        if path.name == "__init__.py":
            continue
        src = path.read_text()
        if "def evaluate" not in src:
            continue
        if "evaluate_filters(" not in src:
            offenders.append(path.name)
    assert not offenders, (
        "these live strategy classes evaluate filters by hand instead of "
        f"calling framework.decision.evaluate_filters: {offenders}"
    )


def test_the_shared_evaluator_can_see_every_field_it_reads():
    """A filter whose field is absent from TradeEvent does not fail loudly.

    `_get` falls back to getattr(..., None), and a numeric filter treats None as
    a failure — so a missing field rejects every live candidate while the
    simulator, which passes dicts straight off the trades row, admits them.
    """
    from framework.pit.events import TradeEvent

    handled = _keys_handled(FILTERS.read_text(), "thesis_filters")
    # Map filter key -> the event attribute it reads.
    attr_for = {
        "min_consecutive_sells": "consecutive_sells_before",
        "max_dip_1mo": "dip_1mo",
        "min_dip_3mo": "dip_3mo",
        "min_value_pct_of_adv": "value_pct_of_adv",
        "min_filing_lag_days": "filing_lag_days",
        "max_pct_off_52w_high": "pct_off_52w_high",
        "above_sma50": "above_sma50",
        "above_sma200": "above_sma200",
        "is_largest_ever": "is_largest_ever",
        "is_rare_reversal": "is_rare_reversal",
        "exclude_10b5_1": "is_10b5_1",
        "exclude_recurring": "is_recurring",
        "exclude_tax_sales": "is_tax_sale",
        "exclude_routine": "cohen_routine",
        "pit_grade": "pit_grade",
        "career_grade": "career_grade",
    }
    fields = set(TradeEvent.__dataclass_fields__)
    unmapped = sorted(handled - set(attr_for))
    assert not unmapped, (
        f"evaluate_filters reads filter key(s) {unmapped} that this test does "
        "not know the TradeEvent field for — add them to attr_for so the field "
        "is checked, or the next new filter no-ops on the live path unnoticed"
    )
    missing = sorted(a for k, a in attr_for.items() if a not in fields)
    assert not missing, (
        f"TradeEvent has no field(s) {missing}, so the live path cannot "
        "evaluate the filter(s) that read them"
    )
    assert "is_duplicate" in fields, (
        "evaluate_filters checks is_duplicate unconditionally; TradeEvent must "
        "carry it or every live candidate reads as non-duplicate by accident"
    )
