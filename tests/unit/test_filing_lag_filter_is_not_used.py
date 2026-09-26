"""No shipped strategy may gate on `min_filing_lag_days`.

The filter's advertised edge — 466 episodes, mean +13.1%, 76% win — was
measured 2026-08-29, one day before `pit_scoring._get_returns` was found to let
a late-filed trade enter its OWN track record and grade itself on its own
realised return. Late filings are precisely the population this filter selects,
so the edge was the defect: a `min_filing_lag_days=21` book backtested at
115.6/55.2/98.8% CAGR and turned $100k into $146.7M.

The grades were re-scored on 2026-09-21 with the strict guard, and on the
corrected corpus the signal screens at t = -0.17 over 2016-2021 filing-anchored
episodes. The filter stays in `evaluate_filters` so it CAN be re-measured; it
must not reach a published book without that measurement.
"""
from __future__ import annotations

from pathlib import Path

import yaml

CONFIGS = Path(__file__).resolve().parents[2] / "strategies" / "cw_strategies" / "configs"


def _filter_blocks(cfg: dict):
    if isinstance(cfg.get("filters"), dict):
        yield cfg["filters"]
    for thesis in (cfg.get("theses") or []):
        if isinstance(thesis, dict) and isinstance(thesis.get("filters"), dict):
            yield thesis["filters"]


def test_no_shipped_config_declares_the_filing_lag_filter():
    offenders = []
    for path in sorted(CONFIGS.glob("*.yaml")):
        cfg = yaml.safe_load(path.read_text()) or {}
        for block in _filter_blocks(cfg):
            if "min_filing_lag_days" in block:
                offenders.append(path.name)
    assert not offenders, (
        "these configs gate on min_filing_lag_days, whose measured edge was the "
        f"self-grading bug: {offenders}"
    )


def test_the_warning_survives_next_to_the_implementation():
    src = (Path(__file__).resolve().parents[2] / "framework" / "decision"
           / "filters.py").read_text()
    # Anchor on the IMPLEMENTATION, not the first mention — the correction
    # itself names the filter, so indexing the first occurrence finds the
    # warning and measures the text before *it*.
    impl = src.index('if "min_filing_lag_days" in thesis_filters:')
    block = src[max(0, impl - 2500):impl]
    assert "self-grading" in block or "DO NOT USE" in block, (
        "the correction above min_filing_lag_days is gone; without it the "
        "docstring reads as an endorsement of a filter that selects for a bug"
    )
