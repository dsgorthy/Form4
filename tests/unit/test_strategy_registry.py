"""The strategy roster has one definition, and the surfaces agree with it.

WHY THIS EXISTS

Renaming the three books on 2026-08-18 meant editing nine files, because the
display names had been retyped in every one of them. Three had already drifted:
the paper-trading dashboard called reversal_dip "Deep Reversal + Dip" while the
portfolio switcher called it "Deep Reversal", and the public methodology page
promised "three independent portfolio strategies" above a list of four.

Retiring tenb51_surprise in the same change made the second half of the problem
visible: the name was wired into monitors, broker account registries, freshness
contracts and a heartbeat map, and a runner that is unloaded but still monitored
pages someone at 06:25 every morning.

So both halves are pinned here. These are cheap string assertions over source
files rather than anything clever, which is the point — they fail on the next
person's partial rename, in CI, before it reaches a subscriber.
"""
import re
from pathlib import Path

import pytest

from api.public_fields import (
    ACTIVE_STRATEGIES,
    STRATEGIES,
    STRATEGY_LABELS,
    strategy_label,
)

REPO = Path(__file__).resolve().parents[2]

RETIRED = "tenb51_surprise"


# ── the registry itself ─────────────────────────────────────────────────────

def test_active_strategies_are_active_in_the_registry():
    for key in ACTIVE_STRATEGIES:
        assert key in STRATEGIES, f"{key} is published but not in the registry"
        assert STRATEGIES[key]["active"], f"{key} is published but marked inactive"


def test_registry_active_set_matches_published_order():
    active = {k for k, v in STRATEGIES.items() if v["active"]}
    assert active == set(ACTIVE_STRATEGIES)


def test_retired_strategy_is_present_but_inactive():
    # Kept deliberately: the label still resolves for historical rows, and the
    # decision stays reversible.
    assert STRATEGIES[RETIRED]["active"] is False
    assert RETIRED not in ACTIVE_STRATEGIES


def test_labels_are_distinct_and_non_empty():
    labels = [v["label"] for v in STRATEGIES.values()]
    assert len(labels) == len(set(labels)), "two strategies share a display name"
    assert all(lbl.strip() for lbl in labels)


def test_strategy_label_falls_back_to_the_key():
    assert strategy_label("quality_notrend") == "The A-List"
    assert strategy_label("not_a_strategy") == "not_a_strategy"


# ── the retired runner is gone from everything that would page someone ──────

# Each entry is a file that drives behaviour: a monitor that alerts, a registry
# the API serves, a contract the preflight audit enforces. History (postmortems,
# handoffs, the data-flow spec) is deliberately excluded — those record what
# happened and should keep saying so.
LIVE_SURFACES = [
    "api/routers/portfolio.py",
    "api/routers/signals.py",
    "api/routers/paper_trading.py",
    "api/main.py",
    "config/freshness_contracts.yaml",
    "config/writer_registry.yaml",
    "framework/oms/alpaca_stream_listener.py",
    "scripts/alpaca_intraday_resolver.py",
    "scripts/monday_paper_monitor.py",
    "scripts/heartbeat_probe.py",
    "scripts/post_deploy_audit.py",
    "scripts/daily_summary.py",
    "scripts/preflight/writer_registry_audit.py",
    "frontend/src/components/signal-badge.tsx",
    "frontend/src/components/portfolio-view.tsx",
    "frontend/src/app/page.tsx",
    "frontend/src/app/onboarding/page.tsx",
    "frontend/src/app/research/methodology/page.tsx",
]


def _code_only(text: str) -> str:
    """Drop comments. A comment explaining why the runner was retired is not a
    reference to it, and the assertion is about behaviour."""
    out = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("#", "//", "*", "/*")):
            continue
        for marker in ("  # ", "  // "):
            if marker in line:
                line = line.split(marker, 1)[0]
        out.append(line)
    return "\n".join(out)


@pytest.mark.parametrize("rel", LIVE_SURFACES)
def test_retired_strategy_absent_from_live_surfaces(rel):
    path = REPO / rel
    assert path.exists(), f"{rel} moved — update this list"
    body = _code_only(path.read_text())
    assert RETIRED not in body, f"{rel} still references {RETIRED}"
    assert "10b5-1 Surprise" not in body, f"{rel} still shows the retired label"


def test_simulator_no_longer_rebuilds_the_retired_book():
    src = (REPO / "pipelines/insider_study/simulate_strategy_portfolio.py").read_text()
    config = src.split("STRATEGY_CONFIG = {", 1)[1].split("\nSTARTING_CAPITAL", 1)[0]
    keys = re.findall(r'^\s{4}"([a-z0-9_]+)":\s*\{', config, flags=re.M)
    assert RETIRED not in keys, "the nightly simulator still rebuilds the retired book"
    for key in ACTIVE_STRATEGIES:
        assert key in keys, f"{key} is published but the simulator does not build it"


# ── the roster agrees across the lists that drive alerting ──────────────────

ROSTER_FILES = [
    "scripts/monday_paper_monitor.py",
    "scripts/heartbeat_probe.py",
    "scripts/post_deploy_audit.py",
    "scripts/daily_summary.py",
    "scripts/backfill_actual_from_portfolio.py",
]


@pytest.mark.parametrize("rel", ROSTER_FILES)
def test_monitor_rosters_match_the_active_set(rel):
    src = (REPO / rel).read_text()
    m = re.search(r"^STRATEGIES = \[([^\]]*)\]", src, flags=re.M)
    assert m, f"{rel} no longer declares a flat STRATEGIES list"
    names = re.findall(r'"([a-z0-9_]+)"', m.group(1))
    assert set(names) == set(ACTIVE_STRATEGIES), (
        f"{rel} monitors {sorted(names)}; active set is {sorted(ACTIVE_STRATEGIES)}. "
        "A monitored-but-unloaded runner pages someone every morning; an "
        "unmonitored live one fails silently."
    )


def test_every_active_strategy_has_a_config_yaml():
    for key in ACTIVE_STRATEGIES:
        yaml_path = REPO / "strategies/cw_strategies/configs" / f"{key}.yaml"
        assert yaml_path.exists(), f"{key} is published with no config"


def test_config_display_names_carry_the_product_label():
    for key in ACTIVE_STRATEGIES:
        body = (REPO / "strategies/cw_strategies/configs" / f"{key}.yaml").read_text()
        m = re.search(r'^display_name:\s*"([^"]+)"', body, flags=re.M)
        assert m, f"{key}.yaml has no display_name"
        assert STRATEGY_LABELS[key] in m.group(1), (
            f"{key}.yaml says {m.group(1)!r}, registry says {STRATEGY_LABELS[key]!r}"
        )
