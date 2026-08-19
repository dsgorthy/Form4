"""What each tier can reach, pinned against the routers themselves.

WHY

Tier copy and tier code drifted in both directions at once, and nobody noticed
because neither half is executed by a test:

  * /pricing sold "CSV export" as Pro+ while /api/v1/export was require_pro,
    so every Pro subscriber already had the feature they were being upsold.
  * The Free column advertised "Congress trades" while /api/v1/congress was
    require_pro_plus, so the cheapest tier was promised the most expensive
    endpoint.

Derek set the rule on 2026-08-18: Pro is everything we compute, Pro+ is only
getting it out of the product — bulk export and the API. These assertions read
the routers, so the rule survives the next person who edits one.
"""
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTERS = REPO / "api/routers"

#: Analysis surfaces — the thing the subscription is for. Pro at most.
PRO_MODULES = ["leaderboard.py", "congress.py"]

#: Only these may demand Pro+.
PRO_PLUS_MODULES = ["export.py"]


def _src(name: str) -> str:
    path = ROUTERS / name
    assert path.exists(), f"{name} moved — update this list"
    return path.read_text()


@pytest.mark.parametrize("name", PRO_MODULES)
def test_analysis_endpoints_are_pro_not_pro_plus(name):
    src = _src(name)
    assert "require_pro_plus" not in src, (
        f"{name} still gates on Pro+. Pro is everything we compute; only "
        f"export and the API sit above it."
    )
    assert "is_pro_plus" not in src, f"{name} branches on is_pro_plus"


@pytest.mark.parametrize("name", PRO_PLUS_MODULES)
def test_export_stays_pro_plus(name):
    assert "require_pro_plus" in _src(name), f"{name} no longer requires Pro+"


def test_no_other_router_demands_pro_plus():
    """A new Pro+ gate is a pricing change and must be a deliberate one."""
    offenders = sorted(
        p.name for p in ROUTERS.glob("*.py")
        if p.name not in PRO_PLUS_MODULES and "require_pro_plus" in p.read_text()
    )
    assert not offenders, (
        f"{offenders} gate on Pro+. Add to PRO_PLUS_MODULES only if the "
        f"pricing page says so too."
    )


def test_every_gating_name_used_is_imported():
    """The failure that nearly shipped: swapping require_pro_plus for
    require_pro in a router's import line while leaving three call sites
    behind. That is a NameError at import time, which takes the whole API down
    rather than degrading one endpoint."""
    broken = []
    for path in sorted(ROUTERS.glob("*.py")):
        src = path.read_text()
        imported: set[str] = set()
        for line in src.splitlines():
            m = re.match(r"from api\.gating import (.+)", line)
            if m:
                imported |= {p.strip() for p in m.group(1).split(",")}
        for name in ("require_pro", "require_pro_plus", "require_auth"):
            if re.search(rf"Depends\({name}\)", src) and name not in imported:
                broken.append(f"{path.name}: uses {name}, never imported")
    assert not broken, broken


def test_quality_filters_on_the_feed_require_pro():
    """min_grade and min_tier both select on t.pit_grade, our scoring output.
    Anonymous callers could use either until 2026-08-18."""
    guard = re.search(
        r"if \(min_grade is not None or min_tier is not None\)[^\n]*not user\.is_pro",
        _src("filings.py"),
    )
    assert guard, "the feed no longer gates grade/tier filtering behind Pro"


def test_scoring_factors_are_not_public():
    """The star rating is the teaser; the factor breakdown is the model."""
    assert _src("filings.py").count('grade.pop("factors", None)') >= 2, (
        "both the list and the detail endpoint must strip trade_grade.factors "
        "for non-Pro callers"
    )
