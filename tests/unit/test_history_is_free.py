"""The 90-day wall is down: free accounts and visitors see the whole record.

History is the SEO asset. The insider and company pages rank because they are
the record of what someone traded; paywalling the record paywalls the thing
the page ranks for. Pro is the judgement on top of it -- grades, track
records, screens, clusters, the strategy books' alerts -- and those gates
stay. Decided in docs/growth_plan_2026-09.md, shipped 2026-09-17.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FE = ROOT / "frontend" / "src"


def _code(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    src = re.sub(r'"""(?:.|\n)*?"""', "", src)
    return re.sub(r"^\s*(//|#).*$", "", src, flags=re.M)


def test_the_flag_is_on_and_the_api_reads_it():
    from api.public_fields import HISTORY_IS_FREE
    from api.auth import UserContext
    assert HISTORY_IS_FREE is True
    assert UserContext(user_id="u", tier="free").has_full_feed
    assert UserContext().has_full_feed, "anonymous readers get the record too; it is what Google indexes"
    assert not UserContext(user_id="u", tier="free").is_pro


def test_analysis_gates_do_not_ride_on_the_record_flag():
    """Clusters and the strategy books' trade log are Pro; they must gate on
    is_pro, or opening the record would have opened them too."""
    for name in ("clusters.py", "portfolio.py"):
        src = _code((ROOT / "api" / "routers" / name).read_text(encoding="utf-8"))
        assert "has_full_feed" not in src, f"{name} gates analysis on the record flag"
        assert "is_pro" in src


def test_no_surface_still_promises_a_90_day_window():
    hits = []
    for p in list(FE.rglob("*.tsx")) + [ROOT / "api" / "email_templates.py"]:
        if ".next" in p.parts:
            continue
        code = _code(p.read_text(encoding="utf-8"))
        for phrase in ("Last 90 days of filings", "90 days of filings", "Limited to last 90 days", "last 90 days is open", "90 days back"):
            if phrase in code:
                hits.append(f"{p.relative_to(ROOT)}: {phrase!r}")
    assert not hits, "\n".join(hits)


def test_pricing_no_longer_sells_history_as_pro():
    src = (FE / "app" / "pricing" / "page.tsx").read_text(encoding="utf-8")
    assert "Full filing history" not in src
    assert "Every filing since 2016" in src
