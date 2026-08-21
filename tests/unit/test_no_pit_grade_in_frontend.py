"""`pit_grade` may not be rendered. It is an input, not a published rating.

api/ratings.py is explicit and CLAUDE.md repeats it: career_grade is the
published Insider Rating; pit_grade feeds the Trade Rating, so showing it beside
that rating shows a reader an input next to its own output. It is also not
monotonic — C ranks BELOW D on mean abnormal return — which is the reason
career_grade was chosen in the first place.

The rule was stated and then broken anyway, because nothing enforced it.
frontend/src/app/page.tsx filtered recent trades to those carrying a pit_grade
and rendered it as a coloured badge, and the insider OG card preferred
best_pit_grade over best_career_grade. Measured 2026-08-21 over filings since
January:

    34,198  showed a letter on the homepage where the insider page said Unrated
    18,241  of those were labelled "D"
    13,767  had a career_grade that disagreed outright

Two things make the "D" cases worse than a mismatch. The published scale has no
D — _GRADE_DISPLAY merges it into C because the two do not separate. And
ratings.py measures Unrated buys at +1.41% against C at -0.38% and D at -0.18%,
so labelling them D inverts the meaning rather than merely differing from it.

The parity test covers the rating FUNCTIONS. It cannot see a component that
bypasses them, which is what happened. This closes that gap.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "frontend" / "src"

#: Files allowed to mention pit_grade, each for a stated reason.
ALLOWED = {
    # Declares the field on a shared payload type. Declaring it is not
    # rendering it, and other views legitimately receive it.
    "lib/types.ts",
    # Falls back to pit_grade ONLY when career_grade is absent, which is the
    # documented last-resort path in insider_rating() for rows predating the
    # career scorer.
    "app/clusters/page.tsx",
    # Admin is internal and SHOULD show the proprietary inputs — that is the
    # point of an admin page. The rule is about what subscribers are shown.
    "app/admin/strategies/[name]/page.tsx",
    # Names the field in a comment explaining what min_grade selects on.
    "components/feed-filters.tsx",
    # The badge component itself documents why pit_grade is not the scale.
    "components/insider-grade-badge.tsx",
}

#: A read is a property access: `t.pit_grade`, `profile.best_pit_grade`.
#: Declaring the field on a type, or naming it in prose, is neither.
_READ = re.compile(r"\.(?:best_)?pit_grade\b")


def _tsx_files() -> list[Path]:
    return [
        p for p in SRC.rglob("*.ts*")
        if "node_modules" not in p.parts and p.suffix in (".ts", ".tsx")
    ]


def test_the_frontend_tree_is_where_we_think_it_is():
    files = _tsx_files()
    assert len(files) > 50, f"only found {len(files)} frontend files — bad path?"


def test_no_component_renders_pit_grade():
    offenders: list[str] = []
    for path in _tsx_files():
        rel = path.relative_to(SRC).as_posix()
        if rel in ALLOWED:
            continue
        text = path.read_text(errors="ignore")
        if "pit_grade" not in text:
            continue
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith(("*", "//", "/*")):
                continue
            # A READ is a property access — `t.pit_grade`, `x.best_pit_grade`.
            # A type declaration (`pit_grade?: string | null;`) is not a read,
            # and prose in a block comment is not either; both mention the name
            # without rendering anything.
            if _READ.search(line):
                offenders.append(f"{rel}: {line.strip()[:90]}")
    assert not offenders, (
        "pit_grade is being read outside the allowed files:\n  "
        + "\n  ".join(offenders)
        + "\n\nUse insiderRating(career_grade) from lib/ratings. pit_grade is not "
          "monotonic and the published scale has no D; rendering it showed 34,198 "
          "filings a letter the insider page did not agree with."
    )


def test_the_homepage_does_not_filter_on_a_grade():
    """Filtering recent trades to graded insiders dropped ~44% of candidates
    and biased the page toward filers with history at that company. Unrated is
    a real rating and outperforms every measured grade below A."""
    page = (SRC / "app" / "page.tsx").read_text()
    assert ".filter((t: RecentTrade) => Boolean(t.pit_grade))" not in page
    assert "Boolean(t.career_grade)" not in page, (
        "the homepage is filtering on career_grade now — same bias, different "
        "column. Show Unrated instead."
    )


def test_the_og_card_prefers_career_grade():
    og = (SRC / "app" / "insider" / "[id]" / "opengraph-image.tsx").read_text()
    assert "best_pit_grade" not in og, (
        "the insider OG card is reading best_pit_grade again. A share card "
        "showing a different letter from the page it links to is worse than "
        "showing none."
    )
    assert "best_career_grade" in og
