"""The insider page has a hierarchy now. Keep it.

WHAT WAS WRONG

Reported as "pretty dull and boxy", and the measurement backed it: THIRTEEN
sections on one page shared the identical container —

    rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4

— wrapping the stat tiles, the track record, companies, related insiders, the
entity group and the volume breakdown alike. Four type sizes were in use and
none of them was a display size. Nothing on the page claimed to matter more
than anything else, and search visitors read 1.15 pages before leaving.

WHAT REPLACED IT

Rules and whitespace instead of enclosure, a serif display face on the name,
and an opening that states the conclusion before the tables detail it.

ONE box survives on purpose: the gated track-record block. That is an offer
rather than content, and the enclosure is what marks it as a separate thing.
The three in the 403 branch are loading skeletons, where a box is correct.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PAGE_PATH = REPO / "frontend" / "src" / "app" / "insider" / "[id]" / "page.tsx"
PAGE = PAGE_PATH.read_text(encoding="utf-8")
FILING = (REPO / "frontend" / "src" / "app" / "filing" / "[id]" / "page.tsx").read_text(encoding="utf-8")
LABEL = REPO / "frontend" / "src" / "components" / "ui" / "section-label.tsx"
VERDICT = (REPO / "frontend" / "src" / "components" / "insider-verdict.tsx").read_text(encoding="utf-8")
LAYOUT = (REPO / "frontend" / "src" / "app" / "layout.tsx").read_text(encoding="utf-8")

BOX = "rounded-lg border border-[#2A2A3A]"


def test_the_page_is_not_a_wall_of_identical_boxes():
    """The exact regression. Thirteen was the count that got it called dull."""
    n = PAGE.count(BOX)
    assert n <= 4, (
        f"{n} sections now share the identical `{BOX}` container. It was 13, "
        "which is what made the page read as boxy, and the redesign replaced "
        "enclosure with rules and whitespace. If a new section needs "
        "separating, give it a SectionLabel — that carries a rule."
    )


def test_the_filing_page_is_not_a_wall_of_boxes_either():
    """It is the MOST crawled surface — 2,263 Googlebot requests in 7 days
    against 508 for insider pages — and carried the same ten containers."""
    n = FILING.count(BOX)
    assert n <= 5, (
        f"{n} containers on the filing page. It was 10. The ones that may "
        "keep a border are scrollable table wrappers and buttons; content "
        "panels are separated by SectionLabel's rule."
    )


def test_section_headings_carry_a_rule():
    """SectionLabel is the separator now, so it has to actually separate."""
    assert LABEL.exists(), "the shared SectionLabel is gone"
    assert "border-b" in LABEL.read_text(encoding="utf-8"), (
        "SectionLabel lost its rule. It is what replaced thirteen borders; "
        "without it the de-boxed sections run together."
    )


def test_section_label_has_exactly_one_definition():
    """There were EIGHT copies — five pages and three components — already
    drifting on margin, and restyling the insider page made a ninth. A
    separator that means different things on different pages is not a
    separator."""
    src = REPO / "frontend" / "src"
    dupes = [
        p.relative_to(src).as_posix()
        for p in src.rglob("*.tsx")
        if ".next" not in p.parts
        and p != LABEL                       # the canonical one, obviously
        and "function SectionLabel(" in p.read_text(encoding="utf-8")
    ]
    assert dupes == [], (
        "SectionLabel is defined locally again in: " + ", ".join(dupes) +
        ". Import it from @/components/ui/section-label instead."
    )


def test_the_name_is_a_display_size():
    assert re.search(r'<h1[^>]*font-serif[^>]*text-\[(\d+)px\]', PAGE), (
        "the insider name is no longer serif display type. It was 24px bold "
        "in a page whose largest other text was 20px, so nothing anchored it."
    )


def test_the_serif_is_actually_loaded():
    """A font-serif class with no face behind it falls back silently."""
    assert "Newsreader" in LAYOUT, "the serif is no longer imported"
    assert '--font-serif' in LAYOUT, "the serif has no CSS variable"
    assert "newsreader.variable" in LAYOUT, (
        "the serif variable is never applied to <body>, so font-serif resolves "
        "to the browser default"
    )


# ── the verdict sentence must not overclaim ────────────────────────────────

def test_the_verdict_reports_history_and_never_predicts():
    body = re.sub(r"/\*[\s\S]*?\*/", "", VERDICT)
    for banned in ("expect", "should ", "likely", "predict", "forecast",
                   "will outperform", "buy signal"):
        assert banned not in body.lower(), (
            f"the verdict sentence says {banned!r}. It states what happened "
            "after these purchases and nothing else — our grades do not "
            "predict forward returns."
        )


def test_the_purchase_total_counts_open_market_buys_only():
    """volume_by_type also carries Award/Grant and option exercises. A
    sentence reading 'purchases totalling $66.9M' must not be summing stock a
    board handed them."""
    assert 'trans_code === "P"' in PAGE, (
        "the verdict's purchase total is no longer filtered to trans_code 'P'"
    )


def test_the_verdict_publishes_its_denominator():
    assert "scored purchases" in VERDICT, (
        "the meter block no longer states how many filings it is built from"
    )


def test_the_verdict_renders_nothing_without_a_record():
    """An insider with no scored buys must get a sentence, not a broken one."""
    assert "Too few scored purchases" in VERDICT, (
        "the no-data branch is gone; a thin profile would render a sentence "
        "with a blank where the number should be"
    )


def test_the_meters_have_a_floored_domain():
    """Without a floor, a quiet record renders as three maxed-out bars and
    reads as a catastrophe."""
    assert re.search(r"Math\.max\(0\.2", VERDICT), (
        "the meter domain floor is gone"
    )


# ── the top of the page states each fact once ──────────────────────────────

def test_the_grade_is_not_rendered_twice():
    """It was: an InsiderGradeBadge beside the H1 and a 62px glyph in the
    verdict block 200px below, the same claim on two scales. That is the exact
    thing InsiderGradeBadge's own "one rating" note exists to prevent, and it
    was reintroduced by adding the verdict without removing the badge."""
    header = PAGE[PAGE.index("{/* Header"):PAGE.index("<InsiderVerdict")]
    assert "<InsiderGradeBadge" not in header, (
        "the grade badge is back beside the H1 while the verdict block still "
        "renders the rating glyph. Keep one — the glyph, which is captioned "
        "and sits with the meters it summarises."
    )


def test_the_role_is_not_repeated_under_the_summary():
    """InsiderSummary already renders 'X is Director & 10% Owner at BiomX
    Inc. (PHGE)'. A fragment repeating it directly below was three of the same
    facts on one screen."""
    header = PAGE[PAGE.index("{/* Header"):PAGE.index("<InsiderVerdict")]
    assert "skipTitle" not in header, (
        "the standalone title line is back under the summary sentence, which "
        "already contains the role and the primary company"
    )


def test_the_company_count_appears_once_above_the_fold():
    header = PAGE[PAGE.index("{/* Header"):PAGE.index("<InsiderVerdict")]
    assert header.count('"company" : "companies"') == 0, (
        "the company count is being rendered as a fragment again; "
        "InsiderSummary already says 'across N companies'"
    )
