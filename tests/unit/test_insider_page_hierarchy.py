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


def test_section_headings_carry_a_rule():
    """SectionLabel is the separator now, so it has to actually separate."""
    i = PAGE.index("function SectionLabel(")
    block = PAGE[i:PAGE.index("\n}", i)]
    assert "border-b" in block, (
        "SectionLabel lost its rule. It is what replaced thirteen borders; "
        "without it the sections run together."
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
