"""What the trade detail page is allowed to say, and what it must get right.

Derek opened /portfolio/trades/{id} for FUNC and asked "why did we enter this
trade then?" — a fair question, because the page answered it with
"Conviction 1.5/10" beside a career grade of A.

Four defects, all on one screen:

  1. CONVICTION WAS PUBLISHED. api/ratings.py lists it in INTERNAL_ONLY_FIELDS
     and states why: it is a strategy's own entry threshold with a floor of
     1.5, so 1.5 means CLEARED THE BAR, not "scored 1.5 out of 10". Rendered as
     a fraction it inverts its own meaning. This is the same complaint Derek
     raised on the filing page in August ("1.5/10 is grade A?"); it was fixed
     there and left standing here.

  2. TARGET HOLD READ 947 DAYS. The simulator wrote target_exit_idx — a
     position in the trading-day calendar array — instead of the hold length,
     but only on the OPEN-position insert. Closed rows were correct at 29-92,
     so every open position on the site reported a ~950-day target against a
     42-day thesis.

  3. THE INSIDER LINK 404'd. It used the raw numeric row id, and
     /insider/{identifier} resolves a slug, a retired slug, an encoded sqid or
     a CIK — never a bare id.

  4. THE HARD STOP READ "50%". stop_pct is stored as a magnitude, so a floor
     rendered as though it were a target.

The page had the real answer all along: entry_reasoning carries the thesis and
the career grade the entry was actually made on.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PAGE = REPO / "frontend" / "src" / "app" / "portfolio" / "trades" / "[id]" / "page.tsx"
ROUTER = REPO / "api" / "routers" / "portfolio.py"
SIM = REPO / "pipelines" / "insider_study" / "simulate_strategy_portfolio.py"


def _code_lines(path: Path) -> list[str]:
    """Source with comment-only lines dropped — every defect here is documented
    in a comment naming the thing it removed, and matching those would let the
    tests pass on prose."""
    out = []
    for line in path.read_text().splitlines():
        st = line.strip()
        if st.startswith(("//", "*", "/*", "#")):
            continue
        out.append(line)
    return out


def test_conviction_is_never_rendered():
    """INTERNAL_ONLY_FIELDS is not advisory."""
    offenders = [
        ln.strip()[:96] for ln in _code_lines(PAGE)
        if "signal_quality" in ln and not re.search(r"signal_quality\??:\s*number", ln)
    ]
    assert not offenders, (
        "the trade detail page is rendering conviction again:\n  "
        + "\n  ".join(offenders)
        + "\n\nIt has a floor of 1.5, so '1.5/10' means 'cleared the bar' and "
          "reads as 'almost no confidence'. Show entry_reasoning instead."
    )


def test_the_word_conviction_is_not_a_visible_label():
    """Belt and braces: the string itself must not reach a user."""
    offenders = [
        ln.strip()[:96] for ln in _code_lines(PAGE)
        if 'label="Conviction"' in ln or ">Conviction<" in ln
    ]
    assert not offenders, offenders


def test_the_page_explains_the_entry():
    """The question the page exists to answer."""
    src = PAGE.read_text()
    assert "er?.career_grade" in src or "er.career_grade" in src, (
        "the page no longer surfaces the career grade the entry was made on, "
        "which is the actual reason the trade was taken"
    )
    assert "INSIDER_RATING_BLURB" in src, (
        "a bare letter is not an explanation — render the published blurb"
    )


def test_the_strategy_label_comes_from_the_api():
    """api/public_fields is the only place a label may be typed."""
    assert "strategy_label" in ROUTER.read_text(), (
        "the trade detail response must carry strategy_label"
    )
    page = PAGE.read_text()
    for label in ("A-List Buys", "Insider Breakout", "Insider Dip Buys"):
        assert label not in page, f"{label!r} is retyped in the page"


def test_open_positions_get_a_hold_length_not_a_calendar_index():
    """The 947-day bug. Guarded at the source rather than in the renderer,
    because the wrong number was in the database."""
    src = SIM.read_text()
    assert "o.target_exit_idx - 0" not in src, (
        "the open-position insert is writing target_exit_idx into target_hold "
        "again — that is an index into the trading-day calendar, not a hold "
        "length, and it renders as ~950 days on every open position."
    )
    # The insert must supply the configured hold.
    assert re.search(r"hold_td: int = 0", src), (
        "persist_positions no longer takes hold_td, so the open-position "
        "insert cannot write the configured hold"
    )


def test_the_insider_link_uses_a_resolvable_identifier():
    """A raw row id is not one of the four things the resolver accepts."""
    src = PAGE.read_text()
    assert "insider_slug" in src, "the page must prefer the slug"
    assert not re.search(r"/insider/\$\{trade\.insider_id\}", src), (
        "the page links the raw insider_id again — /insider/{identifier} "
        "resolves a slug, a retired slug, an encoded sqid or a CIK, never a "
        "bare row id, so this is a guaranteed 404"
    )
    assert "encode_insider_id" in ROUTER.read_text(), (
        "the API must encode the id before putting it in a payload that is "
        "used to build URLs"
    )


def test_the_stop_renders_as_a_loss():
    """stop_pct is a magnitude; unsigned it reads as a target."""
    lines = [ln for ln in _code_lines(PAGE) if "Hard Stop" in ln]
    assert lines, "the Hard Stop row is gone"
    assert any("-" in ln and "Math.abs" in ln for ln in lines), (
        f"Hard Stop must render signed, e.g. -50%: {lines}"
    )


def test_the_title_is_cleaned_by_the_shared_definition():
    """"Unknown" is a stored value, not a job. api/titles owns this."""
    src = ROUTER.read_text()
    assert "clean_title" in src, (
        "the trade detail response must run insider_title through "
        "api.titles.clean_title — otherwise 'Unknown' and 'Dir' reach the page "
        "verbatim, as they did until 2026-08-22"
    )
