"""Posts must be handed over as a schedule, not a block.

WHAT WENT WRONG, TWICE

  2026-08-24  ten posts published in one sitting  -> account suspended
  2026-08-28  three published "within a few seconds of each other" -> flagged

After the first, the CONTENT was audited: no links, no promotion, no irrelevant
cashtags, median pairwise structural similarity 45%, 50 of 50 distinct opening
lines. Stocktwits' own rules welcome bots that are "data feeds". The cause was
recorded as unknown and a daily cap was added.

The daily cap does not address this at all. Five posts inside one minute is
five posts; MAX_POSTS_PER_DAY is satisfied and the account still looks like a
queue draining. Two observations now point the same way: the trigger is
VELOCITY.

There is no posting API here -- a human copies the file to Stocktwits by hand --
so the only lever the generator has is how it PRESENTS the work. It therefore
stamps each post with a time and refuses to render them as an undifferentiated
block.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GEN = REPO / "pipelines" / "generate_stocktwits_posts.py"
SRC = GEN.read_text(encoding="utf-8")


def _const(name: str):
    for node in ast.parse(SRC).body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} is gone from {GEN.name}")


def test_a_spacing_constant_exists_and_is_meaningful():
    gap = _const("MIN_MINUTES_BETWEEN_POSTS")
    assert isinstance(gap, int) and gap >= 15, (
        f"MIN_MINUTES_BETWEEN_POSTS is {gap!r}. Anything under a quarter hour "
        "does not distinguish a person from a queue, which is what got the "
        "account flagged on 2026-08-24 and again on 2026-08-28."
    )


def test_the_gap_spreads_a_full_day_across_hours_not_minutes():
    gap = _const("MIN_MINUTES_BETWEEN_POSTS")
    cap = _const("MAX_POSTS_PER_DAY")
    span_hours = gap * (cap - 1) / 60.0
    assert span_hours >= 2.0, (
        f"a full day of {cap} posts spans only {span_hours:.1f}h. The daily cap "
        "alone never prevented this -- five posts inside a minute satisfies it."
    )


def test_each_rendered_post_carries_a_time():
    """A header saying only 'POST 3/5' invites pasting all five at once."""
    block = SRC[SRC.index("POST {i}/{len(picked)}") - 400:]
    block = block[:block.index("print(post)") + 20]
    assert "MIN_MINUTES_BETWEEN_POSTS" in block, (
        "the per-post header no longer staggers by the spacing constant"
    )
    assert re.search(r"POST AT|POST NOW", block), (
        "the header must carry a concrete time, not a general instruction"
    )


def test_the_reason_is_written_down_next_to_the_constant():
    """Losing the why is how a later reader tunes this back to zero."""
    where = SRC.index("MIN_MINUTES_BETWEEN_POSTS")
    context = SRC[max(0, where - 2500):where]
    assert "2026-08-24" in context and "2026-08-28" in context, (
        "both flagging incidents must be recorded beside the constant"
    )
    assert "VELOCITY" in context or "velocity" in context, (
        "the note must say what the trigger actually was"
    )
