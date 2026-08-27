"""Two invocations on one day must not be two batches of posts.

WHAT WENT WRONG

Stocktwits suspended the account on 2026-08-26. Nothing in the content broke a
published rule — across the 53 posts there were no links, no promotion, no
irrelevant cashtags, and a median pairwise structural similarity of 45% with
50 of 50 distinct opening lines, so "the same message or nearly identical ones
repeatedly" did not describe them either. Their rules explicitly welcome bots
that are "data feeds".

What happened is that on 2026-08-24 the generator RAN TWICE, at 18:08 and
19:10, and put out 20 posts across 19 cashtags in 62 minutes from an account
four days old. `record_posts` is idempotent per FILING, so the second run
posted no duplicates — it took the next ten of the sixty candidates that had
cleared the notability bar. The repeat guard did its job. Nothing objected to
a second batch, because nothing was counting the day.

THE TWO PROPERTIES

  MAX_POSTS_PER_DAY      counted against social_posts, not against this
                         process, so re-running is a no-op rather than a
                         top-up of fresh material.
  TICKER_COOLDOWN_DAYS   a floor under how often one cashtag can reappear,
                         with no escape hatch — deliberately unlike
                         REPOST_QUIET_DAYS, which lets a new insider or a
                         1.5x bigger programme through.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
GEN = REPO / "pipelines" / "generate_stocktwits_posts.py"
SRC = GEN.read_text(encoding="utf-8")


def _const(name: str):
    """Read a module-level constant without importing (the module needs a DB)."""
    tree = ast.parse(SRC)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} is gone from {GEN.name}")


# ── the caps exist and are sane ────────────────────────────────────────────

def test_a_daily_cap_exists():
    cap = _const("MAX_POSTS_PER_DAY")
    assert isinstance(cap, int) and 1 <= cap <= 10, (
        f"MAX_POSTS_PER_DAY is {cap!r}. The account was suspended after 20 posts "
        "in 62 minutes; this is the ceiling that prevents it."
    )


def test_a_ticker_cooldown_exists_and_outlasts_the_story_guard():
    cooldown = _const("TICKER_COOLDOWN_DAYS")
    quiet = _const("REPOST_QUIET_DAYS")
    assert cooldown >= quiet, (
        f"TICKER_COOLDOWN_DAYS ({cooldown}) is shorter than REPOST_QUIET_DAYS "
        f"({quiet}). The cooldown is meant to be the floor UNDER the story "
        "guard, not a weaker version of it — REPOST_QUIET_DAYS can be waived "
        "by a new insider or a bigger programme, and this cannot."
    )


# ── the cap is counted against the DAY, not against the process ────────────

def test_the_cap_is_measured_against_what_is_already_recorded():
    """A process-local counter would not have stopped the 08-24 second run."""
    assert "posts_already_today" in SRC, (
        "nothing reads how many posts already went out today, so a second "
        "invocation starts from zero — which is exactly what happened"
    )
    fn = SRC[SRC.index("def posts_already_today"):]
    fn = fn[:fn.index("\ndef ")]
    # The SQL lives in a module constant, so follow the indirection rather
    # than insisting the string sits inline.
    sql = _const("CTX_TODAY_COUNT") if "CTX_TODAY_COUNT" in fn else fn
    assert "social_posts" in sql, (
        "posts_already_today must count rows in social_posts. Counting "
        "anything process-local re-introduces the defect."
    )
    assert "posted_at" in sql, "the count must be scoped to a calendar day"


def test_a_second_run_on_a_full_day_returns_without_posting():
    body = SRC[SRC.index("    already = posts_already_today"):]
    body = body[:body.index("notable = ")]
    assert "return 0" in body, (
        "when the day's budget is exhausted the generator must return, not "
        "carry on and post the next N candidates"
    )
    assert re.search(r"budget\s*=\s*MAX_POSTS_PER_DAY\s*-\s*already", body), (
        "the budget must be the cap MINUS what already went out"
    )


# ── the cooldown is applied, and cannot be argued with ─────────────────────

def test_the_cooldown_is_actually_applied_to_candidates():
    assert "tickers_in_cooldown" in SRC
    assert re.search(r"r\[.ticker.\]\s+not\s+in\s+cooling", SRC), (
        "tickers_in_cooldown is computed but never filters the candidate list"
    )


def test_the_cooldown_runs_after_the_story_guard():
    """Order matters: the story guard can WAIVE a repeat, so the hard floor
    has to come after it or a waived candidate walks straight through."""
    story = SRC.index("is_repeat_worth_posting(r, day)")
    hard = SRC.index("not in cooling")
    assert story < hard, (
        "the ticker cooldown must be applied AFTER is_repeat_worth_posting, "
        "otherwise a candidate waved through on a new insider bypasses it"
    )


@pytest.mark.parametrize("name", ["MAX_POSTS_PER_DAY", "TICKER_COOLDOWN_DAYS"])
def test_the_reason_is_written_down(name: str):
    """Both numbers were chosen from measurements. Losing the why is how a
    later reader 'optimises' them back to what got us banned."""
    where = SRC.index(name)
    context = SRC[max(0, where - 2000):where]
    assert "2026-08-24" in context or "suspend" in context.lower(), (
        f"{name} has no note near it explaining what it is defending against"
    )
