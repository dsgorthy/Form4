"""The watchdog must page when something CHANGES, not every thirty minutes.

## What happened

`_finish` pushed a high-priority ntfy on every run that had any problem, and
the watchdog runs every thirty minutes. On 2026-10-01/02 five to eight problems
sat unresolved for about eighteen hours — three hung launchd jobs, a stale
congress feed, a Dagster job that had missed two ticks — so each was re-pushed
roughly 36 times. Derek: "ive gotten a ton of push notifications".

Not one of those pushes carried new information. That is the damage, not the
volume: a pager that repeats itself trains the reader to swipe the whole topic
away, which is how the alert that matters gets missed. It is the same failure
that retired the Tailorly probe on 2026-09-30, and the same one that nearly
shipped in the heap probe earlier that day.

The subtle requirement is `problem_key`: "heartbeat_probe has not run for 375
minutes" is a different STRING every cycle, so change-detection over raw text
would fire every single time and fix nothing.
"""
from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog",
    Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
watchdog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(watchdog)

T0 = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)

# Real problem strings from the 2026-10-02 flood.
HUNG = "heartbeat_probe has not run for 375 minutes (budget 60m); last run 2026-10-01 18:45 PDT"
HUNG_LATER = "heartbeat_probe has not run for 405 minutes (budget 60m); last run 2026-10-01 18:45 PDT"
CONGRESS = "congress is 9d stale (budget 5d, latest 2026-09-23)"
PIPELINE = "form4_pipeline has not succeeded since it was last due (Fri 2026-10-02 00:30Z); last success 32.0h ago"


def test_the_moving_number_is_not_a_new_problem():
    """THE CRUX. Without this the whole mechanism is a no-op."""
    assert watchdog.problem_key(HUNG) == watchdog.problem_key(HUNG_LATER), (
        "a problem whose message carries a growing counter reads as a new "
        "problem every cycle, so every cycle pushes and nothing is fixed"
    )
    assert watchdog.problem_key(CONGRESS) != watchdog.problem_key(HUNG)


def test_a_new_problem_pages():
    should, title, state = watchdog.decide_notification([HUNG], {}, T0)
    assert should
    assert "NEW" in title
    assert list(state["problems"]) == [watchdog.problem_key(HUNG)]
    assert state["last_push"] == T0.isoformat()


def test_the_same_problem_set_does_not_page_again():
    """The 36-identical-pushes bug."""
    _, _, s1 = watchdog.decide_notification([HUNG, CONGRESS], {}, T0)
    # ...thirty minutes later, same two problems, counters moved on
    should, _, s2 = watchdog.decide_notification(
        [HUNG_LATER, CONGRESS], s1, T0 + timedelta(minutes=30))
    assert not should, "re-pushed an unchanged problem set"
    assert s2["last_push"] == s1["last_push"], "last_push moved without a push"


def test_thirty_minute_cycles_for_six_hours_push_once():
    """The actual shape of the incident: one problem, twelve cycles."""
    state, pushes = {}, 0
    for i in range(12):
        msg = f"heartbeat_probe has not run for {60 + i*30} minutes (budget 60m)"
        should, _, state = watchdog.decide_notification(
            [msg], state, T0 + timedelta(minutes=30 * i))
        pushes += int(should)
    assert pushes == 1, f"{pushes} pushes for one unchanged problem over 6 hours"


def test_an_additional_problem_pages():
    _, _, s1 = watchdog.decide_notification([HUNG], {}, T0)
    should, title, _ = watchdog.decide_notification(
        [HUNG_LATER, PIPELINE], s1, T0 + timedelta(minutes=30))
    assert should
    assert "1 NEW" in title


def test_resolution_pages_so_recovery_is_visible():
    _, _, s1 = watchdog.decide_notification([HUNG, CONGRESS], {}, T0)
    should, title, _ = watchdog.decide_notification(
        [CONGRESS], s1, T0 + timedelta(minutes=30))
    assert should, "a problem clearing is news too"
    assert "resolved" in title and "remain" in title


def test_everything_clearing_says_all_clear():
    _, _, s1 = watchdog.decide_notification([HUNG], {}, T0)
    should, title, state = watchdog.decide_notification(
        [], s1, T0 + timedelta(minutes=30))
    assert should
    assert title == "Studio watchdog: all clear"
    assert state["problems"] == {}


def test_a_clean_run_after_a_clean_run_stays_quiet():
    should, _, s1 = watchdog.decide_notification([], {}, T0)
    assert not should, "pushed 'all clear' with nothing having been wrong"
    should, _, _ = watchdog.decide_notification([], s1, T0 + timedelta(hours=20))
    assert not should, "a healthy box must never page, however long it stays up"


def test_a_long_running_problem_is_re_asserted_not_forgotten():
    """Silence must not be indistinguishable from resolution forever."""
    _, _, state = watchdog.decide_notification([HUNG], {}, T0)
    # Just under the window: quiet.
    should, _, state2 = watchdog.decide_notification(
        [HUNG], state, T0 + timedelta(hours=watchdog.REASSERT_HOURS - 1))
    assert not should
    # Past it: one re-assert, carrying the age.
    should, title, _ = watchdog.decide_notification(
        [HUNG], state2, T0 + timedelta(hours=watchdog.REASSERT_HOURS + 1))
    assert should
    assert "still 1 problem" in title
    assert "oldest 13h" in title, title


def test_the_reassert_window_is_long_enough_to_be_quiet():
    assert watchdog.REASSERT_HOURS >= 6, (
        "the re-assert window is short enough to recreate the flood: the "
        "watchdog runs every 30 minutes"
    )


def test_first_seen_survives_so_age_is_real():
    _, _, s1 = watchdog.decide_notification([HUNG], {}, T0)
    _, _, s2 = watchdog.decide_notification(
        [HUNG_LATER], s1, T0 + timedelta(hours=5))
    k = watchdog.problem_key(HUNG)
    assert s2["problems"][k] == s1["problems"][k], (
        "first-seen was overwritten, so a problem never looks old and the "
        "re-assert cannot report its age"
    )


def test_corrupt_state_is_not_fatal():
    """A bad state file must not stop the watchdog from watching."""
    should, _, _ = watchdog.decide_notification([HUNG], {"problems": "junk"}, T0)
    assert should
    should, _, _ = watchdog.decide_notification(
        [HUNG], {"problems": {}, "last_push": "not-a-date"}, T0)
    assert should, "an unparseable last_push must fall back to pushing"
