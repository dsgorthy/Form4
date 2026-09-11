"""A null tier floor means NO floor, and must never be compared as a number.

`_get_subscribed_users` sets `min_insider_tier = None` for every non-Pro
account on purpose: applying our own grade filter to a free user's alerts would
silently drop filings on companies they explicitly asked to hear about. Free
means the raw event.

`scan_high_value_filings` then compared `r_tier < user["min_insider_tier"]`
directly, which raises TypeError the moment a non-Pro user is subscribed:

    TypeError: '<' not supported between instances of 'int' and 'NoneType'

It crashed on **every one of 4,182 runs from 2026-08-24, with zero successes**.
Because it is third of six scanners and `_scan` runs them in sequence,
congress_convergence, cluster_formation and activity_spike never executed once
in that window either — four of six notification types silently produced
nothing for seventeen days.

Two things kept it invisible, and both are worth remembering:

  - The plist exited non-zero into a log nobody tailed. The Dagster migration
    on 2026-09-10 surfaced it within a single scheduling tick, which is the
    whole argument for that migration.
  - It is only reachable when a subscribed account is NOT Pro. It became live
    when the last two subscriptions ended (one refunded 2026-08-25, one charged
    back 2026-08-30). A gate exercised only by free users is a gate a paying
    userbase hides.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.notification_scanner import tier_allows  # noqa: E402


@pytest.mark.parametrize("row_tier", [1, 2, 3])
def test_none_floor_admits_every_tier(row_tier):
    """The regression itself. None is 'no filter', not 'filter at zero'."""
    assert tier_allows({"min_insider_tier": None}, row_tier) is True


@pytest.mark.parametrize("row_tier", [1, 2, 3])
def test_missing_key_admits_every_tier(row_tier):
    """A user dict assembled by a different path must not crash either."""
    assert tier_allows({}, row_tier) is True


def test_floor_still_filters_for_pro_users():
    """The None case must not have been bought by disabling the gate.

    A Pro account that set a floor of 3 keeps it: tier 2 is refused, tier 3
    admitted. Making tier_allows always return True would pass the tests above
    and quietly send every subscriber everything.
    """
    user = {"min_insider_tier": 3}
    assert tier_allows(user, 3) is True
    assert tier_allows(user, 2) is False
    assert tier_allows(user, 1) is False


def test_floor_is_inclusive_at_the_boundary():
    """`>=`, matching the original `not (r_tier < floor)`.

    An off-by-one here silently drops exactly the rows sitting on a user's
    chosen threshold — the ones they most expected to receive.
    """
    assert tier_allows({"min_insider_tier": 2}, 2) is True
    assert tier_allows({"min_insider_tier": 2}, 1) is False


def test_scanner_does_not_compare_the_floor_directly():
    """Pin the call site too.

    The helper is only a fix while the crashing comparison is gone. A future
    edit that reintroduces `r_tier < user["min_insider_tier"]` anywhere brings
    back a 17-day silent outage.
    """
    src = (Path(__file__).resolve().parents[2]
           / "pipelines" / "notification_scanner.py").read_text()
    assert 'r_tier < user["min_insider_tier"]' not in src, (
        "a direct comparison against min_insider_tier is back; it raises "
        "TypeError for every non-Pro subscriber. Use tier_allows()."
    )
    assert "tier_allows(user, r_tier)" in src, "the call site no longer uses tier_allows"
