"""Guards on what an anonymous visitor is allowed to see.

The insider profile endpoint used to null its entire `track_record` for non-Pro
viewers, which took the trade counts down with the scores and left the public
page with a name and nothing else. It now returns an allowlisted subset. That
subset is the boundary between the free surface and the paid product, so it gets
a test rather than a code review.

Imports only `api.public_fields`, which pulls in nothing — these run without
FastAPI, a database, or the app.
"""

import re

from api.public_fields import (
    OUTCOME_MARKERS,
    PUBLIC_VOLUME_FIELDS,
    TRACK_RECORD_FIELDS,
)


def test_public_and_gated_sets_are_disjoint():
    """A field cannot be both published and withheld."""
    overlap = set(PUBLIC_VOLUME_FIELDS) & set(TRACK_RECORD_FIELDS)
    assert not overlap, f"published AND gated: {sorted(overlap)}"


def test_no_outcome_measure_is_public():
    """The real invariant: volume is public, performance is not.

    Disjointness alone is too weak — adding "buy_win_rate_60d" to the public
    list would pass it, because that field is not in TRACK_RECORD_FIELDS
    either. This checks the *shape* of the name instead, so a newly computed
    performance metric cannot be published by being new.
    """
    leaked = [
        f
        for f in PUBLIC_VOLUME_FIELDS
        if any(marker in f for marker in OUTCOME_MARKERS)
    ]
    assert not leaked, (
        f"outcome measures in the public set: {leaked}. "
        "Performance is the product; only volume is public."
    )


def test_public_set_is_an_allowlist_not_a_denylist():
    """Rejecting by name would publish every future column by default."""
    src = (
        re.sub(r"\s+", " ", open("api/routers/insiders.py").read())
    )
    assert "for k in PUBLIC_VOLUME_FIELDS if k in tr_full" in src, (
        "the insider profile must build its public track_record by iterating "
        "the allowlist, not by deleting known-bad keys"
    )


def test_counts_survive_for_anonymous_viewers():
    """The specific regression: buy/sell counts are what the summary sentence
    and Google's snippet are built from, so they must be public."""
    for field in ("buy_count", "sell_count", "n_tickers"):
        assert field in PUBLIC_VOLUME_FIELDS, f"{field} must stay public"
