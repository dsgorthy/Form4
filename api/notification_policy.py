"""Which notifications are worth interrupting someone for.

THE ONE DEFINITION. Every surface that decides whether something becomes an
email reads this module. Do not re-type a tier, a cap or a window anywhere
else; `tests/unit/test_notification_policy.py` fails the build on drift.

--------------------------------------------------------------------------
THE MODEL: Event -> Notification -> Delivery
--------------------------------------------------------------------------

Three layers, and the mistake this module exists to correct was collapsing
the last two into one:

  Event         something happened in the world. User-independent.
                A filing, a spike, a strategy entry.

  Notification  a user-scoped fact: an event matched a subscription. This is
                the IN-APP FEED. It is pulled, not pushed. It costs the user
                nothing to receive and a complete history is a feature, so it
                can be generous.

  Delivery      a channel-specific attempt to interrupt the user. Email is
                pushed. It costs attention every single time, and the cost is
                paid whether or not the content was worth it. It must be
                scarce.

`notifications.emailed` fused Notification and Delivery into one row and one
boolean. That is why every fix to one broke the other: capping email volume
meant capping the feed (DAILY_CAP=50 throttled CREATION), and unblocking
email meant unblocking five months of backlog at once.

--------------------------------------------------------------------------
WHY THESE TIERS: measured, not guessed
--------------------------------------------------------------------------

Read rate against volume, all 6,920 notifications ever created (2026-08-24):

    event_type              total    % ever read
    high_value_filing          68          57.4
    cluster_formation         293          24.9
    activity_spike          6,378          12.8      <- 92% of everything
    congress_convergence      175          10.3
    portfolio_alert             4           0.0      <- see note below
    watchlist_activity          2           0.0

**Read rate is inversely proportional to volume.** The type sent about once a
day is read 57% of the time; the type sent eighty times a day is read 12.8%.
That is not a coincidence and it is not fixable by writing better subject
lines -- attention is the scarce resource, and volume is what consumes it.

So the tiers below are set by measured attention, with two overrides:

  - `watchlist_activity` and `portfolio_alert` have almost no history (2 and 4
    rows) so their read rates say nothing. They are tiered on what they ARE:
    a filing on a ticker the user personally chose to follow, and an entry or
    exit in a published strategy. Both are the product's actual promise.
  - `portfolio_alert` having 4 rows ever, none since March, is itself a
    finding: the three live strategies alert through a different path and
    never reach this table.
"""
from __future__ import annotations

from enum import Enum


class Tier(str, Enum):
    """How much interruption an event type has earned."""

    #: Email promptly, on its own. Reserved for things that are the reason
    #: someone subscribed, and that are rare enough to stay rare.
    DIRECT = "direct"

    #: Batch into the daily digest. Worth knowing, not worth interrupting for.
    DIGEST = "digest"

    #: In-app only. Never generates an email under any preference. These are
    #: ambient market colour: useful to scroll, worthless to be told.
    FEED_ONLY = "feed_only"


#: event_type -> tier. The single source of truth.
TIERS: dict[str, Tier] = {
    # The product's promise. A strategy took or closed a position, or a
    # filing landed on something this user explicitly chose to follow.
    "portfolio_alert": Tier.DIRECT,
    "watchlist_activity": Tier.DIRECT,

    # Read 57% and 25% of the time respectively, at low volume.
    "high_value_filing": Tier.DIGEST,
    "cluster_formation": Tier.DIGEST,

    # 6,378 rows at 12.8% read, and 175 at 10.3%. Between them they are 95%
    # of everything ever created and essentially none of what anyone opens.
    "activity_spike": Tier.FEED_ONLY,
    "congress_convergence": Tier.FEED_ONLY,
}

#: An unknown event type is FEED_ONLY. New detectors are usually noisy before
#: they are tuned, and the failure this module exists to prevent came from a
#: detector nobody had measured. Earn the email.
DEFAULT_TIER = Tier.FEED_ONLY

#: Hard ceiling on emails per user per day, across every tier. A DIRECT event
#: storm must degrade into a digest rather than into thirty emails.
MAX_EMAILS_PER_USER_PER_DAY = 4

#: Above this many DIRECT items pending at once, stop sending them singly and
#: fold them into the digest instead.
DIRECT_COLLAPSE_THRESHOLD = 3


def tier_of(event_type: str) -> Tier:
    return TIERS.get(event_type, DEFAULT_TIER)


def may_email(event_type: str) -> bool:
    """Is this event type ever allowed to become an email?"""
    return tier_of(event_type) is not Tier.FEED_ONLY


def emailable_types() -> tuple[str, ...]:
    """Every event type that can reach an inbox. Used to build queries."""
    return tuple(sorted(t for t in TIERS if may_email(t)))


def is_direct(event_type: str) -> bool:
    return tier_of(event_type) is Tier.DIRECT
