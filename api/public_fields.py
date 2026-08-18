"""Which insider track-record fields are public and which are Pro-only.

Deliberately dependency-free — no FastAPI, no DB — so the invariant below can be
tested without standing up the app. `api.gating` re-exports TRACK_RECORD_FIELDS
so existing importers are unaffected.

The split is one line: **volume is public, outcomes are not.**

  Public   what the insider DID — how many buys, how many sells, how many
           tickers, over what span, under what title. Every competitor
           publishes these, they are the figures Google quotes as the result
           snippet, and withholding them left our pages with a name and
           nothing else.

  Pro      how well it WORKED — scores, percentiles, win rates, returns,
           alpha. This is the analysis we compute, and it is the product.

A field that is neither is Pro by default: PUBLIC_VOLUME_FIELDS is an allowlist,
so anything new added to the table stays hidden until someone deliberately puts
it here.
"""

# Nulled for free-tier users. score_tier stays visible as a teaser.
TRACK_RECORD_FIELDS = [
    "score",
    "percentile",
    "buy_win_rate_7d",
    "buy_avg_return_7d",
    "buy_avg_abnormal_7d",
    "sell_win_rate_7d",
    "sell_avg_return_7d",
    "sell_avg_abnormal_7d",
    "return_7d",
    "return_30d",
    "return_90d",
    "abnormal_7d",
    "abnormal_30d",
    "abnormal_90d",
    "score_recency_weighted",
    "tier_recency",
    "pit_blended_score",
]

# Survives on the insider profile for anonymous and free-tier viewers.
PUBLIC_VOLUME_FIELDS = (
    "buy_count",
    "sell_count",
    "n_tickers",
    "primary_title",
    "primary_ticker",
    "buy_first_date",
    "buy_last_date",
    "sell_first_date",
    "sell_last_date",
)

# Substrings that mark a field as an outcome measure rather than a volume one.
# Used by the test that guards the allowlist; kept here so the rule lives next
# to the lists it describes.
OUTCOME_MARKERS = (
    "score",
    "percentile",
    "win_rate",
    "return",
    "abnormal",
    "alpha",
    "tier",
)


# ── where the alert line sits ───────────────────────────────────────────────
#
# The event is free. The judgment is paid.
#
# Being told that someone filed on a company you follow is a fact about the
# world, and it is what brings a visitor back, so it costs nothing. Everything
# that requires Form4 to have an opinion — a grade, a cluster, a spike, a
# convergence, a strategy entry — is the product, and so is the ability to
# filter alerts by any of it. "Tell me when an insider trades NVDA" is free;
# "tell me when an A+ insider trades NVDA" is not.
#
# min_trade_value and high_value_filing stay free deliberately: a dollar
# threshold is a fact about the filing, not a view about it, and a user
# narrowing their own alerts costs us less mail rather than more.

#: Alert event types any signed-in account may enable.
FREE_ALERT_EVENTS = ("watchlist_activity", "high_value_filing")

#: Alert event types that exist only because we computed something.
PRO_ALERT_EVENTS = (
    "cluster_formation",      # our clustering
    "activity_spike",         # our baseline and threshold
    "congress_convergence",   # our cross-source join
    "portfolio_alert",        # our strategies
)

#: Preference fields that filter alerts by our own scoring.
PRO_ALERT_FILTERS = ("min_insider_tier",)

#: Everything a free account may set, so the check is an allowlist and a new
#: field is Pro until someone deliberately says otherwise.
FREE_ALERT_FIELDS = frozenset(
    FREE_ALERT_EVENTS
    + ("email_enabled", "in_app_enabled", "email_frequency", "min_trade_value")
)

PRO_ALERT_FIELDS = frozenset(PRO_ALERT_EVENTS + PRO_ALERT_FILTERS)


# ─── Strategy identity ───────────────────────────────────────────────────────
#
# One registry for the internal key, the public name, and whether we still run
# the thing. Before this existed the display names were retyped in nine places
# — the landing page, onboarding, the portfolio switcher, two content
# generators, two admin routers, the research methodology page and the strategy
# yamls — so renaming a strategy meant finding all nine, and the 2026-08-18
# rename found three that had already drifted.
#
# The KEY IS NOT THE NAME. Keys are written into strategy_portfolio.strategy,
# launchd plist labels, yaml filenames and env-var prefixes; renaming one is a
# data migration, and the user-visible string is the only part that has to
# change. So the keys stay as they were and LABEL carries the product name.
#
# The names, and why:
#
#   The A-List (quality_notrend)
#       An insider with a graded record buys. There is no chart condition at
#       all — the person is the entire signal, which is what the name has to
#       say. This is the strongest book of the three.
#
#   Tailwind (quality_momentum)
#       Same graded insider, but the stock is already above its 50- and
#       200-day averages: conviction and price pointing the same way.
#       Deliberately NOT "Breakout". Nothing in this strategy tests for a
#       breakout — a stock can sit above both averages for a year and still
#       qualify. Naming an alert after a chart event we do not measure is the
#       kind of thing a subscriber notices once and then stops trusting the
#       rest of the product.
#
#   Change of Heart (reversal_dip)
#       An insider whose record is nothing but sells finally buys, into a
#       stock down 25%. Both halves of the reversal in two words.

STRATEGIES = {
    "quality_notrend": {
        "label": "The A-List",
        "thesis": "A proven insider buys. No chart condition.",
        "active": True,
    },
    "quality_momentum": {
        "label": "Tailwind",
        "thesis": "A proven insider buys a stock already trending up",
        "active": True,
    },
    "reversal_dip": {
        "label": "Change of Heart",
        "thesis": "A serial seller finally buys, into a 25% drawdown",
        "active": True,
    },
    # Retired 2026-08-18. Sharpe 0.68 against 1.08 for the weakest survivor,
    # and the edge was a story rather than a result. The runner is unloaded and
    # the name is off every public surface; the config, the PIT strategy class
    # and the ~200 simulated rows stay so the decision stays reversible.
    "tenb51_surprise": {
        "label": "10b5-1 Surprise",
        "thesis": "A scheduled seller breaks pattern to buy",
        "active": False,
    },
}

# Publication order. The A-List leads because it is the strongest book and the
# landing page reads the first entry for its hero chart.
ACTIVE_STRATEGIES = ["quality_notrend", "quality_momentum", "reversal_dip"]

STRATEGY_LABELS = {k: v["label"] for k, v in STRATEGIES.items()}


def strategy_label(key: str) -> str:
    """Public name for a strategy key, falling back to the key itself."""
    entry = STRATEGIES.get(key)
    return entry["label"] if entry else key
