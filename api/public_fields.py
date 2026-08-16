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
