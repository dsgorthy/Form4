"""Declared features, and the generators that explode them.

WHY A REGISTRY

`trades` carries 102 columns. Every feature added there is a migration, a lock
on the table the API reads, and a separate coverage question — and the
2026-08-28 migration was refused four times by an unrelated Dagster run before
it applied. That does not survive generating features in bulk.

So features are DECLARED here and MATERIALISED into `trade_features`, which is
long-format (trade_id, feature, value). Adding a feature is an INSERT. Nothing
is ever ALTERed, and a feature that turns out to leak is deleted rather than
dropped.

THE POINT IS THE GENERATORS

Hand-writing features does not scale either. A raw value crossed with a
grouping is a new feature, so twelve raw values against six groupings is
seventy-two features from code written once:

    value                  ->  value_pctile_by_sector
                               value_pctile_by_role
                               value_pctile_by_insider     <- the sharp one
    filing_lag_days        ->  filing_lag_pctile_by_sector
                               ...

That normalisation is not cosmetic. Biotech files slower than banks; $200k is
enormous for a director and routine for a founder. A raw value conflates the
signal with the norm, which is a live hypothesis for why our outcome deciles
were indistinguishable on every raw feature we had.

PIT IS ENFORCED BY THE FRAMEWORK, NOT BY EACH FEATURE

Every feature declares an `anchor`, resolved through framework.features.anchor
and nowhere else. Three look-aheads in one week came from three scripts each
deriving the session for itself.

Percentiles carry a second, sharper trap: a percentile over the whole table
ranks the past using the future. Every percentile feature therefore declares a
TRAILING WINDOW and is computed only against rows whose observation session is
strictly earlier. `window_days=None` is rejected by the builder, not defaulted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

#: Groupings a raw feature can be ranked within. "insider" is the most
#: interesting: "the 95th-percentile purchase of this person's career" is a
#: sharper claim than is_largest_ever, which is binary and was FLAT across our
#: outcome deciles (55.5% bottom, 56.2% top).
GROUPINGS = {
    "sector":     "m.sector",
    "role":       "COALESCE(t.normalized_title, t.title)",
    "insider":    "t.insider_id::text",
    "ticker":     "t.ticker",
    "mktcap_bin": "width_bucket(f.market_cap, 0, 1e11, 10)::text",
}

#: Default trailing window for percentile features. Long enough that thin
#: groups have a population, short enough to track regime.
DEFAULT_WINDOW_DAYS = 365 * 3


# ── Denominator floors and caps ────────────────────────────────────────────
#
# Ratio features have denominators that approach zero, and the raw output is
# arithmetically correct and useless. Measured on graded buys:
#
#   pct_of_prior_holding   p50 0.014  p99 8.3  p99.9 75,672  max 67,333,011
#   value_pct_of_adv       p50 0.018  p99 6.5  p99.9    113  max    533,491
#
# The first tail is filings where the insider held FEWER THAN TEN SHARES
# beforehand -- three shares to fifty thousand is a first purchase with a
# rounding error in the denominator, not a 16,000x conviction signal. A feature
# whose 99.9th percentile is 9,000x its 99th dominates anything it enters, on
# 0.1% of rows, and it would dominate a clustering just as thoroughly.
#
# These live in the REGISTRY rather than in one pipeline because the first
# build read the unguarded expression and reproduced max=54,339 immediately.
MIN_PRIOR_SHARES = 100
MAX_HOLDING_RATIO = 10.0
MIN_ADV_DOLLARS = 10_000.0
MAX_ADV_MULTIPLE = 100.0
MAX_PRE_RETURN = 5.0


@dataclass(frozen=True)
class Feature:
    """One declared feature.

    `expr` is SQL evaluated in the builder's scope, where `t` is trades, `px`
    is the price series decorated at the OBSERVATION anchor, and `f` is
    previously-materialised features.
    """
    name: str
    expr: str
    description: str
    #: 'observation' — needs the last session closed when the filing appeared.
    #: 'none'        — derived only from the filing's own fields.
    anchor: str = "observation"
    #: Raw features can be exploded into percentiles by these groupings.
    rankable_by: tuple[str, ...] = ()
    #: Trailing window for percentile variants. Never None for a percentile.
    window_days: Optional[int] = DEFAULT_WINDOW_DAYS
    #: Set on generated features so a reader can tell them from declared ones.
    generated_from: Optional[str] = None


def percentile_variants(base: Feature) -> list[Feature]:
    """Explode one raw feature into a percentile per declared grouping."""
    out: list[Feature] = []
    for g in base.rankable_by:
        if g not in GROUPINGS:
            raise ValueError(f"{base.name}: unknown grouping {g!r}")
        if not base.window_days:
            raise ValueError(
                f"{base.name}: a percentile needs a trailing window. Ranking "
                "against the whole table uses the future to rank the past."
            )
        out.append(Feature(
            name=f"{base.name}_pctile_by_{g}",
            expr="",                      # the builder supplies the window fn
            description=(f"{base.description} — percentile within {g} over the "
                         f"trailing {base.window_days}d"),
            anchor=base.anchor,
            window_days=base.window_days,
            generated_from=base.name,
        ))
    return out


# ── Declared raw features ──────────────────────────────────────────────────
#
# The seven already computed on `trades` are declared here so the registry is
# the single description of what exists, even while they are also columns.
# They migrate into the store; the columns are dropped once nothing reads them.

RAW: tuple[Feature, ...] = (
    Feature("filing_lag_days",
            "(t.filing_date::date - t.trade_date::date)",
            "Days between execution and disclosure",
            anchor="none",
            rankable_by=("sector", "role", "insider")),
    Feature("value_pct_of_adv",
            f"CASE WHEN px.adv_20 >= {MIN_ADV_DOLLARS} "
            f"THEN LEAST(t.value / px.adv_20, {MAX_ADV_MULTIPLE}) END",
            "Trade value over 20-session average dollar volume",
            rankable_by=("sector", "role", "insider", "mktcap_bin")),
    Feature("pct_of_prior_holding",
            f"CASE WHEN (t.shares_owned_after - t.qty) >= {MIN_PRIOR_SHARES} "
            f"AND t.qty > 0 THEN LEAST(t.qty::float8 / "
            f"(t.shares_owned_after - t.qty), {MAX_HOLDING_RATIO}) END",
            "Growth in the insider's own stake",
            anchor="none",
            rankable_by=("sector", "role", "insider")),
    Feature("pct_off_52w_high",
            "px.close / NULLIF(px.hi_52w, 0) - 1",
            "Distance below the trailing 52-week high",
            rankable_by=("sector", "mktcap_bin")),
    Feature("ret_20d_pre_filing",
            f"LEAST(px.close / NULLIF(px.close_20, 0) - 1, {MAX_PRE_RETURN})",
            "Price change over the 20 sessions ending at the observation anchor",
            rankable_by=("sector", "mktcap_bin")),
    Feature("ret_60d_pre_filing",
            f"LEAST(px.close / NULLIF(px.close_60, 0) - 1, {MAX_PRE_RETURN})",
            "Price change over the 60 sessions ending at the observation anchor",
            rankable_by=("sector", "mktcap_bin")),
    Feature("ret_trade_to_filing",
            f"LEAST(px.close / NULLIF(px.close_trade, 0) - 1, {MAX_PRE_RETURN})",
            "Move between execution and disclosure",
            rankable_by=("sector",)),
    # ── Volatility ─────────────────────────────────────────────────────
    #
    # Not just features: the reason to build them is that our LABEL is raw
    # abnormal return, so both outcome tails are dominated by volatile names
    # and the top and bottom deciles came out indistinguishable on everything
    # except above_sma50. vol_ratio tests a second thing -- whether volatility
    # was RISING into the filing, which is a different claim from its level.
    Feature("realized_vol_20d",
            "px.vol_20",
            "Annualised realised volatility, 20 sessions to the anchor",
            rankable_by=("sector", "mktcap_bin")),
    Feature("realized_vol_60d",
            "px.vol_60",
            "Annualised realised volatility, 60 sessions to the anchor",
            rankable_by=("sector", "mktcap_bin")),
    Feature("vol_ratio_20_60",
            "px.vol_20 / NULLIF(px.vol_60, 0)",
            "Short-horizon vol over long-horizon vol; >1 means vol is rising",
            rankable_by=("sector",)),
    Feature("value",
            "t.value",
            "Raw dollar value of the purchase",
            anchor="none",
            rankable_by=("sector", "role", "insider", "mktcap_bin")),
)


def all_features() -> list[Feature]:
    """Declared features plus every generated percentile."""
    out = list(RAW)
    for f in RAW:
        out.extend(percentile_variants(f))
    return out


def by_name() -> dict[str, Feature]:
    return {f.name: f for f in all_features()}
