# Four workstreams: market cap, long horizons, short interest, ratings

Written 2026-08-28. Each source below was tested live before being written down.

## 1. Shares outstanding and market cap — SOLVED, free

**Source: EDGAR XBRL company concept API.** Same host we already call for SIC
codes; no key, no account.

```
https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/dei/EntityCommonStockSharesOutstanding.json
```

Verified live: AAPL returns **70 observations back to 2009-06-27**, TSLA 67 back
to 2011. Reported on the cover page of every 10-Q and 10-K, so it is quarterly
and continuous for any company that files.

**The PIT detail that matters.** Each observation carries BOTH `end` (the date
the count is as of) and `filed` (when the filing appeared). These differ, often
by months — TSLA's 2026-01-23 count was not filed until 2026-04-30. **Join on
`filed <= filing_date`, never on `end`**, or every market-cap feature is a
look-ahead. This is the same class of error as `filed_at` being read as UTC.

**Derived features this unlocks**
- `shares_outstanding_pit` — forward-filled from the last filing available at
  the trade's filing_date
- `market_cap_pit` = `shares_outstanding_pit x close(filing_date)`
- `value_pct_of_mktcap` — trade size normalised, the control the literature uses
- `insider_pct_of_company` = `shares_owned_after / shares_outstanding_pit`

**And `shares_owned_after` is already 95.8% populated** (99%+ since 2020;
`value_owned_after` is the empty one at 194 rows, but it is just
`shares x price`). So without any new source we can also build:
- `purchase_pct_of_prior_holding` = `qty / (shares_owned_after - qty)` — how
  much an insider increased their own stake, which is the cleanest available
  proxy for conviction
- `insider_stake_trajectory` — that insider's holding over time, per ticker

## 2. Horizons out to 12 months

`backfill_returns_from_filing.py` computes `HORIZONS = (3, 5, 7, 10, 21, 42)`
trading days. 42td is ~2 months, which is why the cluster question below cannot
currently be answered.

**Extend to `(3, 5, 7, 10, 21, 42, 63, 126, 189, 252)`** — 3mo, 6mo, 9mo, 12mo.
Requires a migration adding six columns to `trade_returns` (with
`SET lock_timeout`) and a re-run. The SQL already walks the SPY calendar by
trading day, so longer horizons need no new logic.

Two consequences to plan for:
- The most recent ~12 months of filings will have NULL 252td by construction.
  `check_attribute_coverage.py` already exempts the current year for immature
  windows; extend that exemption to the new columns.
- Cost matters more at long horizons, not less — a 252td hold is fewer round
  trips. The cost model in `strategy_sweep.py` handles this correctly.

**What it unblocks.** The cluster contradiction: we measure cluster harm at 21d
(+3.25% solo vs -0.08% at 6+) while the literature ties 3+ clusters to
above-market **12-month** returns. Both can be true. We cannot tell today.

## 3. Short interest — SOLVED, free

**Source: FINRA's public API.** Tested live, returns HTTP 200 with **no
authentication**:

```
https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest
```

Fields: `symbolCode`, `settlementDate`, `currentShortPositionQuantity`,
`previousShortPositionQuantity`, `daysToCoverQuantity`,
`averageDailyVolumeQuantity`, `marketClassCode`, `stockSplitFlag`.

Bi-monthly (FINRA Rule 4560), archives back to ~2014. A sibling endpoint
`EquityShortInterest` returns a similar shape.

`averageDailyVolumeQuantity` is a bonus: it gives ADV directly, which the size
work in thesis 5 needs anyway (though `prices.daily_prices.volume` can also
supply it and covers more history).

**The `short_metrics` table already exists and has ZERO rows.** Its schema —
`ticker, date, short_interest, days_to_cover, short_pct_float, borrow_rate,
borrow_available, source` — maps onto the FINRA fields almost exactly.
`short_pct_float` needs shares outstanding from workstream 1.

Bi-monthly settlement dates mean the PIT join is `settlement_date <=
filing_date`, forward-filled — same discipline as shares outstanding.

## 4. Ratings — the top band is selecting for luck

### The diagnosis

`api/ratings.py` justifies sourcing from `career_grade` with:

> career_grade   A+ 4.86%/53.7   A 2.00%/48.5   B 0.42%/47.0
> career_grade orders correctly on win rate at every step.

Measured on corrected data, episode-level, that ordering does not reproduce on
**either** label:

| grade | eps | txn-anchored | filing-anchored | filing win% |
|---|---|---|---|---|
| A+ | 772 | 0.86% | **0.70%** | 50.1 |
| A | 1,604 | 1.16% | 1.27% | 46.8 |
| B | 10,744 | 2.54% | 1.13% | 48.3 |
| C | 25,068 | 0.64% | 0.32% | 47.7 |
| D | 39,579 | 1.35% | **-0.05%** | 45.2 |

**A+ ranks below C on mean return.** The grade still works at the BOTTOM — D is
genuinely worst on the tradeable label, the only clean step in the table.

### Why: winner's curse, and it is quantified

`pit_scoring.py` shrinks average abnormal return toward zero with
`PRIOR_RETURN_N = 3.0` — three pseudo-observations.

| grade | p25 prior trades | median | % with <=5 |
|---|---|---|---|
| **A+** | **0** | **3** | **56.3%** |
| A | 1 | 6 | 49.9% |
| B | 1 | 5 | 52.8% |

The median A+ insider has **three** prior trades and a quarter have **zero**.
At n=3 the shrinkage factor is 3/(3+3) = 0.5, so half the raw sampling noise
flows into the score. Insider abnormal returns disperse by tens of percent per
trade, so estimating a mean from three draws is mostly estimating noise — and
the A+ threshold (score >= 2.5) sits in the extreme tail, which is precisely
where noise dominates. The band is selecting insiders who got lucky three
times.

A+ having the HIGHEST win rate (50.1%) and the LOWEST mean of the top three is
consistent with this: frequent small wins, occasional large losses, no skill.

### The redesign

1. **Shrink much harder, and calibrate it rather than picking it.** Sweep
   `PRIOR_RETURN_N` over roughly 3 -> 50 and select on walk-forward ordering of
   the *tradeable* label, not in-sample fit. The right value is an empirical
   question that has never been asked.
2. **Fit and validate against `abnormal_21td_from_filing`.** The scorer is
   built from transaction-anchored returns, which nobody can trade.
3. **Require a minimum evidence count for the top band.** No A+ on fewer than N
   prior filings; fall back to A or Unrated. `MIN_SCORED_FILINGS` already exists
   for the insider track-record block — reuse the concept, not a new constant.
4. **Consider collapsing A+ into A.** The data does not currently support a
   distinct top band. `api/ratings.py` already maps stored `D -> C` for display,
   so collapsing a band is a precedent, not a new pattern.
5. **Set thresholds as percentiles of the contemporaneous cohort**, not fixed
   scores (A+ >= 2.5). Fixed cutoffs make the band's population drift whenever
   the score distribution shifts — which is exactly what the grade-population
   fix did.
6. **Then test whether the track record earns its place at all.** The screen
   says `is_largest_ever` (t=+5.49) and inverted cluster (t=-4.32) are stronger
   than anything the grade contributes above C. A rating built from
   trade-level features may beat one built from insider history.

### Order

Diagnosis is done. (1) and (2) are one sweep and answer whether the grade is
fixable. (3)-(5) are mechanical once the sweep lands. (6) is the real question
and needs the new features from `docs/feature_theses.md`.

**Nothing here changes a published rating until the sweep is done.**
`career_grade` gates all three live books and `api/ratings.py` publishes it, so
a change here moves both money and the site.
