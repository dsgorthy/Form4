# Feature theses — what to build and why

Written 2026-08-28, after the corrected dataset made most of this measurable
for the first time. Every empirical number below is **episode-level** (one
insider+ticker run = one bet; 270,269 filings collapse to 88,081 episodes, so
filing-level counting inflates n by 3.4x) against `abnormal_21td_from_filing`,
the only tradeable label.

## What the literature says

| Finding | Source |
|---|---|
| Routine vs opportunistic: stripping calendar-driven trades leaves all the predictive power. Opportunistic purchases earn **82bp/month** value-weighted; routine trades earn ~zero. | Cohen, Malloy & Pomorski, *Decoding Inside Information*, J. Finance 2012 |
| The most informed opportunistic traders are **local, non-executive** insiders at geographically concentrated, poorly governed firms. | ibid. |
| **Distance from the 52-week high dominates feature importance at 36%**, superseding insider identity or transaction size. Purchases disclosed *into strength* are more predictive than into weakness. | Gradient-boosting microcap study, arXiv 2602.06198 |
| Transactions disclosed after **>10% price appreciation** yield the highest CAR (6.3%). Momentum *validates* the insider's signal rather than eroding it. | ibid. |
| Classification at the **insider level**, not the transaction level, is where the signal lives, and it is persistent out-of-sample. | Heckmann, *Synthesizing Information-driven Insider Trade Signals* |
| Predictability decays substantially at 6–12 month horizons — favours short rebalancing. | ibid. |
| 3+ insiders buying within 15 days links to above-market **12-month** returns. | cluster literature (see thesis 1 — we measure the opposite at 21d) |
| Insider purchases yield >6%/yr abnormal; sales yield nothing significant. | InsideArbitrage survey |

## What our own data says

| signal | spread (A+/A/B) | t |
|---|---|---|
| `is_largest_ever` | **+2.34pp** | **+5.49** |
| `pit_cluster_size` | **−1.61pp** | **−4.32** |
| `value` | +0.93pp | +2.54 |
| `above_sma50` | +0.93pp | +2.42 |
| `above_sma200` | +0.77pp | +1.87 |
| `industry_buy_pct_90d` | +0.36pp | +0.95 |
| `week52_proximity` | +0.18pp | +0.46 |
| `consecutive_sells_before` | +0.24pp | +0.37 |
| `is_rare_reversal` | +2.52pp (n=165) | +1.40 |

---

## Thesis 1 — Fade the crowd

**Claim.** A lone insider buying carries more information than a crowd. Cluster
size should be *inverted*, not rewarded.

**Evidence.** Monotonic across every bucket on graded names:

| cluster | 0 | 1 | 2 | 3 | 5 | 6+ |
|---|---|---|---|---|---|---|
| mean 21td | **+3.25%** | +1.96% | +2.21% | +0.67% | +0.39% | **−0.08%** |

The documented basis for the current gate — *"4+ insiders + CEO cluster: 57%
WR, +6.1% avg"* — measures **48.2% WR, +0.79%** today, against a +1.65%
baseline. `trade_grade.py` awards up to **+12 points** for this, and conviction
gates entry on it.

**The open question.** The literature ties 3+ clusters to above-market returns
at **12 months**; we measure 21 days. That is not necessarily a contradiction —
a crowd may signal slow-burn value while a solo buyer signals near-term news.
This must be tested at both horizons before inverting anything.

**Features needed**
- `cluster_size_7d` / `_15d` / `_30d` — PIT counts at several windows (have only one)
- `cluster_dispersion` — are the buyers from different role tiers, or one department?
- `cluster_is_first` — was this insider the *first* mover in the cluster?
- Re-run the screen against a 90d/180d label (needs longer filing-anchored labels; current max is 42td)

## Thesis 2 — Buy strength, not weakness

**Claim.** Insider buys into rising prices outperform buys into drawdowns. This
contradicts both `reversal_dip`'s entire premise and `quality_notrend`'s
argument for removing the trend filter.

**Evidence.** `above_sma50` is the strongest signal on the full corpus
(**t=+9.53**), `above_sma200` t=+7.23, and `dip_1mo`/`dip_3mo` spreads are
*positive* — meaning higher (less negative) recent returns do better. The
literature is emphatic and independent: 52-week-high distance at 36% of feature
importance, and >10% prior appreciation giving the highest CAR.

Against this, `reversal_dip` is gated on the two weakest signals we measured,
and cannot beat SPY at any threshold (best case ~3.9% CAGR vs 13.45%).

**Features needed**
- `ret_20d_pre_filing`, `ret_60d_pre_filing` — price run-up *before* disclosure
- `pct_off_52w_high` — the literature's top feature, as a continuous distance rather than our bucketed `week52_proximity`
- `ret_trade_to_filing` — the move between transaction and disclosure, which the buyer saw and we currently ignore
- `vol_20d`, `atr_pct` — normalise all of the above by volatility
- `rsi_14`, `dist_from_sma50_pct` — continuous versions of our booleans

## Thesis 3 — The insider, not the trade

**Claim.** Signal lives at the insider level and persists. Our `career_grade`
does some of this but separates only C/D from everything above — it does **not**
separate A+/A from B (B ≥ A+/A at all six horizons, t=−2.94 at 10td).

**Evidence.** Cohen et al.'s 82bp/month comes entirely from a *trader-level*
classification. Heckmann finds insider-level classification persistent
out-of-sample. Our own grade underperforms this standard.

**Features needed**
- **Run `compute_programmatic.py`** — written, tested, never executed. Gives `is_programmatic`, `prog_cv_interval`, `prog_cv_value`, `prog_median_interval_days`
- `insider_trade_count_prior`, `insider_tickers_traded_prior` — breadth vs depth
- `insider_gap_days` — time since this insider's last filing on any ticker; a long-dormant insider suddenly buying is a different event
- `insider_hit_rate_pit` — walk-forward win rate (we have `pit_win_rate_*`; verify PIT)
- `is_first_ever_buy_on_ticker` vs `is_largest_ever` — currently conflated

## Thesis 4 — Local, non-executive insiders

**Claim.** Cohen et al.'s most specific and least-exploited finding: the best
opportunistic traders are **non-executive** and **local**. We have never tested
either, and our scoring leans the opposite way — `is_csuite` and `title_weight`
reward seniority.

**Evidence.** Our own data hints at it: CEO-with-cluster returns +0.79% while
"neither" returns +1.65%. We have not isolated non-executive status.

**Features needed**
- `issuer_hq_state` / `issuer_hq_city` — **from the same EDGAR submissions JSON already used for SIC codes**, so this is cheap
- `insider_is_local` — `rptowner_state` matches issuer HQ state (33% coverage on the insider side today)
- `insider_distance_km` — needs zip centroid lookup
- `is_non_executive` — derive from `normalized_title`; directors and 10% owners vs officers
- `firm_insider_concentration` — how geographically clustered the firm's insiders are

## Thesis 5 — A size floor, not a size preference

**Claim.** Your hypothesis, refined. Small trades *are* worse, but the
relationship is **hump-shaped**, not monotonic — so the rule is "exclude the
tiny," not "favour the huge."

**Evidence.** By decile of dollar value, graded names:

| decile | up to | mean 21td | win% |
|---|---|---|---|
| 1 | $992 | **0.90%** | 48.6 |
| 2 | $3,024 | **0.99%** | 45.3 |
| 5 | $26,000 | 2.55% | 47.9 |
| **8** | $198,000 | **3.95%** | **52.3** |
| 10 | — | 2.08% | 48.9 |

The bottom two deciles return a third of the middle; the top decile is worse
than the eighth. Our guardrail is `min_dollar_amount: 100.0`, roughly 30x too
low. The literature agrees transaction size ranks *below* market conditions, so
this is a floor to remove noise, not a ranking signal.

**Features needed**
- `value_pct_of_adv` — trade value over 20-day average dollar volume. **This is
  the normalisation that matters** and we can build it today: `prices.daily_prices`
  has `volume`
- `value_vs_insider_median` — this purchase against that insider's own history
- `value_pct_of_mktcap` — blocked, see below
- `qty_pct_of_shares_owned_after` — blocked: `value_owned_after` is populated on
  4 of 13,536 episodes

---

## Infrastructure gaps, ranked

1. **Shares outstanding / market cap — we have none.** Blocks size
   normalisation, the single most-cited control in the literature. Needs a
   source decision (EDGAR company facts XBRL is free and we already call that host).
2. **Longer filing-anchored labels.** Max horizon is 42td (~2 months). Thesis 1's
   12-month cluster question and any long-hold study are unanswerable without
   90/180/365td labels.
3. **`short_metrics` is an empty table.** Schema exists — `short_interest`,
   `days_to_cover`, `short_pct_float`, `borrow_rate`. Short interest appears in the
   literature as a squeeze/conviction interaction. Zero rows today.
4. **Issuer HQ location.** Cheap — same EDGAR endpoint as the SIC backfill.
5. **`trans_timeliness` is 427 of 317,901 rows.** Effectively unusable, but
   filing lag is directly computable from `filing_date − trade_date` and is
   well-distributed (142k at 0–1d, 24k at 10+d). Note 48 rows have *negative*
   lag and one is 730,485 days — needs a validity guard.

## Order of work

1. Run `compute_programmatic.py` (written, never run) — free, thesis 3
2. Compute filing lag + pre-filing momentum features — free, theses 2 and 3
3. `value_pct_of_adv` from `daily_prices.volume` — free, thesis 5
4. Issuer HQ from EDGAR — cheap, thesis 4
5. Extend filing-anchored labels to 90/180td — moderate, unblocks thesis 1
6. Market cap source decision — needed before size work is defensible

Every feature must clear `scripts/check_attribute_coverage.py` per-year before
it is used in a book, and be screened with `scripts/signal_screen.py` at episode
level before it is believed.
