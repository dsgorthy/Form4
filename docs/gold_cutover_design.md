# Gold cutover design — `trades` as a projection of Silver

*2026-09-21. Step 7 of `docs/gold_cutover_plan.md`. This is a design for
Derek to read and decide on; nothing in it has been done. The measurements
are from the Studio on 2026-09-18..21.*

## The claim

`trades` should stop being a table that ingest writes and hand repairs
patch, and become a **projection**: its factual columns derived from Gold
(Silver + identity + classification), its analytical columns computed by
the pipelines that already compute them, and nothing else writing to it.
Every data defect since August — the 48.6% ingestion loss, `price_validator`
overwriting filed prices, 48,000 rows with no CIK or security title,
300,000 planned trades classified as decisions, 5,624 joint filings stamped
with one owner's CIK — is a row that was written once, by a lossy path, and
never re-derivable. A projection is re-derivable by construction.

## What reads and writes `trades` today

138 Python files read it (58 in `pipelines/insider_study`, 21 in
`strategies/insider_catalog`, 20 in `scripts`, 16 API routers, 12 other
pipelines, 10 elsewhere). 27 write to it. The writers split cleanly:

| kind | files | after the cutover |
|---|---|---|
| **ingest and repair** — `backfill_live`, `reparse_bulk`, `backfill_from_sec_datasets`, `backfill_v3_missing_trades`, `backfill_sec_fields`, `backfill_filed_at*`, `dedup_trades`, `entity_resolution`, `consolidate_duplicate_insiders`, `fix_bad_dates`, `normalize_titles`, `price_validator`, `backfill_line_no`, `repair_orphan_trades_from_silver` | 15 | **retired.** Their job — get the facts of a filing into a row — is Gold's. |
| **derivation** — `compute_derived_features`, `compute_career_grades`, `compute_cohen_pit`, `compute_pit_clusters`, `compute_programmatic`, `compute_switch_rate`, `compute_week52_proximity`, `compute_cw_indicators`, `compute_company_net_flow`, `compute_industry_net_flow`, `backfill_pit_grades`, `backfill_returns` | 12 | **kept**, run over the projection exactly as now. |

The columns fall into the same two classes, plus three small ones:

| class | columns | source after cutover |
|---|---|---|
| **facts of the filing** (30) | accession, filing_key, ticker, issuer_cik, rptowner_cik, insider_id, title, trans_code, trans_acquired_disp, trade_date, filing_date, filed_at, price, qty, value, shares_owned_after, direct_indirect, nature_of_ownership, equity_swap, security_title, deemed_execution_date, trans_form_type, is_derivative, is_10b5_1, document_type, period_of_report, is_amendment, superseded_by, line_no, remarks | **Gold** (`gold.form4_line`), one row per line per owner |
| **trigger-derived** (4) | signal_class, trade_type, value_suspect, filing_key | the same SQL functions, fed by Gold's columns |
| **judgement** (5) | price_quality, price_as_filed, value_as_filed, correction_method, suspect_reason | `price_quality` from Silver; `price_as_filed`/`value_as_filed` become identical to price/value (nothing is corrected any more); `correction_method`/`suspect_reason` retired |
| **identity of the row** (5) | trade_id, source, created_at, ingested_at, effective_insider_id | `trade_id` preserved through a map (below); `source` = 'gold'; timestamps kept |
| **derived analytics** (38) | is_csuite, title_weight, normalized_title, is_routine, cohen_routine, is_tax_sale, is_recurring, recurring_period, consecutive_sells_before, pit_*, signal_grade, career_grade, insider_switch_rate, is_rare_reversal, week52_proximity, txn_group_id, dip_*, sma*_rel, above_sma*, purchase_size_ratio, is_largest_ever, net_buyer_flow_90d, industry_buy_pct_90d, pct_of_prior_holding, filing_lag_days, ret_*, pct_off_52w_high, value_pct_of_adv, pit_cluster_size | the derivation pipelines, unchanged |
| **SEC header fields** (13) | rptowner_relationship/text/street1/street2/city/state/zipcode, file_number, filed_at_source, trans_timeliness, no_securities_owned, not_subject_sec16, form3/form4_*_reported | read by one script (`backfill_sec_fields`) and `filings.py`; Silver's header can carry the two the API shows (`document_type`, `date_of_orig_sub`); the rest retire |

Two columns are referenced nowhere (`pit_avg_abnormal_30d`, `sma20_rel`).

## What has to survive: `trade_id`

`trade_id` is the key everything downstream holds:
`strategy_portfolio.trade_id` (every position in the three books),
`trade_returns` (725k rows of forward returns), `social_posts.trade_id`
(every StockTwits post), `bad_trades`, the encoded IDs in every
`/filing/<id>` URL Google has indexed, and `notifications` dedup keys.
A cutover that renumbers rows breaks all of it. So:

**`gold.trade_map (trade_id, accession, rptowner_cik, line_no)`** — built
once by the positional parity join (`pipelines/silver/parity.py`) and the
repair script's line-matching (accession + date + code + qty + price). The
2026-09-18 parity run puts 36.8% of rows in `match`, 18.1% in `mismatch`
(same line, some column differs — those still map), 4.0% `trades_only` (no
Silver line: mostly `sec_form345` bulk rows Bronze never held), and 16.4% +
24.8% Silver-only (lines `trades` never ingested — the ingestion loss and
every derivative line). Expected: ~95% of `trades` rows map to exactly one
Gold line; the unmapped 4% are quarantined, not deleted.

## The cutover, in phases

Each phase is reversible on its own and none changes a number the product
shows until phase 5.

| # | phase | what changes | reversible by |
|---|---|---|---|
| 0 | **Decisions** (Derek): the 10b5-1 interim repair (round 2 of the plan: 300,018 rows / 122,687 filings), the identity merge (2,515 groups, 44,166 rows — plan in `reports/identity_merge_plan.md`), and whether `trades_only` rows are kept as a quarantined source. | nothing yet | — |
| 1 | **Freeze the ingest writers.** `backfill_live` keeps writing Bronze (it already does, via the top-up); its `INSERT INTO trades` becomes the last hand-written path and is the only one left running. Retire the 14 others (archive, not delete). | no data | un-archive |
| 2 | **Build `gold.trade_map`.** Parity join, then the line-match for the remainder. Report the unmapped rows by source and year. | a new table | drop it |
| 3 | **Shadow projection.** `gold.trades_projection` — every Gold line as a `trades`-shaped row: mapped rows carry their old `trade_id`, unmapped Gold lines get new ids from the same sequence, `trades_only` rows are carried through with `source='quarantine'`. Run every derivation pipeline against it in a schema-qualified dry run. Diff against `trades` column by column; the diff *is* the list of things the product will say differently. Read it. | a new table | drop it |
| 4 | **Cut the API over, behind a flag.** `api.db` reads `trades_projection` when `GOLD_PROJECTION=1`; the parity script's company/insider numbers already show what moves. Sandbox first (`deploy-sandbox` exists in CI), then prod. | one env var | flip it |
| 5 | **Rename.** `trades` → `trades_legacy_2026`, `trades_projection` → `trades`. The triggers, indexes and FKs move with the name. The nightly derivation runs as before. | rename back | 5 min |
| 6 | **Live path.** Silver's hourly keep-up already builds every new Bronze document; the projection refresh becomes the last step of `keep_up.py` and `backfill_live` stops inserting into `trades`. Freshness contract: a filing is in the product within one Bronze top-up + one keep-up, ~1 hour, versus the 5-minute live ingest today — **this is the one product regression**, and the fix is running the top-up every 10 minutes for the current day, which the SEC rate budget allows. | keep_up config | restore the 5-min ingest |

Estimates: phase 2 ~½ day, phase 3 ~1 day plus a night of pipeline runs,
phase 4 ~½ day plus the sandbox soak, 5–6 an evening. Nothing before phase 4
is visible to a user.

## What the shadow diff will show, and what to decide about it

From the parity runs and this week's findings, the projection will differ
from `trades` in five known ways. Each is a decision, listed here so they
are made once:

1. **Planned trades become planned.** 122,687 filings move from
   `discretionary_*` to `planned_*` (Silver's 10b5-1 flag, `trades`' own
   rule). Company and insider counts drop (AAPL 261 → 145 open-market
   filings); the grading population loses 9,731 purchases; the books'
   candidate pool shrinks. The 2026-08-24 A-List rule intended exactly this.
2. **The ingestion loss comes back.** 16.4% of Silver's non-derivative lines
   and every derivative line have no `trades` row. Those become rows. Counts
   and values rise where they had been silently missing (BFLY's 6-month sells
   $44M → $99M; the PIF's $1.8B LCID purchase appears).
3. **Joint filings count once on company pages and once per co-filer on
   insider pages.** `filing_key` over-counted by ~4% (506,364 vs 486,136
   since 2016).
4. **Split-adjusted prices stop judging filed prices.** `prices.daily_prices`
   is `adjustment=split` history (Alpaca); filed prices are raw. Silver's
   `outside_band` and `trades.price_quality` both compare the two, and the
   ratios spike at 4, 5, 8, 10, 20 — splits, not typos. Gold's
   `trusted_value` excludes only `implausible` (≥100×) and > $5B. The proper
   fix is a raw-price band (Alpaca `adjustment=raw` into a second column, or
   a splits table); until then any "X% since their fill" line on a stock that
   later split is wrong, and the StockTwits generator's foreign-price guard
   will drop those filings rather than misstate them.
5. **Identities merge.** 2,515 CIK groups fold (name variants); 1,659 need a
   human — renames like Blackstone Group L.P. → Blackstone Inc., and joint
   filings whose lines carry one owner's CIK (5,624 accessions: fix the CIK
   from Silver, do not merge).

## What this does not solve

- `insiders.cik` is not the identity (110,004 disagree with the filed CIK,
  105,367 NULL, many carry the issuer's CIK). Rebuild it from Gold's map
  as part of phase 3.
- Silver's `ticker` is as filed; Gold maps it to the product's symbol via
  `trades`' majority. After the cutover that map has to be maintained from
  somewhere else — the SEC company tickers file (`company_tickers.json`,
  one request) is the honest source.
- Derivative lines have never been in the product (24.8% of Silver). The
  projection carries them with `is_derivative`; the API filters already
  exclude them. Whether to show them is a product question, not a data one.
