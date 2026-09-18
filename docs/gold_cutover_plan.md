# Gold cutover plan — making the product read Silver

*2026-09-18. Gold v0 is built (`migrations/2026-09-18_gold.sql`); the first
parity report is `reports/gold_parity_2026-09-18.md`. This document is what
the report says, what it means, and the ordered work before `trades` can
become a projection of Gold.*

## Why

Every data defect chased since August came from `trades` being a lossy,
hand-patched projection of the filings: the 48.6% ingestion loss (08-26),
`price_validator` overwriting filed prices (09-04), 48,000 rows with no
insider CIK or security title (repaired 09-18 from Silver). Silver is the
faithful layer — every Form 4/5 line as filed, 11.58M of them, with a price
judgement in a separate column. Gold is Silver made product-shaped.

## What Gold v0 is

`gold.form4_line` — a materialized view (2.5 min to build) of Silver plus
the four things `trades` adds that the product depends on:

| column | source | coverage |
|---|---|---|
| `insider_id` | the CIK → insider_id map `trades` carries (`gold.insider_by_cik`, 198,378 CIKs) | **98.1%** of lines |
| `is_joint_copy` | `row_number()` over (accession, line_no) by CIK | 1,494,231 copies (12.9%) |
| `superseded_by` | the earliest 4/A for the same issuer, first owner, period, filed later | 271,292 lines (2.3%) — `trades` marks 1,906 rows |
| `signal_class` | `form4_signal_class()`, the same SQL function `trades`' trigger uses | every line; **10b5-1 unknown** |

## What the first parity report says

Company pages, 242 tickers (94 that people landed on from search in the
last 60 days + top 150 by volume), the numbers under the H1 both ways:

| bucket | all | SEO sample |
|---|---|---|
| same | 21 | 21 |
| gold_has_more_filings | 153 | 39 |
| trades_has_more_filings | 15 | 9 |
| value_differs | 53 | 25 |

Open-market filings per year agree within ~1–2% from 2020 on; 2016–2019
`trades` has 1–7k more per year, 2026 1.6k more (see gap 5). Insider pages:
40 of 106 identical before the joint-copy fix below.

**Every gap has one of five causes, and none of them is Silver being wrong.**

1. **Gold cannot see 10b5-1.** The `aff10b5One` attribute is in Bronze
   (~15% of submissions carry it, ~1.4% true) and Silver's parser does not
   extract it, so Gold classifies every P/S as discretionary. This is the
   whole "gold has more" bucket: AAPL is 261 discretionary + 318 planned in
   `trades` = 579 against 473 in Gold; META 64 + 662 against 692; CRM 3,048 +
   5,503 against 7,000. With the attribute, Gold lands at or below `trades`
   (below where `trades` over-counts joint filers per `filing_key`).
2. **Tickers as filed.** Silver's `ticker` is `issuerTradingSymbol` as the
   filer typed it: "ISCA, ISCB", "ABI/CRA", "(NYSE:FBC)", 10,057 accessions
   under "NONE". `trades` maps issuers to one symbol. That is the whole
   "trades has more" bucket (ISCA 8,458→0, ABI 319→13, ONB 722→185).
3. **Joint copies on insider pages.** A joint filing is one transaction on
   the company page but belongs on every co-filer's page. Excluding copies
   in the insider query zeroed Boaz Weinstein (4,949 co-filed lines, 1,178
   buys in `trades` → 0). Fixed in the parity script; the rule for Gold is:
   company counts by distinct accession, insider views keep every owner.
4. **Value filters differ.** `trades` sums value where `NOT value_suspect`;
   Gold where `price_quality <> 'implausible'`, which admits `outside_band`
   (3× off the band). EQT's 6-month sells read +6,096%. Gold's trusted value
   should exclude `outside_band` too; measure against `value_suspect` on the
   `value_differs` bucket before deciding.
5. **The edges — resolved 2026-09-18.** 2016–2019: every `trades` filing
   has a Silver receipt; the yearly surplus is `trades` counting per
   `filing_key` (accession + trade date + insider), so a filing with two
   trade dates or two co-filers counts twice. Gold's per-accession count is
   the right one. 2026: nothing refreshed `bronze.edgar_index` after
   2026-09-06, so the hourly top-up had nothing to fetch and Bronze stopped
   at filings of 09-04 — and nothing ran Silver's build or assess on a
   schedule at all. Now: index refresh :05 every 6 h, top-up :20 hourly,
   `keep_up.py` (build → assess → 10b5-1 on new lines) :40 hourly, Gold
   refresh 02:30 nightly. Dagster assets, all in the registry.

Also surfaced: `insiders` holds duplicate identities for one CIK ("GOLDMAN
SACHS GROUP INC" and "…INC/", same CIK 0000886982; three Abrams entities).
Gold's CIK map picks the majority insider_id, which is the right merge; the
cutover has to merge the pages.

## The work, in order

| # | step | est. | what it closes |
|---|---|---|---|
| 1 | ✅ **Silver `aff_10b5_1`** (2026-09-18): filing-level flag by `trades`' own rule (checkbox, or "10b5" in remarks/footnotes), the rule in SQL per quarter of Bronze (2 s per quarter to count; the Silver UPDATE dominates); `--pending` hourly for new lines. Gold's `signal_class` takes it. | done; backfill ran 09-18 | gap 1 — the largest |
| 2 | ✅ **`gold.ticker_by_issuer`** (2026-09-18): majority ticker per `issuer_cik` from `trades`; Gold's `ticker` is canonical, `ticker_as_filed` kept. | done | gap 2 |
| 3 | Re-run parity. Target: `same` ≥ 90% of the SEO sample on filing counts. | ½ h | — |
| 4 | **Trusted value** in Gold: decide the `price_quality` set that sums; compare to `value_suspect` on the value_differs bucket. | 2 h | gap 4 |
| 5 | ✅ **Edges** (2026-09-18): named (see gap 5) and the pipeline scheduled end to end. | done | gap 5 |
| 6 | **Identity merges** in `insiders` (same CIK, several rows). | ½ day | insider pages |
| 7 | **Cutover design**: `trades` as a projection of Gold — which columns the derivation pipelines (PIT grades, signals, returns) actually need, and a reload path that keeps `trade_id` stable for `strategy_portfolio`, `trade_returns`, `social_posts`. **Derek's call; nothing before step 3 is worth discussing.** | 1 day to write | — |

## What Gold does not touch

Nothing the product reads. `gold.*` is read-only over Silver and `trades`,
refreshable with `REFRESH MATERIALIZED VIEW gold.form4_line`, and can be
dropped without consequence.
