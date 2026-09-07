# Silver: modelling filer error without destroying it

## The finding that shaped this

The "$231B of phantom value" on AMMA, CNTM and IHT was diagnosed for months as
a parse failure — `price_validator`'s own correction is named
`price_is_total_value`, i.e. "the price field holds the trade's total". On
2026-09-06, with Bronze in place, that was tested against the source for the
first time:

```
IHT  0001493152-25-015819
  trades:  price 22,625   qty 12,500   value $282,812,500
  XML:     <transactionPricePerShare><value>22625</value></...>
           <transactionShares><value>12500</value></...>
```

**62 of 64** suspect rows covered by Bronze match the XML exactly. The parser
is faithful. **The filer entered 22,625 as a per-share price for a stock
trading at $1.50.**

This is bad data in a public SEC filing, not a bug in our pipeline, and it
means no amount of re-parsing fixes it. Everything below follows from that.

## Consequences for the layer model

### Bronze never changes

The document says 22,625. Bronze stores 22,625. There is no case in which
Bronze is edited — a filer correction arrives from SEC as a NEW accession
(an amendment), which is a new Bronze row.

### Silver parses faithfully and JUDGES SEPARATELY

Silver must reproduce `price = 22625`, because that is what was filed. What it
adds is a separate, explicit assessment:

| column | meaning |
|---|---|
| `price_per_share` | exactly as filed. Never adjusted. |
| `shares` | exactly as filed. |
| `value` | GENERATED as `shares * price_per_share`. Not a stored fact — a filing does not carry a total, so storing one invites the two to drift. |
| `price_quality` | `ok` / `outside_band` / `implausible` / `no_reference` |
| `price_quality_note` | why, in words, with the reference range |

The assessment is a column, not an edit. `trades` today does the opposite:
`price_validator.apply_correction` **overwrites `price` and `value`** and moves
the original into `price_as_filed`. That is Silver rewriting a source fact,
and it is how 29 real GOOG/AMZN/CMG trades were destroyed on 2026-09-04 — a
heuristic decided a correct filing was wrong and edited it.

### Gold decides what to publish

Aggregates read `price_quality`. A filing whose per-share price is 15,000x its
own 8-week band does not enter a dollar total. It still appears on its own
filing page, showing what was filed, labelled as implausible — because the
filing is real and a reader looking it up should see what SEC holds.

## Why not just fix the number

Three reasons, in order of weight.

1. **We would be wrong sometimes.** The correction that looks obvious —
   `price / qty` — is an arithmetic identity that fires on coincidence. It
   "corrected" 17 GOOG rows whose real prices were right, because our price
   history is split-adjusted and the filings are not.

2. **We would be inventing a number SEC never published.** The reader's
   recourse is the filing itself; a page showing a figure that is in no SEC
   document cannot be reconciled with the source.

3. **It does not compose.** A corrected `trades` row and an uncorrected
   duplicate of the same filing both survived the 2026-08-26 reload —
   $36.5B of the phantom total is live precisely because a correction landed
   on one copy of two. A quality FLAG derived from source facts is
   recomputable for every copy; an edit is not.

## What this does not solve

The flag suppresses bad data from aggregates. It does not tell us what the
insider actually paid. For IHT the true figure is probably `22,625 / 12,500 =
$1.81`, which is inside the band — but "probably" is doing real work in that
sentence, and it is the same inference that destroyed the GOOG rows. We
publish what was filed, marked as untrustworthy, and we do not guess.

---

# Gold: how the flag is applied when serving

Silver stores the judgement. Gold decides what a reader sees. There is exactly
one definition, `api.filters.trusted_value_filter(alias)`, and
`tests/unit/test_value_aggregates_are_trusted.py` fails the build when a value
aggregate does not carry it or when a copy of it drifts by a byte.

## The shape depends on the question being asked

**Grouped aggregates FILTER the value and leave the counts alone.**

```sql
COUNT(DISTINCT COALESCE(filing_key, accession))          AS total_trades,
SUM(value) FILTER (WHERE NOT COALESCE(value_suspect, FALSE)
                     AND price_quality IS DISTINCT FROM 'implausible')
                                                          AS total_value,
```

A bad price is not a bad filing. Putting the rule in `WHERE` fixes the dollar
total by deleting the row, and the row is a real decision by a real insider
that really appears in EDGAR. Measured on IHT, whose 65 flagged rows are 28.9%
of the ticker:

| insider | real filings | if guarded in WHERE |
|---|---|---|
| WIRTH JAMES F | 53 | 10 |
| Chase JR | 12 | 2 |
| Kutasi Leslie T | 8 | 1 |
| BERG MARC E | 1 | **0 — vanishes from the site** |

The company's most active insider would have lost 43 of 53 filings, and one
insider would have disappeared entirely, to fix a number in a different column.

**Row-level lists guard in WHERE**, because there the question is whether to
show the row at all. `sectors._TOP_BUYS_SQL` ranks individual buys, and a
filer's $22,625/share typo must not be *displayed* as a sector's biggest buy.
No count depends on it, so nothing is lost by excluding it.

## Two things that only appear once it is wired up

**Postgres sorts NULL FIRST under `DESC`.** A group whose every lot is
implausible sums to NULL, so on all five "biggest" rankings the rows we trust
least would have sorted to the top — the exact opposite of the intent. Every
`SUM(...) FILTER (...) DESC` therefore carries `NULLS LAST`, and the test
enforces it.

**`HAVING` inherits the filter and should.** `HAVING SUM(value) FILTER (...) >=
100000` excludes a group that only clears the bar on prices we reject, which is
correct: NULL >= 100000 is NULL, so the group drops out rather than qualifying
on a typo.

## How this was missed the first time

The flag was wired in by adding it "next to every existing `value_suspect`
guard". That rule cannot see a query with no guard to sit beside, and the
company page's headline aggregate was exactly that — it filtered neither flag,
while the roster immediately below it filtered both. One page, two totals, same
rows: $7,207,876,939 against $2,325,931.

The test could not catch it either, because it skipped any router that had no
guard to inspect. **A check that only examines the places already guarded is
not a check.** The replacement has no SKIP branch: it walks every `SUM` over
`value` in every router and fails on any that is unfiltered.

## Verifying a change to this layer

The Mini has no `form4` database, so the unit suite never parses a line of this
SQL, and `studio deploy form4` checks `/api/v1/health` and nothing else. A
change here is unverified by default. Two things close that gap:

- `scripts/smoke_value_surfaces.sh [base_url]` asks all 14 dollar-publishing
  surfaces for a real answer. Run it before and after deploying and compare.
- Extract the modified queries, run them through `config.database.translate_sql`,
  and `PREPARE` them against the live catalog — that parses and plans without
  executing, so a mistyped column fails there instead of in front of a user.

And confirm what actually shipped. `studio deploy form4` pulls `origin/main`;
a commit that exists only locally deploys nothing, the smoke test passes on the
old code, and the values do not move.
