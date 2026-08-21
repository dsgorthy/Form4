# Data model: what is live, what is dead, what to do about it

Inventory taken 2026-08-20 across all 63 tables in `form4`, cross-referencing
size and Postgres access counters against every read and write in the codebase.
Counters cover 9 days of uninterrupted uptime (no `pg_stat_reset`, no restart
since 2026-08-11).

**Headline: the database was 25.0 GB and roughly 12 GB of it is never read.**

Pass 1 has been applied and took it to 22.79 GB. The rest needs decisions.

---

## Applied — `2026-08-20_drop_duplicate_and_dead_tables.sql`

| dropped | size | why |
|---|---|---|
| `research.filing_footnotes` | **1,862 MB** | exact duplicate of `public.filing_footnotes` — both 4,646,211 rows |
| `public.derivative_trades` | 64 kB | empty shell; real data is `research.derivative_trades` (1.16M) |
| `public.nonderiv_holdings` | 48 kB | empty shell; real data is `research.nonderiv_holdings` (616K) |
| 7 × empty speculative tables | ~250 kB | zero rows, zero scans, zero code references |

25.0 GB → **22.79 GB**, 63 tables → 53.

### The `research` schema is a SQLite migration artefact

`backfill.py` still carries `RESEARCH_DB = DB_PATH.parent / "research.db"`. The
`research` PG schema was created by importing that file and the duplicate was
never cleaned up. `search_path` is `"$user", public`, every writer uses
unqualified names, and the only reference to `research.filing_footnotes` in the
entire repo was an `ANALYZE` line added the day before.

The migration guards the assumption: it aborts if `public` has fewer rows than
`research` rather than trusting the analysis.

---

## Decisions needed

### 1. `prices.option_prices` — 7,324 MB, 29% of the database

23.5M rows of ThetaData option EOD. **196 index scans in nine days.** The
subscription was cancelled 2026-06-07, the pull has been dormant since
2026-04-09, and `api/routers/filings.py` records that options performance was
*removed from the product* on 2026-08-13.

`prices.option_pull_status` (314K rows, 41 MB) is the same story.

Memory says the snapshot was deliberately kept as queryable. That is a fair
call — it is unre-fetchable now the subscription is gone. But it is 29% of the
database for something nothing reads, and a `pg_dump -t` to cold storage
preserves it just as well as a live table does.

**Options: (a) dump to a file and drop — reclaims 7.4 GB; (b) keep as-is.**

### 2. `public.filing_footnotes` — 1,445 MB, write-only

4.65M rows, **zero index scans**. Nothing reads footnotes at runtime — the only
non-write reference in the codebase is a `COUNT(*)` inside a backfill script.

This is the surviving copy of the corpus, so it is not redundant, it is simply
unused. Worth keeping only if footnote text is going to power something.

### 3. `research.derivative_trades` + `research.nonderiv_holdings` + `public.derivative_holdings` — 773 MB

1 index scan between them in nine days. CLAUDE.md already calls
`research.derivative_trades` "legacy; superseded by `trades.is_derivative=1`".

Two caveats against dropping blindly:
- It holds **1,162,052** derivative rows against only **11,029** flagged in
  `trades`, so coverage is *not* equivalent.
- Its `max(trade_date)` is **2030-03-19** — a future date, so option expiries
  are being stored in a transaction-date column. That is a data-quality bug in
  a table nobody reads, which is an argument for retiring rather than fixing.

### 4. `trade_decision_audit` — 293 MB, 468K rows, 1 index scan

Name implies an audit trail worth retaining. Needs a **retention policy**
(e.g. 90 days) rather than a drop.

### 5. `score_history` — 199 MB, 1.21M rows, 29 index scans

Overlaps `insider_ticker_scores`, which holds the same scores keyed by
`as_of_date` and is read 17.8M times. `api/routers/insiders.py` serves a score
history endpoint from it, so it is not dead — but it is a second copy of
point-in-time score data and a candidate for a view over
`insider_ticker_scores`.

---

## Left alone deliberately

Nine empty tables — `news`, `edgar_filings`, `event_8k`, `regsho_daily`,
`deploys`, `short_metrics`, `pull_status`, `dataset_manifest`,
`insider_market_sentiment`. A bare-word grep matches each of them in comments
or prompt text, and at 24–48 kB the space is not worth a false positive.

`insider_groups` (1,340 rows) and `insider_group_members` (2,855) look
droppable at 440 kB but are probed **1.3M and 1.07M times** — they are on a hot
join path and are working as intended.

---

## Recovery

`scripts/backup_databases.sh` dumps form4 nightly at 03:15 PT to
`/Users/derekg/backups/postgres/` and rsyncs off-box to the Mini. Four daily
dumps exist at 1.4 GB each.

```
pg_restore -d form4 -t <table> -n <schema> \
  /Users/derekg/backups/postgres/form4_20260820_031504.dump
```
