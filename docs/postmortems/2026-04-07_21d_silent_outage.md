# Postmortem — 21-day silent outage of three live paper trading strategies

**Status:** Resolved 2026-05-02
**Severity:** P0 (extended trading-decision path failure)
**Discovery date:** 2026-05-01 (manual investigation triggered by user question "how have our three trading algos been doing?")
**Outage window:** 2026-04-07 → 2026-04-28 (~21 days)
**Distribution:** Internal only — see `compliance_incidents/2026-04-07_pipeline_outage.md` for the compliance disposition.

## TL;DR

Three live paper trading strategies (`quality_momentum`, `reversal_dip`, `tenb51_surprise`) silently took **zero new positions for 21 days**. Runners stayed alive, heartbeats green, logs clean. Each pre-market scan dutifully reported `scan_signals: 0 candidates across 1 theses` because the analytical columns the strategies filter on (`pit_grade`, `above_sma50`, `dip_3mo`, `consecutive_sells_before`) had been NULL on every fresh trade since the SQLite→PG migration on 2026-04-07. The migration left several scripts behind, and no monitoring caught the resulting feature-population gap.

No customer harm: customer-facing dashboards on form4.app showed historical performance, which remained correct. The outage was invisible to users because the product surfaces only updated through the same broken pipeline. **No public/customer disclosure required.**

## Timeline

| Date (ET) | Event |
|---|---|
| 2026-04-07 | SQLite→PG migration of the trading data warehouse lands. Several legacy scripts (`compute_cw_indicators.py`, `backfill_intraday_events.py`, `price_utils.py`) continue reading from the SQLite `prices.db` cache that nothing writes to anymore. |
| 2026-04-07 → 04-28 | Each morning's pre-market scan returns 0 candidates per strategy. Heartbeats green. No alert fires. `compute-signals` continues running but operates on stale features and produces no fresh strategy-specific tags. |
| 2026-04-15 | `tenb51_surprise` exits MSTR via trailing stop (a previously-open position) — superficial evidence the runners are "alive." This is exit logic, not entry. |
| 2026-04-20 | Orphan-detection workflow re-tracks `BW` on `quality_momentum` (manual cleanup). Looks like normal activity in the order log; masks the deeper silence. |
| 2026-05-01 ~17:30 ET | User asks "how have our three trading algos been doing?" Investigation begins. Alpaca API check shows zero new entries in 14 days on QM, zero positions ever on RD. Logs pulled from Studio reveal `scan_signals: 0 candidates` daily across 21 days. |
| 2026-05-01 ~18:00 ET | PG audit reveals 604 P-code buys in the last 14 days; **zero** pass any strategy filter. Drill-down: `pit_grade` populated on 32% of fresh trades (vs 100% pre-04-07); `above_sma50` populated on <2% (vs 95% pre). |
| 2026-05-01 ~18:30 ET | Root cause identified: `compute_cw_indicators.py` is the script that populates these columns, but it is **not on a launchd schedule**. Its data source `prices.db` (SQLite) was a holdover from the pre-PG architecture and went stale. |
| 2026-05-01 ~18:45 ET | Reactive fix landed: synced PG→SQLite (645,503 rows), re-ran `compute_cw_indicators.py --since 2026-04-01` (15K trades populated), `build_pit_scores.py --start 2026-04-02` (8.7K trades scored), `backfill_pit_grades.py` (6,243 matched). Scheduled new daily `refresh-features.plist` (M-F 09:30 UTC). |
| 2026-05-01 ~22:00 ET | Verified post-backfill filter pass counts in 30-day window: QM 16, tenb51 1, RD 0 (these are the trades the runners *should have* entered while the pipeline was broken). RCG×15 on 2026-04-06 → 2026-04-09 + ISBA on 2026-04-17 are the qualifying trades that were missed. |
| 2026-05-02 | Reliability rebuild plan authored, approved, and Phase 1 begun (this document is part of Phase 1). |

## Root cause

**Direct cause:** the SQLite→PG migration on 2026-04-07 was non-atomic. Critical analytical columns on the `trades` table — `dip_3mo`, `dip_1mo`, `above_sma50`, `above_sma200`, `consecutive_sells_before`, `is_largest_ever`, `is_recurring`, `is_tax_sale`, `cohen_routine`, `is_10b5_1`, `is_rare_reversal`, `pit_grade` — are populated by `pipelines/insider_study/compute_cw_indicators.py` and downstream feeders. None of those feeders had a launchd schedule on Studio. They had to be invoked manually, and prior to 2026-04-07 they were being invoked as part of a since-deprecated workflow.

After the migration, the scripts that *did* still run (`insider-fetch`, `compute-signals`) succeeded with all-green exit codes, but their inputs were stale or NULL. The strategy runners' SQL filter — `WHERE above_sma50 = 1 AND ...` — silently matched zero rows when `above_sma50` was NULL. The runners reported "0 candidates" as a normal output, which is exactly what they would say on a quiet trading day with no qualifying setups. There was no way to distinguish "no qualifying trades exist" from "no qualifying trades can be evaluated because their inputs are missing."

**Contributing causes:**

1. **Silent exception swallowing in `fetch_latest.py:_run_indicators`** — the wrapper that calls `compute_cw_indicators.py` and `backfill_pit_grades.py` as subprocesses caught any failure with `logger.warning()` and continued. `insider-fetch` then marked the filings as "processed" even though indicators didn't compute. On the next run, those filings were skipped (dedup), so the gap persisted permanently.
2. **`compute_cw_indicators.py` reads from SQLite `prices.db`** — a holdover from the pre-PG architecture. The migration didn't touch the script, so it kept reading a cache that had stopped being written to.
3. **`backfill.py:312-316` exception handling caught `(sqlite3.OperationalError, Exception)` with string-match on `"already exists"`** — when `migrate_schema()` tried to ALTER TABLE on PG, `psycopg2.errors.DuplicateColumn` slipped past the catch and aborted the migration, contributing to the broken-state at the migration boundary.
4. **No data-freshness monitoring on the trading-decision path** — heartbeat checks pass; freshness checks did not exist.
5. **No daily zero-candidate alert** — the runner's `scan_signals: 0 candidates` log line was not a metric, and never triggered on a continuous-zero condition.
6. **No backtest-vs-live drift detection** — strategies have published expected CAGR ranges (e.g., quality_momentum: Sharpe 1.18, ~50 trades/yr) but no probe compared live activity to that distribution.

## Impact

**Trading impact:** 16 quality_momentum candidates, 1 tenb51_surprise candidate, and 0 reversal_dip candidates went un-entered during the outage window. Most are RCG×15 (single ticker, multiple insiders, 2026-04-06 to 04-09). Foregone P&L is unknown — would require running each candidate forward against actual price data and comparing to the strategies' historical hit rate.

**Customer impact:** None confirmed. The form4.app dashboards display historical performance + delayed signal info, which remained accurate (just stagnant). No "live position" UI was misrepresenting the state because no live positions were being added or removed.

**Reputational risk:** None observed (no user reports). However, had a customer questioned "why hasn't your strategy traded in three weeks?" we would have had no way to answer in real time. This near-miss is the impetus for the rebuild plan.

**Operational impact:** ~6 hours of investigation + reactive fixes on 2026-05-01–02. Plus 6+ weeks of rebuild work tracked in `~/.claude/plans/here-s-the-housing-thanks-cached-sifakis.md`.

## Why we missed it for 21 days

The system was producing the *correct* error mode — "0 candidates" — but for the *wrong* reason. The strategies were designed with the expectation that quiet days happen; what was missing was the meta-check "is the input data even being computed?". A dashboard that shows "today's signals" looks identical whether or not there are actual signals. A heartbeat shows the runner is alive whether or not its decisions are sound.

The detection method that finally worked: a human operator asked a *non-data-driven question* ("how are the algos doing?") and triggered an investigation. The system contained no automated equivalent.

## Action items

Tracked in the rebuild plan; lifted here for completeness. Each gets a GitHub issue.

| # | Action | Owner | Phase | Status |
|---|---|---|---|---|
| 1 | Schedule a daily `refresh-features` chain (sync prices → compute_cw_indicators → build_pit_scores → backfill_pit_grades) | derek | 1 (DONE 2026-05-01) | ✅ |
| 2 | Replace silent `logger.warning()` failures in `fetch_latest._run_indicators` with `raise IndicatorComputeError + Telegram + non-zero exit` | derek | 1.7 | ✅ |
| 3 | Replace string-match exception swallowing in `backfill.py:migrate_schema` with typed catches (`psycopg2.errors.DuplicateColumn`, `sqlite3.OperationalError`) | derek | 1.7 | ✅ |
| 4 | `sync_prices_sqlite.py` refuses to overwrite cache on 0-row PG fetch | derek | 1.7 | ✅ |
| 5 | `build_pit_scores.py` refuses to "succeed" with 0 trades found | derek | 1.7 | ✅ |
| 6 | Build `framework/contracts/freshness.py` + `config/freshness_contracts.yaml` — fail-closed contract enforcement | derek | 1.4 | ✅ |
| 7 | Build `scripts/freshness_probe.py` — alerts on freshness contract breach (transitions only) | derek | 1.5 | ✅ (needs Studio deploy) |
| 8 | Build `scripts/candidate_count_probe.py` — alerts on 0 candidates per strategy per market day | derek | 1.5 | ✅ (needs Studio deploy) |
| 9 | Plumb fail-closed contracts into `cw_runner.py:_build_thesis_query` and `conviction_score.py:compute_conviction` | derek | 1.4 (remaining) | pending |
| 10 | Migration ledger + idempotent runner (`pipelines/migrations/migrate.py`) | derek | 1.3 | pending |
| 11 | Pre-deploy gate via `studio --check` + preflight scripts | derek | 1.2 | pending |
| 12 | Strip secrets from plists, route through `run_with_env.sh`, rotate burned Alpaca + Telegram keys | derek | 1.1 | pending (key rotation needed) |
| 13 | Prometheus + Grafana + Alertmanager observability stack | derek | 2 | pending |
| 14 | `order_audit` + `trade_decision_audit` tables — every order traceable | derek | 2 | pending |
| 15 | Position reconciliation Alpaca↔DB daily | derek | 2 | pending |
| 16 | Trade-or-halt automation (runner refuses entries when freshness SLO breached) | derek | 3 | pending |
| 17 | Backtest↔live drift detection (Welch t-stat vs declared expected_cagr_*) | derek | 3 | pending |
| 18 | Add `reliability_engineer` persona to Board of Personas (hard gate, ≥7 to approve) | derek | 3 | pending |
| 19 | Quarterly audit checklist + first run | derek | 3 | pending |

## What we changed in our beliefs

1. **A heartbeat is not a freshness check.** Process liveness ≠ data validity. The reliability rebuild monitors data freshness as the primary SLI.
2. **"0 candidates" is a yes/no question, not a normal log line.** A continuous-zero condition over multiple market days is a P0 incident, not background noise.
3. **Silent NULL fallback is the bug pattern, not the fix.** `pit_grade or "C"` looked the same whether the insider was genuinely mid-grade or whether we had no data. The new contract-based runner refuses to operate on NULL inputs.
4. **Migrations are atomic schema events, not content edits.** Every consumer of a migrated table must update with the migration. Going forward, migrations carry a manifest of every reader/writer, and `studio --check` refuses partial migrations.
5. **The deploy gate must run on Mini before SSH.** Catching breakage post-deploy on Studio is too late. The gate is the inner loop.

## What we did right

1. **The investigation, once triggered, was fast and structured.** ~6 hours from "how are the algos doing?" to root cause identified and reactive fix in place.
2. **The reactive fix was minimal and surgical** — synced data, ran scripts, scheduled a recurring chain, no destructive changes to the runners.
3. **The PG migration itself was sound** — the schema is correct, the data is in PG, and the rest of the system uses PG correctly. The legacy SQLite reads were a debt, not a corruption.
4. **The conviction logic, when given proper inputs, produces correct candidates** — verified post-backfill (16 QM, 1 tenb51 over the outage window). The strategies are not broken; only their inputs were.

## What still keeps us up at night

- The investigation only happened because the operator asked. We need automated triggers, not human curiosity.
- The Phase 2 + 3 work (audit logging, reconciliation, drift detection, observability stack) is multi-week. Until it lands, we're in a "fixed but not hardened" state. The Phase 1 probes (#7, #8 in the action items above) are the bridge — they catch the same failure mode within 24 hours.
- We have no record of *what each customer saw* during the outage window. If a future incident has even slightly different shape (e.g., misrepresented live state), we'd struggle to scope customer impact. `order_audit` + `trade_decision_audit` (Phase 2) is the durable fix.
