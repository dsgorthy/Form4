# Trading Framework — Data Flow Spec

Living, comprehensive spec of every data flow in the trading framework. Built as the prerequisite for the **writer registry** that will fail-closed when any contracted column loses its recurring writer (the failure mode that silenced QM and RD for weeks before the 2026-05-16 audit).

**Scope.** Four ends of the system:
- Part 1 — ingestion pipelines (external → DB)
- Part 2 — trading state and Alpaca execution
- Part 3 — cron inventory, freshness contracts, gap analysis, writer-registry proposal
- Part 4 — per-table deep dive on the trades+scoring+signals layer (most PIT-sensitive)

**Generated:** 2026-05-16 / 2026-05-17 (this audit), against live Mac Studio state.

---

## Executive summary

### What the audit found

The framework's data layer is **structurally fragile in a small number of identifiable ways**. The 2026-05-16 strategy outage was not a one-off — it surfaced a pattern that has produced four silent failures in six weeks (career_grade, is_rare_reversal, intraday-backfill, candidate_count_probe). The pattern is always the same: a writer is added during a refactor, the cron wiring slips, the freshness contract still reports "green" (often because it's checking the wrong script's timestamp), and nothing alarms until a strategy stops producing candidates.

The good news: with one focused intervention (a writer registry that asserts wiring + verifies the contract → writer → cron chain at preflight), this entire failure class becomes structurally impossible. The proposal lives in Part 3 §3.5.

The bad news: **live money cannot ship until 2-3 specific code issues are fixed.** Items 1-2 below are show-stoppers — they make a live order's fill invisible to the automated safety nets that recover paper orders today.

### Top-priority findings (P0)

| # | Finding | Section | Blast radius |
|---|---|---|---|
| 1 | WebSocket fill listener + 5-min intraday resolver **hardcode paper-only account lists** | P2.6.2 | A live order's fill is invisible to both safety nets. Today these auto-recover paper timeouts (e.g. ACM on 2026-05-15). Live money cannot ship without fixing `framework/oms/alpaca_stream_listener.py:70-75` and `scripts/alpaca_intraday_resolver.py:54-58`. |
| 2 | `LiveBackend.enable_live=True` safety guard **never runs** in production | P2.6.2 | `cw_runner.get_alpaca` returns `PaperBackend` with a swapped base_url; the dedicated live backend at `framework/execution/live.py` is bypassed. The per-order live preflight gate never fires. |
| 3 | `freshness_contracts.yaml` **mislabels writers** for `is_rare_reversal` + `is_10b5_1` | 3.3.4 | Contract claims `populated_by: compute_cw_indicators.py`, but actual writers are `compute_switch_rate.py` and `backfill_live.parse_form4_xml` respectively. The contract reports green when the named script writes any column, masking real staleness. RD was silent 8 weeks under a green contract. |
| 4 | `career_grade` was **deliberately exempted** from the freshness check | 3.3 | `infra_audit.py:385 NON_CONTRACT_COLS` adds `career_grade` to the exemption list. Whoever added the column to QM filters added the exemption at the same time. The check designed to catch missing writers had an escape hatch for the exact column it should have caught. |
| 5 | `candidate_count_probe` alerts were **downgraded** so the QM silence never paged | 3.3.7 | Alert downgrade from `critical` → `warn` shipped 2026-05-13. Critical only fires after 5+ consecutive zero-candidate days. QM hit exactly 5 and was never paged. The probe also has a `NameError: consecutive_zeros` at line 268 — every market-day run exits non-zero. |

### Top-priority findings (P1)

| # | Finding | Section |
|---|---|---|
| 6 | `insider_track_records` table is **53 days stale** (last write 2026-03-24) and read by 14 routers + 6 pipelines despite CLAUDE.md saying don't use it | P4.9 |
| 7 | 9 plists are **missing from `admin_diagnostics.py:JOB_CATALOG`** including candidate-count-probe itself — the admin UI cannot surface its broken state | 3.3.8 |
| 8 | `compute_signals.py` runs **21 detectors and writes 0 `signal_freshness` rows** — every signal it produces is invisible to monitoring | P4 |
| 9 | `pit_n_trades` / `pit_win_rate_7d` are **48% populated with no recurring writer** — PIT-sensitive columns the strategy SQL reads (`pit_n`, `pit_wr` in candidate dicts) | P4.2.6 |
| 10 | `compute_week52_proximity.py` is **SQLite-only post-PG migration** — orphan writer; column 43.8% populated then frozen | P4.2.5 |
| 11 | `transaction_classifier.py` is **orphan** — `signal_quality`, `signal_category`, `is_routine` columns not refreshed | P4.2.6 |
| 12 | `insider_companies.last_trade = 2029-05-04` — **future-date data bug** survived dedup | P4.11 |
| 13 | `com.openclaw.intraday-backfill.plist` is **loaded with status=1** despite being decommissioned-in-effect since 2026-04-18 | 3.1a |
| 14 | `order_audit` has 6 rows for 2,228 strategy_portfolio rows → `max_daily_buys` guardrail history-window query returns 0 → **guardrail is effectively unlimited** | P2.6.2 |

### Top-priority findings (P2)

15. `client_order_id` is a random uuid on the V1 path (the active path), not deterministic from `decision_id` → Alpaca's server-side dedup never engages.
16. Stream listener overwrites `strategy_portfolio.entry_price` with the Alpaca fill — contradicts the cw_runner design comment ("data-API quote at decision time, NOT Alpaca fill"). Harmless on paper, real divergence on live with slippage.
17. Shared `{strategy}_state.json` between paper + live runners with no lock — race risk if both run.
18. No "live mode at $0 smoke test" mechanism — playbook jumps from preflight straight to real trading.
19. `deploys` table has 0 rows; the `studio` CLI deploy-writer is broken.
20. `processed_filings.processed_at` is stuck at 2026-04-05 and misleading; use `sync_meta.last_fetch_at`.
21. `cw_runner._get_latest_price` uses a fragile `..` path hack to share retry logic with `PaperBackend`.

### Where the framework is healthy

- **All 5 ingestion pipelines** are in steady state except ThetaData (intentionally dormant).
- **Today's Tier 1 fixes** (commit `2dcd9c9`) restored `career_grade`, `is_rare_reversal`, and `consecutive_sells_before` writers; all three now have fresh `signal_freshness` rows.
- **Engine cutover** (commit `8429d8d`) is live across all 3 paper strategies with deterministic same-or-better decisions vs V1.
- **PIT-honest core path** (`framework/pit/*`) is well-tested (84 unit tests) and now drives production filter+conviction.
- **Reconciliation safety nets** (stream listener, intraday resolver, daily reconcile) catch missed paper fills cleanly — the ACM 2026-05-15 timeout was auto-recovered without operator action.

### Recommended sequencing

Live money cannot ship until P0 items 1-2 are fixed. The writer registry (P0 items 3-5 are categorically defeated by it) unblocks both live money and the next 12+ weeks of strategy work. Therefore:

1. **Phase 0 (this audit, done):** spec doc + cross-cutting findings.
2. **Phase 1 (writer registry):** P0 items 3-5, P1 items 6-13. Estimated 2-3 days.
3. **Phase 2 (live-money safety):** P0 items 1-2, P2 items 14-18. Estimated 3-4 days.
4. **Phase 3 (live-money rollout):** install `quality-momentum-live` plist with a $1-2k starter allocation, monitor 2 weeks, scale to $10k.

P2 items 19-21 are cleanup, can lag.

---

## Table of contents

- **Part 1 — Ingestion pipelines** (lines ~155–290)
  - SEC EDGAR EFTS · Alpaca Data · Alpaca Trading · ThetaData · Daily bars
- **Part 2 — Trading state, execution, Alpaca integration** (lines ~298–632)
  - P2.1 Tables · P2.2 Order flow · P2.3 Live vs paper · P2.4 Safety mechanisms · P2.5 Studio state · P2.6 **Live-money readiness gaps**
- **Part 3 — Cron inventory, freshness contracts, gap analysis, registry proposal** (lines ~653–1124)
  - 3.1 Plist inventory · 3.2 Column→writer map · **3.3 Gap analysis** · 3.4 Pre-market dependency graph · **3.5 Writer registry proposal** · 3.6 Open questions
- **Part 4 — Per-table deep dive on scoring + signals layer** (lines ~1132–1597)
  - P4.1 PIT-stamped vs live-queried · P4.2 `trades` column-by-column · P4.3–P4.14 Per-table sections · P4.15 Conviction drift attribution · **P4.16 P0/P1 gaps**

---

# Part 1 — Ingestion Pipelines

Living catalog of every external→database pipeline. Built so a writer-registry preflight can refuse to start a strategy whose contracted column has no recurring writer.

**At-a-glance status:**

| Pipeline | Status | Cadence | Last write |
|---|---|---|---|
| 1. SEC EDGAR EFTS (Form 4) | Steady-state | every 5 min | `trades.filed_at = 2026-05-15 21:59:06`, `sync_meta.last_fetch_at = 2026-05-17 04:31:49` |
| 2. Alpaca Data API (equity quotes) | Steady-state | on demand (cw_runner / daily-prices) | n/a (no write — read-through for live decision logic) |
| 3. Alpaca Trading API (orders) | Steady-state | event-driven via WebSocket + 5-min resolver + daily reconcile | `order_audit.submitted_at = 2026-05-15 06:32:32`, `alpaca_position_snapshots.captured_at = 2026-05-15 13:30:30` |
| 4. ThetaData (options EOD) | **Dormant** since 2026-04-09 | n/a (manual pulls only) | `option_prices.trade_date = 2026-03-27`, ~7-week gap |
| 5. Alpaca daily bars (prices.daily_prices) | Steady-state | weekdays 17:30 PT | `prices.daily_prices.date = 2026-05-15` |

---

## 1. SEC EDGAR EFTS — Form 4 Insider Trades

**What this is:** The single ingest path for `trades`, the foundation of every insider strategy. Polls SEC EDGAR's full-text search (EFTS) endpoint every five minutes for newly-filed Form 4s, fetches each filing's XML, parses non-derivative + derivative transactions, and inserts them into the `trades` and `research.derivative_trades` tables. After a non-empty fetch, it transparently spawns `compute_cw_indicators.py` and `backfill_pit_grades.py` so analytical columns (`dip_3mo`, `above_sma200`, `pit_grade`, etc.) stay current for the cw_runner entry decision. Without this pipeline, none of the live paper strategies have anything to score.

**Why it exists:** EDGAR publishes Form 4s in near-real-time (typically minutes after filing). Quarterly bulk ZIPs are weeks late, so the live strategies need this streaming path. Replacement for the dormant `download_sec_bulk.py` archive flow.

**What depends on it:** All three live paper strategies (`quality_momentum`, `reversal_dip`, `tenb51_surprise`), every PIT scoring step, `compute_cw_indicators`, `backfill_pit_grades`, the entire `/feed`, `/clusters`, `/insiders` product surface.

| Field | Value |
|---|---|
| Source endpoint | `https://efts.sec.gov/LATEST/search-index` (filing index), `https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/` (filing XML). Defined `strategies/insider_catalog/backfill_live.py:80` (`EFTS_URL`), used at `backfill_live.py:151`, `:233`, `:269`. |
| Auth | None (SEC EDGAR is public). Required `User-Agent` header: `Form4/1.0 dsgorthy@hotmail.com` — `backfill_live.py:81`. Rate limit self-throttled at 0.12s/req (`REQUEST_DELAY`, `backfill_live.py:82`). |
| Frequency | Every 5 min (`StartInterval=300`) via launchd `com.openclaw.insider-fetch.plist`. Lookback window `--days 2`. |
| Idempotency | Yes — guarded by `processed_filings` (PK `accession`). `fetch_latest.py:100 get_known_accessions()` loads the seen set; every successful XML processing inserts `mark_processed()` at `:108`. XML-fetch failures still call `mark_processed(... trade_count=0)` (`fetch_latest.py:245`) to avoid retry storms on bad filings. `trades` insertion uses `INSERT OR IGNORE` at `backfill_live.py:796` keyed on the table's natural-key UNIQUE constraint. |
| Error handling | (a) XML fetch failure: counted into `xml_failures`, accession marked processed with 0 trades. (b) Indicator subprocesses (`_run_indicators`, `fetch_latest.py:121`) **hard-fail**: any non-zero exit raises `IndicatorComputeError` after writing to `logs/alerts.ndjson` at severity `critical`. This is the post-April-2026 outage fix — previously these were `logger.warning` and silently marked filings processed → 21-day silent feature gap (see `docs/postmortems/2026-04-07_21d_silent_outage.md`). (c) EDGAR HTTP 500s: requests library raises after timeout=30; the wrapping `for filing in new_filings` loop swallows individually-failed XML fetches and continues. (d) No DB-side retry — sqlite/psql lock failures are caught and treated as duplicate. |
| Writes (table.column) | **`trades`**: `insider_id, ticker, company, title, trade_type, trade_date, filing_date, price, qty, value, is_csuite, title_weight, source='edgar_live', accession, normalized_title, filed_at, trans_code, trans_acquired_disp, direct_indirect, shares_owned_after, value_owned_after, nature_of_ownership, equity_swap, is_10b5_1, security_title, deemed_execution_date, trans_form_type, rptowner_cik, is_derivative` — `backfill_live.py:795`. **`insiders`**: `name, name_normalized, cik, is_entity, display_name` (via `get_or_create_insider` + post-insert UPDATE in `fetch_latest.py:289`). **`processed_filings`**: `accession, filing_date, trade_count, processed_at` (`fetch_latest.py:111`). **`sync_meta`**: `('last_fetch_at', <utc-now>)` (`fetch_latest.py:73`). **`research.derivative_trades`**: same column set as trades but derivative-side (`backfill_live.py:840`). **`signal_freshness`**: row for `public.trades.filing_date` only when `inserted > 0` (`fetch_latest.py:319`). Indirect via subprocess: `compute_cw_indicators` writes most `trades.<indicator>` columns + their `signal_freshness` rows; `backfill_pit_grades` writes `trades.pit_grade`. |
| Freshness contract | `trades.filing_date` — `max_staleness_hours=48`, `required_for: ['*']`, `populated_by: strategies/insider_catalog/fetch_latest.py`. Configured in `config/freshness_contracts.yaml`. **Caveat**: timestamp only refreshes on `inserted > 0`; the 48h SLO covers weekends where the runner does 0-row fetches. |
| Health monitoring | `sync_meta('last_fetch_at')` — updated every successful run regardless of inserts (`fetch_latest.py:329`). Log: `logs/insider-fetch.log`. Alerts: `logs/alerts.ndjson` (severity `critical`) on indicator subprocess failure (`_run_indicators` final `raise IndicatorComputeError`). No external paging — file-based per `framework/alerts/log.py:6`. |
| Known issues | (1) `datetime.utcnow()` DeprecationWarning floods log (`fetch_latest.py:72`). (2) `processed_filings.processed_at` shows `MAX = 2026-04-05 15:05:27` — column populated only on initial backfill insert from trades, never updated since (`INSERT OR IGNORE` on existing accession is a no-op). Use `trades.created_at` (`MAX = 2026-05-15 19:02:57`) or `sync_meta.last_fetch_at` for liveness. (3) Indicator subprocess uses `--since` window of last 7 days (`fetch_latest.py:168`) to avoid recomputing 1.5M historical trades every 5 min — the broader scan is in the 06:00 PT `refresh-features` job. (4) EFTS occasionally returns HTTP 500s during indexing — no explicit retry, the next 5-min run picks them up. |
| Last successful run | `sync_meta('last_fetch_at') = 2026-05-17 04:31:49` UTC. `MAX(trades.filed_at) = 2026-05-15 21:59:06`. `MAX(trades.created_at) = 2026-05-15 19:02:57-07`. Log tail confirms 0-new-filings cycles continuing every 5 min through 21:31 PT 2026-05-16. **Status: HEALTHY**. |

---

## 2. Alpaca Data API — Equity Prices/Trades for Live Quotes

**What this is:** Read-only HTTPS client to `data.alpaca.markets/v2` for equity bars, trade snapshots, and latest quotes. No state lives here — every call is a request-response read-through. Used in two places: (a) `cw_runner._get_latest_price` reaches it for an order-time mark; (b) `update_daily_prices.py` bulk-fetches daily bars for the `prices.daily_prices` ingest covered in pipeline #5. Distinct from the trading API (`paper-api.alpaca.markets`) which executes orders.

**Why it exists:** Without a real-time quote, `cw_runner` can't size a market buy or compute a stop-out reference. The data API is also the only Alpaca-side source for historical daily bars that `prices.daily_prices` requires.

**What depends on it:** `cw_runner` entry execution at the moment a candidate qualifies (sizing), `cw_runner` exit logic for stop comparisons, and the daily-prices ingest pipeline (#5). The decommissioned `intraday-backfill` plist used to call it too — that service is now exited per `launchctl list` (status code 1, no longer reloading).

| Field | Value |
|---|---|
| Source endpoint | `https://data.alpaca.markets/v2/stocks/{symbol}/bars`, `/snapshot`, `/trades/latest`. Base URL `framework/data/alpaca_client.py:62`. Bars adjustment=`raw` or `split`, feed selectable `sip|iex|otc` (`alpaca_client.py:64`). |
| Auth | Headers `APCA-API-KEY-ID` + `APCA-API-SECRET-KEY` (`alpaca_client.py:84`). **Env vars: `ALPACA_DATA_API_KEY` / `ALPACA_DATA_API_SECRET`** (read-only data credentials, shared across all consumers). Loaded from `.env` on Studio via `python-dotenv`. cw_runner code path: `cw_runner.py:191 _data_api_headers()`. daily-prices code path: `update_daily_prices.py:38 load_alpaca_credentials()`. |
| Frequency | On demand — every time `cw_runner` evaluates an entry candidate (a few times per cycle when there are open candidates), and once per weekday at 17:30 PT for the bulk price refresh. |
| Idempotency | N/A — read-only. Repeated calls return the same payload for the same time window. |
| Error handling | 3 retries with exponential backoff (`alpaca_client.py:97 _request`). Retries on 429 + 5xx; respects `Retry-After` header. Self-throttled at 200 req/min (`_RATE_LIMIT_PER_MIN = 200`, `alpaca_client.py:17`). cw_runner's `_get_latest_price` (`cw_runner.py:1018`) tries the trade-account base URL first via a relative `..` hack then falls back to a direct `requests.get` with the data API headers; failures silently return `None` and entry sizing degrades to the previous-day close. **No DB write — failures are operationally invisible.** |
| Writes (table.column) | None directly. Indirect: when used by `update_daily_prices.py`, writes flow into `prices.daily_prices` (see pipeline #5). |
| Freshness contract | None — read-through. |
| Health monitoring | None at the client level. Failures surface as cw_runner WARN logs (`cw_runner.py:1036`) and in `logs/daily-prices.log` (e.g., HTTP 400 lines for invalid symbols in the May 15 log tail). |
| Known issues | (1) SIP feed (paid) has been failing 403 since the subscription lapsed circa 2026-04-26 — `update_daily_prices.py:122` defaults to `iex` now. (2) The `_get_latest_price` URL composition uses a `..` path hack (`cw_runner.py:1021`) — fragile but works because PaperBackend's `base_url` already contains `/v2`. (3) Symbols with punctuation like `CIG,CIGC`, `CFTR-PRA` produce 400s — logged and skipped. |
| Last successful run | n/a (no persisted timestamp). Inferred from `prices.daily_prices.date = 2026-05-15` (latest bar) and cw_runner heartbeats (`quality_momentum_heartbeat.json` mtime 2026-05-16 21:10). |

---

## 3. Alpaca Trading API — Orders, Positions, Account State

**What this is:** Order submission and fill confirmation against `paper-api.alpaca.markets/v2` (paper) — and `api.alpaca.markets/v2` when `quality_momentum_live` launches. Spans three concrete services: (a) `cw_runner` POSTs orders via `PaperBackend.submit_order`, (b) the `alpaca-stream-listener` WebSocket daemon writes fills to `order_audit` + `strategy_portfolio` in real time, (c) the `alpaca-intraday-resolver` 5-min job is the safety-net poller that catches anything the WebSocket missed, and (d) `alpaca-reconcile` runs nightly to snapshot account positions and detect strategy↔broker drift.

**Why it exists:** Trading positions must reconcile with broker state, and the same shared paper accounts can be modified outside the strategy code (manual sell from the Alpaca dashboard, etc.). The two-layer architecture (event stream + safety-net poll + nightly snapshot) is the post-Q1 2026 redesign that closed the "timeout — manual verify needed" class of false-positive critical alerts.

**What depends on it:** All three live paper strategies' fill confirmation; `strategy_portfolio` open/close lifecycle; the Form4.app `/portfolio` view; the `/admin` diagnostics page; the alpaca_position_snapshots time-series for the operator's drift dashboard.

| Field | Value |
|---|---|
| Source endpoint | **Order submit**: `https://paper-api.alpaca.markets/v2/orders` POST — `framework/execution/paper.py:20 PAPER_API_BASE`, `paper.py:116 submit_order`. **Order poll**: `/v2/orders/{id}` GET via `PaperBackend._request` (`paper.py:174`, `alpaca_intraday_resolver.py:112`). **Position snapshot**: `/v2/positions` GET via `PaperBackend.list_positions` (`paper.py:228`) called from `alpaca_reconcile.py`. **WebSocket**: `wss://paper-api.alpaca.markets/stream` — `framework/oms/alpaca_stream_listener.py:66 PAPER_URL` (live path `wss://api.alpaca.markets/stream`, `:67`). |
| Auth | Per-strategy dedicated paper account. Env vars derived from yaml `alpaca_env_prefix`: `ALPACA_API_KEY_QUALITY_MOMENTUM` / `ALPACA_API_SECRET_QUALITY_MOMENTUM`, same pattern for `REVERSAL_DIP` and `TENB51_SURPRISE`. Loaded from `.env` via `python-dotenv` in each consumer's startup (`alpaca_stream_listener.py:51`, `alpaca_intraday_resolver.py:40`). Account registry hard-coded in `alpaca_stream_listener.py:70 ACCOUNTS` and `alpaca_intraday_resolver.py:54`. |
| Frequency | (a) `cw_runner` order submission — on demand at decision time (one cycle per strategy plist, ~once per market minute during active hours). (b) `alpaca-stream-listener` — persistent WebSocket (`KeepAlive=true`), one task per account, plus a heartbeat-write every 60s. (c) `alpaca-intraday-resolver` — every 5 min (`StartInterval=300`), no-op outside market hours by `is_market_open()` check. (d) `alpaca-reconcile` — weekdays 13:30 PT (`StartCalendarInterval`, 30 min after close). |
| Idempotency | (a) Order submission — passes `client_order_id` to Alpaca (`paper.py:113`), which Alpaca de-dups server-side. cw_runner generates a deterministic `order_id` per decision and uses it as `client_order_id` at submission time. (b) `order_audit` write on decision is `INSERT INTO order_audit` (PK `order_id`); subsequent fill-side updates are `UPDATE ... WHERE order_id`. The full audit-side writer (`framework/oms/audit.py:104`) is `ON CONFLICT (order_id) DO UPDATE` (upsert). (c) `alpaca_position_snapshots` — append-only, every reconcile run inserts fresh rows. (d) `alpaca_reconciliation` — `upsert_divergences` keeps one open row per `(strategy, ticker, issue_type, is_live)` and resolves cleared rows by setting `resolved_at`. |
| Error handling | (a) Submit_order: 3 retries on 429/5xx (`paper.py:67`); on final failure returns an `OrderResult(status='rejected', error=str(exc))` — the row is still written to `order_audit` as fill_status `rejected` by `_record_alpaca_outcome` (`cw_runner.py:288`). (b) WebSocket listener: reconnect with exp backoff capped at 300s (`alpaca_stream_listener.py:236`); `auth status != authorized` raises and triggers reconnect. (c) Resolver: per-order Alpaca query failure logs WARN and continues to next order (`alpaca_intraday_resolver.py:114`); only acts on terminal Alpaca statuses (`filled`, `rejected`, `canceled`, `expired`, `done_for_day`). (d) Reconcile: per-strategy try/except, partial failures don't abort the run. |
| Writes (table.column) | **`order_audit`**: `order_id (PK), strategy, ticker, side, qty, order_type, conviction_score, pit_grade, signal_inputs_json, decision_rationale, config_version_sha, config_yaml_sha, decided_at, alpaca_order_id, submitted_at, fill_status, fill_price, fill_qty, filled_at, rejection_reason, is_live`. cw_runner's `_record_alpaca_outcome` (`cw_runner.py:288`) updates submission-side columns. alpaca-stream-listener's `_update_order_audit_fill` (`alpaca_stream_listener.py:82`) and `_update_order_audit_terminal` (`:134`) set terminal state. alpaca-intraday-resolver does the same at `:135` and `:170`. **`strategy_portfolio`** (touched by all three): `entry_price, shares, actual_fill_price` on fill (`alpaca_stream_listener.py:117`, `alpaca_intraday_resolver.py:151`); `status='closed', exit_date, exit_reason, pnl_pct=0, pnl_dollar=0` on reject (`alpaca_stream_listener.py:159`, `alpaca_intraday_resolver.py:178`). **`alpaca_position_snapshots`**: `strategy, ticker, qty, avg_entry_price, market_value, current_price, unrealized_pl, is_live, captured_at` (auto-default `now()`) — `alpaca_reconcile.py:118`. **`alpaca_reconciliation`**: `strategy, ticker, issue_type, severity, db_qty, alpaca_qty, db_entry_price, alpaca_avg_cost, db_status, portfolio_id, detail, detected_at, resolved_at, resolution, is_live` — `alpaca_reconcile.py:256` (open) and `:281` (resolve). **`trade_decision_audit`**: append-only on every decision (`framework/oms/audit.py:39`). |
| Freshness contract | None on `order_audit` or `alpaca_position_snapshots` directly — these are write-by-event tables and a quiet day legitimately has 0 inserts. The reverse contract (a heartbeat) is the alpaca_stream_listener's `alpaca_stream_heartbeat.json` file (`alpaca_stream_listener.py:77`). |
| Health monitoring | (a) WebSocket: `strategies/cw_strategies/data/alpaca_stream_heartbeat.json` rewritten every 60s (`alpaca_stream_listener.py:281 heartbeat_loop`) — content: `{service, timestamp, accounts, status}`. Surfaced on `/admin/system-status`. (b) Resolver: `logs/alpaca-intraday-resolver.log` — repeating "Outside market hours" messages every 5 min are healthy. (c) Reconcile: `logs/alpaca-reconcile.log` — runs once on weekday afternoons. (d) Stream listener uses launchd `KeepAlive=true` + `ThrottleInterval=30` for crash recovery. |
| Known issues | (1) WebSocket connection drops ~daily during the off-hours window — reconnect logs visible at 01:06, 05:01 in current log. Exp backoff masks fully but flares ERROR severity (`alpaca_stream_listener.py:273`). (2) `_update_order_audit_terminal` requires `alpaca_order_id` to be set before the fill arrives — if the order is rejected before cw_runner's `_record_alpaca_outcome` writes back, the row stays in pending forever. The resolver catches this. (3) `alpaca_reconciliation.detected_at` is the column to use, not `created_at` (which doesn't exist). (4) `order_audit.fill_status` accepts free-form strings — no enum constraint. Possible values seen: `pending, filled, rejected, canceled, expired, timeout, partially_filled, new, accepted`. (5) `cw_runner._get_latest_price` shares no retry budget with `PaperBackend._request` — `_request` retries 3x while `_get_latest_price` adds a second fallback path. |
| Last successful run | `MAX(order_audit.submitted_at) = 2026-05-15 06:32:32-07` (Friday open). `MAX(order_audit.decided_at) = 2026-05-15 06:31:01-07`. `MAX(alpaca_position_snapshots.captured_at) = 2026-05-15 13:30:30-07` (Friday reconcile). `MAX(alpaca_reconciliation.detected_at) = 2026-05-13 13:30:25-07` (last divergence event). `MAX(trade_decision_audit.ts) = 2026-05-16 21:08:59-07` (decision audit ran Saturday — cw_runner cycles continue even market-closed). Stream listener heartbeat fresh: `2026-05-17T04:33:04+00:00`. **Status: HEALTHY** — all four moving parts active. |

---

## 4. ThetaData Options Pipeline — EOD Options Bars (DORMANT)

**What this is:** A locked dual-DTE-strike sweep that, for every insider trade event, pulls EOD option bars (4 hold periods × 2 DTE types × 4 strike types = 32 EOD calls per event) from a self-hosted ThetaData Terminal jar listening on `127.0.0.1:25503`. Pull script reads events from the `trades` table, walks expirations, writes structured rows into `prices.option_prices` and per-event status into `prices.option_pull_status`. Phase 0c pull was intended to produce a complete historical options dataset for insider event backtests.

**Why it exists:** The intent is event-study options analysis — for any insider buy/sell, what would a calls/puts position have looked like 7d/14d/30d/60d out at multiple strikes? Currently dormant per `pipeline_options_backfill.md` in Claude memory; no live strategy depends on it. The data is read by archived research scripts (`options_analyze.py`, `options_backtest.py`, `options_grid_sweep.py`) only.

**What depends on it:** Nothing in the live trading path. Pure research substrate. Status field in `pipeline_options_backfill.md` says "DORMANT since 2026-04-09" — confirmed: no `com.openclaw.options*` or `com.openclaw.theta*` plist on Studio.

| Field | Value |
|---|---|
| Source endpoint | `http://127.0.0.1:25503` — defined `pipelines/insider_study/theta_client.py:38 THETA_BASE`, queried at `:219`. Service-side: ThetaData Terminal v3 jar at `/Users/derekg/thetadata/ThetaTerminalv3.jar` (per `ls /Users/derekg/thetadata/` on Studio). **Currently not running** — `ps -ef | grep -i theta` returns nothing; port 25503 is not bound. |
| Auth | ThetaData credentials in `/Users/derekg/thetadata/creds.txt` on Studio (Java terminal reads). No HTTP auth — script trusts localhost binding. |
| Frequency | None — was a manual `--full --from-db` invocation. No plist installed. Last manual completion: 2026-04-09. |
| Idempotency | Yes — `option_prices` insert is `INSERT OR IGNORE` (`options_pull.py:160`). `option_pull_status` is `INSERT ... ON CONFLICT (ticker, trade_date, trade_type) DO UPDATE` (`options_pull.py:172`). The script also keeps an outer dedup against pulled events in PG, replacing the old `theta_cache.db` SQLite which is currently MISSING per `pipeline_options_backfill.md`. |
| Error handling | Async pulls with database-lock retry (`OptionPriceWriter._execute_with_retry`, `options_pull.py:109` — up to 5 retries). Per-event try/except in main loop; failures don't abort the batch. ValueError/KeyError on malformed Theta records silently dropped (`options_pull.py:156`). No external alerts — script only logs to stdout. |
| Writes (table.column) | **`prices.option_prices`**: `ticker, expiration, strike, right (C/P), trade_date, open, high, low, close, volume, bid, ask, bid_size, ask_size, source='thetadata'` — `options_pull.py:160`. **`prices.option_pull_status`**: `ticker, trade_date, trade_type, contracts_found, contracts_empty` — `options_pull.py:172`. No `signal_freshness` writes. |
| Freshness contract | None registered in `config/freshness_contracts.yaml` (correctly — no live consumer). |
| Health monitoring | None active. `pull_monitor.sh` referenced in older docs is **MISSING** on Mini, Studio, and in the repo per the project CLAUDE.md gotcha. `PROGRESS.md` is auto-updated by the script (`update_progress_md`, `options_pull.py:190`) but only while the script runs. |
| Known issues | (1) Java jar not running on Studio — bringing this pipeline back requires `java -jar /Users/derekg/thetadata/lib/202602131.jar` (legacy reference per project CLAUDE.md, actual file appears to be `ThetaTerminalv3.jar` now). (2) `theta_cache.db` missing — resume relies purely on `option_pull_status` per-event dedup. (3) ~26% of events will never have options data (OTC/microcap with no listed options) — these become `contracts_empty>0`. (4) MAX(`option_prices.trade_date`) = `2026-03-27`, ~7-week gap from current date. (5) The script's `INSIDERS_DB` path constant at `options_pull.py:95` still points at the legacy SQLite path — only the `--from-db` flag uses Postgres via `config.database.get_connection`. |
| Last successful run | `MAX(prices.option_prices.trade_date) = 2026-03-27`, `MAX(prices.option_pull_status.trade_date) = 2026-03-27`. Row counts: `option_prices = 23,505,903`, `option_pull_status = 314,026`. **Status: DORMANT** — no java terminal, no plist, no recent inserts. |

---

## 5. Alpaca Daily Bars → prices.daily_prices

**What this is:** Weekday-evening bulk refresh that walks every ticker appearing in recent `trades` rows (plus the benchmark ETFs SPY/QQQ/IWM/TLT/GLD), queries Alpaca's daily-bar endpoint, and upserts new rows into `prices.daily_prices`. Replaces the original CSV→sync ingest that died in March (the upstream CSVs stopped refreshing). All single-price lookups via `price_utils.get_close(ticker, date)` resolve out of this table; the portfolio overlay, idle-cash benchmarks, dip_3mo/sma200 backfills, and the `/portfolio` view all read it.

**Why it exists:** Single source of EOD-quality daily bars covering every ticker the insider catalog ever touches. The CSV path that fed it before March 2026 was abandoned because no upstream process refreshed the CSVs — `update_daily_prices.py` is the structural replacement.

**What depends on it:** Every `prices.daily_prices` reader: `price_utils.get_close`, `pipelines/insider_study/compute_cw_indicators.py` (`above_sma200`, `dip_3mo`), `portfolio-overlay.tsx` idle-cash sleeve, all backtest scripts, the `prices.daily_prices.date` freshness contract that the strategy runner asserts before scanning.

| Field | Value |
|---|---|
| Source endpoint | `https://data.alpaca.markets/v2/stocks/{symbol}/bars` via `AlpacaClient.get_daily_bars` (`framework/data/alpaca_client.py:169`). Adjustment defaults to `split`; feed defaults to `iex` (free) — `update_daily_prices.py:122`. |
| Auth | Shared read-only data credentials — env vars `ALPACA_DATA_API_KEY` / `ALPACA_DATA_API_SECRET` from `.env`, loaded by `load_alpaca_credentials()` at `update_daily_prices.py:38`. |
| Frequency | Weekdays 17:30 PT (4.5h after market close so Alpaca daily bars are settled) via `com.openclaw.daily-prices.plist`. `StartCalendarInterval` repeated Mon–Fri. |
| Idempotency | Yes — `INSERT OR IGNORE INTO daily_prices` on `(ticker, date)` UNIQUE (`update_daily_prices.py:107`). `get_latest_dates()` (`:78`) reads `MAX(date) per ticker` and skips tickers already current. New tickers fetch the last `--lookback-days` (default 60) of bars. |
| Error handling | Per-ticker try/except (`update_daily_prices.py:165`) — failures append the ticker to a `failed` list and continue. `AlpacaClient._request` has 3 retries with backoff on 429/5xx. Invalid symbols (e.g. `CIG,CIGC`, `CFTR-PRA`) return HTTP 400 and are silently dropped. No external alert on per-ticker fail. End-of-run summary logs total inserted, updated, skipped, failed. |
| Writes (table.column) | **`prices.daily_prices`**: `ticker, date, open, high, low, close, volume` (`update_daily_prices.py:108`). **`signal_freshness`**: row for `prices.daily_prices.date` only when `total_rows > 0` (`update_daily_prices.py:187`). |
| Freshness contract | `prices.daily_prices.date` — `max_staleness_hours=72`, `required_for: ['*']`, `populated_by: pipelines/insider_study/update_daily_prices.py` (`config/freshness_contracts.yaml`). 72h covers weekends — the plist runs Mon–Fri only, so Sunday's strategy run reads Friday's data (~63h old). |
| Health monitoring | `logs/daily-prices.log` (last write 2026-05-15 17:42:39). `signal_freshness` row keeps a row-count alongside the timestamp. The runner's `assert_freshness_system_healthy` (`framework/contracts/freshness.py:218`) raises `FreshnessSystemBrokenError` if no `signal_freshness` row exists; `assert_fresh` raises `StaleSignalError` if older than the 72h SLO. |
| Known issues | (1) SIP feed has been failing 403 since the subscription lapsed circa 2026-04-26; `--feed` default switched to `iex` (`update_daily_prices.py:122`). IEX is lower-volume single-venue data but adequate for EOD usage. (2) `--max-tickers=2000` cap (`update_daily_prices.py:119`) — current corpus is 1,589 tickers; cap is comfortable headroom. (3) Symbols with punctuation (commas, dots) produce 400s — see Alpaca-Data section #2 known issues. (4) The plist uses `/usr/bin/python3` (system Python 3.9) rather than `/opt/homebrew/bin/python3.12` like the insider-fetch plist — inconsistent runtime but functional. |
| Last successful run | `MAX(prices.daily_prices.date) = 2026-05-15` (Friday). Log: `2026-05-15 17:42:40,000 INFO daily_prices MAX(date) after update: 2026-05-15`. `signal_freshness` row for `prices.daily_prices.date` confirmed in the recent freshness-by-timestamp query. **Status: HEALTHY**. |

---

## Cross-cutting Notes

- **Signal_freshness coverage gap:** `trades.filing_date` (ingest proxy) and `prices.daily_prices.date` are the only two raw-ingest columns wired to `framework/contracts/freshness_writer.write_freshness`. Every analytical column on `trades` (`dip_3mo`, `pit_grade`, etc.) is written by downstream subprocesses that `insider-fetch` triggers — the compute pipelines, not the ingest pipeline, own those freshness rows. For a writer-registry preflight, ingest contracts are a thin superset of what's listed here (2 columns); the compute side covers ~14 more contracted columns.
- **Single dormant pipeline:** only ThetaData. Everything else is in steady-state as of 2026-05-16.
- **Alert path consolidation:** all four active pipelines write to the same `logs/alerts.ndjson` NDJSON file (`framework/alerts/log.py`) at varying severities. No Telegram, no email, no PagerDuty.
- **Studio-only constraint:** every plist named above is in `~/Library/LaunchAgents/com.openclaw.*` on Studio (`100.78.9.66`). `~/.local/bin/studio` `guard_studio_only_plists` is the deploy-time check that prevents these from autoloading on Mini.

---

# Part 2 — Trading State, Execution, Alpaca Integration

*Scope: the layer where live-money safety lives. Decision-made → order-on-broker → reconciliation. Generated 2026-05-16 against the live state of Mac Studio.*

Live DB: PostgreSQL `form4` on Mac Studio (`derekg@100.78.9.66`, socket `/tmp`).
Plists: `~/Library/LaunchAgents/com.openclaw.*` on Studio.
Live state snapshot taken: 2026-05-16, 21:35 ET.

## P2.1 Table inventory

### P2.1.1 `strategy_portfolio` — canonical strategy state

| Field | Notes |
|---|---|
| **Writers** | `strategies/cw_strategies/cw_runner.py:1451-1488` (entry INSERT), `cw_runner.py:1843-1858` (scheduled exit UPDATE), `cw_runner.py:2028-2054` (check_exits UPDATE), `cw_runner.py:1253-1260` and `1289-1296` (replace_oldest / replace_weakest UPDATEs), `framework/oms/alpaca_stream_listener.py:104-131` (fill-time entry_price/shares correction), `framework/oms/alpaca_stream_listener.py:146-169` (reject/cancel → status=closed), `scripts/alpaca_intraday_resolver.py:150-189` (intraday fill + reject correction), backtest/simulated rows via `pipelines/run_backtest.py`, `scripts/seed_open_positions.py` (boot-time seeding). |
| **Schedule** | Real-time (cw_runner market-hours loop: 30 min for entries, 5 min for exit checks). Stream-listener writes are event-driven from Alpaca WebSocket. |
| **Consumers** | Admin views (`/admin/jobs`, `/admin/system-status`), `api/routers/portfolio.py`, `api/routers/strategies.py`, P&L overlays, daily summary, reconcilers, the runner itself (capacity/dedup checks at lines 1101, 1126, 1198, 2320, 2459). |
| **Freshness** | Live. Healthy if heartbeat files are <30 min old during market hours. |
| **PIT** | N/A — realised-state ledger, not a research surface. |
| **Known issues** | (a) Five `execution_source` values coexist (`backtest`, `backtest_v3`, `simulated`, `paper`, `live`). Runners filter `execution_source IN ('paper','live')` for capacity (lines 1101-1103, 1198-1205, 1888-1889). (b) Three open `tenb51_surprise` rows are `paper`; five others are `simulated` — schema drift between cw_runner and an older backfill. (c) `reversal_dip` has 705 closed `simulated` rows but ZERO open or paper rows despite the plist running — either no entries triggered since plist load or entire history was retro-simulated. (d) `is_live` is FALSE on every one of the 2,228 rows. No live-money trade has happened yet. (e) `entry_price` design intent is "data-API quote at decision time" (`cw_runner.py:1374-1380` comment), but the stream listener overwrites it with actual fill price on every fill (`alpaca_stream_listener.py:117-131`). The design comment and the runtime behavior disagree. |

**Critical invariant from `cw_runner.py:1374-1488`:** strategy_portfolio is canonical and decoupled from Alpaca. The row is inserted BEFORE the order is submitted; an Alpaca exception, timeout, or rejection does NOT roll the row back. The stream listener / intraday resolver are responsible for marking the row `closed` with `exit_reason='alpaca_rejected'`.

### P2.1.2 `order_audit` — every Alpaca submission's decision + outcome

| Field | Notes |
|---|---|
| **Writers** | **V1 path (active today)**: `cw_runner._record_order_decision` (`cw_runner.py:247-285`) inserts `fill_status='pending'`; `cw_runner._record_alpaca_outcome` (288-320) UPDATEs with outcome. **V2 path (env-gated, disabled today)**: `framework/oms/audit.write_order` (audit.py:76-146) upserts via INSERT…ON CONFLICT using the Order state machine. Stream listener `_update_order_audit_fill` (82-101) and `_update_order_audit_terminal` (134-143) update by `alpaca_order_id`. Intraday resolver `resolve_order` (135-189) UPDATEs by `alpaca_order_id`. |
| **Schedule** | Decision-time row at every entry/exit. Outcome UPDATE inline after `wait_for_fill` (300s buy / 90s sell, cw_runner.py:1569 / 2103), via WebSocket event (sub-second), or via 5-min intraday resolver sweep. |
| **Consumers** | `framework.risk.guardrails._count_orders_today` (the daily-buy/sell guardrail), `gate_unresolved_orders` (preflight), admin pages, alpaca-reconcile (joins by alpaca_order_id). |
| **Freshness** | Filled or rejected within minutes. >1h non-terminal blocks live launch. |
| **PIT** | `signal_inputs_json` captures PIT-grade and conviction at decision time so the trade is replayable. |
| **Known issues** | **(a) Only 6 rows total vs 2,228 in strategy_portfolio.** The `_record_order_decision` helper was bolted in during the OMS V2 rebuild (May 2026); all legacy paper entries and backtest rows never wrote here. Implication: the daily-buys / daily-sells guardrail (`guardrails.py:104`) sees near-zero history → effectively no rate limit. The pre-launch `gate_unresolved_orders` is meaningful only forward. (b) `OMS_V2` env is NOT set on Studio. All writes are V1. (c) `client_order_id` (which becomes `order_id`) is a random uuid on V1 (cw_runner.py:1414), deterministic from decision_id ONLY on V2. V1 path does NOT get Alpaca's server-side dedup protection — comment at `paper.py:111` ("Alpaca dedups on client_order_id — closes the dual-host race") is only true on V2. |

### P2.1.3 `alpaca_position_snapshots` — periodic Alpaca-side mirror

| Field | Notes |
|---|---|
| **Writers** | `scripts/alpaca_reconcile.snapshot_alpaca_positions` (102-125). One row per Alpaca position per reconcile run. |
| **Schedule** | Weekdays 13:30 PT via `com.openclaw.alpaca-reconcile` plist (30 min after market close). `KeepAlive=false`, `RunAtLoad=false` — single-shot per day. |
| **Consumers** | Admin diff views. **Not consumed by detect_divergences itself** — that function uses `alpaca.list_positions()` live and treats this table as descriptive only. |
| **Freshness** | Daily. Last write 2026-05-15 13:30:30 PT (10 rows QM, 21 rows tenb51). |
| **PIT** | N/A. |
| **Known issues** | (a) `reversal_dip` has ZERO snapshots ever, because reconcile early-returns when both DB and Alpaca empty (`alpaca_reconcile.py:107`). (b) `is_live` always FALSE today. (c) Per-day row counts aren't 1-per-ticker — earlier snapshots accumulate without per-day dedup. |

### P2.1.4 `alpaca_reconciliation` — divergence ledger

| Field | Notes |
|---|---|
| **Writers** | `scripts/alpaca_reconcile.upsert_divergences` (228-291). Opens rows for new (ticker, issue_type) keys; auto-resolves cleared rows. Scoped by `(strategy, is_live)`. |
| **Schedule** | Daily 13:30 PT (same plist). Exit code is non-zero (line 387) if any `missing_in_alpaca` unresolved. |
| **Consumers** | **`gate_active_divergences` in `live_launch_check.py:83-100` BLOCKS launch** on any warn/critical unresolved row for the matching `(strategy, is_live)`. Admin pages, daily summary. |
| **Freshness** | Daily. Currently **8 unresolved critical, 1 unresolved warn, 2 unresolved info**. |
| **PIT** | N/A. |
| **Known issues** | (a) **3 QM missing_in_alpaca rows (entered 2026-04-27 → 2026-05-05) are unresolved 11-19 days** and will block QM-live launch. 5 tenb51 missing_in_alpaca rows are 63-72 days old. (b) `severity='critical'` is auto-promoted at age >=7 days (alpaca_reconcile.py:147) — no operator opt-out. (c) The only resolution path is auto (divergence clears on its own) or manual `detail` annotation (visible on the BW info rows). No human-acknowledgment workflow. |

### P2.1.5 `bad_trades` — quarantine for suspect EDGAR rows

| Field | Notes |
|---|---|
| **Writers** | `pipelines/insider_study/quarantine_bad_trades.py` (and similar EDGAR-import scripts). |
| **Schedule** | One-shot per backfill; last activity 2026-03-18 per sample data. |
| **Consumers** | The `trades` table excludes `bad_trades.trade_id` from queries; strategy scans inherit this. |
| **Freshness** | One-shot. 1,094 rows currently. |
| **PIT** | The quarantine is itself a static decision; bad rows would have been bad at filing-time too. |
| **Known issues** | No automatic alerting if new bad trades land. |

### P2.1.6 `dataset_manifest` — unused

| Field | Notes |
|---|---|
| **Writers / Consumers** | Schema exists; 0 rows; no active code path writes or reads it. |
| **Known issues** | Either wire to dataset-freshness probe or delete. |

### P2.1.7 `deploys` — broken writer

| Field | Notes |
|---|---|
| **Writers** | `~/.local/bin/studio` CLI should write a row per deploy, but `deploys` table has 0 rows despite recent deploys. Writer is broken or not invoked. |
| **Consumers** | `studio deploy` pre-checks. |
| **Known issues** | Empty. Post-launch SHA tracking won't work until fixed. |

### P2.1.8 `portfolios` — strategy-level metadata

| Field | Notes |
|---|---|
| **Writers** | `cw_runner.ensure_portfolio_row` (327-344) — `INSERT OR IGNORE` keyed on `strategy_name`. |
| **Schedule** | Once per strategy lifetime. |
| **Consumers** | `strategy_portfolio.portfolio_id` foreign key (advisory). API views. |
| **Known issues** | (a) **No row for `quality_momentum_live`** will be created — both QM-paper and QM-live yaml set `strategy_name: quality_momentum`. Live rows will attach to `portfolios.id=75` whose `starting_capital=100000` (set when paper booted), NOT the live $10k. `get_theoretical_equity` (351-359) uses the yaml's `starting_capital`, ignoring the portfolios row — so sizing is correct, but any consumer reading `portfolios` directly will see misleading data. (b) 14 rows including 8 unused historical experiments from March 2026. |

## P2.2 End-to-end order flow narrative

### P2.2.1 Decision → strategy_portfolio → order_audit → Alpaca

Hot path traced from `cw_runner.execute_entries` (`cw_runner.py:1060`):

1. **Loop trigger.** cw_runner main loop hits :00 or :30 during market hours → `run_daily()` (2274).
2. **scan_signals** — PIT engine path by default (`PIT_ENGINE_LEGACY` unset), legacy SQL fallback if set. Writes `trade_decision_audit` rows for every dedup/PIT/conviction decision.
3. **check_exits runs first** (1876) — frees slots before considering new entries.
4. **execute_entries** per candidate (sorted by conviction DESC):
   - **(a) Kill switch.** `_trading_halted` checks `TRADING_HALTED` (global) and `TRADING_HALTED_{STRATEGY}` env vars. First halt observation alerts; subsequent suppressed via `_LAST_HALT_LOG` (1042-1097).
   - **(b) Slots remaining.** `max_concurrent − count(open ∧ paper|live)` (1101-1110).
   - **(c) Circuit breaker.** `equity = starting_capital + sum(pnl_dollar for closed)`. If `1 - equity/starting >= circuit_breaker_dd_pct` (QM-LIVE: 10%, default: 10%), halt this cycle, send alert (1112-1121). **Note: realized P&L only — open drawdowns won't trip the breaker.**
   - **(d) Per-candidate capacity.** Soft cap / hard cap / `at_capacity` rule (skip / replace_weakest / replace_oldest), 1163-1318.
   - **(e) Price fetch.** `_get_latest_price` via shared read-only data API (`ALPACA_DATA_API_KEY`), 1018-1037.
   - **(f) Sizing.** `qty = floor(equity * size_pct / price)`, 1344.
   - **(g) Hard guardrails.** `framework.risk.guardrails.validate_entry_order` — min/max equity, qty, dollar_amount, price, plus daily-buys/sells via order_audit count. Reject → alert.critical + audit + skip (1355-1372).
   - **(h) order_id.** V2 deterministic (`Order.from_intent` → `sha256(decision_id|retry)[:24]`) if `OMS_V2` env set AND candidate has decision_id; else random uuid (1388-1414).
   - **(i) INSERT strategy_portfolio** with `status='open'`, `execution_source = 'live' if live_money else 'paper'`, `is_live=live_money`, `entry_price=current_price`, `planned_exit_date = MarketCalendar.add_trading_days(today, target_hold)`. **Commits BEFORE any Alpaca call** (1449-1489).
   - **(j) order_audit decision row.** `fill_status='pending'` via V1 helper or V2 `write_order` (1494-1520).
   - **(k) alpaca_already_holds check.** If `alpaca.get_position(ticker) is not None` → SKIP submission, mark order_audit `fill_status='skipped'` rejection_reason='alpaca_already_holds'. **strategy_portfolio row stays open** (1525-1550).
   - **(l) alpaca.submit_order** via `PaperBackend.submit_order` (paper.py:91-151). Body includes `client_order_id=order_id`. On HTTP exception returns `OrderResult(status='rejected', error=...)`. On 'new'/'accepted' returns `status='pending'`.
   - **(m) wait_for_fill** with 300s timeout (1569) — polls `/orders/{id}` every 0.5s. Bumped from 90s on 2026-05-13 after a HUBS fill at +150s false-alarmed.
   - **(n) Update order_audit with outcome.** filled / rejected / timeout / exception. Timeout and exception trigger `alert.critical` ("manual verify needed"). **strategy_portfolio row UNCHANGED on timeout** — strategy holds, Alpaca state unknown.

### P2.2.2 Async fill confirmation — the safety nets

**WebSocket listener** (`framework/oms/alpaca_stream_listener.py`, plist `com.openclaw.alpaca-stream-listener`):
- One asyncio task per Alpaca account. Hardcoded `ACCOUNTS` list (lines 70-75). **`quality_momentum_live` is NOT in the list** — comment at line 74: "When live_money launches 2026-06-04 add quality_momentum_live here."
- On `fill`: UPDATE order_audit (`_update_order_audit_fill`, 82-101); UPDATE strategy_portfolio entry_price + shares to actual fill (`_update_strategy_portfolio_fill`, 104-131). This OVERWRITES the decision-time `entry_price` set by cw_runner — conflicting with the "canonical clean price" design comment at cw_runner.py:1374.
- On `rejected/canceled/expired`: UPDATE order_audit terminal; for buys, CLOSE the speculative strategy_portfolio row with `exit_reason='alpaca_rejected'`, `pnl_pct=0` (`_close_portfolio_on_reject`, 146-169).
- Reconnect: exponential backoff to 300s max (line 276). Healthy logs show ~daily reconnects at 01:06 + 05:01 UTC.
- Heartbeat: `strategies/cw_strategies/data/alpaca_stream_heartbeat.json` every 60s. Latest write 2026-05-17 04:33 UTC — listener active.

**Intraday resolver** (`scripts/alpaca_intraday_resolver.py`, plist `com.openclaw.alpaca-intraday-resolver`):
- `StartInterval=300`. Acts only during market hours (`is_market_open`, 67-74); off-hours: "no-op (use --force to override)".
- Sweeps order_audit non-terminal rows with `alpaca_order_id`, last 24h. Queries `/orders/{id}` directly. Same UPDATE pattern as stream listener. ACCOUNTS list also hardcoded (54-58) — same gap for live.

### P2.2.3 Exit path

`cw_runner.check_exits` (1876) iterates open `(paper, live)` rows:
- `fixed_hold`: `planned_exit_date` (precomputed trading days) OR `stop_loss_pct`.
- `trailing_stop`: `peak_return − pnl_pct >= stop_pct`, peak persisted to `{strategy}_state.json`.
- `sma50_break`: `current_price < 50d SMA` from Alpaca daily bars.

`check_scheduled_exits` (1767) runs at 15:45 ET and at market open for overdue rows.

Mirror of entry: UPDATE strategy_portfolio first (canonical), write order_audit decision row, submit sell with 90s wait. On exception, strategy_portfolio is already closed — drift exists until reconcile detects `missing_in_alpaca` (one-way detection: DB-open vs broker-empty; the inverse drift "DB-closed but Alpaca still holds" is harder to surface).

### P2.2.4 Daily reconciliation

`scripts/alpaca_reconcile.py` runs 13:30 PT weekdays. Per `(strategy, is_live)`:
1. Pull Alpaca positions via `backend.list_positions()`.
2. Snapshot into `alpaca_position_snapshots`.
3. Pull DB open rows for matching `(strategy, is_live)`.
4. Compute 4 divergence types.
5. Upsert `alpaca_reconciliation` — open new keys, resolve cleared.
6. Exit non-zero if any `missing_in_alpaca` unresolved.

Severity (alpaca_reconcile.py:128-225):
- `missing_in_alpaca`: warn <7d, critical >=7d
- `orphan_in_alpaca`: info if market_value <$500, warn otherwise
- `qty_mismatch`: always warn
- `price_mismatch`: info <10%, warn >=10%

## P2.3 Live-money differences (current state)

The `live_money: true` yaml flag is the ONLY behavioral switch separating live from paper today.

| What changes when `live_money: true` | Where | Detail |
|---|---|---|
| Alpaca base URL | `cw_runner.get_alpaca:180-181` | `api.alpaca.markets/v2` vs `paper-api.alpaca.markets/v2` |
| Alpaca credentials | `cw_runner.get_alpaca:171-172` | `ALPACA_API_KEY_{prefix}_LIVE` instead of `ALPACA_API_KEY_{prefix}` |
| `strategy_portfolio.execution_source` | cw_runner.py:1450 | `'live'` vs `'paper'` |
| `strategy_portfolio.is_live` | cw_runner.py:1449 | `true` vs `false` |
| `order_audit.is_live` | `_record_order_decision` & V2 `write_order` | passed through |
| Heartbeat file name | cw_runner.py:2186-2192 | `quality_momentum_live_heartbeat.json` vs `quality_momentum_heartbeat.json` |
| Daemon log line | cw_runner.py:183-187 | "LIVE TRADING ENABLED — orders route to https://api.alpaca.markets/v2" |

**What does NOT change automatically:**
- **Decision logic.** `_PIT_STRATEGY_CLASSES['quality_momentum_live'] → QualityMomentumStrategy` (cw_runner.py:394). Same filter, same conviction calc, same exits.
- **Decision-row writes to trade_decision_audit.** Same path; no live-only sampling.
- **Capacity, guardrails defaults, circuit breaker.** All yaml-overridable. Live yaml tightens: `max_concurrent: 5` (vs 10), `max_dollar_amount: 5000` (vs 50000 default), `max_daily_buys: 5` (vs 10), `circuit_breaker_dd_pct: 0.10` (vs 0.15 paper).
- **No approval queue.** Once plist loads + `_LIVE` creds present, the runner trades autonomously.
- **No separate alerting prefix beyond `[QM-LIVE]`** — same `logs/alerts.ndjson`, same SMS gateway.
- **Stream listener / intraday resolver still hardcode paper-only `ACCOUNTS`.** All three accounts have `"live": False`. **A live order's fill will NOT be picked up by either safety net until those Python lists are amended manually.**

## P2.4 Safety mechanism details

### P2.4.1 Kill switch
- Checked in `_trading_halted` (1045-1057). Called inside `execute_entries` (1076), NOT exit paths. Existing positions still get exit-checked.
- Process must restart for env var changes (no live config reload).
- Does NOT prevent the stream listener / intraday resolver from updating DB state for inflight orders. Intentional but worth noting.

### P2.4.2 Circuit breaker
- Realised P&L only. Open drawdowns invisible until positions close.

### P2.4.3 Hard guardrails
- `_count_orders_today` queries `order_audit` for today's (strategy, side) count. With 6 total rows historically, the rate limit defaulted to 0 = no rate-limit-in-practice. **For live, this is a latent bug — a runaway loop could submit unlimited orders day-1.**

### P2.4.4 Decoupling
- strategy_portfolio is canonical; alpaca is side-channel.
- `entry_price` design intent ≠ runtime: cw_runner.py:1374 says "data-API quote at decision time"; stream listener overwrites with fill price.
- `alpaca_already_holds` skip path inserts a DB row but skips Alpaca submission → guaranteed `qty_mismatch` on next reconcile.

### P2.4.5 Heartbeats
- Per-mode (paper / live) filenames so paper + live don't overwrite each other.
- `gate_heartbeat_fresh` in preflight: 24h tolerance generally; missing live heartbeat is warning (not blocker) in live-mode preflight BEFORE the live runner is loaded.

### P2.4.6 Pre-launch validator (10 gates)
1. `is_live_migration` — column present on all 4 tables (blocker)
2. `paper_sharpe_30d` — 30d closed paper Sharpe ≥ 0.5 (blocker; warning if <5 trades)
3. `no_active_divergences` — zero unresolved warn/critical for `(strategy, mode)` (blocker)
4. `no_unresolved_orders` — zero order_audit rows >1h non-terminal (blocker)
5. `freshness_contracts` — signal-freshness contracts pass (blocker)
6. `daily_summary_recent` — last successful daily-summary send <36h ago (blocker)
7. `heartbeat_fresh` — <24h old (blocker, warning if file missing in live-mode preflight)
8. `live_creds` — `verify_live_creds.py` exit 0 (blocker, live mode only)
9. `kill_switch_off` — no `TRADING_HALTED*` env var set (blocker)
10. `recent_deploy` — last commit ≥24h ago (warning, not blocker)

## P2.5 Live state on Studio (2026-05-16 21:35 ET)

### P2.5.1 Plists / launchd

```
PID    Status   Label
68323  -15      com.openclaw.quality-momentum     (KeepAlive=true, RunAtLoad=true)
68326  -15      com.openclaw.reversal-dip         (KeepAlive=true, RunAtLoad=true)
68330  -15      com.openclaw.tenb51-surprise      (KeepAlive=true, RunAtLoad=true)
61619  0        com.openclaw.alpaca-stream-listener (KeepAlive=true, RunAtLoad=true,
                                                     ThrottleInterval=30)
-      0        com.openclaw.alpaca-reconcile     (StartCalendarInterval Mon-Fri 13:30 PT;
                                                   KeepAlive=false, RunAtLoad=false)
-      0        com.openclaw.alpaca-intraday-resolver (StartInterval=300;
                                                       KeepAlive=false, RunAtLoad=false)
```

**No `com.openclaw.quality-momentum-live` plist exists at `~/Library/LaunchAgents/`.** Template at `scripts/launchd/com.openclaw.quality-momentum-live.plist`, never copied.

### P2.5.2 Row counts

```
strategy_portfolio:    2,228 rows total, ALL is_live=false
  quality_momentum:    567 closed (simulated), 4 open (simulated)
  reversal_dip:        705 closed (simulated), 0 open
  tenb51_surprise:     192 closed (simulated), 5 open (paper), 5 open (simulated)
  + 6 other historical strategies

order_audit:           6 rows total
  quality_momentum:    1 (skipped, 2026-05-12)
  tenb51_surprise:     5 (filled, 2026-05-13 → 2026-05-15)

alpaca_position_snapshots:   31 rows
  quality_momentum:    10, last 2026-05-15
  tenb51_surprise:     21, last 2026-05-15

alpaca_reconciliation: 11 rows total
  unresolved critical: 8 (3 QM missing_in_alpaca 7-15d old;
                          5 tenb51 missing_in_alpaca 63-72d old)
  unresolved warn:     1 (tenb51 PANW orphan $38,218 market value)
  unresolved info:     2 (QM BW qty_mismatch + price_mismatch, acknowledged)
  resolved:            1

bad_trades:            1,094 (last quarantine 2026-03-18)
deploys:               0 (writer broken/unwired)
dataset_manifest:      0 (table unused)
portfolios:            14 rows (no quality_momentum_live row)
```

### P2.5.3 Live credentials

```
~/.config/form4/secrets.env — DOES NOT EXIST on Studio.
~/trading-framework/.env — contains paper creds only; no _LIVE variants present.
OMS_V2 env: NOT set (V1 path active).
TRADING_HALTED env: NOT set.
```

### P2.5.4 Recent reconciliation

2026-05-15 13:30 PT (most recent):
- `quality_momentum (paper)`: db_open=4, alpaca=1, 5 divergences (3 missing_in_alpaca, 1 qty, 1 price)
- `quality_momentum (live)`: SKIPPED — `live_creds_not_configured`
- `reversal_dip (paper)`: 0 db, 0 alpaca, 0 divergences
- `tenb51_surprise (paper)`: db=10, alpaca=6, 6 divergences (5 missing, 1 orphan)

Daily output: "Reconcile completed with 8 unresolved missing_in_alpaca rows." **No active resolution workflow exists — they remain open indefinitely.**

### P2.5.5 Stream listener health

- Heartbeat fresh.
- Recent fills captured cleanly (UPST, TFSL, ACM on 2026-05-14/15).
- Note racing behavior: UPST fill showed `DB: 0 order_audit row + 1 strategy_portfolio row updated` (cw_runner's `wait_for_fill` already wrote the order_audit row before the WS event arrived). TFSL showed `1 + 1` (WS won the race). Both fine; illustrates non-determinism in which writer "first" claims the order_audit row.
- Reconnect handling healthy.

## P2.6 Live-money readiness gaps for `quality_momentum_live`

### P2.6.1 Documented in LIVE_LAUNCH.md / LIVE_ACCOUNT_SETUP.md

1. **Live Alpaca account not opened / funded.** `~/.config/form4/secrets.env` doesn't exist. `ALPACA_API_KEY_QUALITY_MOMENTUM_LIVE` is missing. ACH funding takes 1-3 business days.
2. **Live plist not installed.** Template needs `cp` to `~/Library/LaunchAgents/` then `launchctl load -w`.
3. **SMS gateway not verified.** `framework.alerts.sms test` should fire end-to-end before launch.
4. **Two paper days with zero unresolved divergences required.** **Currently failing — 3 unresolved critical QM `missing_in_alpaca` rows from 2026-04-27 → 2026-05-05.** `gate_active_divergences` will BLOCK launch.

### P2.6.2 NOT documented — undocumented gaps

5. **WebSocket listener does NOT know about the live account.** `alpaca_stream_listener.py:70-75` and `alpaca_intraday_resolver.py:54-58` both hardcode paper-only ACCOUNTS lists. The QM-live runner will submit orders that BOTH safety nets silently ignore. Fills will rely entirely on cw_runner's 300s `wait_for_fill`. The needed edit is documented inline ("add quality_momentum_live here" with `"live": True`) but is a manual code change.
6. **`LiveBackend` safety guard is bypassed.** `framework/execution/live.py` exists with explicit `enable_live=True` requirement. `cw_runner.get_alpaca` uses `PaperBackend` directly with `base_url=LIVE_API_BASE`. No functional consequence (PaperBackend is just a URL container) but the codebase advertises a safety guard that's not used in the hot path.
7. **stream listener entry_price overwrite conflicts with cw_runner.py:1374 "canonical clean price" design.** For paper this is harmless (paper fills usually at requested price). For live, slippage exists. **Needs a Derek decision before launch:** which is the canonical entry_price for customer P&L — strategy decision quote or actual fill?
8. **No smoke-test mechanism for "live mode at $0".** LIVE_LAUNCH.md jumps from preflight to plist load to first real entry. Recommendation: a `--smoke-test` mode that submits 1 share on a high-volume ticker against the live account, verifies end-to-end pipeline, closes the position. Today this requires manual orchestration.
9. **Daily reconciler runs once at 13:30 PT.** Between 9:30 AM-1:30 PM, reconciliation drift accumulates with no daytime check. Stream listener is the live-time substitute but drops fills during reconnects (daily reconnect at 01:06 + 05:01 UTC visible in logs).
10. **`portfolios` table won't get a live row** (both yamls set same `strategy_name`). Sizing is unaffected (`get_theoretical_equity` uses yaml `starting_capital`), but any consumer reading `portfolios` directly is misled.
11. **`deploys` table empty.** Studio CLI's deploy-log writer broken/unwired. Post-launch SHA tracking won't work.
12. **`alpaca_already_holds` skip path inserts DB row but skips submission** (cw_runner.py:1448 inserts before 1525 check). Produces a guaranteed `qty_mismatch` on next reconcile.
13. **No global emergency-close switch.** Halting entries doesn't close existing positions. Max wait is target_hold (42 trading days for QM). No `studio close-all-positions <strategy>` exists today.
14. **The `max_daily_buys` guardrail check depends on order_audit being populated.** Today it's near-empty for QM (1 skipped row). `_count_orders_today` returns 0 → effectively unlimited. **Should be backfilled from strategy_portfolio OR the guardrail rewritten to query strategy_portfolio.entry_date.** A runaway loop on day 1 of live could submit unlimited orders before order_audit accumulates a meaningful count.
15. **Reconciler exit-code-non-zero on unresolved missing_in_alpaca** is informational only — no plist consumes the exit code to alert. The launchd-noticed semantic is unused.
16. **No verification that QM-live runner won't conflict with QM-paper runner.** Both call `ensure_portfolio_row(strategy_name='quality_momentum')`, both share `_state.json` for `peak_returns` map (cw_runner.py:1707-1731). The peak_returns is keyed by `pos_id` (strategy_portfolio.id), so the dict can safely hold both paper and live ids. But the file is shared — if both processes try to write `{strategy}_state.json` simultaneously there's a race (lines 1721-1731 use `json.dumps` + `write_text` with no lock). Risk: one process's write clobbers the other's. Low-probability (writes happen on minute boundaries, file is small) but worth a `fcntl` lock or per-mode state file.

### P2.6.3 Recommended additions to LIVE_LAUNCH.md

- Add `quality_momentum_live` entry to `ACCOUNTS` lists in stream listener AND intraday resolver. Deploy, verify subscription log line appears.
- Resolve / manually acknowledge the 3 QM `missing_in_alpaca` rows.
- Decide entry_price canonical source for live; reconcile cw_runner.py:1374 comment vs stream listener overwrite.
- Run a 1-share smoke test against live API; verify order_audit, stream listener fill capture, strategy_portfolio actual_fill_price update, reconcile picks up the position.
- Backfill order_audit (or change `_count_orders_today` source to strategy_portfolio.entry_date) so rate-limit guardrail is meaningful day 1.
- Wire `deploys` writer so post-launch SHA tracking works.
- Audit state-file sharing between paper + live runners; add lock or per-mode file.

## P2.7 Append-only audit completeness

The expected trail per trade:
```
trade_decision_audit (per-stage)
    ├─→ strategy_portfolio (per accepted entry, status=open)
    ├─→ order_audit (per submission, fill_status evolves)
    ├─→ alpaca_position_snapshots (daily, per position per day)
    └─→ alpaca_reconciliation (per divergence)
```

Today: trade_decision_audit ↔ strategy_portfolio link works. **strategy_portfolio ↔ order_audit link is broken for historical paper rows** (6 of 2,228). Forward-going, V1 `_record_order_decision` should populate this. Stream listener / intraday resolver hardcoded ACCOUNTS list is the long-tail risk for live operation.

## P2.8 References

- `strategies/cw_strategies/cw_runner.py` — lines 60-200 (setup, get_alpaca), 247-360 (audit helpers), 820-1011 (scan_signals legacy), 1060-1700 (execute_entries), 1767-2159 (exits), 2274-2540 (run_daily, run_daemon, main)
- `framework/oms/runner.py` — lines 49-381 (evaluate_candidates_v2)
- `framework/oms/order_manager.py` — lines 39-282 (state machine, deterministic IDs)
- `framework/oms/audit.py` — lines 27-146 (write_decision, write_order)
- `framework/oms/alpaca_stream_listener.py` — lines 66-323 (full module)
- `framework/execution/paper.py` — lines 20-281 (full module)
- `framework/execution/live.py` — lines 22-99 (LiveBackend; UNUSED today)
- `framework/risk/guardrails.py` — lines 35-131 (full module)
- `scripts/alpaca_reconcile.py` — lines 64-391 (full module)
- `scripts/alpaca_intraday_resolver.py` — lines 54-252 (full module)
- `scripts/preflight/live_launch_check.py` — lines 45-365 (10 gates)
- `scripts/verify_live_creds.py` — lines 32-112 (full module)
- `strategies/cw_strategies/configs/quality_momentum_live.yaml` — full
- `docs/LIVE_LAUNCH.md` and `docs/LIVE_ACCOUNT_SETUP.md`
- `scripts/launchd/com.openclaw.quality-momentum-live.plist` — full
- Studio live state via `launchctl list`, recent `/Users/derekg/trading-framework/logs/*.log` files, and PG queries on `form4` 2026-05-16 21:35 ET

---

# Part 3 — Cron Inventory, Freshness Contracts, and Writer-Registry Gap Analysis

*Scope: the operational view. Which plist runs which Python script, which columns each script writes, and what's broken or unmonitored. Generated 2026-05-16 against Mac Studio (`derekg@100.78.9.66`).*

Audit driven by the 4 silent failures uncovered on 2026-05-16:

1. `trades.career_grade` had **no recurring writer** (only `backfill_live.main()` wrote it; no plist invokes that function) → QM silenced from 2026-05-12 onward
2. `trades.is_rare_reversal` had a **mislabeled writer** (contract claimed `compute_cw_indicators.consecutive` but that function does not write the column) → RD silenced ~8 weeks since 2026-03-25
3. `com.openclaw.intraday-backfill.plist` is loaded with status code `1`, `RunAtLoad=true` with no `StartInterval` → runs once on plist load and dies, never reruns
4. `scripts/candidate_count_probe.py` crashes with `NameError: name 'consecutive_zeros' is not defined` at line 268 every market day at 22:00 → the probe that should have caught #1 and #2 is itself broken

The whole point of this audit is to make these failure modes structurally impossible.

---

## 3.1 — Master Plist Inventory (Studio)

68 `com.openclaw.*` plists exist in `~/Library/LaunchAgents/`: 23 are trading-framework, 35 are prediction-markets, 10 are mixed-purpose. The full prediction-markets set is listed by name at the end of section 3.1c.

### 3.1a. Trading-framework plists (in scope for writer-registry)

| Label | Script | Schedule | Loaded? | Last successful run | Writes |
|---|---|---|---|---|---|
| `com.openclaw.insider-fetch` | `strategies/insider_catalog/fetch_latest.py --days 2` | `StartInterval=300` (every 5 min) | Yes (`-`) | 2026-05-16 21:31:49 PT (`logs/insider-fetch.log`); `sync_meta.last_fetch_at=2026-05-17 04:31:49 UTC` | `trades.*` (raw ingest), then triggers `compute_cw_indicators.py --since 7d` + `backfill_pit_grades.py --since 7d` as subprocesses |
| `com.openclaw.refresh-features` | `strategies/insider_catalog/refresh_features_daily.sh` | `StartCalendarInterval` Mon–Fri 06:00 PT | Yes (`-`) | 2026-05-15 06:15:15 PT (`logs/refresh-features.log`); Saturday — last run still Friday | Steps 0→6 (see dependency graph in 3.4) |
| `com.openclaw.daily-prices` | `pipelines/insider_study/update_daily_prices.py` | `StartCalendarInterval` Mon–Fri 17:30 PT | Yes (`-`) | 2026-05-15 17:42:40 PT | `prices.daily_prices.{date,open,high,low,close,volume}`, `signal_freshness(prices.daily_prices.date)` |
| `com.openclaw.backfill-returns` | `pipelines/insider_study/backfill_returns.py --max-download 500` | `StartCalendarInterval` daily 05:00 PT | Yes (`-`) | 2026-05-16 05:08:00 PT (`logs/backfill_returns.log`) | `trade_returns.{return_*,abnormal_*,spy_return_*,computed_at}` via subprocess `compute_returns.py`; ALSO `trades.signal_grade` (`backfill_returns.py:341`) |
| `com.openclaw.compute-signals` | `pipelines/insider_study/compute_signals.py` | `StartCalendarInterval` Mon–Fri 17:45 PT | Yes (`-`) | 2026-05-15 17:54:33 PT (`logs/compute-signals.log`) | `trade_signals.{trade_id,signal_type,signal_class,*}` |
| `com.openclaw.quality-momentum` | `strategies/cw_strategies/cw_runner.py --config configs/quality_momentum.yaml` | `KeepAlive=true`, `ThrottleInterval=30`, runner self-paces 5-min cycles | Yes (PID 68323, status `-15`) | Heartbeat `quality_momentum_heartbeat.json` 2026-05-17T00:10:10 ET (sleeping/Weekend) | `order_audit`, `strategy_portfolio`, `trade_decision_audit` |
| `com.openclaw.reversal-dip` | same script, `reversal_dip.yaml` | same | Yes (PID 68326) | Heartbeat 2026-05-17T00:10:10 ET | same |
| `com.openclaw.tenb51-surprise` | same script, `tenb51_surprise.yaml` | same | Yes (PID 68330) | Heartbeat 2026-05-17T00:10:10 ET | same |
| `com.openclaw.alpaca-stream-listener` | `python3 -m framework.oms.alpaca_stream_listener` | `RunAtLoad=true`, `KeepAlive=true` (continuous WebSocket) | Yes (PID 61619) | 60-sec heartbeat `alpaca_stream_heartbeat.json` 2026-05-17T04:33:04 UTC | `order_audit.{fill_status,fill_price,fill_qty,filled_at,…}`, `strategy_portfolio.{actual_fill_price,shares,…}` |
| `com.openclaw.alpaca-intraday-resolver` | `scripts/alpaca_intraday_resolver.py` | `StartInterval=300` (every 5 min) | Yes (`-`) | 2026-05-16 21:27:00 PT (`logs/alpaca-intraday-resolver.log`) | same as stream-listener (safety net) |
| `com.openclaw.alpaca-reconcile` | `scripts/alpaca_reconcile.py` | `StartCalendarInterval` Mon–Fri 13:30 PT | Yes (`-`) | 2026-05-15 13:30:30 PT | `alpaca_position_snapshots`, `alpaca_reconciliation` |
| `com.openclaw.strategy-simulator` | `python3 -m pipelines.insider_study.simulate_strategy_portfolio --all --extend` | `StartCalendarInterval` daily 07:00 PT | Yes (`-`) | 2026-05-16 07:01:00 PT | `strategy_portfolio` simulated rows |
| `com.openclaw.strategy-intraday` | `python3 -m pipelines.insider_study.simulate_portfolio_intraday --all` | `StartInterval=600` (every 10 min, self-gates on market hours) | Yes (`-`) | 2026-05-16 21:27:00 PT | `strategy_portfolio` intraday rebalance |
| `com.openclaw.strategy-health` | `scripts/strategy_health_check.py` | `StartCalendarInterval` daily 17:00 PT (TZ defaults to local PT) | Yes (`-`) | 2026-05-16 17:00:00 PT | Read-only — checks `strategy_portfolio.MAX(entry_date)` + heartbeat staleness |
| `com.openclaw.thesis-monitor` | `python3 -m pipelines.thesis_monitor.monitor` | `StartCalendarInterval` Mon–Fri 13:30 PT | Yes (`-`) | 2026-05-15 13:30:00 PT | Read-only |
| `com.openclaw.freshness-probe` | `scripts/freshness_probe.py` | `StartInterval=1800` (every 30 min) | Yes (`-`) | 2026-05-16 21:11:04 PT | Reads `signal_freshness`; writes `logs/freshness_state.json` + `logs/alerts.ndjson` |
| `com.openclaw.heartbeat-probe` | `scripts/heartbeat_probe.py` | `StartInterval=900` (every 15 min) | Yes (`-`) | 2026-05-16 21:25:00 PT | Reads heartbeat JSON; writes `logs/heartbeat_probe_state.json` |
| `com.openclaw.candidate-count-probe` | `scripts/candidate_count_probe.py` | `StartCalendarInterval` Mon–Fri 22:00 PT | Yes (status `1` — error exit every run) | **Crashes every run** with `NameError: consecutive_zeros`; last log 2026-05-15 22:00 PT | Should write `logs/candidate_count_state.json` and alerts — but crashes during print phase. Alerts ARE written; only stdout printing fails. |
| `com.openclaw.post-deploy-audit` | `scripts/post_deploy_audit.py` | `StartCalendarInterval` one-shot dates 2026-05-12 through 2026-05-15 14:00 PT (exhausted) | Yes (`-`) | 2026-05-15 14:00:06 PT | HTML/JSON report `logs/post-deploy-audit-*.{html,json}`; emails via Resend |
| `com.openclaw.pit-shadow` | `scripts/pit_shadow_run.py` | `StartCalendarInterval` one-shot 2026-05-13 + 2026-05-14 18:00 PT (exhausted) | Yes (`1`) | 2026-05-14 18:00 PT | Read-only PIT shadow comparison |
| `com.openclaw.intraday-backfill` | `pipelines/insider_study/backfill_intraday_events.py --cw-only` | `RunAtLoad=true`, **no `StartInterval`** | Yes (`1` — last exit was error) | 2026-04-18 18:05 PT (log untouched since) | Was supposed to write `trades.filed_at`; **effectively orphaned** — only runs on plist reload (which only happens during deploys) |
| `com.openclaw.daily-summary` | `scripts/daily_summary.py` | `StartCalendarInterval` Mon–Fri 14:30 PT | Yes (`-`) | 2026-05-15 14:30:00 PT | Sends daily summary email; no DB writes |
| `com.openclaw.position-rules-test` | `pipelines/insider_study/test_position_rules.py` | `RunAtLoad=true`, no `StartInterval` (legacy test runner) | Yes (`-`) | 2026-04-18 18:05 PT (orphaned same way as intraday-backfill) | Read-only |

### 3.1b. Trading-framework auxiliary plists (frontend/email, not strategy-critical)

| Label | Script | Schedule | Loaded? | Purpose |
|---|---|---|---|---|
| `com.openclaw.breaking-signal` | `pipelines/run_breaking_signal.sh` | `StartInterval=1800` | Yes (`-`) | Marketing content generator |
| `com.openclaw.ceowatcher-reader` | `pipelines/ceowatcher_reader.py` | daily 08:30 PT | Yes (`-`) | RSS scraper → `trade_context` for `/feed` |
| `com.openclaw.daily-content` | `pipelines/run_daily_content.sh` | daily 17:00 PT | Yes (`-`) | `pipelines/data/content/*.json` daily reports |
| `com.openclaw.trial-emails` | `pipelines/trial_emails.py` | `StartInterval=21600` (4x/day) | Yes (`-`) | Trial-conversion emails |
| `com.openclaw.insideredge-notifications` (label `com.openclaw.form4-notifications`) | `pipelines/notification_scanner.py` | `StartInterval=900` (every 15 min) | Yes (`-`) | User notification scanner |
| `com.openclaw.form4-uptime` | `scripts/uptime_monitor.sh` | `StartInterval=60` | Yes (`-`) | HTTP probe of `form4.app` |
| `com.openclaw.form4-seed-positions` | `scripts/seed_rebalanced_positions.py` | one-shot 2025-04-13 06:25 (exhausted) | Yes (`-`) | One-off |
| `com.openclaw.catchup-cleanup` | `scripts/cleanup_catchup.sh` | daily 09:45 | **Not loaded** | Orphan plist — not in `launchctl list` |
| `com.openclaw.tailorly-tunnel` | `cloudflared tunnel run tailorly` | `RunAtLoad=true`, `KeepAlive=true` | Yes (PID 638) | Cloudflare tunnel for Tailorly (different project) |

### 3.1c. Prediction-markets plists (out of scope for trading-framework writer registry)

35 `com.openclaw.pm-*` / `com.openclaw.prediction-markets.*` plists live in the same `~/Library/LaunchAgents/` directory. They share the Studio host but write to `prediction_markets` PG database. Listed for completeness:

`pm-auditor, pm-backup, pm-bet-handler, pm-bronze-flush-trades, pm-cotrade-edges, pm-dashboard, pm-db-maintenance, pm-kalshi-fetcher, pm-llm-enricher, pm-market-daily, pm-polymarket-fetcher, pm-positions, pm-scd2-classifications, pm-scd2-edge-stats, pm-strategy-cpi-tail-fade, pm-strategy-cross-platform-odds, pm-strategy-econ-nowcast, pm-strategy-favourite-longshot-bias, pm-strategy-geo-risk, pm-strategy-mention-predictor, pm-strategy-nba-favorites, pm-strategy-nba-maker-98c, pm-strategy-overreaction-detector, pm-strategy-smart-money, pm-strategy-sports-scalp, pm-strategy-weather-forecast, pm-wallet-current, pm-wallet-daily, pm-wallet-edge, pm-wallet-perf, pm-web, pm-whale-tracker, pm-api, prediction-markets.production, prediction-markets.staging, prediction-markets.watchdog`. The writer registry pattern proposed in section 3.5 should be parametrizable so prediction-markets can adopt it independently.

---

## 3.2 — Column → Writer Map (the seed of the writer registry)

The canonical map: every column the live strategies depend on, the script that should keep it fresh, the cron schedule, and current `signal_freshness` state.

### 3.2a. Contracted columns (`config/freshness_contracts.yaml`)

15 entries, verified by `framework/contracts/freshness.py` registry load:

| Column | SLA `max_h` | Required for | Declared writer (contract) | Plist that runs it | Last `signal_freshness` row | Probe verdict |
|---|---|---|---|---|---|---|
| `trades.dip_3mo` | 26 | `reversal_dip` | `compute_cw_indicators.py` | `refresh-features` 06:00 PT + `insider-fetch` every 5 min (subprocess) | 2026-05-15 19:04:14 PT | stale (26.1h, weekend quiet window) |
| `trades.dip_1mo` | 26 | `reversal_dip` | `compute_cw_indicators.py` | same | 2026-05-15 19:04:14 PT | stale |
| `trades.above_sma50` | 26 | `quality_momentum` | `compute_cw_indicators.py` | same | 2026-05-15 19:04:17 PT | stale |
| `trades.above_sma200` | 26 | `quality_momentum` | `compute_cw_indicators.py` | same | 2026-05-15 19:04:17 PT | stale |
| `trades.consecutive_sells_before` | 26 | `reversal_dip` | `compute_cw_indicators.py` | same | 2026-05-16 20:26:25 PT | ok (post-audit refresh) |
| `trades.is_recurring` | 48 | `*` | `compute_cw_indicators.py` | same | 2026-05-15 19:04:17 PT | ok |
| `trades.is_tax_sale` | 48 | `*` | `compute_cw_indicators.py` | same | 2026-05-15 19:04:17 PT | ok |
| `trades.cohen_routine` | 48 | `reversal_dip` | `compute_cohen_pit.py` | `refresh-features` only (step 6) | 2026-05-16 05:08:49 PT | ok |
| `trades.is_10b5_1` | 48 | `reversal_dip, tenb51_surprise` | `compute_cw_indicators.py` **(MISLABELED)** | actual writer: SEC ingest in `backfill_live.parse_form4_xml` (called by `fetch_latest.py`) | 2026-05-15 19:04:18 PT | ok (but timestamp meaningless — see 3.3.4) |
| `trades.is_rare_reversal` | 26 | `reversal_dip` | `compute_cw_indicators.py` **(MISLABELED, partially fixed 2026-05-16)** | actual writer: `compute_switch_rate.py` (refresh-features step 4c, post-fix). Contract file still names wrong script. | 2026-05-16 20:26:05 PT | ok |
| `trades.pit_cluster_size` | 26 | `*` | `compute_pit_clusters.py` | `refresh-features` step 5 | 2026-05-16 20:59:08 PT | ok |
| `trades.pit_grade` | 30 | `quality_momentum` | `backfill_pit_grades.py` | `refresh-features` step 4 + `insider-fetch` (subprocess) | 2026-05-16 20:27:40 PT | ok |
| `insider_ticker_scores.blended_score` | 30 | `*` | `strategies/insider_catalog/build_pit_scores.py` | `refresh-features` step 3 | 2026-05-15 06:13:37 PT | **STALE 39.0h** (weekend gap — quiet window suppresses page) |
| `trades.filing_date` | 48 | `*` | `strategies/insider_catalog/fetch_latest.py` | `insider-fetch` every 5 min (only writes when `inserted>0`) | 2026-05-15 19:04:12 PT | ok |
| `prices.daily_prices.date` | 72 | `*` | `pipelines/insider_study/update_daily_prices.py` | `daily-prices` Mon–Fri 17:30 PT + `refresh-features` step 0 | 2026-05-15 17:42:40 PT | ok |

### 3.2b. Untracked columns that strategies actually depend on (CRITICAL GAPS)

These columns appear in `strategies/cw_strategies/configs/*.yaml` filters or in `cw_runner.py` SQL queries, but have NO entry in `freshness_contracts.yaml`:

| Column | Read by | Actual writer | Plist | Risk |
|---|---|---|---|---|
| **`trades.career_grade`** | `quality_momentum.yaml:32` (`career_grade: ["A+","A"]`); `cw_runner.py:641-648` | `pipelines/insider_study/compute_career_grades.py` (created 2026-05-16, commit `2dcd9c9`) | `refresh-features` step 4b (added 2026-05-16) | **CRITICAL.** Before 2026-05-16, only `backfill_live.main()` populated it — no plist invokes that function. Result: QM produced 0 candidates from 2026-05-12 onward. The fix landed Saturday 2026-05-16 20:23 PT but **no freshness contract exists yet** — if `compute_career_grades.py` stops, no probe will catch it. |
| `trades.insider_switch_rate` | `cw_runner.py:521` (returned in `scan_signals` row tuple) | `compute_switch_rate.py` | `refresh-features` step 4c | **MEDIUM.** Writer exists and is wired post 2026-05-16. No contract → no probe alerting. |
| `trades.signal_grade` | `cw_runner.py:548, 618, 626, 830, 924, 1159, 1404` (used in `min_signal_grade` filter — not in current 3 configs) | `compute_signals.py` (`UPDATE trades SET signal_grade`) + `backfill_returns.py:341` (both write to the same column) | `compute-signals` 17:45 PT + `backfill-returns` 05:00 PT | **MEDIUM.** No current strategy filters on it, but two competing writers with no contract is a latent bug. |
| `trades.week52_proximity` | `pipelines/generate_breaking_signal.py:113`, `generate_daily_content.py:107,166`, `portfolio_simulator.py:228,303,386,1070`, `render_video_assets.py:275`, `render_ig_carousel.py:76` | `compute_week52_proximity.py` | **NO RECURRING PLIST** | **HIGH for content pipeline.** Genuine orphan writer with 6 active readers. Strategies don't depend on it, but content surfaces will silently drift. |
| `trades.pit_blended_score` | written alongside `pit_grade` but not contracted | `backfill_pit_grades.py` (same call site as `pit_grade`) | `refresh-features` + `insider-fetch` | Low — `pit_grade` covers freshness transitively. |

### 3.2c. Writers whose target columns are not in any contract (orphan writers from the other direction)

| Script | Plist | Writes columns | Why no contract |
|---|---|---|---|
| `compute_signals.py` | `compute-signals` 17:45 PT | `trade_signals.*`, `trades.signal_grade` | No live strategy filters on these — content/dormant readers only |
| `backfill_returns.py` | `backfill-returns` 05:00 PT | `trade_returns.*` (subprocess), `trades.signal_grade` | `trade_returns` covered transitively via `insider_ticker_scores.blended_score` |
| `compute_returns.py` (called from `backfill_returns.py`) | inherited | `trade_returns.{return_*,abnormal_*,spy_return_*,computed_at}` | Same |
| `compute_week52_proximity.py` | **NONE** | `trades.week52_proximity` | Genuine gap |
| `compute_market_sentiment.py` | **NONE** | `insider_market_sentiment.*` | Content-only readers; should be deleted or scheduled |
| `recency_scoring.py` | **NONE** | `insider_track_records.*` | Per project CLAUDE.md PIT rules, `insider_track_records` should NOT be used PIT-sensitively → effectively dead for strategy POV |
| `compute_sell_metrics.py` | **NONE** | `insider_track_records.{sell_*}` | Same — dead code for strategy path |
| `framework/oms/audit.py` (called from `cw_runner.py`) | indirect via cw_runner | `trade_decision_audit.*`, `order_audit.*` | Event-driven append-only — freshness contract not appropriate |
| `framework/oms/alpaca_stream_listener.py` | `alpaca-stream-listener` | `order_audit.{fill_*,…}`, `strategy_portfolio.*` | Same — event-driven |
| `scripts/alpaca_reconcile.py` | `alpaca-reconcile` | `alpaca_position_snapshots.*`, `alpaca_reconciliation.*` | Same — event-driven |

---

## 3.3 — Gap Analysis

The substance of the audit. Categorized by class of failure.

### 3.3.1 ORPHAN WRITERS (script writes a contracted/depended-on column but no plist runs the script)

| Item | Problem class | Blast radius | Suggested fix |
|---|---|---|---|
| **`compute_week52_proximity.py`** writes `trades.week52_proximity`; 6 content/portfolio scripts read it; **no plist** schedules it. | Orphan writer | Content rendering shows stale 52-week proximity. Not live-strategy critical. | Either: (a) wire as a step in `refresh_features_daily.sh` and add a `trades.week52_proximity` contract; (b) document as content-only; or (c) deprecate. |
| **`compute_career_grades.py`** (writes `trades.career_grade`, `insider_ticker_scores.career_*`) was orphan until 2026-05-16 commit `2dcd9c9`. Now in `refresh_features_daily.sh` step 4b. **But no freshness contract exists**, so if step 4b silently stops, only the runner's NULL filter (silently producing 0 candidates) will catch it. The candidate-count-probe — which is supposed to catch this — is itself broken (see 3.3.7). | Untracked column + partially-fixed orphan writer | QM gets 0 candidates (already happened 2026-05-12 → 2026-05-16). Live paper account idle for 5 days. | Add `trades.career_grade` to `config/freshness_contracts.yaml`: `max_staleness_hours: 30, required_for: [quality_momentum], populated_by: pipelines/insider_study/compute_career_grades.py`. |
| **`compute_switch_rate.py`** (writes `trades.is_rare_reversal`, `trades.insider_switch_rate`) was orphan ~8 weeks (SQLite version never re-pointed at PG post 2026-03-25 migration). Now in `refresh_features_daily.sh` step 4c. Contract `trades.is_rare_reversal` exists but its `populated_by` field still names `compute_cw_indicators.py` (the wrong script). `insider_switch_rate` has no contract at all. | Mislabeled contract + untracked column | RD silenced 8 weeks (2026-03-25 → 2026-05-16). | Fix `config/freshness_contracts.yaml:88-92` `populated_by` → `pipelines/insider_study/compute_switch_rate.py`. Add `trades.insider_switch_rate` contract with same writer. |
| **`com.openclaw.intraday-backfill.plist`** has `RunAtLoad=true` and no `StartInterval`. Effectively a one-shot: runs once when loaded (during deploy or boot), never again. `launchctl list` shows status `1` (errored). Log last touched 2026-04-18 18:05. | Cron config bug | `trades.filed_at` not refreshed by this path. Probably masked because `fetch_latest.py` populates `filed_at` at ingest time, but the safety-net script is dead. | Add `StartCalendarInterval` (daily 06:30 PT, after `refresh-features`), OR delete the plist if no longer needed. Either way `launchctl bootout` first to clear the error state. |
| **`compute_market_sentiment.py`**, **`recency_scoring.py`**, **`compute_sell_metrics.py`** — write to `insider_market_sentiment` and `insider_track_records`. No plist. Per project CLAUDE.md, `insider_track_records` shouldn't be used PIT-sensitively in any case. | Orphan writers with dead-or-discouraged readers | None for live strategies. Content/research dashboards may show stale numbers. | Audit each: schedule, or delete. |

### 3.3.2 ORPHAN READERS (strategy / code reads a column whose writer is missing or unwired)

| Item | Problem class | Blast radius | Suggested fix |
|---|---|---|---|
| `quality_momentum.yaml:32` filters `career_grade: ["A+","A"]` — column had no recurring writer until 2026-05-16. **Still no contract** post-fix. | Orphan reader / untracked column | QM produces 0 candidates if writer stops. Already observed. | See 3.3.1 row 2. |
| `reversal_dip.yaml:33` filters `is_rare_reversal: 1` — column had a mislabeled writer for 8 weeks. Now fixed but contract still wrong. | Mislabeled contract | RD produces 0 candidates. Already observed for 8 weeks. | See 3.3.1 row 3. |
| `cw_runner.py:618` supports `min_signal_grade` filter (no current config uses it). `trades.signal_grade` is written by both `compute_signals.py` and `backfill_returns.py`. No contract; if used in future, would silently fail. | Latent orphan reader | Latent — no strategy filters on it today. | Decide single canonical writer for `trades.signal_grade`. Add contract pointing at it. Remove the redundant write site. |

### 3.3.3 STALE CONTRACTS (freshness contract whose writer's last successful run is past the SLA)

As of 2026-05-17 04:11 UTC (Saturday evening PT, weekend quiet window active):

| Column | SLA | Current age | Status | Why |
|---|---|---|---|---|
| `insider_ticker_scores.blended_score` | 30h | 39.0h | STALE (suppressed by quiet window) | `build_pit_scores.py` only runs as part of `refresh-features` step 3, weekday-only. Weekend stale is structurally inevitable. |
| `trades.above_sma50/200, dip_1mo/3mo` | 26h | 26.1h | STALE (suppressed) | Same root cause — written by `compute_cw_indicators.py` in `refresh-features`. `insider-fetch` does run on weekends and DOES invoke `compute_cw_indicators.py`, but only when `inserted>0` (rare on weekends) → effectively weekday-only freshness. |

**Sane state.** Quiet-window logic in `scripts/freshness_probe.py:_in_quiet_window` correctly suppresses these. Monday 06:00 PT refresh-features should restore them to <1h age.

### 3.3.4 MISLABELED CONTRACTS (contract claims fresh but value is stale or wrong) — THE MOST INSIDIOUS CLASS

| Item | Problem | Detection | Suggested fix |
|---|---|---|---|
| **`trades.is_rare_reversal`** contract said `populated_by: pipelines/insider_study/compute_cw_indicators.py`. That script's `INDICATOR_TO_COLUMNS["consecutive"]` USED to include `is_rare_reversal`, so the script called `write_freshness(column="is_rare_reversal")` after each compute — even though the indicator function only writes `consecutive_sells_before`. The `signal_freshness` row was updated every 5-min insider-fetch cycle, freshness probe was green, but the column value itself was 8 weeks stale (the actual writer was the SQLite-only path that died at the 2026-03-25 PG migration). RD silenced. | The freshness WRITE was decoupled from the data WRITE. `write_freshness` was being called from a function (`_write_freshness_for_indicator`) that did not own the column. | Commit `2dcd9c9` (2026-05-16) removed `is_rare_reversal` and `is_10b5_1` from `INDICATOR_TO_COLUMNS["consecutive"]` and added a proper `write_freshness` call in `compute_switch_rate.py`. **Remaining bug**: `config/freshness_contracts.yaml:88-92` still names `compute_cw_indicators.py`. Update to `compute_switch_rate.py`. |
| **`trades.is_10b5_1`** contract says `populated_by: pipelines/insider_study/compute_cw_indicators.py`. Same problem — `compute_cw_indicators.py` was writing freshness rows but the column is actually set during XML ingestion in `backfill_live.parse_form4_xml` (called by `fetch_latest.py`). No script "computes" `is_10b5_1`; it's a parse-time field. The freshness probe says is_10b5_1 is 26h fresh, but what it actually means is "compute_cw_indicators last ran 26h ago," NOT "every trade has a valid is_10b5_1." | Same blind spot. | Contract should either (a) drop `is_10b5_1` (parse-time fields don't need a compute contract) or (b) reroute `populated_by: strategies/insider_catalog/fetch_latest.py` and accept freshness ≈ ingest freshness. |
| **`compute_cw_indicators.py:_write_freshness_for_indicator`** computes `n_rows_affected` as `SELECT COUNT(*) WHERE col IS NOT NULL` after the fact — NOT as the actual UPDATE rowcount. So even if a future indicator regresses and writes 0 rows, the freshness row will still get the old `COUNT(*)` and a fresh timestamp. | Structural risk of regression | Same blind spot as above — freshness write decoupled from data write. | Restructure: each `compute_*` function should write its own freshness inline, with the UPDATE's actual rowcount. Then orphan-write becomes structurally impossible. |

### 3.3.5 WRITERS WITHOUT FRESHNESS CONTRACTS (silent failure invisible)

| Writer | Column(s) | Why no contract | Risk |
|---|---|---|---|
| `compute_career_grades.py` | `trades.career_grade`, `insider_ticker_scores.{career_blended_score, career_grade}` | Just-added writer; contract never authored | **HIGH** — main filter for QM live |
| `compute_switch_rate.py` | `trades.insider_switch_rate` (alongside contracted `is_rare_reversal`) | New column never contracted | LOW currently — not filtered on |
| `compute_week52_proximity.py` | `trades.week52_proximity` | Content-only readers | LOW for strategies, MEDIUM for content |
| `compute_signals.py` | `trades.signal_grade`, `trade_signals.*` | Filter not active in any current strategy | LOW currently, LATENT |
| `backfill_returns.py` (via subprocess `compute_returns.py`) | `trade_returns.*` | Covered transitively | LOW |

### 3.3.6 UNTRACKED COLUMNS strategies read but aren't in any contract or registry

(Same as 3.2b but framed as the GAP.) The 4 most important:

1. **`trades.career_grade`** (QM filter) — **CRITICAL, fix immediately**
2. `trades.insider_switch_rate` (returned in cw_runner audit/UI rows) — LOW
3. `trades.signal_grade` (latent filter capability via `min_signal_grade`) — LOW NOW, LATENT
4. `trades.week52_proximity` (content readers, 6 sites) — LOW for strategies, MEDIUM for content

### 3.3.7 PROBE-LAYER BUGS (the probes that should catch the above are themselves broken)

| Probe | Bug | Effect |
|---|---|---|
| `scripts/candidate_count_probe.py:268` | `NameError: name 'consecutive_zeros' is not defined`. Refactor on 2026-05-13 split `consecutive_zeros` into `medium_silence` / `long_silence` lists but left a stale reference at line 268 (the JSON output branch) and line 269 (human-readable print). Plist `com.openclaw.candidate-count-probe` shows status `1`. **HOWEVER**: the script's alert dispatch (lines 209–247) runs successfully BEFORE the crash — the alerts ARE written to `logs/alerts.ndjson`. The crash is purely during stdout printing. Logs show every market day at 22:00 UTC, QM=0 and RD=0 were detected, but the script exited non-zero. **Additionally**: the 2026-05-13 tuning downgraded single-day zero from `critical` → `warn`. Critical only fires at 5+ consecutive market days of silence. So the 8-week RD silence and 5-day QM silence produced `warn` alerts that didn't page. | Cosmetic NameError compounds the perception problem (anyone manually running the probe sees a traceback first and assumes the probe is broken). The probe DID notice the silent strategies; the alert severity threshold was the problem. |
| `scripts/freshness_probe.py` | No bug; works correctly with weekend quiet window. **But** it can ONLY detect the writer-stopped case (no recent signal_freshness row). It CANNOT detect the mislabeled-contract case where a wrong script writes freshness rows on schedule. | Same root cause as is_rare_reversal's 8-week silence: probe was green because the WRONG writer was checking in. |
| `framework/contracts/freshness.py:assert_fresh` | Same blind spot — sees only `signal_freshness` rows, can't verify the row was written by the column's RIGHTFUL writer. | Strategy preflight halts on stale data but waves through mislabeled-fresh data. |
| `scripts/post_deploy_audit.py:audit_freshness` (line 124) | Same blind spot. | Audit reports "fail" per logs (because of system-broken errors elsewhere), but doesn't distinguish "freshness row missing" from "freshness row written by a script that doesn't own the column." |
| `scripts/preflight/infra_audit.py:check_freshness_contracts_have_writers` (check #6, line 414) | Grep-level only: looks for `write_freshness` token in the `populated_by` script. **Cannot detect** a writer that calls `write_freshness` for the WRONG column. Cannot detect a contract whose `populated_by` points at the wrong script. | The mislabeled-is_rare_reversal contract passed check #6 because `compute_cw_indicators.py` does call `write_freshness` — just not for `is_rare_reversal`. |
| `scripts/preflight/infra_audit.py:NON_CONTRACT_COLS` (line 385) | Hard-coded escape hatch set **includes `career_grade`**. This was the deliberate exemption when career_grade was new and a contract hadn't been written. The exemption is permanent — no follow-up TODO, no test that fails until career_grade is contracted. | The very check designed to catch "this YAML filter has no contract" was muted for the exact column that caused the QM silence. |

### 3.3.8 JOB_CATALOG ↔ launchd discrepancies (`api/routers/admin_diagnostics.py`)

The `/admin/system-status` page reads `JOB_CATALOG` at `api/routers/admin_diagnostics.py:46-83`. Cross-checked against the 23 trading-framework plists on Studio:

| In `JOB_CATALOG` | Present on Studio? | Note |
|---|---|---|
| `insider-fetch, refresh-features, daily-prices, backfill-returns, quality-momentum, reversal-dip, tenb51-surprise, strategy-intraday, strategy-simulator, alpaca-stream-listener, alpaca-intraday-resolver, freshness-probe, alpaca-reconcile` | all yes | The 13 currently-cataloged jobs all exist |
| **MISSING `candidate-count-probe`** | yes (loaded, status `1`) | **Add to JOB_CATALOG** — the broken NameError state would be visible on `/admin/system-status` if listed |
| **MISSING `heartbeat-probe`** | yes | Should be in `monitoring` category |
| **MISSING `compute-signals`** | yes | Should be in `ingestion` category (writes `signal_grade`, `trade_signals`) |
| **MISSING `strategy-health`** | yes | Should be in `monitoring` |
| **MISSING `thesis-monitor`** | yes | Optional — read-only report |
| **MISSING `post-deploy-audit`** | yes (exhausted) | Optional — one-shot dates |
| **MISSING `pit-shadow`** | yes (exhausted) | Optional — one-shot dates |
| **MISSING `daily-summary`** | yes | Optional — email-only, no DB writes |
| **MISSING `intraday-backfill`** | yes (broken, dead) | Adding would surface the dead-plist state |

**Verdict:** 9 trading-framework plists missing from JOB_CATALOG. The `candidate-count-probe` omission is especially harmful because the broken state would be visible on `/admin/system-status` if it were listed.

---

## 3.4 — Dependency Graph: the Mon 06:00 → 06:25 PT Pre-market Chain

The critical path: cw_runners wake at 06:25 PT and call `assert_freshness_system_healthy` + `assert_all_fresh_for_strategy` before scanning. Every contracted column must have a fresh `signal_freshness` row by then. The chain is driven by `com.openclaw.refresh-features.plist` Mon–Fri 06:00 PT → `strategies/insider_catalog/refresh_features_daily.sh`.

```
 06:00 PT  ┌── refresh-features fires (launchd) ─────────────────────────────────┐
           │                                                                     │
           │  step 0  update_daily_prices.py --max-tickers 2000                  │
           │          → INSERT INTO prices.daily_prices (~1.5K tickers settle)   │
           │          → write_freshness(prices.daily_prices.date)                │
           │          ~60s                                                       │
           │                                                                     │
           │  step 1  sync_prices_sqlite.py --days 240                           │
           │          → mirror PG prices to local SQLite cache for indicators   │
           │          ~10s                                                       │
           │                                                                     │
           │  step 2  compute_cw_indicators.py --since <30d-ago>                 │
           │          → UPDATE trades.{dip_1mo, dip_3mo, above_sma50,            │
           │                            above_sma200, purchase_size_ratio,      │
           │                            is_largest_ever, is_tax_sale,           │
           │                            is_recurring, consecutive_sells_before} │
           │          → write_freshness × 9                                     │
           │          ~5s                                                        │
           │                                                                     │
           │  step 3  build_pit_scores.py --start <30d> --end <today>            │
           │          → UPDATE insider_ticker_scores.{blended_score, …}          │
           │          → write_freshness(insider_ticker_scores.blended_score)    │
           │          ~3s                                                        │
           │                                                                     │
           │  step 4  backfill_pit_grades.py --since <30d>                       │
           │          → UPDATE trades.{pit_grade, pit_blended_score}             │
           │          → write_freshness(trades.pit_grade)                       │
           │          ~2s                                                        │
           │                                                                     │
           │  step 4b compute_career_grades.py --since <30d>          [NEW]      │
           │          → UPDATE trades.career_grade,                              │
           │            insider_ticker_scores.{career_blended_score,             │
           │                                   career_grade}                    │
           │          → write_freshness(trades.career_grade)         [NOT YET   │
           │                                                          IN YAML]   │
           │          ~12s                                                       │
           │                                                                     │
           │  step 4c compute_switch_rate.py --since <30d>            [NEW]      │
           │          → UPDATE trades.{insider_switch_rate, is_rare_reversal}    │
           │          → write_freshness(trades.is_rare_reversal,                │
           │                            trades.insider_switch_rate)             │
           │          ~20s                                                       │
           │                                                                     │
           │  step 5  compute_pit_clusters.py --since <30d>                      │
           │          → UPDATE trades.pit_cluster_size                          │
           │          → write_freshness(trades.pit_cluster_size)                │
           │          ~50s                                                       │
           │                                                                     │
           │  step 6  compute_cohen_pit.py --since <30d>                         │
           │          → UPDATE trades.cohen_routine                              │
           │          → write_freshness(trades.cohen_routine)                   │
           │          ~30s                                                       │
           │                                                                     │
           │  staleness check (psql, prints to log)                              │
           │  ~2s                                                                │
           └─────────────────────────────────────────────────────────────────────┘
           Total: ~3 min typical. No hard timeout enforced at plist level.

 06:25 PT  cw_runner.{quality_momentum, reversal_dip, tenb51_surprise} wake
           (~5-min cadence in market-hours window). Each runs:
                assert_freshness_system_healthy(strategy)
                assert_all_fresh_for_strategy(strategy)
           If anything is stale or unknown → halt strategy-wide, alert, exit cycle.

 06:30 PT  market open. cw_runners start admitting candidates.
```

### Dependencies (read order matters):

- Step 0 must complete before step 2 (compute_cw_indicators reads prices via the sqlite cache populated in step 1, sourced from PG step 0)
- Step 1 must complete before step 2
- Step 3 must complete before step 4 (backfill_pit_grades reads insider_ticker_scores)
- Step 4 must complete before step 4b (compute_career_grades reads insider_ticker_scores and writes back career_*)
- Steps 4b, 4c, 5, 6 are mutually independent (could parallelize; currently serial)
- All steps must complete before 06:25 PT or the runner halts at preflight

### Single-point-of-failure: refresh-features.plist itself

If the bash script crashes between steps, downstream steps don't run. No rollback, no retry. `set -euo pipefail` ensures any failure aborts. But **there is no alert if `refresh-features` exits non-zero** — only the downstream cw_runner preflight catches it indirectly via missing `signal_freshness` rows. The Day-1 catch-on-Day-21 outage of April 2026 had exactly this profile.

**Mitigation:** add an explicit `alert.critical` call at the end of the script if any step failed; surface in JOB_CATALOG with a sentinel marker file.

---

## 3.5 — Writer Registry Proposal

The existing `freshness_contracts.yaml` design is half of what's needed. It documents WHAT must be fresh and HOW fresh, but it doesn't verify WHO is responsible on a recurring basis. Three classes of bugs slip through:

1. **Orphan writer**: contract names a `populated_by` script, but no plist runs that script (the career_grade case until 2026-05-16).
2. **Mislabeled writer**: contract names script A as `populated_by`, but the actual data is written by script B; meanwhile script A writes `signal_freshness` rows that the probe sees, masking the failure (the is_rare_reversal 8-week silence).
3. **Untracked reader**: a strategy filter or query reads a column that has no contract at all (career_grade from another angle).

The proposal: **a single authoritative writer registry**, generated at deploy time and verified at runtime.

### Proposed shape: `config/writer_registry.yaml`

Single file. Single source of truth for "which script writes which column on which schedule." Augments rather than replaces `freshness_contracts.yaml`.

```yaml
# config/writer_registry.yaml
#
# Maps every contracted (or strategy-read) column to the (a) script that writes
# it and (b) plist that schedules the script. Verified by preflight, surfaced
# at runtime by the cw_runner heartbeat.

writers:
  - column: trades.dip_3mo
    script: pipelines/insider_study/compute_cw_indicators.py
    indicator: dip                          # for scripts with sub-modes
    plists:
      - com.openclaw.refresh-features       # daily 06:00 PT (step 2)
      - com.openclaw.insider-fetch          # every 5 min (subprocess)
    write_function: compute_dip_indicators
    freshness_function: _write_freshness_for_indicator
    sla_hours: 26
    required_for: [reversal_dip]

  - column: trades.career_grade
    script: pipelines/insider_study/compute_career_grades.py
    plists:
      - com.openclaw.refresh-features       # daily 06:00 PT (step 4b)
    sla_hours: 30
    required_for: [quality_momentum]

  - column: trades.is_rare_reversal
    script: pipelines/insider_study/compute_switch_rate.py
    plists:
      - com.openclaw.refresh-features       # daily 06:00 PT (step 4c)
    sla_hours: 26
    required_for: [reversal_dip]

  - column: trades.is_10b5_1
    script: strategies/insider_catalog/fetch_latest.py   # ingest-time field
    plists:
      - com.openclaw.insider-fetch
    sla_hours: 48
    required_for: [reversal_dip, tenb51_surprise]
    notes: "parse-time field from Form 4 XML — no compute step"

  # ... one entry per column
```

### Preflight checks (Mini-side, `scripts/preflight/writer_registry_audit.py`)

For each entry:

1. **Script exists** at the declared path
2. **Plists exist** in repo plist templates OR on Studio: `/Users/derekg/Library/LaunchAgents/{plist}.plist`
3. **Plists are loaded** on Studio (`launchctl list | grep {plist}` via heartbeat artifact or SSH)
4. **Plist actually invokes the script:** parse the plist `ProgramArguments` and verify the script path is referenced (handles `bash` wrapper scripts by recursing one level into them)
5. **Script calls `write_freshness(column=...)`** — AST analysis, not grep, so we catch `column="career_grade"` regardless of variable name
6. **No two scripts claim the same column** (or if they do, the registry must declare the canonical one)
7. **`freshness_contracts.yaml` and `writer_registry.yaml` are consistent:** every contract has a registry entry; every registry entry with `required_for` has a contract

### Runtime check (Studio-side, in cw_runner preflight)

Already exists via `assert_freshness_system_healthy` and `assert_all_fresh_for_strategy`. Augment with `assert_writer_wired`:

```python
def assert_writer_wired(conn, column: str) -> None:
    """Verify the most recent signal_freshness row for `column` was
    written by the registry's canonical writer, not by some other script
    that happens to call write_freshness for this column.

    Raises WriterMismatchError if the most recent signal_freshness row's
    populated_by != registry[column].script.
    """
```

This would have caught the is_rare_reversal mislabeling: the contract said `compute_cw_indicators.py`, the registry says `compute_switch_rate.py`, the most recent signal_freshness row (pre-fix) was `populated_by=pipelines/insider_study/compute_cw_indicators.py` → registry mismatch → halt.

### Deploy-time generation (proposed)

Walk the source tree once at deploy time, AST-extract every `write_freshness(table=..., column=...)` call, cross-reference plist `ProgramArguments`, emit a `writer_registry.lock.yaml` that pins the discovered map. Diff against checked-in `writer_registry.yaml`. CI fails if they diverge.

Structurally equivalent to `package-lock.json` for column writers.

### Alternative: pure annotation-driven (no separate YAML)

```python
@writes_column("trades.career_grade",
               sla_hours=30,
               required_for=["quality_momentum"])
def compute_career_grade_for_trade(conn, trade_id):
    ...
```

A startup-time scan walks the codebase, collects every decorator, builds the registry in-memory. Keeps writer and registration in the same file but harder to enforce externally (YAML lets a CI step on a separate machine verify everything without importing Python).

**Recommendation: hybrid.** Keep `writer_registry.yaml` as the human-facing checked-in artifact. Add the decorator for AST-discoverability. Have `scripts/preflight/writer_registry_audit.py` reconcile YAML against decorators.

### Migration path

1. **Author `config/writer_registry.yaml`** from this audit. Start with the 15 contracted columns + the 4 untracked-but-read columns (career_grade, insider_switch_rate, signal_grade, week52_proximity).
2. **Add `scripts/preflight/writer_registry_audit.py`** with checks 1–7 above.
3. **Wire into `scripts/preflight/infra_audit.py`** as check #7.
4. **Add `assert_writer_wired`** to cw_runner preflight (new runbook R-009).
5. **Fix the candidate-count-probe NameError** (3-line patch — change line 268 and line 269 to reference `medium_silence`/`long_silence` instead of `consecutive_zeros`).
6. **Add missing plists to `JOB_CATALOG`** in `api/routers/admin_diagnostics.py` (9 entries).
7. **Remove `career_grade`** from `NON_CONTRACT_COLS` in `infra_audit.py:385` once the contract exists.
8. **Fix `populated_by` in `freshness_contracts.yaml`** for `trades.is_rare_reversal` and decide whether to keep / drop / reroute `trades.is_10b5_1`.

---

## 3.6 — Open Questions / Decisions for Derek

1. **Structural fix for mislabeled freshness writes.** The cleanest fix is: each compute function writes freshness inline at the end of its UPDATE, with the UPDATE's actual ROWCOUNT (psycopg's `cur.rowcount`), not a post-hoc `SELECT COUNT(*)`. This makes orphan writes structurally impossible. **Worth the refactor?**

2. **Weekend SLA gap.** `insider_ticker_scores.blended_score`, `dip_*`, `above_sma*` are STALE every Sat/Sun because `refresh-features` is weekday-only and SLAs can't reach Monday morning. Options: (a) widen SLA to 80h, (b) add `weekday_only: true` flag to contracts, (c) keep current setup (quiet window suppresses pages). Which?

3. **JOB_CATALOG**: should the admin page surface ALL openclaw plists by enumerating `~/Library/LaunchAgents/` at request time, or stay curated? Today it's curated and misses 9. Curated has the advantage of human-classifying each plist's category; enumeration has the advantage of zero-maintenance and surfacing unknown plists.

4. **`intraday-backfill.plist`**: delete or schedule? Dead since 2026-04-18 with nothing depending on it.

5. **`compute_week52_proximity.py`**: schedule (daily? weekly?), delete, or migrate to a content-only cadence?

6. **Registry storage format**: YAML (proposed), JSON, or a small SQL table on Studio so it can be queried from the admin page at request time? YAML wins for review and checked-in-ness. SQL wins for runtime introspection on `/admin/system-status`. Decorators win for keep-in-sync-ness. Hybrid (YAML + decorators) is most robust but most surface area.

---

## 3.7 — File / Path References

For navigation:

- `config/freshness_contracts.yaml` — current 15-column contract list
- `framework/contracts/freshness.py` — registry loader, `assert_fresh`, `get_freshness`, `assert_freshness_system_healthy`
- `framework/contracts/freshness_writer.py` — `write_freshness()` helper
- `framework/contracts/exceptions.py` — `StaleSignalError`, `FreshnessUnknownError`, `FreshnessSystemBrokenError`
- `strategies/insider_catalog/refresh_features_daily.sh` — the 7-step daily chain
- `strategies/insider_catalog/fetch_latest.py` — every-5-min EDGAR poll; triggers compute subprocesses
- `pipelines/insider_study/compute_cw_indicators.py` — 9 indicator columns; the mislabeled-contract bug lived here pre-2026-05-16
- `pipelines/insider_study/compute_career_grades.py` — added 2026-05-16; populates `trades.career_grade`
- `pipelines/insider_study/compute_switch_rate.py` — added 2026-05-16; populates `trades.{insider_switch_rate, is_rare_reversal}`
- `pipelines/insider_study/compute_pit_clusters.py` — populates `trades.pit_cluster_size`
- `pipelines/insider_study/compute_cohen_pit.py` — populates `trades.cohen_routine`
- `pipelines/insider_study/backfill_pit_grades.py` — populates `trades.{pit_grade, pit_blended_score}` from `insider_ticker_scores`
- `pipelines/insider_study/update_daily_prices.py` — populates `prices.daily_prices`
- `pipelines/insider_study/compute_week52_proximity.py` — orphan writer
- `pipelines/insider_study/compute_signals.py` — orphan-ish writer (`signal_grade`, `trade_signals.*`)
- `scripts/freshness_probe.py` — every-30-min probe over `signal_freshness`; weekend quiet window
- `scripts/candidate_count_probe.py` — every-market-day probe; **has the NameError at lines 268-269**
- `scripts/post_deploy_audit.py` — `audit_freshness` at line 124 has same blind spot as freshness_probe
- `scripts/preflight/infra_audit.py` — check #6 (line 414) is the grep-level writer check; `NON_CONTRACT_COLS` (line 385) is the escape hatch
- `api/routers/admin_diagnostics.py:46-83` — `JOB_CATALOG`
- `~/Library/LaunchAgents/com.openclaw.*` on Studio — 68 plists total; 23 trading-framework, 35 prediction-markets, 10 mixed
- `logs/refresh-features.log`, `logs/candidate-count-probe.log`, `logs/freshness-probe.log`, `logs/insider-fetch.log` on Studio — last successful runs
- PG `form4.signal_freshness` table — `(source, table_name, column_name, last_computed_at, n_rows_affected, run_id, populated_by)`

---

# Part 4 — Per-Table Deep Dive: Scoring & Signals Tables

*Complement to Part 3's cron-and-registry view. Part 4 is the per-TABLE view — every PG table derived from trades, every column's writer/consumer/PIT status, and the populated-percentage data from live PG queries on 2026-05-16.*

**Why this section is separate from Part 3:** Part 3 catalogs which plist runs which script. Part 4 catalogs which column lives where and who reads it. Both views are needed — the registry must enforce both "writer exists & runs" (Part 3) AND "every column has a documented writer + consumers" (Part 4). A reader-only consumer with no writer is just as bad as a writer with no scheduler.

Generated 2026-05-16. PG queries on Mac Studio `form4` database. Findings extend `reference_signal_registry.md` in Claude memory.

## P4.0 Table inventory + freshness snapshot (PG query results)

| Table | Rows | Latest data | Latest write | PIT? | Recurring writer? |
|---|---:|---|---|---|---|
| `trades` | 1,680,483 | filing_date=2026-05-15 | insider-fetch every 5 min | (per column) | yes (ingest) |
| `trade_returns` | 1,621,193 | computed_at=2026-05-16 05:02 | backfill-returns daily 05:00 PT | clean | yes |
| `trade_signals` | 1,862,684 | computed_at=2026-05-15 17:54 | compute-signals wkdays 17:45 PT | mostly clean | yes |
| `trade_decision_audit` | 382,899 | ts=2026-05-16 21:08 | cw_runner scan_signals + execute_entries | n/a (audit log) | yes |
| `signal_freshness` | 7,831 | last_computed_at=2026-05-16 20:59 | every compute pipeline | n/a (telemetry) | yes |
| `insider_ticker_scores` | 361,337 | as_of_date=2026-05-15 | build_pit_scores + compute_career_grades + backfill_live | clean | yes |
| `score_history` | 609,241 | as_of_date=2026-05-15 | pit_scoring.upsert_score | clean | yes (append-only) |
| `insider_track_records` | 87,757 | **computed_at=2026-03-24 (53 DAYS STALE)** | backfill.compute_track_records (manual only) | KNOWN VIOLATION | **NO** |
| `insiders` | 126,368 | updated_at=2026-05-15 | fetch_latest.get_or_create_insider | n/a | yes |
| `insider_companies` | 116,087 | last_trade=2029-05-04 **(DATA BUG — future date)** | backfill._build_insider_companies + dedup/price-fix | n/a | **manual only** |
| `insider_groups` | 1,340 | created_at=2026-03-14 (62d stale) | entity_resolution.py (manual) | n/a | **NO** |
| `insider_group_members` | 2,855 | — | entity_resolution.py | n/a | **NO** |
| `pull_status` | 0 | — | resumable_puller.py | n/a | n/a (options DORMANT) |
| `research.derivative_trades` | 1,162,052 | — | backfill_live.insert_derivative_trades (bulk only) | n/a | **NO from 5-min loop** |

## P4.1 The PIT-stamped vs live-queried distinction (the conviction-drift mode)

**This is the single most important architectural distinction in this layer.** Today's three silent failures all lived in the PIT-stamped column class.

PIT-stamped columns are computed ONCE when a trade is processed and frozen on `trades.row`. Live strategies read them direct from `trades.*` at scan time. If the writer orphans, NEW trades get NULL but OLD trades retain their values → silent degradation that's harder to detect than "everything is broken."

Live-queried (= computed-on-read) values are not stored on `trades`; they're recomputed every scan from upstream tables. If the upstream is fresh, conviction is fresh. If the upstream orphans, consumer silently re-uses last-known values → conviction drifts. NOT visible via `signal_freshness` unless the upstream writer is itself contracted.

| Column | Storage model | Failure mode |
|---|---|---|
| `trades.pit_grade`, `pit_blended_score` | PIT-stamped by `backfill_pit_grades.py` | Writer orphan → new trades NULL → QM filter `pit_grade IN ('A+','A')` silently matches zero |
| `trades.career_grade` | PIT-stamped by `compute_career_grades.py` (added 2026-05-16) | **OUTAGE 2026-04-09 → 2026-05-16:** inline-only writer never invoked from insider-fetch. QM died 2026-05-12. |
| `trades.pit_cluster_size` | PIT-stamped by `compute_pit_clusters.py` | **OUTAGE 2026-04-07 → 2026-05-08:** SQLite-only writer never re-pointed at PG |
| `trades.is_rare_reversal`, `insider_switch_rate` | PIT-stamped by `compute_switch_rate.py` (PG rewrite 2026-05-16) | **OUTAGE 2026-03-25 → 2026-05-16 (~8 weeks):** SQLite-only writer. Silently silenced reversal_dip. |
| `trades.consecutive_sells_before`, `is_largest_ever`, `dip_*`, `above_sma*`, `is_recurring`, `is_tax_sale`, `cohen_routine`, `is_10b5_1`, `purchase_size_ratio`, `week52_proximity` | PIT-stamped (compute_cw_indicators / compute_cohen_pit / compute_week52_proximity) | `week52_proximity` is the next orphan — still SQLite-only |
| `trades.signal_grade`, `signal_quality`, `signal_category` | PIT-stamped (signal_grade via `_update_recent_signals`; signal_quality/category via one-shot `transaction_classifier`) | signal_quality/category is orphan |
| `trades.pit_n_trades`, `pit_win_rate_7d`, `pit_avg_abnormal_7d`, `pit_win_rate_30d` | **NO ACTIVE WRITER** (registry #12) | Permanent NULL → consumer COALESCE into global `itr.buy_*` = **PIT VIOLATION** (registry red flag #1) |
| (not on trades) `insider_ticker_scores.blended_score`, `career_blended_score` | **Live-queried** by `pit_helpers.enrich_with_best_pit_grade`, leaderboard CTE | If `build_pit_scores` orphans, ITS stale → live consumers display old scores; conviction drifts NOT VISIBLE via signal_freshness unless contracted |

## P4.2 `trades` — per-column writer map (87 columns, ~26 derived)

### P4.2.1 Raw ingest columns (written once at insert)

Source: Form 4 XML → `backfill_live.insert_trades` line 796, called by `fetch_latest.run_fetch` (5-min loop) and `backfill_live.main` (bulk). Once written, only ad-hoc fix-up scripts (`fix_bad_dates.py`, `normalize_titles.py`, `reparse_bulk.py`) ever touch.

Columns: `trade_id` (seq), `insider_id`, `ticker`, `company`, `title`, `trade_type`, `trade_date`, `filing_date`, `price`, `qty`, `value`, `is_csuite`, `title_weight`, `source`, `accession`, `filed_at`, `trans_code`, `trans_acquired_disp`, `direct_indirect`, `shares_owned_after`, `value_owned_after`, `nature_of_ownership`, `equity_swap`, `is_10b5_1`, `security_title`, `deemed_execution_date`, `trans_form_type`, `rptowner_cik`, `is_derivative`, `created_at`, `filing_key`, `txn_group_id`, `superseded_by`, `is_amendment`.

**Freshness:** `trades.filing_date` 48h SLO, required_for ['*'], populated_by `fetch_latest.py`. Written only on `inserted > 0`.

### P4.2.2 PIT scoring stamps (projected from `insider_ticker_scores`)

#### `pit_grade` — % populated: 68.2%
- Writer: `pipelines/insider_study/backfill_pit_grades.py:main` (79-120). Binary-search ITS for `as_of_date <= filing_date` AND `sufficient_data=1`; map blended_score → letter.
- Schedule: Step 4 of `refresh_features_daily.sh` (wkdays 06:00 PT). Also from `fetch_latest._run_indicators` (172-174) with `--since today-7d` after every fetch.
- Recurring: YES (481 runs / 14d via signal_freshness)
- Contract: 30h, required_for [quality_momentum]
- Consumers: `cw_runner.py:630-648` filter; `trade_grade.py:85`; `api/routers/{filings,companies,signals,clusters,insiders,private_companies,export}.py`; `compute_signals.py` for `insider_returns`/`high_signal`/`quality_momentum_buy`
- PIT: CLEAN (registry #2)
- Issue: If upstream `build_pit_scores` stale, pit_grade shows LAST-KNOWN value (not NULL), masking the staleness — silent drift.

#### `pit_blended_score` — same writer, schedule, % populated as `pit_grade`. CLEAN (registry #14).

#### `career_grade` — % populated: 67.2%
- Writer: `pipelines/insider_study/compute_career_grades.py:main` (90-115). trans_code='P' AND filing_date >= since AND (career_grade IS NULL OR --rebuild). Computes V3 via `BayesianScorerV3` (5y half-life, soft floor).
- Schedule: Step 4b of `refresh_features_daily.sh` — **added 2026-05-16**
- Recurring: YES (as of 2026-05-16). Last write 2026-05-16 20:25.
- Other writers: `backfill_live.py:1184-1191` inline V3 block (bulk only — NOT 5-min loop); `backfill_v3_missing_trades.py` one-shot; `backfill_v3.py` scratch-table one-shot
- Contract: **NONE — gap**
- Consumers: `cw_runner.py:641-648` filter (quality_momentum uses `career_grade IN ('A+','A')`); `api/pit_helpers.py:74,108,186`; `api/routers/portfolio.py:43,57,208`; `api/routers/filings.py:151,192,285,314,360`; leaderboard CTE default sort
- PIT: CLEAN (registry #1). PIT guard: `_get_returns` filters `t.trade_date <= as_of_date - lag AND t.filing_date <= as_of_date`.
- **Issue: HISTORIC OUTAGE 2026-04-09 → 2026-05-16:** only writer was inline in `backfill_live.main()` — not invoked by 5-min `insider-fetch`. QM died 2026-05-12. Today's fix added standalone script.

#### `pit_cluster_size` — % populated: 48.9%
- Writer: `pipelines/insider_study/compute_pit_clusters.py:main` (86-127). Per-(ticker, trans_code) 30d sliding window; counts distinct OTHER insiders filing in prior 30d.
- Schedule: Step 5 of `refresh_features_daily.sh`
- Recurring: YES (was orphaned 2026-04-07 → 2026-05-08; now PG-native)
- Contract: 26h, required_for ['*']
- Consumers: `api/trade_grade.py` cluster_size input (+20 pts if cluster>=3); `conviction_score.py`; `compute_signals.top_trade` cluster path
- PIT: CLEAN (registry #15)
- Issue: 48.9% pop because daily chain uses `--since 30d`; pre-2026-05 trades not backfilled.

### P4.2.3 CW indicator family (writer: `compute_cw_indicators.py`)

Step 2 of `refresh_features_daily.sh`, wkdays 06:00 PT, `--since 30d`. Also called from `fetch_latest._run_indicators` with `--since 7d`.

| Column | Writer fn (line) | Fresh | Required | %pop | Notes |
|---|---|---:|---|---:|---|
| `dip_1mo` | compute_dip_indicators (147-171) | 26h | reversal_dip | 94.5% | required for `dip_1mo` reads |
| `dip_3mo` | same | 26h | reversal_dip | 91.9% | required for `dip_3mo <= -0.25` |
| `dip_1yr` | same | — | — | unknown | `deep_dip_buy` detector |
| `sma50_rel`, `sma200_rel` | compute_sma_context (193-247) | — | — | unknown | momentum_buy detector |
| `above_sma50` | same | 26h | quality_momentum | 92.5% | cw_runner QM filter |
| `above_sma200` | same | 26h | quality_momentum | 85.4% | same |
| `purchase_size_ratio` | compute_purchase_size_metrics (267-311) | — | — | 43.8% | `largest_purchase_ever` |
| `is_largest_ever` | same | — | — | 98.4% | conviction, trade_grade |
| `is_tax_sale` | compute_tax_sale_flag (318-360) | 48h | ['*'] | 0.12% true | `tax_sale_noise`, deep_reversal_dip exclusion |
| `is_recurring` | compute_recurring_purchase (367-442) | 48h | ['*'] | 10.0% | `recurring_buyer_noise`, deep_reversal_dip exclusion |
| `recurring_period` | same | — | — | — | display only |
| `consecutive_sells_before` | compute_consecutive_sells (449-500) | 26h | reversal_dip | 29.7% | `reversal_buy`, `deep_reversal_dip_buy`, `reversal_quality_buy`, conviction reversal path |

**Gaps:** `is_largest_ever`, `purchase_size_ratio`, `is_tax_sale`, `is_recurring` have no `freshness_contracts.yaml` entry. They share the writer with contract-tracked columns so they implicitly get fresh on every refresh — should be added for completeness if any strategy starts filtering on them.

### P4.2.4 `cohen_routine` — % populated: 99.7%

- Writer: `pipelines/insider_study/compute_cohen_pit.py:compute_cohen_pit` (38-161). PIT: for each (insider, ticker, month) check if 3+ consecutive years STRICTLY BEFORE the trade's year.
- Schedule: Step 6 of `refresh_features_daily.sh` (wkdays 06:00 PT). Also called by `backfill_returns._update_recent_signals` (daily 05:00) with `--since today-7d`.
- Recurring: YES (16 runs / 14d)
- Contract: 48h, required_for [reversal_dip]
- Consumers: `trade_grade.py` opportunistic; `conviction_score.py`; `compute_signals.opportunistic_trade`; `compute_cw_indicators.compute_tax_sale_flag` excludes routine=1
- PIT: CLEAN (registry #17)
- Note: Uses `strftime` (SQLite-ism); PG compat layer at `config/database.py` translates. Verified working.

### P4.2.5 `week52_proximity` — % populated: 43.8% — ORPHAN

- Writer: `pipelines/insider_study/compute_week52_proximity.py` (172 lines). Uses `sqlite3.connect(DB_PATH)` direct — **NOT migrated to PG.**
- Schedule: **NONE.** Not in `refresh_features_daily.sh`. No launchd plist.
- Recurring: **NO — ORPHAN since SQLite→PG migration.**
- Consumers: `api/trade_grade.py` 52w-proximity bonus; `conviction_score.py`; also 6 content-pipeline readers (`generate_breaking_signal.py`, `generate_daily_content.py`, `portfolio_simulator.py`, `render_video_assets.py`, `render_ig_carousel.py`)
- Fix: Rewrite for PG, add to refresh chain, add freshness contract.

### P4.2.6 Legacy / suspect derived columns

#### `pit_n_trades`, `pit_win_rate_7d`, `pit_avg_abnormal_7d`, `pit_win_rate_30d`, `pit_avg_abnormal_30d`
- % populated: `pit_n_trades` 48.0%, `pit_win_rate_7d` 40.8%
- Writer: **NONE FOUND.** No active script writes them.
- Recurring: **NO — orphaned columns (registry #12)**
- PIT: **SUSPECT.** When NULL, `portfolio_simulator.compute_signal_quality` + 4 content scripts COALESCE into `insider_track_records.buy_win_rate_7d` / `buy_count` — **PIT VIOLATION** (registry red flag #1)
- Recommended: Either repopulate from PIT data (snapshot from ITS at ingest) OR remove + migrate 5 consumers

#### `signal_grade` (E/S/A/W/P/F) — % populated: 99.2%
- Distribution: B 545K, C 443K, D 297K, A 238K, F 125K, W 10K, S 4.8K, P 2.1K, E 128
- Writer: `pipelines/insider_study/backfill_returns._update_recent_signals` (320-346). Calls `api.trade_grade.compute_trade_grade` then `tg["label"][0]`.
- Schedule: Daily 05:00 PT via `com.openclaw.backfill-returns.plist`. Only trades filed in last 7 days.
- Recurring: YES
- Contract: None
- Consumers: `cw_runner.py:399-406` `min_signal_grade` filter (set in quality_momentum.yaml); `cw_simulation.py:273-275`; daily-content generators
- PIT: CLEAN (registry #13)
- Note: Two letter systems coexist — (a) original `signal_quality` letter from `transaction_classifier` (W/P/F); (b) new `trade_grade["label"][0]` (E/S/A/W/P). 7-day rolling refresh only updates recent — historical letters may be original taxonomy.

#### `signal_quality`, `signal_category`, `is_routine` — ORPHAN
- % populated: `signal_quality` 97.2%, 7 distinct categories
- Writer: `strategies/insider_catalog/transaction_classifier.py:batch_classify` (187-220). Trans-code heuristic, NOT PIT-grounded.
- Schedule: **NOT in refresh chain. Manual only.**
- Recurring: **NO — ORPHAN.**
- Consumers: Limited — `compute_signals.opportunistic_trade` reads `is_routine`. `signal_quality` largely unused (active scoring via `signal_grade`).
- Recommended: Deprecate signal_quality + signal_category. Migrate `opportunistic_trade` to read `cohen_routine` directly.

#### `effective_insider_id`, `normalized_title`
- `effective_insider_id` set by `entity_resolution.py:592` on group resolve. NOT RECURRING (insider_groups.created_at maxes 2026-03-14).
- `normalized_title` set by `normalize_titles.py` one-shot. NOT RECURRING.

## P4.3 `insider_ticker_scores` (361,337 rows; key=(insider_id, ticker, as_of_date))

The PIT scoring substrate. `trades.pit_grade`/`career_grade` are PROJECTIONS from this table.

| Column | Writer | PIT |
|---|---|---|
| `insider_id`, `ticker`, `as_of_date` | `pit_scoring.upsert_score` (563) | n/a (key) |
| `ticker_trade_count`, `ticker_win_rate_7d`, `ticker_avg_abnormal_7d`, `ticker_score` | `upsert_score` | CLEAN |
| `global_trade_count`, `global_win_rate_7d`, `global_avg_abnormal_7d`, `global_score` | same | CLEAN |
| `blended_score` (V2) | same | CLEAN |
| `role_at_ticker`, `role_weight`, `is_primary_company`, `sufficient_data` | same | CLEAN |
| `career_blended_score`, `career_grade` (V3) | `compute_career_grades.py:107-111` OR `backfill_live.py:1184-1188` OR `backfill_v3_missing_trades.py:77-85` | CLEAN |

**% populated:** `blended_score` 69.1%, `career_blended_score` 40.8%, `career_grade` 51.9%, `sufficient_data=1` 69.1%.

**Recurring writers:**
1. `strategies/insider_catalog/build_pit_scores.py:build_walkforward_scores` (133-227) — step 3 of refresh chain
2. `pipelines/insider_study/compute_career_grades.py` — step 4b (added 2026-05-16)
3. `strategies/insider_catalog/backfill_live.py:1152-1197` — newly inserted live trades during bulk only

**Contract:** `insider_ticker_scores.blended_score` 30h, required_for ['*'].

**Consumers:**
- `backfill_pit_grades.py` (projects to `trades.pit_grade`)
- `api/pit_helpers.py` — `enrich_with_best_pit_grade`, `get_best_pit_grade`, `get_ticker_pit_grade`, `get_ticker_grades`
- `api/routers/leaderboard.py` — V3 CTE for default sort
- `api/routers/signals.py:226,277` (sell_cessation)
- `pipelines/insider_study/compute_signals.top_trade` PIT path
- `pit_scoring._get_returns` (486-507) — PIT-guarded

**PIT:** CLEAN. Walk-forward in `build_pit_scores.py` only uses returns observable as of `as_of_date - lag` (10/40/100 days for 7d/30d/90d).

**Issues:**
- ~31% of trades have no ITS row predating their filing_date → NULL `career_grade`. `backfill_v3_missing_trades.py` is manual cleanup.
- A stale ITS row from 2024 satisfies `as_of_date <= filing_date` for 2024 trades — but if `build_pit_scores` stops, NEW 2026 trades won't have an entry at filing_date. Silent-degradation pattern.

## P4.4 `score_history` (609,241 rows, append-only)

Snapshot stream of every score insert.

- Writer: `pit_scoring.py:595-605` `INSERT INTO score_history` — always paired with `upsert_score`
- Schedule: Same as ITS (step 3 of refresh chain)
- Recurring: YES
- Consumers: No active router consumer found. Historical chart endpoints.
- Issue: Append-only — grows unbounded (~1.7× ITS size). No truncation policy.

## P4.5 `trade_returns` (1,621,193 rows; PK=trade_id)

Forward-return outcomes per trade — SOURCE OF TRUTH for what insider trades earned.

Columns: `entry_price`, `exit_price_*`, `return_*`, `spy_return_*`, `abnormal_*` (7d/14d/30d/60d/90d/180d/365d), `computed_at`.

**Coverage:** 7d 99.4%, 14d 99.2%, 30d 98.4%, 60d 97.2%, 90d 96.4%, 180d 93.9%, 365d 88.1%.

**Writer:** `strategies/insider_catalog/compute_returns.py:process_trades` (line 143+). **NOTE:** there is NO `pipelines/insider_study/compute_returns.py` — only the strategies-side version exists; the project CLAUDE.md's reference to a pipelines-side path is stale.

**Called by:** `pipelines/insider_study/backfill_returns.run_compute_returns` (153-172) as subprocess. Daily 05:00 PT via `com.openclaw.backfill-returns.plist`:
1. `find_missing_tickers` → `collect_prices.py` (Alpaca download)
2. `_regenerate_last_dates` → `last_dates.json`
3. `run_compute_returns` → `compute_returns.py --trade-type both` (writes `trade_returns`)
4. `_sync_daily_prices` → INSERT INTO `daily_prices`
5. `_update_recent_signals`:
   - `compute_cohen_pit.py --since 7d` (writes `trades.cohen_routine`)
   - `trade_grade.compute_trade_grade` → UPDATE `trades.signal_grade` (last 7d)

**Recurring:** YES — last computed_at 2026-05-16 05:02.

**Contract:** None directly. Indirectly covered via `prices.daily_prices.date` (72h).

**Consumers:** `build_pit_scores.py` (`abnormal_7d/30d/90d` LEFT JOIN); `pit_scoring._get_returns` (PIT-guarded); `backfill.compute_track_records` (legacy ITR); `compute_sell_metrics.py`; `api/routers/{portfolio,signals,dashboard}.py` (display).

**PIT:** CLEAN — returns are observed (factual). PIT-cleanliness enforced by CONSUMERS (lag observation by window+settlement days). `_get_returns` does this correctly.

## P4.6 `trade_signals` (1,862,684 rows)

Per-trade signal tags from 21 detectors. Each (trade_id, signal_type) is unique (UNIQUE constraint).

**Writer:** `pipelines/insider_study/compute_signals.py:run_detector` (1290-1304). Idempotent: each detector first `DELETE FROM trade_signals WHERE signal_type = ?` then bulk INSERT.

**21 detectors** (all in `pipelines/insider_study/compute_signals.py`):
- Basic: `first_time_buyer`, `insider_returns`, `size_anomaly`, `high_signal`, `top_trade`, `post_vest_dump`, `exercise_and_sell`, `trend_reversal`, `buying_the_dip`, `selling_the_rip`, `contrarian`, `large_holdings_increase`, `small_holdings_increase`, `ten_pct_owner_buy`, `opportunistic_trade`, `deep_dip_buy`, `reversal_buy`, `momentum_buy`, `largest_purchase_ever`, `recurring_buyer_noise`, `tax_sale_noise`
- Composites: `quality_momentum_buy`, `tenb51_surprise_buy`, `deep_reversal_dip_buy`, `reversal_quality_buy`

**Schedule:** Wkdays 17:45 PT via `com.openclaw.compute-signals.plist`. Full-table compute.

**Recurring:** YES — last computed_at 2026-05-15 17:54.

**Contract:** None.

**Consumers:** `api/routers/signals.py`, `api/routers/clusters.py`, `api/signals_enrichment.py`.

**Issues:**
- DELETE→INSERT not transactionally safe — failure mid-run leaves zero rows for that signal_type until next successful run.
- `compute_signals.py` does NOT write `signal_freshness` rows. Drift invisible to freshness system. Recommended: add `write_freshness` after each detector.

## P4.7 `trade_decision_audit` (382,899 rows)

Per-stage decision log from live strategy runners. Powers `/admin/diagnostics`.

**Writer:** `framework/oms/audit.py:write_decision` (27-61). Inputs from:
- `strategies/cw_strategies/cw_runner.py:scan_signals` (line 672+) — dedup/pit_lookup/conviction stages
- `cw_runner.py:execute_entries` (line 1060+) — capacity/circuit stages
- `framework/oms/runner.py:evaluate_candidates_v2` (line 50+) — V2 OMS path (env-flagged behind `OMS_V2`)

**Schedule:** Continuous during market hours via 3 live strategy plists on Studio.

**Recurring:** YES — last ts 2026-05-16 21:08.

**Backfill scripts:**
- `scripts/backfill_decision_audit_from_logs.py` (one-shot)
- `pipelines/insider_study/simulate_decision_audit.py` (offline backtest, `source='shadow'`/`'simulator'`)
- `scripts/pit_shadow_run.py` (PIT shadow validator)

**Schema:** `id (seq), ts, run_id (uuid), strategy, ticker, trade_id, filing_date, thesis, stage, passed, reason, pit_grade, conviction, feature_snapshot (jsonb), source`

**Consumers:** `api/routers/admin_diagnostics.py:111-272` — strategy stage rollups, recent rejections, filter funnel. Internal observability only.

**Issues:** `feature_snapshot` jsonb grows large (no archival). `source` filter `'live'` (admin_diagnostics:140) — if a backfill forgets to set source, pollutes live diagnostics.

## P4.8 `signal_freshness` (7,831 rows)

Freshness telemetry. Every recurring compute pipeline writes a row in the same transaction as its data write (Phase 2 P0 design).

**Writer:** `framework/contracts/freshness_writer.py:write_freshness` (44-106).

**Confirmed recurring callers (with last run + runs in last 14d, from PG query):**
- `compute_pit_clusters.py` → trades.pit_cluster_size (2026-05-16 20:59, 9 runs)
- `backfill_pit_grades.py` → trades.pit_grade (2026-05-16 20:27, 481 runs — high freq from fetch_latest)
- `compute_cw_indicators.py` → 10 columns (608 runs each)
- `compute_switch_rate.py` → trades.is_rare_reversal, insider_switch_rate (1 run — first after today's fix)
- `compute_career_grades.py` → trades.career_grade (1 run — first after today's fix)
- `compute_cohen_pit.py` → trades.cohen_routine (16 runs)
- `fetch_latest.py` → trades.filing_date (600 runs — 5-min cadence)
- `build_pit_scores.py` → insider_ticker_scores.blended_score (8 runs)
- `update_daily_prices.py` → prices.daily_prices.date (10 runs)

**Consumers:** `framework/contracts/freshness.py:assert_fresh` — runner preflight; `api/routers/admin_diagnostics.py:661` `/api/admin/freshness`; `scripts/post_deploy_audit.py`.

**Known coverage gaps:**
- `compute_signals.py` does NOT write rows for `trade_signals` (gap)
- `transaction_classifier.batch_classify` does NOT write rows (orphan)
- `compute_week52_proximity.py` does NOT write rows (orphan)
- `populated_by` is free-form — duplicates produce phantom rows. Recommend allowlist validation.

## P4.9 `insider_track_records` (87,757 rows) — LEGACY / 53-DAY STALE

39-column legacy table. **Latest `computed_at = 2026-03-24 17:53` — 53 days stale on 2026-05-16.**

**Writers (all manual / on-demand):**
- `strategies/insider_catalog/backfill.py:compute_track_records` (758-781), `_compute_scores` (799-955), `_build_insider_companies` (958+) — full DELETE + INSERT bulk. Called from `backfill.main()` (bulk backfill only).
- `pipelines/insider_study/compute_sell_metrics.py:134` — UPDATE sell-side.
- `pipelines/insider_study/recency_scoring.py:299` — UPDATE recency-weighted.
- `strategies/insider_catalog/pit_scoring.py:sync_to_track_records` (608-640) — optional UPDATE score/tier/percentile from ITS.

**Recurring:** **NO.** Primary symptom of legacy status.

**Consumers (14 routers + 6 pipelines):**
- `api/routers/leaderboard.py:104,127` — display + sort options `win_rate|alpha|buy_count|percentile` (registry red flag #4)
- `api/routers/insiders.py:42` — single-insider profile
- `api/routers/companies.py:64,166,260,366,380` (incl. `top_performer` chart filter — registry red flag #2)
- `api/routers/private_companies.py:149,160,262`
- `api/routers/signals.py` (sell_cessation display)
- `api/routers/dashboard.py:191,238,265,287`
- `api/routers/clusters.py:74,124,235,284,309`
- `api/routers/filings.py:154,287,364`
- `api/routers/search.py:54` `ORDER BY itr.score DESC`
- `api/routers/export.py:66,71`, `sitemap.py:56`
- `pipelines/notification_scanner.py:213,216,219` (HVF tier filter — registry red flag #3)
- `pipelines/generate_*.py` content scripts — COALESCE chain (registry red flag #1)
- `pipelines/insider_study/options_pull_longdte.py:65`
- `strategies/insider_catalog/{name_cleaner,lookup,data_quality}.py`

**PIT:** KNOWN VIOLATION for any ranking/scoring (registry #3-7, #18).

**Issues:**
- Stale 53 days; consumers query and get March 24 data.
- `companies.py:380` `score_tier=2` "top performer" gate silently filtering historical trades by stale tier.
- Migration plan: replace with PIT-anchored snapshot or migrate display columns to ITS-derived "best ever" (`api/pit_helpers.py:enrich_with_best_pit_grade` already provides this).

## P4.10 `insiders` (126,368 rows)

Identity table, CIK-keyed.

**Writers:** `backfill.get_or_create_insider` (399); `entity_resolution.py:128,136`; `name_cleaner.py:257,264`; `fetch_latest.py:290`; `backfill_live.py:782,1146`; `backfill.py:1053`

**Recurring:** YES — `fetch_latest._run_fetch_inner` upserts during every 5-min cycle; `name_cleaner.clean_name` cleans new names in same transaction. Last `updated_at = 2026-05-15 18:50`.

**Issues:** `is_entity` flag stale — `entity_resolution.py` is manual; new LLC insiders may be flagged `is_entity=0` until next manual run.

## P4.11 `insider_companies` (116,087 rows) — DATA QUALITY BUG

Per-(insider, ticker) trade aggregates.

| Column | Writer |
|---|---|
| `insider_id`, `ticker`, `company`, `title`, `trade_count`, `total_value`, `first_trade`, `last_trade` | `backfill._build_insider_companies` (958-984) DELETE + INSERT; `dedup_trades.py:199-201`; `price_validator.py:362-364`; `fix_malformed_tickers.py:110,152` (DELETE) |

**Recurring:** **Manual only.** Not in refresh chain.

**Last write proxy:** `last_trade = 2029-05-04` ← **DATA QUALITY BUG (future date).** Suggests >=1 malformed trade survived dedup. TODO: investigate.

**Consumers:**
- `strategies/insider_catalog/pit_scoring.py:511,517` — role + primary company lookup for `compute_insider_ticker_score` (**USED IN SCORING**)
- `lookup.py:143`
- API routers — display

**PIT:** Aggregates over ALL trades (not PIT). Used by `pit_scoring.py` for `role_at_ticker` + `is_primary_company` — both look at latest title/primary, which can drift over an insider's career. Marginal concern (role weight cap ±10%).

**Issues:**
- DELETE + INSERT means table briefly empty during rebuild — concurrent reads see zero rows.
- 2029 date.

## P4.12 `insider_groups` (1,340) / `insider_group_members` (2,855)

LLC/Trust clustering tables.

- Writer: `strategies/insider_catalog/entity_resolution.py:185,304,388,564` (INSERT)
- Recurring: **NO** — latest `created_at = 2026-03-14` (~2 months stale)
- Consumers: Display only on insider profile pages

## P4.13 `pull_status` (0 rows)

ThetaData options pull telemetry. Empty — options pipeline DORMANT (Part A #4).

- Writer: `pipelines/_lib/resumable_puller.py:94` — not called by any recurring job

## P4.14 `research.derivative_trades` (1,162,052 rows) — LEGACY

Per-derivative-transaction data. Superseded by `trades.is_derivative=1` filter.

**Writers:**
- `strategies/insider_catalog/backfill_live.py:insert_derivative_trades` (833-878) — called from `backfill_live.main()` at line 1079 during bulk
- `strategies/insider_catalog/backfill.py` (bulk equivalent)

**Recurring:** **NO from the 5-min fetch loop.** `fetch_latest.py` imports `insert_trades` (33-39) but NOT `insert_derivative_trades`. NEW derivative rows from live ingest are NOT being inserted.

**Consumers:** `api/routers/insiders.py:79` notional-pricing display caveat. Otherwise dormant.

**Issues:** Live ingest gap. Migration plan was to retire this table; not yet done.

## P4.15 Today's conviction drift attribution

The 2026-05-16 audit traced the silent failures to (one or more of):

1. `pit_cluster_size` NULL because `compute_pit_clusters.py` was orphaned (2026-04-07 → 2026-05-08).
2. `is_rare_reversal` NULL because `compute_switch_rate.py` was orphaned (2026-03-25 → 2026-05-16).
3. `career_grade` NULL because inline-only writer never fired from `insider-fetch` (2026-04-09 → 2026-05-16).
4. `pit_n_trades` / `pit_win_rate_7d` permanently NULL → content pipelines COALESCE'd to stale `insider_track_records` (always-on PIT violation, registry red flag #1).

(1), (2), (3) are now fixed via standalone scripts in the refresh chain (`compute_pit_clusters.py` PG rewrite, `compute_switch_rate.py` PG rewrite, `compute_career_grades.py` new file as step 4b). (4) remains.

## P4.16 Specific P0/P1 gaps to fix (complements Part 3 §3.3)

| Pri | Gap | Action |
|---|---|---|
| P0 | `compute_week52_proximity.py` is SQLite-only → orphan | Rewrite for PG; add to refresh chain; freshness contract |
| P0 | `trades.pit_n_trades` / `pit_win_rate_*` have NO writer → COALESCE to ITR = PIT violation | Either repopulate at ingest from ITS or remove + migrate 5 content scripts |
| P1 | `insider_track_records` stale 53d but 14 routers + 6 pipelines read it | Resurrect recurring writer OR migrate consumers to ITS-derived display columns (`pit_helpers.enrich_with_best_pit_grade` exists) |
| P1 | `transaction_classifier` orphan; `signal_quality`/`signal_category` not refreshed | Add to refresh chain OR retire + migrate `compute_signals.opportunistic_trade` to read `cohen_routine` directly |
| P1 | `insider_companies.last_trade=2029-05-04` data bug | Investigate malformed trade; rebuild table |
| P1 | `research.derivative_trades` not written by 5-min loop | Add `insert_derivative_trades` to `fetch_latest._run_fetch_inner` or retire table |
| P2 | `compute_signals.py` doesn't write signal_freshness for trade_signals | Add `write_freshness` after each detector |
| P2 | `entity_resolution.py` manual-only; new LLCs not auto-grouped (62 days stale) | Schedule weekly |
| P2 | `compute_career_grades.py` has no freshness contract entry | Add (30h, required_for: [quality_momentum]) |
| P2 | `score_history` grows unbounded (~1.7× ITS) | Define archival policy (last 90d or last 5 snapshots per (insider, ticker)) |
| P3 | `is_largest_ever`, `purchase_size_ratio`, `is_tax_sale`, `is_recurring` have no freshness contract | Add to YAML for completeness if any strategy starts filtering on them |

## P4.17 Verification SQL

```sql
-- Row counts and recency for scoring + signals tables
SELECT 'trades', COUNT(*), MAX(filing_date)::text FROM trades
UNION ALL SELECT 'insider_ticker_scores', COUNT(*), MAX(as_of_date)::text FROM insider_ticker_scores
UNION ALL SELECT 'score_history', COUNT(*), MAX(as_of_date)::text FROM score_history
UNION ALL SELECT 'trade_returns', COUNT(*), MAX(computed_at)::text FROM trade_returns
UNION ALL SELECT 'trade_signals', COUNT(*), MAX(computed_at)::text FROM trade_signals
UNION ALL SELECT 'trade_decision_audit', COUNT(*), MAX(ts)::text FROM trade_decision_audit
UNION ALL SELECT 'signal_freshness', COUNT(*), MAX(last_computed_at)::text FROM signal_freshness
UNION ALL SELECT 'insider_track_records', COUNT(*), MAX(computed_at) FROM insider_track_records
UNION ALL SELECT 'insider_companies', COUNT(*), MAX(last_trade)::text FROM insider_companies;

-- Per-column populated % (derived columns on trades)
SELECT
  ROUND(100.0 * COUNT(*) FILTER (WHERE pit_grade IS NOT NULL) / COUNT(*), 1) AS pct_pit_grade,
  ROUND(100.0 * COUNT(*) FILTER (WHERE career_grade IS NOT NULL) / COUNT(*), 1) AS pct_career_grade,
  ROUND(100.0 * COUNT(*) FILTER (WHERE pit_cluster_size IS NOT NULL) / COUNT(*), 1) AS pct_pit_cluster_size,
  ROUND(100.0 * COUNT(*) FILTER (WHERE is_rare_reversal=1) / COUNT(*), 4) AS pct_rare_reversal_true,
  ROUND(100.0 * COUNT(*) FILTER (WHERE consecutive_sells_before IS NOT NULL) / COUNT(*), 1) AS pct_consecutive_sells,
  ROUND(100.0 * COUNT(*) FILTER (WHERE pit_n_trades IS NOT NULL) / COUNT(*), 1) AS pct_pit_n_trades,
  ROUND(100.0 * COUNT(*) FILTER (WHERE week52_proximity IS NOT NULL) / COUNT(*), 1) AS pct_week52
FROM trades;

-- Find columns NOT covered by recent signal_freshness rows
SELECT column_name FROM information_schema.columns
WHERE table_name = 'trades' AND column_name NOT IN (
  SELECT DISTINCT column_name FROM signal_freshness
  WHERE table_name = 'trades' AND last_computed_at > NOW() - INTERVAL '30 days'
);

-- All signal_freshness writers in last 30d
SELECT source, table_name, column_name, MAX(last_computed_at) AS last_run,
       COUNT(*) AS runs_last_14d, populated_by
FROM signal_freshness
WHERE last_computed_at > NOW() - INTERVAL '14 days'
GROUP BY 1,2,3,6 ORDER BY MAX(last_computed_at) DESC;
```

End of Part 4.
