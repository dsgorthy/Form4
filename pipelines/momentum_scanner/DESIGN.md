# Momentum Scanner — DESIGN

**OBSERVATION-ONLY research tool. Places no orders.**

## Purpose
Replicate the candidate list of low-float momentum "runners" (the
[trademomentum.org](https://www.trademomentum.org/) / "Momentum" universe — Kev,
@trade.momentum) **without paying $147/mo**, and log candidates + their intraday
continuation-vs-fade path to build a point-in-time dataset that can honestly test
whether the long-breakout rule has any net-of-cost edge.

**Research stance (2026-06-16).** A multi-source, adversarially-verified literature
review **REJECTED** the premise that a naive long-only intraday micro-cap breakout has
positive net-of-cost expectancy for retail: end-of-day reversal is *stronger* for small
caps, the MAX/lottery effect concentrates in exactly this universe, and micro-cap spreads
(~5.6% listed → ~34% near delisting) dwarf any plausible breakout edge. This tool exists
to **observe and collect data**, not to assert tradeability. The candidate *scanner* is
replicable; the *edge* is unproven and contraindicated. See Claude memory
`project_2026-06-16_smallcap_momentum_research`.

## Data flow
```
Alpaca screener (movers + most-actives)            -> candidate symbols
  per symbol:
    snapshot        -> last / day-open / prev-close / prior-day-high
    1-min bars      -> premarket high, VWAP, EMA9/EMA90, round-number break
    daily bars      -> avg daily volume (RVOL denominator)
    Finnhub profile -> shares-outstanding (FLOAT PROXY)
  evaluate_candidate() (pure)                       -> scored CandidateEval
  filter (price/gap/RVOL/float) + rank by score
  -> data/scanner/{date}.jsonl  (dev)
  -> momentum_candidates + momentum_outcomes (Studio PG)  (prod)
```

## Components
| File | Role |
|------|------|
| `framework/data/alpaca_screener.py` | Candidate discovery (movers/most-actives). Additive — kept **separate** from the production `alpaca_client.py` on the live paper-trading path. |
| `framework/data/fundamentals.py` | Finnhub `/stock/profile2` shares-outstanding (float proxy). |
| `pipelines/momentum_scanner/config.py` | `ScannerConfig` — filters + scoring weights (all tunable via CLI). |
| `pipelines/momentum_scanner/signals.py` | Pure `evaluate_candidate()` scoring. **Unit-tested.** |
| `pipelines/momentum_scanner/scanner.py` | Orchestrator + CLI. |
| `tests/unit/test_momentum_scanner.py` | 11 tests (signal logic + payload parsing). |

## Signals & scoring (sum = 100)
above-VWAP 15 · above-EMA9 10 · above-EMA90 10 · broke-premarket-high 15 ·
broke-prior-day-high 15 · gap-magnitude 15 (capped 30%) · RVOL 15 (capped 5x) ·
round-number break 5.

EMA periods are **parameterized** (`ema_fast=9`, `ema_slow=90`). Kev runs the **90 EMA**
on the 1-min as a trend/context line; 9 is the canonical fast trigger. The literature gives
**no special status to any single MA period** (period choice is the textbook data-snooping
trap), so neither is treated as gospel — the score leans on *confluence*, not one line.

## Data sources & known limits
- **Alpaca feed:** `iex` (free; thin premarket on sub-$1 names) vs `sip` (paid, full
  consolidated tape). Default `iex`; SIP is the upgrade for real premarket coverage.
- **Alpaca movers endpoint** historically restricts to **price ≥ $1** + min volume → can
  **miss deep sub-$1 names** (e.g. OBAI in the $0.40s). Mitigation (later stage): a
  price-filtered universe scan via the Trading API `/v2/assets`.
- **Float:** Finnhub `shareOutstanding` is **total** shares (millions) → over-estimates
  tradable float, so a `float<Xm` filter using it is *conservative*. True float via FMP
  `/v4/shares_float` (free tier) or a paid provider — swap behind `shares_outstanding_m`.

## Run location (dev → deploy)
- **Mini:** build + unit tests only. No `form4` DB; the Mini's Alpaca data creds currently
  401 (stale/rotated — production lives on Studio).
- **Studio:** live scanning + `form4` PG + launchd loop, where the working creds live.
  `com.openclaw.momentum-scanner` (**Studio-only**, add to `studio` guard list), market-hours
  incl premarket, ~5-min cadence. Wrap entry point in `framework.observability.pipeline_run`.

## Validation
During the 7-day trademomentum.org trial, compare our candidate list to Kev's nightly **3–5**
watchlist (precision/recall) to confirm replication.

## Open / unevaluated (flagged by the research)
- **LULD halt frequency** on gap>10% low-float names — decisive for fills, not yet sourced.
- **Round-number / $1 clustering** — no confirmed empirical support; treat as folklore until proven.
- **The decision-critical question** — does the *exact* rule clear costs? — is answerable only
  by the `momentum_outcomes` dataset this tool collects. That is the point of building it.

## Staging
1. Resolve live data + float creds (Mini stale / Studio has them).
2. ✅ Discovery + float modules.
3. ✅ Scanner core + DESIGN + tests.
4. Candidate + outcome logging (PG schema + migration `pipelines/migrations/`).
5. Studio launchd loop + `pipeline_run` observability.
6. Dashboard (`/api/v1/momentum-candidates` + `/app/momentum-screener`) + validate vs Kev.
