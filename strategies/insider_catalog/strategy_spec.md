# Follow Proven Insiders — Strategy Specification

**Version:** 1.0.0
**Date:** 2026-03-07
**Type:** Event-driven, variable hold (7d/90d), shares + options
**Validation:** Walk-forward (train first 75% of each insider's trades, test last 25%)

---

## 1. Hypothesis

Insiders who have historically generated abnormal returns after their purchases will continue to do so. By scoring every insider's track record across multiple time horizons (7d, 30d, 90d) and trading alongside only the proven ones, we capture consistent alpha with higher frequency than cluster-only signals.

**Evidence (out-of-sample test period):**
- Qualified insiders: WR 59.9%, median alpha +2.49%, Sharpe 0.46
- Unqualified insiders: WR 51.8%, Sharpe 0.16
- Selection produces 3x better risk-adjusted returns

## 2. How It Runs Alongside Cluster Buy

```
EDGAR RSS Feed (every 10 min)
  │
  ├─ Form 4 filed
  │   │
  │   ├─► Cluster Buy check (existing: 2+ insiders, $5M+, C-suite)
  │   │     └─► Enriched with insider track records from catalog
  │   │
  │   └─► Solo Insider check (NEW: single proven insider)
  │         └─► Shares leg (7d-best) OR Options leg (90d-best)
  │
  └─ Both strategies share the same EDGAR monitor, same Alpaca account,
     independent position tracking
```

**No conflicts:** Cluster buy averages ~34 trades/year. Solo follow adds ~200 trades/year. With position sizing, they share the same $30K portfolio without crowding.

## 3. Entry Rules

### 3a. Shares Leg (7d-best insiders)

| Rule | Threshold |
|------|-----------|
| Insider catalog score tier | >= 2 (top 20%) |
| Best window | 7d |
| Min prior buys | >= 5 with return data |
| Train WR (7d) | >= 55% |
| Train avg abnormal (7d) | > 0% |
| Current trade value | >= $100,000 |
| Entry | T+1 open after Form 4 filing |
| Hold | 7 trading days |
| Stop-loss | -15% from entry |

**Expected:** WR 63.5%, median return +2.95%, Sharpe 0.93 (OOS)
**Signal pool:** 415 insiders, ~160 trades/year

### 3b. Options Leg (90d-best insiders)

| Rule | Threshold |
|------|-----------|
| Insider catalog score tier | >= 2 (top 20%) |
| Best window | 90d |
| Min prior buys | >= 5 with return data |
| Train WR (90d) | >= 55% |
| Train avg abnormal (90d) | > 0% |
| Current trade value | >= $100,000 |
| Entry | Buy 5% OTM call, 90-120 DTE, at T+1 |
| Hold | 60 trading days or profit target |
| Profit target | +100% on option premium |
| Max loss | Premium paid (no stop needed) |

**Expected:** WR 65.3%, 48% of trades >+10% underlying move, 32% >+20%
**Signal pool:** 55 insiders, ~40 trades/year

### 3c. 30d Window — EXCLUDED

30d window showed only 55.2% WR and 0.41 Sharpe in testing. Not viable for either shares (too thin) or options (not enough move for premium). Excluded from v1.

## 4. Risk Controls

| Control | Rule |
|---------|------|
| Max concurrent (shares) | 5 positions |
| Max concurrent (options) | 3 positions |
| Position sizing (shares) | 3% of portfolio per trade |
| Position sizing (options) | 1% of portfolio per trade (premium) |
| Circuit breaker | Halt if 30-day rolling DD > 8% |
| VIX regime | Reduce shares to 2%, skip options when VIX > 30 |
| Overlap check | If insider also triggers a cluster buy, cluster takes priority (don't double up) |
| Max sector | Max 3 concurrent in same GICS sector |
| Catalog staleness | Re-score catalog weekly; if DB > 14 days old, halt new entries |

## 5. Walk-Forward Results

### Shares (7d-best, N=415 insiders)

| Metric | Train | Test | Delta |
|--------|-------|------|-------|
| N trades | ~7,500 | ~2,600 | |
| WR | 73.6% | 63.5% | -10.1pp |
| Avg alpha | +11.6% | +3.9% | -7.7pp |
| Median alpha | +5.8% | +2.5% | |
| Sharpe | 0.62 | 0.93 | +0.31 |

Sharpe *improves* OOS — the N-adjusted scoring successfully filters for insiders whose edge is real, not noise.

### Options (90d-best, N=55 insiders)

| Metric | Test |
|--------|------|
| N trades | 349 |
| WR (absolute) | 65.3% |
| Avg return | +16.9% |
| Median return | +8.1% |
| >+5% return | 55.3% |
| >+10% return | 45.6% |
| >+20% return | 32.1% |
| <-15% return | 8.3% |

With 48% of trades moving >+10%, a 5% OTM call with 90 DTE is profitable if the option captures ~50% of the underlying move (conservative delta assumption).

## 6. Combined Portfolio Simulation

On a $30K portfolio:
- Shares: 3% sizing = $900/trade, max 5 = $4,500 exposure
- Options: 1% sizing = $300/trade (premium), max 3 = $900 exposure
- Cluster buy (existing): 5% sizing = $1,500/trade, max 3 = $4,500 exposure
- **Total max exposure:** ~$10K shares + $900 options premium = ~37% of portfolio

All three strategies combined use <40% of capital at peak, leaving ample cash buffer.

## 7. Implementation Plan

### Phase 1: Integration (shares only)
- Wire `check_solo_trigger()` into `paper_runner.py`
- Add solo signal queue separate from cluster queue
- Track solo trades independently in state.json
- Deploy to paper trading alongside cluster buy

### Phase 2: Options leg
- Extend `options_leg.py` to support 90d hold + 60d exit
- Query Alpaca `/v2/options/contracts` for 90-120 DTE 5% OTM calls
- Submit via existing Alpaca paper account (Level 3 options approved)
- Track options trades in solo_insider_signals table

### Phase 3: Catalog maintenance
- Weekly cron: re-run `compute_returns.py` + `backfill.py --refresh-scores`
- Ingest new Form 4 filings into catalog (not just cluster-qualifying ones)
- Monthly: extend price data and re-validate scores

## 8. Kill Criteria

| Criteria | Threshold | Action |
|----------|-----------|--------|
| Shares OOS Sharpe | < 0.4 after 6 months | Pause shares leg |
| Options OOS WR | < 50% after 30 trades | Pause options leg |
| Combined DD | > 10% rolling 30-day | Circuit breaker halts all |
| Catalog drift | Tier 2+ WR drops below 55% on new trades | Re-score and investigate |
