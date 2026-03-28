# Archived Strategies

Last updated: 2026-03-01

| Strategy | Archived | Category | Sharpe (net) | One-Line Summary | Revival? |
|----------|----------|----------|-------------|------------------|----------|
| spy_0dte_reversal | 2026-03-01 | RETURNED | 0.50 | 0DTE EOD mean-reversion; fees consume 78% of edge at $30K | At $150K+ capital |
| spy_noon_break | 2026-03-01 | RETURNED | 0.28 | Noon range breakout; $16 net P&L across 237 trades | At $150K+ capital |
| spy_orb | 2026-03-01 | RETURNED | -0.18 | Opening range breakout; no edge detected (41% WR) | With stronger filters |
| spy_first30_momentum | 2026-03-01 | RETURNED | -0.97 | Academic first-30-min momentum; loses money | With VIX regime filter |
| spy_vwap_trend | 2026-03-01 | RETURNED | -0.62 | VWAP deviation trend; QQQ result does not transfer to SPY | On QQQ with more data |
| spy_vwap_reclaim | 2026-03-01 | DEPRECATED | N/A | VWAP reclaim bounce; superseded by spy_vwap_trend | Unlikely |
| spy_pm_continuation | 2026-03-01 | ABANDONED | N/A | Empty scaffold; never implemented | N/A |

---

## Archive Process

To archive a strategy:
1. Move its directory from `strategies/{name}/` to `strategies/archive/{name}/`
2. Create an `ARCHIVE.md` manifest using the template in `research/templates/archive_manifest.md`
3. Update this index file
4. Update `README.md` strategy table and `PROGRESS.md`
5. Check for hardcoded imports in `pipelines/` and update if needed

To revive a strategy:
1. Copy its directory from `strategies/archive/{name}/` back to `strategies/{name}/`
2. Remove `ARCHIVE.md` from the active directory
3. Update this index (mark as "Revived {date}" or remove entry)
4. Re-run Board of Personas evaluation
