# Pull-universe SQL definitions

Each `.sql` here resolves to a list of tickers that scopes one of the bulk-data pulls. Run via `psql form4 -f <file>` on Studio. Tune date ranges and thresholds in the file as the pipeline matures — these are starting points, not contracts.

| File | Used by | Approx size |
|---|---|---|
| `insider_active.sql` | Phase 1 #3 — ThetaData EOD options full-universe expansion | ~5–6K tickers |
| `top_liquid_1000.sql` | Phase 1 #1 priority order; Phase 2 #7 TAQ event windows | 1,000 tickers |
| `top_options_500.sql` | Phase 2 #6 — 1-min options for liquid underlyings | 500 tickers |

All queries are date-windowed so re-running them is deterministic for a given as-of date.
