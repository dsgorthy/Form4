# PIT / career grade research harness

Run on Studio (needs the `form4` DB):

```bash
python3 research.py load         # cache the 371k-filing universe to /tmp
python3 research.py population   # production formula, predictor population varied
python3 research.py selectivity  # matched top-k% comparison -- the gate
python3 research.py alist        # A-List admission impact
```

**PIT guarantee.** Every predictor is built only from filings that were both
KNOWN (`filing_date <= as_of`) and whose forward return was OBSERVABLE
(`trade_date <= as_of - lag`, lag 10/40/100 for 7d/30d/90d) at the moment being
scored. Same two guards the production scorer uses, same lags, same
`abnormal_*` columns, one row per filing before anything is counted.

**Dev/holdout split.** Every design choice was made on 2016-2022. 2023-2026 was
not looked at until the choices were locked, and nothing was tuned on it.

Findings and the verdict: `docs/pit_grade_research.md`. Read it before acting
on any of this -- the headline result is negative.
