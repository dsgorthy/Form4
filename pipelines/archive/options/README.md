# Options research — archived 2026-08-20

These read `prices.option_prices`, which no longer exists.

ThetaData was cancelled 2026-06-07, the pull went dormant 2026-04-09, and
options performance was removed from the product on 2026-08-13. The table was
23.5M rows and 7.3 GB — 29% of the database — with 196 index scans in the nine
days before it was dropped.

The scripts are kept because the research they encode (premium selling, DTE
sweeps, the grid searches) is real work and the methodology is reusable if
options ever come back. They will not run as-is.

**To revive:** restore the tables from a nightly dump, then re-point the pull
at a live data subscription.

```
pg_restore -d form4 -n prices -t option_prices \
  /Users/derekg/backups/postgres/form4_YYYYMMDD_HHMMSS.dump
```

Note the dumps rotate on RETENTION_DAYS (default 7). If this data matters
long-term, copy one outside the rotation.

See `migrations/2026-08-20_deprecate_options_data.sql`.
