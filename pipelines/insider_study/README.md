# Insider Cluster Buy Event Study

Standalone event study — no framework dependency. Tests the hypothesis:
"Buy small/midcap stocks on SEC Form 4 cluster insider purchases (2+ insiders buying
within 30 days), hold N days. Measure cumulative abnormal return vs. SPY benchmark."

## Pipeline

1. `download_sec_bulk.py`    — **Fast**: Download pre-parsed SEC quarterly ZIPs (one per quarter)
2. `build_event_calendar.py` — Parse + filter events (P-code, cluster, size, routine/opportunistic)
3. `collect_prices.py`       — Download daily OHLCV for triggered tickers via Alpaca
4. `run_event_study.py`      — Run T+1 entry, hold N days, compute abnormal returns

`download_edgar_data.py` is the legacy per-filing XML downloader (slow, ~40hrs for full 2020-2025).
Use `download_sec_bulk.py` instead — it downloads 24 pre-parsed quarterly ZIPs in ~40 seconds.

## Data Status (2026-02-27)

- **Raw transactions**: `data/edgar_bulk_form4.csv` — 56,153 P-code purchases ≥ $50K (2020–2025)
- **Event calendar**: `data/events_bulk.csv` — 17,341 events (5,550 true clusters, 5,159 tickers)
- **Price data**: `data/prices/` — ~370 tickers collected (collection in progress for full set)

## Recommended Usage

```bash
# Step 1: Download SEC bulk data (24 quarterly ZIPs, ~40 seconds total)
python pipelines/insider_study/download_sec_bulk.py \
  --start 2020-Q1 --end 2025-Q4 \
  --min-value 50000 \
  --output pipelines/insider_study/data/edgar_bulk_form4.csv

# Step 2: Build filtered event calendar (no market cap filter — relies on Alpaca coverage)
python pipelines/insider_study/build_event_calendar.py \
  --input pipelines/insider_study/data/edgar_bulk_form4.csv \
  --format openinsider \
  --output pipelines/insider_study/data/events_bulk.csv \
  --min-value 50000 \
  --no-market-cap

# Step 3: Download daily prices for all triggered tickers via Alpaca (~20 min for 5K tickers)
python pipelines/insider_study/collect_prices.py \
  --events pipelines/insider_study/data/events_bulk.csv \
  --output-dir pipelines/insider_study/data/prices/ \
  --start 2019-01-01 --end 2026-03-01

# Step 4: Run event study across hold periods
python pipelines/insider_study/run_event_study.py \
  --events pipelines/insider_study/data/events_bulk.csv \
  --prices-dir pipelines/insider_study/data/prices/ \
  --hold-days 7 --output pipelines/insider_study/data/results_bulk_7d.csv

python pipelines/insider_study/run_event_study.py \
  --events pipelines/insider_study/data/events_bulk.csv \
  --prices-dir pipelines/insider_study/data/prices/ \
  --hold-days 21 --output pipelines/insider_study/data/results_bulk_21d.csv

python pipelines/insider_study/run_event_study.py \
  --events pipelines/insider_study/data/events_bulk.csv \
  --prices-dir pipelines/insider_study/data/prices/ \
  --hold-days 63 --output pipelines/insider_study/data/results_bulk_63d.csv
```

## Filters Applied (per academic literature)

| Filter | Rationale | Source |
|---|---|---|
| Transaction code = "P" only | Open-market purchases only | SEC Form 4 spec |
| Exclude 10b5-1 plan trades | Pre-planned = no signal | Cohen et al. 2012 |
| Min transaction size $50K | Token purchases noise | Industry standard |
| Market cap $100M-$2B | Alpha zone for insider signal | Lakonishok & Lee 2001 |
| Cluster: 2+ insiders, 7-day window | 2x abnormal return vs single | Alldredge 2019 |
| Routine filter (3+ consecutive years same month) | Near-zero alpha | Cohen et al. 2012 |
| Min 3 days of price history after filing | Data quality | Implementation |
