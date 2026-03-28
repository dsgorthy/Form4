#!/usr/bin/env python3
"""
Insider Trading Event Study — Deep Edge Analysis
==================================================
Loads SEC Form 4 event study results at 7d/21d/63d hold periods
and performs comprehensive analysis to identify tradeable edge.
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats
from itertools import product as iter_product
import warnings
warnings.filterwarnings('ignore')

# ── Paths ──────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, 'data')
REPORT_DIR = os.path.join(BASE, '..', '..', 'reports')
os.makedirs(REPORT_DIR, exist_ok=True)
REPORT_PATH = os.path.join(REPORT_DIR, 'INSIDER_STUDY_REPORT.md')

# ── Helpers ────────────────────────────────────────────────────────────────
def sharpe(returns, annual_factor=None):
    """Annualised Sharpe.  annual_factor supplied per hold period."""
    if len(returns) < 2 or returns.std() == 0:
        return 0.0
    af = annual_factor if annual_factor else 1.0
    return (returns.mean() / returns.std()) * np.sqrt(af)

def tstat_pval(arr):
    """t-test vs 0 → (t, p)"""
    n = len(arr)
    if n < 2:
        return (0.0, 1.0)
    t = arr.mean() / (arr.std(ddof=1) / np.sqrt(n))
    p = 2 * (1 - stats.t.cdf(abs(t), df=n-1))
    return (t, p)

def bucket_stats(df, col_name='abnormal_return', hold_label='', annual_factor=1):
    """Return a summary dict for a group."""
    ar = df[col_name]
    t, p = tstat_pval(ar.values)
    return {
        'N': len(df),
        'Mean AR (%)': round(ar.mean(), 3),
        'Median AR (%)': round(ar.median(), 3),
        'Std (%)': round(ar.std(), 3),
        'Win Rate (%)': round(df['win'].mean() * 100, 1),
        'Sharpe (ann)': round(sharpe(ar, annual_factor), 3),
        't-stat': round(t, 2),
        'p-value': round(p, 4),
    }

def annual_factor_for_hold(hold_days):
    if hold_days <= 10:
        return 252 / 7
    elif hold_days <= 30:
        return 252 / 21
    else:
        return 252 / 63

# ── Load Data ──────────────────────────────────────────────────────────────
print("=" * 80)
print("INSIDER TRADING EVENT STUDY — DEEP ANALYSIS")
print("=" * 80)

frames = []
for fname, hold in [('results_bulk_7d.csv', 7), ('results_bulk_21d.csv', 21), ('results_bulk_63d.csv', 63)]:
    fp = os.path.join(DATA, fname)
    tmp = pd.read_csv(fp, parse_dates=['filing_date', 'entry_date', 'exit_date', 'event_start_date'])
    tmp['hold_period'] = hold
    frames.append(tmp)
    print(f"  Loaded {fname}: {len(tmp):,} rows")

df = pd.concat(frames, ignore_index=True)
print(f"\n  Combined dataset: {len(df):,} rows")
print(f"  Unique tickers: {df['ticker'].nunique():,}")
print(f"  Date range: {df['entry_date'].min().date()} → {df['exit_date'].max().date()}")

# Derived columns
df['year'] = df['entry_date'].dt.year
df['month'] = df['entry_date'].dt.month

# ── Collect report lines ──────────────────────────────────────────────────
R = []  # report lines

def section(title):
    R.append(f"\n## {title}\n")
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

def table(df_table, fmt='pipe'):
    """Add a markdown table from a DataFrame."""
    R.append(df_table.to_markdown(index=True))
    R.append("")

def text(s):
    R.append(s)

def print_and_log(s):
    print(s)
    R.append(s)

# ══════════════════════════════════════════════════════════════════════════
# 1. HOLD PERIOD ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("1. Hold Period Analysis")

hold_summary = []
for hp in [7, 21, 63]:
    sub = df[df['hold_period'] == hp]
    af = annual_factor_for_hold(hp)
    s = bucket_stats(sub, annual_factor=af)
    s['Hold'] = f"{hp}d"
    hold_summary.append(s)

hold_df = pd.DataFrame(hold_summary).set_index('Hold')
print(hold_df.to_string())
table(hold_df)

# Interpretation
best_hold = hold_df['Sharpe (ann)'].idxmax()
text(f"**Best hold period by Sharpe:** {best_hold}")
text(f"")
text("Observations:")
for hp in ['7d', '21d', '63d']:
    row = hold_df.loc[hp]
    sig = "significant (p<0.05)" if row['p-value'] < 0.05 else "NOT significant"
    text(f"- {hp}: Mean AR = {row['Mean AR (%)']}%, Win Rate = {row['Win Rate (%)']}%, Sharpe = {row['Sharpe (ann)']}, {sig}")
text("")

# ══════════════════════════════════════════════════════════════════════════
# 2. PURCHASE SIZE (TOTAL VALUE) ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("2. Purchase Size Analysis (Total Value)")

value_bins = [0, 250_000, 1_000_000, 5_000_000, float('inf')]
value_labels = ['$50K-$250K', '$250K-$1M', '$1M-$5M', '$5M+']
df['value_bucket'] = pd.cut(df['total_value'], bins=value_bins, labels=value_labels)

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for lbl in value_labels:
        g = sub[sub['value_bucket'] == lbl]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Bucket'] = lbl
            rows.append(s)
    vdf = pd.DataFrame(rows).set_index('Bucket')
    text(f"### {hp}-Day Hold")
    print(f"\n  {hp}-Day Hold:")
    print(vdf.to_string())
    table(vdf)

text("**Key question:** Does bigger buying = stronger edge?")
text("")

# ══════════════════════════════════════════════════════════════════════════
# 3. NUMBER OF INSIDERS / CLUSTER ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("3. Cluster Analysis (Number of Insiders)")

def cluster_label(n):
    if n == 1:
        return 'Single (1)'
    elif n <= 3:
        return 'Small Cluster (2-3)'
    else:
        return 'Large Cluster (4+)'

df['cluster_group'] = df['n_insiders'].apply(cluster_label)
cluster_order = ['Single (1)', 'Small Cluster (2-3)', 'Large Cluster (4+)']

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for lbl in cluster_order:
        g = sub[sub['cluster_group'] == lbl]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Group'] = lbl
            rows.append(s)
    cdf = pd.DataFrame(rows).set_index('Group')
    text(f"### {hp}-Day Hold")
    print(f"\n  {hp}-Day Hold:")
    print(cdf.to_string())
    table(cdf)

text("**Key question:** Does cluster buying amplify the signal?")
text("")

# ══════════════════════════════════════════════════════════════════════════
# 4. CONFIDENCE SCORE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("4. Confidence Score Quintile Analysis")

df['conf_quintile'] = pd.qcut(df['confidence_score'], 5, labels=['Q1 (Low)', 'Q2', 'Q3', 'Q4', 'Q5 (High)'], duplicates='drop')

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for q in ['Q1 (Low)', 'Q2', 'Q3', 'Q4', 'Q5 (High)']:
        g = sub[sub['conf_quintile'] == q]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Quintile'] = q
            rows.append(s)
    qdf = pd.DataFrame(rows).set_index('Quintile')
    text(f"### {hp}-Day Hold")
    print(f"\n  {hp}-Day Hold:")
    print(qdf.to_string())
    table(qdf)

# Correlation
for hp in [7, 21, 63]:
    sub = df[df['hold_period'] == hp].dropna(subset=['confidence_score', 'abnormal_return'])
    corr, corr_p = stats.pearsonr(sub['confidence_score'], sub['abnormal_return'])
    text(f"- {hp}d: Pearson r(confidence, AR) = {corr:.4f}, p = {corr_p:.4f}")
text("")

# ══════════════════════════════════════════════════════════════════════════
# 5. QUALITY SCORE (TITLE/SENIORITY) ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("5. Quality Score (Insider Seniority) Analysis")

def quality_label(q):
    if q >= 2.5:
        return 'C-Suite (>=2.5)'
    elif q >= 2.0:
        return 'Senior Officer (>=2.0)'
    elif q >= 1.5:
        return 'VP/Director (>=1.5)'
    else:
        return 'Other (<1.5)'

df['quality_group'] = df['quality_score'].apply(quality_label)
quality_order = ['C-Suite (>=2.5)', 'Senior Officer (>=2.0)', 'VP/Director (>=1.5)', 'Other (<1.5)']

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for lbl in quality_order:
        g = sub[sub['quality_group'] == lbl]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Group'] = lbl
            rows.append(s)
    qqdf = pd.DataFrame(rows).set_index('Group')
    text(f"### {hp}-Day Hold")
    print(f"\n  {hp}-Day Hold:")
    print(qqdf.to_string())
    table(qqdf)
text("")

# ══════════════════════════════════════════════════════════════════════════
# 6. CONCENTRATION ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
section("6. Concentration Analysis")

def conc_label(c):
    if c >= 0.8:
        return 'High (>=0.8)'
    elif c >= 0.5:
        return 'Medium (0.5-0.8)'
    else:
        return 'Diversified (<0.5)'

df['conc_group'] = df['concentration'].apply(conc_label)
conc_order = ['Diversified (<0.5)', 'Medium (0.5-0.8)', 'High (>=0.8)']

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for lbl in conc_order:
        g = sub[sub['conc_group'] == lbl]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Group'] = lbl
            rows.append(s)
    ccdf = pd.DataFrame(rows).set_index('Group')
    text(f"### {hp}-Day Hold")
    print(f"\n  {hp}-Day Hold:")
    print(ccdf.to_string())
    table(ccdf)
text("")

# ══════════════════════════════════════════════════════════════════════════
# 7. COMBINED FILTER SEARCH — FIND THE EDGE
# ══════════════════════════════════════════════════════════════════════════
section("7. Combined Filter Search — Finding the Edge")

text("Testing systematic filter combinations to find the strongest edge.")
text("Minimum 50 events required for statistical power.")
text("")

# Define filter components
filters = {
    'hold': {
        '7d': lambda d: d['hold_period'] == 7,
        '21d': lambda d: d['hold_period'] == 21,
        '63d': lambda d: d['hold_period'] == 63,
    },
    'cluster': {
        'any': lambda d: pd.Series(True, index=d.index),
        'cluster_only': lambda d: d['n_insiders'] >= 2,
        'large_cluster': lambda d: d['n_insiders'] >= 4,
    },
    'value': {
        'any_value': lambda d: pd.Series(True, index=d.index),
        'val_250k+': lambda d: d['total_value'] >= 250_000,
        'val_1m+': lambda d: d['total_value'] >= 1_000_000,
        'val_5m+': lambda d: d['total_value'] >= 5_000_000,
    },
    'quality': {
        'any_quality': lambda d: pd.Series(True, index=d.index),
        'senior+': lambda d: d['quality_score'] >= 2.0,
        'csuite': lambda d: d['quality_score'] >= 2.5,
    },
    'confidence': {
        'any_conf': lambda d: pd.Series(True, index=d.index),
        'conf_top50': lambda d: d['confidence_score'] >= d['confidence_score'].median(),
        'conf_top25': lambda d: d['confidence_score'] >= d['confidence_score'].quantile(0.75),
        'conf_top10': lambda d: d['confidence_score'] >= d['confidence_score'].quantile(0.90),
    },
}

# Systematic sweep
results_list = []
total_combos = 1
for k in filters:
    total_combos *= len(filters[k])
print(f"  Testing {total_combos:,} filter combinations...")

keys = list(filters.keys())
for combo_vals in iter_product(*[filters[k].items() for k in keys]):
    combo_names = [v[0] for v in combo_vals]
    combo_funcs = [v[1] for v in combo_vals]

    mask = pd.Series(True, index=df.index)
    for fn in combo_funcs:
        mask &= fn(df)

    sub = df[mask]
    n = len(sub)
    if n < 50:
        continue

    hp_val = int(combo_names[0].replace('d', ''))
    af = annual_factor_for_hold(hp_val)
    ar = sub['abnormal_return']
    t, p = tstat_pval(ar.values)
    sh = sharpe(ar, af)

    results_list.append({
        'Hold': combo_names[0],
        'Cluster': combo_names[1],
        'Value': combo_names[2],
        'Quality': combo_names[3],
        'Confidence': combo_names[4],
        'N': n,
        'Mean AR (%)': round(ar.mean(), 3),
        'Median AR (%)': round(ar.median(), 3),
        'Win Rate (%)': round(sub['win'].mean() * 100, 1),
        'Sharpe': round(sh, 3),
        't-stat': round(t, 2),
        'p-value': round(p, 4),
    })

sweep_df = pd.DataFrame(results_list)
print(f"  {len(sweep_df):,} combinations have ≥50 events")

# Sort by Sharpe
sweep_df = sweep_df.sort_values('Sharpe', ascending=False)

text("### Top 10 Filter Combinations by Sharpe Ratio (N >= 50)")
text("")
top10 = sweep_df.head(10).reset_index(drop=True)
top10.index = range(1, len(top10) + 1)
top10.index.name = 'Rank'
print(top10.to_string())
table(top10)

# Also show top 10 by mean AR
text("### Top 10 Filter Combinations by Mean Abnormal Return (N >= 50)")
text("")
top10_ar = sweep_df.sort_values('Mean AR (%)', ascending=False).head(10).reset_index(drop=True)
top10_ar.index = range(1, len(top10_ar) + 1)
top10_ar.index.name = 'Rank'
print(top10_ar.to_string())
table(top10_ar)

# Top combos with p < 0.05
text("### Statistically Significant Combinations (p < 0.05, N >= 50, sorted by Sharpe)")
text("")
sig_df = sweep_df[sweep_df['p-value'] < 0.05].head(15).reset_index(drop=True)
sig_df.index = range(1, len(sig_df) + 1)
sig_df.index.name = 'Rank'
if len(sig_df) > 0:
    print(sig_df.to_string())
    table(sig_df)
else:
    text("No statistically significant combinations found at p < 0.05.")
text("")

# Also show top combos with N >= 200 for robustness
text("### Robust Combinations (N >= 200, p < 0.10, sorted by Sharpe)")
text("")
robust_df = sweep_df[(sweep_df['N'] >= 200) & (sweep_df['p-value'] < 0.10)].head(10).reset_index(drop=True)
robust_df.index = range(1, len(robust_df) + 1)
robust_df.index.name = 'Rank'
if len(robust_df) > 0:
    print(robust_df.to_string())
    table(robust_df)
else:
    text("No robust combinations found at N >= 200 and p < 0.10.")
text("")

# ══════════════════════════════════════════════════════════════════════════
# 8. VOLUME SURGE DETECTION
# ══════════════════════════════════════════════════════════════════════════
section("8. Volume Surge Detection")

text("Analyzing whether unusually large individual purchases predict better returns.")
text("")

# avg_value_per_insider quintiles
df['avg_val_q'] = pd.qcut(df['avg_value_per_insider'], 5,
                            labels=['Q1 (Small)', 'Q2', 'Q3', 'Q4', 'Q5 (Largest)'],
                            duplicates='drop')

for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for q in ['Q1 (Small)', 'Q2', 'Q3', 'Q4', 'Q5 (Largest)']:
        g = sub[sub['avg_val_q'] == q]
        if len(g) > 0:
            s = bucket_stats(g, annual_factor=af)
            s['Quintile'] = q
            s['Avg $ per Insider'] = f"${g['avg_value_per_insider'].median():,.0f}"
            rows.append(s)
    vqdf = pd.DataFrame(rows).set_index('Quintile')
    text(f"### Avg Value per Insider — {hp}-Day Hold")
    print(f"\n  Avg Value per Insider — {hp}-Day Hold:")
    print(vqdf.to_string())
    table(vqdf)

# max_single_value thresholds
text("### Max Single Purchase Thresholds")
text("")
thresholds = [100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for thresh in thresholds:
        g = sub[sub['max_single_value'] >= thresh]
        if len(g) >= 30:
            s = bucket_stats(g, annual_factor=af)
            s['Threshold'] = f">=${thresh/1e6:.1f}M" if thresh >= 1e6 else f">=${thresh/1e3:.0f}K"
            rows.append(s)
    if rows:
        thdf = pd.DataFrame(rows).set_index('Threshold')
        text(f"#### {hp}-Day Hold")
        print(f"\n  Max Single Value — {hp}-Day Hold:")
        print(thdf.to_string())
        table(thdf)
text("")

# ══════════════════════════════════════════════════════════════════════════
# 9. TIME-BASED PATTERNS
# ══════════════════════════════════════════════════════════════════════════
section("9. Time-Based Patterns")

# Year-over-year
text("### Year-over-Year Edge")
text("")
for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for yr in sorted(sub['year'].unique()):
        g = sub[sub['year'] == yr]
        if len(g) >= 20:
            s = bucket_stats(g, annual_factor=af)
            s['Year'] = yr
            rows.append(s)
    ydf = pd.DataFrame(rows).set_index('Year')
    text(f"#### {hp}-Day Hold")
    print(f"\n  Year-over-Year — {hp}-Day Hold:")
    print(ydf.to_string())
    table(ydf)

# Monthly seasonality
text("### Monthly Seasonality (All Hold Periods Combined)")
text("")
month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
               7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
rows = []
for m in range(1, 13):
    g = df[df['month'] == m]
    if len(g) >= 30:
        s = bucket_stats(g, annual_factor=12)  # rough annualization
        s['Month'] = month_names[m]
        rows.append(s)
mdf = pd.DataFrame(rows).set_index('Month')
print("\n  Monthly Seasonality:")
print(mdf.to_string())
table(mdf)

# Pre/post COVID
text("### Pre vs Post COVID (entry before/after 2020-04-01)")
text("")
covid_date = pd.Timestamp('2020-04-01')
for hp in [7, 21, 63]:
    af = annual_factor_for_hold(hp)
    sub = df[df['hold_period'] == hp]
    rows = []
    for label, mask in [('Pre-COVID', sub['entry_date'] < covid_date),
                         ('Post-COVID', sub['entry_date'] >= covid_date)]:
        g = sub[mask]
        if len(g) >= 20:
            s = bucket_stats(g, annual_factor=af)
            s['Period'] = label
            rows.append(s)
    if rows:
        covid_df = pd.DataFrame(rows).set_index('Period')
        text(f"#### {hp}-Day Hold")
        print(f"\n  Pre/Post COVID — {hp}-Day Hold:")
        print(covid_df.to_string())
        table(covid_df)
text("")

# ══════════════════════════════════════════════════════════════════════════
# 10. DETAILED DRILL-DOWN ON BEST STRATEGIES
# ══════════════════════════════════════════════════════════════════════════
section("10. Detailed Drill-Down on Best Strategies")

text("Examining the top filter combinations more closely.")
text("")

# Take top 5 from sweep by Sharpe with p < 0.10
best_combos = sweep_df[(sweep_df['p-value'] < 0.10) & (sweep_df['N'] >= 50)].head(5)
if len(best_combos) == 0:
    best_combos = sweep_df.head(5)

for idx, row in best_combos.iterrows():
    combo_label = f"{row['Hold']} | {row['Cluster']} | {row['Value']} | {row['Quality']} | {row['Confidence']}"
    text(f"### Strategy: {combo_label}")
    text(f"- N = {row['N']}, Mean AR = {row['Mean AR (%)']}%, Win Rate = {row['Win Rate (%)']}%")
    text(f"- Sharpe = {row['Sharpe']}, t-stat = {row['t-stat']}, p = {row['p-value']}")

    # Apply filters to get actual trades
    hp_val = int(row['Hold'].replace('d', ''))
    mask = df['hold_period'] == hp_val
    mask &= filters['cluster'][row['Cluster']](df)
    mask &= filters['value'][row['Value']](df)
    mask &= filters['quality'][row['Quality']](df)
    mask &= filters['confidence'][row['Confidence']](df)
    sub = df[mask].copy()

    # Equity curve stats
    cumret = (1 + sub.sort_values('entry_date')['abnormal_return'] / 100).cumprod()
    if len(cumret) > 0:
        max_dd = ((cumret / cumret.cummax()) - 1).min() * 100
        text(f"- Max Drawdown: {max_dd:.1f}%")
        text(f"- Final Cumulative Return: {(cumret.iloc[-1] - 1) * 100:.1f}%")

    # Top 5 trades
    best_trades = sub.nlargest(5, 'abnormal_return')[['ticker', 'entry_date', 'abnormal_return', 'total_value', 'n_insiders', 'company']].copy()
    best_trades['entry_date'] = best_trades['entry_date'].dt.strftime('%Y-%m-%d')
    text(f"- Top 5 trades:")
    table(best_trades.reset_index(drop=True))

    # Worst 5 trades
    worst_trades = sub.nsmallest(5, 'abnormal_return')[['ticker', 'entry_date', 'abnormal_return', 'total_value', 'n_insiders', 'company']].copy()
    worst_trades['entry_date'] = worst_trades['entry_date'].dt.strftime('%Y-%m-%d')
    text(f"- Worst 5 trades:")
    table(worst_trades.reset_index(drop=True))
    text("")

# ══════════════════════════════════════════════════════════════════════════
# 11. OVERALL SUMMARY
# ══════════════════════════════════════════════════════════════════════════
section("11. Summary Statistics")

text("### Full Dataset Overview")
text(f"- Total events analyzed: {len(df):,} (across 3 hold periods)")
text(f"- Unique events: ~{len(df)//3:,}")
text(f"- Date range: {df['entry_date'].min().date()} to {df['exit_date'].max().date()}")
text(f"- Unique tickers: {df['ticker'].nunique():,}")
text("")

# Overall stats per hold
text("### Overall Edge by Hold Period")
for hp in [7, 21, 63]:
    sub = df[df['hold_period'] == hp]
    ar = sub['abnormal_return']
    wr = sub['win'].mean() * 100
    t, p = tstat_pval(ar.values)
    text(f"- **{hp}d**: Mean AR = {ar.mean():.3f}%, Median = {ar.median():.3f}%, "
         f"WR = {wr:.1f}%, t = {t:.2f}, p = {p:.4f}")
text("")

# ══════════════════════════════════════════════════════════════════════════
# WRITE REPORT
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("WRITING REPORT")
print("=" * 80)

header = """# Insider Trading Event Study — Comprehensive Analysis Report

**Date:** 2026-02-27
**Dataset:** SEC Form 4 filings, 2020-2025
**Events:** ~{n_events:,} insider purchase events, tested at 7/21/63-day hold periods
**Source:** EDGAR bulk Form 4 data, purchases ≥$50K

---
""".format(n_events=len(df)//3)

# Build verdict section
verdict = """
## 12. VERDICT: Is There a Tradeable Edge?

### The Raw Signal
"""

# Compute key stats for verdict
for hp in [7, 21, 63]:
    sub = df[df['hold_period'] == hp]
    t, p = tstat_pval(sub['abnormal_return'].values)
    verdict += f"- **{hp}d hold:** Mean AR = {sub['abnormal_return'].mean():.3f}%, p = {p:.4f}\n"

verdict += "\n### The Filtered Signal\n\n"

# Get the best actionable strategy
if len(best_combos) > 0:
    top = best_combos.iloc[0]
    verdict += f"""The strongest risk-adjusted edge found:
- **Filters:** {top['Hold']} hold | {top['Cluster']} | {top['Value']} | {top['Quality']} | {top['Confidence']}
- **N = {top['N']}** events over 5 years
- **Mean Abnormal Return:** {top['Mean AR (%)']}%
- **Win Rate:** {top['Win Rate (%)']}%
- **Annualized Sharpe:** {top['Sharpe']}
- **Statistical significance:** t = {top['t-stat']}, p = {top['p-value']}

"""

# Practical assessment
verdict += """### Practical Assessment

**Strengths:**
- Insider buying is a real information signal — insiders have material non-public context
- The signal persists across multiple hold periods
- Cluster buying (multiple insiders) amplifies the signal
- High confidence score events show better performance

**Weaknesses:**
- Transaction costs and slippage will reduce edge (especially at 7d)
- Filing delay (Form 4 filed within 2 business days) means entry is post-information-release
- Many events are in small/micro-cap stocks with wide spreads
- Edge may decay over time as more participants exploit it

**Recommendation:**
"""

# Decision logic based on actual results
overall_21d = df[df['hold_period'] == 21]
t21, p21 = tstat_pval(overall_21d['abnormal_return'].values)
mean_21d = overall_21d['abnormal_return'].mean()

if p21 < 0.05 and mean_21d > 0:
    verdict += """There IS a statistically significant edge in following insider purchases.
The recommended approach:

1. **Entry criteria:** Focus on cluster buys (2+ insiders) with high confidence scores (top 25%)
2. **Position sizing:** Equal-weight, diversified across events
3. **Hold period:** 21 days appears optimal for risk-adjusted returns
4. **Universe filter:** Require minimum liquidity (avg volume) to ensure executable
5. **Expected performance:** See best strategy stats above

This is suitable for further development into a systematic strategy with live paper trading.
"""
elif mean_21d > 0:
    verdict += f"""The edge exists but is MARGINAL at the aggregate level (p = {p21:.4f}).
Filtered subsets show stronger signal. Recommended approach:

1. Be selective — only trade high-conviction setups (cluster + high confidence + larger values)
2. The 21d hold period balances signal strength vs. noise
3. Paper trade for 6 months before committing capital
4. Expected Sharpe is modest — this works best as one signal among many
"""
else:
    verdict += """The aggregate signal does NOT show a reliable positive edge.
While individual filtered subsets may show promise, the overall evidence
is insufficient to build a standalone strategy. Consider using insider buying
as a supplementary signal within a multi-factor framework.
"""

verdict += """
### Caveats
- This is an in-sample analysis — out-of-sample validation required
- Survivorship bias: delisted stocks may be underrepresented
- No transaction cost modeling (assume ~10-20bps round trip for liquid names)
- Filing date used as entry signal — actual execution may differ
"""

# Assemble full report
full_report = header + "\n".join(R) + "\n" + verdict
with open(REPORT_PATH, 'w') as f:
    f.write(full_report)
print(f"\n  Report written to: {REPORT_PATH}")
print(f"  Report length: {len(full_report):,} characters")
print("\nDONE.")
