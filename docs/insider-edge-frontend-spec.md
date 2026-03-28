# InsiderEdge Frontend Specification

**Version:** 0.1.0 (Draft)
**Date:** 2026-03-11
**Author:** Derek Gorthy
**Status:** Pre-development

---

## Table of Contents

1. [Product Overview](#1-product-overview)
2. [Tech Stack](#2-tech-stack)
3. [Design System](#3-design-system)
4. [Data Architecture](#4-data-architecture)
5. [Page Specifications](#5-page-specifications)
   - 5.1 [Dashboard](#51-dashboard-home)
   - 5.2 [Live Feed](#52-live-feed)
   - 5.3 [Company Page](#53-company-page)
   - 5.4 [Insider Profile](#54-insider-profile)
   - 5.5 [Leaderboard](#55-leaderboard)
   - 5.6 [Cluster Alerts](#56-cluster-alerts)
   - 5.7 [Sell Signals](#57-sell-signals)
   - 5.8 [Screener](#58-screener)
   - 5.9 [Watchlist](#59-watchlist)
   - 5.10 [Alert Settings](#510-alert-settings)
6. [API Contract Summary](#6-api-contract-summary)
7. [Authentication & Authorization](#7-authentication--authorization)
8. [Performance Requirements](#8-performance-requirements)

---

## 1. Product Overview

InsiderEdge is a web application that surfaces actionable insider trading intelligence from SEC Form 4 filings. It ranks insiders by their historical predictive accuracy, detects cluster buying/selling patterns, and delivers real-time alerts to traders.

**Unique differentiators vs existing tools (OpenInsider, Finviz, Quiver):**

- **Track record scoring**: Every insider is scored on actual post-trade outcomes across 7d/30d/90d horizons, with recency weighting and tier assignments (not just raw filings).
- **Cluster detection with quality scoring**: Multi-factor confidence score (value, breadth, insider seniority, concentration) per cluster event, not just "N insiders bought."
- **Sell-side signals**: Conviction sell classification and sell-signal insiders are tracked -- most competitors ignore sells entirely.
- **Outcome tracking**: Every trade is paired with its forward return, abnormal return vs SPY, so users see *results*, not just filings.

**Backend data at launch:**
- 451K insider trades (55K buys, 396K sells), 2020-2026
- 41K unique insiders with CIK mapping
- Track records with win rates, abnormal returns, Sharpe ratios across 7d/14d/30d/60d/90d
- Recency-weighted tier assignments (Tier 0-3)
- Cluster event detection with composite confidence scores (0-100)
- Daily OHLCV for ~5,000 tickers
- EDGAR Form 4 polling (2-10 min latency)

---

## 2. Tech Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Framework | **Next.js 15** (App Router) | SSR for SEO on company/insider pages, RSC for data-heavy views, API routes for backend proxy |
| Language | **TypeScript** (strict mode) | Type safety across API contracts |
| Styling | **Tailwind CSS v4** + **shadcn/ui** | Rapid iteration, dark-mode-first, consistent component library |
| Stock Charts | **Lightweight Charts** (TradingView) | Financial-grade candlestick/line charts with marker overlays |
| Analytics Charts | **Recharts** | Bar, area, scatter charts for performance dashboards |
| Tables | **TanStack Table v8** | Virtual scrolling, column pinning, server-side sorting/filtering |
| State | **Zustand** | Lightweight global state (watchlists, user preferences) |
| Real-time | **Server-Sent Events** (SSE) | Live feed updates; simpler than WebSocket for unidirectional data |
| Auth | **NextAuth.js v5** | Email magic link + Google OAuth |
| Hosting | **Vercel** | Zero-config Next.js deployment, edge functions |
| Database (user data) | **Supabase** (PostgreSQL) | Watchlists, alert configs, saved screens, user preferences |
| Backend API | Existing SQLite (`insiders.db`) behind a **FastAPI** service | All insider/trade/score data lives here |

**Key architectural decision:** The insider data (451K trades, scores, returns) stays in the existing SQLite catalog behind a read-only FastAPI service. User-generated data (watchlists, alerts, preferences) lives in Supabase. The Next.js app stitches both together.

---

## 3. Design System

### 3.1 Color Palette

```
Background (dark):     #0A0A0F (near-black with blue undertone)
Surface:               #12121A
Surface elevated:      #1A1A26
Border:                #2A2A3A
Text primary:          #E8E8ED
Text secondary:        #8888A0
Text muted:            #55556A

Buy / Positive:        #22C55E (green-500)
Sell / Negative:       #EF4444 (red-500)
Neutral / Info:        #3B82F6 (blue-500)
Warning:               #F59E0B (amber-500)

Tier 1 (Gold):         #F59E0B
Tier 2 (Silver):       #94A3B8
Tier 3 (Bronze):       #CD7F32
Tier 0:                #55556A (muted)
```

Light mode uses inverted luminance with the same hue palette.

### 3.2 Typography

- **Headings:** Inter (600/700 weight)
- **Body:** Inter (400)
- **Monospace / numbers:** JetBrains Mono (tabular figures for aligned columns)
- **Base size:** 14px (data-dense application)

### 3.3 Tier Badges

Small pill-shaped badges with tier color background, white text:

| Badge | Background | Label |
|-------|-----------|-------|
| Tier 3 | `#F59E0B` | `T3` or full `Tier 3` |
| Tier 2 | `#94A3B8` | `T2` |
| Tier 1 | `#CD7F32` | `T1` |
| Tier 0 | `#55556A` | `T0` |

Score badges: rounded rectangle showing the 0.00-3.00 composite score with color gradient from red (0) through yellow (1.5) to green (3.0).

### 3.4 Common Components

| Component | Description |
|-----------|-------------|
| `<InsiderBadge>` | Name + tier pill + primary title |
| `<TradeRow>` | Expandable row: ticker, name, value, date, tier, outcome |
| `<SparkChart>` | 50x20px inline chart for quick trend visualization |
| `<ConfidenceBar>` | Horizontal bar (0-100) with color gradient |
| `<OutcomePill>` | Green/red pill showing return % for a completed trade |
| `<SampleSizeBadge>` | "N=47" style badge; gray if <10, yellow 10-30, green 30+ |
| `<EmptyState>` | Illustration + message + CTA, used on every page |
| `<SkeletonLoader>` | Pulse animation matching exact layout of loaded state |

### 3.5 Responsive Breakpoints

| Breakpoint | Width | Layout |
|-----------|-------|--------|
| Desktop | >= 1280px | Full multi-column layout |
| Tablet | 768-1279px | Stacked panels, collapsible sidebar |
| Mobile | < 768px | Single column, bottom nav, swipeable cards |

---

## 4. Data Architecture

### 4.1 Backend Data Model (existing SQLite)

These tables already exist in `insiders.db` and will be exposed via API:

**`insiders`** -- 41K rows
- `insider_id` (PK), `name`, `name_normalized`, `cik`

**`trades`** -- 451K rows
- `trade_id` (PK), `insider_id` (FK), `ticker`, `company`, `title`, `trade_type` (buy/sell), `trade_date`, `filing_date`, `price`, `qty`, `value`, `is_csuite`, `title_weight`, `source`, `accession`

**`trade_returns`** -- joined 1:1 with trades
- `trade_id` (PK/FK), `entry_price`, `return_7d`, `return_30d`, `return_90d`, `spy_return_7d`, `spy_return_30d`, `spy_return_90d`, `abnormal_7d`, `abnormal_30d`, `abnormal_90d`

**`insider_track_records`** -- 41K rows, one per insider
- Buy-side: `buy_count`, `buy_win_rate_7d`, `buy_avg_return_7d`, `buy_median_return_7d`, `buy_avg_abnormal_7d`, `buy_win_rate_30d`, `buy_avg_return_30d`, `buy_win_rate_90d`, `buy_avg_return_90d`, `buy_total_value`, `buy_first_date`, `buy_last_date`
- Sell-side: `sell_count`, `sell_win_rate_7d`, `sell_avg_return_7d`, `sell_total_value`, `sell_win_rate_30d`, `sell_avg_return_30d`, `sell_win_rate_90d`, `sell_avg_return_90d`, `sell_avg_abnormal_7d`, `sell_avg_abnormal_30d`, `sell_avg_abnormal_90d`
- Composite: `score` (0-3), `score_tier` (0-3), `percentile`, `score_recency_weighted`, `recent_win_rate_7d`, `tier_recency`
- Meta: `primary_title`, `primary_ticker`, `n_tickers`

**`insider_companies`** -- many-to-many
- `insider_id`, `ticker`, `company`, `title`, `trade_count`, `total_value`, `first_trade`, `last_trade`

### 4.2 Computed/Derived Data (API layer)

These are computed at query time or by periodic jobs, not stored in the frontend:

| Concept | Computation |
|---------|-------------|
| **Cluster event** | 2+ distinct insiders trading same ticker within 30-day window. Confidence score = f(value_score, breadth_score, quality_score, concentration). Scale 0-100. |
| **Market insider sentiment** | Rolling 20-day ratio of buy events to sell events across all tickers, smoothed. |
| **Company insider sentiment** | Net buy/sell dollar ratio over trailing 90/180/365 days for a specific ticker. |
| **Routine vs conviction sell** | Cohen et al. filter: if insider sold in same calendar month for 3+ consecutive years, mark as routine. Everything else is conviction. |
| **Recency-weighted score** | Exponential decay: 1yr=1.0, 2yr=0.7, 3yr=0.5, 4yr+=0.3. Produces `tier_recency` (0-3). |

### 4.3 User Data (Supabase)

| Table | Columns |
|-------|---------|
| `users` | `id`, `email`, `name`, `created_at`, `plan` (free/pro) |
| `watchlist_items` | `id`, `user_id`, `item_type` (ticker/insider), `item_id`, `alert_on_buy`, `alert_on_sell`, `alert_on_cluster`, `created_at` |
| `saved_screens` | `id`, `user_id`, `name`, `filters_json`, `created_at` |
| `alert_configs` | `id`, `user_id`, `channel` (email/push/telegram/webhook), `channel_target`, `enabled`, `quiet_start`, `quiet_end` |
| `alert_rules` | `id`, `user_id`, `alert_type`, `min_tier`, `min_value`, `trade_types`, `tickers_only`, `enabled` |

---

## 5. Page Specifications

---

### 5.1 Dashboard / Home

**URL:** `/`
**Purpose:** At-a-glance view of today's insider activity, active clusters, and market-wide insider sentiment.

#### Layout

```
+------------------------------------------------------------------+
| TOP BAR: Logo | Search (ticker/insider) | Watchlist | Settings    |
+------------------------------------------------------------------+
| STAT CARDS (4 across)                                             |
| [Signals Today] [Active Clusters] [Buy/Sell Ratio] [Top Mover]   |
+------------------------------------------------------------------+
| LEFT COLUMN (60%)              | RIGHT COLUMN (40%)               |
|                                |                                   |
| Recent High-Confidence Signals | Market Insider Sentiment          |
| (table, 10 rows)              | (area chart, 90 days)             |
|                                |                                   |
|                                +-----------------------------------+
|                                | Today's Notable Sells             |
|                                | (compact list, 5 items)           |
+--------------------------------+-----------------------------------+
| CLUSTER ACTIVITY HEATMAP (full width)                             |
| Calendar heatmap of cluster events, last 90 days                  |
+------------------------------------------------------------------+
```

#### Stat Cards

| Card | Data | Type | Source |
|------|------|------|--------|
| Signals Today | Count of Form 4 filings from Tier 2+ insiders filed today | Integer | `trades` WHERE `filing_date = today AND insider_tier >= 2` |
| Active Clusters | Count of active cluster events (2+ insiders, last 7 days) | Integer | Cluster detection query |
| Buy/Sell Ratio | Dollar-weighted buy:sell ratio, trailing 5 days | Ratio (e.g., "1.3:1") | `SUM(value) GROUP BY trade_type` |
| Top Mover | Ticker with highest confidence cluster event today | Ticker + confidence score | Highest `confidence_score` cluster today |

#### Recent High-Confidence Signals Table

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Time | datetime | Yes | Filing timestamp, relative (e.g., "2h ago") |
| Ticker | string | Yes | Link to `/company/:ticker` |
| Insider | string | Yes | Link to `/insider/:cik`, truncated at 20 chars |
| Tier | badge | Yes | Tier badge component |
| Title | string | No | CEO, CFO, etc. |
| Type | badge | No | "BUY" green or "SELL" red pill |
| Value | currency | Yes | Formatted: "$1.2M", "$340K" |
| Confidence | bar | Yes | 0-100 confidence bar (only for cluster events) |
| 7d Track Record | percentage | Yes | Insider's historical 7d win rate |

Default sort: Time descending.
Row click: navigates to `/company/:ticker`.
Max rows: 10, with "View all" link to `/feed`.

#### Market Insider Sentiment Chart

- **Type:** Area chart (Recharts)
- **X-axis:** Date (trailing 90 days)
- **Y-axis:** Sentiment index (buy-sell dollar ratio, 5-day rolling average)
- **Series:** Single area fill, green above 1.0, red below 1.0
- **Reference line:** Horizontal at 1.0 (neutral)
- **Tooltip:** Date, raw buy $, raw sell $, ratio

#### Cluster Activity Heatmap

- **Type:** Calendar heatmap (similar to GitHub contribution graph)
- **Timeframe:** Last 90 days
- **Cell:** One square per day
- **Color intensity:** Number of cluster events that day (0=empty, 1=light, 2+=darker)
- **Tooltip on hover:** Date, N clusters, top ticker, total value

#### Interactive Elements

- Search bar: global search by ticker or insider name (autocomplete, top 5 results each)
- Date range toggle on sentiment chart: 30d / 90d / 180d / 1Y
- Stat card click-through: each card navigates to the relevant detail page

#### Empty State

- New user with no data: show onboarding card ("Welcome to InsiderEdge. Here's what insiders are doing right now.") with auto-populated data.
- No signals today: "No high-confidence signals today. Markets may be closed or it's early." Show previous day's highlights instead.

#### Loading State

- Skeleton loaders matching exact card and table layout
- Stat cards: pulsing rounded rectangles
- Table: 10 skeleton rows with shimmer animation
- Chart: gray rectangle with pulse

#### Mobile

- Stat cards: 2x2 grid
- Table: horizontal scroll with sticky first column (Ticker)
- Sentiment chart: full width, reduced to 30-day default
- Heatmap: horizontal scroll

---

### 5.2 Live Feed

**URL:** `/feed`
**Purpose:** Real-time stream of all Form 4 filings as they are processed from EDGAR, with inline insider scoring.

#### Layout

```
+------------------------------------------------------------------+
| FILTER BAR                                                        |
| [Trade Type v] [Min Value v] [Title v] [Sector v] [Tier v]       |
| [Cluster Only toggle] [Date Range] [Search ticker/insider]        |
+------------------------------------------------------------------+
| FEED                                                              |
| +--------------------------------------------------------------+ |
| | > AAPL  Tim Cook (CEO)    BUY  $12.4M   T3   2h ago    [+]  | |
| +--------------------------------------------------------------+ |
| | > MSFT  Satya Nadella     BUY  $3.1M    T2   3h ago    [+]  | |
| +--------------------------------------------------------------+ |
| |   TSLA  Board Director    SELL $890K    T0   3h ago          | |
| +--------------------------------------------------------------+ |
| | ... (infinite scroll)                                        | |
| +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
| STATUS BAR: "Last EDGAR poll: 2 min ago | 47 filings today"      |
+------------------------------------------------------------------+
```

#### Feed Row (Collapsed)

| Field | Type | Position |
|-------|------|----------|
| Cluster indicator | dot (blue if part of cluster) | Left edge |
| Ticker | string, bold, linked | Left |
| Company name | string, muted | Below ticker on mobile, inline on desktop |
| Insider name | string, linked to profile | Center-left |
| Title | string, muted | Below name |
| Trade type | pill badge (BUY green / SELL red) | Center |
| Value | currency, formatted | Center-right |
| Tier badge | component | Right-center |
| Filed time | relative timestamp | Right |
| Expand button | chevron icon | Far right |

#### Feed Row (Expanded)

When a row is expanded, a detail panel slides down showing:

```
+--------------------------------------------------------------+
| INSIDER TRACK RECORD SUMMARY                                  |
|                                                                |
| Score: 2.41/3.00 [=========>    ]  Tier 3 (Top 7%)           |
| Buy trades: 47    |  Win rate 7d: 63.2%  |  Avg alpha: +3.8% |
| Sell trades: 12   |  Sell WR 7d: 58.3%                        |
| Active since: 2019-03  |  Companies: AAPL, GOOG, MSFT        |
|                                                                |
| RECENT OUTCOMES (last 5 buys)                                  |
| 2026-02-15  AAPL  +4.2%  |  2026-01-08  GOOG  -1.1%         |
| 2025-11-22  AAPL  +6.8%  |  2025-09-14  MSFT  +2.3%         |
| 2025-08-01  AAPL  +1.5%                                       |
|                                                                |
| [View Full Profile ->]  [Add to Watchlist]                     |
+--------------------------------------------------------------+
```

#### Filters

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Trade Type | Multi-select dropdown | Buy, Sell | Both |
| Min Value | Dropdown | $50K, $100K, $500K, $1M, $5M | $50K |
| Title | Multi-select dropdown | CEO, CFO, COO, President, Chairman, SVP/EVP, VP, Director, 10% Owner, Other | All |
| Sector | Multi-select dropdown | GICS sectors (11) | All |
| Min Tier | Dropdown | Any, Tier 1+, Tier 2+, Tier 3 only | Any |
| Cluster Only | Toggle switch | On/Off | Off |
| Date Range | Date range picker | Custom, Today, Last 7d, Last 30d | Today |
| Search | Text input | Ticker or insider name | Empty |

All filters apply client-side for cached data, server-side for pagination beyond cache.

#### Real-time Updates

- SSE connection to `/api/feed/stream`
- New filings appear at top of feed with a subtle slide-in animation and brief green/red border flash
- Counter badge on browser tab: "InsiderEdge (3)" showing unread count since last scroll-to-top
- "New filings available" banner when user has scrolled down and new items arrive

#### Pagination

- Infinite scroll, loading 50 items per page
- Virtual scrolling for performance (TanStack Virtual)
- "Jump to date" button for historical browsing

#### Empty State

"No filings match your filters. Try broadening your search or check back during market hours (Form 4s are typically filed 4-7pm ET)."

#### Loading State

- Initial: 10 skeleton rows
- Subsequent pages: spinner at bottom of list
- SSE reconnecting: "Reconnecting to live feed..." banner with spinner

#### Mobile

- Full-width card layout instead of table rows
- Filters collapse into a slide-out drawer (filter icon in sticky header)
- Swipe left on card to add to watchlist
- Expand/collapse via tap on card
- Sticky status bar at bottom

---

### 5.3 Company Page

**URL:** `/company/:ticker`
**Purpose:** Complete insider activity view for a single company, with stock chart overlay showing exactly when insiders bought and sold.

#### Layout

```
+------------------------------------------------------------------+
| HEADER                                                            |
| AAPL - Apple Inc.     $187.42  +1.23 (+0.66%)                   |
| Sector: Technology  |  Market Cap: $2.9T  |  52w: $164-$199     |
+------------------------------------------------------------------+
| STOCK CHART WITH INSIDER MARKERS (full width, 400px height)       |
| [1M] [3M] [6M] [1Y] [3Y] [ALL]                                  |
| Candlestick chart with:                                           |
|   - Green triangles (up) for insider buys                         |
|   - Red triangles (down) for insider sells                        |
|   - Triangle size proportional to trade value                     |
|   - Cluster events highlighted with vertical band                 |
+------------------------------------------------------------------+
| LEFT COLUMN (55%)              | RIGHT COLUMN (45%)               |
|                                |                                   |
| INSIDER ROSTER                 | INSIDER SENTIMENT                 |
| Ranked table of all insiders   | Net buy/sell ratio chart          |
| who've traded this stock       | (area chart, trailing 1Y)        |
|                                |                                   |
+--------------------------------+-----------------------------------+
| ACTIVITY TIMELINE (full width)                                    |
| Chronological list of all insider trades at this company          |
+------------------------------------------------------------------+
```

#### Stock Chart with Insider Markers

- **Library:** Lightweight Charts (TradingView)
- **Chart type:** Candlestick (daily OHLCV)
- **Overlay markers:**
  - Buy trades: upward-pointing green triangle at the trade date, plotted at the low of the day
  - Sell trades: downward-pointing red triangle at the trade date, plotted at the high of the day
  - Marker size: small (<$100K), medium ($100K-$1M), large (>$1M)
  - Cluster events: light blue vertical background band spanning the cluster window dates
- **Tooltip on marker hover:** Insider name, title, trade type, shares, value, tier badge
- **Time controls:** 1M, 3M, 6M, 1Y, 3Y, ALL buttons above chart
- **Toggle controls:** Show/Hide buys, Show/Hide sells, Show/Hide clusters (checkbox toggles below chart)

#### Insider Roster Table

Ranked by track record quality. Answers: "Which insiders here actually predict moves?"

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Rank | integer | No | By composite score |
| Insider | string + tier badge | Yes | Link to `/insider/:cik` |
| Title | string | Yes | Most recent title at this company |
| Score | number (0-3.00) | Yes | Composite score with color gradient |
| Tier | badge | Yes | Tier badge |
| Trades | integer | Yes | Total trades at this company |
| Win Rate (7d) | percentage | Yes | Buy-side 7d win rate |
| Avg Alpha | percentage | Yes | Buy-side avg abnormal 7d return |
| Last Trade | date | Yes | Most recent trade date |
| Last Value | currency | Yes | Value of most recent trade |

Default sort: Score descending.

#### Insider Sentiment Chart

- **Type:** Area chart (Recharts)
- **X-axis:** Date (trailing 1 year, toggle to 2Y/3Y/ALL)
- **Y-axis:** Net insider dollars (buy value minus sell value), cumulative rolling 30-day
- **Fill:** Green when positive (net buying), red when negative (net selling)
- **Annotations:** Vertical dashed lines at major cluster events

#### Activity Timeline

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Date (Filed) | date | Yes | Filing date |
| Date (Traded) | date | Yes | Actual transaction date |
| Insider | string + tier badge | Yes | Link to profile |
| Title | string | No | |
| Type | pill | Yes | BUY / SELL |
| Shares | integer | Yes | Formatted with commas |
| Price | currency | Yes | Per-share price |
| Value | currency | Yes | Total trade value |
| 7d Return | percentage + color | Yes | Forward 7d return, green/red |
| 30d Return | percentage + color | Yes | Forward 30d return |
| 90d Return | percentage + color | Yes | Forward 90d return |
| Cluster? | dot/badge | Yes | Blue dot if part of cluster |

Default sort: Date filed descending.
Filterable by trade type (buy/sell/both) and insider.

#### Interactive Elements

- Chart timeframe buttons (1M/3M/6M/1Y/3Y/ALL)
- Marker visibility toggles (buys/sells/clusters)
- Roster and timeline tables: full sort and filter
- "Add to Watchlist" button in header
- "Compare Insiders" mode: select 2-3 insiders from roster to overlay their trades on chart

#### Empty State

"No insider trading activity found for [TICKER]. This could mean the ticker is invalid, it's an ETF (insiders don't file Form 4 for ETFs), or no filings exist in our database."

#### Loading State

- Chart: gray rectangle with centered spinner
- Roster table: 5 skeleton rows
- Timeline: 10 skeleton rows

#### Mobile

- Chart: full width, 250px height, simplified controls
- Roster: horizontal scroll, 3 key columns visible (Name, Score, WR)
- Timeline: card layout with expandable details
- Sentiment chart: below roster, full width

---

### 5.4 Insider Profile

**URL:** `/insider/:cik`
**Purpose:** Complete performance dashboard for a single insider. Answers: "When this person buys, what happens?"

#### Layout

```
+------------------------------------------------------------------+
| HEADER                                                            |
| Marc Benioff          Tier 3 [Gold Badge]                        |
| CEO, Salesforce (CRM)   |   CIK: 0001234567                     |
| Score: 2.64/3.00  |  Active since: 2015                          |
+------------------------------------------------------------------+
| PERFORMANCE SUMMARY CARDS (4 across)                              |
| [Buy WR 7d: 68%]  [Avg Alpha: +4.2%]  [Total Bought: $142M]    |
| [Sell WR 7d: 61%]                                                 |
+------------------------------------------------------------------+
| LEFT (50%)                     | RIGHT (50%)                      |
| WIN RATE BY HORIZON            | CUMULATIVE ALPHA CHART           |
| Grouped bar chart              | Line chart of compounded alpha   |
| 7d / 30d / 90d                 | over time (buy-side)             |
+--------------------------------+----------------------------------+
| FULL TRADE HISTORY (full width, tabbed: Buys | Sells | All)      |
| Table with outcome tracking                                       |
+------------------------------------------------------------------+
| COMPANY HISTORY (full width)                                      |
| Timeline showing all companies this insider has traded at          |
+------------------------------------------------------------------+
```

#### Performance Summary Cards

| Card | Data | Notes |
|------|------|-------|
| Buy Win Rate (7d) | `buy_win_rate_7d` | Percentage, green if >55%, red if <50% |
| Avg Alpha (7d) | `buy_avg_abnormal_7d` | Percentage, signed |
| Total Bought | `buy_total_value` | Formatted: "$142M" |
| Sell Win Rate (7d) | `sell_win_rate_7d` | Percentage; win = stock drops after sell |

Additional cards for Pro users: Sharpe ratio, recency-weighted score, sample size badge.

#### Win Rate by Horizon Chart

- **Type:** Grouped bar chart (Recharts)
- **X-axis:** Horizon (7d, 30d, 90d)
- **Y-axis:** Win rate (0-100%)
- **Bars per group:** 2 -- Buy WR (green), Sell WR (red)
- **Reference line:** 50% horizontal (coin flip)
- **Annotation:** N trades label on each bar
- **Sample size badge:** Below each bar group

#### Cumulative Alpha Chart

- **Type:** Line chart (Recharts)
- **X-axis:** Trade date (chronological, each trade is a point)
- **Y-axis:** Cumulative abnormal return (sum of abnormal_7d for all prior buys)
- **Line color:** Green when trending up, red when trending down
- **Markers:** Each trade is a dot; hover shows trade details
- **Shaded region:** +/- 1 standard error band
- **Toggle:** Switch between 7d / 30d / 90d horizon

#### Full Trade History Table

Tabbed: **Buys** | **Sells** | **All**

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| # | integer | No | Sequential trade number |
| Date Filed | date | Yes | |
| Date Traded | date | Yes | |
| Ticker | string | Yes | Link to `/company/:ticker` |
| Company | string | No | Truncated at 30 chars |
| Title | string | No | Title at time of trade |
| Shares | integer | Yes | |
| Price | currency | Yes | Per share |
| Value | currency | Yes | Total |
| 7d Return | percentage | Yes | Colored green/red |
| 7d Abnormal | percentage | Yes | vs SPY, colored |
| 30d Return | percentage | Yes | |
| 90d Return | percentage | Yes | |
| Outcome | pill | Yes | "Win" (green) / "Loss" (red) based on 7d |

Default sort: Date filed descending.

#### Company History Timeline

For insiders who trade at multiple companies, show a horizontal timeline:

```
|------ CRM (2015-present) ------|
        |--- WORK (2018-2020) ---|
                    |--- DATA (2021-2022) ---|
```

- Each bar: company ticker, date range, total trades, total value
- Click a bar to filter the trade history table to that company

#### Interactive Elements

- Horizon toggle on cumulative alpha chart (7d/30d/90d)
- Trade history tab switching (Buys/Sells/All)
- Company timeline click-to-filter
- "Add to Watchlist" button
- "Compare with..." search box to overlay another insider's performance

#### Empty State

"Insider not found. The CIK [value] doesn't match any insider in our database. This could be a very new filer or an invalid CIK."

For insider with trades but no return data: "This insider has [N] trades on file, but we haven't computed forward returns yet. Check back soon."

#### Loading State

- Header: skeleton text with tier badge placeholder
- Cards: 4 pulsing rectangles
- Charts: gray rectangles with spinners
- Table: 10 skeleton rows

#### Mobile

- Cards: 2x2 grid
- Charts: stacked vertically, full width
- Trade history: card layout with most important fields, expandable for full detail
- Company timeline: vertical timeline instead of horizontal

---

### 5.5 Leaderboard

**URL:** `/leaderboard`
**Purpose:** Ranked table of all insiders by composite track record score.

#### Layout

```
+------------------------------------------------------------------+
| FILTER BAR                                                        |
| [Min Trades v] [Title v] [Sector v] [Recency v] [Trade Type v]  |
| [Sort by: Score v]                                                |
+------------------------------------------------------------------+
| LEADERBOARD TABLE                                                 |
| Virtualized, 50 rows per page, infinite scroll                   |
+------------------------------------------------------------------+
| DISTRIBUTION CHART (collapsible)                                  |
| Histogram of scores across all insiders                           |
+------------------------------------------------------------------+
```

#### Leaderboard Table

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Rank | integer | No | Position in current sort |
| Insider | string + tier badge | Yes | Link to profile |
| Primary Title | string | Yes | Most frequent title |
| Primary Ticker | string | Yes | Link to company page |
| Score | number (0-3.00) | Yes | Color-gradient bar |
| Tier | badge | Yes | |
| Buy Trades | integer | Yes | With sample size badge |
| Win Rate (7d) | percentage | Yes | |
| Avg Alpha (7d) | percentage | Yes | Signed, colored |
| Sharpe | number | Yes | Annualized Sharpe ratio |
| Recency Score | number (0-3.00) | Yes | Recency-weighted composite |
| Last Active | date | Yes | Most recent trade date |

Default sort: Score descending.

#### Filters

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Min Trades | Dropdown | 3, 5, 10, 20, 50 | 5 |
| Title | Multi-select | CEO, CFO, COO, etc. | All |
| Sector | Multi-select | GICS sectors | All |
| Recency | Dropdown | Active last 1Y, 2Y, 3Y, Any | Any |
| Trade Type | Radio | Buy-side scores, Sell-side scores | Buy-side |
| Sort By | Dropdown | Score, Win Rate, Alpha, Sharpe, Recency Score, Trade Count | Score |

#### Distribution Chart

- **Type:** Histogram (Recharts)
- **X-axis:** Score bins (0-0.5, 0.5-1.0, ..., 2.5-3.0)
- **Y-axis:** Count of insiders
- **Fill:** Gradient matching tier colors
- **Vertical lines:** Tier thresholds (67th, 80th, 93rd percentile)
- **Collapsible:** Collapsed by default, toggle with "Show distribution" link

#### Confidence Indicators

Each row shows a sample size badge next to the win rate:
- Gray badge "N=3" for 3-9 trades (low confidence)
- Yellow badge "N=18" for 10-29 trades (moderate confidence)
- Green badge "N=47" for 30+ trades (high confidence)

#### Interactive Elements

- All filters and sort options
- Click any row to navigate to insider profile
- Bulk select rows to compare (max 5)
- Export button: CSV download of current filtered/sorted view
- Pagination: infinite scroll or page number selector

#### Empty State

"No insiders match your filters. Try reducing the minimum trade count or broadening the title filter."

#### Loading State

- Filter bar: present immediately (no loading needed)
- Table: 20 skeleton rows
- Distribution chart: gray rectangle with pulse

#### Mobile

- Table: horizontal scroll with sticky Rank + Name columns
- Filters: slide-out drawer
- Distribution chart: hidden by default
- Top 3 highlighted as cards above the table

---

### 5.6 Cluster Alerts

**URL:** `/clusters`
**Purpose:** View active and historical cluster buy/sell events with quality scoring and historical hit rates.

#### Layout

```
+------------------------------------------------------------------+
| FILTER BAR                                                        |
| [Status: Active / Recent / All] [Min Confidence v] [Type v]      |
| [Min Insiders v] [Date Range]                                     |
+------------------------------------------------------------------+
| ACTIVE CLUSTERS (cards)                                           |
| +---------------------------+  +---------------------------+      |
| | AAPL Cluster Buy          |  | MSFT Cluster Buy          |      |
| | 3 insiders | $14.2M       |  | 2 insiders | $3.1M        |      |
| | Confidence: 78/100        |  | Confidence: 52/100        |      |
| | Started: Mar 8            |  | Started: Mar 10           |      |
| | [CEO, CFO, EVP]           |  | [VP, Director]            |      |
| +---------------------------+  +---------------------------+      |
+------------------------------------------------------------------+
| HISTORICAL CLUSTER TABLE (full width)                             |
| +--------------------------------------------------------------+ |
| | Date | Ticker | Type | Insiders | Value | Conf | 7d | 30d   | |
| +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
| ALERT CONFIGURATION PANEL (collapsible)                           |
+------------------------------------------------------------------+
```

#### Active Cluster Cards

Each active cluster (within last 7 days, position still open) displayed as a card:

| Field | Type | Notes |
|-------|------|-------|
| Ticker + Company | string | Bold, linked to company page |
| Cluster Type | badge | "CLUSTER BUY" (green) or "CLUSTER SELL" (red) |
| N Insiders | integer | "3 insiders" |
| Total Value | currency | Sum of all trades in cluster |
| Confidence Score | progress bar | 0-100, color-coded |
| Date Range | dates | "Mar 5 - Mar 8" (event start to trigger) |
| Participating Insiders | list | Each with name, title, tier badge, trade value |
| Historical Hit Rate | percentage | "Similar clusters (conf 70-80): 64% positive at 7d" |
| Price Since Trigger | percentage | Current return since cluster trigger date |

#### Confidence Score Breakdown (on hover/expand)

| Component | Value | Weight |
|-----------|-------|--------|
| Value Score | 4.2/5.0 | log10(total_value/25K)+1, capped at 5 |
| Breadth Score | 2.6/5.0 | 1+log2(n_insiders), capped at 5 |
| Quality Score | 2.8/3.0 | Mean title weight of all trades |
| Concentration | 0.72 | Max single insider value / total |
| **Composite** | **78/100** | Normalized product |

#### Historical Cluster Table

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Trigger Date | date | Yes | Last filing date in cluster |
| Ticker | string | Yes | Link to company page |
| Company | string | No | |
| Type | pill | Yes | BUY / SELL |
| N Insiders | integer | Yes | |
| Total Value | currency | Yes | |
| Confidence | bar + number | Yes | 0-100 |
| Top Insider | string + tier | No | Highest-tier participant |
| 7d Return | percentage | Yes | Forward return, colored |
| 30d Return | percentage | Yes | |
| 90d Return | percentage | Yes | |
| Outcome | pill | Yes | Win/Loss based on 7d return |

Default sort: Trigger date descending.

#### Filters

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Status | Segment control | Active (7d), Recent (30d), All | Active |
| Min Confidence | Slider | 0-100 | 30 |
| Type | Toggle | Buy clusters, Sell clusters, Both | Both |
| Min Insiders | Dropdown | 2, 3, 4, 5+ | 2 |
| Date Range | Date picker | Custom | Last 90 days |

#### Alert Configuration Panel

Collapsible section at bottom:

| Setting | Type | Default |
|---------|------|---------|
| Alert on new clusters | Toggle | On |
| Min confidence for alert | Slider (0-100) | 50 |
| Min insiders for alert | Dropdown (2-5) | 2 |
| Alert on buy clusters | Toggle | On |
| Alert on sell clusters | Toggle | Off |
| Min total value | Dropdown | $1M |
| Require Tier 2+ insider | Toggle | Off |

#### Empty State

Active: "No active cluster events right now. Cluster buying tends to pick up around earnings season and market dips."
Historical: "No clusters match your filters."

#### Loading State

- Active cards: 2-3 skeleton cards
- Historical table: 10 skeleton rows

#### Mobile

- Active clusters: vertical card stack, swipeable
- Historical table: card layout
- Alert config: full-screen modal

---

### 5.7 Sell Signals

**URL:** `/sells`
**Purpose:** Sell-side dashboard highlighting conviction sells, cluster sells, and sell-warning signals. This is a key differentiator -- most insider tools ignore sell activity.

#### Layout

```
+------------------------------------------------------------------+
| PAGE HEADER                                                       |
| "Sell Signals"                                                    |
| Subtitle: "Most insider sells are routine. We find the ones       |
| that aren't."                                                     |
+------------------------------------------------------------------+
| STAT CARDS (4 across)                                             |
| [Conviction Sells Today] [Cluster Sells (30d)] [Avg Sell WR]     |
| [Watchlist Warnings]                                              |
+------------------------------------------------------------------+
| TABS: [Conviction Sells] [Cluster Sells] [Watchlist Warnings]     |
+------------------------------------------------------------------+
| TAB CONTENT (full width table or cards)                           |
+------------------------------------------------------------------+
| SELL SIGNAL METHODOLOGY (collapsible explainer)                   |
+------------------------------------------------------------------+
```

#### Stat Cards

| Card | Data | Notes |
|------|------|-------|
| Conviction Sells Today | Count of non-routine sells filed today with value > $500K | Integer |
| Cluster Sells (30d) | Count of cluster sell events in last 30 days | Integer |
| Avg Top-Seller WR | Average sell win rate among Tier 2+ insiders | Percentage |
| Watchlist Warnings | Count of user's watchlist tickers with recent conviction sells | Integer (0 if no watchlist) |

#### Tab: Conviction Sells

Table of individual sell trades classified as conviction (not routine per Cohen et al. filter):

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Date | date | Yes | Filing date |
| Ticker | string | Yes | Link to company page |
| Insider | string + tier badge | Yes | Link to profile |
| Title | string | No | |
| Value | currency | Yes | |
| % of Holdings | percentage | Yes | If available (shares sold / shares owned) |
| Sell WR (7d) | percentage | Yes | This insider's historical sell win rate |
| Classification | badge | No | "Conviction" (red), "Large" (orange), "First-time" (yellow) |
| 7d Return | percentage | Yes | Forward return (negative = sell was correct) |

**Classification logic:**
- **Conviction**: Non-routine, value > $500K, insider has sell WR > 55%
- **Large**: Non-routine, value > $2M (regardless of track record)
- **First-time**: Insider's first sell at this company in 2+ years

#### Tab: Cluster Sells

Same structure as Cluster Alerts page but filtered to sell-side only. Uses the cluster sell data from `build_event_calendar.py --trade-type sell`.

| Column | Type | Sortable | Notes |
|--------|------|----------|-------|
| Trigger Date | date | Yes | |
| Ticker | string | Yes | |
| N Sellers | integer | Yes | |
| Total Value | currency | Yes | |
| Confidence | bar | Yes | 0-100 |
| Top Insider | string + tier | No | Best sell-signal insider |
| 7d Return | percentage | Yes | Negative = correct (stock dropped) |
| 30d Return | percentage | Yes | |

#### Tab: Watchlist Warnings

Filtered view showing only conviction sells and cluster sells for tickers/insiders on the user's watchlist.

If no watchlist: "Add tickers to your watchlist to receive sell warnings here."

| Column | Type | Notes |
|--------|------|-------|
| Ticker | string | From user's watchlist |
| Signal Type | badge | "Conviction Sell" / "Cluster Sell" |
| Insider(s) | string | Name(s) and tier(s) |
| Value | currency | |
| Date | date | |
| Alert Sent | boolean icon | Whether alert was dispatched |

#### Sell Signal Methodology (Collapsible)

Expandable section explaining:
- How routine vs conviction is classified (Cohen, Malloy & Pomorski 2012)
- What "sell win rate" means (stock declines after insider sells)
- Historical data: "Across 396K sells in our database, conviction sells predict 7d declines 58% of the time with a t-stat of -16.73"
- Why most tools ignore sells and why that's a mistake

#### Empty State

"No conviction sell signals today. Sell signals are less frequent than buys -- check back during earnings season."

#### Loading State

- Stat cards: pulsing
- Table: 10 skeleton rows

#### Mobile

- Tabs as horizontal scroll pills
- Tables as card layouts
- Methodology section: hidden by default, accessible via "?" icon

---

### 5.8 Screener

**URL:** `/screener`
**Purpose:** Multi-criteria search tool for finding insiders and trades matching specific performance and characteristic criteria.

#### Layout

```
+------------------------------------------------------------------+
| FILTER PANEL (left sidebar, 300px)                                |
|                                                                   |
| PERFORMANCE CRITERIA                                              |
| Win Rate (7d): [___] to [___] %                                  |
| Avg Alpha (7d): [___] to [___] %                                 |
| Sharpe: [___] to [___]                                            |
| Score: [___] to [___]                                             |
| Tier: [checkboxes: 0 1 2 3]                                      |
|                                                                   |
| TRADE CHARACTERISTICS                                             |
| Min Trades: [___]                                                 |
| Trade Type: [Buy / Sell / Both]                                   |
| Min Value (per trade): [___]                                      |
| Max Value (per trade): [___]                                      |
|                                                                   |
| INSIDER ATTRIBUTES                                                |
| Title: [multi-select]                                             |
| Active in last: [1Y / 2Y / 3Y / Any]                             |
|                                                                   |
| COMPANY ATTRIBUTES                                                |
| Sector: [multi-select]                                            |
| Ticker contains: [___]                                            |
|                                                                   |
| [Apply Filters]  [Reset]                                          |
| [Save Screen v]  [Load Screen v]                                  |
+------------------------------------------------------------------+
| RESULTS (right, remaining width)                                  |
|                                                                   |
| RESULTS MODE: [Insiders] [Trades]                                 |
|                                                                   |
| Results table (virtual scroll)                                    |
| "Showing 247 of 41,000 insiders"                                  |
|                                                                   |
| [Export CSV]                                                       |
+------------------------------------------------------------------+
```

#### Filter Panel -- Performance Criteria

| Filter | Type | Range | Default |
|--------|------|-------|---------|
| Win Rate (7d) | Range slider | 0-100% | 0-100 |
| Avg Alpha (7d) | Range inputs | -50% to +50% | No filter |
| Avg Alpha (30d) | Range inputs | -50% to +50% | No filter |
| Sharpe Ratio | Range inputs | -5 to +5 | No filter |
| Composite Score | Range slider | 0-3.00 | 0-3.00 |
| Tier | Checkboxes | 0, 1, 2, 3 | All checked |
| Recency Score | Range slider | 0-3.00 | No filter |

#### Filter Panel -- Trade Characteristics

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Min Trades | Number input | >= 1 | 3 |
| Trade Type Focus | Radio | Buy-side, Sell-side, Both | Buy-side |
| Min Value Per Trade | Currency input | | No filter |
| Max Value Per Trade | Currency input | | No filter |
| Total Lifetime Value | Range | | No filter |

#### Filter Panel -- Insider Attributes

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Title | Multi-select dropdown | CEO, CFO, COO, President, Chairman, SVP/EVP, VP, Director, 10% Owner, Other | All |
| Active In Last | Radio | 1Y, 2Y, 3Y, Any | Any |
| N Companies | Range | 1-20+ | No filter |

#### Filter Panel -- Company Attributes

| Filter | Type | Options | Default |
|--------|------|---------|---------|
| Sector | Multi-select | GICS sectors | All |
| Ticker | Text input | Partial match | Empty |

#### Results Table -- Insiders Mode

| Column | Type | Sortable |
|--------|------|----------|
| Insider | string + tier badge | Yes |
| Primary Title | string | Yes |
| Primary Ticker | string | Yes |
| Score | number | Yes |
| Tier | badge | Yes |
| Buy Count | integer | Yes |
| Buy WR (7d) | percentage | Yes |
| Buy Avg Alpha | percentage | Yes |
| Sell Count | integer | Yes |
| Sell WR (7d) | percentage | Yes |
| Last Active | date | Yes |
| Total Value | currency | Yes |

#### Results Table -- Trades Mode

| Column | Type | Sortable |
|--------|------|----------|
| Date | date | Yes |
| Insider | string + tier | Yes |
| Ticker | string | Yes |
| Type | pill | Yes |
| Value | currency | Yes |
| 7d Return | percentage | Yes |
| 7d Abnormal | percentage | Yes |
| 30d Return | percentage | Yes |
| 90d Return | percentage | Yes |

#### Save/Load Screens

- "Save Screen" dropdown: name the current filter set, stored in `saved_screens` table
- "Load Screen" dropdown: select from previously saved screens
- Prebuilt screens shipped by default:
  - "Top CEOs" (Tier 2+, CEO title, 10+ trades)
  - "High Conviction Buys" (WR > 65%, Alpha > 3%, 20+ trades)
  - "Sell Signal Masters" (Sell WR > 60%, 10+ sells)
  - "Recent Newcomers" (Active last 1Y, 3-10 trades, WR > 55%)

#### Export

- CSV download button
- Exports current filtered + sorted view
- Columns match the visible table
- Filename: `insideredge_screen_YYYY-MM-DD.csv`

#### Empty State

"No insiders match all your criteria. Try relaxing the win rate or minimum trades filter."

#### Loading State

- Filter panel: immediately visible (static)
- Results: skeleton table with count placeholder

#### Mobile

- Filter panel: slide-out drawer from left edge
- Results: card layout
- Save/Load: bottom sheet
- Export: share sheet

---

### 5.9 Watchlist

**URL:** `/watchlist`
**Purpose:** User's curated list of tracked companies and insiders, with a filtered activity feed and per-item alert configuration.

#### Layout

```
+------------------------------------------------------------------+
| TABS: [Companies] [Insiders]                                      |
+------------------------------------------------------------------+
| WATCHLIST TABLE                                                   |
| +--------------------------------------------------------------+ |
| | Ticker | Company | Last Signal | Signal Type | Alert Config  | |
| | AAPL   | Apple   | Mar 9 (Buy) | T3 CEO buy  | [Bell icon]  | |
| | MSFT   | Micro.. | Mar 7 (Sell)| Cluster sell | [Bell icon]  | |
| +--------------------------------------------------------------+ |
| [+ Add Ticker] [+ Add Insider]                                    |
+------------------------------------------------------------------+
| WATCHLIST ACTIVITY FEED (below table)                             |
| Recent trades/events for watchlist items only                     |
+------------------------------------------------------------------+
```

#### Companies Tab

| Column | Type | Notes |
|--------|------|-------|
| Ticker | string | Link to company page |
| Company | string | |
| Last Signal | date + type | Most recent insider trade |
| Signal Summary | string | e.g., "T3 CEO buy $2.1M" or "Cluster sell (3 insiders)" |
| Days Since | integer | Days since last activity |
| Active Insiders | integer | Count of insiders with trades in last 90d |
| Alert Config | icon button | Opens per-item alert configuration |
| Remove | icon button | Remove from watchlist (with confirmation) |

#### Insiders Tab

| Column | Type | Notes |
|--------|------|-------|
| Insider | string + tier badge | Link to profile |
| Primary Title | string | |
| Primary Ticker | string | |
| Score | number | |
| Last Trade | date | |
| Last Trade Detail | string | e.g., "BUY AAPL $1.2M" |
| Alert Config | icon button | |
| Remove | icon button | |

#### Per-Item Alert Configuration (popover on bell icon click)

| Setting | Type | Default |
|---------|------|---------|
| Alert on any buy | Toggle | On |
| Alert on any sell | Toggle | Off |
| Alert on cluster event | Toggle | On |
| Min trade value for alert | Dropdown ($50K, $100K, $500K, $1M) | $100K |
| Min tier for alert | Dropdown (Any, T1+, T2+, T3) | Any |

#### Watchlist Activity Feed

Filtered version of the Live Feed, showing only events for watchlist items. Same row format as `/feed` with expand capability.

Sorted by date descending. Maximum 50 items shown, with "Load more" button.

#### Add to Watchlist

- "Add Ticker" button opens a search modal with ticker autocomplete
- "Add Insider" button opens a search modal with insider name/CIK autocomplete
- Also accessible from: company page header, insider profile header, feed row expand panel, leaderboard context menu

#### Empty State

"Your watchlist is empty. Add companies and insiders to track their activity and get alerts."

With suggestions: "Popular watchlist items: AAPL, MSFT, GOOGL, TSLA, NVDA"

#### Loading State

- Table: 5 skeleton rows
- Activity feed: 5 skeleton rows

#### Mobile

- Tabs as segmented control
- Table as card layout
- Alert config: bottom sheet
- Add: floating action button (FAB) at bottom right
- Activity feed: infinite scroll cards

---

### 5.10 Alert Settings

**URL:** `/settings/alerts`
**Purpose:** Global alert configuration: channels, alert types, thresholds, and quiet hours.

#### Layout

```
+------------------------------------------------------------------+
| SETTINGS NAV: [Profile] [Alerts] [Preferences] [Billing]         |
+------------------------------------------------------------------+
| ALERT CHANNELS                                                    |
| +--------------------------------------------------------------+ |
| | Email        | derek@example.com    | [Enabled toggle] [Test]| |
| | Push         | Browser notifications| [Enabled toggle] [Test]| |
| | Telegram     | @username            | [Connect] / [Connected]| |
| | Webhook      | https://...          | [Configure]            | |
| +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
| ALERT TYPES & THRESHOLDS                                          |
| +--------------------------------------------------------------+ |
| | Alert Type          | Enabled | Threshold Config             | |
| | High-tier buy       | [on]    | Tier 2+, > $100K             | |
| | Cluster event       | [on]    | Confidence > 50, 2+ insiders | |
| | Conviction sell      | [off]   | > $500K, non-routine          | |
| | Watchlist activity   | [on]    | Per-item config               | |
| | Daily digest         | [on]    | 8:00 PM ET                   | |
| | Weekly leaderboard   | [off]   | Sundays 9:00 AM              | |
| +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
| QUIET HOURS                                                       |
| Start: [10:00 PM]  End: [7:00 AM]  Timezone: [US/Eastern v]      |
| [X] Override quiet hours for Tier 3 cluster events                |
+------------------------------------------------------------------+
| [Save Changes]  [Reset to Defaults]                               |
+------------------------------------------------------------------+
```

#### Alert Channels

| Channel | Configuration Fields | Notes |
|---------|---------------------|-------|
| Email | Email address (pre-filled from account) | Toggle on/off, test button sends sample alert |
| Push | Browser notification permission | Request permission flow on toggle-on |
| Telegram | Bot link + chat ID | "Connect" button opens Telegram deep link to @InsiderEdgeBot; once connected, shows "Connected" with chat preview |
| Webhook | URL, optional secret header, payload format (JSON) | "Configure" opens modal with URL input, test button, last 5 delivery statuses |

#### Alert Type Configuration

| Alert Type | Configurable Parameters | Default |
|-----------|------------------------|---------|
| High-Tier Buy | Min tier (1/2/3), min value, C-suite only toggle | Tier 2+, $100K, C-suite off |
| Cluster Event | Min confidence (slider 0-100), min insiders (2-5), buy/sell/both | Conf 50, 2 insiders, buy only |
| Conviction Sell | Min value, min sell WR, sell classification filter | $500K, 55% WR |
| Watchlist Activity | Inherits per-item config from Watchlist page | Per-item |
| Daily Digest | Delivery time, summary content toggles | 8 PM ET, all content |
| Weekly Leaderboard | Delivery day + time, top N insiders | Sunday 9 AM, top 10 |

Each alert type row expands to show its configuration parameters inline.

#### Quiet Hours

| Field | Type | Default |
|-------|------|---------|
| Start time | Time picker | 10:00 PM |
| End time | Time picker | 7:00 AM |
| Timezone | Dropdown | US/Eastern |
| Override for Tier 3 clusters | Checkbox | Checked |

During quiet hours, alerts are queued and delivered at the end of the quiet period, unless the override condition is met.

#### Interactive Elements

- All toggles save immediately (optimistic update with error rollback)
- Test buttons for each channel
- Threshold sliders/inputs validate in real-time
- "Save Changes" button for quiet hours section
- "Reset to Defaults" with confirmation modal

#### Empty State

N/A -- this page always has content (default settings).

#### Loading State

- All sections: skeleton toggles and inputs
- Quick load expected (<500ms from Supabase)

#### Mobile

- Full-width layout, single column
- Channel cards stack vertically
- Alert types as accordion sections
- Quiet hours: simplified time pickers

---

## 6. API Contract Summary

All endpoints served by FastAPI, prefixed `/api/v1/`.

### 6.1 Feed & Filings

| Method | Endpoint | Description | Response |
|--------|----------|-------------|----------|
| GET | `/filings` | Paginated Form 4 filings | `{items: Filing[], total: int, page: int}` |
| GET | `/filings/stream` | SSE stream of new filings | SSE events: `{type: "filing", data: Filing}` |
| GET | `/filings/:id` | Single filing detail | `Filing` with expanded insider record |

**Filing object:**
```typescript
interface Filing {
  trade_id: number;
  ticker: string;
  company: string;
  insider_name: string;
  insider_cik: string;
  insider_id: number;
  title: string;
  trade_type: "buy" | "sell";
  trade_date: string;       // ISO date
  filing_date: string;      // ISO date
  price: number;
  qty: number;
  value: number;
  is_csuite: boolean;
  title_weight: number;
  tier: number;             // 0-3
  score: number;            // 0-3.00
  is_cluster: boolean;
  cluster_id?: number;
  return_7d?: number;
  return_30d?: number;
  return_90d?: number;
  abnormal_7d?: number;
  abnormal_30d?: number;
  abnormal_90d?: number;
}
```

### 6.2 Companies

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/companies/:ticker` | Company overview + insider roster |
| GET | `/companies/:ticker/trades` | Paginated trades at this company |
| GET | `/companies/:ticker/chart` | OHLCV data + insider markers |
| GET | `/companies/:ticker/sentiment` | Insider sentiment time series |

### 6.3 Insiders

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/insiders/:cik` | Insider profile with full track record |
| GET | `/insiders/:cik/trades` | Paginated trade history |
| GET | `/insiders/:cik/companies` | Company history timeline |
| GET | `/insiders/:cik/alpha` | Cumulative alpha time series |

### 6.4 Leaderboard & Screener

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/leaderboard` | Ranked insiders with filters/sort/pagination |
| POST | `/screener` | Complex multi-criteria search (body = filter JSON) |
| GET | `/screener/export` | CSV export of screener results |

### 6.5 Clusters

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/clusters` | Paginated cluster events |
| GET | `/clusters/active` | Currently active clusters (last 7 days) |
| GET | `/clusters/:id` | Single cluster detail with participants |

### 6.6 Sell Signals

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/sells/conviction` | Conviction sells (non-routine, high-value) |
| GET | `/sells/clusters` | Cluster sell events |
| GET | `/sells/watchlist-warnings` | Sells affecting user's watchlist |

### 6.7 Dashboard

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/dashboard/stats` | Today's stat card data |
| GET | `/dashboard/sentiment` | Market-wide sentiment time series |
| GET | `/dashboard/heatmap` | Cluster activity heatmap (90 days) |

### 6.8 User Data (Supabase, via Next.js API routes)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET/POST/DELETE | `/user/watchlist` | CRUD watchlist items |
| GET/POST/PUT/DELETE | `/user/screens` | CRUD saved screens |
| GET/PUT | `/user/alerts` | Get/update alert configuration |
| POST | `/user/alerts/test` | Send test alert to a channel |

### 6.9 Search

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/search?q=...` | Global search (tickers + insiders), returns top 5 each |

---

## 7. Authentication & Authorization

### 7.1 Auth Flow

- **NextAuth.js v5** with two providers:
  - Email magic link (primary)
  - Google OAuth (secondary)
- Session stored in HTTP-only cookie (JWT)
- User record created in Supabase on first login

### 7.2 Access Tiers

| Feature | Anonymous | Free | Pro |
|---------|-----------|------|-----|
| Dashboard | View (cached, 15-min delay) | Real-time | Real-time |
| Live Feed | Last 10 filings | Full feed, 5-min delay | Real-time SSE |
| Company Page | Limited (no returns data) | Full | Full + export |
| Insider Profile | View only | Full | Full + compare |
| Leaderboard | Top 25 | Top 100 | Unlimited + export |
| Clusters | Last 7 days | Last 30 days | Full history + alerts |
| Sell Signals | Hidden | Last 7 days | Full + watchlist warnings |
| Screener | Unavailable | Basic filters | Full filters + save/load |
| Watchlist | Unavailable | 10 items | Unlimited |
| Alerts | Unavailable | Email only, daily digest | All channels, real-time |
| API Access | None | None | REST API with key |

---

## 8. Performance Requirements

| Metric | Target |
|--------|--------|
| First Contentful Paint | < 1.2s |
| Largest Contentful Paint | < 2.5s |
| Time to Interactive | < 3.0s |
| Feed SSE latency (filing to display) | < 5s from EDGAR poll |
| Search autocomplete | < 200ms |
| Table sort/filter (client-side) | < 100ms for 1000 rows |
| API response (p95) | < 500ms |
| Lighthouse score | > 90 (performance) |

### Caching Strategy

| Data | Cache | TTL |
|------|-------|-----|
| Leaderboard | CDN (Vercel edge) | 5 min |
| Company page (chart data) | CDN | 1 min |
| Insider profile | CDN | 5 min |
| Dashboard stats | CDN | 1 min |
| Search autocomplete | Browser (localStorage) | 1 hour |
| User watchlist/alerts | Supabase real-time | Live |

### Database Indexes (API layer)

The existing SQLite schema already has indexes on `trades(ticker)`, `trades(trade_date)`, `trades(filing_date)`, `trades(insider_id)`, and `trades(trade_type)`. Additional composite indexes for the API:

```sql
CREATE INDEX IF NOT EXISTS idx_trades_filing_type ON trades(filing_date DESC, trade_type);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_date ON trades(ticker, filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_tr_score_tier ON insider_track_records(score_tier DESC, score DESC);
CREATE INDEX IF NOT EXISTS idx_tr_buy_wr ON insider_track_records(buy_win_rate_7d DESC) WHERE buy_count >= 3;
```

---

## Appendix A: Key Business Metrics to Track

| Metric | Tracking Method |
|--------|----------------|
| DAU / WAU / MAU | Page views (Vercel Analytics) |
| Feed engagement | Time on feed page, expand rate |
| Watchlist size per user | Supabase aggregate |
| Alert click-through | UTM on alert links |
| Screener saves | Count per user |
| Free-to-Pro conversion | Signup funnel |
| Most-viewed insiders | API access logs |
| Most-watched tickers | Watchlist aggregates |

## Appendix B: Future Considerations (Out of Scope for v1)

- Portfolio simulator ("what if you followed every Tier 3 buy for the last 2 years")
- Options overlay analysis (5% OTM call modeling on insider buys)
- SEC filing NLP (extracting footnotes, derivative transactions, planned dispositions)
- Social features (public watchlists, insider discussion threads)
- Mobile app (React Native, once web is stable)
- Broker integration (one-click trade alongside insider via Alpaca/IBKR)
