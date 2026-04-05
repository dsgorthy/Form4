export interface Filing {
  trade_id: string;
  ticker: string;
  company: string;
  insider_name: string;
  insider_cik: string;
  insider_id: string;
  title: string;
  normalized_title?: string;
  trade_type: "buy" | "sell";
  trade_date: string;
  filing_date: string;
  filed_at?: string;
  price: number;
  qty: number;
  value: number;
  is_csuite: boolean;
  title_weight: number;
  tier: number;
  score: number;
  score_tier?: number;
  is_cluster: boolean;
  cluster_id?: number;
  return_7d?: number;
  return_30d?: number;
  return_90d?: number;
  abnormal_7d?: number;
  abnormal_30d?: number;
  abnormal_90d?: number;
  accession?: string;
  cik?: string;
  trans_code?: string;
  signals?: TradeSignal[];
  signal_types?: string;
  context?: TradeContext[];
  is_10b5_1?: number;
  is_routine?: number;
  price_data_end?: string;
  pit_grade?: string;
  pit_blended_score?: number;
  trade_grade_stars?: number;
  insider_switch_rate?: number;
  is_rare_reversal?: number;
  week52_proximity?: number;
  gated?: boolean;
}

export interface DashboardStats {
  signals_today: number;
  active_clusters: number;
  buy_sell_ratio: number;
  top_mover: { ticker: string; value: number } | null;
}

export interface SentimentPoint {
  date: string;
  buy_value: number;
  sell_value: number;
  ratio: number;
}

export interface HeatmapDay {
  date: string;
  count: number;
  top_ticker: string;
  total_value: number;
}

export interface SellPattern {
  total_sells: number;
  planned_sells: number;
  routine_sells: number;
}

export interface FilingCounts {
  buy: number;
  sell: number;
}

export interface FilingStats {
  buy_win_rate_7d?: number | null;
  buy_avg_return_7d?: number | null;
  buy_avg_abnormal_7d?: number | null;
  sell_win_rate_7d?: number | null;
  sell_avg_return_7d?: number | null;
}

export interface InsiderProfile {
  insider_id: string;
  name: string;
  name_normalized: string;
  cik: string;
  is_entity?: number;
  sell_pattern?: SellPattern;
  filing_counts?: FilingCounts;
  filing_stats?: FilingStats;
  entity_group?: {
    group_id: number;
    group_name: string;
    confidence: number;
    method: string;
    primary_insider_id: string;
    members: {
      insider_id: string;
      name: string;
      is_entity: number;
      is_primary: number;
      relationship: string;
    }[];
  } | null;
  volume_by_type?: {
    trans_code: string;
    label: string;
    trade_type: string;
    count: number;
    total_value: number;
  }[];
  track_record: {
    insider_id: string;
    score: number;
    score_tier: number;
    percentile: number;
    buy_count: number;
    buy_win_rate_7d: number;
    buy_avg_return_7d: number;
    buy_avg_abnormal_7d: number;
    buy_win_rate_30d: number | null;
    buy_avg_return_30d: number | null;
    buy_avg_abnormal_30d: number | null;
    buy_win_rate_90d: number | null;
    buy_avg_return_90d: number | null;
    buy_avg_abnormal_90d: number | null;
    buy_last_date: string | null;
    sell_count: number;
    sell_win_rate_7d: number | null;
    sell_avg_return_7d: number | null;
    sell_win_rate_30d: number | null;
    sell_avg_return_30d: number | null;
    sell_avg_abnormal_30d: number | null;
    sell_win_rate_90d: number | null;
    sell_avg_return_90d: number | null;
    sell_avg_abnormal_90d: number | null;
    sell_last_date: string | null;
    primary_title: string;
    primary_ticker: string;
    n_tickers: number;
    best_window: string | null;
    score_recency_weighted: number;
    tier_recency: string;
  } | null;
}

export interface InsiderCompany {
  ticker: string;
  company: string;
  title: string;
  trade_count: number;
  total_value: number;
  first_trade: string;
  last_trade: string;
}

export interface LeaderboardEntry {
  insider_id: string;
  name: string;
  cik: string;
  primary_title: string;
  primary_ticker: string;
  score: number;
  score_tier: number;
  percentile: number;
  buy_count: number;
  buy_win_rate_7d: number;
  buy_avg_return_7d: number;
  buy_avg_abnormal_7d: number;
  sell_count: number;
  sell_win_rate_7d: number;
  n_tickers: number;
  score_recency_weighted: number;
  tier_recency: string;
  best_pit_grade: string | null;
  best_pit_ticker: string | null;
  n_scored_tickers: number | null;
}

export interface FilingDelayBin {
  label: string;
  count: number;
  pct: number;
}

export interface FilingDelayStats {
  avg_delay: number;
  median_delay: number;
  pct_within_2d: number;
  total: number;
}

export interface FilingDelayData {
  bins: FilingDelayBin[];
  stats: FilingDelayStats;
}

export interface PaginatedResponse<T> {
  total: number;
  limit: number;
  offset: number;
  items: T[];
}

export interface ConvergenceItem {
  ticker: string;
  company: string;
  insider_buys: number;
  insider_total_value: number;
  politician_buys: number;
  politician_total_value_estimate: number;
  first_date: string;
  last_date: string;
}

export interface Inflection {
  ticker: string;
  company: string;
  trade_type: string;
  recent_value: number;
  baseline_weekly_avg: number;
  ratio: number;
  recent_insiders: number;
  latest_filing: string;
}

export interface SellCessationItem {
  insider_id: string;
  name: string;
  cik: string;
  sell_count_12m: number;
  sell_value_12m: number;
  last_sell_date: string;
  days_silent: number;
  tickers: string;
  score: number | null;
  score_tier: number | null;
  pit_grade?: string;
  pit_blended_score?: number;
  best_pit_grade?: string;
  best_pit_ticker?: string;
  n_scored_tickers?: number;
}

export type NotificationEventType =
  | "portfolio_alert"
  | "high_value_filing"
  | "cluster_formation"
  | "activity_spike"
  | "congress_convergence"
  | "watchlist_activity";

export interface Notification {
  id: string;
  event_type: NotificationEventType;
  title: string;
  body: string;
  ticker: string | null;
  is_read: number;
  created_at: string;
}

export interface NotificationPreferences {
  user_id: string;
  email_enabled: boolean;
  in_app_enabled: boolean;
  email_frequency: "realtime" | "daily";
  portfolio_alert: boolean;
  high_value_filing: boolean;
  cluster_formation: boolean;
  activity_spike: boolean;
  congress_convergence: boolean;
  watchlist_activity: boolean;
  min_trade_value: number;
  min_insider_tier: number;
  created_at: string;
  updated_at: string;
}

export interface WatchlistItem {
  ticker: string;
  added_at: string;
}

export interface TradeContext {
  type: string;
  text: string;
}

export interface TradeSignal {
  signal_type: string;
  signal_label: string;
  signal_class: "bullish" | "bearish" | "noise" | "neutral";
  confidence: number;
  metadata?: Record<string, unknown>;
}
