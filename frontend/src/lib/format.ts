export function formatCurrency(value: number | null | undefined): string {
  if (value == null) return "\u2014";
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value.toFixed(0)}`;
}

export function formatPercent(value: number | null | undefined, decimals = 1): string {
  if (value == null) return "\u2014";
  return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(decimals)}%`;
}

/**
 * Check if a return value is permanently unavailable (not just pending).
 * If the trade is old enough that the return should exist but is null,
 * the data is unavailable (ticker delisted, acquired, etc.)
 */
export function isReturnUnavailable(
  tradeDate: string | null | undefined,
  returnValue: number | null | undefined,
  windowDays: number,
): boolean {
  if (returnValue != null || !tradeDate) return false;
  const trade = new Date(tradeDate);
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - windowDays - 14);
  return trade < cutoff;
}

/**
 * Build a tooltip explaining why a return is N/A.
 * Uses price_data_end from the API if available.
 */
export function unavailableReason(priceDataEnd: string | null | undefined): string {
  if (!priceDataEnd || priceDataEnd === "none") {
    return "No price data available for this ticker";
  }
  const d = new Date(priceDataEnd);
  const month = d.toLocaleString("en-US", { month: "short" });
  const year = d.getFullYear();
  return `Price data ends ${month} ${year} \u2014 ticker likely delisted or acquired`;
}

export function formatRelativeTime(dateStr: string): string {
  const now = new Date();
  const date = new Date(dateStr);
  const diffMs = now.getTime() - date.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDays = Math.floor(diffHr / 24);
  if (diffDays <= 7) return `${diffDays}d ago`;
  const month = date.toLocaleString("en-US", { month: "short" });
  const day = date.getDate();
  const year = date.getFullYear();
  if (year === now.getFullYear()) return `${month} ${day}`;
  return `${month} ${day}, ${year}`;
}

export function tierColor(tier: number): string {
  switch (tier) {
    case 3: return "#F59E0B";
    case 2: return "#94A3B8";
    case 1: return "#CD7F32";
    default: return "#55556A";
  }
}

export function tierLabel(tier: number): string {
  return `T${tier}`;
}
