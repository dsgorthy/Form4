/**
 * Number formatting for OpenGraph cards.
 *
 * Lives here because the filing card had its own inline version that divided
 * by 1,000 and appended "K" with no upper tier, so a $235,750,000 block sale
 * rendered as "$235750K". Each card having its own formatter guarantees one of
 * them is wrong; this is the one place to fix it.
 *
 * Cards are read at a glance in a feed, often scaled down, so precision past
 * one decimal is wasted and length is expensive.
 */

/** Compact currency: $235.8M, $1.2B, $450K, $920. */
export function ogMoney(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  const a = Math.abs(v);
  const sign = v < 0 ? "−" : "";
  if (a >= 1_000_000_000) return `${sign}$${(a / 1_000_000_000).toFixed(1)}B`;
  if (a >= 1_000_000) return `${sign}$${(a / 1_000_000).toFixed(1)}M`;
  if (a >= 1_000) return `${sign}$${Math.round(a / 1_000)}K`;
  return `${sign}$${Math.round(a)}`;
}

/** Compact count: 1.7M, 12.4K, 355. */
export function ogCount(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 10_000) return `${(v / 1_000).toFixed(1)}K`;
  return String(Math.round(v));
}
