import OnboardingForm, { type StrategyChoice } from "./onboarding-form";

/**
 * Onboarding — the strategy picker.
 *
 * THE STATS ARE FETCHED, NEVER TYPED.
 *
 * They were typed until 2026-08-23 and were wrong on seven of nine numbers.
 * The worst was trade frequency: A-List Buys advertised "~40 trades/yr" and
 * delivers ~14, so the first thing a new subscriber saw set their expectation
 * for alert volume at nearly three times reality. Insider Breakout claimed a
 * 70% win rate against an actual 56%.
 *
 * This is the same defect as /performance and /research/methodology, found the
 * same afternoon: prose about the books, with nothing between it and the books.
 * The fix is the same — read the API that computes the figures.
 */

// Rendered per request. Prerendered at build the API is unreachable, so the
// page would ship with no figures at all until the first revalidation — on the
// screen where a subscriber chooses which book to follow.
export const dynamic = "force-dynamic";

const API = process.env.API_URL_INTERNAL || "http://localhost:8000/api/v1";

// Keys and theses only. The public name and every figure come from the API;
// api/public_fields.STRATEGIES is the single definition of the label.
const CHOICES = [
  { value: "quality_notrend", brief: "A proven insider buys — the person is the whole signal" },
  { value: "quality_momentum", brief: "A proven insider buys a stock already trending up" },
  { value: "reversal_dip", brief: "A serial seller finally buys, into a 25% drawdown" },
];

type Summary = {
  strategy_label: string;
  excess_vs_spy: number | null;
  win_rate: number | null;
  total_trades: number | null;
  first_trade: string | null;
};

async function getSummary(key: string): Promise<Summary | null> {
  try {
    const res = await fetch(`${API}/portfolio?strategy=${key}`, {
      next: { revalidate: 3600 },
    });
    if (!res.ok) return null;
    return (await res.json())?.summary ?? null;
  } catch {
    return null;
  }
}

/** Trades per year over the book's actual life, not a remembered round number. */
function perYear(s: Summary): string {
  if (!s.total_trades || !s.first_trade) return "";
  const years = (Date.now() - new Date(s.first_trade).getTime()) / 31_557_600_000;
  if (years < 0.5) return "";
  return ` · ~${Math.round(s.total_trades / years)} trades/yr`;
}

function statLine(s: Summary): string {
  const parts: string[] = [];
  if (typeof s.excess_vs_spy === "number") {
    parts.push(`${s.excess_vs_spy > 0 ? "+" : ""}${s.excess_vs_spy.toFixed(0)} pts vs S&P`);
  }
  if (typeof s.win_rate === "number") parts.push(`${s.win_rate.toFixed(0)}% WR`);
  return parts.join(" · ") + perYear(s);
}

export default async function OnboardingPage() {
  const summaries = await Promise.all(CHOICES.map((c) => getSummary(c.value)));
  const strategies: StrategyChoice[] = CHOICES.map((c, i) => {
    const s = summaries[i];
    return {
      value: c.value,
      // Fall back to the key only if the API is unreachable; never a typed name.
      label: s?.strategy_label ?? c.value,
      brief: c.brief,
      stats: s ? statLine(s) : "",
    };
  });
  return <OnboardingForm strategies={strategies} />;
}
