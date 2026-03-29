import Link from "next/link";
import { TierBadge } from "@/components/ui/tier-badge";

export const metadata = {
  title: "How Scoring Works — Form4",
  description:
    "Insider tier scoring methodology: how Form4 ranks insiders based on historical trading performance.",
};

const tiers = [
  {
    tier: 3,
    label: "Elite",
    percentile: "Top 7%",
    description:
      "Consistently profitable insiders with strong alpha across multiple time horizons. These are the highest-conviction signals on the platform.",
  },
  {
    tier: 2,
    label: "Strong",
    percentile: "Top 20% (80th\u201393rd)",
    description:
      "Above-average track records with meaningful alpha. Combined with trade frequency, these insiders produce reliable signals.",
  },
  {
    tier: 1,
    label: "Above Average",
    percentile: "Top 33% (67th\u201380th)",
    description:
      "Better than the majority of scored insiders, but not yet proven enough for high-confidence signals.",
  },
  {
    tier: 0,
    label: "Unproven",
    percentile: "Bottom 67%",
    description:
      "Insufficient data or weak historical performance. Trades from these insiders carry more uncertainty.",
  },
];

const factors = [
  {
    name: "Best-Window Quality",
    weight: "40%",
    description:
      "Win rate + abnormal return at the insider's strongest time horizon (7d, 30d, or 90d). This is the single most important factor \u2014 it captures how well the insider's best trades actually perform.",
  },
  {
    name: "Longer-Horizon Bonus",
    weight: "15%",
    description:
      "Extra credit for alpha at 30-day and 90-day windows. Short-term noise can inflate 7-day numbers, so sustained outperformance over longer periods is weighted more heavily.",
  },
  {
    name: "Multi-Window Consistency",
    weight: "15%",
    description:
      "Percentage of time horizons (7d / 30d / 90d) where the insider shows positive quality. An insider who wins across all three windows is more trustworthy than one who only looks good at a single horizon.",
  },
  {
    name: "Trade Frequency",
    weight: "15%",
    description:
      "More trades means more statistical confidence. Scaled logarithmically \u2014 going from 3 to 10 trades matters more than going from 50 to 100.",
  },
  {
    name: "Window Breadth",
    weight: "10%",
    description:
      "Bonus for having return data across all three time horizons. Insiders with only 7-day data are harder to evaluate than those with full 7d/30d/90d coverage.",
  },
  {
    name: "Trade Size",
    weight: "5%",
    description:
      "Larger dollar purchases signal higher conviction. Also log-scaled \u2014 a $500K buy is more meaningful than a $10K buy, but a $5M buy isn't 10x more meaningful than $500K.",
  },
];

const signalRequirements = [
  { label: "Tier 2+", detail: "Top 20% of scored insiders" },
  { label: "5+ historical buys", detail: "Minimum sample size" },
  { label: "55%+ win rate", detail: "At the 7-day horizon" },
  { label: "1%+ avg 7d return", detail: "Positive expected value" },
];

export default function ScoringPage() {
  return (
    <div className="max-w-3xl">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span className="text-[#8888A0]">Scoring</span>
      </nav>

      {/* Page header */}
      <h1 className="text-2xl font-bold text-[#E8E8ED] mb-2">
        How Scoring Works
      </h1>
      <p className="text-[#8888A0] mb-10 leading-relaxed">
        Every insider with 3+ buy trades receives a score from 0.0 to 3.0 based
        on their historical trading performance. Scores are converted to
        percentile ranks across all scored insiders, then mapped to tiers.
      </p>

      {/* Tier breakdown */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-4">Tiers</h2>
        <div className="space-y-3">
          {tiers.map((t) => (
            <div
              key={t.tier}
              className="flex gap-4 rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4"
            >
              <div className="flex flex-col items-center gap-1 min-w-[72px] pt-0.5">
                <TierBadge tier={t.tier} />
                <span className="text-[10px] text-[#55556A] font-mono">
                  {t.percentile}
                </span>
              </div>
              <div>
                <p className="text-sm font-medium text-[#E8E8ED]">{t.label}</p>
                <p className="text-sm text-[#8888A0] mt-0.5 leading-relaxed">
                  {t.description}
                </p>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Score factors */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">
          Score Calculation
        </h2>
        <p className="text-sm text-[#8888A0] mb-4">
          Each insider&apos;s raw score (0.0 &ndash; 3.0) is built from six
          weighted factors. Abnormal returns are measured against SPY &mdash; a
          positive alpha means the stock outperformed the market after the
          insider&apos;s purchase.
        </p>
        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">
                  Factor
                </th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-20">
                  Weight
                </th>
              </tr>
            </thead>
            <tbody>
              {factors.map((f, i) => (
                <tr
                  key={f.name}
                  className={
                    i < factors.length - 1
                      ? "border-b border-[#2A2A3A]/50"
                      : ""
                  }
                >
                  <td className="px-4 py-3">
                    <p className="font-medium text-[#E8E8ED]">{f.name}</p>
                    <p className="text-[#8888A0] text-xs mt-0.5 leading-relaxed">
                      {f.description}
                    </p>
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-[#E8E8ED] align-top">
                    {f.weight}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Minimums */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">
          Minimum Requirements
        </h2>
        <ul className="space-y-2 text-sm text-[#8888A0]">
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">3+ buy trades</strong> required
              to receive a score. Insiders with fewer trades remain unscored.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">
                Abnormal returns vs SPY
              </strong>{" "}
              &mdash; we measure alpha over the market, not raw returns. A stock
              that rose 5% while SPY rose 4% has +1% alpha.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">
                40% win rate baseline
              </strong>{" "}
              &mdash; below this threshold, the quality component scores zero.
              This prevents low-win-rate insiders from ranking on a few lucky
              outsized returns.
            </span>
          </li>
        </ul>
      </section>

      {/* High-confidence signals */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">
          High-Confidence Signals
        </h2>
        <p className="text-sm text-[#8888A0] mb-4">
          The dashboard&apos;s &ldquo;High-Confidence&rdquo; filter requires all
          four of the following conditions. This is the strictest filter
          available &mdash; only a small fraction of new filings qualify.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {signalRequirements.map((r) => (
            <div
              key={r.label}
              className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-3"
            >
              <p className="text-sm font-medium text-[#E8E8ED]">{r.label}</p>
              <p className="text-xs text-[#55556A] mt-0.5">{r.detail}</p>
            </div>
          ))}
        </div>
      </section>

      {/* How to read the leaderboard */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">
          How to Read the Leaderboard
        </h2>
        <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
          <p>
            The{" "}
            <Link
              href="/leaderboard"
              className="text-[#3B82F6] hover:text-[#60A5FA] transition-colors"
            >
              Leaderboard
            </Link>{" "}
            ranks all scored insiders by their composite score. Here&apos;s what
            each column means:
          </p>
          <ul className="space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Tier</strong> &mdash; the
                insider&apos;s percentile-based tier badge (T0&ndash;T3).
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Score</strong> &mdash; the
                raw composite score (0.0&ndash;3.0) from the six factors above.
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Buys</strong> &mdash; total
                number of buy-side Form 4 filings. More trades = higher
                statistical confidence.
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Win Rate</strong> &mdash;
                percentage of buys that showed positive abnormal returns at the
                7-day mark.
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Avg Alpha</strong> &mdash;
                mean abnormal return (vs SPY) across all buy trades at the 7-day
                horizon.
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
              <span>
                <strong className="text-[#E8E8ED]">Trend</strong> &mdash;
                sparkline of recent trade returns. Upward trend suggests
                improving performance.
              </span>
            </li>
          </ul>
          <p>
            Use the tier and trade-count filters at the top of the leaderboard
            to narrow the list. Sorting by different columns can surface insiders
            optimized for win rate, alpha, or volume.
          </p>
        </div>
      </section>

      {/* CTA */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-6 text-center">
        <p className="text-sm text-[#8888A0] mb-3">
          Ready to explore ranked insiders?
        </p>
        <Link
          href="/leaderboard"
          className="inline-flex rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
        >
          View Leaderboard
        </Link>
      </div>
    </div>
  );
}
