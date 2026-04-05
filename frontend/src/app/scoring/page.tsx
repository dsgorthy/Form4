import Link from "next/link";
import { currentUser } from "@clerk/nextjs/server";
import { TierBadge } from "@/components/ui/tier-badge";
import { getUserTier } from "@/lib/subscription";

export const metadata = {
  title: "How Insider Trading Signals Are Scored — Form4.app",
  description:
    "Form4 uses a two-tier grading system to rank SEC Form 4 insider trades. Insider Grade (A+ to D) measures the person's track record. Trade Grade (1-5 stars) scores each transaction on 13 factors. Built on Bayesian analysis of 196K+ trades.",
  openGraph: {
    title: "How Insider Trading Signals Are Scored — Form4.app",
    description:
      "Two-tier grading system: Insider Grade scores the person, Trade Grade scores the transaction. Bayesian analysis across 196K+ insider trades.",
  },
};

const insiderGrades = [
  { grade: "A+", threshold: ">=2.5", description: "Exceptional track record. Consistently profitable across multiple time horizons with strong abnormal returns." },
  { grade: "A", threshold: ">=2.0", description: "Strong track record. Reliable alpha generation with good win rates across most windows." },
  { grade: "B", threshold: ">=1.2", description: "Above-average performer. Positive alpha with reasonable consistency." },
  { grade: "C", threshold: ">=0.6", description: "Below-average track record. Some positive signals but inconsistent performance." },
  { grade: "D", threshold: ">=0.0", description: "Weak track record. Minimal or negative alpha historically." },
  { grade: "New", threshold: "No data", description: "Insufficient history to score." },
];

const tradeGradeStars = [
  { stars: 5, range: "73+", label: "Exceptional", avgReturn: "+4.78%", publicReturn: "Strong positive", description: "Top-tier transaction with multiple strong factors aligning." },
  { stars: 4, range: "63-72", label: "Strong", avgReturn: "Positive", publicReturn: "Positive", description: "Well above average. Several favorable factors present." },
  { stars: 3, range: "55-62", label: "Average", avgReturn: "Neutral", publicReturn: "Neutral", description: "Typical transaction. Some positive factors but nothing that strongly distinguishes it." },
  { stars: 2, range: "45-54", label: "Below Average", avgReturn: "Weak", publicReturn: "Weak", description: "Below baseline. May have negative factors." },
  { stars: 1, range: "<45", label: "Poor", avgReturn: "-0.74%", publicReturn: "Negative", description: "Multiple negative factors." },
];

const tradeFactors = [
  { name: "Insider PIT Grade", description: "The insider's point-in-time quality grade at the time of the trade" },
  { name: "Role", description: "C-suite at primary company weighted more heavily" },
  { name: "Cluster Size", description: "Multiple insiders buying the same stock within a short window" },
  { name: "Dip Depth", description: "Stock is trading well below recent highs — potential value opportunity" },
  { name: "Opportunistic", description: "Trade appears discretionary rather than part of a compensation plan" },
  { name: "Pre-Planned (negative)", description: "10b5-1 plan trades are less informative — penalized" },
  { name: "Routine (negative)", description: "Regular periodic buying patterns suggest non-signal trades — penalized" },
  { name: "Rare Reversal", description: "Insider who typically sells is now buying — unusual conviction" },
  { name: "Holdings Increase", description: "The purchase meaningfully increases the insider's position size" },
  { name: "Trade Value", description: "Larger dollar amounts signal stronger conviction" },
  { name: "52-Week Proximity", description: "Buying near 52-week highs signals confidence — validated by Lasfer 2024 (12.8% annual alpha)" },
  { name: "Largest Ever", description: "This is the insider's largest-ever purchase in this stock" },
];

function StarDisplay({ count }: { count: number }) {
  return (
    <span className="font-mono text-[#F59E0B] tracking-wide">
      {"*".repeat(count)}
      <span className="text-[#2A2A3A]">{"*".repeat(5 - count)}</span>
    </span>
  );
}

function GatedSection({ visible, label, isAuthed, children }: { visible: boolean; label: string; isAuthed?: boolean; children: React.ReactNode }) {
  if (visible) return <>{children}</>;
  const ctaHref = isAuthed ? "/pricing" : "/sign-up";
  const ctaLabel = isAuthed ? "Upgrade to Pro" : "Start Free Trial";
  return (
    <div className="relative my-6">
      <div className="blur-sm select-none pointer-events-none">{children}</div>
      <div className="absolute inset-0 z-10 bg-gradient-to-b from-transparent via-[#0A0A0F]/60 to-[#0A0A0F]/90 flex items-center justify-center">
        <div className="text-center">
          <div className="text-xs font-semibold uppercase tracking-widest text-[#8888A0] mb-2">{label}</div>
          <Link
            href={ctaHref}
            className="inline-flex rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
          >
            {ctaLabel}
          </Link>
        </div>
      </div>
    </div>
  );
}

export default async function ScoringPage() {
  const user = await currentUser();
  const tier = getUserTier(user ? { publicMetadata: user.publicMetadata, createdAt: user.createdAt } : null);
  const isAuthed = !!user;
  const isPro = tier === "pro" || tier === "trial";

  return (
    <div className="max-w-3xl">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">Dashboard</Link>
        <span>/</span>
        <span className="text-[#8888A0]">Scoring</span>
      </nav>

      {/* Page header — PUBLIC */}
      <h1 className="text-2xl font-bold text-[#E8E8ED] mb-2">How Scoring Works</h1>
      <p className="text-[#8888A0] mb-4 leading-relaxed">
        Form4 uses a two-tier grading system to evaluate insider trading signals.
        Each tier answers a different question:
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 mb-10">
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
          <p className="text-sm font-semibold text-[#E8E8ED] mb-1">Insider Grade</p>
          <p className="text-sm text-[#8888A0]">
            Tells you <strong className="text-[#E8E8ED]">WHO</strong> is trading. Scores the person based on their historical track record.
          </p>
        </div>
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
          <p className="text-sm font-semibold text-[#E8E8ED] mb-1">Trade Grade</p>
          <p className="text-sm text-[#8888A0]">
            Tells you <strong className="text-[#E8E8ED]">HOW GOOD</strong> this specific trade looks. Scores the transaction based on 13 factors.
          </p>
        </div>
      </div>

      {/* ─── INSIDER GRADE ─── PUBLIC (thresholds gated to authed) */}
      <section className="mb-14">
        <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Insider Grade</h2>
        <p className="text-sm text-[#55556A] font-mono mb-4">A+ / A / B / C / D / New</p>
        <p className="text-sm text-[#8888A0] mb-6 leading-relaxed">
          Every insider with historical buy trades receives a point-in-time (PIT)
          quality score that measures their personal track record. The score only
          uses data that was available at the time of each trade &mdash; no look-ahead bias.
        </p>

        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A] mb-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-20">Grade</th>
                {isAuthed && <th className="px-4 py-3 text-left text-[#55556A] font-medium w-28">Score</th>}
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">Description</th>
              </tr>
            </thead>
            <tbody>
              {insiderGrades.map((g, i) => (
                <tr key={g.grade} className={i < insiderGrades.length - 1 ? "border-b border-[#2A2A3A]/50" : ""}>
                  <td className="px-4 py-3 align-top">
                    <TierBadge pitGrade={g.grade === "New" ? undefined : g.grade} />
                  </td>
                  {isAuthed && (
                    <td className="px-4 py-3 font-mono text-[#E8E8ED] align-top">{g.threshold}</td>
                  )}
                  <td className="px-4 py-3 text-[#8888A0] leading-relaxed">{g.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Methodology — public shows headlines, authed shows specifics */}
        <h3 className="text-sm font-semibold text-[#E8E8ED] mb-3">How the Insider Score is Calculated</h3>
        <ul className="space-y-2.5 text-sm text-[#8888A0] leading-relaxed">
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Bayesian shrinkage</strong>
              {isAuthed ? " with Beta(2,2) prior" : ""} &mdash; insiders with few trades are pulled toward the population average. More trades earn more weight on actual performance.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Recency-weighted</strong>
              {isAuthed ? " (1.5-year half-life)" : ""} &mdash; recent trades count more than older ones.
              {isAuthed ? " A trade from 18 months ago has half the weight of one filed today." : ""}
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Multi-window evaluation</strong>
              {isAuthed ? " (7d, 30d, 90d)" : ""} &mdash; forward returns are measured at multiple horizons. Consistency across windows is rewarded.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Ticker-specific + global blend</strong> &mdash; the score blends how the insider performs in a specific stock with their performance across all stocks they trade.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Role-adjusted for C-suite</strong> &mdash; officers at their primary company receive an adjustment reflecting their superior information access.
            </span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-[#3B82F6] mt-1 shrink-0">&bull;</span>
            <span>
              <strong className="text-[#E8E8ED]">Abnormal returns vs SPY</strong> &mdash; all returns are measured as alpha over the market.
            </span>
          </li>
        </ul>
        {!isAuthed && (
          <div className="mt-6 rounded-lg border border-[#3B82F6]/20 bg-[#3B82F6]/5 p-4 text-center">
            <p className="text-sm text-[#8888A0] mb-3">Sign in to see exact scoring thresholds and algorithm parameters</p>
            <Link
              href="/sign-up"
              className="inline-flex rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              Start Free Trial
            </Link>
            <p className="text-[10px] text-[#55556A] mt-2">7 days of full Pro access — no credit card required</p>
          </div>
        )}
      </section>

      {/* ─── PER-TICKER GRADES ─── PUBLIC */}
      <section className="mb-14">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">Per-Ticker Grades</h2>
        <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
          <p>
            Insider Grades are computed <strong className="text-[#E8E8ED]">per ticker</strong> because
            an insider&apos;s information advantage depends on which company they serve.
          </p>
          <p>
            On individual trade rows, you see the grade for <strong className="text-[#E8E8ED]">that specific ticker</strong> at
            the time of the trade. On the{" "}
            <Link href="/leaderboard" className="text-[#3B82F6] hover:text-[#60A5FA] transition-colors">Leaderboard</Link>
            {" "}you see the insider&apos;s <strong className="text-[#E8E8ED]">best grade</strong> across all tickers.
          </p>
        </div>
      </section>

      {/* ─── TRADE GRADE ─── PUBLIC (alpha numbers gated to authed) */}
      <section className="mb-14">
        <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Trade Grade</h2>
        <p className="text-sm text-[#55556A] font-mono mb-4">1 to 5 stars</p>
        <p className="text-sm text-[#8888A0] mb-6 leading-relaxed">
          Each individual transaction is scored on 13 trade-level factors and mapped to a star rating.
        </p>

        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A] mb-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-24">Rating</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-28">Avg 30d Alpha</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">Description</th>
              </tr>
            </thead>
            <tbody>
              {tradeGradeStars.map((s, i) => (
                <tr key={s.stars} className={i < tradeGradeStars.length - 1 ? "border-b border-[#2A2A3A]/50" : ""}>
                  <td className="px-4 py-3 align-top">
                    <StarDisplay count={s.stars} />
                    <span className="text-[10px] text-[#55556A] ml-1.5">{s.label}</span>
                  </td>
                  <td className="px-4 py-3 font-mono text-[#E8E8ED] align-top">
                    {isAuthed ? s.avgReturn : s.publicReturn}
                  </td>
                  <td className="px-4 py-3 text-[#8888A0] leading-relaxed">{s.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* 13 factors — names PUBLIC, detail paragraph PRO */}
        <h3 className="text-sm font-semibold text-[#E8E8ED] mb-3">The 13 Factors</h3>

        {isPro && (
          <p className="text-sm text-[#8888A0] mb-4 leading-relaxed">
            Each factor adds to or subtracts from the baseline score of 50. Two
            factors (pre-planned and routine) are negative &mdash; they reduce the
            score because those trade types carry less informational value.
          </p>
        )}

        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-10">#</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">Factor</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">What It Measures</th>
              </tr>
            </thead>
            <tbody>
              {tradeFactors.map((f, i) => (
                <tr key={f.name} className={i < tradeFactors.length - 1 ? "border-b border-[#2A2A3A]/50" : ""}>
                  <td className="px-4 py-3 font-mono text-[#55556A] align-top">{i + 1}</td>
                  <td className="px-4 py-3 align-top">
                    <span className={`font-medium ${f.name.includes("(negative)") ? "text-[#EF4444]" : "text-[#E8E8ED]"}`}>
                      {f.name}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-[#8888A0] leading-relaxed">{f.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* ─── PUTTING IT TOGETHER ─── PUBLIC */}
      <section className="mb-12">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">Putting It Together</h2>
        <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
          <p>
            The strongest signals come from trades where both grades are high: an
            A-rated insider making a 5-star trade. But the two grades are
            intentionally independent &mdash; a new insider with no history can
            still have a high Trade Grade if the transaction characteristics are strong.
          </p>
          <p>
            On the{" "}
            <Link href="/" className="text-[#3B82F6] hover:text-[#60A5FA] transition-colors">Dashboard</Link>
            , you can filter by both grades to find the intersection that matches your conviction level.
          </p>
        </div>
      </section>

      {/* ─── PORTFOLIO METHODOLOGY ─── PRO ONLY */}
      <GatedSection visible={isPro} label="Portfolio strategies are Pro-only" isAuthed={isAuthed}>
        <section className="mb-14">
          <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Portfolio Strategies</h2>
          <p className="text-sm text-[#55556A] font-mono mb-4">4 validated strategies</p>
          <p className="text-sm text-[#8888A0] mb-6 leading-relaxed">
            Form4 runs three independent portfolio strategies, each built from
            signals that passed rigorous out-of-sample validation on 196K+ trades.
            Every parameter was selected via grid search across 1,350 configurations.
          </p>

          <div className="space-y-4 mb-8">
            {[
              { name: "Quality + Momentum", hold: "30 days", thesis: "A+/A-graded insiders buying stocks in confirmed uptrends (above SMA50 and SMA200). The signal compounds — proven insiders with strong track records buying into momentum." },
              { name: "Deep Reversal", hold: "21 days", thesis: "Insiders who sold 10+ consecutive times then suddenly buy, while the stock is down 25%+ over 3 months. A behavioral reversal combined with a margin of safety." },
              { name: "10b5-1 Surprise", hold: "60 days (trailing stop 15%)", thesis: "Insiders who had SEC-filed 10b5-1 selling plans break pattern and buy. Breaking a legal commitment to buy is costly and deliberate." },
              { name: "Reversal + Quality", hold: "30 days", thesis: "Proven insiders (A-B grade) who persistently sold suddenly buy. The strongest single signal: +4.0% abnormal at 30d, 61% win rate. Robust across years, not driven by repeat insiders." },
            ].map((s) => (
              <div key={s.name} className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-semibold text-[#E8E8ED]">{s.name}</span>
                  <span className="text-xs font-mono text-[#55556A]">{s.hold}</span>
                </div>
                <p className="text-sm text-[#8888A0] leading-relaxed">{s.thesis}</p>
              </div>
            ))}
          </div>

          <h3 className="text-sm font-semibold text-[#E8E8ED] mb-3">Why No Stop Losses on Two Strategies?</h3>
          <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
            <p>
              Quality + Momentum and Deep Reversal use fixed holding periods with
              no stop losses. This is a deliberate, data-driven decision.
            </p>
            <p>
              Our grid search tested every combination of stop losses and trailing stops.{" "}
              <strong className="text-[#E8E8ED]">Stop losses reduced risk-adjusted returns by 18&ndash;25%</strong>.
              Quality insider picks recover from temporary dips, and stops trigger whipsaw exits.
            </p>
            <p>
              These strategies have a <strong className="text-[#E8E8ED]">positive skew profile</strong>:
              win rates of 55&ndash;61% with winners averaging 2&ndash;3x the size of losers.
              The short fixed holding periods (21&ndash;30 days) are the risk management.
            </p>
          </div>
        </section>
      </GatedSection>

      {/* CTA */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-6 text-center">
        <p className="text-sm text-[#8888A0] mb-3">
          {isPro ? "Ready to explore ranked insiders?" : "Unlock full scoring methodology and portfolio strategies"}
        </p>
        <Link
          href={isPro ? "/leaderboard" : isAuthed ? "/pricing" : "/sign-up"}
          className="inline-flex rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
        >
          {isPro ? "View Leaderboard" : isAuthed ? "Upgrade to Pro" : "Start Free Trial"}
        </Link>
      </div>
    </div>
  );
}
