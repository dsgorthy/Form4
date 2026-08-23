import Link from "next/link";
import { currentUser } from "@clerk/nextjs/server";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import { TradeRatingMeter } from "@/components/trade-grade-badge";
import { tradeRatingColor, type TradeRating } from "@/lib/ratings";
import { getUserTier } from "@/lib/subscription";

export const metadata = {
  title: "How Insider Trading Signals Are Scored — Form4.app",
  description:
    "Form4 grades insider trades on three independent signals: Career Grade (long-run track record, A+ to D), Recent Form (recency-weighted, A+ to D), and Trade Grade (per-trade quality, 1-5 stars). Built on Bayesian analysis of 196K+ insider trades.",
  openGraph: {
    title: "How Insider Trading Signals Are Scored — Form4.app",
    description:
      "Three-grade scoring: Career Grade scores the person's track record. Recent Form captures hot/cold streaks. Trade Grade scores each transaction. Bayesian analysis across 196K+ insider trades.",
  },
};

// Measured 2026-08-18: 76,909 open-market buys 2018-2026, held 30 trading
// days, return minus SPY, entry at the first close AFTER the filing was public.
// See api/ratings.py, which is the source of truth for these values.
const insiderGrades = [
  { grade: "A+", share: "3.2%", abnormal: "+4.88%", winRate: "53.7%", description: "The top 3%. Their buys beat the market by the widest margin we measure." },
  { grade: "A", share: "4.0%", abnormal: "+2.00%", winRate: "48.5%", description: "A consistent record of buying ahead of gains." },
  { grade: "B", share: "15.1%", abnormal: "+0.41%", winRate: "47.0%", description: "Slightly better than the market on average." },
  { grade: "C", share: "65.6%", abnormal: "\u22120.25%", winRate: "45.1%", description: "Measured, and no better than the market." },
  { grade: "Unrated", share: "12.1%", abnormal: "+1.41%", winRate: "46.1%", description: "Not enough history at this company to judge \u2014 and not a bad sign. Unrated buys have beaten every graded tier below A." },
];

// Recut 2026-08-18. The previous bands (73/63/55/45) did not separate at the
// top: the best rating returned +1.51% against the second-best's +1.28% and
// had the LOWER win rate. These separate on both measures.
const tradeGradeStars = [
  { stars: 5, range: "80+", label: "Exceptional", share: "1.4%", abnormal: "+2.74%", winRate: "50.1%", description: "Several strong factors at once. The rarest rating we give." },
  { stars: 4, range: "70\u201379", label: "Strong", share: "11.3%", abnormal: "+1.88%", winRate: "49.8%", description: "Clearly above the average filing." },
  { stars: 3, range: "60\u201369", label: "Notable", share: "34.5%", abnormal: "+0.73%", winRate: "47.8%", description: "Something here stands out, but not much." },
  { stars: 2, range: "50\u201359", label: "Modest", share: "35.1%", abnormal: "+0.12%", winRate: "44.4%", description: "Nothing distinguishes this filing." },
  { stars: 1, range: "<50", label: "Weak", share: "17.6%", abnormal: "\u22121.35%", winRate: "42.7%", description: "Negative factors outweigh the positive ones." },
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



/**
 * Pro gate.
 *
 * WITHHOLDS, rather than blurring. Until 2026-08-23 this rendered `children`
 * into the DOM and applied `blur-sm`, so the entire Pro methodology — the
 * strategy theses, the stop levels, the win rates — was readable by anyone
 * with view-source, and served to crawlers. A CSS filter is a visual effect,
 * not access control.
 *
 * The blurred silhouette is kept because it is doing conversion work, but it
 * is now a skeleton with no information in it rather than the real text.
 */
function GateSkeleton() {
  // Deterministic widths — no randomness, so the markup is stable between
  // server and client render.
  const widths = ["92%", "78%", "85%", "61%", "88%", "70%", "94%", "54%"];
  return (
    <div aria-hidden className="space-y-3 select-none pointer-events-none">
      {widths.map((w, i) => (
        <div
          key={i}
          className="h-3 rounded bg-[#2A2A3A]"
          style={{ width: w, opacity: 0.55 }}
        />
      ))}
    </div>
  );
}

function GatedSection({ visible, label, isAuthed, children }: { visible: boolean; label: string; isAuthed?: boolean; children: React.ReactNode }) {
  if (visible) return <>{children}</>;
  const ctaHref = isAuthed ? "/pricing" : "/sign-up";
  const ctaLabel = isAuthed ? "Upgrade to Pro" : "Start Free Trial";
  return (
    <div className="relative my-6 min-h-[280px]">
      <div className="blur-sm"><GateSkeleton /></div>
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
        <Link href="/research" className="hover:text-[#8888A0] transition-colors">Research</Link>
        <span>/</span>
        <span className="text-[#8888A0]">Methodology</span>
      </nav>

      {/* Page header — PUBLIC */}
      <h1 className="text-2xl font-bold text-[#E8E8ED] mb-2">How Scoring Works</h1>
      <p className="text-[#8888A0] mb-4 leading-relaxed">
        Form4 uses three independent grades to evaluate insider trading signals.
        Each answers a different question:
      </p>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-10">
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
          <p className="text-sm font-semibold text-[#E8E8ED] mb-1">Insider Rating</p>
          <p className="text-sm text-[#8888A0]">
            <strong className="text-[#E8E8ED]">WHO</strong> they are &mdash; how this insider&apos;s own past buys performed. One rating per person, per company.
          </p>
        </div>
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
          <p className="text-sm font-semibold text-[#E8E8ED] mb-1">Tags</p>
          <p className="text-sm text-[#8888A0]">
            <strong className="text-[#E8E8ED]">WHAT HAPPENED</strong>. Facts about the filing &mdash; bought a dip, first purchase, unusually large. A filing carries as many as apply, and they never rate it.
          </p>
        </div>
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4">
          <p className="text-sm font-semibold text-[#E8E8ED] mb-1">Trade Rating</p>
          <p className="text-sm text-[#8888A0]">
            <strong className="text-[#E8E8ED]">HOW NOTABLE</strong> this one filing is. Twelve factors, including the insider&apos;s rating. One rating per filing.
          </p>
        </div>
      </div>

      {/* ─── CAREER GRADE ─── PUBLIC (thresholds gated to authed) */}
      <section className="mb-14">
        <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Career Grade</h2>
        <p className="text-sm text-[#55556A] font-mono mb-4">A+ / A / B / C / D / New</p>
        <p className="text-sm text-[#8888A0] mb-6 leading-relaxed">
          Every insider with observable buy trades receives a point-in-time (PIT)
          career score. It uses the full history of trades whose forward returns
          are knowable as of the scoring date &mdash; no look-ahead bias. The score
          uses a 5-year half-life decay, so old trades retain meaningful weight
          and an insider who was strong historically doesn&apos;t lose grade just
          because they&apos;ve been quiet recently.
        </p>

        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A] mb-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-24">Rating</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-24">Share</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-28">Avg 30d vs S&amp;P</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-24">Win rate</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">What it means</th>
              </tr>
            </thead>
            <tbody>
              {insiderGrades.map((g, i) => (
                <tr key={g.grade} className={i < insiderGrades.length - 1 ? "border-b border-[#2A2A3A]/50" : ""}>
                  <td className="px-4 py-3 align-top">
                    <InsiderGradeBadge rating={g.grade} />
                  </td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#55556A] align-top">{g.share}</td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#E8E8ED] align-top">{g.abnormal}</td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#8888A0] align-top">{g.winRate}</td>
                  <td className="px-4 py-3 text-[#8888A0] leading-relaxed">{g.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Methodology — public shows headlines, authed shows specifics */}
        <h3 className="text-sm font-semibold text-[#E8E8ED] mb-3">How the Career Score is Calculated</h3>
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

      {/* ─── RECENT FORM ─── PUBLIC */}
      <section className="mb-14">
        <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Recent Form</h2>
        <p className="text-sm text-[#55556A] font-mono mb-4">A+ / A / B / C / D / New</p>
        <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
          <p>
            Same Bayesian framework as Career Grade, but with a <strong className="text-[#E8E8ED]">1.5-year half-life</strong>.
            Captures whether the insider is currently in form &mdash; their recent
            trades dominate the score, older trades fade.
          </p>
          <p>
            An insider who&apos;s been quiet for 4+ years can have <em>strong Career,
            weak Recent Form</em>. An insider who&apos;s been hot lately but
            historically mediocre is the opposite. The two grades together tell
            you both the long-run skill and the short-run state.
          </p>
          <p>
            Form4&apos;s strategies use whichever grade empirically performs best in
            backtests for that specific signal. Quality-momentum and reversal-dip
            filter on Career Grade; 10b5-1 surprise filters on Recent Form.
          </p>
        </div>
      </section>

      {/* ─── PER-TICKER GRADES ─── PUBLIC */}
      <section className="mb-14">
        <h2 className="text-lg font-semibold text-[#E8E8ED] mb-2">Per-Ticker Grades</h2>
        <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
          <p>
            Career Grades are computed <strong className="text-[#E8E8ED]">per ticker</strong> because
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
          Every filing is scored on twelve factors \u2014 including the insider's own rating \u2014 and lands in one of five bands. Figures below are measured from the first close after the filing was public, not from the transaction date.
        </p>

        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A] mb-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-4 py-3 text-left text-[#55556A] font-medium w-32">Rating</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-24">Share</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-28">Avg 30d vs S&amp;P</th>
                <th className="px-4 py-3 text-right text-[#55556A] font-medium w-24">Win rate</th>
                <th className="px-4 py-3 text-left text-[#55556A] font-medium">What it means</th>
              </tr>
            </thead>
            <tbody>
              {tradeGradeStars.map((s, i) => (
                <tr key={s.stars} className={i < tradeGradeStars.length - 1 ? "border-b border-[#2A2A3A]/50" : ""}>
                  <td className="px-4 py-3 align-top">
                    {/* Same meter and colour the product uses, from
                        lib/ratings — this page had its own star renderer with
                        its own palette table, which is how it came to disagree
                        with the code about what 2 stars was called. */}
                    <span className="inline-flex items-center gap-2">
                      <TradeRatingMeter score={s.range === "<50" ? 40 : parseInt(s.range, 10)} />
                      <span className="text-xs font-medium" style={{ color: tradeRatingColor(s.label as TradeRating) }}>
                        {s.label}
                      </span>
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#55556A] align-top">{s.share}</td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#E8E8ED] align-top">{s.abnormal}</td>
                  <td className="px-4 py-3 text-right font-mono tabular-nums text-[#8888A0] align-top">{s.winRate}</td>
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
            <Link href="/feed" className="text-[#3B82F6] hover:text-[#60A5FA] transition-colors">Feed</Link>
            , you can filter by both grades to find the intersection that matches your conviction level.
          </p>
        </div>
      </section>

      {/* ─── PORTFOLIO METHODOLOGY ─── PRO ONLY */}
      <GatedSection visible={isPro} label="Portfolio strategies are Pro-only" isAuthed={isAuthed}>
        <section className="mb-14">
          <h2 className="text-xl font-bold text-[#E8E8ED] mb-1">Portfolio Strategies</h2>
          <p className="text-sm text-[#55556A] font-mono mb-4">3 validated strategies</p>
          <p className="text-sm text-[#8888A0] mb-6 leading-relaxed">
            Form4 runs three independent portfolio strategies, each built from
            signals that passed rigorous out-of-sample validation on 196K+ trades.
            Every parameter was selected via grid search across 1,350 configurations.
          </p>

          <div className="space-y-4 mb-8">
            {[
              { name: "A-List Buys", hold: "42 trading days", thesis: "An A+/A-graded insider buys, and that is the whole test — no condition on the chart at all. The trend filter the other book applies turns out to cost more in trades foregone than it saves in bad ones." },
              { name: "Insider Breakout", hold: "42 trading days", thesis: "The same graded insider, but the stock is already above its 50- and 200-day averages. Conviction and price pointing the same way, at the cost of a much smaller book." },
              { name: "Insider Dip Buys", hold: "21 trading days", thesis: "An insider who has sold 10+ consecutive times finally buys, while the stock is down 25%+ over three months. A behavioural reversal with a margin of safety attached." },
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

          <h3 className="text-sm font-semibold text-[#E8E8ED] mb-3">Stops, and why they differ by strategy</h3>
          <div className="space-y-3 text-sm text-[#8888A0] leading-relaxed">
            <p>
              All three strategies exit on a fixed holding period. The stop is a
              second exit underneath that, and it is{" "}
              <strong className="text-[#E8E8ED]">not the same on every book</strong>,
              because the same stop does different work depending on how
              concentrated the positions are.
            </p>
            <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/40 p-4 font-mono text-xs">
              <div className="flex justify-between py-1 border-b border-[#2A2A3A]/60">
                <span className="text-[#8888A0]">A-List Buys</span>
                <span className="text-[#E8E8ED]">&minus;50% &middot; backstop, never triggered</span>
              </div>
              <div className="flex justify-between py-1 border-b border-[#2A2A3A]/60">
                <span className="text-[#8888A0]">Insider Breakout</span>
                <span className="text-[#E8E8ED]">&minus;20% &middot; working stop, 19 exits</span>
              </div>
              <div className="flex justify-between py-1">
                <span className="text-[#8888A0]">Insider Dip Buys</span>
                <span className="text-[#E8E8ED]">&minus;50% &middot; backstop, never triggered</span>
              </div>
            </div>
            <p>
              Insider Breakout holds five positions at 20% each and is the most
              volatile of the three. A working stop at{" "}
              <strong className="text-[#E8E8ED]">&minus;20%</strong> cut its worst
              drawdown from 49.9% to 43.8%{" "}
              <em>and raised</em> its return &mdash; the rare case where tightening
              risk costs nothing. It has closed 19 of that book&rsquo;s 85 positions.
            </p>
            <p>
              The other two books concentrate less and recover more reliably, so
              their &minus;50% is a catastrophe backstop rather than a trading
              rule. Neither has ever triggered it, though not by a wide margin
              &mdash; the worst position A-List has held closed at{" "}
              <strong className="text-[#E8E8ED]">&minus;47.5%</strong>.
            </p>
            <p>
              The stop is checked on the{" "}
              <strong className="text-[#E8E8ED]">closing price</strong>, so a stock
              that gaps down through it fills below the level. Thirteen of the
              nineteen stops closed past &minus;20% for exactly that reason, the
              worst at &minus;29.3%. A stop is a backstop, not a guarantee, and the
              published returns include those overshoots rather than assuming a
              clean exit.
            </p>
            <p>
              Checking on the close rather than the intraday low is also
              deliberate. Four positions traded through their stop level during the
              day and closed back above it; cutting them at the low would have
              turned a{" "}
              <strong className="text-[#E8E8ED]">+286.9%</strong> position into a
              &minus;20% loss. Tight and trailing stops were tested across the full
              grid and reduced risk-adjusted returns on every book.
            </p>
            <p>
              These strategies have a{" "}
              <strong className="text-[#E8E8ED]">positive skew profile</strong>: win
              rates of 56&ndash;68%, with winners averaging 1.8&ndash;2.4x the size
              of losers. The fixed holding periods &mdash; 21 to 42 trading days
              &mdash; are the primary risk management.
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
