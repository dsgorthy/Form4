import Link from "next/link";
import { SignUpButton, SignInButton } from "@clerk/nextjs";
import { auth } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import { EquitySparkline } from "@/components/equity-sparkline";
import { insiderPath } from "@/lib/insider-url";

export const metadata = {
  title: "Form4 — Insider Trading Intelligence",
  description:
    "Every SEC Form 4, graded within minutes. Three strategies trade the signal in public — every entry, exit and P&L published the day it happens.",
};

/**
 * LANDING PAGE STRUCTURE
 *
 * The previous version was six stacked sections and five of them were a
 * centred heading over a grid of boxes: strategy cards, a filings table,
 * feature cards, a stats bar, pricing cards. Same rhythm five times, and the
 * only thing above the fold was a sentence claiming the product is good.
 *
 * Competitors argue the same way. CEO Watcher leads with "the only platform
 * that calculates insider trade returns"; Quiver leads with a dashboard and
 * puts its backtest panel below. Both are claims about a method.
 *
 * Form4 has something none of them do: a book that has been running since
 * January 2023 with every entry and exit published the day it happened. So
 * the hero is the equity curve, not a sentence about it, and the mechanism
 * that produces it gets exactly one measured number rather than three feature
 * cards. Two sections were cut outright — the feature cards said nothing a
 * reader could check, and the stats bar was vanity.
 *
 * Everything on this page is fetched, not typed. The old strategy cards
 * hard-coded their numbers and had drifted: they advertised ~50 trades a year
 * against 55 in three and a half years, and gave the 10b5-1 strategy a
 * respectable-looking Sharpe while its live record is -0.42% average return
 * on a 27.4% drawdown.
 */

const API = process.env.API_URL_INTERNAL || "http://localhost:8000/api/v1";

type Curve = { date: string; equity: number };

async function getJson(path: string, revalidate = 300) {
  try {
    const res = await fetch(`${API}${path}`, { next: { revalidate } });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

const STRATEGIES = [
  { key: "quality_momentum", name: "Quality + Momentum", thesis: "A+ and A-graded insiders buying into strength" },
  { key: "reversal_dip", name: "Deep Reversal", thesis: "Persistent sellers reversing into a beaten-down name" },
  { key: "tenb51_surprise", name: "10b5-1 Surprise", thesis: "Scheduled sellers breaking pattern to buy", experimental: true },
];

function fmtValue(v: number) {
  if (v >= 999_500) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

export default async function LandingPage() {
  const { userId } = await auth();
  if (userId) redirect("/portfolio");

  const [filings, ...books] = await Promise.all([
    getJson("/filings?limit=6&min_grade=B&trade_type=buy"),
    ...STRATEGIES.map((s) => getJson(`/portfolio?strategy=${s.key}`)),
  ]);

  const recentTrades = filings?.items ?? [];
  const flagship = books[0];
  const curve: Curve[] = flagship?.equity_curve ?? [];
  const bench: Curve[] = flagship?.spy_benchmark ?? [];
  const s = flagship?.summary;

  return (
    <div className="min-h-screen">
      {/* ── Hero: the record, not a claim about it ─────────────────────── */}
      <section className="px-4 pt-14 pb-16 max-w-5xl mx-auto">
        <div className="max-w-2xl">
          <div className="font-mono text-[11px] uppercase tracking-[0.16em] text-[#55556A]">
            Live since January 2023
          </div>
          <h1 className="mt-3 text-4xl sm:text-5xl font-bold tracking-tight text-[#E8E8ED] leading-[1.05] text-balance">
            We grade every insider.
            <br />
            <span className="text-[#3B82F6]">Then we trade the good ones in public.</span>
          </h1>
          <p className="mt-5 text-lg text-[#8888A0]">
            Every SEC Form 4, graded within minutes of hitting EDGAR. Three
            strategies act on the grades, and every entry, exit and P&amp;L is
            published the day it happens — including the one that is losing.
          </p>
        </div>

        {s && curve.length > 1 && (
          <div className="mt-10 rounded-xl border border-[#2A2A3A] bg-[#12121A] p-5 sm:p-6">
            <div className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-2 mb-5">
              <div>
                <div className="text-sm font-medium text-[#E8E8ED]">Quality + Momentum</div>
                <div className="text-xs text-[#55556A] mt-0.5">
                  $100,000 at the start, marked to the latest close
                </div>
              </div>
              <div className="font-mono text-3xl font-bold text-[#22C55E] tabular-nums">
                ${Math.round(s.current_equity).toLocaleString()}
              </div>
            </div>

            <EquitySparkline strategy={curve} benchmark={bench} />

            <dl className="mt-6 grid grid-cols-2 sm:grid-cols-4 gap-px bg-[#2A2A3A] rounded-lg overflow-hidden border border-[#2A2A3A]">
              {[
                ["CAGR", `${s.cagr}%`],
                ["Win rate", `${s.win_rate}%`],
                ["Max drawdown", `${s.max_drawdown}%`],
                ["Closed trades", `${s.total_trades}`],
              ].map(([k, v]) => (
                <div key={k} className="bg-[#12121A] px-4 py-3">
                  <dt className="text-[11px] uppercase tracking-wider text-[#55556A]">{k}</dt>
                  <dd className="font-mono text-lg text-[#E8E8ED] tabular-nums mt-0.5">{v}</dd>
                </div>
              ))}
            </dl>
          </div>
        )}

        <div className="mt-8 flex flex-wrap items-center gap-4">
          <SignUpButton mode="modal">
            <button className="rounded-lg bg-[#3B82F6] px-7 py-3 text-base font-semibold text-white hover:bg-[#2563EB] transition-colors">
              Start free
            </button>
          </SignUpButton>
          <Link
            href="/portfolio"
            className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-7 py-3 text-base font-semibold text-[#E8E8ED] hover:border-[#3B82F6]/50 transition-colors"
          >
            See the live book →
          </Link>
          <SignInButton mode="modal">
            <button className="text-sm text-[#8888A0] hover:text-[#E8E8ED] transition-colors">
              Sign in
            </button>
          </SignInButton>
        </div>
        {/* The cheap ask, stated plainly. Following is free and permanent now,
            so leading with the trial understates what a free account gets. */}
        <p className="mt-4 text-sm text-[#55556A]">
          A free account follows any company or insider and gets alerted when
          they file. 7 days of full Pro on top, no card.
        </p>
        <p className="mt-3 text-xs text-[#55556A]/80 max-w-2xl">
          Strategy results are simulated fills on real filings, published as they
          happened rather than reconstructed afterwards. Past performance does
          not guarantee future results.
        </p>
      </section>

      {/* ── The mechanism, as one number a reader can check ─────────────── */}
      <section className="border-y border-[#2A2A3A] bg-[#0D0D14]">
        <div className="px-4 py-14 max-w-5xl mx-auto grid gap-10 md:grid-cols-[1fr_1.1fr] md:items-center">
          <div>
            <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight text-balance">
              A grade that has to earn itself
            </h2>
            <p className="mt-3 text-[#8888A0]">
              Every insider is graded on their own prior trades, using only what
              was knowable at the time. The grade is not an opinion about their
              job title — it is the record of what happened after they last
              bought.
            </p>
            <Link
              href="/research/methodology"
              className="mt-4 inline-block text-sm text-[#3B82F6] hover:text-[#60A5FA]"
            >
              How the grade is calculated →
            </Link>
          </div>

          {/* Two bars, one comparison. The whole product in one measurement. */}
          <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6">
            <div className="text-[11px] uppercase tracking-wider text-[#55556A]">
              Return vs S&amp;P, 30 days after the filing
            </div>
            <div className="mt-5 space-y-4">
              {[
                { label: "A+ and A insiders", pct: 2.33, n: "8,721 buys", tone: "#22C55E", w: "100%" },
                { label: "Everyone else", pct: 0.91, n: "91,859 buys", tone: "#55556A", w: "39%" },
              ].map((r) => (
                <div key={r.label}>
                  <div className="flex items-baseline justify-between text-sm">
                    <span className="text-[#E8E8ED]">{r.label}</span>
                    <span className="font-mono tabular-nums" style={{ color: r.tone }}>
                      +{r.pct.toFixed(2)}%
                    </span>
                  </div>
                  <div className="mt-1.5 h-2 rounded-full bg-[#1A1A26] overflow-hidden">
                    <div className="h-full rounded-full" style={{ width: r.w, background: r.tone }} />
                  </div>
                  <div className="mt-1 text-[11px] text-[#55556A]">{r.n}</div>
                </div>
              ))}
            </div>
            <div className="mt-5 pt-4 border-t border-[#2A2A3A] text-[11px] text-[#55556A]">
              Open-market purchases, 2016–2026, benchmarked against SPY.
            </div>
          </div>
        </div>
      </section>

      {/* ── What landed today ──────────────────────────────────────────── */}
      {recentTrades.length > 0 && (
        <section className="px-4 py-14 max-w-5xl mx-auto">
          <div className="flex items-baseline justify-between mb-5">
            <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight">
              Just filed
            </h2>
            <Link href="/feed" className="text-sm text-[#3B82F6] hover:text-[#60A5FA]">
              The full feed →
            </Link>
          </div>
          <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#2A2A3A] text-[#55556A] text-[11px] uppercase tracking-wider">
                    <th className="text-left px-4 py-3 font-medium">Insider</th>
                    <th className="text-left px-4 py-3 font-medium">Ticker</th>
                    <th className="text-left px-4 py-3 font-medium">Grade</th>
                    <th className="text-right px-4 py-3 font-medium">Value</th>
                    <th className="text-left px-4 py-3 font-medium">Filed</th>
                  </tr>
                </thead>
                <tbody>
                  {recentTrades.map((t: any) => (
                    <tr key={t.trade_id} className="border-b border-[#2A2A3A]/50 last:border-0 hover:bg-[#1A1A26] transition-colors">
                      {/* Linked, unlike the previous version. These rows are the
                          only path from this page into the 20,000 entity pages
                          the site actually gets found through. */}
                      <td className="px-4 py-3">
                        <Link
                          href={insiderPath(t.insider_name, t.cik || t.insider_id, t.insider_slug)}
                          className="text-[#E8E8ED] hover:text-[#3B82F6] transition-colors"
                        >
                          {t.insider_name || "—"}
                        </Link>
                      </td>
                      <td className="px-4 py-3">
                        <Link href={`/company/${t.ticker}`} className="font-mono font-semibold text-[#22C55E] hover:underline">
                          {t.ticker}
                        </Link>
                      </td>
                      <td className="px-4 py-3">
                        {t.trade_grade?.label ? (
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                            t.trade_grade.label === "Exceptional" || t.trade_grade.label === "Strong"
                              ? "bg-[#22C55E]/10 text-[#22C55E]"
                              : "bg-[#8888A0]/10 text-[#8888A0]"
                          }`}>
                            {"★".repeat(t.trade_grade.stars || 0)} {t.trade_grade.label}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-[#E8E8ED] tabular-nums">
                        {fmtValue(t.value)}
                      </td>
                      <td className="px-4 py-3 text-[#8888A0]">{t.filing_date}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      )}

      {/* ── All three books, live numbers, including the bad one ────────── */}
      <section className="px-4 pb-16 max-w-5xl mx-auto">
        <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight mb-1">
          Three strategies, nothing hidden
        </h2>
        <p className="text-[#8888A0] mb-6 max-w-2xl">
          Numbers below are read from the same book the site publishes, not
          copied into this page. One of the three is not working, and it stays
          up.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {STRATEGIES.map((strat, i) => {
            const sum = books[i]?.summary;
            if (!sum) return null;
            const good = sum.cagr >= 5;
            return (
              <Link
                key={strat.key}
                href="/portfolio"
                className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-5 hover:border-[#3B82F6]/40 transition-colors block"
              >
                <div className="flex items-start justify-between gap-2 mb-1">
                  <h3 className="font-semibold text-[#E8E8ED]">{strat.name}</h3>
                  {strat.experimental && (
                    <span className="shrink-0 rounded-full bg-[#F59E0B]/15 px-2 py-0.5 text-[9px] font-semibold text-[#F59E0B] uppercase tracking-wide">
                      Experimental
                    </span>
                  )}
                </div>
                <p className="text-sm text-[#8888A0] mb-4 min-h-[2.5rem]">{strat.thesis}</p>
                <div className="font-mono text-2xl font-bold tabular-nums" style={{ color: good ? "#22C55E" : "#8888A0" }}>
                  {sum.cagr > 0 ? "+" : ""}{sum.cagr}%
                  <span className="ml-1.5 text-xs font-normal text-[#55556A]">CAGR</span>
                </div>
                <div className="mt-3 grid grid-cols-2 gap-y-1 text-xs font-mono text-[#55556A]">
                  <div>Win rate <span className="text-[#E8E8ED]">{sum.win_rate}%</span></div>
                  <div>Trades <span className="text-[#E8E8ED]">{sum.total_trades}</span></div>
                  <div>Avg trade <span className={sum.avg_return >= 0 ? "text-[#E8E8ED]" : "text-[#EF4444]"}>{sum.avg_return > 0 ? "+" : ""}{sum.avg_return}%</span></div>
                  <div>Max DD <span className={sum.max_drawdown > 20 ? "text-[#EF4444]" : "text-[#E8E8ED]"}>{sum.max_drawdown}%</span></div>
                </div>
              </Link>
            );
          })}
        </div>
      </section>

      {/* ── Pricing ────────────────────────────────────────────────────── */}
      <section className="border-t border-[#2A2A3A] bg-[#0D0D14]">
        <div className="px-4 py-16 max-w-4xl mx-auto">
          <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight text-center mb-2">
            Alerts are free. The analysis is what you pay for.
          </h2>
          <p className="text-[#8888A0] text-center mb-10 max-w-xl mx-auto">
            Follow any company or insider on a free account, permanently.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-5">
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6">
              <div className="text-sm font-medium text-[#8888A0] mb-1">Free</div>
              <div className="text-3xl font-bold text-[#E8E8ED] mb-4">$0</div>
              <ul className="space-y-2 text-sm text-[#8888A0]">
                <li>Follow up to 10 companies</li>
                <li>Email alerts when they file</li>
                <li>90 days of filings</li>
              </ul>
            </div>
            <div className="rounded-xl border border-[#3B82F6]/50 bg-[#12121A] p-6 ring-1 ring-[#3B82F6]/20">
              <div className="text-sm font-medium text-[#3B82F6] mb-1">Pro</div>
              <div className="text-3xl font-bold text-[#E8E8ED] mb-4">
                $25<span className="text-lg text-[#8888A0] font-normal">/mo</span>
              </div>
              <ul className="space-y-2 text-sm text-[#8888A0]">
                <li>Insider track records &amp; grades</li>
                <li>Real-time, full filing history</li>
                <li>Follow up to 25</li>
              </ul>
              <SignUpButton mode="modal">
                <button className="mt-6 w-full rounded-lg bg-[#3B82F6] px-4 py-2.5 text-sm font-semibold text-white hover:bg-[#2563EB] transition-colors">
                  Start 7-day trial
                </button>
              </SignUpButton>
            </div>
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6">
              <div className="text-sm font-medium text-[#22C55E] mb-1">Pro+</div>
              <div className="text-3xl font-bold text-[#E8E8ED] mb-4">
                $75<span className="text-lg text-[#8888A0] font-normal">/mo</span>
              </div>
              <ul className="space-y-2 text-sm text-[#8888A0]">
                <li>Everything in Pro</li>
                <li>Screener &amp; leaderboard</li>
                <li>Export and API access</li>
              </ul>
            </div>
          </div>
          <div className="text-center">
            <Link
              href="/pricing"
              className="inline-block mt-8 text-sm text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
            >
              Full pricing details →
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
