import Link from "next/link";
import { SignUpButton, SignInButton } from "@clerk/nextjs";
import { auth } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import { EquitySparkline } from "@/components/equity-sparkline";
import { insiderPath } from "@/lib/insider-url";
import { insiderRating, insiderRatingColor } from "@/lib/ratings";

export const metadata = {
  title: "Form4 — Insider Trading Intelligence",
  description:
    "Every SEC Form 4, graded within minutes. Three strategies trade the signal in public — every entry, exit and P&L published the day it happens.",
};

/**
 * LANDING PAGE STRUCTURE
 *
 * The previous rebuild fixed the argument and not the layout. It ran hero →
 * comparison → table → cards → pricing: four full-width sections of the same
 * weight, each a heading over a box, so everything after the hero ran
 * together. Nothing on the page told you what mattered most because every
 * block was the same size.
 *
 * The fix is asymmetry rather than more content. A bento grid — one large
 * tile carrying the argument, small tiles carrying support — makes the layout
 * itself communicate priority, which is why roughly two thirds of the top SaaS
 * sites now use one. Each section also gets a distinct shape and ground so the
 * eye has somewhere to break: a full-bleed chart, an uneven grid, a two-thirds
 * split, a pricing row.
 *
 * Numbers are fetched, never typed. An earlier version hard-coded strategy
 * stats that had drifted so far they advertised "~50 trades/yr" against 55 in
 * three and a half years.
 *
 * THE HEADLINE FIGURE
 *
 * The hero shows the blended book — the insider strategy with idle cash held
 * in SPY — against the same capital in SPY alone. That is the honest
 * comparison: the earlier version showed the insider sleeve in isolation,
 * which penalised it for holding cash it was never going to leave idle, and
 * made a 31.7% CAGR look like 17%.
 */

const API = process.env.API_URL_INTERNAL || "http://localhost:8000/api/v1";

type Curve = { date: string; equity: number };

type RecentTrade = {
  trade_id: number | string;
  ticker: string;
  insider_name: string | null;
  insider_id: number | string | null;
  insider_slug: string | null;
  cik: string | null;
  value: number;
  /**
   * career_grade, NOT pit_grade. This page published pit_grade until
   * 2026-08-21, which api/ratings.py forbids: it is an input to the Trade
   * Rating, it is not monotonic (C ranks below D on mean return), and the
   * published scale has no D at all because _GRADE_DISPLAY merges D into C.
   *
   * 34,198 filings showed a letter here while the insider page said Unrated,
   * and 18,241 of those were labelled "D" — a grade the product does not
   * offer, applied to a cohort that OUTPERFORMS every measured grade below A
   * (+1.41% vs C -0.38%). It inverted the meaning rather than merely
   * disagreeing.
   */
  career_grade: string | null;
};

type BookSummary = {
  cagr: number;
  total_trades: number;
  wins: number;
  win_rate: number;
};

async function getJson(path: string, revalidate = 300) {
  try {
    const res = await fetch(`${API}${path}`, { next: { revalidate } });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// Display names; the key is the internal identifier. Order is deliberate — the
// A-List leads because it is the strongest book, and the landing page reads the
// first entry for the hero chart.
const STRATEGIES = [
  { key: "quality_notrend", name: "A-List Buys", thesis: "A proven insider buys. No chart condition." },
  { key: "quality_momentum", name: "Insider Breakout", thesis: "A proven insider buys a stock already trending up" },
  { key: "reversal_dip", name: "Insider Dip Buys", thesis: "A serial seller finally buys, into a 25% drawdown" },
];

function fmtValue(v: number) {
  if (v >= 999_500) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function cagr(from: number, to: number, years: number) {
  return ((to / from) ** (1 / years) - 1) * 100;
}

function maxDrawdown(series: number[]) {
  let peak = series[0] ?? 0;
  let worst = 0;
  for (const v of series) {
    peak = Math.max(peak, v);
    if (peak > 0) worst = Math.max(worst, (peak - v) / peak);
  }
  return worst * 100;
}

/** Small tile used across the bento grid. */
function Tile({
  children, className = "", span = "",
}: { children: React.ReactNode; className?: string; span?: string }) {
  return (
    <div className={`rounded-xl border border-[#2A2A3A] bg-[#12121A] p-5 ${span} ${className}`}>
      {children}
    </div>
  );
}

function TileLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-[#55556A]">
      {children}
    </div>
  );
}

export default async function LandingPage() {
  const { userId } = await auth();
  if (userId) redirect("/portfolio");

  const [filings, overlay, ...books] = await Promise.all([
    // No min_grade: grade filtering is Pro and this page is fetched
    // anonymously. Over-fetch and keep the rows that carry a grade, which is
    // not the same thing — we are dropping rows with nothing to show, not
    // selecting for quality. The surviving mix is usually a B next to two Ds,
    // which is the section above ("most insider buying is noise")
    // demonstrating itself on live filings.
    getJson("/filings?limit=25&trade_type=buy&min_value=250000"),
    getJson(`/portfolio/overlay?strategy=${STRATEGIES[0].key}`),
    ...STRATEGIES.map((s) => getJson(`/portfolio?strategy=${s.key}`)),
  ]);

  // No grade filter. Filtering to graded insiders dropped ~44% of candidates
  // and quietly biased the page toward filers with history at that company;
  // Unrated is a real rating with real performance behind it, so it is shown.
  const recentTrades: RecentTrade[] = (filings?.items ?? []).slice(0, 6);
  // The lead book backs the hero chart, so its win rate belongs on the same
  // card rather than a fourth stat that repeats the start date under it.
  const lead: BookSummary | null = books[0]?.summary ?? null;
  const rows: Array<Record<string, number | string>> = overlay?.data ?? [];

  const blended: Curve[] = rows.map((r) => ({ date: String(r.date), equity: Number(r.blended_SPY) }));
  const pure: Curve[] = rows.map((r) => ({ date: String(r.date), equity: Number(r.pure_SPY) }));

  let hero: null | {
    final: number; spy: number; years: number;
    cagrBlended: number; cagrSpy: number; dd: number; positions: number; from: string;
  } = null;

  if (blended.length > 2) {
    const first = blended[0], last = blended[blended.length - 1];
    const years = Math.max(
      (new Date(last.date).getTime() - new Date(first.date).getTime()) / 31_557_600_000,
      0.5,
    );
    hero = {
      final: last.equity,
      spy: pure[pure.length - 1].equity,
      years,
      cagrBlended: cagr(first.equity, last.equity, years),
      cagrSpy: cagr(pure[0].equity, pure[pure.length - 1].equity, years),
      dd: maxDrawdown(blended.map((p) => p.equity)),
      positions: Number(rows[rows.length - 1].n_positions ?? 0),
      from: first.date,
    };
  }

  return (
    <div className="min-h-screen">
      {/* ═══ 1. Hero — the record, full bleed ═══════════════════════════ */}
      <section className="px-4 pt-14 pb-14 max-w-5xl mx-auto">
        <div className="max-w-2xl">
          <div className="font-mono text-[11px] uppercase tracking-[0.16em] text-[#55556A]">
            Live since January 2023
          </div>
          <h1 className="mt-3 text-4xl sm:text-[3.25rem] font-bold tracking-[-0.03em] text-[#E8E8ED] leading-[1.03] text-balance">
            Know which insiders
            <br />
            <span className="text-[#22C55E]">are worth following.</span>
          </h1>
          <p className="mt-5 text-lg text-[#8888A0] max-w-xl">
            Some insiders have a record worth following. Most don&apos;t. We
            grade every one of them the minute they file.
          </p>
        </div>

        {hero && (
          <div className="mt-10 rounded-2xl border border-[#2A2A3A] bg-[#12121A] overflow-hidden">
            {/* No strategy name here. The number is the claim; which book
                produced it is a question for /portfolio. */}
            <div className="flex flex-wrap items-end justify-between gap-x-8 gap-y-4 p-6 pb-4">
              <div>
                <div className="font-mono text-4xl sm:text-5xl font-bold text-[#22C55E] tabular-nums tracking-tight">
                  ${Math.round(hero.final).toLocaleString()}
                </div>
                <div className="mt-1.5 text-sm text-[#8888A0]">
                  from $100,000, following our top-graded buys
                </div>
              </div>
              <div className="text-right">
                <TileLabel>The same money in the S&amp;P 500</TileLabel>
                <div className="mt-2 font-mono text-2xl text-[#8888A0] tabular-nums">
                  ${Math.round(hero.spy).toLocaleString()}
                </div>
              </div>
            </div>

            <div className="px-2">
              <EquitySparkline
                strategy={blended}
                benchmark={pure}
                label="Our top-graded buys against the S&P 500, with idle cash held in SPY"
              />
            </div>

            <dl className="grid grid-cols-2 sm:grid-cols-4 gap-px bg-[#2A2A3A] border-t border-[#2A2A3A]">
              {[
                ["Return a year", `${hero.cagrBlended.toFixed(1)}%`, `S&P 500 ${hero.cagrSpy.toFixed(1)}%`],
                ["Winners", lead ? `${lead.win_rate}%` : "—", lead ? `${lead.wins} of ${lead.total_trades} trades` : ""],
                ["Worst drop", `${hero.dd.toFixed(1)}%`, "peak to trough"],
                ["Open today", `${hero.positions}`, "positions"],
              ].map(([k, v, sub]) => (
                <div key={k} className="bg-[#12121A] px-5 py-4">
                  <dt className="text-[10px] uppercase tracking-wider text-[#55556A]">{k}</dt>
                  <dd className="font-mono text-xl text-[#E8E8ED] tabular-nums mt-1">{v}</dd>
                  <dd className="text-[11px] text-[#55556A] mt-0.5">{sub}</dd>
                </div>
              ))}
            </dl>
          </div>
        )}

        {/* One button. The nav already carries Sign In and Start Free Trial,
            so a third hero CTA was competing with our own header. */}
        <div className="mt-8 flex flex-wrap items-center gap-x-6 gap-y-3">
          <SignUpButton mode="modal">
            <button className="rounded-lg bg-[#22C55E] px-7 py-3 text-base font-semibold text-[#06240F] hover:bg-[#16A34A] transition-colors">
              Start free
            </button>
          </SignUpButton>
          <Link
            href="/portfolio"
            className="text-base font-medium text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
          >
            See every trade we&apos;ve made →
          </Link>
        </div>
        <p className="mt-4 text-sm text-[#55556A]">
          Follow any company free, forever. Seven days of Pro included, no card.
        </p>
      </section>

      {/* ═══ 2. The discrimination argument ═════════════════════════════ */}
      <section className="border-y border-[#2A2A3A] bg-[#0D0D14]">
        <div className="px-4 py-14 max-w-5xl mx-auto">
          <h2 className="text-2xl sm:text-3xl font-bold text-[#E8E8ED] tracking-tight text-balance mb-2">
            Most insider buying is noise
          </h2>
          <p className="text-[#8888A0] mb-8 max-w-xl">
            Anyone can read a Form 4. The hard part is knowing which of the
            129,000 people filing them has ever been right before.
          </p>

          <div className="grid gap-4 md:grid-cols-3 md:auto-rows-[minmax(0,1fr)]">
            <Tile span="md:col-span-2 md:row-span-2" className="flex flex-col justify-center">
              <TileLabel>What you make in the month after the filing, above the S&amp;P</TileLabel>
              <div className="mt-6 space-y-6">
                {[
                  { label: "Buying with our top-graded insiders", pct: 2.33, n: "8,721 buys", tone: "#22C55E", w: "100%" },
                  { label: "Buying with every insider", pct: 0.91, n: "91,859 buys", tone: "#55556A", w: "39%" },
                ].map((r) => (
                  <div key={r.label}>
                    <div className="flex items-baseline justify-between gap-4">
                      <span className="text-[#E8E8ED]">{r.label}</span>
                      <span className="font-mono text-2xl tabular-nums font-semibold" style={{ color: r.tone }}>
                        +{r.pct.toFixed(2)}%
                      </span>
                    </div>
                    <div className="mt-2 h-2.5 rounded-full bg-[#1A1A26] overflow-hidden">
                      <div className="h-full rounded-full" style={{ width: r.w, background: r.tone }} />
                    </div>
                    <div className="mt-1.5 text-[11px] text-[#55556A]">{r.n}</div>
                  </div>
                ))}
              </div>
              <div className="mt-6 pt-4 border-t border-[#2A2A3A] text-[11px] text-[#55556A]">
                Open-market purchases, 2016&ndash;2026, measured against SPY.{" "}
                <Link href="/research/methodology" className="text-[#22C55E] hover:underline">
                  How we grade →
                </Link>
              </div>
            </Tile>

            <Tile>
              <TileLabel>You hear about it first</TileLabel>
              <div className="mt-2 font-mono text-3xl font-bold text-[#E8E8ED] tabular-nums">
                &lt; 5 min
              </div>
              <p className="mt-2 text-sm text-[#8888A0]">
                From their filing hitting EDGAR to the alert in your inbox.
              </p>
            </Tile>

            <Tile>
              <TileLabel>Everyone is scored</TileLabel>
              <div className="mt-2 font-mono text-3xl font-bold text-[#E8E8ED] tabular-nums">
                70,808
              </div>
              <p className="mt-2 text-sm text-[#8888A0]">
                Insiders with a grade, each one earned on their own past trades.
              </p>
            </Tile>
          </div>
        </div>
      </section>

      {/* ═══ 3. The books — full width, no longer sharing a row ═════════ */}
      <section className="px-4 py-14 max-w-5xl mx-auto">
        <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight mb-1">
          We follow our own grades, in public
        </h2>
        <p className="text-[#8888A0] mb-6 max-w-2xl">
          Three sets of rules, running since January 2023. Every buy and every
          sell is posted the day it happens and never taken down — including
          the one that is currently losing to the S&amp;P.
        </p>
        <div className="grid gap-4 sm:grid-cols-3">
          {STRATEGIES.map((strat, i) => {
            const sum = books[i]?.summary;
            if (!sum) return null;
            const good = sum.cagr >= 5;
            return (
              <Link
                key={strat.key}
                href="/portfolio"
                className="flex flex-col rounded-xl border border-[#2A2A3A] bg-[#12121A] p-5 hover:border-[#22C55E]/40 transition-colors"
              >
                <span className="font-semibold text-[#E8E8ED]">{strat.name}</span>
                <span className="mt-1 text-xs text-[#8888A0] leading-relaxed flex-1">
                  {strat.thesis}
                </span>
                <span className="mt-4 flex items-baseline gap-2">
                  <span className="font-mono text-2xl font-bold tabular-nums" style={{ color: good ? "#22C55E" : "#8888A0" }}>
                    {sum.cagr > 0 ? "+" : ""}{sum.cagr}%
                  </span>
                  <span className="text-[10px] uppercase tracking-wider text-[#55556A]">
                    a year · {sum.total_trades} trades
                  </span>
                </span>
              </Link>
            );
          })}
        </div>
      </section>

      {/* ═══ 4. Recently filed — its own band ═══════════════════════════ */}
      {recentTrades.length > 0 && (
        <section className="border-y border-[#2A2A3A] bg-[#0D0D14]">
          <div className="px-4 py-14 max-w-5xl mx-auto">
            <div className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1 mb-1">
              <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight">Recently filed</h2>
              <Link href="/feed" className="text-sm text-[#22C55E] hover:underline">
                See the full feed →
              </Link>
            </div>
            <p className="text-[#8888A0] mb-6 text-sm">
              Buys from the last few days, each one already graded.
            </p>
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] divide-y divide-[#2A2A3A]/60">
              {recentTrades.map((t: RecentTrade) => (
                <div key={t.trade_id} className="flex items-center gap-3 px-4 py-3 sm:px-5">
                  <Link
                    href={`/company/${t.ticker}`}
                    className="font-mono font-semibold text-[#22C55E] hover:underline shrink-0 w-16"
                  >
                    {t.ticker}
                  </Link>
                  {/* Only link when there is something to link to. A row with
                      no slug and no id would otherwise render /insider/ and
                      404, which is the bug class we spent this week closing. */}
                  {(t.insider_slug || t.cik || t.insider_id) ? (
                    <Link
                      href={insiderPath(t.insider_name, t.cik ?? t.insider_id ?? "", t.insider_slug)}
                      className="text-sm text-[#E8E8ED] hover:text-[#22C55E] transition-colors truncate flex-1 min-w-0"
                    >
                      {t.insider_name || "—"}
                    </Link>
                  ) : (
                    <span className="text-sm text-[#E8E8ED] truncate flex-1 min-w-0">
                      {t.insider_name || "—"}
                    </span>
                  )}
                  {/* insiderRating() is the single definition — it merges D
                      into C and returns "Unrated" for a null career_grade,
                      so this badge cannot disagree with the insider page. */}
                  {(() => {
                    const rating = insiderRating(t.career_grade);
                    return (
                      <span
                        className="shrink-0 rounded px-1.5 py-0.5 font-mono text-[10px] font-semibold"
                        style={{
                          color: insiderRatingColor(rating),
                          boxShadow: `inset 0 0 0 1px ${insiderRatingColor(rating)}40`,
                        }}
                      >
                        {rating}
                      </span>
                    );
                  })()}
                  <span className="font-mono text-sm text-[#8888A0] tabular-nums shrink-0 w-16 text-right">
                    {fmtValue(t.value)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {/* ═══ 5. Pricing ════════════════════════════════════════════════ */}
      <section className="bg-[#0A0A10]">
        <div className="px-4 py-16 max-w-4xl mx-auto">
          <h2 className="text-2xl sm:text-3xl font-bold text-[#E8E8ED] tracking-tight text-center text-balance mb-2">
            Alerts are free. The judgement is what you pay for.
          </h2>
          <p className="text-[#8888A0] text-center mb-10 max-w-lg mx-auto">
            Follow any company or insider free, permanently. Pro adds the
            grades, the filters and the screener &mdash; everything we compute.
          </p>

          {/* items-stretch plus mt-auto on the buttons: the previous version
              lifted the Pro card with a negative margin, which knocked all
              three out of alignment and left two tiers with no way to act. */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-5 items-stretch">
            {[
              // Mirrors /pricing. Pro is everything we compute; Pro+ is
              // getting it out of the product.
              {
                name: "Free", price: "$0", cadence: "",
                cta: "Get started", featured: false,
                perks: [
                  "Follow up to 10 companies",
                  "Email alerts when they file",
                  "90 days of filings",
                ],
              },
              {
                name: "Pro", price: "$25", cadence: "/mo",
                cta: "Start 7-day trial", featured: true,
                perks: [
                  "Insider grades & full track records",
                  "Filter any feed by grade",
                  "Screener, leaderboard & clusters",
                  "Real-time alerts and full history",
                ],
              },
              {
                name: "Pro+", price: "$75", cadence: "/mo",
                cta: "Get Pro+", featured: false,
                perks: [
                  "Everything in Pro",
                  "CSV export of any view",
                  "Programmatic API access",
                ],
              },
            ].map((tier) => (
              <div
                key={tier.name}
                className={`flex flex-col rounded-xl border bg-[#12121A] p-6 ${
                  tier.featured
                    ? "border-[#22C55E]/50 ring-1 ring-[#22C55E]/20"
                    : "border-[#2A2A3A]"
                }`}
              >
                <div className={`text-sm font-medium mb-1 ${tier.featured ? "text-[#22C55E]" : "text-[#8888A0]"}`}>
                  {tier.name}
                </div>
                <div className="text-3xl font-bold text-[#E8E8ED] mb-5">
                  {tier.price}
                  {tier.cadence && (
                    <span className="text-lg text-[#8888A0] font-normal">{tier.cadence}</span>
                  )}
                </div>
                <ul className="flex-1 space-y-2 text-sm text-[#8888A0]">
                  {tier.perks.map((perk) => (
                    <li key={perk}>{perk}</li>
                  ))}
                </ul>
                <SignUpButton mode="modal">
                  <button
                    className={`mt-6 w-full rounded-lg px-4 py-2.5 text-sm font-semibold transition-colors ${
                      tier.featured
                        ? "bg-[#22C55E] text-[#06240F] hover:bg-[#16A34A]"
                        : "border border-[#2A2A3A] text-[#E8E8ED] hover:border-[#22C55E]/50"
                    }`}
                  >
                    {tier.cta}
                  </button>
                </SignUpButton>
              </div>
            ))}
          </div>

          <p className="mt-10 text-center text-xs text-[#55556A] max-w-2xl mx-auto">
            Strategy results are simulated, using estimated fill prices; actual
            fills will differ. Idle cash is modelled as held in SPY, so the
            S&amp;P is shown alongside. For educational purposes only, not
            investment advice. Past performance does not indicate future
            results.{" "}
            <Link href="/performance" className="text-[#8888A0] hover:text-[#E8E8ED] underline">
              How these figures are calculated, and what they leave out
            </Link>
            .{" "}
            <Link href="/pricing" className="text-[#8888A0] hover:text-[#E8E8ED] underline">
              Full pricing
            </Link>
          </p>
        </div>
      </section>
    </div>
  );
}
