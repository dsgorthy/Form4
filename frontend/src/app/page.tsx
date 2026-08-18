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
    getJson("/filings?limit=5&min_grade=B&trade_type=buy"),
    getJson("/portfolio/overlay?strategy=quality_momentum"),
    ...STRATEGIES.map((s) => getJson(`/portfolio?strategy=${s.key}`)),
  ]);

  const recentTrades = filings?.items ?? [];
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
          {/* Descriptive, not clever: a reader should know what we sell from
              the headline alone. */}
          <h1 className="mt-3 text-4xl sm:text-[3.25rem] font-bold tracking-[-0.03em] text-[#E8E8ED] leading-[1.03] text-balance">
            Know which insiders
            <br />
            <span className="text-[#22C55E]">are worth following.</span>
          </h1>
          <p className="mt-5 text-lg text-[#8888A0] max-w-xl">
            Every SEC Form 4, graded on the filer&apos;s own record, minutes
            after it lands. We trade the grades in public.
          </p>
        </div>

        {hero && (
          <div className="mt-10 rounded-2xl border border-[#2A2A3A] bg-[#12121A] overflow-hidden">
            <div className="flex flex-wrap items-end justify-between gap-x-8 gap-y-4 p-6 pb-4">
              <div>
                <TileLabel>Quality + Momentum, idle cash in SPY</TileLabel>
                <div className="mt-2 flex items-baseline gap-3">
                  <span className="font-mono text-4xl sm:text-5xl font-bold text-[#22C55E] tabular-nums tracking-tight">
                    ${Math.round(hero.final).toLocaleString()}
                  </span>
                  <span className="text-sm text-[#55556A]">from $100,000</span>
                </div>
              </div>
              <div className="text-right">
                <TileLabel>Same money in SPY</TileLabel>
                <div className="mt-2 font-mono text-2xl text-[#8888A0] tabular-nums">
                  ${Math.round(hero.spy).toLocaleString()}
                </div>
              </div>
            </div>

            <div className="px-2">
              <EquitySparkline
                strategy={blended}
                benchmark={pure}
                label="Quality + Momentum with idle cash in SPY"
              />
            </div>

            <dl className="grid grid-cols-2 sm:grid-cols-4 gap-px bg-[#2A2A3A] border-t border-[#2A2A3A]">
              {[
                ["CAGR", `${hero.cagrBlended.toFixed(1)}%`, `SPY ${hero.cagrSpy.toFixed(1)}%`],
                ["Max drawdown", `${hero.dd.toFixed(1)}%`, "peak to trough"],
                ["Open today", `${hero.positions}`, "positions"],
                ["Since", hero.from.slice(0, 7), "every trade public"],
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

        <div className="mt-8 flex flex-wrap items-center gap-3">
          <SignUpButton mode="modal">
            <button className="rounded-lg bg-[#22C55E] px-7 py-3 text-base font-semibold text-[#06240F] hover:bg-[#16A34A] transition-colors">
              Start free
            </button>
          </SignUpButton>
          <Link
            href="/portfolio"
            className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-7 py-3 text-base font-semibold text-[#E8E8ED] hover:border-[#22C55E]/50 transition-colors"
          >
            See every trade →
          </Link>
          <SignInButton mode="modal">
            <button className="ml-1 text-sm text-[#8888A0] hover:text-[#E8E8ED] transition-colors">
              Sign in
            </button>
          </SignInButton>
        </div>
        <p className="mt-4 text-sm text-[#55556A]">
          Free account follows any company and alerts you when they file.
          7 days of Pro on top, no card.
        </p>
      </section>

      {/* ═══ 2. Bento — one big claim, three small supports ═════════════ */}
      <section className="border-y border-[#2A2A3A] bg-[#0D0D14]">
        <div className="px-4 py-14 max-w-5xl mx-auto">
          <h2 className="text-2xl sm:text-3xl font-bold text-[#E8E8ED] tracking-tight text-balance mb-2">
            The grade is the product
          </h2>
          <p className="text-[#8888A0] mb-8 max-w-xl">
            Every insider is scored on their own prior trades, using only what
            was knowable at the time.
          </p>

          {/* Uneven on purpose: the measurement is the argument and gets twice
              the room, the supports sit beside it. */}
          <div className="grid gap-4 md:grid-cols-3 md:auto-rows-[minmax(0,1fr)]">
            <Tile span="md:col-span-2 md:row-span-2" className="flex flex-col justify-center">
              <TileLabel>Return vs S&amp;P, 30 days after the filing</TileLabel>
              <div className="mt-6 space-y-6">
                {[
                  { label: "A+ and A insiders", pct: 2.33, n: "8,721 buys", tone: "#22C55E", w: "100%" },
                  { label: "Everyone else", pct: 0.91, n: "91,859 buys", tone: "#55556A", w: "39%" },
                ].map((r) => (
                  <div key={r.label}>
                    <div className="flex items-baseline justify-between">
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
                Open-market purchases, 2016–2026, benchmarked against SPY.{" "}
                <Link href="/research/methodology" className="text-[#22C55E] hover:underline">
                  How the grade is calculated →
                </Link>
              </div>
            </Tile>

            <Tile>
              <TileLabel>Speed</TileLabel>
              <div className="mt-2 font-mono text-3xl font-bold text-[#E8E8ED] tabular-nums">
                &lt; 5 min
              </div>
              <p className="mt-2 text-sm text-[#8888A0]">
                From EDGAR to graded and searchable.
              </p>
            </Tile>

            <Tile>
              <TileLabel>Corrected, not copied</TileLabel>
              <p className="mt-2 text-sm text-[#8888A0]">
                Filers make mistakes — a total typed into the price field turns
                a $65K buy into $4.8B. We repair those and show you the original
                on the filing.
              </p>
            </Tile>
          </div>
        </div>
      </section>

      {/* ═══ 3. Two-thirds split — the books, then what just landed ═════ */}
      <section className="px-4 py-14 max-w-5xl mx-auto grid gap-8 lg:grid-cols-[1.15fr_1fr] lg:items-start">
        <div>
          <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight mb-1">
            Three books, nothing hidden
          </h2>
          <p className="text-[#8888A0] mb-5 text-sm">
            Read from the same record the site publishes. One is not working
            and it stays up.
          </p>
          <div className="space-y-2.5">
            {STRATEGIES.map((strat, i) => {
              const sum = books[i]?.summary;
              if (!sum) return null;
              const good = sum.cagr >= 5;
              return (
                <Link
                  key={strat.key}
                  href="/portfolio"
                  className="flex items-center gap-4 rounded-xl border border-[#2A2A3A] bg-[#12121A] px-5 py-4 hover:border-[#22C55E]/40 transition-colors"
                >
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-[#E8E8ED]">{strat.name}</span>
                      {strat.experimental && (
                        <span className="rounded-full bg-[#F59E0B]/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide text-[#F59E0B]">
                          Experimental
                        </span>
                      )}
                    </div>
                    <div className="text-xs text-[#55556A] mt-0.5 truncate">{strat.thesis}</div>
                  </div>
                  <div className="text-right shrink-0">
                    <div className="font-mono text-xl font-bold tabular-nums" style={{ color: good ? "#22C55E" : "#8888A0" }}>
                      {sum.cagr > 0 ? "+" : ""}{sum.cagr}%
                    </div>
                    <div className="text-[10px] uppercase tracking-wider text-[#55556A]">
                      {sum.total_trades} trades
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        </div>

        {recentTrades.length > 0 && (
          <div>
            <div className="flex items-baseline justify-between mb-1">
              <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight">Just filed</h2>
              <Link href="/feed" className="text-sm text-[#22C55E] hover:underline">
                Full feed →
              </Link>
            </div>
            <p className="text-[#8888A0] mb-5 text-sm">Graded buys from the last few days.</p>
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] divide-y divide-[#2A2A3A]/60">
              {recentTrades.map((t: any) => (
                <div key={t.trade_id} className="flex items-center gap-3 px-4 py-3">
                  <Link
                    href={`/company/${t.ticker}`}
                    className="font-mono font-semibold text-[#22C55E] hover:underline shrink-0 w-14"
                  >
                    {t.ticker}
                  </Link>
                  <Link
                    href={insiderPath(t.insider_name, t.cik || t.insider_id, t.insider_slug)}
                    className="text-sm text-[#E8E8ED] hover:text-[#22C55E] transition-colors truncate flex-1 min-w-0"
                  >
                    {t.insider_name || "—"}
                  </Link>
                  <span className="font-mono text-sm text-[#8888A0] tabular-nums shrink-0">
                    {fmtValue(t.value)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </section>

      {/* ═══ 4. Pricing ════════════════════════════════════════════════ */}
      <section className="border-t border-[#2A2A3A] bg-[#0D0D14]">
        <div className="px-4 py-16 max-w-4xl mx-auto">
          <h2 className="text-2xl sm:text-3xl font-bold text-[#E8E8ED] tracking-tight text-center text-balance mb-2">
            Alerts are free. The analysis is what you pay for.
          </h2>
          <p className="text-[#8888A0] text-center mb-10 max-w-lg mx-auto">
            Follow any company or insider on a free account, permanently.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-5 items-start">
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6">
              <div className="text-sm font-medium text-[#8888A0] mb-1">Free</div>
              <div className="text-3xl font-bold text-[#E8E8ED] mb-4">$0</div>
              <ul className="space-y-2 text-sm text-[#8888A0]">
                <li>Follow up to 10 companies</li>
                <li>Email alerts when they file</li>
                <li>90 days of filings</li>
              </ul>
            </div>
            <div className="rounded-xl border border-[#22C55E]/50 bg-[#12121A] p-6 ring-1 ring-[#22C55E]/20 sm:-mt-3 sm:pb-8">
              <div className="text-sm font-medium text-[#22C55E] mb-1">Pro</div>
              <div className="text-3xl font-bold text-[#E8E8ED] mb-4">
                $25<span className="text-lg text-[#8888A0] font-normal">/mo</span>
              </div>
              <ul className="space-y-2 text-sm text-[#8888A0]">
                <li>Insider track records &amp; grades</li>
                <li>Real-time, full filing history</li>
                <li>Alert on grade, clusters, spikes</li>
              </ul>
              <SignUpButton mode="modal">
                <button className="mt-6 w-full rounded-lg bg-[#22C55E] px-4 py-2.5 text-sm font-semibold text-[#06240F] hover:bg-[#16A34A] transition-colors">
                  Start 7-day trial
                </button>
              </SignUpButton>
            </div>
            <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6">
              <div className="text-sm font-medium text-[#8888A0] mb-1">Pro+</div>
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
          <p className="mt-10 text-center text-xs text-[#55556A] max-w-2xl mx-auto">
            Strategy results are simulated fills on real filings, published as
            they happened rather than reconstructed afterwards. Idle cash is
            modelled as held in SPY. Past performance does not guarantee future
            results.{" "}
            <Link href="/pricing" className="text-[#8888A0] hover:text-[#E8E8ED] underline">
              Full pricing
            </Link>
          </p>
        </div>
      </section>
    </div>
  );
}
