import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Performance Disclosure — Form4",
  description:
    "Strategy results shown on Form4 are simulated using estimated fill prices. Not investment advice. For educational and research purposes only.",
};

/**
 * Standard performance disclosure.
 *
 * Deliberately conventional in tone — Derek's call, 2026-08-20. Form4 is not a
 * registered investment adviser, so the SEC Marketing Rule does not bind us;
 * the required bar is that published claims are substantiated and not
 * misleading, which the disclosures below meet.
 *
 * The detailed version — selection across eleven variants, the sensitivity
 * band, year concentration, and the restatement history — lives in
 * docs/published_returns_methodology.md and is pinned by
 * tests/unit/test_published_returns.py. It stays on the record internally so
 * the figures can be defended if anyone asks; it is simply not the register
 * for a public page.
 *
 * THE FIGURES ARE FETCHED, NEVER TYPED. They were hard-coded until 2026-08-23
 * and had drifted three days behind the books they describe — 58.6/45.3/37.4
 * against an actual 64.8/64.3/38.4, on the one page whose entire job is to be
 * accurate about them. The landing page learned this lesson already; this one
 * now reads the same API it does.
 */

const API = process.env.API_URL_INTERNAL || "http://localhost:8000/api/v1";

// Rendered per request, not prerendered. The API is not reachable during the
// image build, so a statically prerendered version of this page ships an empty
// figures table and only fills in after the first revalidation. On a
// low-traffic disclosure page whose entire job is to state accurate numbers,
// that trade is the wrong way round.
export const dynamic = "force-dynamic";

// Keys only. The public name is fetched with the figures rather than typed
// here — api/public_fields.STRATEGIES is the single definition of the label.
const STRATEGY_KEYS = ["quality_notrend", "quality_momentum", "reversal_dip"];

type AnnualRow = {
  year: string;
  strategy: number | null;
  /** SPY over this book's own window — like-for-like in a partial first year. */
  spy: number | null;
  /** SPY over the full calendar year, independent of any book. The shared
   *  column must use this, or it shows one book's partial-year index return
   *  beside another book's full year. */
  spy_calendar: number | null;
  partial: boolean;
};

type Summary = {
  strategy_label: string;
  blended_cagr: number | null;
  spy_cagr: number | null;
  excess_vs_spy: number | null;
  max_drawdown_daily: number | null;
  annual_returns: AnnualRow[];
};

async function getSummary(key: string): Promise<Summary | null> {
  try {
    const res = await fetch(`${API}/portfolio?strategy=${key}`, {
      next: { revalidate: 300 },
    });
    if (!res.ok) return null;
    return (await res.json())?.summary ?? null;
  } catch {
    return null;
  }
}

function pct(v: number | null | undefined) {
  return typeof v === "number" ? `${v.toFixed(1)}%` : "—";
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-4 border-b border-[#2A2A3A]/60 py-2 last:border-0">
      <span className="text-[#8888A0]">{label}</span>
      <span className="font-mono tabular-nums text-[#E8E8ED]">{value}</span>
    </div>
  );
}

export default async function PerformancePage() {
  const books = await Promise.all(STRATEGY_KEYS.map(getSummary));
  const years = Array.from(
    new Set(books.flatMap((b) => b?.annual_returns?.map((a) => a.year) ?? [])),
  ).sort();
  // Every book is measured against SPY over its own window, so the index
  // figure is per-strategy rather than one shared number.

  return (
    <div className="mx-auto max-w-3xl space-y-8 py-4">
      <nav className="text-sm text-[#55556A]">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#8888A0]">Performance Disclosure</span>
      </nav>

      <h1 className="text-2xl font-semibold text-[#E8E8ED]">
        Performance Disclosure
      </h1>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <div className="space-y-3 rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 p-5 text-[#F59E0B]">
          <p className="text-base font-semibold">
            Strategy results shown on Form4 are simulated, using estimated fill
            prices.
          </p>
          <p>
            They are provided for educational and research purposes only, are
            not investment advice, and are not a recommendation to buy or sell
            any security. Past performance does not indicate future results.
          </p>
        </div>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Estimated fills</h2>
          <p>
            Each position is priced at the first market price available after
            the filing became public — the closing price if the filing was
            accepted before 4:00pm ET, otherwise the next session&apos;s opening
            price. These are estimates drawn from end-of-day market data.
          </p>
          <p>
            <strong className="text-[#E8E8ED]">Your actual fills will differ.</strong>{" "}
            The figures do not include commissions, bid-ask spreads, slippage or
            taxes, and they assume a position can be filled in full at the
            quoted price. In smaller or less liquid companies that assumption
            may not hold.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            How the figures are constructed
          </h2>
          <p>
            Each strategy starts with $100,000 on 3 January 2023 and follows its
            rules forward. Cash not held in a position is modelled as held in
            SPY, so the S&amp;P 500 over the identical period is always shown
            alongside — the excess over the index is the meaningful comparison.
          </p>
          <p>
            Results are net of no fees and reflect a single set of rules applied
            consistently. They are a model of what the strategy would have done,
            not a record of an account.
          </p>
        </section>

        <section className="space-y-3">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Current figures</h2>
          <p className="text-xs text-[#55556A]">
            Each strategy is measured from its own first trade to the present,
            against the S&amp;P 500 over that identical window. $100,000 start,
            idle cash in SPY. Annualised.
          </p>
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
            {books.every((b) => !b) ? (
              <p className="text-[#8888A0]">
                Figures are temporarily unavailable. They are published on the{" "}
                <Link href="/portfolio" className="text-[#3B82F6] hover:underline">
                  portfolio page
                </Link>{" "}
                and are not being withheld.
              </p>
            ) : (
              STRATEGY_KEYS.map((key, i) => {
                const b = books[i];
                if (!b) return null;
                return (
                  <Row
                    key={key}
                    label={b.strategy_label}
                    value={`${pct(b.blended_cagr)}  ·  S&P 500 ${pct(b.spy_cagr)}`}
                  />
                );
              })
            )}
          </div>
          <p>
            Every strategy we run is published, including periods where one
            trails the index. Figures are recalculated as new filings arrive and
            may be restated if we identify an error in how they were computed.
          </p>
        </section>

        <section className="space-y-3">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Year by year</h2>
          <p>
            A single annualised figure hides how it was earned. One of these
            strategies made most of its return in a single year; the table
            below is the only place that is visible, which is why it is here.
          </p>
          <div className="overflow-x-auto rounded-lg border border-[#2A2A3A] bg-[#12121A]">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#2A2A3A] text-xs uppercase tracking-wider text-[#55556A]">
                  <th className="px-4 py-3 text-left font-medium">Year</th>
                  {books.map((b, i) => (
                    <th key={STRATEGY_KEYS[i]} className="px-4 py-3 text-right font-medium">
                      {b?.strategy_label ?? "—"}
                    </th>
                  ))}
                  <th className="px-4 py-3 text-right font-medium">S&amp;P 500</th>
                </tr>
              </thead>
              <tbody className="font-mono tabular-nums">
                {years.map((y) => (
                  <tr key={y} className="border-b border-[#2A2A3A]/60 last:border-0">
                    <td className="px-4 py-2 text-[#8888A0]">{y}</td>
                    {books.map((b, i) => {
                      const row = b?.annual_returns?.find((a) => a.year === y);
                      const v = row?.strategy;
                      return (
                        <td key={STRATEGY_KEYS[i]} className="px-4 py-2 text-right text-[#E8E8ED]">
                          {typeof v === "number" ? `${v > 0 ? "+" : ""}${v.toFixed(1)}%` : "—"}
                          {row?.partial && <span className="ml-1 text-[#55556A]">*</span>}
                        </td>
                      );
                    })}
                    <td className="px-4 py-2 text-right text-[#8888A0]">
                      {(() => {
                        const sv = books
                          .map((b) => b?.annual_returns?.find((a) => a.year === y)?.spy_calendar)
                          .find((x) => typeof x === "number");
                        return typeof sv === "number" ? `${sv > 0 ? "+" : ""}${sv.toFixed(1)}%` : "—";
                      })()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-xs text-[#55556A]">
            * Partial year — the book started mid-year, so it is not a full
            twelve months against the index. The S&amp;P column is the full
            calendar year in every row. Worst peak-to-trough decline, marked
            daily:{" "}
            {books
              .filter(Boolean)
              .map((b) => `${b!.strategy_label} ${b!.max_drawdown_daily?.toFixed(1) ?? "—"}%`)
              .join(" · ")}
            .
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Data accuracy</h2>
          <p>
            Filing data comes from SEC EDGAR and price data from third-party
            market data providers. We do not generate or verify the underlying
            filings, and we make no warranty that the information is complete,
            current or error-free.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Not advice</h2>
          <p>
            Form4 is not a registered investment adviser, broker-dealer or
            financial planner. Nothing on this site is a recommendation, an
            offer, or a solicitation to buy or sell any security. You are solely
            responsible for your own investment decisions and should consult a
            licensed professional before acting on anything you read here. See
            the{" "}
            <Link href="/disclaimer" className="text-[#3B82F6] hover:underline">
              full financial disclaimer
            </Link>{" "}
            and{" "}
            <Link
              href="/research/methodology"
              className="text-[#3B82F6] hover:underline"
            >
              how the grades are calculated
            </Link>
            .
          </p>
        </section>

        <p className="border-t border-[#2A2A3A] pt-6 text-xs text-[#55556A]">
          Figures update automatically. Last reviewed 23 August 2026.
        </p>
      </div>
    </div>
  );
}
