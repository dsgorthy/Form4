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
 * The detailed version — selection across eleven variants, the 46-56%
 * sensitivity band, year concentration, and the restatement history — lives in
 * docs/published_returns_methodology.md and is pinned by
 * tests/unit/test_published_returns.py. It stays on the record internally so
 * the figures can be defended if anyone asks; it is simply not the register
 * for a public page.
 */

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-4 border-b border-[#2A2A3A]/60 py-2 last:border-0">
      <span className="text-[#8888A0]">{label}</span>
      <span className="font-mono tabular-nums text-[#E8E8ED]">{value}</span>
    </div>
  );
}

export default function PerformancePage() {
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
            3 January 2023 to present. $100,000 start, idle cash in SPY.
            Annualised.
          </p>
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
            <Row label="A-List Buys" value="58.6%" />
            <Row label="Insider Breakout" value="45.3%" />
            <Row label="Insider Dip Buys" value="37.4%" />
            <Row label="S&P 500, same window" value="21.4%" />
          </div>
          <p>
            Every strategy we run is published, including periods where one
            trails the index. Figures are recalculated as new filings arrive and
            may be restated if we identify an error in how they were computed.
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
          Last updated 20 August 2026.
        </p>
      </div>
    </div>
  );
}
