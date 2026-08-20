import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "How We Calculate Performance — Form4",
  description:
    "Every published Form4 return figure is a simulation, not a live trading record. How the numbers are produced, what they exclude, and where they are likely to be too high.",
};

/**
 * WHY THIS PAGE EXISTS
 *
 * The site publishes "+37 points vs the S&P" on the homepage. That number is
 * not a track record — it is a simulation, chosen as the best of eleven
 * variants, over a window in which the S&P itself compounded 21.4%.
 *
 * The financial disclaimer covers "past performance does not guarantee future
 * results", which is the wrong sentence for this claim. Past performance is a
 * record of money that was actually at risk. Hypothetical performance is a
 * different thing with different failure modes — hindsight, selection, no
 * slippage, no fills — and it warrants its own disclosure rather than a
 * borrowed one.
 *
 * The figures here are the same ones in docs/published_returns_methodology.md,
 * which is the internal definition. If the two disagree, that is a bug.
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
        How we calculate performance
      </h1>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <div className="space-y-3 rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 p-5 text-[#F59E0B]">
          <p className="text-base font-semibold">
            These are simulated results. No real money has been traded.
          </p>
          <p>
            Every return figure on this site is produced by replaying our rules
            against historical SEC filings. It is not a record of a live
            account, it is not audited, and no client capital was ever at risk.
            Hypothetical results have inherent limitations, the largest being
            that they are prepared with the benefit of hindsight.
          </p>
        </div>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">What the number means</h2>
          <p>
            We start with $100,000 on 3 January 2023 and follow the rules
            forward. When a strategy has no position, the cash sits in SPY —
            so the figure you see includes whatever the market did, and we
            always show the S&amp;P over the identical window beside it.
          </p>
          <p>
            <strong className="text-[#E8E8ED]">
              The excess over the S&amp;P is the honest claim.
            </strong>{" "}
            Two of our three strategies hold positions less than half the time,
            so most of what a holder would experience is simply the index.
            Quoting the raw return without the benchmark would imply a
            stock-picking result that is mostly market.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            How a trade is priced
          </h2>
          <p>
            A position opens at the first price anyone could actually have paid
            after the filing became public — not at the price the insider got,
            and not at a price that had already printed.
          </p>
          <ul className="ml-5 list-disc space-y-1">
            <li>
              Filing accepted by EDGAR before 4:00pm ET → that day&apos;s
              closing price.
            </li>
            <li>
              Accepted after the bell → the <em>next</em> session&apos;s opening
              price. Roughly three-quarters of filings land after the close.
            </li>
            <li>
              We assume a five-minute delay between EDGAR accepting a filing and
              us seeing it, so a filing at 3:57pm misses that day.
            </li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            What the simulation leaves out
          </h2>
          <p>Each of these makes the published figures better than reality:</p>
          <ul className="ml-5 list-disc space-y-1">
            <li>
              <strong className="text-[#E8E8ED]">No commissions, spreads or
              slippage.</strong> Many of these are small companies where the
              spread is real money.
            </li>
            <li>
              <strong className="text-[#E8E8ED]">Fills are assumed.</strong> We
              assume you get the opening or closing price for the full position
              size. In a thin stock reacting to insider news, you may not.
            </li>
            <li>
              <strong className="text-[#E8E8ED]">No taxes.</strong>
            </li>
            <li>
              <strong className="text-[#E8E8ED]">Delisted names are dropped.</strong>{" "}
              About 2% of qualifying trades are skipped because the stock has no
              usable price history — and a stock with no forward prices has
              usually stopped trading, so this removes bad outcomes rather than
              a random sample.
            </li>
            <li>
              <strong className="text-[#E8E8ED]">Stops can be jumped.</strong> Our
              −30% stop is checked against closing prices, so a stock that gaps
              through it fills lower. Two of our twelve stops came out at −43.6%
              and −42.3%.
            </li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            The strategy was chosen knowing the answer
          </h2>
          <p>
            This is the caveat that matters most, and the one most sites omit.
            Eleven variations were tested against this same history and the best
            one was published. A winner picked from eleven attempts is
            flattered by that selection, and there is no untouched period left
            to check it against.
          </p>
          <p>
            We can put a size on the fragility. The rules score each filing and
            take positions above a threshold. Nudging that score by less than
            one of its twelve components — and re-running — moves our strongest
            strategy across a range of{" "}
            <span className="font-mono text-[#E8E8ED]">46% to 56%</span> a year,
            with a median of 50%. The published figure sits near the top of that
            range, not in the middle.
          </p>
          <p>
            One year also does a lot of the work: 2025 accounts for roughly 47%
            of the strongest strategy&apos;s total profit, and 2024 was close to
            flat. The whole period ran through a bull market in which the
            S&amp;P compounded 21.4% a year.
          </p>
        </section>

        <section className="space-y-3">
          <h2 className="text-lg font-medium text-[#E8E8ED]">
            The published figures
          </h2>
          <p className="text-xs text-[#55556A]">
            3 January 2023 to present. $100,000 start, idle cash in SPY.
          </p>
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
            <Row label="A-List Buys, annualised" value="58.6%" />
            <Row label="Insider Breakout, annualised" value="45.3%" />
            <Row label="Insider Dip Buys, annualised" value="37.4%" />
            <Row label="S&P 500, same window" value="21.4%" />
          </div>
          <p>
            Insider Dip Buys is behind the S&amp;P so far in 2026 (+3.9% against
            +12.6%). We publish it anyway, because a strategy page that only
            shows the good years is not a track record.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Corrections</h2>
          <p>
            These figures were restated four times between 18 and 20 August 2026
            as we found and fixed errors in how they were computed — a timezone
            bug that let trades be entered before their filing was public, a
            flag that mislabelled purchases, and two competing definitions of
            the same number. None of the restatements were caused by market
            events.
          </p>
          <p>
            We would rather say that plainly than have you discover it. If a
            figure changes again, it will be because we found something else, or
            because we changed a rule on purpose.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Not advice</h2>
          <p>
            Form4 is not a registered investment adviser or broker-dealer.
            Nothing here is a recommendation to buy or sell any security. See
            the{" "}
            <Link href="/disclaimer" className="text-[#3B82F6] hover:underline">
              full financial disclaimer
            </Link>
            , and{" "}
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
