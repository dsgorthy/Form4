import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Financial Disclaimer — Form4",
};

export default function DisclaimerPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8 py-4">
      {/* Breadcrumb */}
      <nav className="text-sm text-[#55556A]">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#8888A0]">Financial Disclaimer</span>
      </nav>

      <h1 className="text-2xl font-semibold text-[#E8E8ED]">Financial Disclaimer</h1>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 p-5 text-[#F59E0B] space-y-3">
          <p className="text-base font-semibold">
            Form4 is not a registered investment advisor, broker-dealer, or financial planner.
          </p>
          <p>
            All information provided on this platform is for educational and research purposes
            only and should not be construed as investment advice, a recommendation, or an offer
            to buy or sell any security.
          </p>
        </div>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Public SEC Data</h2>
          <p>
            Form4 aggregates and analyzes data from SEC EDGAR Form 4 filings, which are
            publicly available records of securities transactions by corporate insiders. We do
            not generate, verify, or guarantee the accuracy of the underlying filings. The SEC
            is the authoritative source for these records.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Scores, Tiers, and Signals</h2>
          <p>
            The scoring system, tier classifications, cluster detection, convergence signals,
            and all other analytical features on Form4 are{" "}
            <span className="text-[#E8E8ED] font-medium">statistical models</span> derived
            from historical filing data. They are not buy or sell recommendations. These models
            reflect patterns in past insider behavior and carry no guarantee that similar
            patterns will produce similar outcomes in the future.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">No Guarantee of Accuracy</h2>
          <p>
            While we strive for accuracy, we make no representation or warranty that the
            information on Form4 is complete, current, or error-free. Data may be delayed,
            incomplete, or contain errors from source filings. Do not rely solely on Form4 data
            for any financial decision.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Past Performance</h2>
          <p>
            Past performance of insider trading signals, scores, or any analytical output does
            not guarantee future results. The securities markets are inherently unpredictable.
            Historical patterns shown on this platform may not repeat.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Your Responsibility</h2>
          <p>
            You are solely responsible for your own investment decisions. Before acting on any
            information found on Form4, you should:
          </p>
          <ul className="list-disc space-y-1 pl-5">
            <li>Conduct your own independent research and due diligence</li>
            <li>Consult with a qualified, licensed financial advisor</li>
            <li>Consider your own financial situation, risk tolerance, and investment goals</li>
            <li>
              Understand that all investing involves risk, including the potential loss of
              principal
            </li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">Limitation of Liability</h2>
          <p>
            Under no circumstances shall Form4, its owners, employees, or affiliates be held
            liable for any losses, damages, or costs arising from your reliance on information
            provided by this platform, including but not limited to trading losses, lost profits,
            or any direct or indirect damages.
          </p>
        </section>

        <section className="space-y-2 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
          <p className="text-[#55556A] text-xs leading-relaxed">
            By using Form4, you acknowledge that you have read and understood this disclaimer.
            If you do not agree with these terms, do not use the service. For questions, contact{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>
            .
          </p>
        </section>
      </div>
    </div>
  );
}
