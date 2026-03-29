import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Terms of Service — Form4",
};

export default function TermsPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8 py-4">
      {/* Breadcrumb */}
      <nav className="text-sm text-[#55556A]">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#8888A0]">Terms of Service</span>
      </nav>

      <h1 className="text-2xl font-semibold text-[#E8E8ED]">Terms of Service</h1>
      <p className="text-sm text-[#55556A]">Effective date: March 15, 2026</p>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">1. Acceptance of Terms</h2>
          <p>
            By accessing or using Form4 at{" "}
            <span className="text-[#E8E8ED]">form4.app</span>, you agree to be bound by these
            Terms of Service. If you do not agree, do not use the service.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">2. Account Responsibilities</h2>
          <p>
            You are responsible for maintaining the security of your account credentials. You
            must provide accurate information when creating an account. You are responsible for
            all activity that occurs under your account. Notify us immediately at{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>{" "}
            if you suspect unauthorized access.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">3. Pro Subscription</h2>
          <p>
            <span className="font-medium text-[#E8E8ED]">Billing.</span> Form4 Pro is a
            recurring subscription billed through Stripe. By subscribing, you authorize us to
            charge your payment method on a recurring basis until you cancel.
          </p>
          <p>
            <span className="font-medium text-[#E8E8ED]">Cancellation.</span> You may cancel
            your subscription at any time from your account settings. Cancellation takes effect
            at the end of the current billing period. You will retain Pro access until then.
          </p>
          <p>
            <span className="font-medium text-[#E8E8ED]">Refunds.</span> Subscription fees are
            generally non-refundable. If you believe you were charged in error, contact{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>{" "}
            and we will review your case.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">4. Financial Disclaimer</h2>
          <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 p-4 text-[#F59E0B]">
            <p className="font-medium">
              Form4 provides information derived from publicly available SEC filings for
              educational and research purposes only.
            </p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>Form4 is NOT an investment advisor and does NOT provide investment advice.</li>
              <li>
                Nothing on this platform constitutes a recommendation to buy, sell, or hold any
                security.
              </li>
              <li>
                Scores, tiers, signals, and cluster analysis are statistical models based on
                historical data. They are not predictions or guarantees of future performance.
              </li>
              <li>
                Past performance of insider trading signals does not guarantee future results.
              </li>
              <li>
                You are solely responsible for your own investment decisions. Consult a qualified
                financial advisor before making investment decisions.
              </li>
            </ul>
          </div>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">5. Intellectual Property</h2>
          <p>
            All content, design, scoring algorithms, and software on Form4 are owned by Form4
            or its licensors. You may not copy, modify, distribute, or reverse-engineer any part
            of the service without our written permission.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">6. Prohibited Uses</h2>
          <ul className="list-disc space-y-1 pl-5">
            <li>Scraping, crawling, or automated data extraction from the platform</li>
            <li>Redistributing or reselling Form4 data without written permission</li>
            <li>Attempting to gain unauthorized access to other accounts or systems</li>
            <li>Using the service for any unlawful purpose</li>
            <li>Interfering with or disrupting the service or its infrastructure</li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">7. Limitation of Liability</h2>
          <p>
            To the maximum extent permitted by law, Form4, its owners, employees, and affiliates
            shall not be liable for any indirect, incidental, special, consequential, or
            punitive damages, including but not limited to loss of profits, data, or investment
            losses, arising out of your use of or inability to use the service.
          </p>
          <p>
            The service is provided &quot;as is&quot; and &quot;as available&quot; without
            warranties of any kind, express or implied.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">8. Termination</h2>
          <p>
            We reserve the right to suspend or terminate your account at our discretion if you
            violate these terms. You may close your account at any time by contacting us at{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>
            .
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">9. Governing Law</h2>
          <p>
            These terms are governed by the laws of the State of Washington, without regard to
            its conflict of laws provisions. Any disputes arising from these terms or your use
            of the service shall be resolved in the courts located in King County, Washington.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">10. Changes to These Terms</h2>
          <p>
            We may revise these terms at any time. Material changes will be posted on this page
            with a revised effective date. Continued use of the service after changes constitutes
            acceptance.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">11. Contact</h2>
          <p>
            Questions about these terms? Contact us at{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>
            .
          </p>
          <p>Form4 — Seattle, WA</p>
        </section>
      </div>
    </div>
  );
}
