import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Privacy Policy — Form4",
};

export default function PrivacyPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-8 py-4">
      {/* Breadcrumb */}
      <nav className="text-sm text-[#55556A]">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#8888A0]">Privacy Policy</span>
      </nav>

      <h1 className="text-2xl font-semibold text-[#E8E8ED]">Privacy Policy</h1>
      <p className="text-sm text-[#55556A]">Effective date: March 15, 2026</p>

      <div className="space-y-6 text-sm leading-relaxed text-[#8888A0]">
        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">1. Introduction</h2>
          <p>
            Form4 (&quot;we,&quot; &quot;us,&quot; or &quot;our&quot;) operates the website at{" "}
            <span className="text-[#E8E8ED]">form4.app</span>. This Privacy Policy explains what
            information we collect, how we use it, and your choices regarding that information.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">2. Information We Collect</h2>
          <p>
            <span className="font-medium text-[#E8E8ED]">Account information.</span> When you
            create an account through Clerk, we receive your name, email address, and profile
            information from your chosen sign-in method (email, Google, or GitHub).
          </p>
          <p>
            <span className="font-medium text-[#E8E8ED]">Payment information.</span> If you
            subscribe to Form4 Pro, payment is processed by Stripe. We do not store your full
            credit card number. Stripe provides us with a token, card last four digits, and
            billing details necessary to manage your subscription.
          </p>
          <p>
            <span className="font-medium text-[#E8E8ED]">Usage data.</span> We collect
            anonymized usage analytics (pages visited, feature usage) to improve the product.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">3. SEC Filing Data</h2>
          <p>
            Form4 displays information derived from publicly available SEC EDGAR filings. This
            data relates to corporate insiders&apos; securities transactions and is not personal
            data of our users. We aggregate, score, and present this public information as part
            of our service.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">4. How We Use Your Information</h2>
          <ul className="list-disc space-y-1 pl-5">
            <li>Provide and maintain your account</li>
            <li>Process subscription payments</li>
            <li>Send transactional emails (account changes, billing)</li>
            <li>Improve our product based on usage patterns</li>
            <li>Respond to support requests</li>
          </ul>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">5. We Do Not Sell Your Data</h2>
          <p>
            We do not sell, rent, or trade your personal information to third parties. We share
            data only with service providers necessary to operate Form4 (Clerk for
            authentication, Stripe for payments).
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">6. Cookies</h2>
          <p>
            We use cookies strictly for authentication purposes (Clerk session cookies). We do
            not use third-party tracking cookies or sell cookie data. Google Analytics uses a
            first-party cookie for anonymized usage statistics.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">7. Data Retention and Deletion</h2>
          <p>
            We retain your account data for as long as your account is active. You may request
            deletion of your account and associated data at any time by contacting us at{" "}
            <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">
              support@form4.app
            </a>
            . Upon deletion, your personal data will be removed within 30 days, except where
            retention is required by law.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">8. Children&apos;s Privacy</h2>
          <p>
            Form4 is not intended for use by anyone under the age of 13. We do not knowingly
            collect personal information from children under 13. If we learn that we have
            collected such information, we will delete it promptly.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">9. Changes to This Policy</h2>
          <p>
            We may update this Privacy Policy from time to time. We will notify you of material
            changes by posting the updated policy on this page with a revised effective date.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">10. Contact</h2>
          <p>
            For privacy-related inquiries, contact us at{" "}
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
