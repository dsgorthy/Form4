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
            We do not sell, rent, or trade your personal information to third parties. We
            share data only with the service providers necessary to operate Form4:
            <strong className="text-[#E8E8ED]"> Clerk</strong> (authentication),
            <strong className="text-[#E8E8ED]"> Stripe</strong> (payments),
            <strong className="text-[#E8E8ED]"> Resend</strong> (email delivery, which
            receives your email address and the contents of alerts we send you), and
            <strong className="text-[#E8E8ED]"> PostHog</strong> (product analytics — see
            the next section).
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">6. Cookies</h2>
          <p>
            We use cookies for authentication (Clerk session cookies) and for product
            analytics. We do not sell cookie data.
          </p>
          <p>
            Analytics are provided by PostHog. To be specific about what that involves:
            it records page views, clicks and other interactions, associates them with
            your account once you sign in, and captures session recordings — a replay of
            your interactions with the page. Password fields are masked and never
            recorded. We use this to find where the product is confusing or broken.
          </p>
          <p>
            You can opt out of analytics with your browser&apos;s Do Not Track setting or
            an ad blocker; the product works normally without it. To have existing
            analytics data deleted, email the address in section 7.
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
          <h2 className="text-lg font-medium text-[#E8E8ED]">9. Your Rights Over Your Data</h2>
          <p>
            Wherever you live, you can ask us to show you the personal data we
            hold about you, correct it, delete it, or send you a copy in a
            portable format. Email the address in the contact section and we
            will action it within 30 days. We will not charge you, and we will
            not treat you differently for asking.
          </p>
          <p>
            <strong className="text-[#E8E8ED]">If you are in California</strong>{" "}
            (CCPA/CPRA), you additionally have the right to know what categories
            of personal information we collect and why, to opt out of sale or
            sharing, and to limit the use of sensitive personal information.{" "}
            <strong className="text-[#E8E8ED]">We do not sell or share your
            personal information</strong> as those terms are defined by the
            CCPA, and we do not collect sensitive personal information as
            defined there, so there is nothing to opt out of — but the right to
            ask stands and the response is the same.
          </p>
          <p>
            <strong className="text-[#E8E8ED]">If you are in the UK or EEA</strong>{" "}
            (UK GDPR / GDPR), our lawful bases are: performance of a contract,
            for the account and subscription features you asked us to provide;
            legitimate interests, for product analytics and fraud prevention;
            and consent, where you opted into email alerts. You may object to
            processing based on legitimate interests at any time, withdraw
            consent for alerts from your settings, and lodge a complaint with
            your supervisory authority.
          </p>
          <p>
            Form4 is operated from the United States, and our service providers
            (listed in section 5) process data there. If you use the service
            from outside the US, your information is transferred to and stored
            in the US.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">10. Changes to This Policy</h2>
          <p>
            We may update this Privacy Policy from time to time. We will notify you of material
            changes by posting the updated policy on this page with a revised effective date.
          </p>
        </section>

        <section className="space-y-2">
          <h2 className="text-lg font-medium text-[#E8E8ED]">11. Contact</h2>
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
