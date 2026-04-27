"use client";

import { useUser, useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { useState } from "react";
import { getUserTier, getTrialDaysLeft } from "@/lib/subscription";
import { posthog } from "@/lib/posthog";

const PRO_MONTHLY = process.env.NEXT_PUBLIC_STRIPE_PRO_MONTHLY_PRICE_ID || "";
const PRO_YEARLY = process.env.NEXT_PUBLIC_STRIPE_PRO_YEARLY_PRICE_ID || "";
const PRO_PLUS_MONTHLY = process.env.NEXT_PUBLIC_STRIPE_PRO_PLUS_MONTHLY_PRICE_ID || "";
const PRO_PLUS_YEARLY = process.env.NEXT_PUBLIC_STRIPE_PRO_PLUS_YEARLY_PRICE_ID || "";

function tierFromPriceId(priceId: string): string {
  if (priceId === PRO_PLUS_MONTHLY || priceId === PRO_PLUS_YEARLY) return "pro_plus";
  if (priceId === PRO_MONTHLY || priceId === PRO_YEARLY) return "pro";
  return "unknown";
}

function billingFromPriceId(priceId: string): string {
  if (priceId === PRO_YEARLY || priceId === PRO_PLUS_YEARLY) return "yearly";
  if (priceId === PRO_MONTHLY || priceId === PRO_PLUS_MONTHLY) return "monthly";
  return "unknown";
}

const FREE_FEATURES = [
  "24h-delayed portfolio view",
  "Last 90 days of filings",
  "Basic trade grades",
  "Company & insider pages",
  "Congress trades",
];

const PRO_FEATURES = [
  "Real-time portfolio & positions",
  "Trade entry/exit alerts",
  "Full history (2016+)",
  "Insider grades & track records",
  "Cluster detection & analysis",
  "Activity inflections",
  "Sell cessation signals",
];

const PRO_PLUS_FEATURES = [
  "Everything in Pro",
  "Screener & Leaderboard",
  "Insider profiles & return distributions",
  "Percentile rankings",
  "Congress convergence alerts",
  "CSV export",
  "Programmatic API access (3 keys)",
];

export default function PricingPage() {
  const { user, isLoaded } = useUser();
  const { isSignedIn } = useAuth();
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [billing, setBilling] = useState<"monthly" | "yearly">("monthly");

  const tier = getUserTier(user);
  const userIsPro = tier === "pro" || tier === "pro_plus";
  const userIsProPlus = tier === "pro_plus";
  const trialDaysLeft = getTrialDaysLeft(user);

  async function handleCheckout(priceId: string) {
    if (!isSignedIn) {
      try {
        posthog.capture("checkout_started", {
          target_tier: tierFromPriceId(priceId),
          billing: billingFromPriceId(priceId),
          authenticated: false,
        });
      } catch { /* posthog optional */ }
      window.location.href = "/sign-up";
      return;
    }
    if (!priceId) {
      setError("This plan is not yet available. Check back soon.");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    try {
      posthog.capture("checkout_started", {
        target_tier: tierFromPriceId(priceId),
        billing: billingFromPriceId(priceId),
        authenticated: true,
        current_tier: tier,
      });
    } catch { /* posthog optional */ }
    setLoading(priceId);
    setError(null);
    try {
      const res = await fetch("/api/checkout", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ priceId }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null);
        setError(data?.error || `Checkout failed (HTTP ${res.status})`);
        window.scrollTo({ top: 0, behavior: "smooth" });
        return;
      }
      const data = await res.json();
      if (data.url) {
        window.location.href = data.url;
      } else {
        setError(data.error || "No checkout URL returned. Please try again.");
        window.scrollTo({ top: 0, behavior: "smooth" });
      }
    } catch {
      setError("Something went wrong. Please try again.");
      window.scrollTo({ top: 0, behavior: "smooth" });
    } finally {
      setLoading(null);
    }
  }

  return (
    <div className="max-w-5xl mx-auto py-12">
      <div className="text-center mb-12">
        <h1 className="text-3xl font-bold text-[#E8E8ED] mb-3">
          Watch three live strategies. Or do your own research.
        </h1>
        <p className="text-[#8888A0]">
          Full access for 7 days. Then choose the plan that fits.
        </p>
      </div>

      {/* Billing toggle */}
      <div className="flex items-center justify-center gap-3 mb-10">
        <button
          onClick={() => setBilling("monthly")}
          className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
            billing === "monthly"
              ? "bg-[#3B82F6] text-white"
              : "text-[#8888A0] hover:text-[#E8E8ED]"
          }`}
        >
          Monthly
        </button>
        <button
          onClick={() => setBilling("yearly")}
          className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
            billing === "yearly"
              ? "bg-[#3B82F6] text-white"
              : "text-[#8888A0] hover:text-[#E8E8ED]"
          }`}
        >
          Yearly
          <span className="ml-1 text-xs text-[#22C55E]">Save 17%</span>
        </button>
      </div>

      {error && (
        <div className="mb-6 rounded-lg border border-[#EF4444]/30 bg-[#EF4444]/10 px-4 py-3 text-sm text-[#EF4444]">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Free tier */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6 flex flex-col">
          <h2 className="text-lg font-bold text-[#E8E8ED] mb-1">Free</h2>
          <div className="text-3xl font-bold text-[#E8E8ED] mb-1">
            $0
            <span className="text-sm font-normal text-[#55556A]">/mo</span>
          </div>
          <p className="text-xs text-[#55556A] mb-6">No credit card required</p>

          <ul className="space-y-2 mb-8 flex-1">
            {FREE_FEATURES.map((f) => (
              <li key={f} className="flex items-start gap-2 text-sm text-[#8888A0]">
                <span className="text-[#22C55E] mt-0.5">&#10003;</span>
                {f}
              </li>
            ))}
          </ul>

          {!isSignedIn ? (
            <Link
              href="/sign-up"
              className="block w-full text-center rounded-lg border border-[#2A2A3A] px-4 py-2.5 text-sm font-medium text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors"
            >
              Get Started
            </Link>
          ) : (
            <div className="text-center text-sm text-[#55556A] py-2.5">
              {userIsPro ? "Included" : tier === "free" ? "Current Plan" : "Included"}
            </div>
          )}
        </div>

        {/* Pro tier */}
        <div className="rounded-lg border-2 border-[#3B82F6] bg-[#12121A] p-6 relative flex flex-col">
          <div className="absolute -top-3 left-1/2 -translate-x-1/2 rounded-full bg-[#3B82F6] px-3 py-0.5 text-xs font-bold text-white">
            MOST POPULAR
          </div>
          <h2 className="text-lg font-bold text-[#E8E8ED] mb-1">Pro</h2>
          <div className="text-3xl font-bold text-[#E8E8ED] mb-1">
            {billing === "monthly" ? "$25" : "$21"}
            <span className="text-sm font-normal text-[#55556A]">/mo</span>
          </div>
          <p className="text-xs text-[#55556A] mb-6">
            {billing === "yearly" ? "$250/yr billed annually" : "Billed monthly"}
          </p>

          <ul className="space-y-2 mb-8 flex-1">
            {PRO_FEATURES.map((f) => (
              <li key={f} className="flex items-start gap-2 text-sm text-[#E8E8ED]">
                <span className="text-[#3B82F6] mt-0.5">&#10003;</span>
                {f}
              </li>
            ))}
          </ul>

          {userIsPro && !userIsProPlus ? (
            <div className="text-center text-sm text-[#3B82F6] py-2.5 font-medium">
              Current Plan
            </div>
          ) : userIsProPlus ? (
            <div className="text-center text-sm text-[#55556A] py-2.5">Included in Pro+</div>
          ) : !isSignedIn ? (
            <Link
              href="/sign-up"
              className="block w-full text-center rounded-lg bg-[#3B82F6] px-4 py-2.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              Start Free Trial
            </Link>
          ) : (
            <div>
              <button
                onClick={() =>
                  handleCheckout(billing === "monthly" ? PRO_MONTHLY : PRO_YEARLY)
                }
                disabled={loading !== null}
                className="block w-full text-center rounded-lg bg-[#3B82F6] px-4 py-2.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors disabled:opacity-50"
              >
                {loading ? "Redirecting to Stripe..." : "Upgrade to Pro"}
              </button>
              {tier === "trial" && trialDaysLeft > 0 && (
                <p className="mt-2 text-center text-xs text-[#22C55E]">
                  Your {trialDaysLeft} remaining trial day{trialDaysLeft !== 1 ? "s" : ""} carry over &mdash; billing starts after.
                </p>
              )}
            </div>
          )}
        </div>

        {/* Pro+ tier */}
        <div className="rounded-lg border border-[#22C55E]/50 bg-[#12121A] p-6 relative flex flex-col">
          <div className="absolute -top-3 left-1/2 -translate-x-1/2 rounded-full bg-[#22C55E] px-3 py-0.5 text-xs font-bold text-[#0A0A0F]">
            RESEARCH
          </div>
          <h2 className="text-lg font-bold text-[#E8E8ED] mb-1">Pro+</h2>
          <div className="text-3xl font-bold text-[#E8E8ED] mb-1">
            {billing === "monthly" ? "$75" : "$63"}
            <span className="text-sm font-normal text-[#55556A]">/mo</span>
          </div>
          <p className="text-xs text-[#55556A] mb-6">
            {billing === "yearly" ? "$750/yr billed annually" : "Billed monthly"}
          </p>

          <ul className="space-y-2 mb-8 flex-1">
            {PRO_PLUS_FEATURES.map((f) => (
              <li key={f} className="flex items-start gap-2 text-sm text-[#E8E8ED]">
                <span className="text-[#22C55E] mt-0.5">&#10003;</span>
                {f}
              </li>
            ))}
          </ul>

          {userIsProPlus ? (
            <div className="text-center text-sm text-[#22C55E] py-2.5 font-medium">
              Current Plan
            </div>
          ) : !isSignedIn ? (
            <Link
              href="/sign-up"
              className="block w-full text-center rounded-lg border border-[#22C55E]/50 px-4 py-2.5 text-sm font-medium text-[#22C55E] hover:bg-[#22C55E]/10 transition-colors"
            >
              Start Free Trial
            </Link>
          ) : (
            <button
              onClick={() =>
                handleCheckout(billing === "monthly" ? PRO_PLUS_MONTHLY : PRO_PLUS_YEARLY)
              }
              disabled={loading !== null}
              className="block w-full text-center rounded-lg border border-[#22C55E]/50 px-4 py-2.5 text-sm font-medium text-[#22C55E] hover:bg-[#22C55E]/10 transition-colors disabled:opacity-50"
            >
              {loading ? "Redirecting to Stripe..." : userIsPro ? "Upgrade to Pro+" : "Get Pro+"}
            </button>
          )}
        </div>
      </div>

      <div className="mt-12 text-center text-sm text-[#55556A]">
        All plans include a 7-day free trial. Cancel anytime.
        <br />
        Questions? <a href="mailto:support@form4.app" className="text-[#3B82F6] hover:underline">support@form4.app</a>
      </div>
    </div>
  );
}
