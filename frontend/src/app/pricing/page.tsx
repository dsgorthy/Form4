"use client";

import { useUser, useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { useState } from "react";
import { getUserTier, getTrialDaysLeft } from "@/lib/subscription";

const PRO_MONTHLY = process.env.NEXT_PUBLIC_STRIPE_PRO_MONTHLY_PRICE_ID || "";
const PRO_YEARLY = process.env.NEXT_PUBLIC_STRIPE_PRO_YEARLY_PRICE_ID || "";
const API_MONTHLY = process.env.NEXT_PUBLIC_STRIPE_API_MONTHLY_PRICE_ID || "";

const FREE_FEATURES = [
  "Last 90 days of filings",
  "Basic search & company pages",
  "Congress trades",
  "Insider tier badges (visible)",
  "Dashboard stats & heatmap",
];

const PRO_FEATURES = [
  "Full history (2016+)",
  "Insider scores & track records",
  "Percentile rankings",
  "Cluster detection & analysis",
  "Leaderboard access",
  "Insider profiles & return distributions",
  "Activity inflections",
  "Sell cessation signals",
  "CSV export",
  "Congress convergence alerts",
];

const API_FEATURES = [
  "Programmatic API access",
  "API key management",
  "Up to 3 concurrent keys",
];

export default function PricingPage() {
  const { user, isLoaded } = useUser();
  const { isSignedIn } = useAuth();
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [billing, setBilling] = useState<"monthly" | "yearly">("monthly");

  const tier = getUserTier(user);
  const userIsPro = tier === "pro";
  const trialDaysLeft = getTrialDaysLeft(user);

  async function handleCheckout(priceId: string) {
    if (!isSignedIn) {
      window.location.href = "/sign-up";
      return;
    }
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
    <div className="max-w-4xl mx-auto py-12">
      <div className="text-center mb-12">
        <h1 className="text-3xl font-bold text-[#E8E8ED] mb-3">
          Form4 Plans
        </h1>
        <p className="text-[#8888A0]">
          Actionable insider trading intelligence. Start free, upgrade when ready.
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
            RECOMMENDED
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

          {userIsPro ? (
            <div className="text-center text-sm text-[#3B82F6] py-2.5 font-medium">
              Current Plan
            </div>
          ) : !isSignedIn ? (
            <Link
              href="/sign-up"
              className="block w-full text-center rounded-lg bg-[#3B82F6] px-4 py-2.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              Sign Up to Upgrade
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

        {/* API add-on */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6 flex flex-col">
          <h2 className="text-lg font-bold text-[#E8E8ED] mb-1">API Add-on</h2>
          <div className="text-3xl font-bold text-[#E8E8ED] mb-1">
            +$15
            <span className="text-sm font-normal text-[#55556A]">/mo</span>
          </div>
          <p className="text-xs text-[#55556A] mb-6">Requires Pro subscription</p>

          <ul className="space-y-2 mb-8 flex-1">
            {API_FEATURES.map((f) => (
              <li key={f} className="flex items-start gap-2 text-sm text-[#8888A0]">
                <span className="text-[#22C55E] mt-0.5">&#10003;</span>
                {f}
              </li>
            ))}
          </ul>

          <button
            onClick={() => handleCheckout(API_MONTHLY)}
            disabled={loading !== null || !userIsPro}
            className="block w-full text-center rounded-lg border border-[#2A2A3A] px-4 py-2.5 text-sm font-medium text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors disabled:opacity-50"
          >
            {!userIsPro ? "Pro required" : loading ? "Redirecting to Stripe..." : "Add API Access"}
          </button>
        </div>
      </div>
    </div>
  );
}
