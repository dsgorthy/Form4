"use client";

import { useState } from "react";
import { useUser, useAuth } from "@clerk/nextjs";
import { useRouter } from "next/navigation";
import { posthog } from "@/lib/posthog";

export type StrategyChoice = {
  value: string;
  label: string;
  brief: string;
  stats: string;
};

const REFERRAL_SOURCES = [
  { value: "search", label: "Search" },
  { value: "social", label: "Social Media" },
  { value: "referral", label: "Recommendation" },
  { value: "press", label: "News / Blog" },
  { value: "community", label: "Finance Community" },
  { value: "other", label: "Other" },
];

export default function OnboardingForm({ strategies }: { strategies: StrategyChoice[] }) {
  const { user } = useUser();
  const { getToken } = useAuth();
  const router = useRouter();

  const existing = user?.unsafeMetadata as Record<string, string> | undefined;

  const [selectedStrategy, setSelectedStrategy] = useState<string>(existing?.defaultStrategy || "quality_momentum");
  const [referral, setReferral] = useState<string>(existing?.referralSource || "");
  const [submitting, setSubmitting] = useState(false);

  const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

  async function submit(skipped: boolean) {
    if (!user) return;
    setSubmitting(true);

    try {
      const prev = (user.unsafeMetadata || {}) as Record<string, unknown>;
      await user.update({
        unsafeMetadata: {
          ...prev,
          onboardingComplete: true,
          ...(skipped
            ? { onboardingSkipped: !prev.defaultStrategy }
            : {
                onboardingSkipped: false,
                defaultStrategy: selectedStrategy,
                referralSource: referral || null,
              }),
        },
      });

      if (!skipped) {
        const token = await getToken();
        fetch(`${apiBase}/onboarding`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify({
            default_strategy: selectedStrategy,
            referral_source: referral || null,
          }),
        }).catch(() => {});
      }

      try {
        posthog.capture("onboarding_complete", {
          skipped,
          strategy: skipped ? null : selectedStrategy,
          referral_source: skipped ? null : referral || null,
        });
      } catch {
        // posthog uninitialized in dev — non-blocking
      }

      router.push(`/portfolio?strategy=${selectedStrategy}`);
    } catch {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-screen bg-[#07070C] flex items-start justify-center pt-12 px-4 pb-16">
      <div className="w-full max-w-lg">
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold text-[#E8E8ED] mb-2">
            Welcome to Form<span className="text-[#3B82F6]">4</span>
          </h1>
          <p className="text-sm text-[#8888A0]">
            We run three live insider-trading strategies. Pick the one you want to follow first.
          </p>
        </div>

        {/* The trial, said out loud at the only moment the user is certain to
            be reading.
            
            It was already granted automatically and already surfaced — a
            banner on every page and a six-email ladder from day 0 — but
            nowhere in the signup flow did anything say what had just been
            given. A thin strip above the fold is easy to read as an upsell;
            the sentence below is the one that lands, because it arrives while
            the user is still deciding whether this was worth signing up for.
            
            No card was taken and none is required, so that is stated plainly:
            it is the single most reassuring fact available here and leaving it
            implied wastes it. */}
        <div className="mb-8 rounded-lg border border-[#22C55E]/25 bg-[#22C55E]/5 px-5 py-4 text-center">
          <p className="text-sm font-semibold text-[#22C55E]">
            Your 7 days of Pro start now — free
          </p>
          <p className="mt-1.5 text-xs leading-relaxed text-[#8888A0]">
            Every strategy, the full filing feed, insider ratings and real-time
            alerts. No card required, and nothing to cancel.
          </p>
        </div>

        {/* Strategy selection */}
        <div className="mb-8 space-y-3">
          {strategies.map((s) => (
            <button
              key={s.value}
              type="button"
              onClick={() => setSelectedStrategy(s.value)}
              className={`w-full text-left rounded-lg border p-4 transition-all ${
                selectedStrategy === s.value
                  ? "border-[#3B82F6] bg-[#3B82F6]/10 ring-1 ring-[#3B82F6]/50"
                  : "border-[#2A2A3A] bg-[#12121A] hover:border-[#55556A] hover:bg-[#1A1A26]"
              }`}
            >
              <div className="flex items-center gap-2 mb-1">
                <span className={`text-sm font-semibold ${selectedStrategy === s.value ? "text-[#3B82F6]" : "text-[#E8E8ED]"}`}>
                  {s.label}
                </span>
              </div>
              <div className="text-xs text-[#8888A0] mb-1">{s.brief}</div>
              <div className="text-[10px] text-[#55556A] font-mono">{s.stats}</div>
            </button>
          ))}
        </div>

        {/* Referral (optional, compact) */}
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-[#E8E8ED] mb-2">
            How did you find us? <span className="text-[#55556A] font-normal">Optional</span>
          </h2>
          <div className="flex flex-wrap gap-2">
            {REFERRAL_SOURCES.map((opt) => (
              <button
                key={opt.value}
                type="button"
                onClick={() => setReferral(referral === opt.value ? "" : opt.value)}
                className={`rounded-full px-3 py-1.5 text-xs font-medium transition-all ${
                  referral === opt.value
                    ? "bg-[#3B82F6] text-white"
                    : "border border-[#2A2A3A] text-[#8888A0] hover:border-[#55556A] hover:text-[#E8E8ED]"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>

        <div className="space-y-3">
          <button
            onClick={() => submit(false)}
            disabled={submitting}
            className="w-full rounded-lg bg-[#3B82F6] px-4 py-3 text-sm font-semibold text-white hover:bg-[#2563EB] transition-colors disabled:opacity-40"
          >
            {submitting ? "Setting up..." : "Follow this strategy"}
          </button>
          <button
            onClick={() => submit(true)}
            disabled={submitting}
            className="w-full text-center text-xs text-[#55556A] hover:text-[#8888A0] transition-colors py-1"
          >
            Skip for now
          </button>
        </div>
      </div>
    </div>
  );
}
