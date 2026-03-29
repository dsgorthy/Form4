"use client";

import { useState } from "react";
import { useUser, useAuth } from "@clerk/nextjs";
import { useRouter } from "next/navigation";

const USER_TYPES = [
  { value: "individual", label: "Individual Investor", desc: "Managing my own portfolio" },
  { value: "advisor", label: "Financial Advisor", desc: "Advising clients on investments" },
  { value: "quant", label: "Quant / Analyst", desc: "Data-driven research & modeling" },
  { value: "fund_manager", label: "Fund Manager", desc: "Managing institutional capital" },
  { value: "journalist", label: "Journalist / Researcher", desc: "Investigating & reporting" },
  { value: "student", label: "Student / Academic", desc: "Learning & academic research" },
];

const USE_CASES = [
  { value: "trading_signals", label: "Trading Signals", desc: "Finding actionable trade ideas" },
  { value: "research", label: "Research & Due Diligence", desc: "Deep-dive analysis on companies" },
  { value: "portfolio", label: "Portfolio Monitoring", desc: "Tracking insider activity in my holdings" },
  { value: "compliance", label: "Compliance Monitoring", desc: "Regulatory oversight & reporting" },
  { value: "academic", label: "Academic Research", desc: "Data analysis & studies" },
  { value: "tracking", label: "Tracking Insiders", desc: "Following specific insiders or companies" },
];

const EXPERIENCE_LEVELS = [
  { value: "beginner", label: "New to it" },
  { value: "intermediate", label: "Somewhat familiar" },
  { value: "expert", label: "I use it regularly" },
];

const REFERRAL_SOURCES = [
  { value: "search", label: "Search (Google, etc.)" },
  { value: "social", label: "Social Media" },
  { value: "referral", label: "Recommendation" },
  { value: "press", label: "News / Blog" },
  { value: "community", label: "Financial Community" },
  { value: "other", label: "Other" },
  { value: "not_specified", label: "Prefer not to say" },
];

const DESTINATION_MAP: Record<string, string> = {
  trading_signals: "/feed?trade_type=buy&min_tier=2",
  research: "/screener",
  portfolio: "/settings",
  compliance: "/feed",
  academic: "/leaderboard",
  tracking: "/screener",
};

function OptionCard({
  selected,
  onClick,
  label,
  desc,
}: {
  selected: boolean;
  onClick: () => void;
  label: string;
  desc?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`w-full text-left rounded-lg border p-3 transition-all ${
        selected
          ? "border-[#3B82F6] bg-[#3B82F6]/10 ring-1 ring-[#3B82F6]/50"
          : "border-[#2A2A3A] bg-[#12121A] hover:border-[#55556A] hover:bg-[#1A1A26]"
      }`}
    >
      <div className={`text-sm font-medium ${selected ? "text-[#3B82F6]" : "text-[#E8E8ED]"}`}>
        {label}
      </div>
      {desc && (
        <div className="text-xs text-[#55556A] mt-0.5">{desc}</div>
      )}
    </button>
  );
}

function PillOption({
  selected,
  onClick,
  label,
}: {
  selected: boolean;
  onClick: () => void;
  label: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-4 py-2 text-sm font-medium transition-all ${
        selected
          ? "bg-[#3B82F6] text-white"
          : "border border-[#2A2A3A] text-[#8888A0] hover:border-[#55556A] hover:text-[#E8E8ED]"
      }`}
    >
      {label}
    </button>
  );
}

export default function OnboardingPage() {
  const { user } = useUser();
  const { getToken } = useAuth();
  const router = useRouter();

  const [userType, setUserType] = useState<string | null>(null);
  const [useCase, setUseCase] = useState<string | null>(null);
  const [experience, setExperience] = useState<string | null>(null);
  const [referral, setReferral] = useState<string>("not_specified");
  const [submitting, setSubmitting] = useState(false);

  const canSubmit = userType && useCase && experience;
  const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

  async function submit(skipped: boolean) {
    if (!user) return;
    setSubmitting(true);

    try {
      // Write to Clerk unsafeMetadata
      await user.update({
        unsafeMetadata: {
          onboardingComplete: true,
          ...(skipped
            ? { onboardingSkipped: true }
            : {
                userType,
                primaryUseCase: useCase,
                experienceLevel: experience,
                referralSource: referral,
              }),
        },
      });

      // Persist to backend DB (non-blocking)
      if (!skipped) {
        const token = await getToken();
        fetch(`${apiBase}/onboarding`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify({
            user_type: userType,
            primary_use_case: useCase,
            experience_level: experience,
            referral_source: referral === "not_specified" ? null : referral,
          }),
        }).catch(() => {}); // Best-effort, Clerk metadata is the primary store
      }

      // Redirect to personalized destination
      const destination = skipped ? "/" : (DESTINATION_MAP[useCase || ""] || "/");
      router.push(destination);
    } catch {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-screen bg-[#07070C] flex items-start justify-center pt-12 px-4 pb-16">
      <div className="w-full max-w-lg">
        {/* Header */}
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-[#E8E8ED] mb-2">
            Welcome to Form<span className="text-[#3B82F6]">4</span>
          </h1>
          <p className="text-sm text-[#8888A0]">
            Help us personalize your experience. Takes under 30 seconds.
          </p>
        </div>

        {/* Q1: User type */}
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-[#E8E8ED] mb-3">
            What best describes you?
          </h2>
          <div className="grid grid-cols-2 gap-2">
            {USER_TYPES.map((opt) => (
              <OptionCard
                key={opt.value}
                selected={userType === opt.value}
                onClick={() => setUserType(opt.value)}
                label={opt.label}
                desc={opt.desc}
              />
            ))}
          </div>
        </div>

        {/* Q2: Use case */}
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-[#E8E8ED] mb-3">
            What will you primarily use Form4 for?
          </h2>
          <div className="grid grid-cols-2 gap-2">
            {USE_CASES.map((opt) => (
              <OptionCard
                key={opt.value}
                selected={useCase === opt.value}
                onClick={() => setUseCase(opt.value)}
                label={opt.label}
                desc={opt.desc}
              />
            ))}
          </div>
        </div>

        {/* Q3: Experience */}
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-[#E8E8ED] mb-3">
            How familiar are you with insider trading data?
          </h2>
          <div className="flex flex-wrap gap-2">
            {EXPERIENCE_LEVELS.map((opt) => (
              <PillOption
                key={opt.value}
                selected={experience === opt.value}
                onClick={() => setExperience(opt.value)}
                label={opt.label}
              />
            ))}
          </div>
        </div>

        {/* Q4: Referral (optional) */}
        <div className="mb-10">
          <h2 className="text-sm font-semibold text-[#E8E8ED] mb-1">
            How did you hear about Form4?
          </h2>
          <p className="text-xs text-[#55556A] mb-3">Optional</p>
          <div className="flex flex-wrap gap-2">
            {REFERRAL_SOURCES.map((opt) => (
              <PillOption
                key={opt.value}
                selected={referral === opt.value}
                onClick={() => setReferral(opt.value)}
                label={opt.label}
              />
            ))}
          </div>
        </div>

        {/* Submit */}
        <div className="space-y-3">
          <button
            onClick={() => submit(false)}
            disabled={!canSubmit || submitting}
            className="w-full rounded-lg bg-[#3B82F6] px-4 py-3 text-sm font-semibold text-white hover:bg-[#2563EB] transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {submitting ? "Setting up..." : "Continue"}
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
