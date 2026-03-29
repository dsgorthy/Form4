"use client";

import Link from "next/link";
import { useUser, useAuth } from "@clerk/nextjs";
import { getUserTier, getTrialDaysLeft, getGraceDaysLeft } from "@/lib/subscription";

export function TrialBanner() {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();

  if (!isLoaded) return null;

  // Unauthenticated: prompt to sign up for free trial
  if (!isSignedIn) {
    return (
      <div className="w-full border-b border-[#22C55E]/20 bg-gradient-to-r from-[#22C55E]/10 via-[#22C55E]/5 to-transparent">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-1.5 md:px-6">
          <div className="flex items-center gap-2 text-xs">
            <span className="text-[#E8E8ED] font-medium">
              Get 7 days of full Pro access free
            </span>
            <span className="hidden sm:inline text-[#55556A]">
              — real-time alerts, signal quality grades, full trade history
            </span>
          </div>
          <Link
            href="/sign-up"
            className="shrink-0 rounded-md bg-[#22C55E] px-3 py-1 text-[11px] font-medium text-white hover:bg-[#16A34A] transition-colors"
          >
            Start Free Trial
          </Link>
        </div>
      </div>
    );
  }

  const tier = getUserTier(user);

  // Trial user: show days remaining
  if (tier === "trial") {
    const daysLeft = getTrialDaysLeft(user);
    return (
      <div className="w-full border-b border-[#3B82F6]/20 bg-gradient-to-r from-[#3B82F6]/10 via-[#3B82F6]/5 to-transparent">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-1.5 md:px-6">
          <div className="flex items-center gap-2 text-xs">
            <span className="rounded-full bg-[#3B82F6]/20 px-2 py-0.5 text-[10px] font-semibold text-[#3B82F6] uppercase tracking-wider">
              Free Trial
            </span>
            <span className="text-[#8888A0]">
              {daysLeft} day{daysLeft !== 1 ? "s" : ""} remaining — full Pro access
            </span>
          </div>
          <Link
            href="/pricing"
            className="shrink-0 rounded-md bg-[#3B82F6] px-3 py-1 text-[11px] font-medium text-white hover:bg-[#2563EB] transition-colors"
          >
            Upgrade Now
          </Link>
        </div>
      </div>
    );
  }

  // Grace period: trial ended, signals delayed 24h
  if (tier === "grace") {
    const daysLeft = getGraceDaysLeft(user);
    return (
      <div className="w-full border-b border-[#F59E0B]/20 bg-gradient-to-r from-[#F59E0B]/10 via-[#F59E0B]/5 to-transparent">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-1.5 md:px-6">
          <div className="flex items-center gap-2 text-xs">
            <span className="rounded-full bg-[#F59E0B]/20 px-2 py-0.5 text-[10px] font-semibold text-[#F59E0B] uppercase tracking-wider">
              Grace Period
            </span>
            <span className="text-[#8888A0]">
              Your trial ended. Signals are delayed 24h.
              <span className="hidden sm:inline">
                {" "}{daysLeft} day{daysLeft !== 1 ? "s" : ""} left before full restrictions.
              </span>
            </span>
          </div>
          <Link
            href="/pricing"
            className="shrink-0 rounded-md bg-[#F59E0B] px-3 py-1 text-[11px] font-medium text-[#0A0A0F] hover:bg-[#D97706] transition-colors"
          >
            Upgrade for Real-Time
          </Link>
        </div>
      </div>
    );
  }

  return null;
}
