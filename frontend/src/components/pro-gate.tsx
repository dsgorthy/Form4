"use client";

import Link from "next/link";
import { useUser, useAuth } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";

interface ProGateProps {
  children: React.ReactNode;
  label?: string;
  /** Compact mode: blur only, no CTA overlay. Use for inline table cells. */
  compact?: boolean;
}

/**
 * Inline blurred overlay for gated content within free pages.
 * Wraps children with blur + gradient fade + centered upgrade CTA.
 * Pro and trial users see content normally.
 */
export function ProGate({ children, label = "Unlock with Pro", compact = false }: ProGateProps) {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();

  if (!isLoaded) {
    return <div className="relative">{children}</div>;
  }

  if (isPro(user)) {
    return <>{children}</>;
  }

  if (compact) {
    return (
      <span className="blur-sm select-none pointer-events-none">{children}</span>
    );
  }

  // Different CTA for signed-out vs signed-in free users
  const ctaHref = isSignedIn ? "/pricing" : "/sign-up";
  const ctaLabel = isSignedIn ? "Upgrade to Pro" : "Start Free Trial";
  const subtitle = isSignedIn
    ? undefined
    : "7 days of full Pro access — no credit card required";

  return (
    <div className="relative">
      <div className="blur-sm select-none pointer-events-none">{children}</div>
      <div className="absolute inset-0 z-10 bg-gradient-to-b from-transparent via-[#0A0A0F]/60 to-[#0A0A0F]/90 flex items-center justify-center">
        <div className="text-center">
          <div className="text-xs font-semibold uppercase tracking-widest text-[#8888A0] mb-2">
            {label}
          </div>
          <Link
            href={ctaHref}
            className="inline-flex items-center gap-2 rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
          >
            {ctaLabel}
          </Link>
          {subtitle && (
            <div className="text-[10px] text-[#55556A] mt-2">{subtitle}</div>
          )}
        </div>
      </div>
    </div>
  );
}
