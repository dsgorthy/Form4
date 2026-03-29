"use client";

import Link from "next/link";
import { useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";

interface UpgradePromptProps {
  feature: string;
  children?: React.ReactNode;
}

/**
 * Full-page upgrade overlay for routes where the entire page is Pro content.
 * Shows a teaser preview (blurred) behind the overlay.
 */
export function UpgradePrompt({ feature, children }: UpgradePromptProps) {
  const { user, isLoaded } = useUser();

  if (!isLoaded) return null;
  if (isPro(user)) return null;

  return (
    <div className="relative min-h-[60vh]">
      {children && (
        <div className="blur-sm select-none pointer-events-none opacity-50">
          {children}
        </div>
      )}
      <div className="absolute inset-0 z-10 flex items-center justify-center bg-[#0A0A0F]/80">
        <div className="text-center max-w-md mx-auto p-8">
          <div className="w-12 h-12 rounded-full bg-[#3B82F6]/20 flex items-center justify-center mx-auto mb-4">
            <svg
              className="w-6 h-6 text-[#3B82F6]"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
              />
            </svg>
          </div>
          <h2 className="text-xl font-bold text-[#E8E8ED] mb-2">
            {feature} is a Pro feature
          </h2>
          <p className="text-sm text-[#8888A0] mb-6">
            Upgrade to Form4 Pro for full access to {feature.toLowerCase()},
            insider scores, track records, and more.
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-3">
            <Link
              href="/pricing"
              className="inline-flex items-center gap-2 rounded-lg bg-[#3B82F6] px-6 py-2.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              View Plans
            </Link>
            {!user && (
              <Link
                href="/sign-in"
                className="text-sm text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
              >
                Already a subscriber? Sign in
              </Link>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
