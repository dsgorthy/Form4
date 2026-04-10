"use client";

import { useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { initPostHog, posthog } from "@/lib/posthog";

/**
 * Initializes PostHog and identifies the user once Clerk auth loads.
 * Place inside ClerkProvider in layout.tsx.
 */
export function AnalyticsProvider({ children }: { children: React.ReactNode }) {
  const { isLoaded, isSignedIn } = useAuth();
  const { user } = useUser();

  // Initialize PostHog for all visitors (including auth flow), identify once signed in
  useEffect(() => {
    if (!isLoaded) return;
    initPostHog();

    if (isSignedIn && user) {
      posthog.identify(user.id, {
        email: user.primaryEmailAddress?.emailAddress,
        name: user.fullName,
      });
    }
  }, [isLoaded, isSignedIn, user]);

  return <>{children}</>;
}
