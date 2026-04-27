"use client";

import { useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { initPostHog, posthog } from "@/lib/posthog";

const SIGNUP_FIRED_KEY = "ph_signup_fired";

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
      const tier = (user.publicMetadata as { tier?: string } | undefined)?.tier || "free";
      posthog.identify(user.id, {
        email: user.primaryEmailAddress?.emailAddress,
        name: user.fullName,
        tier,
      });

      // Fire signed_up exactly once per browser per user, when the account is
      // less than 5 minutes old. Older accounts identifying for the first time
      // on a new browser don't qualify as "just signed up".
      try {
        const flagKey = `${SIGNUP_FIRED_KEY}:${user.id}`;
        if (!localStorage.getItem(flagKey)) {
          const ageMs = Date.now() - new Date(user.createdAt || 0).getTime();
          if (ageMs >= 0 && ageMs < 5 * 60 * 1000) {
            posthog.capture("signed_up", { tier });
          }
          localStorage.setItem(flagKey, "1");
        }
      } catch {
        // localStorage unavailable (private mode, etc.) — skip dedupe
      }
    }
  }, [isLoaded, isSignedIn, user]);

  return <>{children}</>;
}
