"use client";

import { useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { usePathname } from "next/navigation";
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

  useScrollDepth();

  return <>{children}</>;
}

/** Milestones, as percent of scrollable height. */
const DEPTHS = [25, 50, 75, 100] as const;

/**
 * How far down the page people actually get.
 *
 * Added 2026-09-10. "They don't scroll far enough to reach the CTA" had been
 * asserted for a week on the strength of 1.06 pageviews per search visitor,
 * and it was never measured — the event being read as "saw the CTA" fired on
 * MOUNT, not on visibility, so it said nothing about scrolling at all. The
 * real cause turned out to be that the component was not rendering. This
 * exists so the next claim about scrolling is a measurement.
 *
 * One event per milestone per pageview, never repeated, so a jittery
 * touchscreen cannot inflate the series. A page shorter than the viewport
 * reports 100 immediately, which is true: there was nothing below the fold
 * and the reader saw all of it.
 */
function useScrollDepth() {
  const pathname = usePathname();

  useEffect(() => {
    if (typeof window === "undefined") return;

    const fired = new Set<number>();

    const emit = (depth: number) => {
      if (fired.has(depth)) return;
      fired.add(depth);
      posthog?.capture?.("scroll_depth", { depth, path: pathname ?? null });
    };

    const measure = () => {
      const doc = document.documentElement;
      const scrollable = doc.scrollHeight - window.innerHeight;
      // Nothing to scroll: the whole page is the fold.
      if (scrollable <= 0) {
        DEPTHS.forEach(emit);
        return;
      }
      const pct = ((window.scrollY || doc.scrollTop || 0) / scrollable) * 100;
      for (const d of DEPTHS) if (pct >= d - 1) emit(d);
    };

    // Measure once on mount: a short page, or a browser restoring a scroll
    // position on a back-navigation, may never fire a scroll event at all.
    measure();
    window.addEventListener("scroll", measure, { passive: true });
    window.addEventListener("resize", measure, { passive: true });
    return () => {
      window.removeEventListener("scroll", measure);
      window.removeEventListener("resize", measure);
    };
  }, [pathname]);
}
