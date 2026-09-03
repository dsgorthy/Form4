"use client";

import { useEffect } from "react";
import { useUser, useAuth } from "@clerk/nextjs";
import { usePathname, useRouter } from "next/navigation";

const SKIP_PATHS = [
  "/onboarding",
  "/sign-in",
  "/sign-up",
  "/pricing",
  "/privacy",
  "/performance",
  "/terms",
  "/disclaimer",
  "/research/methodology",
  "/api",
];

export function OnboardingGuard() {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (!isLoaded || !isSignedIn || !user) return;
    if (SKIP_PATHS.some((p) => pathname.startsWith(p))) return;

    // DO NOT INTERRUPT A PENDING FOLLOW.
    //
    // A visitor arriving from search clicked "get alerted the next time this
    // person files", signed up, and is returning with ?follow= still set.
    // Redirecting them into a generic questionnaire at that exact moment
    // discards the only thing they asked for -- which is what happened before
    // the parameter existed, and why those accounts had no reason to come
    // back.
    //
    // Onboarding is not skipped forever; it fires on their next navigation.
    // It simply does not get to stand in front of the promise.
    if (typeof window !== "undefined" &&
        new URLSearchParams(window.location.search).has("follow")) {
      return;
    }

    const meta = user.unsafeMetadata as Record<string, unknown> | undefined;
    if (!meta?.onboardingComplete) {
      router.push("/onboarding");
    }
  }, [isLoaded, isSignedIn, user, pathname, router]);

  return null;
}
