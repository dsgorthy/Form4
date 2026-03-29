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
  "/terms",
  "/disclaimer",
  "/scoring",
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

    const meta = user.unsafeMetadata as Record<string, unknown> | undefined;
    if (!meta?.onboardingComplete) {
      router.push("/onboarding");
    }
  }, [isLoaded, isSignedIn, user, pathname, router]);

  return null;
}
