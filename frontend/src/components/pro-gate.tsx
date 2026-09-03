"use client";

import Link from "next/link";
import { useEffect } from "react";
import { posthog } from "@/lib/posthog";
import { useUser, useAuth } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";
import { GATED_CLASS } from "@/lib/structured-data";

interface ProGateProps {
  children: React.ReactNode;
  label?: string;
  /** Compact mode: blur only, no CTA overlay. Use for inline table cells. */
  compact?: boolean;
  /**
   * What clears the gate.
   *
   *   "pro"  Pro/trial only — the default, and what every existing caller means.
   *   "auth" any signed-in account, free included.
   *
   * "auth" exists for /explore. All search now lands there, so it is where a
   * cold visitor arrives, and the job of the blur is to convert them into an
   * account rather than to sell Pro. Gating that on isPro would blur the tool
   * for signed-in free users too, which is not the deal.
   */
  requires?: "pro" | "auth";
  /**
   * Entity this block is about ("NVDA", "Jen Hsun Huang"). When set, the CTA is
   * framed as following that entity rather than as a generic upgrade.
   *
   * A visitor who lands here from search has no relationship with the product
   * and mostly will not buy anything today, but a meaningful share will agree
   * to hear about the next filing — so the ask that closes a gated block should
   * be the cheap one. Sign-up starts the 7-day Pro trial, which is what makes
   * the promise real: watchlists are Pro, and the CTA would be a lie otherwise.
   */
  watch?: string;
}

/**
 * Inline blurred overlay for gated content within free pages.
 * Wraps children with blur + gradient fade + centered CTA.
 */
export function ProGate({
  children,
  label = "Unlock with Pro",
  compact = false,
  requires = "pro",
  watch,
}: ProGateProps) {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();

  // Pre-hydration, and therefore what the server renders and a crawler reads
  // without executing JS. Children show unblurred — blurring here and clearing
  // a beat later would flash at every subscriber — but the block still carries
  // GATED_CLASS, because this is the only markup a non-rendering crawl sees. If
  // the class only appeared post-hydration, the JSON-LD would declare a paywall
  // over a selector that matches nothing in the delivered HTML: full content to
  // the crawler with no accompanying declaration, which is the exact shape the
  // paywall markup exists to distinguish from cloaking. Serving it and marking
  // it is Google's sanctioned pattern; serving it silently is not.
  // INSTRUMENTED 2026-09-03.
  //
  // PostHog captures pageviews, autocapture and `checkout_started` -- but
  // checkout_started fires ONLY on /pricing, and 2 of 179 visitors reached
  // /pricing in 90 days. The funnel therefore showed arrivals and (zero)
  // purchases with nothing in between, and there was no way to tell whether
  // gates never render or render and get ignored. Those need opposite fixes:
  // one is placement, the other is the offer.
  //
  // THE HOOK IS CALLED UNCONDITIONALLY, above every early return. A first
  // attempt placed it after `if (cleared) return` / `if (compact) return`,
  // which violates the rules of hooks -- the call order changes between
  // renders as a user's auth state resolves. The CONDITION lives inside the
  // effect instead, which is where it belongs.
  useEffect(() => {
    if (!isLoaded) return;              // auth unresolved: not an impression
    if (requires === "auth" ? !!isSignedIn : isPro(user)) return;  // not gated
    posthog?.capture?.("gate_shown", {
      gate: "pro_gate",
      requires,
      compact: !!compact,
      signed_in: !!isSignedIn,
      watch_entity: watch ?? null,
      label: label ?? null,
      path: typeof window !== "undefined" ? window.location.pathname : null,
    });
    // Fires once auth has resolved and the gate is genuinely blurring
    // something. Re-renders from unrelated state are not new impressions.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, isSignedIn]);

  if (!isLoaded) {
    return <div className={`${GATED_CLASS} relative`}>{children}</div>;
  }

  const cleared = requires === "auth" ? !!isSignedIn : isPro(user);
  if (cleared) {
    return <>{children}</>;
  }

  // GATED_CLASS is what the page's JSON-LD names in hasPart.cssSelector. It has
  // to be on the real gated elements or the paywall declaration describes
  // nothing, and an undeclared wall in front of a crawler is indistinguishable
  // from cloaking. Applying it here means every gate carries it by construction
  // rather than by every call site remembering to.
  if (compact) {
    return (
      <span className={`${GATED_CLASS} blur-sm select-none pointer-events-none`}>
        {children}
      </span>
    );
  }

  // Different CTA for signed-out vs signed-in free users. Under requires="auth"
  // only the signed-out branch is ever reachable, since being signed in clears
  // the gate outright.
  const ctaHref = isSignedIn ? "/pricing" : "/sign-up";
  const ctaLabel = watch
    ? isSignedIn
      ? `Upgrade to follow ${watch}`
      : `Follow ${watch}`
    : isSignedIn
      ? "Upgrade to Pro"
      : "Start Free Trial";
  const subtitle = isSignedIn
    ? undefined
    : watch
      ? `Get alerted on the next ${watch} filing — 7-day free trial, no card required`
      : "7 days of full Pro access — no credit card required";

  return (
    <div className={`${GATED_CLASS} relative`}>
      <div className="blur-sm select-none pointer-events-none">{children}</div>
      <div className="absolute inset-0 z-10 bg-gradient-to-b from-transparent via-[#0A0A0F]/60 to-[#0A0A0F]/90 flex items-center justify-center">
        <div className="text-center">
          <div className="text-xs font-semibold uppercase tracking-widest text-[#8888A0] mb-2">
            {label}
          </div>
          <Link
            href={ctaHref}
            onClick={() =>
              posthog?.capture?.("gate_cta_clicked", {
                gate: "pro_gate",
                requires,
                signed_in: !!isSignedIn,
                destination: ctaHref,
                watch_entity: watch ?? null,
                path:
                  typeof window !== "undefined"
                    ? window.location.pathname
                    : null,
              })
            }
            className="inline-flex items-center gap-2 rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
          >
            {ctaLabel}
          </Link>
          {subtitle && (
            <div className="text-[10px] text-[#81819A] mt-2">{subtitle}</div>
          )}
        </div>
      </div>
    </div>
  );
}
