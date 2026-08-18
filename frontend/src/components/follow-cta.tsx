"use client";

import Link from "next/link";
import { useAuth, useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";
import { GATED_CLASS } from "@/lib/structured-data";

/**
 * The band that closes a gated section.
 *
 * A visitor arriving from search has no relationship with the product and, on
 * this first visit, mostly will not buy. The expensive ask ("subscribe") is the
 * only one the page currently makes, so everyone who is not ready to pay leaves
 * with nothing and no reason to return. This asks the cheap thing instead —
 * follow this company or person and hear about the next filing — which is both
 * a lower bar and the thing that actually brings someone back.
 *
 * Sign-up starts the 7-day Pro trial, no card, and following now outlives it:
 * the watchlist and alert routes moved from `require_pro` to `require_auth`,
 * so a free account keeps them. That makes "follow" a promise we keep past day
 * seven, which is when it matters.
 *
 * The consequence is that this band asks two different things depending on who
 * is reading it. An anonymous visitor is asked to follow, because they cannot.
 * A signed-in free account already can, so asking them to "upgrade to follow"
 * would be selling them something they have — they are asked for Pro on what
 * Pro actually buys, the analytical layer named in `detail`.
 *
 * Renders nothing for subscribers — they already have the feature, and a
 * permanent upsell to an existing customer is just noise.
 */
export function FollowCta({
  entity,
  detail,
  marksGate = true,
}: {
  /** What is being followed, as it should read in a sentence: "NVDA", "Tim Cook". */
  entity: string;
  /** Optional specific of what stays hidden, e.g. "grades for 22 other insiders". */
  detail?: string;
  /**
   * Whether this CTA stands in for content behind the paywall.
   *
   * True (default) tags it with GATED_CLASS, which the page's JSON-LD names as
   * the gated region. Pass false where the CTA is pure retention on a page that
   * withholds nothing — a filing page serves a crawler and an anonymous visitor
   * identical content, and claiming a paywall there would describe a wall that
   * does not exist.
   */
  marksGate?: boolean;
}) {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();

  // Render nothing until Clerk resolves. Flashing an upsell at a paying
  // subscriber for a beat is worse than showing the CTA a beat late.
  if (!isLoaded || isPro(user)) return null;

  const href = isSignedIn ? "/pricing" : "/sign-up";
  const cta = isSignedIn ? "See the full track record" : `Follow ${entity}`;
  const headline = isSignedIn
    ? `Go deeper on ${entity}`
    : `Get alerted the next time ${entity} files`;
  const sub = isSignedIn
    ? `${detail ? `${detail}. ` : ""}Following ${entity} is already included with your account.`
    : `${detail ? `${detail}. ` : ""}7-day free trial, no credit card required.`;

  return (
    <div
      className={`${marksGate ? GATED_CLASS : ""} mt-4 flex flex-col gap-3 rounded-lg border border-[#2A2A3A] bg-[#12121A] px-5 py-4 sm:flex-row sm:items-center sm:justify-between`}
    >
      <div className="min-w-0">
        <div className="text-sm font-medium text-[#E8E8ED]">{headline}</div>
        <div className="mt-0.5 text-xs text-[#8888A0]">{sub}</div>
      </div>
      <Link
        href={href}
        className="inline-flex shrink-0 items-center justify-center rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
      >
        {cta}
      </Link>
    </div>
  );
}
