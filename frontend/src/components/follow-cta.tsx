"use client";

import Link from "next/link";
import { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";
import { posthog } from "@/lib/posthog";
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
  follow,
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
  /**
   * The thing to actually follow, as a stable identifier — NOT the display
   * name in `entity`.
   *
   * Without this the CTA is a promise it cannot keep. A visitor arriving from
   * search reads "get alerted the next time Erez Chimovits files", clicks,
   * creates an account, and lands in onboarding with the insider discarded:
   * they asked for one specific thing and received a form. Whatever the
   * sign-up rate, that account's retention value is near zero, because the
   * reason it was created was never acted on.
   *
   * Carried through sign-up as `?follow=insider:1234` and completed on return.
   */
  follow?: { kind: "insider" | "ticker"; id: string | number };
}) {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();
  const pathname = usePathname();
  const ref = useRef<HTMLDivElement | null>(null);
  const viewed = useRef(false);

  const pro = isPro(user);
  const token = follow ? `${follow.kind}:${follow.id}` : null;

  // HOOK FIRST, unconditionally, above every early return. The condition lives
  // inside. Placing it after `if (isPro(user)) return null` would change hook
  // order between renders as Clerk resolves — the same rules-of-hooks mistake
  // made in pro-gate the same day.
  //
  // Note this no longer waits on isLoaded, because the render below no longer
  // does either. See the block comment on the early return.
  useEffect(() => {
    if (pro) return;
    posthog?.capture?.("follow_cta_shown", {
      entity,
      signed_in: !!isSignedIn,
      has_follow_target: !!token,
      auth_resolved: !!isLoaded,
      path: pathname ?? null,
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSignedIn, pro]);

  // `follow_cta_shown` fires on MOUNT and always did — it never meant the
  // visitor saw anything, only that React rendered it. That reading cost a
  // session: a 8.7% "saw the CTA" rate was read as "they don't scroll",
  // when the component was not mounting at all.
  //
  // This is the honest version, and it is a SECOND event rather than a
  // redefinition of the first, so the existing series keeps one meaning for
  // its whole history.
  useEffect(() => {
    if (pro || !ref.current || typeof IntersectionObserver === "undefined") return;
    const el = ref.current;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (!e.isIntersecting || viewed.current) continue;
          viewed.current = true;
          posthog?.capture?.("follow_cta_viewed", {
            entity,
            signed_in: !!isSignedIn,
            has_follow_target: !!token,
            path: pathname ?? null,
          });
          obs.disconnect();
        }
      },
      // Half the band, so a sliver clipping into view at the moment someone
      // closes the tab does not count as having been read.
      { threshold: 0.5 },
    );
    obs.observe(el);
    return () => obs.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pro]);

  // RENDER BEFORE CLERK RESOLVES, in the anonymous shape.
  //
  // This used to be `if (!isLoaded || pro) return null`, defended as "flashing
  // an upsell at a paying subscriber for a beat is worse than showing the CTA
  // a beat late". Measured 2026-09-10, that trade is strictly bad:
  //
  //   people whose CTA rendered      15    median 97.9s on page
  //   people whose CTA never did     81    median 17.2s on page
  //
  // Search visitors leave in a median of 17 seconds. Clerk does not resolve in
  // that window, so for 84% of them the CTA never mounted and the product
  // never made its cheapest ask. The only visitors who saw it were the ones
  // who stayed 5.7x longer — the ones least in need of persuading.
  //
  // The flicker it was avoiding costs a paying subscriber a fraction of a
  // second. There are currently ZERO active subscriptions, so it was
  // protecting nobody. Even once there are, "subscriber sees a band for one
  // frame" is a smaller harm than "every search visitor is asked for nothing".
  //
  // Anonymous is the correct pre-resolution shape because it is what the
  // overwhelming majority of these page views are, and because it is the only
  // variant that is never WRONG for a signed-out reader. Once Clerk resolves,
  // `isSignedIn` flips the copy and `pro` unmounts the band entirely.
  if (pro) return null;

  // A signed-in free account is NOT asked for money here. They already have
  // following; selling them what they have is noise, and the relationship is
  // too thin to pitch Pro on an entity page. Pro is pitched later, by email,
  // to someone already receiving something useful.
  const href = isSignedIn
    ? (token ? `${pathname}?follow=${encodeURIComponent(token)}` : "/pricing")
    : token
      ? `/sign-up?follow=${encodeURIComponent(token)}&next=${encodeURIComponent(pathname ?? "/")}`
      : "/sign-up";
  const cta = isSignedIn
    ? (token ? `Follow ${entity}` : "See the full track record")
    : `Follow ${entity}`;
  const headline = isSignedIn
    ? (token ? `Get alerted the next time ${entity} files` : `Go deeper on ${entity}`)
    : `Get alerted the next time ${entity} files`;
  const sub = isSignedIn
    ? `${detail ? `${detail}. ` : ""}Following ${entity} is already included with your account.`
    : `${detail ? `${detail}. ` : ""}7-day free trial, no credit card required.`;

  return (
    <div
      ref={ref}
      className={`${marksGate ? GATED_CLASS : ""} mt-4 flex flex-col gap-3 rounded-lg border border-[#2A2A3A] bg-[#12121A] px-5 py-4 sm:flex-row sm:items-center sm:justify-between`}
    >
      <div className="min-w-0">
        <div className="text-sm font-medium text-[#E8E8ED]">{headline}</div>
        <div className="mt-0.5 text-xs text-[#8888A0]">{sub}</div>
      </div>
      <Link
        href={href}
        onClick={() =>
          posthog?.capture?.("follow_cta_clicked", {
            entity,
            signed_in: !!isSignedIn,
            follow_target: token,
            destination: href,
            path: pathname ?? null,
          })
        }
        className="inline-flex shrink-0 items-center justify-center rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
      >
        {cta}
      </Link>
    </div>
  );
}
