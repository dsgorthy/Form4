"use client";

import Link from "next/link";
import { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";
import { posthog } from "@/lib/posthog";
import { useAuth, useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";
import { GATED_CLASS } from "@/lib/structured-data";

/**
 * The follow ask on an insider or company page, in two shapes: `FollowInline`
 * is one line in the first viewport, `FollowCta` the band that closes a gated
 * section further down. Same offer, same events, told apart by `placement`.
 *
 * A visitor arriving from search has no relationship with the product and, on
 * this first visit, mostly will not buy. So the page asks the cheap thing —
 * follow this person or company and get an email about the next filing — which
 * is a lower bar and the thing that actually brings someone back.
 *
 * FOLLOWING IS FREE AND THE COPY HAS TO SAY SO. Until 2026-09-16 the anonymous
 * band read "7-day free trial, no credit card required" under the follow
 * button — the Pro trial's line, attached to a feature a free account already
 * has (10 insiders, 10 companies, an email when they file). Search visitors
 * read a paid product they'd be trialling, and 0 of them ever clicked. Trial
 * copy lives on /pricing only.
 *
 * Who is asked what:
 *   anonymous          — follow for free, via sign-up, with the target carried
 *                        through as `?follow=insider:1234` (see PendingFollow)
 *   signed-in free     — follow directly; they can, so nothing is sold here
 *   signed-in, no target — the one place Pro is mentioned, on what Pro buys
 *   Pro                — nothing; a permanent upsell to a customer is noise
 */

type FollowTarget = { kind: "insider" | "ticker"; id: string | number };
type Placement = "band" | "top";

/**
 * posthog-js drops a capture that arrives before `init()`, and the provider's
 * init runs in an effect — so a child's mount effect fires FIRST and its event
 * is lost. That is why `follow_cta_shown` reached PostHog for 5% of landers
 * while `follow_cta_viewed`, which fires later on scroll, reached it for 20%.
 * Wait for the client to be loaded, briefly, then send.
 */
function captureWhenReady(name: string, props: Record<string, unknown>) {
  if (typeof window === "undefined") return () => {};
  let tries = 0;
  const attempt = () => {
    if ((posthog as unknown as { __loaded?: boolean }).__loaded) {
      posthog.capture(name, props);
      return true;
    }
    return false;
  };
  if (attempt()) return () => {};
  const timer = window.setInterval(() => {
    tries += 1;
    if (attempt() || tries > 40) window.clearInterval(timer);
  }, 250);
  return () => window.clearInterval(timer);
}

function useFollowOffer(entity: string, follow: FollowTarget | undefined, placement: Placement) {
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();
  const pathname = usePathname();
  const ref = useRef<HTMLDivElement | null>(null);
  const viewed = useRef(false);

  const pro = isPro(user);
  const token = follow ? `${follow.kind}:${follow.id}` : null;
  const isTicker = follow?.kind === "ticker";

  // HOOK FIRST, unconditionally, above every early return. The condition lives
  // inside. Placing it after `if (pro) return null` would change hook order
  // between renders as Clerk resolves — the same rules-of-hooks mistake made
  // in pro-gate the same day.
  //
  // Note this does not wait on isLoaded, because the render does not either.
  // See the block comment on the early return.
  useEffect(() => {
    if (pro) return;
    return captureWhenReady("follow_cta_shown", {
      entity,
      placement,
      signed_in: !!isSignedIn,
      has_follow_target: !!token,
      auth_resolved: !!isLoaded,
      path: pathname ?? null,
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSignedIn, pro]);

  // `follow_cta_shown` fires on MOUNT and always did — it never meant the
  // visitor saw anything, only that React rendered it. This is the honest
  // version, and it is a SECOND event rather than a redefinition of the
  // first, so the existing series keeps one meaning for its whole history.
  useEffect(() => {
    if (pro || !ref.current || typeof IntersectionObserver === "undefined") return;
    const el = ref.current;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (!e.isIntersecting || viewed.current) continue;
          viewed.current = true;
          captureWhenReady("follow_cta_viewed", {
            entity,
            placement,
            signed_in: !!isSignedIn,
            has_follow_target: !!token,
            path: pathname ?? null,
          });
          obs.disconnect();
        }
      },
      // Half the element, so a sliver clipping into view at the moment someone
      // closes the tab does not count as having been read.
      { threshold: 0.5 },
    );
    obs.observe(el);
    return () => obs.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pro]);

  // Where the button goes. Anonymous visitors carry the target and the way
  // back through sign-up; the sign-up page hands both to Clerk so the return
  // lands on this page with `?follow=` still set, and PendingFollow finishes
  // the job. Without `name` the sign-up page could not say who they'll hear
  // about.
  const signUpHref = token
    ? `/sign-up?follow=${encodeURIComponent(token)}&next=${encodeURIComponent(pathname ?? "/")}&name=${encodeURIComponent(entity)}`
    : "/sign-up";
  const href = isSignedIn
    ? (token ? `${pathname}?follow=${encodeURIComponent(token)}` : "/pricing")
    : signUpHref;

  const onClick = () =>
    captureWhenReady("follow_cta_clicked", {
      entity,
      placement,
      signed_in: !!isSignedIn,
      follow_target: token,
      destination: href,
      path: pathname ?? null,
    });

  return { pro, isSignedIn, token, isTicker, href, onClick, ref };
}

/**
 * One line, first viewport, no box. Sits under the page header so the ask is
 * on screen in the 27 seconds a search visitor gives the page. Renders nothing
 * for Pro, and nothing for a signed-in account with no target (that case has
 * nothing cheap to ask for and the band below handles it).
 */
export function FollowInline({ entity, follow }: { entity: string; follow: FollowTarget }) {
  const { pro, isSignedIn, token, isTicker, href, onClick, ref } = useFollowOffer(entity, follow, "top");
  if (pro || (isSignedIn && !token)) return null;

  const line = isTicker
    ? `Get an email when an insider at ${entity} files.`
    : `Get an email the next time ${entity} files.`;
  const action = isSignedIn ? `Follow ${entity}` : "Follow for free";

  return (
    <div ref={ref} className="mb-6 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
      <span className="text-[#8888A0]">{line}</span>
      <Link
        href={href}
        onClick={onClick}
        className="font-medium text-[#3B82F6] underline-offset-4 hover:underline"
      >
        {action}
      </Link>
    </div>
  );
}

/**
 * The band that closes a gated section.
 */
export function FollowCta({
  entity,
  detail,
  marksGate = true,
  follow,
}: {
  /** What is being followed, as it should read in a sentence: "NVDA", "Tim Cook". */
  entity: string;
  /**
   * What Pro adds, e.g. "Alerts when they file, saved screens, and the
   * strategy books". Shown only to a signed-in account with nothing to
   * follow here — the one reader for whom Pro is the next step.
   */
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
   * search reads "get an email the next time Erez Chimovits files", clicks,
   * creates an account, and lands in onboarding with the insider discarded:
   * they asked for one specific thing and received a form. Whatever the
   * sign-up rate, that account's retention value is near zero, because the
   * reason it was created was never acted on.
   *
   * Carried through sign-up as `?follow=insider:1234` and completed on return.
   */
  follow?: FollowTarget;
}) {
  const { pro, isSignedIn, token, isTicker, href, onClick, ref } = useFollowOffer(entity, follow, "band");

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
  // never made its cheapest ask. Anonymous is the correct pre-resolution shape
  // because it is what the overwhelming majority of these page views are, and
  // because it is the only variant that is never WRONG for a signed-out
  // reader. Once Clerk resolves, `isSignedIn` flips the copy and `pro`
  // unmounts the band entirely.
  if (pro) return null;

  let headline: string;
  let sub: string;
  let cta: string;
  if (!isSignedIn) {
    headline = isTicker
      ? `Get an email when an insider at ${entity} files`
      : `Get an email the next time ${entity} files`;
    sub = isTicker
      ? `Free account. Follow ${entity} and up to 10 more companies.`
      : "Free account. One email when the next filing comes in.";
    cta = `Follow ${entity} for free`;
  } else if (token) {
    // A signed-in free account is NOT asked for money here. They already have
    // following; selling them what they have is noise, and the relationship
    // is too thin to pitch Pro on an entity page. Pro is pitched later, by
    // email, to someone already receiving something useful.
    headline = isTicker
      ? `Get an email when an insider at ${entity} files`
      : `Get an email the next time ${entity} files`;
    sub = `Following ${entity} is included with your account.`;
    cta = `Follow ${entity}`;
  } else {
    headline = `Go deeper on ${entity}`;
    sub = detail ? `${detail}.` : "Alerts, saved screens and the strategy books.";
    cta = "See what Pro adds";
  }

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
        onClick={onClick}
        className="inline-flex shrink-0 items-center justify-center rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
      >
        {cta}
      </Link>
    </div>
  );
}
