/**
 * Who is on what tier, client side.
 *
 * An account is FREE unless Clerk `publicMetadata.tier` says "pro" or
 * "pro_plus" (written by the Stripe webhook, or by hand for a comp). That is
 * the whole rule.
 *
 * Until 2026-09-17 there was a second rule underneath it: with no metadata,
 * the tier was derived from the account's AGE — days 0–7 "trial" (full Pro),
 * days 8–14 "grace" (signals delayed 24h), then free. Nobody chose that
 * trial; every new account got a countdown in the nav, a banner, and four
 * "your Pro access expires" emails. It made "free account" a lie the moment
 * someone signed up. Derek, 2026-09-17: free users over anonymous users. A
 * Pro trial still exists — as a choice, started from checkout, run by Stripe
 * (see app/api/checkout/route.ts). Nothing here counts days any more.
 *
 * Mirrors api/auth.py — keep the two in sync. test_accounts_are_free_not_trial
 * fails the build if age-based tiering comes back on either side.
 */
export type Tier = "free" | "pro" | "pro_plus";

type UserLike = { publicMetadata?: Record<string, unknown> } | null | undefined;

/**
 * True if a comped tier's end date has passed. `pro_until` is only ever set
 * by hand when comping an account (Stripe never writes it), so its absence
 * means "no expiry" and paid subscribers are untouched. An unparseable value
 * leaves access in place — a typo shouldn't revoke access we promised.
 *
 * Mirrors `comp_lapsed` in api/comp.py — keep the two in sync.
 */
function compLapsed(meta: Record<string, unknown>): boolean {
  const raw = meta.pro_until;
  if (!raw) return false;

  const text = String(raw).trim();
  // Bare YYYY-MM-DD means access through the end of that day, UTC.
  const expires = /^\d{4}-\d{2}-\d{2}$/.test(text)
    ? Date.parse(`${text}T23:59:59Z`)
    : Date.parse(text);

  if (Number.isNaN(expires)) return false;
  return Date.now() > expires;
}

export function getUserTier(user: UserLike): Tier {
  if (!user) return "free";
  const meta = user.publicMetadata || {};
  if (compLapsed(meta)) return "free";
  if ((meta.tier as string) === "pro_plus") return "pro_plus";
  if ((meta.tier as string) === "pro") return "pro";
  return "free";
}

export function hasApiAccess(user: UserLike): boolean {
  if (!user) return false;
  const meta = user.publicMetadata || {};
  return meta.api_access === true;
}

export function isPro(user: UserLike): boolean {
  const tier = getUserTier(user);
  return tier === "pro" || tier === "pro_plus";
}

export function isProPlus(user: UserLike): boolean {
  if (!user) return false;
  return getUserTier(user) === "pro_plus";
}

/** The full feed — no 90-day cutoff, no gated items — is Pro's. */
export function hasFullFeed(user: UserLike): boolean {
  return isPro(user);
}
