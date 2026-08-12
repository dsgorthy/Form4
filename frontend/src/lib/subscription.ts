export type Tier = "free" | "pro" | "pro_plus" | "trial" | "grace";

const TRIAL_DAYS = 7;
const GRACE_DAYS = 7;

/**
 * True if a comped tier's end date has passed. `pro_until` is only ever set
 * by hand when comping an account (Stripe never writes it), so its absence
 * means "no expiry" and paid subscribers are untouched. An unparseable value
 * leaves access in place — a typo shouldn't revoke access we promised.
 *
 * Mirrors `comp_lapsed` in api/auth.py — keep the two in sync.
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

export function getUserTier(user: { publicMetadata?: Record<string, unknown>; createdAt?: number | Date | null } | null | undefined): Tier {
  if (!user) return "free";
  const meta = user.publicMetadata || {};

  // Paid pro / pro+ — a comped tier falls through to trial/grace once it lapses.
  if (!compLapsed(meta)) {
    if ((meta.tier as string) === "pro_plus") return "pro_plus";
    if ((meta.tier as string) === "pro") return "pro";
  }

  // Check account age for trial / grace (Clerk provides createdAt as ms timestamp)
  if (user.createdAt) {
    const created = typeof user.createdAt === "number" ? user.createdAt : new Date(user.createdAt).getTime();
    const ageDays = (Date.now() - created) / 86_400_000;
    if (ageDays <= TRIAL_DAYS) return "trial";
    if (ageDays <= TRIAL_DAYS + GRACE_DAYS) return "grace";
  }

  return "free";
}

export function getTrialDaysLeft(user: { createdAt?: number | Date | null } | null | undefined): number {
  if (!user?.createdAt) return 0;
  const created = typeof user.createdAt === "number" ? user.createdAt : new Date(user.createdAt).getTime();
  const ageDays = (Date.now() - created) / 86_400_000;
  if (ageDays > TRIAL_DAYS) return 0;
  return Math.max(1, Math.ceil(TRIAL_DAYS - ageDays));
}

export function getGraceDaysLeft(user: { createdAt?: number | Date | null } | null | undefined): number {
  if (!user?.createdAt) return 0;
  const created = typeof user.createdAt === "number" ? user.createdAt : new Date(user.createdAt).getTime();
  const ageDays = (Date.now() - created) / 86_400_000;
  if (ageDays <= TRIAL_DAYS || ageDays > TRIAL_DAYS + GRACE_DAYS) return 0;
  return Math.max(1, Math.ceil(TRIAL_DAYS + GRACE_DAYS - ageDays));
}

export function hasApiAccess(user: { publicMetadata?: Record<string, unknown> } | null | undefined): boolean {
  if (!user) return false;
  const meta = user.publicMetadata || {};
  return meta.api_access === true;
}

export function isPro(user: { publicMetadata?: Record<string, unknown>; createdAt?: number | Date | null } | null | undefined): boolean {
  const tier = getUserTier(user);
  return tier === "pro" || tier === "pro_plus" || tier === "trial";
}

export function isProPlus(user: { publicMetadata?: Record<string, unknown> } | null | undefined): boolean {
  if (!user) return false;
  return getUserTier(user) === "pro_plus";
}

export function hasFullFeed(user: { publicMetadata?: Record<string, unknown>; createdAt?: number | Date | null } | null | undefined): boolean {
  const tier = getUserTier(user);
  return tier === "pro" || tier === "pro_plus" || tier === "trial" || tier === "grace";
}
