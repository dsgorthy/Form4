export type Tier = "free" | "pro" | "trial" | "grace";

const TRIAL_DAYS = 7;
const GRACE_DAYS = 7;

export function getUserTier(user: { publicMetadata?: Record<string, unknown>; createdAt?: number | Date | null } | null | undefined): Tier {
  if (!user) return "free";
  const meta = user.publicMetadata || {};

  // Paid pro
  if ((meta.tier as string) === "pro") return "pro";

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
  return tier === "pro" || tier === "trial";
}

export function hasFullFeed(user: { publicMetadata?: Record<string, unknown>; createdAt?: number | Date | null } | null | undefined): boolean {
  const tier = getUserTier(user);
  return tier === "pro" || tier === "trial" || tier === "grace";
}
