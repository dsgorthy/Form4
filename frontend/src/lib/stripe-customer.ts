import { clerkClient, type User } from "@clerk/nextjs/server";
import type Stripe from "stripe";

/**
 * Find a user's Stripe customer — and repair the link if it is missing.
 *
 * WHY THIS EXISTS
 *
 * `publicMetadata.stripe_customer_id` was treated as the source of truth for
 * whether someone has a subscription. It is not a source of truth; it is a
 * CACHE, written in exactly one place — the `checkout.session.completed`
 * webhook — with no fallback and no repair path.
 *
 * On 2026-08-24 that webhook was found to have been returning 500 to every
 * event since a stripe-python upgrade. The consequence for a paying customer:
 * the cache was never written, so the billing portal told him
 * "No subscription found" while Stripe charged him monthly. He could not
 * cancel, could not see his subscription, and had to email support. The
 * failure was silent, permanent, and entirely invisible from our side.
 *
 * The rule this encodes: **Stripe is the source of truth for billing. Clerk
 * metadata is a cache, and every read path must be able to rebuild it.** A
 * missed webhook should degrade to one slower lookup, never to a user locked
 * out of their own subscription.
 */

/** Emails Clerk holds for this user, primary first. */
function emailsFor(user: User | null): string[] {
  if (!user) return [];
  const all = user.emailAddresses ?? [];
  const primary = all.find((e) => e.id === user.primaryEmailAddressId);
  const rest = all.filter((e) => e.id !== user.primaryEmailAddressId);
  return [primary, ...rest]
    .filter(Boolean)
    .map((e) => e!.emailAddress)
    .filter(Boolean);
}

/** Cache the id on Clerk so the slow path runs once, not forever. */
async function cacheOnClerk(userId: string, customerId: string): Promise<void> {
  try {
    const client = await clerkClient();
    await client.users.updateUser(userId, {
      publicMetadata: { stripe_customer_id: customerId },
    });
  } catch (err) {
    // Non-fatal on purpose. The caller already has what it needs; failing to
    // write the cache must never fail the user's request.
    console.error("[stripe-customer] could not cache id on Clerk:", err);
  }
}

export type CustomerLookup = {
  customerId: string | null;
  /** How we found it — useful for telling "no customer" apart from "cache miss". */
  via: "clerk" | "stripe-email" | "none";
};

export async function resolveStripeCustomer(
  stripe: Stripe,
  userId: string,
  user: User | null,
): Promise<CustomerLookup> {
  const cached = user?.publicMetadata?.stripe_customer_id as string | undefined;
  if (cached) {
    return { customerId: cached, via: "clerk" };
  }

  // Cache miss. Ask Stripe directly rather than concluding the user has no
  // subscription — that conclusion is what locked a paying customer out.
  for (const email of emailsFor(user)) {
    try {
      const found = await stripe.customers.list({ email, limit: 100 });
      if (found.data.length === 0) continue;
      // Prefer a customer that actually has a subscription; otherwise the
      // most recently created. Duplicates exist because checkout used to
      // create a fresh customer whenever the cache was empty.
      let best = found.data.sort((a, b) => b.created - a.created)[0];
      for (const c of found.data) {
        const subs = await stripe.subscriptions.list({
          customer: c.id,
          status: "all",
          limit: 1,
        });
        if (subs.data.length > 0) {
          best = c;
          break;
        }
      }
      await cacheOnClerk(userId, best.id);
      return { customerId: best.id, via: "stripe-email" };
    } catch (err) {
      console.error("[stripe-customer] Stripe lookup failed for", email, err);
    }
  }

  return { customerId: null, via: "none" };
}
