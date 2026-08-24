import { auth, currentUser } from "@clerk/nextjs/server";
import { NextRequest, NextResponse } from "next/server";
import Stripe from "stripe";

import { resolveStripeCustomer } from "@/lib/stripe-customer";

function getStripe() {
  return new Stripe(process.env.STRIPE_SECRET_KEY!, {
    apiVersion: "2026-02-25.clover",
  });
}

export async function POST(request: NextRequest) {
  if (!process.env.STRIPE_SECRET_KEY) {
    return NextResponse.json(
      { error: "Stripe is not configured yet." },
      { status: 503 },
    );
  }

  const stripe = getStripe();
  const { userId } = await auth();
  if (!userId) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  const user = await currentUser();

  // NEVER conclude "no subscription" from the Clerk cache alone.
  //
  // This read `publicMetadata.stripe_customer_id` and returned
  // "No subscription found" when it was absent. That field is written by one
  // webhook, and when that webhook was broken a paying customer saw this exact
  // error while Stripe billed him monthly — unable to cancel, unable to see
  // anything. The resolver asks Stripe when the cache misses, and repairs the
  // cache so the slow path runs once.
  const { customerId, via } = await resolveStripeCustomer(stripe, userId, user);

  if (!customerId) {
    // Stripe genuinely has no customer for this account. Not an error — the
    // user has simply never paid — so say that rather than implying a fault.
    return NextResponse.json(
      {
        error: "no_customer",
        message:
          "We could not find any billing history for this account. If you " +
          "believe you have been charged, reply to support@form4.app and we " +
          "will sort it out.",
      },
      { status: 404 },
    );
  }
  if (via === "stripe-email") {
    console.warn(
      "[billing-portal] Clerk cache was missing for", userId,
      "— recovered from Stripe and repaired. A webhook was likely missed.",
    );
  }

  try {
    const session = await stripe.billingPortal.sessions.create({
      customer: customerId,
      return_url: `${process.env.NEXT_PUBLIC_BASE_URL || request.headers.get("origin") || request.nextUrl.origin}/settings`,
    });

    return NextResponse.json({ url: session.url });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Billing portal failed";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
