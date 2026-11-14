import { auth, currentUser } from "@clerk/nextjs/server";
import { NextRequest, NextResponse } from "next/server";
import Stripe from "stripe";

import { resolveStripeCustomer } from "@/lib/stripe-customer";

/** Days of Pro before the card is charged. Stated on /pricing; keep the two in step. */
const PRO_TRIAL_DAYS = 7;

/** A customer with a subscription in one of these states already has one. */
const ALREADY_SUBSCRIBED = new Set<Stripe.Subscription.Status>([
  "active", "trialing", "past_due", "unpaid", "paused",
]);

function getStripe() {
  return new Stripe(process.env.STRIPE_SECRET_KEY!, {
    apiVersion: "2026-02-25.clover",
  });
}

export async function POST(request: NextRequest) {
  if (!process.env.STRIPE_SECRET_KEY) {
    return NextResponse.json(
      { error: "Stripe is not configured yet. Please add STRIPE_SECRET_KEY to .env.local." },
      { status: 503 },
    );
  }

  const stripe = getStripe();
  const { userId } = await auth();
  if (!userId) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  const body = await request.json();
  const priceId = body.priceId as string;

  if (!priceId) {
    return NextResponse.json({ error: "Missing priceId" }, { status: 400 });
  }

  try {
    // Reuse the existing Stripe customer if one exists.
    //
    // This used to read the Clerk cache only, so whenever that cache was empty
    // — including every time the webhook failed — checkout created a SECOND
    // Stripe customer for the same person. That is how one account ends up
    // with duplicate customers and a subscription the portal cannot find.
    const user = await currentUser();
    const { customerId: existingCustomerId } = await resolveStripeCustomer(
      stripe, userId, user,
    );

    const base = process.env.NEXT_PUBLIC_BASE_URL || request.headers.get("origin") || request.nextUrl.origin;
    const subscriptions = existingCustomerId
      ? (await stripe.subscriptions.list({ customer: existingCustomerId, status: "all", limit: 100 })).data
      : [];

    // ONE SUBSCRIPTION PER CUSTOMER. This route used to open a new one for
    // anyone who asked. On 2026-11-11 the webhook failed, the site kept
    // showing the first paying customer as free, and they checked out again:
    // a trial plus a full-price subscription, both live, and still no Pro.
    // Someone who already has a subscription goes to it, never to a second one.
    const live = subscriptions.find((s) => ALREADY_SUBSCRIBED.has(s.status));
    if (live && existingCustomerId) {
      console.warn(
        "[checkout]", userId, "already has", live.id, `(${live.status})`,
        "— sending them to the billing portal, not a second checkout",
      );
      const portal = await stripe.billingPortal.sessions.create({
        customer: existingCustomerId,
        return_url: `${base}/settings`,
      });
      return NextResponse.json({ url: portal.url });
    }

    const sessionParams: Stripe.Checkout.SessionCreateParams = {
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      client_reference_id: userId,
      success_url: `${base}/settings?success=true`,
      cancel_url: `${base}/pricing?canceled=true`,
    };

    // THE PRO TRIAL LIVES HERE, AND ONLY HERE. Until 2026-09-17 every new
    // account was a Pro trial by age, and checkout carried whatever days were
    // left into Stripe. Accounts are free now; the trial is what you start
    // when you choose Pro: a card on file, nothing charged for PRO_TRIAL_DAYS,
    // cancel before then and pay nothing. Stripe runs the clock and the
    // reminders; the webhook maps `trialing` to pro like any active plan.
    //
    // One trial per customer: a subscriber who cancelled and comes back has
    // had theirs. Stripe would happily grant another.
    const hadASubscription = subscriptions.length > 0;
    if (!hadASubscription) {
      sessionParams.subscription_data = { trial_period_days: PRO_TRIAL_DAYS };
    }

    if (existingCustomerId) {
      sessionParams.customer = existingCustomerId;
    } else {
      // Pin the customer to the Clerk email so it stays findable by email if
      // the webhook is ever missed again. Without this Stripe records whatever
      // address is typed at checkout, and the recovery path above cannot match
      // it back to the account.
      const email = user?.emailAddresses?.find(
        (e) => e.id === user?.primaryEmailAddressId,
      )?.emailAddress;
      if (email) sessionParams.customer_email = email;
    }

    const session = await stripe.checkout.sessions.create(sessionParams);

    if (!session.url) {
      console.error("[checkout] Stripe session created but no URL:", session.id, session.status);
      return NextResponse.json(
        { error: "Stripe session created but returned no redirect URL. Please try again." },
        { status: 500 },
      );
    }

    return NextResponse.json({ url: session.url });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Checkout failed";
    console.error("[checkout] Stripe error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
