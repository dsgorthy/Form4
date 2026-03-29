import { auth, currentUser } from "@clerk/nextjs/server";
import { NextRequest, NextResponse } from "next/server";
import Stripe from "stripe";

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
    // Reuse existing Stripe customer if one exists
    const user = await currentUser();
    const existingCustomerId = user?.publicMetadata?.stripe_customer_id as string | undefined;

    const sessionParams: Stripe.Checkout.SessionCreateParams = {
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      client_reference_id: userId,
      success_url: `${process.env.NEXT_PUBLIC_BASE_URL || request.headers.get("origin") || request.nextUrl.origin}/settings?success=true`,
      cancel_url: `${process.env.NEXT_PUBLIC_BASE_URL || request.headers.get("origin") || request.nextUrl.origin}/pricing?canceled=true`,
    };

    // If user is still in their free trial, carry remaining days into Stripe
    // so billing starts after the trial ends (computed server-side, not from client)
    const TRIAL_DAYS = 7;
    if (user?.createdAt) {
      const created = typeof user.createdAt === "number" ? user.createdAt : new Date(user.createdAt).getTime();
      const ageDays = (Date.now() - created) / 86_400_000;
      if (ageDays < TRIAL_DAYS) {
        const remaining = Math.max(1, Math.ceil(TRIAL_DAYS - ageDays));
        sessionParams.subscription_data = {
          trial_period_days: remaining,
        };
      }
    }

    if (existingCustomerId) {
      sessionParams.customer = existingCustomerId;
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
