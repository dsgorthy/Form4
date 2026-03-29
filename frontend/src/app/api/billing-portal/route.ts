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
  const stripeCustomerId = user?.publicMetadata?.stripe_customer_id as string | undefined;

  if (!stripeCustomerId) {
    return NextResponse.json(
      { error: "No subscription found" },
      { status: 400 },
    );
  }

  try {
    const session = await stripe.billingPortal.sessions.create({
      customer: stripeCustomerId,
      return_url: `${process.env.NEXT_PUBLIC_BASE_URL || request.headers.get("origin") || request.nextUrl.origin}/settings`,
    });

    return NextResponse.json({ url: session.url });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Billing portal failed";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
