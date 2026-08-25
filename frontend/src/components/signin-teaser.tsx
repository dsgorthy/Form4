/**
 * Server-rendered sign-in wall for /explore.
 *
 * Deliberately NOT ProGate. That blurs client-side, which has two problems
 * here. It renders unblurred while Clerk is still loading, so a signed-out
 * visitor sees the full page and then watches it snap behind a blur — the
 * worst possible first impression. And the payload ships in the HTML either
 * way, so the blur is cosmetic; view-source defeats it.
 *
 * /explore is a server component and can read auth before fetching, so the
 * data a signed-out visitor is not entitled to is never fetched and never
 * sent. No flash, no leak, and a faster response for the visitor who is most
 * likely to bounce.
 *
 * The blurred bars below are a skeleton, not hidden content. The point of the
 * visual is "there is a lot more here", which a skeleton conveys as well as
 * real data does — and it does not require shipping the real data to make it.
 */
import Link from "next/link";

export function SignInTeaser({
  headline,
  detail,
}: {
  headline: string;
  detail: string;
}) {
  return (
    <div className="relative overflow-hidden rounded-lg border border-[#2A2A3A] bg-[#12121A]">
      {/* Skeleton — suggests volume without being real data. */}
      <div
        className="select-none space-y-3 p-5 blur-[3px]"
        aria-hidden="true"
      >
        {[...Array(7)].map((_, i) => (
          <div key={i} className="flex items-center gap-4">
            <div className="h-3 w-16 rounded bg-[#2A2A3A]" />
            <div className="h-3 flex-1 rounded bg-[#1F1F2B]" style={{ maxWidth: `${70 - i * 6}%` }} />
            <div className="h-3 w-14 rounded bg-[#2A2A3A]" />
            <div className="h-3 w-20 rounded bg-[#1F1F2B]" />
          </div>
        ))}
      </div>

      <div className="absolute inset-0 flex items-center justify-center bg-gradient-to-b from-[#0A0A0F]/70 via-[#0A0A0F]/85 to-[#0A0A0F]">
        <div className="max-w-md px-6 text-center">
          <h3 className="mb-2 text-lg font-bold text-[#E8E8ED]">{headline}</h3>
          <p className="mb-5 text-sm text-[#8888A0]">{detail}</p>
          <div className="flex items-center justify-center gap-3">
            <Link
              href="/sign-up"
              className="rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
            >
              Start Free Trial
            </Link>
            <Link
              href="/sign-in"
              className="rounded-lg border border-[#2A2A3A] px-4 py-2 text-sm font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED]"
            >
              Sign In
            </Link>
          </div>
          <p className="mt-3 text-[10px] text-[#81819A]">
            7 days of full access — no credit card required
          </p>
        </div>
      </div>
    </div>
  );
}
