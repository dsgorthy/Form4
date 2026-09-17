import { SignUp } from "@clerk/nextjs";
import { clerkAppearance } from "@/lib/clerk-appearance";

/**
 * Sign-up, with the reason for signing up written above the form.
 *
 * A visitor from an insider or company page arrives as
 * `/sign-up?follow=insider:1234&next=/insider/jane-doe&name=Jane%20Doe`.
 * Two things have to happen here that Clerk's bare form did not do:
 *
 * 1. Say what they get. The form is Clerk's — "Create your account" — and
 *    nothing on it said the account is free or that it will do the thing
 *    they clicked for. So it does, in one line.
 * 2. Take them back. Clerk redirects where it is told, and until 2026-09-16
 *    it was told nothing: `next=` was carried this far and honoured by no
 *    one, so every sign-up landed on the home page with the follow discarded
 *    and PendingFollow never ran. `forceRedirectUrl` is that instruction,
 *    with `?follow=` kept on it so the page they return to completes the
 *    follow. The same URL is handed to the "already have an account" path.
 *
 * `next` is only ever a path on this site; anything else falls back to home,
 * so a crafted link cannot bounce someone to another origin.
 */
function safePath(next: string | undefined): string {
  if (!next || !next.startsWith("/") || next.startsWith("//")) return "/";
  return next;
}

export default async function SignUpPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = await searchParams;
  const one = (v: string | string[] | undefined) => (Array.isArray(v) ? v[0] : v);
  const follow = one(sp.follow);
  const name = one(sp.name);
  const next = safePath(one(sp.next));

  const isTicker = follow?.startsWith("ticker:");
  const line = follow && name
    ? isTicker
      ? `Free account. You'll get an email when an insider at ${name} files.`
      : `Free account. You'll get an email the next time ${name} files.`
    : "Free account. Follow insiders and companies and get an email when they file.";

  const destination = follow
    ? `${next}${next.includes("?") ? "&" : "?"}follow=${encodeURIComponent(follow)}`
    : undefined;

  return (
    <div className="flex min-h-[60vh] flex-col items-center justify-center gap-4">
      <p className="max-w-sm text-center text-sm text-[#8888A0]">{line}</p>
      <SignUp
        appearance={clerkAppearance}
        forceRedirectUrl={destination}
        signInForceRedirectUrl={destination}
      />
    </div>
  );
}
