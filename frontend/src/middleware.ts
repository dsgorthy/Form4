import { clerkMiddleware } from "@clerk/nextjs/server";
import { NextResponse } from "next/server";

// Permissive middleware — no routes are blocked.
// Onboarding redirect is handled client-side via OnboardingGuard component
// because Clerk doesn't include unsafeMetadata in JWT claims by default.

const API_BASE =
  process.env.API_URL_INTERNAL ||
  process.env.NEXT_PUBLIC_API_URL ||
  "http://localhost:8000/api/v1";

// Retired insider slugs -> current slug.
//
// Correcting a name (see strategies/insider_catalog/name_cleaner.py) changes
// its slug, and insider_slug_aliases keeps every previously-published URL
// resolving. The API resolves an alias transparently, which is enough to avoid
// a 404 — but it leaves the same profile live at two addresses, so crawlers
// see duplicate content. This sends the retired one to the canonical with a
// real 308.
//
// This lives in middleware rather than in the page because a page-level
// permanentRedirect() runs after generateMetadata has already flushed the
// document head; once the response is streaming, the status code is settled
// and the redirect cannot take effect. Middleware runs before any render.
//
// The whole map is fetched once and cached in module scope: it is small and
// changes only when a name is corrected, so canonical URLs — nearly all of
// them — cost a single in-memory lookup and no network call. A failed fetch
// degrades to "no redirect", never to an error page.
const ALIAS_TTL_MS = 10 * 60 * 1000;
let aliases: Record<string, string> = {};
let aliasesFetchedAt = 0;

async function getAliases(): Promise<Record<string, string>> {
  if (Date.now() - aliasesFetchedAt < ALIAS_TTL_MS) return aliases;
  try {
    const res = await fetch(`${API_BASE}/insiders/slug-aliases`, { cache: "no-store" });
    if (res.ok) {
      aliases = (await res.json()) as Record<string, string>;
      aliasesFetchedAt = Date.now();
    }
  } catch {
    // API unreachable — serve the alias at its old URL rather than 500.
  }
  return aliases;
}

export default clerkMiddleware(async (_auth, req) => {
  const match = req.nextUrl.pathname.match(/^\/insider\/([^/]+)\/?$/);
  if (match) {
    const requested = decodeURIComponent(match[1]);
    const canonical = (await getAliases())[requested];
    if (canonical && canonical !== requested) {
      const url = req.nextUrl.clone();
      url.pathname = `/insider/${canonical}`;
      return NextResponse.redirect(url, 308);
    }
  }
});

export const config = {
  matcher: [
    // Skip Next.js internals and static files
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    // Always run for API routes
    "/(api|trpc)(.*)",
  ],
};
