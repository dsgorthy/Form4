import { clerkMiddleware } from "@clerk/nextjs/server";
import { NextResponse, type NextRequest } from "next/server";

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

// The apex is the canonical host. Anything on www is sent there BEFORE it
// renders.
//
// This is not an SEO nicety, it is a hard breakage. NEXT_PUBLIC_API_URL is
// baked in at build time as https://form4.app/api/v1, so a page served from
// www.form4.app makes every client-side fetch CROSS-ORIGIN — and CORS_ORIGINS
// lists only the apex. The API answers 200 with no
// access-control-allow-origin header, the browser discards the response, and
// every component that loads its own data shows its failure state. On
// 2026-08-22 that was a signup landing on www.form4.app/portfolio and reading
// "Failed to load portfolio data" under a correctly rendered heading: the
// server-rendered shell arrives, none of the data does.
//
// Google sign-in through Clerk is what puts people on www, so this cannot be
// left to whoever remembers to type the bare domain.
//
// A redirect rather than widening CORS, because two live hostnames also means
// split Clerk cookies and duplicate crawlable content. CORS_ORIGINS does now
// include www as a second line of defence — if this redirect ever regresses,
// the site degrades to "works, wrong hostname" instead of "loads nothing".
function apexRedirect(req: NextRequest) {
  // The HEADER, not req.nextUrl.hostname. Behind Cloudflare Tunnel -> Caddy ->
  // Next, nextUrl carries the internal origin, so the first version of this
  // compiled correctly, deployed correctly, and never fired: www kept serving
  // 200 with no redirect. x-forwarded-host first, then host, and nextUrl only
  // as a local-dev fallback.
  const host = (
    req.headers.get("x-forwarded-host") ??
    req.headers.get("host") ??
    req.nextUrl.hostname
  )
    .split(":")[0]
    .toLowerCase();

  if (!host.startsWith("www.")) return null;

  const url = req.nextUrl.clone();
  url.host = host.slice(4);
  url.protocol = "https:";
  url.port = "";
  return NextResponse.redirect(url, 308);
}

export default clerkMiddleware(async (_auth, req) => {
  const toApex = apexRedirect(req);
  if (toApex) return toApex;

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
