import Link from "next/link";

// prefetch={false} on the legal links: Next.js prefetches a <Link> when it
// enters the viewport, so scrolling to the bottom of any page pulled the RSC
// payload for all three legal routes. They are almost never clicked, they sit
// on all ~52,000 pages, and the prefetch shows up in origin logs as
// /privacy?_rsc=... requests that look like traffic and are not.
export function Footer() {
  return (
    <footer className="mt-12 border-t border-[#2A2A3A] bg-[#0A0A0F]">
      {/* CONTENT LINKS, SERVER-RENDERED. The nav's "More" dropdown is a client
          component that mounts its list on open, so /explore, /clusters,
          /research and /insider-buying appear in NO page's HTML — Googlebot
          has never seen a link to any of them. Measured 2026-09-04: the hubs
          had zero crawls in 24h against 1,116 on leaf pages, and their only
          route in was a sitemap Google had not fetched in 48 hours.

          A footer is the ordinary place to fix that: it is on all ~52,000
          pages and it is real markup, not a menu. */}
      <div className="mx-auto max-w-7xl px-4 pt-6 md:px-6">
        <nav className="flex flex-wrap gap-x-6 gap-y-2 text-xs text-[#8A8A9E]">
          {[
            ["/insider-buying", "Insider Buying by Sector"],
            ["/leaderboard", "Top Insiders"],
            ["/clusters", "Cluster Buying"],
            ["/congress", "Congressional Trading"],
            ["/feed", "Latest Filings"],
            ["/explore", "Explore"],
            ["/research", "Research"],
          ].map(([href, label]) => (
            <Link
              key={href}
              href={href}
              prefetch={false}
              className="transition-colors hover:text-[#E8E8ED]"
            >
              {label}
            </Link>
          ))}
        </nav>
      </div>
      <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-4 px-4 py-6 sm:flex-row md:px-6">
        <p className="text-xs text-[#81819A]">&copy; 2026 Form4</p>
        <nav className="flex items-center gap-6">
          <Link
            href="/privacy"
            prefetch={false}
            className="text-xs text-[#81819A] transition-colors hover:text-[#8888A0]"
          >
            Privacy Policy
          </Link>
          <Link
            href="/terms"
            prefetch={false}
            className="text-xs text-[#81819A] transition-colors hover:text-[#8888A0]"
          >
            Terms of Service
          </Link>
          <Link
            href="/performance"
            prefetch={false}
            className="text-xs text-[#81819A] transition-colors hover:text-[#8888A0]"
          >
            Performance
          </Link>
          <Link
            href="/disclaimer"
            prefetch={false}
            className="text-xs text-[#81819A] transition-colors hover:text-[#8888A0]"
          >
            Disclaimer
          </Link>
          {/* The European Accessibility Act requires the statement to be
              publicly available; a footer link on every page is how that is
              normally satisfied. See app/accessibility/page.tsx. */}
          <Link
            href="/accessibility"
            prefetch={false}
            className="text-xs text-[#81819A] transition-colors hover:text-[#8888A0]"
          >
            Accessibility
          </Link>
        </nav>
      </div>
    </footer>
  );
}
