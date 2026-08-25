import Link from "next/link";

// prefetch={false} on the legal links: Next.js prefetches a <Link> when it
// enters the viewport, so scrolling to the bottom of any page pulled the RSC
// payload for all three legal routes. They are almost never clicked, they sit
// on all ~52,000 pages, and the prefetch shows up in origin logs as
// /privacy?_rsc=... requests that look like traffic and are not.
export function Footer() {
  return (
    <footer className="mt-12 border-t border-[#2A2A3A] bg-[#0A0A0F]">
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
