"use client";

/**
 * The page that shows when something throws.
 *
 * There was no error boundary anywhere in this app until 2026-08-22, which is
 * why a thrown error produced a blank screen rather than a message. Both
 * fetchers — fetchAPI and fetchAPIAuth — do `if (!res.ok) throw`, so any
 * non-2xx from the API in a server component took the whole page down, and the
 * user saw nav, footer, and nothing between them.
 *
 * That is how /congress shipped broken: it called the unauthenticated fetchAPI
 * against Pro-gated endpoints, got a 403 on every request from every user, and
 * rendered 161 characters. The page is fixed, but the class of failure needs a
 * floor under it — the next unguarded fetch should degrade into something a
 * person can read and act on, not a void.
 *
 * Deliberately plain: no logo, no marketing. Someone reading this is already
 * having a bad time and wants a way out of it.
 */
export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="flex min-h-[60vh] flex-col items-center justify-center px-4 text-center">
      <h1 className="text-xl font-semibold text-[#E8E8ED]">
        Something went wrong loading this page.
      </h1>
      <p className="mt-3 max-w-md text-sm leading-relaxed text-[#8888A0]">
        This is on us, not on you. Try again — if it keeps happening, the rest
        of the site should still work.
      </p>

      <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
        <button
          onClick={reset}
          className="rounded-md border border-[#3B82F6]/40 bg-[#3B82F6]/10 px-4 py-2 text-sm font-medium text-[#3B82F6] transition-colors hover:bg-[#3B82F6]/20"
        >
          Try again
        </button>
        <a
          href="/"
          className="rounded-md border border-[#2A2A3A] px-4 py-2 text-sm text-[#8888A0] transition-colors hover:text-[#E8E8ED]"
        >
          Back to the dashboard
        </a>
      </div>

      {/* The digest is what correlates this screen with the server log line.
          Without it a user can only report "it broke", and the frontend logs
          identify the failure by digest alone. */}
      {error.digest && (
        <p className="mt-8 font-mono text-[11px] text-[#55556A]">
          Reference: {error.digest}
        </p>
      )}
    </div>
  );
}
