"use client";

import Link from "next/link";

export default function MarketOverviewError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="mx-auto max-w-2xl py-16 px-4 text-center">
      <h1 className="text-2xl font-bold text-[#E8E8ED] mb-3">
        Market Overview is unavailable
      </h1>
      <p className="text-[#8888A0] mb-6">
        One or more dashboard data sources returned an error. This is an internal admin view —
        the public-facing site is unaffected.
      </p>
      {error?.digest && (
        <p className="text-xs font-mono text-[#55556A] mb-6">digest: {error.digest}</p>
      )}
      <div className="flex items-center justify-center gap-3">
        <button
          onClick={reset}
          className="rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-semibold text-white hover:bg-[#2563EB] transition-colors"
        >
          Try again
        </button>
        <Link
          href="/admin/strategies"
          className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-4 py-2 text-sm font-medium text-[#E8E8ED] hover:border-[#3B82F6]/50 transition-colors"
        >
          Back to admin
        </Link>
      </div>
    </div>
  );
}
