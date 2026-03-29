import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex min-h-[60vh] flex-col items-center justify-center text-center">
      <h1 className="text-8xl font-bold text-[#E8E8ED]">404</h1>
      <h2 className="mt-4 text-2xl font-semibold text-[#E8E8ED]">
        Page not found
      </h2>
      <p className="mt-2 max-w-md text-[#8888A0]">
        The page you&apos;re looking for doesn&apos;t exist or has been moved.
      </p>
      <div className="mt-8 flex flex-wrap items-center justify-center gap-4">
        <Link
          href="/"
          className="rounded-md bg-[#3B82F6] px-5 py-2.5 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
        >
          Go to Dashboard
        </Link>
        <Link
          href="/screener"
          className="rounded-md border border-[#2A2A3A] px-5 py-2.5 text-sm font-medium text-[#E8E8ED] transition-colors hover:bg-[#1A1A26]"
        >
          Search Tickers
        </Link>
        <Link
          href="/leaderboard"
          className="rounded-md border border-[#2A2A3A] px-5 py-2.5 text-sm font-medium text-[#E8E8ED] transition-colors hover:bg-[#1A1A26]"
        >
          View Leaderboard
        </Link>
      </div>
    </div>
  );
}
