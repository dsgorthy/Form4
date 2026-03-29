import Link from "next/link";

export function Footer() {
  return (
    <footer className="mt-12 border-t border-[#2A2A3A] bg-[#0A0A0F]">
      <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-4 px-4 py-6 sm:flex-row md:px-6">
        <p className="text-xs text-[#55556A]">&copy; 2026 Form4</p>
        <nav className="flex items-center gap-6">
          <Link
            href="/privacy"
            className="text-xs text-[#55556A] transition-colors hover:text-[#8888A0]"
          >
            Privacy Policy
          </Link>
          <Link
            href="/terms"
            className="text-xs text-[#55556A] transition-colors hover:text-[#8888A0]"
          >
            Terms of Service
          </Link>
          <Link
            href="/disclaimer"
            className="text-xs text-[#55556A] transition-colors hover:text-[#8888A0]"
          >
            Disclaimer
          </Link>
        </nav>
      </div>
    </footer>
  );
}
