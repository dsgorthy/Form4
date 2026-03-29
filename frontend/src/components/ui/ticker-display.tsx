import Link from "next/link";

export function companyToSlug(name: string): string {
  // btoa works in both browser and Node 18+
  const b64 = btoa(name);
  return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

interface TickerDisplayProps {
  ticker: string;
  company?: string;
  /** Link destination — defaults to /company/{ticker}. Pass null to disable linking. */
  href?: string | null;
  className?: string;
}

/**
 * Renders a ticker symbol. For private/unlisted companies (ticker = "NONE"),
 * shows the company name with a "Private" badge instead, linked to the shadow profile.
 */
export function TickerDisplay({ ticker, company, href, className = "" }: TickerDisplayProps) {
  const isPrivate = ticker === "NONE";

  if (isPrivate) {
    const slug = company ? companyToSlug(company) : null;
    const content = (
      <span className={`flex items-center gap-1.5 min-w-0 ${className}`}>
        <span className="rounded px-1 py-0.5 text-[10px] font-medium border border-[#55556A]/30 bg-[#55556A]/10 text-[#8888A0] shrink-0">
          Private
        </span>
        <span className="text-[#8888A0] text-xs truncate hover:text-blue-300 transition-colors">{company || "Unlisted"}</span>
      </span>
    );

    if (slug) {
      return (
        <Link href={`/company/private/${slug}`}>
          {content}
        </Link>
      );
    }

    return content;
  }

  const resolvedHref = href === null ? null : (href ?? `/company/${ticker}`);

  if (resolvedHref) {
    return (
      <Link href={resolvedHref} className={`font-mono font-semibold text-[#E8E8ED] hover:text-blue-400 transition-colors ${className}`}>
        {ticker}
      </Link>
    );
  }

  return <span className={`font-mono font-semibold text-[#E8E8ED] ${className}`}>{ticker}</span>;
}
