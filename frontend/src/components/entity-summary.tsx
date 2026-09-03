import { formatCurrency } from "@/lib/format";

/**
 * The prose summary that sits directly under the H1.
 *
 * Every competitor outranking us leads with one, and Google lifts it verbatim
 * as the result snippet — the SERP for "NVDA insider trading" quotes phrases
 * like "30 Form 4 filings (383 transaction rows) disclosed by 4 distinct
 * insiders" and "net sold $249.2M over the trailing 6 months" straight off
 * their pages. We had every one of those numbers in the database and published
 * none of them as sentences.
 *
 * Deliberately prose, not a stat grid. A grid of numbers is not extractable as
 * a snippet; a sentence is. The bold spans are for humans skimming — the value
 * to search is that the figures sit in readable text near the top of the
 * document.
 *
 * Numbers only. No adjectives, no "explosive insider activity" — the figures
 * are the credibility, and inflated language on a finance page reads as spam
 * to both search engines and the audience.
 */

function Num({ children }: { children: React.ReactNode }) {
  return <strong className="font-semibold text-[#E8E8ED]">{children}</strong>;
}

export function CompanySummary({
  ticker,
  company,
  totalTrades,
  distinctInsiders,
  buyValue6mo,
  sellValue6mo,
  firstTrade,
  topInsiderName,
  topInsiderTitle,
}: {
  ticker: string;
  company: string | null;
  totalTrades: number;
  distinctInsiders: number;
  buyValue6mo: number;
  sellValue6mo: number;
  firstTrade?: string | null;
  topInsiderName?: string | null;
  topInsiderTitle?: string | null;
}) {
  const net = buyValue6mo - sellValue6mo;
  const netAbs = Math.abs(net);
  const name = company || ticker;
  const year = firstTrade ? firstTrade.slice(0, 4) : null;

  return (
    <p className="mb-6 max-w-[70ch] text-sm leading-relaxed text-[#8888A0]">
      <Num>{name}</Num> ({ticker}) has{" "}
      <Num>{totalTrades.toLocaleString()}</Num> SEC Form 4 insider transactions
      reported by <Num>{distinctInsiders}</Num> insider
      {distinctInsiders === 1 ? "" : "s"}
      {year ? <> since <Num>{year}</Num></> : null}.
      {netAbs > 0 && (
        <>
          {" "}Over the last six months insiders{" "}
          <Num>net {net >= 0 ? "bought" : "sold"} {formatCurrency(netAbs)}</Num>.
        </>
      )}
      {topInsiderName && (
        <>
          {" "}The most active filer is <Num>{topInsiderName}</Num>
          {topInsiderTitle ? `, ${topInsiderTitle}` : ""}.
        </>
      )}
    </p>
  );
}

export function InsiderSummary({
  name,
  title,
  companyName,
  ticker,
  nCompanies,
  totalTrades,
  lastTrade,
  firstTrade,
}: {
  name: string;
  title?: string | null;
  companyName?: string | null;
  ticker?: string | null;
  nCompanies: number;
  totalTrades: number;
  lastTrade?: string | null;
  firstTrade?: string | null;
}) {
  const year = firstTrade ? firstTrade.slice(0, 4) : null;
  return (
    <p className="mb-7 max-w-[68ch] text-[13.5px] leading-[1.6] text-[#8A8A9E]">
      <Num>{name}</Num>
      {title && companyName ? (
        <> is <Num>{title}</Num> at <Num>{companyName}</Num>{ticker ? ` (${ticker})` : ""}</>
      ) : companyName ? (
        <> files SEC Form 4 disclosures for <Num>{companyName}</Num></>
      ) : null}
      . {totalTrades > 0 && (
        <>
          They have reported <Num>{totalTrades.toLocaleString()}</Num> insider
          transaction{totalTrades === 1 ? "" : "s"} across{" "}
          <Num>{nCompanies}</Num> compan{nCompanies === 1 ? "y" : "ies"}
          {year ? <> since <Num>{year}</Num></> : null}
          {lastTrade ? <>, most recently <Num>{lastTrade}</Num></> : null}.
        </>
      )}
    </p>
  );
}

export function FilingSummary({
  insiderName,
  title,
  company,
  ticker,
  tradeType,
  qty,
  price,
  value,
  tradeDate,
  filingDate,
}: {
  insiderName: string;
  title?: string | null;
  company?: string | null;
  ticker: string;
  tradeType: string;
  qty?: number | null;
  price?: number | null;
  value?: number | null;
  tradeDate?: string | null;
  filingDate?: string | null;
}) {
  const verb = tradeType === "buy" ? "bought" : "sold";
  return (
    <p className="mb-6 max-w-[70ch] text-sm leading-relaxed text-[#8888A0]">
      <Num>{insiderName}</Num>
      {title ? <>, <Num>{title}</Num></> : null}
      {company ? <> at <Num>{company}</Num> ({ticker})</> : <> ({ticker})</>},{" "}
      <Num>{verb}</Num>
      {qty ? <> <Num>{qty.toLocaleString()}</Num> shares</> : <> shares</>}
      {price ? <> at <Num>${price.toFixed(2)}</Num></> : null}
      {value ? <> (<Num>{formatCurrency(value)}</Num>)</> : null}
      {tradeDate ? <> on <Num>{tradeDate}</Num></> : null}
      {filingDate ? <>, disclosed to the SEC on <Num>{filingDate}</Num></> : null}.
    </p>
  );
}
