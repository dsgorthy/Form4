/**
 * JSON-LD for the indexed page types.
 *
 * Two jobs, and the second is the one that matters:
 *
 * 1. Describe the entity so Google can render a richer result.
 * 2. Declare which parts of the page are gated. Google's guidance is explicit
 *    that this markup "helps Google differentiate paywalled content from the
 *    practice of cloaking, which violates spam policies" — serving a crawler
 *    content a visitor cannot see is otherwise indistinguishable from
 *    cloaking. Shipping a wall without this is the risky order of operations.
 *
 * Every gated block on every page carries the single class GATED_CLASS, so one
 * selector describes the whole page no matter how many blocks there are.
 *
 * The site emitted no JSON-LD at all before this, so all of it is additive.
 */

export const GATED_CLASS = "f4-gated";
const BASE = "https://form4.app";

/** The `hasPart` clause naming the gated region. Omit when nothing is gated. */
function gatedPart() {
  return {
    "@type": "WebPageElement",
    isAccessibleForFree: false,
    cssSelector: `.${GATED_CLASS}`,
  };
}

export interface CompanyLd {
  ticker: string;
  company: string | null;
  totalTrades: number;
  distinctInsiders: number;
  firstTrade?: string | null;
  lastTrade?: string | null;
}

/**
 * A company page is a dataset about an organisation, not an article — the
 * page's value is the collection of disclosures, and `Dataset` is what
 * describes that honestly.
 */
export function companyJsonLd(c: CompanyLd) {
  return {
    "@context": "https://schema.org",
    "@type": "Dataset",
    name: `${c.company || c.ticker} (${c.ticker}) insider trading — SEC Form 4`,
    description:
      `${c.totalTrades.toLocaleString()} SEC Form 4 insider transactions ` +
      `reported by ${c.distinctInsiders} insiders at ${c.company || c.ticker} (${c.ticker}).`,
    url: `${BASE}/company/${c.ticker}`,
    isAccessibleForFree: false,
    hasPart: gatedPart(),
    creator: { "@type": "Organization", name: "Form4", url: BASE },
    about: {
      "@type": "Corporation",
      name: c.company || c.ticker,
      tickerSymbol: c.ticker,
    },
    ...(c.firstTrade && c.lastTrade
      ? { temporalCoverage: `${c.firstTrade}/${c.lastTrade}` }
      : {}),
    isBasedOn: {
      "@type": "CreativeWork",
      name: "SEC EDGAR Form 4 filings",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany",
    },
  };
}

export interface InsiderLd {
  name: string;
  slug: string;
  title?: string | null;
  company?: string | null;
  ticker?: string | null;
  totalTrades?: number | null;
}

/**
 * ProfilePage with a Person as its main entity: this page is about a named
 * individual, which is a shape Google understands and treats differently from
 * a generic page.
 */
export function insiderJsonLd(i: InsiderLd) {
  const worksFor = i.company
    ? {
        worksFor: {
          "@type": "Corporation",
          name: i.company,
          ...(i.ticker ? { tickerSymbol: i.ticker } : {}),
        },
      }
    : {};
  return {
    "@context": "https://schema.org",
    "@type": "ProfilePage",
    url: `${BASE}/insider/${i.slug}`,
    isAccessibleForFree: false,
    hasPart: gatedPart(),
    mainEntity: {
      "@type": "Person",
      name: i.name,
      ...(i.title ? { jobTitle: i.title } : {}),
      ...worksFor,
      description:
        `SEC Form 4 insider trading history for ${i.name}` +
        (i.company ? ` at ${i.company}` : "") +
        (i.totalTrades ? `, ${i.totalTrades.toLocaleString()} reported transactions` : "") +
        ".",
    },
  };
}

export interface FilingLd {
  id: string;
  insiderName: string;
  company: string | null;
  ticker: string;
  tradeType: string;
  tradeDate?: string | null;
  filingDate?: string | null;
  value?: number | null;
}

/**
 * Filing pages are deliberately NOT gated — the disclosure is public record on
 * SEC.gov, the page is thin by nature, and a wall on a thin page is what
 * triggers a thin-content judgement. So no hasPart here and
 * isAccessibleForFree stays true.
 */
export function filingJsonLd(f: FilingLd) {
  const verb = f.tradeType === "buy" ? "bought" : "sold";
  return {
    "@context": "https://schema.org",
    "@type": "Article",
    headline: `${f.insiderName} ${verb} ${f.ticker} shares — SEC Form 4`,
    url: `${BASE}/filing/${f.id}`,
    isAccessibleForFree: true,
    ...(f.filingDate ? { datePublished: f.filingDate } : {}),
    author: { "@type": "Organization", name: "Form4", url: BASE },
    publisher: { "@type": "Organization", name: "Form4", url: BASE },
    about: {
      "@type": "Corporation",
      name: f.company || f.ticker,
      tickerSymbol: f.ticker,
    },
    mentions: { "@type": "Person", name: f.insiderName },
  };
}

/**
 * Renders a JSON-LD block. dangerouslySetInnerHTML is the documented way to
 * emit JSON-LD in React; the payload is built from our own typed objects and
 * serialised with JSON.stringify, so there is no untrusted string being
 * injected. `<` is escaped anyway to close off `</script>` injection through a
 * company or person name.
 */
export function jsonLdScript(data: object): { __html: string } {
  return { __html: JSON.stringify(data).replace(/</g, "\\u003c") };
}
