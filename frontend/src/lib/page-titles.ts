/**
 * Title and description text for the entity pages that carry organic traffic.
 *
 * WHY THIS IS A MODULE AND NOT A TEMPLATE LITERAL AT THE CALL SITE
 *
 * Measured on the Search Console export for the 28 days to 2026-09-20, with the
 * scraper queries removed: real demand was 5,483 impressions and 25 clicks, and
 * **85% of those impressions were person-name queries** (4,672 across 801
 * distinct queries). The ones with actual insider-trading intent are NAME PLUS
 * COMPANY, and they were already in the data at positions nobody clicks:
 *
 *     "gianluca romano seagate"        pos 38
 *     "ban seng teh seagate"           pos 33
 *     "duncan mckechnie vertex"        pos 27
 *     "joy liu vertex"                 pos 23
 *     "michel lagarde wellesley ma"    pos 19
 *     "tom hough parthenon"            pos 16
 *     "brian ferdinand liquid holdings" pos 36
 *
 * The insider title was `"{name} — Insider Profile"`. It contained neither the
 * company nor the words anyone searches. The COMPANY page title already got
 * this right — "GME Insider Trading — GameStop Corp." — which is part of why
 * company pages out-earn insider pages per URL.
 *
 * Titles are also where the other half of the problem lives: CTR at positions
 * 4-10 was 0.79%, several times below the norm for those positions, which is a
 * snippet problem rather than a ranking one.
 *
 * Pure functions, so the truncation rules are testable without a browser.
 */

/** Rough pixel budget Google renders before truncating, expressed in chars. */
const TITLE_BUDGET = 52; // the root layout appends " — Form4" (8 more)

/**
 * Corporate suffixes, stripped so the title matches how people type a company.
 * Nobody searches "xponential fitness, inc." — they search "xponential fitness".
 * Order matters: longest first, so "Corporation" is not left as "Corporat".
 */
const SUFFIXES = [
  ", Incorporated", " Incorporated", ", Corporation", " Corporation",
  ", Company", " Company", ", Holdings", ", Group", ", L.P.", " L.P.",
  ", LLC", " LLC", ", Ltd.", " Ltd.", ", Ltd", " Ltd", ", PLC", " PLC",
  ", Corp.", " Corp.", ", Corp", " Corp", ", Inc.", " Inc.", ", Inc", " Inc",
  ", S.A.", " N.V.", " plc",
];

/**
 * Values that pass the symbol shape but name no company. "NONE" is 5,344 rows.
 */
const SENTINEL_TICKERS = new Set(["NONE", "NA", "N", "NULL", "UNKNOWN", "OTC"]);

/**
 * A ticker fit to print, or null.
 *
 * `insider_companies.ticker` is whatever the filing carried, and a count on
 * 2026-09-27 found 5,344 rows reading literally "NONE", plus "[NONE]" (61),
 * "NA" (45), "*" (23), quoted forms like '"WM"', bracketed "[USG]", truncated
 * "NWIN(OB", and comma-joined share classes ("GEF,GEF.B", "LEN,LEN.B"). Any of
 * those reaching a <title> produces "Jane Roe — NONE Insider Trading", which is
 * worse than naming no employer at all: it is visibly broken in a search result.
 *
 * Takes the first symbol of a class list, and admits only a plain symbol with
 * an optional single class suffix.
 */
export function cleanTicker(ticker?: string | null): string | null {
  if (!ticker) return null;
  const first = ticker.split(",")[0].trim().toUpperCase();
  if (!/^[A-Z]{1,6}(\.[A-Z])?$/.test(first)) return null;
  if (SENTINEL_TICKERS.has(first)) return null;
  return first;
}

export function shortCompany(company?: string | null): string | null {
  if (!company) return null;
  let s = company.trim();
  for (const suf of SUFFIXES) {
    if (s.toLowerCase().endsWith(suf.toLowerCase())) {
      s = s.slice(0, -suf.length).trim().replace(/,$/, "");
      break;
    }
  }
  return s || null;
}

/**
 * `{name} — {Company} Insider Trading`, degrading in this order:
 *
 *   1. company name, if the whole thing fits the budget
 *   2. ticker, when the company name is too long to fit
 *   3. neither, for an insider we cannot attribute to a company
 *
 * The NAME always comes first and is never truncated: it is the exact string
 * the query matched, and a title that truncates mid-name matches nothing.
 */
export function insiderPageTitle(
  name: string,
  company?: string | null,
  ticker?: string | null,
): string {
  const co = shortCompany(company);
  const tk = cleanTicker(ticker);
  const tail = " Insider Trading";
  if (co) {
    const withName = `${name} — ${co}${tail}`;
    if (withName.length <= TITLE_BUDGET) return withName;
  }
  if (tk) {
    const withTicker = `${name} — ${tk}${tail}`;
    if (withTicker.length <= TITLE_BUDGET) return withTicker;
  }
  // No room for an employer, or none known. Keep the intent term: it is what
  // distinguishes this page from every other page about a person with this
  // name, and "Insider Profile" said nothing a searcher was looking for.
  return `${name} —${tail}`;
}

/**
 * The meta description. Leads with the company, because the snippet has to
 * answer "is this the right person" before it answers anything else.
 */
export function insiderPageDescription(
  name: string,
  opts: {
    company?: string | null;
    ticker?: string | null;
    jobTitle?: string | null;
    buys?: number | null;
    sells?: number | null;
    nTickers?: number | null;
    grade?: string | null;
  },
): string {
  const co = shortCompany(opts.company);
  const tk = cleanTicker(opts.ticker);
  // "(VRTX)" is recognised faster than a company name half-remembered, and it
  // is the token the query often carries instead of the full legal name.
  const at = co ? (tk ? `${co} (${tk})` : co) : null;
  const who = at
    ? `${name}${opts.jobTitle ? `, ${opts.jobTitle} at ${at}` : ` of ${at}`}`
    : name;
  const facts: string[] = [];
  const buys = opts.buys ?? 0;
  const sells = opts.sells ?? 0;
  if (buys || sells) {
    const bits = [];
    if (buys) bits.push(`${buys.toLocaleString()} open-market ${buys === 1 ? "purchase" : "purchases"}`);
    if (sells) bits.push(`${sells.toLocaleString()} ${sells === 1 ? "sale" : "sales"}`);
    facts.push(bits.join(" and "));
  }
  if (opts.nTickers && opts.nTickers > 1) facts.push(`${opts.nTickers} companies`);
  if (opts.grade) facts.push(`insider rating ${opts.grade}`);
  const body = facts.length
    ? `${who} — ${facts.join(", ")}.`
    : `${who} — SEC Form 4 filing history.`;
  return `${body} Every Form 4 filing, with the price move after each one.`;
}
