/**
 * Normalize and abbreviate insider titles for display.
 *
 * Raw SEC titles come in many forms: "Chief Executive Officer", "TenPercentOwner",
 * "Director;Director,TenPercentOwner", etc. This utility produces clean, abbreviated
 * display strings like "CEO", "10% Owner", "Dir, CEO".
 */

const ABBREVIATIONS: Record<string, string> = {
  CEO: "CEO",
  CFO: "CFO",
  COO: "COO",
  CTO: "CTO",
  CLO: "CLO",
  CMO: "CMO",
  CIO: "CIO",
  CAO: "CAO",
  CSO: "CSO",
  CPO: "CPO",
  CRO: "CRO",
  CHRO: "CHRO",
  CCO: "CCO",
  Chairman: "Chairman",
  President: "Pres",
  VP: "VP",
  Secretary: "Secretary",
  Treasurer: "Treasurer",
  Director: "Dir",
  Founder: "Founder",
  Controller: "Controller",
  "10% Owner": "10% Owner",
  Other: "",
};

// Fallback patterns for raw titles that bypass the normalization pipeline
const RAW_PATTERNS: [RegExp, string][] = [
  [/\bCHIEF EXECUTIVE OFFICER\b/i, "CEO"],
  [/\bCHIEF FINANCIAL OFFICER\b/i, "CFO"],
  [/\bCHIEF OPERATING OFFICER\b/i, "COO"],
  [/\bCHIEF TECHNOLOGY OFFICER\b/i, "CTO"],
  [/\bCHIEF LEGAL OFFICER\b/i, "CLO"],
  [/\bCHIEF MARKETING OFFICER\b/i, "CMO"],
  [/TenPercentOwner|TENPERCENTOWNER/i, "10% Owner"],
  [/\bTEN\s*PERCENT\s*OWNER\b/i, "10% Owner"],
  [/\b10\s*%\s*OWNER\b/i, "10% Owner"],
  [/\bPRESIDENT\b/i, "Pres"],
  [/\bDIRECTOR\b/i, "Dir"],
  [/\bVICE PRESIDENT\b/i, "VP"],
  [/\bCHAIRMAN\b/i, "Chairman"],
  [/\bCHAIRPERSON\b/i, "Chairman"],
  [/\bFOUNDER\b/i, "Founder"],
  [/\bCONTROLLER\b/i, "Controller"],
  [/\bSECRETARY\b/i, "Secretary"],
  [/\bTREASURER\b/i, "Treasurer"],
];

/**
 * Seniority order for multi-role titles.
 *
 * Titles are stored as semicolon-joined canonical tags in no meaningful order —
 * in practice roughly alphabetical, which is why "10% Owner;CEO;Chairman" leads
 * with the least informative role of the three. Rendering them in stored order
 * buries the one fact a reader actually wants.
 *
 * Ordered by how much the role says about the trade: an officer with operational
 * visibility outranks a board seat, which outranks a pure ownership stake.
 * Anything unlisted sorts last but keeps its relative order.
 */
const TITLE_RANK = [
  "CEO", "Chairman", "Pres", "COO", "CFO",
  "CTO", "CLO", "CMO", "CIO", "CAO", "CSO", "CPO", "CRO", "CHRO", "CCO",
  "Founder", "VP", "Dir", "Secretary", "Treasurer", "Controller", "10% Owner",
];

function rankOf(tag: string): number {
  const i = TITLE_RANK.indexOf(tag);
  return i === -1 ? TITLE_RANK.length : i;
}

/**
 * Title as an ordered list of role tags, most senior first.
 * Use for chip/badge rendering, where each role is its own visual element.
 */
export function titleTags(title: string | null | undefined): string[] {
  const formatted = formatTitle(title);
  if (!formatted) return [];
  return formatted
    .split(", ")
    .map((t) => t.trim())
    .filter(Boolean)
    .sort((a, b) => rankOf(a) - rankOf(b));
}

/**
 * Title for running prose — a sentence, a page subtitle, a search snippet.
 *
 * "CFO;President;VP" is unreadable, and even a cleaned "CFO, Pres, VP" is three
 * facts where the reader wants one. A person is introduced by their most senior
 * role; the full list belongs in the table, where there is room for it. Two are
 * joined with "&" because pairs like "CEO & Chairman" are a real, single
 * identity rather than a list. Beyond two, the tail is counted, not spelled out.
 */
export function titleSummary(
  title: string | null | undefined,
  max: number = 2,
): string {
  const tags = titleTags(title);
  if (tags.length === 0) return "";
  if (tags.length <= max) return tags.join(" & ");
  return `${tags.slice(0, max).join(" & ")} +${tags.length - max}`;
}

/**
 * Format a title for display. Handles both normalized (semicolon-separated canonical tags)
 * and raw SEC titles.
 */
export function formatTitle(title: string | null | undefined): string {
  if (!title || title === "Other" || title === "See Remarks" || title === "Unknown") {
    return "";
  }

  // Check if this is a structured title (semicolons, commas, or a known canonical tag)
  if (title.includes(";") || title.includes(",") || Object.keys(ABBREVIATIONS).includes(title)) {
    const tags = title
      .split(/[;,]/)
      .map((t) => t.trim())
      .filter(Boolean);

    // Deduplicate and abbreviate — try ABBREVIATIONS first, then RAW_PATTERNS for unrecognized tags
    const seen = new Set<string>();
    const abbreviated: string[] = [];
    for (const tag of tags) {
      let abbr = ABBREVIATIONS[tag];
      if (abbr === undefined) {
        // Try raw pattern matching on this individual tag
        for (const [pattern, mapped] of RAW_PATTERNS) {
          if (pattern.test(tag)) {
            abbr = mapped;
            break;
          }
        }
      }
      if (abbr === undefined) abbr = tag; // pass through unknown
      if (!abbr || seen.has(abbr)) continue;
      seen.add(abbr);
      abbreviated.push(abbr);
    }
    return abbreviated.join(", ");
  }

  // Raw title — try pattern matching
  const matched = new Set<string>();
  for (const [pattern, abbr] of RAW_PATTERNS) {
    if (pattern.test(title)) {
      matched.add(abbr);
    }
  }
  if (matched.size > 0) {
    return Array.from(matched).join(", ");
  }

  // Fallback: clean up the raw title
  return title
    .replace(/[;,]+/g, ", ")
    .replace(/\s+/g, " ")
    .trim();
}
