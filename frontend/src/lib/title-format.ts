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
