// Semantic color palette for grade displays — single source of truth.
// Same five tiers apply to Insider Grade (A+/A/B/C/D) and Trade Grade (5/4/3/2/1 stars).
// Anywhere a grade is shown, the color reflects strength on this scale.

export const GRADE_COLORS = {
  strong: "#22C55E",   // Green   — A+ insider, 5★ trade
  above: "#3B82F6",    // Blue    — A insider, 4★ trade
  average: "#8888A0",  // Slate   — B insider, 3★ trade
  below: "#F59E0B",    // Amber   — C insider, 2★ trade
  weak: "#EF4444",     // Red     — D insider, 1★ trade
  unknown: "#55556A",  // Dark    — no data / "New"
} as const;

export function insiderGradeColor(grade: string | null | undefined): string {
  if (!grade) return GRADE_COLORS.unknown;
  switch (grade.toUpperCase()) {
    case "A+":
      return GRADE_COLORS.strong;
    case "A":
      return GRADE_COLORS.above;
    case "B":
      return GRADE_COLORS.average;
    case "C":
      return GRADE_COLORS.below;
    case "D":
      return GRADE_COLORS.weak;
    default:
      return GRADE_COLORS.unknown;
  }
}

export function tradeGradeStarsColor(stars: number | null | undefined): string {
  if (stars == null) return GRADE_COLORS.unknown;
  switch (stars) {
    case 5:
      return GRADE_COLORS.strong;
    case 4:
      return GRADE_COLORS.above;
    case 3:
      return GRADE_COLORS.average;
    case 2:
      return GRADE_COLORS.below;
    case 1:
      return GRADE_COLORS.weak;
    default:
      return GRADE_COLORS.unknown;
  }
}
