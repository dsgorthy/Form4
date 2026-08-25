/**
 * What KIND of filing is this? Mirror of api/classification.py.
 *
 * DO NOT edit one side without the other — tests/unit/test_classification_parity.py
 * fails the build on drift, the same way ratings.ts is pinned.
 *
 * WHY THIS EXISTS
 *
 * "Routine" meant five different things across the product. Measured
 * 2026-08-21 over the 126,521 filings since January:
 *
 *   signal_class (canonical)                    68,394 routine
 *   api/narrative.py _is_routine                45,371
 *   feed-list.tsx + insider-trades-table.tsx    35,060
 *   the Stocktwits generator                    30,630
 *   trades-table.tsx (/explore)                 11,477
 *
 * 22,937 10b5-1 planned sells showed "SELL · Routine" in the feed and no label
 * at all on /explore. The cause was each surface assembling its own answer
 * from raw flags with wildly different coverage — is_routine is 16%
 * populated, cohen_routine 100%, is_10b5_1 96%.
 *
 * signal_class is trigger-maintained, 100% populated, and already encodes
 * every distinction those flags were being combined to recover. It is the only
 * source. Components read `filing_kind` off the payload and render it.
 *
 * KIND IS 1-TO-1, RECURRENCE IS A TAG. cohen_routine cuts across signal_class
 * (5,831 discretionary sells carry it), so folding it into the kind would make
 * one filing two kinds at once.
 */

/** Published vocabulary, most-signal first. */
export const FILING_KINDS = [
  "Discretionary",
  "Scheduled",
  "Compensation",
  "Tax",
  "Exercise",
] as const;

export type FilingKind = (typeof FILING_KINDS)[number];

/**
 * signal_class -> published kind. `null` means render nothing: gift,
 * derivative and inconsistent name internal plumbing rather than anything a
 * subscriber recognises, and they are 2% of the corpus between them.
 */
const KIND_OF: Record<string, FilingKind | null> = {
  discretionary_buy: "Discretionary",
  discretionary_sell: "Discretionary",
  planned_sell: "Scheduled",
  planned_buy: "Scheduled",
  compensation: "Compensation",
  tax_withholding: "Tax",
  option_exercise: "Exercise",
  gift: null,
  derivative: null,
  inconsistent: null,
};

/** The classes that represent an actual open-market decision. */
export const DISCRETIONARY_CLASSES = [
  "discretionary_buy",
  "discretionary_sell",
] as const;

export const KIND_META: Record<FilingKind, { blurb: string; signal: boolean }> = {
  Discretionary: {
    blurb: "An open-market decision to buy or sell.",
    signal: true,
  },
  Scheduled: {
    blurb: "Executed under a 10b5-1 plan set up in advance.",
    signal: false,
  },
  Compensation: { blurb: "Shares received as pay, not bought.", signal: false },
  Tax: {
    blurb: "Shares withheld to cover tax on a vesting award.",
    signal: false,
  },
  Exercise: { blurb: "Options converted into shares.", signal: false },
};

/** Published kind for a filing. `null` means render no label. */
export function filingKind(signalClass?: string | null): FilingKind | null {
  if (!signalClass) return null;
  return KIND_OF[signalClass.trim().toLowerCase()] ?? null;
}

/**
 * Did the insider make a decision, as opposed to receiving or scheduling?
 *
 * Replaces the hand-rolled `is_routine === 1 || is_10b5_1 === 1` checks. Note
 * the polarity flip: those asked "is this routine", each with different
 * coverage; this asks the positive question against the one column that is
 * always populated.
 */
export function isDiscretionary(signalClass?: string | null): boolean {
  const s = (signalClass ?? "").trim().toLowerCase();
  return (DISCRETIONARY_CLASSES as readonly string[]).includes(s);
}

/** Pre-arranged under a 10b5-1 plan. */
export function isScheduled(signalClass?: string | null): boolean {
  return filingKind(signalClass) === "Scheduled";
}

/**
 * A behavioural tag, NOT a kind. The insider does this on a rhythm; that is
 * worth saying beside the kind and must never replace it.
 */
export function isRecurringPattern(item: {
  cohen_routine?: number | null;
  is_recurring?: number | null;
}): boolean {
  return Boolean(item.cohen_routine || item.is_recurring);
}

/** Muted for everything that is not a decision; the accent is reserved. */
export function kindColor(kind: FilingKind | null): string {
  switch (kind) {
    case "Discretionary":
      return "#3B82F6";
    case "Scheduled":
    case "Compensation":
    case "Tax":
    case "Exercise":
      return "#81819A";
    default:
      return "#81819A";
  }
}
