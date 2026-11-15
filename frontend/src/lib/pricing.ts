/**
 * The prices Form4 shows. Mirrors api/pricing.py, which is the one definition;
 * tests/unit/test_prices_are_one_definition.py fails the build on drift, and
 * on any page that types a price itself instead of importing it from here.
 *
 * These must equal what checkout charges (the Stripe prices in .env). On
 * 2026-11-14 the site said $25/mo and $250/yr while checkout charged $19/$149.
 */
export const PRO_MONTHLY_USD = 19;
export const PRO_YEARLY_USD = 149;
export const PRO_PLUS_MONTHLY_USD = 75;
export const PRO_PLUS_YEARLY_USD = 750;

/** "$19", or "$12.42" when the amount is not a whole dollar. */
export function usd(amount: number): string {
  return Number.isInteger(amount) ? `$${amount}` : `$${amount.toFixed(2)}`;
}

/** A yearly price shown per month: $149/yr -> "$12.42". */
export function perMonth(yearlyUsd: number): string {
  return usd(Math.round((yearlyUsd / 12) * 100) / 100);
}
