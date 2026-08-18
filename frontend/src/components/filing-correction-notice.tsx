import { formatCurrency } from "@/lib/format";

/**
 * Shown when we display a price that differs from what the filer submitted.
 *
 * Repairing a badly filled-out Form 4 is part of the service — ConnectM put the
 * trade's total in the price-per-share field, so a $65,122 purchase reached us
 * as price 65,122 x 73,680 shares = $4.8B. But a number that disagrees with
 * EDGAR has to say so on its own page. Anyone checking our figure against the
 * source needs to see that we changed it, what it was, and why, without having
 * to ask.
 *
 * Deliberately not a warning. Nothing is wrong with the page — the correction
 * is the product working. It reads as a footnote, not an alert.
 */

const EXPLANATION: Record<string, string> = {
  price_is_total_value:
    "The filer entered the trade's total dollar value in the price-per-share field. We divided it by the share count.",
  power_of_10_shift:
    "The filed price was off by a factor of ten. We corrected the decimal place against the stock's trading range that day.",
  price_equals_qty:
    "The filer entered the share count in the price field. We substituted the market price on the trade date.",
};

export function FilingCorrectionNotice({
  priceAsFiled,
  valueAsFiled,
  method,
  accessionUrl,
  valueSuspect,
}: {
  priceAsFiled?: number | null;
  valueAsFiled?: number | null;
  method?: string | null;
  accessionUrl?: string | null;
  /** True when we could not reconcile the figures and did not repair them. */
  valueSuspect?: boolean | null;
}) {
  // Two things this page might have to admit, and they are not the same.
  // A correction says we fixed it. This says we could not — the figures come
  // straight off the filing and one of them does not survive a sanity check,
  // so printing them as fact would be the actual error. Derivative rows carry
  // notional value into the quadrillions and reach this page by direct URL.
  if (priceAsFiled == null || !method) {
    if (!valueSuspect) return null;
    return (
      <div className="rounded-lg border border-[#8888A0]/25 bg-[#8888A0]/[0.06] p-4">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#8888A0] mb-2">
          Unverified figures
        </div>
        <p className="text-sm text-[#BCBCCB] leading-relaxed">
          The price or value on this filing does not reconcile with what the
          stock was worth on the trade date, and we could not determine the
          right figure. Read the numbers above as reported, not as confirmed.
          This filing is excluded from every total on the site.
        </p>
        {accessionUrl && (
          <a
            href={accessionUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-3 inline-block text-xs text-blue-400 hover:text-blue-300"
          >
            Read the original on SEC EDGAR →
          </a>
        )}
      </div>
    );
  }
  const why = EXPLANATION[method];

  return (
    <div className="rounded-lg border border-[#F59E0B]/25 bg-[#F59E0B]/[0.06] p-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#F59E0B] mb-2">
        Corrected filing
      </div>
      <p className="text-sm text-[#BCBCCB] leading-relaxed">
        {why ?? "We corrected this filing's price against market data."}
      </p>
      <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5 text-sm">
        <dt className="text-[#8888A0]">As filed</dt>
        <dd className="text-right font-mono text-[#8888A0] line-through">
          ${priceAsFiled.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          {valueAsFiled != null && ` · ${formatCurrency(valueAsFiled)}`}
        </dd>
        <dt className="text-[#8888A0]">Shown</dt>
        <dd className="text-right font-mono text-[#E8E8ED]">as corrected above</dd>
      </dl>
      {accessionUrl && (
        <a
          href={accessionUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="mt-3 inline-block text-xs text-blue-400 hover:text-blue-300"
        >
          Check the original on SEC EDGAR →
        </a>
      )}
    </div>
  );
}
