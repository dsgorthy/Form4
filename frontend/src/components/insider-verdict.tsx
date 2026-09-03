/**
 * The insider page's opening: a written verdict, then signed meters.
 *
 * WHY IT EXISTS
 *
 * The page carried thirteen sections in one identical container
 * (`rounded-lg border p-4`) and four type sizes with no display size, so
 * nothing on it claimed to matter more than anything else. A visitor arriving
 * from search — 1.15 pages per visit, they look once and leave — had to
 * assemble the answer out of tiles.
 *
 * Two halves, deliberately:
 *
 *   THE SENTENCE   says what we think in the reader's own language. It is the
 *                  fastest way to tell a stranger what they are looking at,
 *                  and it is the half that works when someone lands cold.
 *   THE METERS     put the same three numbers on a signed scale against a zero
 *                  line, so the SIGN reads before the digits do. That is the
 *                  half that works for someone comparing six insiders.
 *
 * EVERY NUMBER IS ALREADY PUBLIC. This renders filing_stats, which is
 * allowlisted in api/public_fields.PUBLIC_FILING_STAT_FIELDS and floored at
 * MIN_SCORED_FILINGS upstream of gating. It computes nothing new and reveals
 * nothing gated.
 *
 * THE SENTENCE MUST NOT OVERCLAIM. It reports what happened after these
 * purchases; it never says the insider is good, bad, or worth following, and
 * it never predicts. Our grades do not predict forward returns — three
 * experiments this month failed to show it — so history is all this states.
 */

export interface VerdictStats {
  buy_win_rate_7d?: number | null;
  buy_win_rate_30d?: number | null;
  buy_win_rate_90d?: number | null;
  buy_avg_return_7d?: number | null;
  buy_avg_return_30d?: number | null;
  buy_avg_return_90d?: number | null;
  buy_avg_abnormal_7d?: number | null;
  buy_avg_abnormal_30d?: number | null;
  buy_avg_abnormal_90d?: number | null;
  buy_scored_filings_7d?: number | null;
  buy_scored_filings_30d?: number | null;
  buy_scored_filings_90d?: number | null;
}

interface Props {
  name: string;
  stats?: VerdictStats | null;
  buyCount: number;
  sellCount: number;
  /** Open-market purchase value only — never grants or exercises. */
  purchaseValue?: number | null;
  firstBuyYear?: string | null;
  grade?: string | null;
}

const pct = (v: number) => `${v >= 0 ? "+" : "−"}${Math.abs(v * 100).toFixed(1)}%`;

function money(v: number): string {
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${Math.round(v / 1e3)}K`;
  return `$${Math.round(v)}`;
}

/** The longest window that cleared the publishing floor. */
function deepest(s: VerdictStats) {
  for (const w of ["90d", "30d", "7d"] as const) {
    const move = s[`buy_avg_return_${w}` as keyof VerdictStats] as number | null | undefined;
    const alpha = s[`buy_avg_abnormal_${w}` as keyof VerdictStats] as number | null | undefined;
    const n = s[`buy_scored_filings_${w}` as keyof VerdictStats] as number | null | undefined;
    if (move != null && n) {
      return { window: w, label: w === "90d" ? "ninety days" : w === "30d" ? "thirty days" : "seven days", move, alpha: alpha ?? null, n };
    }
  }
  return null;
}

function Meter({ label, value, domain }: { label: string; value: number | null | undefined; domain: number }) {
  const has = value != null;
  const v = has ? Math.max(-domain, Math.min(domain, value as number)) : 0;
  const half = Math.abs(v) / domain / 2; // fraction of the FULL track
  const neg = v < 0;
  return (
    <div className="grid grid-cols-[2.2rem_minmax(0,17rem)_4rem] items-center gap-3">
      <span className="font-mono text-[11px] text-[#8A8A9E]">{label}</span>
      <span className="relative block h-[6px] rounded-full bg-[#23232E]">
        {/* The zero line, and it has to be VISIBLE — a signed bar with no
            baseline is just a bar. */}
        <span className="absolute -top-[4px] -bottom-[4px] left-1/2 w-px bg-[#4A4A5C]" aria-hidden="true" />
        {has && (
          <span
            className={`absolute top-0 bottom-0 rounded-full ${neg ? "bg-[#ED6A70]" : "bg-[#46CC8D]"}`}
            style={
              neg
                ? { right: "50%", width: `${half * 100}%` }
                : { left: "50%", width: `${half * 100}%` }
            }
          />
        )}
      </span>
      <span
        className={`text-right font-mono text-[13px] tabular-nums ${
          !has ? "text-[#63636F]" : neg ? "text-[#ED6A70]" : "text-[#46CC8D]"
        }`}
      >
        {has ? pct(value as number) : "—"}
      </span>
    </div>
  );
}

export function InsiderVerdict({
  name, stats, buyCount, sellCount, purchaseValue, firstBuyYear, grade,
}: Props) {
  const s = stats || {};
  const d = deepest(s);

  const alphas = [s.buy_avg_abnormal_7d, s.buy_avg_abnormal_30d, s.buy_avg_abnormal_90d]
    .filter((v): v is number => v != null);
  // A shared domain across the three meters, so their lengths are comparable
  // to each other. Floored at 20% so a quiet record does not render as three
  // maxed-out bars.
  const domain = Math.max(0.2, ...alphas.map((a) => Math.abs(a) * 1.15));

  if (!buyCount && !sellCount) return null;

  const firstClause = (
    <>
      {buyCount.toLocaleString()} open-market {buyCount === 1 ? "purchase" : "purchases"}
      {purchaseValue ? <> totalling {money(purchaseValue)}</> : null}
      {firstBuyYear ? <> since {firstBuyYear}</> : null}
      {sellCount === 0 ? ", never a sale" : `, against ${sellCount.toLocaleString()} ${sellCount === 1 ? "sale" : "sales"}`}.
    </>
  );

  return (
    <div className="mb-10">
      <p className="mb-8 max-w-[58ch] border-l-2 border-[#D9A441] pl-4 font-serif text-[18px] leading-[1.5] text-[#CFCFD8] sm:pl-5 sm:text-[21px]">
        {firstClause}{" "}
        {d ? (
          <>
            Across {d.n} scored {d.n === 1 ? "buy" : "buys"} the stock is{" "}
            <span className={d.move < 0 ? "text-[#ED6A70]" : "text-[#46CC8D]"}>
              {d.move < 0 ? "down" : "up"} {Math.abs(d.move * 100).toFixed(1)}% at {d.label}
            </span>
            {d.alpha != null && (
              <>
                , and{" "}
                <span className={d.alpha < 0 ? "text-[#ED6A70]" : "text-[#46CC8D]"}>
                  {Math.abs(d.alpha * 100).toFixed(1)} points{" "}
                  {d.alpha < 0 ? "behind" : "ahead of"} the S&amp;P
                </span>{" "}
                over the same stretch
              </>
            )}
            .
          </>
        ) : (
          <>Too few scored purchases to measure a record yet.</>
        )}
      </p>

      {alphas.length > 0 && (
        <div className="grid gap-x-8 gap-y-5 sm:grid-cols-[auto_1fr] sm:items-start">
          {grade && (
            <div className="flex items-baseline gap-3 sm:block">
              <div className="font-serif text-[44px] font-semibold leading-[0.92] text-[#D9A441] sm:text-[56px]">
                {grade}
              </div>
              <div className="font-mono text-[9.5px] uppercase tracking-[0.15em] text-[#63636F] sm:mt-2">
                Insider rating
              </div>
            </div>
          )}
          <div className={grade ? "sm:border-l sm:border-[#24242F] sm:pl-8" : ""}>
            <div className="mb-3 font-mono text-[10px] uppercase tracking-[0.14em] text-[#63636F]">
              Alpha vs SPY after a buy
            </div>
            <div className="grid gap-3">
              <Meter label="7d" value={s.buy_avg_abnormal_7d} domain={domain} />
              <Meter label="30d" value={s.buy_avg_abnormal_30d} domain={domain} />
              <Meter label="90d" value={s.buy_avg_abnormal_90d} domain={domain} />
            </div>
            <p className="mt-4 max-w-[38ch] font-mono text-[10.5px] leading-[1.5] text-[#63636F] sm:max-w-none">
              {s.buy_scored_filings_7d ?? 0} scored purchases · one row per
              filing · discretionary only
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
