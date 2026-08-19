"use client";

/**
 * The one entity search. Companies and insiders, one input, one destination.
 *
 * Replaces search-bar.tsx (nav) and explore-search.tsx (explore body), which
 * were two ~250-line implementations of the same feature that had drifted:
 * the nav had Cmd+K, grade badges and scores but no keyboard navigation, no
 * result counts, and built its own insider URLs; the explore one had keyboard
 * nav, dynamic group sizing and counts but no Cmd+K and no grades. Worse, they
 * sent the same query to different places — the nav to the public /company and
 * /insider pages, the explore one into the tool — so where you landed depended
 * on which box you happened to type in rather than on what you asked for.
 *
 * Everything now routes into /explore. Signed-out visitors land there too and
 * get a teaser with a sign-in CTA, which converts better than handing them a
 * complete but thinner page. /explore carries a canonical pointing back at the
 * public page so the tool does not compete with the SEO surface.
 *
 * Two variants, one behaviour:
 *   nav   compact, lives in the top bar on every page, owns Cmd+K
 *   hero  large, ONLY on the /explore empty state
 *
 * The hero disappears once there is a result, so the input "moving" happens
 * once on empty->result rather than on every search. That was the other half
 * of the old confusion: explore rendered its search bar three separate times,
 * in a different position per page mode.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";

interface TickerResult {
  ticker: string;
  company: string | null;
  trade_count?: number;
  total_value?: number;
}

interface InsiderResult {
  insider_id: string;
  name: string;
  cik: string | null;
  slug?: string | null;
  score?: number | null;
  best_pit_grade?: string | null;
  best_career_grade?: string | null;
  primary_title: string | null;
  primary_ticker: string | null;
}

interface SearchResponse {
  tickers: TickerResult[];
  insiders: InsiderResult[];
  ticker_total?: number;
  insider_total?: number;
}

// Total rows across both groups. Allocated dynamically rather than a fixed
// 5+5: "smith" matches ~10 companies but ~780 insiders, so a rigid split
// wastes half the panel on the thin side.
const PANEL_ROWS = 8;
const MIN_PER_GROUP = 2;
const DEBOUNCE_MS = 180;

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

/** Company view inside the tool. */
export function tickerHref(ticker: string): string {
  return `/explore?ticker=${encodeURIComponent(ticker)}`;
}

/**
 * Insider view inside the tool.
 *
 * Prefers the stored slug. The old nav bar passed `cik || insider_id` into
 * insiderPath() with no slug, and cik is null for most rows, so it built
 * /insider/{name}-{sqid} — a third URL shape for an entity that already has a
 * canonical one.
 */
export function insiderHref(ins: Pick<InsiderResult, "slug" | "insider_id">): string {
  return `/explore?insider=${encodeURIComponent(ins.slug || ins.insider_id)}`;
}

/** Bold the matched substring so it is obvious WHY a row is in the list. */
function Highlight({ text, q }: { text: string; q: string }) {
  const i = text.toLowerCase().indexOf(q.toLowerCase());
  if (!q || i === -1) return <>{text}</>;
  return (
    <>
      {text.slice(0, i)}
      <mark className="bg-transparent font-semibold text-[#E8E8ED]">
        {text.slice(i, i + q.length)}
      </mark>
      {text.slice(i + q.length)}
    </>
  );
}

/**
 * Group heading with a count. Without "8 of 6,257" a truncated list reads as
 * "these are all the matches", which is the core clarity failure of a
 * fixed-size multi-type dropdown. The count is a link to the full results.
 */
function GroupHeader({
  label,
  shown,
  total,
  onSeeAll,
}: {
  label: string;
  shown: number;
  total: number;
  onSeeAll: () => void;
}) {
  return (
    <div className="flex items-baseline justify-between px-3 py-1">
      <span className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
        {label}
      </span>
      {total > shown && (
        <button
          type="button"
          onMouseDown={(e) => {
            e.preventDefault();
            onSeeAll();
          }}
          className="text-[10px] text-[#55556A] transition-colors hover:text-[#8888A0]"
        >
          {shown} of {total.toLocaleString()} &rarr;
        </button>
      )}
    </div>
  );
}

export function EntitySearch({
  variant = "nav",
  initial = "",
}: {
  variant?: "nav" | "hero";
  initial?: string;
}) {
  const router = useRouter();
  const [q, setQ] = useState(initial);
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState(-1);
  const boxRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const isHero = variant === "hero";

  const run = useCallback(async (term: string) => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(term)}`);
      if (res.ok) {
        setResults(await res.json());
        setActive(-1);
        setOpen(true);
      }
    } catch {
      setResults(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (q.trim().length < 1) {
      setResults(null);
      return;
    }
    const t = setTimeout(() => run(q.trim()), DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [q, run]);

  // Close on outside click so the panel doesn't sit over page content.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  // Cmd/Ctrl+K focuses the NAV instance only — two inputs fighting over the
  // same shortcut would be a coin flip as to which one wins.
  useEffect(() => {
    if (isHero) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        inputRef.current?.focus();
      }
      if (e.key === "Escape") {
        setOpen(false);
        inputRef.current?.blur();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [isHero]);

  const go = (path: string) => {
    setOpen(false);
    if (!isHero) setQ("");
    router.push(path);
  };

  const term = q.trim();
  const tickerTotal = results?.ticker_total ?? results?.tickers.length ?? 0;
  const insiderTotal = results?.insider_total ?? results?.insiders.length ?? 0;
  const seeAll = () => go(`/explore?q=${encodeURIComponent(term)}`);

  // Allocate the panel by what actually matched: a small floor for each side
  // so neither vanishes, then hand the remainder to whichever type has more.
  const { tickerSlots, insiderSlots } = useMemo(() => {
    const tAvail = results?.tickers.length ?? 0;
    const iAvail = results?.insiders.length ?? 0;
    if (!tAvail) return { tickerSlots: 0, insiderSlots: Math.min(iAvail, PANEL_ROWS) };
    if (!iAvail) return { tickerSlots: Math.min(tAvail, PANEL_ROWS), insiderSlots: 0 };
    const tFloor = Math.min(tAvail, MIN_PER_GROUP);
    const iFloor = Math.min(iAvail, MIN_PER_GROUP);
    const rest = PANEL_ROWS - tFloor - iFloor;
    const share = tickerTotal / Math.max(1, tickerTotal + insiderTotal);
    const tExtra = Math.min(tAvail - tFloor, Math.round(rest * share));
    const iExtra = Math.min(iAvail - iFloor, rest - tExtra);
    return { tickerSlots: tFloor + tExtra, insiderSlots: iFloor + iExtra };
  }, [results, tickerTotal, insiderTotal]);

  const shownTickers = (results?.tickers ?? []).slice(0, tickerSlots);
  const shownInsiders = (results?.insiders ?? []).slice(0, insiderSlots);

  // An exact ticker hit ("NVDA") is unambiguous, so companies lead. Otherwise
  // a word-like query is far more often a person, and insiders outnumber
  // companies ~30x, so lead with whichever group has the mass.
  const exactTicker = results?.tickers.some(
    (t) => t.ticker.toLowerCase() === term.toLowerCase(),
  );
  const companiesFirst = exactTicker || tickerTotal >= insiderTotal;

  // Flattened for keyboard nav so ArrowDown crosses the group boundary the way
  // a user expects instead of trapping them in one section.
  const flat = useMemo(() => {
    const t = shownTickers.map((x) => ({ key: `t${x.ticker}`, href: tickerHref(x.ticker) }));
    const i = shownInsiders.map((x) => ({ key: `i${x.insider_id}`, href: insiderHref(x) }));
    return companiesFirst ? [...t, ...i] : [...i, ...t];
  }, [shownTickers, shownInsiders, companiesFirst]);

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, flat.length - 1));
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, -1));
      return;
    }
    if (e.key === "Escape") {
      setOpen(false);
      return;
    }
    if (e.key !== "Enter") return;
    if (active >= 0 && flat[active]) {
      go(flat[active].href);
      return;
    }
    // No selection. A single obvious hit goes straight through; anything
    // ambiguous goes to the full results page rather than guessing.
    if (exactTicker) {
      const hit = results!.tickers.find(
        (t) => t.ticker.toLowerCase() === term.toLowerCase(),
      )!;
      go(tickerHref(hit.ticker));
      return;
    }
    if (term) seeAll();
  };

  const hasResults =
    results && (results.tickers.length > 0 || results.insiders.length > 0);

  const inputClass = isHero
    ? "w-full rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-3 text-base text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6]/60 focus:outline-none"
    : "w-full md:w-56 rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-1.5 pl-8 text-sm text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6] focus:outline-none transition-colors";

  return (
    <div ref={boxRef} className={isHero ? "relative w-full max-w-2xl" : "relative"}>
      <div className="relative">
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => hasResults && setOpen(true)}
          onKeyDown={onKeyDown}
          placeholder={
            isHero
              ? "Search a company or insider — e.g. NVDA, Roger S. Penske"
              : "Search companies, insiders…"
          }
          aria-label="Search companies and insiders"
          className={inputClass}
        />
        {!isHero && (
          <svg
            className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-[#55556A]"
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-4.35-4.35M17 11a6 6 0 11-12 0 6 6 0 0112 0z" />
          </svg>
        )}
        {loading && (
          <div className={`absolute ${isHero ? "right-4 top-4" : "right-2 top-1/2 -translate-y-1/2"} h-3 w-3 animate-spin rounded-full border border-[#3B82F6] border-t-transparent`} />
        )}
      </div>

      {open && hasResults && (
        <div className={`absolute left-0 z-50 mt-1 overflow-hidden rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-2xl ${isHero ? "right-0" : "right-0 w-full md:w-96"}`}>
          {(companiesFirst ? ["t", "i"] : ["i", "t"]).map((kind, gi) =>
            kind === "t"
              ? shownTickers.length > 0 && (
                  <div key="t" className={gi > 0 ? "border-t border-[#2A2A3A] py-1" : "py-1"}>
                    <GroupHeader label="Companies" shown={shownTickers.length} total={tickerTotal} onSeeAll={seeAll} />
                    {shownTickers.map((t) => {
                      const idx = flat.findIndex((f) => f.key === `t${t.ticker}`);
                      return (
                        <button
                          key={t.ticker}
                          onMouseEnter={() => setActive(idx)}
                          onClick={() => go(tickerHref(t.ticker))}
                          className={`flex w-full items-center gap-3 px-3 py-2 text-left transition-colors ${idx === active ? "bg-[#1A1A26]" : "hover:bg-[#1A1A26]/60"}`}
                        >
                          <span className="w-14 shrink-0 font-mono text-sm font-bold text-[#E8E8ED]">
                            <Highlight text={t.ticker} q={term} />
                          </span>
                          <span className="flex-1 truncate text-xs text-[#8888A0]">
                            <Highlight text={t.company || ""} q={term} />
                          </span>
                          {t.trade_count != null && (
                            <span className="shrink-0 text-[10px] text-[#55556A]">
                              {t.trade_count} trades
                            </span>
                          )}
                        </button>
                      );
                    })}
                  </div>
                )
              : shownInsiders.length > 0 && (
                  <div key="i" className={gi > 0 ? "border-t border-[#2A2A3A] py-1" : "py-1"}>
                    <GroupHeader label="Insiders" shown={shownInsiders.length} total={insiderTotal} onSeeAll={seeAll} />
                    {shownInsiders.map((ins) => {
                      const idx = flat.findIndex((f) => f.key === `i${ins.insider_id}`);
                      return (
                        <button
                          key={ins.insider_id}
                          onMouseEnter={() => setActive(idx)}
                          onClick={() => go(insiderHref(ins))}
                          className={`flex w-full items-center gap-3 px-3 py-2 text-left transition-colors ${idx === active ? "bg-[#1A1A26]" : "hover:bg-[#1A1A26]/60"}`}
                        >
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2">
                              <span className="truncate text-sm text-[#E8E8ED]">
                                <Highlight text={ins.name} q={term} />
                              </span>
                              {(ins.best_career_grade || ins.best_pit_grade) && (
                                <InsiderGradeBadge grade={ins.best_career_grade} />
                              )}
                            </div>
                            {ins.primary_title && (
                              <div className="truncate text-[10px] text-[#55556A]">
                                {ins.primary_title}
                                {ins.primary_ticker && ` at ${ins.primary_ticker}`}
                              </div>
                            )}
                          </div>
                          {ins.score != null && (
                            <span className="shrink-0 font-mono text-xs text-[#8888A0]">
                              {ins.score.toFixed(2)}
                            </span>
                          )}
                        </button>
                      );
                    })}
                  </div>
                ),
          )}
          <button
            type="button"
            onClick={seeAll}
            className="flex w-full items-center justify-between border-t border-[#2A2A3A] px-3 py-1.5 text-left text-[10px] text-[#55556A] transition-colors hover:bg-[#1A1A26]/60 hover:text-[#8888A0]"
          >
            <span>&uarr;&darr; navigate &middot; &crarr; open</span>
            <span>All results for &ldquo;{term}&rdquo; &rarr;</span>
          </button>
        </div>
      )}

      {open && results && !hasResults && !loading && (
        <div className={`absolute left-0 right-0 top-full z-50 mt-1 rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-6 text-center text-sm text-[#8888A0] shadow-xl ${isHero ? "" : "md:w-96"}`}>
          No companies or insiders match &ldquo;{term}&rdquo;
        </div>
      )}
    </div>
  );
}
