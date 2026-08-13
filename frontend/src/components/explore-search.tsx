"use client";

/**
 * Entity search for /explore.
 *
 * Deliberately one input, not a company/insider toggle: a visitor types
 * "penske" or "AAPL" without first deciding which kind of thing that is.
 * Making them pick a mode before typing is friction that buys nothing.
 *
 * Shares the /search endpoint with the nav SearchBar, which already returns
 * tickers and insiders grouped. The difference is only where results go —
 * the nav sends you to the public pages (/company, /insider), this stays
 * inside the tool (/explore?ticker=, /explore?insider=).
 */

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";

interface TickerResult {
  ticker: string;
  company: string | null;
  trade_count?: number;
}

interface InsiderResult {
  insider_id: string;
  name: string;
  cik: string | null;
  slug?: string | null;
  best_pit_grade?: string | null;
  primary_title: string | null;
  primary_ticker: string | null;
}

interface SearchResponse {
  tickers: TickerResult[];
  insiders: InsiderResult[];
  ticker_total?: number;
  insider_total?: number;
}

// Total rows shown across both groups. Slots are allocated dynamically rather
// than a fixed 5+5: "smith" matches 10 companies but 783 insiders, so a rigid
// split wastes half the panel on the thin side.
const PANEL_ROWS = 8;
const MIN_PER_GROUP = 2;

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
 * fixed-size multi-type dropdown.
 */
function GroupHeader({ label, shown, total }: { label: string; shown: number; total: number }) {
  const truncated = total > shown;
  return (
    <div className="flex items-baseline justify-between px-3 py-1">
      <span className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
        {label}
      </span>
      {truncated && (
        <span className="text-[10px] text-[#55556A]">
          {shown} of {total.toLocaleString()}
        </span>
      )}
    </div>
  );
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export function ExploreSearch({ initial = "" }: { initial?: string }) {
  const router = useRouter();
  const [q, setQ] = useState(initial);
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState(-1);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (q.trim().length < 1) {
      setResults(null);
      return;
    }
    const t = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q.trim())}`);
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
    }, 180);
    return () => clearTimeout(t);
  }, [q]);

  // Close on outside click so the panel doesn't sit over the page content.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const go = (path: string) => {
    setOpen(false);
    router.push(path);
  };

  const term = q.trim();
  const tickerTotal = results?.ticker_total ?? results?.tickers.length ?? 0;
  const insiderTotal = results?.insider_total ?? results?.insiders.length ?? 0;

  // Allocate the panel by what actually matched. Give each side a small floor
  // so neither disappears, then hand the remaining rows to whichever type has
  // more to show.
  const { tickerSlots, insiderSlots } = (() => {
    const tAvail = results?.tickers.length ?? 0;
    const iAvail = results?.insiders.length ?? 0;
    if (!tAvail) return { tickerSlots: 0, insiderSlots: Math.min(iAvail, PANEL_ROWS) };
    if (!iAvail) return { tickerSlots: Math.min(tAvail, PANEL_ROWS), insiderSlots: 0 };
    const tFloor = Math.min(tAvail, MIN_PER_GROUP);
    const iFloor = Math.min(iAvail, MIN_PER_GROUP);
    let rest = PANEL_ROWS - tFloor - iFloor;
    const tExtra = Math.min(tAvail - tFloor, Math.round(rest * (tickerTotal / Math.max(1, tickerTotal + insiderTotal))));
    const iExtra = Math.min(iAvail - iFloor, rest - tExtra);
    return { tickerSlots: tFloor + tExtra, insiderSlots: iFloor + iExtra };
  })();

  // An exact ticker hit ("NVDA") is unambiguous, so companies lead. Otherwise
  // a word-like query is far more often a person, and insiders outnumber
  // companies by ~30x, so lead with whichever group actually has the mass.
  const exactTicker = results?.tickers.some((t) => t.ticker.toLowerCase() === term.toLowerCase());
  const companiesFirst = exactTicker || tickerTotal >= insiderTotal;

  const shownTickers = (results?.tickers ?? []).slice(0, tickerSlots);
  const shownInsiders = (results?.insiders ?? []).slice(0, insiderSlots);

  // Flatten for keyboard nav so ArrowDown crosses the group boundary the way
  // a user expects, rather than trapping them in one section.
  const flat: { kind: "t" | "i"; href: string; key: string }[] = [
    ...(companiesFirst ? shownTickers : []).map((t) => ({
      kind: "t" as const, href: `/explore?ticker=${encodeURIComponent(t.ticker)}`, key: `t${t.ticker}`,
    })),
    ...shownInsiders.map((ins) => ({
      kind: "i" as const,
      href: `/explore?insider=${encodeURIComponent(ins.slug || ins.insider_id)}`,
      key: `i${ins.insider_id}`,
    })),
    ...(companiesFirst ? [] : shownTickers).map((t) => ({
      kind: "t" as const, href: `/explore?ticker=${encodeURIComponent(t.ticker)}`, key: `t${t.ticker}`,
    })),
  ];

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
    if (e.key === "Escape") { setOpen(false); return; }
    if (e.key !== "Enter") return;
    if (active >= 0 && flat[active]) { go(flat[active].href); return; }
    // No selection: an exact/first ticker beats a first insider, and a bare
    // token falls through as a ticker guess ("NVDA" + Enter).
    if (shownTickers[0]) go(`/explore?ticker=${encodeURIComponent(shownTickers[0].ticker)}`);
    else if (shownInsiders[0]) go(`/explore?insider=${encodeURIComponent(shownInsiders[0].slug || shownInsiders[0].insider_id)}`);
    else if (term) go(`/explore?ticker=${encodeURIComponent(term.toUpperCase())}`);
  };

  const hasResults =
    results && (results.tickers.length > 0 || results.insiders.length > 0);

  return (
    <div ref={boxRef} className="relative w-full max-w-xl">
      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        onFocus={() => results && setOpen(true)}
        onKeyDown={onKeyDown}
        placeholder="Search a company or insider — e.g. NVDA, Roger S. Penske"
        className="w-full rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-2.5 text-sm text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6]/60 focus:outline-none"
      />
      {loading && (
        <div className="absolute right-3 top-3 text-xs text-[#55556A]">…</div>
      )}

      {open && hasResults && (
        <div className="absolute left-0 right-0 top-full z-50 mt-1 overflow-hidden rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-xl">
          {(companiesFirst ? ["t", "i"] : ["i", "t"]).map((kind, gi) =>
            kind === "t" ? (
              shownTickers.length > 0 && (
                <div key="t" className={gi > 0 ? "border-t border-[#2A2A3A] py-1" : "py-1"}>
                  <GroupHeader
                    label="Companies"
                    shown={shownTickers.length}
                    total={tickerTotal}
                  />
                  {shownTickers.map((t) => {
                    const idx = flat.findIndex((f) => f.key === `t${t.ticker}`);
                    return (
                      <button
                        key={t.ticker}
                        onMouseEnter={() => setActive(idx)}
                        onClick={() => go(`/explore?ticker=${encodeURIComponent(t.ticker)}`)}
                        className={`flex w-full items-center gap-3 px-3 py-2 text-left transition-colors ${
                          idx === active ? "bg-[#1A1A26]" : "hover:bg-[#1A1A26]/60"
                        }`}
                      >
                        <span className="w-16 shrink-0 font-mono text-sm text-[#E8E8ED]">
                          <Highlight text={t.ticker} q={term} />
                        </span>
                        <span className="truncate text-xs text-[#8888A0]">
                          <Highlight text={t.company || ""} q={term} />
                        </span>
                      </button>
                    );
                  })}
                </div>
              )
            ) : (
              shownInsiders.length > 0 && (
                <div key="i" className={gi > 0 ? "border-t border-[#2A2A3A] py-1" : "py-1"}>
                  <GroupHeader
                    label="Insiders"
                    shown={shownInsiders.length}
                    total={insiderTotal}
                  />
                  {shownInsiders.map((ins) => {
                    const idx = flat.findIndex((f) => f.key === `i${ins.insider_id}`);
                    return (
                      <button
                        key={ins.insider_id}
                        onMouseEnter={() => setActive(idx)}
                        onClick={() =>
                          go(`/explore?insider=${encodeURIComponent(ins.slug || ins.insider_id)}`)
                        }
                        className={`flex w-full items-center gap-3 px-3 py-2 text-left transition-colors ${
                          idx === active ? "bg-[#1A1A26]" : "hover:bg-[#1A1A26]/60"
                        }`}
                      >
                        <span className="truncate text-sm text-[#E8E8ED]">
                          <Highlight text={ins.name} q={term} />
                        </span>
                        {ins.primary_title && (
                          <span className="truncate text-xs text-[#8888A0]">{ins.primary_title}</span>
                        )}
                        {ins.primary_ticker && (
                          <span className="ml-auto shrink-0 font-mono text-xs text-[#55556A]">
                            {ins.primary_ticker}
                          </span>
                        )}
                      </button>
                    );
                  })}
                </div>
              )
            ),
          )}
          <div className="border-t border-[#2A2A3A] px-3 py-1.5 text-[10px] text-[#55556A]">
            &uarr;&darr; to navigate &middot; &crarr; to open
          </div>
        </div>
      )}

      {open && results && !hasResults && !loading && (
        <div className="absolute left-0 right-0 top-full z-50 mt-1 rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-6 text-center text-sm text-[#8888A0] shadow-xl">
          No companies or insiders match &ldquo;{term}&rdquo;
        </div>
      )}
    </div>
  );
}
