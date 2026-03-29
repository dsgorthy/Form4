"use client";

import { useState, useEffect } from "react";
import { useAuth } from "@clerk/nextjs";
import { formatCurrency } from "@/lib/format";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

interface Horizon {
  window: string;
  days: number;
  stock_return: number;
  spy_return: number | null;
  alpha: number | null;
  entry_price: number | null;
  exit_price: number | null;
  pnl_10k: number;
}

interface OptionResult {
  strike_label: string;
  hold: string;
  option_type: string;
  strike: number;
  dte: number;
  entry_ask: number;
  exit_bid: number;
  return_pct: number;
  pnl_1k: number;
}

interface WhatIfData {
  ticker: string;
  trade_type: string;
  filing_date: string;
  horizons: Horizon[];
  options: OptionResult[];
}

export function WhatIfSimulator({ tradeId }: { tradeId: string }) {
  const { getToken } = useAuth();
  const [data, setData] = useState<WhatIfData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetch_data() {
      try {
        const token = await getToken();
        const res = await fetch(`${apiBase}/filings/${tradeId}/what-if`, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (res.ok) setData(await res.json());
      } catch {}
      setLoading(false);
    }
    fetch_data();
  }, [tradeId, getToken]);

  if (loading) {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5 animate-pulse">
        <div className="h-4 w-40 bg-[#12121A] rounded mb-4" />
        <div className="space-y-2">
          {[1, 2, 3].map(i => <div key={i} className="h-8 bg-[#12121A] rounded" />)}
        </div>
      </div>
    );
  }

  if (!data || data.horizons.length === 0) return null;

  const isSell = data.trade_type === "sell";

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-2">
        What If You Followed This Trade?
      </div>
      <div className="text-xs text-[#8888A0] mb-4">
        Entry at market open on the first trading day after the SEC filing became public.
        {data.horizons[0]?.entry_price && (
          <span className="font-mono ml-1">
            (${data.horizons[0].entry_price.toFixed(2)} on {(() => {
              // Filing date + 1 business day
              const fd = new Date(data.filing_date);
              fd.setDate(fd.getDate() + 1);
              while (fd.getDay() === 0 || fd.getDay() === 6) fd.setDate(fd.getDate() + 1);
              return fd.toISOString().slice(0, 10);
            })()})
          </span>
        )}
      </div>

      {/* Stock performance table */}
      <div className="text-xs text-[#55556A] mb-2">Stock Performance (hypothetical $10K position)</div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[#55556A] border-b border-[#2A2A3A]/50">
              <th className="text-left py-1.5 pr-3 font-medium">Horizon</th>
              <th className="text-right py-1.5 px-2 font-medium">Stock</th>
              <th className="text-right py-1.5 px-2 font-medium">SPY</th>
              <th className="text-right py-1.5 px-2 font-medium">Alpha</th>
              <th className="text-right py-1.5 pl-2 font-medium">P&L ($10K)</th>
            </tr>
          </thead>
          <tbody className="font-mono">
            {data.horizons.map((h) => {
              const isGood = isSell ? h.stock_return < 0 : h.stock_return > 0;
              return (
                <tr key={h.window} className="border-b border-[#2A2A3A]/30">
                  <td className="py-1.5 pr-3 text-[#8888A0]">{h.window}</td>
                  <td className={`text-right py-1.5 px-2 ${isGood ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                    {h.stock_return > 0 ? "+" : ""}{h.stock_return}%
                  </td>
                  <td className="text-right py-1.5 px-2 text-[#55556A]">
                    {h.spy_return != null ? `${h.spy_return > 0 ? "+" : ""}${h.spy_return}%` : "\u2014"}
                  </td>
                  <td className={`text-right py-1.5 px-2 ${h.alpha != null ? (isSell ? (h.alpha < 0 ? "text-[#22C55E]" : "text-[#EF4444]") : (h.alpha > 0 ? "text-[#22C55E]" : "text-[#EF4444]")) : "text-[#55556A]"}`}>
                    {h.alpha != null ? `${h.alpha > 0 ? "+" : ""}${h.alpha}%` : "\u2014"}
                  </td>
                  <td className={`text-right py-1.5 pl-2 ${isGood ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                    {h.pnl_10k > 0 ? "+" : ""}{formatCurrency(h.pnl_10k)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Options performance */}
      {data.options.length > 0 && (
        <>
          <div className="text-xs text-[#55556A] mt-4 mb-2">
            Options Performance (hypothetical $1K premium, entry at ASK, exit at BID)
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[#55556A] border-b border-[#2A2A3A]/50">
                  <th className="text-left py-1.5 pr-2 font-medium">Strike</th>
                  <th className="text-left py-1.5 px-2 font-medium">Hold</th>
                  <th className="text-right py-1.5 px-2 font-medium">DTE</th>
                  <th className="text-right py-1.5 px-2 font-medium">Entry</th>
                  <th className="text-right py-1.5 px-2 font-medium">Exit</th>
                  <th className="text-right py-1.5 px-2 font-medium">Return</th>
                  <th className="text-right py-1.5 pl-2 font-medium">P&L ($1K)</th>
                </tr>
              </thead>
              <tbody className="font-mono">
                {data.options.map((o, i) => {
                  const isGood = o.return_pct > 0;
                  return (
                    <tr key={i} className="border-b border-[#2A2A3A]/30">
                      <td className="py-1.5 pr-2 text-[#8888A0]">{o.strike_label}</td>
                      <td className="py-1.5 px-2 text-[#8888A0]">{o.hold}</td>
                      <td className="text-right py-1.5 px-2 text-[#55556A]">{o.dte}d</td>
                      <td className="text-right py-1.5 px-2 text-[#E8E8ED]">${o.entry_ask.toFixed(2)}</td>
                      <td className="text-right py-1.5 px-2 text-[#E8E8ED]">${o.exit_bid.toFixed(2)}</td>
                      <td className={`text-right py-1.5 px-2 ${isGood ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                        {o.return_pct > 0 ? "+" : ""}{o.return_pct}%
                      </td>
                      <td className={`text-right py-1.5 pl-2 ${isGood ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                        {o.pnl_1k > 0 ? "+" : ""}{formatCurrency(o.pnl_1k)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      <div className="text-[10px] text-[#55556A] mt-3">
        Hypothetical returns based on actual market data. Past performance does not guarantee future results.
      </div>
    </div>
  );
}
