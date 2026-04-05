"use client";

import { useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay } from "@/components/ui/ticker-display";
import { formatCurrency, formatPercent } from "@/lib/format";
import { RelativeTime } from "@/components/relative-time";
import { FilingDetailPanel } from "@/components/filing-detail-panel";
import type { Filing } from "@/lib/types";

interface SignalsTableProps {
  filings: Filing[];
}

export function SignalsTable({ filings }: SignalsTableProps) {
  const [selected, setSelected] = useState<Filing | null>(null);

  if (filings.length === 0) {
    return (
      <div className="flex h-40 items-center justify-center text-[#55556A]">
        No recent signals
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow className="border-[#2A2A3A] hover:bg-transparent">
            <TableHead className="text-[#55556A]">Ticker</TableHead>
            <TableHead className="text-[#55556A]">Insider</TableHead>
            <TableHead className="text-[#55556A]">Type</TableHead>
            <TableHead className="hidden md:table-cell text-[#55556A]">Tier</TableHead>
            <TableHead className="text-[#55556A] text-right">Value</TableHead>
            <TableHead className="hidden md:table-cell text-[#55556A] text-right">7d Stock</TableHead>
            <TableHead className="hidden md:table-cell text-[#55556A] text-right">Filed</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {filings.map((f) => (
            <TableRow
              key={f.trade_id}
              className="border-[#2A2A3A] hover:bg-[#1A1A26]/50 cursor-pointer"
              onClick={() => setSelected(f)}
            >
              <TableCell>
                <TickerDisplay ticker={f.ticker} company={f.company} />
              </TableCell>
              <TableCell>
                <div className="text-[#E8E8ED] text-sm">{f.insider_name}</div>
                <div className="text-[#55556A] text-xs">{f.title}</div>
              </TableCell>
              <TableCell>
                <Badge
                  variant="outline"
                  className={`text-xs font-mono ${
                    f.trade_type === "buy"
                      ? "border-green-500/30 text-green-500"
                      : "border-red-500/30 text-red-500"
                  }`}
                >
                  {f.trade_type.toUpperCase()}
                </Badge>
              </TableCell>
              <TableCell className="hidden md:table-cell">
                <TierBadge tier={f.score_tier ?? f.tier} pitGrade={f.pit_grade} compact />
              </TableCell>
              <TableCell className="text-right font-mono text-[#E8E8ED]">
                {formatCurrency(f.value)}
              </TableCell>
              <TableCell
                className={`hidden md:table-cell text-right font-mono ${
                  f.return_7d != null
                    ? (f.trade_type === "buy" ? f.return_7d >= 0 : f.return_7d <= 0)
                      ? "text-green-500"
                      : "text-red-500"
                    : "text-[#55556A]"
                }`}
              >
                {formatPercent(f.return_7d)}
              </TableCell>
              <TableCell className="hidden md:table-cell text-right text-[#8888A0] text-sm">
                <RelativeTime date={f.filing_date} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <FilingDetailPanel filing={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
