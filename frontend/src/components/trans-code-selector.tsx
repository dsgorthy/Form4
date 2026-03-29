"use client";

import { useState, useCallback } from "react";

const TRANS_CODES = [
  { code: "P", label: "Purchase", color: "bg-[#22C55E]/20 text-[#22C55E] border-[#22C55E]/30" },
  { code: "S", label: "Sale", color: "bg-[#EF4444]/20 text-[#EF4444] border-[#EF4444]/30" },
  { code: "M", label: "Exercise", color: "bg-[#8B5CF6]/20 text-[#8B5CF6] border-[#8B5CF6]/30" },
  { code: "A", label: "Award", color: "bg-[#F59E0B]/20 text-[#F59E0B] border-[#F59E0B]/30" },
  { code: "F", label: "Tax", color: "bg-[#6B7280]/20 text-[#6B7280] border-[#6B7280]/30" },
  { code: "G", label: "Gift", color: "bg-[#6B7280]/20 text-[#6B7280] border-[#6B7280]/30" },
  { code: "X", label: "RSU", color: "bg-[#6B7280]/20 text-[#6B7280] border-[#6B7280]/30" },
] as const;

interface TransCodeSelectorProps {
  selected: Set<string>;
  onChange: (selected: Set<string>) => void;
}

export function TransCodeSelector({ selected, onChange }: TransCodeSelectorProps) {
  const toggle = useCallback(
    (code: string) => {
      const next = new Set(selected);
      if (next.has(code)) {
        if (next.size > 1) next.delete(code);
      } else {
        next.add(code);
      }
      onChange(next);
    },
    [selected, onChange],
  );

  return (
    <div className="flex flex-wrap gap-1.5">
      {TRANS_CODES.map(({ code, label, color }) => {
        const active = selected.has(code);
        return (
          <button
            key={code}
            onClick={() => toggle(code)}
            className={`rounded-md border px-2 py-1 text-xs font-mono transition-all ${
              active
                ? color
                : "border-[#2A2A3A] text-[#55556A] hover:text-[#8888A0] hover:border-[#3A3A4A]"
            }`}
          >
            {code}
            <span className="hidden sm:inline ml-1 font-sans">{label}</span>
          </button>
        );
      })}
    </div>
  );
}
