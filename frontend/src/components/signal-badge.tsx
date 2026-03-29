import type { TradeSignal } from "@/lib/types";

const CLASS_COLORS: Record<string, string> = {
  bullish: "bg-[#22C55E]/15 text-[#22C55E] border-[#22C55E]/30",
  bearish: "bg-[#EF4444]/15 text-[#EF4444] border-[#EF4444]/30",
  noise: "bg-[#6B7280]/15 text-[#6B7280] border-[#6B7280]/30",
  neutral: "bg-[#3B82F6]/15 text-[#3B82F6] border-[#3B82F6]/30",
};

export function SignalBadge({ signal }: { signal: TradeSignal }) {
  const colors = CLASS_COLORS[signal.signal_class] || CLASS_COLORS.neutral;
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-medium ${colors}`}
      title={`${signal.signal_label} (${Math.round(signal.confidence * 100)}% confidence)`}
    >
      {signal.signal_label}
    </span>
  );
}

export function SignalBadges({ signals }: { signals?: TradeSignal[] }) {
  if (!signals || signals.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1">
      {signals.map((s) => (
        <SignalBadge key={s.signal_type} signal={s} />
      ))}
    </div>
  );
}
