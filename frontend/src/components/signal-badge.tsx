import type { TradeSignal } from "@/lib/types";

const CLASS_COLORS: Record<string, string> = {
  bullish: "bg-[#22C55E]/15 text-[#22C55E] border-[#22C55E]/30",
  bearish: "bg-[#EF4444]/15 text-[#EF4444] border-[#EF4444]/30",
  noise: "bg-[#6B7280]/15 text-[#6B7280] border-[#6B7280]/30",
  neutral: "bg-[#3B82F6]/15 text-[#3B82F6] border-[#3B82F6]/30",
};

const COMPOSITE_STYLE = "bg-[#F59E0B]/15 text-[#F59E0B] border-[#F59E0B]/50 ring-1 ring-[#F59E0B]/20";

const COMPOSITE_TYPES = new Set([
  "quality_momentum_buy",
  "tenb51_surprise_buy",
  "deep_reversal_dip_buy",
]);

export function SignalBadge({ signal }: { signal: TradeSignal }) {
  const isComposite = COMPOSITE_TYPES.has(signal.signal_type);
  const colors = isComposite ? COMPOSITE_STYLE : (CLASS_COLORS[signal.signal_class] || CLASS_COLORS.neutral);
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
