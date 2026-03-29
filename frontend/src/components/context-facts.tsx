import type { TradeContext } from "@/lib/types";

interface ContextFactsProps {
  context?: TradeContext[];
  limit?: number;
  /** Render as inline comma-separated text instead of vertical bullets */
  inline?: boolean;
}

export function ContextFacts({ context, limit, inline }: ContextFactsProps) {
  if (!context || context.length === 0) return null;
  const items = limit ? context.slice(0, limit) : context;
  const remaining = limit && context.length > limit ? context.length - limit : 0;

  if (inline) {
    return (
      <p className="text-xs text-[#8888A0]">
        {items.map((c, i) => (
          <span key={`${c.type}-${i}`}>
            {c.text}{i < items.length - 1 && <span className="text-[#55556A]"> · </span>}
          </span>
        ))}
        {remaining > 0 && (
          <span className="text-[#55556A]"> +{remaining} more</span>
        )}
      </p>
    );
  }

  return (
    <ul className="space-y-0.5">
      {items.map((c, i) => (
        <li key={`${c.type}-${i}`} className="flex items-start gap-1.5 text-xs text-[#8888A0]">
          <span className="text-[#55556A] mt-px shrink-0">&bull;</span>
          <span>{c.text}</span>
        </li>
      ))}
      {remaining > 0 && (
        <li className="text-xs text-[#55556A]">+{remaining} more</li>
      )}
    </ul>
  );
}
