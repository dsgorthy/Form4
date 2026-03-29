import { Badge } from "@/components/ui/badge";
import { tierColor, tierLabel } from "@/lib/format";

export function TierBadge({ tier }: { tier: number | null | undefined }) {
  if (tier == null) return <span className="text-[#55556A]">{"\u2014"}</span>;
  return (
    <Badge
      style={{ backgroundColor: tierColor(tier), color: "#fff" }}
      className="text-xs font-mono px-1.5 py-0.5"
    >
      {tierLabel(tier)}
    </Badge>
  );
}
