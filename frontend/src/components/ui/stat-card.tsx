import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: "up" | "down" | "neutral";
}

export function StatCard({ title, value, subtitle, trend }: StatCardProps) {
  const trendColor = trend === "up" ? "text-green-500" : trend === "down" ? "text-red-500" : "text-muted-foreground";
  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-[#8888A0]">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold font-mono text-[#E8E8ED]">{value}</div>
        {subtitle && <p className={`text-xs mt-1 ${trendColor}`}>{subtitle}</p>}
      </CardContent>
    </Card>
  );
}
