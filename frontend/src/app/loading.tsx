import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function Loading() {
  return (
    <div className="space-y-6">
      {/* Stat cards skeleton */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i} className="bg-[#12121A] border-[#2A2A3A]">
            <CardHeader className="pb-2">
              <Skeleton className="h-4 w-24 bg-[#2A2A3A]" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-20 bg-[#2A2A3A]" />
              <Skeleton className="mt-2 h-3 w-16 bg-[#2A2A3A]" />
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Content area skeleton */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
        {/* Table skeleton */}
        <Card className="bg-[#12121A] border-[#2A2A3A] lg:col-span-3">
          <CardHeader>
            <Skeleton className="h-5 w-48 bg-[#2A2A3A]" />
          </CardHeader>
          <CardContent className="space-y-3">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full bg-[#2A2A3A]" />
            ))}
          </CardContent>
        </Card>

        {/* Chart skeleton */}
        <Card className="bg-[#12121A] border-[#2A2A3A] lg:col-span-2">
          <CardHeader>
            <Skeleton className="h-5 w-40 bg-[#2A2A3A]" />
          </CardHeader>
          <CardContent>
            <Skeleton className="h-[300px] w-full bg-[#2A2A3A]" />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
