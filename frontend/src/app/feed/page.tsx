export const dynamic = "force-dynamic";

import { SyncStatus } from "@/components/sync-status";
import { ExportButton } from "@/components/export-button";
import { FeedList } from "@/components/feed-list";

interface Props {
  searchParams: Promise<{
    ticker?: string;
  }>;
}

export default async function FeedPage({ searchParams }: Props) {
  const sp = await searchParams;
  const ticker = sp.ticker || "";

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Filing Feed</h1>
          <div className="flex items-center gap-3 mt-1">
            <SyncStatus />
          </div>
        </div>
        <ExportButton params={{ trade_type: "", min_value: "", ticker }} />
      </div>

      {/* Feed with unified filters */}
      <FeedList initialTicker={ticker} />
    </div>
  );
}
