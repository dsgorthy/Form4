"use client";

interface PaginationProps {
  total: number;
  limit: number;
  offset: number;
  onPageChange: (offset: number) => void;
}

export function Pagination({ total, limit, offset, onPageChange }: PaginationProps) {
  if (total <= limit) return null;

  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.ceil(total / limit);
  const hasPrev = offset > 0;
  const hasNext = offset + limit < total;

  return (
    <div className="flex items-center justify-between pt-3">
      <span className="text-xs text-[#55556A]">
        {offset + 1}-{Math.min(offset + limit, total)} of {total}
      </span>
      <div className="flex items-center gap-1">
        <button
          onClick={() => onPageChange(0)}
          disabled={!hasPrev}
          className="rounded px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 disabled:opacity-30 disabled:pointer-events-none transition-colors"
        >
          First
        </button>
        <button
          onClick={() => onPageChange(offset - limit)}
          disabled={!hasPrev}
          className="rounded px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 disabled:opacity-30 disabled:pointer-events-none transition-colors"
        >
          Prev
        </button>
        <span className="px-2 text-xs text-[#55556A]">
          {currentPage} / {totalPages}
        </span>
        <button
          onClick={() => onPageChange(offset + limit)}
          disabled={!hasNext}
          className="rounded px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 disabled:opacity-30 disabled:pointer-events-none transition-colors"
        >
          Next
        </button>
        <button
          onClick={() => onPageChange((totalPages - 1) * limit)}
          disabled={!hasNext}
          className="rounded px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 disabled:opacity-30 disabled:pointer-events-none transition-colors"
        >
          Last
        </button>
      </div>
    </div>
  );
}
