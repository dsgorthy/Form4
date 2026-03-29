"use client";

import { useAuth, useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";

interface ExportButtonProps {
  params?: Record<string, string>;
}

export function ExportButton({ params }: ExportButtonProps) {
  const { getToken } = useAuth();
  const { user } = useUser();

  if (!isPro(user)) return null;

  async function handleExport() {
    const token = await getToken();
    const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
    const url = new URL(`${apiBase}/export/filings`);
    if (params) {
      Object.entries(params).forEach(([k, v]) => {
        if (v) url.searchParams.set(k, v);
      });
    }

    const res = await fetch(url.toString(), {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });

    if (!res.ok) return;

    const blob = await res.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "form4_filings.csv";
    a.click();
    URL.revokeObjectURL(a.href);
  }

  return (
    <button
      onClick={handleExport}
      className="rounded-md border border-[#2A2A3A] px-3 py-1.5 text-xs font-medium text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors"
    >
      Export CSV
    </button>
  );
}
