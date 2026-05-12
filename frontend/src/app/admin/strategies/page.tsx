"use client";

import { useEffect, useState } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import Link from "next/link";
import { fetchAPIWithAuth } from "@/lib/api";

interface DecisionSummary {
  total_evaluations: number;
  rejected: number;
  by_stage: Record<string, { passed: number; rejected: number; total: number }>;
}

interface StrategyRow {
  name: string;
  label: string;
  thesis: string;
  decision_summary: DecisionSummary;
  freshness_stale_count: number;
  freshness_total: number;
  latest_live_decision_at: string | null;
  recent_critical_alerts: number;
}

export default function AdminStrategiesPage() {
  const { isSignedIn, getToken } = useAuth();
  const { user } = useUser();
  const [strategies, setStrategies] = useState<StrategyRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const token = await getToken();
        const data = await fetchAPIWithAuth<{ strategies: StrategyRow[] }>(
          "/admin/diagnostics/strategies",
          token,
        );
        setStrategies(data.strategies);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("403")) {
          setError("Access denied. This page is restricted to admin users.");
        } else {
          setError(msg);
        }
      } finally {
        setLoading(false);
      }
    }
    if (isSignedIn) load();
  }, [isSignedIn, getToken]);

  if (!isSignedIn) {
    return (
      <div className="text-[#E8E8ED] py-10">
        <h1 className="text-2xl font-bold">Admin Diagnostics</h1>
        <p className="text-[#8888A0] mt-2">Sign in to view.</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="text-[#8888A0] py-10">Loading admin diagnostics…</div>
    );
  }

  if (error) {
    return (
      <div className="py-10">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Admin Diagnostics</h1>
        <p className="text-[#EF4444] mt-4">{error}</p>
        <p className="text-[#55556A] text-sm mt-2">
          Signed in as <code className="text-[#8888A0]">{user?.id}</code>. If
          this is wrong, set <code>ADMIN_USER_IDS</code> on Studio to include
          your Clerk user ID.
        </p>
      </div>
    );
  }

  return (
    <div className="text-[#E8E8ED] py-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold">Admin — Strategy Diagnostics</h1>
        <p className="text-sm text-[#55556A] mt-1">
          Private to your account. Per-strategy filter pass/fail, freshness
          contracts, recent rejections, and alerts. Distinct from the public{" "}
          <Link href="/portfolio" className="text-[#3B82F6] hover:underline">
            /portfolio
          </Link>{" "}
          page (which shows live trades only).
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        {strategies?.map((s) => (
          <Link
            key={s.name}
            href={`/admin/strategies/${s.name}`}
            className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 hover:bg-[#1A1A26] transition"
          >
            <div className="flex items-baseline justify-between mb-2">
              <h2 className="text-lg font-semibold">{s.label}</h2>
              <code className="text-xs text-[#55556A]">{s.name}</code>
            </div>
            <p className="text-xs text-[#8888A0] mb-4 line-clamp-2">{s.thesis}</p>

            <div className="grid grid-cols-2 gap-3 text-sm">
              <Stat
                label="Evaluations"
                value={s.decision_summary.total_evaluations.toLocaleString()}
                hint={`${s.decision_summary.rejected.toLocaleString()} rejected`}
              />
              <Stat
                label="Freshness"
                value={
                  s.freshness_stale_count === 0
                    ? `${s.freshness_total} ✓`
                    : `${s.freshness_stale_count}/${s.freshness_total} stale`
                }
                tone={s.freshness_stale_count > 0 ? "warn" : "ok"}
              />
              <Stat
                label="Last decision"
                value={s.latest_live_decision_at?.slice(0, 16) ?? "—"}
              />
              <Stat
                label="P0 alerts (24h)"
                value={s.recent_critical_alerts}
                tone={s.recent_critical_alerts > 0 ? "warn" : "ok"}
              />
            </div>
          </Link>
        ))}
      </div>

      <div className="mt-8 text-xs text-[#55556A]">
        <p>
          <Link href="/admin/jobs" className="text-[#3B82F6] hover:underline font-semibold">→ System Jobs Monitor</Link>{" "}<br />
          See also: <Link href="/admin/strategies/quality_momentum" className="text-[#3B82F6] hover:underline">quality_momentum</Link>{" "}
          · <Link href="/admin/strategies/reversal_dip" className="text-[#3B82F6] hover:underline">reversal_dip</Link>{" "}
          · <Link href="/admin/strategies/tenb51_surprise" className="text-[#3B82F6] hover:underline">tenb51_surprise</Link>
        </p>
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string | number;
  hint?: string;
  tone?: "ok" | "warn";
}) {
  const valueColor =
    tone === "warn" ? "text-[#F59E0B]" : tone === "ok" ? "text-[#22C55E]" : "text-[#E8E8ED]";
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-[#55556A]">
        {label}
      </div>
      <div className={`text-base font-medium ${valueColor}`}>{value}</div>
      {hint && <div className="text-[10px] text-[#55556A]">{hint}</div>}
    </div>
  );
}
