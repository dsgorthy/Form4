"use client";

import { useUser, useAuth } from "@clerk/nextjs";
import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import { isPro, hasApiAccess } from "@/lib/subscription";
import { ProBadge } from "@/components/pro-badge";
import { TickerAutocomplete } from "@/components/ticker-autocomplete";
import type { NotificationPreferences, WatchlistItem } from "@/lib/types";

interface ApiKeyInfo {
  id: string;
  name: string;
  hint: string;
  created_at: string | null;
}

export default function SettingsPage() {
  const { user, isLoaded } = useUser();
  const { isSignedIn, getToken } = useAuth();
  const [newKey, setNewKey] = useState<string | null>(null);
  const [keyName, setKeyName] = useState("");
  const [keys, setKeys] = useState<ApiKeyInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const [prefs, setPrefs] = useState<NotificationPreferences | null>(null);
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [watchlistTicker, setWatchlistTicker] = useState("");
  const [prefsSaving, setPrefsSaving] = useState(false);
  const searchParams = useSearchParams();
  const justSubscribed = searchParams.get("success") === "true";

  const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

  // Force Clerk to refetch user data after checkout redirect
  useEffect(() => {
    if (justSubscribed && user) {
      user.reload();
    }
  }, [justSubscribed, user]);

  const fetchKeys = useCallback(async () => {
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/api-keys`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (res.ok) {
        const data = await res.json();
        setKeys(data.keys || []);
      }
    } catch {
      // silent
    }
  }, [apiBase, getToken]);

  const fetchPrefs = useCallback(async () => {
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/notifications/preferences`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (res.ok) setPrefs(await res.json());
    } catch {
      // silent
    }
  }, [apiBase, getToken]);

  const fetchWatchlist = useCallback(async () => {
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/notifications/watchlist`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (res.ok) {
        const data = await res.json();
        setWatchlist(data.items || []);
      }
    } catch {
      // silent
    }
  }, [apiBase, getToken]);

  async function updatePref(updates: Partial<NotificationPreferences>) {
    setPrefsSaving(true);
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/notifications/preferences`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(updates),
      });
      if (res.ok) setPrefs(await res.json());
    } catch {
      // silent
    } finally {
      setPrefsSaving(false);
    }
  }

  async function removeWatchlistTicker(ticker: string) {
    try {
      const token = await getToken();
      await fetch(`${apiBase}/notifications/watchlist/${ticker}`, {
        method: "DELETE",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      await fetchWatchlist();
    } catch {
      // silent
    }
  }

  // Load keys on mount
  useEffect(() => {
    if (isLoaded && isSignedIn) {
      fetchKeys();
    }
  }, [isLoaded, isSignedIn, fetchKeys]);

  // Load notification prefs + watchlist for Pro users
  useEffect(() => {
    if (isLoaded && isSignedIn && isPro(user)) {
      fetchPrefs();
      fetchWatchlist();
    }
  }, [isLoaded, isSignedIn, user, fetchPrefs, fetchWatchlist]);

  if (!isLoaded) return null;

  if (!isSignedIn) {
    return (
      <div className="flex items-center justify-center min-h-[40vh]">
        <p className="text-[#8888A0]">Sign in to manage your account.</p>
      </div>
    );
  }

  const userIsPro = isPro(user);
  const userHasApi = hasApiAccess(user);

  async function handleBillingPortal() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/billing-portal", { method: "POST" });
      const data = await res.json();
      if (data.error) {
        setError(data.error);
      } else if (data.url) {
        window.location.href = data.url;
      }
    } catch {
      setError("Failed to open billing portal.");
    } finally {
      setLoading(false);
    }
  }

  async function handleCreateApiKey() {
    setLoading(true);
    setError(null);
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/api-keys`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({ name: keyName || "Untitled Key" }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail || "Failed to create API key.");
      } else if (data.api_key) {
        setNewKey(data.api_key);
        setKeyName("");
        await fetchKeys();
      }
    } catch {
      setError("Failed to create API key.");
    } finally {
      setLoading(false);
    }
  }

  async function handleRevokeKey(keyId: string) {
    setRevokingId(keyId);
    setError(null);
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/api-keys/${keyId}`, {
        method: "DELETE",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.detail || "Failed to revoke key.");
      } else {
        await fetchKeys();
        if (newKey) setNewKey(null);
      }
    } catch {
      setError("Failed to revoke key.");
    } finally {
      setRevokingId(null);
    }
  }

  function handleCopy() {
    if (newKey) {
      navigator.clipboard.writeText(newKey);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }

  return (
    <div className="max-w-2xl mx-auto py-8 space-y-8">
      <h1 className="text-2xl font-bold text-[#E8E8ED]">Account Settings</h1>

      {justSubscribed && (
        <div className="rounded-lg border border-[#22C55E]/30 bg-[#22C55E]/10 px-4 py-3 text-sm text-[#22C55E]">
          Subscription activated! You now have full access to Form4 Pro.
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-[#EF4444]/30 bg-[#EF4444]/10 px-4 py-3 text-sm text-[#EF4444]">
          {error}
        </div>
      )}

      {/* Profile */}
      {(() => {
        const meta = user?.unsafeMetadata as Record<string, unknown> | undefined;
        const skipped = meta?.onboardingSkipped || !meta?.userType;

        const labels: Record<string, string> = {
          individual: "Individual Investor", advisor: "Financial Advisor", quant: "Quant / Analyst",
          fund_manager: "Fund Manager", journalist: "Journalist / Researcher", student: "Student / Academic",
          trading_signals: "Trading Signals", research: "Research & Due Diligence", portfolio: "Portfolio Monitoring",
          compliance: "Compliance Monitoring", academic: "Academic Research", tracking: "Tracking Insiders",
          beginner: "New to it", intermediate: "Somewhat familiar", expert: "Regular user",
        };

        if (skipped) {
          return (
            <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/5 p-6">
              <h2 className="text-sm font-semibold uppercase tracking-wider text-[#F59E0B] mb-4">
                Action Required
              </h2>
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-[#E8E8ED]">
                    Complete your profile to unlock personalized signals
                  </p>
                  <p className="text-xs text-[#8888A0] mt-1">
                    Takes 30 seconds — we'll tailor your dashboard, alerts, and recommendations.
                  </p>
                </div>
                <a
                  href="/onboarding"
                  onClick={() => { const prev = user?.unsafeMetadata || {}; user?.update({ unsafeMetadata: { ...prev, onboardingComplete: false } }); }}
                  className="shrink-0 rounded-lg bg-[#F59E0B] px-4 py-2 text-sm font-semibold text-[#0A0A0F] hover:bg-[#D97706] transition-colors"
                >
                  Set Up Profile
                </a>
              </div>
            </div>
          );
        }

        return (
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6">
            <h2 className="text-sm font-semibold uppercase tracking-wider text-[#55556A] mb-4">
              Profile
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div>
                <p className="text-xs text-[#55556A] mb-1">Role</p>
                <p className="text-sm text-[#E8E8ED]">{labels[meta?.userType as string] || String(meta?.userType || "—")}</p>
              </div>
              <div>
                <p className="text-xs text-[#55556A] mb-1">Primary Use</p>
                <p className="text-sm text-[#E8E8ED]">{labels[meta?.primaryUseCase as string] || String(meta?.primaryUseCase || "—")}</p>
              </div>
              <div>
                <p className="text-xs text-[#55556A] mb-1">Experience</p>
                <p className="text-sm text-[#E8E8ED]">{labels[meta?.experienceLevel as string] || String(meta?.experienceLevel || "—")}</p>
              </div>
            </div>
            <div className="mt-4 pt-4 border-t border-[#2A2A3A]">
              <a
                href="/onboarding"
                onClick={() => { const prev = user?.unsafeMetadata || {}; user?.update({ unsafeMetadata: { ...prev, onboardingComplete: false } }); }}
                className="text-xs text-[#55556A] hover:text-[#8888A0] transition-colors"
              >
                Edit profile
              </a>
            </div>
          </div>
        );
      })()}

      {/* Subscription */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6">
        <h2 className="text-sm font-semibold uppercase tracking-wider text-[#55556A] mb-4">
          Subscription
        </h2>
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-lg font-bold text-[#E8E8ED]">
                {userIsPro ? "Pro" : "Free"}
              </span>
              {userIsPro && <ProBadge />}
            </div>
            <p className="text-sm text-[#8888A0] mt-1">
              {userIsPro
                ? "Full access to all Form4 features"
                : "Limited to last 90 days, no insider scores"}
            </p>
          </div>
          {userIsPro ? (
            <button
              onClick={handleBillingPortal}
              disabled={loading}
              className="rounded-lg border border-[#2A2A3A] px-4 py-2 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors disabled:opacity-50"
            >
              Manage Billing
            </button>
          ) : (
            <a
              href="/pricing"
              className="rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              Upgrade to Pro
            </a>
          )}
        </div>
      </div>

      {/* API Keys */}
      {userIsPro && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#55556A] mb-4">
            API Access
          </h2>

          {!userHasApi ? (
            <div className="flex items-center justify-between">
              <p className="text-sm text-[#8888A0]">
                Add programmatic API access to your Pro subscription.
              </p>
              <a
                href="/pricing"
                className="rounded-lg border border-[#2A2A3A] px-4 py-2 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors"
              >
                Add API ($15/mo)
              </a>
            </div>
          ) : (
            <div className="space-y-5">
              {/* Newly created key */}
              {newKey && (
                <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 p-4 space-y-3">
                  <p className="text-sm text-[#F59E0B] font-medium">
                    Save this key now — it won&apos;t be shown again.
                  </p>
                  <div className="flex items-center gap-2">
                    <code className="flex-1 rounded-md bg-[#1A1A26] border border-[#2A2A3A] p-3 text-sm font-mono text-[#E8E8ED] break-all">
                      {newKey}
                    </code>
                    <button
                      onClick={handleCopy}
                      className="shrink-0 rounded-md border border-[#2A2A3A] px-3 py-2 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors"
                    >
                      {copied ? "Copied!" : "Copy"}
                    </button>
                  </div>
                  <button
                    onClick={() => setNewKey(null)}
                    className="text-xs text-[#55556A] hover:text-[#8888A0]"
                  >
                    Dismiss
                  </button>
                </div>
              )}

              {/* Existing keys */}
              {keys.length > 0 && (
                <div>
                  <div className="text-xs text-[#55556A] mb-2">
                    {keys.length} of 3 keys used
                  </div>
                  <div className="space-y-2">
                    {keys.map((k) => (
                      <div
                        key={k.id}
                        className="flex items-center justify-between rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/30 px-4 py-3"
                      >
                        <div>
                          <div className="text-sm font-medium text-[#E8E8ED]">
                            {k.name}
                          </div>
                          <div className="text-xs text-[#55556A] font-mono mt-0.5">
                            ie_{k.hint}
                            {k.created_at && (
                              <span className="ml-2 font-sans">
                                Created {k.created_at}
                              </span>
                            )}
                          </div>
                        </div>
                        <button
                          onClick={() => handleRevokeKey(k.id)}
                          disabled={revokingId === k.id}
                          className="text-xs text-[#EF4444]/70 hover:text-[#EF4444] transition-colors disabled:opacity-50"
                        >
                          {revokingId === k.id ? "Revoking..." : "Revoke"}
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Create new key */}
              {keys.length < 3 && (
                <div className="border-t border-[#2A2A3A]/50 pt-4">
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      value={keyName}
                      onChange={(e) => setKeyName(e.target.value)}
                      placeholder="Key name (e.g. Production)"
                      className="flex-1 rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-2 text-sm text-[#E8E8ED] placeholder-[#55556A] focus:outline-none focus:border-[#3B82F6]"
                    />
                    <button
                      onClick={handleCreateApiKey}
                      disabled={loading}
                      className="shrink-0 rounded-lg border border-[#2A2A3A] px-4 py-2 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A] transition-colors disabled:opacity-50"
                    >
                      {loading ? "Generating..." : "Generate Key"}
                    </button>
                  </div>
                </div>
              )}

              {keys.length >= 3 && (
                <p className="text-xs text-[#55556A]">
                  Maximum 3 keys reached. Revoke an existing key to create a new one.
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {/* Notifications */}
      {userIsPro && prefs && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#55556A] mb-4">
            Notifications
          </h2>

          <div className="space-y-5">
            {/* Channels */}
            <div>
              <div className="text-xs text-[#55556A] mb-3">Channels</div>
              <div className="space-y-2">
                <ToggleRow
                  label="In-app notifications"
                  checked={prefs.in_app_enabled}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ in_app_enabled: v })}
                />
                <ToggleRow
                  label="Email notifications"
                  checked={prefs.email_enabled}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ email_enabled: v })}
                />
              </div>
            </div>

            {/* Email frequency */}
            {prefs.email_enabled && (
              <div>
                <div className="text-xs text-[#55556A] mb-2">Email frequency</div>
                <div className="flex gap-2">
                  {(["realtime", "daily"] as const).map((freq) => (
                    <button
                      key={freq}
                      onClick={() => updatePref({ email_frequency: freq })}
                      disabled={prefsSaving}
                      className={`rounded-md border px-3 py-1.5 text-xs font-medium transition-colors ${
                        prefs.email_frequency === freq
                          ? "border-[#3B82F6] bg-[#3B82F6]/10 text-[#3B82F6]"
                          : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#55556A]"
                      }`}
                    >
                      {freq === "realtime" ? "Realtime" : "Daily Digest"}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Event types */}
            <div className="border-t border-[#2A2A3A]/50 pt-4">
              <div className="text-xs text-[#55556A] mb-3">Alert types</div>
              <div className="space-y-2">
                <ToggleRow
                  label="Portfolio trade alerts"
                  desc="Entry/exit notifications from the Form4 Insider Portfolio"
                  checked={prefs.portfolio_alert ?? true}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ portfolio_alert: v })}
                />
                <ToggleRow
                  label="High-value filings"
                  desc="Tier 2+ insider buys/sells above your threshold"
                  checked={prefs.high_value_filing}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ high_value_filing: v })}
                />
                <ToggleRow
                  label="Cluster formations"
                  desc="Multiple insiders trading the same ticker"
                  checked={prefs.cluster_formation}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ cluster_formation: v })}
                />
                <ToggleRow
                  label="Activity spikes"
                  desc="Ticker activity jumps 2x+ above baseline"
                  checked={prefs.activity_spike}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ activity_spike: v })}
                />
                <ToggleRow
                  label="Congress convergence"
                  desc="Insider + politician alignment on same ticker"
                  checked={prefs.congress_convergence}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ congress_convergence: v })}
                />
                <ToggleRow
                  label="Watchlist activity"
                  desc="Any new filing on your watched tickers"
                  checked={prefs.watchlist_activity}
                  disabled={prefsSaving}
                  onChange={(v) => updatePref({ watchlist_activity: v })}
                />
              </div>
            </div>

            {/* Thresholds */}
            <div className="border-t border-[#2A2A3A]/50 pt-4">
              <div className="text-xs text-[#55556A] mb-3">Filing alert thresholds</div>
              <div className="flex items-center gap-4">
                <div>
                  <label className="block text-xs text-[#8888A0] mb-1">Min trade value</label>
                  <select
                    value={prefs.min_trade_value}
                    onChange={(e) => updatePref({ min_trade_value: Number(e.target.value) })}
                    disabled={prefsSaving}
                    className="rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-1.5 text-sm text-[#E8E8ED] focus:outline-none focus:border-[#3B82F6]"
                  >
                    <option value={25000}>$25K+</option>
                    <option value={50000}>$50K+</option>
                    <option value={100000}>$100K+</option>
                    <option value={250000}>$250K+</option>
                    <option value={500000}>$500K+</option>
                    <option value={1000000}>$1M+</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs text-[#8888A0] mb-1">Min insider tier</label>
                  <select
                    value={prefs.min_insider_tier}
                    onChange={(e) => updatePref({ min_insider_tier: Number(e.target.value) })}
                    disabled={prefsSaving}
                    className="rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-1.5 text-sm text-[#E8E8ED] focus:outline-none focus:border-[#3B82F6]"
                  >
                    <option value={1}>Tier 1+</option>
                    <option value={2}>Tier 2+</option>
                    <option value={3}>Tier 3+</option>
                    <option value={4}>Tier 4+</option>
                    <option value={5}>Tier 5 only</option>
                  </select>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Watchlist */}
      {userIsPro && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#55556A] mb-4">
            Watchlist
          </h2>
          <p className="text-xs text-[#55556A] mb-4">
            Get notified when new filings appear for these tickers.
          </p>

          {/* Add ticker */}
          <div className="mb-4">
            <TickerAutocomplete
              value={watchlistTicker}
              onChange={setWatchlistTicker}
              onSelect={async (ticker) => {
                setWatchlistTicker("");
                try {
                  const token = await getToken();
                  const res = await fetch(`${apiBase}/notifications/watchlist`, {
                    method: "POST",
                    headers: {
                      "Content-Type": "application/json",
                      ...(token ? { Authorization: `Bearer ${token}` } : {}),
                    },
                    body: JSON.stringify({ ticker }),
                  });
                  if (res.ok) {
                    await fetchWatchlist();
                  } else {
                    const data = await res.json();
                    setError(data.detail || "Failed to add ticker");
                  }
                } catch {
                  setError("Failed to add ticker");
                }
              }}
              placeholder="Search and add ticker (e.g. AAPL, NVDA)"
              className="max-w-sm"
            />
          </div>

          {/* Ticker list */}
          {watchlist.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {watchlist.map((w) => (
                <div
                  key={w.ticker}
                  className="flex items-center gap-1.5 rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/30 px-3 py-1.5"
                >
                  <span className="text-sm font-mono font-medium text-[#E8E8ED]">
                    {w.ticker}
                  </span>
                  <button
                    onClick={() => removeWatchlistTicker(w.ticker)}
                    className="text-[#55556A] hover:text-[#EF4444] transition-colors"
                    title="Remove"
                  >
                    <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-[#55556A]">No tickers watched yet.</p>
          )}
          <div className="text-xs text-[#55556A] mt-3">
            {watchlist.length} of 50 slots used
          </div>
        </div>
      )}
    </div>
  );
}

function ToggleRow({
  label,
  desc,
  checked,
  disabled,
  onChange,
}: {
  label: string;
  desc?: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between py-1">
      <div>
        <div className="text-sm text-[#E8E8ED]">{label}</div>
        {desc && <div className="text-xs text-[#55556A] mt-0.5">{desc}</div>}
      </div>
      <button
        onClick={() => onChange(!checked)}
        disabled={disabled}
        className={`relative h-5 w-9 rounded-full transition-colors ${
          checked ? "bg-[#3B82F6]" : "bg-[#2A2A3A]"
        } ${disabled ? "opacity-50" : ""}`}
      >
        <span
          className={`absolute top-0.5 left-0.5 h-4 w-4 rounded-full bg-white transition-transform ${
            checked ? "translate-x-4" : ""
          }`}
        />
      </button>
    </div>
  );
}
