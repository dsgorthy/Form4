"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import type { Notification } from "@/lib/types";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

const EVENT_LABELS: Record<string, string> = {
  portfolio_alert: "Portfolio",
  high_value_filing: "Filing",
  cluster_formation: "Cluster",
  activity_spike: "Spike",
  congress_convergence: "Convergence",
  watchlist_activity: "Watchlist",
};

const EVENT_COLORS: Record<string, string> = {
  portfolio_alert: "text-[#22C55E]",
  high_value_filing: "text-[#3B82F6]",
  cluster_formation: "text-[#F59E0B]",
  activity_spike: "text-[#EF4444]",
  congress_convergence: "text-[#A855F7]",
  watchlist_activity: "text-[#06B6D4]",
};

export function NotificationBell() {
  const { getToken } = useAuth();
  const [count, setCount] = useState(0);
  const [open, setOpen] = useState(false);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [loading, setLoading] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const fetchCount = useCallback(async () => {
    try {
      const token = await getToken();
      if (!token) return;
      const res = await fetch(`${apiBase}/notifications/unread-count`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        const data = await res.json();
        setCount(data.count);
      }
    } catch {
      // silent
    }
  }, [getToken]);

  const fetchNotifications = useCallback(async () => {
    setLoading(true);
    try {
      const token = await getToken();
      if (!token) return;
      const res = await fetch(`${apiBase}/notifications?limit=10`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        const data = await res.json();
        setNotifications(data.items);
      }
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }, [getToken]);

  // Poll unread count every 60s
  useEffect(() => {
    fetchCount();
    const interval = setInterval(fetchCount, 60_000);
    return () => clearInterval(interval);
  }, [fetchCount]);

  // Fetch notifications when dropdown opens
  useEffect(() => {
    if (open) fetchNotifications();
  }, [open, fetchNotifications]);

  // Close dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    if (open) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  async function markRead(id: string) {
    const token = await getToken();
    if (!token) return;
    await fetch(`${apiBase}/notifications/${id}/read`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
    });
    setNotifications((prev) =>
      prev.map((n) => (n.id === id ? { ...n, is_read: 1 } : n))
    );
    setCount((c) => Math.max(0, c - 1));
  }

  async function markAllRead() {
    const token = await getToken();
    if (!token) return;
    await fetch(`${apiBase}/notifications/read-all`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
    });
    setNotifications((prev) => prev.map((n) => ({ ...n, is_read: 1 })));
    setCount(0);
  }

  function timeAgo(dateStr: string): string {
    const now = Date.now();
    const then = new Date(dateStr + "Z").getTime();
    const mins = Math.floor((now - then) / 60000);
    if (mins < 1) return "now";
    if (mins < 60) return `${mins}m`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h`;
    const days = Math.floor(hours / 24);
    return `${days}d`;
  }

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setOpen(!open)}
        className="relative rounded-md p-1.5 text-[#55556A] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 transition-colors"
        title="Notifications"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        {count > 0 && (
          <span className="absolute -top-0.5 -right-0.5 flex h-4 min-w-[16px] items-center justify-center rounded-full bg-[#EF4444] px-1 text-[10px] font-bold text-white">
            {count > 99 ? "99+" : count}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-2 w-80 rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-xl z-50">
          {/* Header */}
          <div className="flex items-center justify-between border-b border-[#2A2A3A] px-4 py-3">
            <span className="text-sm font-semibold text-[#E8E8ED]">Notifications</span>
            {count > 0 && (
              <button
                onClick={markAllRead}
                className="text-xs text-[#3B82F6] hover:text-[#60A5FA] transition-colors"
              >
                Mark all read
              </button>
            )}
          </div>

          {/* Body */}
          <div className="max-h-96 overflow-y-auto">
            {loading && notifications.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm text-[#55556A]">Loading...</div>
            ) : notifications.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm text-[#55556A]">No notifications yet</div>
            ) : (
              notifications.map((n) => (
                <button
                  key={n.id}
                  onClick={() => {
                    if (!n.is_read) markRead(n.id);
                    if (n.ticker) {
                      window.location.href = `/company/${n.ticker}`;
                      setOpen(false);
                    }
                  }}
                  className={`w-full text-left px-4 py-3 border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/50 transition-colors ${
                    !n.is_read ? "bg-[#1A1A26]/30" : ""
                  }`}
                >
                  <div className="flex items-start gap-2">
                    {!n.is_read && (
                      <span className="mt-1.5 h-2 w-2 shrink-0 rounded-full bg-[#3B82F6]" />
                    )}
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className={`text-[10px] font-semibold uppercase ${EVENT_COLORS[n.event_type] || "text-[#8888A0]"}`}>
                          {EVENT_LABELS[n.event_type] || n.event_type}
                        </span>
                        <span className="text-[10px] text-[#55556A]">{timeAgo(n.created_at)}</span>
                      </div>
                      <div className="text-sm text-[#E8E8ED] mt-0.5 truncate">{n.title}</div>
                      <div className="text-xs text-[#8888A0] mt-0.5 line-clamp-2">{n.body}</div>
                    </div>
                  </div>
                </button>
              ))
            )}
          </div>

          {/* Footer */}
          <div className="border-t border-[#2A2A3A] px-4 py-2">
            <Link
              href="/settings"
              className="text-xs text-[#55556A] hover:text-[#8888A0] transition-colors"
              onClick={() => setOpen(false)}
            >
              Notification settings
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}
