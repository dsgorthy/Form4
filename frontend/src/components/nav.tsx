"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { UserButton, useUser, useAuth } from "@clerk/nextjs";
import { SearchBar } from "@/components/search-bar";
import { isPro, getUserTier, getTrialDaysLeft } from "@/lib/subscription";
import { ProBadge } from "@/components/pro-badge";
import { NotificationBell } from "@/components/notification-bell";

const links = [
  { href: "/", label: "Dashboard" },
  { href: "/feed", label: "Feed" },
  { href: "/portfolio", label: "Portfolio" },
  { href: "/clusters", label: "Clusters" },
  { href: "/screener", label: "Screener" },
  { href: "/leaderboard", label: "Leaderboard" },
  { href: "/congress", label: "Congress" },
];

export function Nav() {
  const pathname = usePathname();
  const { isSignedIn, isLoaded } = useAuth();
  const { user } = useUser();
  const [mobileOpen, setMobileOpen] = useState(false);

  const userIsPro = isPro(user);
  const userTier = getUserTier(user);
  const trialDaysLeft = getTrialDaysLeft(user);

  return (
    <nav className="sticky top-0 z-50 w-full border-b border-[#2A2A3A] bg-[#0A0A0F]/95 backdrop-blur supports-[backdrop-filter]:bg-[#0A0A0F]/80">
      <div className="mx-auto flex h-14 max-w-7xl items-center px-4 md:px-6">
        {/* Logo */}
        <Link href="/" className="mr-4 md:mr-8 flex items-center">
          <img
            src={process.env.NEXT_PUBLIC_API_URL?.includes("sandbox") ? "/logo-sandbox.png" : "/logo.png"}
            alt="Form4"
            className="h-7"
          />
        </Link>

        {/* Desktop nav links */}
        <div className="hidden md:flex items-center space-x-1">
          {links.map((link) => {
            const isActive = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-[#1A1A26] text-[#E8E8ED]"
                    : "text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
                }`}
              >
                {link.label}
              </Link>
            );
          })}
        </div>

        {/* Right side */}
        <div className="ml-auto flex items-center gap-3">
          <div className="hidden md:block">
            <SearchBar />
          </div>
          {isLoaded && isSignedIn && (
            <>
              {userIsPro && <NotificationBell />}
              {userTier === "pro" ? (
                <span className="hidden md:inline-flex"><ProBadge /></span>
              ) : userTier === "trial" ? (
                <Link
                  href="/pricing"
                  className="hidden md:inline-flex items-center gap-1.5 rounded-md border border-[#3B82F6]/30 bg-[#3B82F6]/10 px-2.5 py-1 text-xs font-medium text-[#3B82F6] hover:bg-[#3B82F6]/20 transition-colors"
                >
                  <span className="w-1.5 h-1.5 rounded-full bg-[#3B82F6] animate-pulse" />
                  Trial · {trialDaysLeft}d left
                </Link>
              ) : (
                <Link
                  href="/pricing"
                  className="hidden md:inline-flex rounded-md border border-[#3B82F6]/30 bg-[#3B82F6]/10 px-2.5 py-1 text-xs font-medium text-[#3B82F6] hover:bg-[#3B82F6]/20 transition-colors"
                >
                  Upgrade
                </Link>
              )}
              <Link
                href="/settings"
                className="hidden md:inline-flex rounded-md p-1.5 text-[#55556A] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 transition-colors"
                title="Settings"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                </svg>
              </Link>
              <span className="hidden md:inline-flex">
                <UserButton
                  appearance={{
                    elements: {
                      avatarBox: "w-8 h-8",
                    },
                  }}
                />
              </span>
            </>
          )}
          {isLoaded && !isSignedIn && (
            <>
              <Link
                href="/sign-in"
                className="hidden md:inline-flex rounded-md px-3 py-1.5 text-sm font-medium text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
              >
                Sign In
              </Link>
              <Link
                href="/sign-up"
                className="hidden md:inline-flex rounded-md bg-[#3B82F6] px-3 py-1.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
              >
                Start Free Trial
              </Link>
            </>
          )}

          {/* Hamburger button — mobile only */}
          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="md:hidden rounded-md p-1.5 text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 transition-colors"
            aria-label="Toggle menu"
          >
            {mobileOpen ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            )}
          </button>
        </div>
      </div>

      {/* Mobile menu panel */}
      {mobileOpen && (
        <div className="md:hidden border-t border-[#2A2A3A] bg-[#0A0A0F] px-4 pb-4 pt-2 space-y-1">
          {/* Search */}
          <div className="pb-2">
            <SearchBar />
          </div>

          {/* Nav links */}
          {links.map((link) => {
            const isActive = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                onClick={() => setMobileOpen(false)}
                className={`block rounded-md px-3 py-2 text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-[#1A1A26] text-[#E8E8ED]"
                    : "text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
                }`}
              >
                {link.label}
              </Link>
            );
          })}

          {/* Account section */}
          {isLoaded && isSignedIn && (
            <div className="border-t border-[#2A2A3A] pt-2 mt-2 flex items-center gap-3">
              <UserButton
                appearance={{
                  elements: {
                    avatarBox: "w-8 h-8",
                  },
                }}
              />
              {userIsPro ? (
                <ProBadge />
              ) : (
                <Link
                  href="/pricing"
                  onClick={() => setMobileOpen(false)}
                  className="rounded-md border border-[#3B82F6]/30 bg-[#3B82F6]/10 px-2.5 py-1 text-xs font-medium text-[#3B82F6] hover:bg-[#3B82F6]/20 transition-colors"
                >
                  Upgrade
                </Link>
              )}
              <Link
                href="/settings"
                onClick={() => setMobileOpen(false)}
                className="rounded-md p-1.5 text-[#55556A] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50 transition-colors ml-auto"
                title="Settings"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                </svg>
              </Link>
            </div>
          )}
          {isLoaded && !isSignedIn && (
            <div className="border-t border-[#2A2A3A] pt-2 mt-2 flex items-center gap-2">
              <Link
                href="/sign-in"
                onClick={() => setMobileOpen(false)}
                className="rounded-md px-3 py-1.5 text-sm font-medium text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
              >
                Sign In
              </Link>
              <Link
                href="/sign-up"
                onClick={() => setMobileOpen(false)}
                className="rounded-md bg-[#3B82F6] px-3 py-1.5 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
              >
                Start Free Trial
              </Link>
            </div>
          )}
        </div>
      )}
    </nav>
  );
}
