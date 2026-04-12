"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { UserButton, useUser, useAuth } from "@clerk/nextjs";
import { SearchBar } from "@/components/search-bar";
import { isPro, getUserTier, getTrialDaysLeft } from "@/lib/subscription";
import { ProBadge } from "@/components/pro-badge";
import { NotificationBell } from "@/components/notification-bell";

function SettingsIcon() {
  return (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
    </svg>
  );
}

function ResearchDropdown({ pathname }: { pathname: string }) {
  const [open, setOpen] = useState(false);
  const isActive = researchLinks.some((l) => pathname === l.href);

  return (
    <div className="relative" onMouseEnter={() => setOpen(true)} onMouseLeave={() => setOpen(false)}>
      <button
        className={`flex items-center gap-1 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
          isActive
            ? "bg-[#1A1A26] text-[#E8E8ED]"
            : "text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
        }`}
      >
        Research
        <svg className={`w-3.5 h-3.5 transition-transform ${open ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="absolute left-0 top-full pt-1 z-50">
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] py-1 shadow-xl min-w-[160px]">
            {researchLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                onClick={() => setOpen(false)}
                className={`block px-4 py-2 text-sm transition-colors ${
                  pathname === link.href
                    ? "text-[#E8E8ED] bg-[#1A1A26]"
                    : "text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
                }`}
              >
                {link.label}
              </Link>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

const primaryLinks = [
  { href: "/portfolio", label: "Portfolio" },
  { href: "/feed", label: "Today's Trades" },
];

const researchLinks = [
  { href: "/screener", label: "Screener" },
  { href: "/leaderboard", label: "Leaderboard" },
  { href: "/clusters", label: "Clusters" },
  { href: "/congress", label: "Congress" },
  { href: "/signals", label: "Signals" },
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
          {primaryLinks.map((link) => {
            const isActive = pathname === link.href || (link.href === "/feed" && pathname === "/feed");
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
          <ResearchDropdown pathname={pathname} />
        </div>

        {/* Right side */}
        <div className="ml-auto flex items-center gap-3">
          <div className="hidden md:block">
            <SearchBar />
          </div>
          {isLoaded && isSignedIn && (
            <>
              {userIsPro && <NotificationBell />}
              {(userTier === "pro" || userTier === "pro_plus") ? (
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
              <span className="hidden md:inline-flex">
                <UserButton
                  appearance={{
                    elements: {
                      avatarBox: "w-8 h-8",
                    },
                  }}
                >
                  <UserButton.MenuItems>
                    <UserButton.Link
                      label="Settings"
                      labelIcon={<SettingsIcon />}
                      href="/settings"
                    />
                  </UserButton.MenuItems>
                </UserButton>
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

          {/* Primary links */}
          {primaryLinks.map((link) => {
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

          {/* Research section */}
          <div className="pt-2 mt-1 border-t border-[#2A2A3A]">
            <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">Research</div>
            {researchLinks.map((link) => {
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
          </div>

          {/* Account section */}
          {isLoaded && isSignedIn && (
            <div className="border-t border-[#2A2A3A] pt-2 mt-2 flex items-center gap-3">
              <UserButton
                appearance={{
                  elements: {
                    avatarBox: "w-8 h-8",
                  },
                }}
              >
                <UserButton.MenuItems>
                  <UserButton.Link
                    label="Settings"
                    labelIcon={<SettingsIcon />}
                    href="/settings"
                  />
                </UserButton.MenuItems>
              </UserButton>
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
