import posthog from "posthog-js";

export function initPostHog() {
  if (typeof window === "undefined") return;
  if (!process.env.NEXT_PUBLIC_POSTHOG_KEY) return;

  posthog.init(process.env.NEXT_PUBLIC_POSTHOG_KEY, {
    api_host: process.env.NEXT_PUBLIC_POSTHOG_HOST || "https://us.i.posthog.com",
    person_profiles: "always",
    capture_pageview: true,
    capture_pageleave: true,
    autocapture: true,
    session_recording: {
      // Mask every input. This was false, with only passwords masked, so
      // session replays captured whatever anyone typed on an authenticated
      // financial product — search terms, watchlist entries, email addresses
      // in settings. Derek's call, 2026-08-19: mask by default.
      //
      // What replay still shows: clicks, scrolls, navigation and page state,
      // which is what it is actually used for. Keystrokes were never the
      // point.
      maskAllInputs: true,
    },
  });
}

export { posthog };
