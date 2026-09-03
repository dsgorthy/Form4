"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { posthog } from "@/lib/posthog";

/**
 * Completes a follow that was requested before the visitor had an account.
 *
 * WHY THIS EXISTS
 *
 * FollowCta promised anonymous visitors "get alerted the next time Erez
 * Chimovits files" and linked to a bare `/sign-up`. No redirect, no entity, no
 * pending action — so the visitor created an account, onboarding-guard pushed
 * them to a questionnaire, and the insider was discarded. They asked for one
 * specific thing and received a form.
 *
 * Whatever the sign-up rate was, the retention value of those accounts was near
 * zero: the reason each was created was never acted on.
 *
 * Zillow's saved search is the model. The alert IS the account — name it, give
 * an email, choose a frequency, and the account falls out of setting up the
 * alert rather than standing in front of it.
 *
 * HOW
 *
 * FollowCta appends `?follow=insider:1234` (and `&next=` for the anonymous
 * path). This component watches for that parameter, and the moment Clerk
 * reports a signed-in user it performs the follow and strips the parameter.
 * It renders a single confirmation line and nothing else.
 *
 * Deliberately idempotent-ish: `done` guards against React double-invoking the
 * effect in development, and the API treats a duplicate follow as success.
 */
export function PendingFollow() {
  const { isSignedIn, isLoaded, getToken } = useAuth();
  const params = useSearchParams();
  const pathname = usePathname();
  const router = useRouter();
  const done = useRef(false);
  const [msg, setMsg] = useState<string | null>(null);

  const token = params?.get("follow") ?? null;

  useEffect(() => {
    if (!isLoaded || !isSignedIn || !token || done.current) return;
    done.current = true;

    const [kind, ...rest] = token.split(":");
    const id = rest.join(":");
    if (!id || (kind !== "insider" && kind !== "ticker")) return;

    const apiBase =
      process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

    (async () => {
      try {
        const jwt = await getToken();
        const res = await fetch(
          `${apiBase}/notifications/watchlist${kind === "insider" ? "/insiders" : ""}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              ...(jwt ? { Authorization: `Bearer ${jwt}` } : {}),
            },
            body: JSON.stringify(
              kind === "insider" ? { insider_id: Number(id) } : { ticker: id },
            ),
          },
        );
        // 409/duplicate is a success from the reader's point of view: they
        // asked to be told, and they will be told.
        const ok = res.ok || res.status === 409;
        setMsg(
          ok
            ? "You'll be emailed the next time they file."
            : "We couldn't set that alert up — it's on your watchlist page.",
        );
        posthog?.capture?.("follow_completed", {
          follow_target: token,
          kind,
          ok,
          status: res.status,
          path: pathname ?? null,
        });
      } catch {
        setMsg("We couldn't set that alert up — it's on your watchlist page.");
        posthog?.capture?.("follow_completed", {
          follow_target: token,
          kind,
          ok: false,
          status: 0,
          path: pathname ?? null,
        });
      } finally {
        // Strip the parameter so a refresh or a shared link does not re-follow
        // and does not re-show the confirmation.
        const next = new URLSearchParams(params?.toString() ?? "");
        next.delete("follow");
        next.delete("next");
        const qs = next.toString();
        router.replace(qs ? `${pathname}?${qs}` : (pathname ?? "/"), {
          scroll: false,
        });
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, isSignedIn, token]);

  if (!msg) return null;
  return (
    <div className="mt-4 rounded-lg border border-[#22C55E]/30 bg-[#22C55E]/10 px-4 py-3 text-sm text-[#22C55E]">
      {msg}
    </div>
  );
}
