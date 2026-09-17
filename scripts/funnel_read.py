#!/usr/bin/env python3
"""The signup funnel, read from PostHog, Clerk and Stripe in one go.

    python3 scripts/funnel_read.py                  # last 30 days, CTA series since 2026-09-11
    python3 scripts/funnel_read.py --since 2026-09-17 --days 14

Keys come from the repo .env (POSTHOG_PERSONAL_API_KEY, POSTHOG_PROJECT_ID,
CLERK_SECRET_KEY, STRIPE_SECRET_KEY) and never reach a shell command line --
a live Stripe key printed into a transcript on 2026-09-10 and had to be
rotated. PostHog personal keys are project-scoped, so the numeric project id
is used; the API host is us.posthog.com, not the ingestion host.

What to read, and what it means (docs/growth_plan_2026-09.md):
  landers -> follow_cta_viewed -> follow_cta_clicked -> follow_completed
  is the leading indicator (n ~ 130/week is readable in two weeks);
  signups are not (n ~ 5/month).
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def hogql(env: dict, query: str):
    req = urllib.request.Request(
        f"https://us.posthog.com/api/projects/{env.get('POSTHOG_PROJECT_ID', '377137')}/query/",
        data=json.dumps({"query": {"kind": "HogQLQuery", "query": query}}).encode(),
        headers={"Authorization": f"Bearer {env['POSTHOG_PERSONAL_API_KEY']}",
                 "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        d = json.load(r)
    return d.get("columns", []), d.get("results", [])


def show(title: str, cols, rows) -> None:
    print(f"\n## {title}")
    print(" | ".join(str(c) for c in cols))
    for row in rows:
        print(" | ".join(str(x) for x in row))


SEO = "(properties.$pathname like '/insider/%' or properties.$pathname like '/company/%')"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-09-11", help="start of the CTA series (the render fix shipped 2026-09-11)")
    ap.add_argument("--days", type=int, default=30, help="window for the traffic tables")
    args = ap.parse_args()
    env = load_env()
    since = f"toDateTime('{args.since} 00:00:00')"
    d = args.days

    show(f"Traffic, last {d} days", *hogql(env, f"""
        select countIf(event = '$pageview') as pageviews,
               uniqIf(person_id, event = '$pageview') as people,
               uniqIf(person_id, event = '$pageview' and properties.$referring_domain like '%google%') as from_google,
               uniqIf(person_id, event = '$pageview' and {SEO}) as seo_landers
        from events where timestamp > now() - interval {d} day"""))

    show("Weekly: people, SEO landers, CTA viewed / clicked / completed, signups", *hogql(env, f"""
        select toStartOfWeek(timestamp) as wk,
               uniqIf(person_id, event = '$pageview') as people,
               uniqIf(person_id, event = '$pageview' and {SEO}) as seo_landers,
               uniqIf(person_id, event = 'follow_cta_viewed') as cta_viewed,
               uniqIf(person_id, event = 'follow_cta_clicked') as cta_clicked,
               uniqIf(person_id, event = 'follow_completed') as follow_completed,
               uniqIf(person_id, event = 'signed_up') as signups
        from events where timestamp > now() - interval {d + 35} day
        group by wk order by wk"""))

    show(f"CTA by placement since {args.since} (distinct people)", *hogql(env, f"""
        select properties.placement as placement, event, uniq(person_id) as people
        from events where event in ('follow_cta_shown', 'follow_cta_viewed', 'follow_cta_clicked')
          and timestamp > {since}
        group by placement, event order by placement, event"""))

    show(f"SEO landers since {args.since}: scroll depth reached (distinct people)", *hogql(env, f"""
        select properties.depth as depth, uniq(person_id) as people
        from events where event = 'scroll_depth' and timestamp > {since} and {SEO}
        group by depth order by depth"""))

    show(f"Pages per SEO visitor, last {d} days", *hogql(env, f"""
        select multiIf(pv = 1, '1', pv = 2, '2', pv <= 5, '3-5', '6+') as pages, count() as people from (
          select person_id, count() as pv from events where event = '$pageview' and timestamp > now() - interval {d} day
          and person_id in (select distinct person_id from events where event = '$pageview' and timestamp > now() - interval {d} day and {SEO})
          group by person_id) group by pages order by pages"""))

    # Clerk: who signed up, and did anything follow
    req = urllib.request.Request("https://api.clerk.com/v1/users?limit=100&order_by=-created_at",
                                 headers={"Authorization": f"Bearer {env['CLERK_SECRET_KEY']}",
                                          "User-Agent": "form4-ops/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        users = json.load(r)
    print(f"\n## Clerk accounts: {len(users)}")
    print("created | last sign-in | tier | onboarding | strategy")
    for u in users:
        c = datetime.fromtimestamp(u["created_at"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        l = u.get("last_sign_in_at")
        l = datetime.fromtimestamp(l / 1000, tz=timezone.utc).strftime("%Y-%m-%d") if l else "-"
        pm = u.get("public_metadata") or {}
        um = u.get("unsafe_metadata") or {}
        print(f"{c} | {l} | {pm.get('tier', 'free')} | "
              f"{'done' if um.get('onboardingComplete') else '-'} | {um.get('defaultStrategy', '-')}")

    # Stripe: subscriptions and recent checkouts
    def stripe_get(path, params=None):
        url = f"https://api.stripe.com/v1/{path}" + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {env['STRIPE_SECRET_KEY']}"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    subs = stripe_get("subscriptions", {"status": "all", "limit": 50})["data"]
    print(f"\n## Stripe subscriptions: {len(subs)}")
    for s in subs:
        p = s["items"]["data"][0]["price"]
        print(f"{datetime.fromtimestamp(s['created'], tz=timezone.utc):%Y-%m-%d} | {s['status']} | "
              f"{(p.get('unit_amount') or 0) / 100:.0f} {p.get('currency')}/{(p.get('recurring') or {}).get('interval')}")
    since_ts = int((datetime.now(timezone.utc) - timedelta(days=d)).timestamp())
    cs = stripe_get("checkout/sessions", {"limit": 50, "created[gte]": since_ts})["data"]
    print(f"\n## Stripe checkout sessions, last {d} days: {len(cs)}")
    for s in cs:
        print(f"{datetime.fromtimestamp(s['created'], tz=timezone.utc):%Y-%m-%d %H:%M} | {s['status']} | "
              f"payment={s.get('payment_status')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
