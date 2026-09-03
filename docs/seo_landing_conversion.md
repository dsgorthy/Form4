# Converting SEO landings: the promise isn't kept

Written 2026-09-03, from a session that landed on `/insider/erez-chimovits`.

## Entity pages are the traffic

90 days of PostHog, by surface:

| surface | views | **visitors** |
|---|---|---|
| filing page | 162 | **57** |
| home | 123 | 69 |
| insider page | 62 | **25** |
| company page | 58 | **24** |
| /portfolio | 158 | 7 |
| **/pricing** | **5** | **2** |

Entity pages together draw **106 visitors against the homepage's 69**. And 2 of
179 visitors (1.1%) ever reached `/pricing`, which is the only page where
`checkout_started` can fire. Pushing checkout on these surfaces is not a weak
strategy, it is aimed at a page almost nobody sees.

## The strategy is already right

`FollowCta` renders on the insider page and asks anonymous visitors the cheap
thing — *"Get alerted the next time Erez Chimovits files"* → `/sign-up`, not
`/pricing`. Watchlist and alert routes were already moved from `require_pro` to
`require_auth`, so a free account keeps them, and sign-up starts a 7-day trial
with no card. The component's own docstring argues the case.

So this is not a strategy change. **It is a broken promise.**

## The gap: intent does not survive sign-up

```tsx
const href = isSignedIn ? "/pricing" : "/sign-up";
```

A bare path. No redirect, no entity, no pending action. The actual journey:

1. Visitor lands on `/insider/erez-chimovits` from Google
2. Reads *"Get alerted the next time Erez Chimovits files"*
3. Clicks — arrives at `/sign-up`
4. Creates an account
5. `onboarding-guard` pushes them to `/onboarding`
6. **Erez Chimovits is gone.** No follow exists. They are in a generic
   onboarding form with no memory of why they came.

They asked for one specific thing and received a form. Whatever the sign-up
rate is, the retention value of that account is near zero, because the reason
they created it was never acted on.

This is precisely what Zillow's saved-search flow gets right: **the alert IS the
account.** Name the search, give an email, choose a frequency — the account is
a side effect of setting up the alert, not a separate step that discards it.

## What to change

1. **Carry the intent.** `/sign-up?follow=insider:erez-chimovits&next=/insider/erez-chimovits`.
2. **Act on it immediately after sign-up**, before onboarding: create the
   watchlist entry, then return the visitor to the page they came from with
   *"You'll be emailed the next time Erez Chimovits files"* already true.
3. **Let onboarding come second, or not at all.** A generic questionnaire
   standing between a visitor and the thing they just asked for is the most
   expensive interruption on the site. `onboarding-guard` should skip when a
   pending follow exists.
4. **Instrument it.** `follow-cta` has zero PostHog calls today, so none of
   this is currently measurable. `gate_shown` / `gate_cta_clicked` shipped for
   `pro-gate`; `follow_cta_shown` / `follow_cta_clicked` / `follow_completed`
   are the equivalent here, and `follow_completed` is the one that says whether
   the promise was kept.
5. **Stop asking signed-in free accounts for money on entity pages.** Today
   `isSignedIn` sends them to `/pricing`. They should be asked to follow more
   entities. Pitch Pro on what Pro buys, once there is a relationship and an
   email address.

## On CeoWatcher's model: a curated daily email

Right for them, wrong as our *first* ask, and worth having as a second product.

**Why it does not fit the entity page.** A visitor who searched a person's name
and landed on that person's page has a *specific* interest. Offering a general
digest answers a question they did not ask, and converts a precise promise
("tell me when THIS person files") into a vague one ("we'll send you things").
The specific ask is both easier to say yes to and far easier to keep.

**Where a digest does fit.** The homepage draws 69 visitors with the *lowest*
engagement on the site (1.6 views per visitor against 10.0 for `/explore`).
Those people arrived without a specific interest, and a daily digest is exactly
the right ask for them. Same for `/feed`.

So: **entity pages ask for a specific follow, the homepage offers the digest.**
They are different visitors with different intents and should not get the same
CTA.

**One caution.** A digest is a real product with a real cost — the notification
architecture doc is explicit that email is pushed and scarce, that read rate is
inversely proportional to volume, and that `activity_spike` is 92% of
everything created and 12.8% of what gets opened. A daily email is a promise to
have something worth reading every day. A follow-alert only fires when the
thing the reader asked about actually happens, which is why it is the cheaper
promise to keep as well as the cheaper one to make.

## Sequencing

The order matters and it is not the obvious one:

1. **Instrument first.** Without `follow_cta_shown` we cannot tell whether the
   CTA is unseen or unconvincing, and those need opposite fixes.
2. **Then keep the promise** — intent through sign-up, follow created, return
   to the page. This is the fix with the largest expected effect and it is
   invisible in the funnel until step 1 exists.
3. **Then reconsider onboarding**, which currently interrupts the moment of
   highest intent.
4. **Then the digest**, on the homepage, as a separate ask to a different
   visitor.

Pro is pitched after all of that, by email, to someone who already receives
something useful from us.
