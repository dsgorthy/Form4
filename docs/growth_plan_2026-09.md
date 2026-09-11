# Growth plan — SEO, gating, ads

Written 2026-09-10. Supersedes nothing; there was no prior written plan.

All numbers below are measured, not estimated. Sources: PostHog project 377137
(HogQL over `events`), Clerk `/v1/users`, Stripe `/v1/subscriptions` and
`/v1/events`, and the `form4` database on Studio.

---

## What is actually true today

### Traffic — real, tiny, and compounding for the first time

| metric (last 30d) | value |
|---|---|
| pageviews | 583 |
| unique people | 331 |
| Google organic | 151 views / 143 visitors |
| direct | 366 views / 147 people |
| views per Google visitor | **1.06** |
| daily pageviews, Aug | ~15 |
| daily pageviews, Sep | ~30 |

The September step-change is the sitemap fix landing. The sitemap crossed the
50,000-URL protocol cap and Google **rejects** an oversized sitemap rather than
truncating it — last successful index was 2026-04-03, so from spring until
mid-August nothing new was being indexed. It is fixed and live (verified
2026-09-10: valid `<sitemapindex>`, four filing chunks, `lastmod` current).

1.06 views per Google visitor is the signature of a single-page bounce. They
arrive, they read one page, they leave.

### The sitemap budget is spent on the wrong surface

Search-referred pageviews only (Google/Bing/DDG/Yahoo/ChatGPT), last 60 days,
against what we actually publish:

| surface | published URLs | search views | **views per 1,000 URLs** |
|---|---|---|---|
| company | 17,866 | 50 | **2.80** |
| insider | 45,000 | 69 | **1.53** |
| filing | ~80,000 (4 chunks) | 34 | **0.43** |
| other + home | 29 | 16 | — |

Insider and company pages are **71% of search traffic**. Individual filings are
20% — not nothing, but they consume **56% of the published URL budget** at 6.5×
worse yield per URL than a company page.

Companies are nearly fully published already (17,866 of 21,493 distinct
tickers). Insiders are the surface that is both efficient *and* starved — but
by less than first stated. The eligibility rule is not "≥5 filings" across all
212,983 insiders; it is `buy_count >= 2` on `insider_track_records`, which
yields **51,747 eligible against a 45,000 cap — 6,747 excluded**, not 73,475.
That is still where the next URL should go, not into a fifth filing chunk.

This is a priority ordering, not a strict zero-sum trade — crawl budget is not a
fixed pool that reallocates 1:1, and filings do bring a fifth of search traffic.
But the ranking of where to add URLs next is unambiguous.

### The funnel — the bottleneck is visibility, not policy

| SEO landing funnel (30d) | people | rate |
|---|---|---|
| landed on `/insider/*` or `/company/*` | 208 | — |
| clicked anything at all (`$autocapture`) | 27 | 13% |
| **saw the follow CTA** (`follow_cta_shown`) | **18** | **8.7%** |
| signed up | 1 | 0.5% |

**91% of SEO visitors never see the conversion element.** The follow CTA lives
below the track-record tables, near the bottom of a long page, and the audience
it was written for does not scroll that far.

`gate_shown` fired 75 times but from **one person** — a signed-in user hitting
walls repeatedly. Anonymous traffic is essentially not encountering the gate at
all. So *what we gate* is, right now, nearly irrelevant to conversion. *What
they see before they leave* is everything.

Note the one encouraging number: of the 18 people who did see the CTA, 4 signed
up across the site in the same window. The offer converts when it is seen.

### Revenue — zero, and briefly negative

- **0 active Stripe subscriptions.** Both subscriptions ever created are
  `canceled`.
- One refund (2026-08-25) and one **chargeback** (`charge.dispute.created`,
  2026-08-30).
- 9 Clerk users total. 5 created since 2026-08-22, none carrying `tier`
  metadata.

This is downstream of the dead-webhook incident: stripe-python 15.x stopped
making `StripeObject` a dict subclass, every `.get()` in the handler raised, the
webhook returned 500 to every event, and paying customers never received
`stripe_customer_id` — so the billing portal told them "no subscription found"
while Stripe kept charging. They went to their bank instead.

The handler is fixed (`to_dict_recursive()` before any `.get()`), the endpoint
is enabled, and `pending_webhooks=0` across all recent events. But `stripe` is
**unpinned** in `api/requirements.txt`, which is exactly how this happened.

There is no MRR to protect. That is bad news that buys one thing: gating can be
changed aggressively without breaking a single paying customer.

### AdSense — not a decision yet

At 583 pageviews/month, finance-vertical RPM of $10–30 yields **$6–17/month**.
Applying now also means being reviewed against ~143,000 templated URLs with
almost no engagement signal, which is the profile Google's scaled-content policy
targets. A rejection is harder to reverse than a delay.

**Defer until ~50,000 pageviews/month.** At that point it is $500–1,500/month
and a real decision. Ads are a harvest step; there is nothing to harvest yet.

### PostHog — free, by a factor of 370

2,704 events in 30 days against a 1M events/month free tier. This stays free at
every traffic level this plan targets. No action needed.

---

## The plan

Ordered by dependency, not by appeal. Each phase has a checkpoint that can be
read from data we already collect.

### Phase 0 — Stop the bleeding (now)

| # | task | why |
|---|---|---|
| 0.1 | Rotate the Stripe secret key | Leaked into a session transcript 2026-09-10 |
| 0.2 | Pin `stripe==X.Y.Z` in `api/requirements.txt` | Unpinned is how the 15.x break happened; it will recur |
| 0.3 | Chunk `insiders.xml` | Single file at **45,000 of a 50,000 hard cap**, while eligibility has already reached **51,747** — the section could not be made whole without breaking it |
| 0.4 | Finish the scheduler work in flight | 6 jobs sat dead 57h with `exit code = 0`; 3 migrated to Dagster, 3 need calendar schedules |
| 0.5 | Add stall detection to `offbox_watchdog.py` | It reported "all checks passed" through the entire 57h outage |

**Checkpoint 0 — MET 2026-09-10.** Sitemap sections all ≤20,000 (insiders now
51,747 across three chunks, every eligible insider published for the first
time); `stripe` pinned to 15.6.1; **zero `StartInterval` agents remain loaded on
Studio** and all six formerly-dead services verified firing at 17:00 PT. The
off-box watchdog reports all-clear on a run that names each service and its
budget — the same message it was printing while six of them were dead, now
actually earned.

### Phase 1 — Make the landing page convert (weeks 1–3)

The measured problem is that 91% of SEO visitors never see the offer. Fix
visibility before touching policy.

| # | task |
|---|---|
| 1.1 | Surface the follow CTA above the fold on `/insider/[id]` and `/company/[ticker]` |
| 1.2 | Instrument scroll depth so "did not scroll" is measurable rather than inferred |
| 1.3 | Add a `cta_clicked` event — only `follow_cta_shown` exists, so intent is currently unmeasurable |

**Checkpoint 1:** `follow_cta_shown` people ÷ SEO landers goes from **8.7% to
>50%**. That ratio has n≈208/month behind it, so it is readable in 2–3 weeks.
This is the only checkpoint in the plan that current traffic can actually
support.

### Phase 2 — Re-cut the ladder (weeks 3–6, after Checkpoint 1)

Until people see the offer, changing the offer teaches nothing. Once they do:

| # | task |
|---|---|
| 2.1 | Move the paid axis from **depth** to **immediacy** — drop the 90-day history wall, gate on how fast you are told |
| 2.2 | Make signup worth it: unlimited follows, alerts, no ads |
| 2.3 | Redefine Pro now that history is free — grades, track records, alert filters, real-time |

**Why not gate timeframe.** History is the SEO asset. The insider and company
pages rank *because* they are the record of what someone traded. Paywalling that
paywalls the thing the page ranks for: Google sees content, the visitor hits a
wall, engagement falls, and both ranking and future ad RPM go with it. For this
product speed is what is worth money and the archive is what is worth traffic.
This also matches the principle already written into the notification
architecture — the feed is pulled and generous, email is pushed and scarce. This
is that same split one level up.

**Checkpoint 2:** signup rate on SEO landers above 3%, up from 0.5%. Reading
this needs ~1,000 landers/month, so it depends on Phase 3.

### Phase 3 — Grow the surface (parallel, ongoing)

| # | task |
|---|---|
| 3.1 | Raise `INSIDER_CHUNKS` as eligibility grows past 60,000 capacity |
| 3.2 | Track Search Console coverage as the restored sitemap re-indexes |

**Checkpoint 3:** 50,000 pageviews/month. This is the gate on Phase 4.

### Phase 4 — AdSense (blocked on Checkpoint 3)

| # | task |
|---|---|
| 4.1 | Apply once traffic clears Checkpoint 3 |
| 4.2 | Serve ads to anonymous visitors only; suppress for signed-in and paid |

---

## How we validate, given the sample size

**We cannot A/B test signups.** Detecting a 1.2% → 2.4% signup improvement needs
roughly 2,000 people per arm. At 331 people/month that is a year per arm. Any
plan that proposes split-testing conversion here is proposing something the
traffic cannot pay for.

So:

1. **Optimize on the highest-count metric that moves first.** CTA-seen rate
   (n≈208) and scroll depth (n≈331) are readable in weeks. Signups (n=4) are
   not. Every Phase 1 checkpoint is deliberately a leading indicator.
2. **Ship sequentially and read week-over-week**, not split-tested. One change
   at a time so a movement has one candidate explanation.
3. **Treat traffic growth as the enabler of every later test.** Each doubling of
   organic doubles the sample, which is what makes Checkpoint 2 readable at all.
4. **Never read a signup delta as significant at this volume.** Four signups is
   four signups. It is a sign of life, not evidence.

---

## Backlog

Ordered. `[ ]` open, `[~]` in flight, `[x]` done.

**Phase 0**
- [x] 0.1 Rotate Stripe secret key — done; new key verified live, site never dropped
- [x] 0.2 Pin `stripe` in `api/requirements.txt` — pinned to 15.6.1
- [x] 0.3 Chunk `insiders.xml` → `insiders-0..2`; API ceiling off the protocol cap; 5 tests
- [x] 0.4a Dagster assets for `form4-notifications`, `refresh-open-position-prices`, `strategy-intraday` — deployed, firing
- [x] 0.4b `StartCalendarInterval` for `form4-uptime`, `heartbeat-probe`, `freshness-probe` — all firing
- [x] 0.4c Three migrated plists unloaded and archived before the Dagster restart — no double-run
- [x] 0.5 `SERVICE_HEARTBEAT` stall check in `offbox_watchdog.py` — catches all 6 dead services, passes `insider_fetch`
- [ ] 0.6 Convert `offbox_watchdog`'s own `StartInterval=1800` to a calendar schedule *(runs on the Mini, short uptime, so not yet bitten)*

**Phase 1**
- [ ] 1.1 Follow CTA above the fold on `/insider/[id]` and `/company/[ticker]`
- [ ] 1.2 Scroll-depth instrumentation
- [ ] 1.3 `cta_clicked` event
- [ ] 1.4 Read Checkpoint 1 after 2–3 weeks

**Phase 2**
- [ ] 2.1 Drop the 90-day wall; design the immediacy gate
- [ ] 2.2 Signup value: unlimited follows, alerts, no ads
- [ ] 2.3 Redefine Pro; update `/pricing`
- [ ] 2.4 Default `tier` metadata on signup — the 5 newest Clerk users have none

**Phase 3**
- [ ] 3.1 Raise `INSIDER_CHUNKS` as eligibility grows past 60,000
- [ ] 3.2 Search Console coverage tracking

**Phase 4**
- [ ] 4.1 AdSense application *(blocked on Checkpoint 3)*
- [ ] 4.2 Anonymous-only ad serving

---

## Open questions for Derek

1. **Search Console** — is form4.app verified, and what does Coverage say now?
   That is the one input that tells us whether the sitemap fix is being ingested
   rather than merely served.
2. **The chargeback** — was it disputed with Stripe? An unchallenged dispute
   counts against the account's dispute rate.
3. **Phase 2 scope** — dropping the 90-day wall means free users see all
   history. Confirm that is acceptable before 2.1 is built.
