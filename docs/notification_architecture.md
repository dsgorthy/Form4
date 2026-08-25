# Notification architecture

**Status: implemented 2026-08-25, except where marked "not built".**
`api/notification_policy.py` is the single definition of tiers and caps.
`tests/unit/test_notification_policy.py` fails the build on drift.

---

## The one idea

**The in-app feed and the inbox are different products and need different
policies.**

| | in-app feed | email |
|---|---|---|
| direction | pulled — the user opens it | pushed — it interrupts |
| cost per item | ~zero | attention, paid whether or not it was worth it |
| completeness | a feature; history is the point | irrelevant |
| right volume | generous | scarce |
| failure mode | a cluttered list | unsubscribes, spam folder, lost trust |

Everything below follows from that table.

---

## Three layers

```
   EVENT                     NOTIFICATION                  DELIVERY
   something happened   ->   a user-scoped fact       ->   a channel attempt
   in the world              (an event matched a           (email, later push)
   user-independent          subscription)
                             = the IN-APP FEED             = the INBOX
   a Form 4 lands            generous, keep it all         scarce, must earn it
```

The defect this replaces: **layers 2 and 3 were one row and one boolean**
(`notifications.emailed`). Consequences, all observed:

- Protecting the inbox meant throttling *creation* — `DAILY_CAP = 50` capped
  the feed to limit email. Wrong layer.
- Unblocking email meant unblocking **five months of backlog at once**: 6,890
  unsent notifications, oldest from March.
- There was nowhere to record *when* something was delivered, so a
  per-user-per-day send cap could not be expressed at all.

---

## Tiers, set from measured attention

All 6,920 notifications ever created, as of 2026-08-24:

| event_type | rows | % ever read | tier |
|---|---|---|---|
| `high_value_filing` | 68 | **57.4%** | DIGEST |
| `cluster_formation` | 293 | 24.9% | DIGEST |
| `activity_spike` | 6,378 (92% of all) | 12.8% | **FEED_ONLY** |
| `congress_convergence` | 175 | 10.3% | **FEED_ONLY** |
| `portfolio_alert` | 4 | — | DIRECT |
| `watchlist_activity` | 2 | — | DIRECT |

**Read rate is inversely proportional to volume.** The type sent about once a
day is read 57% of the time; the type sent eighty times a day is read 12.8%.
That is not fixable with better subject lines — attention is the scarce
resource and volume is what consumes it.

- **DIRECT** — email promptly, on its own. `portfolio_alert` (a published
  strategy entered or exited a position) and `watchlist_activity` (a filing on
  a ticker or insider this user personally chose). These are the product's
  actual promise and they are rare.
- **DIGEST** — batched into one daily email.
- **FEED_ONLY** — never emails under any preference. Between them
  `activity_spike` and `congress_convergence` are 95% of all volume and about
  12% of all reads. They remain in the feed, where scrolling past them is free.

`portfolio_alert` and `watchlist_activity` have almost no history (4 and 2
rows) so their read rates say nothing; they are tiered on what they *are*.
That `portfolio_alert` has four rows ever, none since March, is a separate
finding: **the three live strategies alert through a different path and never
reach this table.**

**Unknown event types default to FEED_ONLY.** A new detector is noisy before
it is tuned, and this whole incident began with one nobody had measured.

---

## Four gates on delivery

Applied in order; each removes something the others do not.

1. **Not already sent or expired** — `emailed = PENDING`.
2. **The type has earned an email** — `event_type IN emailable_types()`.
3. **Not already read in the app** — `is_read = 0`. *Email exists for the
   absent user.* If they saw it in the feed an hour ago, the notification did
   its job; mailing it anyway is the purest form of the spam this prevents.
4. **Under the daily send cap** — `MAX_EMAILS_PER_USER_PER_DAY = 4`, enforced
   at the delivery layer, counted by `COUNT(DISTINCT emailed_at)` so one
   digest of forty items is one email.

**The cap applies to the realtime path too**, which matters more than the
digest: DIRECT includes `watchlist_activity`, Pro follows became unlimited on
2026-08-24, and there are ~308 meaningful filings a day across ~68 tickers. A
heavy follower on `realtime` would otherwise receive dozens of separate
emails. Over the ceiling the notification still lands in the feed and stays
`PENDING`; only the email is withheld. Realtime sends stamp `emailed_at`, or
the ceiling could only ever see digests and would not bind on the one path
that can actually flood someone. The check sits inside the delivery guard and
fails open — if the count cannot be read, the cost of guessing wrong is one
extra email and the cost of the other guess is silencing a paying subscriber.

---

## The queue cannot accumulate

`EMAIL_TTL_DAYS = 3`. Anything older is marked `EMAIL_EXPIRED` — a value
distinct from `EMAIL_SENT`, because "we chose not to email this" and "we
emailed this" are different facts worth being able to audit apart.

This is the property that makes the system safe to leave broken. **However
long delivery is down, the queue holds at most three days of sendable
material.** There is no state in which turning email back on produces a flood,
and therefore no decision to make about a backlog. When this shipped it
expired 6,874 of the 6,890 stranded rows without anyone having to choose.

**Backpressure is loud.** More than `BACKPRESSURE_THRESHOLD = 200` pending
inside the TTL window is logged at ERROR: no digest size fixes it, it means
filters are wrong or a detector is misfiring. Its absence is how one user
accumulated 5,114 unread `activity_spike` rows unremarked.

---

## Relevance at write time

The cheapest notification is the one never created. `activity_spike` fired
**159 times a day on average, peaking at 282**. Three defects:

- gated on `is_routine`, a barely-populated boolean, instead of `signal_class`
- no absolute floor, so a near-zero baseline produced enormous ratios from
  nothing — a live example read *"ACHR sell at 989.7x baseline"*, which is a
  small number divided by a smaller one
- a single insider was enough

Now: `signal_class IN MEANINGFUL_CLASSES`, `>= $1M` in the window, `>= 2`
insiders. Measured over eight sample days: **159.6/day → 20.2** (peak 37). It
also honours the subscriber's own `min_trade_value`, which this path ignored
entirely — only `high_value_filing` ever read it.

Scanner order matters and is now pinned by test: `DAILY_CAP` is shared across
event types, so whichever runs first spends the budget. `activity_spike` runs
last.

---

## Deliverability

Every digest carries a visible unsubscribe link plus RFC 8058 one-click
headers:

```
List-Unsubscribe: <https://form4.app/api/v1/notifications/unsubscribe?...>
List-Unsubscribe-Post: List-Unsubscribe=One-Click
```

Gmail and Yahoo have required this of bulk senders since February 2024;
without it mail is throttled or spam-foldered regardless of content — for an
alerts product, the same outcome as the outage, by another route. The endpoint
accepts POST and acts without a confirmation step, as the header promises, and
is idempotent.

---

## Not built

Ranked by what I would do next.

1. **A `deliveries` table.** `emailed` + `emailed_at` on the notification row
   is a two-thirds measure: it cannot express a failed attempt, a retry, a
   second channel, or which notifications a given email actually covered. The
   full form is an outbox — one row per (user, channel, attempt) with a
   deterministic idempotency key and a state machine
   (`pending → sent | failed | suppressed`), with the reason for suppression
   recorded. That makes "why did this person not get an email" answerable,
   which today it is not.
2. **Bounce and complaint handling.** Resend posts webhooks for both. A hard
   bounce or a spam complaint must disable the address immediately; continuing
   to send to it damages the sending domain for everyone else.
3. **Collapsing by type in the digest.** Currently top-12 by recency plus
   "+N more". Grouping — *"6 cluster formations, 2 high-value filings"* — reads
   better and scales further.
4. **Per-category frequency.** Today `email_frequency` is one global setting.
   The natural next step is per-tier: direct for portfolio alerts, daily for
   the rest.
5. **Quiet hours and timezone.** The digest fires 07:00 PT for everyone,
   which is fine for a US-market product and wrong for anyone else.
6. **Why `portfolio_alert` is empty.** Four rows ever, none since March, while
   three strategies alert live. The core product's own alerts are not in the
   notification system.

---

## Scheduling

Dagster, not launchd. `form4_notification_digest` asset →
`form4_alerts` job → `form4_alerts_daily` schedule, 07:00 PT.
`com.openclaw.insideredge-digest.plist` was deleted: it existed in the repo,
passed `--digest`, and had never been installed on Studio, which is the direct
reason no digest email had ever been sent.

Picking up new Dagster definitions requires restarting `dagster-daemon` and
`dagster-webserver` on Studio — a production restart, so it needs approval.
Until that happens the schedule is committed and validated but not running.
