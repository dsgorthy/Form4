"""Email templates for Form4.app.

The account sequence is 4 emails at days 0, 3, 10 and 30 after signup (see
EMAIL_SEQUENCE, driven by pipelines/trial_emails.py; the day-30 one only to an
account that has gone quiet). It replaced a 6-step trial funnel on 2026-09-17
when accounts stopped being trials.

comp_grant_email is separate: a hand-sent note for when an account is comped
to Pro via scripts/comp_user.py. It is not part of the sequence and is never
sent automatically.

All share a common layout matching Form4's dark brand.
"""
from __future__ import annotations

APP_URL = "https://form4.app"


def _layout(content: str, cta_text: str, cta_url: str, unsubscribe_url: str = "") -> str:
    """Shared email layout: logo header, content block, CTA button, footer."""
    unsub = ""
    if unsubscribe_url:
        unsub = (
            f'<p style="margin-top:24px;font-size:11px;color:#55556A;text-align:center;">'
            f'<a href="{unsubscribe_url}" style="color:#55556A;text-decoration:underline;">Unsubscribe</a>'
            f"</p>"
        )

    return f"""\
<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#07070C;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;">

  <!-- Logo -->
  <div style="margin-bottom:24px;">
    <span style="font-size:22px;font-weight:bold;color:#E8E8ED;">Form<span style="color:#3B82F6;">4</span></span>
  </div>

  <!-- Body -->
  <div style="background:#0F0F17;border:1px solid #1E1E2E;border-radius:10px;padding:28px 24px;">
    {content}
  </div>

  <!-- CTA -->
  <div style="text-align:center;margin-top:24px;">
    <a href="{cta_url}" style="display:inline-block;background:#3B82F6;color:#ffffff;font-size:14px;font-weight:600;padding:12px 28px;border-radius:6px;text-decoration:none;">
      {cta_text}
    </a>
  </div>

  <!-- Footer -->
  <div style="margin-top:32px;padding-top:16px;border-top:1px solid #1E1E2E;text-align:center;">
    <p style="font-size:11px;color:#55556A;margin:0;">
      Form4.app &mdash; Insider trading intelligence, delivered.
    </p>
    {unsub}
  </div>

</div>
</body></html>"""


# ── The account sequence ───────────────────────────────────────────────────
#
# Four emails to a FREE account: what it does (day 0), what the people they
# follow did (day 3), what Pro adds -- once (day 10), and a note if they have
# gone quiet (day 30, only then). Until 2026-09-17 this was a six-step trial
# funnel -- "your trial starts now", "2 days left", "your trial has ended",
# "your grace period has ended" -- sent to people who never chose a trial,
# including everyone the search-landing pages had just promised a free
# account. Accounts are free now (api/auth.py); the sequence says so and
# stops selling after one mention.
#
# Plain sentences. No countdowns, no "what you'll lose", no exclamation marks.

_P = 'style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;"'
_H = 'style="margin:0 0 12px;font-size:18px;color:#E8E8ED;"'
_STRONG = 'style="color:#E8E8ED;"'


def _filing_rows(items: list[dict], empty: str) -> str:
    """One line per filing: ticker, who, bought/sold how much, when."""
    rows = ""
    for f in items[:5]:
        action = "bought" if f.get("trade_type") == "buy" else "sold"
        val = f.get("value") or 0
        val_str = f"${val:,.0f}" if val else ""
        when = f.get("filing_date") or ""
        rows += f"""\
        <div style="padding:10px 0;border-bottom:1px solid #1E1E2E;">
          <div style="font-size:14px;color:#E8E8ED;font-weight:600;">{f.get('ticker', '')} &mdash; {f.get('insider_name', 'an insider')}</div>
          <div style="font-size:12px;color:#8888A0;margin-top:3px;">{action} {val_str} &middot; filed {when}</div>
        </div>"""
    return rows or f'<p style="font-size:13px;color:#8888A0;">{empty}</p>'


# --- Day 0: Welcome ---

def welcome_email(follows: list[str], strategy_label: str | None = None,
                  unsubscribe_url: str = "") -> tuple[str, str]:
    """(subject, html). `follows` are the names/tickers already followed --
    the search-landing path creates one before the account exists."""
    if follows:
        shown = ", ".join(follows[:5]) + (f" and {len(follows) - 5} more" if len(follows) > 5 else "")
        following = f"<p {_P}>You are following <strong {_STRONG}>{shown}</strong>. The next time any of them files, you get one email.</p>"
        cta = ("Open your feed", f"{APP_URL}/feed")
    else:
        following = (f"<p {_P}>You are not following anyone yet. Pick one company or one insider you already keep an eye on; "
                     f"that is where the emails come from.</p>")
        cta = ("Pick someone to follow", f"{APP_URL}/explore")
    book = (f"<p {_P}>Your portfolio page opens on <strong {_STRONG}>{strategy_label}</strong>. You can switch books there.</p>"
            if strategy_label else "")
    content = f"""\
    <h2 {_H}>Welcome to Form4</h2>
    <p {_P}>Your account is free, and it stays free. It does two things: you can follow up to 10 insiders and 10 companies, and you get an email when any of them files a Form 4. Every filing from the last 90 days is open to you.</p>
    {following}
    {book}
    <p {_P}>Pro is there when you want the grades, the screener and the full history. No hurry.</p>"""
    return ("Welcome to Form4", _layout(content, cta[0], cta[1], unsubscribe_url))


# --- Day 3: what the people you follow did ---

def your_week_email(items: list[dict], following: bool, unsubscribe_url: str = "") -> tuple[str, str]:
    """(subject, html). `items` are recent filings by the people/companies
    they follow; when they follow nobody, the market's most notable instead."""
    if following:
        subject = "What the people you follow filed this week"
        intro = f"<p {_P}>Filings from the insiders and companies you follow, last three days.</p>"
        empty = "Nobody you follow has filed in the last three days. That is normal; most insiders file a few times a year."
    else:
        subject = "What stood out this week"
        intro = (f"<p {_P}>You are not following anyone yet, so here is what stood out across the market. "
                 f"Follow a company or an insider and this email becomes about them.</p>")
        empty = "A quiet few days. Check the feed for the latest."
    content = f"""\
    <h2 {_H}>{subject}</h2>
    {intro}
    {_filing_rows(items, empty)}"""
    return (subject, _layout(content, "Open your feed", f"{APP_URL}/feed", unsubscribe_url))


# --- Day 10: what Pro adds, said once ---

def pro_once_email(unsubscribe_url: str = "") -> tuple[str, str]:
    """(subject, html). The only Pro pitch in the sequence."""
    content = f"""\
    <h2 {_H}>What Pro adds</h2>
    <p {_P}>Free is the record: who filed, what they did, 90 days back, and an email when the people you follow file again.</p>
    <p {_P}>Pro is the judgement on top of it: each insider's grade and full track record, any feed filtered by grade, the screener and clusters, every filing back to 2016, and entry and exit alerts from the strategy books.</p>
    <p {_P}>It is $25 a month. There is a 7-day free trial, started from the pricing page, and nothing is charged until it ends.</p>
    <p {_P}>If free does what you need, keep it. This is the only time we will bring Pro up.</p>"""
    return ("What Pro adds", _layout(content, "See what Pro adds", f"{APP_URL}/pricing", unsubscribe_url))


# --- Day 30: only if they have gone quiet ---

def win_back_email(items: list[dict], following: bool, unsubscribe_url: str = "") -> tuple[str, str]:
    """(subject, html). Sent only to an account with no sign-in for two weeks."""
    intro = (f"<p {_P}>Filings from the insiders and companies you follow, last thirty days.</p>" if following
             else f"<p {_P}>The month's most notable insider filings. Follow someone and this becomes about them.</p>")
    content = f"""\
    <h2 {_H}>Since you were last here</h2>
    {intro}
    {_filing_rows(items, "A quiet month. The feed has the latest.")}"""
    return ("Since you were last here", _layout(content, "Open Form4", f"{APP_URL}/feed", unsubscribe_url))


# --- One-off: comped Pro access (not part of the sequence) ---

def comp_grant_email(
    first_name: str = "",
    until_date: str = "",
    unsubscribe_url: str = "",
    refunded: bool = False,
) -> tuple[str, str]:
    """Returns (subject, html) for the note announcing a comped Pro window.

    Sent by hand after scripts/comp_user.py, to someone whose trial already
    lapsed and who has therefore already received the automated "your grace
    period has ended" mail. It reads as a reversal of that, so the tone is a
    founder note rather than a marketing blast.

    first_name: may be empty — Clerk has no name for some accounts.
    until_date: human-readable comp end, e.g. "November 11th".
    refunded: state the refund explicitly (paying customers hit by the outage).
    """
    greeting = f"Hi {first_name}," if first_name else "Hi there,"
    # until_date arrives already human-formatted, e.g. "November 11th".
    expires = f" (expires on {until_date})" if until_date else ""

    # Paying customers caught by the outage were refunded; say so explicitly
    # rather than letting the comp stand in for it.
    refund_para = ""
    if refunded:
        refund_para = """
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      I've refunded your payment in full. I appreciate your business.
    </p>"""

    content = f"""\
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">{greeting}</p>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      I'm Derek, the founder of Form4. I'm reaching out to ask for your feedback
      and to apologize for the last couple of weeks.
    </p>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      We ran into an unexpected error and the site was down for a number of days.
      If you tried to access the site during that time and saw an error, that's why.
    </p>{refund_para}
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      In order to make up for this, I've turned on
      <strong style="color:#3B82F6;">Pro for your account for the next three
      months for free</strong>{expires}.
    </p>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      I'd appreciate any feedback that you have on Form4 Pro during this time.
      What's missing? What's confusing? What do you wish Form4 did? Any feedback
      is helpful and I really appreciate your understanding.
    </p>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      If you have any questions, concerns, and/or feedback, just reply to this
      email at any time.
    </p>
    <p style="margin:0;font-size:14px;color:#8888A0;line-height:1.6;">
      Thanks,<br>
      Derek<br>
      <span style="font-size:12px;color:#55556A;">Form4</span>
    </p>"""

    return (
        "Sorry for the downtime, and 3 months of Pro",
        _layout(content, "Take a look", f"{APP_URL}/", unsubscribe_url),
    )


# Registry: maps (email_name, target_day) for the sequence runner
EMAIL_SEQUENCE = [
    ("welcome", 0),
    ("your_week", 3),
    ("pro_once", 10),
    ("win_back", 30),   # only if there has been no sign-in for WIN_BACK_QUIET_DAYS
]

#: A day-30 note goes only to an account that has gone quiet; someone who
#: signed in this week does not need to be told what they missed.
WIN_BACK_QUIET_DAYS = 14
