"""Trial email sequence templates for Form4.app.

6 emails sent at days 0, 3, 5, 7, 14, 30 after signup.
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


# --- Day 0: Welcome ---

def welcome_email(unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the welcome email."""
    content = """\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">Welcome to Form4</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      You now have <strong style="color:#3B82F6;">7 days of full Pro access</strong> &mdash; every signal, every score, every insider track record. No credit card required.
    </p>
    <p style="margin:0 0 8px;font-size:13px;font-weight:600;color:#E8E8ED;">Here's how to get the most out of it:</p>
    <ul style="margin:0;padding:0 0 0 18px;font-size:13px;color:#8888A0;line-height:1.8;">
      <li><strong style="color:#E8E8ED;">Browse the feed</strong> &mdash; newest insider filings with signal grades</li>
      <li><strong style="color:#E8E8ED;">Check the leaderboard</strong> &mdash; insiders ranked by track record</li>
      <li><strong style="color:#E8E8ED;">Explore clusters</strong> &mdash; multiple insiders buying the same stock</li>
    </ul>"""

    return (
        "Welcome to Form4 — your trial starts now",
        _layout(content, "Open the Feed", f"{APP_URL}/", unsubscribe_url),
    )


# --- Day 3: Value ---

def value_email(top_signals: list[dict], unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the value email showing recent top signals.

    top_signals: list of dicts with keys: ticker, insider_name, trade_type, value, return_7d
    """
    rows = ""
    for s in top_signals[:3]:
        ret = s.get("return_7d")
        ret_str = f"{ret:+.1f}%" if ret is not None else "pending"
        ret_color = "#22C55E" if ret and ret > 0 else "#EF4444" if ret and ret < 0 else "#8888A0"
        action = "bought" if s.get("trade_type") == "buy" else "sold"
        val = s.get("value", 0)
        val_str = f"${val:,.0f}" if val else ""
        rows += f"""\
        <div style="padding:12px 0;border-bottom:1px solid #1E1E2E;">
          <div style="font-size:14px;color:#E8E8ED;font-weight:600;">{s.get('ticker', '')} &mdash; {s.get('insider_name', 'Insider')}</div>
          <div style="font-size:12px;color:#8888A0;margin-top:4px;">
            {action} {val_str} &middot; 7-day return: <span style="color:{ret_color};font-weight:600;">{ret_str}</span>
          </div>
        </div>"""

    if not rows:
        rows = '<p style="font-size:13px;color:#8888A0;">Check the feed to see what insiders have been doing.</p>'

    content = f"""\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">Here's what insiders did this week</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      These are the top signals from your first few days on Form4:
    </p>
    {rows}"""

    return (
        "Here's what insiders did this week",
        _layout(content, "See All Signals", f"{APP_URL}/signals", unsubscribe_url),
    )


# --- Day 5: Urgency ---

def urgency_email(unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the 2-days-left urgency email."""
    content = """\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">2 days left on your trial</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      Your Pro access expires in 2 days. After that, you'll enter a 7-day grace period with
      <strong style="color:#F59E0B;">24-hour signal delays</strong>, then move to the free tier.
    </p>
    <p style="margin:0 0 8px;font-size:13px;font-weight:600;color:#E8E8ED;">What you'll lose:</p>
    <ul style="margin:0;padding:0 0 0 18px;font-size:13px;color:#8888A0;line-height:1.8;">
      <li>Real-time insider filing alerts</li>
      <li>Insider track records &amp; quality scores</li>
      <li>Full trade history (free tier: last 90 days only)</li>
      <li>Signal quality grades (A through F)</li>
    </ul>"""

    return (
        "2 days left on your Form4 trial",
        _layout(content, "Upgrade to Pro", f"{APP_URL}/pricing", unsubscribe_url),
    )


# --- Day 7: Trial ended ---

def trial_ended_email(unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the trial-ended email."""
    content = """\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">Your trial has ended</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      You're now in a <strong style="color:#F59E0B;">7-day grace period</strong>.
      You can still see all insider filings, but signals are <strong style="color:#F59E0B;">delayed by 24 hours</strong>.
    </p>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      After the grace period ends, you'll move to the free tier:
    </p>
    <ul style="margin:0;padding:0 0 0 18px;font-size:13px;color:#8888A0;line-height:1.8;">
      <li>Last 90 days of filings only</li>
      <li>Track records and insider scores hidden</li>
      <li>Identifying details redacted on older filings</li>
    </ul>
    <p style="margin:16px 0 0;font-size:14px;color:#8888A0;line-height:1.6;">
      Upgrade now to keep full, real-time access.
    </p>"""

    return (
        "Your Form4 trial has ended",
        _layout(content, "Upgrade to Pro", f"{APP_URL}/pricing", unsubscribe_url),
    )


# --- Day 14: Hard gate ---

def hard_gate_email(unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the grace-period-ended email."""
    content = """\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">Your grace period has ended</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      You're now on the free tier. You can still browse the last 90 days of filings, but
      track records, insider scores, and full details are reserved for Pro members.
    </p>
    <p style="margin:0 0 0;font-size:14px;color:#8888A0;line-height:1.6;">
      Upgrade anytime to restore full access &mdash; your watchlist and settings are still saved.
    </p>"""

    return (
        "Your Form4 grace period has ended",
        _layout(content, "Upgrade to Pro", f"{APP_URL}/pricing", unsubscribe_url),
    )


# --- Day 30: Win-back ---

def win_back_email(top_signals: list[dict], unsubscribe_url: str = "") -> tuple[str, str]:
    """Returns (subject, html) for the 30-day win-back email.

    top_signals: list of dicts with keys: ticker, insider_name, trade_type, value, return_7d
    """
    rows = ""
    for s in top_signals[:5]:
        ret = s.get("return_7d")
        ret_str = f"{ret:+.1f}%" if ret is not None else "pending"
        ret_color = "#22C55E" if ret and ret > 0 else "#EF4444" if ret and ret < 0 else "#8888A0"
        action = "bought" if s.get("trade_type") == "buy" else "sold"
        val = s.get("value", 0)
        val_str = f"${val:,.0f}" if val else ""
        rows += f"""\
        <div style="padding:10px 0;border-bottom:1px solid #1E1E2E;">
          <div style="font-size:14px;color:#E8E8ED;font-weight:600;">{s.get('ticker', '')} &mdash; {s.get('insider_name', 'Insider')}</div>
          <div style="font-size:12px;color:#8888A0;margin-top:4px;">
            {action} {val_str} &middot; 7-day return: <span style="color:{ret_color};font-weight:600;">{ret_str}</span>
          </div>
        </div>"""

    if not rows:
        rows = '<p style="font-size:13px;color:#8888A0;">Check the feed for recent insider activity.</p>'

    content = f"""\
    <h2 style="margin:0 0 12px;font-size:18px;color:#E8E8ED;">Here's what you missed this month</h2>
    <p style="margin:0 0 16px;font-size:14px;color:#8888A0;line-height:1.6;">
      These were the top insider signals from the last 30 days. Pro members saw them in real time.
    </p>
    {rows}"""

    return (
        "Here's what you missed on Form4 this month",
        _layout(content, "Upgrade to Pro", f"{APP_URL}/pricing", unsubscribe_url),
    )


# Registry: maps (email_name, target_day) for the sequence runner
EMAIL_SEQUENCE = [
    ("welcome", 0),
    ("value", 3),
    ("urgency", 5),
    ("trial_ended", 7),
    ("hard_gate", 14),
    ("win_back", 30),
]
