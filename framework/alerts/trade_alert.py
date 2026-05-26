"""Format + dispatch trade-candidate alerts for `execution_mode: alert_only`.

The body is intentionally short — most of it should fit in an iMessage
notification preview without expanding (5 short lines, ~250 chars). Order
of information matters: ticker + price first (scannable at a glance), then
exit conditions (so you don't have to look them up), then context.

Usage from cw_runner.execute_entries when execution_mode == 'alert_only':

    from framework.alerts.trade_alert import send_entry_alert
    send_entry_alert(
        strategy="quality_momentum",
        prefix="[QM]",
        ticker="AAPL",
        entry_price=180.50,
        qty=55,
        target_hold=42,
        stop_pct=None,
        conviction=8.2,
        signal_grade="A+",
        career_grade="A+",
        insider_name="Tim Cook",
        insider_title="CEO",
        dollar_amount=9927.50,
        trade_id=1234567,
    )
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from framework.alerts.ntfy import send_ntfy

logger = logging.getLogger(__name__)


def _fmt_stop(stop_pct: Optional[float]) -> str:
    """Format the stop-loss leg of the exit description."""
    if stop_pct is None or stop_pct == 0:
        return "no stop"
    # stop_pct is stored as negative float (-0.30 = -30%) in the YAMLs
    return f"stop {stop_pct * 100:+.0f}%"


def _truncate(s: Optional[str], n: int) -> str:
    if not s:
        return ""
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _first_sentence(text: Optional[str], max_len: int = 140) -> Optional[str]:
    """Extract the first sentence (or the whole thing if shorter than max_len).
    Used to compress LLM narrative output into push-notification preview size."""
    if not text:
        return None
    text = text.strip()
    # Strip leading {"..."} JSON-array artifacts that some LLM runs return
    if text.startswith("{\"") or text.startswith('{"'):
        return None
    # Split on period+space; first piece is the lead sentence
    parts = text.split(". ", 1)
    head = parts[0].rstrip(".")
    if len(head) > max_len:
        head = head[: max_len - 1] + "…"
    return head


def format_entry_alert(
    *,
    prefix: str,
    ticker: str,
    entry_price: float,
    qty: int,
    dollar_amount: float,
    target_hold: int,
    stop_pct: Optional[float],
    conviction: Optional[float],
    signal_grade: Optional[str] = None,
    career_grade: Optional[str] = None,
    insider_name: Optional[str] = None,
    insider_title: Optional[str] = None,
    is_rare_reversal: bool = False,
    trade_id: Optional[int] = None,
    narrative: Optional[dict] = None,
) -> str:
    """Build the body for a push-notification entry alert.

    Layout (fits ntfy / iOS preview):
        [QM] BUY AAPL  ~$180.50  conv 8.2
        55 sh ≈ $9.9k | hold 42td, no stop
        career A+ · Tim Cook (CEO)

        Why: down 18% YTD on antitrust pressure; insider buying accelerating
        ⚡ Q4 earnings 11/02 — catalyst window

        → form4.app/filing/<encoded>

    `narrative` is the optional dict from trade_narrative (or None if the
    LLM hasn't generated yet). When present, includes a 1-sentence excerpt
    of summary + a 1-sentence catalyst hook. When None, alert still fires
    with the structural info — the URL gives full context once narrative
    catches up (cron runs every 5 min).
    """
    lines = []

    # Line 1: header — strategy prefix + action + ticker + price + conviction
    conv_str = f"conv {conviction:.1f}" if conviction is not None else ""
    line1 = f"{prefix} BUY {ticker}  ~${entry_price:,.2f}"
    if conv_str:
        line1 += f"  {conv_str}"
    lines.append(line1)

    # Line 2: position size + exit conditions
    if dollar_amount >= 1_000_000:
        dollar_str = f"${dollar_amount / 1_000_000:.1f}M"
    elif dollar_amount >= 1_000:
        dollar_str = f"${dollar_amount / 1_000:.1f}k"
    else:
        dollar_str = f"${dollar_amount:,.0f}"
    stop_str = _fmt_stop(stop_pct)
    lines.append(
        f"{qty:,} sh ≈ {dollar_str} | hold {target_hold}td, {stop_str}"
    )

    # Line 3: condensed grade + insider on one line (saves space for narrative)
    grade_bits = []
    if career_grade:
        grade_bits.append(f"career {career_grade}")
    if signal_grade and signal_grade != career_grade:
        grade_bits.append(f"signal {signal_grade}")
    if is_rare_reversal:
        grade_bits.append("rare reversal")
    grade_str = " / ".join(grade_bits) if grade_bits else ""
    if insider_name:
        insider = _truncate(insider_name, 28)
        if insider_title:
            insider += f" ({_truncate(insider_title, 16)})"
        if grade_str:
            lines.append(f"{grade_str} · {insider}")
        else:
            lines.append(insider)
    elif grade_str:
        lines.append(grade_str)

    # NEW: narrative excerpt — 1 sentence of summary + 1 catalyst hook,
    # but only when we have a real LLM narrative (skip the templated tiers
    # since those add no signal beyond what the alert already conveys).
    is_llm = narrative and narrative.get("tier") in ("high_signal",)
    if is_llm:
        summary_lead = _first_sentence(narrative.get("summary"), max_len=160)
        if summary_lead:
            lines.append("")  # visual break
            lines.append(f"Why: {summary_lead}.")
        catalyst_lead = _first_sentence(narrative.get("catalysts"), max_len=120)
        if catalyst_lead and "insufficient" not in catalyst_lead.lower():
            lines.append(f"⚡ {catalyst_lead}.")

    # Final line: deep link — only if we can encode the trade_id
    if trade_id:
        try:
            from api.id_encoding import encode_trade_id
            encoded = encode_trade_id(trade_id)
            if encoded:
                lines.append("")
                lines.append(f"→ form4.app/filing/{encoded}")
        except Exception as exc:
            logger.debug("trade_id encoding failed: %s", exc)

    return "\n".join(lines)


def send_entry_alert(**kwargs) -> bool:
    """Format + send the entry alert via ntfy.sh.

    Reads NTFY_ALERT_TOPIC from env. Logs WARNING and returns False if
    unconfigured (cw_runner can fall back to NDJSON log).

    Notification title is derived from the strategy prefix; body is the
    multi-line trade detail. Tap on iOS opens the filing detail page.
    """
    body = format_entry_alert(**kwargs)
    # Extract a click_url from the body — the deep link line starts with
    # "→ form4.app/" so we reconstruct the https:// URL for ntfy's Click header.
    click_url = None
    for line in body.splitlines():
        s = line.strip()
        if s.startswith("→ form4.app/"):
            click_url = "https://" + s[2:].strip()
            break
        if s.startswith("https://"):
            click_url = s
            break
    return send_ntfy(
        body,
        title=f"Buy {kwargs.get('ticker', '?')} {kwargs.get('prefix', '')}".strip(),
        tags=["chart_with_upwards_trend"],
        priority=4,                     # high — wake the screen
        click_url=click_url,
    )


def format_exit_alert(
    *,
    prefix: str,
    ticker: str,
    reason: str,            # "time" | "stop" | "manual"
    entry_date: str,
    entry_price: float,
    current_price: Optional[float],
    qty: int,
    hold_days_actual: int,
    target_hold: int,
    trade_id: Optional[int] = None,
) -> str:
    """Build the iMessage body for an exit alert (in alert_only mode).

    Layout:
        [QM] SELL AAPL  ~$220.00  +21.9%
        time-exit @ 42td (entry 2026-03-13)
        55 sh ≈ +$2.2k
        https://form4.app/filing/<encoded>
    """
    lines = []
    pnl_pct = ((current_price - entry_price) / entry_price * 100
               if (current_price and entry_price) else None)
    pnl_str = f"  {pnl_pct:+.1f}%" if pnl_pct is not None else ""
    price_str = f"~${current_price:,.2f}" if current_price else "~$—"
    lines.append(f"{prefix} SELL {ticker}  {price_str}{pnl_str}")
    lines.append(f"{reason}-exit @ {hold_days_actual}td (entry {entry_date})")
    if current_price and entry_price:
        dollar_pnl = (current_price - entry_price) * qty
        sign = "+" if dollar_pnl >= 0 else "−"
        lines.append(f"{qty:,} sh ≈ {sign}${abs(dollar_pnl)/1000:,.1f}k")
    if trade_id:
        try:
            from api.id_encoding import encode_trade_id
            encoded = encode_trade_id(trade_id)
            if encoded:
                lines.append(f"https://form4.app/filing/{encoded}")
        except Exception:
            pass
    return "\n".join(lines)


def send_exit_alert(**kwargs) -> bool:
    body = format_exit_alert(**kwargs)
    click_url = None
    for line in body.splitlines():
        if line.startswith("https://"):
            click_url = line
            break
    return send_ntfy(
        body,
        title=f"Sell {kwargs.get('ticker', '?')} {kwargs.get('prefix', '')}".strip(),
        tags=["bell"],
        priority=4,
        click_url=click_url,
    )
