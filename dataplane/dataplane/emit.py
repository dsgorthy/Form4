"""Strategy alert emission — push triggered observations to ntfy.

Called from the Dagster asset wrapper after observations are written.
NOT called from the CLI/desk backfill path — replay shouldn't spam the
phone with historical alerts that already fired (or never did).

Per the strategy YAML's `emit:` block:
    channel: ntfy            # only ntfy for now
    cooldown: 5d per ticker  # parsed to days; the "per ticker" is implicit

Cooldown is enforced by querying signal_observations for prior triggered
rows for the same (strategy, ticker) within the window. If any exist,
skip — the user already got pinged.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import requests


# Skip emit for partitions older than this — protects against backfill
# spam if someone manually triggers a historical Dagster run.
_MAX_PARTITION_AGE = timedelta(days=2)


def _parse_cooldown(spec: Optional[str]) -> Optional[timedelta]:
    """'5d per ticker' / '12h' / '30d' → timedelta. None on garbage input."""
    if not spec:
        return None
    m = re.match(r"\s*(\d+)\s*([dhm])", str(spec).lower())
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    return {
        "d": timedelta(days=n),
        "h": timedelta(hours=n),
        "m": timedelta(minutes=n),
    }[unit]


def _has_recent_alert(
    conn, signal_id: str, ticker: str, window: timedelta
) -> bool:
    """True if we already wrote a triggered=true observation for this
    (strategy, ticker) within the cooldown window."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
              FROM signal_observations
             WHERE signal_id LIKE %s
               AND ticker = %s
               AND (value->>'triggered')::boolean = true
               AND ingested_at >= now() - INTERVAL '1 second' * %s
             LIMIT 1
            """,
            (f"{signal_id}%", ticker, int(window.total_seconds())),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _format_alert(strategy_name: str, obs_value: dict, ticker: str) -> str:
    tv = obs_value.get("trigger_value") or {}
    insider = tv.get("insider_name") or "—"
    qty = tv.get("qty")
    val = tv.get("value")
    trans = tv.get("trans_code") or "?"
    tt = tv.get("trade_type") or "?"

    head = f"{ticker} · {strategy_name}"
    line1 = f"{tt.upper()} {trans}"
    if qty:
        line1 += f" qty={qty:,}"
    if val:
        line1 += f" ${val:,.0f}"
    if insider and insider != "—":
        line1 += f" by {insider}"

    gates_summary = []
    for g in obs_value.get("gates") or []:
        sid_short = g.get("signal", "").rsplit(".", 1)[-1]
        if g.get("passed"):
            gates_summary.append(f"{sid_short}: ok")
        else:
            gates_summary.append(f"{sid_short}: {g.get('reason', 'fail')}")
    line2 = " · ".join(gates_summary) if gates_summary else ""

    return f"{head}\n{line1}" + (f"\n{line2}" if line2 else "")


def emit_alerts(
    conn,
    strategy_signal_instance,
    observations: List,
    partition_date: datetime,
) -> int:
    """Iterate observations, push ntfy for triggered ones that pass cooldown.

    Returns count of pushes attempted.
    """
    spec = getattr(strategy_signal_instance, "_strategy_spec", None) or {}
    emit_cfg = spec.get("emit") or {}
    if emit_cfg.get("channel") != "ntfy":
        return 0

    topic = os.environ.get("NTFY_ALERT_TOPIC")
    if not topic:
        return 0  # silently skip in dev

    # Backfill spam guard: only emit for recent partitions.
    if partition_date.tzinfo is None:
        partition_date = partition_date.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - partition_date
    if age > _MAX_PARTITION_AGE:
        return 0

    cooldown = _parse_cooldown(emit_cfg.get("cooldown")) or timedelta(days=1)

    strategy_id = strategy_signal_instance.signal_id  # "strategy.<name>"
    name = strategy_id.removeprefix("strategy.")

    pushed = 0
    for obs in observations:
        v = obs.value if isinstance(obs.value, dict) else {}
        if not v.get("triggered"):
            continue
        if _has_recent_alert(conn, strategy_id, obs.ticker, cooldown):
            continue
        body = _format_alert(name, v, obs.ticker)
        try:
            requests.post(
                f"https://ntfy.sh/{topic}",
                data=body,
                headers={
                    "Title": f"Pyrrho · {name}",
                    "Priority": "high",
                    "Tags": "bell",
                },
                timeout=8,
            )
            pushed += 1
        except Exception:
            # Alerting must never break ingestion.
            pass
    return pushed
