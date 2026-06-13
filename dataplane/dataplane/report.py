"""Strategy report — summarize a strategy's evaluation tape over a window.

The M1 demo artifact: 'every alert it would have fired since March, with
reasons'. This produces it. Reads the strategy's observations from
signal_observations and prints:

  - total evaluations
  - outcome breakdown (triggered, suppressed-by-which-reason)
  - the triggered alerts themselves with one-line summaries

Used by ``python3 -m dataplane report <strategy_id> --from --to``.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List, Optional

import psycopg2


@contextmanager
def _conn():
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN", "dbname=pyrrho_data_dev host=localhost"
    )
    c = psycopg2.connect(dsn)
    try:
        yield c
    finally:
        c.close()


@dataclass
class StrategyReport:
    signal_id: str
    from_date: str
    to_date: str
    total: int = 0
    triggered: int = 0
    outcomes: Counter = field(default_factory=Counter)
    alerts: List[dict] = field(default_factory=list)


def report(
    signal_ref: str,
    from_date: str,
    to_date: str,
    show_alerts: int = 50,
) -> StrategyReport:
    rep = StrategyReport(signal_id=signal_ref, from_date=from_date, to_date=to_date)

    sql = """
        SELECT ticker, as_of_date, value
          FROM signal_observations
         WHERE signal_id LIKE %s
           AND as_of_date >= %s::date
           AND as_of_date <  (%s::date + INTERVAL '1 day')
         ORDER BY as_of_date
    """
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (f"{signal_ref}%", from_date, to_date))
            rows = cur.fetchall()
        finally:
            cur.close()

    for ticker, as_of, value in rows:
        rep.total += 1
        if isinstance(value, str):
            value = json.loads(value)
        triggered = bool(value.get("triggered"))
        if triggered:
            rep.triggered += 1
            rep.outcomes["(triggered)"] += 1
            if len(rep.alerts) < show_alerts:
                rep.alerts.append({
                    "ticker": ticker,
                    "as_of": as_of.isoformat(),
                    "trigger": value.get("trigger_value", {}),
                    "gates": value.get("gates", []),
                })
        else:
            reason = value.get("fail_reason") or "(suppressed, unknown reason)"
            rep.outcomes[reason] += 1

    return rep


def format_report(rep: StrategyReport) -> str:
    lines: List[str] = []
    lines.append(f"strategy report  {rep.signal_id}  [{rep.from_date} → {rep.to_date}]")
    lines.append(f"  evaluations:  {rep.total:>8,}")
    lines.append(f"  triggered:    {rep.triggered:>8,}")
    lines.append("  outcomes:")
    for reason, n in rep.outcomes.most_common():
        lines.append(f"    {n:>6}  {reason}")

    if rep.alerts:
        lines.append("")
        lines.append(f"  triggered alerts (first {len(rep.alerts)}):")
        for a in rep.alerts:
            tv = a["trigger"]
            t = tv.get("trade_type", "?")
            qty = tv.get("qty", "?")
            val = tv.get("value", 0) or 0
            try:
                val_str = f"${val:,.0f}"
            except (TypeError, ValueError):
                val_str = str(val)
            insider = tv.get("insider_name", "?")
            gate_str = "; ".join(
                f"{g['signal'].split('.', 1)[-1]}: {g['reason']}"
                for g in a["gates"]
            )
            lines.append(
                f"    {a['as_of'][:19]}  {a['ticker']:<6}  {t.upper():<4}  "
                f"qty={qty:>8}  {val_str:>14}  by {insider}"
            )
            if gate_str:
                lines.append(f"        gates: {gate_str}")
    else:
        lines.append("")
        lines.append("  (no triggered alerts in this window)")
    return "\n".join(lines)
