"""StrategySignal — generic composite signal driven by YAML.

A strategy = composite signal = triggers + gates + emit policy. Each YAML
file in ``dataplane/strategies/`` loads into a StrategySignal subclass
that materializes one row per evaluation (triggered OR suppressed), with
per-gate reasons in ``value``. That means **replay == backfill**: re-run
the strategy's partitions and you get the historical alert tape.

YAML shape (minimum-viable; richer keys can be added without breaking
existing strategies):

    strategy: agrade_drawdown_buy
    version: v1
    owner: derek
    sla_hours: 24
    cadence: daily            # M1: daily evaluation; M2: 5min
    universe: all             # 'all' means whatever the trigger feed emits
    triggers:
      - signal: insider.trades.raw.v1
        when: value.trade_type == 'buy' and value.value >= 50000
    gates:
      - signal: insider.career_grade.v3
        when: value.grade in ['A+', 'A']
        max_staleness: 30h
      - signal: price.daily.close.v1
        window: 90d
        when: latest.close / earliest.close - 1 <= -0.10
        max_staleness: 24h
    emit:
      channel: ntfy
      cooldown: 5d per ticker  # emit-time concern; not enforced here

Per evaluation, the observation's ``value`` carries:
    {
      "triggered": bool,
      "trigger_signal": str,
      "trigger_value": {...},        # the upstream observation that triggered
      "gates": [
         {"signal": "...", "passed": bool, "reason": "..."},
         ...
      ],
      "fail_reason": str | null,     # short tag if not triggered
    }
"""
from __future__ import annotations

import ast
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import psycopg2
import yaml

from dataplane.observation import SignalObservation
from dataplane.signal import Signal
from dataplane.upstream import Upstream


# ── Safe expression evaluator ───────────────────────────────────────────

_ALLOWED_AST = (
    ast.Expression, ast.Compare, ast.BoolOp, ast.BinOp, ast.UnaryOp,
    ast.And, ast.Or, ast.Not,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow, ast.USub, ast.UAdd,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn,
    ast.Name, ast.Constant,
    ast.Attribute, ast.Subscript, ast.Index, ast.Slice,
    ast.List, ast.Tuple, ast.Set,
    ast.Load,
)


class _AttrDict:
    """Dot-access wrapper around a JSON dict. Recurses on nested dicts."""

    def __init__(self, data: Optional[dict]):
        self._data = data or {}

    def __getattr__(self, name: str):
        if name == "_data":
            raise AttributeError(name)
        if name not in self._data:
            return None
        v = self._data[name]
        return _AttrDict(v) if isinstance(v, dict) else v

    def __getitem__(self, key):
        v = self._data[key]
        return _AttrDict(v) if isinstance(v, dict) else v

    def __repr__(self):
        return f"AttrDict({self._data!r})"


def _safe_eval(expr: str, namespace: Dict[str, Any]) -> Any:
    """Eval a YAML-supplied predicate inside a restricted namespace.

    Trusted-YAML model: the strategy YAML is committed in-repo, not user
    input. The whitelist still exists as a forcing function so a typo
    can't accidentally hit __import__ or open a file.
    """
    tree = ast.parse(expr, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_AST):
            raise ValueError(
                f"strategy expression not allowed: {expr!r} "
                f"contains {type(node).__name__}"
            )
    return eval(  # noqa: S307 — whitelisted nodes only
        compile(tree, "<strategy>", "eval"),
        {"__builtins__": {}},
        namespace,
    )


# ── Duration parsing ────────────────────────────────────────────────────

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")


def parse_duration(s) -> timedelta:
    """Parse '30h', '90d', '5m' style durations into a timedelta."""
    if isinstance(s, timedelta):
        return s
    if isinstance(s, (int, float)):
        return timedelta(seconds=float(s))
    m = _DURATION_RE.match(str(s))
    if not m:
        raise ValueError(f"can't parse duration {s!r}; use forms like '30h' or '90d'")
    n, unit = int(m.group(1)), m.group(2)
    return {
        "s": timedelta(seconds=n),
        "m": timedelta(minutes=n),
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
    }[unit]


# ── StrategySignal ──────────────────────────────────────────────────────

class StrategySignal(Signal):
    """Generic composite signal. Subclasses set _strategy_spec at class
    creation time (via load_strategy_yaml) and inherit materialize_partition.

    Always emits per_partition_events; one observation per evaluated
    trigger event in the partition.
    """

    materialization_mode = "per_partition_events"
    _dataplane_abstract = True  # base class isn't itself registerable

    # Strategies register under the 'composite' signal_class regardless of
    # the strategy.<name> signal_id prefix (the catalog enum has no
    # 'strategy' value, and DESIGN.md models strategies as composite signals).
    signal_class_override = "composite"

    # Set by load_strategy_yaml on the dynamic subclass:
    _strategy_spec: dict = {}

    def materialize_partition(
        self, partition_date: datetime
    ) -> List[SignalObservation]:
        spec = self._strategy_spec
        observations: List[SignalObservation] = []

        if not spec.get("triggers"):
            return observations
        if len(spec["triggers"]) != 1:
            raise NotImplementedError(
                "M1 supports exactly one trigger; multi-trigger composition is M3."
            )
        trigger = spec["triggers"][0]
        trigger_signal_id = trigger["signal"]
        trigger_when = trigger.get("when")

        # Set PIT clock for read() — gates pass an explicit as_of override.
        self._pit_as_of = partition_date

        events = self._fetch_trigger_events(trigger_signal_id, partition_date)

        for event in events:
            ticker = event["ticker"]
            event_ts = event["as_of_date"]
            event_value = event.get("value") or {}

            # 1) Trigger predicate
            if trigger_when:
                try:
                    triggered_at_edge = bool(_safe_eval(
                        trigger_when, {"value": _AttrDict(event_value)}
                    ))
                except Exception as exc:
                    triggered_at_edge = False
                    skip_reason = f"trigger eval error: {exc}"
                else:
                    skip_reason = None if triggered_at_edge else "trigger predicate false"
            else:
                triggered_at_edge = True
                skip_reason = None

            gate_results: List[dict] = []
            fail_reason: Optional[str] = skip_reason

            if triggered_at_edge:
                for gate in spec.get("gates") or []:
                    gr = self._evaluate_gate(gate, ticker, event_ts)
                    gate_results.append(gr)
                    if not gr["passed"] and fail_reason is None:
                        fail_reason = (
                            f"gate {gr['signal']} failed: {gr['reason']}"
                        )

            triggered_final = triggered_at_edge and all(
                g["passed"] for g in gate_results
            )

            payload = {
                "triggered": triggered_final,
                "trigger_signal": trigger_signal_id,
                "trigger_value": event_value,
                "gates": gate_results,
                "fail_reason": fail_reason if not triggered_final else None,
            }
            observations.append(SignalObservation(
                signal_id=f"{self.signal_id}.{self.version}",
                ticker=ticker,
                as_of_date=event_ts,
                value=payload,
                source_run_id=self._run_id,
                metadata={"partition_date": partition_date.date().isoformat()},
            ))

        return observations

    # ── helpers ────────────────────────────────────────────────────────

    def _fetch_trigger_events(
        self, trigger_signal_id: str, partition_date: datetime
    ) -> List[dict]:
        """All observations of the trigger feed on the partition date."""
        day_start = partition_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        cur = self._conn.cursor()
        try:
            cur.execute(
                """
                SELECT ticker, as_of_date, value, ingested_at
                  FROM signal_observations
                 WHERE signal_id LIKE %s
                   AND as_of_date >= %s
                   AND as_of_date <  %s
                 ORDER BY as_of_date ASC
                """,
                (f"{trigger_signal_id}%", day_start, day_end),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def _evaluate_gate(
        self, gate: dict, ticker: str, trigger_ts: datetime
    ) -> dict:
        """Evaluate one gate at the trigger's as_of timestamp."""
        gate_signal_id = gate["signal"]
        window_str = gate.get("window")
        max_staleness = (
            parse_duration(gate["max_staleness"])
            if gate.get("max_staleness")
            else None
        )

        try:
            if window_str:
                lookback = parse_duration(window_str)
                rows = self.read(
                    gate_signal_id, ticker, as_of=trigger_ts, lookback=lookback
                )
                if not rows:
                    return {
                        "signal": gate_signal_id,
                        "passed": False,
                        "reason": f"no upstream in last {window_str}",
                    }
                latest = rows[0]
                earliest = rows[-1]
                if max_staleness and (
                    trigger_ts - latest["as_of_date"] > max_staleness
                ):
                    return {
                        "signal": gate_signal_id,
                        "passed": False,
                        "reason": (
                            f"stale (latest {latest['as_of_date'].isoformat()},"
                            f" > {gate['max_staleness']})"
                        ),
                    }
                ns = {
                    "latest": _AttrDict(latest["value"]),
                    "earliest": _AttrDict(earliest["value"]),
                    "window_rows": len(rows),
                }
            else:
                rows = self.read(gate_signal_id, ticker, as_of=trigger_ts)
                if not rows:
                    return {
                        "signal": gate_signal_id,
                        "passed": False,
                        "reason": "no upstream value",
                    }
                latest = rows[0]
                if max_staleness and (
                    trigger_ts - latest["as_of_date"] > max_staleness
                ):
                    return {
                        "signal": gate_signal_id,
                        "passed": False,
                        "reason": (
                            f"stale (latest {latest['as_of_date'].isoformat()},"
                            f" > {gate['max_staleness']})"
                        ),
                    }
                ns = {"value": _AttrDict(latest["value"])}

            try:
                passed = bool(_safe_eval(gate["when"], ns))
            except TypeError:
                # Null in a numeric expression — typically a no_data row at
                # the window's edge (upstream backfill hadn't reached that
                # date yet). Fail closed; surface the lookback depth gap.
                return {
                    "signal": gate_signal_id,
                    "passed": False,
                    "reason": (
                        "incomplete upstream window — "
                        "extend backfill depth to fix"
                    ),
                }
            except Exception as exc:
                return {
                    "signal": gate_signal_id,
                    "passed": False,
                    "reason": f"eval error: {exc}",
                }
            return {
                "signal": gate_signal_id,
                "passed": passed,
                "reason": "ok" if passed else f"predicate false ({gate['when']})",
            }
        except Exception as exc:
            return {
                "signal": gate_signal_id,
                "passed": False,
                "reason": f"read error: {exc}",
            }


# ── YAML loader ─────────────────────────────────────────────────────────

def make_strategy_class(spec: dict) -> Type[StrategySignal]:
    """Build a StrategySignal subclass from an already-parsed spec dict.

    Used by load_strategy_yaml(path) and by the desk composer's dry-run
    (which works from a candidate dict without touching the filesystem).
    """
    name = spec["strategy"]
    version = spec["version"]
    owner = spec.get("owner", "derek")
    sla_hours = float(spec.get("sla_hours", 24))

    # Upstream is the union of trigger.signal + every gate.signal.
    seen_upstream: Dict[str, Upstream] = {}
    for t in spec.get("triggers") or []:
        sid = t["signal"]
        seen_upstream.setdefault(sid, Upstream(sid, pit_lag=timedelta(0)))
    for g in spec.get("gates") or []:
        sid = g["signal"]
        seen_upstream.setdefault(sid, Upstream(sid, pit_lag=timedelta(0)))

    cls = type(
        f"Strategy_{name}_{version}".replace(".", "_").replace("-", "_"),
        (StrategySignal,),
        {
            "signal_id": f"strategy.{name}",
            "version": version,
            "owner": owner,
            "sla_hours": sla_hours,
            "upstream": list(seen_upstream.values()),
            "description": spec.get("description", f"strategy {name}"),
            "output_schema": {
                "triggered": "boolean",
                "trigger_signal": "text",
                "trigger_value": "jsonb",
                "gates": "jsonb",
                "fail_reason": "text",
            },
            "_strategy_spec": spec,
            "_dataplane_abstract": False,
        },
    )
    return cls


def load_strategy_yaml(path: Path) -> Type[StrategySignal]:
    """Load one YAML file and return a dynamically-created StrategySignal subclass."""
    return make_strategy_class(yaml.safe_load(path.read_text()))


def discover_strategies(
    strategies_dir: Path,
) -> List[Type[StrategySignal]]:
    """Walk strategies_dir for *.yaml/*.yml and return the loaded subclasses."""
    if not strategies_dir.exists():
        return []
    out: List[Type[StrategySignal]] = []
    for p in sorted(list(strategies_dir.glob("*.yaml")) + list(strategies_dir.glob("*.yml"))):
        try:
            out.append(load_strategy_yaml(p))
        except Exception:
            continue
    return out
