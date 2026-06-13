"""Strategy composer — form → spec dict → YAML / dry-run / save.

All operations are pure functions over a spec dict so they're easy to
test and reuse. The Starlette routes just translate FormData → spec and
call into here.
"""
from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import yaml

from dataplane.strategy import make_strategy_class


# Where YAMLs live. Same path the dispatcher discovers from.
STRATEGIES_DIR = Path(__file__).resolve().parents[2] / "strategies"


# ── Form → spec ────────────────────────────────────────────────────────

def spec_from_form(form: Dict[str, Any]) -> Dict[str, Any]:
    """Translate flat form fields into the nested strategy spec dict.

    Form fields expected:
        strategy, version, owner, sla_hours, cadence, universe, description,
        trigger_signal, trigger_when,
        gate_signal (list), gate_when (list), gate_window (list), gate_max_staleness (list),
        emit_channel, emit_cooldown.
    """
    name = (form.get("strategy") or "").strip()
    version = (form.get("version") or "v1").strip() or "v1"

    spec: Dict[str, Any] = {
        "strategy":    name,
        "version":     version,
        "owner":       (form.get("owner") or "derek").strip(),
        "sla_hours":   float(form.get("sla_hours") or 24),
        "cadence":     (form.get("cadence") or "daily").strip(),
        "universe":    (form.get("universe") or "all").strip(),
        "description": (form.get("description") or "").strip() or f"strategy {name}",
    }

    trigger_signal = (form.get("trigger_signal") or "").strip()
    trigger_when = (form.get("trigger_when") or "").strip()
    if trigger_signal:
        spec["triggers"] = [{"signal": trigger_signal, "when": trigger_when}]

    gate_signals    = form.get("gate_signal", [])
    gate_whens      = form.get("gate_when", [])
    gate_windows    = form.get("gate_window", [])
    gate_stalenesses = form.get("gate_max_staleness", [])

    gates: List[dict] = []
    for sig, when, window, staleness in zip(
        gate_signals, gate_whens, gate_windows, gate_stalenesses
    ):
        sig = (sig or "").strip()
        when = (when or "").strip()
        window = (window or "").strip()
        staleness = (staleness or "").strip()
        if not sig or not when:
            continue
        g: Dict[str, Any] = {"signal": sig, "when": when}
        if window:
            g["window"] = window
        if staleness:
            g["max_staleness"] = staleness
        gates.append(g)
    if gates:
        spec["gates"] = gates

    emit_channel = (form.get("emit_channel") or "ntfy").strip()
    emit_cooldown = (form.get("emit_cooldown") or "").strip()
    spec["emit"] = {"channel": emit_channel}
    if emit_cooldown:
        spec["emit"]["cooldown"] = emit_cooldown

    return spec


# ── Validation ─────────────────────────────────────────────────────────

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_VERSION_RE = re.compile(r"^v\d+(\.\d+)*$")


@dataclass
class ValidationError:
    field: str
    message: str


def validate_spec(spec: dict) -> List[ValidationError]:
    errors: List[ValidationError] = []

    name = spec.get("strategy") or ""
    if not name:
        errors.append(ValidationError("strategy", "name is required"))
    elif not _NAME_RE.match(name):
        errors.append(ValidationError(
            "strategy",
            "use lowercase letters, digits, and underscores; start with a letter",
        ))

    version = spec.get("version") or ""
    if not _VERSION_RE.match(version):
        errors.append(ValidationError(
            "version",
            "use vN or vN.M form (e.g. v1, v1.0)",
        ))

    triggers = spec.get("triggers") or []
    if len(triggers) != 1:
        errors.append(ValidationError(
            "trigger",
            "exactly one trigger is required (M1 limitation)",
        ))
    else:
        if not triggers[0].get("signal"):
            errors.append(ValidationError("trigger_signal", "trigger signal is required"))
        if not triggers[0].get("when"):
            errors.append(ValidationError("trigger_when", "trigger predicate is required"))

    for i, gate in enumerate(spec.get("gates") or []):
        if not gate.get("signal"):
            errors.append(ValidationError(f"gate_{i}_signal", f"gate {i+1}: signal is required"))
        if not gate.get("when"):
            errors.append(ValidationError(f"gate_{i}_when", f"gate {i+1}: predicate is required"))

    # Try compiling: catches obviously bad spec shape before save/dryrun.
    try:
        make_strategy_class(spec)
    except Exception as exc:
        errors.append(ValidationError(
            "spec",
            f"can't construct strategy class: {exc}",
        ))

    return errors


# ── YAML rendering ─────────────────────────────────────────────────────

def render_yaml(spec: dict) -> str:
    """Produce a clean, human-readable YAML for the spec.

    Uses block style, preserves field order, multiline description.
    """
    # We control field order by handing yaml a dict with insertion order.
    ordered: Dict[str, Any] = {}
    for key in ("strategy", "version", "owner", "sla_hours", "cadence", "universe", "description"):
        if key in spec:
            ordered[key] = spec[key]
    if "triggers" in spec:
        ordered["triggers"] = spec["triggers"]
    if "gates" in spec:
        ordered["gates"] = spec["gates"]
    if "emit" in spec:
        ordered["emit"] = spec["emit"]
    return yaml.dump(
        ordered, sort_keys=False, default_flow_style=False, width=88,
        allow_unicode=True,
    )


# ── Save ───────────────────────────────────────────────────────────────

def yaml_path_for(name: str, version: str) -> Path:
    return STRATEGIES_DIR / f"{name}.{version}.yaml"


def save_yaml(spec: dict, overwrite: bool = False) -> Path:
    """Write the YAML to dataplane/strategies/<name>.<version>.yaml.

    Returns the path. Raises FileExistsError if the file exists and
    overwrite is False — caller decides whether to allow updates.
    """
    STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)
    p = yaml_path_for(spec["strategy"], spec["version"])
    if p.exists() and not overwrite:
        raise FileExistsError(str(p))
    p.write_text(render_yaml(spec))
    return p


# ── Dry-run ────────────────────────────────────────────────────────────

@dataclass
class DryRunResult:
    spec: dict
    from_date: str
    to_date: str
    total: int = 0
    triggered: int = 0
    outcomes: Dict[str, int] = field(default_factory=dict)
    sample_triggered: List[dict] = field(default_factory=list)


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


def dry_run(spec: dict, from_date: str, to_date: str, sample: int = 20) -> DryRunResult:
    """Run the candidate strategy against existing data without persisting.

    Walks each day in the window, calls materialize_partition, counts
    outcomes. No write_observation calls — the desk DB stays clean.
    """
    cls = make_strategy_class(spec)
    result = DryRunResult(spec=spec, from_date=from_date, to_date=to_date)
    start = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc)

    with _conn() as conn:
        signal = cls(conn=conn)
        cur = start
        while cur <= end:
            try:
                obs_list = signal.materialize_partition(cur)
            except Exception as exc:
                result.outcomes[f"materialize error: {exc}"] = result.outcomes.get(
                    f"materialize error: {exc}", 0
                ) + 1
                cur = cur + timedelta(days=1)
                continue
            for obs in obs_list:
                result.total += 1
                v = obs.value if isinstance(obs.value, dict) else {}
                triggered = bool(v.get("triggered"))
                if triggered:
                    result.triggered += 1
                    result.outcomes["(triggered)"] = result.outcomes.get("(triggered)", 0) + 1
                    if len(result.sample_triggered) < sample:
                        result.sample_triggered.append({
                            "ticker": obs.ticker,
                            "as_of": obs.as_of_date.isoformat(),
                            "trigger": v.get("trigger_value", {}),
                            "gates": v.get("gates", []),
                        })
                else:
                    reason = v.get("fail_reason") or "(suppressed)"
                    result.outcomes[reason] = result.outcomes.get(reason, 0) + 1
            cur = cur + timedelta(days=1)

    return result


# ── Helpers for the composer page ──────────────────────────────────────

def _major_version(version: str) -> str:
    """'v3.0.1' → 'v3'; 'v1' → 'v1'."""
    m = re.match(r"^(v\d+)", version or "")
    return m.group(1) if m else (version or "v1")


def signals_for_composer() -> List[Tuple[str, str, dict]]:
    """Return (signal_id_pinned_to_major, signal_class, output_schema)
    for every non-strategy signal. The pinned id is what strategy YAMLs
    reference (e.g. insider.trades.raw.v1) — version-family pinning."""
    sql = """
        SELECT signal_id, version, signal_class::text, output_schema::text
          FROM signal_definitions
         WHERE status = 'active'
           AND signal_class != 'composite'
         ORDER BY signal_class, signal_id
    """
    out: List[Tuple[str, str, dict]] = []
    with _conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            for sid, version, klass, schema in cur.fetchall():
                pinned = f"{sid}.{_major_version(version)}"
                try:
                    out.append((pinned, klass, json.loads(schema or "{}")))
                except Exception:
                    out.append((pinned, klass, {}))
        finally:
            cur.close()
    return out
