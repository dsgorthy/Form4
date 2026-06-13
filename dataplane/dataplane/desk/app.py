"""Starlette + Jinja2 app for the Pyrrho Dataplane Desk.

Phase A — understanding:
  GET /                  home (status snapshot)
  GET /signals           catalog list
  GET /signals/{id}      signal detail (metadata + sample observations)
  GET /strategies        catalog filtered to composite signals
  GET /strategies/{id}   strategy detail (YAML + fail distribution + eval tape)
  GET /ticker            redirect (search box uses ?symbol=)
  GET /ticker/{symbol}   ticker view
  GET /pipelines         Dagster recent runs
  GET /pipelines/{id}    one-run failure trace
  GET /api/status.json   JSON (kept for tooling parity with the old desk)
  GET /healthz           ok
"""
from __future__ import annotations

import json
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from dataplane.backfill import backfill as run_backfill
from dataplane.desk import composer, queries
from dataplane.status import gather_status


_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STRATEGIES_DIR = Path(__file__).resolve().parents[2] / "strategies"

templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


# ── Template helpers ──────────────────────────────────────────────────

def fmt_ts(d: Optional[datetime]) -> str:
    if d is None:
        return "—"
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone().strftime("%Y-%m-%d %H:%M")


def fmt_age(td_hours: Optional[float]) -> str:
    if td_hours is None:
        return "—"
    if td_hours < 1:
        return f"{int(td_hours * 60)}m"
    if td_hours < 48:
        return f"{td_hours:.1f}h"
    return f"{td_hours / 24:.1f}d"


def run_color(status: str) -> str:
    return {
        "SUCCESS":  "green",
        "FAILURE":  "red",
        "STARTED":  "yellow",
        "CANCELED": "unknown",
        "QUEUED":   "unknown",
    }.get(status, "unknown")


def summarize_value(signal_class: str, value: dict) -> str:
    """Compact one-line preview of a value dict. Tailored per class.
    Uses HTML-safe text only (no <tags>) so we don't need autoescape off."""
    if not isinstance(value, dict):
        return str(value)[:120]
    sc = signal_class or ""
    if sc == "insider":
        parts = []
        for k in ("trade_type", "trans_code", "qty", "value", "insider_name"):
            if k in value and value[k] not in (None, ""):
                v = value[k]
                if k == "value" and isinstance(v, (int, float)):
                    parts.append(f"${v:,.0f}")
                else:
                    parts.append(f"{k}={v}")
        return "  ".join(parts) or json.dumps(value, default=str)[:120]
    if sc == "price":
        c = value.get("close")
        if isinstance(c, (int, float)):
            return f"close={c:.2f}  vol={value.get('volume', '?')}"
        return value.get("error") or value.get("status") or json.dumps(value, default=str)[:120]
    if sc == "composite":
        trig = bool(value.get("triggered"))
        return ("triggered" if trig else "suppressed: " + (value.get("fail_reason") or "?"))[:120]
    return json.dumps(value, default=str)[:120]


def summarize_value_generic(value: dict) -> str:
    return summarize_value("", value)


# ── Routes ────────────────────────────────────────────────────────────

async def home(request: Request):
    snap = gather_status()
    raw = [s for s in snap.signals if not s.is_strategy]
    strats = [s for s in snap.signals if s.is_strategy]
    return templates.TemplateResponse(request, "home.html", {
        "active": "home",
        "snap": snap,
        "raw_signals": raw,
        "strategies": strats,
        "strategy_outcomes": snap.strategies,
        "recent_runs": snap.recent_runs,
        "ts": snap.as_of.astimezone().strftime("%a %Y-%m-%d %H:%M %Z"),
        "fmt_ts": fmt_ts,
        "fmt_age": fmt_age,
        "run_color": run_color,
    })


async def signals_list(request: Request):
    signals = queries.all_signals()
    return templates.TemplateResponse(request, "signals_list.html", {
        "active": "signals",
        "signals": signals,
        "fmt_ts": fmt_ts,
        "fmt_age": fmt_age,
    })


async def strategies_list(request: Request):
    signals = [s for s in queries.all_signals() if s.is_strategy]
    return templates.TemplateResponse(request, "signals_list.html", {
        "active": "strategies",
        "signals": signals,
        "fmt_ts": fmt_ts,
        "fmt_age": fmt_age,
    })


async def signal_detail(request: Request):
    sid = request.path_params["signal_id"]
    signal = queries.get_signal(sid)
    if signal is None:
        return PlainTextResponse(f"unknown signal {sid}", status_code=404)
    if signal.is_strategy:
        return RedirectResponse(f"/strategies/{sid}", status_code=302)

    ticker_filter = (request.query_params.get("ticker") or "").strip() or None
    try:
        limit = int(request.query_params.get("limit") or 50)
    except ValueError:
        limit = 50

    observations = queries.recent_observations(sid, ticker_filter, limit)
    known_ids = {s.signal_id for s in queries.all_signals()}
    history = queries.signal_run_history(signal.signal_id, signal.version, limit=10)

    today = datetime.now(timezone.utc).date()
    default_from = (today - timedelta(days=7)).isoformat()
    default_to = today.isoformat()

    return templates.TemplateResponse(request, "signal_detail.html", {
        "active": "signals",
        "signal": signal,
        "observations": observations,
        "ticker_filter": ticker_filter,
        "limit": limit,
        "output_schema_pretty": json.dumps(signal.output_schema, indent=2),
        "known_signal_ids": known_ids,
        "history": history,
        "backfill_url": f"/signals/{sid}/backfill",
        "default_from": default_from,
        "default_to": default_to,
        "fmt_ts": fmt_ts,
        "fmt_age": fmt_age,
        "run_color": run_color,
        "summarize_value": summarize_value,
    })


async def strategy_detail(request: Request):
    sid = request.path_params["signal_id"]
    signal = queries.get_signal(sid)
    if signal is None:
        return PlainTextResponse(f"unknown strategy {sid}", status_code=404)
    if not signal.is_strategy:
        return RedirectResponse(f"/signals/{sid}", status_code=302)

    ticker_filter = (request.query_params.get("ticker") or "").strip() or None
    outcome_filter = (request.query_params.get("outcome") or "").strip() or None
    from_date = (request.query_params.get("from_date") or "").strip() or None
    to_date = (request.query_params.get("to_date") or "").strip() or None
    try:
        limit = int(request.query_params.get("limit") or 100)
    except ValueError:
        limit = 100

    evaluations = queries.strategy_evaluations(
        sid, ticker=ticker_filter, outcome=outcome_filter,
        from_date=from_date, to_date=to_date, limit=limit,
    )

    # Reuse status outcomes (24h/7d totals) for the header tile
    snap = gather_status(recent_runs_limit=0)
    outcomes = snap.strategies.get(sid)

    fail_dist = queries.fail_reason_distribution(sid)

    strategy_yaml = _load_strategy_yaml(sid)
    history = queries.signal_run_history(signal.signal_id, signal.version, limit=10)

    today = datetime.now(timezone.utc).date()
    default_from = (today - timedelta(days=30)).isoformat()
    default_to = today.isoformat()

    return templates.TemplateResponse(request, "strategy_detail.html", {
        "active": "strategies",
        "signal": signal,
        "outcomes": outcomes,
        "fail_distribution": fail_dist,
        "evaluations": evaluations,
        "ticker_filter": ticker_filter,
        "outcome_filter": outcome_filter,
        "from_date": from_date,
        "to_date": to_date,
        "limit": limit,
        "strategy_yaml": strategy_yaml,
        "history": history,
        "backfill_url": f"/strategies/{sid}/backfill",
        "default_from": default_from,
        "default_to": default_to,
        "fmt_ts": fmt_ts,
        "fmt_age": fmt_age,
        "run_color": run_color,
        "summarize_value": summarize_value,
    })


def _load_strategy_yaml(signal_id: str) -> Optional[str]:
    """Return the raw YAML text for a strategy signal, if found in
    dataplane/strategies/<name>.<version>.yaml."""
    name = signal_id.removeprefix("strategy.")
    # Try each YAML in the folder; match strategy + version
    if not _STRATEGIES_DIR.exists():
        return None
    for p in _STRATEGIES_DIR.glob("*.yaml"):
        try:
            spec = yaml.safe_load(p.read_text())
            if spec.get("strategy") == name:
                return p.read_text()
        except Exception:
            continue
    return None


async def ticker_redirect(request: Request):
    sym = (request.query_params.get("symbol") or "").strip().upper()
    if not sym:
        return RedirectResponse("/", status_code=302)
    return RedirectResponse(f"/ticker/{sym}", status_code=302)


async def ticker_view(request: Request):
    sym = request.path_params["symbol"].upper()
    rows = queries.ticker_summary(sym)
    return templates.TemplateResponse(request, "ticker.html", {
        "active": "ticker",
        "ticker": sym,
        "rows": rows,
        "fmt_ts": fmt_ts,
        "summarize_value_generic": summarize_value_generic,
    })


async def pipelines(request: Request):
    runs = queries.recent_runs(limit=40)
    return templates.TemplateResponse(request, "pipelines.html", {
        "active": "pipelines",
        "runs": runs,
        "fmt_ts": fmt_ts,
        "run_color": run_color,
    })


async def run_failure(request: Request):
    run_id = request.path_params["run_id"]
    message = queries.run_failure_message(run_id)
    return templates.TemplateResponse(request, "run_failure.html", {
        "active": "pipelines",
        "run_id": run_id,
        "message": message,
        "fmt_ts": fmt_ts,
    })


async def status_json(request: Request):
    snap = gather_status()
    # Reuse the old _snapshot_to_json shape — inline for tooling parity.
    def sig(s):
        return {
            "signal_id":          s.signal_id,
            "version":            s.version,
            "signal_class":       s.signal_class,
            "owner":              s.owner,
            "sla_hours":          s.sla_hours,
            "row_count":          s.row_count,
            "rows_24h":           s.rows_24h,
            "rows_7d":            s.rows_7d,
            "latest_ingested_at": s.latest_ingested_at.isoformat() if s.latest_ingested_at else None,
            "latest_as_of":       s.latest_as_of.isoformat() if s.latest_as_of else None,
            "freshness_status":   s.freshness_status,
            "age_hours":          s.age_hours,
            "is_strategy":        s.is_strategy,
        }
    return JSONResponse({
        "as_of":         snap.as_of.isoformat(),
        "healthy":       snap.healthy_pipelines,
        "total":         snap.non_strategy_count,
        "evals_24h":     snap.total_evals_24h,
        "triggered_24h": snap.total_triggered_24h,
        "signals":       [sig(s) for s in snap.signals],
    })


async def healthz(request: Request):
    return PlainTextResponse("ok")


# ── Composer routes (Phase B) ──────────────────────────────────────────

async def new_strategy_form(request: Request):
    return templates.TemplateResponse(request, "new_strategy.html", {
        "active": "new",
        "available_signals": composer.signals_for_composer(),
    })


def _form_to_spec_dict(form_data) -> dict:
    """Translate Starlette's FormData into the shape composer.spec_from_form
    wants — single-value strings + list[str] for the repeated gate fields."""
    return {
        "strategy":           form_data.get("strategy", ""),
        "version":            form_data.get("version", ""),
        "owner":              form_data.get("owner", ""),
        "sla_hours":          form_data.get("sla_hours", "24"),
        "cadence":            form_data.get("cadence", "daily"),
        "universe":           form_data.get("universe", "all"),
        "description":        form_data.get("description", ""),
        "trigger_signal":     form_data.get("trigger_signal", ""),
        "trigger_when":       form_data.get("trigger_when", ""),
        "gate_signal":        form_data.getlist("gate_signal"),
        "gate_when":          form_data.getlist("gate_when"),
        "gate_window":        form_data.getlist("gate_window"),
        "gate_max_staleness": form_data.getlist("gate_max_staleness"),
        "emit_channel":       form_data.get("emit_channel", "ntfy"),
        "emit_cooldown":      form_data.get("emit_cooldown", ""),
    }


async def new_strategy_preview(request: Request):
    form = await request.form()
    flat = _form_to_spec_dict(form)
    spec = composer.spec_from_form(flat)
    return PlainTextResponse(composer.render_yaml(spec))


async def new_strategy_dryrun(request: Request):
    form = await request.form()
    flat = _form_to_spec_dict(form)
    spec = composer.spec_from_form(flat)
    errors = composer.validate_spec(spec)
    if errors:
        return templates.TemplateResponse(request, "_dryrun_result.html", {
            "errors": errors, "result": None, "outcomes_sorted": [],
        })
    # Default 90-day window ending today UTC
    today = datetime.now(timezone.utc).date()
    from_date = (today - timedelta(days=90)).isoformat()
    to_date = today.isoformat()
    try:
        result = composer.dry_run(spec, from_date, to_date)
    except Exception as exc:
        return templates.TemplateResponse(request, "_dryrun_result.html", {
            "errors": [composer.ValidationError("dryrun", f"dry-run failed: {exc}")],
            "result": None, "outcomes_sorted": [],
        })
    outcomes_sorted = sorted(result.outcomes.items(), key=lambda kv: -kv[1])
    return templates.TemplateResponse(request, "_dryrun_result.html", {
        "errors": [], "result": result, "outcomes_sorted": outcomes_sorted,
    })


async def new_strategy_save(request: Request):
    form = await request.form()
    flat = _form_to_spec_dict(form)
    spec = composer.spec_from_form(flat)
    errors = composer.validate_spec(spec)
    if errors:
        return templates.TemplateResponse(request, "_save_result.html", {
            "errors": errors,
        })
    overwrite = (request.query_params.get("overwrite") == "1")
    try:
        path = composer.save_yaml(spec, overwrite=overwrite)
        return templates.TemplateResponse(request, "_save_result.html", {
            "errors": [], "path": str(path), "name": spec["strategy"],
        })
    except FileExistsError as exc:
        return templates.TemplateResponse(request, "_save_result.html", {
            "errors": [], "file_exists": True, "path": str(exc),
            "name": spec["strategy"],
        })


async def new_strategy_gate_row(request: Request):
    return templates.TemplateResponse(request, "_gate_row.html", {
        "available_signals": composer.signals_for_composer(),
    })


# ── Backfill routes (Phase C) ──────────────────────────────────────────

async def backfill_signal(request: Request):
    """Run a backfill synchronously and return a result fragment.

    Same impl as the CLI's `dataplane backfill` — calls dataplane.backfill.
    Long ranges block the request thread; UI surfaces the limitation.
    """
    sid = request.path_params["signal_id"]
    return await _do_backfill(request, sid)


async def backfill_strategy(request: Request):
    sid = request.path_params["signal_id"]
    return await _do_backfill(request, sid)


async def _do_backfill(request: Request, signal_id: str):
    form = await request.form()
    from_date = (form.get("from_date") or "").strip()
    to_date = (form.get("to_date") or "").strip()
    tickers_raw = (form.get("tickers") or "").strip()
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()] or None

    started = datetime.now()
    try:
        result = run_backfill(
            signal_ref=signal_id,
            from_date=from_date,
            to_date=to_date,
            tickers=tickers,
        )
    except Exception as exc:
        return templates.TemplateResponse(request, "_backfill_result.html", {
            "error": str(exc),
        })
    duration = int((datetime.now() - started).total_seconds())

    # Tail the last 8 partition lines for visibility (most recent wins)
    tail = result.partitions[-8:] if len(result.partitions) > 8 else result.partitions
    lines = "\n".join(
        f"{p.partition_date}: {p.written:>6,} written"
        + (f"  ⚠ {p.errors} errors" if p.errors else "")
        for p in tail
    )

    return templates.TemplateResponse(request, "_backfill_result.html", {
        "result": result,
        "duration_seconds": duration,
        "recent_partition_lines": lines,
    })


# ── App factory ───────────────────────────────────────────────────────

app = Starlette(routes=[
    Route("/",                          home),
    Route("/signals",                   signals_list),
    Route("/signals/{signal_id}",       signal_detail),
    Route("/strategies",                strategies_list),
    Route("/strategies/{signal_id}",    strategy_detail),
    Route("/ticker",                    ticker_redirect),
    Route("/ticker/{symbol}",           ticker_view),
    Route("/pipelines",                 pipelines),
    Route("/pipelines/{run_id}",        run_failure),
    Route("/new/strategy",              new_strategy_form),
    Route("/new/strategy/preview",      new_strategy_preview, methods=["POST"]),
    Route("/new/strategy/dryrun",       new_strategy_dryrun,  methods=["POST"]),
    Route("/new/strategy/save",         new_strategy_save,    methods=["POST"]),
    Route("/new/strategy/gate-row",     new_strategy_gate_row),
    Route("/signals/{signal_id}/backfill",    backfill_signal,   methods=["POST"]),
    Route("/strategies/{signal_id}/backfill", backfill_strategy, methods=["POST"]),
    Route("/api/status.json",           status_json),
    Route("/healthz",                   healthz),
])
