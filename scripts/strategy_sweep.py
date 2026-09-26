#!/usr/bin/env python3
"""Parameter sweep over the real simulator, into a sandbox, with walk-forward.

WHY A HARNESS RATHER THAN EDITING YAMLS

Every number in the three published configs was swept on a dataset that held
48.6% of the filings, with career_grade computed over a population that was
82% compensation grants and option exercises. Both are fixed. The configs are
therefore unmoored from the evidence that chose them, and re-deriving them by
hand-editing yaml and eyeballing CAGR is how they got overfit the first time.

THREE RULES THIS ENFORCES

1. SANDBOX ONLY. Every run writes to --table, never strategy_portfolio. A
   re-simulation writing into the table the site reads is precisely how
   14.3/28.9/0.1 CAGR reached live subscribers on 2026-08-27.

2. WALK-FORWARD, NOT IN-SAMPLE. --folds splits the window into consecutive
   periods and reports each separately. A parameter that only wins in-sample
   shows up here as a config that wins one fold and loses the rest. The
   published min_conviction=1.5 survived exactly this test in August; nothing
   else has had it applied.

3. THE BAND, NOT THE POINT. Conviction is built from ~12 half-point components,
   so a +/-0.25 nudge pushes a whole cohort across the gate. Sweeps report
   every fold so the spread is visible rather than a single flattering number.

4. SCORE WHAT THE SITE PUBLISHES. Added 2026-09-25. This used to report only
   the SLEEVE CAGR parsed out of the simulator's log line — uninvested capital
   earning nothing, no benchmark, no drawdown. Three ways that picks the wrong
   config: it flatters a book for being idle, it cannot see that A-List's 3x33%
   sizing costs a 77% drawdown, and it has no idea whether the book beat SPY.
   Every fold now reports blended CAGR, SPY over the identical window, the
   excess, and the DAILY max drawdown, all from
   framework.analysis.blended — the same function that computes
   summary.blended_cagr for the site. A sweep that scores a different quantity
   than the site publishes cannot choose a config for the site.

Usage:
    python3 scripts/strategy_sweep.py --strategy reversal_dip \\
        --set filters.min_consecutive_sells=5,10 \\
        --set filters.min_dip_3mo=-0.25,-0.15 \\
        --start 2016-01-01 --folds 3
"""
from __future__ import annotations

import argparse
import copy
import itertools
import json
import logging
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection  # noqa: E402
from framework.analysis.blended import blended_and_benchmark  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SIM = REPO / "pipelines/insider_study/simulate_strategy_portfolio.py"
BASE_YAML = REPO / "strategies/cw_strategies/configs"


def set_path(cfg: dict, dotted: str, value):
    node = cfg
    parts = dotted.split(".")
    for p in parts[:-1]:
        node = node.setdefault(p, {})
    node[parts[-1]] = value


def coerce(v: str):
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    if v.lower() in ("true", "false"):
        return v.lower() == "true"
    if v.lower() in ("null", "none"):
        return None
    if v.startswith("[") and v.endswith("]"):
        return [x.strip().strip('"\'') for x in v[1:-1].split("|") if x.strip()]
    return v


def folds(start: str, end: str, n: int):
    """Consecutive, non-overlapping periods. Equal calendar length."""
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    span = (e - s).days // n
    out = []
    for i in range(n):
        a = s.fromordinal(s.toordinal() + i * span)
        b = s.fromordinal(s.toordinal() + (i + 1) * span) if i < n - 1 else e
        out.append((a.isoformat(), b.isoformat()))
    return out


def run_one(strategy: str, yaml_path: Path, table: str,
            start: str, end: str, cfg_slots: int | None = None) -> dict | None:
    """One simulation. Returns the summary line's numbers."""
    cmd = [sys.executable, str(SIM), "--strategy", strategy, "--rebuild",
           "--table", table, "--start", start, "--end", end,
           "--config", str(yaml_path)]
    p = subprocess.run(cmd, capture_output=True, text=True,
                       env={**__import__("os").environ,
                            "PYTHONPATH": str(REPO),
                            "PGOPTIONS": "-c lock_timeout=5s"})
    out = p.stdout + p.stderr
    if p.returncode != 0:
        logger.warning("run failed: %s", out.strip().splitlines()[-3:])
        return None
    import re
    m = re.search(r"sim done in [\d.]+s — closed=(\d+), open=(\d+), "
                  r"final_equity=\$(-?[\d,]+)", out)
    if not m:
        return None
    closed, open_, eq = int(m.group(1)), int(m.group(2)), float(m.group(3).replace(",", ""))
    slots = int(cfg_slots or 3)
    years = max((date.fromisoformat(end) - date.fromisoformat(start)).days / 365.25, 0.01)
    row = {"closed": closed, "open": open_, "final_equity": eq,
           "total_return_pct": round(100.0 * (eq / 100_000.0 - 1), 2),
           "cagr_sleeve_pct": round(100.0 * ((eq / 100_000.0) ** (1 / years) - 1), 2)}
    row.update(cost_adjusted(eq / 100_000.0, closed, slots, years))
    row.update(published_metrics(strategy, table, start, end))
    return row


def published_metrics(strategy: str, table: str, start: str, end: str) -> dict:
    """Blended CAGR, SPY over the identical window, excess, daily max drawdown.

    Read from the SANDBOX table through the same function that computes
    `summary.blended_cagr` for the site, so a sweep and the published page
    cannot disagree about what a book returned.

    `years` mirrors the API exactly: FIRST ENTRY -> the window's end, not the
    window's whole length. A book that starts trading late is measured over the
    span it actually ran, and one that stops early still carries the idle tail.
    """
    blank = {"blended_cagr_pct": None, "spy_cagr_pct": None,
             "excess_pct": None, "max_dd_daily_pct": None, "win_rate_pct": None}
    try:
        conn = get_connection(readonly=True)
    except Exception as exc:                       # pragma: no cover
        logger.warning("scoring: no DB connection (%s)", exc)
        return blank
    try:
        agg = conn.execute(
            f"SELECT MIN(entry_date) AS first_entry, COUNT(*) AS n, "
            f"       SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins, "
            f"       SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS n_closed "
            f"  FROM {table} WHERE strategy = ? AND execution_source = 'simulated'",
            (strategy,)).fetchone()
        first_entry = agg["first_entry"] if agg else None
        if not first_entry:
            return blank
        years = max((date.fromisoformat(end) - date.fromisoformat(first_entry[:10])).days
                    / 365.25, 0.01)
        bl = blended_and_benchmark(conn, strategy, 100_000.0, years,
                                   table=table, end=end)
        if not bl:
            return blank
        n_closed = int(agg["n_closed"] or 0)
        return {
            "blended_cagr_pct": round(bl["cagr"], 2),
            "spy_cagr_pct": round(bl["spy"], 2),
            "excess_pct": round(bl["cagr"] - bl["spy"], 2),
            "max_dd_daily_pct": bl["max_dd_daily"],
            "win_rate_pct": (round(100.0 * int(agg["wins"] or 0) / n_closed, 1)
                             if n_closed else None),
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def cost_adjusted(growth: float, n_trades: int, slots: int, years: float) -> dict:
    """CAGR after a round-trip cost charged once per closed position.

    THE SIMULATOR MODELS NO TRANSACTION COSTS, and without this a sweep will
    happily recommend the config with the most turnover. On A-List over
    2016-2026 the gross table says hold_days=10 returns 1167% against 516% for
    the shipped 42 -- but 10 does 430 trades to 42's 161, and once a round trip
    is charged the ranking INVERTS:

        hold   @0%     @1%     @2%
          10   26.9%   11.1%   -2.8%
          42   18.6%   12.9%   +7.5%

    Acting on the gross number would have tripled turnover to destroy the book.
    Insider strategies trade small caps, where a 1% round trip is optimistic.

    THE DIRECTION OF THE ERROR IS THE OPPOSITE OF WHAT THIS ONCE CLAIMED.

    An earlier version said the model overstates drag because the book sits
    ~40% in cash, and to read @1% as a floor. That is wrong. Positions are
    sized as a fixed fraction of equity and position_size_pct x max_concurrent
    = 1.0 on all three books, so per-slot capital compounds as
    C_{j+1} = C_j(p_j - c) -- exactly what is computed here. Idle time is
    already captured by the measured trade COUNT; it does not reduce the cost
    of the trades that did happen. The model is exact when idle cash earns
    nothing, and when idle cash earns something (the books overlay SPY) the
    per-trade gross factor p is smaller than the g derived from total growth,
    so c/p > c/g and the model UNDERSTATES the drag.

    Read @1% as a CEILING on net performance, not a floor.

    One more caveat: the result depends on `slots`. The hold=42 over hold=10
    inversion above is computed at slots=3 (A-List). At slots=5 (Insider
    Breakout) the same growth and trade counts give hold=10 at 17.3% against
    hold=42 at 15.0% -- the inversion REVERSES. Do not quote one book's table
    for another. Positions still open at --end are also excluded from
    n_trades, so every fold understates cost slightly.
    """
    out = {}
    per_slot = max(n_trades / max(slots, 1), 1e-9)
    if growth <= 0 or n_trades <= 0:
        return {f"cagr_at_{int(c*1000)}bp_pct": None for c in (0.005, 0.01, 0.02)}
    g = growth ** (1.0 / per_slot)
    for c in (0.005, 0.01, 0.02):
        net = g - c
        out[f"cagr_at_{int(c*1000)}bp_pct"] = (
            round(100.0 * (net ** per_slot) ** (1 / years) - 100.0, 2)
            if net > 0 else None)
    return out


def _n(v) -> str:
    return "--" if v is None else f"{v:+.1f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", required=True)
    ap.add_argument("--set", action="append", default=[],
                    help="dotted.path=v1,v2,v3 — swept as a grid")
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--folds", type=int, default=1)
    ap.add_argument("--table", default="strategy_portfolio_exp")
    ap.add_argument("--json-out", default=None,
                    help="write the full result list here as well as stdout")
    args = ap.parse_args()
    if args.table == "strategy_portfolio":
        ap.error("--table must be a sandbox, never the published book")

    base = yaml.safe_load((BASE_YAML / f"{args.strategy}.yaml").read_text())

    axes = []
    for spec in args.set:
        path, _, vals = spec.partition("=")
        axes.append((path, [coerce(v) for v in vals.split(",")]))

    combos = list(itertools.product(*[v for _, v in axes])) if axes else [()]
    windows = folds(args.start, args.end, args.folds)

    logger.info("%d config(s) x %d fold(s) = %d simulations",
                len(combos), len(windows), len(combos) * len(windows))

    results = []
    with tempfile.TemporaryDirectory() as td:
        for combo in combos:
            cfg = copy.deepcopy(base)
            label = []
            for (path, _), val in zip(axes, combo):
                set_path(cfg, path, val)
                label.append(f"{path.split('.')[-1]}={val}")
            ypath = Path(td) / "variant.yaml"
            ypath.write_text(yaml.safe_dump(cfg))

            row = {"config": ", ".join(label) or "(base)", "folds": []}
            for a, b in windows:
                r = run_one(args.strategy, ypath, args.table, a, b,
                            cfg.get('max_concurrent'))
                row["folds"].append({"start": a, "end": b, **(r or {})})
            results.append(row)
            fs = " | ".join(
                f"{f.get('closed','--'):>4}tr "
                f"bl {_n(f.get('blended_cagr_pct')):>7} "
                f"spy {_n(f.get('spy_cagr_pct')):>6} "
                f"exc {_n(f.get('excess_pct')):>7} "
                f"dd {_n(f.get('max_dd_daily_pct')):>5} "
                f"slv@1% {_n(f.get('cagr_at_10bp_pct')):>7}"
                for f in row["folds"])
            logger.info("  %-46s %s", row["config"], fs)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
