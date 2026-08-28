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
           "cagr_gross_pct": round(100.0 * ((eq / 100_000.0) ** (1 / years) - 1), 2)}
    row.update(cost_adjusted(eq / 100_000.0, closed, slots, years))
    return row


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

    The model charges one round trip per closed position against a fully
    committed slot, so it OVERSTATES drag to the extent the book sits in cash --
    A-List runs roughly 40% idle. Treat the @1% column as a floor, not a point
    estimate.
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", required=True)
    ap.add_argument("--set", action="append", default=[],
                    help="dotted.path=v1,v2,v3 — swept as a grid")
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--folds", type=int, default=1)
    ap.add_argument("--table", default="strategy_portfolio_exp")
    args = ap.parse_args()

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
                f"{f.get('cagr_gross_pct','--'):>7}%g "
                f"{f.get('cagr_at_10bp_pct','--'):>7}%@1%"
                for f in row["folds"])
            logger.info("  %-46s %s", row["config"], fs)

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
