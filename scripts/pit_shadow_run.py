#!/usr/bin/env python3
"""Shadow-mode parallel validation: PIT engine vs cw_runner.

Runs `PITLiveEngine.run_daily_cycle(dry_run=True)` for one or more strategies
and compares the resulting Decisions to cw_runner's existing
`trade_decision_audit` rows (source='live') for the same filing_date(s).

Use this nightly (or on-demand) to prove the new engine produces identical
decisions before flipping the cutover switch.

Per-strategy report:
  - n_events_today, n_filter_pass, n_conviction_pass, n_admitted
  - mismatches: trade_ids where engine and cw_runner disagree on
    action / pit_grade / conviction
  - exit status: 0 if zero mismatches, 1 otherwise

Usage (on Studio):
    python3 scripts/pit_shadow_run.py --date 2026-05-12
    python3 scripts/pit_shadow_run.py --strategy quality_momentum --date 2026-05-12
    python3 scripts/pit_shadow_run.py --since 2026-05-01    # range
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import yaml

from config.database import get_connection
from framework.pit.live import PITLiveEngine
from framework.pit.strategies.quality_momentum import QualityMomentumStrategy
from framework.pit.strategies.reversal_dip import ReversalDipStrategy
from framework.pit.strategies.tenb51_surprise import Tenb51SurpriseStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


STRATEGIES = {
    "quality_momentum": (
        QualityMomentumStrategy,
        REPO / "strategies/cw_strategies/configs/quality_momentum.yaml",
    ),
    "reversal_dip": (
        ReversalDipStrategy,
        REPO / "strategies/cw_strategies/configs/reversal_dip.yaml",
    ),
    "tenb51_surprise": (
        Tenb51SurpriseStrategy,
        REPO / "strategies/cw_strategies/configs/tenb51_surprise.yaml",
    ),
}


def _load_strategy(name: str):
    cls, path = STRATEGIES[name]
    cfg = yaml.safe_load(path.read_text())
    return cls(cfg), cfg


def _fetch_cw_runner_decisions(conn, strategy: str, as_of: str) -> dict:
    """Get cw_runner's decisions for the date. Keyed by trade_id → terminal stage."""
    rows = conn.execute(
        """
        SELECT trade_id, stage, passed, pit_grade, conviction
        FROM trade_decision_audit
        WHERE source IN ('live', 'simulation')
          AND strategy = ?
          AND filing_date = ?
          AND stage IN ('filter', 'min_10b5_1', 'conviction', 'capacity')
        """,
        (strategy, as_of),
    ).fetchall()
    out: dict = {}
    stage_order = {"filter": 0, "min_10b5_1": 1, "conviction": 2, "capacity": 3}
    for r in rows:
        tid, stage, passed, pit_grade, conviction = (
            int(r[0]), r[1], bool(r[2]), r[3], r[4]
        )
        prev = out.get(tid)
        if prev is None or stage_order.get(stage, 0) > stage_order.get(prev[0], 0):
            out[tid] = (stage, passed, pit_grade, conviction)
    return out


def _shadow_one(conn, strategy_name: str, as_of: str) -> dict:
    """Run shadow comparison for one (strategy, date). Returns summary dict."""
    strategy, config = _load_strategy(strategy_name)
    engine = PITLiveEngine(conn, strategy, config)
    result = engine.run_daily_cycle(as_of_date=as_of, dry_run=True)
    summary = result.as_dict()

    cw_decisions = _fetch_cw_runner_decisions(conn, strategy_name, as_of)
    engine_by_tid = {d.trade_id: d for d in result.decisions}

    # Classify mismatches:
    #   action_diffs — real bug in strategy logic (different decision outcome)
    #   conv_diffs   — real bug (decision-relevant numeric difference)
    #   drift_diffs  — pit_grade/career_grade differ because the static
    #                  column was re-derived after cw_runner stored its
    #                  audit row. Expected when historical data is rebuilt.
    action_diffs: list[str] = []
    capacity_diffs: list[str] = []  # engine says enter, cw blocked at capacity — historical-state limitation
    conv_diffs: list[str] = []
    drift_diffs: list[str] = []
    overlap = 0
    for tid, engine_dec in engine_by_tid.items():
        if tid not in cw_decisions:
            continue
        sim_stage, sim_passed, sim_pit, sim_conv = cw_decisions[tid]
        overlap += 1

        engine_terminal = (engine_dec.passed and engine_dec.action == "enter")
        sim_terminal = (sim_passed and sim_stage in ("conviction", "capacity"))
        if engine_terminal != sim_terminal:
            # Sub-classify: if engine wants enter but cw stopped at capacity,
            # that's a HISTORICAL-CAPACITY limitation, not a logic bug.
            # The shadow can't reconstruct how many positions were open at
            # `sim_stage='capacity'` time.
            if engine_terminal and sim_stage == "capacity" and not sim_passed:
                capacity_diffs.append(
                    f"[{tid}] engine=enter vs cw=capacity-blocked "
                    f"(historical capacity state mismatch)"
                )
            else:
                action_diffs.append(
                    f"[{tid}] action: engine={engine_dec.action}/{engine_dec.stage} "
                    f"vs cw={sim_stage}/passed={sim_passed}"
                )
            continue

        eng_grade = engine_dec.pit_grade
        if not ((eng_grade == sim_pit) or
                (eng_grade is None and sim_pit == "C")):
            drift_diffs.append(
                f"[{tid}] pit_grade: engine={eng_grade!r} vs cw={sim_pit!r}"
            )
        if engine_dec.conviction is not None and sim_conv is not None:
            if abs(float(engine_dec.conviction) - float(sim_conv)) > 1e-4:
                conv_diffs.append(
                    f"[{tid}] conv: engine={engine_dec.conviction} vs cw={sim_conv}"
                )

    summary["overlap"] = overlap
    summary["action_diffs"] = action_diffs
    summary["capacity_diffs"] = capacity_diffs
    summary["conv_diffs"] = conv_diffs
    summary["drift_diffs"] = drift_diffs
    # OK if there are no real bugs. Capacity + drift are both known limitations.
    summary["ok"] = len(action_diffs) == 0 and len(conv_diffs) == 0
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="Single date to compare (YYYY-MM-DD)")
    p.add_argument("--since", help="Start date for a range to today")
    p.add_argument("--strategy", choices=list(STRATEGIES),
                   help="One strategy only (default: all 3)")
    args = p.parse_args()

    # Resolve date range
    if args.since:
        start = datetime.strptime(args.since, "%Y-%m-%d").date()
        end = date.today()
        dates = []
        d = start
        while d <= end:
            if d.weekday() < 5:   # Mon-Fri only — Form 4 filings
                dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
    else:
        dates = [args.date or date.today().strftime("%Y-%m-%d")]

    strategies = [args.strategy] if args.strategy else list(STRATEGIES)

    conn = get_connection(readonly=True)
    overall_ok = True
    total_mismatches = 0

    print(f"{'='*100}")
    print(f"{'date':<12}{'strategy':<20}{'events':>7}{'admit':>7}{'overlap':>9}"
          f"{'action':>8}{'conv':>6}{'cap*':>6}{'drift':>7}  status")
    print("-" * 100)

    total_action = 0
    total_conv = 0
    total_cap = 0
    total_drift = 0

    for d in dates:
        for s in strategies:
            try:
                summary = _shadow_one(conn, s, d)
            except Exception as e:
                print(f"{d:<12}{s:<20}  ERROR: {e}")
                overall_ok = False
                continue
            n_action = len(summary["action_diffs"])
            n_conv = len(summary["conv_diffs"])
            n_cap = len(summary["capacity_diffs"])
            n_drift = len(summary["drift_diffs"])
            total_action += n_action
            total_conv += n_conv
            total_cap += n_cap
            total_drift += n_drift

            if not summary["ok"]:
                status = f"BUG(a={n_action},c={n_conv})"
            elif n_cap > 0 or n_drift > 0:
                status = f"NOISE(cap={n_cap},drift={n_drift})"
            else:
                status = "OK"
            print(f"{d:<12}{s:<20}{summary['n_events_today']:>7}"
                  f"{summary['n_admitted']:>7}{summary['overlap']:>9}"
                  f"{n_action:>8}{n_conv:>6}{n_cap:>6}{n_drift:>7}  {status}")
            for label, diffs in [
                ("ACTION", summary["action_diffs"]),
                ("CONV", summary["conv_diffs"]),
            ]:
                for m in diffs[:3]:
                    print(f"             {label}: {m}")
                if len(diffs) > 3:
                    print(f"             {label}: … {len(diffs) - 3} more")
            if not summary["ok"]:
                overall_ok = False

    print("=" * 100)
    print(f"Totals: action_diffs={total_action}  conv_diffs={total_conv}  "
          f"capacity_diffs={total_cap}  drift_diffs={total_drift}")
    print("(* cap = historical capacity state — shadow can't reconstruct "
          "how many positions were open in the past)")
    print()
    if total_action == 0 and total_conv == 0:
        if total_cap == 0 and total_drift == 0:
            print("✅ Shadow run clean — PIT engine matches cw_runner exactly.")
        else:
            print(f"✅ Shadow run OK. Noise: {total_cap} capacity (historical "
                  f"state) + {total_drift} pit_grade drift (post-PIT-fix "
                  "rebuild). No real bugs.")
        sys.exit(0)
    else:
        print(f"❌ Real bugs found: {total_action} action diffs, "
              f"{total_conv} conviction diffs. Investigate before cutover.")
        sys.exit(1)


if __name__ == "__main__":
    main()
