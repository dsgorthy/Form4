"""Integration test: PITBacktestEngine reproduces simulate_decision_audit.

Runs the new rails (`framework/pit/engine.PITBacktestEngine` +
`framework/pit/strategies/quality_momentum.QualityMomentumStrategy`) over a
sample of historical filing_dates and compares the resulting Decisions to
the rows already in `trade_decision_audit` (source='simulation', written
by `pipelines/insider_study/simulate_decision_audit.py`).

We don't expect 100% match — the existing simulator handles
dedup/capacity/multi-thesis logic the Phase 1 engine doesn't yet. But for
trades that pass through the same filter+conviction path, action and
pit_grade and career_grade and conviction should agree.

This test REQUIRES the form4 PG database. Skipped if not reachable —
typically that means it's running on Mini (no local form4 DB).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

# Path setup for the conftest's repo-root insertion
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))


@pytest.fixture(scope="module")
def conn():
    """form4 DB connection. Skip if unavailable."""
    try:
        from config.database import get_connection
        c = get_connection()
        # Smoke probe
        c.execute("SELECT 1 FROM trades LIMIT 1").fetchone()
    except Exception as e:
        pytest.skip(f"form4 DB not reachable (run on Studio): {e}")
    yield c
    try:
        c.close()
    except Exception:
        pass


@pytest.fixture(scope="module")
def qm_strategy():
    cfg = yaml.safe_load((REPO / "strategies/cw_strategies/configs/quality_momentum.yaml").read_text())
    from framework.pit.strategies.quality_momentum import QualityMomentumStrategy
    return QualityMomentumStrategy(cfg)


@pytest.fixture(scope="module")
def rd_strategy():
    cfg = yaml.safe_load((REPO / "strategies/cw_strategies/configs/reversal_dip.yaml").read_text())
    from framework.pit.strategies.reversal_dip import ReversalDipStrategy
    return ReversalDipStrategy(cfg)


@pytest.fixture(scope="module")
def tenb51_strategy():
    cfg = yaml.safe_load((REPO / "strategies/cw_strategies/configs/tenb51_surprise.yaml").read_text())
    from framework.pit.strategies.tenb51_surprise import Tenb51SurpriseStrategy
    return Tenb51SurpriseStrategy(cfg)


def _pick_sample_filing_dates(conn, strategy: str = "quality_momentum",
                              n: int = 5) -> list[str]:
    """Sample n recent filing_dates that have a non-empty simulation
    footprint for the given strategy."""
    rows = conn.execute(
        """
        SELECT DISTINCT filing_date::text AS d
        FROM trade_decision_audit
        WHERE source = 'simulation'
          AND strategy = ?
          AND stage = 'conviction'
          AND passed IS NOT NULL
        ORDER BY d DESC
        LIMIT 100
        """,
        (strategy,),
    ).fetchall()
    if not rows:
        return []
    candidates = [r[0] for r in rows]
    step = max(1, len(candidates) // n)
    return candidates[::step][:n]


def _run_equivalence(conn, strategy_obj, strategy_name: str, n_sample: int = 5):
    """Generic equivalence harness — runs the engine against `strategy_obj`
    over sample dates from the simulator's audit table for `strategy_name`.
    Returns (compared, mismatches)."""
    from framework.pit.engine import PITBacktestEngine

    sample_dates = _pick_sample_filing_dates(conn, strategy_name, n=n_sample)
    if not sample_dates:
        pytest.skip(f"No simulation rows for {strategy_name}")

    engine = PITBacktestEngine(conn)
    mismatches: list[str] = []
    compared = 0

    for d in sample_dates:
        result = engine.run(strategy_obj, d, d, trading_day_loader=lambda a, b: [d])
        # Build dict[trade_id] → Decision from engine output
        engine_by_tid = {dec.trade_id: dec for dec in result.decisions}

        # Pull existing simulator rows for the same date
        sim_rows = conn.execute(
            """
            SELECT trade_id, stage, passed, pit_grade, conviction
            FROM trade_decision_audit
            WHERE source = 'simulation'
              AND strategy = ?
              AND filing_date = ?
              AND stage IN ('filter', 'conviction', 'min_10b5_1')
            """,
            (strategy_name, d),
        ).fetchall()

        # Group simulator rows by trade_id → terminal stage
        sim_by_tid: dict[int, tuple[str, bool, str, float]] = {}
        for r in sim_rows:
            tid, stage, passed, pit_grade, conviction = (
                int(r[0]), r[1], bool(r[2]), r[3], r[4]
            )
            # Take the most-progressed stage in pipeline order.
            stage_order = {"filter": 0, "min_10b5_1": 1, "conviction": 2}
            prev = sim_by_tid.get(tid)
            if prev is None or stage_order.get(stage, 0) > stage_order.get(prev[0], 0):
                sim_by_tid[tid] = (stage, passed, pit_grade, conviction)

        for tid, engine_dec in engine_by_tid.items():
            if tid not in sim_by_tid:
                continue
            sim_stage, sim_passed, sim_pit, sim_conv = sim_by_tid[tid]
            compared += 1
            engine_action = engine_dec.action
            engine_passed = engine_dec.passed
            # Map: if engine_dec.stage == sim_stage we can compare passed.
            if engine_dec.stage != sim_stage:
                # Different terminal stages — the engine reached a later
                # stage than the simulator (or vice versa). Accept iff
                # both agree on the broad action (skip vs enter).
                if (engine_action == "enter") != (sim_passed and sim_stage == "conviction"):
                    mismatches.append(
                        f"[{d}/{tid}] stage divergence: engine={engine_dec.stage}/"
                        f"{engine_action} vs sim={sim_stage}/passed={sim_passed}"
                    )
                continue

            if engine_passed != sim_passed:
                mismatches.append(
                    f"[{d}/{tid}] passed: engine={engine_passed} vs sim={sim_passed} "
                    f"(stage={sim_stage})"
                )
            # The legacy simulator coerces NULL pit_grade to 'C' before
            # writing to the audit table (display convention). The engine
            # preserves None. Treat None and 'C' as equivalent for the
            # NULL-fallback case; require exact match otherwise.
            eng_grade = engine_dec.pit_grade
            if not ((eng_grade == sim_pit) or
                    (eng_grade is None and sim_pit == "C")):
                mismatches.append(
                    f"[{d}/{tid}] pit_grade: engine={eng_grade!r} vs sim={sim_pit!r}"
                )
            if engine_dec.conviction is not None and sim_conv is not None:
                if abs(float(engine_dec.conviction) - float(sim_conv)) > 1e-4:
                    mismatches.append(
                        f"[{d}/{tid}] conviction: engine={engine_dec.conviction} vs sim={sim_conv}"
                    )

    return compared, mismatches


def test_qm_engine_decisions_match_simulator(conn, qm_strategy):
    compared, mismatches = _run_equivalence(conn, qm_strategy, "quality_momentum")
    assert compared > 0, "No overlapping trade_ids for QM"
    assert not mismatches, (
        f"{len(mismatches)} QM mismatches across {compared}:\n"
        + "\n".join(mismatches[:20])
    )


def test_rd_engine_decisions_match_simulator(conn, rd_strategy):
    compared, mismatches = _run_equivalence(conn, rd_strategy, "reversal_dip")
    assert compared > 0, "No overlapping trade_ids for RD"
    assert not mismatches, (
        f"{len(mismatches)} RD mismatches across {compared}:\n"
        + "\n".join(mismatches[:20])
    )


def test_tenb51_engine_decisions_match_simulator(conn, tenb51_strategy):
    compared, mismatches = _run_equivalence(conn, tenb51_strategy, "tenb51_surprise")
    assert compared > 0, "No overlapping trade_ids for 10b5"
    assert not mismatches, (
        f"{len(mismatches)} 10b5 mismatches across {compared}:\n"
        + "\n".join(mismatches[:20])
    )


def test_engine_audit_tape_clean(conn, qm_strategy):
    """The engine's BacktestResult must record max_knowledge_date <= as_of_date."""
    from framework.pit.engine import PITBacktestEngine

    sample = _pick_sample_filing_dates(conn, "quality_momentum", n=1)
    if not sample:
        pytest.skip("No sim rows to anchor")
    d = sample[0]

    engine = PITBacktestEngine(conn)
    result = engine.run(qm_strategy, d, d, trading_day_loader=lambda a, b: [d])
    assert result.max_knowledge_date_seen is not None
    assert result.max_knowledge_date_seen <= d, (
        f"PIT audit: knowledge_date {result.max_knowledge_date_seen} > as_of {d}"
    )
