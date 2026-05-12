#!/usr/bin/env python3
"""Walk-forward simulator: backfill trade_decision_audit for every P-trade
since each strategy's start date.

For every P-code insider trade in the lifetime of each strategy, this
simulator decides — using the SAME filter logic as cw_runner.scan_signals —
whether the strategy WOULD HAVE entered. Each decision produces audit rows:
one per filter stage, with pass/fail, reason, pit_grade, conviction, and a
feature snapshot. source='simulation' on every row, distinguishing it
sharply from live cw_runner output (source='live').

Determinism: the strategy's filter logic is deterministic given inputs.
This simulator and live cw_runner should produce IDENTICAL decisions for
the same (strategy, ticker, filing_date). Discrepancies indicate either a
non-determinism bug or input drift between simulation and live.

Usage (run on Studio — needs PG access):
    python3 -m pipelines.insider_study.simulate_decision_audit
    python3 -m pipelines.insider_study.simulate_decision_audit --strategy quality_momentum
    python3 -m pipelines.insider_study.simulate_decision_audit --start 2026-04-01
    python3 -m pipelines.insider_study.simulate_decision_audit --replace   # nukes existing simulation rows first
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

import yaml

from config.database import get_connection
from pipelines.insider_study.conviction_score import (
    compute_conviction,
    pit_score_to_grade,
    _categorize_insider,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Strategy registry ───────────────────────────────────────────────────────
# Source-of-truth: each strategy's start date and YAML config path.
# `started_at` matches paper_trading.py STRATEGIES.

STRATEGIES = [
    {
        "name": "quality_momentum",
        "config_path": REPO / "strategies/cw_strategies/configs/quality_momentum.yaml",
        "started_at": "2020-03-06",
    },
    {
        "name": "reversal_dip",
        "config_path": REPO / "strategies/cw_strategies/configs/reversal_dip.yaml",
        "started_at": "2020-03-06",  # same era; refine if needed
    },
    {
        "name": "tenb51_surprise",
        "config_path": REPO / "strategies/cw_strategies/configs/tenb51_surprise.yaml",
        "started_at": "2023-01-01",  # 10b5-1 reform window
    },
]


# ── Filter logic (mirrors cw_runner._build_thesis_query EXACTLY) ────────────

def evaluate_filters(thesis_filters: dict, trade: dict) -> tuple[bool, list[str]]:
    """Apply each filter clause against a single trade. Returns
    (all_passed, list_of_failed_reasons). Empty list when all pass.

    Mirrors cw_runner._build_thesis_query — kept in sync deliberately.
    Any logic change there must come back here too.
    """
    failures: list[str] = []

    # is_duplicate must be 0 (pre-filter at SQL level in live; we mirror)
    if trade.get("is_duplicate"):
        failures.append("is_duplicate=1 (excluded)")

    # is_rare_reversal == 1
    if thesis_filters.get("is_rare_reversal") and not trade.get("is_rare_reversal"):
        failures.append("is_rare_reversal != 1")

    # min_consecutive_sells
    if "min_consecutive_sells" in thesis_filters:
        v = trade.get("consecutive_sells_before")
        threshold = int(thesis_filters["min_consecutive_sells"])
        if v is None or v < threshold:
            failures.append(
                f"consecutive_sells_before={v} < required {threshold}"
            )

    # max_dip_1mo (e.g., -0.15)
    if "max_dip_1mo" in thesis_filters:
        v = trade.get("dip_1mo")
        threshold = float(thesis_filters["max_dip_1mo"])
        if v is None or v > threshold:
            failures.append(f"dip_1mo={v} > required {threshold}")

    # above_sma50 == 1
    if thesis_filters.get("above_sma50") and trade.get("above_sma50") != 1:
        failures.append(f"above_sma50={trade.get('above_sma50')} != 1")

    # above_sma200 == 1
    if thesis_filters.get("above_sma200") and trade.get("above_sma200") != 1:
        failures.append(f"above_sma200={trade.get('above_sma200')} != 1")

    # is_largest_ever == 1
    if thesis_filters.get("is_largest_ever") and trade.get("is_largest_ever") != 1:
        failures.append(f"is_largest_ever={trade.get('is_largest_ever')} != 1")

    # pit_grade in [...]  (Recent Form, V2 scorer — 1.5y half-life)
    if "pit_grade" in thesis_filters:
        grades = thesis_filters["pit_grade"]
        if isinstance(grades, str):
            grades = [grades]
        if trade.get("pit_grade") not in grades:
            failures.append(
                f"pit_grade={trade.get('pit_grade')!r} not in {grades}"
            )

    # career_grade in [...]  (Career Grade, V3 scorer — 5y half-life).
    # QM swapped from pit_grade → career_grade on 2026-05-07. Mirrors
    # cw_runner._build_thesis_query (lines 421-428).
    if "career_grade" in thesis_filters:
        grades = thesis_filters["career_grade"]
        if isinstance(grades, str):
            grades = [grades]
        if trade.get("career_grade") not in grades:
            failures.append(
                f"career_grade={trade.get('career_grade')!r} not in {grades}"
            )

    # min_dip_3mo (e.g., -0.25 — actual dip must be ≤ this)
    if "min_dip_3mo" in thesis_filters:
        v = trade.get("dip_3mo")
        threshold = float(thesis_filters["min_dip_3mo"])
        if v is None or v > threshold:
            failures.append(f"dip_3mo={v} > required ≤{threshold}")

    # exclude flags
    if thesis_filters.get("exclude_10b5_1") and trade.get("is_10b5_1"):
        failures.append("is_10b5_1=1 (exclude_10b5_1)")
    if thesis_filters.get("exclude_recurring") and trade.get("is_recurring"):
        failures.append("is_recurring=1 (exclude_recurring)")
    if thesis_filters.get("exclude_tax_sales") and trade.get("is_tax_sale"):
        failures.append("is_tax_sale=1 (exclude_tax_sales)")
    if thesis_filters.get("exclude_routine") and trade.get("cohen_routine"):
        failures.append("cohen_routine=1 (exclude_routine)")

    return len(failures) == 0, failures


# ── PIT lookup helpers ──────────────────────────────────────────────────────

def lookup_pit_grade(conn, insider_id: int, ticker: str, as_of: str) -> str | None:
    """PIT-correct insider_ticker_scores lookup. Mirrors cw_runner."""
    row = conn.execute(
        """SELECT blended_score FROM insider_ticker_scores
            WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC LIMIT 1""",
        (insider_id, ticker, as_of),
    ).fetchone()
    return pit_score_to_grade(row[0] if row else None)


def count_prior_10b5_1_sells(conn, insider_id: int, ticker: str, as_of: str) -> int:
    """Count historical 10b5-1 S-trades by this insider on this ticker
    BEFORE the as_of filing_date. PIT-correct."""
    row = conn.execute(
        """SELECT COUNT(*) FROM trades
            WHERE insider_id = ? AND ticker = ?
              AND trans_code = 'S' AND is_10b5_1 = 1
              AND filing_date < ?""",
        (insider_id, ticker, as_of),
    ).fetchone()
    return int(row[0]) if row else 0


# ── The simulator ──────────────────────────────────────────────────────────

def simulate_strategy(
    conn,
    strategy_name: str,
    config: dict,
    started_at: str,
    end_date: str,
    replace_existing: bool = False,
) -> dict:
    """Walk forward through every P-trade since `started_at` and decide
    what cw_runner WOULD have done. Writes per-stage rows to
    trade_decision_audit with source='simulation'.

    Returns counts: {evaluated, entered, dedup_skipped, filter_failed,
                     conviction_skipped, audit_rows}.
    """
    theses = config.get("theses", [])
    if not theses:
        if "filters" in config and "exit" in config:
            theses = [{"name": strategy_name,
                       "filters": config["filters"],
                       "exit": config["exit"]}]
    if not theses:
        raise ValueError(f"{strategy_name}: no theses or filters in config")

    min_conv_default = float(config.get("min_conviction", 5.0))
    hold_days = int(theses[0].get("exit", {}).get("hold_days", 30))
    max_concurrent = int(config.get("max_concurrent", 10))

    # Optionally clear prior simulation rows for this strategy
    if replace_existing:
        n = conn.execute(
            "DELETE FROM trade_decision_audit WHERE strategy = ? AND source = 'simulation'",
            (strategy_name,),
        ).rowcount
        conn.commit()
        logger.info("[%s] cleared %d existing simulation rows", strategy_name, n or 0)

    # Pull every P-trade in lifetime, with all filter columns + insider_id.
    # ORDER BY filing_date so we process chronologically; tracks held state.
    logger.info("[%s] loading P-trades since %s …", strategy_name, started_at)
    t0 = time.monotonic()
    rows = conn.execute(
        f"""
        SELECT
            t.trade_id, t.insider_id, t.ticker, t.filing_date, t.filed_at,
            t.price, t.title,
            COALESCE(i.display_name, i.name) AS insider_name,
            t.company,
            COALESCE(t.is_duplicate, 0)  AS is_duplicate,
            t.is_rare_reversal,
            t.consecutive_sells_before,
            t.dip_1mo, t.dip_3mo,
            t.above_sma50, t.above_sma200,
            t.is_csuite, t.is_largest_ever,
            t.is_10b5_1, t.is_recurring, t.is_tax_sale, t.cohen_routine,
            t.pit_grade, t.career_grade, t.signal_grade,
            t.pit_n_trades, t.pit_win_rate_7d
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trans_code = 'P'
          AND t.filing_date >= ?
          AND t.filing_date <= ?
        ORDER BY t.filing_date ASC, t.trade_id ASC
        """,
        (started_at, end_date),
    ).fetchall()
    logger.info("[%s] %d P-trades loaded in %.1fs",
                strategy_name, len(rows), time.monotonic() - t0)

    # State accrued during simulation (mimics live cw_runner state)
    held_until: dict[str, str] = {}    # ticker → exit_date (filing_date + hold_days, business-day approx)
    used_trade_ids: set[int] = set()

    counts = {
        "evaluated": 0,
        "entered": 0,
        "dedup_skipped": 0,
        "filter_failed": 0,
        "pit_lookup_only": 0,
        "min_10b5_1_skipped": 0,
        "conviction_skipped": 0,
        "capacity_skipped": 0,
        "audit_rows": 0,
    }

    audit_buffer: list[tuple] = []
    BATCH = 1000

    def _flush():
        if not audit_buffer:
            return
        try:
            conn.executemany(
                """INSERT INTO trade_decision_audit
                   (run_id, strategy, ticker, trade_id, filing_date, thesis,
                    stage, passed, reason, pit_grade, conviction, feature_snapshot,
                    source, ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'simulation', ?)""",
                audit_buffer,
            )
            conn.commit()
            counts["audit_rows"] += len(audit_buffer)
        except Exception as e:
            conn.rollback()
            logger.exception("[%s] flush of %d rows failed: %s",
                             strategy_name, len(audit_buffer), e)
        finally:
            audit_buffer.clear()

    def _audit(run_id, ticker, trade_id, filing_date, thesis, stage, passed,
               reason=None, pit_grade=None, conviction=None, snapshot=None,
               ts=None):
        audit_buffer.append((
            run_id, strategy_name, ticker, trade_id, filing_date, thesis,
            stage, passed, reason, pit_grade, conviction,
            json.dumps(snapshot) if snapshot is not None else None,
            ts,
        ))
        if len(audit_buffer) >= BATCH:
            _flush()

    last_progress = time.monotonic()

    # Bucket all P-trades by filing_date. Each day is processed as a unit:
    # pass 1 evaluates each trade through conviction; pass 2 sorts passing
    # candidates by conviction DESC (mirroring cw_runner.scan_signals' sort
    # at line 547) and applies max_concurrent + same-day-same-ticker dedup
    # in cw_runner.execute_entries' order. Without this two-pass structure,
    # the simulator picks the lowest-trade_id passing candidate per ticker
    # rather than the highest-conviction one, which diverges from live.
    from collections import defaultdict
    rows_by_date: dict[str, list] = defaultdict(list)
    for r in rows:
        if hasattr(r, "_asdict"):
            d = r._asdict()
        elif hasattr(r, "keys"):
            d = {k: r[k] for k in r.keys()}
        else:
            d = dict(r)
        rows_by_date[d["filing_date"]].append(d)

    thesis = theses[0]
    thesis_name = thesis["name"]
    thesis_filters = thesis.get("filters", {}) or {}

    for filing_date in sorted(rows_by_date.keys()):
        # Release any holds whose simulated exit has now passed
        for held_ticker, exit_date in list(held_until.items()):
            if exit_date <= filing_date:
                del held_until[held_ticker]

        run_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"simulation/{strategy_name}/{filing_date}",
        ))
        try:
            sim_ts = datetime.strptime(filing_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc,
            )
        except ValueError:
            sim_ts = datetime.now(timezone.utc)

        # Pass 1 — evaluate every P-trade for this date through to conviction.
        # held_until reflects state at start-of-day (exits already released
        # above) and is NOT mutated mid-pass. Passing candidates collected for
        # the capacity stage in pass 2.
        day_candidates: list[dict] = []

        for t in rows_by_date[filing_date]:
            counts["evaluated"] += 1
            ticker = t["ticker"]
            tid = t["trade_id"]

            # Stage 1: dedup
            if tid in used_trade_ids:
                _audit(run_id, ticker, tid, filing_date, thesis_name, "dedup", False,
                       reason="trade_id already in strategy_portfolio", ts=sim_ts)
                counts["dedup_skipped"] += 1
                continue
            if ticker in held_until:
                _audit(run_id, ticker, tid, filing_date, thesis_name, "dedup", False,
                       reason=f"ticker held until {held_until[ticker]} (open position)",
                       ts=sim_ts)
                counts["dedup_skipped"] += 1
                continue
            _audit(run_id, ticker, tid, filing_date, thesis_name, "dedup", True,
                   ts=sim_ts)

            # Stage 2: filter clauses
            passed_filters, failures = evaluate_filters(thesis_filters, t)
            if not passed_filters:
                _audit(run_id, ticker, tid, filing_date, thesis_name, "filter", False,
                       reason="; ".join(failures),
                       pit_grade=t.get("pit_grade"),
                       snapshot={
                           "is_rare_reversal": t.get("is_rare_reversal"),
                           "consecutive_sells_before": t.get("consecutive_sells_before"),
                           "dip_1mo": t.get("dip_1mo"),
                           "dip_3mo": t.get("dip_3mo"),
                           "above_sma50": t.get("above_sma50"),
                           "above_sma200": t.get("above_sma200"),
                           "is_largest_ever": t.get("is_largest_ever"),
                           "pit_grade": t.get("pit_grade"),
                           "is_10b5_1": t.get("is_10b5_1"),
                           "is_recurring": t.get("is_recurring"),
                           "is_tax_sale": t.get("is_tax_sale"),
                           "cohen_routine": t.get("cohen_routine"),
                           "insider_name": t.get("insider_name"),
                           "company": t.get("company"),
                       },
                       ts=sim_ts)
                counts["filter_failed"] += 1
                continue
            _audit(run_id, ticker, tid, filing_date, thesis_name, "filter", True,
                   reason=f"all {len(thesis_filters)} filter(s) passed",
                   pit_grade=t.get("pit_grade"), ts=sim_ts)

            # Stage 3: PIT lookup (trade-level pit_grade is already PIT-correct)
            pit_grade = t.get("pit_grade") or "C"
            _audit(run_id, ticker, tid, filing_date, thesis_name, "pit_lookup", True,
                   reason=f"pit_grade={pit_grade} (from trades column, PIT-as-of {filing_date})",
                   pit_grade=pit_grade, ts=sim_ts)

            # Stage 4: min_prior_10b5_1_sells (tenb51_surprise only)
            min_10b5_1 = thesis_filters.get("min_prior_10b5_1_sells")
            if min_10b5_1:
                n = count_prior_10b5_1_sells(conn, t["insider_id"], ticker, filing_date)
                if n < int(min_10b5_1):
                    _audit(run_id, ticker, tid, filing_date, thesis_name,
                           "min_10b5_1", False,
                           reason=f"prior_10b5_1_sells={n} < required {min_10b5_1}",
                           pit_grade=pit_grade, ts=sim_ts)
                    counts["min_10b5_1_skipped"] += 1
                    continue
                _audit(run_id, ticker, tid, filing_date, thesis_name,
                       "min_10b5_1", True,
                       reason=f"prior_10b5_1_sells={n} >= {min_10b5_1}",
                       pit_grade=pit_grade, ts=sim_ts)

            # Stage 5: conviction
            conv = compute_conviction(
                thesis=thesis_name,
                signal_grade=pit_grade,
                consecutive_sells=t.get("consecutive_sells_before"),
                dip_1mo=t.get("dip_1mo"),
                is_largest_ever=bool(t.get("is_largest_ever")),
                above_sma50=bool(t.get("above_sma50")),
                above_sma200=bool(t.get("above_sma200")),
                insider_title=t.get("title"),
                is_csuite=bool(t.get("is_csuite")),
            )
            role = _categorize_insider(t.get("title"), bool(t.get("is_csuite")))
            snap = {
                "consecutive_sells_before": t.get("consecutive_sells_before"),
                "dip_1mo": t.get("dip_1mo"),
                "is_largest_ever": bool(t.get("is_largest_ever")),
                "above_sma50": bool(t.get("above_sma50")),
                "above_sma200": bool(t.get("above_sma200")),
                "insider_title": t.get("title"),
                "is_csuite": bool(t.get("is_csuite")),
                "insider_name": t.get("insider_name"),
                "company": t.get("company"),
                "role": role,
            }
            if conv < min_conv_default:
                _audit(run_id, ticker, tid, filing_date, thesis_name,
                       "conviction", False,
                       reason=f"conv={conv:.1f} < threshold {min_conv_default:.1f} grade={pit_grade} role={role}",
                       pit_grade=pit_grade, conviction=conv, snapshot=snap, ts=sim_ts)
                counts["conviction_skipped"] += 1
                continue
            _audit(run_id, ticker, tid, filing_date, thesis_name,
                   "conviction", True,
                   reason=f"conv={conv:.1f} >= threshold {min_conv_default:.1f} grade={pit_grade} role={role}",
                   pit_grade=pit_grade, conviction=conv, snapshot=snap, ts=sim_ts)

            day_candidates.append({
                "tid": tid,
                "ticker": ticker,
                "conv": conv,
                "pit_grade": pit_grade,
                "snap": snap,
                "role": role,
            })

        # Pass 2 — capacity stage. Sort by conviction DESC (matches
        # cw_runner.scan_signals ordering); same-day-same-ticker resolves
        # to the highest-conviction variant; max_concurrent enforced under
        # at_capacity=skip semantics.
        day_candidates.sort(key=lambda c: c["conv"], reverse=True)
        entered_today: set[str] = set()
        for cand in day_candidates:
            tid = cand["tid"]
            ticker = cand["ticker"]
            conv = cand["conv"]
            pit_grade = cand["pit_grade"]
            snap = cand["snap"]
            slots_used = len(held_until) + len(entered_today)

            if ticker in entered_today:
                _audit(run_id, ticker, tid, filing_date, thesis_name,
                       "capacity", False,
                       reason=("same-day same-ticker — higher-conviction "
                               "variant taken first"),
                       pit_grade=pit_grade, conviction=conv, snapshot=snap,
                       ts=sim_ts)
                counts["capacity_skipped"] += 1
                continue

            if slots_used >= max_concurrent:
                _audit(run_id, ticker, tid, filing_date, thesis_name,
                       "capacity", False,
                       reason=(f"at max_concurrent={max_concurrent} "
                               f"(open={len(held_until)} entered_today={len(entered_today)}) "
                               f"at_capacity=skip"),
                       pit_grade=pit_grade, conviction=conv, snapshot=snap,
                       ts=sim_ts)
                counts["capacity_skipped"] += 1
                continue

            _audit(run_id, ticker, tid, filing_date, thesis_name,
                   "capacity", True,
                   reason=f"slot {slots_used + 1}/{max_concurrent} "
                          f"(open={len(held_until)} entered_today={len(entered_today)})",
                   pit_grade=pit_grade, conviction=conv, snapshot=snap,
                   ts=sim_ts)
            used_trade_ids.add(tid)
            entered_today.add(ticker)
            counts["entered"] += 1

            try:
                entry_dt = datetime.strptime(filing_date, "%Y-%m-%d")
                exit_dt = entry_dt + timedelta(days=int(hold_days * 1.4))
                held_until[ticker] = exit_dt.strftime("%Y-%m-%d")
            except Exception:
                held_until[ticker] = filing_date

        # Periodic progress
        if time.monotonic() - last_progress > 5.0:
            elapsed = time.monotonic() - t0
            rate = counts["evaluated"] / elapsed if elapsed > 0 else 0
            logger.info(
                "[%s] %d/%d evaluated (%d entered, %d capacity_skipped, "
                "%d audit queued) %.0f trades/s",
                strategy_name, counts["evaluated"], len(rows),
                counts["entered"], counts["capacity_skipped"],
                len(audit_buffer), rate,
            )
            last_progress = time.monotonic()

    _flush()
    return counts


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=[s["name"] for s in STRATEGIES],
                   help="Run only this strategy")
    p.add_argument("--start", help="Override start date (YYYY-MM-DD)")
    p.add_argument("--end", help="End date (default: today)")
    p.add_argument("--replace", action="store_true",
                   help="Delete existing simulation rows for the strategy first")
    args = p.parse_args()

    end = args.end or date.today().isoformat()
    conn = get_connection()
    grand_total = 0

    for s in STRATEGIES:
        if args.strategy and s["name"] != args.strategy:
            continue
        config = yaml.safe_load(s["config_path"].read_text())
        started = args.start or s["started_at"]
        logger.info(
            "─" * 60 + "\n[%s] simulating %s → %s",
            s["name"], started, end,
        )
        t_strategy = time.monotonic()
        counts = simulate_strategy(conn, s["name"], config, started, end,
                                    replace_existing=args.replace)
        elapsed = time.monotonic() - t_strategy
        logger.info(
            "[%s] DONE in %.1fs — evaluated=%d entered=%d dedup=%d "
            "filter_failed=%d conviction_skipped=%d 10b5_1_skipped=%d "
            "capacity_skipped=%d audit=%d",
            s["name"], elapsed,
            counts["evaluated"], counts["entered"],
            counts["dedup_skipped"], counts["filter_failed"],
            counts["conviction_skipped"], counts["min_10b5_1_skipped"],
            counts["capacity_skipped"], counts["audit_rows"],
        )
        grand_total += counts["audit_rows"]

    conn.close()
    logger.info("Simulation complete. %d total simulation audit rows written.",
                grand_total)


if __name__ == "__main__":
    main()
