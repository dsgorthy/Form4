"""OMS V2 scan/runner adapters — Phase 2 P2 day 3b.

Translates cw_runner.scan_signals to use framework.oms.decision.Decision
objects + framework.oms.audit.write_decisions. Behaviorally identical to
V1: same SQL, same candidate output, same audit row pattern.

Activation: cw_runner.scan_signals branches on env var `OMS_V2`. When
truthy, it calls evaluate_candidates_v2(); otherwise V1 inline path runs.
This lets paper trading run V1 by default while we burn-in V2 in a
parallel arm.

Layering:
  framework.oms.decision    — Decision dataclass + factories
  framework.oms.audit       — write_decision() helpers
  framework.oms.runner      — strategy-runtime glue (THIS FILE)

The runner does NOT own DB connections — caller (cw_runner) passes the
connection. Lazy-imports cw_runner helpers (_build_thesis_query, etc.)
to avoid hard layering coupling at module load time.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Optional

logger = logging.getLogger(__name__)


def is_oms_v2_enabled() -> bool:
    """True if env var OMS_V2 is truthy. Cached per-call (cheap)."""
    val = os.getenv("OMS_V2", "").strip().lower()
    return val in ("1", "true", "yes", "on")


def _strategy_version(config: dict) -> str:
    """Build f'{yaml_sha}:{git_sha}' for Decision.strategy_version.

    Mirrors cw_runner._yaml_sha + cw_runner._git_head_sha. Imported lazily
    so framework.oms doesn't depend on cw_runner at module load.
    """
    from strategies.cw_strategies.cw_runner import _git_head_sha, _yaml_sha
    yaml_sha = _yaml_sha(config.get("_yaml_path"))
    git_sha = _git_head_sha()
    return f"{yaml_sha}:{git_sha}"


def evaluate_candidates_v2(conn, config: dict) -> list[dict]:
    """V2 of cw_runner.scan_signals using Decision objects + write_decisions.

    Behaviorally identical to V1:
      - Same freshness preflight + halt-on-failure semantics
      - Same SQL queries (lazy-imports _build_thesis_query from cw_runner)
      - Same per-candidate evaluation (dedup → pit_lookup → min_10b5_1
        → conviction)
      - Same trade_decision_audit row pattern (passed-True intermediate
        rows + reject rows + final pass row, all matching V1's _audit())
      - Same candidates sorted by conviction descending
      - Same return shape (list[dict])

    Differences from V1 (internal only):
      - Uses framework.oms.decision.Decision instead of inline tuples
      - Uses framework.oms.audit.write_decisions for batch insert
      - Each Decision is type-checked + frozen for audit safety

    The audit-row equivalence means a diff between V1 and V2 audit rows
    in trade_decision_audit should be empty modulo run_id (which is
    per-call random anyway).
    """
    # Lazy imports to avoid circular dependency at module load
    from framework.alerts.log import alert as _alert
    from framework.contracts.exceptions import (
        FreshnessSystemBrokenError,
        FreshnessUnknownError,
        StaleSignalError,
    )
    from framework.contracts.freshness import (
        assert_all_fresh_for_strategy,
        assert_freshness_system_healthy,
    )
    from framework.oms.audit import write_decisions
    from framework.oms.decision import Decision
    from pipelines.insider_study.conviction_score import (
        _categorize_insider,
        compute_conviction,
        pit_score_to_grade,
    )
    from strategies.cw_strategies.cw_runner import _build_thesis_query

    strategy_name = config["strategy_name"]
    lookback = config.get("filing_lookback_days", 2)
    theses = config.get("theses", [])
    all_candidates: list[dict] = []
    seen_trade_ids: set[int] = set()

    # ── Freshness preflight (mirrors cw_runner.scan_signals exactly) ────────
    try:
        assert_freshness_system_healthy(conn, strategy_name)
        assert_all_fresh_for_strategy(conn, strategy_name)
    except FreshnessSystemBrokenError as e:
        _alert.critical(
            f"cw_runner.{strategy_name}",
            f"HALT — freshness system broken: {e}. "
            f"Entries skipped. Runbook R-008. "
            f"Run scripts/backfill_signal_freshness.py to seed.",
            strategy=strategy_name, breach=str(e),
            missing_columns=list(e.missing_columns),
        )
        logger.error("[%s] FRESHNESS_SYSTEM_BROKEN — %s", strategy_name, e)
        return []
    except FreshnessUnknownError as e:
        _alert.critical(
            f"cw_runner.{strategy_name}",
            f"HALT — freshness unknown for {e.table}.{e.column}: "
            f"compute pipeline never wrote signal_freshness. "
            f"Entries skipped. Runbook R-007.",
            strategy=strategy_name, breach=str(e),
            table=e.table, column=e.column,
        )
        logger.error("[%s] FRESHNESS_UNKNOWN — %s", strategy_name, e)
        return []
    except StaleSignalError as e:
        _alert.critical(
            f"cw_runner.{strategy_name}",
            f"HALT — input freshness contract breached: {e}. "
            f"Entries skipped this cycle. Runbook R-002.",
            strategy=strategy_name, breach=str(e),
        )
        logger.error("[%s] STALE_INPUT_HALT — %s", strategy_name, e)
        return []

    # ── Same DB lookups V1 does ─────────────────────────────────────────────
    held_tickers = {
        r["ticker"]
        for r in conn.execute(
            "SELECT ticker FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
            (strategy_name,),
        ).fetchall()
    }
    used_trade_ids = {
        r["trade_id"]
        for r in conn.execute(
            "SELECT trade_id FROM strategy_portfolio WHERE strategy = ?",
            (strategy_name,),
        ).fetchall()
        if r["trade_id"] is not None
    }

    # ── Audit context ───────────────────────────────────────────────────────
    strategy_version = _strategy_version(config)
    run_id = str(uuid.uuid4())
    decisions: list[Decision] = []

    def _common_kw(r, _thesis_name):
        """Common Decision constructor kwargs for this row + thesis."""
        return {
            "run_id": run_id,
            "strategy": strategy_name,
            "strategy_version": strategy_version,
            "trade_id": r["trade_id"],
            "ticker": r["ticker"],
            "filing_date": r["filing_date"],
            "thesis": _thesis_name,
        }

    # ── Per-thesis evaluation ───────────────────────────────────────────────
    for thesis in theses:
        thesis_name = thesis["name"]
        where_clause, where_params = _build_thesis_query(thesis, lookback)

        require_cluster = thesis.get("filters", {}).get("require_cluster", False)
        if require_cluster:
            join_clause = "JOIN trade_signals ts ON ts.trade_id = t.trade_id AND ts.signal_type = 'top_trade'"
        else:
            join_clause = ""

        sql = f"""
            SELECT
                t.trade_id,
                t.ticker,
                t.filing_date,
                t.filed_at,
                t.price,
                COALESCE(i.display_name, i.name) AS insider_name,
                t.company,
                t.title,
                t.signal_quality,
                t.signal_grade,
                t.is_rare_reversal,
                t.consecutive_sells_before,
                t.dip_1mo,
                t.dip_3mo,
                t.above_sma50,
                t.above_sma200,
                t.is_csuite,
                t.is_largest_ever,
                t.pit_n_trades,
                t.pit_win_rate_7d
            FROM trades t
            JOIN insiders i ON t.insider_id = i.insider_id
            {join_clause}
            WHERE {where_clause}
            ORDER BY t.filing_date DESC
        """

        rows = conn.execute(sql, where_params).fetchall()

        for r in rows:
            tid = r["trade_id"]
            ticker = r["ticker"]
            kw = _common_kw(r, thesis_name)

            # ── Stage: dedup ────────────────────────────────────────────────
            if tid in used_trade_ids or tid in seen_trade_ids:
                decisions.append(Decision.reject(
                    **kw, stage="dedup",
                    reason="trade_id already seen this strategy",
                ))
                continue
            if ticker in held_tickers:
                decisions.append(Decision.reject(
                    **kw, stage="dedup",
                    reason="ticker has open position",
                ))
                continue
            decisions.append(Decision.advance(**kw, stage="dedup"))

            # ── Stage: PIT lookup ───────────────────────────────────────────
            pit_row = conn.execute('''
                SELECT blended_score FROM insider_ticker_scores
                WHERE insider_id = (SELECT insider_id FROM trades WHERE trade_id = ?)
                  AND ticker = ? AND as_of_date <= ?
                ORDER BY as_of_date DESC LIMIT 1
            ''', (tid, ticker, r["filing_date"])).fetchone()

            pit_grade = pit_score_to_grade(pit_row[0] if pit_row else None) or "C"
            decisions.append(Decision.advance(
                **kw, stage="pit_lookup",
                reason=("score=%.3f" % pit_row[0]) if pit_row else "no_pit_score → fallback C",
                pit_grade=pit_grade,
            ))

            # ── Stage: min_prior_10b5_1_sells (tenb51_surprise only) ────────
            min_10b5_1 = thesis.get("filters", {}).get("min_prior_10b5_1_sells")
            if min_10b5_1:
                insider_id_row = conn.execute(
                    "SELECT insider_id FROM trades WHERE trade_id = ?", (tid,)
                ).fetchone()
                if insider_id_row:
                    cnt_row = conn.execute("""
                        SELECT COUNT(*) FROM trades
                        WHERE insider_id = ? AND ticker = ?
                          AND trans_code = 'S' AND is_10b5_1 = 1
                          AND filing_date < ?
                    """, (insider_id_row[0], ticker, r["filing_date"])).fetchone()
                    n = (cnt_row[0] or 0)
                    if n < int(min_10b5_1):
                        decisions.append(Decision.reject(
                            **kw, stage="min_10b5_1",
                            reason=f"prior_10b5_1_sells={n} < required {min_10b5_1}",
                            pit_grade=pit_grade,
                        ))
                        continue
                    decisions.append(Decision.advance(
                        **kw, stage="min_10b5_1",
                        reason=f"prior_10b5_1_sells={n}",
                        pit_grade=pit_grade,
                    ))
                else:
                    decisions.append(Decision.reject(
                        **kw, stage="min_10b5_1",
                        reason="insider_id not resolved",
                        pit_grade=pit_grade,
                    ))
                    continue

            # ── Stage: conviction ───────────────────────────────────────────
            conv = compute_conviction(
                thesis=thesis_name,
                signal_grade=pit_grade,
                consecutive_sells=r["consecutive_sells_before"],
                dip_1mo=r["dip_1mo"],
                is_largest_ever=bool(r["is_largest_ever"]),
                above_sma50=bool(r["above_sma50"]),
                above_sma200=bool(r["above_sma200"]),
                insider_title=r["title"],
                is_csuite=bool(r["is_csuite"]),
            )
            min_conv = config.get("min_conviction", 5.0)
            role = _categorize_insider(r["title"], bool(r["is_csuite"]))
            snapshot = {
                "consecutive_sells_before": r["consecutive_sells_before"],
                "dip_1mo": r["dip_1mo"],
                "is_largest_ever": bool(r["is_largest_ever"]),
                "above_sma50": bool(r["above_sma50"]),
                "above_sma200": bool(r["above_sma200"]),
                "insider_title": r["title"],
                "is_csuite": bool(r["is_csuite"]),
                "insider_name": r["insider_name"],
                "company": r["company"],
                "role": role,
            }

            if conv < min_conv:
                logger.info(
                    "  SKIP %s %s conv=%.1f < %.1f grade=%s role=%s %s",
                    ticker, r["filing_date"], conv, min_conv, pit_grade, role,
                    r["insider_name"],
                )
                decisions.append(Decision.reject(
                    **kw, stage="conviction",
                    reason=f"conv={conv:.1f} < threshold {min_conv:.1f}",
                    pit_grade=pit_grade, conviction=conv,
                    feature_snapshot=snapshot,
                ))
                continue
            logger.info(
                "  PASS %s %s conv=%.1f >= %.1f grade=%s role=%s %s",
                ticker, r["filing_date"], conv, min_conv, pit_grade, role,
                r["insider_name"],
            )
            decisions.append(Decision.advance(
                **kw, stage="conviction",
                reason=f"conv={conv:.1f} >= threshold {min_conv:.1f}",
                pit_grade=pit_grade, conviction=conv,
                feature_snapshot=snapshot,
            ))

            seen_trade_ids.add(tid)
            all_candidates.append({
                "trade_id": tid,
                "ticker": ticker,
                "filing_date": r["filing_date"],
                "filed_at": r["filed_at"],
                "price": r["price"],
                "insider_name": r["insider_name"],
                "company": r["company"],
                "title": r["title"],
                "signal_quality": r["signal_quality"],
                "signal_grade": pit_grade,
                "conviction": conv,
                "is_rare_reversal": bool(r["is_rare_reversal"]),
                "consecutive_sells_before": r["consecutive_sells_before"],
                "dip_1mo": r["dip_1mo"],
                "pit_n": r["pit_n_trades"],
                "pit_wr": r["pit_win_rate_7d"],
                "thesis_name": thesis_name,
                "exit_config": thesis["exit"],
            })

    # Sort by conviction (highest first) — matches V1
    all_candidates.sort(key=lambda c: c.get("conviction", 0), reverse=True)

    # ── Flush decisions via the OMS audit module ────────────────────────────
    if decisions:
        try:
            write_decisions(conn, decisions)
            conn.commit()
        except Exception as e:
            logger.warning(
                "OMS V2 decision audit write failed: %s "
                "(non-fatal — admin view will be incomplete)", e,
            )

    logger.info(
        "scan_signals_v2: %d candidates across %d theses (decisions: %d)",
        len(all_candidates), len(theses), len(decisions),
    )
    return all_candidates
