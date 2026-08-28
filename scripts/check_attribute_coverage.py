#!/usr/bin/env python3
"""Per-year coverage for every attribute a strategy reads. Freshness is not coverage.

WHY THIS EXISTS

On 2026-08-27 the three published books were re-simulated after the SEC reload
and came out at 14.3% / 28.9% / 0.1% CAGR against published figures of 69.8% /
64.1% / 37.9%. Insider Dip Buys returned a SIX position book. Those numbers
reached the live site.

Nothing was stale. Every freshness contract passed, every writer was wired, the
rebuild chain reported all eleven steps OK. What had actually happened is that
`abnormal_21td_from_filing` -- the only tradeable return label -- was populated
on 28.9% of 2024 discretionary buys and 29.6% of 2025, because
`backfill_returns_from_filing` is an UPDATE that runs chronologically from 2016
and had never reached the years the books trade. The books were simulated over a
window where seven candidates in ten had no label at all.

freshness_contracts.yaml asks "was this written recently?". writer_registry.yaml
asks "who writes it?". Neither asks "is it actually THERE, in the years we
trade?" -- and that is the question that was wrong.

WHAT IT CHECKS

For every column under a freshness contract, plus the return labels the books
are scored on, the populated percentage BY YEAR over the eligible population.
A column is failing when it drops below its floor in any year inside the
trading window, because a book cannot be evaluated over years its inputs do not
cover.

Usage:
    python3 scripts/check_attribute_coverage.py              # report + gate
    python3 scripts/check_attribute_coverage.py --report     # never exit 1
    python3 scripts/check_attribute_coverage.py --since 2023-01-01
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "config" / "freshness_contracts.yaml"

# The books all start 2023-01. A gap before that weakens fitting; a gap inside
# the window invalidates the simulation outright, so that is what gates.
TRADING_WINDOW_START = "2023"

# Floor below which a column cannot support a simulation. Deliberately low --
# this is a "the data is missing" alarm, not a quality bar. The 2024 label
# coverage that caused the incident was 28.9%.
DEFAULT_MIN_COVERAGE = 70.0

# Scored on these but absent from the freshness contracts, which is part of how
# this went unseen: the labels are in trade_returns, and nothing declared them.
LABEL_COLUMNS = {
    "trade_returns.abnormal_7td_from_filing":  "tradeable 7td label",
    "trade_returns.abnormal_21td_from_filing": "tradeable 21td label",
    "trade_returns.abnormal_42td_from_filing": "tradeable 42td label",
}

ELIGIBLE = """
    t.signal_class = 'discretionary_buy'
    AND NOT COALESCE(t.value_suspect, FALSE)
    AND t.filing_date >= ?
"""


def load_columns() -> dict[str, dict]:
    """Every column to check. Missing contracts is a HARD ERROR.

    An earlier revision did `if CONTRACTS.exists()` and carried on with only
    the three label columns when the path was wrong -- reporting a confident
    green on 3 of 11 inputs. That is the same fail-open shape that let the
    coverage hole reach the site in the first place, so it now refuses to run.
    """
    if not CONTRACTS.exists():
        raise SystemExit(
            f"freshness contracts not found at {CONTRACTS}.\n"
            "Refusing to report partial coverage -- a gate that silently "
            "checks a subset is worse than no gate. Run from the repo root."
        )
    out: dict[str, dict] = {}
    if True:
        for key, spec in (yaml.safe_load(CONTRACTS.read_text()) or {}).items():
            spec = spec or {}
            out[key] = {
                "required_for": spec.get("required_for") or [],
                "description": spec.get("description") or "",
                "min_coverage": spec.get("min_coverage", DEFAULT_MIN_COVERAGE),
            }
    for key, desc in LABEL_COLUMNS.items():
        out[key] = {"required_for": ["ALL BOOKS (scoring label)"],
                    "description": desc,
                    "min_coverage": DEFAULT_MIN_COVERAGE}
    return out


def coverage_by_year(conn, table: str, column: str, since: str):
    """Populated % per filing year, over discretionary buys."""
    if table == "trades":
        join, col = "", f"t.{column}"
    elif table == "trade_returns":
        join, col = "LEFT JOIN trade_returns r USING (trade_id)", f"r.{column}"
    else:
        return None
    sql = f"""
        SELECT left(t.filing_date, 4) AS yr,
               count(*)          AS eligible,
               count({col})      AS populated
          FROM trades t {join}
         WHERE {ELIGIBLE}
         GROUP BY 1 ORDER BY 1
    """
    try:
        return conn.execute(sql, (since,)).fetchall()
    except Exception as exc:                       # column genuinely gone
        return [("ERROR", str(exc)[:60], 0)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01")
    ap.add_argument("--report", action="store_true",
                    help="print the table but always exit 0")
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    columns = load_columns()
    failures: list[str] = []

    print(f"Attribute coverage by filing year -- discretionary buys, "
          f"{args.since} onward")
    print(f"Checking {len(columns)} columns "
          f"({len(columns) - len(LABEL_COLUMNS)} under contract, "
          f"{len(LABEL_COLUMNS)} scoring labels)")
    print(f"Gate: >= {DEFAULT_MIN_COVERAGE:.0f}% in every year from "
          f"{TRADING_WINDOW_START} (the books' trading window)\n")

    for key in sorted(columns):
        table, _, column = key.partition(".")
        spec = columns[key]
        rows = coverage_by_year(conn, table, column, args.since)
        if not rows:
            continue
        if rows and rows[0][0] == "ERROR":
            print(f"{key}\n    MISSING: {rows[0][1]}\n")
            failures.append(f"{key}: column does not exist")
            continue

        cells, worst_yr, worst_pct = [], None, 101.0
        for yr, eligible, populated in rows:
            pct = 100.0 * populated / max(eligible, 1)
            flag = ""
            if yr >= TRADING_WINDOW_START and pct < spec["min_coverage"]:
                flag = " <<"
                if pct < worst_pct:
                    worst_yr, worst_pct = yr, pct
            cells.append(f"{yr}:{pct:5.1f}%{flag}")

        status = "FAIL" if worst_yr else "ok  "
        print(f"[{status}] {key}")
        print(f"         needed by: {', '.join(spec['required_for']) or '(nobody)'}")
        print(f"         {'  '.join(cells)}")
        print()

        if worst_yr:
            failures.append(
                f"{key}: {worst_pct:.1f}% populated in {worst_yr} "
                f"(needs >= {spec['min_coverage']:.0f}%), required by "
                f"{', '.join(spec['required_for'])}")

    if failures:
        print("=" * 72)
        print(f"{len(failures)} attribute(s) below floor inside the trading window:\n")
        for f in failures:
            print(f"  - {f}")
        print("\nA book simulated over these years is measuring missing data, "
              "not strategy performance.")
        return 0 if args.report else 1

    print("All attributes clear their floor across the trading window.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
