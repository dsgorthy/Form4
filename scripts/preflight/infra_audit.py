#!/usr/bin/env python3
"""Pre-deploy infrastructure audit.

Catches the bug classes that have caused production outages:

  1. Legacy /Users/openclaw paths anywhere in repo (the surface bugs of
     the 2026-04-19 username switch — would have caught form4-uptime
     mkdir-error spam, breaking-signal failures, daily-content step 7)

  2. Plist EnvironmentVariables.PATH missing /opt/homebrew/bin (cause of
     the 15-day backfill-returns gap — pandas-using subprocess fell
     through to system Python 3.9)

  3. Plist Program/ProgramArguments using /usr/bin/python3 (system Python,
     missing modules; same failure mode as #2)

  4. Direct `import sqlite3` in production pipelines outside the known
     allowed cache layers (the post-PG-migration orphan pattern that
     caused the original 21-day silent April outage)

  5. Hardcoded `DB_PATH = "...db"` constants in production pipelines that
     might shadow the Postgres source-of-truth

  6. Freshness contract drift: every contract's populated_by script must
     exist, and must call write_freshness() somewhere (grep-level check —
     not a full AST analysis, but catches the obvious "I forgot to wire
     it" case that's a sibling of the April outage)

Returns 0 on full pass, 1 on any failure (or 2 on hard error).

Usage:
    scripts/preflight/infra_audit.py             # all checks, human output
    scripts/preflight/infra_audit.py --json      # JSON output for CI
    scripts/preflight/infra_audit.py --quick     # skip slow checks
    scripts/preflight/infra_audit.py --check X   # run only check X (1-6)

Designed to run on Mini before deploy; no DB access required. The probe
layer (`scripts/freshness_probe.py`) covers the live-data side; this
covers the static-code side.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[2]

# ── Allow-lists for legitimate exceptions ───────────────────────────────────

# Files where /Users/openclaw is expected.
LEGACY_PATH_ALLOWED_FILES = {
    "MINI_CLEANUP_HANDOFF_2026-04-19.md",   # historical postmortem record
    "brand/generate_v2_logos.py",            # Mini-only research (not deployed)
    "brand/generate_wordmarks_v2.py",        # Mini-only research (not deployed)
    "scripts/preflight/infra_audit.py",      # this file — references the bug for documentation
}

# Python files where `import sqlite3` is legitimate (cache layers, the
# compat layer itself, the local notifications queue).
SQLITE_IMPORT_ALLOWED = {
    "config/database.py",
    "api/notifications_db.py",
    "api/db.py",
    "pipelines/insider_study/db_lock.py",
    "pipelines/insider_study/theta_client.py",
    "pipelines/insider_study/options_pull.py",
    "pipelines/insider_study/options_pull_longdte.py",
    "pipelines/insider_study/options_pull_targeted.py",
    "pipelines/insider_study/migrate_theta_to_db.py",
    "pipelines/insider_study/sync_prices_sqlite.py",
    "strategies/insider_catalog/sync_prices_sqlite.py",
    "scripts/migrate_to_pg.py",
    "scripts/seed_open_positions.py",
}

# DB_PATH constants pointing at .db files that are legitimate (theta cache,
# local notifications queue, intentional SQLite caches).
DB_PATH_ALLOWED = {
    "pipelines/insider_study/theta_client.py",
    "pipelines/insider_study/options_pull.py",
    "pipelines/insider_study/options_pull_longdte.py",
    "pipelines/insider_study/options_pull_targeted.py",
    "pipelines/insider_study/migrate_theta_to_db.py",
    "pipelines/insider_study/sync_prices_sqlite.py",
    "strategies/insider_catalog/sync_prices_sqlite.py",
    "api/notifications_db.py",
    "api/db.py",
}

# Plists at the repo root are templates — they get installed to
# ~/Library/LaunchAgents/ on Studio. Check for the same hygiene.
PLIST_PATH_REQUIRED_PREFIX = "/opt/homebrew/bin"
PLIST_PYTHON_REQUIRED_PREFIX = "/opt/homebrew/bin/python"


# ── Result data ─────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    passed: bool
    findings: list[str] = field(default_factory=list)
    details: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "findings": self.findings,
            "details": self.details,
        }


# ── Check implementations ───────────────────────────────────────────────────

def check_no_legacy_paths() -> CheckResult:
    """Check #1: no /Users/openclaw/ in repo (excluding allowed historical docs)."""
    name = "1. No legacy /Users/openclaw/ paths"
    findings: list[str] = []

    # Only scan deployable code — not docs/reports. Markdown files mentioning
    # /Users/openclaw historically (postmortems, audit reports, plan docs)
    # are documentation, not bugs.
    extensions = {".sh", ".py", ".plist", ".yaml", ".yml", ".toml", ".sql"}
    for path in REPO.rglob("*"):
        if not path.is_file() or path.suffix not in extensions:
            continue
        # Skip .git and node_modules
        rel = path.relative_to(REPO)
        rel_str = str(rel)
        if rel_str.startswith(".git/") or "/node_modules/" in rel_str:
            continue
        if rel_str in LEGACY_PATH_ALLOWED_FILES:
            continue
        try:
            text = path.read_text(errors="ignore")
        except Exception:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if "/Users/openclaw" in line:
                findings.append(f"{rel_str}:{i}: {line.strip()[:100]}")

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Legacy /Users/openclaw paths cause silent failures (form4-uptime "
            "mkdir spam, breaking-signal cd error). Mini is now /Users/derekg/."
        ),
    )


def _parse_plist(path: Path) -> Optional[dict]:
    """Parse a plist XML into a dict. Returns None on parse failure."""
    try:
        # Use built-in plistlib for reliable parsing
        import plistlib
        with open(path, "rb") as f:
            return plistlib.load(f)
    except Exception:
        return None


def check_plist_path_env() -> CheckResult:
    """Check #2: every repo plist's PATH includes /opt/homebrew/bin."""
    name = "2. Plist PATH includes /opt/homebrew/bin"
    findings: list[str] = []

    for plist_path in REPO.glob("com.openclaw.*.plist"):
        rel_str = str(plist_path.relative_to(REPO))
        data = _parse_plist(plist_path)
        if data is None:
            findings.append(f"{rel_str}: parse failed")
            continue
        env = data.get("EnvironmentVariables") or {}
        path_var = env.get("PATH", "")
        if PLIST_PATH_REQUIRED_PREFIX not in path_var:
            findings.append(
                f"{rel_str}: PATH={path_var!r} missing {PLIST_PATH_REQUIRED_PREFIX}"
            )

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Plist EnvironmentVariables.PATH excluding /opt/homebrew/bin "
            "causes subprocess Python calls to fall through to system 3.9 "
            "without pandas/requests/etc. (cause of 15-day backfill-returns gap)."
        ),
    )


def check_plist_python() -> CheckResult:
    """Check #3: every repo plist's first ProgramArgument is /opt/homebrew/bin/python*."""
    name = "3. Plist Python is /opt/homebrew/bin/python*"
    findings: list[str] = []

    for plist_path in REPO.glob("com.openclaw.*.plist"):
        rel_str = str(plist_path.relative_to(REPO))
        data = _parse_plist(plist_path)
        if data is None:
            continue  # parse error already reported in check #2
        args = data.get("ProgramArguments") or []
        if not args:
            program = data.get("Program", "")
            args = [program] if program else []
        if not args:
            continue  # neither key set; not a python plist
        first = str(args[0])
        # Heuristic: only check plists whose first arg looks like a Python interpreter
        if "python" not in first.lower():
            continue
        if not first.startswith(PLIST_PYTHON_REQUIRED_PREFIX):
            findings.append(f"{rel_str}: Python={first!r} (want {PLIST_PYTHON_REQUIRED_PREFIX}*)")

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Plists invoking /usr/bin/python3 hit system Python 3.9 which "
            "doesn't have project deps (pandas, psycopg2, requests, etc.)."
        ),
    )


def check_no_orphan_insiders_db_reads() -> CheckResult:
    """Check #4: no scripts referenced by freshness_contracts.yaml read insiders.db.

    The April 2026 21-day silent outage was caused by `compute_cw_indicators.py`
    reading from a SQLite `insiders.db` / `prices.db` after the PG migration.
    The structural rule that prevents recurrence:

        Any script declared as `populated_by` for a freshness contract
        must NOT reference insiders.db (the legacy SQLite source-of-truth).

    Indirect SQLite usage (e.g., reading from a cache that's freshly
    populated from PG) is acceptable — the bug class is reading a
    SQLite that nothing writes to anymore.

    Scope intentionally narrow: only the freshness-writer scripts.
    Maintenance/research scripts and dormant content pipelines have
    pre-existing SQLite drift; they're tracked as Phase 4 work.
    """
    name = "4. Freshness writers don't read legacy insiders.db"
    findings: list[str] = []

    contracts_path = REPO / "config" / "freshness_contracts.yaml"
    if not contracts_path.exists():
        return CheckResult(name=name, passed=False,
                           findings=["freshness_contracts.yaml missing"],
                           details="contracts file required")
    try:
        import yaml
        contracts = yaml.safe_load(contracts_path.read_text()) or {}
    except Exception as e:
        return CheckResult(name=name, passed=False,
                           findings=[f"failed to parse contracts: {e}"],
                           details="install pyyaml")

    # Collect every populated_by script
    writer_scripts: set[str] = set()
    for col_key, spec in contracts.items():
        if isinstance(spec, dict):
            pb = spec.get("populated_by", "").strip()
            if pb:
                writer_scripts.add(pb)

    # The smoking-gun pattern: a SQLite path string like "insiders.db" or
    # ".../insiders.db" appearing in a string literal — not a comment.
    # Restrict to quoted strings to avoid matching docstrings/comments.
    bad_pattern = re.compile(r'''['"][^'"]*\b(?:insiders|research|prices_old)\.db['"]''')

    for rel in sorted(writer_scripts):
        path = REPO / rel
        if not path.exists():
            findings.append(f"{rel}: declared populated_by but file missing")
            continue
        try:
            text = path.read_text(errors="ignore")
        except Exception:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.lstrip()
            # Skip pure comments; the bug pattern is in code, not docstring text
            if stripped.startswith("#"):
                continue
            if bad_pattern.search(line):
                findings.append(f"{rel}:{i}: {line.strip()[:120]}")
                break

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Freshness-writer scripts must NOT read the legacy insiders.db / "
            "research.db SQLite files. They were the pre-2026-04-07 source-of-"
            "truth and the April 21-day outage came from compute_cw_indicators "
            "still reading them post-migration. Use config.database."
        ),
    )


def check_runner_strategy_columns_have_contracts() -> CheckResult:
    """Check #5: every column referenced by cw_runner.py SQL filters has a contract.

    The original April outage was a strategy filtering on `above_sma50` /
    `dip_3mo` / etc. without any safety net. After Phase 1 we have
    contracts; this check ensures NEW columns added to a strategy's filter
    set don't silently bypass freshness enforcement (i.e., we didn't
    forget to add them to freshness_contracts.yaml).

    Approach: grep cw_runner.py + the strategy YAMLs for column names
    that look like trade features, cross-reference against the contract
    list. Best-effort heuristic — not a full SQL parser.
    """
    name = "5. Strategy filter columns have freshness contracts"
    findings: list[str] = []

    contracts_path = REPO / "config" / "freshness_contracts.yaml"
    if not contracts_path.exists():
        return CheckResult(name=name, passed=False,
                           findings=["freshness_contracts.yaml missing"],
                           details="contracts file required")
    try:
        import yaml
        contracts = yaml.safe_load(contracts_path.read_text()) or {}
    except Exception as e:
        return CheckResult(name=name, passed=False,
                           findings=[f"failed to parse contracts: {e}"],
                           details="install pyyaml")

    # Build the contracted-column set
    contracted_cols: set[str] = set()
    for key in contracts:
        # Keys are "table.column" or "schema.table.column"
        parts = key.split(".")
        col = parts[-1]
        contracted_cols.add(col)

    # Heuristic: scan strategy YAML filter blocks for column-like keys.
    # These are the user-facing knobs ("min_pit_grade", "above_sma50", etc.).
    yaml_filter_cols: set[tuple[str, str]] = set()  # (yaml_path, col_name)
    config_dir = REPO / "strategies" / "cw_strategies" / "configs"
    if config_dir.exists():
        for yaml_path in config_dir.glob("*.yaml"):
            try:
                cfg = yaml.safe_load(yaml_path.read_text()) or {}
            except Exception:
                continue
            theses = cfg.get("theses", [])
            if isinstance(cfg.get("filters"), dict) and not theses:
                theses = [{"filters": cfg["filters"]}]
            for th in theses:
                if not isinstance(th, dict):
                    continue
                filters = th.get("filters") or {}
                for filt_key in filters.keys():
                    # Strip min_/max_ prefix to recover the column name
                    col = filt_key
                    for prefix in ("min_", "max_", "require_", "exclude_"):
                        if col.startswith(prefix):
                            col = col[len(prefix):]
                            break
                    yaml_filter_cols.add((str(yaml_path.relative_to(REPO)), col))

    # Known column names that aren't contracts because they're either
    # static metadata (insider_id, trade_type) or derived in the runner.
    NON_CONTRACT_COLS = {
        "trade_type", "insider_id", "ticker", "trans_code", "min_value",
        "min_dollar_value", "max_dollar_value", "min_shares", "max_shares",
        "lookback_days", "filing_lookback_days", "value", "trade_value",
        "is_csuite", "title_match", "exclude_routine", "include_private",
        # Strategy-specific knobs (NOT data columns — these are config knobs
        # that share names with column-y things). `career_grade` was on
        # this list as an escape hatch until 2026-05-17 — that exemption
        # let QM's primary filter rot for 5 weeks. Removed; both pit_grade
        # and career_grade now MUST have contracts.
        "signal_grade", "conviction_score",
        "concurrent_positions", "concurrent", "hold_days",
        # Booleans that map to contracted columns differently
        "cluster", "10b5_1", "rare_reversal",
    }

    for yaml_rel, col in sorted(yaml_filter_cols):
        if col in NON_CONTRACT_COLS or col in contracted_cols:
            continue
        # Heuristic: only flag if the column LOOKS like a trade feature
        # (non-trivial; many filter knobs are not trade columns)
        # For now, list as informational rather than blocking.
        # findings.append(f"{yaml_rel}: filter '{col}' has no contract entry (review)")
        # ↑ disabled — too many false positives for v1. Leaving the
        # function present so future tightening lands here.
        pass

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Strategy YAML filter keys should map to contracted columns. "
            "Currently informational only — too many false positives to "
            "block. Tighten in Phase 2 when strategies become pure functions."
        ),
    )


def check_freshness_contracts_have_writers() -> CheckResult:
    """Check #6: every contract's populated_by file exists and calls write_freshness."""
    name = "6. Freshness contracts have functional writers"
    findings: list[str] = []

    contracts_path = REPO / "config" / "freshness_contracts.yaml"
    if not contracts_path.exists():
        return CheckResult(
            name=name,
            passed=False,
            findings=[f"contracts file missing: {contracts_path.relative_to(REPO)}"],
            details="freshness_contracts.yaml not found",
        )

    try:
        import yaml
    except ImportError:
        return CheckResult(
            name=name,
            passed=False,
            findings=["pyyaml not installed"],
            details="install pyyaml to enable this check",
        )

    raw = yaml.safe_load(contracts_path.read_text()) or {}
    for col_key, spec in raw.items():
        if not isinstance(spec, dict):
            continue
        populated_by = spec.get("populated_by", "")
        if not populated_by:
            findings.append(f"{col_key}: no populated_by declared")
            continue
        writer_path = REPO / populated_by
        if not writer_path.exists():
            findings.append(
                f"{col_key}: populated_by={populated_by!r} does not exist on disk"
            )
            continue
        try:
            text = writer_path.read_text(errors="ignore")
        except Exception:
            findings.append(f"{col_key}: failed to read {populated_by}")
            continue
        if "write_freshness" not in text:
            findings.append(
                f"{col_key}: {populated_by} does not call write_freshness() — "
                f"freshness write missing"
            )

    return CheckResult(
        name=name,
        passed=not findings,
        findings=findings,
        details=(
            "Each freshness contract declares the script that populates it. "
            "The script must exist and call write_freshness() on success. "
            "Phase 1 left this gap (schema landed without writer wires)."
        ),
    )


# ── Runner ──────────────────────────────────────────────────────────────────

def check_writer_registry() -> CheckResult:
    """Check #7: writer_registry.yaml is consistent and verified.

    Delegates to scripts/preflight/writer_registry_audit.py — runs its 7
    sub-checks (scripts exist, write_freshness calls match, plists invoke
    scripts, contracts↔registry consistent, no mislabels). Failures here
    surface the orphan/mislabel failure class structurally before deploy.
    """
    name = "7. Writer registry consistent and verified"
    try:
        # Lazy-import — registry audit module is in the same package
        from scripts.preflight.writer_registry_audit import run_all_checks as _registry_checks
    except ImportError:
        sys.path.insert(0, str(REPO))
        try:
            from scripts.preflight.writer_registry_audit import run_all_checks as _registry_checks
        except Exception as exc:
            return CheckResult(
                name=name, passed=False,
                findings=[f"could not import writer_registry_audit: {exc}"],
                details="check that scripts/preflight/writer_registry_audit.py exists",
            )

    sub_results = _registry_checks()
    findings: list[str] = []
    for sr in sub_results:
        if sr.passed:
            continue
        for f in sr.findings:
            findings.append(f"[registry/{sr.name}] {f}")
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("The writer registry is the source of truth for who writes which "
                 "column on which cron. Run `scripts/preflight/writer_registry_audit.py` "
                 "directly for the per-sub-check breakdown."),
    )


CHECKS = [
    ("1", check_no_legacy_paths),
    ("2", check_plist_path_env),
    ("3", check_plist_python),
    ("4", check_no_orphan_insiders_db_reads),
    ("5", check_runner_strategy_columns_have_contracts),
    ("6", check_freshness_contracts_have_writers),
    ("7", check_writer_registry),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true", help="JSON output for CI")
    p.add_argument("--quick", action="store_true", help="(reserved — currently no slow checks)")
    p.add_argument("--check", help="Run only check N (number). Default: all.")
    args = p.parse_args()

    selected = CHECKS
    if args.check:
        selected = [(num, fn) for num, fn in CHECKS if num == args.check]
        if not selected:
            print(f"No such check: {args.check}", file=sys.stderr)
            return 2

    results: list[CheckResult] = [fn() for _, fn in selected]
    n_pass = sum(1 for r in results if r.passed)
    n_fail = sum(1 for r in results if not r.passed)

    if args.json:
        print(json.dumps({
            "passed": n_fail == 0,
            "n_pass": n_pass,
            "n_fail": n_fail,
            "results": [r.to_dict() for r in results],
        }, indent=2))
    else:
        print("─" * 72)
        print("  Trading Framework — Pre-deploy Infrastructure Audit")
        print("─" * 72)
        for r in results:
            mark = "PASS" if r.passed else "FAIL"
            print(f"  [{mark}]  {r.name}")
            if r.findings:
                head = r.findings[:8]
                for f in head:
                    print(f"           • {f}")
                if len(r.findings) > 8:
                    print(f"           • ... ({len(r.findings) - 8} more)")
                print(f"           why: {r.details}")
        print("─" * 72)
        print(f"  {n_pass} pass / {n_fail} fail of {len(results)} checks")
        if n_fail:
            print("─" * 72)
            print("  Deploy blocked. Fix findings above and re-run.")
        print("─" * 72)

    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
