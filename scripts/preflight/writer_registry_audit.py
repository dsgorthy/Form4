#!/usr/bin/env python3
"""Writer Registry Audit — verify every column has a recurring writer.

Designed to prevent the failure class that produced four silent outages in
the six weeks before 2026-05-16: a writer is added during a refactor, the
cron wiring slips, the freshness contract still reports "green" (often
because the named populated_by happens to also touch some unrelated column),
and nothing alarms until a strategy stops producing candidates.

Checks (each maps to a `CheckResult`):

  1. Every registry entry's `script` exists at the declared repo path.
  2. Every registry entry's `script` actually contains a `write_freshness`
     call for the declared column. AST analysis for literal column kwargs +
     grep fallback for dict-driven indirect writes (compute_cw_indicators).
  3. Every registry entry's `plists` exist in `scripts/launchd/` OR are
     documented as Studio-only. Plist's ProgramArguments references the
     declared script (with optional `bash` wrapper recursion).
  4. Every contracted column in `freshness_contracts.yaml` has a registry
     entry. (Contract without writer = orphan reader.)
  5. Every registry entry whose `required_for` is non-empty has a matching
     contract entry. (Writer for a strategy-required column with no
     contract = unmonitored, will silently rot.)
  6. `populated_by` in `freshness_contracts.yaml` matches the registered
     `script`. (Catches the mislabel class.)
  7. No two registry entries claim the same column.

Returns 0 on full pass, 1 on any failure, 2 on hard error.

Usage:
    scripts/preflight/writer_registry_audit.py            # human output
    scripts/preflight/writer_registry_audit.py --json     # JSON output
    scripts/preflight/writer_registry_audit.py --check N  # run check N only

Read-only; safe to run on Mini without DB. Pairs with the runtime
`assert_writer_wired` check inside `framework/contracts/freshness.py` that
verifies the registry against the actual `signal_freshness` rows at scan
time.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

REPO = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO / "config" / "writer_registry.yaml"
CONTRACTS_PATH = REPO / "config" / "freshness_contracts.yaml"
PLIST_TEMPLATE_DIR = REPO / "scripts" / "launchd"

# Plists that we know live only on Studio (not in the repo templates).
# Add new Studio-only plists here when needed.
STUDIO_ONLY_PLISTS = {
    # The 3 cw_runner strategy plists ship from ~/Library/LaunchAgents directly.
    "com.openclaw.quality-momentum",
    "com.openclaw.reversal-dip",
    "com.openclaw.tenb51-surprise",
    "com.openclaw.quality-momentum-live",
    # Refresh-features chain orchestrator
    "com.openclaw.refresh-features",
    "com.openclaw.insider-fetch",
    "com.openclaw.backfill-returns",
    "com.openclaw.daily-prices",
    "com.openclaw.alpaca-stream-listener",
    "com.openclaw.alpaca-intraday-resolver",
    "com.openclaw.alpaca-reconcile",
    "com.openclaw.strategy-simulator",
    "com.openclaw.strategy-intraday",
    "com.openclaw.candidate-count-probe",
    "com.openclaw.freshness-probe",
    "com.openclaw.post-deploy-audit",
    "com.openclaw.compute-signals",
}


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


# ── Loaders ─────────────────────────────────────────────────────────────────


def _load_yaml(path: Path) -> dict:
    import yaml
    return yaml.safe_load(path.read_text()) or {}


def _load_registry() -> list[dict]:
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"missing {REGISTRY_PATH}")
    return _load_yaml(REGISTRY_PATH).get("writers", [])


def _load_contracts() -> dict[str, dict]:
    if not CONTRACTS_PATH.exists():
        return {}
    raw = _load_yaml(CONTRACTS_PATH)
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


# ── AST analysis ────────────────────────────────────────────────────────────


def _extract_write_freshness_columns(script_path: Path) -> set[str]:
    """Find every column literal passed to `write_freshness(column=...)`
    in `script_path`. Returns the set of literal column names.

    Catches both:
        write_freshness(conn, table="trades", column="career_grade", ...)
        write_freshness(column="career_grade", ...)

    Indirect calls (column=variable) are missed by AST — the grep fallback
    in `_script_could_write_column` covers them.
    """
    found: set[str] = set()
    try:
        tree = ast.parse(script_path.read_text())
    except (SyntaxError, OSError):
        return found

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        # Match call where func is named write_freshness (any attribute path).
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        if func_name != "write_freshness":
            continue
        for kw in node.keywords:
            if kw.arg != "column":
                continue
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                found.add(kw.value.value)
    return found


def _script_could_write_column(script_path: Path, column: str) -> bool:
    """Best-effort: returns True iff `script_path` contains both:

      (a) any `write_freshness(...)` call (literal or indirect), AND
      (b) the column name as a string literal anywhere in the source.

    Designed to accept dict-driven writers like
    `INDICATOR_TO_COLUMNS = {"dip": ["dip_1mo", "dip_3mo"]}` followed by
    `write_freshness(column=col, ...)` inside a loop.
    """
    try:
        text = script_path.read_text()
    except OSError:
        return False
    if "write_freshness" not in text:
        return False
    # Match the column name as a string literal — single or double quoted.
    pattern = re.compile(
        rf'["\']{re.escape(column)}["\']'
    )
    return bool(pattern.search(text))


# ── Plist analysis ──────────────────────────────────────────────────────────


def _plist_template_path(plist_label: str) -> Optional[Path]:
    """Locate `<label>.plist` under `scripts/launchd/`. Returns None if not present."""
    candidate = PLIST_TEMPLATE_DIR / f"{plist_label}.plist"
    return candidate if candidate.exists() else None


def _plist_program_arguments(plist_path: Path) -> list[str]:
    """Extract <ProgramArguments><string>...</string></ProgramArguments>."""
    try:
        tree = ET.parse(plist_path)
    except Exception:
        return []
    root = tree.getroot()
    # plist files have <plist><dict>...
    dict_node = root.find("dict")
    if dict_node is None:
        return []
    args: list[str] = []
    children = list(dict_node)
    for i, child in enumerate(children):
        if child.tag == "key" and child.text == "ProgramArguments" and i + 1 < len(children):
            arr = children[i + 1]
            if arr.tag == "array":
                for s in arr.findall("string"):
                    args.append((s.text or "").strip())
            break
    return args


def _plist_invokes_script(plist_path: Path, script_path: Path) -> bool:
    """True iff plist's ProgramArguments includes a string referencing
    `script_path`. Handles bash wrappers by reading the wrapped shell
    script one level deep — looking for the script path inside.
    """
    args = _plist_program_arguments(plist_path)
    if not args:
        return False
    script_rel = str(script_path).replace(str(REPO), "").lstrip("/")
    for a in args:
        if script_rel in a or str(script_path) in a:
            return True
    # Bash wrapper case: ProgramArguments = ["/bin/bash", "/path/to/wrapper.sh"]
    for a in args:
        if a.endswith(".sh"):
            wrapper = Path(a)
            if not wrapper.exists():
                # Resolve relative to repo if needed
                wrapper = REPO / a.lstrip("/")
            if wrapper.exists():
                try:
                    body = wrapper.read_text()
                    if script_rel in body or str(script_path) in body:
                        return True
                except OSError:
                    pass
    return False


# ── Checks ──────────────────────────────────────────────────────────────────


def check_scripts_exist(registry: list[dict]) -> CheckResult:
    """Check #1: every registry entry's `script` exists at the declared path."""
    name = "1. Registry scripts exist"
    findings: list[str] = []
    for entry in registry:
        column = entry.get("column", "<unknown>")
        script_rel = entry.get("script", "")
        if not script_rel:
            findings.append(f"{column}: no script declared")
            continue
        if not (REPO / script_rel).exists():
            findings.append(f"{column}: script not found at {script_rel}")
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Every registry entry must point at an actual file in the repo. "
                 "A missing script = the writer was deleted without removing the registry entry."),
    )


def check_write_freshness_calls(registry: list[dict]) -> CheckResult:
    """Check #2: every script actually calls write_freshness for its declared column.

    Two-tier match:
      - AST: literal `write_freshness(column="X", ...)` call
      - Grep fallback: script contains both `write_freshness` and `"X"` literal
        (for dict-driven writers where column comes through a variable)

    Columns with `recompute: false` are exempt — they're set at INSERT (e.g.
    XML-parse-time fields like is_10b5_1) and have no compute-step writer.
    Their freshness piggybacks on the ingest plist's filing_date row.
    """
    name = "2. Scripts call write_freshness for their column"
    findings: list[str] = []
    for entry in registry:
        column = entry.get("column", "<unknown>")
        script_rel = entry.get("script", "")
        if not script_rel:
            continue  # Caught by check #1
        script_path = REPO / script_rel
        if not script_path.exists():
            continue  # Caught by check #1

        # Parse-time columns explicitly opt out of the write_freshness contract.
        if entry.get("recompute", True) is False:
            continue

        # Dynamic-columns writers (compute_signals.py style) call
        # write_freshness in a loop with `column=variable`. Require only that
        # the script contains a write_freshness call somewhere; trust the
        # author that it covers the declared column.
        if entry.get("dynamic_columns", False) is True:
            if "write_freshness" not in script_path.read_text(errors="ignore"):
                findings.append(
                    f"{column}: declared dynamic_columns=true but {script_rel} "
                    f"contains no write_freshness call at all"
                )
            continue

        column_basename = column.rsplit(".", 1)[-1]
        literals = _extract_write_freshness_columns(script_path)
        if column_basename in literals:
            continue
        if _script_could_write_column(script_path, column_basename):
            continue
        findings.append(
            f"{column}: {script_rel} does not call write_freshness(column='{column_basename}') "
            f"(neither AST literal nor grep fallback found a match)"
        )
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Every registered writer must actually call write_freshness for "
                 "its column, otherwise the freshness contract reports green forever. "
                 "Use `recompute: false` for parse-time columns. This is the "
                 "structural defense against the is_rare_reversal mislabel class of bug."),
    )


def check_plists_exist_and_invoke(registry: list[dict]) -> CheckResult:
    """Check #3: every registered plist exists (in repo templates OR documented
    as Studio-only) and its ProgramArguments references the script."""
    name = "3. Plists exist and invoke the script"
    findings: list[str] = []
    for entry in registry:
        column = entry.get("column", "<unknown>")
        script_rel = entry.get("script", "")
        plists = entry.get("plists") or []
        for label in plists:
            template = _plist_template_path(label)
            if template is None:
                if label in STUDIO_ONLY_PLISTS:
                    continue  # Trusted — verified manually at deploy time
                findings.append(
                    f"{column}: plist {label} missing — not in {PLIST_TEMPLATE_DIR.relative_to(REPO)}/ "
                    f"and not on the STUDIO_ONLY_PLISTS allowlist"
                )
                continue
            if not _plist_invokes_script(template, REPO / script_rel):
                findings.append(
                    f"{column}: plist {label} exists but its ProgramArguments "
                    f"does NOT reference {script_rel}"
                )
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Plists must actually invoke the declared script (either directly "
                 "in ProgramArguments or through a bash wrapper). The intraday-backfill "
                 "/usr/bin/python3 issue would have been caught here."),
    )


def check_contracts_have_registry(registry: list[dict],
                                  contracts: dict[str, dict]) -> CheckResult:
    """Check #4: every freshness contract has a writer registry entry."""
    name = "4. Every freshness contract has a registry entry"
    registry_columns = {e.get("column") for e in registry}
    findings: list[str] = []
    for column in contracts:
        if column not in registry_columns:
            findings.append(f"{column}: in freshness_contracts.yaml but NOT in writer_registry.yaml")
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Every contracted column must have a registered writer. Missing entry = "
                 "either an orphan contract (delete) or a missing writer (add to registry)."),
    )


def check_required_for_have_contracts(registry: list[dict],
                                      contracts: dict[str, dict]) -> CheckResult:
    """Check #5: every registry entry with `required_for: [strategy]` has a contract."""
    name = "5. Registry entries with required_for have a contract"
    findings: list[str] = []
    for entry in registry:
        column = entry.get("column", "<unknown>")
        required_for = entry.get("required_for") or []
        if not required_for:
            continue  # informational entries don't need contracts
        if column not in contracts:
            findings.append(
                f"{column}: registry says required_for={required_for}, "
                f"but no freshness contract exists. Add to freshness_contracts.yaml."
            )
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("A column that gates a strategy MUST have a freshness contract — "
                 "otherwise it silently rots when a writer is removed (the career_grade "
                 "outage)."),
    )


def check_populated_by_matches_script(registry: list[dict],
                                      contracts: dict[str, dict]) -> CheckResult:
    """Check #6: contracts' `populated_by` matches the registry's `script` field."""
    name = "6. Contracts' populated_by matches registry script (mislabel guard)"
    findings: list[str] = []
    registry_by_col = {e.get("column"): e for e in registry}
    for column, c in contracts.items():
        populated_by = (c.get("populated_by") or "").strip()
        if not populated_by:
            continue  # not all contracts declare a writer; check #4 catches truly orphaned ones
        entry = registry_by_col.get(column)
        if entry is None:
            continue  # caught by check #4
        registered_script = entry.get("script", "")
        if populated_by != registered_script:
            findings.append(
                f"{column}: contract populated_by={populated_by!r} but "
                f"registry script={registered_script!r}. One of them is mislabeled — "
                f"this is the is_rare_reversal failure mode."
            )
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Mislabeled writers are the most insidious failure class because the "
                 "contract reports green while the data is stale (some other script "
                 "writes signal_freshness rows under the wrong name)."),
    )


def check_unique_columns(registry: list[dict]) -> CheckResult:
    """Check #7: no two registry entries claim the same column."""
    name = "7. Each column has exactly one registry entry"
    findings: list[str] = []
    seen: dict[str, str] = {}
    for entry in registry:
        column = entry.get("column", "")
        if not column:
            continue
        script = entry.get("script", "")
        if column in seen:
            findings.append(
                f"{column}: declared twice — first by {seen[column]}, then by {script}"
            )
        else:
            seen[column] = script
    return CheckResult(
        name=name, passed=not findings, findings=findings,
        details=("Two entries for the same column makes the registry ambiguous. If two "
                 "scripts legitimately write the same column, pick the canonical one and "
                 "note the other in `notes:`."),
    )


# ── Orchestration ───────────────────────────────────────────────────────────


def run_all_checks() -> list[CheckResult]:
    registry = _load_registry()
    contracts = _load_contracts()
    return [
        check_scripts_exist(registry),
        check_write_freshness_calls(registry),
        check_plists_exist_and_invoke(registry),
        check_contracts_have_registry(registry, contracts),
        check_required_for_have_contracts(registry, contracts),
        check_populated_by_matches_script(registry, contracts),
        check_unique_columns(registry),
    ]


def _format_human(results: list[CheckResult]) -> str:
    lines = []
    for r in results:
        marker = "PASS" if r.passed else "FAIL"
        lines.append(f"[{marker}] {r.name}")
        if not r.passed:
            for f in r.findings:
                lines.append(f"  · {f}")
            if r.details:
                lines.append(f"  ─ {r.details}")
    n_pass = sum(1 for r in results if r.passed)
    lines.append("")
    lines.append(f"{n_pass}/{len(results)} checks passed")
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true", help="emit JSON")
    p.add_argument("--check", type=int, help="run only check N (1-7)")
    args = p.parse_args()

    try:
        results = run_all_checks()
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if args.check is not None:
        if not (1 <= args.check <= len(results)):
            print(f"--check must be 1..{len(results)}", file=sys.stderr)
            return 2
        results = [results[args.check - 1]]

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(_format_human(results))

    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
