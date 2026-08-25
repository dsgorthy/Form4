"""Every third-party import must be in a requirements file.

WHAT HAPPENED

`pipelines/congress_scraper/scrape_capitol_trades.py` imports bs4 at module
scope. beautifulsoup4 was in neither requirements.txt nor api/requirements.txt,
so CI could not import it, and `test_congress_date_parse.py` failed at
COLLECTION. CI ran pytest with `-x`, so that one ImportError stopped the run:
**60 consecutive red builds**, every one of them reporting a single missing
package while 1,396 tests were never executed at all.

It survived because CI installed the requirements files PLUS a hand-maintained
`pip install httpx stripe PyJWT pydantic python-dotenv resend slowapi` line.
Adding an import to the codebase did not require touching that line, so nothing
connected the two. That line is gone; the requirements files are now the only
declaration, and this test keeps them honest.

Module scope only. A deferred import inside a function does not break
collection, and try/except ImportError is a deliberate optional dependency.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SOURCE_DIRS = ("api", "pipelines", "framework", "config", "scripts")

_PACKAGE_ROOTS = {"api", "pipelines", "framework", "config", "scripts",
                  "strategies", "dataplane", "tests", "board",
                  "dagster_project"}


def _first_party() -> set[str]:
    """Package roots PLUS every module name that exists as a file in the repo.

    Several scripts under pipelines/insider_study import their siblings flat
    (`from pit_scoring import ...`, `from theta_client import ...`) because
    they are run directly rather than as a package. Those are first-party and
    a static list of top-level package names does not see them -- the first
    version of this test reported six of them as missing dependencies.
    """
    names = set(_PACKAGE_ROOTS)
    # strategies/ is scanned for NAMES but not for imports: scripts under
    # pipelines/insider_study import strategies/insider_catalog/pit_scoring.py
    # flat, because they run as scripts rather than as a package.
    for d in SOURCE_DIRS + ("strategies",):
        for f in (REPO / d).rglob("*.py"):
            names.add(f.stem)
            if f.name == "__init__.py":
                names.add(f.parent.name)
    return names


FIRST_PARTY = _first_party()

#: import name -> distribution name, where they differ.
DIST_ALIASES = {
    "bs4": "beautifulsoup4",
    "jwt": "pyjwt",
    "yaml": "pyyaml",
    "dotenv": "python-dotenv",
    "psycopg2": "psycopg2-binary",
    "dateutil": "python-dateutil",
    "telegram": "python-telegram-bot",
    "sklearn": "scikit-learn",
    "PIL": "pillow",
    "fitz": "pymupdf",
    "cv2": "opencv-python",
}


def _declared() -> set[str]:
    names = set()
    for f in ("requirements.txt", "api/requirements.txt"):
        for line in (REPO / f).read_text().splitlines():
            line = line.split("#")[0].strip()
            if not line:
                continue
            # strip extras and version pins: PyJWT[crypto]>=2 -> pyjwt
            names.add(re.split(r"[\[<>=!;\s]", line)[0].strip().lower())
    return names


def _module_scope_imports(path: Path) -> set[str]:
    """Top-level imports only, excluding try/except ImportError guards."""
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return set()

    guarded = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Try) and any(
                isinstance(h.type, ast.Name) and h.type.id in
                ("ImportError", "ModuleNotFoundError")
                or (isinstance(h.type, ast.Tuple) and any(
                    getattr(e, "id", "") in ("ImportError", "ModuleNotFoundError")
                    for e in h.type.elts))
                for h in node.handlers):
            for sub in ast.walk(node):
                guarded.add(id(sub))

    found = set()
    for node in tree.body:                       # module scope only
        stack = [node]
        while stack:
            n = stack.pop()
            if id(n) in guarded:
                continue
            if isinstance(n, ast.Import):
                found.update(a.name.split(".")[0] for a in n.names)
            elif isinstance(n, ast.ImportFrom):
                if n.level == 0 and n.module:
                    found.add(n.module.split(".")[0])
            elif isinstance(n, ast.Try):
                stack.extend(ast.iter_child_nodes(n))
    return found


def _third_party() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for d in SOURCE_DIRS:
        for path in (REPO / d).rglob("*.py"):
            if "archive" in path.parts or ".venv" in path.parts:
                continue
            for mod in _module_scope_imports(path):
                if mod in FIRST_PARTY or mod in sys.stdlib_module_names:
                    continue
                if mod.startswith("_"):
                    continue
                out.setdefault(mod, []).append(
                    str(path.relative_to(REPO)))
    return out


THIRD_PARTY = _third_party()


@pytest.mark.parametrize("module", sorted(THIRD_PARTY))
def test_every_third_party_import_is_declared(module):
    dist = DIST_ALIASES.get(module, module).lower()
    declared = _declared()
    assert dist in declared, (
        f"`import {module}` (needs `{dist}`) is not in requirements.txt or "
        f"api/requirements.txt, so CI cannot install it.\n"
        f"  imported at module scope by: {THIRD_PARTY[module][0]}\n"
        f"  add it, or guard the import with try/except ImportError if it is "
        f"genuinely optional.")


def test_ci_installs_only_from_the_requirements_files():
    """A hand-maintained pip list in CI is how bs4 went undeclared for months:
    adding an import never required touching it."""
    ci = (REPO / ".github/workflows/ci.yml").read_text()
    install = ci[ci.index("Install dependencies"):ci.index("Lint")]
    for line in install.splitlines():
        line = line.strip()
        if not line.startswith("pip install"):
            continue
        if "-r requirements.txt" in line:
            continue
        # Test tooling is not an application dependency and is allowed here.
        pkgs = set(line.replace("pip install", "").split())
        assert pkgs <= {"ruff", "pytest"}, (
            f"CI installs application packages outside the requirements "
            f"files: {sorted(pkgs - {'ruff', 'pytest'})}")


def test_ci_does_not_stop_at_the_first_failure():
    """`-x` turned 1,396 unknown-state tests into one reported error."""
    ci = (REPO / ".github/workflows/ci.yml").read_text()
    run = [l for l in ci.splitlines() if "pytest tests/unit" in l]
    assert run, "no pytest invocation found in ci.yml"
    for line in run:
        assert " -x" not in line, (
            "CI runs pytest with -x, so one collection error hides the rest "
            "of the suite")


# ── the CI contract itself ──────────────────────────────────────────────────

def test_lint_covers_more_than_the_api_directory():
    """api/ alone missed three F821s.

    E9/F63/F7/F82 are bugs, not style: syntax errors, undefined names,
    invalid comparisons, f-strings with no placeholders. Restricting them to
    api/ meant `import notify` against a module deleted months earlier, and
    two dangling forward references, all sat unflagged.
    """
    ci = (REPO / ".github/workflows/ci.yml").read_text()
    lint = [l for l in ci.splitlines() if "ruff check" in l]
    assert lint, "no ruff invocation in ci.yml"
    scope = " ".join(ci[ci.index("ruff check"):ci.index("- name: Test")].split())
    for d in ("pipelines/", "framework/", "scripts/"):
        assert d in scope, f"ruff does not lint {d}: {scope[:160]}"


def test_pushes_do_not_race_the_deploy():
    """Every push to main ends in a self-hosted `git pull` + deploy in one
    shared working copy. Overlapping runs fight over the same checkout."""
    import yaml
    ci = yaml.safe_load((REPO / ".github/workflows/ci.yml").read_text())
    conc = ci.get("concurrency")
    assert conc, "ci.yml has no concurrency group, so pushes race the deploy"
    assert conc.get("cancel-in-progress") is False, (
        "cancel-in-progress would kill a deploy midway, which is worse than "
        "queueing behind it")


def test_the_production_deploy_is_gated_on_tests():
    import yaml
    ci = yaml.safe_load((REPO / ".github/workflows/ci.yml").read_text())
    needs = ci["jobs"]["deploy-prod"].get("needs") or []
    assert "backend" in needs and "frontend" in needs, (
        "the production deploy must depend on both test jobs")
