"""A rebuild of the API image must ship code changes, not library upgrades.

Only stripe was pinned until 2026-11-14. An unpinned stripe upgrade (15.x)
broke the Stripe webhook in August, and any rebuild would also have moved
fastapi, uvicorn, starlette and pydantic to whatever was newest that day,
which is why the API went six weeks without one. api/constraints.txt holds the
exact versions production runs, and the image installs under it.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO / "deploy/Dockerfile.api"
CONSTRAINTS = REPO / "api/constraints.txt"
REQUIREMENTS = REPO / "api/requirements.txt"


def _pins(path: Path) -> dict[str, str]:
    pins = {}
    for line in path.read_text().splitlines():
        m = re.match(r"^\s*([A-Za-z0-9_.\-]+)(?:\[[^\]]*\])?==(\S+)", line)
        if m:
            pins[m.group(1).lower().replace("_", "-")] = m.group(2)
    return pins


def test_the_image_installs_under_the_constraints():
    src = DOCKERFILE.read_text()
    assert "COPY api/constraints.txt /app/api/constraints.txt" in src, "constraints never reach the image"
    install = src[src.index("RUN pip install"):].split("\nCOPY", 1)[0]
    assert "-c /app/api/constraints.txt" in install, "pip install ignores the constraints"


def test_the_libraries_that_can_break_the_api_are_pinned():
    pins = _pins(CONSTRAINTS)
    missing = [lib for lib in ("fastapi", "uvicorn", "starlette", "pydantic", "pydantic-core",
                               "stripe", "psycopg2-binary", "httpx", "slowapi", "pyjwt")
               if lib not in pins]
    assert not missing, f"unpinned in api/constraints.txt: {missing}"


def test_requirements_and_constraints_agree():
    constraints = _pins(CONSTRAINTS)
    clash = {name: (version, constraints.get(name)) for name, version in _pins(REQUIREMENTS).items()
             if constraints.get(name) != version}
    assert not clash, f"api/requirements.txt and api/constraints.txt disagree: {clash}"
