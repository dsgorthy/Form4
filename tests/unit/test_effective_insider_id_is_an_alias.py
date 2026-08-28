"""effective_insider_id is an ALIAS POINTER, not a foreign key. Never count it.

It is set only when two insider records were merged, so it is NULL on ~98% of
rows BY DESIGN. That is correct and nothing needs fixing in the column.

What needs fixing is how it gets used. On 2026-08-27 I reported "graded
insiders: 565 -> 568, +0.5%" from COUNT(DISTINCT effective_insider_id) after a
reload that added 83,506 insiders. The real figure was 64,539. The metric was
not flat; it was measuring a column that is almost always NULL.

Use insider_id to COUNT. Use effective_insider_id only to RESOLVE a merged
identity, which is what api/routers/filings.py does.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_nothing_counts_distinct_effective_insider_id():
    offenders = []
    for path in list((REPO / "api").rglob("*.py")) + \
                list((REPO / "pipelines").rglob("*.py")) + \
                list((REPO / "scripts").rglob("*.py")):
        if "archive" in str(path):
            continue
        src = path.read_text(encoding="utf-8", errors="ignore").lower()
        if "count(distinct effective_insider_id" in src.replace("\n", " "):
            offenders.append(str(path.relative_to(REPO)))
    assert not offenders, (
        f"{offenders} counts DISTINCT effective_insider_id. It is NULL on ~98% "
        "of rows by design — it only points somewhere when two insider records "
        "were merged. Count insider_id instead."
    )
