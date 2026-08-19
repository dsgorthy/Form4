"""Catalysts and risks are lists, and must never reach the reader as a literal.

scripts/demo_narratives.py asks the model for "1-3 SPECIFIC catalysts", so it
returns a list. That list went straight to psycopg2 for a TEXT column;
psycopg2 adapts a Python list to a Postgres ARRAY literal, and the cast to
text left the literal itself in the database:

    {"Q2 2026 earnings on August 11, 2026, showed double-digit adjusted
     EBITDA growth...","CEO Sabo retires year-end..."}

The filing page printed that verbatim, braces and quotes included, on 1,897 of
4,503 catalysts rows and 1,527 risks rows. Repaired by
migrations/2026-08-18_narrative_array_literals.sql.
"""
import json
from pathlib import Path

import pytest

from api.narrative import as_bullets

REPO = Path(__file__).resolve().parents[2]


def test_legacy_postgres_array_literal_becomes_a_list():
    raw = '{"first catalyst","second catalyst"}'
    assert as_bullets(raw) == ["first catalyst", "second catalyst"]


def test_commas_inside_an_item_are_not_split_on():
    """The failure a naive split would produce. Real catalysts read
    "Q2 2026 earnings on August 11, 2026, showed..." — three commas in one
    item, which a str.split(',') shreds into fragments mid-sentence."""
    raw = '{"Q2 2026 earnings on August 11, 2026, showed growth.","CEO retires."}'
    assert as_bullets(raw) == [
        "Q2 2026 earnings on August 11, 2026, showed growth.",
        "CEO retires.",
    ]


def test_json_array_string_becomes_a_list():
    assert as_bullets(json.dumps(["a", "b"])) == ["a", "b"]


def test_a_real_list_passes_through():
    assert as_bullets(["a", "b"]) == ["a", "b"]


def test_prose_becomes_a_single_bullet():
    assert as_bullets("Insufficient data — needs manual review.") == [
        "Insufficient data — needs manual review."
    ]


@pytest.mark.parametrize("empty", [None, "", "   ", "{}", "[]"])
def test_empty_values_are_none_not_an_empty_bullet(empty):
    assert as_bullets(empty) is None


def test_writer_json_encodes_lists():
    """The other half. Without this the next generation run rewrites the same
    literals the migration just cleaned up."""
    src = (REPO / "scripts/demo_narratives.py").read_text()
    assert "_as_json_text(narrative.get(\"catalysts\"))" in src
    assert "_as_json_text(narrative.get(\"risks\"))" in src
    assert "def _as_json_text" in src


def test_filing_page_renders_bullets_not_a_paragraph():
    src = (REPO / "frontend/src/app/filing/[id]/page.tsx").read_text()
    assert "Array.isArray(value)" in src, "renderer must handle a list"
    assert "<p className=\"text-[#22C55E]/90\">{filing.narrative.catalysts}</p>" not in src
