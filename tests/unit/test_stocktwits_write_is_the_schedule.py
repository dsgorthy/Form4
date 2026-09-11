"""--write must produce the file that is actually read, in the form it is read.

WHAT WENT WRONG (2026-09-10)

`--write` wrote pipelines/data/content/2026-09-10_stocktwits.txt on the Studio:
generate_daily_content's directory, a different filename, and the three bodies
joined by rules -- no title, no velocity warning, no times. Every file anyone
has ever posted from lives at data/content/stocktwits_{date}.txt on the Mini,
and 09-10's was typed out by hand from the terminal to match the older ones.

The two paths parted at 63d046b, which added the schedule (the warning and the
"POST NOW / POST AT ~HH:MM" stamps) to the print path only. The stamps exist
because the account was suspended on 2026-08-24 (ten posts at once) and flagged
on 2026-08-28 (three within seconds); a file without them is exactly the
artefact that invited pasting the posts together.

So: one function composes the document, stdout and --write both take it, and
it lands where it is read.
"""
from __future__ import annotations

import ast
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("psycopg2")

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import pipelines.generate_stocktwits_posts as gen  # noqa: E402

SRC = (REPO / "pipelines" / "generate_stocktwits_posts.py").read_text(encoding="utf-8")

DAY = "2026-09-10"
NOW = datetime(2026, 9, 10, 23, 19)
PICKED = [{"ticker": "GME"}, {"ticker": "QVCG"}, {"ticker": "WBD"}]
BODIES = [
    "$GME — President, CEO and Chairman Ryan Cohen bought $20.4M\n\nNot investment advice.",
    "$QVCG — 10% Owner Goldentree bought $39.9M over a month\n\nNot investment advice.",
    "$WBD — 7 insiders have sold $44.4M here in a month\n\nNot investment advice.",
]

HEADER = re.compile(r"^POST (\d+) of (\d+) — \$(\S+)\s+(POST NOW|POST AT ~(\d\d:\d\d))$")


def _doc(**kw) -> str:
    return gen.compose_schedule(DAY, PICKED, BODIES, now=NOW, **kw)


def test_the_document_opens_with_the_velocity_warning():
    """The warning has to come before the first post, not after it or nowhere:
    the file is read top-down by a person about to paste."""
    doc = _doc()
    head = doc[:doc.index("POST 1 of")]
    assert doc.startswith(f"STOCKTWITS — {DAY}\n"), doc[:40]
    assert (f"*** POST THESE {gen.MIN_MINUTES_BETWEEN_POSTS} MINUTES APART. "
            "DO NOT PASTE THEM TOGETHER. ***") in head
    assert "2026-08-24" in head and "2026-08-28" in head, (
        "both flagging incidents belong in the file, next to the instruction "
        "they justify"
    )


def test_one_stamped_block_per_post_in_order():
    """Three posts, three 'POST N of 3' headers, each with a clock time
    MIN_MINUTES_BETWEEN_POSTS after the last."""
    doc = _doc()
    headers = [HEADER.match(line) for line in doc.splitlines() if line.startswith("POST ")]
    assert len(headers) == len(PICKED) and all(headers), doc
    for i, (m, t) in enumerate(zip(headers, PICKED), 1):
        assert int(m.group(1)) == i and int(m.group(2)) == len(PICKED)
        assert m.group(3) == t["ticker"]
        expected = NOW + timedelta(minutes=gen.MIN_MINUTES_BETWEEN_POSTS * (i - 1))
        if i == 1:
            assert m.group(4) == "POST NOW"
        else:
            assert m.group(5) == f"{expected:%H:%M}", (
                f"post {i} is stamped {m.group(5)}, expected {expected:%H:%M}"
            )
    # Bodies verbatim, in the order they were ranked, each under its header.
    positions = [doc.index(b) for b in BODIES]
    assert positions == sorted(positions)
    for m, b in zip(headers, BODIES):
        assert doc.index(m.group(0)) < doc.index(b)


def test_a_no_record_run_says_so():
    """Tomorrow's cooldown cannot see a post that was never recorded; the
    reader has to be told, as the hand-written 09-10 file was."""
    assert "--no-record" not in _doc()
    assert f"{gen.TICKER_COOLDOWN_DAYS}-day cooldown" in _doc(no_record=True)


def test_write_lands_where_it_is_read(tmp_path):
    """<repo>/data/content/stocktwits_{date}.txt, the name and place of every
    file ever posted from. Not pipelines/data/content/{date}_stocktwits.txt,
    which is where --write put it on 2026-09-10."""
    assert gen.OUTPUT_DIR == REPO / "data" / "content", gen.OUTPUT_DIR
    doc = _doc()
    p = gen.write_schedule(doc, DAY, out_dir=tmp_path)
    assert p == tmp_path / f"stocktwits_{DAY}.txt"
    assert p.read_text(encoding="utf-8") == doc, "the file is the document, byte for byte"


def test_stdout_and_the_file_are_one_document():
    """main() prints exactly what --write writes. A second rendering, however
    similar, is how the file lost the schedule at 63d046b."""
    body = SRC[SRC.index("def main("):]
    printed = re.findall(r"\bprint\(\s*(\w+)", body)
    assert printed == ["doc"], (
        f"main() prints {printed}; it must print the composed document once "
        "and nothing else, so stdout over ssh equals the file"
    )
    assert re.search(r"write_schedule\(\s*doc\b", body), (
        "--write must hand write_schedule the same document that was printed"
    )


def test_the_mini_invocation_is_documented():
    """The generator needs the Studio's database and the file is read on the
    Mini; the docstring must say how one command does both."""
    doc = ast.get_docstring(ast.parse(SRC)) or ""
    assert "--count 3 --write" in doc and "ssh" in doc
    assert "data/content/stocktwits_{date}.txt" in doc
    assert "def _run_on_studio" in SRC and "def _on_studio" in SRC
