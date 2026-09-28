"""`\\u2014` in JSX TEXT renders as the six characters, not an em-dash.

Inside braces it is a JavaScript string literal and escapes normally:

    {value ?? "\\u2014"}          -> renders  —

Directly in the markup it is not:

    <p>one lot \\u2014 one decision</p>   -> renders  \\u2014

Both forms sat four lines apart on the insider page and only the second was
wrong, which is exactly why it shipped: the surrounding code looked like
precedent. Derek caught it on the live page.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "frontend/src"
FILES = sorted(SRC.rglob("*.tsx"))


def _unquoted_escapes(src: str) -> list[tuple[int, str]]:
    """Every \\uXXXX that is NOT inside a quoted string.

    The first version of this tracked BRACE DEPTH and only looked at depth 0,
    reasoning that JSX text lives outside {...}. That is wrong: the whole
    component body sits inside an arrow function, so depth was never 0 and the
    scan collected nothing -- it passed against the very bug it was written
    for. Mutation testing caught it; the assertion had never been exercised.

    Quote state is the thing that actually decides. Inside a string literal
    the escape is processed by JS; anywhere else it is literal markup.
    Comments are stripped first so prose about the bug is not mistaken for it.
    """
    found, quote, line, i = [], None, 1, 0
    while i < len(src):
        c = src[i]
        # Comments are recognised HERE, inside the same state machine, and only
        # when not already in a string. Stripping them with a regex first was
        # wrong: `//` occurs inside every URL literal
        # ("http://localhost:8000/api/v1"), so the strip ate the rest of the
        # line INCLUDING the template literal's closing backtick. A backtick
        # deliberately survives newlines, so one URL flipped the quote state
        # for the whole remainder of the file and every escape below it was
        # misjudged -- reported as 3 failures on a page whose JSX never
        # changed (2026-09-27), and it would equally have HIDDEN a real one.
        if quote is None and c == "/" and i + 1 < len(src):
            if src[i + 1] == "/":
                while i < len(src) and src[i] != "\n":
                    i += 1
                continue
            if src[i + 1] == "*":
                end = src.find("*/", i + 2)
                end = len(src) if end == -1 else end + 2
                line += src[i:end].count("\n")
                i = end
                continue
        if c == "\n":
            line += 1
            # A ' or " string cannot span a newline, so reset. Without this a
            # single apostrophe in JSX prose ("what if you'd bought") opens a
            # quote that never closes and every escape below it in the file is
            # misjudged -- which produced 8 false-positive files on the first
            # run. Backticks legitimately span lines, so they persist.
            if quote in ("'", '"'):
                quote = None
        elif quote:
            if c == "\\":
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in "\"'`":
            quote = c
        elif c == "\\" and re.match(r"\\u[0-9a-fA-F]{4}", src[i:i + 6]):
            found.append((line, src[i:i + 6]))
        i += 1
    return found


@pytest.mark.parametrize("path", FILES, ids=lambda p: str(p.relative_to(SRC)))
def test_no_unicode_escape_sits_in_jsx_markup(path):
    bad = [f"{path.relative_to(SRC)}:{lineno}: {esc}"
           for lineno, esc in _unquoted_escapes(path.read_text())]
    assert not bad, (
        "unicode escape in JSX text -- it will render literally. Use the "
        "character itself:\n  " + "\n  ".join(bad))
