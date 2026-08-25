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
    src = re.sub(r"/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"),
                 src, flags=re.S)
    src = re.sub(r"//[^\n]*", "", src)

    found, quote, line, i = [], None, 1, 0
    while i < len(src):
        c = src[i]
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
