"""Every published /insider/... URL shape must resolve.

The filing page builds /insider/{name-slug}-{cik} whenever the insider row
carries a CIK. The resolver only ever matched a CIK against the *whole*
identifier, so all 143,653 filing pages belonging to the 10,279 insiders that
have one linked to a soft 404 — /insider/benjamin-wood-0002123683 was the
reported case.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="API deps live in the Docker image")

from api.routers.insiders import resolve_insider_id  # noqa: E402


class _Row(dict):
    def __getitem__(self, k):
        return dict.__getitem__(self, k)


class _StubConn:
    """Stands in for the one insider the resolver is allowed to find."""

    def __init__(self, slug=None, cik=None, insider_id=126985, aliases=None):
        self.slug, self.cik, self.insider_id = slug, cik, insider_id
        self.aliases = aliases or {}
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.queries.append((sql, params))
        value = params[0] if params else None
        if "insider_slug_aliases" in sql:
            hit = self.aliases.get(value)
            self._row = _Row(insider_id=hit) if hit else None
        elif "WHERE slug = ?" in sql:
            self._row = _Row(insider_id=self.insider_id) if value == self.slug else None
        elif "WHERE insider_id = ?" in sql:
            self._row = _Row(insider_id=value) if value == self.insider_id else None
        elif "WHERE cik = ?" in sql:
            self._row = _Row(insider_id=self.insider_id) if value == self.cik else None
        else:
            self._row = None
        return self

    def fetchone(self):
        return self._row


def test_slug_plus_cik_resolves():
    """The shape the filing page actually emits."""
    conn = _StubConn(slug="benjamin-wood", cik="0002123683")
    assert resolve_insider_id(conn, "benjamin-wood-0002123683") == 126985


def test_bare_cik_still_resolves():
    conn = _StubConn(slug="benjamin-wood", cik="0002123683")
    assert resolve_insider_id(conn, "0002123683") == 126985


def test_clean_slug_wins_before_any_chopping():
    """A hyphenated name must not be mangled into its last segment."""
    conn = _StubConn(slug="roger-s-penske", cik=None)
    assert resolve_insider_id(conn, "roger-s-penske") == 126985


def test_retired_slug_resolves_via_alias():
    conn = _StubConn(slug="james-d-farley-jr", aliases={"jr-james-d-farley": 4242})
    assert resolve_insider_id(conn, "jr-james-d-farley") == 4242


def test_unknown_identifier_is_none():
    """"zzz" decodes cleanly as a sqid, so this only holds because the
    resolver checks the decoded id against a real row before trusting it."""
    conn = _StubConn(slug="benjamin-wood", cik="0002123683")
    assert resolve_insider_id(conn, "nobody-at-all-zzz") is None


def test_decodable_trailing_segment_does_not_shadow_the_cik():
    """A slug ending in alphabet characters must not hijack the lookup."""
    conn = _StubConn(slug=None, cik="zzz", insider_id=99)
    assert resolve_insider_id(conn, "some-name-zzz") == 99


def test_empty_identifier_is_none():
    assert resolve_insider_id(_StubConn(), "") is None


def test_cik_lookup_tries_both_whole_and_trailing():
    """Whole first, so a slug that happens to end in digits cannot shadow it."""
    conn = _StubConn(slug=None, cik="0002123683")
    resolve_insider_id(conn, "benjamin-wood-0002123683")
    cik_params = [p[0] for sql, p in conn.queries if "WHERE cik = ?" in sql]
    assert cik_params == ["benjamin-wood-0002123683", "0002123683"]
