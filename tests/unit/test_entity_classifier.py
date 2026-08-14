"""Entity vs. person classification for insider names.

is_entity drives more than casing: leaderboard.py and companies.py filter on
it, so a person wrongly flipped drops off the leaderboard, and an entity
wrongly left as a person gets its first token rotated to the end
("Bulldog Investors General Partnership" -> "Investors General Partnership
Bulldog").

The hard cases are two-letter legal forms that are also human surnames and
initials. Those are pinned here because the failure is silent and plausible
in both directions.
"""
import pytest

from strategies.insider_catalog.entity_resolution import is_entity_name


class TestUnambiguousEntities:
    @pytest.mark.parametrize("name", [
        "Bulldog Investors General Partnership",   # \bpartners?\b misses "partnership"
        "GLAXOSMITHKLINE PLC",
        "Taurus4757 GmbH",
        "FFBW, MHC",
        "Ktown, LP",
        "Abingworth LLP",
        "Gali SCSp",
        "Landgame S.A.R.L.",
        "Essetifin SPA",
        "MASSACHUSETTS MUTUAL LIFE INSURANCE CO",
        "Laura R. Sherman GRAT 2019-2",
        "James F Heyneman Conservatorship",
    ])
    def test_entity(self, name):
        assert is_entity_name(name) is True


class TestUnambiguousPeople:
    @pytest.mark.parametrize("name", [
        "NADELLA SATYA",
        "DELL MICHAEL S",
        "SMITH JOHN MICHAEL",
        "Young Smith Denise",
        "Simmons Smith Catherine A.",
        "O'BRIEN JAMES M",
    ])
    def test_person(self, name):
        assert is_entity_name(name) is False


class TestAmbiguousTwoLetterForms:
    """S.A./N.V./B.V./C.V. are legal forms AND surnames AND initials."""

    @pytest.mark.parametrize("name", [
        "NESTLE SA",
        "Danone S.A.",
        "Prosus N.V.",
        "Stellantis N.V.",
        "Schlumberger B.V.",
        "Iberdrola, S.A.",
        "Banco Santander, S.A.",
        "JAB Cosmetics B.V.",
    ])
    def test_company(self, name):
        assert is_entity_name(name) is True

    @pytest.mark.parametrize("name", [
        "SA THOMAS A",         # surname Sa, given Thomas
        "Paul de Sa",          # surname Sa behind a particle
        "Smith Hatton C.V.",   # C.V. are middle initials, not a legal form
    ])
    def test_person_not_misread_as_company(self, name):
        assert is_entity_name(name) is False

    def test_matched_token_is_excluded_from_the_human_check(self):
        # "Sa" is itself a surname in the lexicon. If the matched token counted
        # as evidence of a human, every "<Company> SA" would read as a person
        # and the ambiguous branch would never fire.
        assert is_entity_name("NESTLE SA") is True
        assert is_entity_name("SA THOMAS A") is False


class TestDegradation:
    def test_empty(self):
        assert is_entity_name("") is False
        assert is_entity_name(None) is False
