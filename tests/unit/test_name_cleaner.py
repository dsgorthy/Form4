"""Name normalization: EDGAR "LAST FIRST MIDDLE" -> "First Middle Last".

The compound-surname cases are the reason this file exists. Rotating token 0
to the end is correct for a one-token surname and silently wrong for a
two-token one, and the wrong output is plausible enough to survive review —
"Smith Catherine A. Simmons" looks like a name. These tests pin both the fix
and the false-positive classes it must not touch.
"""
import pytest

from strategies.insider_catalog.name_cleaner import (
    GIVEN_TOKENS,
    SURNAME_TOKENS,
    clean_entity_name,
    clean_person_name,
)


class TestSingleTokenSurname:
    """The common case: rotation is right and must stay untouched."""

    @pytest.mark.parametrize("raw,expected", [
        ("NADELLA SATYA", "Satya Nadella"),
        ("DELL MICHAEL S", "Michael S. Dell"),
        ("SMITH BRADFORD L", "Bradford L. Smith"),
        ("Komin Robert Patrick Jr.", "Robert Patrick Komin Jr."),
        ("O'BRIEN JAMES M", "James M. O'Brien"),
        ("MCDONALD ANGUS", "Angus McDonald"),
    ])
    def test_rotation(self, raw, expected):
        assert clean_person_name(raw) == expected

    def test_two_given_ish_tokens_stay_default(self):
        # "John" and "Michael" both read as given names, so the compound test
        # abstains and the plain rotation stands.
        assert clean_person_name("SMITH JOHN MICHAEL") == "John Michael Smith"


class TestCompoundSurname:
    """[LAST1 LAST2 FIRST] and [LAST1 LAST2 FIRST INITIAL]."""

    @pytest.mark.parametrize("raw,expected", [
        ("Young Smith Denise", "Denise Young Smith"),
        ("SIMMONS SMITH CATHERINE A", "Catherine A. Simmons Smith"),
        ("Simmons Smith Catherine A.", "Catherine A. Simmons Smith"),
        ("FORRESTER ROGERS JULIA P.", "Julia P. Forrester Rogers"),
        ("Matos Rodriguez Felix V.", "Felix V. Matos Rodriguez"),
        ("Perez Garcia Andrea G", "Andrea G. Perez Garcia"),
    ])
    def test_compound(self, raw, expected):
        assert clean_person_name(raw) == expected

    def test_suffix_survives_reorder(self):
        assert clean_person_name("Young Smith Denise Jr") == "Denise Young Smith Jr."


class TestEdgarPunctuationQuirks:
    """Shapes EDGAR emits that the plain LAST-FIRST-MIDDLE model doesn't cover."""

    @pytest.mark.parametrize("raw,expected", [
        # Comma as surname/given separator — was riding into the output.
        ("Maas, Jacob", "Jacob Maas"),
        ("WESCHLER, R. TED", "R. Ted Weschler"),
        ("Bailey Anne E,", "Anne E. Bailey"),
        ("Lindqvist Lars, Goran", "Lars Goran Lindqvist"),
    ])
    def test_comma_is_a_separator(self, raw, expected):
        assert clean_person_name(raw) == expected

    @pytest.mark.parametrize("raw,expected", [
        # Suffix immediately after the surname, not at the end.
        ("Ruch, Jr. Charles E.", "Charles E. Ruch Jr."),
        ("KENNEDY, III BRYAN F", "Bryan F. Kennedy III"),
        ("Cook, II John M.", "John M. Cook II"),
        # ...and the ordinary trailing form still works.
        ("Cobb Paul W, Jr.", "Paul W. Cobb Jr."),
        ("SUH, DAVID JR.", "David Suh Jr."),
    ])
    def test_suffix_after_surname(self, raw, expected):
        assert clean_person_name(raw) == expected

    def test_apostrophe_restored(self):
        # EDGAR strips the apostrophe; a bare one-letter surname is the tell.
        assert clean_person_name("O'BRIEN JAMES M") == "James M. O'Brien"
        assert clean_person_name("D AMBROSE MICHAEL") == "Michael D'Ambrose"


class TestFalsePositiveGuards:
    """Classes that look compound to the frequency signal but are not."""

    def test_cjk_names_keep_plain_rotation(self):
        # Already surname-first in the source; flipping yields "Jun Kong Xiao".
        assert clean_person_name("KONG XIAO JUN") == "Xiao Jun Kong"
        assert clean_person_name("WANG XIAO MING") == "Xiao Ming Wang"

    def test_nobiliary_particle_keeps_lowercase_form(self):
        # The particle branch owns these; the compound branch must defer so
        # the lowercase "de" is preserved.
        assert clean_person_name("DE BOCK PETER") == "Peter de Bock"

    def test_middle_initial_is_not_a_second_surname(self):
        assert clean_person_name("SMITH J ALBERT") == "J. Albert Smith"

    def test_five_tokens_abstain(self):
        # Out of the shapes the discriminator was trained for.
        out = clean_person_name("SMITH JOHN PAUL GEORGE RINGO")
        assert out.endswith("Smith")


class TestLexicon:
    """A missing/empty lexicon must degrade to plain rotation, not crash."""

    def test_lexicon_loaded(self):
        assert "catherine" in GIVEN_TOKENS
        assert "smith" in SURNAME_TOKENS
        # Disjointness is the whole point of the ratio thresholds.
        assert not (GIVEN_TOKENS & SURNAME_TOKENS)

    def test_absent_token_abstains(self):
        # "Salaam" is a real given name with too little evidence to clear the
        # floor, so we decline rather than guess.
        assert "salaam" not in GIVEN_TOKENS
        assert clean_person_name("Coleman Smith Salaam") == "Smith Salaam Coleman"


class TestEntities:
    def test_entity_untouched_when_mixed_case(self):
        assert clean_entity_name("RES Business Management LLC") == "RES Business Management LLC"

    def test_entity_titlecased_when_shouting(self):
        assert clean_entity_name("TTWFGP LLC") == "Ttwfgp LLC"
