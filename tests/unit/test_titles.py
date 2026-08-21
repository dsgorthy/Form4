"""Filer job titles have to survive contact with a public page.

The title field on a Form 4 is typed by the filer and validated by nobody, so
the corpus stores things like "GroupPresident IntlVehiclePmts" and
"Director,TenPercentOwner; Director". Those shipped verbatim: on 2026-08-21 the
CPAY filing page read "Alan King, GroupPresident IntlVehiclePmts at CORPAY,
INC.", and the Stocktwits post said the same.

api/titles.py is the single definition. pipelines/insider_study/annotate_trade
imports it rather than keeping a copy, so the website and the social posts
cannot drift apart on what somebody's job is.
"""
from __future__ import annotations

import pytest

from api.titles import clean_title


@pytest.mark.parametrize(
    "raw,expected",
    [
        # The filing that prompted this.
        ("GroupPresident IntlVehiclePmts",
         "Group President International Vehicle Payments"),
        # 11,703 of the 13,672 run-together filings.
        ("TenPercentOwner", "10% Owner"),
        ("Director,TenPercentOwner", "Director, 10% Owner"),
        # Repeated components collapse rather than stuttering.
        ("Director,TenPercentOwner; Director", "Director, 10% Owner"),
        ("Director; Director,TenPercentOwner", "Director, 10% Owner"),
        # "Other" carries no information and must not read as a job.
        ("Director,TenPercentOwner,Other", "Director, 10% Owner"),
        # Whole-title abbreviations.
        ("Dir", "Director"),
        ("dir.", "Director"),
        ("10%", "10% Owner"),
        # Word-level ones inside a longer title.
        ("EVP, GenCnsl & Secy", "EVP, General Counsel & Secretary"),
        # Already clean titles are left exactly alone.
        ("CEO", "CEO"),
        ("Chief Executive Officer", "Chief Executive Officer"),
        ("President & CEO", "President & CEO"),
        ("Chief Revenue Officer", "Chief Revenue Officer"),
    ],
)
def test_clean_title(raw, expected):
    assert clean_title(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", None, "Unknown", "n/a", "NONE", "-", "Other"])
def test_nothing_usable_reads_as_the_generic_noun(raw):
    """Never render "Unknown" or an empty string as though it were a job title.

    "Unknown Goldman Sachs Group Inc. bought $10.3M" shipped as a public post.
    """
    assert clean_title(raw) == "Insider"


def test_an_acronym_is_not_split():
    """The run-together split keys on lower->upper, so all-caps survives."""
    for t in ("CEO", "CFO", "EVP", "SVP", "COO"):
        assert clean_title(t) == t


def test_output_is_always_renderable():
    """Every caller puts this straight into a sentence; None would print."""
    for raw in ["", None, "Other", "???", "Director", "GroupPresident IntlVehiclePmts"]:
        out = clean_title(raw)
        assert isinstance(out, str) and out.strip(), repr(raw)


def test_the_social_posts_and_the_website_share_one_definition():
    """A second copy is how the two surfaces drift."""
    from pipelines.insider_study.annotate_trade import clean_title as from_pipeline
    assert from_pipeline is clean_title, (
        "annotate_trade has its own clean_title again. It must import from "
        "api.titles — two implementations means the post and the page can "
        "describe the same person differently."
    )


def test_normalized_title_is_not_a_substitute():
    """Documents why the DB column that looks like the fix is not the fix.

    `normalized_title` buckets to Director / CEO / CFO / 10% Owner / Other.
    "GroupPresident IntlVehiclePmts" normalises to "Other", which clean_title
    correctly refuses to render as a job. Use the column to filter, never to
    display.
    """
    assert clean_title("Other") == "Insider"
    assert clean_title("GroupPresident IntlVehiclePmts") != "Insider"
