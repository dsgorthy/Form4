"""Everything the sitemap reads must have a job that writes it.

This is the orphan-writer failure, twice on the same surface:

- `insiders.slug` is the canonical URL segment. `scripts/backfill_insider_slugs.py`
  was written to be run BY HAND, and nothing ever scheduled it — no plist, no
  Dagster asset, no cron. By 2026-09-27, 84,414 of 213,238 insiders had no slug,
  and 5,034 of those were being submitted in the sitemap at the derived
  `/insider/{name}-{sqid}` form rather than the stored canonical `/insider/{name}`.
- `sitemap_quality_{insiders,companies}` decide which pages get submitted at all.
  A frozen table does not error; it silently stops admitting pages that became
  eligible after it froze.

Same shape as `value_pct_of_adv`, which stopped being written on 2026-08-28 and
left every September filing NULL while the strategy read it. See the memory
`feedback_writer_registry_pattern`: a column the product depends on needs a
registered writer AND a schedule, and the check has to be mechanical because
nothing about a missing writer is visible from the read side.

Ordering matters too: slugs must be assigned before the quality tables are read,
or a newly eligible insider is published at a derived URL for a day.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPS = ROOT / "dataplane" / "dagster_project" / "assets" / "form4_ops.py"
REGISTRY = ROOT / "dataplane" / "deploy" / "scheduled_work.yaml"

#: asset function name -> the schedule that must fire it
REQUIRED = {
    "ops_insider_slugs": "ops_insider_slugs_daily",
    "ops_sitemap_quality": "ops_sitemap_quality_daily",
    # Not a sitemap input — the record of whether anyone is READING the sitemap.
    # The ten days it took to find the 2026-09-27 deindexing bug were ten days
    # without Googlebot's request rate, because the access log rotates daily.
    "ops_crawler_activity": "ops_crawler_activity_10min",
}


def _ops() -> str:
    return OPS.read_text()


def _cron_of(src: str, sched: str) -> str:
    m = re.search(rf'_sched\("{sched}",[^)]*?"([^"]+)"\s*\)', src, re.S)
    assert m, f"schedule {sched} not found"
    return m.group(1)


def test_each_sitemap_input_is_defined_as_an_asset():
    src = _ops()
    for fn in REQUIRED:
        assert re.search(rf"^def {fn}\(", src, re.M), f"{fn} is not defined"


def test_each_asset_is_exported_so_dagster_loads_it():
    """An asset that is defined but absent from form4_ops_assets never runs,
    and looks completely fine in the file."""
    src = _ops()
    exported = src[src.index("form4_ops_assets = ["):]
    exported = exported[: exported.index("]")]
    for fn in REQUIRED:
        assert fn in exported, (
            f"{fn} is defined but missing from form4_ops_assets, so Dagster "
            "never loads it and nothing writes what the sitemap reads"
        )


def test_each_asset_has_a_schedule():
    src = _ops()
    for fn, sched in REQUIRED.items():
        m = re.search(rf'_sched\("{sched}",\s*\[([^\]]*)\]', src, re.S)
        assert m, f"{fn} has no schedule named {sched}"
        assert fn in m.group(1), f"schedule {sched} does not select {fn}"


def test_the_crawler_counter_runs_inside_the_logs_retention_window():
    """MEASURED 2026-09-28: the access log retains NINETEEN MINUTES — 9,176
    lines spanning 16:03 to 16:22, because Caddy logs every request header at
    ~1.6 KB a line against a 30 MB cap, at ~29,000 bot requests an hour.

    So the schedule must fire at least as often as the slot size, or slots go
    unobserved. A first version ran hourly and recorded per-hour counts, which
    would have seen a third of each hour AND overwritten fuller counts with
    partial ones. Both wrong, and wrong quietly.
    """
    src = _ops()
    cron = _cron_of(src, REQUIRED["ops_crawler_activity"])
    minute, hour = cron.split()[0], cron.split()[1]
    assert hour == "*", f"crawler activity runs at {cron!r}, not every hour"
    m = re.fullmatch(r"\*/(\d+)", minute)
    assert m, (
        f"crawler activity runs at minute {minute!r}; it has to be a */N step so "
        "it fires inside the retention window"
    )
    step = int(m.group(1))
    script = (ROOT / "scripts" / "record_crawler_activity.py").read_text()
    slot = int(re.search(r"SLOT_MINUTES = (\d+)", script).group(1))
    assert step <= slot, (
        f"the job runs every {step} min but writes {slot}-min slots; slots would "
        "go unobserved"
    )
    assert step <= 19, f"every {step} minutes is outside the 19-minute retention"


def test_a_partial_read_can_never_lower_a_count():
    """The newest slot is still filling. An assigning upsert would let a later
    partial read overwrite a fuller earlier one."""
    src = (ROOT / "scripts" / "record_crawler_activity.py").read_text()
    up = src[src.index("ON CONFLICT (slot, crawler)"):]
    up = up[: up.index('"""', 1)] if '"""' in up else up[:600]
    # Per COLUMN. A bare "GREATEST in up" passed while `requests` had been
    # changed to a plain assignment, because `errors` still used GREATEST.
    # Mutation testing caught it.
    for col in ("requests", "errors"):
        assert re.search(rf"{col} = GREATEST", up), (
            f"the upsert assigns {col} instead of taking GREATEST, so a mid-slot "
            "run can lower a count that a fuller run already recorded"
        )


def test_the_crawler_counter_verifies_search_engines_by_address():
    """A Googlebot user agent is a free-text header, and this very session sent
    one dozens of times while testing the fix. A UA-based count would have
    recorded its own traffic as a crawl recovery."""
    src = (ROOT / "scripts" / "record_crawler_activity.py").read_text()
    # Anchored on the definition and the USE, not the bare name: renaming the
    # constant to GOOGLE_PREFIXES_UNUSED left the substring intact and this
    # assertion passed against a script that no longer verified anything.
    assert re.search(r"^GOOGLE_PREFIXES = \(", src, re.M), (
        "GOOGLE_PREFIXES is no longer defined; Googlebot is not IP-verified"
    )
    assert "66.249." in src, "Google's main crawl range is gone"
    body = src[src.index("def classify("): src.index("def read_log(")]
    assert "startswith(GOOGLE_PREFIXES)" in body, (
        "classify() no longer checks the client address against Google's ranges"
    )
    ip_at = body.index("startswith(GOOGLE_PREFIXES)")
    ua_at = body.index("googlebot_ua_only")
    assert ip_at < ua_at, (
        "the user-agent check runs before the address check, so a spoofed UA "
        "would be counted as a verified crawl"
    )
    assert "googlebot_ua_only" in src, (
        "unverified claims are folded into the real number instead of being "
        "recorded separately"
    )


def test_slugs_are_assigned_before_the_quality_tables_are_read():
    """A new insider that becomes eligible must already have its slug, or the
    sitemap publishes a derived URL that the page then canonicalises away."""
    src = _ops()
    slug_cron = _cron_of(src, REQUIRED["ops_insider_slugs"])
    qual_cron = _cron_of(src, REQUIRED["ops_sitemap_quality"])

    def minutes(cron: str) -> int:
        minute, hour = cron.split()[0], cron.split()[1]
        assert minute.isdigit() and hour.isdigit(), (
            f"expected a fixed daily time, got {cron!r}"
        )
        return int(hour) * 60 + int(minute)

    assert minutes(slug_cron) < minutes(qual_cron), (
        f"slugs run at {slug_cron!r}, quality at {qual_cron!r} — the slug pass "
        "must come first"
    )


def test_the_slug_writer_is_the_one_that_writes():
    """The asset must invoke the script that actually assigns slugs, with
    --apply. A dry run exits 0 and writes nothing, so the asset would be green
    and the column would stay empty — which is exactly how this went unnoticed."""
    src = _ops()
    body = src[src.index("def ops_insider_slugs("):]
    body = body[: body.index("@asset")]
    assert "backfill_insider_slugs.py" in body, (
        "ops_insider_slugs does not call the slug writer"
    )
    assert '"--apply"' in body, (
        "ops_insider_slugs runs the slug script WITHOUT --apply; it would exit "
        "0 having written nothing, and the asset would report success"
    )


def test_the_registry_counts_match_the_module():
    """dataplane/deploy/scheduled_work.yaml is the one place that answers 'what
    is scheduled'. A count that drifts is how a nine-day migration stall went
    unnoticed in 2026-08."""
    src = _ops()
    n_assets = len(re.findall(r"^def (ops_\w+)\(", src, re.M))
    n_sched = len(re.findall(r'_sched\("', src))
    reg = REGISTRY.read_text()
    m = re.search(r'schedule: "(\d+) schedules, (\d+) assets', reg)
    assert m, "the form4_ops entry no longer declares its counts"
    assert (int(m.group(1)), int(m.group(2))) == (n_sched, n_assets), (
        f"registry says {m.group(1)} schedules / {m.group(2)} assets; the module "
        f"has {n_sched} / {n_assets}"
    )
