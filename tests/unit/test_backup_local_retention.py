"""The Studio's own backup copy keeps the newest N verified dumps per database.

2026-11-08 the local copy went from 7 days to the newest 2 dumps per database,
to free the Studio's disk (95% full; eight nights of form4 were 74 GB of it).
The array keeps its 7 days. At two days an AGE rule is dangerous: `find -mtime`
deletes on schedule whether or not tonight's dump worked, so two failed nights
in a row would leave no local dump at all. A COUNT only drops a dump once a
newer one exists -- and that holds only if every *.dump on disk verified, which
is why a dump is written as .partial and renamed after `pg_restore --list`.

These run the real script with pg_dump / pg_restore / ssh / rsync replaced by
exported shell functions. Functions win over PATH (which the script resets), so
nothing here touches a database or the network.
"""
import os
import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backup_databases.sh"

STUBS = r"""
pg_dump() {
    local f=""
    while [ $# -gt 0 ]; do [ "$1" = "-f" ] && f="$2"; shift; done
    [ -n "${FAIL_DUMP:-}" ] && { echo "stub: dump failed" >&2; return 1; }
    printf 'PGDMP' > "$f"
    [ -n "${KILL_IN_DUMP:-}" ] && kill -9 $$   # the box rebooting mid-dump
    return 0
}
pg_restore() { echo "1; 1259 1 TABLE DATA public t owner"; }
ssh() { return 0; }
rsync() { return 0; }
export -f pg_dump pg_restore ssh rsync
exec bash "$0" "$@"
"""

_CONTROLLED = ("LOCAL_KEEP", "RETENTION_DAYS", "FAIL_DUMP", "KILL_IN_DUMP")


def _run(root: Path, *dbs: str, **env: str) -> subprocess.CompletedProcess:
    base = {k: v for k, v in os.environ.items() if k not in _CONTROLLED}
    base.update(
        BACKUP_ROOT=str(root),
        OFFBOX_HOST="invalid.invalid",
        OFFBOX_DIR=str(root / "mirror"),
        **env,
    )
    return subprocess.run(
        ["bash", "-c", STUBS, str(SCRIPT), *dbs],
        env=base, capture_output=True, text=True, timeout=60,
    )


def _seed(root: Path, *names: str) -> None:
    for n in names:
        (root / n).write_bytes(b"PGDMP")


def _dumps(root: Path, db: str) -> list[str]:
    return sorted(p.name for p in root.glob(f"{db}_*.dump"))


def test_keeps_the_newest_two_per_database(tmp_path):
    _seed(
        tmp_path,
        "testdb_20000101_031500.dump",
        "testdb_20000102_031500.dump",
        "testdb_20000103_031500.dump",
        "otherdb_20000101_031500.dump",      # a database this run did not dump
        "testdb_20000101_031500.dump.err",   # not a dump
    )
    r = _run(tmp_path, "testdb")
    assert r.returncode == 0, r.stdout + r.stderr

    kept = _dumps(tmp_path, "testdb")
    assert len(kept) == 2, kept
    assert kept[0] == "testdb_20000103_031500.dump", "the newest older dump must survive"
    assert kept[1] > "testdb_20000103_031500.dump", "tonight's dump must survive"
    assert (tmp_path / "otherdb_20000101_031500.dump").exists()
    assert (tmp_path / "testdb_20000101_031500.dump.err").exists()


def test_failed_nights_delete_nothing(tmp_path):
    """Two failed nights in a row: an age rule would have emptied the folder."""
    good = ["testdb_20000101_031500.dump", "testdb_20000102_031500.dump"]
    _seed(tmp_path, *good)
    three_days_ago = time.time() - 3 * 86400   # past any two-day age rule
    for n in good:
        os.utime(tmp_path / n, (three_days_ago, three_days_ago))
    for _ in range(2):
        r = _run(tmp_path, "testdb", FAIL_DUMP="1")
        assert r.returncode != 0
    assert _dumps(tmp_path, "testdb") == good


def test_a_run_killed_mid_dump_leaves_no_dump(tmp_path):
    """An unverified file must never carry the *.dump name the prune counts."""
    _run(tmp_path, "testdb", KILL_IN_DUMP="1")
    assert _dumps(tmp_path, "testdb") == []
    assert list(tmp_path.glob("testdb_*.dump.partial"))


def test_partials_do_not_count_and_stale_ones_are_cleared(tmp_path):
    _seed(tmp_path, "testdb_20000101_031500.dump")
    stale = tmp_path / "testdb_20000102_031500.dump.partial"   # a killed run
    fresh = tmp_path / "testdb_20000103_031500.dump.partial"   # a run still going
    for p in (stale, fresh):
        p.write_bytes(b"PGD")
    day_ago = time.time() - 86400
    os.utime(stale, (day_ago, day_ago))

    r = _run(tmp_path, "testdb")
    assert r.returncode == 0, r.stdout + r.stderr
    kept = _dumps(tmp_path, "testdb")
    assert len(kept) == 2 and kept[0] == "testdb_20000101_031500.dump", kept
    assert not stale.exists()
    assert fresh.exists()


def test_a_bad_local_keep_prunes_nothing(tmp_path):
    names = [f"testdb_2000010{i}_031500.dump" for i in range(1, 5)]
    _seed(tmp_path, *names)
    r = _run(tmp_path, "testdb", LOCAL_KEEP="0")
    assert r.returncode != 0
    assert set(names) <= set(_dumps(tmp_path, "testdb"))
