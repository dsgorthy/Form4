"""Any migration that runs DDL on a hot table must set lock_timeout first.

WHAT WENT WRONG

On 2026-08-27 `migrations/2026-08-27_drop_dead_annotation_columns.sql` took
form4.app down for about thirty minutes. Nothing in it was wrong; it never even
acquired its lock, and it dropped nothing.

The mechanism is that Postgres grants lock requests IN ORDER:

  1. A 220-minute diagnostic query held AccessShareLock on `trades`.
  2. `ALTER TABLE trades DROP COLUMN ...` requested AccessExclusiveLock and,
     finding the table busy, sat down at the head of the queue.
  3. Every read that arrived AFTERWARDS queued behind the ALTER -- even though
     those reads only needed AccessShare and were perfectly compatible with the
     query actually holding the table.
  4. 86 API requests piled up, hit max_connections=100, and the API could no
     longer get a connection to start. It crash-looped. Dagster jobs then failed
     on connect and pushed a phone notification per failed run.

One blocked DDL statement converted a healthy table into a full stop.

THE PROPERTY

`lock_timeout` makes that impossible: the statement aborts rather than queueing,
so it can never become the head of a queue. This test pins it on every migration
that issues blocking DDL against a table the API reads.

It is deliberately a source-level check. The failure is not observable in a unit
test -- it needs a busy table and a concurrent reader -- so the only thing that
can protect us is refusing to merge the unguarded statement.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
MIGRATIONS = REPO / "migrations"

# DDL that takes ACCESS EXCLUSIVE and therefore can head a lock queue.
BLOCKING_DDL = re.compile(
    r"^\s*ALTER\s+TABLE\s+(?!.*\bSET\s+STATISTICS\b)"
    r"|^\s*DROP\s+TABLE\s+"
    r"|^\s*TRUNCATE\s+"
    r"|^\s*REINDEX\s+",
    re.IGNORECASE | re.MULTILINE,
)

# Tables the API reads on the request path. DDL against these is what takes the
# site down; a scratch or archive table stalling hurts nobody.
HOT_TABLES = ("trades", "trade_returns", "insiders", "insider_ticker_scores",
              "strategy_portfolio", "score_history")


def _sql_files():
    if not MIGRATIONS.is_dir():
        pytest.skip("no migrations directory")
    return sorted(p for p in MIGRATIONS.glob("*.sql"))


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


@pytest.mark.parametrize("path", _sql_files(), ids=lambda p: p.name)
def test_blocking_ddl_on_a_hot_table_sets_lock_timeout(path: Path):
    raw = path.read_text(encoding="utf-8", errors="ignore")
    body = _strip_comments(raw)

    if not BLOCKING_DDL.search(body):
        return  # no blocking DDL; nothing to guard

    lowered = body.lower()
    if not any(re.search(rf"\b{t}\b", lowered) for t in HOT_TABLES):
        return  # DDL, but not against a table the API reads

    assert re.search(r"set\s+lock_timeout\s*=", lowered), (
        f"{path.name} issues blocking DDL against a table the API reads but "
        "never sets lock_timeout.\n\n"
        "Postgres grants lock requests in order, so an ALTER that cannot get "
        "its lock immediately becomes the head of a queue and blocks every "
        "later read -- including reads that would have been perfectly "
        "compatible with whatever is actually holding the table. That is how "
        "this exact file took form4.app down on 2026-08-27.\n\n"
        "Add before the DDL:\n\n    SET lock_timeout = '3s';\n"
    )


def test_the_migration_that_caused_the_outage_is_guarded():
    """Named explicitly so deleting the guard fails loudly, not silently."""
    p = MIGRATIONS / "2026-08-27_drop_dead_annotation_columns.sql"
    if not p.exists():
        pytest.skip("migration has been applied and removed")
    assert "lock_timeout" in p.read_text(encoding="utf-8"), (
        "the drop-columns migration lost its lock_timeout guard. This is the "
        "exact file that took the site down on 2026-08-27."
    )
