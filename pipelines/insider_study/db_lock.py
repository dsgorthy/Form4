"""
Exclusive write lock for insiders.db.

All scripts that WRITE to insiders.db must use this lock to prevent
concurrent writes that corrupt the WAL. Readers (API containers) don't
need this — they use read-only mounts.

Usage:
    from pipelines.insider_study.db_lock import db_write_lock

    with db_write_lock():
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE ...")
        conn.commit()
        conn.close()

    # Or for long-running scripts that do many small writes:
    with db_write_lock():
        for batch in work:
            do_write(batch)

The lock is an flock on a file next to the DB. It's advisory but all
our writers honor it. If a writer crashes, the OS releases the lock.
"""

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path

LOCK_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / ".insiders.db.lock"


@contextmanager
def db_write_lock(timeout_msg: str = ""):
    """Acquire exclusive write lock on insiders.db. Blocks until available."""
    LOCK_PATH.touch(exist_ok=True)
    fd = os.open(str(LOCK_PATH), os.O_RDWR)
    try:
        if timeout_msg:
            print(f"Waiting for write lock... ({timeout_msg})", flush=True)
        fcntl.flock(fd, fcntl.LOCK_EX)  # blocks until lock is free
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
