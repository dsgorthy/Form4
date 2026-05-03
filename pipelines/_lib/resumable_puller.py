"""Resumable bulk-data puller framework.

A thin base class encapsulating the patterns that already work in
`pull_1sec_bars.py` and `insider_study/options_pull.py`:

  * HTTP session with urllib3 retry + 429 honoring
  * Per-item completion tracking (PG `pull_status` table OR file-presence)
  * Rate limiting (requests/sec)
  * Periodic progress logging
  * Manifest updates after each batch
  * Safe interruption + resume

Subclass and implement two methods:

    class MyPuller(ResumablePuller):
        dataset = "my_dataset"
        items_total = 1000

        def fetch_item(self, session, item):
            # return whatever the writer needs, or None to skip
            ...

        def write_item(self, item, fetched):
            # persist; return (rows_written, bytes_written)
            ...

Then call `MyPuller(items, ...).run()`. See the bottom of this file for a
minimal usage sketch.
"""
from __future__ import annotations

import logging
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines._lib import manifest

logger = logging.getLogger(__name__)


# ── HTTP session factory ─────────────────────────────────────────────────────

def make_session(
    headers: Optional[dict] = None,
    *,
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    pool_maxsize: int = 4,
) -> requests.Session:
    """A requests.Session pre-wired with sensible retry behavior.

    Honors 429 Retry-After, retries 5xx, recreates on connection error
    (callers should catch ConnectionError and call make_session again,
    matching the pattern in `pull_1sec_bars.py`).
    """
    s = requests.Session()
    if headers:
        s.headers.update(headers)
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD", "POST"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=pool_maxsize)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ── Pull-status DB helpers (pull_status PG table) ────────────────────────────

def _pg_mark(dataset: str, item_key: str, status: str,
             rows: int = 0, bytes_written: int = 0,
             error: Optional[str] = None) -> None:
    """Write a row to public.pull_status. Best-effort — silently no-ops if PG
    or the table is unavailable so dev work without DB access still functions."""
    with suppress(Exception):
        from config.database import get_connection
        conn = get_connection()
        try:
            sql = """
                INSERT INTO pull_status
                  (dataset, item_key, status, attempt_count, rows_written,
                   bytes_written, error_message, started_at, completed_at)
                VALUES (?, ?, ?, 1, ?, ?, ?, NOW(), CASE WHEN ? IN ('done','failed') THEN NOW() ELSE NULL END)
                ON CONFLICT (dataset, item_key) DO UPDATE SET
                  status = EXCLUDED.status,
                  attempt_count = pull_status.attempt_count + 1,
                  rows_written = COALESCE(EXCLUDED.rows_written, pull_status.rows_written),
                  bytes_written = COALESCE(EXCLUDED.bytes_written, pull_status.bytes_written),
                  error_message = EXCLUDED.error_message,
                  completed_at = CASE WHEN EXCLUDED.status IN ('done','failed') THEN NOW()
                                      ELSE pull_status.completed_at END
            """
            conn.execute(sql, (dataset, item_key, status, rows, bytes_written, error, status))
            conn.commit()
        finally:
            conn.close()


def _pg_done_set(dataset: str) -> set[str]:
    """Return the set of item_keys already marked done. {} on any DB error."""
    with suppress(Exception):
        from config.database import get_connection
        conn = get_connection(readonly=True)
        try:
            rows = conn.execute(
                "SELECT item_key FROM pull_status WHERE dataset = ? AND status = 'done'",
                (dataset,),
            ).fetchall()
            return {r["item_key"] for r in rows}
        finally:
            conn.close()
    return set()


# ── Rate limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Simple sleep-based limiter for requests/sec or events/sec.

    Not an exact token bucket — fine for polite API pulls where ±10% is OK.
    Pass `0` or negative to disable.
    """
    def __init__(self, max_per_sec: float):
        self._min_interval = 1.0 / max_per_sec if max_per_sec and max_per_sec > 0 else 0
        self._last = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last = time.monotonic()


# ── Base puller ──────────────────────────────────────────────────────────────

@dataclass
class PullStats:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    rows_written: int = 0
    bytes_written: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def rate(self) -> float:
        elapsed = time.monotonic() - self.started_at
        return self.attempted / elapsed if elapsed > 0 else 0


class ResumablePuller:
    """Base class. Subclass and override:

      * `dataset`              — class attribute, name in pull_status / manifest
      * `storage_root`         — Path where output files land (manifest input)
      * `item_key(item)`       — stable identifier per item (default: str(item))
      * `fetch_item(session, item)` — return data or None (skip)
      * `write_item(item, fetched)` — persist; return (rows, bytes)
      * `is_done(item)`        — optional override; default uses pull_status PG

    Lifecycle: __init__(items, ...) → run().
    """

    dataset: str = "unset"
    storage_root: Optional[Path] = None
    description: Optional[str] = None

    def __init__(
        self,
        items: Iterable[dict],
        *,
        session_headers: Optional[dict] = None,
        rate_limit_per_sec: float = 0,
        progress_every_n: int = 25,
        manifest_every_n: int = 100,
        on_session_error: Optional[Callable[[Exception], None]] = None,
        completion_strategy: str = "pg",  # 'pg' | 'disk' | 'none'
        disk_marker: Optional[Callable[[dict], Path]] = None,
    ):
        self.items: list[dict] = list(items)
        self.items_total = len(self.items)
        self._headers = session_headers
        self._limiter = RateLimiter(rate_limit_per_sec)
        self._progress_every = max(1, progress_every_n)
        self._manifest_every = max(1, manifest_every_n)
        self._on_session_error = on_session_error
        self._completion = completion_strategy
        self._disk_marker = disk_marker
        self.stats = PullStats()
        if self.dataset == "unset":
            raise RuntimeError(f"{type(self).__name__} must set class attribute `dataset`")

    # ── overridable hooks ─────────────────────────────────────────────────

    def item_key(self, item: dict) -> str:
        return str(item)

    def fetch_item(self, session: requests.Session, item: dict) -> Any:
        raise NotImplementedError

    def write_item(self, item: dict, fetched: Any) -> tuple[int, int]:
        raise NotImplementedError

    def is_done(self, item: dict) -> bool:
        if self._completion == "none":
            return False
        if self._completion == "disk":
            if not self._disk_marker:
                return False
            p = self._disk_marker(item)
            return p.exists() and p.stat().st_size > 0
        # default 'pg' — checked in bulk via _done_keys cache below
        return self.item_key(item) in self._done_keys

    # ── helpers exposed to subclasses ─────────────────────────────────────

    def make_session(self) -> requests.Session:
        return make_session(self._headers)

    # ── main loop ─────────────────────────────────────────────────────────

    def run(self) -> PullStats:
        """Execute the pull. Returns final PullStats."""
        # Bulk-load completion state once at start (cheap & avoids per-item DB roundtrips)
        self._done_keys: set[str] = (
            _pg_done_set(self.dataset) if self._completion == "pg" else set()
        )
        n_to_skip = sum(1 for it in self.items if self.is_done(it))
        n_to_pull = self.items_total - n_to_skip
        logger.info(
            "puller %s: %d items total, %d already complete, %d to pull",
            self.dataset, self.items_total, n_to_skip, n_to_pull,
        )

        session = self.make_session()
        last_progress = 0
        last_manifest = 0

        for i, item in enumerate(self.items, start=1):
            if self.is_done(item):
                self.stats.skipped += 1
                continue

            key = self.item_key(item)
            self._limiter.wait()
            self.stats.attempted += 1

            try:
                fetched = self._fetch_with_session_recovery(session, item)
            except Exception as e:
                self.stats.failed += 1
                _pg_mark(self.dataset, key, "failed", error=str(e)[:500])
                logger.exception("fetch failed for %s: %s", key, e)
                continue

            if fetched is None:
                # treat None as a benign "no data available" — still mark done
                _pg_mark(self.dataset, key, "done", rows=0, bytes_written=0)
                self.stats.succeeded += 1
            else:
                try:
                    rows, bytes_ = self.write_item(item, fetched)
                except Exception as e:
                    self.stats.failed += 1
                    _pg_mark(self.dataset, key, "failed", error=f"write: {e}"[:500])
                    logger.exception("write failed for %s: %s", key, e)
                    continue

                self.stats.succeeded += 1
                self.stats.rows_written += rows
                self.stats.bytes_written += bytes_
                _pg_mark(self.dataset, key, "done", rows=rows, bytes_written=bytes_)

            if self.stats.attempted - last_progress >= self._progress_every:
                self._log_progress(i, key)
                last_progress = self.stats.attempted

            if self.stats.attempted - last_manifest >= self._manifest_every:
                self._update_manifest(last_item=key)
                last_manifest = self.stats.attempted

        # final manifest + log
        self._update_manifest(last_item=self.item_key(self.items[-1]) if self.items else None)
        self._log_progress(self.items_total, "DONE", final=True)
        with suppress(Exception):
            session.close()
        return self.stats

    def _fetch_with_session_recovery(self, session: requests.Session, item: dict) -> Any:
        """Wrap fetch_item with one connection-error recovery pass.

        Mirrors pull_1sec_bars.py's pattern: on ConnectionError, rebuild the
        session and try once more before giving up.
        """
        try:
            return self.fetch_item(session, item)
        except requests.exceptions.ConnectionError as e:
            logger.warning("connection error on %s, recreating session: %s", self.item_key(item), e)
            if self._on_session_error:
                self._on_session_error(e)
            with suppress(Exception):
                session.close()
            time.sleep(2)
            new_session = self.make_session()
            # mutate the caller's reference is not possible; subclasses that need
            # the live session should use self._session via override. Simpler path:
            # replace the session arg with the new one for this call only.
            return self.fetch_item(new_session, item)

    # ── logging / manifest ────────────────────────────────────────────────

    def _log_progress(self, position: int, last_key: str, *, final: bool = False) -> None:
        rate = self.stats.rate()
        eta_sec = ((self.items_total - position) / rate) if rate > 0 else 0
        prefix = "DONE" if final else "..."
        logger.info(
            "%s %s [%d/%d] attempted=%d ok=%d fail=%d skip=%d rows=%s bytes=%.1f MB rate=%.1f/s eta=%.0fm last=%s",
            prefix, self.dataset, position, self.items_total,
            self.stats.attempted, self.stats.succeeded, self.stats.failed, self.stats.skipped,
            f"{self.stats.rows_written:,}",
            self.stats.bytes_written / 1e6, rate, eta_sec / 60,
            last_key,
        )

    def _update_manifest(self, last_item: Optional[str]) -> None:
        if not self.storage_root:
            return
        files, bytes_ = manifest.scan_dir_size(self.storage_root)
        manifest.update(
            self.dataset,
            storage_root=str(self.storage_root),
            item_count=files,
            bytes_on_disk=bytes_,
            last_item_pulled=last_item,
            description=self.description,
        )


# ── Tiny usage demo (importable, runnable for smoke test) ───────────────────

if __name__ == "__main__":
    """Smoke test against httpbin — proves the framework end-to-end."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    OUT = Path("/tmp/resumable_puller_smoke")
    OUT.mkdir(parents=True, exist_ok=True)

    class HttpbinPuller(ResumablePuller):
        dataset = "smoke_test_httpbin"
        storage_root = OUT
        description = "Smoke test against httpbin.org/uuid"

        def item_key(self, item):
            return str(item["i"])

        def fetch_item(self, session, item):
            r = session.get("https://httpbin.org/uuid", timeout=10)
            r.raise_for_status()
            return r.json()

        def write_item(self, item, fetched):
            outfile = OUT / f"{item['i']:04d}.json"
            payload = str(fetched).encode()
            outfile.write_bytes(payload)
            return 1, len(payload)

    items = [{"i": i} for i in range(5)]
    stats = HttpbinPuller(
        items,
        rate_limit_per_sec=2,
        progress_every_n=2,
        manifest_every_n=3,
        completion_strategy="disk",
        disk_marker=lambda it: OUT / f"{it['i']:04d}.json",
    ).run()

    print("\nFinal stats:")
    print(f"  attempted   {stats.attempted}")
    print(f"  succeeded   {stats.succeeded}")
    print(f"  failed      {stats.failed}")
    print(f"  skipped     {stats.skipped}")
    print(f"  rows        {stats.rows_written}")
    print(f"  bytes       {stats.bytes_written}")
