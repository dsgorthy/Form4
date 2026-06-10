"""PIT (point-in-time) enforcement.

Three layers of discipline, all enforced by this module + the Signal base:

  1. Compile-time. The @PIT.strict decorator marks compute() as PIT-bound.
     Inside compute(), Signal.read() is the only sanctioned data accessor;
     direct DB imports (psycopg2, asyncpg, etc.) at the top of a signal
     module are caught by tests in tests/test_pit_imports.py.

  2. Test-time. PITValidator runs each declared test case twice:
       - normal mode: full DB visible
       - frozen mode: rows where ingested_at > current_as_of are hidden
     If results differ, the signal has a PIT leak. Raises PITViolationError.

  3. Runtime. Backfills walk chronologically. read() raises if you try to
     read past (current_as_of - upstream.pit_lag).

The decorator does NOT itself rewrite reads — Signal.read() does the
enforcement. The decorator's role is (a) marking the method so the test
harness knows to run it under both modes, and (b) plumbing current_as_of
through to Signal.read().
"""
from __future__ import annotations

from datetime import datetime
from functools import wraps
from typing import Callable


class PITViolationError(Exception):
    """Raised when a signal reads data outside its declared PIT envelope.

    Three triggers:
      - read() called with a signal_id not in the signal's upstream list
      - read() returned rows past (current_as_of - upstream.pit_lag)
      - normal-mode vs frozen-mode computation diverged (test-time)
    """


class PIT:
    """Namespace for PIT decorators + helpers.

    Use:
        class MySignal(Signal):
            @PIT.strict
            def compute(self, ticker, as_of):
                ...
    """

    @staticmethod
    def strict(fn: Callable) -> Callable:
        """Mark compute() as PIT-enforced. Plumbs current_as_of through.

        After @PIT.strict, ``self._pit_as_of`` is set during the call so
        ``self.read()`` can enforce the PIT envelope. The marker
        ``fn._pit_strict = True`` lets PITValidator and static analysis
        find decorated methods.
        """
        fn._pit_strict = True

        @wraps(fn)
        def wrapper(self, ticker: str, as_of: datetime):
            if as_of.tzinfo is None:
                raise PITViolationError(
                    f"as_of must be timezone-aware: {as_of!r}"
                )
            previous = getattr(self, "_pit_as_of", None)
            self._pit_as_of = as_of
            try:
                return fn(self, ticker, as_of)
            finally:
                self._pit_as_of = previous

        return wrapper
