"""Signal discovery — walk the signals/ package and return every concrete
Signal subclass.

Lives in the dataplane core (not under dagster_project) so the backfill
CLI can use it without pulling in Dagster.
"""
from __future__ import annotations

import importlib
import inspect
import pkgutil
from typing import List, Optional, Type

from dataplane.signal import Signal


# Default ticker universe for per_ticker_per_day signals. Conservative;
# strategies and backfills can override. Keep in lockstep with the dagster
# asset wrapper's expectations — both default to this list.
DEFAULT_TICKERS: List[str] = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
    "AMD", "AVGO", "ADBE", "NFLX", "INTC",
    "JPM", "BAC", "WFC", "GS",
    "JNJ", "PFE", "MRK", "LLY",
    "XOM", "CVX",
    "BRK.B", "V", "MA", "HD", "WMT", "PG",
]


def discover_signal_classes() -> List[Type[Signal]]:
    """Return every concrete Signal subclass found under the signals/ package."""
    import signals as signals_pkg

    found: List[Type[Signal]] = []
    for _, modname, _ in pkgutil.walk_packages(
        signals_pkg.__path__, prefix=f"{signals_pkg.__name__}."
    ):
        try:
            mod = importlib.import_module(modname)
        except Exception:
            continue
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls is Signal:
                continue
            if not issubclass(cls, Signal):
                continue
            if getattr(cls, "_dataplane_abstract", False):
                continue
            if cls.signal_id and cls.version and cls not in found:
                found.append(cls)
    return found


def find_signal(signal_ref: str) -> Optional[Type[Signal]]:
    """Resolve a signal by id (optionally with version suffix).

    Accepts:
      "insider.trades.raw"        → latest registered class with that id
      "insider.trades.raw.v1"     → same id, exact version match
      "insider.trades.raw.v1.0.0" → same id, exact version match
    """
    classes = discover_signal_classes()
    by_id = {c for c in classes if c.signal_id == signal_ref}
    if by_id:
        return sorted(by_id, key=lambda c: c.version, reverse=True)[0]
    for c in classes:
        if f"{c.signal_id}.{c.version}" == signal_ref:
            return c
        if signal_ref.startswith(f"{c.signal_id}.") and c.version.startswith(
            signal_ref.removeprefix(f"{c.signal_id}.")
        ):
            return c
    return None
