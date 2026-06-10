"""Pyrrho data plane — modular alt-data ingestion + PIT-correct signal pipelines.

Every output in the system, regardless of source or computation method, lands
as a SignalObservation row keyed by (signal_id, ticker, as_of_date) in the
signal_observations table. Downstream consumers (Claude / strategies /
backtests) query this single table; they never touch raw ingestion tables.

Authoring a new signal:

    from dataplane import Signal, PIT, Upstream
    from datetime import timedelta

    class MySignal(Signal):
        signal_id = "options.unusual_volume"
        version   = "v0.1"
        owner     = "derek"
        sla_hours = 26
        upstream  = [
            Upstream("options.eod_volume.v1", pit_lag=timedelta(0)),
        ]
        output_schema = {"z_score": "float", "raw_volume": "int"}

        @PIT.strict
        def compute(self, ticker, as_of):
            rows = self.read("options.eod_volume.v1", ticker, as_of)
            ...
            return self.observation(ticker, as_of, value=...)

See README.md for full docs and the PIT discipline rules.
"""
from dataplane.observation import SignalObservation
from dataplane.upstream import Upstream
from dataplane.pit import PIT, PITViolationError
from dataplane.signal import Signal
from dataplane.testing import PITTestCase, PITValidator

__version__ = "0.1.0"
__all__ = [
    "Signal",
    "SignalObservation",
    "Upstream",
    "PIT",
    "PITViolationError",
    "PITTestCase",
    "PITValidator",
]
