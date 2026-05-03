"""Contracts module — fail-closed guarantees for the trading-decision path.

The April 2026 silent outage taught us that strategies must REFUSE to operate
on stale or NULL inputs, not silently degrade. This module provides the typed
exceptions and freshness-registry primitives that turn 'fail-closed' into a
structural property of the system rather than a behavioral one.

Public surface:
    from framework.contracts.freshness import assert_fresh
    from framework.contracts.exceptions import (
        StaleSignalError, DataQualityHaltError, ConvictionInputMissing,
        ReconciliationDriftError,
    )
"""
from framework.contracts.exceptions import (
    ConvictionInputMissing,
    DataQualityHaltError,
    ReconciliationDriftError,
    StaleSignalError,
)
from framework.contracts.freshness import assert_fresh, get_freshness, FreshnessRegistry

__all__ = [
    "assert_fresh",
    "get_freshness",
    "FreshnessRegistry",
    "StaleSignalError",
    "DataQualityHaltError",
    "ConvictionInputMissing",
    "ReconciliationDriftError",
]
