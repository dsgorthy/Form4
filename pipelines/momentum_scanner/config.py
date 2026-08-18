"""Configuration for the low-float momentum scanner.

Thresholds are intentionally explicit and tunable — this is a research /
observation tool, so every filter and scoring weight is visible and overridable
(CLI flags map onto this dataclass).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScannerConfig:
    # --- universe filters (define the candidate POOL we observe) ---
    max_price: float = 10.0          # gap-and-go small/micro-cap focus (sub-$10)
    min_price: float = 0.30          # avoid sub-penny noise; lower toward 0 to include deep sub-$1
    min_gap_pct: float = 10.0        # % move vs prior close (uses change% if the open is unavailable)
    min_rvol: float = 3.0            # cumulative today volume / avg daily volume
    max_float_m: float | None = 50.0  # shares-outstanding proxy (millions); None disables the filter

    # --- EMA periods on the 1-min chart ---
    # Kev/"Momentum" runs the 90 EMA as a trend/context line; 9 is the canonical fast trigger.
    # Both are parameterized: the literature gives no special status to any single period
    # (MA-period choice is the textbook data-snooping trap), so we keep them configurable.
    ema_fast: int = 9
    ema_slow: int = 90

    # --- round-number / psychological-level logic ---
    round_break_lookback: int = 15   # bars to look back for a *fresh* round-number break

    # --- scoring weights (technical confluence; sum = 100) ---
    w_vwap: float = 15.0             # holding above session VWAP
    w_ema_fast: float = 10.0         # above the 9 EMA
    w_ema_slow: float = 10.0         # above the 90 EMA
    w_pm_high: float = 15.0          # broke the premarket high
    w_pd_high: float = 15.0          # broke the prior-day high
    w_gap: float = 15.0              # gap magnitude (scaled, capped at 30%)
    w_rvol: float = 15.0             # relative volume (scaled, capped at 5x)
    w_round: float = 5.0             # fresh break of a round-number level
