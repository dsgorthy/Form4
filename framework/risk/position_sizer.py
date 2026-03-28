"""
Position sizing for the trading framework.
Constructor-param version: no settings import.
"""

import logging
import math

logger = logging.getLogger(__name__)

_DEFAULT_MIN_PCT = 2.0
_DEFAULT_MAX_PCT = 5.0


class PositionSizer:
    def __init__(
        self,
        starting_capital: float = 30_000.0,
        min_pct: float = _DEFAULT_MIN_PCT,
        max_pct: float = _DEFAULT_MAX_PCT,
    ) -> None:
        self.starting_capital = starting_capital
        self.min_pct = min_pct
        self.max_pct = max_pct
        logger.info(
            "PositionSizer initialized | starting_capital=$%.2f | size_range=[%.1f%%, %.1f%%]",
            starting_capital, min_pct, max_pct,
        )

    @staticmethod
    def kelly_criterion(win_rate: float, avg_win: float, avg_loss: float, fraction: float = 0.25) -> float:
        if avg_loss <= 0:
            return 0.0
        win_rate = max(0.0, min(1.0, win_rate))
        b = avg_win / avg_loss
        p = win_rate
        q = 1.0 - p
        kelly_raw = (b * p - q) / b
        kelly_fraction = fraction * kelly_raw
        kelly_pct = kelly_fraction * 100.0
        return max(0.0, kelly_pct)

    def calculate_size(
        self,
        capital: float,
        entry_price: float,
        confidence: float,
        vix_level: float,
        consecutive_losses: int = 0,
    ) -> dict:
        if capital <= 0:
            return {"num_contracts": 0, "total_cost": 0.0, "pct_of_capital": 0.0, "sizing_method": "blocked — no capital"}
        if entry_price <= 0:
            return {"num_contracts": 0, "total_cost": 0.0, "pct_of_capital": 0.0, "sizing_method": "blocked — invalid entry price"}

        adjustments = []
        confidence = max(0.0, min(1.0, confidence))

        if confidence <= 0.5:
            alloc_pct = self.min_pct
            adjustments.append(f"low confidence ({confidence:.2f}) -> {self.min_pct}%")
        elif confidence >= 0.9:
            alloc_pct = self.max_pct
            adjustments.append(f"high confidence ({confidence:.2f}) -> {self.max_pct}%")
        else:
            slope = (self.max_pct - self.min_pct) / (0.9 - 0.5)
            alloc_pct = self.min_pct + slope * (confidence - 0.5)
            adjustments.append(f"confidence ({confidence:.2f}) -> {alloc_pct:.2f}%")

        if vix_level > 30:
            alloc_pct *= 0.75
            adjustments.append(f"high VIX ({vix_level:.1f}) -> 75% reduction")

        if consecutive_losses >= 2:
            alloc_pct *= 0.50
            adjustments.append(f"{consecutive_losses} consecutive losses -> 50% reduction")

        alloc_pct = max(0.0, min(alloc_pct, self.max_pct))
        allocation_dollars = capital * (alloc_pct / 100.0)
        cost_per_contract = entry_price * 100
        num_contracts = max(1, math.floor(allocation_dollars / cost_per_contract))

        total_cost = num_contracts * cost_per_contract
        if total_cost > allocation_dollars and num_contracts > 1:
            num_contracts -= 1
            total_cost = num_contracts * cost_per_contract

        actual_pct = (total_cost / capital) * 100.0
        sizing_method = "base 3%"
        if adjustments:
            sizing_method += " | " + " | ".join(adjustments)

        return {
            "num_contracts": num_contracts,
            "total_cost": total_cost,
            "pct_of_capital": round(actual_pct, 4),
            "sizing_method": sizing_method,
        }
