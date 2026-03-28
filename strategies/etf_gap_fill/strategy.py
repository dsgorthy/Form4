"""
ETF Gap Fill Strategy — v1.0.0

Multi-symbol satellite extension of the SPY gap fill edge.
Symbol is driven entirely by data.primary_symbol in each config YAML.
All logic lives in SPYGapFillStrategy — this module is a thin re-export
so the strategy loader can discover it under the etf_gap_fill namespace.
"""
from strategies.spy_gap_fill.strategy import SPYGapFillStrategy


class ETFGapFillStrategy(SPYGapFillStrategy):
    """Multi-symbol gap fill. Config drives the symbol; logic is identical."""

    def strategy_name(self) -> str:
        return "etf_gap_fill"
