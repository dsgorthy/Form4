"""Position storage for the thesis monitor.

Positions live in `positions.yaml` next to this module. Flat list with
`thesis` ("oil" | "data_center"), `type` ("equity" | "option"), and the
fields each kind needs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import yaml

POSITIONS_YAML = Path(__file__).parent / "positions.yaml"


@dataclass
class Position:
    thesis: str
    ticker: str
    type: str
    qty: float
    basis: float
    entered: str
    # Option-only
    underlying: Optional[str] = None
    side: Optional[str] = None
    strike: Optional[float] = None
    expiry: Optional[str] = None
    notes: str = ""

    def label(self) -> str:
        """Human label e.g. 'FRO' or 'FRO Jan 2027 $45 C'."""
        if self.type == "equity":
            return self.ticker
        ex = self.expiry or "?"
        return f"{self.underlying} {ex} ${self.strike:g}{(self.side or '?').upper()[0]}"

    def cost(self) -> float:
        """Total cost basis. Options use $100 multiplier."""
        mult = 100 if self.type == "option" else 1
        return self.qty * self.basis * mult


def load_positions(path: Path = POSITIONS_YAML) -> list[Position]:
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text()) or {}
    return [Position(**p) for p in raw.get("positions", [])]


def save_positions(positions: list[Position], path: Path = POSITIONS_YAML) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"positions": [asdict(p) for p in positions]}
    path.write_text(yaml.safe_dump(payload, sort_keys=False, default_flow_style=False))


def add_position(p: Position, path: Path = POSITIONS_YAML) -> None:
    positions = load_positions(path)
    positions.append(p)
    save_positions(positions, path)


def remove_position(index: int, path: Path = POSITIONS_YAML) -> Position:
    positions = load_positions(path)
    removed = positions.pop(index)
    save_positions(positions, path)
    return removed


def underlyings(positions: list[Position]) -> list[str]:
    """Distinct equity tickers we need prices for (incl. option underlyings)."""
    out: set[str] = set()
    for p in positions:
        out.add(p.underlying if p.type == "option" and p.underlying else p.ticker)
    return sorted(out)
