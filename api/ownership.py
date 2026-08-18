"""Filing-level position maths: what an insider held before and after.

WHY THIS EXISTS

`trades.shares_owned_after` is reported per *ownership line*, not per person.
One Form 4 can report the same decision across several lines — a fund holding
through seven partnerships, an officer holding some shares directly and some
in a trust — and each line carries its own running balance.

Every consumer in this repo used to collapse those lines with a single
aggregate and each picked a different one:

    generate_stocktwits_posts   ARRAY_AGG(... ORDER BY date DESC)[1]  -> last line
    api/routers/filings         max(lot_soa)                          -> biggest line
    api/routers/companies       MAX(t.shares_owned_after)             -> biggest line
    generate_breaking_signal    MAX(t.shares_owned_after)             -> biggest line

All four are wrong the moment a filing spans more than one line, because they
divide a total quantity sold by a single line's balance. Two examples from
2026-08-17, both of which shipped as public posts:

    CHYM  DST Global sold 1,237,950 of 49,431,194 shares held across seven
          partnerships. Reported as "cut their stake by 73%". True: 3%.
    BFLY  Larry Robbins sold 2,297,733 of 17,005,450 across two lines.
          Reported as "cut their stake by 44%". True: 14%.

A 3% trim and a 73% exit are different stories. Getting this wrong is worse
than saying nothing, so `position_change` returns None rather than a guess
whenever the reported balances cannot be reconciled.

HOW LINES ARE IDENTIFIED

Two passes, because the SEC's own line identity is only sometimes usable:

  1. Group by (direct_indirect, nature_of_ownership). DST Global names each
     partnership in the nature field, so this separates all seven cleanly.
  2. Within each group, walk the lots in time order and start a new line
     wherever the reported balance does not reconcile with the quantity
     traded. Larry Robbins files every line as "See footnotes", so pass 1
     leaves them in one bucket; the balance jump from 11,743,530 to 4,371,387
     on a 175,300-share sale is what separates them.

Pass 2 alone would over-split interleaved lines (CHYM's seven partnerships
alternate in filing order). Pass 1 alone would under-split "See footnotes".
Together they handle both.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence

__all__ = ["PositionChange", "position_change", "split_ownership_lines"]

#: Reported balances are integers in almost every filing, but fractional
#: shares from DRIPs and stock plans do occur. A line is treated as continuous
#: when the balance lands within this many shares of the expected value —
#: large enough to absorb rounding, far too small to merge two real lines.
_RECONCILE_TOLERANCE = 1.0


@dataclass(frozen=True)
class PositionChange:
    """What one insider's stake in one security did over one filing.

    `fraction` is of the *prior* position: 0.135 means a sell cut the stake by
    13.5%. For a sell it is bounded at 1.0 by construction — anything above
    means the balances did not reconcile, and `position_change` returns None
    instead of emitting one.
    """

    before: float
    after: float
    qty: float
    fraction: float
    lines: int

    @property
    def is_full_exit(self) -> bool:
        return self.after <= 0


def _lot_sort_key(lot: dict) -> tuple:
    return (lot.get("trade_date") or "", lot.get("trade_id") or 0)


def _line_identity(lot: dict) -> tuple:
    """The SEC's own ownership-line label, where the filer supplied one."""
    return (
        (lot.get("direct_indirect") or "").strip().upper(),
        (lot.get("nature_of_ownership") or "").strip().lower(),
    )


def split_ownership_lines(
    lots: Sequence[dict], is_buy: bool
) -> Optional[list[list[dict]]]:
    """Partition a filing's lots into distinct ownership lines.

    Returns None when any lot is missing the balance needed to reconcile,
    because a partial answer here silently becomes a wrong percentage
    downstream.
    """
    usable = list(lots or [])
    if not usable:
        return None
    for lot in usable:
        if lot.get("shares_owned_after") is None or lot.get("qty") is None:
            return None

    # Pass 1 — the filer's own line labels.
    buckets: dict[tuple, list[dict]] = {}
    for lot in usable:
        buckets.setdefault(_line_identity(lot), []).append(lot)

    # Pass 2 — split each bucket wherever the running balance breaks.
    lines: list[list[dict]] = []
    for bucket in buckets.values():
        bucket.sort(key=_lot_sort_key)
        current: list[dict] = [bucket[0]]
        for prev, lot in zip(bucket, bucket[1:]):
            step = float(lot["qty"]) if is_buy else -float(lot["qty"])
            expected = float(prev["shares_owned_after"]) + step
            if abs(float(lot["shares_owned_after"]) - expected) <= _RECONCILE_TOLERANCE:
                current.append(lot)
            else:
                lines.append(current)
                current = [lot]
        lines.append(current)
    return lines


def position_change(
    lots: Sequence[dict], is_buy: bool
) -> Optional[PositionChange]:
    """Reconcile a filing's lots into one before/after position.

    `lots` are rows carrying at least qty, shares_owned_after, trade_date and
    trade_id; direct_indirect and nature_of_ownership sharpen the split when
    present. Returns None whenever the result would be untrustworthy — missing
    balances, a non-positive prior position, or a sell larger than the stake it
    came out of. Callers should drop the claim, not substitute a fallback.
    """
    lines = split_ownership_lines(lots, is_buy)
    if not lines:
        return None

    before = after = qty = 0.0
    for line in lines:
        first, last = line[0], line[-1]
        opening = float(first["shares_owned_after"])
        opening += -float(first["qty"]) if is_buy else float(first["qty"])
        before += opening
        after += float(last["shares_owned_after"])
        qty += sum(float(lot["qty"]) for lot in line)

    if before <= 0 or qty <= 0:
        return None
    fraction = qty / before
    # You cannot sell more than you held. If the arithmetic says otherwise the
    # balances are not describing the position we think they are.
    if not is_buy and fraction > 1.0 + 1e-9:
        return None

    return PositionChange(
        before=before, after=after, qty=qty,
        fraction=fraction, lines=len(lines),
    )


def position_change_from_row(row: dict, lots: Optional[Iterable[Any]] = None,
                             is_buy: Optional[bool] = None) -> Optional[PositionChange]:
    """Convenience wrapper for callers holding a filing row plus its lots.

    Falls back to treating the row itself as a single lot, which is the common
    case — most filings are one line and one lot, and that path must stay
    cheap.
    """
    if is_buy is None:
        sc = row.get("signal_class")
        is_buy = (sc == "discretionary_buy") if sc else (row.get("trade_type") == "buy")
    lot_list = [dict(lot) for lot in lots] if lots else []
    if not lot_list:
        lot_list = [row]
    return position_change(lot_list, bool(is_buy))
