"""Position management CLI.

Examples:
  # Equity fill
  thesis-add-fill FRO 165 36.85 --thesis oil

  # Option fill (explicit fields)
  thesis-add-fill FRO 6 2.50 --thesis oil --option \\
      --strike 45 --expiry 2027-01-15 --side call

  # List positions with cost basis
  thesis-list

  # Remove by index (use `list` to find the index)
  thesis-remove 2
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

from pipelines.thesis_monitor.positions import (
    Position,
    add_position,
    load_positions,
    remove_position,
)


def _cmd_add(args) -> int:
    if args.option:
        if not (args.strike and args.expiry and args.side):
            print("ERR: --option requires --strike, --expiry, --side", file=sys.stderr)
            return 2
        side = args.side.lower()
        if side not in {"call", "put"}:
            print(f"ERR: --side must be 'call' or 'put', got '{side}'", file=sys.stderr)
            return 2
        # ticker arg interpreted as underlying for options
        ticker_key = f"{args.ticker}-{args.expiry.replace('-', '')[2:]}{side[0].upper()}{args.strike:g}"
        p = Position(
            thesis=args.thesis,
            ticker=ticker_key,
            type="option",
            qty=args.qty,
            basis=args.price,
            entered=args.entered,
            underlying=args.ticker,
            side=side,
            strike=args.strike,
            expiry=args.expiry,
            notes=args.notes or "",
        )
    else:
        p = Position(
            thesis=args.thesis,
            ticker=args.ticker,
            type="equity",
            qty=args.qty,
            basis=args.price,
            entered=args.entered,
            notes=args.notes or "",
        )
    add_position(p)
    print(f"Added {p.label()} ({p.thesis}, {p.qty:g} @ ${p.basis:.2f}, cost ${p.cost():,.2f})")
    return 0


def _cmd_list(_args) -> int:
    positions = load_positions()
    if not positions:
        print("(no positions)")
        return 0
    print(f"{'#':>3}  {'thesis':<12} {'label':<32} {'qty':>6} {'basis':>8} {'cost':>10} {'entered':<11} notes")
    print("-" * 100)
    for i, p in enumerate(positions):
        print(
            f"{i:>3}  {p.thesis:<12} {p.label():<32} {p.qty:>6g} {p.basis:>8.2f} {p.cost():>10,.0f} {p.entered:<11} {p.notes}"
        )
    total = sum(p.cost() for p in positions)
    print("-" * 100)
    print(f"{'TOTAL':>{50}}{'':>10} ${total:>9,.0f}")
    return 0


def _cmd_remove(args) -> int:
    try:
        removed = remove_position(args.index)
        print(f"Removed [{args.index}] {removed.label()}")
        return 0
    except IndexError:
        print(f"ERR: no position at index {args.index}", file=sys.stderr)
        return 2


def main():
    p = argparse.ArgumentParser(description="Thesis monitor position CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", help="Add a fill")
    a.add_argument("ticker", help="Equity ticker (or underlying for options)")
    a.add_argument("qty", type=float, help="Shares or contracts")
    a.add_argument("price", type=float, help="Fill price (per share or per contract)")
    a.add_argument("--thesis", required=True, choices=["oil", "data_center"])
    a.add_argument("--option", action="store_true", help="Position is an option")
    a.add_argument("--strike", type=float, help="Option strike")
    a.add_argument("--expiry", help="Option expiry, YYYY-MM-DD")
    a.add_argument("--side", help="call or put")
    a.add_argument("--entered", default=date.today().isoformat(), help="Fill date YYYY-MM-DD")
    a.add_argument("--notes", help="Free-form notes")
    a.set_defaults(func=_cmd_add)

    a = sub.add_parser("list", help="List positions")
    a.set_defaults(func=_cmd_list)

    a = sub.add_parser("remove", help="Remove position by index")
    a.add_argument("index", type=int, help="Index from `list`")
    a.set_defaults(func=_cmd_remove)

    args = p.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
