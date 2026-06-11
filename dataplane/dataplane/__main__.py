"""Dataplane CLI entry point: ``python3 -m dataplane <subcommand> ...``.

Subcommands:
  backfill   Walk a signal's partitions and write observations.
  list       List discovered signals.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from typing import List, Optional, Sequence

from dataplane.backfill import PartitionResult, backfill
from dataplane.discovery import DEFAULT_TICKERS, discover_signal_classes


def _parse_tickers(spec: Optional[str]) -> Optional[List[str]]:
    if not spec:
        return None
    return [t.strip().upper() for t in spec.split(",") if t.strip()]


def _today_utc() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).date().isoformat()


def _cmd_backfill(args: argparse.Namespace) -> int:
    tickers = _parse_tickers(args.tickers)

    print(
        f"backfilling {args.signal} from {args.from_date} to {args.to_date}"
        + (f" ({len(tickers)} tickers)" if tickers else "")
    )

    def progress(pr: PartitionResult) -> None:
        line = f"  {pr.partition_date}: {pr.written:>6} written"
        if pr.errors:
            line += f"  ⚠ {pr.errors} errors"
            if pr.error_samples:
                line += f"  (e.g. {pr.error_samples[0]})"
        print(line)

    try:
        result = backfill(
            args.signal,
            args.from_date,
            args.to_date,
            tickers=tickers,
            progress=None if args.quiet else progress,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2

    n_partitions = len(result.partitions)
    if args.dry_run:
        print(
            f"DRY RUN — would walk {n_partitions} partitions of "
            f"{result.signal_id}.{result.version} (mode={result.mode})"
        )
        return 0

    print(
        f"DONE. {result.signal_id}.{result.version}  mode={result.mode}\n"
        f"      {n_partitions} partitions, {result.total_written:,} rows written, "
        f"{result.total_errors} errors total"
    )
    return 0 if result.total_errors == 0 else 1


def _cmd_list(args: argparse.Namespace) -> int:
    classes = discover_signal_classes()
    if not classes:
        print("no signals discovered under signals/")
        return 1
    width = max(len(c.signal_id) for c in classes)
    for c in sorted(classes, key=lambda x: (x.signal_id, x.version)):
        print(
            f"  {c.signal_id:<{width}}  {c.version:<10}  "
            f"mode={c.materialization_mode:<22}  owner={c.owner}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dataplane",
        description="Dataplane CLI — backfill and inspect signals.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    pb = sub.add_parser(
        "backfill",
        help="Walk a signal's daily partitions and upsert observations.",
    )
    pb.add_argument(
        "signal",
        help="signal_id, optionally with version (e.g. insider.trades.raw or insider.trades.raw.v1)",
    )
    pb.add_argument(
        "--from",
        dest="from_date",
        required=True,
        help="start date inclusive, YYYY-MM-DD",
    )
    pb.add_argument(
        "--to",
        dest="to_date",
        default=_today_utc(),
        help="end date inclusive, YYYY-MM-DD (default: today UTC)",
    )
    pb.add_argument(
        "--tickers",
        help="comma-separated ticker override for per_ticker_per_day signals "
        f"(default: {len(DEFAULT_TICKERS)} tickers; ignored for per_partition_events)",
    )
    pb.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve the signal + date range, but don't compute or write",
    )
    pb.add_argument(
        "--quiet",
        action="store_true",
        help="suppress per-partition progress output",
    )
    pb.set_defaults(func=_cmd_backfill)

    pl = sub.add_parser("list", help="List discovered signals.")
    pl.set_defaults(func=_cmd_list)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
