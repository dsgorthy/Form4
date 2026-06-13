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
from dataplane.parity import DEFAULT_KEY_FIELDS, compare as parity_compare
from dataplane.report import format_report, report as strategy_report


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


def _cmd_parity(args: argparse.Namespace) -> int:
    key_fields = (
        tuple(k.strip() for k in args.key.split(",") if k.strip())
        if args.key
        else DEFAULT_KEY_FIELDS
    )
    result = parity_compare(
        signal_a=args.signal_a,
        signal_b=args.signal_b,
        from_date=args.from_date,
        to_date=args.to_date,
        key_fields=key_fields,
    )
    print(
        f"parity {result.signal_a} vs {result.signal_b}  "
        f"[{result.from_date} → {result.to_date}]"
    )
    print(f"  key fields:   {', '.join(result.key_fields)}")
    print(f"  rows in A:    {result.count_a:>8,}")
    print(f"  rows in B:    {result.count_b:>8,}")
    print(f"  matched:      {result.matched:>8,}")
    print(f"  only in A:    {result.only_in_a:>8,}")
    print(f"  only in B:    {result.only_in_b:>8,}")
    if result.count_a and result.count_b:
        coverage_a = 100 * result.matched / result.count_a
        coverage_b = 100 * result.matched / result.count_b
        print(
            f"  coverage:     A→{coverage_a:5.1f}%   B→{coverage_b:5.1f}%"
        )
    if result.sample_only_in_a:
        print("  --- sample only in A ---")
        for row in result.sample_only_in_a:
            print(f"    {row}")
    if result.sample_only_in_b:
        print("  --- sample only in B ---")
        for row in result.sample_only_in_b:
            print(f"    {row}")
    # Exit nonzero if either side has unmatched rows — caller can gate cutover.
    return 0 if (result.only_in_a == 0 and result.only_in_b == 0) else 1


def _cmd_report(args: argparse.Namespace) -> int:
    rep = strategy_report(
        signal_ref=args.strategy,
        from_date=args.from_date,
        to_date=args.to_date,
        show_alerts=args.alerts,
    )
    print(format_report(rep))
    return 0


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

    pr = sub.add_parser(
        "report",
        help="Strategy evaluation report — what triggered, what didn't, why.",
    )
    pr.add_argument("strategy", help="strategy signal_id, e.g. strategy.agrade_drawdown_buy")
    pr.add_argument("--from", dest="from_date", required=True)
    pr.add_argument("--to", dest="to_date", default=_today_utc())
    pr.add_argument("--alerts", type=int, default=50, help="max triggered alerts to show")
    pr.set_defaults(func=_cmd_report)

    pp = sub.add_parser(
        "parity",
        help="Diff two signals' observations on a fingerprint join key.",
    )
    pp.add_argument("signal_a", help="signal_id of the new/candidate signal")
    pp.add_argument("signal_b", help="signal_id of the baseline signal")
    pp.add_argument("--from", dest="from_date", required=True)
    pp.add_argument("--to", dest="to_date", required=True)
    pp.add_argument(
        "--key",
        help=(
            "comma-separated JSON value fields to join on "
            f"(default: {','.join(DEFAULT_KEY_FIELDS)})"
        ),
    )
    pp.set_defaults(func=_cmd_parity)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
