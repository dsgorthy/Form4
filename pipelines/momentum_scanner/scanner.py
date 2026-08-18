"""Low-float momentum scanner — orchestrator + CLI (OBSERVATION-ONLY).

Pulls the day's candidate pool from the Alpaca screener, enriches each name with
a snapshot + intraday 1-min bars + a shares-outstanding (float) proxy, scores them
via the pure logic in signals.py, and writes passing candidates to a per-day JSONL
(the seed of the continuation-vs-fade dataset). Places NO orders.

Run (needs ALPACA_DATA_API_KEY/SECRET; FINNHUB_API_KEY optional for float):
    python3 -m pipelines.momentum_scanner.scanner --feed iex
    python3 -m pipelines.momentum_scanner.scanner --feed sip --max-price 20 --min-gap 5

Live data + the form4 DB live on Studio — run the continuous loop there (see DESIGN.md).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pytz

from framework.data import alpaca_screener
from framework.data.alpaca_client import AlpacaClient
from framework.data.fundamentals import shares_outstanding_m

from .config import ScannerConfig
from .signals import CandidateEval, evaluate_candidate

_EASTERN = pytz.timezone("US/Eastern")
_REPO = Path(__file__).resolve().parents[2]
_OUT_DIR = _REPO / "data" / "scanner"
_ENV = _REPO / ".env"


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV)
        return
    except Exception:
        pass
    if _ENV.exists():
        for line in _ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _snapshot_fields(snap: dict) -> dict:
    lt = snap.get("latestTrade") or {}
    mb = snap.get("minuteBar") or {}
    db = snap.get("dailyBar") or {}
    pdb = snap.get("prevDailyBar") or {}
    last = lt.get("p") or mb.get("c") or db.get("c")
    return {
        "last_price": float(last) if last else None,
        "day_open": float(db["o"]) if db.get("o") else None,
        "prev_close": float(pdb["c"]) if pdb.get("c") else None,
        "prior_day_high": float(pdb["h"]) if pdb.get("h") else None,
    }


def evaluate_symbol(
    client: AlpacaClient,
    symbol: str,
    *,
    now_et: datetime,
    cfg: ScannerConfig,
    finnhub_key: Optional[str] = None,
) -> Optional[CandidateEval]:
    """Enrich one symbol and score it. Returns None if it can't be priced."""
    try:
        snap = client.get_snapshot(symbol)
    except Exception as exc:
        print(f"[scan] {symbol} snapshot failed: {repr(exc)[:120]}", file=sys.stderr)
        return None
    f = _snapshot_fields(snap)
    if not f["last_price"]:
        return None

    day = now_et.strftime("%Y-%m-%d")
    start = _EASTERN.localize(datetime.strptime(f"{day} 04:00", "%Y-%m-%d %H:%M"))
    intraday = None
    try:
        intraday = client.get_bars_df(symbol, start.isoformat(), now_et.isoformat(), "1Min")
    except Exception as exc:
        print(f"[scan] {symbol} bars failed: {repr(exc)[:120]}", file=sys.stderr)

    avg_daily_volume = None
    try:
        daily = client.get_daily_bars(
            symbol, (now_et - timedelta(days=45)).date().isoformat(), now_et.date().isoformat()
        )
        if len(daily) > 1:  # exclude today's partial bar, average the prior ~20 sessions
            avg_daily_volume = float(daily["volume"].iloc[:-1].tail(20).mean())
    except Exception:
        pass

    so_m = shares_outstanding_m(symbol, finnhub_key)
    return evaluate_candidate(
        symbol,
        intraday=intraday,
        last_price=f["last_price"],
        prev_close=f["prev_close"],
        day_open=f["day_open"],
        prior_day_high=f["prior_day_high"],
        avg_daily_volume=avg_daily_volume,
        shares_outstanding_m=so_m,
        cfg=cfg,
    )


def run_scan(
    cfg: ScannerConfig,
    *,
    data_key: str,
    data_secret: str,
    feed: str = "iex",
    finnhub_key: Optional[str] = None,
    now_et: Optional[datetime] = None,
    top: int = 50,
    include_most_actives: bool = True,
    max_symbols: int = 80,
) -> List[CandidateEval]:
    now_et = now_et or datetime.now(_EASTERN)
    client = AlpacaClient(data_key, data_secret, feed=feed)
    try:
        symbols = alpaca_screener.candidate_symbols(
            data_key, data_secret, top=top, min_gainer_pct=cfg.min_gap_pct,
            include_most_actives=include_most_actives,
        )
    except Exception as exc:
        print(f"[scan] screener failed: {repr(exc)[:160]}", file=sys.stderr)
        symbols = []

    evals: List[CandidateEval] = []
    for sym in symbols[:max_symbols]:
        ev = evaluate_symbol(client, sym, now_et=now_et, cfg=cfg, finnhub_key=finnhub_key)
        if ev is not None:
            evals.append(ev)
    evals.sort(key=lambda e: e.score, reverse=True)
    return evals


def _print_table(evals: List[CandidateEval], limit: int = 25) -> None:
    header = (f"{'SYM':<8}{'PRICE':>8}{'GAP%':>8}{'RVOL':>7}{'FLOAT_M':>9}  "
              f"{'VWAP':>5}{'E9':>4}{'E90':>5}{'PMH':>5}{'PDH':>5}{'RND':>5}{'SCORE':>8}")
    print("\n" + header)
    print("-" * len(header))
    for e in [x for x in evals if x.passes_filters][:limit]:
        g = e.gap_pct if e.gap_pct is not None else e.change_pct
        print(
            f"{e.symbol:<8}{(e.last_price or 0):>8.2f}{(g or 0):>8.1f}{(e.rvol or 0):>7.1f}"
            f"{(e.shares_outstanding_m or 0):>9.1f}  "
            f"{'Y' if e.above_vwap else '.':>5}{'Y' if e.above_ema_fast else '.':>4}"
            f"{'Y' if e.above_ema_slow else '.':>5}{'Y' if e.broke_pm_high else '.':>5}"
            f"{'Y' if e.broke_pd_high else '.':>5}{'Y' if e.broke_round else '.':>5}{e.score:>8.1f}"
        )
    passing = sum(1 for e in evals if e.passes_filters)
    print(f"\n{passing} passing / {len(evals)} scanned")


def _write_jsonl(evals: List[CandidateEval], now_et: datetime) -> Path:
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = _OUT_DIR / f"{now_et.strftime('%Y-%m-%d')}.jsonl"
    scan_ts = now_et.isoformat()
    with path.open("a") as fh:
        for e in evals:
            if not e.passes_filters:
                continue
            row = e.to_row()
            row["scan_ts"] = scan_ts
            fh.write(json.dumps(row) + "\n")
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="Low-float momentum scanner (observation-only)")
    ap.add_argument("--feed", default="iex", choices=["iex", "sip", "otc"])
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--max-price", type=float, default=ScannerConfig.max_price)
    ap.add_argument("--min-gap", type=float, default=ScannerConfig.min_gap_pct)
    ap.add_argument("--min-rvol", type=float, default=ScannerConfig.min_rvol)
    ap.add_argument("--max-float", type=float, default=ScannerConfig.max_float_m)
    ap.add_argument("--no-most-actives", action="store_true")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--limit", type=int, default=25)
    args = ap.parse_args()

    _load_env()
    dk, ds = os.getenv("ALPACA_DATA_API_KEY"), os.getenv("ALPACA_DATA_API_SECRET")
    fk = os.getenv("FINNHUB_API_KEY")
    if not dk or not ds:
        print("ERROR: ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET not set in env/.env", file=sys.stderr)
        sys.exit(2)

    cfg = ScannerConfig(
        max_price=args.max_price, min_gap_pct=args.min_gap,
        min_rvol=args.min_rvol, max_float_m=args.max_float,
    )
    evals = run_scan(
        cfg, data_key=dk, data_secret=ds, feed=args.feed, finnhub_key=fk,
        top=args.top, include_most_actives=not args.no_most_actives,
    )
    _print_table(evals, limit=args.limit)
    if not args.no_write:
        path = _write_jsonl(evals, datetime.now(_EASTERN))
        print(f"wrote {sum(1 for e in evals if e.passes_filters)} passing -> {path}")


if __name__ == "__main__":
    main()
