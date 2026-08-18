"""The form4 derived chain, as a dependency graph instead of four crons.

Today these run as independent launchd jobs ordered only by wall-clock time:

    daily-prices      17:30  ->  form4.prices.daily_prices
    backfill-returns  05:00  ->  form4.trade_returns
    refresh-features  06:00  ->  insider_ticker_scores, grades, indicators
    compute-signals   17:45  ->  trade_signals

Nothing connects them. On 2026-07-28 prices went stale and all three
downstream jobs still ran, computed on stale input, and exited 0 — for 18
days. The clock said "it is 05:00, run", so it ran. That is the failure this
module removes.

Two properties do the work:

  1. Real edges. returns depends on prices, features on returns, signals on
     features. Dagster will not run a downstream asset in a materialization
     whose upstream failed.

  2. FAIL CLOSED on stale input. Each asset asserts its upstream data is
     within a freshness budget BEFORE doing anything, and raises otherwise.
     A dependency edge alone does not protect a manually triggered or
     partially-failed run — the gate does. Loud failure beats a green log
     over silently-wrong numbers.

The compute bodies shell out to the existing, proven production scripts
rather than reimplementing them. This module changes *orchestration*, not
*logic*, so cutover risk stays low: identical work, ordered properly, with
a gate in front.

Cutover: these are unscheduled until a parallel run proves parity. Disable
the matching launchd jobs before adding this to a schedule, or the work runs
twice.
"""
# NOTE: deliberately no `from __future__ import annotations`. PEP 563 turns
# annotations into strings, and Dagster's context-type validation compares the
# annotation against the real AssetExecutionContext class — with the future
# import every asset here fails to define. Runs on 3.12, so `str | None` is
# native anyway.

import os
import subprocess
from datetime import date, datetime, timedelta

import psycopg2
from dagster import AssetExecutionContext, MetadataValue, Output, asset

REPO = "/Users/derekg/trading-framework"
FORM4_DSN = os.environ.get("FORM4_DSN", "dbname=form4 host=localhost")

# Shared environment for the wrapped scripts. backfill_returns.py shells out
# to compute_returns.py WITHOUT setting cwd or PYTHONPATH itself — it inherits
# them from its launchd plist. Run it any other way and the subprocess dies on
# `import config` while the parent still reports success, silently freezing
# trade_returns. Set both explicitly here.
SCRIPT_ENV = {
    **os.environ,
    "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": REPO,
}


def _scalar(sql: str) -> str | None:
    with psycopg2.connect(FORM4_DSN) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
    return None if not row or row[0] is None else str(row[0])


def _assert_fresh(context: AssetExecutionContext, label: str, sql: str,
                  max_age_days: int) -> date:
    """Raise unless `sql` returns a date within `max_age_days`.

    This is the fail-closed gate. Without it a downstream asset happily
    recomputes on months-old inputs and reports success — which is precisely
    what happened for 18 days.
    """
    raw = _scalar(sql)
    if raw is None:
        raise RuntimeError(f"{label}: upstream returned no value — refusing to run")
    try:
        d = datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError(f"{label}: unparseable upstream date {raw!r}") from exc

    age = (date.today() - d).days
    if age > max_age_days:
        raise RuntimeError(
            f"{label} is {age}d stale (latest {d}, budget {max_age_days}d). "
            f"Refusing to compute on stale input — fix the upstream feed first."
        )
    context.log.info("%s freshness ok: %s (%dd old)", label, d, age)
    return d


def _run(context: AssetExecutionContext, args: list[str], timeout: int = 3600) -> str:
    context.log.info("running: %s", " ".join(args))
    proc = subprocess.run(args, cwd=REPO, env=SCRIPT_ENV, timeout=timeout,
                          capture_output=True, text=True)
    tail = (proc.stdout or "")[-2000:] + (proc.stderr or "")[-2000:]
    if proc.returncode != 0:
        raise RuntimeError(f"{args[0]} exited {proc.returncode}:\n{tail[-1500:]}")
    return tail


@asset(group_name="form4_pipeline", compute_kind="python",
       description="EOD prices from Alpaca into form4.prices.daily_prices.")
def form4_daily_prices(context: AssetExecutionContext) -> Output:
    # --max-tickers defaults to 2000 and truncates a SORTED list. The refresh
    # universe is recent filers UNION every ticker we already carry that is not
    # older than 400 days — about 5,300 live symbols — so the default would
    # silently refresh only the front of the alphabet and leave the rest to rot.
    # That is how 4,843 of 8,212 tickers ended up more than four months stale
    # by 2026-08, which blanked above_sma50/above_sma200 (they need history) and
    # made backtested positions exit at last-seen prices instead of real ones.
    _run(context, ["/usr/bin/python3",
                   f"{REPO}/pipelines/insider_study/update_daily_prices.py",
                   "--max-tickers", "12000"])
    latest = _scalar("SELECT max(date) FROM prices.daily_prices")
    n_fresh = _scalar(
        "SELECT count(*) FROM (SELECT ticker FROM prices.daily_prices "
        "GROUP BY ticker HAVING max(date) >= to_char(CURRENT_DATE - 7,'YYYY-MM-DD')) q"
    )
    return Output(latest, metadata={
        "latest_price_date": MetadataValue.text(str(latest)),
        # Surfaced because MAX(date) alone hid the outage: SPY was current every
        # single day while most of the table was months behind.
        "tickers_fresh_within_7d": MetadataValue.int(int(n_fresh or 0)),
    })


@asset(group_name="form4_pipeline", compute_kind="python",
       description="Sector/industry from yfinance into form4.ticker_metadata.")
def form4_ticker_metadata(context: AssetExecutionContext) -> Output:
    """Sector and industry, which the cohort work treats as a real input.

    Moved off com.openclaw.refresh-ticker-metadata (Sundays 09:00) so it sits
    in the same graph as everything else rather than beside it on a clock.

    No upstream freshness gate: this feed depends on nothing inside the plane —
    it reads a ticker list and asks yfinance. The gate that matters here is on
    the OUTPUT, below.
    """
    _run(context, ["/opt/homebrew/bin/python3",
                   f"{REPO}/scripts/refresh_ticker_metadata.py"], timeout=7200)

    total = int(_scalar("SELECT count(*) FROM ticker_metadata") or 0)
    with_sector = int(_scalar("SELECT count(sector) FROM ticker_metadata") or 0)
    pct = (100.0 * with_sector / total) if total else 0.0

    # 56.4% is near the ceiling, not a bug: 2,451 tickers do not resolve at all,
    # 862 are mutual funds and 125 are ETFs, none of which have a sector. Only
    # ~361 rows are genuinely recoverable (273 timeouts + 88 equities that
    # returned nothing). So the alarm threshold is set below the structural
    # floor — it fires on collapse, not on the normal shortfall.
    if pct < 45.0:
        raise RuntimeError(
            f"ticker_metadata sector coverage fell to {pct:.1f}% ({with_sector}/{total}). "
            f"Structural floor is ~56%; below 45% means the fetch broke, not that "
            f"more tickers became unclassifiable."
        )
    context.log.info("sector coverage %.1f%% (%d/%d)", pct, with_sector, total)
    return Output(with_sector, metadata={
        "rows": MetadataValue.int(total),
        "with_sector": MetadataValue.int(with_sector),
        "pct_sector": MetadataValue.float(round(pct, 1)),
    })


@asset(group_name="form4_pipeline", compute_kind="python",
       deps=[form4_daily_prices],
       description="Forward returns. Fails closed if prices are stale.")
def form4_trade_returns(context: AssetExecutionContext) -> Output:
    _assert_fresh(context, "daily_prices",
                  "SELECT max(date) FROM prices.daily_prices", 4)
    _run(context, ["/opt/homebrew/bin/python3",
                   f"{REPO}/pipelines/insider_study/backfill_returns.py",
                   "--max-download", "500"])
    latest = _scalar(
        "SELECT max(t.filing_date) FROM trade_returns r "
        "JOIN trades t ON t.trade_id = r.trade_id")
    return Output(latest, metadata={"latest_return_filing": MetadataValue.text(str(latest))})


@asset(group_name="form4_pipeline", compute_kind="python",
       deps=[form4_trade_returns],
       description="PIT scores, grades and indicators. Fails closed on stale prices.")
def form4_features(context: AssetExecutionContext) -> Output:
    _assert_fresh(context, "daily_prices",
                  "SELECT max(date) FROM prices.daily_prices", 4)
    _run(context, ["/bin/bash",
                   f"{REPO}/strategies/insider_catalog/refresh_features_daily.sh"],
         timeout=7200)
    latest = _scalar("SELECT max(as_of_date)::text FROM insider_ticker_scores")
    return Output(latest, metadata={"latest_score_as_of": MetadataValue.text(str(latest))})


@asset(group_name="form4_pipeline", compute_kind="python",
       deps=[form4_features],
       description="Tagged trade signals. Fails closed if PIT scores are stale.")
def form4_signals(context: AssetExecutionContext) -> Output:
    _assert_fresh(context, "insider_ticker_scores",
                  "SELECT max(as_of_date)::text FROM insider_ticker_scores", 4)
    out = _run(context, ["/usr/bin/python3",
                         f"{REPO}/pipelines/insider_study/compute_signals.py"])
    return Output("ok", metadata={"tail": MetadataValue.text(out[-800:])})


form4_pipeline_assets = [
    form4_daily_prices,
    form4_trade_returns,
    form4_features,
    form4_signals,
]

# Deliberately NOT in form4_pipeline_assets. That job runs every weekday at
# 17:30 and finishes in ~20 minutes; ticker metadata is a weekly, up-to-2-hour
# yfinance crawl over ~9,400 symbols with nothing downstream waiting on it.
# Folding it in would multiply the daily runtime for data that changes about as
# often as a company changes sector.
form4_weekly_assets = [
    form4_ticker_metadata,
]
