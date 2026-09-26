"""The published blended-equity definition. ONE implementation, two readers.

`summary.blended_cagr` on /portfolio and every number a parameter sweep scores
come from this function. They used to come from two implementations, and the
two disagreed: a scratch harness annualised over a 252-day trading year while
the API used 365.25, and the published annual returns were re-litigated for a
day because of it (methodology doc, 2026-08-24, "Quote the API, not a
harness"). A sweep that scores a different quantity than the site publishes
cannot choose a config for the site.

`table` exists so a sweep can score a SANDBOX book with the published maths.
It is an identifier interpolated into SQL, so it goes through the same guard
the simulator uses.

Extracted verbatim from api/routers/portfolio.py on 2026-09-25. Behaviour is
unchanged, including the two things a reader should know:

  - rows are filtered on `execution_source = 'simulated'` ONLY. Unlike the
    summary query it does not exclude `entry_before_publication`, and it
    includes open positions (exit_date IS NULL marks to market). Left as it
    was: changing what the site publishes is a decision, not a refactor.
  - `years` is supplied by the caller. The API passes first trade -> TODAY
    deliberately (a book that stops trading must carry the cost of stopping);
    a sweep scoring a historical fold must pass that fold's own length.
"""
from __future__ import annotations

import re


def _valid_table(name: str) -> str:
    """Identifier guard — this value is interpolated into SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name or ""):
        raise ValueError(f"unsafe table name: {name!r}")
    return name.lower()


def blended_and_benchmark(conn, strategy: str, starting: float, years: float,
                          table: str = "strategy_portfolio",
                          end: str | None = None,
                          start: str | None = None):
    """Blended CAGR (idle cash in SPY), SPY's CAGR, DAILY max drawdown, annuals.

    Returns None when the price data is not there — a missing benchmark must
    not fabricate a number.

    `end` truncates the day series (default: the last session in the price
    table, i.e. today — what the live book wants). A WALK-FORWARD FOLD MUST
    PASS ITS OWN END. Without it the sandbox book stops trading at the fold
    boundary while idle cash goes on compounding in SPY to the present, and
    the resulting equity gets annualised over the fold's length — a 2016-2018
    fold would be credited with eight years of index return.

    `start` bounds the series from below (default: the book's first entry,
    which is what the live page wants). PASS IT WHENEVER THE TABLE HOLDS MORE
    HISTORY THAN THE WINDOW BEING MEASURED. Without it, asking a
    2016-2026 table for its 2022-onward performance silently returns the
    2016-onward answer — measured on the live A-List book, "the holdout" came
    back as −1.17% with a 77.4% drawdown, which are the full-period figures.
    A sweep is safe either way because the simulator only wrote the fold, but
    safe-by-accident is not the property you want in the function that decides
    whether a book ships.
    """
    from collections import defaultdict

    spy = {r["date"]: float(r["close"]) for r in conn.execute(
        "SELECT date::text AS date, close FROM prices.daily_prices "
        "WHERE ticker = 'SPY' ORDER BY date")}
    rows = [dict(r) for r in conn.execute(
        "SELECT ticker, entry_date, exit_date, entry_price, dollar_amount "
        f"FROM {_valid_table(table)} WHERE strategy = ? AND execution_source = 'simulated'",
        (strategy,))]
    if not spy or not rows:
        return None

    first = min(r["entry_date"] for r in rows if r["entry_date"])
    if start and start > first:
        first = start
    days = sorted(d for d in spy if d >= first and (end is None or d <= end))
    if len(days) < 30:
        return None

    tickers = {r["ticker"] for r in rows}
    px = defaultdict(dict)
    for r in conn.execute(
        "SELECT ticker, date::text AS d, close FROM prices.daily_prices "
        "WHERE ticker = ANY(?) AND date >= ?", (list(tickers), first)):
        px[r["ticker"]][r["d"]] = float(r["close"])

    opens = defaultdict(list)
    for r in rows:
        opens[r["entry_date"]].append(r)

    equity = idle = starting
    held, prev = [], None
    peak, daily_dd = starting, 0.0
    # Year -> [first equity seen, last equity seen] and the same for SPY, so
    # annual returns come out of the same single pass.
    yr: dict[str, list] = {}
    for d in days:
        if prev and spy.get(prev):
            idle *= 1 + (spy[d] - spy[prev]) / spy[prev]
        prev = d
        keep = []
        for h in held:
            if h["exit"] and h["exit"] <= d:
                idle += h["sh"] * (px[h["tk"]].get(d) or h["px"])
            else:
                keep.append(h)
        held = keep
        for t in opens.get(d, []):
            p, cap = t["entry_price"], (t["dollar_amount"] or 0)
            if not p or p <= 0 or cap <= 0:
                continue
            idle -= cap
            held.append({"tk": t["ticker"], "sh": cap / p,
                         "exit": t["exit_date"], "px": p})
        equity = idle + sum(h["sh"] * (px[h["tk"]].get(d) or h["px"]) for h in held)
        if equity > peak:
            peak = equity
        if peak > 0:
            daily_dd = max(daily_dd, (peak - equity) / peak)
        y = d[:4]
        if y not in yr:
            yr[y] = [equity, equity, spy[d], spy[d]]
        yr[y][1] = equity
        yr[y][3] = spy[d]

    if equity <= 0 or years <= 0:
        return None
    blended = ((equity / starting) ** (1 / years) - 1) * 100
    bench = ((spy[days[-1]] / spy[days[0]]) ** (1 / years) - 1) * 100
    # SPY per year comes in TWO flavours and they are not interchangeable.
    #
    #   `spy`          — over this book's own window, so the first (partial)
    #                    year is a like-for-like comparison.
    #   `spy_calendar` — the full calendar year, independent of any book.
    #
    # A shared SPY column across three books that started on three different
    # dates has to use the calendar figure, otherwise it silently shows one
    # book's partial-year index return next to another book's full year. That
    # shipped for about an hour on 2026-08-23: the table read +14.8% for 2023,
    # which is SPY from A-List's 15 February start, not SPY for 2023.
    cal: dict[str, list] = {}
    for d in sorted(spy):
        y = d[:4]
        if y not in cal:
            cal[y] = [spy[d], spy[d]]
        cal[y][1] = spy[d]
    annual = [
        {"year": y,
         "strategy": round((v[1] / v[0] - 1) * 100, 1) if v[0] else None,
         "spy": round((v[3] / v[2] - 1) * 100, 1) if v[2] else None,
         "spy_calendar": (round((cal[y][1] / cal[y][0] - 1) * 100, 1)
                          if y in cal and cal[y][0] else None),
         "partial": y == days[0][:4] and days[0][5:] > "01-05"}
        for y, v in sorted(yr.items())
    ]
    return {"cagr": blended, "spy": bench,
            "max_dd_daily": round(daily_dd * 100, 1), "annual": annual}
