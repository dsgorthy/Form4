"""PITDataView — the only legal surface for strategies to read DB data.

The view wraps a `PITClock` and a DB connection. Every accessor:

  1. Builds a query whose WHERE clause includes `knowledge_date <= ?`
     bound to `clock.as_of_date`.
  2. For each row returned, calls `clock.assert_known(...)` so the read
     tape captures the row's knowledge_date. This is belt-and-suspenders —
     a query bug that leaks future rows is caught by the clock; a clock
     bug is caught by the query.
  3. Returns immutable dataclasses (TradeEvent, InsiderScore) — never
     raw tuples or dicts.

Strategies receive a `PITDataView`; they never receive a raw connection.
The convention is enforced at the strategy interface (`PITStrategy.evaluate`
takes a view, not a conn).

Note on prices: `prices.daily_prices.date` is overloaded — it's both the
trading date (transaction) and the date on which we knew the close
(knowledge). For backtest purposes we treat them as equal — a close known
"on or before" `as_of_date` is admissible. After-hours filings on the same
date observe the close in the next morning's open price; that's a fidelity
gap acknowledged in the design doc, not a PIT bug per se.
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from framework.pit.clock import PITClock
from framework.pit.events import InsiderScore, TradeEvent

logger = logging.getLogger(__name__)

# Observable-return lags (must match strategies/insider_catalog/pit_scoring.py)
RETURN_LAG_DAYS = {"7d": 10, "30d": 40, "90d": 100}


class PITDataView:
    """Read-only PIT-enforced data accessor.

    Construct one per (clock, conn). Do NOT share a view across clocks —
    that would silently allow the wrong `as_of_date` to leak in.
    """

    def __init__(self, clock: PITClock, conn) -> None:
        self.clock = clock
        self.conn = conn

    # ── Prices ──────────────────────────────────────────────────────────

    def get_close(self, ticker: str, on_or_before: Optional[str] = None,
                  lookback_days: int = 5) -> Optional[Tuple[str, float]]:
        """Most recent close at or before `on_or_before` (default: clock.as_of_date).
        Searches up to `lookback_days` calendar days back. Returns (date, close)
        or None.

        The price's `date` is treated as its knowledge_date. We assert it's
        ≤ clock.as_of_date.
        """
        target = on_or_before or self.clock.as_of_date
        if target > self.clock.as_of_date:
            raise ValueError(
                f"get_close called with on_or_before={target} > as_of={self.clock.as_of_date}"
            )
        from datetime import datetime, timedelta
        floor = (datetime.strptime(target, "%Y-%m-%d") -
                 timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        row = self.conn.execute(
            """
            SELECT date::text, close FROM prices.daily_prices
            WHERE ticker = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            (ticker, floor, target),
        ).fetchone()
        if not row or not row[1]:
            return None
        date_str = row[0]
        close = float(row[1])
        self.clock.assert_known(date_str, source=f"prices.daily_prices[{ticker}]")
        return date_str, close

    # ── Insider scoring ─────────────────────────────────────────────────

    def get_insider_score(self, insider_id: int, ticker: str) -> InsiderScore:
        """Most recent (insider, ticker) score with as_of_date ≤ clock.as_of_date.

        Returns an InsiderScore with empty defaults if no row qualifies.
        """
        row = self.conn.execute(
            """
            SELECT as_of_date::text, blended_score, career_blended_score,
                   career_grade, ticker_trade_count, global_trade_count,
                   sufficient_data
            FROM insider_ticker_scores
            WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC LIMIT 1
            """,
            (insider_id, ticker, self.clock.as_of_date),
        ).fetchone()
        if not row:
            return InsiderScore(
                insider_id=insider_id, ticker=ticker,
                as_of_date=self.clock.as_of_date, sufficient_data=False,
            )
        as_of = row[0]
        self.clock.assert_known(as_of,
                                source=f"insider_ticker_scores[{insider_id}/{ticker}]")
        # Convert career_blended_score (V3) → grade via the same mapping live uses.
        # Import inside method to avoid circular imports.
        from strategies.insider_catalog.pit_scoring import pit_score_to_grade
        career_blended = row[2]
        career_grade = row[3] or (
            pit_score_to_grade(career_blended) if career_blended is not None else None
        )
        return InsiderScore(
            insider_id=insider_id,
            ticker=ticker,
            as_of_date=as_of,
            blended_score=row[1],
            career_blended_score=career_blended,
            pit_grade=pit_score_to_grade(row[1]) if row[1] is not None else None,
            career_grade=career_grade,
            ticker_trade_count=int(row[4] or 0),
            global_trade_count=int(row[5] or 0),
            sufficient_data=bool(row[6]),
        )

    # ── Prior trades ────────────────────────────────────────────────────

    def get_prior_trades(self, insider_id: int, ticker: Optional[str] = None,
                         trade_type: str = "buy") -> List[TradeEvent]:
        """All trades by this insider whose `filing_date <= clock.as_of_date`.
        These are the trades we COULD have known about.

        If `ticker` is given, restrict to that ticker. `trade_type` is 'buy'
        by default; pass None to get all types.
        """
        sql = """
            SELECT t.trade_id, t.insider_id, t.ticker,
                   t.trade_date::text, t.filing_date::text,
                   t.trade_type, t.title, t.is_csuite,
                   t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
                   t.above_sma50, t.above_sma200, t.is_largest_ever,
                   t.is_rare_reversal, t.is_10b5_1, t.is_recurring,
                   t.is_tax_sale, t.cohen_routine,
                   t.pit_grade, t.career_grade, t.pit_blended_score,
                   t.company
            FROM trades t
            WHERE t.insider_id = ?
              AND t.filing_date <= ?
        """
        params = [insider_id, self.clock.as_of_date]
        if ticker:
            sql += " AND t.ticker = ?"
            params.append(ticker)
        if trade_type:
            sql += " AND t.trade_type = ?"
            params.append(trade_type)
        sql += " ORDER BY t.filing_date, t.trade_id"

        rows = self.conn.execute(sql, tuple(params)).fetchall()
        out: List[TradeEvent] = []
        for r in rows:
            filing_date = r[4]
            self.clock.assert_known(filing_date,
                                    source=f"trades[insider_id={insider_id}]")
            out.append(TradeEvent(
                trade_id=int(r[0]), insider_id=int(r[1]), ticker=r[2],
                trade_date=r[3], filing_date=filing_date, trade_type=r[5],
                insider_title=r[6], is_csuite=bool(r[7]) if r[7] is not None else None,
                consecutive_sells_before=r[8], dip_1mo=r[9], dip_3mo=r[10],
                above_sma50=r[11], above_sma200=r[12], is_largest_ever=r[13],
                is_rare_reversal=r[14], is_10b5_1=r[15], is_recurring=r[16],
                is_tax_sale=r[17], cohen_routine=r[18],
                pit_grade=r[19], career_grade=r[20], pit_blended_score=r[21],
                company=r[22],
            ))
        return out

    # ── Observable returns (with lag) ────────────────────────────────────

    def observable_returns(self, insider_id: int, ticker: Optional[str],
                           window: str = "7d") -> List[Tuple[str, float]]:
        """Returns the abnormal-return time series we COULD observe by now.

        Two constraints, both enforced in the query:
          1. trade_date <= (as_of - lag_days)  — the return endpoint has elapsed
          2. filing_date <= as_of              — we knew about the trade by now

        `lag_days` is 10/40/100 for 7d/30d/90d windows respectively, matching
        the convention in `strategies/insider_catalog/pit_scoring.py`.
        """
        if window not in RETURN_LAG_DAYS:
            raise ValueError(f"window must be one of {list(RETURN_LAG_DAYS)}, got {window}")
        lag = RETURN_LAG_DAYS[window]
        cutoff = self.clock.cutoff(lag_days=lag)
        col = f"abnormal_{window}"
        sql = f"""
            SELECT t.trade_date::text, tr.{col}
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ?
              AND t.trade_type = 'buy'
              AND t.trade_date <= ?
              AND t.filing_date <= ?
              AND tr.{col} IS NOT NULL
        """
        params = [insider_id, cutoff, self.clock.as_of_date]
        if ticker:
            sql += " AND t.ticker = ?"
            params.append(ticker)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        # We don't tape every return — it's the same insider, same filing_date
        # space, which we tape via get_prior_trades. But we DO assert each
        # trade_date <= cutoff (which is < as_of) as a defense-in-depth check.
        out: List[Tuple[str, float]] = []
        for r in rows:
            td = r[0]
            if td > cutoff:
                # This should be impossible given the SQL, but a sloppy
                # query rewrite could break it. Assert.
                from framework.pit.clock import LookaheadError
                raise LookaheadError(
                    f"observable_returns({insider_id}, {ticker}, {window}) "
                    f"returned trade_date={td} > cutoff={cutoff}"
                )
            out.append((td, float(r[1])))
        return out

    # ── Today's events ───────────────────────────────────────────────────

    def events_filed_on(self, date: str, trade_type: str = "buy") -> List[TradeEvent]:
        """All trades whose filing_date == `date`. Used by the engine to
        present "today's signals" to the strategy.

        Asserts date <= clock.as_of_date — you can't ask about future filings.
        """
        if date > self.clock.as_of_date:
            raise ValueError(
                f"events_filed_on({date}) called on clock with as_of={self.clock.as_of_date}"
            )
        sql = """
            SELECT t.trade_id, t.insider_id, t.ticker,
                   t.trade_date::text, t.filing_date::text,
                   t.trade_type, t.title, t.is_csuite,
                   t.consecutive_sells_before, t.dip_1mo, t.dip_3mo,
                   t.above_sma50, t.above_sma200, t.is_largest_ever,
                   t.is_rare_reversal, t.is_10b5_1, t.is_recurring,
                   t.is_tax_sale, t.cohen_routine,
                   t.pit_grade, t.career_grade, t.pit_blended_score,
                   t.company
            FROM trades t
            WHERE t.filing_date = ?
              AND t.trade_type = ?
            ORDER BY t.trade_id
        """
        rows = self.conn.execute(sql, (date, trade_type)).fetchall()
        out: List[TradeEvent] = []
        for r in rows:
            self.clock.assert_known(r[4], source=f"trades[filing_date={date}]")
            out.append(TradeEvent(
                trade_id=int(r[0]), insider_id=int(r[1]), ticker=r[2],
                trade_date=r[3], filing_date=r[4], trade_type=r[5],
                insider_title=r[6], is_csuite=bool(r[7]) if r[7] is not None else None,
                consecutive_sells_before=r[8], dip_1mo=r[9], dip_3mo=r[10],
                above_sma50=r[11], above_sma200=r[12], is_largest_ever=r[13],
                is_rare_reversal=r[14], is_10b5_1=r[15], is_recurring=r[16],
                is_tax_sale=r[17], cohen_routine=r[18],
                pit_grade=r[19], career_grade=r[20], pit_blended_score=r[21],
                company=r[22],
            ))
        return out
