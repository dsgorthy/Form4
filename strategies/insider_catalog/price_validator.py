#!/usr/bin/env python3
"""
Post-import price validation for insider trades.

SEC Form 4 filings frequently carry corrupted price/qty data, especially from
micro- and nano-cap filers. Three patterns account for nearly all of it:

  - the TOTAL dollar value sitting in the price-per-share field, so that
    value = price * qty squares the trade
  - a decimal shift (100x, 10000x)
  - the share count copied into the price field

ConnectM is the clearest example. On 2024-08-29 the stock traded between $0.84
and $0.90 and the filing reports price 65,122 against qty 73,680 — 73,680
shares at $0.88 is $65,122, which is what the "price" actually is. The stored
value is price x qty = $4,798,188,960. A $65K purchase became a $4.8B one.

THE REFERENCE IS A BAND, NOT A CLOSE

A real open-market execution has to land inside the day's trading range, so
prices.daily_prices high/low is the natural check — and it holds: 86.8% of
purchase and sale rows sit exactly inside their own day's low-to-high, on 96.9%
coverage. The band is widened to BAND_WEEKS so that a filing whose trade_date
falls on a holiday, a halt, or a slightly-off reported date still has something
to be measured against.

WHY A NAIVE BAND CHECK WOULD CORRUPT GOOD DATA

prices.daily_prices is SPLIT-ADJUSTED. trades.price is as-filed. So every
stock that split after a trade shows the split factor as a "deviation":

    CRWD  2026-07-01   filed $780.73   adjusted band $191.44-$196.40   = 3.98x
    AMCR               median ratio 0.200 across 21 rows, spread 0.4%
    ATUS               median ratio 9.618 across 123 rows, spread 5.1%

None of those are filing errors, and "repairing" them would destroy correct
prices. The discriminator is the SHAPE of the ratios, not their size:

    a split or ADR-ratio change  ->  every filer that day moves by the SAME
                                     factor               (CRWD: 3.96-4.00)
    a parse error                ->  one filer is wrong by their own arbitrary
                                     amount               (CNTM: 1,204-80,209)

So the second gate is the day's OTHER filers. Every Form 4 for a stock on a
given day is quoted on the same basis as every other, whichever basis our price
history happens to be on, so filer agreement means our reference is on a
different footing rather than that the filing is wrong. That is a fact about
the data, not an inference, which is why it beat the earlier attempt to detect
split factors statistically — an eight-week band that straddles a split has a
4x-wide range and no usable midpoint.

WHAT GETS REPAIRED AUTOMATICALLY

Only price_is_total_value, and only when two independent things agree: the
arithmetic identity (price / qty lands inside the band) and the band itself.
Decimal shifts are annotated, never auto-applied — a foreign private issuer
reporting in rupees is a 10x+ "deviation" that dividing by 100 would appear to
fix while leaving the number wrong.

Everything else that misses the band gets a note in suspect_reason and nothing
more. In particular it does NOT get value_suspect: a band miss with no peer to
confirm it is a suspicion, and value_suspect removes a row from every dollar
aggregate on the site. See note_uncorrectable for the filing that settles it.

Corrections are recorded on the row (price_as_filed, value_as_filed,
correction_method) so the filing page can show what the filer submitted next to
what we display.

USAGE

    # Report only. Always start here — this is the default.
    python3 strategies/insider_catalog/price_validator.py

    python3 strategies/insider_catalog/price_validator.py --since 2026-01-01
    python3 strategies/insider_catalog/price_validator.py --ticker CNTM

    # Write the safe class of correction.
    python3 strategies/insider_catalog/price_validator.py --apply

    # Opt into a riskier class explicitly, after reviewing the report.
    python3 strategies/insider_catalog/price_validator.py --apply \
        --methods price_is_total_value,power_of_10_shift

Runs on Studio — Mini has no form4 database.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import logging
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

#: How far back the reference band reaches from the trade date. Eight weeks is
#: wide enough that a missing or slightly-wrong trade_date still resolves, and
#: narrow enough that a stock's own range still means something.
BAND_WEEKS = 8

#: How far outside the band a price may sit before it is called suspect. The
#: band is already the extremes of forty trading days, so past 1.5x of it is
#: not a price move.
BAND_TOLERANCE = 1.5

#: A correction must land within this multiple of the band edges.
CORRECTION_TOLERANCE = 1.5

#: Peer agreement. How many OTHER filers of the same stock on the same day are
#: needed before their consensus outranks our price history, and how far this
#: filing may sit from that consensus. 15% covers a full day's trading range
#: plus the rounding filers apply to weighted averages.
PEER_MIN_ROWS = 2
PEER_TOLERANCE = 1.15

BRK_TICKERS = {"BRK-A", "BRK.A", "BRKA", "BRK/A"}


# ── reference band ──────────────────────────────────────────────────────────

def _scope(since, ticker, alias="t"):
    where = [f"{alias}.price > 0", f"{alias}.qty > 0", f"{alias}.is_derivative = 0",
             f"{alias}.trans_code IN ('P','S')"]
    params = []
    if since:
        where.append(f"{alias}.trade_date >= ?")
        params.append(since)
    if ticker:
        where.append(f"{alias}.ticker = ?")
        params.append(ticker.upper())
    return where, params


def load_rows(conn, since, ticker) -> list[dict]:
    where, params = _scope(since, ticker)
    return [dict(r) for r in conn.execute(
        f"""SELECT t.trade_id, t.ticker, t.trade_date, t.price, t.qty, t.value
              FROM trades t
             WHERE {' AND '.join(where)}
             ORDER BY t.trade_id""",
        tuple(params)).fetchall()]


def load_bands(conn, since: str | None, ticker: str | None) -> dict:
    """{(ticker, trade_date): (low, high)} over the trailing BAND_WEEKS.

    Computed against the distinct (ticker, trade_date) pairs actually present
    rather than per row, so a ticker with 500 filings on one day costs one
    window instead of 500.
    """
    where, params = _scope(since, ticker)
    rows = conn.execute(
        f"""
        WITH wanted AS (
            SELECT DISTINCT t.ticker, t.trade_date
              FROM trades t
             WHERE {' AND '.join(where)}
        )
        SELECT w.ticker, w.trade_date,
               MIN(d.low)  AS band_low,
               MAX(d.high) AS band_high
          FROM wanted w
          JOIN prices.daily_prices d
            ON d.ticker = w.ticker
           AND d.date <= w.trade_date
           AND d.date >= to_char(w.trade_date::date - INTERVAL '{BAND_WEEKS} weeks',
                                 'YYYY-MM-DD')
         WHERE d.low > 0 AND d.high > 0
         GROUP BY w.ticker, w.trade_date
        """,
        tuple(params),
    ).fetchall()
    return {(r["ticker"], r["trade_date"]):
            (float(r["band_low"]), float(r["band_high"])) for r in rows}


def peer_prices(rows: list[dict]) -> dict:
    """{(ticker, trade_date): [prices]} — what everyone else filed that day.

    Peers are the split-proof reference. Every Form 4 for a stock on a given
    day is quoted on the same basis as every other, whatever our price history
    happens to be adjusted to, so agreement among filers says "our reference is
    on a different footing", not "this filing is wrong". That single fact
    replaces trying to infer split factors: on 2026-07-01 forty CrowdStrike
    insiders all filed near $776 against an adjusted band of $191-$196, and
    they are all correct.
    """
    peers: dict[tuple, list[float]] = defaultdict(list)
    for r in rows:
        peers[(r["ticker"], r["trade_date"])].append(float(r["price"]))
    return peers


def peers_agree(row: dict, peers: dict) -> bool:
    """True when enough other filers that day quoted essentially this price."""
    same_day = peers.get((row["ticker"], row["trade_date"]), [])
    others = [p for p in same_day if p != float(row["price"])]
    # A lone dissenter among identical prices is still agreement; require the
    # cohort to be real before trusting it.
    if len(same_day) - 1 < PEER_MIN_ROWS:
        return False
    med = statistics.median(others) if others else statistics.median(same_day)
    if med <= 0:
        return False
    ratio = float(row["price"]) / med
    return 1 / PEER_TOLERANCE <= ratio <= PEER_TOLERANCE


# ── detection and repair ────────────────────────────────────────────────────

#: Split ratios common enough to be worth recognising. A filing at N x the
#: adjusted band, where N is one of these, is far more likely a real trade
#: against split-adjusted history than a parse failure.
SPLIT_RATIOS = (2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 25, 30, 50, 100)

#: A ticker needs this many banded filings before the self-consistency
#: test means anything. Below it, one row proves nothing either way.
MIN_FILINGS_FOR_SPLIT_TEST = 3
#: ...of which this share must miss the band high. A split moves every
#: filing; parse errors are a minority against a compliant majority.
SPLIT_MAJORITY = 0.75
#: ...and they must be off by a SIMILAR factor. GOOG's ratios span ~1.6x
#: end to end; IHT's parse errors span four orders of magnitude.
SPLIT_SPREAD = 8.0
SPLIT_RATIO_TOLERANCE = 0.15   # +/-15%. Generous on purpose: the
#: nearest genuine parse error sits at 2,551x the band, so there is a
#: two-order-of-magnitude gap to spend. At 6% a GOOG row at 21.2x — a
#: 20:1 split plus ordinary price drift — fell through by 0.05%.


def looks_like_a_split(price: float, lo: float, hi: float) -> int | None:
    """Return the split ratio that explains this price, or None.

    WHY THIS EXISTS, 2026-09-04.

    price_is_total_value is an arithmetic identity and it fires on
    coincidences. GOOG sold at $2,076.19 x 30 shares in March 2021 is a real
    $62,285 trade, but our price history is adjusted for the 2022 20:1 split,
    so the band reads $69 and 2076.19/30 = $69.21 lands right in it. The rule
    then "corrects" a correct row into a $2,076 trade at $69 a share.

    Applied unattended across all history this rewrote 29 real trades: 17
    GOOG, 4 AMZN (20:1, 2022) and 8 CMG (50:1, 2024). Every one had to be
    restored from price_as_filed.

    note_uncorrectable already documents the same trap from the other side --
    the Lucid PIF filing missing its band only because the history is adjusted
    for a later REVERSE split. This is that hazard, in the direction where a
    correction is available and wrong.

    The test is deliberately cheap and does not need split reference data: if
    the filed price sits at a round split multiple of the band, the band is
    describing post-split prices and cannot judge this row.
    """
    mid = (lo + hi) / 2
    if mid <= 0 or price <= 0:
        return None
    ratio = price / mid
    for n in SPLIT_RATIOS:
        if abs(ratio - n) / n <= SPLIT_RATIO_TOLERANCE:
            return n
    return None


def attempt_correction(price: float, qty: float, lo: float, hi: float) -> dict | None:
    """Explain a bad price as one of the three known parse failures, or not at all.

    price/qty is tried first: when it is right it is an exact arithmetic
    identity, whereas a power of ten fits loosely against all sorts of things.
    Returns None rather than a best guess.
    """
    def in_band(candidate: float) -> bool:
        return lo / CORRECTION_TOLERANCE <= candidate <= hi * CORRECTION_TOLERANCE

    # A price that is a round split multiple of the band means the BAND is
    # wrong for this row, not the price. Refuse to correct rather than guess:
    # find_suspects still records it, and note_uncorrectable writes the doubt.
    split = looks_like_a_split(price, lo, hi)
    if split:
        return None

    # 1. The price field holds the trade's total value. True price = price/qty,
    #    and the true VALUE is the number sitting in the price field.
    if qty > 1:
        candidate = price / qty
        if in_band(candidate):
            return {"price": round(candidate, 6), "value": round(price, 2),
                    "method": "price_is_total_value"}

    # 2. Decimal shift.
    mid = (lo + hi) / 2
    if mid > 0 and price > 0:
        ratio = price / mid
        power = round(math.log10(ratio)) if ratio > 0 else 0
        if power != 0 and abs(math.log10(ratio) - power) < 0.3:
            candidate = price / (10 ** power)
            if in_band(candidate):
                return {"price": round(candidate, 6),
                        "value": round(candidate * qty, 2),
                        "method": "power_of_10_shift"}

    # 3. The share count was copied into the price field.
    if abs(price - qty) < 0.01 and in_band(mid):
        return {"price": round(mid, 6), "value": round(mid * qty, 2),
                "method": "price_equals_qty"}

    return None


def split_adjusted_tickers(rows: list[dict], bands: dict) -> set:
    """Tickers whose BAND is on a different scale to their own FILINGS.

    THE TEST IS SELF-CONSISTENCY, not a ratio. An earlier version asked
    whether a row sat at a round split multiple of its band; that flagged GOOG
    correctly and then flagged IHT too, because one IHT parse error happened
    to land near 20x. A whole ticker's genuine corrections were refused on one
    coincidence.

    The reliable question is whether the ticker's filings AGREE WITH EACH
    OTHER:

      GOOG  filings cluster at $1,725-$2,963 across 2020-2022 and the band
            says $69. Every filing agrees; the BAND is the outlier, because
            the history is adjusted for the 2022 20:1 split. Refuse.

      IHT   filings cluster at $1-$2.50 with occasional $52,316. The filings
            disagree with each other; the outliers are parse failures and the
            band is fine. Correct them.

    So: a ticker is split-adjusted when MOST of its filings miss the band in
    the same direction by a similar factor. A ticker with parse errors has a
    few wild outliers against a compliant majority.
    """
    from collections import defaultdict
    by_ticker = defaultdict(list)
    for r in rows:
        band = bands.get((r["ticker"], r["trade_date"]))
        if not band or band[0] <= 0 or r["price"] <= 0:
            continue
        by_ticker[r["ticker"]].append(r["price"] / ((band[0] + band[1]) / 2))

    flagged = set()
    for ticker, ratios in by_ticker.items():
        if len(ratios) < MIN_FILINGS_FOR_SPLIT_TEST:
            continue
        # What share of this ticker's filings sit well above the band? If it is
        # most of them, the band is describing a different share class.
        off = [x for x in ratios if x >= 1.5]
        if len(off) / len(ratios) < SPLIT_MAJORITY:
            continue
        # And they must be off by a SIMILAR factor — a split moves every
        # filing by the same ratio, while parse errors scatter over orders of
        # magnitude.
        #
        # PERCENTILES, NOT MIN/MAX. GOOG's off-band ratios sit tightly around
        # 20x, but a single 209x row (a 20:1 split compounded with a decimal
        # typo) and a handful just over the 1.5 threshold made min/max read
        # 139x, so the ticker was not flagged and the 209x row was then
        # "corrected". One outlier at each end must not decide this.
        off.sort()
        lo_r = off[int(len(off) * 0.10)]
        hi_r = off[min(len(off) - 1, int(len(off) * 0.90))]
        if lo_r > 0 and hi_r / lo_r <= SPLIT_SPREAD:
            flagged.add(ticker)
    return flagged


def find_suspects(rows: list[dict], bands: dict, peers: dict,
                  split_tickers: set | None = None):
    """Rows whose price cannot be reconciled with what the stock was worth.

    Two gates, in order:

      1. Inside the band, fine. That is 86.8% of rows before the window is even
         widened past their own trade date.
      2. Outside the band but the day's other filers agree, fine. This is the
         split and ADR-ratio case, and peers are the only reference immune to
         it.

    What survives both is a price that neither market data nor any other filer
    supports.
    """
    suspects, peer_excused, no_band = [], 0, 0
    for r in rows:
        tk = r["ticker"]
        if tk.upper() in BRK_TICKERS:
            continue                              # $695K a share is real
        band = bands.get((tk, r["trade_date"]))
        if not band:
            no_band += 1
            continue                              # nothing to compare against
        price, qty = float(r["price"]), float(r["qty"])
        lo, hi = band

        if lo / BAND_TOLERANCE <= price <= hi * BAND_TOLERANCE:
            continue
        if peers_agree(r, peers):
            peer_excused += 1
            continue

        suspects.append({
            "trade_id": r["trade_id"], "ticker": tk, "trade_date": r["trade_date"],
            "price": price, "qty": qty, "value": float(r["value"] or 0),
            "band_lo": lo, "band_hi": hi,
            "excess": price / hi if price > hi else lo / price,
            # No correction is offered for a ticker whose band is on a
            # different scale to its filings. The row-level check catches
            # clean multiples; this catches the rest of that ticker's rows,
            # which are measured against the same wrong scale.
            "correction": (
                None if (split_tickers and r["ticker"] in split_tickers)
                else attempt_correction(price, qty, lo, hi)
            ),
        })
    return suspects, peer_excused, no_band


def apply_correction(conn, s: dict) -> None:
    """Rewrite price and value, keeping what the filer actually submitted.

    value_suspect is maintained by its own trigger, which fires on price/value.
    """
    c = s["correction"]
    conn.execute(
        """UPDATE trades
              SET price_as_filed    = COALESCE(price_as_filed, price),
                  value_as_filed    = COALESCE(value_as_filed, value),
                  correction_method = ?,
                  price = ?, value = ?,
                  suspect_reason = ?
            WHERE trade_id = ?""",
        (c["method"], c["price"], c["value"],
         f"corrected: {c['method']} (filed ${s['price']:,.2f})", s["trade_id"]),
    )


def note_uncorrectable(conn, s: dict) -> None:
    """Record the doubt. Do NOT set value_suspect.

    A band miss with no peer to confirm it is a suspicion, not a finding, and
    value_suspect hides a row from every dollar aggregate on the site. Lucid is
    the case that settles it: the PIF's 265,693,703 shares at $6.83 is a real
    $1.8B filing, and it misses the band only because our price history is
    adjusted for the later reverse split while the filing is not. It has no
    same-day peers, so nothing vouches for it — flagging it would delete a real
    transaction from the product to tidy up a reference-data mismatch.

    value_suspect stays reserved for what is impossible on its own terms:
    over $5B, or a trade dated after it was filed. Both are enforced by the
    trigger, not here.
    """
    conn.execute(
        """UPDATE trades SET suspect_reason = ? WHERE trade_id = ?""",
        (f"price_outside_{BAND_WEEKS}w_band_{s['excess']:.0f}x "
         f"(${s['price']:,.2f} vs ${s['band_lo']:,.2f}-${s['band_hi']:,.2f}, "
         f"no same-day peer to confirm)",
         s["trade_id"]),
    )


#: Corrections safe to write unattended. `price_is_total_value` is an
#: arithmetic identity — price x qty already equals the stored value, so
#: `price` is holding the total. `power_of_10_shift` is deliberately excluded:
#: it cannot tell a decimal typo from a foreign currency, and the CLI keeps it
#: opt-in for the same reason.
AUTO_APPLY_METHODS = ("price_is_total_value",)


def run_validation(conn, since: str | None = None, ticker: str | None = None,
                   apply_methods: tuple = AUTO_APPLY_METHODS) -> dict:
    """Validate recently-ingested prices. Called from the ingest path.

    THIS FUNCTION DID NOT EXIST until 2026-09-04, and fetch_latest.py has been
    calling it since it was written:

        from price_validator import run_validation
        run_validation(conn)

    wrapped in `except Exception` and logged as a WARNING. So every ingest run
    reported success while validating nothing, and the ImportError scrolled
    past in a log nobody reads. That is why 40,049 non-derivative P/S rows
    carry a price more than 10x from that day's close.

    SCOPED BY DEFAULT. `since=None` here means TODAY, not "everything" — the
    CLI's None means the whole table, which is right for a manual sweep and
    catastrophic on a job that runs every five minutes against 1.65M rows.
    Callers wanting the full pass should use the CLI.

    WRITES ONLY THE ARITHMETIC-IDENTITY CLASS. Everything else that misses its
    band is recorded in suspect_reason and left alone, which is the existing
    doctrine: a band miss with no same-day peer is a suspicion, not a finding,
    and value_suspect stays reserved for what is impossible on its own terms.
    """
    if since is None:
        since = _dt.date.today().isoformat()

    rows = load_rows(conn, since, ticker)
    if not rows:
        return {"checked": 0, "corrected": 0, "flagged": 0}

    bands = load_bands(conn, since, ticker)
    peers = peer_prices(rows)
    split_tickers = split_adjusted_tickers(rows, bands)
    suspects, _peer_excused, _no_band = find_suspects(rows, bands, peers,
                                                      split_tickers)

    allowed = set(apply_methods)
    corrected = flagged = 0
    for s in suspects:
        fix = s.get("correction")
        if fix and fix["method"] in allowed:
            apply_correction(conn, s)
            corrected += 1
        else:
            note_uncorrectable(conn, s)
            flagged += 1
    conn.commit()
    return {"checked": len(rows), "corrected": corrected, "flagged": flagged}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="Only trades on/after this date (YYYY-MM-DD)")
    ap.add_argument("--ticker", help="Only this ticker")
    ap.add_argument("--apply", action="store_true",
                    help="Write corrections. Default is report-only.")
    ap.add_argument("--methods", default="price_is_total_value",
                    help="Correction classes to write. Defaults to the "
                         "arithmetic-identity class; power_of_10_shift is "
                         "opt-in because it cannot tell a decimal typo from a "
                         "foreign currency.")
    ap.add_argument("--show", type=int, default=25, help="Examples to print")
    args = ap.parse_args()

    conn = get_connection()
    logger.info("loading rows...")
    rows = load_rows(conn, args.since, args.ticker)
    logger.info("loading %s-week bands for %s rows...", BAND_WEEKS, len(rows))
    bands = load_bands(conn, args.since, args.ticker)
    peers = peer_prices(rows)
    logger.info("%s bands, %s ticker-days of peer prices", len(bands), len(peers))

    split_tickers = split_adjusted_tickers(rows, bands)
    if split_tickers:
        logger.info("%d ticker(s) have split-adjusted history; no corrections "
                    "will be offered for them", len(split_tickers))
    suspects, peer_excused, no_band = find_suspects(rows, bands, peers,
                                                    split_tickers)

    allowed = {m.strip() for m in args.methods.split(",") if m.strip()}
    fixable = [s for s in suspects
               if s["correction"] and s["correction"]["method"] in allowed]
    fixable_ids = {s["trade_id"] for s in fixable}
    unfixable = [s for s in suspects if s["trade_id"] not in fixable_ids]

    by_method = defaultdict(int)
    for s in suspects:
        if s["correction"]:
            by_method[s["correction"]["method"]] += 1

    print(f"\nrows checked         {len(rows):,}")
    print(f"  no price history   {no_band:,}")
    print(f"  peers agree        {peer_excused:,}   (split / ADR basis, not errors)")
    print(f"\nsuspect rows         {len(suspects):,}")
    print(f"  will correct       {len(fixable):,}   (--methods {args.methods})")
    for method, n in sorted(by_method.items(), key=lambda kv: -kv[1]):
        print(f"    {method:<24} {n:>7,}   "
              f"{'write' if method in allowed else 'flag only'}")
    print(f"  no safe correction {len(suspects) - sum(by_method.values()):,}")
    print(f"\ndollar value as stored   ${sum(s['value'] for s in suspects):,.0f}")
    print(f"dollar value if repaired ${sum(s['correction']['value'] for s in fixable):,.0f}"
          f"  (rows being written)")

    print(f"\nlargest {args.show} by stored value:")
    for s in sorted(suspects, key=lambda x: -x["value"])[:args.show]:
        fix = s["correction"]
        if fix and s["trade_id"] in fixable_ids:
            arrow = f"-> ${fix['price']:,.4f} / ${fix['value']:,.0f}  [{fix['method']}]"
        elif fix:
            arrow = f"-> FLAG ONLY ({fix['method']} not in --methods)"
        else:
            arrow = "-> NO SAFE CORRECTION"
        print(f"  {s['ticker']:>6s} {s['trade_date']}  "
              f"${s['price']:>12,.2f} x {s['qty']:>10,.0f} = ${s['value']:>16,.0f}  "
              f"(band ${s['band_lo']:,.2f}-${s['band_hi']:,.2f}) {arrow}")

    if not args.apply:
        print("\nreport only — pass --apply to write these changes")
        return 0

    for s in fixable:
        apply_correction(conn, s)
    for s in unfixable:
        note_uncorrectable(conn, s)
    conn.commit()
    logger.info("corrected %s rows, annotated %s for review",
                len(fixable), len(unfixable))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
