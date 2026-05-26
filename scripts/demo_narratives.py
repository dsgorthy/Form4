#!/usr/bin/env python3
"""Demo: generate "Why this trade matters" narratives for the top N high-signal
insider buys from the last 30 days, write to trade_narrative.

Run on Studio (needs Ollama + DB + yfinance).

Usage:
    python3 scripts/demo_narratives.py                  # default: 20 trades
    python3 scripts/demo_narratives.py --limit 5        # smoke test
    python3 scripts/demo_narratives.py --since 2026-05-15
    python3 scripts/demo_narratives.py --regenerate     # re-run even if cached

High-signal filter (mirror of the spec we agreed on):
    trans_code='P' AND dollar_amount >= $10k AND filing_date >= NOW()-30d
    AND NOT (is_10b5_1 OR is_recurring OR is_tax_sale OR cohen_routine)
    AND (is_csuite OR is_rare_reversal OR is_largest_ever OR pit_cluster_size>=3
         OR pit_grade IN ('A+','A') OR career_grade IN ('A+','A'))

Excludes 95% of filings. Routine 10b5-1 sells, tax sales, etc. never get a
narrative — they're noise by construction.

LLM: Ollama on Studio (GLM-4.7-flash), JSON output mode, ~5s per trade.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import httpx

from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "glm-4.7-flash"
OLLAMA_TIMEOUT = 120.0          # GLM-4.7 needs a generous timeout on cold start


SYSTEM_PROMPT = """You are an equity analyst writing for sophisticated traders.
Given the data about an insider purchase, output a JSON object with these fields:

- "summary": EXACTLY 2 sentences. Plain English: what the trade is, why it might matter.
- "price_context": 1-2 sentences explaining recent price action. CITE SPECIFIC NUMBERS
  (percentage changes, distance from 52w high/low, days since earnings).
- "catalysts": 1-3 SPECIFIC catalysts in the next 90 days. Each MUST cite a date or
  number. Sources to mine FIRST before claiming "insufficient":
    1) earnings.next_earnings_date → "Q? earnings on {date}"
    2) recent_news_7d → look for upcoming product launches, FDA dates, court rulings,
       analyst days, conference calls; cite the headline + date as the catalyst.
    3) earnings.last_eps_actual vs last_eps_estimate → if surprise, the next print
       is a setup ("Coming off a {surprise}% {beat/miss}, next print {date}").
    4) signal_flags.is_largest_ever or cluster_size → catalyst is the cluster itself
       ("{cluster_size} other insiders bought in last 30d; track that momentum").
  Only write "Insufficient data — needs manual review" if NONE of those 4 apply.
- "risks": 1-2 SPECIFIC risks to the bull case. Same rules: no generic phrases like
  "macro headwinds". Cite a specific datapoint: short interest %, a negative
  analyst note from recent_news, a missed earnings figure, sector underperformance.

Hard rules:
- Use ONLY facts present in the input data. Never invent.
- For each claim, the supporting datapoint must be in the input.
- Specific numbers and dates beat generalizations. Always.
- Output VALID JSON only. No prose before or after.
- Do not include reasoning, just the four fields.
"""


def high_signal_query(since: str, limit: int) -> str:
    return f"""
        SELECT
            t.trade_id, t.insider_id, t.ticker, t.company,
            t.filing_date::text AS filing_date,
            t.trade_date::text AS trade_date,
            t.title AS insider_title,
            COALESCE(i.display_name, i.name) AS insider_name,
            t.value AS dollar_amount,
            t.qty AS shares,
            t.price AS price_per_share,
            t.is_csuite, t.is_rare_reversal, t.is_largest_ever,
            COALESCE(t.consecutive_sells_before, 0) AS consecutive_sells_before,
            t.pit_grade, t.career_grade, t.pit_cluster_size,
            t.net_buyer_flow_90d, t.industry_buy_pct_90d,
            tm.sector, tm.industry
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        LEFT JOIN ticker_metadata tm ON tm.ticker = t.ticker
        WHERE t.trans_code = 'P'
          AND t.ticker IS NOT NULL AND t.ticker != 'NONE'
          AND COALESCE(t.is_10b5_1, 0) = 0
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
          AND t.value >= 10000
          AND t.filing_date >= '{since}'
          AND (
            t.is_csuite = 1
            OR t.is_rare_reversal = 1
            OR t.is_largest_ever = 1
            OR t.pit_cluster_size >= 3
            OR t.pit_grade IN ('A+', 'A')
            OR t.career_grade IN ('A+', 'A')
          )
        ORDER BY t.filing_date DESC, t.value DESC
        LIMIT {limit}
    """


def fetch_price_action(conn, ticker: str, ref_date: str) -> dict:
    """Compute % returns at 5d/30d/90d/1y + 52w high/low distance from
    prices.daily_prices, anchored at ref_date (the trade's filing_date)."""
    row = conn.execute(
        """SELECT date::text AS d, close FROM prices.daily_prices
           WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1""",
        (ticker, ref_date),
    ).fetchone()
    if not row:
        return {}
    current = float(row["close"])

    def _close_n_days_back(n: int) -> Optional[float]:
        r = conn.execute(
            """SELECT close FROM prices.daily_prices
               WHERE ticker = ? AND date::date <= (?::date - ?::int)
               ORDER BY date DESC LIMIT 1""",
            (ticker, ref_date, n),
        ).fetchone()
        return float(r["close"]) if r else None

    def _pct(prev: Optional[float]) -> Optional[float]:
        return None if (prev is None or prev <= 0) else round((current - prev) / prev * 100, 2)

    high_low = conn.execute(
        """SELECT MAX(high) AS hi, MIN(low) AS lo FROM prices.daily_prices
           WHERE ticker = ? AND date::date BETWEEN (?::date - INTERVAL '365 days')::date AND ?::date""",
        (ticker, ref_date, ref_date),
    ).fetchone()
    hi = float(high_low["hi"]) if high_low and high_low["hi"] else None
    lo = float(high_low["lo"]) if high_low and high_low["lo"] else None

    return {
        "current_price": current,
        "pct_chg_5d": _pct(_close_n_days_back(7)),
        "pct_chg_30d": _pct(_close_n_days_back(30)),
        "pct_chg_90d": _pct(_close_n_days_back(90)),
        "pct_chg_1y": _pct(_close_n_days_back(365)),
        "distance_from_52w_high_pct": _pct(hi),
        "distance_from_52w_low_pct": _pct(lo),
        "high_52w": hi,
        "low_52w": lo,
    }


def fetch_yfinance(ticker: str) -> dict:
    """Pull fundamentals + recent news + earnings from yfinance.

    Returns a dict with whatever was available. Missing fields → None.
    yfinance is flaky on some tickers (ETFs, delisted, etc.) — we catch
    and continue rather than fail the whole run.
    """
    out = {
        "market_cap": None, "pe_ttm": None, "pe_forward": None,
        "dividend_yield": None, "beta": None,
        "next_earnings_date": None, "last_earnings_date": None,
        "last_eps_actual": None, "last_eps_estimate": None,
        "last_surprise_pct": None, "recent_news": [],
    }
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)

        try:
            info = tk.info or {}
            out["market_cap"] = info.get("marketCap")
            out["pe_ttm"] = info.get("trailingPE")
            out["pe_forward"] = info.get("forwardPE")
            out["dividend_yield"] = info.get("dividendYield")
            out["beta"] = info.get("beta")
            out["short_pct_float"] = info.get("shortPercentOfFloat")
        except Exception as exc:
            logger.debug("yfinance.info failed for %s: %s", ticker, exc)

        try:
            ed = tk.earnings_dates
            if ed is not None and len(ed) > 0:
                future = ed[ed.index > datetime.now(ed.index.tz)]
                if len(future) > 0:
                    out["next_earnings_date"] = future.index[0].strftime("%Y-%m-%d")
                past = ed[ed.index <= datetime.now(ed.index.tz)]
                if len(past) > 0:
                    last = past.iloc[0]
                    out["last_earnings_date"] = past.index[0].strftime("%Y-%m-%d")
                    out["last_eps_actual"] = (
                        float(last.get("EPS Actual")) if last.get("EPS Actual") is not None else None
                    )
                    out["last_eps_estimate"] = (
                        float(last.get("EPS Estimate")) if last.get("EPS Estimate") is not None else None
                    )
                    out["last_surprise_pct"] = (
                        float(last.get("Surprise(%)")) if last.get("Surprise(%)") is not None else None
                    )
        except Exception as exc:
            logger.debug("yfinance.earnings_dates failed for %s: %s", ticker, exc)

        try:
            news = tk.news or []
            for n in news[:10]:
                content = n.get("content") or n
                title = content.get("title") or n.get("title")
                summary = content.get("summary") or n.get("summary")
                if not title:
                    continue
                # Robust date extraction across yfinance schema variants
                pub_date = content.get("pubDate") or content.get("displayTime")
                if not pub_date and "providerPublishTime" in n:
                    pub_date = datetime.fromtimestamp(n["providerPublishTime"]).isoformat()
                provider = (content.get("provider") or {}).get("displayName") or n.get("publisher")
                out["recent_news"].append({
                    "title": title.strip(),
                    "summary": (summary or "").strip()[:500] if summary else None,
                    "date": str(pub_date)[:10] if pub_date else None,
                    "source": provider,
                })
        except Exception as exc:
            logger.debug("yfinance.news failed for %s: %s", ticker, exc)

    except ImportError:
        logger.error("yfinance not installed")
    except Exception as exc:
        logger.warning("yfinance lookup failed for %s: %s", ticker, exc)
    return out


def build_payload(trade: dict, price_action: dict, yf_data: dict) -> dict:
    """Compose the structured payload we send to the LLM."""
    return {
        "ticker": trade["ticker"],
        "company": trade.get("company"),
        "sector": trade.get("sector"),
        "industry": trade.get("industry"),
        "insider": {
            "name": trade.get("insider_name"),
            "title": trade.get("insider_title"),
            "is_csuite": bool(trade.get("is_csuite")),
        },
        "trade": {
            "filing_date": trade["filing_date"],
            "trade_date": trade["trade_date"],
            "dollar_amount": float(trade["dollar_amount"] or 0),
            "shares": int(trade["shares"] or 0),
            "price_per_share": (
                round(float(trade["price_per_share"]), 2)
                if trade.get("price_per_share") else None
            ),
        },
        "signal_flags": {
            "is_csuite": bool(trade.get("is_csuite")),
            "is_rare_reversal": bool(trade.get("is_rare_reversal")),
            "is_largest_ever": bool(trade.get("is_largest_ever")),
            "consecutive_sells_before": int(trade.get("consecutive_sells_before") or 0),
            "career_grade": trade.get("career_grade"),
            "recent_grade": trade.get("pit_grade"),
            "pit_cluster_size": trade.get("pit_cluster_size"),
            "net_buyer_flow_90d": (
                round(float(trade.get("net_buyer_flow_90d")), 2)
                if trade.get("net_buyer_flow_90d") is not None else None
            ),
            "industry_buy_pct_90d": (
                round(float(trade.get("industry_buy_pct_90d")), 4)
                if trade.get("industry_buy_pct_90d") is not None else None
            ),
        },
        "price_action": price_action,
        "fundamentals": {
            "market_cap": yf_data.get("market_cap"),
            "pe_ttm": yf_data.get("pe_ttm"),
            "pe_forward": yf_data.get("pe_forward"),
            "dividend_yield": yf_data.get("dividend_yield"),
            "beta": yf_data.get("beta"),
            "short_pct_float": yf_data.get("short_pct_float"),
        },
        "earnings": {
            "next_earnings_date": yf_data.get("next_earnings_date"),
            "last_earnings_date": yf_data.get("last_earnings_date"),
            "last_eps_actual": yf_data.get("last_eps_actual"),
            "last_eps_estimate": yf_data.get("last_eps_estimate"),
            "last_surprise_pct": yf_data.get("last_surprise_pct"),
        },
        "recent_news_7d": yf_data.get("recent_news") or [],
    }


def hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


def call_ollama(payload: dict) -> tuple[dict | None, str | None, int]:
    """Returns (parsed_json, error_str, generation_ms)."""
    t0 = time.monotonic()
    body = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content":
                "Generate the narrative JSON for this insider purchase:\n\n"
                + json.dumps(payload, indent=2, default=str)},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.2},  # deterministic-ish output
    }
    try:
        r = httpx.post(OLLAMA_URL, json=body, timeout=OLLAMA_TIMEOUT)
    except httpx.HTTPError as exc:
        return None, f"ollama HTTP error: {exc}", int((time.monotonic() - t0) * 1000)

    if r.status_code != 200:
        return None, f"ollama HTTP {r.status_code}: {r.text[:200]}", int((time.monotonic() - t0) * 1000)

    try:
        content = r.json()["message"]["content"]
        parsed = json.loads(content)
        return parsed, None, int((time.monotonic() - t0) * 1000)
    except (KeyError, json.JSONDecodeError) as exc:
        return None, f"parse error: {exc} — raw: {r.text[:200]}", int((time.monotonic() - t0) * 1000)


def upsert_narrative(conn, trade_id: int, payload: dict, narrative: dict | None,
                     error: str | None, generation_ms: int) -> None:
    inputs_sha = hash_payload(payload)
    if narrative is None:
        narrative = {}
    conn.execute(
        """INSERT INTO trade_narrative (
              trade_id, inputs_sha, summary, price_context, catalysts, risks,
              input_data, model_name, generation_ms, error
           ) VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
           ON CONFLICT (trade_id) DO UPDATE SET
              generated_at = NOW(),
              inputs_sha = EXCLUDED.inputs_sha,
              summary = EXCLUDED.summary,
              price_context = EXCLUDED.price_context,
              catalysts = EXCLUDED.catalysts,
              risks = EXCLUDED.risks,
              input_data = EXCLUDED.input_data,
              model_name = EXCLUDED.model_name,
              generation_ms = EXCLUDED.generation_ms,
              error = EXCLUDED.error""",
        (
            trade_id, inputs_sha,
            narrative.get("summary"),
            narrative.get("price_context"),
            narrative.get("catalysts"),
            narrative.get("risks"),
            json.dumps(payload, default=str),
            OLLAMA_MODEL,
            generation_ms,
            error,
        ),
    )
    conn.commit()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=20,
                   help="max trades to process per run (production uses 20)")
    p.add_argument("--since", default=None,
                   help="filing_date >= this (default: 30 days ago). "
                        "Accepts ISO date or shortcuts: '24h', '7d', '90d'")
    p.add_argument("--regenerate", action="store_true",
                   help="ignore cached narratives; re-run all matched trades")
    p.add_argument("--no-pipeline-run", action="store_true",
                   help="skip pipeline_run wrapper (for ad-hoc manual runs)")
    args = p.parse_args()

    # Parse --since shortcuts
    if args.since and args.since.endswith(("h", "d")):
        unit = args.since[-1]
        n = int(args.since[:-1])
        hours = n if unit == "h" else n * 24
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        since_str = cutoff.strftime("%Y-%m-%d")
    elif args.since:
        since_str = args.since
    else:
        since_str = date.fromordinal(date.today().toordinal() - 30).isoformat()

    if args.no_pipeline_run:
        _run(since_str, args.limit, args.regenerate, telemetry=None)
        return

    from framework.observability import pipeline_run
    with pipeline_run(
        "enrich_high_signal_trades",
        log_path="/Users/derekg/trading-framework/logs/enrich-narratives.log",
    ) as prun:
        stats = _run(since_str, args.limit, args.regenerate, telemetry=prun)
        prun.set_rows_written(stats["matched"])
        prun.set_metadata({
            "since": since_str,
            "limit": args.limit,
            "candidates_after_filter": stats["candidates_after_filter"],
            "matched": stats["matched"],
            "failed": stats["failed"],
            "skipped_already_done": stats["skipped"],
        })


def _run(since_str: str, limit: int, regenerate: bool, telemetry) -> dict:
    """Returns {candidates, matched, failed, skipped} for pipeline_run metadata."""
    conn = get_connection()
    trades = conn.execute(high_signal_query(since_str, limit)).fetchall()
    candidates_total = len(trades)
    logger.info("Candidate high-signal trades: %d", candidates_total)

    skipped = 0
    if not regenerate:
        # Only skip trades that successfully generated a narrative. Failed
        # rows (summary IS NULL, error column populated) get retried — the
        # most common failure is Ollama timeout on dense input, which often
        # succeeds on retry once cache warms or load drops.
        already = {
            r["trade_id"] for r in conn.execute(
                "SELECT trade_id FROM trade_narrative WHERE summary IS NOT NULL"
            ).fetchall()
        }
        before = len(trades)
        trades = [t for t in trades if t["trade_id"] not in already]
        skipped = before - len(trades)
        logger.info("After dedup against existing trade_narrative: %d "
                    "(skipped %d already-done)", len(trades), skipped)

    if not trades:
        logger.info("Nothing to do.")
        return {"candidates_after_filter": candidates_total, "matched": 0,
                "failed": 0, "skipped": skipped}

    print()
    print(f"{'#':>3}  {'ticker':6}  {'insider':28}  {'$ amount':>12}  status")
    print(f"{'---':>3}  {'-' * 6}  {'-' * 28}  {'-' * 12}  ------")

    matched = 0
    failed = 0
    for i, t in enumerate(trades, 1):
        t = dict(t)
        ticker = t["ticker"]
        insider = (t.get("insider_name") or "")[:28]
        amt = t.get("dollar_amount") or 0

        price_action = fetch_price_action(conn, ticker, t["filing_date"])
        yf_data = fetch_yfinance(ticker)
        payload = build_payload(t, price_action, yf_data)
        narrative, error, ms = call_ollama(payload)
        upsert_narrative(conn, t["trade_id"], payload, narrative, error, ms)

        if narrative:
            matched += 1
            status = "ok"
        else:
            failed += 1
            status = f"FAIL: {error[:30]}"
        print(f"{i:>3}  {ticker:6}  {insider:28}  ${amt:>11,.0f}  {status} ({ms}ms)")

    return {"candidates_after_filter": candidates_total, "matched": matched,
            "failed": failed, "skipped": skipped}

    conn.close()
    logger.info("Done. Review with:")
    logger.info("  /opt/homebrew/bin/psql -d form4 -c \"SELECT trade_id, summary, "
                "catalysts FROM trade_narrative ORDER BY generated_at DESC LIMIT 5;\"")


if __name__ == "__main__":
    main()
