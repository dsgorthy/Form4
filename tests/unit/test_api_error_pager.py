"""The API error tail pushes per INCIDENT, not per error.

2026-11-07..09: a chronic low-rate fault (PoolError 500s on ~0.3% of requests)
ran for 60 hours. The old tail pushed per error under a ROLLING 6/hour cap whose
"cap reached" notice re-fired whenever the count dipped below 6: 413 pushes,
every one saying the same thing. See scripts/api_error_tail.py for the history.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("api_error_tail", ROOT / "scripts" / "api_error_tail.py")
tail = importlib.util.module_from_spec(_spec)
sys.modules["api_error_tail"] = tail   # @dataclass resolves its module through sys.modules
_spec.loader.exec_module(tail)

POOL = ("psycopg2.pool.PoolError", "connection pool exhausted")


def _pager():
    return tail.IncidentPager()


# ── parsing ──────────────────────────────────────────────────────────────────

def test_an_access_line_500_is_parsed():
    line = ('INFO:     172.18.0.6:52314 - "GET /api/v1/filings/brvgjv/related?x=1 '
            'HTTP/1.1" 500 Internal Server Error')
    assert tail.parse_line(line) == (
        "500", "GET /api/v1/filings/brvgjv/related?x=1 HTTP/1.1",
        "/api/v1/filings/brvgjv/related?x=1")


def test_a_200_is_not_an_error():
    assert tail.parse_line('INFO:     1.2.3.4:1 - "GET /api/v1/x HTTP/1.1" 200 OK') is None


def test_route_templates_collapse_ids_and_tickers():
    t = tail.route_template
    assert t("/api/v1/filings/brvgjv/related?limit=5") == "/api/v1/filings/{id}/related"
    assert t("/api/v1/insiders/kevin-m-lalande-vzyjb8/related") == "/api/v1/insiders/{id}/related"
    assert t("/api/v1/companies/PLAN") == "/api/v1/companies/{T}"
    assert t("/api/v1/trades/12345") == "/api/v1/trades/{id}"


def test_traceback_final_lines_are_exceptions_and_log_lines_are_not():
    yes = ["psycopg2.pool.PoolError: connection pool exhausted",
           "psycopg2.errors.DeadlockDetected: deadlock detected",
           "KeyError: 'ticker'",
           "AttributeError: 'StripeObject' object has no attribute 'get'"]
    no = ["INFO:     172.18.0.6:1 - \"GET / HTTP/1.1\" 200 OK",
          "ERROR:    Exception in ASGI application",
          '  File "/app/config/database.py", line 127, in _checkout',
          "    raise PoolError(\"connection pool exhausted\")",
          "During handling of the above exception, another exception occurred:",
          "Traceback (most recent call last):"]
    for line in yes:
        assert tail.parse_line(line) and tail.parse_line(line)[0] == "exc", line
    for line in no:
        assert tail.parse_line(line) is None, line


# ── policy ───────────────────────────────────────────────────────────────────

def test_one_error_is_one_push_and_a_blip_closes_quietly():
    p = _pager()
    pushes = p.on_500(0, "/api/v1/filings/abcdef")
    pushes += p.on_exception(0.01, *POOL)
    for t in range(1, tail.QUIET_S + 3600, 30):
        pushes += p.on_tick(t)
    assert [x.title for x in pushes] == ["form4 API errors started"]
    assert "PoolError" in pushes[0].body, "the first push should name the cause"
    assert p.inc is None, "QUIET_S of quiet should have closed the incident"


def test_the_weekend_that_pushed_413_times():
    """Hourly 500 counts from logs/api-errors.log, 2026-11-07 00:00 PT onward,
    each 500 followed by its PoolError line, replayed through the pager."""
    hourly = {0: 42, 1: 10, 2: 9, 3: 44, 4: 51, 22: 145,
              24: 56, 25: 2, 26: 38, 27: 37, 33: 17, 34: 81, 35: 67, 37: 47,
              38: 175, 39: 92, 40: 50, 41: 279, 42: 103, 43: 44, 44: 105,
              45: 225, 46: 250, 47: 27, 48: 134, 49: 85, 50: 124, 51: 30,
              52: 147, 53: 153, 54: 106, 55: 40, 56: 84}
    p = _pager()
    pushes = []
    for hour in range(0, 57):
        n = hourly.get(hour, 0)
        for i in range(n):
            t = hour * 3600 + (i + 0.5) * 3600 / n
            pushes += p.on_500(t, "/api/v1/filings/abcdef")
            pushes += p.on_exception(t + 0.01, *POOL)
        for t in range(hour * 3600, (hour + 1) * 3600, 30):
            pushes += p.on_tick(t)
    titles = [x.title for x in pushes]
    assert len(pushes) <= 8, titles
    assert sum(t == "form4 API: a new kind of error" for t in titles) == 0, (
        "one cause all weekend must not keep announcing itself as new")
    assert titles[0] == "form4 API errors started"


def test_a_different_failure_breaks_through_a_chronic_one():
    p = _pager()
    p.on_500(0, "/api/v1/filings/abcdef")
    p.on_exception(0.01, *POOL)
    p.on_tick(10)                                     # the start push
    t, pushes = 20.0, []
    for _ in range(50):                               # chronic PoolErrors: silent
        pushes += p.on_500(t, "/api/v1/insiders/someone")
        pushes += p.on_exception(t + 0.01, *POOL)
        t += 30
    assert pushes == []
    pushes += p.on_500(t, "/api/v1/stripe/webhook")
    pushes += p.on_exception(t + 0.01, "AttributeError", "'StripeObject' object has no attribute 'get'")
    assert [x.title for x in pushes] == ["form4 API: a new kind of error"]
    assert "StripeObject" in pushes[0].body
    # ...once: the same new class again is counted, not pushed
    assert p.on_exception(t + 1, "AttributeError", "again") == []


def test_new_kinds_are_capped_per_incident():
    p = _pager()
    p.on_500(0, "/x"); p.on_exception(0.01, *POOL); p.on_tick(10)
    new = []
    for i, cls in enumerate(["KeyError", "TypeError", "ValueError", "IndexError", "OSError"]):
        p.on_500(20 + i, "/x")
        new += p.on_exception(20.01 + i, cls, "boom")
    assert len(new) == tail.MAX_NEW_CLASS_PUSHES


def test_a_long_incident_sends_a_summary_every_12_hours():
    p = _pager()
    pushes = []
    for t in range(0, 30 * 3600, 60):                 # a 500 a minute for 30h
        pushes += p.on_500(t, "/api/v1/filings/abcdef")
        pushes += p.on_exception(t + 0.01, *POOL)
    titles = [x.title for x in pushes]
    assert titles == ["form4 API errors started", "form4 API errors continuing",
                      "form4 API errors continuing"], titles
    assert "PoolError" in pushes[1].body and "/api/v1/filings/{id}" in pushes[1].body


def test_an_incident_that_stops_says_so():
    p = _pager()
    pushes = []
    for t in range(0, 600, 10):
        pushes += p.on_500(t, "/x")
    pushes += p.on_tick(600 + tail.QUIET_S)
    assert [x.title for x in pushes] == ["form4 API errors started", "form4 API errors stopped"]


def test_exceptions_without_a_failing_request_never_page():
    """Handled exceptions logged with a traceback are not an outage."""
    p = _pager()
    pushes = []
    for t in range(0, 3600, 5):
        pushes += p.on_exception(t, *POOL)
        pushes += p.on_tick(t)
    assert pushes == [] and p.inc is None


def test_a_restart_mid_incident_does_not_announce_it_again():
    p = _pager()
    p.on_500(0, "/x"); p.on_tick(10)
    revived = tail.IncidentPager().load_state(p.to_state())
    pushes = []
    for t in range(20, 3000, 20):
        pushes += revived.on_500(t, "/x")
    assert pushes == []
    assert tail.IncidentPager().load_state(None).inc is None
    assert tail.IncidentPager().load_state({"inc": "garbage"}).inc is None
