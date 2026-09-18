"""Bronze only grows if the EDGAR index it fetches from grows.

The hourly top-up's work list is "accessions in bronze.edgar_index with no
submission". The index was loaded once, on 2026-09-06, and never refreshed,
so the top-up succeeded every hour with nothing to do while 3,849 of the
last 14 days' 4,236 filings sat outside Bronze (Gold parity, 2026-09-18).
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPS = (ROOT / "dataplane" / "dagster_project" / "assets" / "form4_ops.py").read_text(encoding="utf-8")
REG = (ROOT / "dataplane" / "deploy" / "scheduled_work.yaml").read_text(encoding="utf-8")
LOADER = (ROOT / "scripts" / "fetch_sec_index.py").read_text(encoding="utf-8")


def test_the_index_refresh_is_an_asset_with_a_schedule():
    assert "def ops_edgar_index_refresh(" in OPS
    assert 'fetch_sec_index.py"]' in OPS
    assert '_sched("ops_edgar_index_refresh_6h", [ops_edgar_index_refresh], "5 */6 * * *")' in OPS
    assert "ops_bronze_topup, ops_edgar_index_refresh," in OPS, "the asset is not registered in the group list"


def test_it_runs_before_the_topup_each_hour_it_fires():
    assert '"5 */6 * * *"' in OPS and '"20 * * * *"' in OPS   # :05 refresh, :20 top-up


def test_it_is_in_the_registry():
    assert "name: edgar-index-refresh" in REG
    assert 'schedule: "5 */6 * * *"' in REG


def test_the_loader_rereads_the_current_quarter():
    assert "qtr != current_q" in LOADER, "a completed current quarter would never be re-read"


def test_silver_keeps_up_hourly_and_gold_refreshes_nightly():
    assert "def ops_silver_keep_up(" in OPS and "def ops_gold_refresh(" in OPS
    assert '_sched("ops_silver_keep_up_hourly", [ops_silver_keep_up], "40 * * * *")' in OPS
    assert '_sched("ops_gold_refresh_daily", [ops_gold_refresh], "30 2 * * *")' in OPS
    assert "ops_silver_keep_up, ops_gold_refresh," in OPS
    assert "name: silver-keep-up" in REG and "name: gold-refresh" in REG
    keep = (ROOT / "pipelines" / "silver" / "keep_up.py").read_text(encoding="utf-8")
    for step in ("build.py", "assess.py", "backfill_10b51.py", "--pending", "REFRESH MATERIALIZED VIEW gold.form4_line"):
        assert step in keep
