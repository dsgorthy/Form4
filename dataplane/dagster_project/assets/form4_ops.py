"""Operational jobs, moved off launchd onto Dagster.

Dagster owns scheduling. These twelve were still on cron on 2026-08-26, ordered
by wall clock and visible only by SSHing into Studio and reading plists out of
four different directories. Moving them changes ORCHESTRATION, NOT LOGIC: each
asset shells out to the exact command its plist ran, same interpreter, same
arguments, same working directory.

Why bother, when a cron that works is a cron that works:

  - one place to see whether it ran, what it printed, and how long it took
  - retries and failure sensors instead of a plist that exits non-zero into a
    log nobody tails
  - the remainder becomes countable. dataplane/deploy/scheduled_work.yaml plus
    scripts/check_scheduled_work.py fail if a `dagster` unit still has a loaded
    plist, which is the double-run this file is careful to avoid.

MOST OF THESE ALREADY WRAP framework.observability.wrap, which records to
form4.pipeline_runs and surfaces at /admin/pipelines. That is deliberately
preserved rather than replaced — it is the existing telemetry and the admin
page reads it. Dagster is the scheduler here, not a second observability story.

WHAT IS NOT HERE, AND WHY: anything that writes to `trades` or
`strategy_portfolio` is held back while the 2026-08-26 SEC reload and recompute
are in flight — strategy-simulator in particular runs the very script the
rebuild is running with --rebuild. insider-fetch is held back because it is the
core ingestion and deserves its own change with its parity gate. See the
`pending` section of scheduled_work.yaml.
"""
# NOTE: deliberately no `from __future__ import annotations` — PEP 563
# stringifies annotations and Dagster's context-type validation rejects them,
# so every asset in the module silently fails to define. Same note as
# form4_pipeline.py; it has bitten this codebase before.

import os
import subprocess

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

REPO = "/Users/derekg/trading-framework"

# The plists set WorkingDirectory and inherit PATH. Reproduce both explicitly:
# backfill_returns.py is the known case where a wrapped script shells out to
# another WITHOUT setting cwd or PYTHONPATH, inherits them from its plist, and
# dies on `import config` in the child while the parent reports success.
SCRIPT_ENV = {
    **os.environ,
    "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": REPO,
}

BREW = "/opt/homebrew/bin/python3"
BREW312 = "/opt/homebrew/bin/python3.12"
SYS = "/usr/bin/python3"

GROUP = "form4_ops"


def _run(context: AssetExecutionContext, args: list[str], timeout: int = 3600) -> Output:
    """Run one wrapped command, exactly as its plist did."""
    context.log.info("running: %s", " ".join(args))
    proc = subprocess.run(args, cwd=REPO, env=SCRIPT_ENV, timeout=timeout,
                          capture_output=True, text=True)
    out = (proc.stdout or "")[-4000:]
    err = (proc.stderr or "")[-2000:]
    if proc.returncode != 0:
        raise RuntimeError(f"exited {proc.returncode}\nSTDOUT:\n{out[-1500:]}\n"
                           f"STDERR:\n{err[-1000:]}")
    return Output(None, metadata={
        "command": MetadataValue.text(" ".join(args)),
        "stdout_tail": MetadataValue.md(f"```\n{out[-1500:] or '(empty)'}\n```"),
    })


def _wrapped(name: str, *cmd: str) -> list[str]:
    """framework.observability.wrap records the run to form4.pipeline_runs.

    Kept because /admin/pipelines reads that table and because it is the
    existing Stage 2.5 telemetry. Dagster replaces the CLOCK, not this.
    """
    return [BREW, "-m", "framework.observability.wrap", name, "--", *cmd]


# ── every 5 minutes ────────────────────────────────────────────────────────

@asset(group_name=GROUP, compute_kind="python",
       description="LLM narratives for recent filings (was com.openclaw.enrich-narratives).")
def ops_enrich_narratives(context: AssetExecutionContext) -> Output:
    # The plist fired at :02,:07,:12… — twelve calendar entries rather than a
    # StartInterval, which is a cron expressing "every 5 minutes" the long way.
    return _run(context, [BREW, f"{REPO}/scripts/demo_narratives.py",
                          "--since", "24h", "--limit", "20"], timeout=900)


# ── every 30 minutes ───────────────────────────────────────────────────────

@asset(group_name=GROUP, compute_kind="python",
       description="Breaking-signal scan (was com.openclaw.breaking-signal).")
def ops_breaking_signal(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("breaking_signal", "/bin/bash",
                                  f"{REPO}/pipelines/run_breaking_signal.sh"), timeout=1800)


# ── every 6 hours ──────────────────────────────────────────────────────────

@asset(group_name=GROUP, compute_kind="python",
       description="Trial lifecycle email sequence (was com.openclaw.trial-emails).")
def ops_trial_emails(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("trial_emails", BREW,
                                  f"{REPO}/pipelines/trial_emails.py"), timeout=1800)


# ── daily / weekday ────────────────────────────────────────────────────────

@asset(group_name=GROUP, compute_kind="python",
       description="CEO Watcher reader (was com.openclaw.ceowatcher-reader, 08:30).")
def ops_ceowatcher_reader(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("ceowatcher_reader", SYS,
                                  f"{REPO}/pipelines/ceowatcher_reader.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Monday paper-account monitor (was com.openclaw.monday-paper-monitor, Mon 07:30).")
def ops_monday_paper_monitor(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("monday_paper_monitor", BREW,
                                  f"{REPO}/scripts/monday_paper_monitor.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Alpaca position reconciliation (was com.openclaw.alpaca-reconcile, weekdays 13:30).")
def ops_alpaca_reconcile(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("alpaca_reconcile", SYS,
                                  f"{REPO}/scripts/alpaca_reconcile.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Thesis monitor (was com.openclaw.thesis-monitor, weekdays 13:30).")
def ops_thesis_monitor(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("thesis_monitor", BREW312, "-m",
                                  "pipelines.thesis_monitor.monitor"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Post-deploy audit (was com.openclaw.post-deploy-audit, weekdays 14:00).")
def ops_post_deploy_audit(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("post_deploy_audit", BREW,
                                  f"{REPO}/scripts/post_deploy_audit.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Daily summary (was com.openclaw.daily-summary, weekdays 14:30).")
def ops_daily_summary(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("daily_summary", SYS,
                                  f"{REPO}/scripts/daily_summary.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="Daily content generation (was com.openclaw.daily-content, 17:00).")
def ops_daily_content(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("daily_content", "/bin/bash",
                                  f"{REPO}/pipelines/run_daily_content.sh"), timeout=3600)


@asset(group_name=GROUP, compute_kind="python",
       description="Strategy health check (was com.openclaw.strategy-health, 17:00).")
def ops_strategy_health(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("strategy_health", SYS,
                                  f"{REPO}/scripts/strategy_health_check.py"), timeout=1800)


@asset(group_name=GROUP, compute_kind="python",
       description="PIT shadow run (was com.openclaw.pit-shadow, 18:00).")
def ops_pit_shadow(context: AssetExecutionContext) -> Output:
    return _run(context, _wrapped("pit_shadow", BREW,
                                  f"{REPO}/scripts/pit_shadow_run.py"), timeout=3600)


# ── jobs + schedules ───────────────────────────────────────────────────────
#
# Times are the plists' times, unchanged. Moving the clock and the scheduler in
# one step would make a regression impossible to attribute.

# ── Strategy runners ───────────────────────────────────────────────────────
#
# These were the last three launchd daemons, and the reason they were never
# migrated is that they are DAEMONS, not crons: cw_runner's main path is a
# `while True` with adaptive sleeps (60s/300s/1800s/3600s by market state).
# Dagster schedules jobs; it does not host a process that sleeps.
#
# Two things make the conversion safe rather than a rewrite:
#
#   1. cw_runner ALREADY has --once, which runs one cycle through run_daily()
#      and exits. No refactor of the loop was needed.
#   2. Dedup is DB-BACKED, not in-memory:
#           used_trade_ids = SELECT trade_id FROM strategy_portfolio
#                             WHERE strategy = ?
#      plus held_tickers from open positions. A scan is therefore idempotent
#      ACROSS PROCESS BOUNDARIES -- running it fresh every few minutes reaches
#      the same decisions a daemon would, and cannot re-alert on something it
#      already alerted on. Had dedup lived in process memory this migration
#      would have double-alerted on every tick.
#
# Scheduled on market hours only. The daemon spent nights and weekends asleep
# in a loop; a schedule simply does not fire, which is the same behaviour
# without a process to die quietly. That matters: these three were unloaded on
# 2026-08-26 for the SEC reload and nobody noticed for SEVEN DAYS, because a
# daemon that is gone looks exactly like a daemon that is idle.

_STRATS = "/Users/derekg/trading-framework/strategies/cw_strategies/configs"


def _runner(context: AssetExecutionContext, name: str) -> Output:
    return _run(context, _wrapped(f"cw_runner_{name}", BREW,
                                  f"{REPO}/strategies/cw_strategies/cw_runner.py",
                                  "--config", f"{_STRATS}/{name}.yaml",
                                  "--once"), timeout=900)


@asset(group_name=GROUP, compute_kind="python",
       description="A-List Buys scan (was com.openclaw.quality-notrend, a daemon).")
def ops_runner_quality_notrend(context: AssetExecutionContext) -> Output:
    return _runner(context, "quality_notrend")


@asset(group_name=GROUP, compute_kind="python",
       description="Insider Breakout scan (was com.openclaw.quality-momentum, a daemon).")
def ops_runner_quality_momentum(context: AssetExecutionContext) -> Output:
    return _runner(context, "quality_momentum")


@asset(group_name=GROUP, compute_kind="python",
       description="Insider Dip Buys scan (was com.openclaw.reversal-dip, a daemon).")
def ops_runner_reversal_dip(context: AssetExecutionContext) -> Output:
    return _runner(context, "reversal_dip")


form4_ops_assets = [
    ops_enrich_narratives, ops_breaking_signal, ops_trial_emails,
    ops_ceowatcher_reader, ops_monday_paper_monitor, ops_alpaca_reconcile,
    ops_thesis_monitor, ops_post_deploy_audit, ops_daily_summary,
    ops_daily_content, ops_strategy_health, ops_pit_shadow,
    ops_runner_quality_notrend, ops_runner_quality_momentum,
    ops_runner_reversal_dip,
]

PT = "America/Los_Angeles"


def _sched(name: str, assets: list, cron: str) -> ScheduleDefinition:
    return ScheduleDefinition(
        name=name,
        job=define_asset_job(name=f"job_{name}", selection=[a.key for a in assets]),
        cron_schedule=cron,
        execution_timezone=PT,
        default_status=DefaultScheduleStatus.RUNNING,
    )


form4_ops_schedules = [
    # Every 10 minutes, market hours, weekdays only. The daemon polled at
    # 60s-3600s depending on state; 10 minutes is inside the tightest of those
    # and cheap, because a scan that finds nothing new exits in seconds.
    _sched("ops_runner_notrend",    [ops_runner_quality_notrend],  "*/10 6-13 * * 1-5"),
    _sched("ops_runner_momentum",   [ops_runner_quality_momentum], "*/10 6-13 * * 1-5"),
    _sched("ops_runner_dip",        [ops_runner_reversal_dip],     "*/10 6-13 * * 1-5"),
    _sched("ops_narratives_5min",   [ops_enrich_narratives],     "*/5 * * * *"),
    _sched("ops_breaking_30min",    [ops_breaking_signal],       "*/30 * * * *"),
    _sched("ops_trial_emails_6h",   [ops_trial_emails],          "0 */6 * * *"),
    _sched("ops_morning_weekday",   [ops_monday_paper_monitor],  "30 7 * * 1"),
    _sched("ops_ceowatcher_daily",  [ops_ceowatcher_reader],     "30 8 * * *"),
    _sched("ops_midday_weekday",    [ops_alpaca_reconcile,
                                     ops_thesis_monitor],        "30 13 * * 1-5"),
    _sched("ops_afternoon_weekday", [ops_post_deploy_audit],     "0 14 * * 1-5"),
    _sched("ops_summary_weekday",   [ops_daily_summary],         "30 14 * * 1-5"),
    _sched("ops_evening_daily",     [ops_daily_content,
                                     ops_strategy_health],       "0 17 * * *"),
    _sched("ops_pit_shadow_daily",  [ops_pit_shadow],            "0 18 * * *"),
]
