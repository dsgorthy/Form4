"""Observability primitives for batch pipelines.

`pipeline_run` is a context manager that records one row in pipeline_runs
per invocation. Used by every scheduled batch job so we get queryable run
history (start, end, status, rows written, error) without standing up a
real orchestrator.

Usage:
    from framework.observability import pipeline_run

    with pipeline_run("strategy_simulator", log_path="logs/strategy-simulator.log") as run:
        ...do work...
        run.set_metadata({"strategy_results": results})
        run.set_rows_written(total)
"""
from framework.observability.pipeline_runner import pipeline_run, RunState

__all__ = ["pipeline_run", "RunState"]
