"""dbt project integration.

Wraps the dbt models in `dbt_project/` as Dagster assets via
`@dbt_assets`. The dbt manifest is generated at parse time; production
deploys regenerate it via `dbt parse`.

Note: no `from __future__ import annotations` — Dagster's decorator
validation inspects live types on the @dbt_assets compute function.
"""
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


# Resolve the dbt project relative to the dataplane package root so the
# same path works on Mini (dev) and Studio (prod).
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,   # profiles.yml lives next to dbt_project.yml
)

# Prepare the manifest at module import time. Idempotent; cheap when warm.
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dataplane_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models in dbt_project/ surface here. Lineage in the Dagster
    UI links each dbt model back to its sources."""
    yield from dbt.cli(["build"], context=context).stream()
