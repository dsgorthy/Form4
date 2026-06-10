"""Top-level Dagster Definitions object.

This is the single entry point Dagster discovers (set in pyproject.toml
under [tool.dagster]). It collects all assets and resources.
"""
from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_project.assets.dbt import dataplane_dbt_assets, dbt_project
from dagster_project.assets.signals import build_signal_assets
from dagster_project.resources import (
    dataplane_resource,
    form4_resource,
)


import os
import shutil

# dbt executable must be discoverable; fall through env, then venv.
_DBT_EXECUTABLE = (
    os.environ.get("DBT_EXECUTABLE")
    or shutil.which("dbt")
    or "/Users/derekg/dataplane_venv/bin/dbt"
)

defs = Definitions(
    assets=[*build_signal_assets(), dataplane_dbt_assets],
    resources={
        "dataplane_conn": dataplane_resource(),
        "form4_conn":     form4_resource(),
        "dbt":            DbtCliResource(
            project_dir=str(dbt_project.project_dir),
            dbt_executable=_DBT_EXECUTABLE,
        ),
    },
)
