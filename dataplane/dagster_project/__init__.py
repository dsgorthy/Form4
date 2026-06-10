"""Dagster project for the Pyrrho data plane.

The single Definitions object lives in dagster_project.definitions. It
auto-discovers Signal subclasses from the signals/ package and wraps each
as a Dagster asset, plus pulls in the dbt project's models as additional
assets via dagster-dbt.

Run locally:
    cd dataplane
    source /Users/derekg/dataplane_venv/bin/activate
    DAGSTER_HOME=/Users/derekg/dataplane_dagster_home dagster dev
"""
