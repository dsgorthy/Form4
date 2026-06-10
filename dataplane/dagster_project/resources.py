"""Dagster resources — DB connections for the dataplane.

Resources are dependency-injected into assets. The signal-asset wrapper
asks for `dataplane_conn` and `form4_conn`; this module provides both.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """A thin wrapper around psycopg2 with a context-manager interface.

    Assets receive an instance of this class; call `.connection()` inside a
    with-statement to get a real connection that closes cleanly.
    """

    dsn: str

    @contextmanager
    def connection(self):
        conn = psycopg2.connect(self.dsn)
        try:
            yield conn
        finally:
            conn.close()


# ── Resource definitions consumed by Definitions ────────────────────────

def dataplane_resource() -> PostgresResource:
    """Connection to pyrrho_data_dev (or pyrrho_data_prod via env var override).

    Env var override: PYRRHO_DATAPLANE_DSN. Falls back to local dev.
    """
    dsn = os.environ.get(
        "PYRRHO_DATAPLANE_DSN",
        "dbname=pyrrho_data_dev host=localhost",
    )
    return PostgresResource(dsn=dsn)


def form4_resource() -> PostgresResource:
    """Connection to the form4 database for the phase-1 bridge.

    Drops once insider.trades.raw is in-plane.
    """
    dsn = os.environ.get(
        "FORM4_DSN",
        "dbname=form4 host=localhost",
    )
    return PostgresResource(dsn=dsn)
