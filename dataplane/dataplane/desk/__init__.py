"""Pyrrho Dataplane Desk — operator workbench (Starlette + Jinja2 + HTMX).

Three modes:
  - Monitor   /                — current status, healthy pipelines, alerts
  - Understand /signals, /strategies, /ticker/<sym>
  - Author    /new/strategy    (Phase B)

Run: ``python -m dataplane.desk --host 100.78.9.66 --port 3031``
"""
