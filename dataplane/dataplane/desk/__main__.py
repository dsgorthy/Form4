"""Entry point: `python -m dataplane.desk`."""
from __future__ import annotations

import argparse

import uvicorn

from dataplane.desk.app import app


def main(argv=None):
    p = argparse.ArgumentParser(prog="dataplane.desk")
    p.add_argument(
        "--host", default="100.78.9.66",
        help="bind address (default: Studio tailnet IP)",
    )
    p.add_argument("--port", type=int, default=3031)
    args = p.parse_args(argv)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
