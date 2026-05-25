"""Generic wrapper that records a pipeline_runs row around any command.

Used by launchd plists to instrument services without modifying their
source. For services where the underlying script can emit richer metadata
(rows_written, per-strategy results), instrument the Python entry point
directly with `pipeline_run()` instead — that's strictly better.

Usage in a plist's ProgramArguments:
    /opt/homebrew/bin/python3
    -m
    framework.observability.wrap
    <service_name>
    --
    /opt/homebrew/bin/python3
    /path/to/script.py
    --some-arg

CLI is intentionally rigid (`<service_name> -- <argv...>`) to avoid
arg-parsing collisions with the wrapped command.
"""
from __future__ import annotations

import subprocess
import sys

from framework.observability import pipeline_run


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "usage: python -m framework.observability.wrap <service> -- <command...>",
            file=sys.stderr,
        )
        sys.exit(2)

    service = sys.argv[1]
    rest = sys.argv[2:]
    if rest and rest[0] == "--":
        rest = rest[1:]
    if not rest:
        print("error: no command to run", file=sys.stderr)
        sys.exit(2)

    with pipeline_run(service) as prun:
        # Stream subprocess stdout/stderr to our own — launchd has already
        # set up StandardOutPath/StandardErrorPath for the wrapping invocation,
        # so the child's output naturally lands in the same log file.
        result = subprocess.run(rest)
        prun.set_metadata({"argv": rest, "exit_code": result.returncode})
        if result.returncode != 0:
            # pipeline_run's failure path triggers on any unhandled exception;
            # SystemExit with nonzero captures the wrapped command's exit code
            # and marks the run failed in the same code path as a Python
            # exception in an instrumented service.
            sys.exit(result.returncode)


if __name__ == "__main__":
    main()
