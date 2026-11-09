#!/usr/bin/env bash
# Entry point kept for launchd: com.openclaw.form4-error-tail runs this file.
#
# The tail and its push policy live in api_error_tail.py. Its docstring has the
# history; the short version is that every version of this script pushed per
# ERROR and then tried to cap the flood, and on 2026-11-07..09 the rolling 6/hr
# cap let a chronic low-rate fault push 413 times in one weekend. It now pushes
# per INCIDENT: when 500s start, when a new kind of error appears, a summary
# every 12 hours while they continue, and when they stop.
exec /opt/homebrew/bin/python3 /Users/derekg/trading-framework/scripts/api_error_tail.py "$@"
