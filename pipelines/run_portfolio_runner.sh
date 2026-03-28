#!/usr/bin/env bash
# Wrapper script for portfolio_runner.py — sources .env for Alpaca credentials
cd /Users/openclaw/trading-framework

# Source environment variables (Alpaca keys, Clerk, etc.)
# Use set +e because .env may have lines that fail in strict mode
set -a
source .env 2>/dev/null
set +a

exec /usr/bin/python3 pipelines/portfolio_runner.py "$@"
