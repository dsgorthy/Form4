# Pyrrho MCP server — Claude Desktop wiring

The MCP server is `python -m dataplane.mcp_server`. It runs over stdio
and exposes read-only tools over the signal store.

## Tools

| Tool | Use |
|---|---|
| `list_signals` | catalog browse — what signals exist |
| `get_signal(signal_id)` | full metadata + observation rollup + freshness |
| `read_observations(signal_id, ticker?, from?, to?, limit≤1000)` | newest-first rows |
| `read_strategy_evaluations(strategy_id, ticker?, outcome?, from?, to?, limit≤2000)` | strategy eval tape |
| `ticker_snapshot(ticker)` | every signal's latest value for one ticker |
| `fail_reason_distribution(strategy_id, from?, to?)` | strategy tuning histogram |
| `health()` | per-signal freshness vs SLA |

No write / backfill / strategy authoring — those belong in the Pyrrho
Desk workbench. Claude is the reasoning, this server is the retrieval.

## Wiring into Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS). Add under `mcpServers`:

### Option A — running locally on Studio

If Claude Desktop is on Studio (or you've set up tailnet PG access),
use the direct path:

```json
{
  "mcpServers": {
    "pyrrho": {
      "command": "/Users/derekg/dataplane_venv/bin/python",
      "args": ["-m", "dataplane.mcp_server"],
      "cwd": "/Users/derekg/trading-framework/dataplane",
      "env": {
        "PYRRHO_DATAPLANE_DSN": "dbname=pyrrho_data_dev host=localhost"
      }
    }
  }
}
```

### Option B — Claude Desktop on Mini, MCP runs on Studio over SSH

Recommended for the current setup (PG only listens on Studio
localhost, Claude Desktop is on Mini):

```json
{
  "mcpServers": {
    "pyrrho": {
      "command": "ssh",
      "args": [
        "derekg@100.78.9.66",
        "cd /Users/derekg/trading-framework/dataplane && /Users/derekg/dataplane_venv/bin/python -m dataplane.mcp_server"
      ]
    }
  }
}
```

Restart Claude Desktop after editing. The `pyrrho` server should
appear in the MCP indicator. Try: `What does Pyrrho currently know
about NVDA?`

## Wiring into Claude Code

Same shape, but in your `~/.claude.json` or per-project config under
`mcpServers`. Identical fields.

## Smoke test (without Claude)

```
cd /Users/derekg/trading-framework/dataplane
/Users/derekg/dataplane_venv/bin/python -c "
from dataplane.mcp_server import list_signals, ticker_snapshot, health
print(list_signals())
print(ticker_snapshot('NVDA'))
print(health())
"
```

If those return data, the server is healthy.
