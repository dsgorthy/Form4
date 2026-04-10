# Form4.app Operations Runbook

This is the on-call cheat sheet for Form4.app production. If a Telegram alert
fires, start here.

## Architecture (one paragraph)

Production traffic: Cloudflare Tunnel → Caddy (Docker, port 80) → API container
(Docker, internal port 8000) and Frontend container (Docker, internal port
3000). API talks to PostgreSQL on the host via `host.docker.internal`. Three
containers managed by docker-compose: `trading-framework-api-1`,
`trading-framework-frontend-1`, `trading-framework-caddy-1`. Deploys go via
GitHub Actions self-hosted runner → `deploy/deploy.sh --env prod`.

## Monitoring & Alerts

| Alert source | What it watches | How often | Threshold |
|---|---|---|---|
| `com.openclaw.form4-uptime` | 5 critical endpoints | every 60s | 3 consecutive failures |
| `com.openclaw.form4-error-tail` | API container logs | continuous | 1× per error (deduped 5min) |
| `deploy.sh` smoke test | 14 endpoints, post-deploy | every deploy | any failure |

All alerts go to Telegram chat `8585305446` via bot token in the scripts.

## Health endpoints

- `GET /api/v1/health` — shallow check, exercises a `SELECT 1` against PG. Returns 200 + `db_roundtrip_ms` if OK, 503 if DB unreachable. **This is what monitors should hit.**
- `GET /api/v1/health/deep` — deeper check, runs `SELECT MAX(filing_date) FROM trades`. Slower but catches schema/search_path issues.

## Common alerts and how to respond

### "form4.app DOWN" — uptime monitor

1. Confirm: `curl -i https://form4.app/api/v1/health`
2. Check container status: `docker ps | grep trading-framework`
3. If a container is missing or restarting:
   ```
   docker logs trading-framework-api-1 --tail 100
   docker logs trading-framework-frontend-1 --tail 50
   ```
4. If the API is up but DB unreachable: `psql -h localhost -d form4 -c "SELECT 1"`
5. To restart manually: `docker restart trading-framework-api-1` (or `frontend-1`)
6. To redeploy from scratch: `cd ~/trading-framework && ./deploy/deploy.sh --env prod`

### "form4 API 500" — error tail

1. Note the endpoint from the alert
2. Get the traceback:
   ```
   docker logs trading-framework-api-1 --tail 200 | grep -B 2 -A 25 'Traceback' | tail -60
   ```
3. If it's a known PG-translation issue: extend `config/database.py` compat layer
4. If it's a NULL pointer / connection error: check `psql -h localhost -d form4` is alive, restart api container

### "form4 API Exception" — error tail caught psycopg2 issue

1. Likely DB-side: PG restart, network blip, or connection exhaustion
2. Check active PG connections: `psql -h localhost -d form4 -c "SELECT count(*) FROM pg_stat_activity"`
3. If close to 100: something is leaking — restart API container

## Deploys

Standard deploy:
```bash
cd ~/trading-framework
./deploy/deploy.sh --env prod
```

What it does:
1. Pre-deploy Python syntax check on changed `.py` files
2. `docker compose build` (uses cache when possible)
3. **Rolling restart** — api first, sleep 3, then frontend (no simultaneous downtime)
4. Health check on `/api/v1/health` and `/`
5. **Smoke test** — `scripts/smoke_test.sh` hits 14 endpoints, fails the deploy on any non-200/non-JSON response
6. On smoke failure: Telegram alert + exit non-zero (so CI surfaces it)

CI/CD: pushes to `main` trigger the deploy via the self-hosted runner.

## DB connection model

We use **direct connections, not a pool**. Every API request opens a fresh
`psycopg2.connect()` and closes it on exit.

Why: psycopg2's `ThreadedConnectionPool` caused two outages in 24h on
2026-04-09 — first by handing out connections that had been silently closed by
PG (`InterfaceError: connection already closed`), then by getting deadlocked
with all worker threads waiting on dead pool entries (`connection pointer is
NULL`). The retry-loop fix made it more robust but didn't eliminate the
deadlock. PG has 100 `max_connections` and our real concurrency is ~2-10, so
the ~5ms overhead per request is negligible compared to the reliability gain.

If you're tempted to re-add pooling: don't. Use `psycopg_pool` from psycopg3
or SQLAlchemy with `pool_pre_ping=True` instead — they handle dead connections
correctly.

## Useful commands

```bash
# Status of all monitor jobs
launchctl list | grep openclaw

# Tail uptime log
tail -f ~/trading-framework/logs/uptime.log

# Tail error log
tail -f ~/trading-framework/logs/api-errors.log

# Full health probe
curl -s https://form4.app/api/v1/health/deep | python3 -m json.tool

# Run smoke test manually
./scripts/smoke_test.sh https://form4.app

# Docker status
docker ps --format "table {{.Names}}\t{{.Status}}"

# PG connection count
psql -h localhost -d form4 -c "SELECT count(*), state FROM pg_stat_activity WHERE datname='form4' GROUP BY state"
```

## Files

| Purpose | Path |
|---|---|
| Smoke test | `scripts/smoke_test.sh` |
| Uptime monitor | `scripts/uptime_monitor.sh` |
| Error tail | `scripts/api_error_tail.sh` |
| Deploy script | `deploy/deploy.sh` |
| Uptime launchd | `~/Library/LaunchAgents/com.openclaw.form4-uptime.plist` |
| Error tail launchd | `~/Library/LaunchAgents/com.openclaw.form4-error-tail.plist` |
| Health endpoints | `api/main.py` (`/api/v1/health`, `/api/v1/health/deep`) |
| DB connection | `config/database.py` (`get_db()` — direct, no pool) |
| Logs | `logs/uptime.log`, `logs/api-errors.log`, `logs/deploy-prod-*.log` |
