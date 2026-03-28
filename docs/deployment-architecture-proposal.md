# Deployment Architecture Proposal
## Local → Sandbox → Production Pipeline

**Author:** Claude (Staff Engineer)
**Date:** 2026-03-28
**Status:** PROPOSAL — awaiting Derek's review

---

## Goals

1. Three clean environments: local dev, sandbox, production
2. No live keys ever touch local or sandbox
3. Sandbox mirrors prod closely enough to catch issues before rollout
4. Database isolation with ability to transfer data between environments
5. Scale to 1K users without re-architecting
6. Simple enough that one person can operate it

---

## Architecture Overview

```
LOCAL (your machine)                    MAC MINI / STUDIO
  ├── Next.js dev server (port 3000)    ┌──────────────────────────────┐
  ├── API uvicorn (port 8000)           │  SANDBOX (Docker)            │
  ├── SQLite: dev copy                  │    API: port 8100            │
  └── .env.development (test keys)      │    Frontend: port 3100       │
                                        │    Caddy: sandbox.form4.app  │
                                        │    DB: sandbox/insiders.db   │
                                        │    .env.sandbox              │
                                        ├──────────────────────────────┤
                                        │  PRODUCTION (Docker)         │
                                        │    API: port 8000            │
                                        │    Frontend: port 3000       │
                                        │    Caddy: form4.app          │
                                        │    DB: prod/insiders.db      │
                                        │    .env (live keys)          │
                                        └──────────────────────────────┘
```

---

## Environment Configuration

### Keys & Services

| Service | Local Dev | Sandbox | Production |
|---------|-----------|---------|------------|
| Stripe | Test keys (`sk_test_`) | Test keys | Live keys (`sk_live_`) |
| Clerk | Dev instance | Dev instance | Live instance (`clerk.form4.app`) |
| Alpaca | Paper trading | Paper trading | Paper trading |
| Domain | localhost:3000 | sandbox.form4.app | form4.app |
| CORS | localhost | sandbox.form4.app | form4.app |
| Database | Local copy (optional thin) | Full copy | Production |

### Environment Files

```
.env                    → prod (exists today, live keys)
.env.sandbox            → sandbox (test Stripe, dev Clerk, sandbox DB path)
.env.development        → local dev (test Stripe, dev Clerk, local DB path)
```

Frontend:
```
frontend/.env.local          → local dev (test keys, localhost API)
frontend/.env.sandbox        → sandbox build (test keys, sandbox API URL)
frontend/.env.production     → prod build (live keys, prod API URL)
```

---

## Database Strategy

### The Problem
SQLite doesn't support replication. We have 3 databases totaling ~10GB. Pipelines (EDGAR ingest, price backfill, trade returns) write to the DB continuously.

### The Solution: Copy + Selective Sync

```
strategies/insider_catalog/
  prod/
    insiders.db          ← production (live runner writes here)
    prices.db
    research.db
  sandbox/
    insiders.db          ← sandbox (testing, schema experiments)
    prices.db
    research.db
```

**Read-only tables** (trades, insiders, daily_prices, trade_returns, option_prices):
- These are populated by ingest pipelines, not user actions
- Sandbox gets a weekly snapshot from prod: `cp prod/*.db sandbox/`
- Or: sandbox attaches prod DBs as read-only and only has its own `strategy_portfolio` table

**Write tables** (strategy_portfolio, notifications, api_keys, user preferences):
- These are per-environment and never shared
- Sandbox portfolio can be re-simulated independently

**Schema migrations:**
- Test in sandbox first
- If migration adds columns (additive): safe to run on prod after sandbox validation
- If migration transforms data: run on sandbox copy, validate, then run on prod
- Script: `pipelines/db_migrate.py --env sandbox|prod`

### Data Transfer Commands

```bash
# Snapshot prod → sandbox (weekly, or on-demand)
python3 pipelines/db_sync.py --snapshot prod sandbox

# Transfer specific table from sandbox → prod (after testing)
python3 pipelines/db_sync.py --transfer sandbox prod --table strategy_portfolio

# Re-simulate portfolio in sandbox with latest prod data
python3 pipelines/db_sync.py --snapshot prod sandbox --tables trades,daily_prices,trade_returns
python3 pipelines/portfolio_simulator.py --portfolio form4_insider --env sandbox
```

---

## Docker Compose Setup

### Current (single stack)
```
docker-compose.yml              → base services
docker-compose.prod.yml         → prod overrides
docker-compose.dev.yml          → local dev overrides
```

### Proposed (two stacks on same host)
```
docker-compose.yml              → base services (shared)
docker-compose.prod.yml         → prod: ports 8000/3000, prod DB, live keys
docker-compose.sandbox.yml      → sandbox: ports 8100/3100, sandbox DB, test keys
docker-compose.dev.yml          → local dev (no Docker, just env vars)
```

Both stacks run simultaneously. Caddy routes:
- `form4.app` → prod frontend (port 3000) / prod API (port 8000)
- `sandbox.form4.app` → sandbox frontend (port 3100) / sandbox API (port 8100)

### Deployment Commands

```bash
# Deploy to sandbox (test first)
./deploy/deploy.sh --env sandbox

# Validate sandbox
./deploy/deploy.sh --env sandbox --status

# Promote sandbox → prod
./deploy/deploy.sh --env prod

# Rollback prod
./deploy/deploy.sh --env prod --rollback
```

---

## Deployment Flow

### For feature work:
```
1. Develop locally (npm run dev + uvicorn)
   - Uses .env.development (test keys)
   - Optional: local SQLite copy or connect to sandbox DB

2. Deploy to sandbox
   - ./deploy/deploy.sh --env sandbox
   - Test on sandbox.form4.app
   - Verify: Stripe checkout (test mode), data display, new features

3. Promote to prod
   - ./deploy/deploy.sh --env prod
   - Verify: form4.app health check
   - Monitor for 15 min
```

### For database changes:
```
1. Test migration on sandbox DB
   - python3 pipelines/db_migrate.py --env sandbox
   - Verify data integrity

2. Run migration on prod
   - python3 pipelines/db_migrate.py --env prod
   - API auto-reconnects (WAL mode)

3. If migration fails: restore from snapshot
   - Snapshots taken before every migration automatically
```

### For portfolio/signal changes:
```
1. Re-simulate in sandbox
   - python3 pipelines/portfolio_simulator.py --env sandbox
   - Review results on sandbox.form4.app/portfolio

2. If satisfied, re-simulate in prod
   - python3 pipelines/portfolio_simulator.py --env prod
   - Deploy API if code changed
```

---

## Scaling Considerations (to 1K users)

### Current bottleneck: SQLite
- SQLite handles reads well (WAL mode, multiple readers)
- Writes are serialized — fine for current volume
- At 1K users with real-time notifications, we may hit write contention
- **Migration path:** PostgreSQL when needed (schema is simple, ~2 day migration)
- **When to migrate:** If API p99 latency exceeds 500ms or write queue backs up

### Current bottleneck: Single machine
- Mac mini has 24GB RAM, handles both stacks fine
- Mac Studio (arriving soon) has significantly more RAM
- **Migration path:** Split API and frontend to separate containers, add Redis for caching
- **When to split:** If API memory exceeds 4GB or CPU consistently >70%

### What doesn't need to change yet:
- Docker Compose (works fine under 1K users)
- SQLite (reads scale well, writes are low-frequency)
- Single-machine deployment (Mac Studio has plenty of headroom)
- Cloudflare tunnel (handles traffic, provides DDoS protection)

---

## Implementation Plan

### Phase 1: Environment separation (2-3 hours)
1. Create `.env.development` and `.env.sandbox` with test keys
2. Create `frontend/.env.development` and `frontend/.env.sandbox`
3. Update `deploy/deploy.sh` to accept `--env` flag
4. Create `docker-compose.sandbox.yml`

### Phase 2: Database separation (1-2 hours)
1. Create `strategies/insider_catalog/sandbox/` directory
2. Snapshot prod DBs to sandbox
3. Update API `db.py` to read DB path from env
4. Create `pipelines/db_sync.py` for snapshot/transfer commands

### Phase 3: Caddy routing (30 min)
1. Add `sandbox.form4.app` to Caddy config
2. Add DNS record for sandbox subdomain (Cloudflare)
3. Test routing

### Phase 4: Documentation (30 min)
1. Update README with deployment flow
2. Add `docs/environments.md` with all the details
3. Update CLAUDE.md with new commands

**Total estimate: 4-6 hours**

---

## What I'd skip for now:
- CI/CD automation (manual deploys are fine at this scale)
- Automated testing in sandbox (manual visual QA is faster)
- Database replication (snapshots are sufficient)
- Kubernetes / container orchestration (Docker Compose is fine under 1K)
- Monitoring/alerting stack (health check endpoint + Telegram is enough)

---

## Open Questions for Derek:
1. Do you want sandbox accessible from your phone (public URL) or just from the Mac?
2. Should sandbox run the same launchd pipelines (EDGAR ingest, portfolio runner)?
3. Do you want a separate Clerk dev instance for sandbox, or share the prod Clerk?
4. Budget for the virtual mailbox + any additional domains?
