# SPECIFICATION.md — Prefect 3.x on Snowpark Container Services

## Overview

Deploy a production-grade, self-hosted Prefect 3.x orchestration server on
Snowpark Container Services (SPCS), with hybrid worker support for multi-cloud
execution.

## Goals

1. **Self-hosted Prefect server** running entirely on SPCS (no Prefect Cloud dependency)
2. **External Access Integration** — workers can call external REST APIs
3. **Hybrid orchestration** — SPCS workers + external workers (GCP, laptop) connecting to the same server
4. **Developer-friendly flow deployment** — write flows locally, deploy to SPCS with one command
5. **Production-ready** — separate migration container, Redis messaging, persistent storage, health checks
6. **Fully tested** — 300+ offline tests, local integration tests, E2E SPCS tests

## Architecture

### Services (5 SPCS services + Snowflake Managed Postgres)

| Service | Container | Role | Compute Pool |
|---------|-----------|------|-------------|
| PF_REDIS | `redis:7` | Messaging broker | PREFECT_INFRA_POOL |
| PF_MIGRATE | `prefecthq/prefect:3-python3.12` | One-shot database migration | PREFECT_CORE_POOL |
| PF_SERVER | `prefecthq/prefect:3-python3.12` | API + UI (port 4200, public) | PREFECT_CORE_POOL (CPU_X64_M) |
| PF_SERVICES | `prefecthq/prefect:3-python3.12` | Background services | PREFECT_CORE_POOL |
| PF_WORKER | Custom (prefect + snowflake-connector) | Flow execution | PREFECT_WORKER_POOL (CPU_X64_S) |

**Database**: Snowflake Managed Postgres (PREFECT_PG instance, STANDARD_M) — persistent across service drops.

### Startup Order

```
Snowflake Postgres (always-on) ──→ migrate (completed) → server (healthy) → services + worker
redis (healthy) ────────────────────────────────────────↗
```

### Networking

- **Intra-cluster**: Services communicate via DNS names (`pf-server`, `pf-redis`)
- **Snowflake Postgres**: SPCS services reach `*.postgres.snowflake.app:5432` via PREFECT_PG_EAI
- **Public ingress**: PF_SERVER exposes port 4200 with `public: true` endpoint
- **External egress**: PF_WORKER has EAI for outbound HTTPS to approved hosts
- **Monitoring egress**: PF_MONITOR has EAI for `smtp.gmail.com:587`, `discord.com:443`, Postgres host. Slack egress (`hooks.slack.com:443`, `api.slack.com:443`) is opt-in via `SLACK_ENABLED=true`

### Hybrid Workers

External workers connect to the SPCS server's public endpoint:

```
PREFECT_API_URL=https://<spcs-server-endpoint>/api prefect worker start --pool gcp-pool --type process
```

Work pool routing directs flows to the appropriate worker:
- `spcs-pool` → SPCS worker (runs inside Snowflake)
- `gcp-pool` → GCP worker (runs on external VM)
- `local-pool` → Local compose worker (development only)

### Flow Deployment Model

Three storage options for flow code:

1. **Stage-based** (Snowflake-native): Upload flows to `@PREFECT_FLOWS`, worker mounts stage as volume
2. **Git-based** (team/CI): Workers clone repo at runtime via `GitRepository` storage
3. **Image-baked** (simplest): Flow code built into worker Docker image

## User Stories

### US-1: Local Development
As a developer, I can `docker compose up -d` and have a fully functional Prefect
server + worker running locally, then deploy flows with one command.

### US-2: SPCS Deployment
As an operator, I can run `./scripts/deploy.sh` to set up the entire Prefect
infrastructure on SPCS, including database, networking, compute pools, and services.

### US-3: Flow Deployment to SPCS
As a developer, I can point `PREFECT_API_URL` to my SPCS server and run
`python flows/deploy.py` to register flow deployments from my laptop.

### US-4: External API Access
As a flow author, I can write flows that call external REST APIs, and the SPCS
worker can reach them via External Access Integration.

### US-5: Hybrid Orchestration
As a team lead, I can run Prefect workers on both SPCS and GCP, with the SPCS
server coordinating work across both via work pools.

### US-6: Teardown and Cost Control
As an operator, I can suspend all compute pools with one SQL script to stop
billing, and resume them later without data loss.

## Non-Goals

- Prefect Cloud integration (this is self-hosted only)
- Multi-tenant Prefect server (single deployment per org)
- Automated CI/CD pipeline (manual scripts, CI template provided)
- Snowflake Native App packaging (future consideration)

## Success Criteria

1. `docker compose up -d` starts local stack in < 90 seconds
2. All 6 SPCS services reach healthy state within 5 minutes
3. Example flow completes on SPCS worker
4. External API flow succeeds (proves EAI works)
5. GCP worker connects to SPCS server and completes a flow run
6. 300+ offline tests pass
7. Flow deployment from laptop to SPCS server works in one command
