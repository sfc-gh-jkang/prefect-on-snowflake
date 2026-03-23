# Azure Hybrid Worker

Runs a Prefect worker on an Azure VM that polls the SPCS-hosted
Prefect server via an nginx auth-proxy sidecar.

## Architecture

```
┌─────────────────────────────────────────────┐
│  Azure VM (Docker Compose)                  │
│                                             │
│  ┌──────────────┐   ┌───────────────────┐   │
│  │  auth-proxy   │──▶│  Prefect Worker   │   │
│  │  (nginx)      │   │  (azure-pool)     │   │
│  │  :4200        │   │                   │   │
│  └──────┬───────┘   └───────────────────┘   │
│         │                                   │
└─────────┼───────────────────────────────────┘
          │ HTTPS + Snowflake PAT
          ▼
    SPCS Prefect Server
```

The auth-proxy injects `Authorization: Snowflake Token="<PAT>"` into every
request so the Prefect worker talks to the SPCS endpoint transparently.

Flow code is delivered via `git_clone` pull step — no bind mounts needed.

## Prerequisites

- Azure VM with Docker and Docker Compose installed
- Snowflake Programmatic Access Token (PAT) for `PREFECT_SVC` service user
- Git access token for the private repo
- Network access to the SPCS public endpoint

## Quick Start

```bash
# Set required env vars
export SNOWFLAKE_PAT="eyJraWQ..."
export SPCS_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
export GIT_ACCESS_TOKEN="glpat-xxxx"
export SNOWFLAKE_ACCOUNT="ORGNAME-ACCTNAME"
export SNOWFLAKE_USER="PREFECT_SVC"

# Primary worker (azure-pool)
docker compose -f docker-compose.azure.yaml up -d

# Backup worker (azure-pool-backup) — optional, runs alongside primary
docker compose -f docker-compose.azure-backup.yaml up -d
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.azure.yaml` | Primary worker polling `azure-pool` |
| `docker-compose.azure-backup.yaml` | Backup worker polling `azure-pool-backup` |
| `Dockerfile.worker` | Shared worker image |
| `nginx.conf` | Auth-proxy config template |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SNOWFLAKE_PAT` | Yes | Snowflake PAT for SPCS auth |
| `SPCS_ENDPOINT` | Yes | SPCS hostname (no `https://`) |
| `GIT_ACCESS_TOKEN` | Yes | Token for private git repo |
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `GIT_REPO_URL` | No | Git repo URL (defaults to project repo) |
| `GIT_BRANCH` | No | Git branch (defaults to `main`) |
| `SNOWFLAKE_PASSWORD` | No | Alternative to PAT for Snowflake flows |
| `SNOWFLAKE_WAREHOUSE` | No | Warehouse (defaults to `COMPUTE_WH`) |
| `SNOWFLAKE_DATABASE` | No | Database (defaults to `PREFECT_DB`) |
| `SNOWFLAKE_SCHEMA` | No | Schema (defaults to `PREFECT_SCHEMA`) |

## Health Checks

Both workers run with `--with-healthcheck`, which starts a webserver on port
8080 inside the container. Docker polls `http://127.0.0.1:8080/health` every
30 seconds. The check treats any HTTP response (including 503 "busy") as
healthy, but connection refused or timeout (hung process) as unhealthy. After
3 consecutive failures Docker restarts the container automatically via
`restart: unless-stopped`.

## Ports

| Service | Host Port | Container Port |
|---------|-----------|----------------|
| auth-proxy (primary) | 4204 | 4200 |
| auth-proxy (backup) | 4205 | 4200 |
