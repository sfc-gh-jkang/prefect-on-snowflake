# GCP Hybrid Worker

Runs a Prefect worker on a GCP Compute Engine VM that polls the SPCS-hosted
Prefect server via an nginx auth-proxy sidecar.

## Architecture

```
┌─────────────────────────────────────────────┐
│  GCP VM (Docker Compose)                    │
│                                             │
│  ┌──────────────┐   ┌───────────────────┐   │
│  │  auth-proxy   │──▶│  Prefect Worker   │   │
│  │  (nginx)      │   │  (gcp-pool)       │   │
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

- GCP VM with Docker and Docker Compose installed
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

# Primary worker (gcp-pool)
docker compose -f docker-compose.gcp.yaml up -d

# Backup worker (gcp-pool-backup) — optional, runs alongside primary
docker compose -f docker-compose.gcp-backup.yaml up -d
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.gcp.yaml` | Primary worker polling `gcp-pool` |
| `docker-compose.gcp-backup.yaml` | Backup worker polling `gcp-pool-backup` |
| `Dockerfile.worker` | Shared worker image (symlink to `../aws/Dockerfile.worker`) |
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

## Ports

| Service | Host Port | Container Port |
|---------|-----------|----------------|
| auth-proxy (primary) | 4200 | 4200 |
| auth-proxy (backup) | 4202 | 4200 |

## Automated Setup

For fully automated VM provisioning, see `setup_gcp_worker.sh`:

```bash
./setup_gcp_worker.sh
```

This creates the GCP VM, installs Docker, copies configs, and starts the worker.
