# CONSTITUTION.md — Governing Principles

This document establishes the non-negotiable principles and development standards
for the Prefect SPCS project. All implementation decisions must align with these
principles.

## 1. Architecture Principles

### 1.1 Compose-First Development
All SPCS service architectures MUST be prototyped and validated locally using
`docker-compose.yaml` before translating to SPCS service specs. The local compose
stack is the canonical source of truth for container relationships, environment
variables, and health checks.

### 1.2 One-Shot Migration Pattern
Database schema migrations MUST run as a separate one-shot container
(`EXECUTE JOB SERVICE` on SPCS, `service_completed_successfully` in Compose)
rather than embedded in application startup. Set
`PREFECT_API_DATABASE_MIGRATE_ON_START=false` on all non-migration containers.

### 1.3 Service Separation
The Prefect server MUST be split into two services:
- **pf-server**: `prefect server start --host 0.0.0.0 --no-services` (API + UI)
- **pf-services**: `prefect server services start` (background schedulers)

This allows independent scaling and restart isolation.

### 1.4 Redis Messaging
Production deployments MUST use Redis as the messaging broker. SQLite-based
messaging is only acceptable for single-node development. Set:
```
PREFECT_MESSAGING_BROKER=prefect_redis.messaging
PREFECT_MESSAGING_CACHE=prefect_redis.messaging
```

## 2. SPCS Gotchas (13 Rules)

These are hard-won lessons from the Airflow SPCS project. Violations cause
silent failures.

1. **DNS hyphenation**: SPCS service names use underscores (`PF_SERVER`) but
   DNS hostnames use hyphens (`pf-server`). Always reference other services
   by their hyphenated DNS name in env vars and configs.

2. **HTTP-only readiness probes**: SPCS readiness probes only support HTTP.
   For TCP-only services (PostgreSQL, Redis), use `path: /` which returns
   a connection-refused vs. timeout distinction.

3. **Stage volume UID/GID**: Stage volumes mount as `nobody:nogroup` by
   default. Specify `uid` and `gid` in the volume spec to match the
   container's user.

4. **BLOCK storage DROP FORCE**: Block volumes persist across service restarts.
   Use `DROP SERVICE ... FORCE` to release them if stuck.

5. **EXECUTE JOB SERVICE**: One-shot containers MUST use `EXECUTE JOB SERVICE`
   instead of `CREATE SERVICE`. The container runs once and exits.

6. **PGDATA subdirectory**: When using BLOCK storage for PostgreSQL, set
   `PGDATA=/var/lib/postgresql/data/pgdata` (subdirectory) because the BLOCK
   volume mount creates a lost+found directory at the mount root.

7. **Secret injection syntax**: Use `{{secret.<name>.secret_string}}` in spec
   YAML. No quotes around the template expression in the env value.

8. **Image registry path**: Images must be pushed to the full registry path:
   `<account>.registry.snowflakecomputing.com/<db>/<schema>/<repo>/<image>:<tag>`

9. **Public endpoints**: Services requiring external access must declare
   `endpoints: [{name: ..., port: ..., public: true}]` in the spec.

10. **EAI attachment**: External Access Integrations are attached at service
    creation: `EXTERNAL_ACCESS_INTEGRATIONS = (...)` in `CREATE SERVICE`.

11. **SYSTEM$WAIT_FOR_SERVICES**: Use this to sequence service creation when
    one service depends on another being healthy.

12. **Compute pool scaling**: Set `MIN_NODES = MAX_NODES = 1` for stateful
    services (postgres, redis). Only stateless services should auto-scale.

13. **No internet by default**: SPCS containers have no outbound internet.
    ALL external network access requires an EAI with explicit host allowlists.

## 3. Testing Standards

### 3.1 Three-Tier Pyramid
- **Offline tests** (default): No Docker, no network, no SPCS. Run in CI.
  Parse YAML, validate SQL, check cross-file consistency.
- **Local tests** (`@local`): Require `docker compose up`. Validate service
  health, flow execution, end-to-end local stack.
- **E2E tests** (`@e2e`): Require live SPCS cluster. Validate real deployment,
  EAI connectivity, hybrid worker orchestration.

### 3.2 Drift Detection
Tests MUST enforce 3-axis consistency:
- **Horizontal**: All SPCS specs use the same image tags, secret names, env vars
- **Vertical**: Compose ↔ SPCS specs match on images, ports, env vars
- **Infrastructure**: Scripts reference the same objects as SQL and specs

### 3.3 Target: 300+ Offline Tests
Offline tests are cheap. Every parseable file should have validation tests.

## 4. Security

- Secrets MUST use Snowflake SECRET objects on SPCS and `.env` files locally.
- `sql/03_setup_secrets.sql` is NEVER committed. Only the `.template` is tracked.
- Network egress MUST be explicitly allowed per-host via EAI.
- The Prefect server public endpoint enables hybrid workers. Understand that
  this endpoint is accessible to anyone with the URL.

## 5. Development Workflow

1. Write/edit flows in `flows/` directory
2. Test locally: `docker compose up -d`, then `PREFECT_API_URL=http://localhost:4200/api python flows/deploy.py`
3. Run flow manually from Prefect UI at http://localhost:4200
4. Deploy to SPCS: `PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py`
5. SPCS worker picks up deployment, executes flow

## 6. Spec-Kit Workflow

This project uses [spec-kit](https://github.com/github/spec-kit) for
spec-driven development. The workflow is:
1. `/speckit.constitution` → This document
2. `/speckit.specify` → `SPECIFICATION.md`
3. `/speckit.plan` → Technical implementation plan
4. `/speckit.tasks` → Actionable task breakdown
5. `/speckit.implement` → Execute tasks
6. `/speckit.analyze` → Post-implementation validation
