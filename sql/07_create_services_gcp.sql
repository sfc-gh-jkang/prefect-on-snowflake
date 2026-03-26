-- =============================================================================
-- 07_create_services_gcp.sql — Create all SPCS services for Prefect (GCP variant)
--
-- GCP does not have Snowflake Managed Postgres, so this script creates a
-- postgres:16 container (PF_POSTGRES) as an SPCS service. The DB connection URL
-- uses pf-postgres:5432 instead of the Managed Postgres endpoint.
--
-- IMPORTANT: pf_postgres.yaml mounts block storage at /pgdata (NOT /var/lib/
-- postgresql/data). Block storage persists across DROP SERVICE on the same
-- compute pool, and POSTGRES_PASSWORD is only used during initial initdb.
-- If you need to rotate the password, you must use a fresh mount path.
--
-- For AWS/Azure, use 07_create_services.sql instead.
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- Upload specs and flows to stages first:
--   snow stage copy specs/ @PREFECT_SPECS/ --overwrite --connection <conn>

-- 1. Redis (infrastructure — TCP endpoint required for DNS resolution)
CREATE SERVICE IF NOT EXISTS PF_REDIS
    IN COMPUTE POOL PREFECT_INFRA_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_redis.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 2. Postgres container (GCP only — replaces Snowflake Managed Postgres)
CREATE SERVICE IF NOT EXISTS PF_POSTGRES
    IN COMPUTE POOL PREFECT_INFRA_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_postgres.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- Wait for both infrastructure services
CALL SYSTEM$WAIT_FOR_SERVICES(300, 'PREFECT_DB.PREFECT_SCHEMA.PF_REDIS', 'PREFECT_DB.PREFECT_SCHEMA.PF_POSTGRES');

-- 3. Run database migration against the containerized Postgres
EXECUTE JOB SERVICE
    IN COMPUTE POOL PREFECT_CORE_POOL
    NAME = PF_MIGRATE
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_migrate.yaml';

-- 4. Prefect Server (core — API + UI, public endpoint)
CREATE SERVICE IF NOT EXISTS PF_SERVER
    IN COMPUTE POOL PREFECT_CORE_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_server.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- Wait for server to be healthy before starting dependents
CALL SYSTEM$WAIT_FOR_SERVICES(300, 'PREFECT_DB.PREFECT_SCHEMA.PF_SERVER');

-- 5. Prefect Services (core — background scheduler, foreman, etc.)
CREATE SERVICE IF NOT EXISTS PF_SERVICES
    IN COMPUTE POOL PREFECT_CORE_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_services.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 6. Prefect Worker (worker pool — flow execution)
--    PREFECT_MONITOR_EAI provides access to Observe's collect endpoint for
--    direct OTLP trace export (cross-service OTLP does not work in SPCS).
CREATE SERVICE IF NOT EXISTS PF_WORKER
    IN COMPUTE POOL PREFECT_WORKER_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_worker.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 3
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_WORKER_EAI, PREFECT_MONITOR_EAI);

-- 7. Deploy flows (one-shot job — registers deployments with server)
CALL SYSTEM$WAIT_FOR_SERVICES(120, 'PREFECT_DB.PREFECT_SCHEMA.PF_SERVICES', 'PREFECT_DB.PREFECT_SCHEMA.PF_WORKER');

EXECUTE JOB SERVICE
    IN COMPUTE POOL PREFECT_CORE_POOL
    NAME = PF_DEPLOY_JOB
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_deploy_job.yaml';

-- 8. Monitoring stack (Prometheus + Grafana + Loki + exporters)
--    GCP uses internal PF_POSTGRES (no SSL, no EAI needed).
--    The GRAFANA_DB_DSN secret uses sslmode=disable for GCP.
CREATE SERVICE IF NOT EXISTS PF_MONITOR
    IN COMPUTE POOL PREFECT_MONITOR_POOL
    FROM @MONITOR_STAGE
    SPECIFICATION_FILE = 'specs/pf_monitor.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- Show all services
SHOW SERVICES IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;
