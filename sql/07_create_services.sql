-- =============================================================================
-- 07_create_services.sql — Create all SPCS services for Prefect
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- Upload specs and flows to stages first:
--   snow stage copy specs/ @PREFECT_SPECS/ --overwrite --connection <conn>
--   PUT 'file:///path/to/flows/*.py' @PREFECT_FLOWS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- 1. Redis (infrastructure — TCP endpoint required for DNS resolution)
CREATE SERVICE IF NOT EXISTS PF_REDIS
    IN COMPUTE POOL PREFECT_INFRA_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_redis.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1;

-- 2. Wait for Redis, then run database migration against Snowflake Postgres (one-shot job)
--    Postgres is now managed by Snowflake Postgres (PREFECT_PG instance), not an SPCS container.
CALL SYSTEM$WAIT_FOR_SERVICES(300, 'PREFECT_DB.PREFECT_SCHEMA.PF_REDIS');

EXECUTE JOB SERVICE
    IN COMPUTE POOL PREFECT_CORE_POOL
    NAME = PF_MIGRATE
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_PG_EAI)
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_migrate.yaml';

-- 3. Prefect Server (core — API + UI, public endpoint)
CREATE SERVICE IF NOT EXISTS PF_SERVER
    IN COMPUTE POOL PREFECT_CORE_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_server.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_PG_EAI);

-- Wait for server to be healthy before starting dependents
CALL SYSTEM$WAIT_FOR_SERVICES(300, 'PREFECT_DB.PREFECT_SCHEMA.PF_SERVER');

-- 4. Prefect Services (core — background scheduler, foreman, etc.)
CREATE SERVICE IF NOT EXISTS PF_SERVICES
    IN COMPUTE POOL PREFECT_CORE_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_services.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_PG_EAI);

-- 5. Prefect Worker (worker pool — flow execution, stage-mounted flows)
--    MAX_INSTANCES = 3 enables auto-scaling: SPCS adds worker instances
--    as flow concurrency demands. PREFECT_WORKER_POOL max_nodes must be >= 3.
CREATE SERVICE IF NOT EXISTS PF_WORKER
    IN COMPUTE POOL PREFECT_WORKER_POOL
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_worker.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 3
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_WORKER_EAI);

-- 6. Deploy flows (one-shot job — registers deployments with server)
CALL SYSTEM$WAIT_FOR_SERVICES(120, 'PREFECT_DB.PREFECT_SCHEMA.PF_SERVICES', 'PREFECT_DB.PREFECT_SCHEMA.PF_WORKER');

EXECUTE JOB SERVICE
    IN COMPUTE POOL PREFECT_CORE_POOL
    NAME = PF_DEPLOY_JOB
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_deploy_job.yaml';

-- 7. Monitoring stack (Prometheus + Grafana + Loki + exporters + Observe Agent)
--    Requires PREFECT_PG_EAI to reach Snowflake Managed Postgres for
--    postgres-exporter metrics and Grafana's persistent database.
--    Requires OBSERVE_INGEST_ACCESS_INTEGRATION for observe-agent to
--    forward OTLP traces/metrics to Observe's ingest endpoint.
CREATE SERVICE IF NOT EXISTS PF_MONITOR
    IN COMPUTE POOL PREFECT_MONITOR_POOL
    FROM @MONITOR_STAGE
    SPECIFICATION_FILE = 'specs/pf_monitor.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_PG_EAI, OBSERVE_INGEST_ACCESS_INTEGRATION);

-- Show all services
SHOW SERVICES IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;
