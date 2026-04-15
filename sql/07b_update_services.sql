-- =============================================================================
-- 07b_update_services.sql — Update all SPCS services via ALTER SERVICE
--
-- This performs a rolling upgrade of existing services, preserving the
-- ingress URL. Use this for ALL updates after the initial deployment.
--
-- Prerequisites:
--   - Services must already exist (created via 07_create_services.sql)
--   - Updated spec files must be uploaded to @PREFECT_SPECS stage
--   - Updated Docker images must be pushed to the image repository
--
-- WARNING: NEVER use CREATE-OR-REPLACE SERVICE — that drops and recreates
-- the service, which generates a NEW ingress URL and breaks bookmarks,
-- PAT tokens, auth proxies, and GCP worker configurations.
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- 1. Redis (infrastructure — no external access needed)
ALTER SERVICE PF_REDIS
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_redis.yaml';

-- 2. Prefect Server (core — API + UI, public endpoint)
ALTER SERVICE PF_SERVER
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_server.yaml';

-- 3. Prefect Services (core — background scheduler, foreman, etc.)
ALTER SERVICE PF_SERVICES
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_services.yaml';

-- 4. Prefect Worker (worker pool — flow execution, auto-scaled)
--    PREFECT_WORKER_EAI: pypi, gitlab, httpbin egress for flow execution.
--    MONITOR_EGRESS_EAI: Observe collect endpoint egress for the co-located
--      observe-agent sidecar that relays OTLP traces (gRPC localhost:4317).
ALTER SERVICE PF_WORKER
    FROM @PREFECT_SPECS
    SPECIFICATION_FILE = 'pf_worker.yaml';

ALTER SERVICE PF_WORKER SET
    MIN_INSTANCES = 1
    MAX_INSTANCES = 3;

-- 5. Monitoring stack (Prometheus + Grafana + Loki + Observe Agent)
ALTER SERVICE PF_MONITOR
    FROM @MONITOR_STAGE
    SPECIFICATION_FILE = 'specs/pf_monitor.yaml';

ALTER SERVICE PF_MONITOR SET
    EXTERNAL_ACCESS_INTEGRATIONS = (PREFECT_PG_EAI, OBSERVE_INGEST_ACCESS_INTEGRATION);
