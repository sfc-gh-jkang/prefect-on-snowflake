-- =============================================================================
-- 05_setup_compute_pools.sql — Compute pools for Prefect SPCS services
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- Infrastructure pool: postgres + redis
CREATE COMPUTE POOL IF NOT EXISTS PREFECT_INFRA_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_S
    COMMENT = 'Prefect infrastructure: PostgreSQL and Redis';

-- Core pool: prefect-server + prefect-services
CREATE COMPUTE POOL IF NOT EXISTS PREFECT_CORE_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_M
    COMMENT = 'Prefect core: API server and background services';

-- Worker pool: prefect-worker (scales independently)
CREATE COMPUTE POOL IF NOT EXISTS PREFECT_WORKER_POOL
    MIN_NODES = 1
    MAX_NODES = 3
    INSTANCE_FAMILY = CPU_X64_S
    COMMENT = 'Prefect worker: flow execution';

-- Dashboard pool: observability dashboard (isolated from monitored services)
CREATE COMPUTE POOL IF NOT EXISTS PREFECT_DASHBOARD_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    COMMENT = 'Observability dashboard: isolated from Prefect workload pools';

GRANT USAGE, MONITOR ON COMPUTE POOL PREFECT_INFRA_POOL TO ROLE PREFECT_ROLE;
GRANT USAGE, MONITOR ON COMPUTE POOL PREFECT_CORE_POOL TO ROLE PREFECT_ROLE;
GRANT USAGE, MONITOR ON COMPUTE POOL PREFECT_WORKER_POOL TO ROLE PREFECT_ROLE;
GRANT USAGE, MONITOR ON COMPUTE POOL PREFECT_DASHBOARD_POOL TO ROLE PREFECT_ROLE;
