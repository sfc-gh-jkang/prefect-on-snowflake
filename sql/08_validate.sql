-- =============================================================================
-- 08_validate.sql — Health checks for all Prefect SPCS services
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- Check service status
SELECT
    SERVICE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    COMPUTE_POOL,
    CURRENT_INSTANCES,
    TARGET_INSTANCES
FROM TABLE(INFORMATION_SCHEMA.SERVICES())
WHERE SERVICE_NAME LIKE 'PF_%'
ORDER BY SERVICE_NAME;

-- Check container logs for each service (Postgres is now Snowflake-managed, not an SPCS service)
SELECT SYSTEM$GET_SERVICE_LOGS('PF_REDIS', 0, 'pf-redis', 20);
SELECT SYSTEM$GET_SERVICE_LOGS('PF_SERVER', 0, 'pf-server', 20);
SELECT SYSTEM$GET_SERVICE_LOGS('PF_SERVICES', 0, 'pf-services', 20);
SELECT SYSTEM$GET_SERVICE_LOGS('PF_WORKER', 0, 'pf-worker', 20);

-- Show public endpoint for server (used by hybrid workers + flow deployment)
SHOW ENDPOINTS IN SERVICE PF_SERVER;
