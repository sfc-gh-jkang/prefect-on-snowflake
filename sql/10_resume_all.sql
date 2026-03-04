-- =============================================================================
-- 10_resume_all.sql — Resume all compute pools
-- =============================================================================
USE ROLE ACCOUNTADMIN;

ALTER COMPUTE POOL PREFECT_INFRA_POOL RESUME;
ALTER COMPUTE POOL PREFECT_CORE_POOL RESUME;
ALTER COMPUTE POOL PREFECT_WORKER_POOL RESUME;
ALTER COMPUTE POOL PREFECT_DASHBOARD_POOL RESUME;
