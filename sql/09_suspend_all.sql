-- =============================================================================
-- 09_suspend_all.sql — Suspend all compute pools (cost savings)
-- =============================================================================
USE ROLE ACCOUNTADMIN;

ALTER COMPUTE POOL PREFECT_WORKER_POOL SUSPEND;
ALTER COMPUTE POOL PREFECT_CORE_POOL SUSPEND;
ALTER COMPUTE POOL PREFECT_INFRA_POOL SUSPEND;
ALTER COMPUTE POOL PREFECT_DASHBOARD_POOL SUSPEND;
