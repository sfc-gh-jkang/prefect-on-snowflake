-- =============================================================================
-- 11_setup_watchdog.sql — SPCS PF_WORKER auto-recovery watchdog
--
-- Mirrors the VM-side systemd watchdog (workers/gcp + workers/aws) to close
-- the same failure mode on the SPCS side:
--
--   Problem:  `ALTER SERVICE ... SUSPEND` keeps a service suspended until
--             explicitly resumed. `AUTO_RESUME = TRUE` only triggers resume
--             on inbound endpoint traffic — for an outbound-polling worker
--             there is no such trigger. So if someone (or a bad deploy)
--             suspends PF_WORKER, the gcp-pool / aws-pool / spcs-pool
--             equivalent goes dark until manually resumed.
--
--   Fix:      A Snowflake task runs every 2 minutes, checks PF_WORKER status,
--             and issues RESUME if it finds the service SUSPENDED. Idempotent
--             — no-op when service is already RUNNING.
--
-- Requires ACCOUNTADMIN to CREATE TASK + EXECUTE TASK grants (handled below).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- -----------------------------------------------------------------------------
-- Stored procedure: check PF_WORKER status and resume if suspended
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PF_WORKER_WATCHDOG()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    svc_status STRING;
    res STRING DEFAULT 'noop';
BEGIN
    SHOW SERVICES LIKE 'PF_WORKER' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;
    SELECT "status" INTO :svc_status
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        LIMIT 1;

    IF (svc_status = 'SUSPENDED') THEN
        ALTER SERVICE PREFECT_DB.PREFECT_SCHEMA.PF_WORKER RESUME;
        res := 'resumed';
    ELSEIF (svc_status = 'RUNNING') THEN
        res := 'running';
    ELSE
        res := 'unexpected:' || COALESCE(svc_status, 'null');
    END IF;

    RETURN res;
END;
$$;

-- -----------------------------------------------------------------------------
-- Task: run every 2 minutes
-- -----------------------------------------------------------------------------
-- NOTE: SNOWADHOC is DQL-only — use COMPUTE_WH (or any warehouse the
-- PREFECT_ROLE owner can use). COMPUTE_WH is already in use by the rest of
-- the Prefect stack.
CREATE OR REPLACE TASK PF_WORKER_WATCHDOG_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 MINUTE'
    COMMENT = 'Auto-resume PF_WORKER if suspended. Mirrors the VM systemd watchdog on gcp/aws worker pools.'
AS
    CALL PF_WORKER_WATCHDOG();

-- Grant and start
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;
ALTER TASK PF_WORKER_WATCHDOG_TASK RESUME;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SHOW TASKS LIKE 'PF_WORKER_WATCHDOG_TASK' IN SCHEMA PREFECT_DB.PREFECT_SCHEMA;
CALL PF_WORKER_WATCHDOG();
