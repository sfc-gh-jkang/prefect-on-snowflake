-- =============================================================================
-- 04_setup_networking.sql — External Access Integration for SPCS worker
--
-- Uses CREATE ... IF NOT EXISTS to avoid disrupting running services on re-run.
-- To update an existing rule/EAI, use ALTER or DROP + CREATE manually.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- Network rule: allow worker to reach external APIs
CREATE NETWORK RULE IF NOT EXISTS PREFECT_DB.PREFECT_SCHEMA.PREFECT_WORKER_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'httpbin.org:443',              -- EAI demo flow target
        'pypi.org:443',                 -- pip install at runtime (if needed)
        'files.pythonhosted.org:443',   -- pip package downloads
        'github.com:443'                -- git_clone pull step (flow code) — change to your git host
    );

-- External Access Integration for worker outbound network
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS PREFECT_WORKER_EAI
    ALLOWED_NETWORK_RULES = (PREFECT_DB.PREFECT_SCHEMA.PREFECT_WORKER_EGRESS_RULE)
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION PREFECT_WORKER_EAI TO ROLE PREFECT_ROLE;

-- ---------------------------------------------------------------------------
-- Dashboard (Streamlit in Snowflake) — separate EAI for pip + SPCS API access
-- ---------------------------------------------------------------------------
CREATE NETWORK RULE IF NOT EXISTS PREFECT_DB.PREFECT_SCHEMA.DASHBOARD_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'pypi.org:443',                -- pip install at container runtime startup
        'files.pythonhosted.org:443'   -- pip package downloads
    );

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS PREFECT_DASHBOARD_EAI
    ALLOWED_NETWORK_RULES = (PREFECT_DB.PREFECT_SCHEMA.DASHBOARD_EGRESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (PREFECT_DB.PREFECT_SCHEMA.PREFECT_SVC_PAT)
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION PREFECT_DASHBOARD_EAI TO ROLE PREFECT_ROLE;

-- ---------------------------------------------------------------------------
-- Observe Agent — EAI for OTLP trace/metric egress to Observe ingest endpoint
-- ---------------------------------------------------------------------------
-- The network rule and EAI are created in SEND_TO_OBSERVE.O4S by the O4S
-- setup (see monitoring/o4s/ docs). Here we just grant usage to PREFECT_ROLE
-- so PF_MONITOR can reference OBSERVE_INGEST_ACCESS_INTEGRATION.
GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO ROLE PREFECT_ROLE;
