-- =============================================================================
-- 08b_setup_o4s.sql — Observe for Snowflake (O4S) Native App Setup
--
-- Prerequisites:
--   1. Install "Observe for Snowflake" from Snowflake Marketplace
--   2. Create secrets + network rule + EAI (see below)
--   3. Configure connection in O4S Streamlit UI
--   4. Enable datasets and start tasks in O4S UI
--
-- This file documents the SQL grants needed. The dataset configuration
-- (which ACCOUNT_USAGE views to collect) is done through the O4S Streamlit
-- UI in Snowsight — the app's ADD_ACCOUNT_USAGE_HISTORY/OBJECT procedures
-- use an internal whitelist not accessible via direct SQL.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- 1. O4S configuration database and secrets
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SEND_TO_OBSERVE;
CREATE SCHEMA IF NOT EXISTS SEND_TO_OBSERVE.O4S;

-- Observe ingest token (datastream token from Observe UI)
-- Format: <datastream_id>:<token_value>
CREATE SECRET IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_OBSERVE_TOKEN>';

-- Observe collection endpoint
-- Format: https://<customer_id>.collect.observeinc.com/v1/http/snowflake
CREATE SECRET IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_OBSERVE_ENDPOINT>';

-- ---------------------------------------------------------------------------
-- 2. Network rule + External Access Integration for O4S
-- ---------------------------------------------------------------------------
CREATE NETWORK RULE IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_INGEST_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('<CUSTOMER_ID>.collect.observeinc.com:443');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS OBSERVE_INGEST_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (SEND_TO_OBSERVE.O4S.OBSERVE_INGEST_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (
        SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN,
        SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT
    )
    ENABLED = TRUE;

-- ---------------------------------------------------------------------------
-- 3. Grants to O4S native app
-- ---------------------------------------------------------------------------
GRANT USAGE ON DATABASE SEND_TO_OBSERVE TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT USAGE ON SCHEMA SEND_TO_OBSERVE.O4S TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO APPLICATION OBSERVE_FOR_SNOWFLAKE;

-- O4S needs to create/manage tasks and access ACCOUNT_USAGE
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT USAGE ON DATABASE SNOWFLAKE TO APPLICATION OBSERVE_FOR_SNOWFLAKE;

-- ---------------------------------------------------------------------------
-- 4. Warehouse for O4S tasks (dedicated for visibility)
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS PREFECT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for O4S data collection tasks';

-- Register warehouse with O4S app
CALL OBSERVE_FOR_SNOWFLAKE.CONFIG.REGISTER_SINGLE_REFERENCE(
    'ENT_REF_WAREHOUSE_O4S', 'ADD', 'PREFECT_WH'
);

-- ---------------------------------------------------------------------------
-- 5. Provision the O4S connector
-- ---------------------------------------------------------------------------
CALL OBSERVE_FOR_SNOWFLAKE.PUBLIC.PROVISION_CONNECTOR(
    PARSE_JSON('{
        "observe_token": "SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN",
        "observe_endpoint": "SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT"
    }')
);

-- ---------------------------------------------------------------------------
-- 6. Grant EAI to PREFECT_ROLE for observe-agent sidecar in SPCS
-- ---------------------------------------------------------------------------
GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO ROLE PREFECT_ROLE;

-- ---------------------------------------------------------------------------
-- 7. MANUAL STEP: Open O4S app in Snowsight
--    - Go to: Data Products → Apps → OBSERVE_FOR_SNOWFLAKE
--    - Click "App" tab
--    - Enable all ACCOUNT_USAGE history views (toggle RUNNING = true):
--        QUERY_HISTORY, WAREHOUSE_METERING_HISTORY, WAREHOUSE_LOAD_HISTORY,
--        TASK_HISTORY, LOGIN_HISTORY, METERING_HISTORY, METERING_DAILY_HISTORY,
--        SNOWPARK_CONTAINER_SERVICES_HISTORY, DATA_TRANSFER_HISTORY
--    - Enable ACCOUNT_USAGE object views:
--        DATABASES, SCHEMATA, TABLES, VIEWS, FUNCTIONS, PROCEDURES,
--        STAGES, WAREHOUSES, ROLES, USERS, GRANTS_TO_ROLES, GRANTS_TO_USERS
--    - Add Event Table: snowflake.telemetry.events
--    - Click "Save Changes" then "Start All"
--    - Stagger schedules 1-2 min apart for best performance
-- ---------------------------------------------------------------------------
