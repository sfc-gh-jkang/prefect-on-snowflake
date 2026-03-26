-- =============================================================================
-- 08b_setup_o4s.sql — Observe for Snowflake (O4S) Native App Setup
--
-- Steps:
--   1. Install "Observe for Snowflake" from Snowflake Marketplace
--   2. Run this SQL script (all steps below)
--   3. Open O4S Streamlit UI in Snowsight to enable datasets (Step 9)
--
-- Reference: https://docs.observeinc.com/docs/configure-the-observe-for-snowflake-app
-- Observe-side: https://docs.observeinc.com/docs/prepare-observe-to-receive-data-from-snowflake
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- 1. O4S configuration database and secrets
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SEND_TO_OBSERVE;
CREATE SCHEMA IF NOT EXISTS SEND_TO_OBSERVE.O4S;

-- Observe APP INGEST token (from Snowflake app → Connections tab in Observe UI)
-- Format: <datastream_id>:<token_value>
-- WARNING: Must be an app ingest token, NOT a generic datastream token or API token.
CREATE SECRET IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_OBSERVE_TOKEN>';

-- Observe collection endpoint
-- Format: <customer_id>.collect.observeinc.com
CREATE SECRET IF NOT EXISTS SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_OBSERVE_ENDPOINT>';

-- ---------------------------------------------------------------------------
-- 2. Network rule + External Access Integration
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
-- 3. Enable telemetry event sharing
-- ---------------------------------------------------------------------------
ALTER APPLICATION OBSERVE_FOR_SNOWFLAKE SET AUTHORIZE_TELEMETRY_EVENT_SHARING = TRUE;
ALTER APPLICATION OBSERVE_FOR_SNOWFLAKE SET SHARED TELEMETRY EVENTS ('SNOWFLAKE$ALL');

-- ---------------------------------------------------------------------------
-- 4. Grant ACCOUNT_USAGE access and task execution
--    (Without these, the O4S procedures fail with "unsupported view"
--     and tasks cannot be created.)
-- ---------------------------------------------------------------------------
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;

-- ---------------------------------------------------------------------------
-- 5. Grant secrets and EAI to O4S native app
-- ---------------------------------------------------------------------------
GRANT USAGE ON DATABASE SEND_TO_OBSERVE TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT USAGE ON SCHEMA SEND_TO_OBSERVE.O4S TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT READ ON SECRET SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT TO APPLICATION OBSERVE_FOR_SNOWFLAKE;
GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO APPLICATION OBSERVE_FOR_SNOWFLAKE;

-- ---------------------------------------------------------------------------
-- 6. Grant event table access (for SNOWFLAKE.TELEMETRY.EVENTS)
-- ---------------------------------------------------------------------------
GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_ADMIN TO APPLICATION OBSERVE_FOR_SNOWFLAKE;

-- ---------------------------------------------------------------------------
-- 7. Warehouse for O4S tasks
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS PREFECT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for O4S data collection tasks';

-- Register warehouse with O4S app (must USE APPLICATION first)
CALL OBSERVE_FOR_SNOWFLAKE.CONFIG.REGISTER_SINGLE_REFERENCE(
    'warehouse', 'ADD', SYSTEM$REFERENCE('WAREHOUSE', 'PREFECT_WH', 'PERSISTENT', 'USAGE'));

-- ---------------------------------------------------------------------------
-- 8. Provision the O4S connector
-- ---------------------------------------------------------------------------
CALL OBSERVE_FOR_SNOWFLAKE.PUBLIC.PROVISION_CONNECTOR(
    PARSE_JSON('{
        "observe_token": "SEND_TO_OBSERVE.O4S.OBSERVE_TOKEN",
        "observe_endpoint": "SEND_TO_OBSERVE.O4S.OBSERVE_ENDPOINT",
        "external_access_integration": "OBSERVE_INGEST_ACCESS_INTEGRATION"
    }')
);

-- ---------------------------------------------------------------------------
-- 8b. Grant EAI to PREFECT_ROLE for observe-agent sidecar in SPCS
-- ---------------------------------------------------------------------------
GRANT USAGE ON INTEGRATION OBSERVE_INGEST_ACCESS_INTEGRATION TO ROLE PREFECT_ROLE;

-- ---------------------------------------------------------------------------
-- 9. MANUAL STEP: Open O4S Streamlit UI in Snowsight
--    Navigate: Data Products → Apps → OBSERVE_FOR_SNOWFLAKE → App tab
--
--    a) History Views — click "+ Add View" and add:
--       QUERY_HISTORY, WAREHOUSE_METERING_HISTORY, METERING_HISTORY,
--       LOGIN_HISTORY, SNOWPARK_CONTAINER_SERVICES_HISTORY, TASK_HISTORY
--
--    b) Event Tables — click "+ Add" and add:
--       snowflake.telemetry.events
--
--    c) Click "Save Changes" then "Start All"
--       Stagger schedules 1-2 min apart for best performance.
--
--    The ADD_ACCOUNT_USAGE_HISTORY procedure uses an internal whitelist
--    only accessible through the Streamlit UI dropdown — direct SQL calls
--    with view names will fail with "unsupported view".
-- ---------------------------------------------------------------------------
