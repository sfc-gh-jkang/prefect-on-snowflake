-- =============================================================================
-- 02_setup_stages.sql — Create stages for flow code and configuration
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

-- Stage for Prefect flow code (mounted into worker container)
CREATE STAGE IF NOT EXISTS PREFECT_FLOWS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Prefect flow code uploaded from local development';

-- Stage for SPCS spec YAML files
CREATE STAGE IF NOT EXISTS PREFECT_SPECS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'SPCS service specification YAML files';

-- Stage for SPCS Observability Dashboard (Streamlit in Snowflake)
CREATE STAGE IF NOT EXISTS PREFECT_DASHBOARD
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Streamlit dashboard source files for SPCS observability';
