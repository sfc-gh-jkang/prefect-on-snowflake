-- =============================================================================
-- 06_setup_image_repo.sql — Image repository for SPCS container images
-- =============================================================================
USE ROLE PREFECT_ROLE;
USE DATABASE PREFECT_DB;
USE SCHEMA PREFECT_SCHEMA;

CREATE IMAGE REPOSITORY IF NOT EXISTS PREFECT_REPOSITORY
    COMMENT = 'Container images for Prefect SPCS deployment';

-- Show the registry URL (needed for docker push)
SHOW IMAGE REPOSITORIES LIKE 'PREFECT_REPOSITORY';
