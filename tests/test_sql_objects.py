"""Tests for SQL setup scripts.

Validates SQL file ordering, object names, grants, and patterns.
"""

import re

import pytest
from conftest import SQL_DIR

EXPECTED_SQL_FILES = [
    "01_setup_database.sql",
    "02_setup_stages.sql",
    "03_setup_secrets.sql.template",
    "04_setup_networking.sql",
    "05_setup_compute_pools.sql",
    "06_setup_image_repo.sql",
    "07_create_services.sql",
    "07_create_services_gcp.sql",
    "07b_update_services.sql",
    "08_validate.sql",
    "09_suspend_all.sql",
    "10_resume_all.sql",
]

# Long-running SPCS services (excludes one-shot jobs like PF_MIGRATE, PF_DEPLOY_JOB)
LONG_RUNNING_SERVICES = ["PF_REDIS", "PF_SERVER", "PF_SERVICES", "PF_WORKER"]


class TestSQLDiscovery:
    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_sql_file_exists(self, filename):
        assert (SQL_DIR / filename).exists(), f"Missing: {filename}"

    def test_sql_file_count(self):
        actual = set(
            f.name
            for f in SQL_DIR.iterdir()
            if f.suffix in (".sql", ".template") and not f.name.startswith(".")
        )
        expected = set(EXPECTED_SQL_FILES)
        # Allow 03_setup_secrets.sql to exist (gitignored, user-created)
        actual.discard("03_setup_secrets.sql")
        assert actual == expected, (
            f"Mismatch: extra={actual - expected}, missing={expected - actual}"
        )


class TestSQLObjectCreation:
    """Verify SQL creates all expected Snowflake objects."""

    def test_creates_database(self, sql_files):
        assert "CREATE DATABASE" in sql_files["01_setup_database.sql"]
        assert "PREFECT_DB" in sql_files["01_setup_database.sql"]

    def test_creates_schema(self, sql_files):
        assert "CREATE SCHEMA" in sql_files["01_setup_database.sql"]
        assert "PREFECT_SCHEMA" in sql_files["01_setup_database.sql"]

    def test_creates_role(self, sql_files):
        assert "CREATE ROLE" in sql_files["01_setup_database.sql"]
        assert "PREFECT_ROLE" in sql_files["01_setup_database.sql"]

    def test_grants_bind_service_endpoint(self, sql_files):
        content = sql_files["01_setup_database.sql"]
        assert "BIND SERVICE ENDPOINT" in content

    def test_creates_flows_stage(self, sql_files):
        content = sql_files["02_setup_stages.sql"]
        assert "PREFECT_FLOWS" in content

    def test_creates_specs_stage(self, sql_files):
        content = sql_files["02_setup_stages.sql"]
        assert "PREFECT_SPECS" in content

    def test_creates_dashboard_stage(self, sql_files):
        content = sql_files["02_setup_stages.sql"]
        assert "PREFECT_DASHBOARD" in content

    def test_creates_secret(self, sql_files):
        content = sql_files["03_setup_secrets.sql.template"]
        assert "CREATE SECRET" in content
        assert "PREFECT_DB_PASSWORD" in content
        assert "GENERIC_STRING" in content

    def test_creates_network_rule(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "CREATE" in content
        assert "NETWORK RULE" in content
        assert "MODE = EGRESS" in content

    def test_creates_eai(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "EXTERNAL ACCESS INTEGRATION" in content
        assert "PREFECT_WORKER_EAI" in content

    def test_eai_grants_to_role(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "GRANT USAGE ON INTEGRATION" in content
        assert "PREFECT_ROLE" in content

    def test_creates_dashboard_network_rule(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "DASHBOARD_EGRESS_RULE" in content

    def test_creates_dashboard_eai(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "PREFECT_DASHBOARD_EAI" in content

    def test_dashboard_eai_has_secret(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        assert "PREFECT_SVC_PAT" in content

    def test_dashboard_eai_grants_to_role(self, sql_files):
        content = sql_files["04_setup_networking.sql"]
        # Should have at least two GRANT USAGE lines (worker EAI + dashboard EAI)
        assert content.count("GRANT USAGE ON INTEGRATION") >= 2

    def test_worker_egress_includes_git_host(self, sql_files):
        """Worker egress rule must include git host for git_clone pull steps."""
        content = sql_files["04_setup_networking.sql"]
        assert "github.com" in content, (
            "Worker egress rule must include a git host (e.g. github.com) for "
            "git_clone pull steps. Without this, the worker cannot clone flow code."
        )

    def test_creates_infra_pool(self, sql_files):
        content = sql_files["05_setup_compute_pools.sql"]
        assert "PREFECT_INFRA_POOL" in content
        assert "CPU_X64_S" in content

    def test_creates_core_pool(self, sql_files):
        content = sql_files["05_setup_compute_pools.sql"]
        assert "PREFECT_CORE_POOL" in content
        assert "CPU_X64_M" in content

    def test_creates_worker_pool(self, sql_files):
        content = sql_files["05_setup_compute_pools.sql"]
        assert "PREFECT_WORKER_POOL" in content

    def test_creates_dashboard_pool(self, sql_files):
        content = sql_files["05_setup_compute_pools.sql"]
        assert "PREFECT_DASHBOARD_POOL" in content
        assert "CPU_X64_XS" in content

    def test_dashboard_pool_is_isolated(self, sql_files):
        """Dashboard pool must be separate from workload pools."""
        content = sql_files["05_setup_compute_pools.sql"]
        # Should have 4 distinct pool CREATE statements
        assert content.count("CREATE COMPUTE POOL") == 4

    def test_creates_image_repository(self, sql_files):
        content = sql_files["06_setup_image_repo.sql"]
        assert "IMAGE REPOSITORY" in content
        assert "PREFECT_REPOSITORY" in content


class TestSQLServiceCreation:
    @pytest.mark.parametrize("service", LONG_RUNNING_SERVICES)
    def test_creates_each_service(self, service, sql_files):
        content = sql_files["07_create_services.sql"]
        assert service in content, f"07_create_services.sql: missing service {service}"

    def test_postgres_not_spcs_service(self, sql_files):
        """Primary SQL (AWS/Azure) should not create PF_POSTGRES — uses Snowflake Managed Postgres."""
        content = sql_files["07_create_services.sql"]
        assert "CREATE SERVICE IF NOT EXISTS PF_POSTGRES" not in content

    def test_gcp_sql_creates_postgres_service(self, sql_files):
        """GCP SQL must create PF_POSTGRES (no Snowflake Managed Postgres on GCP)."""
        content = sql_files["07_create_services_gcp.sql"]
        assert "PF_POSTGRES" in content

    def test_worker_has_eai(self, sql_files):
        content = sql_files["07_create_services.sql"]
        assert "EXTERNAL_ACCESS_INTEGRATIONS" in content

    def test_uses_execute_job_for_migration(self, sql_files):
        content = sql_files["07_create_services.sql"]
        assert "EXECUTE JOB SERVICE" in content

    def test_waits_for_services(self, sql_files):
        content = sql_files["07_create_services.sql"]
        assert "SYSTEM$WAIT_FOR_SERVICES" in content

    def test_services_on_correct_pools(self, sql_files):
        content = sql_files["07_create_services.sql"]
        # PF_REDIS on INFRA pool, PF_SERVER on CORE pool, PF_WORKER on WORKER pool
        assert "PF_REDIS" in content and "PREFECT_INFRA_POOL" in content
        assert "PF_SERVER" in content and "PREFECT_CORE_POOL" in content
        assert "PF_WORKER" in content and "PREFECT_WORKER_POOL" in content


class TestSQLValidation:
    def test_validate_queries_services(self, sql_files):
        content = sql_files["08_validate.sql"]
        assert "INFORMATION_SCHEMA.SERVICES" in content

    def test_validate_gets_logs(self, sql_files):
        content = sql_files["08_validate.sql"]
        assert "SYSTEM$GET_SERVICE_LOGS" in content

    def test_validate_shows_endpoints(self, sql_files):
        content = sql_files["08_validate.sql"]
        assert "SHOW ENDPOINTS" in content

    @pytest.mark.parametrize("service", LONG_RUNNING_SERVICES)
    def test_validate_checks_service(self, service, sql_files):
        content = sql_files["08_validate.sql"]
        assert service in content, f"08_validate.sql: should check service {service}"


class TestSQLSuspendResume:
    @pytest.mark.parametrize(
        "pool",
        [
            "PREFECT_INFRA_POOL",
            "PREFECT_CORE_POOL",
            "PREFECT_WORKER_POOL",
            "PREFECT_DASHBOARD_POOL",
        ],
    )
    def test_suspend_has_pool(self, pool, sql_files):
        assert pool in sql_files["09_suspend_all.sql"]

    @pytest.mark.parametrize(
        "pool",
        [
            "PREFECT_INFRA_POOL",
            "PREFECT_CORE_POOL",
            "PREFECT_WORKER_POOL",
            "PREFECT_DASHBOARD_POOL",
        ],
    )
    def test_resume_has_pool(self, pool, sql_files):
        assert pool in sql_files["10_resume_all.sql"]

    def test_suspend_uses_alter(self, sql_files):
        content = sql_files["09_suspend_all.sql"]
        assert "ALTER COMPUTE POOL" in content
        assert "SUSPEND" in content

    def test_resume_uses_alter(self, sql_files):
        content = sql_files["10_resume_all.sql"]
        assert "ALTER COMPUTE POOL" in content
        assert "RESUME" in content


class TestSQLRoleUsage:
    """Verify scripts use correct roles."""

    @pytest.mark.parametrize(
        "filename",
        [
            "01_setup_database.sql",
            "05_setup_compute_pools.sql",
            "04_setup_networking.sql",
        ],
    )
    def test_uses_accountadmin(self, filename, sql_files):
        assert "ACCOUNTADMIN" in sql_files[filename]

    @pytest.mark.parametrize(
        "filename",
        [
            "02_setup_stages.sql",
            "06_setup_image_repo.sql",
            "07_create_services.sql",
            "07b_update_services.sql",
            "08_validate.sql",
        ],
    )
    def test_uses_prefect_role(self, filename, sql_files):
        assert "PREFECT_ROLE" in sql_files[filename]


# ===========================================================================
# 07b_update_services.sql — ALTER SERVICE pattern
# ===========================================================================
class TestSQLServiceUpdate:
    """Verify 07b_update_services.sql uses ALTER SERVICE (not CREATE)
    to preserve the public endpoint URL during rolling upgrades."""

    def test_update_file_exists(self):
        assert (SQL_DIR / "07b_update_services.sql").exists()

    @pytest.mark.parametrize("service", LONG_RUNNING_SERVICES)
    def test_alters_each_service(self, service, sql_files):
        """07b must ALTER every long-running service."""
        content = sql_files["07b_update_services.sql"]
        assert service in content, f"07b_update_services.sql: must ALTER service {service}"

    @pytest.mark.parametrize("service", LONG_RUNNING_SERVICES)
    def test_uses_alter_not_create(self, service, sql_files):
        """07b must use ALTER SERVICE, never CREATE SERVICE."""
        content = sql_files["07b_update_services.sql"]
        assert "CREATE SERVICE" not in content, (
            "07b_update_services.sql: must NOT use CREATE SERVICE "
            "(use ALTER SERVICE to preserve endpoint URL)"
        )
        assert "ALTER SERVICE" in content, "07b_update_services.sql: must use ALTER SERVICE"

    def test_no_create_or_replace(self, sql_files):
        """CREATE OR REPLACE SERVICE drops and recreates, generating a new URL."""
        content = sql_files["07b_update_services.sql"]
        assert "CREATE OR REPLACE SERVICE" not in content, (
            "07b_update_services.sql: must NOT use CREATE OR REPLACE SERVICE "
            "(this drops the service and generates a new ingress URL)"
        )

    def test_references_prefect_specs_stage(self, sql_files):
        """ALTER SERVICE should reference @PREFECT_SPECS stage."""
        content = sql_files["07b_update_services.sql"]
        assert "@PREFECT_SPECS" in content, (
            "07b_update_services.sql: must reference @PREFECT_SPECS stage"
        )

    def test_uses_specification_file(self, sql_files):
        """Each ALTER must use SPECIFICATION_FILE to point to a spec YAML."""
        content = sql_files["07b_update_services.sql"]
        assert "SPECIFICATION_FILE" in content

    def test_uses_from_stage_syntax(self, sql_files):
        """ALTER SERVICE (spec updates) must use FROM @PREFECT_SPECS syntax."""
        content = sql_files["07b_update_services.sql"]
        assert "FROM @PREFECT_SPECS" in content
        # Count only spec-update ALTER SERVICE lines (exclude ALTER SERVICE ... SET
        # which is used for property changes like MIN/MAX_INSTANCES)
        lines = content.splitlines()
        alter_count = sum(
            1
            for line in lines
            if "ALTER SERVICE" in line and not line.strip().startswith("--") and "SET" not in line
        )
        from_count = content.count("FROM @PREFECT_SPECS")
        assert alter_count == from_count, (
            f"Each spec-update ALTER SERVICE must have FROM @PREFECT_SPECS: "
            f"{alter_count} ALTERs but {from_count} FROM clauses"
        )

    def test_uses_prefect_role(self, sql_files):
        content = sql_files["07b_update_services.sql"]
        assert "PREFECT_ROLE" in content

    def test_no_execute_job_service(self, sql_files):
        """07b should NOT run one-shot jobs — those are handled separately."""
        content = sql_files["07b_update_services.sql"]
        assert "EXECUTE JOB SERVICE" not in content, (
            "07b_update_services.sql: should not contain EXECUTE JOB SERVICE "
            "(migration and deploy jobs are run separately)"
        )

    def test_create_and_update_cover_same_services(self, sql_files):
        """07_create and 07b_update must cover the same set of long-running services."""
        create_content = sql_files["07_create_services.sql"]
        update_content = sql_files["07b_update_services.sql"]
        for service in LONG_RUNNING_SERVICES:
            assert service in create_content, f"07_create_services.sql: missing service {service}"
            assert service in update_content, f"07b_update_services.sql: missing service {service}"

    def test_create_and_update_reference_same_spec_files(self, sql_files):
        """Both 07 and 07b must reference the same SPECIFICATION_FILE values."""
        spec_pattern = re.compile(r"SPECIFICATION_FILE\s*=\s*'([^']+)'")
        create_specs = set(spec_pattern.findall(sql_files["07_create_services.sql"]))
        update_specs = set(spec_pattern.findall(sql_files["07b_update_services.sql"]))
        # 07_create may have extra specs for jobs (pf_migrate.yaml, pf_deploy_job.yaml)
        # 07b should be a subset covering all long-running service specs
        assert update_specs, "07b_update_services.sql: no SPECIFICATION_FILE references found"
        assert update_specs.issubset(create_specs), (
            f"07b references specs not in 07_create. Extra in 07b: {update_specs - create_specs}"
        )
        # Every spec in 07b must correspond to a long-running service
        long_running_specs = {
            "pf_redis.yaml",
            "pf_server.yaml",
            "pf_services.yaml",
            "pf_worker.yaml",
        }
        assert update_specs == long_running_specs, (
            f"07b should cover exactly the long-running service specs. "
            f"Expected: {long_running_specs}, Got: {update_specs}"
        )
