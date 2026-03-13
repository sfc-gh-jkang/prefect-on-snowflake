"""Tests for README accuracy.

Validates that the README references real files, services,
and commands that actually exist in the project.
"""

import pytest
from conftest import FLOWS_DIR, PROJECT_DIR, SCRIPTS_DIR, SPECS_DIR, SQL_DIR


class TestReadmeExists:
    def test_readme_exists(self):
        assert (PROJECT_DIR / "README.md").exists()


class TestConstitutionExists:
    def test_constitution_exists(self):
        assert (PROJECT_DIR / "CONSTITUTION.md").exists()

    def test_constitution_has_content(self):
        content = (PROJECT_DIR / "CONSTITUTION.md").read_text()
        assert len(content) > 200, "CONSTITUTION.md should have substantial content"

    def test_constitution_mentions_13_gotchas(self):
        content = (PROJECT_DIR / "CONSTITUTION.md").read_text()
        # Should mention DNS hyphenation, readiness probes, etc.
        assert "DNS" in content or "dns" in content
        assert "readiness" in content.lower()


class TestSpecificationExists:
    def test_specification_exists(self):
        assert (PROJECT_DIR / "SPECIFICATION.md").exists()

    def test_specification_has_services_table(self):
        content = (PROJECT_DIR / "SPECIFICATION.md").read_text()
        assert "PF_SERVER" in content

    def test_specification_mentions_hybrid(self):
        content = (PROJECT_DIR / "SPECIFICATION.md").read_text()
        assert "hybrid" in content.lower() or "gcp" in content.lower()

    def test_specification_mentions_eai(self):
        content = (PROJECT_DIR / "SPECIFICATION.md").read_text()
        assert "EAI" in content or "External Access" in content

    def test_specification_mentions_flow_deployment(self):
        content = (PROJECT_DIR / "SPECIFICATION.md").read_text()
        assert "deploy" in content.lower()


class TestReadmeUpdateMode:
    """README must document the ALTER SERVICE update workflow."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_mentions_update_flag(self, readme):
        assert "--update" in readme, "README should document the --update flag"

    def test_readme_mentions_alter_service(self, readme):
        assert "ALTER SERVICE" in readme, "README should mention ALTER SERVICE"

    def test_readme_mentions_07b(self, readme):
        assert "07b_update_services.sql" in readme, (
            "README should reference 07b_update_services.sql"
        )

    def test_readme_warns_against_create_or_replace(self, readme):
        """README should warn users not to use CREATE OR REPLACE SERVICE."""
        readme_lower = readme.lower()
        assert "create or replace" in readme_lower or "create-or-replace" in readme_lower, (
            "README should warn against CREATE OR REPLACE SERVICE"
        )


class TestProjectFileCompleteness:
    """Verify all project files referenced in docs actually exist."""

    EXPECTED_DIRS = ["flows", "specs", "sql", "scripts", "images", "tests", "workers"]

    @pytest.mark.parametrize("dirname", EXPECTED_DIRS)
    def test_directory_exists(self, dirname):
        assert (PROJECT_DIR / dirname).is_dir()

    def test_spec_files_count(self):
        specs = list(SPECS_DIR.glob("*.yaml"))
        assert len(specs) == 8

    def test_sql_files_count(self):
        sqls = list(SQL_DIR.glob("*.sql")) + list(SQL_DIR.glob("*.sql.template"))
        assert len(sqls) >= 10

    def test_script_files_count(self):
        scripts = list(SCRIPTS_DIR.glob("*.sh"))
        assert len(scripts) >= 5

    def test_flow_files_count(self):
        flows = [f for f in FLOWS_DIR.glob("*.py") if f.name != "__init__.py"]
        assert len(flows) >= 4


class TestCrossCloudDocumentation:
    """Verify documentation covers cross-cloud deployment requirements."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_mentions_gcp_postgres(self, readme):
        """README should document that GCP requires a containerized Postgres."""
        readme_lower = readme.lower()
        has_gcp_pg = (
            ("gcp" in readme_lower and "postgres" in readme_lower)
            or "pf_postgres" in readme_lower
            or "07_create_services_gcp" in readme_lower
        )
        assert has_gcp_pg, (
            "README should document that GCP uses a containerized Postgres "
            "(PF_POSTGRES) instead of Snowflake Managed Postgres"
        )

    def test_readme_mentions_short_dns(self, readme):
        """README should document SPCS short DNS name convention."""
        readme_lower = readme.lower()
        assert "short" in readme_lower and "dns" in readme_lower or "pf-server" in readme, (
            "README should document using short DNS names (e.g., pf-server) "
            "instead of FQDNs for cross-account portability"
        )


class TestReadmeDeployPyDocumentation:
    """Verify README documents deploy.py --cloud flag and usage."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_deploy_py_cloud_flag(self, readme):
        assert "--cloud aws" in readme

    def test_readme_documents_deploy_py_cloud_all(self, readme):
        assert "--cloud all" in readme

    def test_readme_documents_spcs_endpoint_per_cloud(self, readme):
        assert "SPCS_ENDPOINT_AWS" in readme
        assert "SPCS_ENDPOINT_AZURE" in readme
        assert "SPCS_ENDPOINT_GCP" in readme

    def test_readme_documents_snowflake_pat_per_cloud(self, readme):
        assert "SNOWFLAKE_PAT_AWS" in readme
        assert "SNOWFLAKE_PAT_AZURE" in readme
        assert "SNOWFLAKE_PAT_GCP" in readme

    def test_readme_shows_deploy_py_usage_examples(self, readme):
        assert "uv run python flows/deploy.py" in readme

    def test_readme_documents_deploy_py_validate_flag(self, readme):
        assert "--validate" in readme

    def test_readme_documents_deploy_py_diff_flag(self, readme):
        assert "--diff" in readme

    def test_readme_documents_source_env_before_deploy(self, readme):
        """Deploy instructions should remind to source .env first."""
        assert "source .env" in readme


class TestReadmeSecretsRotationDocumentation:
    """Verify README documents rotate_secrets.sh --all-clouds and --smtp."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_all_clouds_flag(self, readme):
        assert "--all-clouds" in readme

    def test_readme_documents_smtp_flag(self, readme):
        assert "--smtp" in readme

    def test_readme_documents_all_9_secrets(self, readme):
        expected = [
            "PREFECT_DB_PASSWORD",
            "GIT_ACCESS_TOKEN",
            "POSTGRES_EXPORTER_DSN",
            "GRAFANA_DB_DSN",
            "GRAFANA_ADMIN_PASSWORD",
            "GRAFANA_SMTP_PASSWORD",
            "GRAFANA_SMTP_USER",
            "PREFECT_SVC_PAT",
            "SLACK_WEBHOOK_URL",
        ]
        for secret in expected:
            assert secret in readme, f"README missing secret: {secret}"

    def test_readme_documents_7_menu_options(self, readme):
        assert "7" in readme and "menu" in readme.lower() or "interactive" in readme.lower()

    def test_readme_documents_one_shot_smtp_fix(self, readme):
        assert "rotate_secrets.sh --all-clouds --smtp" in readme

    def test_readme_documents_check_flag(self, readme):
        assert "--check" in readme


class TestReadmeSmtpTroubleshooting:
    """Verify README documents SMTP troubleshooting and Google password revocation."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_warns_about_google_password_revocation(self, readme):
        readme_lower = readme.lower()
        assert "google" in readme_lower and "revoke" in readme_lower

    def test_readme_mentions_apppasswords_url(self, readme):
        assert "myaccount.google.com/apppasswords" in readme

    def test_readme_mentions_535_bad_credentials(self, readme):
        assert "535" in readme or "BadCredentials" in readme

    def test_readme_smtp_uses_port_587(self, readme):
        assert "smtp.gmail.com:587" in readme

    def test_readme_mentions_mandatory_starttls(self, readme):
        assert "MandatoryStartTLS" in readme


class TestReadmeQuickOperationsReference:
    """Verify README has a Quick Operations Reference section."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_has_quick_operations_section(self, readme):
        assert "Quick Operations Reference" in readme

    def test_quick_ref_includes_deploy_command(self, readme):
        assert "uv run python flows/deploy.py --cloud" in readme

    def test_quick_ref_includes_secrets_check(self, readme):
        assert "rotate_secrets.sh --all-clouds --check" in readme

    def test_quick_ref_includes_smtp_fix(self, readme):
        assert "rotate_secrets.sh --all-clouds --smtp" in readme

    def test_quick_ref_includes_pytest(self, readme):
        assert "uv run pytest" in readme


class TestReadmeGrafanaSmtpConfig:
    """Verify README documents GRAFANA_SMTP_* env vars."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_grafana_smtp_user_env_var(self, readme):
        assert "GRAFANA_SMTP_USER" in readme

    def test_readme_documents_grafana_smtp_password_env_var(self, readme):
        assert "GRAFANA_SMTP_PASSWORD" in readme

    def test_readme_documents_grafana_smtp_recipients_env_var(self, readme):
        assert "GRAFANA_SMTP_RECIPIENTS" in readme


class TestReadmeSlackEgressDocs:
    """Verify README documents Slack egress requirements."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_hooks_slack_com(self, readme):
        assert "hooks.slack.com" in readme

    def test_readme_documents_api_slack_com(self, readme):
        assert "api.slack.com" in readme

    def test_readme_documents_slack_webhook_url_secret(self, readme):
        assert "GF_ALERTING_SLACK_WEBHOOK_URL" in readme


class TestReadmeTelemetryDocs:
    """Verify README documents Prefect server telemetry silencing."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_analytics_env_var(self, readme):
        assert "PREFECT_SERVER_ANALYTICS_ENABLED" in readme

    def test_readme_mentions_api_prefect_io(self, readme):
        assert "api.prefect.io" in readme


class TestReadmeContactPointCleanup:
    """Verify README documents deleteContactPoints provisioning cleanup."""

    @pytest.fixture(scope="class")
    def readme(self):
        return (PROJECT_DIR / "README.md").read_text()

    def test_readme_documents_delete_contact_points(self, readme):
        assert "deleteContactPoints" in readme

    def test_readme_documents_webhook_receiver_cleanup(self, readme):
        assert "webhook-receiver" in readme

    def test_readme_documents_hooks_example_placeholder(self, readme):
        assert "hooks.example.com" in readme
