"""Tests for environment configuration files.

Validates .env.example, .gitignore, and pytest.ini.
"""

from conftest import PROJECT_DIR


class TestEnvExample:
    def test_env_example_exists(self):
        assert (PROJECT_DIR / ".env.example").exists()

    def test_has_snowflake_connection(self):
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "SNOWFLAKE_CONNECTION" in content

    def test_has_spcs_registry(self):
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "SPCS_REGISTRY" in content

    def test_has_postgres_password(self):
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "POSTGRES_PASSWORD" in content

    def test_has_spcs_api_url(self):
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "PREFECT_SPCS_API_URL" in content

    def test_has_gcp_config(self):
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "GCP_PROJECT" in content


class TestGitignore:
    def test_gitignore_exists(self):
        assert (PROJECT_DIR / ".gitignore").exists()

    def test_ignores_pycache(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert "__pycache__" in content

    def test_ignores_env_files(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert ".env" in content

    def test_ignores_secrets_sql(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert "03_setup_secrets.sql" in content

    def test_ignores_claude_settings(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert "settings.local.json" in content

    def test_ignores_pytest_cache(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert ".pytest_cache" in content

    def test_ignores_ds_store(self):
        content = (PROJECT_DIR / ".gitignore").read_text()
        assert ".DS_Store" in content


class TestPytestIni:
    def test_pytest_ini_exists(self):
        assert (PROJECT_DIR / "pytest.ini").exists()

    def test_excludes_e2e_by_default(self):
        content = (PROJECT_DIR / "pytest.ini").read_text()
        assert "not e2e" in content

    def test_excludes_local_by_default(self):
        content = (PROJECT_DIR / "pytest.ini").read_text()
        assert "not local" in content

    def test_defines_e2e_marker(self):
        content = (PROJECT_DIR / "pytest.ini").read_text()
        assert "e2e:" in content

    def test_defines_local_marker(self):
        content = (PROJECT_DIR / "pytest.ini").read_text()
        assert "local:" in content


class TestEnvExampleCompleteness:
    """Ensure .env.example documents key env vars used across the project."""

    def test_gcp_compose_vars_documented(self):
        """Key GCP compose env vars should appear in .env.example."""
        env_example = (PROJECT_DIR / ".env.example").read_text()
        # These are critical vars needed by the GCP hybrid worker
        for var in ["SNOWFLAKE_PAT", "SPCS_ENDPOINT"]:
            assert var in env_example, f".env.example should document {var} (used by GCP compose)"

    def test_no_real_secrets_in_env_example(self):
        """The .env.example file must not contain real credentials."""
        content = (PROJECT_DIR / ".env.example").read_text()
        assert "eyJ" not in content, ".env.example must not contain real JWT tokens"
        assert "glpat-" not in content, ".env.example must not contain GitLab PATs"

    def test_monitoring_endpoint_vars_documented(self):
        """Monitoring endpoint vars must be in .env.example for GCP worker sidecars."""
        env_example = (PROJECT_DIR / ".env.example").read_text()
        assert "SPCS_MONITOR_ENDPOINT" in env_example, (
            ".env.example must document SPCS_MONITOR_ENDPOINT "
            "(used by docker-compose.monitoring.yml for remote worker sidecars)"
        )
        assert "SPCS_MONITOR_LOKI_ENDPOINT" in env_example, (
            ".env.example must document SPCS_MONITOR_LOKI_ENDPOINT "
            "(used by docker-compose.monitoring.yml for remote worker sidecars)"
        )

    def test_monitoring_endpoint_vars_use_placeholder(self):
        """Monitoring endpoint vars must use placeholder, not real URLs."""
        content = (PROJECT_DIR / ".env.example").read_text()
        for line in content.splitlines():
            if line.startswith("SPCS_MONITOR_ENDPOINT=") or line.startswith(
                "SPCS_MONITOR_LOKI_ENDPOINT="
            ):
                # Real endpoints have account locators like xxxxx- prefix
                assert "xxx" in line, (
                    f"Monitoring endpoint in .env.example must use placeholder URL, got: {line}"
                )
