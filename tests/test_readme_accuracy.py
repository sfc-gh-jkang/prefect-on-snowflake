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
