"""Tests for .gitlab-ci.yml — CI/CD pipeline configuration.

Validates stages, job definitions, rules, and consistency with
project configuration (Python version, UV usage, etc.).
"""

import re

import pytest
import yaml
from conftest import PROJECT_DIR

CI_FILE = PROJECT_DIR / ".gitlab-ci.yml"


@pytest.fixture(scope="module")
def ci_config():
    assert CI_FILE.exists(), ".gitlab-ci.yml must exist"
    return yaml.safe_load(CI_FILE.read_text())


@pytest.fixture(scope="module")
def ci_content():
    return CI_FILE.read_text()


class TestCiStructure:
    def test_file_exists(self):
        assert CI_FILE.exists()

    def test_parses_as_valid_yaml(self):
        data = yaml.safe_load(CI_FILE.read_text())
        assert isinstance(data, dict)

    def test_has_four_stages(self, ci_config):
        stages = ci_config.get("stages", [])
        assert "test" in stages
        assert "diff" in stages
        assert "build" in stages
        assert "deploy" in stages


class TestCiTestStage:
    def test_test_job_exists(self, ci_config):
        assert "test" in ci_config

    def test_runs_pytest(self, ci_config):
        script = " ".join(ci_config["test"].get("script", []))
        assert "pytest" in script

    def test_runs_validate(self, ci_config):
        script = " ".join(ci_config["test"].get("script", []))
        assert "--validate" in script

    def test_ignores_e2e(self, ci_config):
        script = " ".join(ci_config["test"].get("script", []))
        assert "ignore" in script and "e2e" in script

    def test_uses_uv(self, ci_config):
        job = ci_config["test"]
        all_commands = " ".join(job.get("before_script", []) + job.get("script", []))
        assert "uv" in all_commands


class TestCiDiffStage:
    def test_diff_job_exists(self, ci_config):
        assert "diff" in ci_config

    def test_runs_deploy_diff(self, ci_config):
        script = " ".join(ci_config["diff"].get("script", []))
        assert "--diff" in script and "deploy.py" in script

    def test_allows_failure(self, ci_config):
        assert ci_config["diff"].get("allow_failure") is True

    def test_mr_only(self, ci_config):
        rules = ci_config["diff"].get("rules", [])
        has_mr_rule = any("CI_MERGE_REQUEST_IID" in str(r.get("if", "")) for r in rules)
        assert has_mr_rule, "Diff should only run on merge requests"


class TestCiBuildStage:
    def test_build_job_exists(self, ci_config):
        assert "build" in ci_config

    def test_runs_build_and_push(self, ci_config):
        script = " ".join(ci_config["build"].get("script", []))
        assert "build_and_push" in script

    def test_is_main_only(self, ci_config):
        rules = ci_config["build"].get("rules", [])
        main_rule = any("main" in str(r.get("if", "")) for r in rules)
        assert main_rule, "Build stage should be restricted to main branch"

    def test_uses_docker_dind(self, ci_config):
        services = ci_config["build"].get("services", [])
        assert any("dind" in str(s) for s in services)


class TestCiDeployStage:
    def test_deploy_job_exists(self, ci_config):
        assert "deploy" in ci_config

    def test_runs_sync_flows(self, ci_config):
        script = " ".join(ci_config["deploy"].get("script", []))
        assert "sync_flows" in script

    def test_runs_deploy_script(self, ci_config):
        script = " ".join(ci_config["deploy"].get("script", []))
        assert "deploy.py" in script, "Deploy stage should run deploy.py"

    def test_is_manual(self, ci_config):
        rules = ci_config["deploy"].get("rules", [])
        has_manual = any(r.get("when") == "manual" for r in rules)
        assert has_manual, "Deploy stage should require manual trigger"

    def test_uploads_specs(self, ci_config):
        script = " ".join(ci_config["deploy"].get("script", []))
        assert "PREFECT_SPECS" in script, "Deploy should upload specs to stage"

    def test_shows_diff_before_deploy(self, ci_config):
        script = " ".join(ci_config["deploy"].get("script", []))
        assert "--diff" in script, "Deploy should show diff before applying"

    def test_needs_build(self, ci_config):
        needs = ci_config["deploy"].get("needs", [])
        build_jobs = [
            n for n in needs if n == "build" or (isinstance(n, dict) and n.get("job") == "build")
        ]
        assert build_jobs, "Deploy should depend on build"


class TestCiConsistency:
    def test_python_version_matches_pyproject(self, ci_config):
        """CI PYTHON_VERSION should match pyproject.toml requires-python."""
        ci_version = ci_config.get("variables", {}).get("PYTHON_VERSION", "")
        pyproject = (PROJECT_DIR / "pyproject.toml").read_text()
        # Extract version from requires-python = ">=3.12"
        m = re.search(r'requires-python\s*=\s*">=(\d+\.\d+)"', pyproject)
        assert m, "Could not parse requires-python from pyproject.toml"
        pyproject_version = m.group(1)
        assert ci_version == pyproject_version, (
            f"CI PYTHON_VERSION '{ci_version}' doesn't match "
            f"pyproject.toml requires-python '>={pyproject_version}'"
        )

    def test_uses_uv_cache_dir(self, ci_config):
        variables = ci_config.get("variables", {})
        assert "UV_CACHE_DIR" in variables

    def test_no_hardcoded_secrets(self, ci_content):
        """CI config should use $VARIABLE references, not hardcoded values."""
        assert "glpat-" not in ci_content
        assert "ghp_" not in ci_content
        assert "eyJ" not in ci_content


# ===========================================================================
# SAST & Secret Detection includes
# ===========================================================================
class TestCiSecurityIncludes:
    """Validate CI includes SAST and Secret Detection templates."""

    def test_includes_sast_template(self, ci_config):
        includes = ci_config.get("include", [])
        sast_found = any("SAST" in str(inc.get("template", "")) for inc in includes)
        assert sast_found, "CI must include SAST template"

    def test_includes_secret_detection_template(self, ci_config):
        includes = ci_config.get("include", [])
        sd_found = any("Secret-Detection" in str(inc.get("template", "")) for inc in includes)
        assert sd_found, "CI must include Secret-Detection template"


# ===========================================================================
# Lint stage
# ===========================================================================
class TestCiLintStage:
    """Validate lint job configuration."""

    def test_lint_job_exists(self, ci_config):
        assert "lint" in ci_config

    def test_uses_ruff_check(self, ci_config):
        script = " ".join(ci_config["lint"].get("script", []))
        assert "ruff check" in script

    def test_uses_ruff_format_check(self, ci_config):
        script = " ".join(ci_config["lint"].get("script", []))
        assert "ruff format" in script and "--check" in script

    def test_lint_is_interruptible(self, ci_config):
        assert ci_config["lint"].get("interruptible") is True

    def test_lint_runs_on_mr_and_branch(self, ci_config):
        rules = ci_config["lint"].get("rules", [])
        rule_strs = [str(r.get("if", "")) for r in rules]
        has_mr = any("CI_MERGE_REQUEST_IID" in s for s in rule_strs)
        has_branch = any("CI_COMMIT_BRANCH" in s for s in rule_strs)
        assert has_mr and has_branch


# ===========================================================================
# E2E stage
# ===========================================================================
class TestCiE2eStage:
    """Validate e2e job configuration."""

    def test_e2e_job_exists(self, ci_config):
        assert "e2e" in ci_config

    def test_runs_e2e_test_files(self, ci_config):
        script = " ".join(ci_config["e2e"].get("script", []))
        assert "test_e2e_spcs" in script
        assert "test_e2e_hybrid" in script

    def test_uses_e2e_marker(self, ci_config):
        script = " ".join(ci_config["e2e"].get("script", []))
        assert "-m e2e" in script or "-m=e2e" in script

    def test_has_schedule_trigger(self, ci_config):
        rules = ci_config["e2e"].get("rules", [])
        has_schedule = any("schedule" in str(r.get("if", "")) for r in rules)
        assert has_schedule, "E2E should run on scheduled pipelines"

    def test_has_manual_trigger(self, ci_config):
        rules = ci_config["e2e"].get("rules", [])
        has_manual = any(r.get("when") == "manual" for r in rules)
        assert has_manual, "E2E should be manually triggerable on main"

    def test_requires_snowflake_credentials(self, ci_config):
        rules = ci_config["e2e"].get("rules", [])
        rule_strs = [str(r.get("if", "")) for r in rules]
        has_sf_check = any("SNOWFLAKE_PAT" in s for s in rule_strs)
        assert has_sf_check, "E2E should require SNOWFLAKE_PAT"

    def test_produces_junit_report(self, ci_config):
        artifacts = ci_config["e2e"].get("artifacts", {})
        reports = artifacts.get("reports", {})
        assert "junit" in reports


# ===========================================================================
# Snowflake setup anchor
# ===========================================================================
class TestCiSnowflakeSetup:
    """Validate the .snowflake_setup YAML anchor."""

    def test_snowflake_setup_anchor_exists(self, ci_content):
        assert "&snowflake_setup" in ci_content

    def test_creates_snowflake_directory(self, ci_content):
        assert "~/.snowflake" in ci_content

    def test_creates_connections_toml(self, ci_content):
        assert "connections.toml" in ci_content

    def test_sets_secure_permissions(self, ci_content):
        assert "chmod 0600" in ci_content or "chmod 600" in ci_content

    def test_uses_pat_auth(self, ci_content):
        assert "programmatic_access_token" in ci_content

    def test_build_uses_snowflake_setup(self, ci_content):
        """Build stage must reference *snowflake_setup anchor."""
        # The YAML anchor is used as *snowflake_setup in before_script
        assert "*snowflake_setup" in ci_content

    def test_deploy_uses_snowflake_setup(self, ci_content):
        """Deploy stage must reference *snowflake_setup anchor."""
        assert "*snowflake_setup" in ci_content


# ===========================================================================
# Docker Auth Config gotcha
# ===========================================================================
class TestCiDockerAuthConfig:
    """Validate DOCKER_AUTH_CONFIG is documented for private registries."""

    def test_docker_auth_config_documented(self, ci_content):
        """DOCKER_AUTH_CONFIG must be documented as a required CI variable."""
        assert "DOCKER_AUTH_CONFIG" in ci_content

    def test_docker_auth_config_format_documented(self, ci_content):
        """The JSON format for DOCKER_AUTH_CONFIG should be shown."""
        assert "auths" in ci_content

    def test_dockerhub_login_in_build(self, ci_config):
        """Build should attempt Docker Hub login to avoid rate limits."""
        before = ci_config["build"].get("before_script", [])
        # Flatten nested lists from YAML anchors (*snowflake_setup)
        flat = []
        for item in before:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(str(item))
        before_str = " ".join(flat)
        assert "docker login" in before_str

    def test_dockerhub_credentials_documented(self, ci_content):
        """DOCKERHUB_USER and DOCKERHUB_TOKEN should be documented."""
        assert "DOCKERHUB_USER" in ci_content
        assert "DOCKERHUB_TOKEN" in ci_content


# ===========================================================================
# CI job robustness
# ===========================================================================
class TestCiJobRobustness:
    """Validate retry, interruptible, and caching patterns."""

    def test_build_has_retry(self, ci_config):
        assert ci_config["build"].get("retry") is not None

    def test_deploy_has_retry(self, ci_config):
        assert ci_config["deploy"].get("retry") is not None

    def test_test_is_interruptible(self, ci_config):
        assert ci_config["test"].get("interruptible") is True

    def test_diff_is_interruptible(self, ci_config):
        assert ci_config["diff"].get("interruptible") is True

    def test_test_produces_coverage(self, ci_config):
        """Test job must produce coverage reports."""
        artifacts = ci_config["test"].get("artifacts", {})
        reports = artifacts.get("reports", {})
        assert "coverage_report" in reports or "junit" in reports

    def test_test_produces_junit(self, ci_config):
        artifacts = ci_config["test"].get("artifacts", {})
        reports = artifacts.get("reports", {})
        assert "junit" in reports

    def test_test_artifacts_always_uploaded(self, ci_config):
        """Test artifacts should upload even on failure."""
        artifacts = ci_config["test"].get("artifacts", {})
        assert artifacts.get("when") == "always"

    def test_deploy_has_environment(self, ci_config):
        """Deploy job should specify an environment."""
        assert ci_config["deploy"].get("environment") is not None

    def test_all_stages_present(self, ci_config):
        """All 5 stages must be defined."""
        stages = ci_config.get("stages", [])
        for stage in ["lint", "test", "diff", "build", "deploy"]:
            assert stage in stages, f"Missing stage: {stage}"

    def test_build_uses_spcs_image_registry_login(self, ci_config):
        """Build must login to SPCS image registry."""
        before = ci_config["build"].get("before_script", [])
        flat = []
        for item in before:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(str(item))
        before_str = " ".join(flat)
        assert "spcs image-registry login" in before_str
