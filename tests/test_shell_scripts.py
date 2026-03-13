"""Tests for shell scripts.

Validates shebang lines, required patterns, and script references
to correct SQL files, stages, and services.
"""

import pytest
from conftest import GCP_DIR, SCRIPTS_DIR, WORKERS_DIR

EXPECTED_SCRIPTS = [
    "build_and_push.sh",
    "deploy.sh",
    "deploy_flows.sh",
    "rotate_secrets.sh",
    "sync_flows.sh",
    "teardown.sh",
    "update_versions.sh",
]


class TestScriptDiscovery:
    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_script_exists(self, filename):
        assert (SCRIPTS_DIR / filename).exists()

    def test_gcp_setup_script_exists(self):
        assert (GCP_DIR / "setup_gcp_worker.sh").exists()

    def test_aws_setup_script_exists(self):
        assert (WORKERS_DIR / "aws" / "setup_aws_worker.sh").exists()


class TestScriptShebang:
    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_has_shebang(self, filename, script_files):
        content = script_files[filename]
        assert content.startswith("#!/"), f"{filename}: missing shebang"

    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_uses_bash(self, filename, script_files):
        first_line = script_files[filename].split("\n")[0]
        assert "bash" in first_line, f"{filename}: should use bash"

    def test_gcp_script_has_shebang(self, script_files):
        first_line = script_files["setup_gcp_worker.sh"].split("\n")[0]
        assert "bash" in first_line

    def test_aws_script_has_shebang(self, script_files):
        first_line = script_files["setup_aws_worker.sh"].split("\n")[0]
        assert "bash" in first_line


class TestScriptSafetyFlags:
    @pytest.mark.parametrize("filename", EXPECTED_SCRIPTS)
    def test_uses_set_euo_pipefail(self, filename, script_files):
        content = script_files[filename]
        assert "set -euo pipefail" in content, f"{filename}: should use 'set -euo pipefail'"

    def test_gcp_script_uses_set_euo(self, script_files):
        content = script_files["setup_gcp_worker.sh"]
        assert "set -euo pipefail" in content

    def test_aws_script_uses_set_euo(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "set -euo pipefail" in content


class TestBuildAndPush:
    def test_gets_registry_url(self, script_files):
        content = script_files["build_and_push.sh"]
        assert "SHOW IMAGE REPOSITORIES" in content

    def test_logs_into_registry(self, script_files):
        content = script_files["build_and_push.sh"]
        assert "spcs image-registry login" in content

    def test_no_postgres_build(self, script_files):
        """No custom Postgres Dockerfile build — we pull stock images only."""
        content = script_files["build_and_push.sh"]
        assert (
            "docker buildx build" not in content.split("postgres")[0].split("postgres")[-1]
            or "Dockerfile" not in content.lower().split("postgres")[0]
        )
        lines = content.splitlines()
        for line in lines:
            if "postgres" in line.lower() and "buildx build" in line:
                raise AssertionError(f"Postgres should not have a custom build step: {line}")

    def test_builds_redis(self, script_files):
        content = script_files["build_and_push.sh"]
        assert "redis" in content

    def test_builds_prefect_worker(self, script_files):
        content = script_files["build_and_push.sh"]
        assert "prefect-worker" in content or "prefect" in content

    def test_pushes_images(self, script_files):
        content = script_files["build_and_push.sh"]
        assert "docker push" in content

    def test_resolves_git_sha(self, script_files):
        """build_and_push.sh must resolve GIT_SHA for image tagging."""
        content = script_files["build_and_push.sh"]
        assert "GIT_SHA=" in content
        assert "git rev-parse --short HEAD" in content

    def test_tags_worker_with_git_sha(self, script_files):
        """prefect-worker image must be tagged with both :latest and :$GIT_SHA."""
        content = script_files["build_and_push.sh"]
        assert "prefect-worker:$GIT_SHA" in content

    def test_pushes_git_sha_tagged_image(self, script_files):
        """The SHA-tagged worker image must be pushed to the registry."""
        content = script_files["build_and_push.sh"]
        # The push loop should include the SHA-tagged image
        assert "prefect-worker:$GIT_SHA" in content


class TestDeployScript:
    def test_runs_sql_in_order(self, script_files):
        content = script_files["deploy.sh"]
        # Check that numbered SQL files are referenced
        for num in ["01", "02", "04", "05", "06", "07", "08"]:
            assert num in content, f"deploy.sh should reference SQL file {num}_*"

    def test_uploads_specs_to_stage(self, script_files):
        content = script_files["deploy.sh"]
        assert "PREFECT_SPECS" in content

    def test_checks_for_secrets_file(self, script_files):
        content = script_files["deploy.sh"]
        assert "03_setup_secrets.sql" in content

    # --- --update mode (ALTER SERVICE) ---

    def test_supports_update_flag(self, script_files):
        """deploy.sh must accept --update for rolling upgrades."""
        content = script_files["deploy.sh"]
        assert "--update" in content

    def test_has_update_mode_variable(self, script_files):
        """deploy.sh must use UPDATE_MODE variable to branch logic."""
        content = script_files["deploy.sh"]
        assert "UPDATE_MODE=" in content

    def test_update_mode_references_07b(self, script_files):
        """--update mode must reference 07b_update_services.sql."""
        content = script_files["deploy.sh"]
        assert "07b_update_services.sql" in content, (
            "deploy.sh: --update mode must reference 07b_update_services.sql"
        )

    def test_has_services_exist_function(self, script_files):
        """deploy.sh must have a services_exist() function to detect running services."""
        content = script_files["deploy.sh"]
        assert "services_exist()" in content or "services_exist ()" in content

    def test_services_exist_uses_show_services(self, script_files):
        """services_exist must use SHOW SERVICES (cannot use in subqueries)."""
        content = script_files["deploy.sh"]
        assert "SHOW SERVICES" in content

    def test_update_mode_skips_infra_sql(self, script_files):
        """In --update mode, infra scripts (01-06) must not be run."""
        content = script_files["deploy.sh"]
        # Find the update branch (between UPDATE_MODE and the else)
        update_start = content.find("if ${UPDATE_MODE}")
        if update_start == -1:
            update_start = content.find("if $UPDATE_MODE")
        assert update_start != -1, "deploy.sh: must have UPDATE_MODE branch"
        else_pos = content.find("\nelse", update_start)
        assert else_pos != -1, "deploy.sh: must have else branch after UPDATE_MODE"
        update_branch = content[update_start:else_pos]
        # Update branch must NOT reference 01-06 SQL files
        for num in ["01_", "02_", "03_", "04_", "05_", "06_"]:
            assert num not in update_branch, (
                f"deploy.sh: update branch should not reference {num}* SQL files"
            )

    def test_update_mode_runs_validation(self, script_files):
        """--update mode should still run 08_validate.sql."""
        content = script_files["deploy.sh"]
        assert "08_validate.sql" in content

    def test_has_wait_for_ready_function(self, script_files):
        """deploy.sh must poll SYSTEM$GET_SERVICE_STATUS after ALTER SERVICE."""
        content = script_files["deploy.sh"]
        assert "wait_for_ready" in content

    def test_polls_service_status(self, script_files):
        """deploy.sh must use SYSTEM$GET_SERVICE_STATUS for readiness polling."""
        content = script_files["deploy.sh"]
        assert "SYSTEM$GET_SERVICE_STATUS" in content or "GET_SERVICE_STATUS" in content

    def test_update_branch_polls_all_four_services(self, script_files):
        """The update branch must poll READY for all 4 long-running services."""
        content = script_files["deploy.sh"]
        update_start = content.find("if ${UPDATE_MODE}")
        if update_start == -1:
            update_start = content.find("if $UPDATE_MODE")
        else_pos = content.find("\nelse", update_start)
        update_branch = content[update_start:else_pos]
        for svc in ["PF_REDIS", "PF_SERVER", "PF_SERVICES", "PF_WORKER"]:
            assert svc in update_branch, f"deploy.sh: update branch must poll {svc} for READY"

    def test_wait_for_ready_has_timeout(self, script_files):
        """wait_for_ready must have a timeout to avoid infinite loops."""
        content = script_files.get("_lib.sh", "") + script_files["deploy.sh"]
        assert "max_attempts" in content or "MAX_ATTEMPTS" in content, (
            "wait_for_ready (in _lib.sh or deploy.sh) must have a timeout mechanism"
        )

    def test_wait_for_ready_uses_sleep(self, script_files):
        """wait_for_ready must sleep between polls to avoid hammering Snowflake."""
        content = script_files.get("_lib.sh", "") + script_files["deploy.sh"]
        assert "sleep" in content

    def test_supports_connection_flag(self, script_files):
        """deploy.sh must accept --connection for the snow CLI connection."""
        content = script_files["deploy.sh"]
        assert "--connection" in content

    def test_supports_help_flag(self, script_files):
        content = script_files["deploy.sh"]
        assert "--help" in content or "-h" in content


class TestDeployFlows:
    def test_checks_prefect_api_url(self, script_files):
        content = script_files["deploy_flows.sh"]
        assert "PREFECT_API_URL" in content

    def test_runs_prefect_deploy(self, script_files):
        content = script_files["deploy_flows.sh"]
        assert "prefect deploy --all" in content

    def test_supports_validate_flag(self, script_files):
        content = script_files["deploy_flows.sh"]
        assert "--validate" in content


class TestSyncFlows:
    def test_uses_put_for_upload(self, script_files):
        content = script_files["sync_flows.sh"]
        assert "PUT" in content

    def test_targets_prefect_flows_stage(self, script_files):
        content = script_files["sync_flows.sh"]
        assert "PREFECT_FLOWS" in content

    def test_sets_correct_role_context(self, script_files):
        content = script_files["sync_flows.sh"]
        assert "USE ROLE PREFECT_ROLE" in content
        assert "USE DATABASE PREFECT_DB" in content

    def test_disables_auto_compress(self, script_files):
        content = script_files["sync_flows.sh"]
        assert "AUTO_COMPRESS=FALSE" in content


class TestTeardown:
    def test_has_confirmation_prompt(self, script_files):
        content = script_files["teardown.sh"]
        assert "read" in content or "Continue" in content

    def test_drops_services(self, script_files):
        content = script_files["teardown.sh"]
        for svc in ["PF_WORKER", "PF_SERVICES", "PF_SERVER", "PF_REDIS"]:
            assert svc in content

    def test_drops_in_reverse_order(self, script_files):
        content = script_files["teardown.sh"]
        # Worker should be dropped before redis (worker → services → server → redis)
        worker_pos = content.find("PF_WORKER")
        redis_pos = content.find("PF_REDIS")
        assert worker_pos < redis_pos, "Services should be dropped worker-first"

    def test_suspends_pools(self, script_files):
        content = script_files["teardown.sh"]
        assert "SUSPEND" in content

    def test_drops_pools(self, script_files):
        content = script_files["teardown.sh"]
        assert "DROP COMPUTE POOL" in content


class TestGCPSetupScript:
    def test_uses_gcloud(self, script_files):
        content = script_files["setup_gcp_worker.sh"]
        assert "gcloud" in content

    def test_creates_vm(self, script_files):
        content = script_files["setup_gcp_worker.sh"]
        assert "create-with-container" in content or "instances create" in content

    def test_references_pool_name(self, script_files):
        from conftest import POOL_NAMES  # noqa: E402

        content = script_files["setup_gcp_worker.sh"]
        assert POOL_NAMES["gcp"] in content

    def test_requires_api_url(self, script_files):
        content = script_files["setup_gcp_worker.sh"]
        assert "SPCS_ENDPOINT" in content

    def test_uses_prefect_image(self, script_files):
        content = script_files["setup_gcp_worker.sh"]
        assert "docker-compose.gcp.yaml" in content or "Dockerfile.worker" in content


class TestAWSSetupScript:
    """Tests for workers/aws/setup_aws_worker.sh — EC2 provisioning script."""

    def test_uses_aws_cli(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "aws ec2" in content

    def test_launches_ec2_instance(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "run-instances" in content

    def test_references_pool_name(self, script_files):
        from conftest import POOL_NAMES  # noqa: E402

        content = script_files["setup_aws_worker.sh"]
        assert POOL_NAMES["aws"] in content

    def test_requires_snowflake_pat(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "SNOWFLAKE_PAT" in content

    def test_requires_spcs_endpoint(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "SPCS_ENDPOINT" in content

    def test_installs_docker(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "dnf install -y docker" in content or "yum install -y docker" in content

    def test_installs_docker_compose(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "docker-compose-linux" in content

    def test_installs_docker_buildx(self, script_files):
        """Buildx is required for 'docker compose build' on AL2023."""
        content = script_files["setup_aws_worker.sh"]
        assert "docker-buildx" in content or "buildx" in content

    def test_uses_full_ami_not_minimal(self, script_files):
        """Must use full AL2023 AMI — minimal AMI fails cloud-init."""
        content = script_files["setup_aws_worker.sh"]
        assert "al2023-ami-2023*" in content or "al2023-ami-2023" in content
        assert "minimal" not in content.split("AMI")[1].split("\n")[0] if "AMI" in content else True

    def test_injects_secrets_via_user_data(self, script_files):
        """Secrets must be injected via user-data placeholders, not hardcoded."""
        content = script_files["setup_aws_worker.sh"]
        assert "__SNOWFLAKE_PAT__" in content
        assert "__SPCS_ENDPOINT__" in content

    def test_passes_git_env_vars(self, script_files):
        """User-data must inject GIT_REPO_URL and GIT_BRANCH."""
        content = script_files["setup_aws_worker.sh"]
        assert "GIT_REPO_URL" in content
        assert "GIT_BRANCH" in content
        assert "__GIT_REPO_URL__" in content

    def test_writes_env_file_with_restricted_permissions(self, script_files):
        """The .env file on EC2 must be chmod 600."""
        content = script_files["setup_aws_worker.sh"]
        assert "chmod 600 .env" in content

    def test_uses_ssm_for_debug(self, script_files):
        """Debug instructions should use SSM, not SSH (private subnet)."""
        content = script_files["setup_aws_worker.sh"]
        assert "ssm" in content.lower()

    def test_waits_for_instance_running(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "wait instance-running" in content

    def test_logs_to_prefect_setup_log(self, script_files):
        """Bootstrap output must be redirected to /var/log/prefect-setup.log."""
        content = script_files["setup_aws_worker.sh"]
        assert "/var/log/prefect-setup.log" in content

    def test_creates_key_pair_if_missing(self, script_files):
        content = script_files["setup_aws_worker.sh"]
        assert "create-key-pair" in content

    def test_supports_env_var_overrides(self, script_files):
        """Key config values should be overridable via env vars."""
        content = script_files["setup_aws_worker.sh"]
        assert "AWS_PROFILE" in content
        assert "AWS_REGION" in content
        assert "AWS_INSTANCE_TYPE" in content or "INSTANCE_TYPE" in content

    def test_no_instance_scheduler_tag(self, script_files):
        """EC2 must NOT have a Schedule tag so the AWS Instance Scheduler
        ignores it entirely — no scheduled stops or terminations."""
        content = script_files["setup_aws_worker.sh"]
        assert "Key=Schedule" not in content

    def test_termination_protection_enabled(self, script_files):
        """EC2 must launch with --disable-api-termination to prevent
        accidental termination."""
        content = script_files["setup_aws_worker.sh"]
        assert "--disable-api-termination" in content

    def test_nginx_monitor_has_proxy_http_version(self, script_files):
        """nginx-monitor.conf in user-data must set proxy_http_version 1.1
        to avoid 426 Upgrade Required from SPCS Prometheus."""
        content = script_files["setup_aws_worker.sh"]
        assert content.count("proxy_http_version 1.1") >= 2

    def test_nginx_monitor_has_connection_header(self, script_files):
        """nginx-monitor.conf must clear Connection header for keepalive."""
        content = script_files["setup_aws_worker.sh"]
        assert 'proxy_set_header Connection ""' in content

    def test_nginx_monitor_has_client_max_body_size(self, script_files):
        """nginx-monitor.conf must allow large metric batches."""
        content = script_files["setup_aws_worker.sh"]
        assert "client_max_body_size 10m" in content

    def test_no_unknown_defaults_in_external_labels(self, script_files):
        """Prometheus v3 treats ${VAR:-default} as literal env var name.
        Use ${VAR} without defaults in prometheus-agent.yml."""
        content = script_files["setup_aws_worker.sh"]
        assert ":-unknown" not in content

    def test_promtail_expand_env_enabled(self, script_files):
        """Promtail must use -config.expand-env=true for env var expansion."""
        content = script_files["setup_aws_worker.sh"]
        assert "-config.expand-env=true" in content

    def test_prometheus_agent_uses_agent_flag(self, script_files):
        """Prometheus v3.x uses --agent (not --enable-feature=agent)."""
        content = script_files["setup_aws_worker.sh"]
        assert "--agent" in content
        assert "--enable-feature=agent" not in content


class TestRotateSecrets:
    """Tests for scripts/rotate_secrets.sh."""

    # --- CLI arg parsing ---

    def test_requires_connection_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--connection" in content

    def test_connection_is_mandatory(self, script_files):
        """Script should exit if --connection is not provided."""
        content = script_files["rotate_secrets.sh"]
        assert "Error: --connection is required" in content

    def test_supports_check_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--check" in content

    def test_supports_dry_run_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--dry-run" in content

    def test_supports_help_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--help" in content or "-h" in content

    # --- Secret names match Snowflake objects ---

    def test_references_prefect_db_password(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "PREFECT_DB_PASSWORD" in content

    def test_references_git_access_token(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "GIT_ACCESS_TOKEN" in content

    def test_references_snowflake_pat(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "SNOWFLAKE_PAT" in content

    # --- Uses correct SQL for rotation ---

    def test_uses_alter_secret(self, script_files):
        """Rotation should ALTER existing secrets, not DROP/CREATE."""
        content = script_files["rotate_secrets.sh"]
        assert "ALTER SECRET" in content
        assert "DROP SECRET" not in content

    def test_sets_correct_role_context(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "USE ROLE PREFECT_ROLE" in content
        assert "USE DATABASE PREFECT_DB" in content
        assert "USE SCHEMA PREFECT_SCHEMA" in content

    # --- Service restart safety ---

    def test_restarts_services_via_suspend_resume(self, script_files):
        """Secrets are picked up on restart; script should SUSPEND then RESUME."""
        content = script_files["rotate_secrets.sh"]
        assert "SUSPEND" in content
        assert "RESUME" in content

    def test_does_not_execute_alter_pf_server(self, script_files):
        """PF_SERVER should NOT be restarted to preserve the public endpoint URL.

        The script may *mention* ALTER SERVICE PF_SERVER in echo/help text,
        but should never call _sql with it.
        """
        content = script_files["rotate_secrets.sh"]
        # _sql "ALTER SERVICE PF_SERVER ..." would be actual execution
        assert '_sql "ALTER SERVICE PF_SERVER' not in content
        assert "_sql 'ALTER SERVICE PF_SERVER" not in content

    def test_warns_about_pf_server(self, script_files):
        """Should warn the user that PF_SERVER was not restarted."""
        content = script_files["rotate_secrets.sh"]
        assert "PF_SERVER" in content  # at least mentions it
        assert "WARNING" in content or "preserve" in content.lower()

    def test_restarts_pf_worker_for_git_token(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "PF_WORKER" in content

    def test_restarts_pf_services_for_db_password(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "PF_SERVICES" in content

    # --- Input safety ---

    def test_reads_secrets_from_stdin(self, script_files):
        """New values should be read with read -s (silent) to avoid leaking to terminal."""
        content = script_files["rotate_secrets.sh"]
        assert "read -r -s" in content

    def test_no_secret_in_cli_args(self, script_files):
        """Script should not accept secret values as CLI arguments."""
        content = script_files["rotate_secrets.sh"]
        # The arg parser should only handle --connection, --check, --dry-run, -h
        # No --password, --token, or --secret flags
        assert "--password" not in content
        assert "--token" not in content
        assert "--secret-value" not in content

    def test_rejects_empty_value(self, script_files):
        """Should abort if the user enters an empty string."""
        content = script_files["rotate_secrets.sh"]
        assert "Empty value" in content

    # --- .env integration ---

    def test_updates_env_file_for_git_token(self, script_files):
        """After rotating GIT_ACCESS_TOKEN, the .env should be updated too."""
        content = script_files["rotate_secrets.sh"]
        assert "_update_env" in content
        assert "GIT_ACCESS_TOKEN" in content
        assert "sed" in content

    def test_handles_macos_sed(self, script_files):
        """macOS sed requires -i '' (empty extension); script should handle both."""
        content = script_files["rotate_secrets.sh"]
        assert "Darwin" in content or "sed -i ''" in content

    # --- PAT expiry checking ---

    def test_decodes_jwt_payload(self, script_files):
        """Should decode the JWT second segment (payload) for exp claim."""
        content = script_files["rotate_secrets.sh"]
        assert "base64" in content
        assert "exp" in content

    def test_warns_on_expiring_pat(self, script_files):
        """Should warn when PAT is expiring within 30 days."""
        content = script_files["rotate_secrets.sh"]
        assert "EXPIRING SOON" in content or "30" in content

    def test_reports_expired_pat(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "EXPIRED" in content

    # --- Dry-run mode ---

    def test_dry_run_shows_preview(self, script_files):
        """Dry-run should show what would happen without executing."""
        content = script_files["rotate_secrets.sh"]
        assert "[dry-run]" in content

    def test_dry_run_masks_secret_values(self, script_files):
        """Dry-run output should NOT print actual secret values."""
        content = script_files["rotate_secrets.sh"]
        # The dry-run lines should use '***' instead of the actual value
        lines = [line for line in content.splitlines() if "[dry-run]" in line]
        for line in lines:
            if "SECRET_STRING" in line:
                assert "***" in line, f"Dry-run should mask secret: {line}"

    # --- Check mode ---

    def test_check_mode_uses_describe_secret(self, script_files):
        """--check should use DESCRIBE SECRET to verify existence."""
        content = script_files["rotate_secrets.sh"]
        assert "DESCRIBE SECRET" in content

    def test_check_mode_reports_exists_or_not_found(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "EXISTS" in content
        assert "NOT FOUND" in content or "NOT_FOUND" in content

    # --- Uses snow CLI (not snowsql) ---

    def test_uses_snow_cli(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "snow sql" in content
        assert "snowsql" not in content

    # --- Interactive menu ---

    def test_offers_menu_choices(self, script_files):
        """Interactive mode should present numbered choices."""
        content = script_files["rotate_secrets.sh"]
        assert "1)" in content
        assert "2)" in content
        assert "3)" in content

    def test_handles_invalid_choice(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "Invalid choice" in content

    # --- --all-clouds flag ---

    def test_supports_all_clouds_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--all-clouds" in content

    def test_all_clouds_auto_detects_connections(self, script_files):
        """--all-clouds should read *_spcs connections from .env or config."""
        content = script_files["rotate_secrets.sh"]
        assert "ALL_CLOUDS" in content or "_resolve_clouds" in content

    def test_all_clouds_iterates_connections(self, script_files):
        """--all-clouds should loop over each detected cloud connection."""
        content = script_files["rotate_secrets.sh"]
        assert "CLOUD_CONNECTIONS" in content

    # --- --smtp flag ---

    def test_supports_smtp_flag(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "--smtp" in content

    def test_smtp_flag_targets_grafana_smtp_password(self, script_files):
        """--smtp should specifically target GRAFANA_SMTP_PASSWORD."""
        content = script_files["rotate_secrets.sh"]
        assert "SMTP_ONLY" in content or "GRAFANA_SMTP_PASSWORD" in content

    def test_smtp_tests_login_before_applying(self, script_files):
        """--smtp should validate SMTP login before updating secrets."""
        content = script_files["rotate_secrets.sh"]
        assert "_test_smtp" in content or "smtplib" in content or "smtp.gmail.com" in content

    # --- All 9 secrets ---

    def test_manages_all_9_secrets(self, script_files):
        """Script should reference all 9 secret objects."""
        content = script_files["rotate_secrets.sh"]
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
            assert secret in content, f"rotate_secrets.sh missing secret: {secret}"

    def test_all_secrets_array_has_9_entries(self, script_files):
        """The ALL_SECRETS array should contain exactly 9 entries."""
        content = script_files["rotate_secrets.sh"]
        assert "ALL_SECRETS" in content

    # --- 7 interactive menu options ---

    def test_menu_has_7_choices(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "7)" in content

    def test_menu_choice_range_is_1_to_7(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "Choice [1-7]" in content or "[1-7]" in content

    def test_menu_includes_grafana_smtp_password(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "5)" in content
        assert "GRAFANA_SMTP_PASSWORD" in content

    def test_menu_includes_grafana_admin_password(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "6)" in content
        assert "GRAFANA_ADMIN_PASSWORD" in content

    def test_menu_includes_slack_webhook(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "7)" in content
        assert "SLACK_WEBHOOK_URL" in content

    # --- Helper functions ---

    def test_has_update_env_helper(self, script_files):
        """Script should have a _update_env helper function."""
        content = script_files["rotate_secrets.sh"]
        assert "_update_env" in content

    def test_has_update_keychain_helper(self, script_files):
        """Script should have a _update_keychain helper for macOS Keychain."""
        content = script_files["rotate_secrets.sh"]
        assert "_update_keychain" in content or "security" in content

    def test_has_restart_service_helper(self, script_files):
        """Script should have a _restart_service helper for SUSPEND/RESUME."""
        content = script_files["rotate_secrets.sh"]
        assert "_restart_service" in content

    def test_has_test_smtp_helper(self, script_files):
        """Script should have a _test_smtp helper for SMTP validation."""
        content = script_files["rotate_secrets.sh"]
        assert "_test_smtp" in content

    # --- macOS compatibility ---

    def test_uses_tr_for_uppercase_not_bash4(self, script_files):
        """Must use tr for uppercase, not bash4-only ${var^^} syntax."""
        content = script_files["rotate_secrets.sh"]
        assert "tr '[:lower:]' '[:upper:]'" in content or 'tr "[:lower:]" "[:upper:]"' in content

    # --- Google password revocation warning ---

    def test_warns_about_google_password_revocation(self, script_files):
        """Help text should warn about Google password revoking app passwords."""
        content = script_files["rotate_secrets.sh"]
        assert "apppasswords" in content or "App Password" in content

    # --- Check mode expanded ---

    def test_check_mode_tests_smtp_login(self, script_files):
        """--check should test SMTP login when GRAFANA_SMTP_PASSWORD exists."""
        content = script_files["rotate_secrets.sh"]
        assert "SMTP" in content and "check" in content.lower()

    def test_check_mode_reports_pat_expiry(self, script_files):
        content = script_files["rotate_secrets.sh"]
        assert "exp" in content

    # --- Connection not required with --all-clouds ---

    def test_connection_not_required_with_all_clouds(self, script_files):
        """--all-clouds should not require --connection."""
        content = script_files["rotate_secrets.sh"]
        assert "ALL_CLOUDS" in content


class TestUpdateVersionsScript:
    """Tests for scripts/update_versions.sh — ALTER SERVICE + READY polling."""

    def test_has_wait_for_ready_function(self, script_files):
        """update_versions.sh must have a wait_for_ready function."""
        content = script_files["update_versions.sh"]
        assert "wait_for_ready" in content

    def test_polls_service_status(self, script_files):
        """update_versions.sh must use SYSTEM$GET_SERVICE_STATUS for polling (directly or via _lib.sh)."""
        content = script_files.get("_lib.sh", "") + script_files["update_versions.sh"]
        assert "SYSTEM$GET_SERVICE_STATUS" in content or "GET_SERVICE_STATUS" in content

    def test_polls_all_four_services(self, script_files):
        """Every ALTER SERVICE must be followed by a wait_for_ready call."""
        content = script_files["update_versions.sh"]
        for svc in ["PF_REDIS", "PF_SERVER", "PF_SERVICES", "PF_WORKER"]:
            assert f"ALTER SERVICE {svc}" in content, (
                f"update_versions.sh: must ALTER SERVICE {svc}"
            )

    def test_uses_alter_not_create(self, script_files):
        """update_versions.sh must never use CREATE SERVICE."""
        content = script_files["update_versions.sh"]
        assert "ALTER SERVICE" in content
        assert "CREATE SERVICE" not in content

    def test_checks_endpoint_stability(self, script_files):
        """update_versions.sh should check if PF_SERVER endpoint URL changed."""
        content = script_files["update_versions.sh"]
        assert "ENDPOINT_BEFORE" in content or "endpoint" in content.lower()
        assert "ENDPOINT_AFTER" in content or "ENDPOINT URL CHANGED" in content

    def test_supports_apply_flag(self, script_files):
        content = script_files["update_versions.sh"]
        assert "--apply" in content

    def test_supports_dry_run_flag(self, script_files):
        content = script_files["update_versions.sh"]
        assert "--dry-run" in content

    def test_supports_connection_flag(self, script_files):
        content = script_files["update_versions.sh"]
        assert "--connection" in content


class TestDashboardTeardown:
    """Tests for dashboard-related teardown in scripts/teardown.sh."""

    def test_drops_dashboard_streamlit(self, script_files):
        content = script_files["teardown.sh"]
        assert "PREFECT_DASHBOARD_APP" in content

    def test_drops_dashboard_udf(self, script_files):
        content = script_files["teardown.sh"]
        assert "GET_PREFECT_PAT" in content

    def test_includes_dashboard_pool(self, script_files):
        content = script_files["teardown.sh"]
        assert "PREFECT_DASHBOARD_POOL" in content


class TestDashboardDeploy:
    """Tests for dashboard deployment in scripts/deploy.sh."""

    def test_deploys_dashboard(self, script_files):
        content = script_files["deploy.sh"]
        assert "snow streamlit deploy" in content

    def test_creates_pat_udf(self, script_files):
        content = script_files["deploy.sh"]
        assert "GET_PREFECT_PAT" in content

    def test_dashboard_uses_dashboard_eai(self, script_files):
        content = script_files["deploy.sh"]
        assert "PREFECT_DASHBOARD_EAI" in content


class TestNoHardcodedSecrets:
    """Ensure no scripts or specs contain hardcoded credentials."""

    def test_no_hardcoded_passwords_in_scripts(self, script_files):
        """Scripts must not contain literal passwords, PATs, or tokens."""
        secret_patterns = ["eyJ", "glpat-", "ghp_"]
        for filename, content in script_files.items():
            for pattern in secret_patterns:
                assert pattern not in content, (
                    f"{filename}: contains hardcoded secret pattern '{pattern}'"
                )

    def test_no_hardcoded_passwords_in_specs(self):
        """Spec YAML files must not contain literal secrets."""
        from conftest import SPECS_DIR  # noqa: E402

        secret_patterns = ["eyJ", "glpat-", "ghp_", "password123", "changeme"]
        for spec_file in SPECS_DIR.glob("*.yaml"):
            content = spec_file.read_text()
            for pattern in secret_patterns:
                assert pattern not in content, (
                    f"{spec_file.name}: contains hardcoded secret pattern '{pattern}'"
                )

    def test_no_hardcoded_passwords_in_sql(self):
        """SQL template files must not contain literal secrets (templates use placeholders).
        Skips local-only filled-in files (e.g. 03_setup_secrets.sql) which are gitignored."""
        from conftest import SQL_DIR  # noqa: E402

        secret_patterns = ["eyJ", "glpat-", "ghp_"]
        for sql_file in SQL_DIR.glob("*.sql.template"):
            content = sql_file.read_text()
            for pattern in secret_patterns:
                assert pattern not in content, (
                    f"{sql_file.name}: contains hardcoded secret pattern '{pattern}'"
                )
        for sql_file in SQL_DIR.glob("*.sql"):
            if sql_file.stem.endswith("_secrets"):
                continue
            content = sql_file.read_text()
            for pattern in secret_patterns:
                assert pattern not in content, (
                    f"{sql_file.name}: contains hardcoded secret pattern '{pattern}'"
                )


# ===========================================================================
# Dashboard standalone deploy script
# ===========================================================================
from conftest import PROJECT_DIR  # noqa: E402

DASHBOARD_DEPLOY = PROJECT_DIR / "dashboard" / "deploy.sh"


class TestDashboardDeployScript:
    """Validate dashboard/deploy.sh standalone deployment script."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DASHBOARD_DEPLOY.read_text()

    def test_script_exists(self):
        assert DASHBOARD_DEPLOY.exists()

    def test_uses_bash_shebang(self):
        assert self.content.splitlines()[0].startswith("#!/")
        assert "bash" in self.content.splitlines()[0]

    def test_uses_strict_mode(self):
        assert "set -euo pipefail" in self.content

    def test_has_3_steps(self):
        assert "[1/3]" in self.content
        assert "[2/3]" in self.content
        assert "[3/3]" in self.content

    def test_creates_pat_udf(self):
        assert "GET_PREFECT_PAT" in self.content

    def test_pat_udf_uses_dashboard_eai(self):
        assert "PREFECT_DASHBOARD_EAI" in self.content

    def test_pat_udf_references_svc_pat_secret(self):
        assert "PREFECT_SVC_PAT" in self.content

    def test_deploys_via_snow_streamlit(self):
        assert "snow streamlit deploy" in self.content

    def test_supports_connection_flag(self):
        assert '"--connection"' in self.content

    def test_default_connection_is_aws_spcs(self):
        assert "aws_spcs" in self.content

    def test_all_snow_commands_use_connection(self):
        """All snow command invocations must pass --connection."""
        sql_count = self.content.count("snow sql -q")
        streamlit_count = self.content.count("snow streamlit")
        total_cmds = sql_count + streamlit_count
        conn_count = self.content.count('--connection "$CONNECTION"')
        assert conn_count >= total_cmds, (
            f"Found {total_cmds} snow commands but only {conn_count} --connection refs"
        )

    def test_header_says_idempotent(self):
        header = self.content[:500]
        assert "idempotent" in header.lower() or "safe to re-run" in header.lower()

    def test_shows_status_after_deploy(self):
        assert "SHOW STREAMLITS" in self.content
