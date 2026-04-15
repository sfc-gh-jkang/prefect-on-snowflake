"""Tests for SPCS service specification YAML files.

Validates structure, required fields, image references, probes,
volumes, endpoints, and env vars for all SPCS specs.
"""

import pytest
import yaml
from conftest import SPECS_DIR

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
EXPECTED_SPECS = [
    "pf_redis",
    "pf_server",
    "pf_services",
    "pf_worker",
    "pf_migrate",
    "pf_deploy_job",
    "pf_trigger_job",
    "pf_postgres",
]


class TestSpecDiscovery:
    """Validate that all expected spec files exist."""

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_spec_file_exists(self, name):
        assert (SPECS_DIR / f"{name}.yaml").exists(), f"Missing spec: {name}.yaml"

    def test_no_unexpected_specs(self):
        actual = {f.stem for f in SPECS_DIR.glob("*.yaml")}
        expected = set(EXPECTED_SPECS)
        unexpected = actual - expected
        assert not unexpected, f"Unexpected spec files: {unexpected}"


class TestSpecStructure:
    """Validate YAML structure of each spec."""

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_spec_parses_as_valid_yaml(self, name):
        content = (SPECS_DIR / f"{name}.yaml").read_text()
        data = yaml.safe_load(content)
        assert isinstance(data, dict), f"{name}: root must be a dict"

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_spec_has_top_level_spec_key(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        assert "spec" in data, f"{name}: missing top-level 'spec' key"

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_spec_has_containers(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        containers = data["spec"].get("containers", [])
        assert len(containers) >= 1, f"{name}: must have at least one container"

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_container_has_name(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for c in data["spec"]["containers"]:
            assert "name" in c, f"{name}: container missing 'name'"

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_container_has_image(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for c in data["spec"]["containers"]:
            assert "image" in c, f"{name}: container missing 'image'"

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_container_name_matches_filename(self, name):
        """Container name should be the hyphenated version of the spec filename."""
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        expected_name = name.replace("_", "-")
        container_names = [c["name"] for c in data["spec"]["containers"]]
        assert expected_name in container_names, (
            f"{name}: expected container named '{expected_name}', got {container_names}"
        )


class TestSpecImages:
    """Validate image references in specs."""

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_image_has_registry_prefix(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for c in data["spec"]["containers"]:
            img = c["image"]
            assert img.startswith("/"), (
                f"{name}: image '{img}' should start with '/' (SPCS registry path)"
            )

    @pytest.mark.parametrize("name", EXPECTED_SPECS)
    def test_image_has_tag(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for c in data["spec"]["containers"]:
            img = c["image"]
            assert ":" in img, f"{name}: image '{img}' should have a tag"


class TestSpecReadinessProbes:
    """Validate readiness probes for stateful services."""

    PROBED_SERVICES = ["pf_server", "pf_worker"]

    @pytest.mark.parametrize("name", PROBED_SERVICES)
    def test_has_readiness_probe(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        container = data["spec"]["containers"][0]
        assert "readinessProbe" in container, f"{name}: missing readinessProbe"

    @pytest.mark.parametrize("name", PROBED_SERVICES)
    def test_readiness_probe_has_port(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        probe = data["spec"]["containers"][0]["readinessProbe"]
        assert "port" in probe, f"{name}: readinessProbe missing 'port'"

    # Only HTTP probes need a path; TCP probes (redis) just need a port
    HTTP_PROBED_SERVICES = ["pf_server", "pf_worker"]

    @pytest.mark.parametrize("name", HTTP_PROBED_SERVICES)
    def test_readiness_probe_has_path(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        probe = data["spec"]["containers"][0]["readinessProbe"]
        assert "path" in probe, f"{name}: readinessProbe missing 'path'"

    # SPCS only supports HTTP readiness probes; redis uses TCP endpoint instead
    def test_redis_has_tcp_endpoint(self):
        data = yaml.safe_load((SPECS_DIR / "pf_redis.yaml").read_text())
        endpoints = data["spec"].get("endpoints", [])
        assert any(e["port"] == 6379 and e["protocol"] == "TCP" for e in endpoints), (
            "pf_redis: expected TCP endpoint on port 6379"
        )

    def test_server_probe_port(self, spec_files):
        probe = spec_files["pf_server"]["spec"]["containers"][0]["readinessProbe"]
        assert probe["port"] == 4200

    def test_server_probe_path(self, spec_files):
        probe = spec_files["pf_server"]["spec"]["containers"][0]["readinessProbe"]
        assert probe["path"] == "/api/health"

    def test_worker_probe_port(self, spec_files):
        probe = spec_files["pf_worker"]["spec"]["containers"][0]["readinessProbe"]
        assert probe["port"] == 8080

    def test_worker_probe_path(self, spec_files):
        probe = spec_files["pf_worker"]["spec"]["containers"][0]["readinessProbe"]
        assert probe["path"] == "/health"

    def test_worker_enables_webserver(self, spec_files):
        env = spec_files["pf_worker"]["spec"]["containers"][0]["env"]
        assert env["PREFECT_WORKER_WEBSERVER_HOST"] == "0.0.0.0"
        assert env["PREFECT_WORKER_WEBSERVER_PORT"] == "8080"


class TestSpecVolumes:
    """Validate volume configuration."""

    STATEFUL_SERVICES = ["pf_redis"]

    @pytest.mark.parametrize("name", STATEFUL_SERVICES)
    def test_has_volumes(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        assert "volumes" in data["spec"], f"{name}: missing volumes"

    @pytest.mark.parametrize("name", STATEFUL_SERVICES)
    def test_uses_block_storage(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for vol in data["spec"]["volumes"]:
            assert vol.get("source") == "block", f"{name}: volume should use block storage"

    @pytest.mark.parametrize("name", STATEFUL_SERVICES)
    def test_block_volume_has_size(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        for vol in data["spec"]["volumes"]:
            if vol.get("source") == "block":
                assert "size" in vol, f"{name}: block volume missing size"

    @pytest.mark.parametrize("name", STATEFUL_SERVICES)
    def test_container_has_volume_mounts(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        container = data["spec"]["containers"][0]
        assert "volumeMounts" in container, f"{name}: missing volumeMounts"

    def test_worker_has_stage_volume_as_backup(self, spec_files):
        """Worker has stage volumes for flows backup and observe-agent config.

        Primary code delivery is git_clone (prefect.yaml). The stage mount at
        /opt/prefect/flows is a fallback when GIT_REPO_URL is not configured.
        The observe-config stage mount provides observe-agent sidecar config.
        Both volumeMounts and volumes must be present to avoid sidecar crashes.
        """
        worker = spec_files["pf_worker"]
        volumes = worker["spec"].get("volumes", [])
        stage_volumes = [v for v in volumes if v.get("source") == "stage" or "stageConfig" in v]
        assert len(stage_volumes) == 2, (
            "Worker should have exactly two stage volumes (flows + observe-config)"
        )
        stage_names = {v.get("stageConfig", {}).get("name", "") for v in stage_volumes}
        assert any("PREFECT_FLOWS" in n for n in stage_names), (
            "Stage volumes should include @PREFECT_FLOWS"
        )

        # volumeMounts must exist — without them the sidecar crashes
        container = worker["spec"]["containers"][0]
        mounts = container.get("volumeMounts", [])
        flow_mounts = [m for m in mounts if m.get("mountPath") == "/opt/prefect/flows"]
        assert len(flow_mounts) == 1, (
            "Worker container must have volumeMounts for /opt/prefect/flows "
            "(missing volumeMounts causes sidecar crash)"
        )

    @pytest.mark.parametrize("name", ["pf_deploy_job"])
    def test_deploy_jobs_have_stage_volume(self, name):
        data = yaml.safe_load((SPECS_DIR / f"{name}.yaml").read_text())
        volumes = data["spec"].get("volumes", [])
        assert len(volumes) >= 1, f"{name} should have a stage volume for flows"
        has_flows_stage = False
        for v in volumes:
            source = v.get("source", "")
            if "PREFECT_FLOWS" in source:
                has_flows_stage = True
            stage_config = v.get("stageConfig", {})
            if "PREFECT_FLOWS" in stage_config.get("name", ""):
                has_flows_stage = True
        assert has_flows_stage, f"{name} volume should reference @PREFECT_FLOWS stage"

    def test_deploy_job_has_deploy_flags_env(self):
        """Unified deploy job spec must expose DEPLOY_FLAGS for GCP override."""
        data = yaml.safe_load((SPECS_DIR / "pf_deploy_job.yaml").read_text())
        container = data["spec"]["containers"][0]
        env = container.get("env", {})
        assert "DEPLOY_FLAGS" in env, "pf_deploy_job must have DEPLOY_FLAGS env var"
        assert env["DEPLOY_FLAGS"] == "", "DEPLOY_FLAGS default must be empty string"

    def test_deploy_job_command_uses_deploy_flags(self):
        """Deploy job command must pass $DEPLOY_FLAGS to deploy.py."""
        data = yaml.safe_load((SPECS_DIR / "pf_deploy_job.yaml").read_text())
        container = data["spec"]["containers"][0]
        cmd = " ".join(container.get("command", []))
        assert "$DEPLOY_FLAGS" in cmd, "command must include $DEPLOY_FLAGS"
        assert "deploy.py" in cmd, "command must invoke deploy.py"

    def test_no_separate_gcp_deploy_job_spec(self):
        """pf_deploy_gcp_job.yaml should not exist (merged into pf_deploy_job)."""
        assert not (SPECS_DIR / "pf_deploy_gcp_job.yaml").exists(), (
            "pf_deploy_gcp_job.yaml should be deleted — use DEPLOY_FLAGS env var instead"
        )


class TestSpecEndpoints:
    """Validate endpoint configuration."""

    def test_server_has_endpoints(self, spec_files):
        endpoints = spec_files["pf_server"]["spec"].get("endpoints", [])
        assert len(endpoints) >= 1, "Server must have at least one endpoint"

    def test_server_endpoint_is_public(self, spec_files):
        endpoints = spec_files["pf_server"]["spec"]["endpoints"]
        api_endpoints = [e for e in endpoints if e.get("name") == "api"]
        assert len(api_endpoints) == 1, "Server must have an 'api' endpoint"
        assert api_endpoints[0].get("public") is True, "API endpoint must be public"

    def test_server_endpoint_port(self, spec_files):
        endpoints = spec_files["pf_server"]["spec"]["endpoints"]
        api = [e for e in endpoints if e.get("name") == "api"][0]
        assert api["port"] == 4200

    @pytest.mark.parametrize("name", ["pf_redis", "pf_services", "pf_worker"])
    def test_non_server_has_no_public_endpoint(self, name, spec_files):
        endpoints = spec_files[name]["spec"].get("endpoints", [])
        public = [e for e in endpoints if e.get("public")]
        assert not public, f"{name}: should not have public endpoints"


class TestSpecEnvVars:
    """Validate environment variable configuration."""

    DB_SERVICES = ["pf_server", "pf_services"]

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_has_database_connection_url(self, name, spec_files):
        """DB connection URL is injected via SPCS secrets field, not env."""
        secrets = spec_files[name]["spec"]["containers"][0].get("secrets", [])
        env_var_names = [s.get("envVarName") for s in secrets]
        assert "PREFECT_API_DATABASE_CONNECTION_URL" in env_var_names

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_migrate_on_start_disabled(self, name, spec_files):
        env = spec_files[name]["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_API_DATABASE_MIGRATE_ON_START") == "false"

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_has_redis_messaging_config(self, name, spec_files):
        env = spec_files[name]["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_MESSAGING_BROKER") == "prefect_redis.messaging"
        assert env.get("PREFECT_MESSAGING_CACHE") == "prefect_redis.messaging"
        assert "PREFECT_REDIS_MESSAGING_HOST" in env
        assert "PREFECT_REDIS_MESSAGING_PORT" in env

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_redis_host_uses_dns_name(self, name, spec_files):
        env = spec_files[name]["spec"]["containers"][0].get("env", {})
        host = env.get("PREFECT_REDIS_MESSAGING_HOST", "")
        assert "pf-redis" in host, f"{name}: Redis host should contain 'pf-redis', got '{host}'"

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_db_url_uses_snowflake_postgres(self, name, spec_files):
        """DB URL is injected via SPCS secrets — verify secret structure."""
        secrets = spec_files[name]["spec"]["containers"][0].get("secrets", [])
        db_secrets = [
            s for s in secrets if s.get("envVarName") == "PREFECT_API_DATABASE_CONNECTION_URL"
        ]
        assert len(db_secrets) == 1, f"{name}: should have exactly one DB URL secret"
        assert db_secrets[0]["secretKeyRef"] == "secret_string"

    @pytest.mark.parametrize("name", DB_SERVICES)
    def test_db_url_uses_secret(self, name, spec_files):
        """DB password is injected via snowflakeSecret, not template syntax."""
        secrets = spec_files[name]["spec"]["containers"][0].get("secrets", [])
        db_secrets = [
            s for s in secrets if s.get("envVarName") == "PREFECT_API_DATABASE_CONNECTION_URL"
        ]
        assert len(db_secrets) == 1, f"{name}: should have DB URL secret"
        assert "snowflakeSecret" in db_secrets[0]
        assert "objectName" in db_secrets[0]["snowflakeSecret"]

    def test_worker_has_api_url(self, spec_files):
        env = spec_files["pf_worker"]["spec"]["containers"][0].get("env", {})
        assert "PREFECT_API_URL" in env

    def test_worker_api_url_points_to_server(self, spec_files):
        env = spec_files["pf_worker"]["spec"]["containers"][0].get("env", {})
        url = env.get("PREFECT_API_URL", "")
        assert "pf-server" in url, "Worker API URL should reference pf-server"
        assert ":4200" in url, "Worker API URL should use port 4200"

    def test_migrate_has_database_connection_url(self, spec_files):
        """Migrate spec injects DB URL via SPCS secrets field."""
        secrets = spec_files["pf_migrate"]["spec"]["containers"][0].get("secrets", [])
        db_secrets = [
            s for s in secrets if s.get("envVarName") == "PREFECT_API_DATABASE_CONNECTION_URL"
        ]
        assert len(db_secrets) == 1, "Migrate should have DB URL secret"
        assert db_secrets[0]["snowflakeSecret"]["objectName"] == "prefect_db_password"


class TestSpecCommands:
    """Validate container commands match expected Prefect CLI patterns."""

    def test_server_command(self, spec_files):
        cmd = spec_files["pf_server"]["spec"]["containers"][0].get("command", [])
        cmd_str = " ".join(cmd)
        assert "prefect" in cmd_str
        assert "server" in cmd_str
        assert "start" in cmd_str
        assert "--no-services" in cmd_str

    def test_services_command(self, spec_files):
        cmd = spec_files["pf_services"]["spec"]["containers"][0].get("command", [])
        cmd_str = " ".join(cmd)
        assert "prefect" in cmd_str
        assert "server" in cmd_str
        assert "services" in cmd_str
        assert "start" in cmd_str

    def test_worker_command(self, spec_files):
        cmd = spec_files["pf_worker"]["spec"]["containers"][0].get("command", [])
        cmd_str = " ".join(cmd)
        assert "prefect" in cmd_str
        assert "worker" in cmd_str
        assert "start" in cmd_str
        assert "--pool" in cmd_str
        assert "--with-healthcheck" in cmd_str

    def test_server_binds_all_interfaces(self, spec_files):
        cmd = spec_files["pf_server"]["spec"]["containers"][0].get("command", [])
        env = spec_files["pf_server"]["spec"]["containers"][0].get("env", {})
        has_host_flag = "0.0.0.0" in cmd
        has_host_env = env.get("PREFECT_SERVER_API_HOST") == "0.0.0.0"
        assert has_host_flag or has_host_env, "Server must bind to 0.0.0.0"

    def test_server_disables_analytics(self, spec_files):
        """SPCS cannot reach api.prefect.io — analytics must be disabled."""
        env = spec_files["pf_server"]["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_SERVER_ANALYTICS_ENABLED") == "false"

    def test_services_disables_analytics(self, spec_files):
        env = spec_files["pf_services"]["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_SERVER_ANALYTICS_ENABLED") == "false"


class TestPostgresSpec:
    """Validate the GCP-only Postgres container spec."""

    def test_postgres_has_tcp_endpoint(self, spec_files):
        endpoints = spec_files["pf_postgres"]["spec"].get("endpoints", [])
        assert any(e["port"] == 5432 and e.get("protocol") == "TCP" for e in endpoints), (
            "pf_postgres: expected TCP endpoint on port 5432"
        )

    def test_postgres_has_block_storage(self, spec_files):
        volumes = spec_files["pf_postgres"]["spec"].get("volumes", [])
        assert any(v.get("source") == "block" for v in volumes), (
            "pf_postgres: should use block storage for data persistence"
        )

    def test_postgres_password_uses_secret(self, spec_files):
        """Password must come from a Snowflake secret, not a hardcoded env var."""
        container = spec_files["pf_postgres"]["spec"]["containers"][0]
        secrets = container.get("secrets", [])
        pw_secrets = [s for s in secrets if s.get("envVarName") == "POSTGRES_PASSWORD"]
        assert len(pw_secrets) == 1, "POSTGRES_PASSWORD should be injected via snowflakeSecret"
        assert "snowflakeSecret" in pw_secrets[0]

    def test_postgres_has_pgdata_env(self, spec_files):
        env = spec_files["pf_postgres"]["spec"]["containers"][0].get("env", {})
        assert "PGDATA" in env, "pf_postgres: should set PGDATA for data directory"

    def test_postgres_has_volume_mount(self, spec_files):
        container = spec_files["pf_postgres"]["spec"]["containers"][0]
        mounts = container.get("volumeMounts", [])
        assert any("/pgdata" in m.get("mountPath", "") for m in mounts), (
            "pf_postgres: should mount volume at /pgdata"
        )
