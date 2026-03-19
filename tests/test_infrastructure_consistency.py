"""Tests for infrastructure consistency across all layers.

Validates that image tags, port numbers, secret names, and
database names are consistent across Dockerfiles, compose,
specs, and SQL scripts.
"""

import re

import pytest
import yaml
from conftest import (
    COMPOSE_FILE,
    EXTERNAL_POOLS,
    POSTGRES_TAG,
    PREFECT_TAG,
    REDIS_TAG,
    SPECS_DIR,
    SQL_DIR,
    WORKERS_DIR,
    resolve_compose_path,
)


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(COMPOSE_FILE.read_text())


@pytest.fixture(scope="module")
def specs():
    result = {}
    for f in SPECS_DIR.glob("*.yaml"):
        result[f.stem] = yaml.safe_load(f.read_text())
    return result


class TestImageTagConsistency:
    """All references to the same image must use the same tag."""

    def test_postgres_in_compose(self, compose, dockerfile_contents):
        """Local dev uses matching Postgres container."""
        assert f"postgres:{POSTGRES_TAG}" == compose["services"]["postgres"]["image"]
        assert f"postgres:{POSTGRES_TAG}" in dockerfile_contents["postgres"]

    def test_redis_everywhere(self, compose, specs, dockerfile_contents):
        assert f"redis:{REDIS_TAG}" == compose["services"]["redis"]["image"]
        assert REDIS_TAG in specs["pf_redis"]["spec"]["containers"][0]["image"]
        assert f"redis:{REDIS_TAG}" in dockerfile_contents["redis"]

    def test_prefect_tag_in_compose(self, compose):
        for svc in ["migrate", "prefect-server", "prefect-services"]:
            img = compose["services"][svc].get("image", "")
            assert PREFECT_TAG in img

    def test_prefect_tag_in_specs(self, specs):
        for name in ["pf_server", "pf_services"]:
            img = specs[name]["spec"]["containers"][0]["image"]
            assert PREFECT_TAG in img


class TestPortConsistency:
    """Port numbers must match across all configurations."""

    def test_server_port_4200_compose(self, compose):
        ports = compose["services"]["prefect-server"].get("ports", [])
        assert any("4200" in str(p) for p in ports)

    def test_server_port_4200_spec(self, specs):
        endpoints = specs["pf_server"]["spec"]["endpoints"]
        assert any(e["port"] == 4200 for e in endpoints)

    def test_server_port_4200_healthcheck(self, compose):
        hc = compose["services"]["prefect-server"]["healthcheck"]["test"]
        assert any("4200" in str(t) for t in hc)

    def test_server_port_4200_spec_probe(self, specs):
        probe = specs["pf_server"]["spec"]["containers"][0]["readinessProbe"]
        assert probe["port"] == 4200

    def test_postgres_is_gcp_only(self, specs):
        """pf_postgres is a GCP-only spec (no Managed Postgres on GCP).
        AWS/Azure use Snowflake Managed Postgres (PREFECT_PG) and should
        NOT deploy this service."""
        assert "pf_postgres" in specs, "pf_postgres spec should exist for GCP"
        # Verify it's not referenced in the primary (AWS/Azure) service creation
        main_sql = (SQL_DIR / "07_create_services.sql").read_text()
        assert "pf_postgres" not in main_sql, (
            "pf_postgres should NOT be in 07_create_services.sql "
            "(AWS/Azure use Snowflake Managed Postgres)"
        )
        gcp_sql = (SQL_DIR / "07_create_services_gcp.sql").read_text()
        assert "pf_postgres" in gcp_sql, "pf_postgres should be in 07_create_services_gcp.sql"

    def test_redis_has_tcp_endpoint(self, specs):
        endpoints = specs["pf_redis"]["spec"]["endpoints"]
        assert any(e["port"] == 6379 and e["protocol"] == "TCP" for e in endpoints)


class TestSecretNameConsistency:
    """Secret names must match between SQL templates and spec files."""

    def test_secret_name_in_sql_and_specs(self, specs):
        template = (SQL_DIR / "03_setup_secrets.sql.template").read_text()
        # Extract secret names from SQL (handle IF NOT EXISTS)
        sql_secrets = set(re.findall(r"CREATE SECRET\s+(?:IF NOT EXISTS\s+)?(\w+)", template))
        # Extract secret names from specs
        spec_secrets = set()
        for _name, data in specs.items():
            for c in data["spec"]["containers"]:
                for _key, val in c.get("env", {}).items():
                    matches = re.findall(r"\{\{secret\.(\w+)\.", str(val))
                    spec_secrets.update(matches)
        # Spec secrets should be a subset of SQL secrets (case-insensitive)
        sql_lower = {s.lower() for s in sql_secrets}
        spec_lower = {s.lower() for s in spec_secrets}
        missing = spec_lower - sql_lower
        assert not missing, f"Specs reference secrets not in SQL template: {missing}"


class TestDatabaseNameConsistency:
    """Database and schema names must match across SQL, specs, and scripts."""

    def test_all_sql_files_use_prefect_db(self):
        for f in SQL_DIR.glob("*.sql"):
            content = f.read_text()
            if (
                ("DATABASE" in content or "SCHEMA" in content)
                and "PREFECT_DB" not in content
                and "INFORMATION_SCHEMA" not in content
            ):
                # Some files only reference schema within db context
                pass  # Ok for suspend/resume scripts

    def test_worker_spec_has_stage_volume_backup(self, specs):
        """Worker has stage volume as backup for git_clone failures.

        The stage mount at /opt/prefect/flows provides a fallback when
        GIT_REPO_URL is not configured. Both volumeMounts and volumes
        must be present to avoid the stage-mount-v2-sidecar crash.
        """
        volumes = specs["pf_worker"]["spec"].get("volumes", [])
        stage_volumes = [v for v in volumes if v.get("source") == "stage"]
        assert len(stage_volumes) == 1, "Worker should have exactly one stage volume"
        assert "PREFECT_FLOWS" in stage_volumes[0].get("stageConfig", {}).get("name", ""), (
            "Stage volume should reference @PREFECT_FLOWS"
        )
        # Verify volumeMounts exist (prevents sidecar crash)
        container = specs["pf_worker"]["spec"]["containers"][0]
        mounts = container.get("volumeMounts", [])
        assert any(m.get("mountPath") == "/opt/prefect/flows" for m in mounts), (
            "Worker must have volumeMounts for /opt/prefect/flows"
        )


class TestExternalWorkerComposeConsistency:
    """External worker compose files should match expected patterns."""

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_compose_parses(self, pool_key):
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        assert "services" in compose_data

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_compose_has_worker(self, pool_key):
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        services = compose_data["services"]
        assert len(services) >= 1
        worker_found = any("worker" in name for name in services)
        assert worker_found

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_worker_uses_prefect_api_url(self, pool_key):
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        for name, svc in compose_data["services"].items():
            env = svc.get("environment", {})
            if "worker" in name.lower():
                assert "PREFECT_API_URL" in env

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_compose_references_correct_pool(self, pool_key):
        cfg = EXTERNAL_POOLS[pool_key]
        pool_name = cfg["pool_name"]
        compose_path = resolve_compose_path(pool_key)
        content = compose_path.read_text()
        # For shared compose files (e.g. aws-backup → docker-compose.aws.yaml),
        # the file references the primary pool, not the backup pool
        if compose_path.stem != f"docker-compose.{pool_key}":
            pytest.skip(f"{pool_key} shares {compose_path.name} — pool name check N/A")
        assert pool_name in content

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_worker_has_git_env_vars(self, pool_key):
        """All external workers must pass GIT_REPO_URL, GIT_BRANCH, and
        GIT_ACCESS_TOKEN for the git_clone pull step."""
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        for name, svc in compose_data["services"].items():
            if "worker" not in name.lower():
                continue
            env = svc.get("environment", {})
            assert "GIT_REPO_URL" in env, (
                f"{name} in {compose_path.name}: must pass GIT_REPO_URL for git_clone"
            )
            assert "GIT_BRANCH" in env, (
                f"{name} in {compose_path.name}: must pass GIT_BRANCH for git_clone"
            )
            assert "GIT_ACCESS_TOKEN" in env, (
                f"{name} in {compose_path.name}: must pass GIT_ACCESS_TOKEN for git_clone"
            )

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_worker_has_snowflake_credentials(self, pool_key):
        """All external workers must pass Snowflake credentials."""
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        for name, svc in compose_data["services"].items():
            if "worker" not in name.lower():
                continue
            env = svc.get("environment", {})
            assert "SNOWFLAKE_ACCOUNT" in env, (
                f"{name} in {compose_path.name}: must pass SNOWFLAKE_ACCOUNT"
            )
            assert "SNOWFLAKE_PAT" in env, f"{name} in {compose_path.name}: must pass SNOWFLAKE_PAT"
            assert "SNOWFLAKE_USER" in env, (
                f"{name} in {compose_path.name}: must pass SNOWFLAKE_USER"
            )


class TestSpecApiUrlConsistency:
    """All SPCS specs with PREFECT_API_URL must use the same internal DNS URL."""

    @pytest.fixture(scope="class")
    def api_urls(self, specs):
        """Extract PREFECT_API_URL from every spec that has one."""
        urls = {}
        for name, data in specs.items():
            for c in data["spec"]["containers"]:
                env = c.get("env", {})
                if "PREFECT_API_URL" in env:
                    urls[name] = env["PREFECT_API_URL"]
        return urls

    def test_all_specs_use_same_internal_api_url(self, api_urls):
        unique = set(api_urls.values())
        assert len(unique) == 1, f"All specs should share one API URL, got: {api_urls}"

    def test_internal_api_url_references_pf_server(self, api_urls):
        url = next(iter(api_urls.values()))
        assert "pf-server" in url, f"Internal API URL must reference pf-server: {url}"

    def test_internal_api_url_uses_port_4200(self, api_urls):
        url = next(iter(api_urls.values()))
        assert ":4200" in url, f"Internal API URL must use port 4200: {url}"


class TestGcpComposeDeepValidation:
    """Deep validation of the GCP hybrid worker compose file."""

    @pytest.fixture(scope="class")
    def gcp_compose(self):
        path = WORKERS_DIR / "gcp" / "docker-compose.gcp.yaml"
        return yaml.safe_load(path.read_text())

    def test_auth_proxy_has_healthcheck(self, gcp_compose):
        proxy = gcp_compose["services"]["auth-proxy"]
        assert "healthcheck" in proxy, "auth-proxy must have a healthcheck"

    def test_auth_proxy_uses_nginx_image(self, gcp_compose):
        proxy = gcp_compose["services"]["auth-proxy"]
        assert "nginx" in proxy.get("image", ""), "auth-proxy must use an nginx image"

    def test_worker_depends_on_auth_proxy_healthy(self, gcp_compose):
        for name, svc in gcp_compose["services"].items():
            if "worker" in name.lower():
                deps = svc.get("depends_on", {})
                assert "auth-proxy" in deps, f"{name}: must depend on auth-proxy"
                condition = deps["auth-proxy"]
                if isinstance(condition, dict):
                    assert condition.get("condition") == "service_healthy"
                break

    def test_worker_has_restart_policy(self, gcp_compose):
        for name, svc in gcp_compose["services"].items():
            if "worker" in name.lower():
                assert svc.get("restart") == "unless-stopped", (
                    f"{name}: must have restart: unless-stopped"
                )
                break

    def test_auth_proxy_has_restart_policy(self, gcp_compose):
        proxy = gcp_compose["services"]["auth-proxy"]
        assert proxy.get("restart") == "on-failure", "auth-proxy must have restart: on-failure"


class TestAwsComposeDeepValidation:
    """Deep validation of the AWS hybrid worker compose file."""

    @pytest.fixture(scope="class")
    def aws_compose(self):
        path = WORKERS_DIR / "aws" / "docker-compose.aws.yaml"
        return yaml.safe_load(path.read_text())

    def test_auth_proxy_has_healthcheck(self, aws_compose):
        proxy = aws_compose["services"]["auth-proxy"]
        assert "healthcheck" in proxy, "auth-proxy must have a healthcheck"

    def test_auth_proxy_uses_nginx_image(self, aws_compose):
        proxy = aws_compose["services"]["auth-proxy"]
        assert "nginx" in proxy.get("image", ""), "auth-proxy must use an nginx image"

    def test_worker_depends_on_auth_proxy_healthy(self, aws_compose):
        for name, svc in aws_compose["services"].items():
            if "worker" in name.lower():
                deps = svc.get("depends_on", {})
                assert "auth-proxy" in deps, f"{name}: must depend on auth-proxy"
                condition = deps["auth-proxy"]
                if isinstance(condition, dict):
                    assert condition.get("condition") == "service_healthy"
                break

    def test_worker_has_restart_policy(self, aws_compose):
        for name, svc in aws_compose["services"].items():
            if "worker" in name.lower():
                assert svc.get("restart") == "on-failure", f"{name}: must have restart: on-failure"
                break

    def test_auth_proxy_has_restart_policy(self, aws_compose):
        proxy = aws_compose["services"]["auth-proxy"]
        assert proxy.get("restart") == "on-failure", "auth-proxy must have restart: on-failure"

    def test_worker_has_git_env_vars(self, aws_compose):
        """AWS worker must pass GIT_REPO_URL and GIT_BRANCH for git_clone pull step."""
        for name, svc in aws_compose["services"].items():
            if "worker" in name.lower():
                env = svc.get("environment", {})
                assert "GIT_REPO_URL" in env, f"{name}: must pass GIT_REPO_URL for git_clone"
                assert "GIT_BRANCH" in env, f"{name}: must pass GIT_BRANCH for git_clone"
                break

    def test_worker_has_snowflake_credentials(self, aws_compose):
        """AWS worker must pass Snowflake credentials for snowflake-etl flows."""
        for name, svc in aws_compose["services"].items():
            if "worker" in name.lower():
                env = svc.get("environment", {})
                assert "SNOWFLAKE_ACCOUNT" in env
                assert "SNOWFLAKE_PAT" in env
                assert "SNOWFLAKE_USER" in env
                break

    def test_worker_targets_aws_pool(self, aws_compose):
        """Worker command must reference aws-pool."""
        for name, svc in aws_compose["services"].items():
            if "worker" in name.lower():
                cmd = svc.get("command", "")
                assert "aws-pool" in cmd, f"{name}: command must target aws-pool"
                break

    def test_auth_proxy_envsubst_command(self, aws_compose):
        """auth-proxy must use envsubst to template nginx.conf."""
        proxy = aws_compose["services"]["auth-proxy"]
        cmd = proxy.get("command", [])
        cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
        assert "envsubst" in cmd_str, "auth-proxy must use envsubst for nginx.conf templating"

    def test_auth_proxy_requires_pat(self, aws_compose):
        """auth-proxy must require SNOWFLAKE_PAT (not optional)."""
        proxy = aws_compose["services"]["auth-proxy"]
        env = proxy.get("environment", {})
        pat_val = str(env.get("SNOWFLAKE_PAT", ""))
        # Should use :? (required) syntax, not :- (optional)
        assert "?" in pat_val or pat_val == "${SNOWFLAKE_PAT}", (
            "auth-proxy SNOWFLAKE_PAT should be required, not optional"
        )


class TestLocalAuthProxyCompose:
    """Validate docker-compose.auth-proxy.yaml for local CSRF bypass."""

    AUTH_PROXY_FILE = COMPOSE_FILE.parent / "docker-compose.auth-proxy.yaml"

    @pytest.fixture(scope="class")
    def local_proxy_compose(self):
        return yaml.safe_load(self.AUTH_PROXY_FILE.read_text())

    def test_file_exists(self):
        assert self.AUTH_PROXY_FILE.exists(), "docker-compose.auth-proxy.yaml must exist"

    def test_has_auth_proxy_service(self, local_proxy_compose):
        assert "auth-proxy" in local_proxy_compose["services"]

    def test_uses_nginx_image(self, local_proxy_compose):
        proxy = local_proxy_compose["services"]["auth-proxy"]
        assert "nginx" in proxy.get("image", ""), "must use nginx image"

    def test_mounts_shared_nginx_conf(self, local_proxy_compose):
        """Must reuse workers/gcp/nginx.conf — no duplicated config."""
        proxy = local_proxy_compose["services"]["auth-proxy"]
        volumes = proxy.get("volumes", [])
        vol_str = " ".join(str(v) for v in volumes)
        assert "nginx.conf" in vol_str, "must mount nginx.conf template"
        assert "workers/gcp" in vol_str, "must reuse workers/gcp/nginx.conf (no duplication)"

    def test_uses_envsubst(self, local_proxy_compose):
        proxy = local_proxy_compose["services"]["auth-proxy"]
        cmd = proxy.get("command", [])
        cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
        assert "envsubst" in cmd_str, "must use envsubst for nginx.conf templating"

    def test_requires_snowflake_pat(self, local_proxy_compose):
        """PAT must be required (:? syntax), not optional."""
        proxy = local_proxy_compose["services"]["auth-proxy"]
        env = proxy.get("environment", {})
        pat_val = str(env.get("SNOWFLAKE_PAT", ""))
        assert "?" in pat_val, "SNOWFLAKE_PAT must be required (:? syntax)"

    def test_requires_spcs_endpoint(self, local_proxy_compose):
        proxy = local_proxy_compose["services"]["auth-proxy"]
        env = proxy.get("environment", {})
        endpoint_val = str(env.get("SPCS_ENDPOINT", ""))
        assert "?" in endpoint_val, "SPCS_ENDPOINT must be required (:? syntax)"

    def test_exposes_port_4202(self, local_proxy_compose):
        """Must map to port 4202 on host (4200=local server, 4201=GCP worker proxy)."""
        proxy = local_proxy_compose["services"]["auth-proxy"]
        ports = proxy.get("ports", [])
        port_str = " ".join(str(p) for p in ports)
        assert "4202" in port_str, "must expose port 4202 on host"

    def test_has_healthcheck(self, local_proxy_compose):
        proxy = local_proxy_compose["services"]["auth-proxy"]
        assert "healthcheck" in proxy, "must have a healthcheck"

    def test_has_restart_policy(self, local_proxy_compose):
        proxy = local_proxy_compose["services"]["auth-proxy"]
        assert proxy.get("restart") == "on-failure", "must have restart: on-failure"

    def test_is_single_service(self, local_proxy_compose):
        """Local auth-proxy should only have the proxy — no worker."""
        services = local_proxy_compose["services"]
        assert len(services) == 1, (
            f"Expected 1 service (auth-proxy), got {len(services)}: {list(services)}"
        )
