"""Tests for docker-compose.yaml configuration.

Validates services, dependencies, health checks, volumes,
ports, environment variables, and startup ordering.
"""

import pytest
import yaml
from conftest import COMPOSE_FILE, POSTGRES_TAG, REDIS_TAG


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(COMPOSE_FILE.read_text())


EXPECTED_SERVICES = [
    "postgres",
    "redis",
    "migrate",
    "prefect-server",
    "prefect-services",
    "prefect-worker",
]


class TestServiceDiscovery:
    @pytest.mark.parametrize("svc", EXPECTED_SERVICES)
    def test_service_exists(self, svc, compose):
        assert svc in compose["services"], f"Missing service: {svc}"

    def test_service_count(self, compose):
        assert len(compose["services"]) == 6

    def test_no_unexpected_services(self, compose):
        actual = set(compose["services"].keys())
        expected = set(EXPECTED_SERVICES)
        unexpected = actual - expected
        assert not unexpected, f"Unexpected services: {unexpected}"


class TestServiceImages:
    def test_postgres_image(self, compose):
        assert compose["services"]["postgres"]["image"] == f"postgres:{POSTGRES_TAG}"

    def test_redis_image(self, compose):
        assert compose["services"]["redis"]["image"] == f"redis:{REDIS_TAG}"

    def test_migrate_image(self, compose):
        assert "prefect" in compose["services"]["migrate"]["image"]

    def test_server_image(self, compose):
        assert "prefect" in compose["services"]["prefect-server"]["image"]

    def test_services_image(self, compose):
        assert "prefect" in compose["services"]["prefect-services"]["image"]

    def test_worker_uses_build(self, compose):
        assert "build" in compose["services"]["prefect-worker"]


class TestHealthChecks:
    HEALTHCHECKED = ["postgres", "redis", "prefect-server"]

    @pytest.mark.parametrize("svc", HEALTHCHECKED)
    def test_has_healthcheck(self, svc, compose):
        assert "healthcheck" in compose["services"][svc], f"{svc}: missing healthcheck"

    @pytest.mark.parametrize("svc", HEALTHCHECKED)
    def test_healthcheck_has_test(self, svc, compose):
        hc = compose["services"][svc]["healthcheck"]
        assert "test" in hc, f"{svc}: healthcheck missing 'test'"

    def test_postgres_healthcheck_command(self, compose):
        test = compose["services"]["postgres"]["healthcheck"]["test"]
        assert any("pg_isready" in str(t) for t in test)

    def test_redis_healthcheck_command(self, compose):
        test = compose["services"]["redis"]["healthcheck"]["test"]
        assert any("redis-cli" in str(t) for t in test)

    def test_server_healthcheck_uses_api_health(self, compose):
        test = compose["services"]["prefect-server"]["healthcheck"]["test"]
        test_str = " ".join(str(t) for t in test)
        assert "/api/health" in test_str


class TestDependencies:
    def test_migrate_depends_on_postgres(self, compose):
        deps = compose["services"]["migrate"]["depends_on"]
        assert "postgres" in deps

    def test_migrate_waits_for_postgres_healthy(self, compose):
        deps = compose["services"]["migrate"]["depends_on"]
        assert deps["postgres"]["condition"] == "service_healthy"

    def test_server_depends_on_migrate_completed(self, compose):
        deps = compose["services"]["prefect-server"]["depends_on"]
        assert "migrate" in deps
        assert deps["migrate"]["condition"] == "service_completed_successfully"

    def test_server_depends_on_postgres_healthy(self, compose):
        deps = compose["services"]["prefect-server"]["depends_on"]
        assert deps["postgres"]["condition"] == "service_healthy"

    def test_server_depends_on_redis_healthy(self, compose):
        deps = compose["services"]["prefect-server"]["depends_on"]
        assert deps["redis"]["condition"] == "service_healthy"

    def test_services_depends_on_server(self, compose):
        deps = compose["services"]["prefect-services"]["depends_on"]
        assert "prefect-server" in deps
        assert deps["prefect-server"]["condition"] == "service_healthy"

    def test_worker_depends_on_server(self, compose):
        deps = compose["services"]["prefect-worker"]["depends_on"]
        assert "prefect-server" in deps
        assert deps["prefect-server"]["condition"] == "service_healthy"


class TestPorts:
    def test_postgres_exposes_5432(self, compose):
        ports = compose["services"]["postgres"].get("ports", [])
        assert any("5432" in str(p) for p in ports)

    def test_server_exposes_4200(self, compose):
        ports = compose["services"]["prefect-server"].get("ports", [])
        assert any("4200" in str(p) for p in ports)

    def test_redis_does_not_expose_ports(self, compose):
        """Redis is internal-only in local dev."""
        ports = compose["services"]["redis"].get("ports", [])
        assert len(ports) == 0, "Redis should not expose ports"


class TestVolumes:
    def test_postgres_has_volume(self, compose):
        vols = compose["services"]["postgres"].get("volumes", [])
        assert any("postgres_data" in str(v) for v in vols)

    def test_redis_has_volume(self, compose):
        vols = compose["services"]["redis"].get("volumes", [])
        assert any("redis_data" in str(v) for v in vols)

    def test_worker_mounts_flows(self, compose):
        vols = compose["services"]["prefect-worker"].get("volumes", [])
        assert any("flows" in str(v) for v in vols)

    def test_named_volumes_defined(self, compose):
        assert "postgres_data" in compose.get("volumes", {})
        assert "redis_data" in compose.get("volumes", {})


class TestEnvironment:
    DB_URL_KEY = "PREFECT_API_DATABASE_CONNECTION_URL"

    @pytest.mark.parametrize("svc", ["migrate", "prefect-server", "prefect-services"])
    def test_has_db_url(self, svc, compose):
        env = compose["services"][svc].get("environment", {})
        assert self.DB_URL_KEY in env, f"{svc}: missing {self.DB_URL_KEY}"

    @pytest.mark.parametrize("svc", ["migrate", "prefect-server", "prefect-services"])
    def test_db_url_uses_asyncpg(self, svc, compose):
        env = compose["services"][svc]["environment"]
        url = env[self.DB_URL_KEY]
        assert "postgresql+asyncpg" in url

    @pytest.mark.parametrize("svc", ["prefect-server", "prefect-services"])
    def test_migrate_on_start_disabled(self, svc, compose):
        env = compose["services"][svc]["environment"]
        val = env.get("PREFECT_API_DATABASE_MIGRATE_ON_START", "")
        assert val == "false"

    @pytest.mark.parametrize("svc", ["prefect-server", "prefect-services"])
    def test_redis_messaging_config(self, svc, compose):
        env = compose["services"][svc]["environment"]
        assert env.get("PREFECT_MESSAGING_BROKER") == "prefect_redis.messaging"
        assert env.get("PREFECT_MESSAGING_CACHE") == "prefect_redis.messaging"

    def test_worker_has_api_url(self, compose):
        env = compose["services"]["prefect-worker"]["environment"]
        assert "PREFECT_API_URL" in env

    def test_worker_api_url_points_to_server(self, compose):
        env = compose["services"]["prefect-worker"]["environment"]
        url = env["PREFECT_API_URL"]
        assert "prefect-server" in url
        assert "4200" in url


class TestCommands:
    def test_migrate_command(self, compose):
        cmd = compose["services"]["migrate"]["command"]
        assert "database" in cmd
        assert "upgrade" in cmd

    def test_server_no_services_flag(self, compose):
        cmd = compose["services"]["prefect-server"]["command"]
        assert "--no-services" in cmd

    def test_services_command(self, compose):
        cmd = compose["services"]["prefect-services"]["command"]
        assert "services" in cmd
        assert "start" in cmd

    def test_worker_command(self, compose):
        cmd = compose["services"]["prefect-worker"]["command"]
        assert "worker" in cmd
        assert "start" in cmd
        assert "--pool" in cmd


class TestBestPractices:
    """Docker compose security and reliability best practices."""

    def test_worker_has_restart_policy(self, compose):
        svc = compose["services"]["prefect-worker"]
        assert svc.get("restart") == "on-failure", "prefect-worker should have restart: on-failure"

    def test_migrate_has_no_restart_or_restart_no(self, compose):
        """migrate is a one-shot container; it should not auto-restart."""
        svc = compose["services"]["migrate"]
        restart = svc.get("restart", "no")
        assert restart in ("no", '"no"'), (
            f"migrate is one-shot and should have restart 'no', got '{restart}'"
        )

    def test_no_privileged_containers(self, compose):
        for name, svc in compose["services"].items():
            assert svc.get("privileged") is not True, f"{name}: should not run as privileged"

    def test_no_host_network_mode(self, compose):
        for name, svc in compose["services"].items():
            assert svc.get("network_mode") != "host", f"{name}: should not use host network mode"
