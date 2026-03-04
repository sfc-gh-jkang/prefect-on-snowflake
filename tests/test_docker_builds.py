"""Tests for Dockerfiles.

Validates FROM images, correct base images, and build context.
"""

import pytest
from conftest import GCP_DIR, IMAGES_DIR, POSTGRES_TAG, PYTHON_TAG, REDIS_TAG


class TestDockerfileDiscovery:
    @pytest.mark.parametrize("img", ["prefect", "postgres", "redis"])
    def test_dockerfile_exists(self, img):
        assert (IMAGES_DIR / img / "Dockerfile").exists()

    def test_gcp_worker_dockerfile_exists(self):
        assert (GCP_DIR / "Dockerfile.worker").exists()


class TestDockerfileContent:
    def test_postgres_uses_correct_version(self, dockerfile_contents):
        assert f"FROM postgres:{POSTGRES_TAG}" in dockerfile_contents["postgres"]

    def test_redis_uses_correct_version(self, dockerfile_contents):
        assert f"FROM redis:{REDIS_TAG}" in dockerfile_contents["redis"]

    def test_prefect_uses_correct_python_base(self, dockerfile_contents):
        assert f"FROM python:{PYTHON_TAG}" in dockerfile_contents["prefect"]

    def test_prefect_uses_uv_sync(self, dockerfile_contents):
        content = dockerfile_contents["prefect"]
        assert "uv sync" in content

    def test_prefect_copies_pyproject(self, dockerfile_contents):
        content = dockerfile_contents["prefect"]
        assert "pyproject.toml" in content

    def test_gcp_worker_uses_prefect_base(self, dockerfile_contents):
        assert "FROM prefecthq/prefect" in dockerfile_contents["gcp-worker"]


class TestPyprojectToml:
    def test_pyproject_exists(self):
        assert (IMAGES_DIR / "prefect" / "pyproject.toml").exists()

    def test_pyproject_has_snowflake_connector(self):
        content = (IMAGES_DIR / "prefect" / "pyproject.toml").read_text()
        assert "snowflake-connector-python" in content

    def test_pyproject_has_requests(self):
        content = (IMAGES_DIR / "prefect" / "pyproject.toml").read_text()
        assert "requests" in content

    def test_pyproject_has_prefect_redis(self):
        content = (IMAGES_DIR / "prefect" / "pyproject.toml").read_text()
        assert "prefect-redis" in content, "Worker image pyproject.toml must include prefect-redis"

    def test_pyproject_requires_python_312(self):
        content = (IMAGES_DIR / "prefect" / "pyproject.toml").read_text()
        assert "3.12" in content, "Worker image pyproject.toml must target Python 3.12+"

    def test_pyproject_has_build_system(self):
        content = (IMAGES_DIR / "prefect" / "pyproject.toml").read_text()
        assert "[build-system]" in content, (
            "Worker image pyproject.toml must have a [build-system] section"
        )


class TestDockerfileQuality:
    def test_no_dockerfiles_use_latest_tag(self, dockerfile_contents):
        for name, content in dockerfile_contents.items():
            for line in content.splitlines():
                if line.strip().startswith("FROM "):
                    assert ":latest" not in line, (
                        f"{name} Dockerfile uses :latest tag — pin a specific version"
                    )

    def test_gcp_worker_uses_prefect_base(self, dockerfile_contents):
        content = dockerfile_contents.get("gcp-worker", "")
        assert "prefecthq/prefect" in content, (
            "GCP worker Dockerfile must use prefecthq/prefect base image"
        )
