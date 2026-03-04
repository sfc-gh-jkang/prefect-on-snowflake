"""Tests for pools.yaml — work pool configuration schema and cross-references.

Validates structure, required fields, uniqueness constraints, and
consistency with SPCS specs and GCP worker compose files.
"""

import pytest
import yaml
from conftest import POOLS, POOLS_FILE, PROJECT_DIR, SPECS_DIR, WORKERS_DIR


class TestPoolsYamlStructure:
    def test_file_exists(self):
        assert POOLS_FILE.exists(), "pools.yaml must exist at project root"

    def test_parses_as_valid_yaml(self):
        data = yaml.safe_load(POOLS_FILE.read_text())
        assert isinstance(data, dict)
        assert "pools" in data, "pools.yaml must have a top-level 'pools' key"

    def test_has_at_least_one_pool(self):
        assert len(POOLS) >= 1, "pools.yaml must define at least one pool"


class TestPoolsYamlSchema:
    REQUIRED_FIELDS = ["type", "pool_name", "suffix", "tag"]

    @pytest.mark.parametrize("pool_key", list(POOLS.keys()))
    def test_required_fields_present(self, pool_key):
        cfg = POOLS[pool_key]
        for field in self.REQUIRED_FIELDS:
            assert field in cfg, f"Pool '{pool_key}' missing required field '{field}'"

    @pytest.mark.parametrize("pool_key", list(POOLS.keys()))
    def test_type_is_valid(self, pool_key):
        pool_type = POOLS[pool_key].get("type", "")
        assert pool_type in ("spcs", "external"), (
            f"Pool '{pool_key}' has invalid type '{pool_type}' (expected 'spcs' or 'external')"
        )

    def test_no_duplicate_pool_names(self):
        names = [cfg["pool_name"] for cfg in POOLS.values()]
        assert len(names) == len(set(names)), (
            f"Duplicate pool_name values: {[n for n in names if names.count(n) > 1]}"
        )

    def test_no_duplicate_suffixes(self):
        suffixes = [cfg["suffix"] for cfg in POOLS.values()]
        assert len(suffixes) == len(set(suffixes)), (
            f"Duplicate suffix values: {[s for s in suffixes if suffixes.count(s) > 1]}"
        )


class TestExternalPoolWorkerDirs:
    EXTERNAL = {k: v for k, v in POOLS.items() if v.get("type") == "external"}

    @pytest.mark.parametrize("pool_key", list(EXTERNAL.keys()))
    def test_external_pools_have_worker_dir(self, pool_key):
        cfg = POOLS[pool_key]
        assert "worker_dir" in cfg, f"External pool '{pool_key}' must have 'worker_dir' field"

    @pytest.mark.parametrize("pool_key", list(EXTERNAL.keys()))
    def test_external_worker_dirs_exist(self, pool_key):
        cfg = POOLS[pool_key]
        worker_dir = PROJECT_DIR / cfg["worker_dir"]
        assert worker_dir.is_dir(), (
            f"Pool '{pool_key}': worker_dir '{cfg['worker_dir']}' does not exist at {worker_dir}"
        )


class TestPoolNameCrossReferences:
    def test_spcs_pool_name_matches_worker_spec(self):
        """pools.yaml spcs pool_name must match --pool arg in pf_worker.yaml."""
        spcs_cfg = POOLS.get("spcs")
        if spcs_cfg is None:
            pytest.skip("No 'spcs' pool defined")
        expected_pool = spcs_cfg["pool_name"]
        spec = yaml.safe_load((SPECS_DIR / "pf_worker.yaml").read_text())
        cmd = spec["spec"]["containers"][0].get("command", [])
        # Find the value after --pool
        for i, arg in enumerate(cmd):
            if arg == "--pool" and i + 1 < len(cmd):
                actual_pool = cmd[i + 1]
                assert actual_pool == expected_pool, (
                    f"pf_worker.yaml --pool '{actual_pool}' doesn't match "
                    f"pools.yaml spcs pool_name '{expected_pool}'"
                )
                return
        pytest.fail("pf_worker.yaml command does not contain --pool flag")

    def test_gcp_pool_name_matches_gcp_compose(self):
        """pools.yaml gcp pool_name must match --pool arg in GCP compose."""
        gcp_cfg = POOLS.get("gcp")
        if gcp_cfg is None:
            pytest.skip("No 'gcp' pool defined")
        expected_pool = gcp_cfg["pool_name"]
        compose_path = WORKERS_DIR / "gcp" / "docker-compose.gcp.yaml"
        compose_data = yaml.safe_load(compose_path.read_text())
        for name, svc in compose_data["services"].items():
            if "worker" in name.lower():
                cmd = svc.get("command", "")
                if isinstance(cmd, str):
                    assert expected_pool in cmd, (
                        f"GCP compose worker command doesn't contain pool name '{expected_pool}'"
                    )
                    return
        pytest.fail("GCP compose has no worker service with --pool in command")
