"""Cross-file consistency tests.

Validates that compose, SPCS specs, SQL scripts, and shell scripts
reference the same objects consistently (3-axis drift detection).
"""

import re

import pytest
import yaml
from conftest import (
    COMPOSE_FILE,
    EXTERNAL_POOLS,
    FLOWS_DIR,
    MONITORING_DIR,
    PROJECT_DIR,
    SCRIPTS_DIR,
    SPECS_DIR,
    SQL_DIR,
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


@pytest.fixture(scope="module")
def all_sql():
    content = ""
    for f in sorted(SQL_DIR.glob("*.sql")) + sorted(SQL_DIR.glob("*.sql.template")):
        content += f.read_text() + "\n"
    return content


# ---------------------------------------------------------------------------
# Vertical: Compose ↔ SPCS spec consistency
# ---------------------------------------------------------------------------
class TestComposeSpecConsistency:
    """Ensure compose and SPCS specs agree on images and env vars."""

    def test_postgres_image_consistent_in_compose(self, compose):
        """Local dev compose uses Postgres; SPCS uses Snowflake Postgres (no SPCS spec)."""
        compose_img = compose["services"]["postgres"]["image"]
        assert "postgres:" in compose_img

    def test_redis_image_versions_match(self, compose, specs):
        compose_img = compose["services"]["redis"]["image"]
        spec_img = specs["pf_redis"]["spec"]["containers"][0]["image"]
        compose_tag = compose_img.split(":")[-1]
        spec_tag = spec_img.split(":")[-1]
        assert compose_tag == spec_tag

    def test_server_migrate_on_start_matches(self, compose, specs):
        compose_val = compose["services"]["prefect-server"]["environment"].get(
            "PREFECT_API_DATABASE_MIGRATE_ON_START"
        )
        spec_val = specs["pf_server"]["spec"]["containers"][0]["env"].get(
            "PREFECT_API_DATABASE_MIGRATE_ON_START"
        )
        assert compose_val == spec_val

    def test_redis_messaging_broker_matches(self, compose, specs):
        for svc_compose, svc_spec in [
            ("prefect-server", "pf_server"),
            ("prefect-services", "pf_services"),
        ]:
            compose_val = compose["services"][svc_compose]["environment"].get(
                "PREFECT_MESSAGING_BROKER"
            )
            spec_val = specs[svc_spec]["spec"]["containers"][0]["env"].get(
                "PREFECT_MESSAGING_BROKER"
            )
            assert compose_val == spec_val, f"PREFECT_MESSAGING_BROKER mismatch: {svc_compose}"

    def test_redis_port_matches(self, compose, specs):
        for svc_compose, svc_spec in [
            ("prefect-server", "pf_server"),
            ("prefect-services", "pf_services"),
        ]:
            compose_val = compose["services"][svc_compose]["environment"].get(
                "PREFECT_REDIS_MESSAGING_PORT"
            )
            spec_val = specs[svc_spec]["spec"]["containers"][0]["env"].get(
                "PREFECT_REDIS_MESSAGING_PORT"
            )
            assert compose_val == spec_val


# ---------------------------------------------------------------------------
# Horizontal: Spec-to-spec consistency
# ---------------------------------------------------------------------------
class TestSpecToSpecConsistency:
    """All SPCS specs should use the same image registry prefix, secret names, etc."""

    def test_all_specs_use_same_registry_prefix(self, specs):
        prefixes = set()
        for _name, data in specs.items():
            for c in data["spec"]["containers"]:
                img = c["image"]
                # Extract registry path: everything before the last /
                parts = img.rsplit("/", 1)
                if len(parts) > 1:
                    prefixes.add(parts[0])
        assert len(prefixes) == 1, f"Multiple registry prefixes: {prefixes}"

    def test_db_services_use_same_secret_name(self, specs):
        """pf_server, pf_services, pf_migrate should reference the same DB password secret."""
        secret_names = set()
        for name in ["pf_server", "pf_services", "pf_migrate"]:
            secrets = specs[name]["spec"]["containers"][0].get("secrets", [])
            for s in secrets:
                obj_name = s.get("snowflakeSecret", {}).get("objectName")
                if obj_name:
                    secret_names.add(obj_name)
        assert len(secret_names) == 1, (
            f"DB services should use the same secret, found: {secret_names}"
        )

    def test_server_and_services_share_redis_config(self, specs):
        """pf_server and pf_services must have identical Redis messaging env vars."""
        redis_keys = [
            "PREFECT_MESSAGING_BROKER",
            "PREFECT_MESSAGING_CACHE",
            "PREFECT_REDIS_MESSAGING_HOST",
            "PREFECT_REDIS_MESSAGING_PORT",
            "PREFECT_REDIS_MESSAGING_DB",
        ]
        server_env = specs["pf_server"]["spec"]["containers"][0]["env"]
        services_env = specs["pf_services"]["spec"]["containers"][0]["env"]
        for key in redis_keys:
            assert server_env.get(key) == services_env.get(key), (
                f"Redis config mismatch on {key}: "
                f"server={server_env.get(key)}, services={services_env.get(key)}"
            )


# ---------------------------------------------------------------------------
# Infrastructure: SQL ↔ Spec ↔ Script consistency
# ---------------------------------------------------------------------------
class TestInfrastructureConsistency:
    """SQL scripts reference the same database, schema, roles, etc. as specs and scripts."""

    def test_sql_references_prefect_db(self, all_sql):
        assert "PREFECT_DB" in all_sql

    def test_sql_references_prefect_schema(self, all_sql):
        assert "PREFECT_SCHEMA" in all_sql

    def test_sql_references_prefect_role(self, all_sql):
        assert "PREFECT_ROLE" in all_sql

    def test_sql_creates_all_compute_pools(self, all_sql):
        for pool in ["PREFECT_INFRA_POOL", "PREFECT_CORE_POOL", "PREFECT_WORKER_POOL"]:
            assert pool in all_sql, f"SQL missing compute pool: {pool}"

    def test_sql_creates_image_repo(self, all_sql):
        assert "PREFECT_REPOSITORY" in all_sql

    def test_sql_creates_stages(self, all_sql):
        assert "PREFECT_FLOWS" in all_sql
        assert "PREFECT_SPECS" in all_sql

    def test_sql_creates_eai(self, all_sql):
        assert "PREFECT_WORKER_EAI" in all_sql

    def test_sql_creates_all_services(self, all_sql):
        for svc in ["PF_REDIS", "PF_SERVER", "PF_SERVICES", "PF_WORKER"]:
            assert svc in all_sql, f"SQL missing service: {svc}"

    def test_spec_filenames_match_sql_references(self, specs, all_sql):
        """SQL should reference each spec filename."""
        # Utility specs used for ad-hoc jobs, not in main service creation SQL
        utility_specs = {"pf_trigger_job"}
        for name in specs:
            if name in utility_specs:
                continue
            assert f"{name}.yaml" in all_sql, f"SQL doesn't reference spec file: {name}.yaml"

    def test_worker_eai_in_create_service(self, all_sql):
        """Worker service creation should include EAI."""
        # Find the CREATE SERVICE for PF_WORKER and check EXTERNAL_ACCESS_INTEGRATIONS
        assert "EXTERNAL_ACCESS_INTEGRATIONS" in all_sql
        assert "PREFECT_WORKER_EAI" in all_sql

    def test_suspend_resumes_all_pools(self):
        suspend = (SQL_DIR / "09_suspend_all.sql").read_text()
        resume = (SQL_DIR / "10_resume_all.sql").read_text()
        for pool in ["PREFECT_INFRA_POOL", "PREFECT_CORE_POOL", "PREFECT_WORKER_POOL"]:
            assert pool in suspend, f"Suspend script missing pool: {pool}"
            assert pool in resume, f"Resume script missing pool: {pool}"


# ---------------------------------------------------------------------------
# 07b_update_services.sql ↔ Spec file consistency
# ---------------------------------------------------------------------------
class TestUpdateServiceConsistency:
    """Verify 07b_update_services.sql references match actual spec files
    and covers all long-running services."""

    LONG_RUNNING_SERVICES = ["PF_REDIS", "PF_SERVER", "PF_SERVICES", "PF_WORKER"]

    def test_07b_spec_filenames_match_actual_specs(self, specs):
        """Every SPECIFICATION_FILE in 07b must point to an actual spec file."""
        update_sql = (SQL_DIR / "07b_update_services.sql").read_text()
        spec_refs = re.findall(r"SPECIFICATION_FILE\s*=\s*'([^']+)'", update_sql)
        assert spec_refs, "07b_update_services.sql has no SPECIFICATION_FILE references"
        for ref in spec_refs:
            spec_name = ref.replace(".yaml", "")
            assert spec_name in specs, (
                f"07b_update_services.sql references '{ref}' but file not in specs/"
            )

    def test_07b_covers_all_long_running_services(self):
        """07b must ALTER every long-running service."""
        update_sql = (SQL_DIR / "07b_update_services.sql").read_text()
        for service in self.LONG_RUNNING_SERVICES:
            assert service in update_sql, f"07b_update_services.sql: must update service {service}"

    def test_07_and_07b_share_spec_file_refs(self):
        """Spec files in 07b must be a subset of those in 07 (07 has extra job specs)."""
        create_sql = (SQL_DIR / "07_create_services.sql").read_text()
        update_sql = (SQL_DIR / "07b_update_services.sql").read_text()
        create_specs = set(re.findall(r"SPECIFICATION_FILE\s*=\s*'([^']+)'", create_sql))
        update_specs = set(re.findall(r"SPECIFICATION_FILE\s*=\s*'([^']+)'", update_sql))
        assert update_specs.issubset(create_specs), (
            f"07b references specs not found in 07_create: {update_specs - create_specs}"
        )

    def test_every_long_running_spec_on_disk_in_07b(self, specs):
        """Reverse check: every long-running service spec file on disk must be
        referenced in 07b_update_services.sql. A new spec added to specs/ but
        missing from 07b would go undetected without this test."""
        update_sql = (SQL_DIR / "07b_update_services.sql").read_text()
        spec_refs = set(re.findall(r"SPECIFICATION_FILE\s*=\s*'([^']+)'", update_sql))
        # Long-running service specs follow naming convention pf_<name>.yaml
        # Exclude job/utility specs (pf_migrate, pf_trigger_job, pf_deploy_*)
        # Exclude pf_postgres — GCP-only, not in primary 07b (AWS/Azure) update script
        job_prefixes = ("pf_migrate", "pf_trigger", "pf_deploy")
        gcp_only_specs = {"pf_postgres"}
        long_running_specs = {
            f"{name}.yaml"
            for name in specs
            if not any(name.startswith(p) for p in job_prefixes) and name not in gcp_only_specs
        }
        missing = long_running_specs - spec_refs
        assert not missing, (
            f"Spec files on disk not referenced in 07b_update_services.sql: {missing}. "
            f"Add ALTER SERVICE statements for these services."
        )

    def test_deploy_sh_update_branch_matches_07b_services(self):
        """deploy.sh update branch must poll the same services listed in 07b."""
        deploy_sh = (SCRIPTS_DIR / "deploy.sh").read_text()
        update_sql = (SQL_DIR / "07b_update_services.sql").read_text()
        # Extract services from 07b
        sql_services = set(re.findall(r"ALTER SERVICE\s+(\w+)", update_sql))
        # Extract services from deploy.sh update branch (the for loop)
        update_start = deploy_sh.find("if ${UPDATE_MODE}")
        if update_start == -1:
            update_start = deploy_sh.find("if $UPDATE_MODE")
        else_pos = deploy_sh.find("\nelse", update_start)
        update_branch = deploy_sh[update_start:else_pos]
        # Find all PF_ service names in the update branch
        branch_services = set(re.findall(r"\bPF_\w+\b", update_branch))
        # Every service in 07b must appear in the update branch
        missing = sql_services - branch_services
        assert not missing, (
            f"deploy.sh update branch missing services from 07b: {missing}. "
            f"These services won't be polled for READY after ALTER."
        )


# ---------------------------------------------------------------------------
# prefect.yaml ↔ worker docker-compose env var consistency
# ---------------------------------------------------------------------------
class TestPullStepEnvVarConsistency:
    """Every env var referenced in prefect.yaml pull steps must be defined
    in all external worker docker-compose files.

    This prevents the bug where a worker starts but git_clone fails because
    an env var like GIT_REPO_URL is missing from the compose environment.
    """

    PREFECT_YAML = PROJECT_DIR / "prefect.yaml"

    @pytest.fixture(scope="class")
    def pull_step_env_vars(self):
        """Extract env var names from prefect.yaml pull steps.

        Prefect template syntax: {{ $VAR_NAME }} → env var VAR_NAME.
        """
        config = yaml.safe_load(self.PREFECT_YAML.read_text())
        pull = config.get("pull", [])
        env_vars = set()
        for step in pull:
            for step_config in step.values():
                for value in (step_config or {}).values():
                    for match in re.findall(r"\{\{\s*\$(\w+)\s*\}\}", str(value)):
                        env_vars.add(match)
        return env_vars

    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_worker_compose_has_all_pull_step_vars(self, pool_key, pull_step_env_vars):
        """Each external worker's docker-compose must define every env var
        that prefect.yaml pull steps reference."""
        compose_path = resolve_compose_path(pool_key)
        compose_data = yaml.safe_load(compose_path.read_text())
        for name, svc in compose_data["services"].items():
            if "worker" not in name.lower():
                continue
            env = svc.get("environment", {})
            env_keys = set(env.keys())
            missing = pull_step_env_vars - env_keys
            assert not missing, (
                f"Worker '{name}' in {compose_path.name} is missing env vars "
                f"referenced by prefect.yaml pull steps: {missing}. "
                f"The git_clone step will fail at runtime without these."
            )


# ---------------------------------------------------------------------------
# prefect.yaml entrypoints ↔ set_working_directory consistency
# ---------------------------------------------------------------------------
class TestEntrypointWorkingDirConsistency:
    """Verify that entrypoints are relative to the set_working_directory path.

    If set_working_directory ends with '/flows', entrypoints must NOT start
    with 'flows/'. This prevents the bug where changing one without the
    other causes FileNotFoundError at deployment registration or runtime.
    """

    PREFECT_YAML = PROJECT_DIR / "prefect.yaml"

    @pytest.fixture(scope="class")
    def prefect_config(self):
        return yaml.safe_load(self.PREFECT_YAML.read_text())

    def test_entrypoints_relative_to_working_dir(self, prefect_config):
        """If working directory ends with a path suffix (e.g., '/flows'),
        entrypoints must not duplicate that suffix."""
        pull = prefect_config.get("pull", [])
        wd_suffix = None
        for step in pull:
            for key, config in step.items():
                if "set_working_directory" in key:
                    directory = str(config.get("directory", ""))
                    # Extract the last path component after clone.directory
                    if "/" in directory:
                        wd_suffix = directory.rsplit("/", 1)[-1]

        if wd_suffix is None:
            pytest.skip("No set_working_directory step found")

        for d in prefect_config.get("deployments", []):
            ep = d["entrypoint"]
            assert not ep.startswith(f"{wd_suffix}/"), (
                f"Deployment '{d['name']}': entrypoint '{ep}' starts with "
                f"'{wd_suffix}/' but set_working_directory already points there. "
                f"Entrypoints must be relative to the working directory."
            )

    def test_entrypoint_files_exist_relative_to_working_dir(self, prefect_config):
        """Every entrypoint file must exist relative to FLOWS_DIR
        (the local equivalent of set_working_directory)."""
        for d in prefect_config.get("deployments", []):
            file_part = d["entrypoint"].split(":")[0]
            full_path = FLOWS_DIR / file_part
            assert full_path.is_file(), (
                f"Deployment '{d['name']}': entrypoint file '{file_part}' "
                f"not found at {full_path}. Is the path relative to flows/?"
            )


# ---------------------------------------------------------------------------
# deploy.py ↔ prefect.yaml pull step consistency
# ---------------------------------------------------------------------------
class TestDeployPyPrefectYamlSync:
    """Verify that deploy.py and prefect.yaml use the same pull step
    patterns (same env var templates, same working directory).

    These are two deployment mechanisms that must stay in sync — if someone
    updates one but forgets the other, deployments from the out-of-date
    source will break.
    """

    PREFECT_YAML = PROJECT_DIR / "prefect.yaml"
    DEPLOY_PY = FLOWS_DIR / "deploy.py"

    @pytest.fixture(scope="class")
    def yaml_pull_config(self):
        config = yaml.safe_load(self.PREFECT_YAML.read_text())
        return config.get("pull", [])

    @pytest.fixture(scope="class")
    def deploy_py_source(self):
        return self.DEPLOY_PY.read_text()

    def test_both_use_git_clone(self, yaml_pull_config, deploy_py_source):
        """Both mechanisms should use git_clone as the pull step."""
        yaml_has_clone = any("git_clone" in key for step in yaml_pull_config for key in step)
        assert yaml_has_clone, "prefect.yaml missing git_clone step"
        assert "git_clone" in deploy_py_source, "deploy.py missing git_clone reference"

    def test_same_working_directory_suffix(self, yaml_pull_config, deploy_py_source):
        """Both must set working directory to clone.directory + /flows."""
        # Check prefect.yaml
        yaml_wd = None
        for step in yaml_pull_config:
            for key, config in step.items():
                if "set_working_directory" in key:
                    yaml_wd = str(config.get("directory", ""))

        assert yaml_wd is not None, "prefect.yaml missing set_working_directory"
        assert yaml_wd.endswith("/flows"), (
            f"prefect.yaml working directory should end with /flows, got: {yaml_wd}"
        )

        # Check deploy.py — look for the clone.directory + /flows pattern
        assert "clone.directory" in deploy_py_source, (
            "deploy.py should reference clone.directory in pull steps"
        )
        assert "{{ clone.directory }}/flows" in deploy_py_source, (
            "deploy.py should set working directory to '{{ clone.directory }}/flows'"
        )

    def test_same_env_var_template_for_access_token(self, yaml_pull_config, deploy_py_source):
        """Both must use {{ $GIT_ACCESS_TOKEN }} for the access token."""
        yaml_str = str(yaml_pull_config)
        assert "GIT_ACCESS_TOKEN" in yaml_str, (
            "prefect.yaml pull steps should reference GIT_ACCESS_TOKEN"
        )
        assert "GIT_ACCESS_TOKEN" in deploy_py_source, (
            "deploy.py pull steps should reference GIT_ACCESS_TOKEN"
        )


# ---------------------------------------------------------------------------
# Portability: No hardcoded SPCS FQDNs in specs or configs
# ---------------------------------------------------------------------------
class TestNoHardcodedFQDN:
    """Prevent account-specific SPCS FQDNs from being committed.

    SPCS containers resolve short service names (e.g., pf-server) via the
    search domain in /etc/resolv.conf. FQDNs like pf-server.fzuj.svc.spcs.internal
    contain an account-specific prefix and break portability across Snowflake accounts.
    """

    FQDN_PATTERN = re.compile(r"svc\.spcs\.internal")

    @pytest.mark.parametrize("name", [f.stem for f in SPECS_DIR.glob("*.yaml")])
    def test_no_fqdn_in_spec(self, name):
        content = (SPECS_DIR / f"{name}.yaml").read_text()
        assert not self.FQDN_PATTERN.search(content), (
            f"specs/{name}.yaml contains a hardcoded SPCS FQDN (svc.spcs.internal). "
            f"Use short service names (e.g., pf-server) for cross-account portability."
        )

    def test_no_fqdn_in_prometheus_config(self):
        prom_config = MONITORING_DIR / "prometheus" / "prometheus.yml"
        if not prom_config.exists():
            pytest.skip("No prometheus config found")
        content = prom_config.read_text()
        assert not self.FQDN_PATTERN.search(content), (
            "prometheus.yml contains a hardcoded SPCS FQDN. "
            "Use short service names for cross-account portability."
        )

    def test_no_fqdn_in_monitoring_specs(self):
        monitor_specs = MONITORING_DIR / "specs"
        if not monitor_specs.exists():
            pytest.skip("No monitoring specs directory found")
        for f in monitor_specs.glob("*.yaml"):
            content = f.read_text()
            assert not self.FQDN_PATTERN.search(content), (
                f"monitoring/specs/{f.name} contains a hardcoded SPCS FQDN. "
                f"Use short service names for cross-account portability."
            )
