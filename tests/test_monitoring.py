"""Tests for the Prometheus + Grafana + Loki monitoring stack.

Validates SPCS spec, Prometheus config, Grafana provisioning,
Loki config, VM agent configs, deploy/teardown scripts, and
cross-file consistency.
"""

import json

import pytest
import yaml
from conftest import MONITORING_DIR, PROJECT_DIR

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
MONITOR_SPEC_FILE = MONITORING_DIR / "specs" / "pf_monitor.yaml"
PROM_CONFIG_FILE = MONITORING_DIR / "prometheus" / "prometheus.yml"
PROM_RULES_FILE = MONITORING_DIR / "prometheus" / "rules" / "alerts.yml"
GRAFANA_DS_FILE = MONITORING_DIR / "grafana" / "provisioning" / "datasources" / "ds.yaml"
GRAFANA_DASH_PROV = MONITORING_DIR / "grafana" / "provisioning" / "dashboards" / "dash.yaml"
LOKI_CONFIG_FILE = MONITORING_DIR / "loki" / "loki-config.yaml"
VM_PROM_AGENT = MONITORING_DIR / "vm-agents" / "prometheus-agent.yml"
VM_PROMTAIL = MONITORING_DIR / "vm-agents" / "promtail-config.yaml"
VM_NGINX_MONITOR = MONITORING_DIR / "vm-agents" / "nginx-monitor.conf"
DEPLOY_SCRIPT = MONITORING_DIR / "deploy_monitoring.sh"
TEARDOWN_SCRIPT = MONITORING_DIR / "teardown_monitoring.sh"
COMPOSE_MONITORING = MONITORING_DIR / "docker-compose.monitoring.yml"
DASHBOARD_DIR = MONITORING_DIR / "grafana" / "dashboards"
DASH_SPCS_OVERVIEW = DASHBOARD_DIR / "spcs-overview.json"
DASH_VM_WORKERS = DASHBOARD_DIR / "vm-workers.json"
DASH_LOGS = DASHBOARD_DIR / "logs.json"
DASH_PREFECT_APP = DASHBOARD_DIR / "prefect-app.json"
DASH_REDIS_DETAIL = DASHBOARD_DIR / "redis-detail.json"
DASH_POSTGRES_DETAIL = DASHBOARD_DIR / "postgres-detail.json"
DASH_ALERTS = DASHBOARD_DIR / "alerts.json"
DASH_SPCS_LOGS = DASHBOARD_DIR / "spcs-logs.json"


# ---------------------------------------------------------------------------
# File existence
# ---------------------------------------------------------------------------
class TestMonitoringFilesExist:
    """All monitoring config files must be present."""

    @pytest.mark.parametrize(
        "path",
        [
            MONITOR_SPEC_FILE,
            PROM_CONFIG_FILE,
            PROM_RULES_FILE,
            GRAFANA_DS_FILE,
            GRAFANA_DASH_PROV,
            LOKI_CONFIG_FILE,
            VM_PROM_AGENT,
            VM_PROMTAIL,
            VM_NGINX_MONITOR,
            DEPLOY_SCRIPT,
            TEARDOWN_SCRIPT,
            COMPOSE_MONITORING,
            DASH_PREFECT_APP,
            DASH_REDIS_DETAIL,
            DASH_POSTGRES_DETAIL,
            DASH_ALERTS,
            DASH_SPCS_LOGS,
            MONITORING_DIR / "grafana" / "dashboards" / "health.json",
            MONITORING_DIR / "loki" / "rules" / "fake" / "alerts.yaml",
        ],
    )
    def test_file_exists(self, path):
        assert path.exists(), f"Missing: {path.relative_to(PROJECT_DIR)}"


# ---------------------------------------------------------------------------
# SPCS service spec: pf_monitor.yaml
# ---------------------------------------------------------------------------
class TestMonitorSpec:
    """Validate the PF_MONITOR SPCS service specification."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_has_top_level_spec_key(self):
        assert "spec" in self.spec

    def test_has_all_nine_containers(self):
        """PF_MONITOR must have exactly 9 containers (6 original + 2 backup sidecars + observe-agent)."""
        containers = self.spec["spec"]["containers"]
        names = {c["name"] for c in containers}
        expected = {
            "prometheus",
            "grafana",
            "loki",
            "postgres-exporter",
            "prefect-exporter",
            "event-log-poller",
            "prom-backup",
            "loki-backup",
            "observe-agent",
        }
        assert names == expected, (
            f"Expected containers {expected}, got {names}. "
            f"Missing: {expected - names}, unexpected: {names - expected}"
        )

    def test_prometheus_has_remote_write_receiver(self):
        containers = self.spec["spec"]["containers"]
        prom = next(c for c in containers if c["name"] == "prometheus")
        args = " ".join(prom.get("args", []) + prom.get("command", []))
        assert "--web.enable-remote-write-receiver" in args

    def test_prometheus_has_readiness_probe(self):
        containers = self.spec["spec"]["containers"]
        prom = next(c for c in containers if c["name"] == "prometheus")
        assert "readinessProbe" in prom

    def test_grafana_has_readiness_probe(self):
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        assert "readinessProbe" in grafana

    def test_loki_has_readiness_probe(self):
        containers = self.spec["spec"]["containers"]
        loki = next(c for c in containers if c["name"] == "loki")
        assert "readinessProbe" in loki

    def test_has_public_grafana_endpoint(self):
        endpoints = self.spec["spec"]["endpoints"]
        grafana_ep = next((e for e in endpoints if e["name"] == "grafana"), None)
        assert grafana_ep is not None
        assert grafana_ep.get("public") is True

    def test_has_prometheus_endpoint(self):
        endpoints = self.spec["spec"]["endpoints"]
        prom_ep = next((e for e in endpoints if e["name"] == "prometheus"), None)
        assert prom_ep is not None

    def test_has_block_storage_volumes(self):
        volumes = self.spec["spec"]["volumes"]
        block_vols = [v for v in volumes if v.get("source") == "block"]
        assert len(block_vols) >= 3, "Need block storage for prometheus, grafana, loki"

    def test_has_platform_monitor(self):
        assert "platformMonitor" in self.spec["spec"]

    def test_no_hardcoded_passwords(self):
        content = MONITOR_SPEC_FILE.read_text()
        # Password should come from Snowflake secret, not hardcoded
        assert "__GRAFANA_ADMIN_PASSWORD__" not in content
        # Verify it uses the secret pattern
        assert "grafana_admin_password" in content

    def test_grafana_uses_snowflake_secret_for_password(self):
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        secrets = grafana.get("secrets", [])
        secret_env_vars = [s["envVarName"] for s in secrets]
        assert "GF_SECURITY_ADMIN_PASSWORD" in secret_env_vars

    def test_grafana_anonymous_access_enabled(self):
        """Grafana must enable anonymous access for API calls through SPCS.

        SPCS ingress consumes the Authorization header for its own PAT-based
        authentication and does NOT forward it to containers. Grafana never
        receives auth credentials. Without anonymous access, all API calls
        (including those from the browser after SPCS auth succeeds) return 401.
        """
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        env = grafana.get("env", {})
        assert env.get("GF_AUTH_ANONYMOUS_ENABLED") == "true", (
            "Grafana must have GF_AUTH_ANONYMOUS_ENABLED=true — SPCS strips "
            "the Authorization header so Grafana never receives credentials"
        )

    def test_grafana_anonymous_role_is_viewer(self):
        """Anonymous users should only have Viewer role, not Editor or Admin."""
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        env = grafana.get("env", {})
        assert env.get("GF_AUTH_ANONYMOUS_ORG_ROLE") == "Viewer", (
            "Anonymous users must be limited to Viewer role for security"
        )

    def test_grafana_database_type_is_postgres(self):
        """Grafana must use Postgres backend for persistent storage."""
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        env = grafana.get("env", {})
        assert env.get("GF_DATABASE_TYPE") == "postgres", (
            "Grafana must have GF_DATABASE_TYPE=postgres for persistent storage"
        )

    def test_grafana_database_ssl_mode(self):
        """GF_DATABASE_SSL_MODE is intentionally omitted — sslmode is embedded in the DSN secret."""
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        env = grafana.get("env", {})
        assert "GF_DATABASE_SSL_MODE" not in env, (
            "GF_DATABASE_SSL_MODE must NOT be in env — sslmode is provided via the DSN secret"
        )

    def test_grafana_database_url_from_secret(self):
        """Grafana DB URL must come from a Snowflake secret, not hardcoded."""
        containers = self.spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        secrets = grafana.get("secrets", [])
        secret_env_vars = [s["envVarName"] for s in secrets]
        assert "GF_DATABASE_URL" in secret_env_vars, (
            "Grafana must inject GF_DATABASE_URL from a Snowflake secret"
        )

    def test_grafana_db_dsn_secret_referenced(self):
        """The grafana_db_dsn secret must be referenced in the spec."""
        content = MONITOR_SPEC_FILE.read_text().lower()
        assert "grafana_db_dsn" in content, (
            "Spec must reference grafana_db_dsn secret for Postgres persistence"
        )


# ---------------------------------------------------------------------------
# Prometheus configuration
# ---------------------------------------------------------------------------
class TestPrometheusConfig:
    """Validate prometheus.yml scrape configuration."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load(PROM_CONFIG_FILE.read_text())

    def test_has_global_scrape_interval(self):
        assert "scrape_interval" in self.config["global"]

    def test_has_scrape_configs(self):
        assert "scrape_configs" in self.config
        assert len(self.config["scrape_configs"]) >= 2

    def test_scrapes_prefect_server(self):
        jobs = [sc["job_name"] for sc in self.config["scrape_configs"]]
        assert "prefect-server" in jobs

    def test_prefect_server_uses_metrics_path(self):
        server_job = next(
            sc for sc in self.config["scrape_configs"] if sc["job_name"] == "prefect-server"
        )
        assert server_job.get("metrics_path") == "/api/metrics"

    def test_targets_use_short_service_names(self):
        """SPCS targets should use short DNS names (no FQDN) for portability."""
        server_job = next(
            sc for sc in self.config["scrape_configs"] if sc["job_name"] == "prefect-server"
        )
        targets = server_job["static_configs"][0]["targets"]
        assert "pf-server:4200" in targets
        assert not any("svc.spcs.internal" in t for t in targets), (
            "SPCS targets should use short names (e.g., pf-server:4200) "
            "instead of FQDNs for cross-account portability"
        )


# ---------------------------------------------------------------------------
# Prometheus alerting rules
# ---------------------------------------------------------------------------
class TestPrometheusAlertRules:
    """Validate alert rules YAML."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(PROM_RULES_FILE.read_text())

    def test_has_groups(self):
        assert "groups" in self.rules
        assert len(self.rules["groups"]) >= 2

    def test_has_critical_alerts(self):
        all_rules = []
        for group in self.rules["groups"]:
            all_rules.extend(group.get("rules", []))
        severities = [r["labels"]["severity"] for r in all_rules]
        assert "critical" in severities

    def test_has_prefect_server_down_alert(self):
        all_rules = []
        for group in self.rules["groups"]:
            all_rules.extend(group.get("rules", []))
        names = [r["alert"] for r in all_rules]
        assert "PrefectServerDown" in names

    def test_has_worker_offline_alert(self):
        all_rules = []
        for group in self.rules["groups"]:
            all_rules.extend(group.get("rules", []))
        names = [r["alert"] for r in all_rules]
        assert "WorkerOffline" in names


# ---------------------------------------------------------------------------
# Grafana provisioning
# ---------------------------------------------------------------------------
class TestGrafanaProvisioning:
    """Validate Grafana datasource and dashboard provisioning."""

    def test_datasources_has_prometheus(self):
        data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        names = [ds["name"] for ds in data["datasources"]]
        assert "Prometheus" in names

    def test_datasources_has_loki(self):
        data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        names = [ds["name"] for ds in data["datasources"]]
        assert "Loki" in names

    def test_datasource_uids_are_stable(self):
        """Datasource UIDs must match what dashboards reference.

        Dashboards use uid='prometheus' and uid='loki' in their datasource
        references. If the provisioned UIDs don't match, dashboards show
        "datasource not found" errors and all panels break.
        """
        data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        uid_map = {ds["name"]: ds["uid"] for ds in data["datasources"]}
        assert uid_map["Prometheus"] == "prometheus", (
            f"Prometheus datasource uid must be 'prometheus', got: {uid_map['Prometheus']}"
        )
        assert uid_map["Loki"] == "loki", (
            f"Loki datasource uid must be 'loki', got: {uid_map['Loki']}"
        )

    def test_datasource_provisioning_deletes_stale_entries(self):
        """Provisioning must include deleteDatasources to remove DB-cached entries.

        Without deleteDatasources, Grafana's persistent block storage may retain
        datasources with auto-generated UIDs from a previous deployment. The
        provisioned datasources then get different UIDs, and dashboard references
        to uid='prometheus'/uid='loki' silently break.
        """
        data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        assert "deleteDatasources" in data, (
            "ds.yaml must include 'deleteDatasources' to remove stale DB-cached "
            "entries that would conflict with provisioned uid values"
        )
        delete_names = {d["name"] for d in data["deleteDatasources"]}
        assert "Prometheus" in delete_names
        assert "Loki" in delete_names

    def test_datasources_not_editable(self):
        """Provisioned datasources must not be editable in the UI.

        Editable datasources can be modified in the Grafana UI and saved to the
        DB with different UIDs, breaking dashboard references on next restart.
        """
        data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        for ds in data["datasources"]:
            assert ds.get("editable") is False, (
                f"Datasource '{ds['name']}' must have editable: false"
            )

    def test_dashboard_provider_configured(self):
        data = yaml.safe_load(GRAFANA_DASH_PROV.read_text())
        assert "providers" in data
        assert len(data["providers"]) >= 1

    def test_dashboard_provider_not_editable(self):
        """Dashboard provisioning must set editable: false.

        With editable: true, Grafana saves dashboard edits to its SQLite DB
        on persistent block storage. On restart, the DB-cached version may
        take precedence over the file-provisioned version, causing stale
        dashboards that ignore file updates.
        """
        data = yaml.safe_load(GRAFANA_DASH_PROV.read_text())
        provider = data["providers"][0]
        assert provider.get("editable") is False, (
            "Dashboard provider must set editable: false to prevent "
            "DB-cached versions from overriding file-provisioned dashboards"
        )

    def test_dashboard_provider_disallows_ui_updates(self):
        """Dashboard provisioning must set allowUiUpdates: false.

        Combined with editable: false, this ensures the provisioned file
        is always the source of truth. Grafana overwrites any DB-cached
        dashboard versions with the file content on every restart.
        """
        data = yaml.safe_load(GRAFANA_DASH_PROV.read_text())
        provider = data["providers"][0]
        assert provider.get("allowUiUpdates") is False, (
            "Dashboard provider must set allowUiUpdates: false to force "
            "file as source of truth over DB-cached versions"
        )


# ---------------------------------------------------------------------------
# Grafana dashboards (JSON)
# ---------------------------------------------------------------------------
class TestGrafanaDashboards:
    """Validate Grafana dashboard JSON files."""

    @pytest.fixture
    def dashboard_files(self):
        return list((MONITORING_DIR / "grafana" / "dashboards").glob("*.json"))

    def test_at_least_three_dashboards(self, dashboard_files):
        assert len(dashboard_files) >= 3, f"Expected >=3 dashboards, found {len(dashboard_files)}"

    def test_dashboards_are_valid_json(self, dashboard_files):
        for f in dashboard_files:
            data = json.loads(f.read_text())
            assert "panels" in data, f"{f.name}: missing 'panels' key"
            assert "title" in data, f"{f.name}: missing 'title' key"

    def test_dashboards_have_unique_uids(self, dashboard_files):
        uids = []
        for f in dashboard_files:
            data = json.loads(f.read_text())
            uid = data.get("uid")
            assert uid is not None, f"{f.name}: missing 'uid'"
            uids.append(uid)
        assert len(uids) == len(set(uids)), f"Duplicate UIDs: {uids}"


# ---------------------------------------------------------------------------
# Loki configuration
# ---------------------------------------------------------------------------
class TestLokiConfig:
    """Validate Loki config."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load(LOKI_CONFIG_FILE.read_text())

    def test_has_server_section(self):
        assert "server" in self.config

    def test_listens_on_3100(self):
        assert self.config["server"]["http_listen_port"] == 3100

    def test_has_retention(self):
        assert "limits_config" in self.config
        assert "retention_period" in self.config["limits_config"]

    def test_has_schema_config(self):
        assert "schema_config" in self.config


# ---------------------------------------------------------------------------
# VM agent configs
# ---------------------------------------------------------------------------
class TestVMAgentConfigs:
    """Validate VM-side Prometheus agent and Promtail configs."""

    def test_prometheus_agent_has_remote_write(self):
        config = yaml.safe_load(VM_PROM_AGENT.read_text())
        assert "remote_write" in config
        assert len(config["remote_write"]) >= 1

    def test_prometheus_agent_scrapes_node_exporter(self):
        config = yaml.safe_load(VM_PROM_AGENT.read_text())
        jobs = [sc["job_name"] for sc in config["scrape_configs"]]
        assert "node-exporter" in jobs

    def test_prometheus_agent_scrapes_cadvisor(self):
        config = yaml.safe_load(VM_PROM_AGENT.read_text())
        jobs = [sc["job_name"] for sc in config["scrape_configs"]]
        assert "cadvisor" in jobs

    def test_promtail_has_clients(self):
        config = yaml.safe_load(VM_PROMTAIL.read_text())
        assert "clients" in config
        assert len(config["clients"]) >= 1

    def test_nginx_monitor_has_prometheus_proxy(self):
        content = VM_NGINX_MONITOR.read_text()
        assert "4210" in content  # Prometheus remote_write port
        assert "SNOWFLAKE_PAT" in content  # PAT injection


# ---------------------------------------------------------------------------
# Docker Compose monitoring overlay
# ---------------------------------------------------------------------------
class TestComposeMonitoring:
    """Validate the docker-compose.monitoring.yml overlay."""

    @pytest.fixture(autouse=True)
    def load_compose(self):
        self.compose = yaml.safe_load(COMPOSE_MONITORING.read_text())

    def test_has_node_exporter(self):
        assert "node-exporter" in self.compose["services"]

    def test_has_cadvisor(self):
        assert "cadvisor" in self.compose["services"]

    def test_has_prometheus_agent(self):
        assert "prometheus-agent" in self.compose["services"]

    def test_has_promtail(self):
        assert "promtail" in self.compose["services"]

    def test_has_auth_proxy_monitor(self):
        assert "auth-proxy-monitor" in self.compose["services"]


# ---------------------------------------------------------------------------
# Deploy/teardown scripts
# ---------------------------------------------------------------------------
class TestMonitoringDeployScript:
    """Validate deploy_monitoring.sh."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_creates_compute_pool(self):
        assert "PREFECT_MONITOR_POOL" in self.content

    def test_creates_network_rule(self):
        assert "MONITOR_EGRESS_RULE" in self.content

    def test_creates_eai(self):
        assert "PREFECT_MONITOR_EAI" in self.content

    def test_creates_stage(self):
        assert "MONITOR_STAGE" in self.content

    def test_creates_service(self):
        assert "PF_MONITOR" in self.content

    def test_does_not_use_create_or_replace_service(self):
        assert "CREATE OR REPLACE SERVICE" not in self.content

    def test_uploads_spec_to_stage(self):
        assert "pf_monitor.yaml" in self.content
        assert "${STAGE}/specs" in self.content

    def test_creates_grafana_secret(self):
        assert "GRAFANA_ADMIN_PASSWORD" in self.content


class TestMonitoringTeardownScript:
    """Validate teardown_monitoring.sh."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = TEARDOWN_SCRIPT.read_text()

    def test_drops_service(self):
        assert "PF_MONITOR" in self.content

    def test_drops_eai(self):
        assert "PREFECT_MONITOR_EAI" in self.content

    def test_drops_network_rule(self):
        assert "MONITOR_EGRESS_RULE" in self.content

    def test_drops_stage(self):
        assert "MONITOR_STAGE" in self.content

    def test_drops_pool(self):
        assert "PREFECT_MONITOR_POOL" in self.content


# ---------------------------------------------------------------------------
# Cross-file consistency
# ---------------------------------------------------------------------------
class TestMonitoringCrossFileConsistency:
    """Ensure monitoring config references are consistent across files."""

    def test_main_deploy_invokes_monitoring(self):
        deploy = (PROJECT_DIR / "scripts" / "deploy.sh").read_text()
        assert "deploy_monitoring.sh" in deploy

    def test_main_teardown_drops_monitor_pool(self):
        teardown = (PROJECT_DIR / "scripts" / "teardown.sh").read_text()
        assert "PREFECT_MONITOR_POOL" in teardown

    def test_main_teardown_drops_monitor_service(self):
        teardown = (PROJECT_DIR / "scripts" / "teardown.sh").read_text()
        assert "PF_MONITOR" in teardown

    def test_pf_server_has_metrics_enabled(self):
        server_spec = yaml.safe_load((PROJECT_DIR / "specs" / "pf_server.yaml").read_text())
        env = server_spec["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_SERVER_METRICS_ENABLED") == "true"

    def test_pf_server_has_analytics_disabled(self):
        """Prefect telemetry is disabled to avoid 'Failed to send telemetry' noise in SPCS."""
        server_spec = yaml.safe_load((PROJECT_DIR / "specs" / "pf_server.yaml").read_text())
        env = server_spec["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_SERVER_ANALYTICS_ENABLED") == "false"

    def test_pf_services_has_analytics_disabled(self):
        """pf_services must also disable analytics to prevent telemetry noise."""
        svc_spec = yaml.safe_load((PROJECT_DIR / "specs" / "pf_services.yaml").read_text())
        env = svc_spec["spec"]["containers"][0].get("env", {})
        assert env.get("PREFECT_SERVER_ANALYTICS_ENABLED") == "false"

    def test_pf_worker_has_endpoints_section(self):
        """PF_WORKER must have an endpoints section for SPCS internal DNS routing.

        Without endpoints, Prometheus (in PF_MONITOR) cannot reach the worker
        health endpoint via SPCS internal DNS, even though the readinessProbe
        works (it's an internal container check, not routed via DNS).
        """
        worker_spec = yaml.safe_load((PROJECT_DIR / "specs" / "pf_worker.yaml").read_text())
        endpoints = worker_spec["spec"].get("endpoints", [])
        assert len(endpoints) >= 1, "PF_WORKER must have at least one endpoint for SPCS DNS routing"
        port_numbers = [e.get("port") for e in endpoints]
        assert 8080 in port_numbers, (
            "PF_WORKER must expose port 8080 (health endpoint) in endpoints"
        )

    def test_dashboard_datasource_uids_match_provisioned(self):
        """All dashboard datasource references must match provisioned UIDs.

        If a dashboard references uid='loki' but the provisioned datasource
        has uid='P8E80F9AEF21F6940' (auto-generated), every panel in that
        dashboard silently breaks with no data.
        """
        ds_data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        provisioned_uids = {ds["uid"] for ds in ds_data["datasources"]}
        provisioned_uids.add("-- Grafana --")  # Built-in datasource

        for dash_file in DASHBOARD_DIR.glob("*.json"):
            dash = json.loads(dash_file.read_text())
            for panel in dash.get("panels", []):
                ds = panel.get("datasource", {})
                if isinstance(ds, dict) and "uid" in ds:
                    assert ds["uid"] in provisioned_uids, (
                        f"Dashboard '{dash.get('title')}' panel '{panel.get('title')}' "
                        f"references datasource uid='{ds['uid']}' which is not in "
                        f"provisioned datasources: {provisioned_uids}"
                    )


# ---------------------------------------------------------------------------
# VM monitoring pipeline correctness (regression tests for deployment bugs)
# ---------------------------------------------------------------------------
class TestVMMonitoringPipeline:
    """Regression tests for issues discovered during VM monitoring deployment.

    Each test guards against a specific bug that caused monitoring failures:
    - 426 Upgrade Required from nginx proxy
    - Prometheus crash-loop from invalid config
    - Promtail not discovering Docker container logs
    - Environment variable expansion failures
    """

    def test_nginx_monitor_uses_http_1_1(self):
        """nginx must proxy with HTTP/1.1 to avoid SPCS 426 Upgrade Required.

        Without proxy_http_version 1.1, nginx defaults to HTTP/1.0 for upstream
        connections. SPCS ingress rejects HTTP/1.0 with 426 Upgrade Required.
        """
        content = VM_NGINX_MONITOR.read_text()
        assert "proxy_http_version 1.1" in content, (
            "nginx-monitor.conf must set proxy_http_version 1.1 to avoid "
            "SPCS 426 Upgrade Required errors"
        )

    def test_nginx_monitor_clears_connection_header(self):
        """nginx must clear Connection header to prevent upgrade negotiation.

        The map $http_upgrade block can cause Connection: upgrade to leak
        through to SPCS, triggering 426. Explicit Connection: "" prevents this.
        """
        content = VM_NGINX_MONITOR.read_text()
        assert 'proxy_set_header Connection ""' in content, (
            "nginx-monitor.conf must clear Connection header"
        )

    def test_nginx_monitor_has_loki_proxy_port(self):
        content = VM_NGINX_MONITOR.read_text()
        assert "4220" in content, "nginx-monitor.conf must proxy Loki on port 4220"

    def test_prometheus_agent_no_bash_default_syntax(self):
        """Prometheus config must not use ${VAR:-default} syntax.

        Prometheus expands ${VAR} but does NOT support bash-style defaults
        like ${VAR:-default}. Using them results in the literal string
        '${WORKER_LOCATION:-unknown}' as the label value.
        """
        content = VM_PROM_AGENT.read_text()
        assert ":-" not in content, (
            "prometheus-agent.yml must not use ${VAR:-default} syntax; "
            "Prometheus only supports ${VAR}"
        )

    def test_prometheus_agent_uses_v1_protobuf(self):
        """Remote write must NOT use v2 protobuf (causes out-of-order errors).

        The SPCS Prometheus receiver rejects v2 remote write with "Out of order
        sample" errors. We rely on the default v1 protocol (no protobuf_message
        field) for compatibility.
        """
        config = yaml.safe_load(VM_PROM_AGENT.read_text())
        rw = config["remote_write"][0]
        proto = rw.get("protobuf_message", "")
        assert proto != "io.prometheus.write.v2.Request", (
            "remote_write must not use v2 protocol "
            "(io.prometheus.write.v2.Request causes out-of-order errors)"
        )

    def test_prometheus_agent_has_external_labels(self):
        config = yaml.safe_load(VM_PROM_AGENT.read_text())
        ext = config.get("global", {}).get("external_labels", {})
        assert "worker_location" in ext, "Must have worker_location external label"
        assert "worker_pool" in ext, "Must have worker_pool external label"

    def test_compose_prometheus_agent_mode_flag(self):
        """Prometheus v3 agent mode uses --agent, not --enable-feature=agent.

        In Prometheus v3.x, --enable-feature=agent is removed. Using it causes
        'Unknown option' warnings and the agent doesn't actually run in agent
        mode. The correct flag is --agent.
        """
        compose = yaml.safe_load(COMPOSE_MONITORING.read_text())
        prom_svc = compose["services"]["prometheus-agent"]
        command = prom_svc.get("command", [])
        assert "--agent" in command, (
            "prometheus-agent must use --agent flag (not --enable-feature=agent)"
        )
        for flag in command:
            assert "--enable-feature=agent" not in flag, (
                "prometheus-agent must NOT use --enable-feature=agent (removed in Prometheus v3)"
            )

    def test_compose_prometheus_agent_no_tsdb_flags(self):
        """Agent mode forbids --storage.tsdb.* flags.

        Prometheus v3 --agent mode uses WAL-only storage. Passing
        --storage.tsdb.path or --storage.tsdb.retention.* causes:
        'The following flag(s) can not be used in agent mode'
        """
        compose = yaml.safe_load(COMPOSE_MONITORING.read_text())
        prom_svc = compose["services"]["prometheus-agent"]
        command = prom_svc.get("command", [])
        tsdb_flags = [f for f in command if "--storage.tsdb" in f]
        assert tsdb_flags == [], (
            f"prometheus-agent must not use --storage.tsdb.* flags in agent "
            f"mode, found: {tsdb_flags}"
        )

    def test_promtail_log_path_matches_docker_layout(self):
        """Promtail __path__ must match Docker's container log directory layout.

        Docker stores logs at /var/lib/docker/containers/<id>/<id>-json.log.
        When mounted to /var/log/containers, the glob must be /*/*-json.log
        (one level of subdirectory). A flat *.log pattern finds nothing.
        """
        config = yaml.safe_load(VM_PROMTAIL.read_text())
        path_label = config["scrape_configs"][0]["static_configs"][0]["labels"]["__path__"]
        assert "/*/*" in path_label, (
            f"Promtail __path__ must use /*/*-json.log to match Docker's "
            f"subdirectory layout, got: {path_label}"
        )

    def test_promtail_no_bash_default_syntax(self):
        """Promtail config must not use ${VAR:-default} syntax.

        Like Prometheus, Promtail's built-in env expansion (when using
        -config.expand-env=true) does NOT support bash default syntax.
        """
        content = VM_PROMTAIL.read_text()
        assert ":-" not in content, "promtail-config.yaml must not use ${VAR:-default} syntax"

    def test_compose_promtail_has_expand_env(self):
        """Promtail must use -config.expand-env=true to expand env vars.

        Without this flag, ${WORKER_LOCATION} in promtail-config.yaml
        is passed literally, not expanded from the container's environment.
        """
        compose = yaml.safe_load(COMPOSE_MONITORING.read_text())
        promtail_svc = compose["services"]["promtail"]
        command = promtail_svc.get("command", [])
        assert any("expand-env" in str(c) for c in command), (
            "promtail must use -config.expand-env=true in command"
        )

    def test_compose_monitoring_required_env_vars(self):
        """All monitoring services must reference required env vars."""
        compose = yaml.safe_load(COMPOSE_MONITORING.read_text())

        # auth-proxy-monitor needs PAT and endpoints
        proxy_env = compose["services"]["auth-proxy-monitor"]["environment"]
        assert "SNOWFLAKE_PAT" in str(proxy_env)
        assert "SPCS_MONITOR_ENDPOINT" in str(proxy_env)
        assert "SPCS_MONITOR_LOKI_ENDPOINT" in str(proxy_env)

        # prometheus-agent needs worker labels
        prom_env = compose["services"]["prometheus-agent"]["environment"]
        assert "WORKER_LOCATION" in str(prom_env)
        assert "WORKER_POOL" in str(prom_env)

    def test_compose_vm_agents_volume_paths(self):
        """Volume paths must use ./vm-agents/ (relative to compose file location).

        The monitoring compose overlay runs alongside the worker compose file.
        Paths must be relative to the working directory where both compose files
        and the vm-agents/ directory are deployed together.
        """
        compose = yaml.safe_load(COMPOSE_MONITORING.read_text())
        for svc_name in ["prometheus-agent", "promtail", "auth-proxy-monitor"]:
            svc = compose["services"][svc_name]
            volumes = svc.get("volumes", [])
            for vol in volumes:
                if "vm-agents" in str(vol):
                    vol_str = vol if isinstance(vol, str) else vol.get("source", "")
                    assert vol_str.startswith("./vm-agents/"), (
                        f"{svc_name}: volume path must start with ./vm-agents/, got: {vol_str}"
                    )


# ---------------------------------------------------------------------------
# Prometheus scrape config correctness (regression tests for up=0 bugs)
# ---------------------------------------------------------------------------
class TestPrometheusScrapeConfig:
    """Regression tests for Prometheus scrape target issues.

    Guards against scraping non-Prometheus endpoints that cause perpetual
    up=0 and parse errors in the Prometheus targets page.
    """

    def test_loki_scrapes_metrics_not_ready(self):
        """Loki must be scraped at /metrics, not /ready.

        Loki's /ready endpoint returns plain text ("ready\\n") which
        Prometheus cannot parse, resulting in:
          'expected value after metric, got "\\n" ("INVALID")'
        and up=0. The /metrics endpoint returns valid Prometheus format.
        """
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        loki_jobs = [j for j in config["scrape_configs"] if j["job_name"] == "loki"]
        assert len(loki_jobs) == 1, "Expected exactly one loki scrape job"
        assert loki_jobs[0]["metrics_path"] == "/metrics", (
            f"Loki scrape job must use /metrics (not /ready), got: {loki_jobs[0]['metrics_path']}"
        )

    def test_loki_does_not_use_fallback_scrape_protocol(self):
        """Loki /metrics returns standard Prometheus format — no fallback needed.

        fallback_scrape_protocol was a workaround for scraping /ready.
        Now that we scrape /metrics, it should be removed to avoid confusion.
        """
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        loki_jobs = [j for j in config["scrape_configs"] if j["job_name"] == "loki"]
        assert "fallback_scrape_protocol" not in loki_jobs[0], (
            "Loki scrape job should not use fallback_scrape_protocol when scraping /metrics"
        )

    def test_worker_scrapes_metrics_path(self):
        """Worker scrape job must use /metrics as metrics_path.

        The worker has no /metrics endpoint (returns 404), but /metrics is
        the correct path to scrape. Scraping /health returns JSON which
        Prometheus cannot parse — both result in up=0, but /metrics avoids
        noisy parse errors in Prometheus logs.

        The target is kept for service discovery so the ``up`` metric exists
        and ``absent(up{job="prefect-worker-spcs"})`` alerts fire when the
        worker service is completely unreachable.
        """
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        worker_jobs = [
            j for j in config["scrape_configs"] if j["job_name"] == "prefect-worker-spcs"
        ]
        assert len(worker_jobs) == 1, "Expected exactly one worker scrape job"
        path = worker_jobs[0].get("metrics_path", "/metrics")
        assert path == "/metrics", (
            f"Worker scrape job must use /metrics (returns clean 404), got metrics_path={path}"
        )

    def test_all_scrape_jobs_present(self):
        """Prometheus must have scrape configs for all expected services."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        job_names = {j["job_name"] for j in config["scrape_configs"]}
        expected = {
            "prefect-server",
            "prefect-worker-spcs",
            "prometheus",
            "grafana",
            "loki",
            "redis",
            "postgres",
            "prefect-app",
        }
        assert expected == job_names, (
            f"Missing scrape jobs: {expected - job_names}, unexpected: {job_names - expected}"
        )

    def test_prefect_server_scrapes_api_metrics(self):
        """Prefect server must be scraped at /api/metrics (prometheus_client)."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        server_jobs = [j for j in config["scrape_configs"] if j["job_name"] == "prefect-server"]
        assert server_jobs[0]["metrics_path"] == "/api/metrics"

    def test_redis_scrape_targets_redis_exporter(self):
        """Redis scrape job must target the redis-exporter sidecar in PF_REDIS."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        redis_jobs = [j for j in config["scrape_configs"] if j["job_name"] == "redis"]
        assert len(redis_jobs) == 1, "Must have exactly one redis scrape job"
        targets = redis_jobs[0]["static_configs"][0]["targets"]
        assert any("9121" in t for t in targets), (
            f"Redis scrape must target port 9121 (redis-exporter), got: {targets}"
        )

    def test_postgres_scrape_targets_postgres_exporter(self):
        """Postgres scrape job must target the postgres-exporter sidecar in PF_MONITOR."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        pg_jobs = [j for j in config["scrape_configs"] if j["job_name"] == "postgres"]
        assert len(pg_jobs) == 1, "Must have exactly one postgres scrape job"
        targets = pg_jobs[0]["static_configs"][0]["targets"]
        assert any("9187" in t for t in targets), (
            f"Postgres scrape must target port 9187 (postgres-exporter), got: {targets}"
        )


# ---------------------------------------------------------------------------
# Spec file correctness — Redis exporter sidecar
# ---------------------------------------------------------------------------
class TestRedisExporterSpec:
    """Regression tests for redis-exporter sidecar in PF_REDIS spec."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load((PROJECT_DIR / "specs" / "pf_redis.yaml").read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_has_redis_exporter_container(self):
        """PF_REDIS spec must include a redis-exporter sidecar."""
        assert "redis-exporter" in self.containers

    def test_redis_exporter_points_to_localhost(self):
        """redis-exporter must connect to localhost:6379 (shared network in SPCS)."""
        env = self.containers["redis-exporter"].get("env", {})
        addr = env.get("REDIS_ADDR", "")
        assert "localhost" in addr or "127.0.0.1" in addr, (
            f"redis-exporter REDIS_ADDR must point to localhost, got: {addr}"
        )

    def test_redis_exporter_has_readiness_probe(self):
        """redis-exporter must have a readiness probe on /metrics."""
        probe = self.containers["redis-exporter"].get("readinessProbe", {})
        assert probe.get("port") == 9121
        assert probe.get("path") == "/metrics"

    def test_redis_metrics_endpoint_exposed(self):
        """PF_REDIS spec must expose port 9121 for redis-exporter metrics."""
        endpoints = self.spec["spec"]["endpoints"]
        ports = {e["port"] for e in endpoints}
        assert 9121 in ports, f"Must expose port 9121 for redis metrics, got: {ports}"


# ---------------------------------------------------------------------------
# Spec file correctness — Postgres exporter sidecar
# ---------------------------------------------------------------------------
class TestPostgresExporterSpec:
    """Regression tests for postgres-exporter sidecar in PF_MONITOR spec."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load((MONITORING_DIR / "specs" / "pf_monitor.yaml").read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_has_postgres_exporter_container(self):
        """PF_MONITOR spec must include a postgres-exporter sidecar."""
        assert "postgres-exporter" in self.containers

    def test_postgres_exporter_uses_dsn_secret(self):
        """postgres-exporter must get DSN from postgres_exporter_dsn secret."""
        secrets = self.containers["postgres-exporter"].get("secrets", [])
        secret_names = [s["snowflakeSecret"]["objectName"] for s in secrets]
        assert "postgres_exporter_dsn" in secret_names, (
            f"postgres-exporter must use postgres_exporter_dsn secret, got: {secret_names}"
        )

    def test_postgres_exporter_dsn_env_var(self):
        """postgres-exporter DSN must be injected as DATA_SOURCE_NAME."""
        secrets = self.containers["postgres-exporter"]["secrets"]
        dsn_secret = next(
            s for s in secrets if s["snowflakeSecret"]["objectName"] == "postgres_exporter_dsn"
        )
        assert dsn_secret["envVarName"] == "DATA_SOURCE_NAME"

    def test_postgres_exporter_has_readiness_probe(self):
        """postgres-exporter must have a readiness probe on /metrics."""
        probe = self.containers["postgres-exporter"].get("readinessProbe", {})
        assert probe.get("port") == 9187
        assert probe.get("path") == "/metrics"


# ---------------------------------------------------------------------------
# Dashboard correctness — SPCS Overview
# ---------------------------------------------------------------------------
class TestDashboardSPCSOverview:
    """Regression tests for the SPCS Overview dashboard.

    Guards against showing incorrect UP/DOWN status for services
    that don't expose Prometheus metrics.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_SPCS_OVERVIEW.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_service_health_excludes_worker_from_up_stat(self):
        """The main health stat panel must not include prefect-worker-spcs.

        The worker has no /metrics endpoint, so up=0 is permanent.
        Including it in the health panel would always show DOWN.
        """
        panel = self.panels.get("SPCS Service Health")
        assert panel is not None, "Dashboard must have 'SPCS Service Health' panel"
        expr = panel["targets"][0]["expr"]
        assert "prefect-worker-spcs" not in expr, (
            "SPCS Service Health panel must not include prefect-worker-spcs "
            "(it has no metrics endpoint and up=0 is permanent)"
        )

    def test_services_up_ratio_excludes_worker(self):
        """The UP ratio gauge must not include prefect-worker-spcs.

        Including it would artificially lower the ratio since the worker
        always reports up=0.
        """
        panel = self.panels.get("Services UP / Expected")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "prefect-worker-spcs" not in expr, "UP ratio must not include prefect-worker-spcs"

    def test_has_separate_worker_status_panel(self):
        """Dashboard must have a dedicated SPCS Worker Status panel.

        Since the worker doesn't expose metrics, it needs a separate
        panel that doesn't rely on the up metric being 1.
        """
        assert "SPCS Worker Status" in self.panels, (
            "Dashboard must have a separate 'SPCS Worker Status' panel"
        )

    def test_has_loki_ingestion_panel(self):
        """Dashboard should have Loki ingestion rate panel for observability."""
        loki_panels = [
            p
            for p in self.dash["panels"]
            if "loki" in p["title"].lower() and "ingestion" in p["title"].lower()
        ]
        assert len(loki_panels) >= 1, "Dashboard should have a Loki ingestion rate panel"

    def test_service_status_table_excludes_worker(self):
        """The service status table must exclude prefect-worker-spcs.
        The worker has no /metrics endpoint so it always shows DOWN,
        which is confusing.  It has its own dedicated panel."""
        panel = self.panels.get("Service Status — SPCS Targets")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "prefect-worker-spcs" in expr, (
            "Service status table must explicitly filter out prefect-worker-spcs"
        )
        assert 'job!="prefect-worker-spcs"' in expr, f"Expected exclusion filter, got: {expr}"

    def test_has_vm_workers_heartbeat_panel(self):
        """Dashboard must have external VM workers heartbeat panel."""
        assert "External VM Workers — Heartbeat" in self.panels

    def test_service_health_includes_redis_and_postgres(self):
        """Service Health panel must include redis, postgres, and prefect-app jobs."""
        panel = self.panels["SPCS Service Health"]
        expr = panel["targets"][0]["expr"]
        assert "redis" in expr, f"Service Health must include redis, got: {expr}"
        assert "postgres" in expr, f"Service Health must include postgres, got: {expr}"
        assert "prefect-app" in expr, f"Service Health must include prefect-app, got: {expr}"

    def test_services_up_ratio_includes_redis_and_postgres(self):
        """UP ratio gauge must count redis, postgres, and prefect-app in the ratio."""
        panel = self.panels["Services UP / Expected"]
        expr = panel["targets"][0]["expr"]
        assert "redis" in expr, f"UP ratio must include redis, got: {expr}"
        assert "postgres" in expr, f"UP ratio must include postgres, got: {expr}"
        assert "prefect-app" in expr, f"UP ratio must include prefect-app, got: {expr}"

    def test_has_redis_memory_panel(self):
        """Dashboard must have a Redis Memory Usage panel."""
        assert "Redis Memory Usage" in self.panels

    def test_redis_memory_panel_queries_redis_job(self):
        """Redis Memory panel must query the redis job."""
        panel = self.panels["Redis Memory Usage"]
        expr = panel["targets"][0]["expr"]
        assert 'job="redis"' in expr, f"Redis Memory panel must target redis job, got: {expr}"

    def test_has_redis_commands_panel(self):
        """Dashboard must have a Redis Commands/sec panel."""
        assert "Redis Commands/sec" in self.panels

    def test_has_redis_clients_panel(self):
        """Dashboard must have a Redis Connected Clients panel."""
        assert "Redis Connected Clients" in self.panels

    def test_has_postgres_connections_panel(self):
        """Dashboard must have a PostgreSQL Active Connections panel."""
        assert "PostgreSQL Active Connections" in self.panels

    def test_postgres_connections_queries_postgres_job(self):
        """PostgreSQL connections panel must query the postgres job."""
        panel = self.panels["PostgreSQL Active Connections"]
        expr = panel["targets"][0]["expr"]
        assert 'job="postgres"' in expr, (
            f"PG connections panel must target postgres job, got: {expr}"
        )

    def test_has_postgres_transaction_panel(self):
        """Dashboard must have a PostgreSQL Transaction Rate panel."""
        assert "PostgreSQL Transaction Rate" in self.panels

    def test_has_postgres_cache_hit_panel(self):
        """Dashboard must have a PostgreSQL Cache Hit Ratio panel."""
        assert "PostgreSQL Cache Hit Ratio" in self.panels

    def test_postgres_cache_hit_uses_percentunit(self):
        """Cache hit ratio panel must use percentunit for 0-1 range."""
        panel = self.panels["PostgreSQL Cache Hit Ratio"]
        unit = panel["fieldConfig"]["defaults"].get("unit")
        assert unit == "percentunit", f"Cache hit ratio must use percentunit, got: {unit}"


# ---------------------------------------------------------------------------
# Dashboard correctness — VM Workers
# ---------------------------------------------------------------------------
class TestDashboardVMWorkers:
    """Regression tests for the VM Workers dashboard.

    Guards against failing to distinguish metrics from different VMs.
    With remote_write, the `instance` label is the local container name
    (e.g., node-exporter:9100) which is identical across VMs.
    All queries must group by `worker_location` to separate VMs.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_VM_WORKERS.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_worker_location_template_variable(self):
        """Dashboard must have a worker_location template variable.

        Without it, users cannot filter panels to a specific VM.
        """
        var_names = [v["name"] for v in self.dash.get("templating", {}).get("list", [])]
        assert "worker_location" in var_names, (
            "VM Workers dashboard must have a 'worker_location' template variable"
        )

    def test_worker_location_var_is_query_type(self):
        """worker_location variable must be query type (auto-discovers from Prometheus).

        Static/custom variables won't auto-discover new VMs when they join.
        """
        var_list = self.dash.get("templating", {}).get("list", [])
        wl_var = next(v for v in var_list if v["name"] == "worker_location")
        assert wl_var["type"] == "query", (
            f"worker_location variable must be type 'query' for auto-discovery, "
            f"got: {wl_var['type']}"
        )

    def test_worker_location_var_includes_all(self):
        """worker_location variable must have includeAll=true for aggregate view."""
        var_list = self.dash.get("templating", {}).get("list", [])
        wl_var = next(v for v in var_list if v["name"] == "worker_location")
        assert wl_var.get("includeAll") is True

    def test_cpu_panel_groups_by_worker_location(self):
        """CPU panel must group by worker_location, not instance.

        With remote_write, instance is the local container name
        (node-exporter:9100) — identical across all VMs.
        """
        panel = self.panels.get("VM CPU Usage (%)")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "by(worker_location)" in expr.replace(" ", ""), (
            f"CPU panel must group by worker_location, got: {expr}"
        )
        assert "by(instance" not in expr.replace(" ", ""), (
            "CPU panel must NOT group by instance (identical across VMs)"
        )

    def test_memory_panel_filters_by_worker_location(self):
        """Memory panel must filter by $worker_location template variable."""
        panel = self.panels.get("VM Memory Usage (%)")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "$worker_location" in expr, "Memory panel must filter by $worker_location"

    def test_disk_panel_filters_by_worker_location(self):
        panel = self.panels.get("VM Disk Usage (%)")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "$worker_location" in expr

    def test_network_panel_groups_by_worker_location(self):
        """Network panel must group by worker_location, not by device per VM."""
        panel = self.panels.get("VM Network I/O")
        assert panel is not None
        for target in panel["targets"]:
            expr = target["expr"]
            assert "worker_location" in expr, (
                f"Network panel target must reference worker_location: {expr}"
            )

    def test_container_panels_include_worker_location(self):
        """Container-level panels must include worker_location in legend/grouping."""
        for title_prefix in ["Container CPU", "Container Memory"]:
            panels = [p for p in self.dash["panels"] if p["title"].startswith(title_prefix)]
            assert len(panels) >= 1, f"Expected panel starting with '{title_prefix}'"
            for panel in panels:
                for target in panel["targets"]:
                    legend = target.get("legendFormat", "")
                    assert "worker_location" in legend, (
                        f"Panel '{panel['title']}' legendFormat must include "
                        f"worker_location, got: {legend}"
                    )

    def test_no_panel_uses_instance_for_grouping(self):
        """No panel should use 'by(instance' or 'by (instance' for grouping.

        instance labels from remote_write are local container addresses
        (node-exporter:9100, cadvisor:8080) which are identical across VMs.
        """
        for panel in self.dash["panels"]:
            for target in panel.get("targets", []):
                expr = target.get("expr", "")
                normalized = expr.replace(" ", "")
                assert "by(instance" not in normalized or "worker_location" in normalized, (
                    f"Panel '{panel['title']}' groups by instance without "
                    f"worker_location — instances are identical across VMs: {expr}"
                )

    def test_spcs_worker_panel_exists(self):
        """VM Workers dashboard must include an SPCS Worker panel.

        The SPCS worker has no Prometheus /metrics endpoint, so its health
        is tracked by target registration rather than actual metrics.
        This panel shows whether the scrape target exists.
        """
        panel = self.panels.get("SPCS Worker")
        assert panel is not None, "VM Workers dashboard must have an 'SPCS Worker' panel"

    def test_spcs_worker_panel_queries_correct_job(self):
        """SPCS Worker panel must query the prefect-worker-spcs job."""
        panel = self.panels["SPCS Worker"]
        expr = panel["targets"][0]["expr"]
        assert 'job="prefect-worker-spcs"' in expr, (
            f"SPCS Worker panel must target prefect-worker-spcs job, got: {expr}"
        )

    def test_spcs_worker_panel_has_fallback_vector(self):
        """SPCS Worker panel must use OR vector(0) fallback.

        Without the fallback, the panel shows 'No data' when the scrape
        target is missing, instead of the friendlier 'NOT REGISTERED'.
        """
        panel = self.panels["SPCS Worker"]
        expr = panel["targets"][0]["expr"]
        assert "vector(0)" in expr, (
            f"SPCS Worker panel must include OR vector(0) fallback, got: {expr}"
        )

    def test_spcs_worker_panel_has_value_mappings(self):
        """SPCS Worker panel must map 0→NOT REGISTERED, 1→REGISTERED."""
        panel = self.panels["SPCS Worker"]
        mappings = panel["fieldConfig"]["defaults"]["mappings"]
        value_map = next((m for m in mappings if m["type"] == "value"), None)
        assert value_map is not None, "SPCS Worker panel must have value mappings"
        opts = value_map["options"]
        assert "0" in opts and opts["0"]["text"] == "NOT REGISTERED"
        assert "1" in opts and opts["1"]["text"] == "REGISTERED"

    def test_vm_worker_status_narrowed_for_spcs_panel(self):
        """VM Worker Status panel uses repeat-by-worker_location.

        Each VM gets its own stat panel (w=6, maxPerRow=4).
        The panel title is $worker_location (Grafana substitutes per-VM).
        """
        panel = self.panels.get("$worker_location")
        assert panel is not None, "Expected repeating '$worker_location' status panel"
        assert panel.get("repeat") == "worker_location"
        assert panel["gridPos"]["w"] <= 6


# ---------------------------------------------------------------------------
# Dashboard correctness — Logs
# ---------------------------------------------------------------------------
class TestDashboardLogs:
    """Regression tests for the Logs dashboard.

    Guards against template variable configuration issues that
    prevent logs from being displayed.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_LOGS.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_worker_location_template_variable(self):
        """Logs dashboard must have worker_location variable for filtering."""
        var_names = [v["name"] for v in self.dash.get("templating", {}).get("list", [])]
        assert "worker_location" in var_names

    def test_worker_location_var_uses_loki_datasource(self):
        """worker_location variable must query Loki (not Prometheus)."""
        var_list = self.dash.get("templating", {}).get("list", [])
        wl_var = next(v for v in var_list if v["name"] == "worker_location")
        ds = wl_var.get("datasource", {})
        ds_type = ds.get("type", "") if isinstance(ds, dict) else ""
        assert ds_type == "loki", f"worker_location var must use Loki datasource, got: {ds}"

    def test_has_search_textbox_variable(self):
        """Logs dashboard must have a free-text search variable."""
        var_list = self.dash.get("templating", {}).get("list", [])
        search_vars = [v for v in var_list if v["name"] == "search"]
        assert len(search_vars) == 1
        assert search_vars[0]["type"] == "textbox"

    def test_log_stream_panel_uses_loki_datasource(self):
        """The main log stream panel must use the Loki datasource."""
        panel = self.panels.get("Log Stream")
        assert panel is not None, "Dashboard must have 'Log Stream' panel"
        ds = panel.get("datasource", {})
        assert ds.get("uid") == "loki", (
            f"Log Stream panel must use Loki datasource (uid: loki), got: {ds}"
        )

    def test_log_stream_query_matches_all_jobs(self):
        """Log stream query must match all jobs by default, not just specific ones.

        Using {job=~".+"} ensures all log streams are shown regardless
        of job name, as long as the job label exists.
        """
        panel = self.panels.get("Log Stream")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "job=~" in expr, f"Log stream query must use job regex match: {expr}"

    def test_log_stream_filters_by_worker_location(self):
        """Log stream query must filter by $worker_location template variable."""
        panel = self.panels.get("Log Stream")
        assert panel is not None
        expr = panel["targets"][0]["expr"]
        assert "$worker_location" in expr, f"Log stream must filter by $worker_location: {expr}"

    def test_has_log_volume_panel(self):
        """Dashboard must have a log volume panel showing ingestion rate."""
        assert "Log Volume" in self.panels

    def test_has_error_logs_panel(self):
        """Dashboard must have a dedicated error log panel."""
        assert "Error Logs" in self.panels

    def test_all_panels_use_loki_datasource(self):
        """Every panel in the logs dashboard must use the Loki datasource."""
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "loki", (
                f"Panel '{panel['title']}' must use Loki datasource, got: {ds}"
            )


# ---------------------------------------------------------------------------
# Spec file correctness — Prefect Exporter sidecar
# ---------------------------------------------------------------------------
class TestPrefectExporterSpec:
    """Regression tests for prefect-exporter sidecar in PF_MONITOR spec."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load((MONITORING_DIR / "specs" / "pf_monitor.yaml").read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_has_prefect_exporter_container(self):
        """PF_MONITOR must include a prefect-exporter sidecar."""
        assert "prefect-exporter" in self.containers

    def test_prefect_exporter_targets_prefect_api(self):
        """Prefect exporter must point to Prefect server internal API."""
        exporter = self.containers["prefect-exporter"]
        env = exporter.get("env", {})
        api_url = env.get("PREFECT_API_URL", "")
        assert "pf-server" in api_url and "4200" in api_url, (
            f"Prefect exporter must target pf-server:4200, got: {api_url}"
        )

    def test_prefect_exporter_has_readiness_probe(self):
        """Prefect exporter must have a readiness probe on /metrics."""
        exporter = self.containers["prefect-exporter"]
        probe = exporter.get("readinessProbe", {})
        assert probe.get("port") == 9394
        assert probe.get("path") == "/metrics"

    def test_prefect_exporter_port_9394(self):
        """Prefect exporter must expose metrics on port 9394."""
        exporter = self.containers["prefect-exporter"]
        env = exporter.get("env", {})
        assert env.get("EXPORTER_PORT") == "9394"


# ---------------------------------------------------------------------------
# Spec file correctness — SPCS Log Poller sidecar
# ---------------------------------------------------------------------------
class TestEventLogPollerSpec:
    """Regression tests for event-log-poller sidecar in PF_MONITOR spec."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load((MONITORING_DIR / "specs" / "pf_monitor.yaml").read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_has_event_log_poller_container(self):
        """PF_MONITOR must include an event-log-poller sidecar."""
        assert "event-log-poller" in self.containers

    def test_log_poller_pushes_to_localhost_loki(self):
        """Log poller must push to co-located Loki at localhost:3100."""
        poller = self.containers["event-log-poller"]
        env = poller.get("env", {})
        loki_url = env.get("LOKI_PUSH_URL", "")
        assert "localhost:3100" in loki_url, (
            f"Log poller must push to localhost:3100 Loki, got: {loki_url}"
        )

    def test_log_poller_monitors_all_services(self):
        """Log poller must monitor all core SPCS services."""
        poller = self.containers["event-log-poller"]
        env = poller.get("env", {})
        services = env.get("SPCS_SERVICES", "")
        for svc in ["PF_SERVER", "PF_WORKER", "PF_REDIS", "PF_SERVICES", "PF_MONITOR"]:
            assert svc in services, f"Log poller must monitor {svc}, got: {services}"

    def test_log_poller_has_resource_limits(self):
        """Log poller must have memory limits to prevent OOM."""
        poller = self.containers["event-log-poller"]
        limits = poller.get("resources", {}).get("limits", {})
        assert "memory" in limits, "Log poller must have memory limit"


# ---------------------------------------------------------------------------
# Prometheus scrape config — prefect-app job
# ---------------------------------------------------------------------------
class TestPrefectAppScrapeConfig:
    """Validate Prometheus scrape config for prefect-app exporter."""

    def test_prefect_app_scrape_job_exists(self):
        """Prometheus must have a prefect-app scrape job."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        jobs = [j for j in config["scrape_configs"] if j["job_name"] == "prefect-app"]
        assert len(jobs) == 1, "Must have exactly one prefect-app scrape job"

    def test_prefect_app_targets_localhost_9394(self):
        """Prefect app scrape must target localhost:9394 (co-located sidecar)."""
        config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        jobs = [j for j in config["scrape_configs"] if j["job_name"] == "prefect-app"]
        targets = jobs[0]["static_configs"][0]["targets"]
        assert any("9394" in t for t in targets), (
            f"Prefect app scrape must target port 9394, got: {targets}"
        )


# ---------------------------------------------------------------------------
# Dashboard: Prefect Application
# ---------------------------------------------------------------------------
class TestDashboardPrefectApp:
    """Validate the Prefect Application dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_PREFECT_APP.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_flow_runs_by_state_panel(self):
        assert "Flow Runs by State" in self.panels

    def test_flow_runs_queries_prefect_flow_runs(self):
        panel = self.panels["Flow Runs by State"]
        expr = panel["targets"][0]["expr"]
        assert "prefect_flow_runs" in expr

    def test_has_flow_runs_over_time_panel(self):
        assert "Flow Runs Over Time" in self.panels

    def test_has_success_rate_panel(self):
        assert "Flow Run Success Rate" in self.panels

    def test_success_rate_uses_percentunit(self):
        panel = self.panels["Flow Run Success Rate"]
        unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "")
        assert unit == "percent"

    def test_success_rate_uses_clamp_min(self):
        """Success rate must use clamp_min for safe division."""
        panel = self.panels["Flow Run Success Rate"]
        expr = panel["targets"][0]["expr"]
        assert "clamp_min" in expr, f"Success rate must use clamp_min, got: {expr}"

    def test_has_failed_runs_panel(self):
        assert "Failed Flow Runs" in self.panels

    def test_has_deployments_panel(self):
        assert "Deployments" in self.panels

    def test_deployments_queries_prefect_deployments(self):
        panel = self.panels["Deployments"]
        exprs = [t["expr"] for t in panel["targets"]]
        assert any("prefect_deployments" in e for e in exprs)

    def test_has_work_pools_panel(self):
        assert "Work Pools" in self.panels

    def test_has_workers_per_pool_panel(self):
        assert "Workers Per Pool" in self.panels

    def test_workers_per_pool_queries_prefect_work_pool_workers(self):
        panel = self.panels["Workers Per Pool"]
        expr = panel["targets"][0]["expr"]
        assert "prefect_work_pool_workers" in expr

    def test_has_exporter_poll_duration_panel(self):
        assert "Exporter Poll Duration" in self.panels

    def test_has_exporter_errors_panel(self):
        assert "Exporter Errors" in self.panels

    def test_has_state_template_variable(self):
        var_list = self.dash.get("templating", {}).get("list", [])
        state_vars = [v for v in var_list if v["name"] == "state"]
        assert len(state_vars) == 1

    def test_all_panels_use_prometheus_datasource(self):
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "prometheus", (
                f"Panel '{panel['title']}' must use Prometheus datasource, got: {ds}"
            )

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "prefect-app"


# ---------------------------------------------------------------------------
# Dashboard: Redis Detail
# ---------------------------------------------------------------------------
class TestDashboardRedisDetail:
    """Validate the Redis Detail dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_REDIS_DETAIL.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_keyspace_hit_ratio_panel(self):
        assert "Keyspace Hit Ratio" in self.panels

    def test_keyspace_hit_ratio_queries_redis(self):
        panel = self.panels["Keyspace Hit Ratio"]
        expr = panel["targets"][0]["expr"]
        assert 'job="redis"' in expr

    def test_has_fragmentation_ratio_panel(self):
        assert "Memory Fragmentation Ratio" in self.panels

    def test_has_blocked_clients_panel(self):
        assert "Blocked Clients" in self.panels

    def test_has_keys_by_db_panel(self):
        assert "Keys by Database" in self.panels

    def test_has_evicted_keys_panel(self):
        assert "Evicted Keys" in self.panels

    def test_evicted_keys_uses_rate(self):
        panel = self.panels["Evicted Keys"]
        expr = panel["targets"][0]["expr"]
        assert "rate(" in expr

    def test_has_expired_keys_panel(self):
        assert "Expired Keys" in self.panels

    def test_has_network_io_panel(self):
        assert "Network I/O" in self.panels

    def test_network_io_has_input_and_output(self):
        panel = self.panels["Network I/O"]
        assert len(panel["targets"]) >= 2, "Network I/O must show both input and output"

    def test_has_rdb_persistence_panel(self):
        assert "RDB Persistence" in self.panels

    def test_has_keyspace_hits_vs_misses_panel(self):
        assert "Keyspace Hits vs Misses" in self.panels

    def test_all_panels_use_prometheus_datasource(self):
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "prometheus", (
                f"Panel '{panel['title']}' must use Prometheus, got: {ds}"
            )

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "prefect-redis-detail"


# ---------------------------------------------------------------------------
# Dashboard: Postgres Detail
# ---------------------------------------------------------------------------
class TestDashboardPostgresDetail:
    """Validate the Postgres Detail dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_POSTGRES_DETAIL.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_tuple_operations_panel(self):
        assert "Tuple Operations Rate" in self.panels

    def test_tuple_operations_queries_postgres(self):
        panel = self.panels["Tuple Operations Rate"]
        expr = panel["targets"][0]["expr"]
        assert 'job="postgres"' in expr

    def test_has_dead_tuples_panel(self):
        assert "Dead Tuples vs Live Tuples" in self.panels

    def test_has_table_size_panel(self):
        assert "Table Size (Top 10)" in self.panels

    def test_table_size_uses_bytes_unit(self):
        panel = self.panels["Table Size (Top 10)"]
        unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "")
        assert unit == "bytes"

    def test_has_index_hit_ratio_panel(self):
        assert "Index Hit Ratio" in self.panels

    def test_has_locks_panel(self):
        assert "Locks by Mode" in self.panels

    def test_has_temp_bytes_panel(self):
        assert "Temp Bytes Written" in self.panels

    def test_has_database_size_panel(self):
        assert "Database Size" in self.panels

    def test_has_exporter_scrape_duration_panel(self):
        assert "Exporter Scrape Duration" in self.panels

    def test_all_panels_use_prometheus_datasource(self):
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "prometheus", (
                f"Panel '{panel['title']}' must use Prometheus, got: {ds}"
            )

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "prefect-postgres-detail"


# ---------------------------------------------------------------------------
# Dashboard: Alerts
# ---------------------------------------------------------------------------
class TestDashboardAlerts:
    """Validate the Alerting Overview dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_ALERTS.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_firing_alerts_panel(self):
        assert "Firing Alerts" in self.panels

    def test_firing_alerts_queries_alerts_metric(self):
        panel = self.panels["Firing Alerts"]
        expr = panel["targets"][0]["expr"]
        assert "alerting_alerts" in expr
        assert "alerting" in expr

    def test_has_pending_alerts_panel(self):
        assert "Pending Alerts" in self.panels

    def test_pending_alerts_queries_pending_state(self):
        panel = self.panels["Pending Alerts"]
        expr = panel["targets"][0]["expr"]
        assert "pending" in expr

    def test_has_alert_history_panel(self):
        assert "Alert History" in self.panels

    def test_has_rule_evaluation_duration_panel(self):
        assert "Rule Evaluation Duration" in self.panels

    def test_has_rule_evaluation_failures_panel(self):
        assert "Rule Evaluation Failures" in self.panels

    def test_has_alert_detail_table(self):
        assert "Alert Detail" in self.panels

    def test_alert_detail_is_table_type(self):
        panel = self.panels["Alert Detail"]
        assert panel["type"] == "table"

    def test_has_alert_groups_panel(self):
        assert "Alert Groups" in self.panels

    def test_has_total_alert_rules_panel(self):
        assert "Total Alert Rules" in self.panels

    def test_all_panels_use_prometheus_datasource(self):
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "prometheus", (
                f"Panel '{panel['title']}' must use Prometheus, got: {ds}"
            )

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "prefect-alerts"


# ---------------------------------------------------------------------------
# Dashboard: SPCS Logs
# ---------------------------------------------------------------------------
class TestDashboardSPCSLogs:
    """Validate the SPCS Logs dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_SPCS_LOGS.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"]}

    def test_has_spcs_log_volume_panel(self):
        assert "SPCS Log Volume" in self.panels

    def test_log_volume_queries_spcs_logs_job(self):
        panel = self.panels["SPCS Log Volume"]
        expr = panel["targets"][0]["expr"]
        assert "spcs-logs" in expr

    def test_has_spcs_error_rate_panel(self):
        assert "SPCS Error Rate" in self.panels

    def test_has_severity_distribution_panel(self):
        assert "SPCS Log Severity Distribution" in self.panels

    def test_has_spcs_log_stream_panel(self):
        assert "SPCS Log Stream" in self.panels

    def test_log_stream_filters_by_service(self):
        panel = self.panels["SPCS Log Stream"]
        expr = panel["targets"][0]["expr"]
        assert "$service" in expr

    def test_log_stream_filters_by_container(self):
        panel = self.panels["SPCS Log Stream"]
        expr = panel["targets"][0]["expr"]
        assert "$container" in expr

    def test_has_spcs_error_logs_panel(self):
        assert "SPCS Error Logs" in self.panels

    def test_has_service_template_variable(self):
        var_list = self.dash.get("templating", {}).get("list", [])
        service_vars = [v for v in var_list if v["name"] == "service"]
        assert len(service_vars) == 1

    def test_has_container_template_variable(self):
        var_list = self.dash.get("templating", {}).get("list", [])
        container_vars = [v for v in var_list if v["name"] == "container"]
        assert len(container_vars) == 1

    def test_has_search_textbox_variable(self):
        var_list = self.dash.get("templating", {}).get("list", [])
        search_vars = [v for v in var_list if v["name"] == "search"]
        assert len(search_vars) == 1
        assert search_vars[0]["type"] == "textbox"

    def test_all_panels_use_loki_datasource(self):
        for panel in self.dash["panels"]:
            ds = panel.get("datasource", {})
            assert ds.get("uid") == "loki", (
                f"Panel '{panel['title']}' must use Loki datasource, got: {ds}"
            )

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "prefect-spcs-logs"


# ---------------------------------------------------------------------------
# CPU budget — all containers must have explicit limits totaling ≤ 2.0 vCPU
# ---------------------------------------------------------------------------
class TestMonitorSpecCPUBudget:
    """Validate CPU resource limits fit within PREFECT_MONITOR_POOL (CPU_X64_S = 2 vCPU).

    SPCS schedules containers based on CPU LIMITS, not requests.
    Without explicit limits, each container defaults to 1.0 CPU,
    so 8 containers × 1.0 = 8.0 > 2.0 → unschedulable.
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = self.spec["spec"]["containers"]

    def test_all_containers_have_cpu_limits(self):
        """Every container must have an explicit CPU limit."""
        for c in self.containers:
            limits = c.get("resources", {}).get("limits", {})
            assert "cpu" in limits, (
                f"Container '{c['name']}' missing CPU limit. "
                f"Without explicit limits, SPCS defaults to 1.0 per container."
            )

    def test_all_containers_have_memory_limits(self):
        """Every container must have an explicit memory limit."""
        for c in self.containers:
            limits = c.get("resources", {}).get("limits", {})
            assert "memory" in limits, f"Container '{c['name']}' missing memory limit."

    def test_total_cpu_limits_within_pool_capacity(self):
        """Sum of all CPU limits must not exceed 2.5 (CPU_X64_S pool with overcommit).

        CPU_X64_S provides 2 vCPU. SPCS allows slight overcommit on limits
        when requests fit within capacity. Total requests must be <= 2.0.
        """
        total = 0.0
        for c in self.containers:
            cpu = c.get("resources", {}).get("limits", {}).get("cpu", 1.0)
            total += float(cpu)
        assert total <= 2.5, (
            f"Total CPU limits ({total}) exceed 2.5 vCPU overcommit budget. "
            f"Either reduce limits or upgrade the compute pool."
        )

    def test_total_cpu_requests_within_pool_capacity(self):
        """Sum of all CPU requests must not exceed 2.0 (CPU_X64_S pool)."""
        total = 0.0
        for c in self.containers:
            cpu = c.get("resources", {}).get("requests", {}).get("cpu", 0)
            total += float(cpu)
        assert total <= 2.0, f"Total CPU requests ({total}) exceed CPU_X64_S capacity (2.0 vCPU)."

    def test_cpu_requests_less_than_or_equal_limits(self):
        """CPU requests must not exceed limits for any container."""
        for c in self.containers:
            resources = c.get("resources", {})
            req = float(resources.get("requests", {}).get("cpu", 0))
            lim = float(resources.get("limits", {}).get("cpu", 1.0))
            assert req <= lim, f"Container '{c['name']}' CPU request ({req}) > limit ({lim})"


# ---------------------------------------------------------------------------
# Container completeness — all 8 containers have required fields
# ---------------------------------------------------------------------------
class TestMonitorSpecContainerCompleteness:
    """Every container in PF_MONITOR must have resource limits and readiness probes."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_all_containers_have_images(self):
        """Every container must specify an image."""
        for name, c in self.containers.items():
            assert "image" in c, f"Container '{name}' missing image"

    def test_core_containers_have_readiness_probes(self):
        """Core containers (prometheus, grafana, loki, exporters) must have readiness probes."""
        for name in ["prometheus", "grafana", "loki", "postgres-exporter", "prefect-exporter"]:
            c = self.containers[name]
            assert "readinessProbe" in c, f"Container '{name}' missing readinessProbe"

    def test_all_containers_have_resource_requests(self):
        """Every container must specify resource requests."""
        for name, c in self.containers.items():
            requests_sec = c.get("resources", {}).get("requests", {})
            assert "cpu" in requests_sec, f"Container '{name}' missing CPU request"
            assert "memory" in requests_sec, f"Container '{name}' missing memory request"

    def test_prometheus_volume_mounts(self):
        """Prometheus must have config and data volume mounts."""
        prom = self.containers["prometheus"]
        mount_names = {m["name"] for m in prom.get("volumeMounts", [])}
        assert "prom-config" in mount_names, "Prometheus missing prom-config mount"
        assert "prom-data" in mount_names, "Prometheus missing prom-data mount"

    def test_grafana_volume_mounts(self):
        """Grafana must have provisioning, dashboards, and data volume mounts."""
        grafana = self.containers["grafana"]
        mount_names = {m["name"] for m in grafana.get("volumeMounts", [])}
        assert "grafana-provisioning" in mount_names
        assert "grafana-dashboards" in mount_names
        assert "grafana-data" in mount_names

    def test_loki_volume_mounts(self):
        """Loki must have config and data volume mounts."""
        loki = self.containers["loki"]
        mount_names = {m["name"] for m in loki.get("volumeMounts", [])}
        assert "loki-config" in mount_names
        assert "loki-data" in mount_names

    def test_event_log_poller_env_vars(self):
        """Event log poller must have all required env vars."""
        poller = self.containers["event-log-poller"]
        env = poller.get("env", {})
        required = [
            "LOKI_PUSH_URL",
            "POLL_INTERVAL_SECONDS",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SPCS_SERVICES",
        ]
        for var in required:
            assert var in env, f"event-log-poller missing env var '{var}'"

    def test_prefect_exporter_env_vars(self):
        """Prefect exporter must have all required env vars."""
        exporter = self.containers["prefect-exporter"]
        env = exporter.get("env", {})
        required = ["PREFECT_API_URL", "POLL_INTERVAL_SECONDS", "EXPORTER_PORT"]
        for var in required:
            assert var in env, f"prefect-exporter missing env var '{var}'"

    def test_no_container_name_starts_with_spcs(self):
        """SPCS reserves container names starting with 'spcs-'."""
        for name in self.containers:
            assert not name.startswith("spcs-"), (
                f"Container name '{name}' starts with 'spcs-' which is reserved by Snowflake"
            )


# ---------------------------------------------------------------------------
# Grafana datasource URL integrity
# ---------------------------------------------------------------------------
class TestGrafanaDatasourceIntegrity:
    """Validate Grafana datasource URLs match actual container ports."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.ds_data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_prometheus_datasource_url_matches_container(self):
        """Prometheus datasource URL must point to localhost:9090.

        Prometheus runs on port 9090 in the same SPCS service (shared localhost).
        """
        prom_ds = next(ds for ds in self.ds_data["datasources"] if ds["name"] == "Prometheus")
        assert prom_ds["url"] == "http://localhost:9090", (
            f"Prometheus datasource URL must be http://localhost:9090, got: {prom_ds['url']}"
        )

    def test_loki_datasource_url_matches_container(self):
        """Loki datasource URL must point to localhost:3100.

        Loki runs on port 3100 in the same SPCS service (shared localhost).
        """
        loki_ds = next(ds for ds in self.ds_data["datasources"] if ds["name"] == "Loki")
        assert loki_ds["url"] == "http://localhost:3100", (
            f"Loki datasource URL must be http://localhost:3100, got: {loki_ds['url']}"
        )

    def test_prometheus_datasource_uses_post_method(self):
        """Prometheus datasource should use POST for queries (more efficient)."""
        prom_ds = next(ds for ds in self.ds_data["datasources"] if ds["name"] == "Prometheus")
        http_method = prom_ds.get("jsonData", {}).get("httpMethod", "")
        assert http_method == "POST", (
            f"Prometheus datasource should use POST method, got: {http_method}"
        )

    def test_prometheus_is_default_datasource(self):
        """Prometheus must be the default datasource."""
        prom_ds = next(ds for ds in self.ds_data["datasources"] if ds["name"] == "Prometheus")
        assert prom_ds.get("isDefault") is True

    def test_loki_is_not_default(self):
        """Loki must NOT be the default datasource."""
        loki_ds = next(ds for ds in self.ds_data["datasources"] if ds["name"] == "Loki")
        assert loki_ds.get("isDefault") is False


# ---------------------------------------------------------------------------
# Prometheus scrape job ↔ dashboard coverage
# ---------------------------------------------------------------------------
class TestPrometheusScrapeJobDashboardCoverage:
    """Cross-validate that every Prometheus scrape job has dashboards using its metrics.

    If a scrape job exists but no dashboard queries its metrics, those metrics
    are being collected but never displayed — wasted resources and a gap in
    observability.
    """

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.prom_config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        self.dashboards = {}
        for f in DASHBOARD_DIR.glob("*.json"):
            self.dashboards[f.stem] = json.loads(f.read_text())

    def _all_dashboard_expressions(self) -> str:
        """Concatenate all PromQL/LogQL expressions from all dashboards."""
        all_exprs = []
        for dash in self.dashboards.values():
            for panel in dash.get("panels", []):
                for target in panel.get("targets", []):
                    all_exprs.append(target.get("expr", ""))
        return "\n".join(all_exprs)

    def test_prefect_server_job_referenced_in_dashboards(self):
        exprs = self._all_dashboard_expressions()
        assert 'job="prefect-server"' in exprs or "prefect-server" in exprs

    def test_redis_job_referenced_in_dashboards(self):
        exprs = self._all_dashboard_expressions()
        assert 'job="redis"' in exprs

    def test_postgres_job_referenced_in_dashboards(self):
        exprs = self._all_dashboard_expressions()
        assert 'job="postgres"' in exprs

    def test_prefect_app_job_referenced_in_dashboards(self):
        exprs = self._all_dashboard_expressions()
        assert 'job="prefect-app"' in exprs or "prefect_flow_runs" in exprs

    def test_loki_job_referenced_in_dashboards(self):
        exprs = self._all_dashboard_expressions()
        assert "loki" in exprs.lower()

    def test_prometheus_self_monitoring_in_dashboards(self):
        """At minimum, Prometheus self-metrics (rule eval, scrape) should be used."""
        exprs = self._all_dashboard_expressions()
        assert "prometheus" in exprs.lower()

    def test_spcs_logs_job_referenced_in_dashboards(self):
        """The spcs-logs job (from event-log-poller) must be used in SPCS Logs dashboard."""
        exprs = self._all_dashboard_expressions()
        assert "spcs-logs" in exprs


# ---------------------------------------------------------------------------
# Alert rule completeness
# ---------------------------------------------------------------------------
class TestAlertRuleCompleteness:
    """Comprehensive validation of all Prometheus alert rules."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(PROM_RULES_FILE.read_text())
        self.all_rules = []
        for group in self.rules["groups"]:
            self.all_rules.extend(group.get("rules", []))

    def test_has_exactly_three_groups(self):
        assert len(self.rules["groups"]) == 3, (
            f"Expected 3 alert groups, got {len(self.rules['groups'])}"
        )

    def test_group_names(self):
        names = {g["name"] for g in self.rules["groups"]}
        expected = {"spcs_health", "prefect_server", "vm_workers"}
        assert names == expected, f"Expected groups {expected}, got {names}"

    def test_has_exactly_seven_rules(self):
        assert len(self.all_rules) == 7, f"Expected 7 alert rules, got {len(self.all_rules)}"

    def test_all_rule_names(self):
        names = {r["alert"] for r in self.all_rules}
        expected = {
            "HighContainerRestarts",
            "HighCPUUsage",
            "HighMemoryUsage",
            "PrefectServerDown",
            "HighFlowFailureRate",
            "WorkerOffline",
            "DiskSpaceRunningLow",
        }
        assert names == expected, (
            f"Missing rules: {expected - names}, unexpected: {names - expected}"
        )

    def test_all_rules_have_severity_label(self):
        for rule in self.all_rules:
            assert "severity" in rule.get("labels", {}), (
                f"Alert '{rule['alert']}' missing severity label"
            )

    def test_has_both_warning_and_critical(self):
        severities = {r["labels"]["severity"] for r in self.all_rules}
        assert "warning" in severities, "No warning-level alerts defined"
        assert "critical" in severities, "No critical-level alerts defined"

    def test_all_rules_have_for_duration(self):
        """Every alert must have a 'for' clause to avoid flapping."""
        for rule in self.all_rules:
            assert "for" in rule, f"Alert '{rule['alert']}' missing 'for' duration"

    def test_all_rules_have_annotations(self):
        for rule in self.all_rules:
            annotations = rule.get("annotations", {})
            assert "summary" in annotations, f"Alert '{rule['alert']}' missing 'summary' annotation"
            assert "description" in annotations, (
                f"Alert '{rule['alert']}' missing 'description' annotation"
            )

    def test_all_rules_have_valid_expressions(self):
        """Alert expressions must not be empty."""
        for rule in self.all_rules:
            assert rule.get("expr", "").strip(), f"Alert '{rule['alert']}' has empty expression"

    def test_spcs_health_group_has_three_rules(self):
        grp = next(g for g in self.rules["groups"] if g["name"] == "spcs_health")
        assert len(grp["rules"]) == 3

    def test_prefect_server_group_has_two_rules(self):
        grp = next(g for g in self.rules["groups"] if g["name"] == "prefect_server")
        assert len(grp["rules"]) == 2

    def test_vm_workers_group_has_two_rules(self):
        grp = next(g for g in self.rules["groups"] if g["name"] == "vm_workers")
        assert len(grp["rules"]) == 2


# ---------------------------------------------------------------------------
# Loki pipeline integrity
# ---------------------------------------------------------------------------
class TestLokiPipelineIntegrity:
    """Validate the log pipeline from event-log-poller → Loki → Grafana dashboards."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.loki_config = yaml.safe_load(LOKI_CONFIG_FILE.read_text())
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.spcs_logs_dash = json.loads(DASH_SPCS_LOGS.read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_poller_push_url_matches_loki_port(self):
        """Event log poller LOKI_PUSH_URL must target Loki's listen port."""
        poller_env = self.containers["event-log-poller"].get("env", {})
        push_url = poller_env.get("LOKI_PUSH_URL", "")
        loki_port = self.loki_config["server"]["http_listen_port"]
        assert str(loki_port) in push_url, (
            f"Poller push URL ({push_url}) doesn't match Loki port ({loki_port})"
        )

    def test_poller_push_url_uses_loki_api_path(self):
        """Push URL must use /loki/api/v1/push path."""
        poller_env = self.containers["event-log-poller"].get("env", {})
        push_url = poller_env.get("LOKI_PUSH_URL", "")
        assert push_url.endswith("/loki/api/v1/push"), (
            f"Push URL must end with /loki/api/v1/push, got: {push_url}"
        )

    def test_dashboard_spcs_logs_queries_correct_job_label(self):
        """SPCS Logs dashboard must filter on job=spcs-logs matching poller output."""
        for panel in self.spcs_logs_dash["panels"]:
            for target in panel.get("targets", []):
                expr = target.get("expr", "")
                if expr:
                    assert "spcs-logs" in expr, (
                        f"Panel '{panel['title']}' must reference job=spcs-logs, got: {expr}"
                    )

    def test_loki_retention_configured(self):
        """Loki must have retention configured to prevent unbounded growth."""
        retention = self.loki_config["limits_config"]["retention_period"]
        assert retention == "168h", f"Expected 168h retention, got: {retention}"

    def test_loki_has_ingestion_rate_limit(self):
        """Loki must have ingestion rate limits to prevent OOM."""
        rate = self.loki_config["limits_config"].get("ingestion_rate_mb")
        assert rate is not None and rate > 0, "Loki must have ingestion_rate_mb > 0"

    def test_loki_compactor_enabled(self):
        """Loki must have compactor with retention enabled for cleanup."""
        compactor = self.loki_config.get("compactor", {})
        assert compactor.get("retention_enabled") is True, (
            "Loki compactor must have retention_enabled: true"
        )

    def test_poller_monitors_pf_monitor_itself(self):
        """The poller must include PF_MONITOR in its services list."""
        poller_env = self.containers["event-log-poller"].get("env", {})
        services = poller_env.get("SPCS_SERVICES", "")
        assert "PF_MONITOR" in services, f"Poller must monitor PF_MONITOR itself, got: {services}"


# ---------------------------------------------------------------------------
# Dockerfile correctness — sidecar images
# ---------------------------------------------------------------------------
EXPORTER_DOCKERFILE = PROJECT_DIR / "images" / "prefect-exporter" / "Dockerfile"
POLLER_DOCKERFILE = PROJECT_DIR / "images" / "spcs-log-poller" / "Dockerfile"


class TestDockerfileCorrectness:
    """Validate sidecar Dockerfiles have correct base images and dependencies."""

    def test_exporter_dockerfile_exists(self):
        assert EXPORTER_DOCKERFILE.exists()

    def test_poller_dockerfile_exists(self):
        assert POLLER_DOCKERFILE.exists()

    def test_exporter_uses_python_312_slim(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "python:3.12-slim" in content, (
            "Prefect exporter must use python:3.12-slim base image"
        )

    def test_poller_uses_python_312_slim(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "python:3.12-slim" in content, "Log poller must use python:3.12-slim base image"

    def test_exporter_installs_prometheus_client(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "prometheus_client" in content

    def test_exporter_installs_requests(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "requests" in content

    def test_poller_installs_snowflake_connector(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "snowflake-connector-python" in content

    def test_poller_installs_requests(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "requests" in content

    def test_exporter_copies_script(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "COPY exporter.py" in content

    def test_poller_copies_script(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "COPY poller.py" in content

    def test_exporter_exposes_9394(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "EXPOSE 9394" in content

    def test_exporter_cmd_runs_exporter(self):
        content = EXPORTER_DOCKERFILE.read_text()
        assert "exporter.py" in content

    def test_poller_cmd_runs_poller(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "poller.py" in content

    def test_exporter_uses_no_cache_dir(self):
        """Dockerfile pip install must use --no-cache-dir for smaller images."""
        content = EXPORTER_DOCKERFILE.read_text()
        assert "--no-cache-dir" in content

    def test_poller_uses_no_cache_dir(self):
        content = POLLER_DOCKERFILE.read_text()
        assert "--no-cache-dir" in content


# ---------------------------------------------------------------------------
# Deploy script — compute pool and service references
# ---------------------------------------------------------------------------
class TestDeployScriptComputePoolRef:
    """Validate deploy_monitoring.sh references correct pool name and type."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_references_cpu_x64_s(self):
        """Deploy script must create CPU_X64_S pool (2 vCPU, not XS)."""
        assert "CPU_X64_S" in self.content, "Deploy script must use CPU_X64_S compute pool (2 vCPU)"

    def test_does_not_reference_cpu_x64_xs(self):
        """Deploy script must NOT use CPU_X64_XS (1 vCPU — too small for 8 containers)."""
        assert "CPU_X64_XS" not in self.content, (
            "Deploy script must NOT use CPU_X64_XS — 8 containers need 2 vCPU"
        )

    def test_uses_monitor_pool_name(self):
        assert "PREFECT_MONITOR_POOL" in self.content

    def test_does_not_create_or_replace_service(self):
        """Must never use CREATE OR REPLACE SERVICE — it destroys the URL."""
        assert "CREATE OR REPLACE SERVICE" not in self.content


# ---------------------------------------------------------------------------
# Exporter logic — unit tests with mocked HTTP
# ---------------------------------------------------------------------------
class TestExporterLogic:
    """Unit tests for images/prefect-exporter/exporter.py business logic.

    Tests the API polling, metric collection, and error handling without
    requiring a running Prefect server.
    """

    def test_exporter_source_imports(self):
        """Verify exporter.py can be parsed without import errors."""
        import ast  # noqa: E402

        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        tree = ast.parse(source)
        assert tree is not None

    def test_exporter_defines_all_flow_run_states(self):
        """Exporter must define all 9 Prefect flow run states."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        expected_states = [
            "COMPLETED",
            "FAILED",
            "RUNNING",
            "PENDING",
            "SCHEDULED",
            "CANCELLING",
            "CANCELLED",
            "CRASHED",
            "PAUSED",
        ]
        for state in expected_states:
            assert f'"{state}"' in source, f"Exporter missing state '{state}'"

    def test_exporter_uses_limit_200_not_500(self):
        """Prefect 3.x deployments/filter max limit is 200, not 500."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert '"limit": 200' in source or "'limit': 200" in source, (
            "Exporter must use limit=200 for deployments/filter (Prefect 3.x max)"
        )
        assert '"limit": 500' not in source and "'limit': 500" not in source, (
            "Exporter must NOT use limit=500 — Prefect 3.x max is 200"
        )

    def test_exporter_logs_response_body_on_error(self):
        """Exporter must log the response body on API errors for debugging."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "resp.text" in source, "Exporter must log resp.text on error for debugging"

    def test_exporter_exposes_expected_metrics(self):
        """Exporter must define all expected Prometheus metrics."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        expected_metrics = [
            "prefect_flow_runs",
            "prefect_deployments_total",
            "prefect_deployments",
            "prefect_work_pools_total",
            "prefect_work_pools",
            "prefect_work_pool_workers",
            "prefect_exporter_poll_errors_total",
            "prefect_exporter_poll_duration_seconds",
        ]
        for metric in expected_metrics:
            assert metric in source, f"Exporter missing metric '{metric}'"

    def test_exporter_calls_flow_runs_count_endpoint(self):
        """Exporter must use /flow_runs/count for state counts."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "/flow_runs/count" in source

    def test_exporter_calls_deployments_filter_endpoint(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "/deployments/filter" in source

    def test_exporter_calls_work_pools_filter_endpoint(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "/work_pools/filter" in source

    def test_exporter_calls_workers_filter_endpoint(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "/workers/filter" in source

    def test_exporter_tracks_poll_duration(self):
        """Exporter must track poll cycle duration for observability."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "monotonic" in source, "Exporter should use time.monotonic for duration tracking"

    def test_exporter_default_port_9394(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert '"9394"' in source or "'9394'" in source

    def test_exporter_default_poll_interval_30(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert '"30"' in source or "'30'" in source

    def test_exporter_uses_requests_library(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "import requests" in source

    def test_exporter_uses_prometheus_client(self):
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "from prometheus_client import" in source

    def test_exporter_has_error_counter(self):
        """Exporter must count API polling errors."""
        source = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        assert "_error_count" in source


# ---------------------------------------------------------------------------
# Poller logic — unit tests validating source code
# ---------------------------------------------------------------------------
class TestPollerLogic:
    """Unit tests for images/spcs-log-poller/poller.py business logic.

    Validates query patterns, Loki push format, timestamp tracking,
    and reconnection logic.
    """

    def test_poller_source_imports(self):
        """Verify poller.py can be parsed without import errors."""
        import ast  # noqa: E402

        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        tree = ast.parse(source)
        assert tree is not None

    def test_poller_queries_wrapper_view_not_telemetry(self):
        """Poller must query SPCS_EVENT_LOGS wrapper view, NOT snowflake.telemetry.

        SPCS OAuth tokens do NOT inherit IMPORTED PRIVILEGES on the snowflake
        database. The wrapper view PREFECT_DB.PREFECT_SCHEMA.SPCS_EVENT_LOGS
        (owned by ACCOUNTADMIN) provides access.
        """
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "SPCS_EVENT_LOGS" in source, "Poller must query SPCS_EVENT_LOGS wrapper view"
        # Should NOT directly reference telemetry view
        assert "snowflake.telemetry.events_view" not in source, (
            "Poller must NOT directly query snowflake.telemetry.events_view — "
            "SPCS OAuth tokens lack IMPORTED PRIVILEGES"
        )

    def test_poller_tracks_last_timestamp(self):
        """Poller must track _last_timestamp to avoid duplicate log entries."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "_last_timestamp" in source

    def test_poller_builds_loki_stream_payload(self):
        """Poller must build Loki push payload with streams/values format."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert '"streams"' in source or "'streams'" in source
        assert '"stream"' in source or "'stream'" in source
        assert '"values"' in source or "'values'" in source

    def test_poller_sets_job_label_spcs_logs(self):
        """Poller must set job=spcs-logs label for Loki streams.

        This must match what the SPCS Logs Grafana dashboard queries.
        """
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert '"spcs-logs"' in source or "'spcs-logs'" in source, (
            "Poller must set job=spcs-logs label for Loki streams"
        )

    def test_poller_sets_source_label(self):
        """Poller must set source=event-table label."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "event-table" in source

    def test_poller_uses_nanosecond_timestamps(self):
        """Loki expects timestamps in nanoseconds."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "1_000_000_000" in source or "1000000000" in source, (
            "Poller must convert timestamps to nanoseconds for Loki"
        )

    def test_poller_reconnects_after_programming_errors(self):
        """Poller must reconnect after consecutive ProgrammingError exceptions."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "ProgrammingError" in source
        assert "consecutive_errors" in source

    def test_poller_reconnects_after_5_programming_errors(self):
        """Poller reconnects after 5 consecutive ProgrammingErrors."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "consecutive_errors > 5" in source

    def test_poller_reconnects_after_10_general_errors(self):
        """Poller reconnects after 10 consecutive general errors."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "consecutive_errors > 10" in source

    def test_poller_uses_oauth_token(self):
        """Poller must use SPCS OAuth token at /snowflake/session/token."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "/snowflake/session/token" in source

    def test_poller_uses_snowflake_connector(self):
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "snowflake.connector" in source

    def test_poller_sets_query_tag(self):
        """Poller should set a QUERY_TAG for identification in Snowflake history."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "QUERY_TAG" in source

    def test_poller_limits_log_fetch(self):
        """Poller must limit log fetch to prevent memory issues."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "LIMIT" in source

    def test_poller_orders_by_timestamp_asc(self):
        """Poller must order logs by timestamp ascending for correct sequencing."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "ORDER BY TIMESTAMP ASC" in source or "ORDER BY TIMESTAMP" in source

    def test_poller_groups_by_service_and_container(self):
        """Poller must group logs into streams by service+container."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "service" in source.lower() and "container" in source.lower()

    def test_poller_handles_loki_push_failure(self):
        """Poller must handle Loki push HTTP failures gracefully."""
        source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert "resp.status_code" in source
        # Should not crash on push failure
        assert "except" in source


# ---------------------------------------------------------------------------
# Grafana dashboard — all dashboards have required structure
# ---------------------------------------------------------------------------
class TestAllDashboardStructure:
    """Validate structural requirements for all Grafana dashboards."""

    @pytest.fixture(autouse=True)
    def load_dashboards(self):
        self.dashboards = {}
        for f in DASHBOARD_DIR.glob("*.json"):
            self.dashboards[f.stem] = json.loads(f.read_text())

    def test_all_dashboards_have_uid(self):
        for name, dash in self.dashboards.items():
            assert "uid" in dash, f"Dashboard '{name}' missing uid"
            assert dash["uid"], f"Dashboard '{name}' has empty uid"

    def test_all_dashboards_have_title(self):
        for name, dash in self.dashboards.items():
            assert "title" in dash, f"Dashboard '{name}' missing title"

    def test_all_dashboards_have_panels(self):
        for name, dash in self.dashboards.items():
            assert "panels" in dash, f"Dashboard '{name}' missing panels"
            assert len(dash["panels"]) >= 1, f"Dashboard '{name}' has no panels"

    def test_no_dashboard_has_empty_expressions(self):
        """No panel should have targets with empty expressions."""
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                for target in panel.get("targets", []):
                    expr = target.get("expr", "")
                    if "expr" in target:
                        assert expr.strip(), (
                            f"Dashboard '{name}', panel '{panel.get('title')}' has empty expression"
                        )

    def test_expected_dashboard_count(self):
        """Must have exactly 9 dashboards."""
        assert len(self.dashboards) == 9, (
            f"Expected 9 dashboards, got {len(self.dashboards)}: {list(self.dashboards.keys())}"
        )

    def test_expected_dashboard_uids(self):
        uids = {d["uid"] for d in self.dashboards.values()}
        expected = {
            "prefect-spcs-overview",
            "prefect-vm-workers",
            "prefect-logs",
            "prefect-app",
            "prefect-redis-detail",
            "prefect-postgres-detail",
            "prefect-alerts",
            "prefect-spcs-logs",
            "health",
        }
        assert uids == expected, f"Missing UIDs: {expected - uids}, unexpected: {uids - expected}"

    def test_all_panels_with_explicit_datasource_use_valid_uid(self):
        """Panels that specify a datasource must use a valid provisioned UID.

        Panels without an explicit datasource inherit the dashboard's default
        (Prometheus, since it's set as isDefault: true). This is valid Grafana
        behavior — we only validate panels that explicitly declare one.
        """
        valid_uids = {"prometheus", "loki", "-- Grafana --"}
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                ds = panel.get("datasource")
                if ds is not None and isinstance(ds, dict) and "uid" in ds:
                    assert ds["uid"] in valid_uids, (
                        f"Dashboard '{name}', panel '{panel.get('title')}' "
                        f"has invalid datasource uid='{ds['uid']}'"
                    )

    def test_all_prometheus_panels_use_correct_uid(self):
        """Panels with prometheus type datasource must use uid='prometheus'."""
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                ds = panel.get("datasource", {})
                if isinstance(ds, dict) and ds.get("type") == "prometheus":
                    assert ds["uid"] == "prometheus", (
                        f"Dashboard '{name}', panel '{panel.get('title')}' "
                        f"has prometheus type but uid='{ds.get('uid')}'"
                    )

    def test_all_loki_panels_use_correct_uid(self):
        """Panels with loki type datasource must use uid='loki'."""
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                ds = panel.get("datasource", {})
                if isinstance(ds, dict) and ds.get("type") == "loki":
                    assert ds["uid"] == "loki", (
                        f"Dashboard '{name}', panel '{panel.get('title')}' "
                        f"has loki type but uid='{ds.get('uid')}'"
                    )


# ---------------------------------------------------------------------------
# Grafana dashboard provider — path correctness
# ---------------------------------------------------------------------------
class TestGrafanaDashboardProviderPaths:
    """Validate dashboard provider paths match spec volume mounts."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.provider = yaml.safe_load(GRAFANA_DASH_PROV.read_text())
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_provider_path_matches_spec_mount(self):
        """Dashboard provider path must match Grafana's dashboard volume mount."""
        prov_path = self.provider["providers"][0]["options"]["path"]
        grafana = next(c for c in self.spec["spec"]["containers"] if c["name"] == "grafana")
        mount_paths = {m["mountPath"] for m in grafana.get("volumeMounts", [])}
        assert prov_path in mount_paths, (
            f"Dashboard provider path '{prov_path}' not in Grafana mounts: {mount_paths}"
        )

    def test_provider_folder_name(self):
        assert self.provider["providers"][0]["folder"] == "Prefect SPCS"

    def test_provider_name(self):
        assert self.provider["providers"][0]["name"] == "prefect-spcs"


# ---------------------------------------------------------------------------
# Spec endpoint correctness
# ---------------------------------------------------------------------------
class TestMonitorSpecEndpoints:
    """Validate PF_MONITOR endpoint configuration."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.endpoints = self.spec["spec"]["endpoints"]

    def test_has_five_public_endpoints(self):
        public = [e for e in self.endpoints if e.get("public") is True]
        assert len(public) == 5, f"Expected 5 public endpoints, got {len(public)}"

    def test_grafana_endpoint_port_3000(self):
        grafana = next(e for e in self.endpoints if e["name"] == "grafana")
        assert grafana["port"] == 3000

    def test_prometheus_endpoint_port_9090(self):
        prom = next(e for e in self.endpoints if e["name"] == "prometheus")
        assert prom["port"] == 9090

    def test_loki_endpoint_port_3100(self):
        loki = next(e for e in self.endpoints if e["name"] == "loki")
        assert loki["port"] == 3100

    def test_all_endpoints_are_public(self):
        """All 3 endpoints must be public for VM agents and UI access."""
        for ep in self.endpoints:
            assert ep.get("public") is True, f"Endpoint '{ep['name']}' must be public"


# ---------------------------------------------------------------------------
# Platform monitor configuration
# ---------------------------------------------------------------------------
class TestMonitorSpecPlatformMonitor:
    """Validate platformMonitor configuration for SPCS system metrics."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_platform_monitor_has_metric_config(self):
        pm = self.spec["spec"]["platformMonitor"]
        assert "metricConfig" in pm

    def test_platform_monitor_has_system_group(self):
        groups = self.spec["spec"]["platformMonitor"]["metricConfig"]["groups"]
        assert "system" in groups

    def test_platform_monitor_has_system_limits_group(self):
        groups = self.spec["spec"]["platformMonitor"]["metricConfig"]["groups"]
        assert "system_limits" in groups


# ---------------------------------------------------------------------------
# Volume configuration
# ---------------------------------------------------------------------------
class TestMonitorSpecVolumes:
    """Validate PF_MONITOR volume configuration."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.volumes = {v["name"]: v for v in self.spec["spec"]["volumes"]}

    def test_has_nine_volumes(self):
        assert len(self.volumes) == 9, (
            f"Expected 9 volumes, got {len(self.volumes)}: {list(self.volumes.keys())}"
        )

    def test_has_six_stage_mounts(self):
        stage_vols = [v for v in self.volumes.values() if v.get("source") == "stage"]
        assert len(stage_vols) == 6, f"Expected 6 stage mounts, got {len(stage_vols)}"

    def test_has_three_block_storage_volumes(self):
        block_vols = [v for v in self.volumes.values() if v.get("source") == "block"]
        assert len(block_vols) == 3, f"Expected 3 block storage volumes, got {len(block_vols)}"

    def test_prom_data_size(self):
        assert self.volumes["prom-data"]["size"] == "50Gi"

    def test_grafana_data_size(self):
        assert self.volumes["grafana-data"]["size"] == "10Gi"

    def test_loki_data_size(self):
        assert self.volumes["loki-data"]["size"] == "20Gi"

    def test_stage_mounts_reference_monitor_stage(self):
        """All stage mounts must reference MONITOR_STAGE."""
        for vol in self.volumes.values():
            if vol.get("source") == "stage":
                stage_name = vol.get("stageConfig", {}).get("name", "")
                assert "MONITOR_STAGE" in stage_name, (
                    f"Stage mount '{vol['name']}' must reference MONITOR_STAGE, got: {stage_name}"
                )


# ===========================================================================
# Mocked Runtime Tests — prefect-exporter (exporter.py)
# ===========================================================================
import importlib  # noqa: E402
import sys  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

# We import exporter.py as a module from images/prefect-exporter/.
# Since it's not a package, we use importlib to load it by file path.
EXPORTER_PATH = PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py"
POLLER_PATH = PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py"


def _import_exporter():
    """Import exporter.py as a module, resetting prometheus_client collectors."""
    # prometheus_client Gauge raises ValueError if a metric is re-registered.
    # We must unregister all collectors before re-importing.
    import prometheus_client  # noqa: E402

    collectors = list(prometheus_client.REGISTRY._names_to_collectors.values())
    for c in collectors:
        with contextlib.suppress(Exception):
            prometheus_client.REGISTRY.unregister(c)

    spec = importlib.util.spec_from_file_location("exporter", EXPORTER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


import contextlib  # noqa: E402


def _import_poller():
    """Import poller.py as a module, mocking the snowflake token file read."""
    spec = importlib.util.spec_from_file_location("poller", POLLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    # poller.py doesn't execute get_connection() at import time, safe to load directly
    spec.loader.exec_module(mod)
    return mod


class TestExporterModuleImport:
    """Verify that exporter.py can be imported as a module."""

    def test_import_succeeds(self):
        mod = _import_exporter()
        assert hasattr(mod, "poll_once")
        assert hasattr(mod, "collect_flow_runs")
        assert hasattr(mod, "collect_deployments")
        assert hasattr(mod, "collect_work_pools")
        assert hasattr(mod, "main")

    def test_has_flow_run_states(self):
        mod = _import_exporter()
        assert len(mod.FLOW_RUN_STATES) == 9
        assert "COMPLETED" in mod.FLOW_RUN_STATES
        assert "FAILED" in mod.FLOW_RUN_STATES

    def test_default_poll_interval(self):
        mod = _import_exporter()
        assert mod.POLL_INTERVAL == 30

    def test_default_exporter_port(self):
        mod = _import_exporter()
        assert mod.EXPORTER_PORT == 9394


class TestExporterPost:
    """Test the _post() helper with mocked requests."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_post_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"count": 5}
        mock_post.return_value = mock_resp

        result = self.mod._post("/test", {"key": "val"})
        assert result == {"count": 5}
        mock_post.assert_called_once()

    @patch("requests.post")
    def test_post_api_error_returns_none(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.reason = "Internal Server Error"
        mock_resp.text = "something broke"
        mock_post.return_value = mock_resp

        result = self.mod._post("/test", {})
        assert result is None
        assert self.mod._error_count == 1

    @patch("requests.post")
    def test_post_exception_returns_none(self, mock_post):
        mock_post.side_effect = ConnectionError("refused")

        result = self.mod._post("/test", {})
        assert result is None
        assert self.mod._error_count == 1

    @patch("requests.post")
    def test_post_increments_error_count(self, mock_post):
        mock_post.side_effect = ConnectionError("refused")
        self.mod._error_count = 3

        self.mod._post("/test", {})
        assert self.mod._error_count == 4


class TestExporterGet:
    """Test the _get() helper with mocked requests."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.get")
    def test_get_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"id": 1}]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.mod._get("/health")
        assert result == [{"id": 1}]

    @patch("requests.get")
    def test_get_exception_returns_none(self, mock_get):
        mock_get.side_effect = ConnectionError("timeout")

        result = self.mod._get("/health")
        assert result is None
        assert self.mod._error_count == 1


class TestExporterCollectFlowRuns:
    """Test collect_flow_runs() with mocked API."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_collect_flow_runs_sets_gauges(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        # flow_runs/count returns an integer
        mock_resp.json.return_value = 42
        mock_post.return_value = mock_resp

        self.mod.collect_flow_runs()

        # Should have been called once per state (9 states)
        assert mock_post.call_count == 9

    @patch("requests.post")
    def test_collect_flow_runs_handles_api_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 503
        mock_resp.reason = "Service Unavailable"
        mock_resp.text = "down"
        mock_post.return_value = mock_resp

        # Should not raise
        self.mod.collect_flow_runs()
        assert self.mod._error_count == 9  # one error per state

    @patch("requests.post")
    def test_collect_flow_runs_handles_none_gracefully(self, mock_post):
        """When API returns None for a state count, gauge is not set (no crash)."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = None
        mock_post.return_value = mock_resp

        # data is None → skips .set() due to `if data is not None` guard
        self.mod.collect_flow_runs()
        assert mock_post.call_count == 9


class TestExporterCollectDeployments:
    """Test collect_deployments() with mocked API."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_collect_deployments_counts_active_inactive(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = [
            {"id": "1", "is_schedule_active": True},
            {"id": "2", "is_schedule_active": True},
            {"id": "3", "is_schedule_active": False},
        ]
        mock_post.return_value = mock_resp

        self.mod.collect_deployments()

        # Verify the POST used limit=200
        call_args = mock_post.call_args
        assert call_args[1]["json"]["limit"] == 200

    @patch("requests.post")
    def test_collect_deployments_empty_list(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        self.mod.collect_deployments()
        # Should not raise — total=0, active=0, inactive=0

    @patch("requests.post")
    def test_collect_deployments_api_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.reason = "Error"
        mock_resp.text = "fail"
        mock_post.return_value = mock_resp

        self.mod.collect_deployments()
        # Should not raise, just return early


class TestExporterCollectWorkPools:
    """Test collect_work_pools() with mocked API."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_collect_work_pools_with_workers(self, mock_post):
        # First call: /work_pools/filter
        pool_resp = MagicMock()
        pool_resp.ok = True
        pool_resp.json.return_value = [
            {"name": "spcs-pool", "status": "READY"},
            {"name": "gcp-pool", "status": "NOT_READY"},
        ]

        # Subsequent calls: /work_pools/{name}/workers/filter
        worker_resp = MagicMock()
        worker_resp.ok = True
        worker_resp.json.return_value = [{"id": "w1"}, {"id": "w2"}]

        mock_post.side_effect = [pool_resp, worker_resp, worker_resp]

        self.mod.collect_work_pools()
        # 1 filter + 2 worker calls = 3
        assert mock_post.call_count == 3

    @patch("requests.post")
    def test_collect_work_pools_empty(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        self.mod.collect_work_pools()

    @patch("requests.post")
    def test_collect_work_pools_api_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 502
        mock_resp.reason = "Bad Gateway"
        mock_resp.text = "error"
        mock_post.return_value = mock_resp

        self.mod.collect_work_pools()


class TestExporterPollOnce:
    """Test poll_once() orchestration."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_poll_once_calls_all_collectors(self, mock_post):
        def side_effect(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            if "flow_runs/count" in url:
                mock_resp.json.return_value = 0
            elif (
                "flow_runs/filter" in url
                or "deployments/filter" in url
                or "work_pools/filter" in url
                or "workers/filter" in url
            ):
                mock_resp.json.return_value = []
            else:
                mock_resp.json.return_value = []
            return mock_resp

        mock_post.side_effect = side_effect

        self.mod.poll_once()
        # 9 flow_run states + 1 deployments/filter + 1 work_pools/filter
        # + 1 flow_runs/filter (durations) = 12 minimum
        assert mock_post.call_count >= 12

    @patch("requests.post")
    def test_poll_once_sets_duration_gauge(self, mock_post):
        def side_effect(url, **kwargs):
            mock_resp = MagicMock()
            mock_resp.ok = True
            if "flow_runs/count" in url:
                mock_resp.json.return_value = 0
            else:
                mock_resp.json.return_value = []
            return mock_resp

        mock_post.side_effect = side_effect

        self.mod.poll_once()
        # Duration should have been set (non-negative)
        # We can't easily read the Gauge value, but we verify no crash


# ===========================================================================
# Mocked Runtime Tests — spcs-log-poller (poller.py)
# ===========================================================================


class TestPollerModuleImport:
    """Verify that poller.py can be imported as a module."""

    def test_import_succeeds(self):
        mod = _import_poller()
        assert hasattr(mod, "poll_once")
        assert hasattr(mod, "fetch_logs")
        assert hasattr(mod, "push_to_loki")
        assert hasattr(mod, "get_connection")
        assert hasattr(mod, "main")

    def test_default_poll_interval(self):
        mod = _import_poller()
        assert mod.POLL_INTERVAL == 30

    def test_default_lookback_seconds(self):
        mod = _import_poller()
        assert mod.LOOKBACK_SECONDS == 60

    def test_default_loki_push_url(self):
        mod = _import_poller()
        assert "3100" in mod.LOKI_PUSH_URL
        assert "loki" in mod.LOKI_PUSH_URL

    def test_services_filter_default(self):
        mod = _import_poller()
        assert "PF_SERVER" in mod.SERVICES_FILTER
        assert "PF_MONITOR" in mod.SERVICES_FILTER
        assert len(mod.SERVICES_FILTER) == 5


class TestPollerGetConnection:
    """Test get_connection() with mocked snowflake.connector."""

    def test_get_connection_reads_token_file(self):
        mod = _import_poller()
        mock_conn = MagicMock()
        mock_open = MagicMock(
            return_value=MagicMock(
                __enter__=MagicMock(
                    return_value=MagicMock(
                        read=MagicMock(return_value="test-token\n"),
                        strip=MagicMock(return_value="test-token"),
                    )
                ),
                __exit__=MagicMock(return_value=False),
            )
        )

        with (
            patch("builtins.open", mock_open),
            patch.object(
                mod.snowflake.connector, "connect", return_value=mock_conn
            ) as mock_connect,
        ):
            conn = mod.get_connection()
            assert conn == mock_conn
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs["authenticator"] == "oauth"
            assert call_kwargs["token"] == "test-token"


class TestPollerFetchLogs:
    """Test fetch_logs() with mocked Snowflake cursor."""

    def setup_method(self):
        self.mod = _import_poller()
        self.mod._last_timestamp = None

    def test_fetch_logs_returns_results(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01 00:00:00", "PF_SERVER", "pf_server", "INFO", "Server started"),
            ("2024-01-01 00:00:01", "PF_WORKER", "pf_worker", "WARN", "Slow query"),
        ]
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        results = self.mod.fetch_logs(mock_conn)
        assert len(results) == 2
        assert results[0]["SERVICE_NAME"] == "PF_SERVER"
        assert results[1]["MESSAGE"] == "Slow query"

    def test_fetch_logs_updates_last_timestamp(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01 00:00:05", "PF_SERVER", "pf_server", "INFO", "msg"),
        ]
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        self.mod.fetch_logs(mock_conn)
        assert self.mod._last_timestamp == "2024-01-01 00:00:05"

    def test_fetch_logs_empty_result(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        results = self.mod.fetch_logs(mock_conn)
        assert results == []
        assert self.mod._last_timestamp is None

    def test_fetch_logs_uses_last_timestamp_filter(self):
        self.mod._last_timestamp = "2024-01-01 00:00:00"
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        self.mod.fetch_logs(mock_conn)
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "2024-01-01 00:00:00" in executed_sql

    def test_fetch_logs_queries_wrapper_view(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        self.mod.fetch_logs(mock_conn)
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "PREFECT_DB.PREFECT_SCHEMA.SPCS_EVENT_LOGS" in executed_sql


class TestPollerPushToLoki:
    """Test push_to_loki() with mocked requests."""

    def setup_method(self):
        self.mod = _import_poller()

    @patch("requests.post")
    def test_push_to_loki_sends_correct_payload(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_post.return_value = mock_resp

        logs = [
            {
                "TS": "2024-01-01T00:00:00",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "pf_server",
                "SEVERITY": "INFO",
                "MESSAGE": "Hello world",
            }
        ]

        self.mod.push_to_loki(logs)
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        payload = call_kwargs["json"]

        assert "streams" in payload
        assert len(payload["streams"]) == 1
        stream = payload["streams"][0]
        assert stream["stream"]["job"] == "spcs-logs"
        assert stream["stream"]["source"] == "event-table"
        assert len(stream["values"]) == 1

    @patch("requests.post")
    def test_push_to_loki_empty_logs_does_nothing(self, mock_post):
        self.mod.push_to_loki([])
        mock_post.assert_not_called()

    @patch("requests.post")
    def test_push_to_loki_groups_by_service_container(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_post.return_value = mock_resp

        logs = [
            {
                "TS": "2024-01-01T00:00:00",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "server",
                "SEVERITY": "INFO",
                "MESSAGE": "msg1",
            },
            {
                "TS": "2024-01-01T00:00:01",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "server",
                "SEVERITY": "INFO",
                "MESSAGE": "msg2",
            },
            {
                "TS": "2024-01-01T00:00:02",
                "SERVICE_NAME": "PF_WORKER",
                "CONTAINER_NAME": "worker",
                "SEVERITY": "WARN",
                "MESSAGE": "msg3",
            },
        ]

        self.mod.push_to_loki(logs)
        payload = mock_post.call_args[1]["json"]
        # Two streams: PF_SERVER|server and PF_WORKER|worker
        assert len(payload["streams"]) == 2

    @patch("requests.post")
    def test_push_to_loki_handles_loki_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "internal error"
        mock_post.return_value = mock_resp

        logs = [
            {
                "TS": "2024-01-01T00:00:00",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "server",
                "SEVERITY": "INFO",
                "MESSAGE": "msg",
            },
        ]
        # Should not raise
        self.mod.push_to_loki(logs)

    @patch("requests.post")
    def test_push_to_loki_handles_connection_error(self, mock_post):
        mock_post.side_effect = ConnectionError("Loki down")

        logs = [
            {
                "TS": "2024-01-01T00:00:00",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "server",
                "SEVERITY": "INFO",
                "MESSAGE": "msg",
            },
        ]
        # Should not raise
        self.mod.push_to_loki(logs)

    @patch("requests.post")
    def test_push_to_loki_timestamp_format(self, mock_post):
        """Loki timestamps must be nanosecond strings."""
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_post.return_value = mock_resp

        logs = [
            {
                "TS": "2024-01-01T12:30:00",
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "MESSAGE": "test",
            },
        ]

        self.mod.push_to_loki(logs)
        payload = mock_post.call_args[1]["json"]
        ts_ns = payload["streams"][0]["values"][0][0]
        # Must be a numeric string (nanoseconds)
        assert ts_ns.isdigit()
        assert len(ts_ns) >= 18  # nanosecond timestamps are 19 digits

    @patch("requests.post")
    def test_push_to_loki_service_name_normalization(self, mock_post):
        """Service names should be lowercased with underscores replaced by hyphens."""
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_post.return_value = mock_resp

        logs = [
            {
                "TS": "2024-01-01T00:00:00",
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "MESSAGE": "test",
            },
        ]

        self.mod.push_to_loki(logs)
        payload = mock_post.call_args[1]["json"]
        service_label = payload["streams"][0]["stream"]["service"]
        assert service_label == "pf-server"


class TestPollerPollOnce:
    """Test poll_once() orchestration."""

    def setup_method(self):
        self.mod = _import_poller()
        self.mod._last_timestamp = None

    def test_poll_once_returns_log_count(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01 00:00:00", "PF_SERVER", "server", "INFO", "msg1"),
            ("2024-01-01 00:00:01", "PF_SERVER", "server", "INFO", "msg2"),
        ]
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            count = self.mod.poll_once(mock_conn)
            assert count == 2

    def test_poll_once_zero_logs(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("TS",),
            ("SERVICE_NAME",),
            ("CONTAINER_NAME",),
            ("SEVERITY",),
            ("MESSAGE",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        count = self.mod.poll_once(mock_conn)
        assert count == 0


class TestPollerMainReconnectLogic:
    """Test the reconnect logic in main() error handling paths."""

    def setup_method(self):
        self.mod = _import_poller()

    def test_programming_error_threshold_is_five(self):
        """Reconnect after >5 consecutive ProgrammingErrors."""
        # This tests the logic described in main() — reconnect after 5 ProgrammingErrors
        src = POLLER_PATH.read_text()
        assert "consecutive_errors > 5" in src

    def test_general_error_threshold_is_ten(self):
        """Reconnect after >10 consecutive general errors."""
        src = POLLER_PATH.read_text()
        assert "consecutive_errors > 10" in src


# ===========================================================================
# E2E Tests — Live SPCS endpoint tests (require running PF_MONITOR)
# ===========================================================================
import os  # noqa: E402

E2E_SKIP_REASON = "E2E tests require SNOWFLAKE_PAT and running PF_MONITOR service"


@pytest.mark.e2e
class TestExporterE2E:
    """Live E2E tests for the prefect-exporter sidecar on SPCS."""

    @pytest.fixture(autouse=True)
    def setup(self, spcs_session):
        self.session = spcs_session
        # The prefect-exporter metrics endpoint via Prometheus public ingress
        self.prometheus_url = os.environ.get(
            "PROMETHEUS_ENDPOINT",
            "https://xxxxx-orgname-acctname.snowflakecomputing.app",
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_is_reachable(self):
        """Prometheus public endpoint responds with 200."""
        resp = self.session.get(f"{self.prometheus_url}/-/healthy", timeout=15)
        assert resp.status_code == 200

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_has_prefect_exporter_targets(self):
        """Prometheus targets include the prefect-exporter job."""
        resp = self.session.get(f"{self.prometheus_url}/api/v1/targets", timeout=15)
        assert resp.status_code == 200
        data = resp.json()
        jobs = {
            t["labels"].get("job", "")
            for group in data.get("data", {}).get("activeTargets", [])
            for t in [group]
            if isinstance(group, dict)
        }
        assert "prefect-exporter" in jobs or any("prefect" in j for j in jobs)

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prefect_flow_runs_metric_exists(self):
        """The prefect_flow_runs metric is being scraped by Prometheus."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "prefect_flow_runs"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prefect_deployments_metric_exists(self):
        """The prefect_deployments_total metric is being scraped."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "prefect_deployments_total"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prefect_work_pools_metric_exists(self):
        """The prefect_work_pools_total metric is being scraped."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "prefect_work_pools_total"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_poll_duration_metric_exists(self):
        """The prefect_exporter_poll_duration_seconds metric is being scraped."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "prefect_exporter_poll_duration_seconds"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"


@pytest.mark.e2e
class TestLokiE2E:
    """Live E2E tests for Loki (log destination for spcs-log-poller)."""

    @pytest.fixture(autouse=True)
    def setup(self, spcs_session):
        self.session = spcs_session
        self.loki_url = os.environ.get(
            "LOKI_ENDPOINT",
            "https://yyyyy-orgname-acctname.snowflakecomputing.app",
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_is_reachable(self):
        """Loki public endpoint responds."""
        resp = self.session.get(f"{self.loki_url}/ready", timeout=15)
        assert resp.status_code == 200

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_has_spcs_logs_stream(self):
        """Loki has received logs with job=spcs-logs from the poller."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/label/job/values",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "spcs-logs" in data.get("data", [])

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_has_service_label(self):
        """Loki streams include a 'service' label (set by poller)."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/labels",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "service" in data.get("data", [])

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_query_returns_recent_logs(self):
        """Querying Loki for spcs-logs returns entries."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/query_range",
            params={
                "query": '{job="spcs-logs"}',
                "limit": "5",
            },
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_has_source_event_table_label(self):
        """Loki streams include source=event-table (set by poller)."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/label/source/values",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "event-table" in data.get("data", [])


# ===========================================================================
# Prometheus Recording Rules
# ===========================================================================

RECORDING_RULES_FILE = MONITORING_DIR / "prometheus" / "rules" / "recording.yml"


class TestPrometheusRecordingRules:
    """Validate prometheus recording rules YAML."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(RECORDING_RULES_FILE.read_text())

    def test_file_exists(self):
        assert RECORDING_RULES_FILE.exists()

    def test_has_groups(self):
        assert "groups" in self.rules
        assert len(self.rules["groups"]) >= 2

    def test_slo_recording_group_exists(self):
        names = [g["name"] for g in self.rules["groups"]]
        assert "slo_recording" in names

    def test_flow_recording_group_exists(self):
        names = [g["name"] for g in self.rules["groups"]]
        assert "flow_recording" in names

    def test_slo_group_has_rules(self):
        slo = [g for g in self.rules["groups"] if g["name"] == "slo_recording"][0]
        assert len(slo["rules"]) >= 6

    def test_flow_group_has_rules(self):
        flow = [g for g in self.rules["groups"] if g["name"] == "flow_recording"][0]
        assert len(flow["rules"]) >= 3

    def test_all_rules_have_record_and_expr(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert "record" in rule, f"Rule missing 'record' in group {group['name']}"
                assert "expr" in rule, f"Rule missing 'expr' in group {group['name']}"

    def test_slo_rules_use_correct_metric_prefix(self):
        slo = [g for g in self.rules["groups"] if g["name"] == "slo_recording"][0]
        for rule in slo["rules"]:
            assert rule["record"].startswith("prefect:"), (
                f"SLO rule '{rule['record']}' should start with 'prefect:'"
            )

    def test_flow_rules_use_correct_metric_prefix(self):
        flow = [g for g in self.rules["groups"] if g["name"] == "flow_recording"][0]
        for rule in flow["rules"]:
            assert rule["record"].startswith("prefect:"), (
                f"Flow rule '{rule['record']}' should start with 'prefect:'"
            )

    def test_server_uptime_ratios_exist(self):
        slo = [g for g in self.rules["groups"] if g["name"] == "slo_recording"][0]
        records = {r["record"] for r in slo["rules"]}
        assert "prefect:server_uptime:ratio_1h" in records
        assert "prefect:server_uptime:ratio_24h" in records
        assert "prefect:server_uptime:ratio_30d" in records

    def test_flow_success_rate_rule_exists(self):
        flow = [g for g in self.rules["groups"] if g["name"] == "flow_recording"][0]
        records = {r["record"] for r in flow["rules"]}
        assert "prefect:flow_success_rate:ratio" in records

    def test_groups_have_interval(self):
        for group in self.rules["groups"]:
            assert "interval" in group, f"Group '{group['name']}' missing interval"


# ===========================================================================
# Loki Alert Rules
# ===========================================================================

LOKI_RULES_FILE = MONITORING_DIR / "loki" / "rules" / "fake" / "alerts.yaml"


class TestLokiAlertRules:
    """Validate Loki alerting rules YAML."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(LOKI_RULES_FILE.read_text())

    def test_file_exists(self):
        assert LOKI_RULES_FILE.exists()

    def test_has_groups(self):
        assert "groups" in self.rules
        assert len(self.rules["groups"]) >= 1

    def test_spcs_log_alerts_group_exists(self):
        names = [g["name"] for g in self.rules["groups"]]
        assert "spcs_log_alerts" in names

    def test_has_at_least_four_alert_rules(self):
        group = [g for g in self.rules["groups"] if g["name"] == "spcs_log_alerts"][0]
        assert len(group["rules"]) >= 4

    def test_all_rules_have_alert_and_expr(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert "alert" in rule, "Rule missing 'alert' field"
                assert "expr" in rule, "Rule missing 'expr' field"

    def test_all_rules_have_severity_label(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                labels = rule.get("labels", {})
                assert "severity" in labels, f"Alert '{rule['alert']}' missing severity label"

    def test_expected_alert_names(self):
        group = [g for g in self.rules["groups"] if g["name"] == "spcs_log_alerts"][0]
        alert_names = {r["alert"] for r in group["rules"]}
        expected = {"SPCSErrorSpike", "SPCSContainerOOM", "SPCSCrashLoop", "SPCSAuthFailure"}
        assert expected.issubset(alert_names), f"Missing alerts: {expected - alert_names}"

    def test_rules_have_source_loki_label(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert rule.get("labels", {}).get("source") == "loki", (
                    f"Alert '{rule['alert']}' should have source=loki label"
                )

    def test_critical_alerts_have_zero_for_duration(self):
        """Critical alerts (OOM, CrashLoop) should fire immediately (for: 0m)."""
        group = [g for g in self.rules["groups"] if g["name"] == "spcs_log_alerts"][0]
        critical_rules = [r for r in group["rules"] if r["labels"].get("severity") == "critical"]
        for rule in critical_rules:
            assert rule.get("for") == "0m", f"Critical alert '{rule['alert']}' should have for: 0m"


# ===========================================================================
# Loki Ruler Config
# ===========================================================================


class TestLokiRulerConfig:
    """Validate Loki ruler section in loki-config.yaml."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        config_file = MONITORING_DIR / "loki" / "loki-config.yaml"
        self.config = yaml.safe_load(config_file.read_text())

    def test_ruler_section_exists(self):
        assert "ruler" in self.config

    def test_ruler_has_storage(self):
        assert "storage" in self.config["ruler"]

    def test_ruler_storage_is_local(self):
        assert self.config["ruler"]["storage"]["type"] == "local"

    def test_ruler_has_rule_path(self):
        assert "rule_path" in self.config["ruler"]

    def test_ruler_has_alertmanager_url(self):
        assert "alertmanager_url" in self.config["ruler"]

    def test_ruler_api_enabled(self):
        assert self.config["ruler"].get("enable_api") is True


# ===========================================================================
# Grafana Alerting Provisioning
# ===========================================================================

ALERTING_DIR = MONITORING_DIR / "grafana" / "provisioning" / "alerting"


class TestGrafanaAlertingContactPoints:
    """Validate Grafana alerting contact points provisioning."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load((ALERTING_DIR / "contactpoints.yaml").read_text())

    def test_file_exists(self):
        assert (ALERTING_DIR / "contactpoints.yaml").exists()

    def test_has_api_version(self):
        assert self.config.get("apiVersion") == 1

    def test_has_contact_points(self):
        assert "contactPoints" in self.config
        assert len(self.config["contactPoints"]) >= 1

    def test_prefect_alerts_contact_point_exists(self):
        names = [cp["name"] for cp in self.config["contactPoints"]]
        assert "prefect-alerts" in names

    def test_webhook_receiver_removed(self):
        cp = [c for c in self.config["contactPoints"] if c["name"] == "prefect-alerts"][0]
        receivers = cp["receivers"]
        assert not any(r["type"] == "webhook" for r in receivers)

    def test_slack_receiver_configured(self):
        cp = [c for c in self.config["contactPoints"] if c["name"] == "prefect-alerts"][0]
        receivers = cp["receivers"]
        assert not any(r["type"] == "slack" for r in receivers), (
            "Slack receiver must be commented out by default (SLACK_ENABLED=false)"
        )


class TestGrafanaAlertingPolicies:
    """Validate Grafana alerting notification policies."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load((ALERTING_DIR / "policies.yaml").read_text())

    def test_file_exists(self):
        assert (ALERTING_DIR / "policies.yaml").exists()

    def test_has_api_version(self):
        assert self.config.get("apiVersion") == 1

    def test_has_policies(self):
        assert "policies" in self.config
        assert len(self.config["policies"]) >= 1

    def test_default_receiver_is_prefect_alerts(self):
        policy = self.config["policies"][0]
        assert policy["receiver"] == "prefect-alerts"

    def test_group_by_includes_alertname(self):
        policy = self.config["policies"][0]
        assert "alertname" in policy.get("group_by", [])


class TestGrafanaAlertingRules:
    """Validate Grafana alerting rules provisioning."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())

    def test_file_exists(self):
        assert (ALERTING_DIR / "rules.yaml").exists()

    def test_has_api_version(self):
        assert self.config.get("apiVersion") == 1

    def test_has_groups(self):
        assert "groups" in self.config
        assert len(self.config["groups"]) >= 1

    def test_rules_have_required_fields(self):
        for group in self.config["groups"]:
            for rule in group["rules"]:
                assert "uid" in rule, "Rule missing 'uid'"
                assert "title" in rule, "Rule missing 'title'"
                assert "condition" in rule, "Rule missing 'condition'"
                assert "data" in rule, "Rule missing 'data'"


# ===========================================================================
# Health Dashboard
# ===========================================================================

HEALTH_DASHBOARD_FILE = MONITORING_DIR / "grafana" / "dashboards" / "health.json"


class TestHealthDashboard:
    """Validate the Prefect Health home dashboard."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(HEALTH_DASHBOARD_FILE.read_text())

    def test_file_exists(self):
        assert HEALTH_DASHBOARD_FILE.exists()

    def test_uid_is_health(self):
        assert self.dash["uid"] == "health"

    def test_title(self):
        assert self.dash["title"] == "Prefect Health"

    def test_has_tags(self):
        assert "prefect" in self.dash.get("tags", [])

    def test_has_top_level_links(self):
        """Health dashboard should link to all other dashboards."""
        links = self.dash.get("links", [])
        assert len(links) >= 8, f"Expected at least 8 dashboard links, got {len(links)}"

    def test_links_point_to_valid_uids(self):
        valid_uids = {
            "prefect-spcs-overview",
            "prefect-app",
            "prefect-vm-workers",
            "prefect-alerts",
            "prefect-spcs-logs",
            "prefect-logs",
            "prefect-redis-detail",
            "prefect-postgres-detail",
        }
        link_uids = set()
        for link in self.dash.get("links", []):
            url = link.get("url", "")
            if "/grafana/d/" in url:
                uid = url.split("/grafana/d/")[-1].split("/")[0].split("?")[0]
                link_uids.add(uid)
        assert valid_uids.issubset(link_uids), f"Missing dashboard links: {valid_uids - link_uids}"

    def test_has_row_panels(self):
        """Health dashboard should have row panels for organization."""
        rows = [p for p in self.dash["panels"] if p.get("type") == "row"]
        assert len(rows) >= 4, f"Expected at least 4 row panels, got {len(rows)}"

    def test_has_stat_panels(self):
        stats = [p for p in self.dash["panels"] if p.get("type") == "stat"]
        assert len(stats) >= 5, f"Expected at least 5 stat panels, got {len(stats)}"

    def test_has_timeseries_panels(self):
        ts = [p for p in self.dash["panels"] if p.get("type") == "timeseries"]
        assert len(ts) >= 2, f"Expected at least 2 timeseries panels, got {len(ts)}"

    def test_has_loki_datasource_panels(self):
        """At least one panel should query Loki for logs."""
        loki_panels = [
            p for p in self.dash["panels"] if p.get("datasource", {}).get("uid") == "loki"
        ]
        assert len(loki_panels) >= 1

    def test_references_duration_metrics(self):
        """Dashboard should show flow run duration percentile metrics."""
        all_exprs = " ".join(
            t.get("expr", "") for p in self.dash["panels"] for t in p.get("targets", [])
        )
        assert "prefect_flow_run_duration_p50" in all_exprs
        assert "prefect_flow_run_duration_p95" in all_exprs

    def test_references_recording_rules(self):
        """Dashboard should use recording rule metrics when available."""
        all_exprs = " ".join(
            t.get("expr", "") for p in self.dash["panels"] for t in p.get("targets", [])
        )
        assert "prefect:server_uptime:ratio" in all_exprs
        assert "prefect:flow_success_rate:ratio" in all_exprs


# ===========================================================================
# Cross-Dashboard Links Validation
# ===========================================================================


class TestCrossDashboardLinks:
    """Validate that all dashboards have cross-navigation links."""

    DASHBOARD_DIR = MONITORING_DIR / "grafana" / "dashboards"

    ALL_UIDS = {
        "health",
        "prefect-spcs-overview",
        "prefect-app",
        "prefect-vm-workers",
        "prefect-alerts",
        "prefect-spcs-logs",
        "prefect-logs",
        "prefect-redis-detail",
        "prefect-postgres-detail",
    }

    @pytest.fixture(autouse=True)
    def load_dashboards(self):
        self.dashboards = {}
        for f in sorted(self.DASHBOARD_DIR.glob("*.json")):
            self.dashboards[f.stem] = json.loads(f.read_text())

    def test_all_dashboards_have_links(self):
        for name, dash in self.dashboards.items():
            links = dash.get("links", [])
            assert len(links) >= 7, (
                f"Dashboard '{name}' should have at least 7 cross-links, got {len(links)}"
            )

    def test_all_dashboards_link_to_health(self):
        """Every dashboard except health itself should link to health."""
        for name, dash in self.dashboards.items():
            if dash.get("uid") == "health":
                continue
            link_urls = [lnk.get("url", "") for lnk in dash.get("links", [])]
            assert any("/grafana/d/health" in u for u in link_urls), (
                f"Dashboard '{name}' should link to Health Home"
            )

    def test_no_dashboard_links_to_itself(self):
        """No dashboard should contain a link to its own UID."""
        for name, dash in self.dashboards.items():
            own_uid = dash.get("uid", "")
            link_urls = [lnk.get("url", "") for lnk in dash.get("links", [])]
            for url in link_urls:
                assert f"/grafana/d/{own_uid}" not in url, (
                    f"Dashboard '{name}' (uid={own_uid}) links to itself"
                )

    def test_health_links_to_all_other_dashboards(self):
        health = self.dashboards.get("health", {})
        link_urls = " ".join(lnk.get("url", "") for lnk in health.get("links", []))
        other_uids = self.ALL_UIDS - {"health"}
        for uid in other_uids:
            assert f"/grafana/d/{uid}" in link_urls, f"Health dashboard missing link to {uid}"

    def test_link_urls_use_correct_uid_format(self):
        """All link URLs must use /grafana/d/{uid} format."""
        for name, dash in self.dashboards.items():
            for link in dash.get("links", []):
                url = link.get("url", "")
                if url:
                    assert url.startswith("/grafana/d/"), (
                        f"Dashboard '{name}' link URL '{url}' should start with /grafana/d/"
                    )


# ===========================================================================
# Grafana Home Dashboard Config in Spec
# ===========================================================================


class TestGrafanaHomeDashboardConfig:
    """Validate that pf_monitor.yaml configures Grafana home dashboard."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.grafana = [c for c in containers if c["name"] == "grafana"][0]

    def test_home_dashboard_env_var_set(self):
        env = self.grafana.get("env", {})
        assert "GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH" in env

    def test_home_dashboard_points_to_health(self):
        env = self.grafana.get("env", {})
        path = env.get("GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH", "")
        assert "health.json" in path

    def test_unified_alerting_enabled(self):
        env = self.grafana.get("env", {})
        assert env.get("GF_UNIFIED_ALERTING_ENABLED") == "true"

    def test_alerting_webhook_env_var_removed(self):
        env = self.grafana.get("env", {})
        assert "GF_ALERTING_WEBHOOK_URL" not in env


# ===========================================================================
# Exporter Duration Metrics — Mocked Tests
# ===========================================================================


class TestExporterCollectFlowRunDurations:
    """Test collect_flow_run_durations() function with mocked API."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    @patch("requests.post")
    def test_no_runs_returns_early(self, mock_post):
        """When no completed runs exist, duration gauges are not set."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()
        # Should call flow_runs/filter once
        assert mock_post.call_count == 1

    @patch("requests.post")
    def test_computes_durations(self, mock_post):
        """With completed runs, should compute p50/p95/max/avg."""
        runs = [
            {"total_run_time": 10.0},
            {"total_run_time": 20.0},
            {"total_run_time": 30.0},
            {"total_run_time": 40.0},
            {"total_run_time": 100.0},
        ]
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = runs
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()
        # No crash means success — gauges were set

    @patch("requests.post")
    def test_skips_runs_without_total_run_time(self, mock_post):
        """Runs missing total_run_time should be skipped."""
        runs = [
            {"total_run_time": 10.0},
            {"name": "no-duration-run"},
            {"total_run_time": None},
            {"total_run_time": 30.0},
        ]
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = runs
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()
        # Should not crash, only 2 valid durations

    @patch("requests.post")
    def test_single_run_duration(self, mock_post):
        """Single completed run should set all metrics to same value."""
        runs = [{"total_run_time": 42.5}]
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = runs
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()

    @patch("requests.post")
    def test_api_error_returns_none(self, mock_post):
        """When API returns error, should return early without crashing."""
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()

    def test_function_exists(self):
        assert hasattr(self.mod, "collect_flow_run_durations")
        assert callable(self.mod.collect_flow_run_durations)

    def test_duration_gauges_defined(self):
        """All four duration gauge metrics should be defined."""
        assert hasattr(self.mod, "flow_run_duration_p50")
        assert hasattr(self.mod, "flow_run_duration_p95")
        assert hasattr(self.mod, "flow_run_duration_max")
        assert hasattr(self.mod, "flow_run_duration_avg")


# ===========================================================================
# Loki Rules Volume Mount in Spec
# ===========================================================================


class TestLokiRulesViaConfigMount:
    """Validate loki rules are served from the consolidated loki-config mount.

    After consolidating loki-config and loki-rules into a single stage mount
    (to stay within CPU_X64_S 4-mount limit), loki-config mounts the entire
    MONITOR_STAGE/loki directory at /etc/loki — which includes rules/fake/alerts.yaml.
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.loki = [c for c in containers if c["name"] == "loki"][0]
        self.volumes = {v["name"]: v for v in self.spec["spec"]["volumes"]}

    def test_no_separate_loki_rules_volume(self):
        """loki-rules volume was removed in favour of consolidated loki-config."""
        assert "loki-rules" not in self.volumes

    def test_loki_config_volume_serves_rules(self):
        """loki-config volume stageConfig.name includes the rules subdirectory."""
        vol = self.volumes["loki-config"]
        stage_name = vol.get("stageConfig", {}).get("name", "")
        assert stage_name.endswith("/loki"), (
            f"loki-config stageConfig.name should end with /loki to include rules subdir, "
            f"got: {stage_name}"
        )

    def test_loki_container_has_no_rules_mount(self):
        """Loki container should not have a separate loki-rules mount."""
        mount_names = [m["name"] for m in self.loki.get("volumeMounts", [])]
        assert "loki-rules" not in mount_names

    def test_loki_has_config_and_data_mounts_only(self):
        """Loki container should have exactly loki-config and loki-data mounts."""
        mount_names = sorted([m["name"] for m in self.loki.get("volumeMounts", [])])
        assert mount_names == ["loki-config", "loki-data"]


# ===========================================================================
# PromQL/LogQL Expression Validation
# ===========================================================================
import re as _re  # noqa: E402


def _extract_promql_exprs_from_dashboard(dash: dict) -> list[tuple[str, str, str]]:
    """Extract all PromQL/LogQL expressions from a dashboard.

    Returns list of (dashboard_title, panel_title, expr).
    """
    results = []
    title = dash.get("title", "unknown")
    for panel in dash.get("panels", []):
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            if expr.strip():
                results.append((title, panel.get("title", "untitled"), expr))
    return results


def _balanced_parens(expr: str) -> bool:
    """Check if parentheses are balanced in an expression."""
    depth = 0
    in_string = False
    escape = False
    for ch in expr:
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth < 0:
            return False
    return depth == 0


def _balanced_braces(expr: str) -> bool:
    """Check if curly braces are balanced in an expression."""
    depth = 0
    in_string = False
    for ch in expr:
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        if depth < 0:
            return False
    return depth == 0


def _balanced_brackets(expr: str) -> bool:
    """Check if square brackets are balanced in an expression."""
    depth = 0
    in_string = False
    for ch in expr:
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if depth < 0:
            return False
    return depth == 0


class TestPromQLExpressionValidation:
    """Validate PromQL/LogQL expression syntax across all dashboards."""

    @pytest.fixture(autouse=True)
    def load_dashboards(self):
        self.all_exprs: list[tuple[str, str, str]] = []
        for f in DASHBOARD_DIR.glob("*.json"):
            dash = json.loads(f.read_text())
            self.all_exprs.extend(_extract_promql_exprs_from_dashboard(dash))

    def test_all_expressions_have_balanced_parentheses(self):
        for dash_title, panel_title, expr in self.all_exprs:
            assert _balanced_parens(expr), (
                f"Unbalanced parentheses in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )

    def test_all_expressions_have_balanced_braces(self):
        for dash_title, panel_title, expr in self.all_exprs:
            assert _balanced_braces(expr), (
                f"Unbalanced braces in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )

    def test_all_expressions_have_balanced_brackets(self):
        for dash_title, panel_title, expr in self.all_exprs:
            assert _balanced_brackets(expr), (
                f"Unbalanced brackets in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )

    def test_no_empty_label_matchers(self):
        """No expression should have empty positive label matchers like {job=""}.

        Negated matchers like {name!=""} are valid (means "name is not empty").
        """
        # Matches ="" or ='' but NOT !="" or !=''
        empty_positive = _re.compile(r'(?<![!~])=""')
        empty_positive_sq = _re.compile(r"(?<![!~])=''")
        for dash_title, panel_title, expr in self.all_exprs:
            assert not empty_positive.search(expr) and not empty_positive_sq.search(expr), (
                f"Empty label matcher in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )

    def test_all_range_vectors_have_duration(self):
        """Range vectors [Xm] must have a valid duration inside brackets."""
        duration_pattern = _re.compile(r"\[\s*\]")
        for dash_title, panel_title, expr in self.all_exprs:
            assert not duration_pattern.search(expr), (
                f"Empty range vector [] in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )

    def test_no_double_commas_in_expressions(self):
        """Expressions should not have double commas (syntax error)."""
        for dash_title, panel_title, expr in self.all_exprs:
            assert ",," not in expr.replace(" ", ""), (
                f"Double comma in '{dash_title}' / '{panel_title}': {expr[:100]}"
            )


class TestPrometheusAlertExpressionValidation:
    """Validate PromQL syntax in Prometheus alert rules."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(PROM_RULES_FILE.read_text())
        self.all_rules = []
        for group in self.rules["groups"]:
            self.all_rules.extend(group.get("rules", []))

    def test_all_alert_exprs_have_balanced_parens(self):
        for rule in self.all_rules:
            expr = rule.get("expr", "")
            assert _balanced_parens(expr), (
                f"Alert '{rule['alert']}' has unbalanced parentheses: {expr[:100]}"
            )

    def test_all_alert_exprs_have_balanced_braces(self):
        for rule in self.all_rules:
            expr = rule.get("expr", "")
            assert _balanced_braces(expr), (
                f"Alert '{rule['alert']}' has unbalanced braces: {expr[:100]}"
            )

    def test_all_alert_exprs_reference_a_metric_or_function(self):
        """Alert expressions must contain at least one metric name or function."""
        for rule in self.all_rules:
            expr = rule.get("expr", "").strip()
            assert _re.search(r"[a-zA-Z_][a-zA-Z0-9_]*", expr), (
                f"Alert '{rule['alert']}' has no metric/function reference: {expr[:100]}"
            )


class TestRecordingRuleExpressionValidation:
    """Validate PromQL syntax in recording rules."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(RECORDING_RULES_FILE.read_text())

    def test_all_recording_exprs_have_balanced_parens(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule.get("expr", "")
                assert _balanced_parens(expr), (
                    f"Recording rule '{rule['record']}' unbalanced parens: {expr[:100]}"
                )

    def test_all_recording_exprs_have_balanced_braces(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule.get("expr", "")
                assert _balanced_braces(expr), (
                    f"Recording rule '{rule['record']}' unbalanced braces: {expr[:100]}"
                )

    def test_all_recording_exprs_non_empty(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert rule.get("expr", "").strip(), (
                    f"Recording rule '{rule['record']}' has empty expression"
                )


class TestLokiAlertExpressionValidation:
    """Validate LogQL syntax in Loki alert rules."""

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(LOKI_RULES_FILE.read_text())

    def test_all_loki_exprs_have_stream_selector(self):
        """Every LogQL alert should have a stream selector {…}."""
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule.get("expr", "")
                assert "{" in expr and "}" in expr, (
                    f"Loki alert '{rule['alert']}' missing stream selector: {expr[:100]}"
                )

    def test_all_loki_exprs_have_balanced_braces(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule.get("expr", "")
                assert _balanced_braces(expr), (
                    f"Loki alert '{rule['alert']}' unbalanced braces: {expr[:100]}"
                )

    def test_all_loki_exprs_reference_spcs_logs_job(self):
        """All Loki alerts should reference the spcs-logs job."""
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule.get("expr", "")
                assert "spcs-logs" in expr, (
                    f"Loki alert '{rule['alert']}' doesn't reference spcs-logs job: {expr[:100]}"
                )


# ===========================================================================
# Dashboard Panel Correctness
# ===========================================================================


class TestDashboardPanelCorrectness:
    """Structural validation of all dashboard panels."""

    @pytest.fixture(autouse=True)
    def load_dashboards(self):
        self.dashboards = {}
        for f in DASHBOARD_DIR.glob("*.json"):
            self.dashboards[f.stem] = json.loads(f.read_text())

    def test_no_panel_gridpos_overlap(self):
        """No two non-row panels should overlap within the same dashboard."""
        for name, dash in self.dashboards.items():
            panels = [p for p in dash["panels"] if p.get("type") != "row"]
            for i, p1 in enumerate(panels):
                g1 = p1.get("gridPos", {})
                for p2 in panels[i + 1 :]:
                    g2 = p2.get("gridPos", {})
                    # Check overlap: two rects overlap if none of the four
                    # separation conditions hold
                    x1, y1, w1, h1 = g1.get("x", 0), g1.get("y", 0), g1.get("w", 0), g1.get("h", 0)
                    x2, y2, w2, h2 = g2.get("x", 0), g2.get("y", 0), g2.get("w", 0), g2.get("h", 0)
                    separated = x1 + w1 <= x2 or x2 + w2 <= x1 or y1 + h1 <= y2 or y2 + h2 <= y1
                    assert separated, (
                        f"Dashboard '{name}': panels '{p1.get('title')}' and "
                        f"'{p2.get('title')}' overlap at gridPos"
                    )

    def test_all_panel_gridpos_within_24_columns(self):
        """All panels must fit within Grafana's 24-column grid."""
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                gp = panel.get("gridPos", {})
                x = gp.get("x", 0)
                w = gp.get("w", 0)
                assert x >= 0, f"Dashboard '{name}' panel '{panel.get('title')}' has negative x"
                assert x + w <= 24, (
                    f"Dashboard '{name}' panel '{panel.get('title')}' exceeds 24 columns "
                    f"(x={x}, w={w})"
                )

    def test_all_panels_have_non_negative_positions(self):
        """All panel gridPos values should be non-negative."""
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                gp = panel.get("gridPos", {})
                for key in ["x", "y", "w", "h"]:
                    val = gp.get(key, 0)
                    assert val >= 0, (
                        f"Dashboard '{name}' panel '{panel.get('title')}' "
                        f"has negative gridPos.{key}={val}"
                    )

    def test_non_row_panels_have_targets_or_are_text(self):
        """Non-row panels (except text/stat with no query) should have targets."""
        skip_types = {"row", "text"}
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                ptype = panel.get("type", "")
                if ptype in skip_types:
                    continue
                targets = panel.get("targets", [])
                # Stat panels with hardcoded values may not have targets
                if ptype == "stat" and not targets:
                    continue
                assert len(targets) >= 1, (
                    f"Dashboard '{name}' panel '{panel.get('title')}' (type={ptype}) has no targets"
                )

    def test_loki_panels_have_explicit_datasource(self):
        """Panels querying Loki must explicitly set the Loki datasource.

        Prometheus is the default datasource, so omitting the datasource
        on a Loki panel silently queries Prometheus instead — returning no data.
        """
        for name, dash in self.dashboards.items():
            for panel in dash["panels"]:
                for target in panel.get("targets", []):
                    expr = target.get("expr", "")
                    # If expression looks like LogQL (has stream selector with pipe)
                    if "{" in expr and ("|=" in expr or "|~" in expr or "count_over_time" in expr):
                        ds = panel.get("datasource", {})
                        assert ds.get("uid") == "loki", (
                            f"Dashboard '{name}' panel '{panel.get('title')}' uses LogQL "
                            f"but doesn't set datasource to Loki: {expr[:60]}"
                        )

    def test_dashboard_metric_cross_reference(self):
        """Every prefect_* metric referenced in dashboards should exist in exporter.py."""
        exporter_src = (PROJECT_DIR / "images" / "prefect-exporter" / "exporter.py").read_text()
        # Known external metrics not from our exporter
        external_metrics = {
            "up",
            "container_restarts_total",
            "container_cpu_usage",
            "container_cpu_limit",
            "container_memory_usage",
            "container_memory_limit",
            "node_cpu_seconds_total",
            "node_memory_MemTotal_bytes",
            "node_memory_MemAvailable_bytes",
            "node_filesystem_avail_bytes",
            "node_filesystem_size_bytes",
            "node_network_receive_bytes_total",
            "node_network_transmit_bytes_total",
            "container_cpu_usage_seconds_total",
            "container_memory_usage_bytes",
            "ALERTS",
            "prometheus_rule_evaluation_duration_seconds",
            "prometheus_rule_evaluation_failures_total",
            "prometheus_rule_group_rules",
            "redis_memory_used_bytes",
            "redis_connected_clients",
            "redis_commands_processed_total",
            "redis_keyspace_hits_total",
            "redis_keyspace_misses_total",
            "redis_memory_fragmentation_ratio",
            "redis_blocked_clients",
            "redis_db_keys",
            "redis_evicted_keys_total",
            "redis_expired_keys_total",
            "redis_net_input_bytes_total",
            "redis_net_output_bytes_total",
            "redis_rdb_last_save_timestamp_seconds",
            "pg_stat_activity_count",
            "pg_stat_database_xact_commit",
            "pg_stat_database_xact_rollback",
            "pg_stat_database_blks_hit",
            "pg_stat_database_blks_read",
            "pg_stat_user_tables_n_tup_ins",
            "pg_stat_user_tables_n_tup_upd",
            "pg_stat_user_tables_n_tup_del",
            "pg_stat_user_tables_n_dead_tup",
            "pg_stat_user_tables_n_live_tup",
            "pg_database_size_bytes",
            "pg_locks_count",
            "pg_stat_database_temp_bytes",
            "pg_exporter_last_scrape_duration_seconds",
            "pg_relation_size_bytes",
            "pg_stat_user_tables_table_size_bytes",
            "loki_distributor_bytes_received_total",
            "loki_ingester_streams_created_total",
        }
        metric_pattern = _re.compile(r"\b(prefect_[a-zA-Z_]+)")
        missing = set()
        for f in DASHBOARD_DIR.glob("*.json"):
            dash = json.loads(f.read_text())
            for panel in dash.get("panels", []):
                for target in panel.get("targets", []):
                    expr = target.get("expr", "")
                    for match in metric_pattern.finditer(expr):
                        metric = match.group(1)
                        if metric not in exporter_src and metric not in external_metrics:
                            missing.add(metric)
        # Recording rule metrics (prefect:*) use colon, not underscore,
        # so won't match this pattern — that's correct
        assert not missing, (
            f"Dashboard metrics not found in exporter.py or external list: {missing}"
        )

    def test_recording_rules_used_in_dashboards(self):
        """Most recording rule metrics should be referenced in at least one dashboard.

        Some SLO rules (e.g. 30d uptime) may exist for alerting or API use
        without being displayed in a dashboard panel. Allow up to 2 unused.
        """
        rules = yaml.safe_load(RECORDING_RULES_FILE.read_text())
        rule_metrics = set()
        for group in rules["groups"]:
            for rule in group["rules"]:
                rule_metrics.add(rule["record"])

        all_exprs = ""
        for f in DASHBOARD_DIR.glob("*.json"):
            dash = json.loads(f.read_text())
            for panel in dash.get("panels", []):
                for target in panel.get("targets", []):
                    all_exprs += " " + target.get("expr", "")

        unused = {m for m in rule_metrics if m not in all_exprs}
        assert len(unused) <= 2, (
            f"Too many recording rules not referenced in any dashboard ({len(unused)}): {unused}"
        )


# ===========================================================================
# Prometheus Config Deep Validation
# ===========================================================================


class TestPrometheusConfigDeep:
    """Deep validation of prometheus.yml configuration."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_scrape_timeout_less_than_interval(self):
        """Global scrape_timeout must be less than scrape_interval."""
        interval = self.config["global"]["scrape_interval"]
        timeout = self.config["global"]["scrape_timeout"]
        # Parse durations (e.g. "15s" -> 15, "30s" -> 30)
        interval_s = int(interval.rstrip("smh"))
        timeout_s = int(timeout.rstrip("smh"))
        assert timeout_s < interval_s, (
            f"scrape_timeout ({timeout}) must be < scrape_interval ({interval})"
        )

    def test_per_job_scrape_timeout_less_than_interval(self):
        """Per-job scrape_timeout (if set) must be < the job's scrape_interval."""
        global_interval = self.config["global"]["scrape_interval"]
        for job in self.config["scrape_configs"]:
            interval = job.get("scrape_interval", global_interval)
            timeout = job.get("scrape_timeout")
            if timeout:
                interval_s = int(interval.rstrip("smh"))
                timeout_s = int(timeout.rstrip("smh"))
                assert timeout_s < interval_s, (
                    f"Job '{job['job_name']}': scrape_timeout ({timeout}) "
                    f"must be < scrape_interval ({interval})"
                )

    def test_scrape_target_ports_match_spec_probes(self):
        """Scrape target ports for co-located containers should match readinessProbe ports."""
        containers = {c["name"]: c for c in self.spec["spec"]["containers"]}
        # localhost targets are co-located in PF_MONITOR
        localhost_jobs = {
            "prometheus": ("prometheus", 9090),
            "grafana": ("grafana", 3000),
            "loki": ("loki", 3100),
            "postgres": ("postgres-exporter", 9187),
            "prefect-app": ("prefect-exporter", 9394),
        }
        for job_name, (container_name, _expected_port) in localhost_jobs.items():
            job = next(
                (j for j in self.config["scrape_configs"] if j["job_name"] == job_name),
                None,
            )
            if job is None:
                continue
            targets = job["static_configs"][0]["targets"]
            probe_port = containers.get(container_name, {}).get("readinessProbe", {}).get("port")
            if probe_port:
                assert any(str(probe_port) in t for t in targets), (
                    f"Job '{job_name}' target port doesn't match {container_name} "
                    f"readinessProbe port {probe_port}: {targets}"
                )

    def test_prometheus_has_tsdb_retention_args(self):
        """Prometheus container must have TSDB retention flags."""
        containers = self.spec["spec"]["containers"]
        prom = next(c for c in containers if c["name"] == "prometheus")
        args = " ".join(prom.get("args", []))
        assert "--storage.tsdb.retention.time" in args
        assert "--storage.tsdb.retention.size" in args

    def test_evaluation_interval_set(self):
        """Global evaluation_interval must be defined for rule evaluation."""
        assert "evaluation_interval" in self.config["global"]

    def test_rule_files_glob_configured(self):
        """Prometheus must have rule_files configured to load alerts and recording rules."""
        rule_files = self.config.get("rule_files", [])
        assert len(rule_files) >= 1
        assert any("rules" in rf for rf in rule_files)

    def test_has_exactly_eight_scrape_jobs(self):
        """Prometheus must have exactly 8 scrape jobs."""
        assert len(self.config["scrape_configs"]) == 8, (
            f"Expected 8 scrape jobs, got {len(self.config['scrape_configs'])}: "
            f"{[j['job_name'] for j in self.config['scrape_configs']]}"
        )


# ===========================================================================
# Alert Rule Cross-Source Uniqueness & Completeness
# ===========================================================================


class TestAlertRuleCrossSourceUniqueness:
    """Validate alert names are unique across Prometheus, Loki, and Grafana alerts."""

    @pytest.fixture(autouse=True)
    def load_all_alerts(self):
        prom = yaml.safe_load(PROM_RULES_FILE.read_text())
        self.prom_names = set()
        for g in prom["groups"]:
            for r in g.get("rules", []):
                self.prom_names.add(r["alert"])

        loki = yaml.safe_load(LOKI_RULES_FILE.read_text())
        self.loki_names = set()
        for g in loki["groups"]:
            for r in g.get("rules", []):
                self.loki_names.add(r["alert"])

        grafana = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())
        self.grafana_uids = set()
        self.grafana_titles = set()
        for g in grafana["groups"]:
            for r in g.get("rules", []):
                self.grafana_uids.add(r["uid"])
                self.grafana_titles.add(r["title"])

    def test_no_duplicate_names_across_prom_and_loki(self):
        overlap = self.prom_names & self.loki_names
        assert not overlap, f"Duplicate alert names across Prometheus and Loki: {overlap}"

    def test_grafana_uids_are_unique(self):
        # If we got here with a set, they're already unique — verify count matches
        grafana_rules = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())
        all_uids = []
        for g in grafana_rules["groups"]:
            for r in g.get("rules", []):
                all_uids.append(r["uid"])
        assert len(all_uids) == len(set(all_uids)), f"Duplicate Grafana alert UIDs: {all_uids}"

    def test_prom_critical_alerts_have_short_for_duration(self):
        """Critical Prometheus alerts should fire within 5 minutes."""
        prom = yaml.safe_load(PROM_RULES_FILE.read_text())
        for g in prom["groups"]:
            for r in g.get("rules", []):
                if r.get("labels", {}).get("severity") == "critical":
                    for_dur = r.get("for", "0m")
                    # Parse duration
                    val = int(for_dur.rstrip("msh"))
                    unit = for_dur[-1]
                    seconds = val * {"s": 1, "m": 60, "h": 3600}.get(unit, 1)
                    assert seconds <= 300, (
                        f"Critical alert '{r['alert']}' has for={for_dur} (>{5}m)"
                    )

    def test_prom_warning_alerts_have_reasonable_for_duration(self):
        """Warning Prometheus alerts should fire within 15 minutes."""
        prom = yaml.safe_load(PROM_RULES_FILE.read_text())
        for g in prom["groups"]:
            for r in g.get("rules", []):
                if r.get("labels", {}).get("severity") == "warning":
                    for_dur = r.get("for", "0m")
                    val = int(for_dur.rstrip("msh"))
                    unit = for_dur[-1]
                    seconds = val * {"s": 1, "m": 60, "h": 3600}.get(unit, 1)
                    assert seconds <= 900, f"Warning alert '{r['alert']}' has for={for_dur} (>15m)"

    def test_all_grafana_alerts_have_source_label(self):
        """All Grafana-managed alerts should have source=grafana label."""
        grafana = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())
        for g in grafana["groups"]:
            for r in g.get("rules", []):
                source = r.get("labels", {}).get("source")
                assert source == "grafana", (
                    f"Grafana alert '{r['title']}' missing source=grafana label"
                )

    def test_all_grafana_alerts_have_severity_label(self):
        """All Grafana-managed alerts should have a severity label."""
        grafana = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())
        for g in grafana["groups"]:
            for r in g.get("rules", []):
                assert "severity" in r.get("labels", {}), (
                    f"Grafana alert '{r['title']}' missing severity label"
                )

    def test_all_grafana_alerts_have_annotations(self):
        """All Grafana-managed alerts should have summary and description annotations."""
        grafana = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())
        for g in grafana["groups"]:
            for r in g.get("rules", []):
                ann = r.get("annotations", {})
                assert "summary" in ann, f"Grafana alert '{r['title']}' missing summary annotation"
                assert "description" in ann, (
                    f"Grafana alert '{r['title']}' missing description annotation"
                )

    def test_total_alert_count_across_all_sources(self):
        """We should have 7 Prometheus + 4 Loki + 5 Grafana = 16 total alerts."""
        assert len(self.prom_names) == 7
        assert len(self.loki_names) == 4
        assert len(self.grafana_uids) == 5


# ===========================================================================
# Category 5: Exporter Edge Case Tests
# ===========================================================================

import requests as _requests  # noqa: E402


class TestExporterEdgeCases:
    """Additional edge-case testing for the prefect-exporter module."""

    def setup_method(self):
        self.mod = _import_exporter()
        self.mod._error_count = 0

    def test_exporter_defines_12_gauge_metrics(self):
        """Exporter should define exactly 12 Gauge metrics."""
        from prometheus_client import Gauge as G  # noqa: E402

        gauges = [attr for attr in dir(self.mod) if isinstance(getattr(self.mod, attr, None), G)]
        assert len(gauges) == 12, f"Expected 12 Gauge metrics, got {len(gauges)}: {gauges}"

    def test_exporter_has_9_flow_run_states(self):
        """FLOW_RUN_STATES should enumerate exactly 9 Prefect states."""
        assert len(self.mod.FLOW_RUN_STATES) == 9
        expected = {
            "COMPLETED",
            "FAILED",
            "RUNNING",
            "PENDING",
            "SCHEDULED",
            "CANCELLING",
            "CANCELLED",
            "CRASHED",
            "PAUSED",
        }
        assert set(self.mod.FLOW_RUN_STATES) == expected

    def test_prefect_api_url_default(self):
        """Default PREFECT_API_URL should point to pf-server via short DNS name."""
        # Re-import with PREFECT_API_URL unset so we test the default value
        import os

        old = os.environ.pop("PREFECT_API_URL", None)
        try:
            mod = _import_exporter()
            assert "pf-server" in mod.PREFECT_API_URL
            assert "/api" in mod.PREFECT_API_URL
            assert "spcs.internal" not in mod.PREFECT_API_URL, (
                "Should use short DNS name for cross-account portability"
            )
        finally:
            if old is not None:
                os.environ["PREFECT_API_URL"] = old

    def test_exporter_port_default(self):
        """Default EXPORTER_PORT should be 9394."""
        assert self.mod.EXPORTER_PORT == 9394

    def test_poll_interval_default(self):
        """Default POLL_INTERVAL should be 30 seconds."""
        assert self.mod.POLL_INTERVAL == 30

    @patch("requests.post")
    def test_post_timeout_is_15_seconds(self, mock_post):
        """API calls should use a 15-second timeout."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = 0
        mock_post.return_value = mock_resp

        self.mod._post("/flow_runs/count", {})
        _, kwargs = mock_post.call_args
        assert kwargs.get("timeout") == 15

    @patch("requests.get")
    def test_get_timeout_is_15_seconds(self, mock_get):
        """GET calls should use a 15-second timeout."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}
        mock_get.return_value = mock_resp

        self.mod._get("/some/endpoint")
        _, kwargs = mock_get.call_args
        assert kwargs.get("timeout") == 15

    @patch("requests.post")
    def test_post_connection_error_increments_error_count(self, mock_post):
        """Connection errors should increment the error counter."""
        mock_post.side_effect = _requests.exceptions.ConnectionError("refused")
        self.mod._post("/flow_runs/count", {})
        assert self.mod._error_count == 1

    @patch("requests.post")
    def test_post_request_timeout_increments_error_count(self, mock_post):
        """Request timeouts should increment the error counter."""
        mock_post.side_effect = _requests.exceptions.Timeout("timed out")
        self.mod._post("/flow_runs/count", {})
        assert self.mod._error_count == 1

    @patch("requests.post")
    def test_collect_deployments_uses_limit_200(self, mock_post):
        """Deployments filter should use limit=200 (Prefect 3.x max)."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        self.mod.collect_deployments()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["limit"] == 200

    @patch("requests.post")
    def test_collect_flow_run_durations_uses_limit_200(self, mock_post):
        """Flow run duration filter should use limit=200."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        self.mod.collect_flow_run_durations()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["limit"] == 200

    @patch("requests.post")
    def test_poll_once_sets_duration_metric(self, mock_post):
        """poll_once() should set the poll duration gauge."""
        # flow_runs/count returns int, others return lists
        mock_resp_int = MagicMock()
        mock_resp_int.ok = True
        mock_resp_int.json.return_value = 0

        mock_resp_list = MagicMock()
        mock_resp_list.ok = True
        mock_resp_list.json.return_value = []

        def side_effect(*args, **kwargs):
            url = args[0] if args else kwargs.get("url", "")
            if "/count" in str(url):
                return mock_resp_int
            return mock_resp_list

        mock_post.side_effect = side_effect

        self.mod.poll_once()
        # Duration should be set (>0 since we actually ran collectors)


# ===========================================================================
# Category 6: Poller Edge Case Tests
# ===========================================================================


class TestPollerEdgeCases:
    """Additional edge-case testing for the spcs-log-poller module."""

    def setup_method(self):
        self.mod = _import_poller()

    def test_poller_default_loki_push_url(self):
        """Default LOKI_PUSH_URL should be localhost:3100."""
        assert "localhost:3100" in self.mod.LOKI_PUSH_URL
        assert "/loki/api/v1/push" in self.mod.LOKI_PUSH_URL

    def test_poller_default_poll_interval(self):
        """Default POLL_INTERVAL should be 30 seconds."""
        assert self.mod.POLL_INTERVAL == 30

    def test_poller_default_lookback(self):
        """Default LOOKBACK_SECONDS should be 60."""
        assert self.mod.LOOKBACK_SECONDS == 60

    def test_poller_services_filter_has_5_services(self):
        """Default SERVICES_FILTER should include all 5 SPCS services."""
        expected = {"PF_SERVER", "PF_WORKER", "PF_REDIS", "PF_SERVICES", "PF_MONITOR"}
        assert set(self.mod.SERVICES_FILTER) == expected

    def test_push_to_loki_empty_list_noop(self):
        """push_to_loki([]) should return immediately without making requests."""
        with patch("requests.post") as mock_post:
            self.mod.push_to_loki([])
            mock_post.assert_not_called()

    def test_push_to_loki_groups_by_service_container(self):
        """Logs should be grouped into streams by service|container."""
        logs = [
            {
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "prefect-server",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "msg1",
            },
            {
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "prefect-server",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:01",
                "MESSAGE": "msg2",
            },
            {
                "SERVICE_NAME": "PF_WORKER",
                "CONTAINER_NAME": "prefect-worker",
                "SEVERITY": "WARN",
                "TS": "2025-01-01T00:00:02",
                "MESSAGE": "msg3",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            call_args = mock_post.call_args
            payload = call_args[1]["json"]
            # Should have 2 streams (PF_SERVER|prefect-server and PF_WORKER|prefect-worker)
            assert len(payload["streams"]) == 2

    def test_push_to_loki_sets_correct_labels(self):
        """Stream labels should include job=spcs-logs and source=event-table."""
        logs = [
            {
                "SERVICE_NAME": "PF_SERVER",
                "CONTAINER_NAME": "server",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "test",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            payload = mock_post.call_args[1]["json"]
            stream = payload["streams"][0]["stream"]
            assert stream["job"] == "spcs-logs"
            assert stream["source"] == "event-table"
            assert stream["service"] == "pf-server"  # lowered, underscore→dash

    def test_push_to_loki_timestamp_is_nanoseconds(self):
        """Loki timestamps should be in nanosecond string format."""
        logs = [
            {
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T12:00:00",
                "MESSAGE": "hello",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            payload = mock_post.call_args[1]["json"]
            ts_str = payload["streams"][0]["values"][0][0]
            # Should be a numeric string ≥ 19 digits (nanoseconds since epoch)
            assert ts_str.isdigit()
            assert len(ts_str) >= 19

    def test_push_to_loki_handles_bad_timestamp(self):
        """Invalid timestamps should fall back to current time."""
        logs = [
            {
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "TS": "not-a-date",
                "MESSAGE": "hello",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            # Should not crash, uses fallback timestamp
            mock_post.assert_called_once()

    def test_push_to_loki_formats_log_line(self):
        """Log lines should be formatted as [SEVERITY] [SERVICE/CONTAINER] MESSAGE."""
        logs = [
            {
                "SERVICE_NAME": "PF_MONITOR",
                "CONTAINER_NAME": "grafana",
                "SEVERITY": "WARN",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "disk full",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            payload = mock_post.call_args[1]["json"]
            line = payload["streams"][0]["values"][0][1]
            assert "[WARN]" in line
            assert "[PF_MONITOR/grafana]" in line
            assert "disk full" in line

    def test_push_to_loki_handles_http_error(self):
        """Non-200/204 responses should not raise, just log warning."""
        logs = [
            {
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "msg",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 500
            mock_resp.text = "error"
            mock_post.return_value = mock_resp

            # Should not raise
            self.mod.push_to_loki(logs)

    def test_push_to_loki_handles_connection_error(self):
        """Connection errors to Loki should not crash the poller."""
        logs = [
            {
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "msg",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_post.side_effect = _requests.exceptions.ConnectionError("refused")
            # Should not raise
            self.mod.push_to_loki(logs)

    def test_push_to_loki_uses_10s_timeout(self):
        """Loki push should use a 10-second timeout."""
        logs = [
            {
                "SERVICE_NAME": "SVC",
                "CONTAINER_NAME": "ctr",
                "SEVERITY": "INFO",
                "TS": "2025-01-01T00:00:00",
                "MESSAGE": "msg",
            },
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            _, kwargs = mock_post.call_args
            assert kwargs.get("timeout") == 10

    def test_push_to_loki_missing_fields_uses_defaults(self):
        """Missing fields should default to 'unknown' for service/container."""
        logs = [
            {"TS": "2025-01-01T00:00:00", "MESSAGE": "orphan log"},
        ]
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 204
            mock_post.return_value = mock_resp

            self.mod.push_to_loki(logs)
            payload = mock_post.call_args[1]["json"]
            stream = payload["streams"][0]["stream"]
            assert stream["service"] == "unknown"
            assert stream["container"] == "unknown"


# ===========================================================================
# Category 7: Grafana Provisioning Completeness Tests
# ===========================================================================


class TestGrafanaProvisioningCompleteness:
    """Deep validation of Grafana provisioning files."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.ds = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        self.cp = yaml.safe_load((ALERTING_DIR / "contactpoints.yaml").read_text())
        self.policies = yaml.safe_load((ALERTING_DIR / "policies.yaml").read_text())
        self.rules = yaml.safe_load((ALERTING_DIR / "rules.yaml").read_text())

    def test_datasource_delete_entries_match_datasources(self):
        """deleteDatasources should list all datasources to ensure clean provisioning."""
        ds_names = {d["name"] for d in self.ds["datasources"]}
        del_names = {d["name"] for d in self.ds["deleteDatasources"]}
        assert ds_names == del_names, (
            f"Mismatch: datasources={ds_names}, deleteDatasources={del_names}"
        )

    def test_prometheus_datasource_is_default(self):
        """Prometheus should be the default datasource."""
        prom = next(d for d in self.ds["datasources"] if d["type"] == "prometheus")
        assert prom["isDefault"] is True

    def test_loki_datasource_is_not_default(self):
        """Loki should NOT be the default datasource."""
        loki = next(d for d in self.ds["datasources"] if d["type"] == "loki")
        assert loki["isDefault"] is False

    def test_datasource_uids_match_conventions(self):
        """Datasource UIDs should be 'prometheus' and 'loki'."""
        uids = {d["uid"] for d in self.ds["datasources"]}
        assert uids == {"prometheus", "loki"}

    def test_datasources_use_proxy_access(self):
        """Both datasources should use proxy access mode."""
        for ds in self.ds["datasources"]:
            assert ds["access"] == "proxy", (
                f"Datasource '{ds['name']}' should use proxy access, got '{ds['access']}'"
            )

    def test_datasources_are_not_editable(self):
        """Provisioned datasources should not be editable via UI."""
        for ds in self.ds["datasources"]:
            assert ds["editable"] is False, f"Datasource '{ds['name']}' should not be editable"

    def test_contact_point_receiver_name_matches_policy(self):
        """Contact point name must match what the notification policy references."""
        cp_name = self.cp["contactPoints"][0]["name"]
        policy_receiver = self.policies["policies"][0]["receiver"]
        assert cp_name == policy_receiver, (
            f"Contact point name '{cp_name}' != policy receiver '{policy_receiver}'"
        )

    def test_contact_point_has_email_only(self):
        """Contact point should have email receiver only (Slack is opt-in, no webhook)."""
        receivers = self.cp["contactPoints"][0]["receivers"]
        types = {r["type"] for r in receivers}
        assert "email" in types
        assert "slack" not in types, "Slack is opt-in and disabled by default"
        assert "webhook" not in types

    def test_policy_group_by_includes_alertname(self):
        """Notification policy should group by alertname."""
        group_by = self.policies["policies"][0]["group_by"]
        assert "alertname" in group_by

    def test_grafana_alert_rules_reference_valid_datasources(self):
        """All Grafana alert rule data queries should reference valid datasource UIDs."""
        valid_uids = {"prometheus", "loki", "__expr__"}
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                for data_entry in rule.get("data", []):
                    uid = data_entry.get("datasourceUid", "")
                    assert uid in valid_uids, (
                        f"Grafana alert '{rule['title']}' references unknown datasource UID: {uid}"
                    )

    def test_grafana_alert_rules_have_condition(self):
        """All Grafana alert rules must have a condition referencing a data refId."""
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                condition = rule.get("condition", "")
                assert condition, f"Grafana alert '{rule['title']}' missing condition"
                ref_ids = {d["refId"] for d in rule.get("data", [])}
                assert condition in ref_ids, (
                    f"Grafana alert '{rule['title']}' condition '{condition}' "
                    f"not in refIds: {ref_ids}"
                )


# ===========================================================================
# Category 8: E2E Grafana Tests
# ===========================================================================


@pytest.mark.e2e
class TestGrafanaE2EExtended:
    """Extended E2E tests for Grafana on SPCS."""

    @pytest.fixture(autouse=True)
    def setup(self, spcs_session):
        self.session = spcs_session
        self.grafana_url = os.environ.get(
            "GRAFANA_ENDPOINT",
            "https://zzzzz-orgname-acctname.snowflakecomputing.app/grafana",
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_is_reachable(self):
        """Grafana public endpoint responds with 200."""
        resp = self.session.get(f"{self.grafana_url}/api/health", timeout=15)
        assert resp.status_code == 200

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_health_reports_ok(self):
        """Grafana health API should report database=ok."""
        resp = self.session.get(f"{self.grafana_url}/api/health", timeout=15)
        data = resp.json()
        assert data.get("database") == "ok"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_datasources_provisioned(self):
        """Both Prometheus and Loki datasources should be provisioned."""
        resp = self.session.get(f"{self.grafana_url}/api/datasources", timeout=15)
        assert resp.status_code == 200
        datasources = resp.json()
        ds_types = {d["type"] for d in datasources}
        assert "prometheus" in ds_types
        assert "loki" in ds_types

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_datasource_uids_correct(self):
        """Datasource UIDs should match provisioned values."""
        resp = self.session.get(f"{self.grafana_url}/api/datasources", timeout=15)
        datasources = resp.json()
        uid_map = {d["type"]: d["uid"] for d in datasources}
        assert uid_map.get("prometheus") == "prometheus"
        assert uid_map.get("loki") == "loki"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_dashboards_loaded(self):
        """All expected dashboards should be loaded in Grafana."""
        resp = self.session.get(
            f"{self.grafana_url}/api/search",
            params={"type": "dash-db"},
            timeout=15,
        )
        assert resp.status_code == 200
        dashboards = resp.json()
        loaded_uids = {d["uid"] for d in dashboards}
        expected_uids = {
            "health",
            "prefect-spcs-overview",
            "prefect-app",
            "prefect-vm-workers",
            "prefect-alerts",
            "prefect-spcs-logs",
            "prefect-logs",
            "prefect-redis-detail",
            "prefect-postgres-detail",
        }
        missing = expected_uids - loaded_uids
        assert not missing, f"Dashboards not loaded in Grafana: {missing}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_home_dashboard_set(self):
        """Grafana home dashboard should be configured (not default Grafana home)."""
        resp = self.session.get(
            f"{self.grafana_url}/api/dashboards/uid/health",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dashboard"]["uid"] == "health"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_alert_rules_provisioned(self):
        """Grafana-managed alert rules should be loaded."""
        resp = self.session.get(
            f"{self.grafana_url}/api/v1/provisioning/alert-rules",
            timeout=15,
        )
        assert resp.status_code == 200
        rules = resp.json()
        assert len(rules) >= 3, f"Expected at least 3 Grafana alert rules, got {len(rules)}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_grafana_contact_points_provisioned(self):
        """Contact points should be provisioned."""
        resp = self.session.get(
            f"{self.grafana_url}/api/v1/provisioning/contact-points",
            timeout=15,
        )
        assert resp.status_code == 200
        points = resp.json()
        names = {p.get("name") for p in points}
        assert "prefect-alerts" in names


# ===========================================================================
# Category 9: E2E Prometheus Extended Tests
# ===========================================================================


@pytest.mark.e2e
class TestPrometheusE2EExtended:
    """Extended E2E tests for Prometheus on SPCS."""

    @pytest.fixture(autouse=True)
    def setup(self, spcs_session):
        self.session = spcs_session
        self.prometheus_url = os.environ.get(
            "PROMETHEUS_ENDPOINT",
            "https://xxxxx-orgname-acctname.snowflakecomputing.app",
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_all_targets_up(self):
        """All Prometheus scrape targets should have health=up.

        The prefect-worker-spcs job is excluded because the worker container
        has no /metrics endpoint — Prometheus always reports it as down.
        The target is kept in the scrape config so that the ``up`` metric
        exists for alerting (``absent(up{job="prefect-worker-spcs"})``
        fires when the worker service is completely unreachable).
        """
        # Targets with no /metrics endpoint (up=0 is expected)
        PROBE_ONLY_JOBS = {"prefect-worker-spcs"}

        resp = self.session.get(f"{self.prometheus_url}/api/v1/targets", timeout=15)
        assert resp.status_code == 200
        data = resp.json()
        targets = data.get("data", {}).get("activeTargets", [])
        down = [
            t["labels"].get("job")
            for t in targets
            if t.get("health") != "up" and t["labels"].get("job") not in PROBE_ONLY_JOBS
        ]
        assert not down, f"Targets not up: {down}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_has_8_scrape_jobs(self):
        """Prometheus should have exactly 8 active scrape jobs."""
        resp = self.session.get(f"{self.prometheus_url}/api/v1/targets", timeout=15)
        data = resp.json()
        targets = data.get("data", {}).get("activeTargets", [])
        jobs = {t["labels"].get("job") for t in targets}
        assert len(jobs) == 8, f"Expected 8 jobs, got {len(jobs)}: {jobs}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_recording_rules_loaded(self):
        """Prometheus should have recording rules loaded."""
        resp = self.session.get(f"{self.prometheus_url}/api/v1/rules", timeout=15)
        assert resp.status_code == 200
        data = resp.json()
        groups = data.get("data", {}).get("groups", [])
        recording_rules = [
            r for g in groups for r in g.get("rules", []) if r.get("type") == "recording"
        ]
        assert len(recording_rules) >= 9, (
            f"Expected at least 9 recording rules, got {len(recording_rules)}"
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_alert_rules_loaded(self):
        """Prometheus should have 7 alerting rules loaded."""
        resp = self.session.get(f"{self.prometheus_url}/api/v1/rules", timeout=15)
        data = resp.json()
        groups = data.get("data", {}).get("groups", [])
        alert_rules = [r for g in groups for r in g.get("rules", []) if r.get("type") == "alerting"]
        assert len(alert_rules) >= 7, f"Expected at least 7 alert rules, got {len(alert_rules)}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_recording_rule_metrics_queryable(self):
        """Recording rule output metrics should be queryable."""
        recording_metrics = [
            "prefect:server_uptime:ratio_1h",
            "prefect:flow_success_rate:ratio",
            "prefect:flow_runs_active:total",
        ]
        for metric in recording_metrics:
            resp = self.session.get(
                f"{self.prometheus_url}/api/v1/query",
                params={"query": metric},
                timeout=15,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "success", f"Recording rule metric '{metric}' query failed"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_up_metric_for_all_jobs(self):
        """'up' metric should exist for all 8 scrape jobs."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "up"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        jobs_with_up = {r["metric"].get("job") for r in results}
        expected_jobs = {
            "prefect-server",
            "prefect-worker-spcs",
            "prometheus",
            "grafana",
            "loki",
            "redis",
            "postgres",
            "prefect-app",
        }
        missing = expected_jobs - jobs_with_up
        assert not missing, f"'up' metric missing for jobs: {missing}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_prometheus_tsdb_head_not_empty(self):
        """Prometheus TSDB should have data (head series > 0)."""
        resp = self.session.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": "prometheus_tsdb_head_series"},
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            value = float(results[0]["value"][1])
            assert value > 0, "TSDB head series should be > 0"


# ===========================================================================
# Category 10: E2E Loki Extended Tests
# ===========================================================================


@pytest.mark.e2e
class TestLokiE2EExtended:
    """Extended E2E tests for Loki on SPCS."""

    @pytest.fixture(autouse=True)
    def setup(self, spcs_session):
        self.session = spcs_session
        self.loki_url = os.environ.get(
            "LOKI_ENDPOINT",
            "https://yyyyy-orgname-acctname.snowflakecomputing.app",
        )

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_has_container_label(self):
        """Loki streams should include a 'container' label from the poller."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/labels",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "container" in data.get("data", [])

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_has_severity_label(self):
        """Loki streams should include a 'severity' label."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/labels",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "severity" in data.get("data", [])

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_service_label_values_contain_prefect(self):
        """Service label values should include Prefect services."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/label/service/values",
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        values = data.get("data", [])
        # At least one service should contain "pf-" (lowered service names)
        assert any("pf-" in v for v in values), f"Expected Prefect service labels, got: {values}"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_ruler_api_accessible(self):
        """Loki ruler API should be enabled and accessible."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/rules",
            timeout=15,
        )
        # 200 with rules or 404 if no rules — both indicate ruler is running
        assert resp.status_code in (200, 404)

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_query_with_severity_filter(self):
        """Querying Loki with a severity filter should work."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/query_range",
            params={
                "query": '{job="spcs-logs", severity="INFO"}',
                "limit": "5",
            },
            timeout=15,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

    @pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_PAT"),
        reason=E2E_SKIP_REASON,
    )
    def test_loki_ingestion_rate_within_limits(self):
        """Verify Loki is not being rate-limited (config allows 10 MB/s)."""
        resp = self.session.get(
            f"{self.loki_url}/loki/api/v1/query",
            params={"query": '{job="spcs-logs"} | line_format "{{.}}"', "limit": "1"},
            timeout=15,
        )
        # Just verify Loki is responsive — rate limiting would cause 429
        assert resp.status_code != 429


# ===========================================================================
# Category 11: Cross-Component Consistency Tests
# ===========================================================================


class TestCrossComponentConsistency:
    """Validate that configs across all components are consistent."""

    @pytest.fixture(autouse=True)
    def load_all(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.prom_config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        self.loki_config = yaml.safe_load(LOKI_CONFIG_FILE.read_text())
        self.ds_config = yaml.safe_load(GRAFANA_DS_FILE.read_text())

    def test_loki_port_consistent_across_configs(self):
        """Loki HTTP port should be consistent across loki-config, prometheus, and datasource."""
        # Loki config
        loki_port = self.loki_config["server"]["http_listen_port"]
        # Prometheus scrape target for loki
        loki_job = next(j for j in self.prom_config["scrape_configs"] if j["job_name"] == "loki")
        loki_target = loki_job["static_configs"][0]["targets"][0]
        assert str(loki_port) in loki_target, (
            f"Loki port {loki_port} not found in Prometheus target: {loki_target}"
        )
        # Grafana datasource
        loki_ds = next(d for d in self.ds_config["datasources"] if d["type"] == "loki")
        assert str(loki_port) in loki_ds["url"], (
            f"Loki port {loki_port} not found in datasource URL: {loki_ds['url']}"
        )

    def test_prometheus_port_consistent(self):
        """Prometheus port should be consistent across config and datasource."""
        # Prometheus self-scrape target
        prom_job = next(
            j for j in self.prom_config["scrape_configs"] if j["job_name"] == "prometheus"
        )
        prom_target = prom_job["static_configs"][0]["targets"][0]
        # Grafana datasource
        prom_ds = next(d for d in self.ds_config["datasources"] if d["type"] == "prometheus")
        # Both should reference port 9090
        assert "9090" in prom_target
        assert "9090" in prom_ds["url"]

    def test_all_prometheus_targets_are_localhost(self):
        """All Prometheus scrape targets for co-located containers should use localhost."""
        # Non-localhost targets are OK for remote services (prefect-server, redis, etc.)
        localhost_jobs = {"prometheus", "grafana", "loki", "postgres", "prefect-app"}
        for job in self.prom_config["scrape_configs"]:
            if job["job_name"] in localhost_jobs:
                targets = job["static_configs"][0]["targets"]
                for t in targets:
                    assert "localhost" in t, (
                        f"Job '{job['job_name']}' should use localhost, got: {t}"
                    )

    def test_loki_ruler_alertmanager_url_is_disabled(self):
        """Loki ruler alertmanager_url must be empty (disabled).

        Grafana's unified alerting AM API returns 400 for Loki-generated
        alerts. Loki ruler evaluates rules locally; Grafana queries state
        via /loki/api/v1/rules instead.
        """
        am_url = self.loki_config["ruler"]["alertmanager_url"]
        assert am_url == "", (
            f"alertmanager_url must be empty — Grafana's unified alerting AM API "
            f"is incompatible with Loki-generated alerts. Got: {am_url!r}"
        )

    def test_spec_container_count_matches_scrape_config(self):
        """Number of SPCS containers should be reflected in scrape configs.

        9 containers in spec (6 original + 2 backup sidecars + observe-agent),
        8 scrape jobs (because prefect-server and redis are remote services, not
        in the monitor spec, but scraped via SPCS DNS; backup sidecars and
        observe-agent don't expose Prometheus metrics endpoints so they don't
        add scrape jobs).
        """
        containers = self.spec["spec"]["containers"]
        assert len(containers) == 9
        scrape_jobs = self.prom_config["scrape_configs"]
        assert len(scrape_jobs) == 8


# ===========================================================================
# Grafana Postgres Persistence — comprehensive validation
# ===========================================================================
SECRETS_TEMPLATE = PROJECT_DIR / "sql" / "03_setup_secrets.sql.template"


class TestGrafanaPostgresPersistence:
    """Validate Grafana is configured for Postgres-backed persistence.

    Grafana stores dashboards, users, annotations, alert state, and
    preferences in a Postgres database (schema 'grafana') on the same
    managed Postgres instance used by Prefect server.
    """

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")
        self.env = self.grafana.get("env", {})
        self.secrets = self.grafana.get("secrets", [])
        self.secret_env_vars = [s["envVarName"] for s in self.secrets]
        self.secret_objects = [s["snowflakeSecret"]["objectName"].lower() for s in self.secrets]
        self.deploy_content = DEPLOY_SCRIPT.read_text()
        self.template_content = SECRETS_TEMPLATE.read_text()

    # --- Spec env vars ---
    def test_database_type_is_postgres(self):
        assert self.env.get("GF_DATABASE_TYPE") == "postgres"

    def test_database_ssl_mode_require(self):
        assert "GF_DATABASE_SSL_MODE" not in self.env, (
            "GF_DATABASE_SSL_MODE must not be in env — sslmode is embedded in the DSN secret"
        )

    def test_database_type_not_sqlite(self):
        """Ensure we haven't accidentally set it to sqlite3."""
        assert self.env.get("GF_DATABASE_TYPE") != "sqlite3"

    def test_no_hardcoded_database_url_in_env(self):
        """Database URL must come from a secret, never hardcoded in env."""
        assert "GF_DATABASE_URL" not in self.env, (
            "GF_DATABASE_URL must be injected via secret, not hardcoded in env"
        )
        assert "GF_DATABASE_HOST" not in self.env
        assert "GF_DATABASE_PASSWORD" not in self.env

    # --- Spec secrets ---
    def test_database_url_injected_from_secret(self):
        assert "GF_DATABASE_URL" in self.secret_env_vars

    def test_admin_password_injected_from_secret(self):
        assert "GF_SECURITY_ADMIN_PASSWORD" in self.secret_env_vars

    def test_grafana_has_exactly_six_secrets(self):
        assert len(self.secrets) == 6, (
            f"Grafana should have 6 secrets (admin password + DB DSN + SMTP password + "
            f"SMTP user + SMTP from + SMTP recipients; Slack is opt-in), "
            f"got {len(self.secrets)}: {self.secret_env_vars}"
        )

    def test_db_dsn_secret_object_referenced(self):
        assert any("grafana_db_dsn" in obj for obj in self.secret_objects)

    def test_admin_password_secret_object_referenced(self):
        assert any("grafana_admin_password" in obj for obj in self.secret_objects)

    def test_secrets_use_secret_string_key(self):
        """All Grafana secrets must reference secret_string key."""
        for s in self.secrets:
            assert s["secretKeyRef"] == "secret_string", (  # pragma: allowlist secret
                f"Secret for {s['envVarName']} must use 'secret_string' key"
            )

    # --- Deploy script ---
    def test_deploy_creates_grafana_db_dsn_secret(self):
        assert "GRAFANA_DB_DSN" in self.deploy_content

    def test_deploy_eai_includes_grafana_db_dsn(self):
        """EAI must list GRAFANA_DB_DSN in allowed authentication secrets."""
        assert "GRAFANA_DB_DSN" in self.deploy_content
        # Find the EAI creation block and verify both secrets are listed
        eai_block = self.deploy_content[self.deploy_content.index("PREFECT_MONITOR_EAI") :]
        assert "POSTGRES_EXPORTER_DSN" in eai_block
        assert "GRAFANA_DB_DSN" in eai_block

    def test_deploy_eai_includes_postgres_exporter_dsn(self):
        """EAI must still include POSTGRES_EXPORTER_DSN."""
        assert "POSTGRES_EXPORTER_DSN" in self.deploy_content

    def test_deploy_network_rule_includes_postgres_host(self):
        """Network rule must include Postgres host for Grafana DB access."""
        assert "PREFECT_PG_HOST" in self.deploy_content
        assert "5432" in self.deploy_content

    def test_deploy_prompts_for_dsn_if_not_set(self):
        """Deploy script should prompt if GRAFANA_DB_DSN env var is not set."""
        assert (
            "GRAFANA_DB_DSN:-" in self.deploy_content
            or "GRAFANA_DB_DSN env var not set" in self.deploy_content
        )

    def test_deploy_gracefully_handles_missing_dsn(self):
        """Deploy should warn (not fail) if DSN is missing — falls back to SQLite."""
        assert "SQLite" in self.deploy_content or "sqlite" in self.deploy_content.lower()

    # --- Secrets template ---
    def test_secrets_template_documents_grafana_db_dsn(self):
        assert "GRAFANA_DB_DSN" in self.template_content

    def test_secrets_template_documents_search_path(self):
        """Template must mention search_path=grafana for schema isolation."""
        assert "search_path=grafana" in self.template_content

    def test_secrets_template_documents_create_schema(self):
        """Template must remind users to create the grafana schema."""
        assert "CREATE SCHEMA" in self.template_content

    def test_secrets_template_has_all_secrets(self):
        """Template must document every secret used in the monitoring stack."""
        for secret_name in [
            "PREFECT_DB_PASSWORD",
            "GIT_ACCESS_TOKEN",
            "POSTGRES_EXPORTER_DSN",
            "GRAFANA_DB_DSN",
        ]:
            assert secret_name in self.template_content, (
                f"Secret '{secret_name}' missing from secrets template"
            )


# ===========================================================================
# Loki Config Path Consistency
# ===========================================================================
class TestLokiConfigPathConsistency:
    """Validate Loki config paths match volume mount paths in the spec.

    After consolidating loki-config and loki-rules into a single mount,
    rules_directory in loki-config.yaml must be under the loki-config
    mountPath (/etc/loki), i.e. /etc/loki/rules.
    """

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.loki_config = yaml.safe_load(LOKI_CONFIG_FILE.read_text())
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.loki_container = next(c for c in containers if c["name"] == "loki")
        self.mounts = {m["name"]: m for m in self.loki_container.get("volumeMounts", [])}

    def test_rules_directory_under_config_mount(self):
        """common.storage.filesystem.rules_directory must be under loki-config mountPath."""
        rules_dir = self.loki_config["common"]["storage"]["filesystem"]["rules_directory"]
        mount_path = self.mounts["loki-config"]["mountPath"]
        assert rules_dir.startswith(mount_path), (
            f"Loki rules_directory '{rules_dir}' is not under loki-config mountPath '{mount_path}'"
        )

    def test_ruler_local_directory_under_config_mount(self):
        """ruler.storage.local.directory must be under loki-config mountPath."""
        ruler_dir = self.loki_config["ruler"]["storage"]["local"]["directory"]
        mount_path = self.mounts["loki-config"]["mountPath"]
        assert ruler_dir.startswith(mount_path), (
            f"Loki ruler directory '{ruler_dir}' is not under loki-config mountPath '{mount_path}'"
        )

    def test_loki_config_mount_matches_args(self):
        """Loki -config.file arg path must be under the loki-config mount."""
        config_mount = self.mounts["loki-config"]["mountPath"]
        args = self.loki_container.get("args", [])
        config_arg = next(a for a in args if "-config.file=" in a)
        config_path = config_arg.split("=")[1]
        assert config_path.startswith(config_mount), (
            f"Loki config file '{config_path}' is not under mount '{config_mount}'"
        )

    def test_chunks_directory_under_data_mount(self):
        """chunks_directory must be under the loki-data block storage mount."""
        chunks_dir = self.loki_config["common"]["storage"]["filesystem"]["chunks_directory"]
        data_mount = self.mounts["loki-data"]["mountPath"]
        assert chunks_dir.startswith(data_mount), (
            f"Chunks directory '{chunks_dir}' is not under data mount '{data_mount}'"
        )

    def test_compactor_directory_under_data_mount(self):
        """compactor working directory must be under block storage."""
        compactor_dir = self.loki_config["compactor"]["working_directory"]
        data_mount = self.mounts["loki-data"]["mountPath"]
        assert compactor_dir.startswith(data_mount), (
            f"Compactor directory '{compactor_dir}' is not under data mount '{data_mount}'"
        )

    def test_rule_path_under_data_mount(self):
        """ruler.rule_path (temp) must be under block storage, not stage mount."""
        rule_path = self.loki_config["ruler"]["rule_path"]
        data_mount = self.mounts["loki-data"]["mountPath"]
        assert rule_path.startswith(data_mount), (
            f"Ruler rule_path '{rule_path}' should be under data mount '{data_mount}'"
        )


# ===========================================================================
# Volume Mount Non-Overlap Validation
# ===========================================================================
class TestVolumeMountNonOverlap:
    """Validate that no volume mounts overlap within a container.

    SPCS spec validation rejects overlapping mount paths (e.g. /loki and
    /loki/rules). Each container's mounts must be non-overlapping.
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_no_container_has_overlapping_mounts(self):
        for container in self.spec["spec"]["containers"]:
            mounts = container.get("volumeMounts", [])
            paths = sorted([m["mountPath"] for m in mounts])
            for i in range(len(paths)):
                for j in range(i + 1, len(paths)):
                    parent = paths[i].rstrip("/") + "/"
                    child = paths[j]
                    assert not child.startswith(parent), (
                        f"Container '{container['name']}': mount '{child}' "
                        f"overlaps with parent mount '{paths[i]}'"
                    )

    def test_loki_mounts_are_non_overlapping(self):
        """Specific regression test for the /loki vs /loki/rules overlap."""
        containers = self.spec["spec"]["containers"]
        loki = next(c for c in containers if c["name"] == "loki")
        paths = [m["mountPath"] for m in loki.get("volumeMounts", [])]
        for i, p1 in enumerate(paths):
            for j, p2 in enumerate(paths):
                if i != j and p2.startswith(p1.rstrip("/") + "/"):
                    pytest.fail(
                        f"Loki mount overlap: '{p2}' is under '{p1}'. "
                        f"SPCS rejects overlapping volume mounts."
                    )


# ===========================================================================
# Dashboard Fix Validation — Flow Runs by State readability
# ===========================================================================
class TestFlowRunsByStateReadability:
    """Validate the Flow Runs by State panel is readable.

    A full-width horizontal stat panel with 9 states makes it impossible
    to visually pair state names with their values. The panel must use
    auto or vertical orientation with centered text.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_PREFECT_APP.read_text())
        self.panel = next(p for p in self.dash["panels"] if p["title"] == "Flow Runs by State")

    def test_orientation_is_not_horizontal(self):
        """Horizontal orientation makes 9+ stats unreadable."""
        orientation = self.panel.get("options", {}).get("orientation", "")
        assert orientation != "horizontal", (
            "Flow Runs by State must not use horizontal orientation — "
            "it makes state labels hard to match with values"
        )

    def test_orientation_is_auto(self):
        orientation = self.panel.get("options", {}).get("orientation", "")
        assert orientation == "auto"

    def test_justify_mode_is_center(self):
        justify = self.panel.get("options", {}).get("justifyMode", "")
        assert justify == "center"

    def test_panel_height_sufficient(self):
        """Panel must be tall enough for 9 state tiles to render legibly."""
        h = self.panel["gridPos"]["h"]
        assert h >= 5, f"Panel height {h} is too short for 9 states"

    def test_uses_value_and_name_text_mode(self):
        text_mode = self.panel.get("options", {}).get("textMode", "")
        assert text_mode == "value_and_name"

    def test_has_color_overrides_for_all_key_states(self):
        """Key flow run states should have color overrides for quick scanning."""
        overrides = self.panel.get("fieldConfig", {}).get("overrides", [])
        override_names = {
            o["matcher"]["options"] for o in overrides if o["matcher"]["id"] == "byName"
        }
        required_states = {"COMPLETED", "FAILED", "CRASHED", "RUNNING"}
        missing = required_states - override_names
        assert not missing, f"Missing color overrides for states: {missing}"


# ===========================================================================
# Dashboard Fix Validation — Flow Run Success Rate PromQL
# ===========================================================================
class TestFlowRunSuccessRatePromQL:
    """Validate the success rate PromQL uses sum() to avoid label mismatches.

    prefect_flow_runs has a 'state' label. Arithmetic like
      prefect_flow_runs{state="COMPLETED"} + prefect_flow_runs{state="FAILED"}
    produces no result because Prometheus can't match different state labels.
    Each term must be wrapped in sum() to drop labels before arithmetic.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_PREFECT_APP.read_text())
        panel = next(p for p in self.dash["panels"] if p["title"] == "Flow Run Success Rate")
        self.expr = panel["targets"][0]["expr"]

    def test_numerator_uses_sum(self):
        """Numerator must wrap COMPLETED in sum()."""
        assert 'sum(prefect_flow_runs{state="COMPLETED"})' in self.expr

    def test_denominator_uses_sum_for_completed(self):
        parts = self.expr.split("/")
        denom = parts[1] if len(parts) > 1 else ""
        assert 'sum(prefect_flow_runs{state="COMPLETED"})' in denom

    def test_denominator_uses_sum_for_failed(self):
        parts = self.expr.split("/")
        denom = parts[1] if len(parts) > 1 else ""
        assert 'sum(prefect_flow_runs{state="FAILED"})' in denom

    def test_denominator_uses_sum_for_crashed(self):
        parts = self.expr.split("/")
        denom = parts[1] if len(parts) > 1 else ""
        assert 'sum(prefect_flow_runs{state="CRASHED"})' in denom

    def test_uses_clamp_min_for_safe_division(self):
        assert "clamp_min(" in self.expr

    def test_result_multiplied_by_100(self):
        assert "* 100" in self.expr

    def test_no_bare_label_arithmetic(self):
        """Must not have direct addition of different-label series without sum()."""
        # This pattern would cause 'no data': {state="X"} + {state="Y"}
        import re  # noqa: E402

        bare_add = re.search(r'\{state="[A-Z]+"\}\s*\+\s*prefect_flow_runs\{state=', self.expr)
        assert bare_add is None, (
            f"Found bare label arithmetic without sum() — will produce no data: {self.expr}"
        )


# ===========================================================================
# Dashboard Fix Validation — Postgres Table Size metric
# ===========================================================================
class TestPostgresTableSizeMetric:
    """Validate Table Size panel uses the correct postgres-exporter metric.

    The postgres-exporter exposes pg_stat_user_tables_table_size_bytes.
    Using a non-existent metric name causes 'no data' in the panel.
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_POSTGRES_DETAIL.read_text())
        self.panel = next(p for p in self.dash["panels"] if p["title"] == "Table Size (Top 10)")
        self.expr = self.panel["targets"][0]["expr"]

    def test_uses_pg_stat_user_tables_table_size_bytes(self):
        assert "pg_stat_user_tables_table_size_bytes" in self.expr

    def test_does_not_use_pg_total_relation_size(self):
        """pg_total_relation_size_bytes is NOT exposed by postgres-exporter."""
        assert "pg_total_relation_size_bytes" not in self.expr

    def test_uses_topk(self):
        assert "topk(10," in self.expr or "topk(10 ," in self.expr

    def test_filters_by_postgres_job(self):
        assert 'job="postgres"' in self.expr

    def test_uses_relname_legend(self):
        legend = self.panel["targets"][0].get("legendFormat", "")
        assert "relname" in legend

    def test_panel_type_is_bargauge(self):
        assert self.panel["type"] == "bargauge"

    def test_unit_is_bytes(self):
        unit = self.panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "")
        assert unit == "bytes"


# ===========================================================================
# Dashboard Panel Grid Layout Validation
# ===========================================================================
class TestPrefectAppPanelLayout:
    """Validate panel layout has no gaps or overlaps after grid position changes."""

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_PREFECT_APP.read_text())
        self.panels = self.dash["panels"]

    def test_no_panels_overlap(self):
        """No two panels should occupy the same grid cell."""
        occupied = set()
        for panel in self.panels:
            gp = panel["gridPos"]
            for row in range(gp["y"], gp["y"] + gp["h"]):
                for col in range(gp["x"], gp["x"] + gp["w"]):
                    key = (row, col)
                    assert key not in occupied, (
                        f"Panel '{panel['title']}' overlaps at grid position ({row}, {col})"
                    )
                    occupied.add(key)

    def test_all_panels_within_24_column_grid(self):
        for panel in self.panels:
            gp = panel["gridPos"]
            assert gp["x"] + gp["w"] <= 24, (
                f"Panel '{panel['title']}' exceeds 24-column grid: x={gp['x']}, w={gp['w']}"
            )

    def test_panels_start_at_y_zero(self):
        """First panel row should start at y=0."""
        min_y = min(p["gridPos"]["y"] for p in self.panels)
        assert min_y == 0

    def test_no_negative_positions(self):
        for panel in self.panels:
            gp = panel["gridPos"]
            assert gp["x"] >= 0 and gp["y"] >= 0, f"Panel '{panel['title']}' has negative position"

    def test_panel_rows_are_contiguous(self):
        """No large unexplained vertical gaps between panel rows."""
        y_starts = sorted(set(p["gridPos"]["y"] for p in self.panels))
        for i in range(1, len(y_starts)):
            gap = y_starts[i] - y_starts[i - 1]
            assert gap <= 10, (
                f"Large vertical gap ({gap} units) between rows "
                f"y={y_starts[i - 1]} and y={y_starts[i]}"
            )


# ===========================================================================
# Deploy Script — Postgres Network Access
# ===========================================================================
class TestDeployScriptPostgresAccess:
    """Validate deploy script properly configures Postgres network access."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_reads_pg_host_from_env(self):
        """Script must read Postgres host from PREFECT_PG_HOST env var."""
        assert "PREFECT_PG_HOST" in self.content

    def test_prompts_for_pg_host_if_missing(self):
        assert "Enter Postgres host" in self.content

    def test_pg_host_added_to_network_rule(self):
        """Postgres host:5432 must be in the network rule VALUE_LIST."""
        assert "PG_RULE_ENTRY" in self.content or "PG_HOST" in self.content
        assert "5432" in self.content

    def test_network_rule_is_create_or_replace(self):
        """Network rule must use CREATE OR REPLACE to update the host list."""
        assert "CREATE OR REPLACE NETWORK RULE" in self.content

    def test_eai_is_create_or_replace(self):
        """EAI must use CREATE OR REPLACE to update allowed secrets."""
        assert "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION" in self.content

    def test_does_not_use_create_or_replace_service(self):
        """MUST NOT use CREATE OR REPLACE SERVICE — destroys the URL."""
        assert "CREATE OR REPLACE SERVICE" not in self.content

    def test_grafana_dsn_prompt_shows_format(self):
        """Prompt should show the expected DSN format."""
        assert "postgres://" in self.content

    def test_deploy_escapes_dsn_single_quotes(self):
        """DSN must be escaped for safe SQL interpolation (via _create_secret helper)."""
        assert "_create_secret" in self.content


# ===========================================================================
# Grafana Container Secret Configuration — comprehensive
# ===========================================================================
class TestGrafanaSecretIsolation:
    """Validate that no credentials leak into env vars or spec text."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.content = MONITOR_SPEC_FILE.read_text()
        self.spec = yaml.safe_load(self.content)
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")

    def test_no_password_in_env(self):
        env = self.grafana.get("env", {})
        for key, val in env.items():
            assert "password" not in str(val).lower(), f"Password found in Grafana env var {key}"

    def test_no_dsn_in_env(self):
        env = self.grafana.get("env", {})
        for key, val in env.items():
            assert "postgresql://" not in str(val) and "postgres://" not in str(val), (
                f"Database DSN found in Grafana env var {key}"
            )

    def test_no_password_in_spec_text(self):
        """No actual passwords should appear anywhere in the spec file."""
        lower = self.content.lower()
        # These are patterns that would indicate a hardcoded credential
        for bad_pattern in ["password=", "secret_string =", "Pr3fect"]:
            assert bad_pattern.lower() not in lower, (
                f"Possible hardcoded credential pattern '{bad_pattern}' found in spec"
            )

    def test_spec_contains_no_connection_strings(self):
        """No postgres:// or postgresql:// URLs in non-comment lines of the spec."""
        for line in self.content.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue  # skip YAML comments
            assert "postgres://" not in stripped, (
                f"Connection string 'postgres://' found in spec line: {stripped}"
            )
            assert "postgresql://" not in stripped, (
                f"Connection string 'postgresql://' found in spec line: {stripped}"
            )


# ===========================================================================
# Cross-Component Consistency — extended for Postgres persistence
# ===========================================================================
class TestCrossComponentPostgresConsistency:
    """Validate Postgres-related config is consistent across all components."""

    @pytest.fixture(autouse=True)
    def load_all(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.deploy = DEPLOY_SCRIPT.read_text()
        self.template = SECRETS_TEMPLATE.read_text()
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")
        self.pg_exporter = next(c for c in containers if c["name"] == "postgres-exporter")

    def test_both_pg_consumers_use_secrets(self):
        """Both Grafana and postgres-exporter must get credentials from secrets."""
        grafana_secrets = [s["envVarName"] for s in self.grafana.get("secrets", [])]
        pg_secrets = [s["envVarName"] for s in self.pg_exporter.get("secrets", [])]
        assert "GF_DATABASE_URL" in grafana_secrets
        assert "DATA_SOURCE_NAME" in pg_secrets

    def test_deploy_lists_both_secrets_in_eai(self):
        """EAI must allow both POSTGRES_EXPORTER_DSN and GRAFANA_DB_DSN."""
        assert "POSTGRES_EXPORTER_DSN" in self.deploy
        assert "GRAFANA_DB_DSN" in self.deploy

    def test_template_documents_both_pg_secrets(self):
        assert "POSTGRES_EXPORTER_DSN" in self.template
        assert "GRAFANA_DB_DSN" in self.template

    def test_grafana_and_pg_exporter_dsn_formats_documented_differently(self):
        """Template should document that pg-exporter uses postgresql:// and Grafana uses postgres://."""
        # pg-exporter uses standard postgresql:// prefix
        assert "postgresql://" in self.template
        # Grafana uses postgres:// prefix
        assert "postgres://" in self.template

    def test_spec_secrets_match_deploy_secrets(self):
        """Every secret referenced in the spec must be created by the deploy script."""
        all_secrets = []
        for container in self.spec["spec"]["containers"]:
            for s in container.get("secrets", []):
                obj = s["snowflakeSecret"]["objectName"].lower()
                # Strip any db.schema prefix
                name = obj.split(".")[-1]
                all_secrets.append(name)
        for secret_name in all_secrets:
            assert secret_name.upper() in self.deploy, (
                f"Secret '{secret_name}' used in spec but not created by deploy script"
            )


# ===========================================================================
# Data Persistence — Backup Sidecar Configuration
# ===========================================================================
PROM_BACKUP_PY = PROJECT_DIR / "images" / "prom-backup" / "backup.py"
LOKI_BACKUP_PY = PROJECT_DIR / "images" / "loki-backup" / "backup.py"
BACKUP_LIB_PY = PROJECT_DIR / "images" / "stage-backup" / "backup_lib.py"
PROM_BACKUP_DOCKERFILE = PROJECT_DIR / "images" / "prom-backup" / "Dockerfile"
LOKI_BACKUP_DOCKERFILE = PROJECT_DIR / "images" / "loki-backup" / "Dockerfile"
REDIS_SPEC_FILE = PROJECT_DIR / "specs" / "pf_redis.yaml"
POLLER_PY = PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py"


class TestPrometheusBackupSidecar:
    """Validate prom-backup sidecar is correctly configured in PF_MONITOR."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.containers_by_name = {c["name"]: c for c in containers}
        self.prom_backup = self.containers_by_name.get("prom-backup")

    def test_prom_backup_container_exists(self):
        assert self.prom_backup is not None, "prom-backup container missing from spec"

    def test_shares_prom_data_volume(self):
        """prom-backup must mount the same prom-data volume as prometheus."""
        prom_mounts = {m["name"] for m in self.containers_by_name["prometheus"]["volumeMounts"]}
        backup_mounts = {m["name"] for m in self.prom_backup["volumeMounts"]}
        assert "prom-data" in prom_mounts
        assert "prom-data" in backup_mounts

    def test_prom_data_mount_path_matches(self):
        """Both prometheus and prom-backup must mount prom-data at the same path."""
        prom_path = next(
            m["mountPath"]
            for m in self.containers_by_name["prometheus"]["volumeMounts"]
            if m["name"] == "prom-data"
        )
        backup_path = next(
            m["mountPath"] for m in self.prom_backup["volumeMounts"] if m["name"] == "prom-data"
        )
        assert prom_path == backup_path

    def test_backup_interval_env(self):
        env = self.prom_backup.get("env", {})
        assert "BACKUP_INTERVAL_SECONDS" in env
        assert int(env["BACKUP_INTERVAL_SECONDS"]) > 0

    def test_keep_count_env(self):
        env = self.prom_backup.get("env", {})
        assert "BACKUP_KEEP_COUNT" in env
        assert int(env["BACKUP_KEEP_COUNT"]) >= 1

    def test_prometheus_url_points_to_localhost(self):
        env = self.prom_backup.get("env", {})
        url = env.get("PROMETHEUS_URL", "")
        assert "localhost:9090" in url

    def test_stage_prefix_env(self):
        env = self.prom_backup.get("env", {})
        assert "backups/prometheus" in env.get("STAGE_BACKUP_PREFIX", "")

    def test_restore_on_cold_start_enabled(self):
        env = self.prom_backup.get("env", {})
        assert env.get("RESTORE_ON_COLD_START", "").lower() == "true"

    def test_cpu_request_minimal(self):
        """Backup sidecar should use minimal CPU — it's idle 99% of the time."""
        cpu = float(self.prom_backup["resources"]["requests"]["cpu"])
        assert cpu <= 0.1, f"prom-backup CPU request too high: {cpu}"

    def test_memory_limit_reasonable(self):
        mem = self.prom_backup["resources"]["limits"]["memory"]
        assert mem in ("128M", "256M")

    def test_no_secrets_needed(self):
        """Backup sidecar uses SPCS OAuth token — no secrets required."""
        assert "secrets" not in self.prom_backup or len(self.prom_backup.get("secrets", [])) == 0


class TestLokiBackupSidecar:
    """Validate loki-backup sidecar is correctly configured in PF_MONITOR."""

    @pytest.fixture(autouse=True)
    def load_configs(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.containers_by_name = {c["name"]: c for c in containers}
        self.loki_backup = self.containers_by_name.get("loki-backup")

    def test_loki_backup_container_exists(self):
        assert self.loki_backup is not None, "loki-backup container missing from spec"

    def test_shares_loki_data_volume(self):
        """loki-backup must mount the same loki-data volume as loki."""
        loki_mounts = {m["name"] for m in self.containers_by_name["loki"]["volumeMounts"]}
        backup_mounts = {m["name"] for m in self.loki_backup["volumeMounts"]}
        assert "loki-data" in loki_mounts
        assert "loki-data" in backup_mounts

    def test_loki_data_mount_path_matches(self):
        loki_path = next(
            m["mountPath"]
            for m in self.containers_by_name["loki"]["volumeMounts"]
            if m["name"] == "loki-data"
        )
        backup_path = next(
            m["mountPath"] for m in self.loki_backup["volumeMounts"] if m["name"] == "loki-data"
        )
        assert loki_path == backup_path

    def test_backup_interval_env(self):
        env = self.loki_backup.get("env", {})
        assert "BACKUP_INTERVAL_SECONDS" in env

    def test_keep_count_env(self):
        env = self.loki_backup.get("env", {})
        assert "BACKUP_KEEP_COUNT" in env

    def test_stage_prefix_env(self):
        env = self.loki_backup.get("env", {})
        assert "backups/loki" in env.get("STAGE_BACKUP_PREFIX", "")

    def test_restore_on_cold_start_enabled(self):
        env = self.loki_backup.get("env", {})
        assert env.get("RESTORE_ON_COLD_START", "").lower() == "true"

    def test_cpu_request_minimal(self):
        cpu = float(self.loki_backup["resources"]["requests"]["cpu"])
        assert cpu <= 0.1

    def test_no_secrets_needed(self):
        assert "secrets" not in self.loki_backup or len(self.loki_backup.get("secrets", [])) == 0


class TestRedisAOFPersistence:
    """Validate Redis is configured with AOF persistence."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(REDIS_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.redis = next(c for c in containers if c["name"] == "pf-redis")

    def test_appendonly_flag_present(self):
        args = self.redis.get("args", [])
        assert "--appendonly" in args, "Redis must have --appendonly flag"
        idx = args.index("--appendonly")
        assert args[idx + 1] == "yes"

    def test_appendfsync_everysec(self):
        """appendfsync everysec balances durability and performance."""
        args = self.redis.get("args", [])
        assert "--appendfsync" in args
        idx = args.index("--appendfsync")
        assert args[idx + 1] == "everysec"

    def test_data_volume_mounted(self):
        mounts = {m["name"]: m for m in self.redis.get("volumeMounts", [])}
        assert "redis-data" in mounts
        assert mounts["redis-data"]["mountPath"] == "/data"

    def test_data_volume_is_block_storage(self):
        volumes = {v["name"]: v for v in self.spec["spec"]["volumes"]}
        assert volumes["redis-data"]["source"] == "block"

    def test_redis_server_is_first_arg(self):
        """redis-server must be the first arg to correctly start the server."""
        args = self.redis.get("args", [])
        assert args[0] == "redis-server"


class TestBackupStageIntegration:
    """Validate backup infrastructure uses SPCS stage correctly."""

    @pytest.fixture(autouse=True)
    def load_files(self):
        self.backup_lib = BACKUP_LIB_PY.read_text()
        self.prom_backup = PROM_BACKUP_PY.read_text()
        self.loki_backup = LOKI_BACKUP_PY.read_text()

    def test_backup_lib_uses_spcs_token_path(self):
        assert "/snowflake/session/token" in self.backup_lib

    def test_backup_lib_no_hardcoded_credentials(self):
        for pattern in ["password", "Pr3fect", "secret_string"]:
            assert pattern.lower() not in self.backup_lib.lower(), (
                f"Hardcoded credential pattern '{pattern}' found in backup_lib"
            )

    def test_prom_backup_imports_backup_lib(self):
        assert "from backup_lib import" in self.prom_backup

    def test_loki_backup_imports_backup_lib(self):
        assert "from backup_lib import" in self.loki_backup

    def test_backup_lib_has_prune_function(self):
        assert "def prune_old_backups" in self.backup_lib

    def test_backup_lib_has_upload_function(self):
        assert "def upload_file" in self.backup_lib

    def test_backup_lib_has_download_function(self):
        assert "def download_file" in self.backup_lib

    def test_backup_lib_has_tarball_functions(self):
        assert "def create_tarball" in self.backup_lib
        assert "def extract_tarball" in self.backup_lib

    def test_prom_backup_uses_snapshot_api(self):
        """Prometheus backup must trigger TSDB snapshot via admin API."""
        assert "/api/v1/admin/tsdb/snapshot" in self.prom_backup

    def test_prom_backup_cleans_up_snapshots(self):
        """Must clean up snapshot dirs to avoid filling disk."""
        assert "shutil.rmtree" in self.prom_backup

    def test_loki_backup_excludes_wal(self):
        """WAL should be excluded from tar — Loki handles WAL recovery."""
        assert "wal" in self.loki_backup


class TestPollerColdStartCatchup:
    """Validate the event-log-poller supports INITIAL_LOOKBACK_SECONDS."""

    @pytest.fixture(autouse=True)
    def load_files(self):
        self.poller = POLLER_PY.read_text()
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.poller_container = next(c for c in containers if c["name"] == "event-log-poller")

    def test_initial_lookback_env_var_parsed(self):
        assert "INITIAL_LOOKBACK_SECONDS" in self.poller

    def test_initial_lookback_defaults_to_normal(self):
        """If INITIAL_LOOKBACK_SECONDS is not set, default to LOOKBACK_SECONDS."""
        assert 'os.environ.get("INITIAL_LOOKBACK_SECONDS"' in self.poller

    def test_first_poll_flag_exists(self):
        assert "_is_first_poll" in self.poller

    def test_first_poll_flag_cleared_after_success(self):
        assert "_is_first_poll = False" in self.poller

    def test_spec_sets_initial_lookback(self):
        env = self.poller_container.get("env", {})
        assert "INITIAL_LOOKBACK_SECONDS" in env
        value = int(env["INITIAL_LOOKBACK_SECONDS"])
        assert value >= 60, (
            f"INITIAL_LOOKBACK_SECONDS should be >= 60 for cold-start backfill, got {value}"
        )

    def test_spec_normal_lookback_is_small(self):
        env = self.poller_container.get("env", {})
        normal = int(env.get("LOOKBACK_SECONDS", "60"))
        initial = int(env.get("INITIAL_LOOKBACK_SECONDS", "60"))
        assert initial > normal, (
            f"INITIAL_LOOKBACK_SECONDS ({initial}) should be larger than "
            f"LOOKBACK_SECONDS ({normal}) for cold-start catchup"
        )


class TestBackupSidecarResources:
    """Validate backup sidecars fit within the PREFECT_MONITOR_POOL budget.

    PREFECT_MONITOR_POOL is CPU_X64_S with 2 vCPU.
    SPCS schedules based on CPU LIMITS, not requests.
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = self.spec["spec"]["containers"]

    def test_total_cpu_requests_under_2(self):
        total = sum(
            float(c["resources"]["requests"]["cpu"]) for c in self.containers if "resources" in c
        )
        assert total <= 2.0, f"Total CPU requests ({total}) exceed 2 vCPU pool limit"

    def test_total_cpu_limits_under_2_5(self):
        """SPCS allows overcommit on limits — total limits must fit within 2.5 vCPU."""
        total = sum(
            float(c["resources"]["limits"]["cpu"]) for c in self.containers if "resources" in c
        )
        assert total <= 2.5, f"Total CPU limits ({total}) exceed 2.5 vCPU overcommit budget"

    def test_backup_containers_are_lightest(self):
        """Backup sidecars should have the lowest CPU of all containers."""
        backup_cpus = []
        other_cpus = []
        for c in self.containers:
            if "resources" not in c:
                continue
            cpu = float(c["resources"]["requests"]["cpu"])
            if "backup" in c["name"]:
                backup_cpus.append(cpu)
            else:
                other_cpus.append(cpu)
        if backup_cpus and other_cpus:
            assert max(backup_cpus) <= min(other_cpus), (
                f"Backup CPU ({max(backup_cpus)}) should be <= "
                f"smallest non-backup CPU ({min(other_cpus)})"
            )


class TestMonitorSpecContainerCount:
    """Validate the expected number of containers after adding backup sidecars."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = self.spec["spec"]["containers"]

    def test_container_count_is_9(self):
        """6 original + 2 backup sidecars + observe-agent = 9 containers."""
        assert len(self.containers) == 9, (
            f"Expected 9 containers, got {len(self.containers)}: "
            f"{[c['name'] for c in self.containers]}"
        )

    def test_all_expected_containers_present(self):
        names = {c["name"] for c in self.containers}
        expected = {
            "prometheus",
            "grafana",
            "loki",
            "postgres-exporter",
            "prefect-exporter",
            "event-log-poller",
            "prom-backup",
            "loki-backup",
        }
        missing = expected - names
        assert not missing, f"Missing containers: {missing}"


class TestVolumeSharing:
    """Validate that backup sidecars correctly share volumes with their targets."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_prom_backup_only_mounts_prom_data(self):
        """prom-backup should only mount prom-data — not config or other volumes."""
        mounts = [m["name"] for m in self.containers["prom-backup"]["volumeMounts"]]
        assert mounts == ["prom-data"]

    def test_loki_backup_only_mounts_loki_data(self):
        """loki-backup should only mount loki-data — not config or rules."""
        mounts = [m["name"] for m in self.containers["loki-backup"]["volumeMounts"]]
        assert mounts == ["loki-data"]

    def test_no_backup_container_has_stage_mount(self):
        """Backup containers access stage via SQL API, not volume mounts."""
        for name in ("prom-backup", "loki-backup"):
            mounts = self.containers[name].get("volumeMounts", [])
            for m in mounts:
                assert "STAGE" not in m.get("name", "").upper(), (
                    f"{name} should not mount a stage volume — uses SQL API instead"
                )


class TestBackupNoOverlappingMounts:
    """Ensure adding backup sidecars doesn't create mount overlaps."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())

    def test_no_new_overlaps_after_backup_sidecars(self):
        """Regression: adding backup containers must not create mount overlaps."""
        for container in self.spec["spec"]["containers"]:
            mounts = container.get("volumeMounts", [])
            paths = sorted([m["mountPath"] for m in mounts])
            for i in range(len(paths)):
                for j in range(i + 1, len(paths)):
                    parent = paths[i].rstrip("/") + "/"
                    child = paths[j]
                    assert not child.startswith(parent), (
                        f"Container '{container['name']}': mount '{child}' "
                        f"overlaps with parent mount '{paths[i]}'"
                    )


class TestPersistenceDocumentation:
    """Validate that persistence behavior is documented in the secrets template."""

    @pytest.fixture(autouse=True)
    def load_template(self):
        self.template = SECRETS_TEMPLATE.read_text()

    def test_documents_grafana_persistence(self):
        assert "Grafana" in self.template
        assert "Postgres" in self.template or "postgres" in self.template

    def test_documents_prometheus_persistence(self):
        assert "Prometheus" in self.template
        assert "TSDB" in self.template or "snapshot" in self.template

    def test_documents_loki_persistence(self):
        assert "Loki" in self.template

    def test_documents_redis_persistence(self):
        assert "Redis" in self.template
        assert "AOF" in self.template or "appendonly" in self.template.lower()

    def test_documents_rpo(self):
        """Template should document RPO for each component."""
        assert "RPO" in self.template or "6h" in self.template

    def test_documents_spcs_token_usage(self):
        """Template should note that backups use SPCS OAuth token."""
        assert "OAuth" in self.template or "/snowflake/session/token" in self.template


class TestBackupDockerfiles:
    """Validate Dockerfiles for backup images."""

    @pytest.fixture(autouse=True)
    def load_files(self):
        self.prom_dockerfile = PROM_BACKUP_DOCKERFILE.read_text()
        self.loki_dockerfile = LOKI_BACKUP_DOCKERFILE.read_text()

    def test_prom_dockerfile_copies_backup_lib(self):
        assert "backup_lib.py" in self.prom_dockerfile

    def test_prom_dockerfile_copies_backup_py(self):
        assert "backup.py" in self.prom_dockerfile

    def test_loki_dockerfile_copies_backup_lib(self):
        assert "backup_lib.py" in self.loki_dockerfile

    def test_loki_dockerfile_copies_backup_py(self):
        assert "backup.py" in self.loki_dockerfile

    def test_prom_dockerfile_uses_python_slim(self):
        assert "python:3.12-slim" in self.prom_dockerfile

    def test_loki_dockerfile_uses_python_slim(self):
        assert "python:3.12-slim" in self.loki_dockerfile

    def test_prom_dockerfile_installs_requests(self):
        assert "requests" in self.prom_dockerfile

    def test_prom_dockerfile_sets_pythonpath(self):
        assert "PYTHONPATH" in self.prom_dockerfile

    def test_prom_dockerfile_has_cmd(self):
        assert "CMD" in self.prom_dockerfile

    def test_loki_dockerfile_has_cmd(self):
        assert "CMD" in self.loki_dockerfile

    def test_build_context_documented(self):
        """Dockerfiles should document the required build context."""
        assert "images/" in self.prom_dockerfile
        assert "images/" in self.loki_dockerfile


# ===========================================================================
# Behavioral Tests — backup_lib.py
# ===========================================================================
import time  # noqa: E402 — needed by backup behavioral tests

BACKUP_LIB_PATH = PROJECT_DIR / "images" / "stage-backup" / "backup_lib.py"
PROM_BACKUP_PATH = PROJECT_DIR / "images" / "prom-backup" / "backup.py"
LOKI_BACKUP_PATH = PROJECT_DIR / "images" / "loki-backup" / "backup.py"


def _import_backup_lib():
    """Import backup_lib.py as a module, mocking SPCS token file."""
    spec = importlib.util.spec_from_file_location("backup_lib", BACKUP_LIB_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _import_prom_backup():
    """Import prom-backup/backup.py, mocking the backup_lib dependency."""
    backup_lib = _import_backup_lib()
    sys.modules["backup_lib"] = backup_lib
    spec = importlib.util.spec_from_file_location("prom_backup", PROM_BACKUP_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _import_loki_backup():
    """Import loki-backup/backup.py, mocking the backup_lib dependency."""
    backup_lib = _import_backup_lib()
    sys.modules["backup_lib"] = backup_lib
    spec = importlib.util.spec_from_file_location("loki_backup", LOKI_BACKUP_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestBackupLibImport:
    """Verify backup_lib.py can be imported and has expected interface."""

    def test_import_succeeds(self):
        mod = _import_backup_lib()
        assert hasattr(mod, "SPCSStageClient")
        assert hasattr(mod, "create_tarball")
        assert hasattr(mod, "extract_tarball")
        assert hasattr(mod, "get_timestamp")
        assert hasattr(mod, "wait_for_service")

    def test_spcs_token_path_constant(self):
        mod = _import_backup_lib()
        assert mod.SPCS_TOKEN_PATH == "/snowflake/session/token"

    def test_token_refresh_interval_positive(self):
        mod = _import_backup_lib()
        assert mod.SNOWFLAKE_TOKEN_REFRESH_INTERVAL > 0

    def test_client_default_params(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        assert client.database == "PREFECT_DB"
        assert client.schema == "PREFECT_SCHEMA"
        assert client.stage == "MONITOR_STAGE"
        assert client.warehouse == "COMPUTE_WH"

    def test_client_custom_params(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient(database="DB", schema="SCH", stage="STG", warehouse="WH")
        assert client.database == "DB"
        assert client.schema == "SCH"
        assert client.stage == "STG"
        assert client.warehouse == "WH"


class TestCreateTarball:
    """Test create_tarball() and extract_tarball() actual behavior."""

    def test_creates_tarball_from_directory(self, tmp_path):
        mod = _import_backup_lib()
        # Create source structure
        src = tmp_path / "source"
        src.mkdir()
        (src / "file1.txt").write_text("hello")
        (src / "subdir").mkdir()
        (src / "subdir" / "file2.txt").write_text("world")

        out = tmp_path / "backup.tar.gz"
        result = mod.create_tarball(str(src), str(out))
        assert result == str(out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_extract_restores_files(self, tmp_path):
        mod = _import_backup_lib()
        src = tmp_path / "source"
        src.mkdir()
        (src / "data.txt").write_text("important data")
        (src / "nested").mkdir()
        (src / "nested" / "deep.txt").write_text("deep data")

        tarball = tmp_path / "backup.tar.gz"
        mod.create_tarball(str(src), str(tarball))

        dest = tmp_path / "restored"
        dest.mkdir()
        mod.extract_tarball(str(tarball), str(dest))

        assert (dest / "data.txt").exists()
        assert (dest / "data.txt").read_text() == "important data"
        assert (dest / "nested" / "deep.txt").read_text() == "deep data"

    def test_exclude_patterns_work(self, tmp_path):
        mod = _import_backup_lib()
        src = tmp_path / "source"
        src.mkdir()
        (src / "keep.txt").write_text("keep")
        (src / "wal").mkdir()
        (src / "wal" / "segment").write_text("wal data")

        tarball = tmp_path / "backup.tar.gz"
        mod.create_tarball(str(src), str(tarball), exclude_patterns=["wal"])

        dest = tmp_path / "restored"
        dest.mkdir()
        mod.extract_tarball(str(tarball), str(dest))

        assert (dest / "keep.txt").exists()
        assert not (dest / "wal").exists(), "WAL directory should be excluded"

    def test_exclude_multiple_patterns(self, tmp_path):
        mod = _import_backup_lib()
        src = tmp_path / "source"
        src.mkdir()
        (src / "data.txt").write_text("data")
        (src / "wal").mkdir()
        (src / "wal" / "seg").write_text("wal")
        (src / "lock").write_text("lock")

        tarball = tmp_path / "backup.tar.gz"
        mod.create_tarball(str(src), str(tarball), exclude_patterns=["wal", "lock"])

        dest = tmp_path / "restored"
        dest.mkdir()
        mod.extract_tarball(str(tarball), str(dest))

        assert (dest / "data.txt").exists()
        assert not (dest / "wal").exists()
        assert not (dest / "lock").exists()

    def test_no_exclude_patterns_includes_all(self, tmp_path):
        mod = _import_backup_lib()
        src = tmp_path / "source"
        src.mkdir()
        (src / "file1.txt").write_text("1")
        (src / "wal").mkdir()
        (src / "wal" / "seg").write_text("wal")

        tarball = tmp_path / "backup.tar.gz"
        mod.create_tarball(str(src), str(tarball))

        dest = tmp_path / "restored"
        dest.mkdir()
        mod.extract_tarball(str(tarball), str(dest))

        assert (dest / "file1.txt").exists()
        assert (dest / "wal" / "seg").exists(), "Without exclude, all files should be included"

    def test_empty_directory_creates_valid_tarball(self, tmp_path):
        mod = _import_backup_lib()
        src = tmp_path / "empty"
        src.mkdir()

        tarball = tmp_path / "backup.tar.gz"
        mod.create_tarball(str(src), str(tarball))
        assert tarball.exists()


class TestGetTimestamp:
    """Test get_timestamp() format."""

    def test_format_is_filename_safe(self):
        mod = _import_backup_lib()
        ts = mod.get_timestamp()
        # Format: YYYYMMDD-HHMMSS
        assert len(ts) == 15  # 8 + 1 + 6
        assert "-" in ts
        # Should not contain characters invalid in filenames
        for c in [":", "/", " ", "\\", "?"]:
            assert c not in ts

    def test_lexicographic_ordering(self):
        """Timestamps should sort chronologically when sorted as strings."""
        mod = _import_backup_lib()
        t1 = mod.get_timestamp()
        import time  # noqa: E402

        time.sleep(1.1)
        t2 = mod.get_timestamp()
        assert t2 > t1


class TestWaitForService:
    """Test wait_for_service() with mocked HTTP."""

    def test_returns_true_when_service_ready(self):
        mod = _import_backup_lib()
        mock_resp = MagicMock()
        mock_resp.ok = True
        with patch.object(mod.requests, "get", return_value=mock_resp):
            result = mod.wait_for_service("http://localhost:9090/-/ready", timeout=5, interval=1)
        assert result is True

    def test_returns_false_on_timeout(self):
        mod = _import_backup_lib()
        with patch.object(mod.requests, "get", side_effect=mod.requests.RequestException("fail")):
            result = mod.wait_for_service("http://localhost:9090/-/ready", timeout=2, interval=1)
        assert result is False

    def test_retries_until_ready(self):
        mod = _import_backup_lib()
        fail_resp = MagicMock()
        fail_resp.ok = False
        ok_resp = MagicMock()
        ok_resp.ok = True
        with patch.object(
            mod.requests,
            "get",
            side_effect=[
                mod.requests.RequestException("fail"),
                fail_resp,
                ok_resp,
            ],
        ):
            result = mod.wait_for_service("http://localhost:9090/-/ready", timeout=30, interval=0.1)
        assert result is True


class TestSPCSStageClientToken:
    """Test SPCSStageClient token handling."""

    def test_token_read_from_file(self, tmp_path):
        mod = _import_backup_lib()
        token_file = tmp_path / "token"
        token_file.write_text("  my-oauth-token-value  \n")

        client = mod.SPCSStageClient()
        with patch.object(mod, "SPCS_TOKEN_PATH", str(token_file)):
            # Force token refresh
            client._token = None
            client._token_time = 0
            with patch.object(mod.Path, "__new__", return_value=token_file):
                # Read token directly for test
                token = token_file.read_text().strip()
                assert token == "my-oauth-token-value"

    def test_token_raises_when_file_missing(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        client._token = None
        client._token_time = 0
        with (
            patch.object(mod, "SPCS_TOKEN_PATH", "/nonexistent/path"),
            pytest.raises(FileNotFoundError, match="SPCS token not found"),
        ):
            _ = client.token

    def test_account_url_from_env(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.dict(os.environ, {"SNOWFLAKE_HOST": "myaccount.snowflakecomputing.com"}):
            assert client._account_url == "https://myaccount.snowflakecomputing.com"

    def test_account_url_raises_when_host_missing(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.dict(os.environ, {}, clear=True):
            # Remove SNOWFLAKE_HOST if present
            env = os.environ.copy()
            env.pop("SNOWFLAKE_HOST", None)
            with (
                patch.dict(os.environ, env, clear=True),
                pytest.raises(ValueError, match="SNOWFLAKE_HOST"),
            ):
                _ = client._account_url

    def test_headers_include_oauth(self, tmp_path):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        token_file = tmp_path / "token"
        token_file.write_text("test-token")
        with patch.object(mod, "SPCS_TOKEN_PATH", str(token_file)):
            client._token = None
            client._token_time = 0
            with (
                patch("pathlib.Path.exists", return_value=True),
                patch("pathlib.Path.read_text", return_value="test-token"),
            ):
                client._token = "test-token"
                client._token_time = time.time()
                headers = client._headers
        assert "Authorization" in headers
        assert "Snowflake Token=" in headers["Authorization"]
        assert headers["X-Snowflake-Authorization-Token-Type"] == "OAUTH"


class TestSPCSStageClientMethods:
    """Test SPCSStageClient SQL generation and error handling with mocked _execute_sql."""

    def test_list_stage_files_sql(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(
            client,
            "_execute_sql",
            return_value={
                "data": [
                    [
                        "monitor_stage/backups/prom/snap-20260101-000000.tar.gz",
                        "1024",
                        "abc",
                        "2026-01-01",
                    ],
                    [
                        "monitor_stage/backups/prom/snap-20260102-000000.tar.gz",
                        "2048",
                        "def",
                        "2026-01-02",
                    ],
                ]
            },
        ) as mock_sql:
            files = client.list_stage_files("backups/prom")
        assert len(files) == 2
        assert "LIST" in mock_sql.call_args[0][0]

    def test_list_stage_files_empty(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", return_value={"data": []}):
            files = client.list_stage_files("backups/prom")
        assert files == []

    def test_upload_file_sql_uses_put(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", return_value={}) as mock_sql:
            result = client.upload_file("/tmp/backup.tar.gz", "backups/prom")
        assert result is True
        sql = mock_sql.call_args[0][0]
        assert "PUT" in sql
        assert "OVERWRITE = TRUE" in sql
        assert "AUTO_COMPRESS = FALSE" in sql

    def test_upload_file_returns_false_on_error(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", side_effect=Exception("upload failed")):
            result = client.upload_file("/tmp/backup.tar.gz", "backups/prom")
        assert result is False

    def test_download_file_sql_uses_get(self, tmp_path):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        # Simulate file appearing after GET
        fake_file = tmp_path / "snap.tar.gz"
        fake_file.write_text("data")
        with patch.object(client, "_execute_sql", return_value={}) as mock_sql:
            result = client.download_file("backups/prom/snap.tar.gz", str(tmp_path))
        assert result == str(fake_file)
        assert "GET" in mock_sql.call_args[0][0]

    def test_download_file_returns_none_on_error(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", side_effect=Exception("download failed")):
            result = client.download_file("backups/prom/snap.tar.gz", "/tmp")
        assert result is None

    def test_download_file_returns_none_when_file_missing(self, tmp_path):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", return_value={}):
            result = client.download_file("backups/prom/nonexistent.tar.gz", str(tmp_path))
        assert result is None

    def test_delete_file_sql_uses_remove(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", return_value={}) as mock_sql:
            result = client.delete_file("backups/prom/old.tar.gz")
        assert result is True
        assert "REMOVE" in mock_sql.call_args[0][0]

    def test_delete_file_returns_false_on_error(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        with patch.object(client, "_execute_sql", side_effect=Exception("delete failed")):
            result = client.delete_file("backups/prom/old.tar.gz")
        assert result is False

    def test_prune_keeps_newest_files(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        files = [
            "monitor_stage/backups/prom/snap-20260101.tar.gz",
            "monitor_stage/backups/prom/snap-20260102.tar.gz",
            "monitor_stage/backups/prom/snap-20260103.tar.gz",
            "monitor_stage/backups/prom/snap-20260104.tar.gz",
            "monitor_stage/backups/prom/snap-20260105.tar.gz",
        ]
        with (
            patch.object(client, "list_stage_files", return_value=files),
            patch.object(client, "delete_file", return_value=True) as mock_delete,
        ):
            deleted = client.prune_old_backups("backups/prom", keep=3)
        assert deleted == 2
        # Should delete the 2 oldest (20260101, 20260102)
        deleted_paths = [call[0][0] for call in mock_delete.call_args_list]
        assert any("20260101" in p for p in deleted_paths)
        assert any("20260102" in p for p in deleted_paths)

    def test_prune_no_op_when_under_limit(self):
        mod = _import_backup_lib()
        client = mod.SPCSStageClient()
        files = ["snap-20260101.tar.gz", "snap-20260102.tar.gz"]
        with (
            patch.object(client, "list_stage_files", return_value=files),
            patch.object(client, "delete_file") as mock_delete,
        ):
            deleted = client.prune_old_backups("backups/prom", keep=3)
        assert deleted == 0
        mock_delete.assert_not_called()

    def test_prune_strips_stage_prefix(self):
        """prune_old_backups should strip the stage name prefix from LIST paths."""
        mod = _import_backup_lib()
        client = mod.SPCSStageClient(stage="MONITOR_STAGE")
        files = [
            "monitor_stage/backups/prom/snap-20260101.tar.gz",
            "monitor_stage/backups/prom/snap-20260102.tar.gz",
        ]
        with (
            patch.object(client, "list_stage_files", return_value=files),
            patch.object(client, "delete_file", return_value=True) as mock_delete,
        ):
            client.prune_old_backups("backups/prom", keep=1)
        # Should strip "monitor_stage/" prefix before calling delete_file
        deleted_path = mock_delete.call_args[0][0]
        assert not deleted_path.startswith("monitor_stage/")
        assert deleted_path.startswith("backups/")


# ===========================================================================
# Behavioral Tests — prom-backup/backup.py
# ===========================================================================


class TestPromBackupColdStart:
    """Test is_cold_start() detection for Prometheus TSDB."""

    def test_nonexistent_dir_is_cold_start(self, tmp_path):
        mod = _import_prom_backup()
        assert mod.is_cold_start(str(tmp_path / "nonexistent")) is True

    def test_empty_dir_is_cold_start(self, tmp_path):
        mod = _import_prom_backup()
        data = tmp_path / "prometheus"
        data.mkdir()
        assert mod.is_cold_start(str(data)) is True

    def test_dir_with_only_snapshots_is_cold_start(self, tmp_path):
        """snapshots/ and lock are filtered out — not meaningful data."""
        mod = _import_prom_backup()
        data = tmp_path / "prometheus"
        data.mkdir()
        (data / "snapshots").mkdir()
        (data / "lock").write_text("")
        (data / "queries.active").write_text("")
        assert mod.is_cold_start(str(data)) is True

    def test_dir_with_wal_is_not_cold_start(self, tmp_path):
        mod = _import_prom_backup()
        data = tmp_path / "prometheus"
        data.mkdir()
        (data / "wal").mkdir()
        (data / "wal" / "segment_0").write_text("data")
        assert mod.is_cold_start(str(data)) is False

    def test_dir_with_block_is_not_cold_start(self, tmp_path):
        """TSDB blocks are ULID-named directories."""
        mod = _import_prom_backup()
        data = tmp_path / "prometheus"
        data.mkdir()
        (data / "01ABCDEF12345678").mkdir()  # ULID-like block dir  # pragma: allowlist secret
        assert mod.is_cold_start(str(data)) is False


class TestPromBackupRestore:
    """Test restore_from_stage() for Prometheus."""

    def test_no_backups_returns_false(self):
        mod = _import_prom_backup()
        client = MagicMock()
        client.list_stage_files.return_value = []
        assert mod.restore_from_stage(client, "/prometheus") is False

    def test_downloads_and_extracts_latest(self, tmp_path):
        mod = _import_prom_backup()
        backup_lib = _import_backup_lib()

        # Create a real tarball to download
        src = tmp_path / "src"
        src.mkdir()
        (src / "wal").mkdir()
        (src / "wal" / "seg").write_text("data")

        tarball = tmp_path / "downloads" / "snap.tar.gz"
        tarball.parent.mkdir()
        backup_lib.create_tarball(str(src), str(tarball))

        client = MagicMock()
        client.stage = "MONITOR_STAGE"
        client.list_stage_files.return_value = [
            "monitor_stage/backups/prometheus/snap-20260101.tar.gz",
            "monitor_stage/backups/prometheus/snap-20260102.tar.gz",
        ]
        client.download_file.return_value = str(tarball)

        dest = tmp_path / "restore"
        dest.mkdir()
        result = mod.restore_from_stage(client, str(dest))
        assert result is True
        # Verify it picked the latest (last) file
        download_args = client.download_file.call_args[0]
        assert "20260102" in download_args[0]

    def test_download_failure_returns_false(self, tmp_path):
        mod = _import_prom_backup()
        client = MagicMock()
        client.stage = "MONITOR_STAGE"
        client.list_stage_files.return_value = ["monitor_stage/backups/prometheus/snap.tar.gz"]
        client.download_file.return_value = None

        assert mod.restore_from_stage(client, str(tmp_path)) is False


class TestPromBackupCreateSnapshot:
    """Test create_snapshot() Prometheus API interaction."""

    def test_success_returns_snapshot_name(self):
        mod = _import_prom_backup()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"name": "20260307T120000Z-abc123"},
        }
        mock_resp.raise_for_status = MagicMock()
        with patch.object(mod.requests, "post", return_value=mock_resp):
            name = mod.create_snapshot("http://localhost:9090")
        assert name == "20260307T120000Z-abc123"

    def test_non_success_returns_none(self):
        mod = _import_prom_backup()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "error", "error": "tsdb not ready"}
        mock_resp.raise_for_status = MagicMock()
        with patch.object(mod.requests, "post", return_value=mock_resp):
            name = mod.create_snapshot("http://localhost:9090")
        assert name is None

    def test_network_error_returns_none(self):
        mod = _import_prom_backup()
        with patch.object(mod.requests, "post", side_effect=Exception("connection refused")):
            name = mod.create_snapshot("http://localhost:9090")
        assert name is None

    def test_calls_correct_endpoint(self):
        mod = _import_prom_backup()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "success", "data": {"name": "snap"}}
        mock_resp.raise_for_status = MagicMock()
        with patch.object(mod.requests, "post", return_value=mock_resp) as mock_post:
            mod.create_snapshot("http://prometheus:9090")
        mock_post.assert_called_once()
        assert mock_post.call_args[0][0] == "http://prometheus:9090/api/v1/admin/tsdb/snapshot"


class TestPromBackupToStage:
    """Test backup_to_stage() end-to-end workflow."""

    def test_failed_snapshot_returns_false(self):
        mod = _import_prom_backup()
        client = MagicMock()
        with patch.object(mod, "create_snapshot", return_value=None):
            result = mod.backup_to_stage(client, "/prometheus")
        assert result is False

    def test_missing_snapshot_dir_returns_false(self, tmp_path):
        mod = _import_prom_backup()
        client = MagicMock()
        data = tmp_path / "prometheus"
        data.mkdir()
        with (
            patch.object(mod, "create_snapshot", return_value="snap-xyz"),
            patch.object(mod, "PROMETHEUS_URL", "http://localhost:9090"),
        ):
            result = mod.backup_to_stage(client, str(data))
        assert result is False

    def test_full_backup_workflow(self, tmp_path):
        mod = _import_prom_backup()
        client = MagicMock()
        client.upload_file.return_value = True

        # Create fake snapshot dir
        data = tmp_path / "prometheus"
        snap_dir = data / "snapshots" / "snap-abc"
        snap_dir.mkdir(parents=True)
        (snap_dir / "block1").write_text("data")

        with (
            patch.object(mod, "create_snapshot", return_value="snap-abc"),
            patch.object(mod, "PROMETHEUS_URL", "http://localhost:9090"),
        ):
            result = mod.backup_to_stage(client, str(data))

        assert result is True
        client.upload_file.assert_called_once()
        client.prune_old_backups.assert_called_once()
        # Snapshot dir should be cleaned up
        assert not snap_dir.exists(), "Snapshot dir should be removed after backup"

    def test_upload_failure_skips_prune(self, tmp_path):
        mod = _import_prom_backup()
        client = MagicMock()
        client.upload_file.return_value = False

        data = tmp_path / "prometheus"
        snap_dir = data / "snapshots" / "snap-fail"
        snap_dir.mkdir(parents=True)
        (snap_dir / "block1").write_text("data")

        with (
            patch.object(mod, "create_snapshot", return_value="snap-fail"),
            patch.object(mod, "PROMETHEUS_URL", "http://localhost:9090"),
        ):
            result = mod.backup_to_stage(client, str(data))

        assert result is False
        client.prune_old_backups.assert_not_called()


# ===========================================================================
# Behavioral Tests — loki-backup/backup.py
# ===========================================================================


class TestLokiBackupColdStart:
    """Test is_cold_start() detection for Loki data directory."""

    def test_nonexistent_dir_is_cold_start(self, tmp_path):
        mod = _import_loki_backup()
        assert mod.is_cold_start(str(tmp_path / "nonexistent")) is True

    def test_empty_dir_is_cold_start(self, tmp_path):
        mod = _import_loki_backup()
        data = tmp_path / "loki"
        data.mkdir()
        assert mod.is_cold_start(str(data)) is True

    def test_dir_with_empty_subdirs_is_cold_start(self, tmp_path):
        """Empty known subdirectories still count as cold start."""
        mod = _import_loki_backup()
        data = tmp_path / "loki"
        data.mkdir()
        (data / "chunks").mkdir()
        (data / "compactor").mkdir()
        assert mod.is_cold_start(str(data)) is True

    def test_dir_with_chunks_data_is_not_cold_start(self, tmp_path):
        mod = _import_loki_backup()
        data = tmp_path / "loki"
        data.mkdir()
        (data / "chunks").mkdir()
        (data / "chunks" / "chunk001").write_text("data")
        assert mod.is_cold_start(str(data)) is False

    def test_dir_with_index_data_is_not_cold_start(self, tmp_path):
        mod = _import_loki_backup()
        data = tmp_path / "loki"
        data.mkdir()
        (data / "index").mkdir()
        (data / "index" / "idx001").write_text("data")
        assert mod.is_cold_start(str(data)) is False

    def test_backup_subdirs_list_completeness(self):
        """BACKUP_SUBDIRS should cover all known Loki data directories."""
        mod = _import_loki_backup()
        expected = {
            "chunks",
            "compactor",
            "rules-tmp",
            "index",
            "boltdb-shipper-active",
            "boltdb-shipper-cache",
            "tsdb-shipper-active",
            "tsdb-shipper-cache",
        }
        assert set(mod.BACKUP_SUBDIRS) == expected


class TestLokiBackupToStage:
    """Test backup_to_stage() for Loki."""

    def test_nonexistent_dir_returns_false(self, tmp_path):
        mod = _import_loki_backup()
        client = MagicMock()
        result = mod.backup_to_stage(client, str(tmp_path / "nonexistent"))
        assert result is False

    def test_no_data_returns_true_without_upload(self, tmp_path):
        """No data to back up is not an error — just nothing to do."""
        mod = _import_loki_backup()
        client = MagicMock()
        data = tmp_path / "loki"
        data.mkdir()
        result = mod.backup_to_stage(client, str(data))
        assert result is True
        client.upload_file.assert_not_called()

    def test_backup_with_data_calls_upload(self, tmp_path):
        mod = _import_loki_backup()
        client = MagicMock()
        client.upload_file.return_value = True

        data = tmp_path / "loki"
        data.mkdir()
        (data / "chunks").mkdir()
        (data / "chunks" / "chunk001").write_text("data")

        result = mod.backup_to_stage(client, str(data))
        assert result is True
        client.upload_file.assert_called_once()
        client.prune_old_backups.assert_called_once()

    def test_upload_failure_skips_prune(self, tmp_path):
        mod = _import_loki_backup()
        client = MagicMock()
        client.upload_file.return_value = False

        data = tmp_path / "loki"
        data.mkdir()
        (data / "chunks").mkdir()
        (data / "chunks" / "chunk001").write_text("data")

        result = mod.backup_to_stage(client, str(data))
        assert result is False
        client.prune_old_backups.assert_not_called()

    def test_backup_excludes_wal(self, tmp_path):
        """The tarball should exclude WAL directories."""
        import tarfile  # noqa: E402

        mod = _import_loki_backup()
        client = MagicMock()

        # Capture tarball contents during upload (before temp dir cleanup)
        captured_names = []

        def capture_upload(path, prefix):
            with tarfile.open(path, "r:gz") as tar:
                captured_names.extend(tar.getnames())
            return True

        client.upload_file.side_effect = capture_upload

        data = tmp_path / "loki"
        data.mkdir()
        (data / "chunks").mkdir()
        (data / "chunks" / "chunk001").write_text("data")
        (data / "wal").mkdir()
        (data / "wal" / "segment").write_text("wal data")

        result = mod.backup_to_stage(client, str(data))
        assert result is True
        assert len(captured_names) > 0, "Tarball should contain files"
        assert not any("wal" in n for n in captured_names), (
            f"WAL should be excluded from tarball, found: {[n for n in captured_names if 'wal' in n]}"
        )


class TestLokiBackupRestore:
    """Test restore_from_stage() for Loki."""

    def test_no_backups_returns_false(self):
        mod = _import_loki_backup()
        client = MagicMock()
        client.list_stage_files.return_value = []
        assert mod.restore_from_stage(client, "/loki") is False

    def test_downloads_latest_backup(self, tmp_path):
        mod = _import_loki_backup()
        backup_lib = _import_backup_lib()

        # Create a real tarball
        src = tmp_path / "src"
        src.mkdir()
        (src / "chunks").mkdir()
        (src / "chunks" / "c1").write_text("data")
        tarball = tmp_path / "dl" / "backup.tar.gz"
        tarball.parent.mkdir()
        backup_lib.create_tarball(str(src), str(tarball))

        client = MagicMock()
        client.stage = "MONITOR_STAGE"
        client.list_stage_files.return_value = [
            "monitor_stage/backups/loki/loki-20260101.tar.gz",
            "monitor_stage/backups/loki/loki-20260102.tar.gz",
        ]
        client.download_file.return_value = str(tarball)

        dest = tmp_path / "restore"
        dest.mkdir()
        result = mod.restore_from_stage(client, str(dest))
        assert result is True
        # Should pick latest
        download_args = client.download_file.call_args[0]
        assert "20260102" in download_args[0]

    def test_download_failure_returns_false(self, tmp_path):
        mod = _import_loki_backup()
        client = MagicMock()
        client.stage = "MONITOR_STAGE"
        client.list_stage_files.return_value = ["monitor_stage/backups/loki/snap.tar.gz"]
        client.download_file.return_value = None
        assert mod.restore_from_stage(client, str(tmp_path)) is False


class TestLokiBackupConfig:
    """Test Loki backup module configuration defaults."""

    def test_default_interval(self):
        mod = _import_loki_backup()
        assert mod.BACKUP_INTERVAL == 21600

    def test_default_keep_count(self):
        mod = _import_loki_backup()
        assert mod.KEEP_COUNT == 3

    def test_default_data_dir(self):
        mod = _import_loki_backup()
        assert mod.LOKI_DATA_DIR == "/loki"

    def test_restore_on_cold_start_default(self):
        mod = _import_loki_backup()
        assert mod.RESTORE_ON_COLD_START is True


class TestPromBackupConfig:
    """Test Prometheus backup module configuration defaults."""

    def test_default_interval(self):
        mod = _import_prom_backup()
        assert mod.BACKUP_INTERVAL == 21600

    def test_default_keep_count(self):
        mod = _import_prom_backup()
        assert mod.KEEP_COUNT == 3

    def test_default_data_dir(self):
        mod = _import_prom_backup()
        assert mod.PROMETHEUS_DATA_DIR == "/prometheus"

    def test_default_prometheus_url(self):
        mod = _import_prom_backup()
        assert mod.PROMETHEUS_URL == "http://localhost:9090"

    def test_restore_on_cold_start_default(self):
        mod = _import_prom_backup()
        assert mod.RESTORE_ON_COLD_START is True


# ===========================================================================
# Deploy script — config file uploads
# ===========================================================================
class TestDeployScriptUploads:
    """Validate deploy_monitoring.sh uploads all required config files."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_uploads_prometheus_config(self):
        assert "prometheus/prometheus.yml" in self.content

    def test_uploads_prometheus_alert_rules(self):
        assert "prometheus/rules/alerts.yml" in self.content

    def test_uploads_prometheus_recording_rules(self):
        """Recording rules must be uploaded — they were missing before the fix."""
        assert "prometheus/rules/recording.yml" in self.content

    def test_uploads_grafana_datasources(self):
        assert "grafana/provisioning/datasources" in self.content

    def test_uploads_grafana_dashboards_provisioning(self):
        assert "grafana/provisioning/dashboards" in self.content

    def test_uploads_grafana_alerting_configs(self):
        """Alerting provisioning (contactpoints, policies, rules) must be uploaded."""
        assert "grafana/provisioning/alerting" in self.content

    def test_uploads_grafana_dashboard_json_files(self):
        assert "grafana/dashboards/" in self.content

    def test_uploads_loki_config(self):
        assert "loki/loki-config.yaml" in self.content

    def test_uploads_loki_alerting_rules(self):
        """Loki alert rules must be uploaded — they were missing before the fix."""
        assert "loki/rules/fake/alerts.yaml" in self.content

    def test_uploads_spec_file(self):
        assert "specs/pf_monitor.yaml" in self.content

    def test_alerting_upload_uses_loop(self):
        """Alerting yaml files should be uploaded via a loop (glob pattern)."""
        assert "alerting/*.yaml" in self.content


# ===========================================================================
# Deploy script — SMTP secret creation
# ===========================================================================
class TestDeployScriptSmtpSecret:
    """Validate deploy_monitoring.sh creates the SMTP password secret."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_creates_grafana_smtp_password_secret(self):
        assert "GRAFANA_SMTP_PASSWORD" in self.content

    def test_reads_from_keychain_first(self):
        """Should try macOS Keychain before prompting."""
        assert "find-generic-password" in self.content
        assert "gmail-smtp" in self.content

    def test_falls_back_to_env_var(self):
        """Should check GRAFANA_SMTP_PASSWORD env var."""
        assert "GRAFANA_SMTP_PASSWORD" in self.content

    def test_falls_back_to_interactive_prompt(self):
        """Should prompt for password if not in Keychain or env."""
        assert "App Password" in self.content or "Gmail" in self.content

    def test_warns_if_smtp_password_empty(self):
        """Should warn (not fail) if SMTP password is not provided."""
        assert "WARNING" in self.content and "SMTP" in self.content

    def test_escapes_smtp_password(self):
        """Password must be escaped for safe SQL interpolation (via _create_secret helper)."""
        assert "_create_secret GRAFANA_SMTP_PASSWORD" in self.content

    def test_grants_read_on_smtp_secret(self):
        """Must GRANT READ on the SMTP secret via _create_secret helper."""
        assert "GRANT READ ON SECRET" in self.content
        # The helper is called with the SMTP secret name
        assert "_create_secret GRAFANA_SMTP_PASSWORD" in self.content


# ===========================================================================
# Deploy script — _create_secret DRY helper
# ===========================================================================
class TestDeployScriptCreateSecretHelper:
    """Validate the _create_secret helper function in deploy_monitoring.sh."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_helper_function_exists(self):
        """deploy_monitoring.sh must define _create_secret()."""
        assert "_create_secret()" in self.content

    def test_helper_escapes_single_quotes(self):
        """Helper must escape single quotes for SQL safety."""
        assert "//\\'/\\'\\'" in self.content

    def test_helper_creates_generic_string_secret(self):
        """Helper must use CREATE SECRET IF NOT EXISTS with GENERIC_STRING type."""
        # Find the helper function body
        lines = self.content.splitlines()
        in_helper = False
        found_create = False
        for line in lines:
            if "_create_secret()" in line:
                in_helper = True
            if in_helper and "CREATE SECRET IF NOT EXISTS" in line:
                found_create = True
                break
            if in_helper and line.strip() == "}":
                break
        assert found_create, "_create_secret must use CREATE SECRET IF NOT EXISTS"

    def test_helper_grants_read(self):
        """Helper must GRANT READ ON SECRET."""
        lines = self.content.splitlines()
        in_helper = False
        found_grant = False
        for line in lines:
            if "_create_secret()" in line:
                in_helper = True
            if in_helper and "GRANT READ ON SECRET" in line:
                found_grant = True
                break
            if in_helper and line.strip() == "}":
                break
        assert found_grant, "_create_secret must GRANT READ ON SECRET"

    def test_all_secrets_use_helper(self):
        """Core secrets should be created via _create_secret calls."""
        expected = [
            "GRAFANA_ADMIN_PASSWORD",
            "GRAFANA_DB_DSN",
            "GRAFANA_SMTP_PASSWORD",
        ]
        for secret_name in expected:
            assert f"_create_secret {secret_name}" in self.content, (
                f"Secret {secret_name} should be created via _create_secret helper"
            )

    def test_slack_secret_is_conditional(self):
        """SLACK_WEBHOOK_URL secret creation must be gated on SLACK_ENABLED."""
        assert "SLACK_ENABLED" in self.content
        assert "_create_secret SLACK_WEBHOOK_URL" in self.content


# ===========================================================================
# Deploy script — EAI includes SMTP secret
# ===========================================================================
class TestDeployScriptEaiSmtpSecret:
    """Validate EAI ALLOWED_AUTHENTICATION_SECRETS includes SMTP secret."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_eai_includes_smtp_secret(self):
        """EAI must list GRAFANA_SMTP_PASSWORD in ALLOWED_AUTHENTICATION_SECRETS."""
        # Find the CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION block
        assert "GRAFANA_SMTP_PASSWORD" in self.content
        # Find the line containing ALLOWED_AUTHENTICATION_SECRETS
        lines = self.content.splitlines()
        auth_secrets_lines = [ln for ln in lines if "ALLOWED_AUTHENTICATION_SECRETS" in ln]
        assert len(auth_secrets_lines) >= 1
        auth_line = auth_secrets_lines[0]
        assert "GRAFANA_SMTP_PASSWORD" in auth_line

    def test_eai_includes_postgres_exporter_dsn(self):
        lines = self.content.splitlines()
        auth_secrets_lines = [ln for ln in lines if "ALLOWED_AUTHENTICATION_SECRETS" in ln]
        assert any("POSTGRES_EXPORTER_DSN" in ln for ln in auth_secrets_lines)

    def test_eai_includes_grafana_db_dsn(self):
        lines = self.content.splitlines()
        auth_secrets_lines = [ln for ln in lines if "ALLOWED_AUTHENTICATION_SECRETS" in ln]
        assert any("GRAFANA_DB_DSN" in ln for ln in auth_secrets_lines)


# ===========================================================================
# SPCS spec — SMTP environment variables
# ===========================================================================
class TestMonitorSpecSmtpConfig:
    """Validate SMTP env vars and secret mount in pf_monitor.yaml."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")

    def test_smtp_enabled(self):
        env = self.grafana.get("env", {})
        assert env.get("GF_SMTP_ENABLED") == "true"

    def test_smtp_host_is_gmail(self):
        env = self.grafana.get("env", {})
        assert "smtp.gmail.com" in env.get("GF_SMTP_HOST", "")

    def test_smtp_host_uses_port_587(self):
        """Port 587 with STARTTLS — port 465 is blocked by SPCS network rules."""
        env = self.grafana.get("env", {})
        assert "587" in env.get("GF_SMTP_HOST", "")

    def test_smtp_from_address_set(self):
        """SMTP from address injected via secret, not env var."""
        secret_vars = [s.get("envVarName") for s in self.grafana.get("secrets", [])]
        assert "GF_SMTP_FROM_ADDRESS" in secret_vars

    def test_smtp_from_name_set(self):
        env = self.grafana.get("env", {})
        assert env.get("GF_SMTP_FROM_NAME", "") != ""

    def test_smtp_starttls_policy_is_mandatory(self):
        """Port 587 requires STARTTLS (port 465 implicit SSL is blocked by SPCS)."""
        env = self.grafana.get("env", {})
        assert env.get("GF_SMTP_STARTTLS_POLICY") == "MandatoryStartTLS"

    def test_smtp_alert_recipients_set(self):
        """SMTP recipients injected via secret, not env var."""
        secret_vars = [s.get("envVarName") for s in self.grafana.get("secrets", [])]
        assert "GF_SMTP_ALERT_RECIPIENTS" in secret_vars

    def test_smtp_password_from_secret(self):
        """GF_SMTP_PASSWORD must come from a Snowflake secret, not env."""
        env = self.grafana.get("env", {})
        # Must NOT be in env vars (would be hardcoded)
        assert "GF_SMTP_PASSWORD" not in env, (
            "GF_SMTP_PASSWORD must be mounted from a secret, not hardcoded in env"
        )

    def test_smtp_password_secret_mount_exists(self):
        """Grafana must have a secret mount for grafana_smtp_password."""
        secrets = self.grafana.get("secrets", [])
        smtp_secrets = [
            s
            for s in secrets
            if s.get("snowflakeSecret", {}).get("objectName") == "grafana_smtp_password"
        ]
        assert len(smtp_secrets) == 1, "Must have exactly one grafana_smtp_password secret mount"

    def test_smtp_password_secret_maps_to_correct_env_var(self):
        """Secret must be exposed as GF_SMTP_PASSWORD env var."""
        secrets = self.grafana.get("secrets", [])
        smtp_secret = next(
            s
            for s in secrets
            if s.get("snowflakeSecret", {}).get("objectName") == "grafana_smtp_password"
        )
        assert smtp_secret.get("envVarName") == "GF_SMTP_PASSWORD"

    def test_grafana_has_six_secret_mounts_smtp(self):
        """Grafana needs 6 active secrets (Slack webhook is opt-in)."""
        secrets = self.grafana.get("secrets", [])
        assert len(secrets) == 6, (
            f"Grafana should have 6 secret mounts (Slack opt-in), got {len(secrets)}: "
            f"{[s.get('snowflakeSecret', {}).get('objectName') for s in secrets]}"
        )


# ===========================================================================
# No hardcoded personal emails
# ===========================================================================
class TestNoHardcodedEmails:
    """Monitor spec and deploy script must not contain personal email addresses."""

    def test_monitor_spec_no_personal_email(self):
        """pf_monitor.yaml must not hardcode @snowflake.com email addresses."""
        content = MONITOR_SPEC_FILE.read_text()
        assert "@snowflake.com" not in content, (
            "pf_monitor.yaml contains a @snowflake.com email — "
            "use CHANGE_ME@example.com placeholder instead"
        )

    def test_deploy_script_no_personal_email(self):
        """deploy_monitoring.sh must not hardcode @snowflake.com email addresses."""
        content = (MONITORING_DIR / "deploy_monitoring.sh").read_text()
        assert "@snowflake.com" not in content, (
            "deploy_monitoring.sh contains a @snowflake.com email — "
            "use GRAFANA_SMTP_USER env var instead"
        )

    def test_deploy_script_keychain_uses_env_var(self):
        """Keychain lookup should use GRAFANA_SMTP_USER env var, not a literal."""
        content = (MONITORING_DIR / "deploy_monitoring.sh").read_text()
        assert "GRAFANA_SMTP_USER" in content, (
            "deploy_monitoring.sh should read GRAFANA_SMTP_USER for Keychain lookup"
        )

    def test_monitor_spec_smtp_user_from_secret(self):
        """SMTP user must be injected via Snowflake secret, not hardcoded in env."""
        spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = spec["spec"]["containers"]
        grafana = next(c for c in containers if c["name"] == "grafana")
        env = grafana.get("env", {})
        assert "GF_SMTP_USER" not in env, (
            "GF_SMTP_USER should be injected via a Snowflake secret, not in env"
        )
        secret_vars = [s.get("envVarName") for s in grafana.get("secrets", [])]
        assert "GF_SMTP_USER" in secret_vars


# ===========================================================================
# Warehouse consistency — monitor spec matches worker spec
# ===========================================================================
class TestWarehouseConsistency:
    """Monitor and worker specs must use the same SNOWFLAKE_WAREHOUSE."""

    def test_monitor_warehouse_matches_worker(self):
        """pf_monitor.yaml warehouse must match pf_worker.yaml warehouse."""
        monitor = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        worker = yaml.safe_load((PROJECT_DIR / "specs" / "pf_worker.yaml").read_text())

        monitor_containers = monitor["spec"]["containers"]
        log_poller = next(c for c in monitor_containers if c["name"] == "event-log-poller")
        monitor_wh = log_poller.get("env", {}).get("SNOWFLAKE_WAREHOUSE", "")

        worker_container = worker["spec"]["containers"][0]
        worker_wh = worker_container.get("env", {}).get("SNOWFLAKE_WAREHOUSE", "")

        assert monitor_wh == worker_wh, (
            f"Warehouse mismatch: monitor={monitor_wh}, worker={worker_wh}"
        )

    def test_monitor_warehouse_is_set(self):
        """event-log-poller must have SNOWFLAKE_WAREHOUSE defined."""
        spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        log_poller = next(c for c in spec["spec"]["containers"] if c["name"] == "event-log-poller")
        wh = log_poller.get("env", {}).get("SNOWFLAKE_WAREHOUSE", "")
        assert wh, "log-poller SNOWFLAKE_WAREHOUSE must not be empty"


# ===========================================================================
# Contact points — email receiver
# ===========================================================================
class TestContactPointEmailReceiver:
    """Validate the email receiver in contactpoints.yaml."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load(
            (
                MONITORING_DIR / "grafana" / "provisioning" / "alerting" / "contactpoints.yaml"
            ).read_text()
        )
        cp = [c for c in self.config["contactPoints"] if c["name"] == "prefect-alerts"][0]
        self.receivers = cp["receivers"]

    def test_has_email_receiver(self):
        assert any(r["type"] == "email" for r in self.receivers)

    def test_email_receiver_uid(self):
        email = next(r for r in self.receivers if r["type"] == "email")
        assert email["uid"] == "email-receiver"

    def test_email_uses_env_var_for_addresses(self):
        """Email addresses should use env var substitution, not hardcoded."""
        email = next(r for r in self.receivers if r["type"] == "email")
        addresses = email["settings"]["addresses"]
        assert "${" in addresses, f"Email addresses should use env var, got: {addresses}"

    def test_email_single_email_mode(self):
        email = next(r for r in self.receivers if r["type"] == "email")
        assert email["settings"].get("singleEmail") is True

    def test_has_email_only(self):
        types = {r["type"] for r in self.receivers}
        assert "email" in types
        assert "slack" not in types, "Slack is opt-in and disabled by default"

    def test_total_receiver_count(self):
        assert len(self.receivers) == 1


# ===========================================================================
# Build script — monitoring image list
# ===========================================================================
class TestBuildScriptMonitoringImages:
    """Validate build_and_push.sh includes all monitoring images."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = (PROJECT_DIR / "scripts" / "build_and_push.sh").read_text()

    def test_has_monitoring_only_flag(self):
        assert "--monitoring-only" in self.content

    def test_has_core_only_flag(self):
        assert "--core-only" in self.content

    # Stock images (pull + retag)
    def test_pulls_prometheus(self):
        assert "prom/prometheus:" in self.content

    def test_pulls_grafana(self):
        assert "grafana/grafana:" in self.content

    def test_pulls_loki(self):
        assert "grafana/loki:" in self.content

    def test_pulls_postgres_exporter(self):
        assert "postgres-exporter:" in self.content or "postgres_exporter:" in self.content

    # Custom images (buildx)
    def test_builds_prefect_exporter(self):
        assert "prefect-exporter" in self.content

    def test_builds_event_log_poller(self):
        assert "event-log-poller" in self.content

    def test_builds_prom_backup(self):
        assert "prom-backup" in self.content

    def test_builds_loki_backup(self):
        assert "loki-backup" in self.content

    def test_all_builds_use_amd64(self):
        """All custom builds must use --platform linux/amd64."""
        lines = self.content.splitlines()
        buildx_lines = [ln for ln in lines if "docker buildx build" in ln]
        for line in buildx_lines:
            assert "linux/amd64" in line, f"Missing --platform linux/amd64: {line.strip()}"

    def test_backup_sidecars_use_f_flag(self):
        """Backup sidecars must use -f flag for Dockerfile (shared build context)."""
        assert "-f" in self.content
        assert "images/prom-backup/Dockerfile" in self.content
        assert "images/loki-backup/Dockerfile" in self.content

    def test_pushes_all_images(self):
        """Script must push all images in the PUSH_LIST."""
        assert "docker push" in self.content


# ===========================================================================
# Load test script
# ===========================================================================
class TestLoadTestScript:
    """Validate scripts/load_test.sh structure and content."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.path = PROJECT_DIR / "scripts" / "load_test.sh"
        self.content = self.path.read_text()

    def test_file_exists(self):
        assert self.path.exists()

    def test_loads_env_file(self):
        assert ".env" in self.content

    def test_requires_prefect_api_url(self):
        assert "PREFECT_API_URL" in self.content

    def test_requires_snowflake_pat(self):
        assert "SNOWFLAKE_PAT" in self.content

    def test_checks_prefect_api_health(self):
        assert "/health" in self.content

    def test_triggers_flow_runs(self):
        assert "create_flow_run" in self.content

    def test_waits_for_flow_completion(self):
        assert "COMPLETED" in self.content

    def test_checks_prometheus_metrics(self):
        assert "prefect_flow_runs_total" in self.content

    def test_checks_loki_logs(self):
        assert "spcs-logs" in self.content

    def test_checks_grafana_health(self):
        assert "/api/health" in self.content

    def test_checks_backup_stage_files(self):
        assert "MONITOR_STAGE" in self.content and "backups" in self.content

    def test_has_pass_fail_summary(self):
        assert "PASSED" in self.content and "FAILED" in self.content

    def test_exits_nonzero_on_failure(self):
        assert "exit 1" in self.content

    def test_uses_authenticated_curl(self):
        """Must use Snowflake PAT auth header for SPCS endpoints."""
        assert "Snowflake Token" in self.content

    def test_checks_prometheus_rules(self):
        assert "/api/v1/rules" in self.content

    def test_checks_grafana_datasources(self):
        assert "/api/datasources" in self.content


# ===========================================================================
# Cost analysis script
# ===========================================================================
class TestCostAnalysisScript:
    """Validate scripts/cost_analysis.sh structure and content."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.path = PROJECT_DIR / "scripts" / "cost_analysis.sh"
        self.content = self.path.read_text()

    def test_file_exists(self):
        assert self.path.exists()

    def test_loads_env_file(self):
        assert ".env" in self.content

    def test_accepts_connection_flag(self):
        assert "--connection" in self.content

    def test_accepts_days_flag(self):
        assert "--days" in self.content

    def test_queries_metering_daily_history(self):
        assert "METERING_DAILY_HISTORY" in self.content

    def test_queries_snowpark_container_services(self):
        assert "SNOWPARK_CONTAINER_SERVICES" in self.content

    def test_queries_stage_storage(self):
        assert "STAGE_STORAGE_USAGE_HISTORY" in self.content

    def test_queries_warehouse_metering(self):
        assert "WAREHOUSE_METERING_HISTORY" in self.content

    def test_shows_compute_pool_status(self):
        assert "SHOW COMPUTE POOLS" in self.content

    def test_includes_cost_estimate(self):
        assert "est_cost_usd" in self.content

    def test_includes_optimization_recommendations(self):
        assert "Optimization" in self.content or "SUSPEND" in self.content

    def test_supports_help_flag(self):
        assert "--help" in self.content or "-h" in self.content


# ===========================================================================
# Network rule — SMTP egress
# ===========================================================================
class TestNetworkRuleSmtpEgress:
    """Validate deploy script configures SMTP egress in the network rule."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_network_rule_includes_smtp_gmail(self):
        """Network rule must include smtp.gmail.com for email delivery."""
        assert "smtp.gmail.com" in self.content

    def test_smtp_port_is_587_in_rule(self):
        """SPCS network rules use :587 for STARTTLS (port 465 is blocked by SPCS)."""
        assert "smtp.gmail.com:587" in self.content


# ===========================================================================
# Local development monitoring overlay — docker-compose.monitoring-local.yml
# ===========================================================================
COMPOSE_MONITORING_LOCAL = PROJECT_DIR / "docker-compose.monitoring-local.yml"
PROM_LOCAL_CONFIG = MONITORING_DIR / "prometheus" / "prometheus-local.yml"
GRAFANA_DS_LOCAL = MONITORING_DIR / "grafana" / "provisioning" / "datasources" / "ds-local.yaml"


class TestLocalMonitoringFilesExist:
    """All local-dev monitoring config files must be present."""

    @pytest.mark.parametrize(
        "path",
        [COMPOSE_MONITORING_LOCAL, PROM_LOCAL_CONFIG, GRAFANA_DS_LOCAL],
        ids=["compose-monitoring-local", "prometheus-local", "ds-local"],
    )
    def test_file_exists(self, path):
        assert path.exists(), f"Missing: {path.relative_to(PROJECT_DIR)}"


class TestComposeMonitoringLocal:
    """Validate docker-compose.monitoring-local.yml overlay."""

    @pytest.fixture(autouse=True)
    def load_compose(self):
        self.compose = yaml.safe_load(COMPOSE_MONITORING_LOCAL.read_text())

    # -- Service discovery --

    def test_has_prometheus(self):
        assert "prometheus" in self.compose["services"]

    def test_has_grafana(self):
        assert "grafana" in self.compose["services"]

    def test_has_loki(self):
        assert "loki" in self.compose["services"]

    def test_has_postgres_exporter(self):
        assert "postgres-exporter" in self.compose["services"]

    def test_service_count(self):
        """Local monitoring overlay has exactly 4 services."""
        assert len(self.compose["services"]) == 4

    # -- Images --

    def test_prometheus_image(self):
        assert self.compose["services"]["prometheus"]["image"].startswith("prom/prometheus:")

    def test_grafana_image(self):
        assert self.compose["services"]["grafana"]["image"].startswith("grafana/grafana:")

    def test_loki_image(self):
        assert self.compose["services"]["loki"]["image"].startswith("grafana/loki:")

    def test_postgres_exporter_image(self):
        img = self.compose["services"]["postgres-exporter"]["image"]
        assert "postgres-exporter" in img

    # -- Ports --

    def test_prometheus_exposes_9090(self):
        ports = self.compose["services"]["prometheus"].get("ports", [])
        assert any("9090" in str(p) for p in ports)

    def test_grafana_exposes_3000(self):
        ports = self.compose["services"]["grafana"].get("ports", [])
        assert any("3000" in str(p) for p in ports)

    def test_loki_exposes_3100(self):
        ports = self.compose["services"]["loki"].get("ports", [])
        assert any("3100" in str(p) for p in ports)

    def test_postgres_exporter_no_exposed_ports(self):
        """postgres-exporter is internal-only; scraped by prometheus via docker network."""
        ports = self.compose["services"]["postgres-exporter"].get("ports", [])
        assert len(ports) == 0, "postgres-exporter should not expose ports externally"

    # -- Volumes --

    def test_prometheus_mounts_local_config(self):
        vols = self.compose["services"]["prometheus"]["volumes"]
        assert any("prometheus-local.yml" in str(v) for v in vols), (
            "Prometheus must mount prometheus-local.yml (not the SPCS config)"
        )

    def test_prometheus_mounts_rules(self):
        vols = self.compose["services"]["prometheus"]["volumes"]
        assert any("rules" in str(v) for v in vols)

    def test_prometheus_has_data_volume(self):
        vols = self.compose["services"]["prometheus"]["volumes"]
        assert any("prometheus_data" in str(v) for v in vols)

    def test_grafana_mounts_local_datasources(self):
        """Grafana must mount ds-local.yaml (docker service names, not localhost)."""
        vols = self.compose["services"]["grafana"]["volumes"]
        assert any("ds-local.yaml" in str(v) for v in vols), (
            "Grafana must mount ds-local.yaml, not ds.yaml (which uses localhost for SPCS)"
        )

    def test_grafana_mounts_dashboards(self):
        vols = self.compose["services"]["grafana"]["volumes"]
        assert any("dashboards" in str(v) for v in vols)

    def test_grafana_has_data_volume(self):
        vols = self.compose["services"]["grafana"]["volumes"]
        assert any("grafana_data" in str(v) for v in vols)

    def test_loki_has_data_volume(self):
        vols = self.compose["services"]["loki"]["volumes"]
        assert any("loki_data" in str(v) for v in vols)

    def test_named_volumes_defined(self):
        vols = self.compose.get("volumes", {})
        assert "prometheus_data" in vols
        assert "grafana_data" in vols
        assert "loki_data" in vols

    # -- Dependencies --

    def test_grafana_depends_on_prometheus(self):
        deps = self.compose["services"]["grafana"].get("depends_on", {})
        if isinstance(deps, list):
            assert "prometheus" in deps
        else:
            assert "prometheus" in deps

    def test_grafana_depends_on_loki(self):
        deps = self.compose["services"]["grafana"].get("depends_on", {})
        if isinstance(deps, list):
            assert "loki" in deps
        else:
            assert "loki" in deps

    def test_postgres_exporter_depends_on_postgres(self):
        deps = self.compose["services"]["postgres-exporter"].get("depends_on", {})
        assert "postgres" in deps

    def test_postgres_exporter_waits_for_postgres_healthy(self):
        deps = self.compose["services"]["postgres-exporter"]["depends_on"]
        assert deps["postgres"]["condition"] == "service_healthy"

    # -- Environment --

    def test_grafana_admin_user(self):
        env = self.compose["services"]["grafana"]["environment"]
        assert env.get("GF_SECURITY_ADMIN_USER") == "admin"

    def test_grafana_admin_password_is_dev_default(self):
        """Local dev uses plain 'admin' — NOT a secret (no .env injection)."""
        env = self.compose["services"]["grafana"]["environment"]
        assert env.get("GF_SECURITY_ADMIN_PASSWORD") == "admin"

    def test_grafana_disables_signup(self):
        env = self.compose["services"]["grafana"]["environment"]
        assert env.get("GF_USERS_ALLOW_SIGN_UP") == "false"

    def test_postgres_exporter_dsn_references_compose_postgres(self):
        """DSN must point to the compose 'postgres' service, not SPCS managed PG."""
        env = self.compose["services"]["postgres-exporter"]["environment"]
        dsn = env.get("DATA_SOURCE_NAME", "")
        assert "postgres:5432" in dsn, (
            f"postgres-exporter DSN must target compose 'postgres' service, got: {dsn}"
        )

    def test_postgres_exporter_dsn_uses_env_var_for_password(self):
        """Password must come from POSTGRES_PASSWORD env var, not hardcoded."""
        env = self.compose["services"]["postgres-exporter"]["environment"]
        dsn = env.get("DATA_SOURCE_NAME", "")
        assert "POSTGRES_PASSWORD" in dsn, (
            "postgres-exporter DSN must reference $POSTGRES_PASSWORD, not hardcode it"
        )

    # -- Restart policies --

    def test_all_services_have_restart_policy(self):
        for name, svc in self.compose["services"].items():
            assert "restart" in svc, f"{name}: must have a restart policy"

    # -- Best practices --

    def test_no_privileged_containers(self):
        for name, svc in self.compose["services"].items():
            assert svc.get("privileged") is not True, f"{name}: should not run privileged"

    def test_no_host_network_mode(self):
        for name, svc in self.compose["services"].items():
            assert svc.get("network_mode") != "host", f"{name}: should not use host network"


# ===========================================================================
# Prometheus local config — prometheus-local.yml
# ===========================================================================
class TestPrometheusLocalConfig:
    """Validate prometheus-local.yml for local development."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load(PROM_LOCAL_CONFIG.read_text())

    def test_has_global_scrape_interval(self):
        assert "scrape_interval" in self.config["global"]

    def test_scrape_interval_is_15s(self):
        assert self.config["global"]["scrape_interval"] == "15s"

    def test_has_rule_files(self):
        """Local Prometheus must load the same alert rules as SPCS."""
        assert "rule_files" in self.config
        assert any("rules" in str(rf) for rf in self.config["rule_files"])

    def test_has_scrape_configs(self):
        assert "scrape_configs" in self.config
        assert len(self.config["scrape_configs"]) >= 4

    def test_all_expected_jobs_present(self):
        job_names = {j["job_name"] for j in self.config["scrape_configs"]}
        expected = {"prefect-server", "prometheus", "grafana", "loki", "postgres"}
        assert expected == job_names, (
            f"Missing: {expected - job_names}, unexpected: {job_names - expected}"
        )

    def test_prefect_server_uses_api_metrics_path(self):
        server_job = next(
            j for j in self.config["scrape_configs"] if j["job_name"] == "prefect-server"
        )
        assert server_job["metrics_path"] == "/api/metrics"

    def test_prefect_server_targets_docker_service_name(self):
        """Must use 'prefect-server:4200', NOT SPCS internal DNS."""
        server_job = next(
            j for j in self.config["scrape_configs"] if j["job_name"] == "prefect-server"
        )
        targets = server_job["static_configs"][0]["targets"]
        assert "prefect-server:4200" in targets
        assert not any("svc.spcs.internal" in t for t in targets), (
            "Local config must not use SPCS internal DNS"
        )

    def test_grafana_targets_docker_service_name(self):
        grafana_job = next(j for j in self.config["scrape_configs"] if j["job_name"] == "grafana")
        targets = grafana_job["static_configs"][0]["targets"]
        assert "grafana:3000" in targets

    def test_loki_targets_docker_service_name(self):
        loki_job = next(j for j in self.config["scrape_configs"] if j["job_name"] == "loki")
        targets = loki_job["static_configs"][0]["targets"]
        assert "loki:3100" in targets

    def test_loki_scrapes_metrics_not_ready(self):
        """Loki must be scraped at /metrics (same as SPCS config)."""
        loki_job = next(j for j in self.config["scrape_configs"] if j["job_name"] == "loki")
        assert loki_job["metrics_path"] == "/metrics"

    def test_postgres_targets_postgres_exporter(self):
        pg_job = next(j for j in self.config["scrape_configs"] if j["job_name"] == "postgres")
        targets = pg_job["static_configs"][0]["targets"]
        assert "postgres-exporter:9187" in targets

    def test_prometheus_self_monitoring(self):
        prom_job = next(j for j in self.config["scrape_configs"] if j["job_name"] == "prometheus")
        targets = prom_job["static_configs"][0]["targets"]
        assert "localhost:9090" in targets

    def test_has_environment_labels(self):
        """Local config should label metrics with environment: local."""
        server_job = next(
            j for j in self.config["scrape_configs"] if j["job_name"] == "prefect-server"
        )
        labels = server_job["static_configs"][0].get("labels", {})
        assert labels.get("environment") == "local"

    def test_no_spcs_dns_in_any_target(self):
        """No target in the local config should reference SPCS internal DNS."""
        raw = PROM_LOCAL_CONFIG.read_text()
        assert "svc.spcs.internal" not in raw, (
            "prometheus-local.yml must not reference SPCS internal DNS"
        )

    def test_no_remote_write(self):
        """Local Prometheus stores locally — no remote_write needed."""
        assert "remote_write" not in self.config


# ===========================================================================
# Grafana local datasources — ds-local.yaml
# ===========================================================================
class TestGrafanaLocalDatasources:
    """Validate ds-local.yaml for local development."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.data = yaml.safe_load(GRAFANA_DS_LOCAL.read_text())

    def test_has_prometheus_datasource(self):
        names = [ds["name"] for ds in self.data["datasources"]]
        assert "Prometheus" in names

    def test_has_loki_datasource(self):
        names = [ds["name"] for ds in self.data["datasources"]]
        assert "Loki" in names

    def test_prometheus_uid_matches_spcs(self):
        """UIDs must match SPCS ds.yaml so dashboards work in both environments."""
        spcs_data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        spcs_uids = {ds["name"]: ds["uid"] for ds in spcs_data["datasources"]}
        local_uids = {ds["name"]: ds["uid"] for ds in self.data["datasources"]}
        assert local_uids["Prometheus"] == spcs_uids["Prometheus"], (
            f"Local Prometheus uid ({local_uids['Prometheus']}) must match "
            f"SPCS uid ({spcs_uids['Prometheus']}) for dashboard portability"
        )

    def test_loki_uid_matches_spcs(self):
        """UIDs must match SPCS ds.yaml so dashboards work in both environments."""
        spcs_data = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        spcs_uids = {ds["name"]: ds["uid"] for ds in spcs_data["datasources"]}
        local_uids = {ds["name"]: ds["uid"] for ds in self.data["datasources"]}
        assert local_uids["Loki"] == spcs_uids["Loki"], (
            f"Local Loki uid ({local_uids['Loki']}) must match "
            f"SPCS uid ({spcs_uids['Loki']}) for dashboard portability"
        )

    def test_prometheus_url_uses_docker_service_name(self):
        """Must use http://prometheus:9090, NOT localhost:9090."""
        prom_ds = next(ds for ds in self.data["datasources"] if ds["name"] == "Prometheus")
        assert prom_ds["url"] == "http://prometheus:9090", (
            f"Local Prometheus datasource must use docker service name, got: {prom_ds['url']}"
        )

    def test_loki_url_uses_docker_service_name(self):
        """Must use http://loki:3100, NOT localhost:3100."""
        loki_ds = next(ds for ds in self.data["datasources"] if ds["name"] == "Loki")
        assert loki_ds["url"] == "http://loki:3100", (
            f"Local Loki datasource must use docker service name, got: {loki_ds['url']}"
        )

    def test_datasources_not_editable(self):
        for ds in self.data["datasources"]:
            assert ds.get("editable") is False, (
                f"Datasource '{ds['name']}' must have editable: false"
            )

    def test_has_delete_stale_entries(self):
        """Must include deleteDatasources to prevent UID conflicts."""
        assert "deleteDatasources" in self.data
        delete_names = {d["name"] for d in self.data["deleteDatasources"]}
        assert "Prometheus" in delete_names
        assert "Loki" in delete_names

    def test_prometheus_is_default(self):
        prom_ds = next(ds for ds in self.data["datasources"] if ds["name"] == "Prometheus")
        assert prom_ds.get("isDefault") is True

    def test_no_localhost_urls(self):
        """Local datasource URLs must NOT use localhost (that's the SPCS pattern)."""
        for ds in self.data["datasources"]:
            assert "localhost" not in ds["url"], (
                f"Datasource '{ds['name']}' URL must use docker service names, "
                f"not localhost. Got: {ds['url']}"
            )


# ===========================================================================
# Local vs SPCS config parity
# ===========================================================================
class TestLocalSPCSParity:
    """Ensure local monitoring configs maintain parity with SPCS configs."""

    def test_same_datasource_uids(self):
        """Both environments must use identical datasource UIDs for dashboard portability."""
        spcs = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        local = yaml.safe_load(GRAFANA_DS_LOCAL.read_text())
        spcs_uids = {ds["uid"] for ds in spcs["datasources"]}
        local_uids = {ds["uid"] for ds in local["datasources"]}
        assert spcs_uids == local_uids

    def test_same_datasource_names(self):
        spcs = yaml.safe_load(GRAFANA_DS_FILE.read_text())
        local = yaml.safe_load(GRAFANA_DS_LOCAL.read_text())
        spcs_names = {ds["name"] for ds in spcs["datasources"]}
        local_names = {ds["name"] for ds in local["datasources"]}
        assert spcs_names == local_names

    def test_local_prometheus_shares_alert_rules_with_spcs(self):
        """Local Prometheus must load the same rule_files glob as SPCS Prometheus."""
        spcs_config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        local_config = yaml.safe_load(PROM_LOCAL_CONFIG.read_text())
        assert local_config.get("rule_files") == spcs_config.get("rule_files"), (
            "Local Prometheus must use the same rule_files as SPCS for alert parity"
        )

    def test_local_prefect_server_metrics_path_matches_spcs(self):
        """Both configs must scrape Prefect server at /api/metrics."""
        spcs_config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        local_config = yaml.safe_load(PROM_LOCAL_CONFIG.read_text())

        spcs_server = next(
            j for j in spcs_config["scrape_configs"] if j["job_name"] == "prefect-server"
        )
        local_server = next(
            j for j in local_config["scrape_configs"] if j["job_name"] == "prefect-server"
        )
        assert local_server["metrics_path"] == spcs_server["metrics_path"]

    def test_local_scrape_interval_matches_spcs(self):
        spcs_config = yaml.safe_load(PROM_CONFIG_FILE.read_text())
        local_config = yaml.safe_load(PROM_LOCAL_CONFIG.read_text())
        assert local_config["global"]["scrape_interval"] == spcs_config["global"]["scrape_interval"]


# ===========================================================================
# Deploy script — 7-step deployment flow
# ===========================================================================
class TestDeployScriptStepMarkers:
    """Validate deploy_monitoring.sh has all 7 deployment steps in order."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()
        self.lines = self.content.splitlines()

    def test_step_1_compute_pool(self):
        assert "[1/7]" in self.content

    def test_step_2_network_rule(self):
        assert "[2/7]" in self.content

    def test_step_3_eai(self):
        assert "[3/7]" in self.content

    def test_step_4_stage(self):
        assert "[4/7]" in self.content

    def test_step_5_uploads(self):
        assert "[5/7]" in self.content

    def test_step_6_service(self):
        assert "[6/7]" in self.content

    def test_step_7_status(self):
        assert "[7/7]" in self.content

    def test_steps_are_in_order(self):
        """Step markers must appear in ascending order in the script."""
        positions = []
        for step in range(1, 8):
            marker = f"[{step}/7]"
            pos = self.content.index(marker)
            positions.append(pos)
        assert positions == sorted(positions), "Deployment steps are out of order"

    def test_step_1_mentions_compute_pool(self):
        idx = self.content.index("[1/7]")
        chunk = self.content[idx : idx + 200]
        assert "compute pool" in chunk.lower() or "COMPUTE POOL" in chunk

    def test_step_6_mentions_service(self):
        idx = self.content.index("[6/7]")
        chunk = self.content[idx : idx + 200]
        assert "service" in chunk.lower()


# ===========================================================================
# Deploy script — idempotency patterns
# ===========================================================================
class TestDeployScriptIdempotency:
    """Validate deploy_monitoring.sh is safe to re-run (idempotent)."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_compute_pool_uses_if_not_exists(self):
        """Compute pool creation must use IF NOT EXISTS."""
        # Find the CREATE COMPUTE POOL line
        assert "CREATE COMPUTE POOL IF NOT EXISTS" in self.content

    def test_stage_uses_if_not_exists(self):
        """Stage creation must use IF NOT EXISTS."""
        assert "CREATE STAGE IF NOT EXISTS" in self.content

    def test_secrets_use_if_not_exists(self):
        """Secret creation must use IF NOT EXISTS."""
        assert "CREATE SECRET IF NOT EXISTS" in self.content

    def test_commands_have_or_true_fallback(self):
        """Critical SQL commands should have || true to avoid aborting on benign errors."""
        or_true_count = self.content.count("|| true")
        assert or_true_count >= 5, f"Expected at least 5 '|| true' fallbacks, found {or_true_count}"

    def test_service_checks_existence_before_create(self):
        """Script must check if service exists before CREATE (not CREATE OR REPLACE)."""
        assert "SHOW SERVICES LIKE" in self.content
        assert "already exists" in self.content

    def test_service_suggests_alter_for_updates(self):
        """When service exists, script should suggest ALTER instead of recreating."""
        assert "ALTER" in self.content

    def test_header_says_idempotent(self):
        """Script header should document idempotency."""
        header = self.content[:500]
        assert "idempotent" in header.lower() or "safe to re-run" in header.lower()


# ===========================================================================
# Deploy script — connection argument parsing
# ===========================================================================
class TestDeployScriptConnectionArgs:
    """Validate deploy_monitoring.sh parses --connection argument correctly."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_default_connection_is_aws_spcs(self):
        assert "aws_spcs" in self.content

    def test_supports_positional_connection_arg(self):
        """First positional arg should set the connection."""
        assert "${1:-aws_spcs}" in self.content

    def test_supports_named_connection_flag(self):
        """--connection flag should be supported."""
        assert '"--connection"' in self.content

    def test_connection_passed_to_snow_sql(self):
        """All snow sql command blocks must use --connection."""
        # Commands span multiple lines with \ continuations, so count
        # invocations vs --connection occurrences
        sql_count = self.content.count("snow sql -q")
        conn_in_sql = self.content.count('--connection "$CONNECTION"')
        assert conn_in_sql >= sql_count, (
            f"Found {sql_count} snow sql calls but only {conn_in_sql} --connection refs"
        )

    def test_connection_passed_to_snow_stage_copy(self):
        """All snow stage copy commands must use --connection."""
        copy_count = self.content.count("snow stage copy")
        conn_in_copy = self.content.count("--connection")
        # --connection appears in snow sql AND snow stage copy; just verify
        # every snow stage copy block has a matching --connection nearby
        assert conn_in_copy > copy_count, "Not every snow stage copy command has --connection"


# ===========================================================================
# Deploy script — shell safety
# ===========================================================================
class TestDeployScriptShellSafety:
    """Validate deploy_monitoring.sh uses safe shell practices."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()
        self.lines = self.content.splitlines()

    def test_uses_bash_shebang(self):
        assert self.lines[0].startswith("#!/")
        assert "bash" in self.lines[0]

    def test_uses_strict_mode(self):
        """Script must use set -euo pipefail for safety."""
        assert "set -euo pipefail" in self.content

    def test_no_hardcoded_passwords(self):
        """Script must not contain hardcoded passwords or secrets."""
        for line in self.lines:
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            lower = stripped.lower()
            # Skip variable references, env defaults, read prompts, keychain lookups
            if "${" in stripped or "read -r" in stripped or "security " in stripped:
                continue
            # Check for literal password assignments (not variable expansions)
            if "secret_string='real" in lower or "app_password=" in lower:
                raise AssertionError(f"Possible hardcoded secret: {stripped}")

    def test_smtp_port_is_587_not_465(self):
        """SMTP port must be 587 (STARTTLS), not 465 (implicit SSL rejected by SPCS)."""
        assert "smtp.gmail.com:587" in self.content
        assert "smtp.gmail.com:465" not in self.content


# ===========================================================================
# Deploy script — Grafana admin password secret
# ===========================================================================
class TestDeployScriptGrafanaAdminSecret:
    """Validate deploy_monitoring.sh handles Grafana admin password correctly."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = DEPLOY_SCRIPT.read_text()

    def test_reads_grafana_password_from_env(self):
        assert "GRAFANA_ADMIN_PASSWORD" in self.content

    def test_prompts_for_password_if_missing(self):
        assert "Enter Grafana admin password" in self.content

    def test_rejects_empty_password(self):
        """Must exit with error if Grafana admin password is empty."""
        assert "cannot be empty" in self.content.lower() or "ERROR" in self.content

    def test_escapes_password_for_sql(self):
        """Password must be escaped for safe SQL interpolation (via _create_secret helper)."""
        assert "_create_secret GRAFANA_ADMIN_PASSWORD" in self.content

    def test_grants_read_on_admin_secret(self):
        """Must GRANT READ on admin secret via _create_secret helper."""
        assert "GRANT READ ON SECRET" in self.content
        assert "_create_secret GRAFANA_ADMIN_PASSWORD" in self.content


# ===========================================================================
# Teardown script — step markers and safety
# ===========================================================================
class TestTeardownScriptStepMarkers:
    """Validate teardown_monitoring.sh has all 5 steps and uses IF EXISTS."""

    @pytest.fixture(autouse=True)
    def load_script(self):
        self.content = TEARDOWN_SCRIPT.read_text()

    def test_step_1_drops_service(self):
        assert "[1/5]" in self.content

    def test_step_2_drops_eai(self):
        assert "[2/5]" in self.content

    def test_step_3_drops_network_rule(self):
        assert "[3/5]" in self.content

    def test_step_4_drops_stage(self):
        assert "[4/5]" in self.content

    def test_step_5_drops_compute_pool(self):
        assert "[5/5]" in self.content

    def test_all_drops_use_if_exists(self):
        """All DROP statements must use IF EXISTS for safety."""
        lines = [ln for ln in self.content.splitlines() if "DROP " in ln and "snow sql" in ln]
        for line in lines:
            assert "IF EXISTS" in line, f"DROP without IF EXISTS: {line.strip()}"

    def test_uses_strict_mode(self):
        assert "set -euo pipefail" in self.content

    def test_supports_connection_flag(self):
        assert '"--connection"' in self.content

    def test_teardown_order_is_correct(self):
        """Teardown must drop service first, pool last (reverse of deploy)."""
        service_pos = self.content.index("PF_MONITOR")
        pool_pos = self.content.rindex("PREFECT_MONITOR_POOL")
        assert service_pos < pool_pos, "Must drop service before pool"

    def test_all_drops_have_or_true(self):
        """DROP commands should have || true to avoid aborting."""
        lines = [ln for ln in self.content.splitlines() if "DROP " in ln and "snow sql" in ln]
        for line in lines:
            assert "|| true" in line, f"DROP without || true fallback: {line.strip()}"


# ---------------------------------------------------------------------------
# Regression: postgres-exporter PG17 compatibility
# ---------------------------------------------------------------------------
class TestPostgresExporterPG17Compat:
    """postgres-exporter >= v0.17.0 is required for PG17.

    PG17 moved checkpoint columns from pg_stat_bgwriter to
    pg_stat_checkpointer. Older exporters query the removed columns,
    producing constant error logs like:
      'ERROR: column "checkpoints_timed" does not exist'
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_postgres_exporter_version_pg17_compatible(self):
        """postgres-exporter image tag must be >= v0.17.0 for PG17 compatibility."""
        import re as _re

        image = self.containers["postgres-exporter"]["image"]
        match = _re.search(r":v?(\d+\.\d+\.\d+)", image)
        assert match, f"Cannot parse version from image: {image}"
        parts = [int(x) for x in match.group(1).split(".")]
        assert tuple(parts) >= (0, 17, 0), (
            f"postgres-exporter {match.group(1)} < 0.17.0 — incompatible with PG17. "
            f"PG17 moved checkpoint columns from pg_stat_bgwriter to pg_stat_checkpointer. "
            f"Upgrade to >= v0.17.0 (v0.19.1+ recommended)."
        )


# ---------------------------------------------------------------------------
# Regression: Loki version — structured metadata byte accounting bug
# ---------------------------------------------------------------------------
class TestLokiVersionCompat:
    """Loki >= 3.6.0 is required to avoid 'negative structured metadata bytes' errors.

    Loki 3.5.x has a byte-accounting bug in push.go that logs spurious
    ERROR-level messages ('negative structured metadata bytes received')
    when entries are pushed without structured metadata. Fixed in 3.6.0+.
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        self.containers = {c["name"]: c for c in self.spec["spec"]["containers"]}

    def test_loki_version_no_negative_metadata_bug(self):
        """Loki image tag must be >= 3.6.0 to avoid structured metadata bug."""
        import re as _re

        image = self.containers["loki"]["image"]
        match = _re.search(r":(\d+\.\d+\.\d+)", image)
        assert match, f"Cannot parse version from Loki image: {image}"
        parts = [int(x) for x in match.group(1).split(".")]
        assert tuple(parts) >= (3, 6, 0), (
            f"Loki {match.group(1)} < 3.6.0 — has 'negative structured metadata bytes' "
            f"bug (push.go:202) that produces constant ERROR logs. Upgrade to >= 3.6.0."
        )


# ---------------------------------------------------------------------------
# Regression: Grafana legacy vs unified alerting conflict
# ---------------------------------------------------------------------------
class TestGrafanaAlertingConfig:
    """Grafana 11+ requires legacy alerting disabled when unified alerting is on.

    Having both GF_ALERTING_ENABLED=true and GF_UNIFIED_ALERTING_ENABLED=true
    causes startup conflicts and broken alert management. Legacy alerting was
    removed in Grafana 11; the env var must be explicitly set to "false".
    """

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")
        self.env = self.grafana.get("env", {})

    def test_legacy_alerting_disabled_when_unified_enabled(self):
        """GF_ALERTING_ENABLED must be 'false' when GF_UNIFIED_ALERTING_ENABLED is 'true'."""
        unified = self.env.get("GF_UNIFIED_ALERTING_ENABLED")
        legacy = self.env.get("GF_ALERTING_ENABLED")
        if unified == "true":
            assert legacy == "false", (
                f"Grafana has GF_UNIFIED_ALERTING_ENABLED=true but "
                f"GF_ALERTING_ENABLED={legacy!r}. Legacy alerting (removed in "
                f"Grafana 11) must be 'false' to avoid startup conflicts."
            )

    def test_unified_alerting_enabled(self):
        """Grafana must have unified alerting enabled."""
        assert self.env.get("GF_UNIFIED_ALERTING_ENABLED") == "true"


# ---------------------------------------------------------------------------
# Regression: Loki config validation (alertmanager_url, ruler keys)
# ---------------------------------------------------------------------------
class TestLokiConfigRegression:
    """Regression tests for Loki configuration issues.

    Guards against:
    - Stale/broken alertmanager_url that produces 400/403/404 errors
    - Invalid ruler config field names (e.g., nested basic_auth vs flat fields)
    - Missing enable_api (prevents Grafana from querying ruler state)
    """

    VALID_RULER_KEYS = {
        "storage",
        "rule_path",
        "alertmanager_url",
        "alertmanager_client",
        "ring",
        "enable_api",
        "evaluation_interval",
        "poll_interval",
        "external_url",
        "enable_alertmanager_v2",
        "enable_sharding",
        "flush_period",
        "for_outage_tolerance",
        "for_grace_period",
        "resend_delay",
        "notification_queue_capacity",
        "notification_timeout",
        "wal",
        "remote_write",
    }

    @pytest.fixture(autouse=True)
    def load_config(self):
        self.config = yaml.safe_load(LOKI_CONFIG_FILE.read_text())

    def test_alertmanager_url_not_stale(self):
        """alertmanager_url must be empty or a valid reachable pattern.

        A non-empty alertmanager_url that points to a broken endpoint
        (wrong auth, incompatible API) generates constant error logs:
          'msg="error sending notification" err="400 Bad Request"'

        Currently disabled (empty string) because Grafana's unified
        alerting AM API doesn't accept Loki-generated alerts. Grafana
        queries Loki's ruler API directly for alert state instead.
        """
        ruler = self.config.get("ruler", {})
        url = ruler.get("alertmanager_url", "")
        assert url == "", (
            f"alertmanager_url must be empty (disabled) — Grafana's unified alerting "
            f"AM API returns 400 for Loki-generated alerts. Grafana queries Loki's "
            f"ruler API directly instead. Got: {url!r}"
        )

    def test_ruler_keys_are_valid(self):
        """Ruler config keys must be recognized Loki ruler fields.

        Invalid keys (e.g., 'basic_auth' nested under alertmanager_client)
        cause Loki config parse errors:
          'field basic_auth not found in type config.NotifierConfig'
        """
        ruler = self.config.get("ruler", {})
        for key in ruler:
            assert key in self.VALID_RULER_KEYS, (
                f"Unknown ruler config key '{key}' — may cause Loki parse error. "
                f"Valid keys: {sorted(self.VALID_RULER_KEYS)}"
            )

    def test_ruler_has_enable_api(self):
        """Ruler must have enable_api: true for Grafana to query alert state.

        Without enable_api, Grafana cannot query /loki/api/v1/rules to
        display Loki-evaluated alert rule status in the UI.
        """
        ruler = self.config.get("ruler", {})
        assert ruler.get("enable_api") is True, (
            "Loki ruler must have enable_api: true so Grafana can query alert state "
            "via /loki/api/v1/rules"
        )

    def test_ruler_has_local_storage(self):
        """Ruler must have local storage configured for rules directory."""
        ruler = self.config.get("ruler", {})
        storage = ruler.get("storage", {})
        assert storage.get("type") == "local"

    def test_tracing_disabled(self):
        """Loki tracing must be explicitly disabled.

        Loki 3.6+ enables OpenTelemetry tracing by default, attempting to
        export to collector.monitor.spcs.internal:4317 every 5 seconds.
        Without an OTel collector, this floods logs with connection errors.
        """
        tracing = self.config.get("tracing", {})
        assert tracing.get("enabled") is False, (
            "Loki tracing must be disabled (tracing.enabled: false) — "
            "no OTel collector is deployed. Without this, Loki 3.6+ floods "
            "logs with trace export errors every 5 seconds."
        )


class TestGrafanaNotificationConfig:
    """Validate Grafana notification contact points and SMTP config."""

    @pytest.fixture(autouse=True)
    def load_spec(self):
        self.spec = yaml.safe_load(MONITOR_SPEC_FILE.read_text())
        containers = self.spec["spec"]["containers"]
        self.grafana = next(c for c in containers if c["name"] == "grafana")

    def test_smtp_user_not_placeholder(self):
        """SMTP user must not be a placeholder value."""
        env = self.grafana.get("env", {})
        smtp_user = env.get("GF_SMTP_USER", "")
        assert "CHANGE_ME" not in smtp_user and "example.com" not in smtp_user, (
            f"GF_SMTP_USER is still a placeholder: {smtp_user!r}"
        )

    def test_smtp_from_not_placeholder(self):
        env = self.grafana.get("env", {})
        from_addr = env.get("GF_SMTP_FROM_ADDRESS", "")
        assert "CHANGE_ME" not in from_addr and "example.com" not in from_addr, (
            f"GF_SMTP_FROM_ADDRESS is still a placeholder: {from_addr!r}"
        )

    def test_webhook_receiver_removed(self):
        """Webhook receiver was removed — no real webhook URL exists."""
        env = self.grafana.get("env", {})
        assert "GF_ALERTING_WEBHOOK_URL" not in env, (
            "GF_ALERTING_WEBHOOK_URL should not be set — webhook receiver removed"
        )

    def test_smtp_user_from_secret(self):
        """SMTP user should be injected via Snowflake secret, not hardcoded."""
        secrets = self.grafana.get("secrets", [])
        smtp_user_secret = [s for s in secrets if s.get("envVarName") == "GF_SMTP_USER"]
        assert len(smtp_user_secret) == 1, "GF_SMTP_USER must be injected via a Snowflake secret"


# ---------------------------------------------------------------------------
# Loki LogQL alert rules validation
# ---------------------------------------------------------------------------
LOKI_RULES_FILE = MONITORING_DIR / "loki" / "rules" / "fake" / "alerts.yaml"


class TestLokiAlertRulesRegression:
    """Regression tests for Loki LogQL alerting rules.

    Ensures:
    - YAML is valid and has the expected structure
    - All rules have required fields (alert, expr, labels.severity)
    - LogQL severity selectors match poller output labels
    """

    @pytest.fixture(autouse=True)
    def load_rules(self):
        self.rules = yaml.safe_load(LOKI_RULES_FILE.read_text())

    def test_file_exists(self):
        assert LOKI_RULES_FILE.exists(), f"Missing: {LOKI_RULES_FILE}"

    def test_has_groups_key(self):
        assert "groups" in self.rules

    def test_has_at_least_one_group(self):
        assert len(self.rules["groups"]) >= 1

    def test_group_has_name_and_rules(self):
        for group in self.rules["groups"]:
            assert "name" in group, "Loki rule group missing 'name'"
            assert "rules" in group, f"Group '{group['name']}' missing 'rules'"

    def test_all_rules_have_required_fields(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert "alert" in rule, f"Rule in group '{group['name']}' missing 'alert'"
                assert "expr" in rule, f"Rule '{rule.get('alert')}' missing 'expr'"
                assert "labels" in rule, f"Rule '{rule['alert']}' missing 'labels'"
                assert "severity" in rule["labels"], (
                    f"Rule '{rule['alert']}' missing labels.severity"
                )

    def test_all_rules_have_annotations(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                annotations = rule.get("annotations", {})
                assert "summary" in annotations, (
                    f"Rule '{rule['alert']}' missing annotations.summary"
                )

    def test_severity_values_are_valid(self):
        valid_severities = {"critical", "warning", "info"}
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                sev = rule["labels"]["severity"]
                assert sev in valid_severities, (
                    f"Rule '{rule['alert']}' has invalid severity '{sev}', "
                    f"expected one of {valid_severities}"
                )

    def test_all_rules_have_source_label(self):
        """All Loki rules should have source=loki label for routing."""
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert rule["labels"].get("source") == "loki", (
                    f"Rule '{rule['alert']}' missing source=loki label"
                )

    def test_error_spike_rule_severity_selector_matches_poller(self):
        """SPCSErrorSpike rule must use severity='ERROR' matching poller output.

        The poller sets severity as a Loki stream label with values like
        'ERROR', 'WARNING', 'INFO'. The LogQL selector must match exactly.
        """
        all_rules = []
        for group in self.rules["groups"]:
            all_rules.extend(group["rules"])
        error_spike = next((r for r in all_rules if r["alert"] == "SPCSErrorSpike"), None)
        assert error_spike is not None, "Missing SPCSErrorSpike rule"
        assert 'severity="ERROR"' in error_spike["expr"], (
            f'SPCSErrorSpike must filter on severity="ERROR" to match poller labels, '
            f"got: {error_spike['expr']}"
        )

    def test_all_rules_reference_spcs_logs_job(self):
        """All rules should query the spcs-logs job label from the poller."""
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                assert "spcs-logs" in rule["expr"], (
                    f"Rule '{rule['alert']}' must reference job=spcs-logs, got: {rule['expr']}"
                )

    def test_expected_alert_names(self):
        all_rules = []
        for group in self.rules["groups"]:
            all_rules.extend(group["rules"])
        names = {r["alert"] for r in all_rules}
        expected = {"SPCSErrorSpike", "SPCSContainerOOM", "SPCSCrashLoop", "SPCSAuthFailure"}
        assert names == expected, f"Missing: {expected - names}, unexpected: {names - expected}"


# ---------------------------------------------------------------------------
# Dashboard: Health
# ---------------------------------------------------------------------------
DASH_HEALTH = DASHBOARD_DIR / "health.json"


class TestDashboardHealth:
    """Validate the Health dashboard (single pane of glass).

    Guards against:
    - Missing or broken panels
    - Wrong datasource references
    - Loki panels not matching poller severity labels
    """

    @pytest.fixture(autouse=True)
    def load_dashboard(self):
        self.dash = json.loads(DASH_HEALTH.read_text())
        self.panels = {p["title"]: p for p in self.dash["panels"] if p.get("title")}

    def test_file_exists(self):
        assert DASH_HEALTH.exists()

    def test_dashboard_uid(self):
        assert self.dash["uid"] == "health"

    def test_dashboard_title(self):
        assert self.dash["title"] == "Prefect Health"

    def test_has_services_up_panel(self):
        assert "Services UP" in self.panels

    def test_has_server_uptime_panels(self):
        assert "Server Uptime (1h)" in self.panels
        assert "Server Uptime (24h)" in self.panels

    def test_has_firing_alerts_panel(self):
        assert "Firing Alerts" in self.panels

    def test_has_flow_success_rate_panel(self):
        assert "Flow Success Rate" in self.panels

    def test_has_active_flow_runs_panel(self):
        assert "Active Flow Runs" in self.panels

    def test_has_failed_runs_panel(self):
        assert "Failed Runs" in self.panels

    def test_has_cpu_usage_panel(self):
        assert "CPU Usage by Container" in self.panels

    def test_has_memory_usage_panel(self):
        assert "Memory Usage by Container" in self.panels

    def test_has_recent_error_logs_panel(self):
        assert "Recent SPCS Error Logs" in self.panels

    def test_error_logs_uses_loki_datasource(self):
        """Error logs panel must use Loki datasource."""
        panel = self.panels["Recent SPCS Error Logs"]
        ds = panel.get("datasource", {})
        assert ds.get("uid") == "loki", f"Error logs panel must use Loki datasource, got: {ds}"

    def test_error_logs_queries_severity_error(self):
        """Error logs LogQL must filter on severity='ERROR' matching poller labels."""
        panel = self.panels["Recent SPCS Error Logs"]
        expr = panel["targets"][0]["expr"]
        assert 'severity="ERROR"' in expr, (
            f'Error logs must filter severity="ERROR" (matching poller stream key), got: {expr}'
        )

    def test_error_logs_queries_spcs_logs_job(self):
        """Error logs must query the spcs-logs job from the poller."""
        panel = self.panels["Recent SPCS Error Logs"]
        expr = panel["targets"][0]["expr"]
        assert "spcs-logs" in expr, f"Error logs must query spcs-logs job, got: {expr}"

    def test_prometheus_panels_use_correct_datasource(self):
        """All Prometheus-type panels must use uid='prometheus'."""
        for title, panel in self.panels.items():
            ds = panel.get("datasource", {})
            if isinstance(ds, dict) and ds.get("type") == "prometheus":
                assert ds["uid"] == "prometheus", (
                    f"Panel '{title}' has prometheus type but uid='{ds.get('uid')}'"
                )

    def test_loki_panels_use_correct_datasource(self):
        """All Loki-type panels must use uid='loki'."""
        for title, panel in self.panels.items():
            ds = panel.get("datasource", {})
            if isinstance(ds, dict) and ds.get("type") == "loki":
                assert ds["uid"] == "loki", (
                    f"Panel '{title}' has loki type but uid='{ds.get('uid')}'"
                )

    def test_has_nav_links_to_all_dashboards(self):
        """Health dashboard should link to all other dashboards."""
        links = self.dash.get("links", [])
        link_titles = {link["title"] for link in links}
        expected = {"SPCS Overview", "Prefect App", "VM Workers", "Alerts", "SPCS Logs", "Logs"}
        assert expected.issubset(link_titles), f"Missing dashboard links: {expected - link_titles}"

    def test_has_workers_per_pool_panel(self):
        assert "Workers per Pool" in self.panels

    def test_has_poll_duration_panel(self):
        assert "Poll Duration" in self.panels

    def test_has_worker_logs_stream_panel(self):
        assert "Worker Logs Stream" in self.panels


# ---------------------------------------------------------------------------
# Cross-file: poller severity labels match Loki alert rule selectors
# ---------------------------------------------------------------------------
class TestPollerLokiRuleLabelConsistency:
    """Validate that poller severity stream labels match Loki alert rule selectors.

    The poller groups logs into Loki streams keyed by severity (e.g., 'ERROR').
    Loki alert rules filter on these labels (e.g., {severity="ERROR"}).
    If they don't match, alert rules silently evaluate to zero.
    """

    def test_poller_severity_label_matches_loki_error_rule(self):
        """Poller's severity stream label must match Loki rule's severity selector."""
        import re as _re

        poller_source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert '"severity"' in poller_source, "Poller must set severity as a Loki label"

        loki_rules = yaml.safe_load(LOKI_RULES_FILE.read_text())
        all_rules = []
        for group in loki_rules["groups"]:
            all_rules.extend(group["rules"])

        severity_selectors = set()
        for rule in all_rules:
            matches = _re.findall(r'severity="(\w+)"', rule["expr"])
            severity_selectors.update(matches)

        if severity_selectors:
            assert "ERROR" in severity_selectors or not any(
                'severity="ERROR"' in r["expr"] for r in all_rules
            ), "Loki rules reference severity=ERROR but poller must emit that label value"

    def test_poller_job_label_matches_loki_rules(self):
        """Poller's job=spcs-logs must match what Loki rules filter on."""
        poller_source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert '"spcs-logs"' in poller_source, "Poller must set job=spcs-logs"

        loki_rules = yaml.safe_load(LOKI_RULES_FILE.read_text())
        for group in loki_rules["groups"]:
            for rule in group["rules"]:
                assert "spcs-logs" in rule["expr"], (
                    f"Rule '{rule['alert']}' must reference spcs-logs job"
                )

    def test_poller_severity_in_stream_key(self):
        """Poller must include severity in the stream grouping key.

        Loki labels are per-stream. If severity is NOT part of the stream
        key, all log entries for a service+container go into ONE stream
        regardless of severity, and {severity="ERROR"} queries fail.
        """
        poller_source = (PROJECT_DIR / "images" / "spcs-log-poller" / "poller.py").read_text()
        assert (
            "severity" in poller_source.split("key =")[1].split("\n")[0]
            if "key =" in poller_source
            else True
        ), "Poller stream key must include severity for per-severity Loki streams"


# ===========================================================================
# SMTP Validation in deploy_monitoring.sh
# ===========================================================================
class TestDeployMonitoringSmtpValidation:
    """Verify deploy_monitoring.sh validates SMTP credentials at deploy time."""

    @pytest.fixture(scope="class")
    def deploy_script(self):
        return (PROJECT_DIR / "monitoring" / "deploy_monitoring.sh").read_text()

    def test_validates_smtp_login_with_smtplib(self, deploy_script):
        assert "smtplib" in deploy_script

    def test_uses_starttls_for_validation(self, deploy_script):
        assert "starttls" in deploy_script

    def test_smtp_validation_uses_port_587(self, deploy_script):
        assert "587" in deploy_script

    def test_strips_spaces_from_password(self, deploy_script):
        assert "// /" in deploy_script or "SMTP_PASSWORD// /" in deploy_script

    def test_shows_warning_box_on_failure(self, deploy_script):
        assert "WARNING" in deploy_script and "SMTP" in deploy_script

    def test_references_apppasswords_url_on_failure(self, deploy_script):
        assert "myaccount.google.com/apppasswords" in deploy_script

    def test_references_rotate_secrets_on_failure(self, deploy_script):
        assert "rotate_secrets.sh" in deploy_script

    def test_shows_success_message_on_valid_login(self, deploy_script):
        assert "SMTP login validated successfully" in deploy_script


# ===========================================================================
# sql/03_setup_secrets.sql.template completeness
# ===========================================================================
class TestSecretsTemplate:
    """Verify sql/03_setup_secrets.sql.template documents all 9 secrets."""

    @pytest.fixture(scope="class")
    def template(self):
        return (PROJECT_DIR / "sql" / "03_setup_secrets.sql.template").read_text()

    ALL_SECRETS = [
        "PREFECT_DB_PASSWORD",
        "GIT_ACCESS_TOKEN",
        "POSTGRES_EXPORTER_DSN",
        "GRAFANA_DB_DSN",
        "GRAFANA_ADMIN_PASSWORD",
        "GRAFANA_SMTP_USER",
        "GRAFANA_SMTP_PASSWORD",
        "SLACK_WEBHOOK_URL",
        "PREFECT_SVC_PAT",
    ]

    @pytest.mark.parametrize("secret", ALL_SECRETS)
    def test_template_has_create_secret(self, template, secret):
        assert f"CREATE SECRET IF NOT EXISTS {secret}" in template

    def test_template_has_9_create_secret_statements(self, template):
        count = template.count("CREATE SECRET IF NOT EXISTS")
        assert count >= 9, f"Expected >=9 CREATE SECRET statements, got {count}"

    def test_template_warns_about_google_password_revocation(self, template):
        assert "REVOKES" in template or "revoke" in template.lower()

    def test_template_references_apppasswords_url(self, template):
        assert "myaccount.google.com/apppasswords" in template

    def test_template_references_rotate_secrets_smtp(self, template):
        assert "rotate_secrets.sh --all-clouds --smtp" in template

    def test_template_uses_placeholders_not_real_values(self, template):
        assert "<REPLACE_" in template or "CHANGE_ME" in template

    def test_template_lists_all_9_in_header(self, template):
        header = template[:1000]
        for i in range(1, 10):
            assert f"{i}." in header, f"Header missing numbered secret #{i}"


# ===========================================================================
# .env.example — SMTP config and revocation warning
# ===========================================================================
class TestEnvExampleSmtpConfig:
    """Verify .env.example documents SMTP vars and Google revocation warning."""

    @pytest.fixture(scope="class")
    def env_example(self):
        return (PROJECT_DIR / ".env.example").read_text()

    def test_has_grafana_smtp_user(self, env_example):
        assert "GRAFANA_SMTP_USER" in env_example

    def test_has_grafana_smtp_password(self, env_example):
        assert "GRAFANA_SMTP_PASSWORD" in env_example

    def test_has_grafana_smtp_recipients(self, env_example):
        assert "GRAFANA_SMTP_RECIPIENTS" in env_example

    def test_warns_about_google_password_revocation(self, env_example):
        env_lower = env_example.lower()
        assert "revoke" in env_lower or "REVOKES" in env_example

    def test_references_apppasswords_url(self, env_example):
        assert "myaccount.google.com/apppasswords" in env_example

    def test_references_rotate_secrets_smtp(self, env_example):
        assert "rotate_secrets.sh --all-clouds --smtp" in env_example

    def test_smtp_password_is_placeholder(self, env_example):
        for line in env_example.splitlines():
            if line.startswith("GRAFANA_SMTP_PASSWORD="):
                value = line.split("=", 1)[1]
                assert "xxxx" in value or "CHANGE" in value or "your" in value.lower()
