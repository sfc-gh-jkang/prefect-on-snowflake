"""
SPCS Observability Dashboard (SiS Container Runtime)
=====================================================
Comprehensive monitoring for Prefect on Snowpark Container Services.

Deployed as a Streamlit-in-Snowflake Container Runtime app.
Uses st.connection("snowflake") for data access and a SQL UDF for secrets.

Also runs locally via ``streamlit run app.py`` with a
``.streamlit/secrets.toml`` that configures the same connection name.

Modeled after Kubernetes observability best practices (USE method):
- Utilization: CPU, memory, network across services and compute pools
- Saturation: Queue depth, pending pods, scaling pressure
- Errors: Container restarts, failed flow runs, error logs
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Runtime detection
# ---------------------------------------------------------------------------

IS_LOCAL = not os.path.exists("/opt/streamlit-runtime")

if IS_LOCAL:
    # Load .env from project root for SNOWFLAKE_PAT etc.
    from pathlib import Path

    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        for line in _env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "PREFECT_DB")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PREFECT_SCHEMA")
PREFECT_SERVICES = os.environ.get(
    "PREFECT_SERVICES", "PF_REDIS,PF_SERVER,PF_SERVICES,PF_WORKER"
).split(",")
PREFECT_COMPUTE_POOLS = os.environ.get(
    "PREFECT_COMPUTE_POOLS",
    "PREFECT_INFRA_POOL,PREFECT_CORE_POOL,PREFECT_WORKER_POOL",
).split(",")
WORK_POOLS = os.environ.get("WORK_POOLS", "spcs-pool,gcp-pool,aws-pool,aws-pool-backup").split(",")

# Prefect API (public SPCS endpoint)
PREFECT_API_URL = os.environ.get(
    "PREFECT_API_URL",
    "https://YOUR_SPCS_ENDPOINT.snowflakecomputing.app/api",
)


def _get_pat() -> str:
    """Retrieve Snowflake PAT for Prefect API auth.

    On Snowflake Container Runtime: calls the GET_PREFECT_PAT() SQL UDF
    (which wraps _snowflake.get_generic_secret_string).

    Locally: reads SNOWFLAKE_PAT from environment / .env file.
    """
    if IS_LOCAL:
        return os.environ.get("SNOWFLAKE_PAT", "")
    try:
        session = get_session()
        result = session.sql("SELECT GET_PREFECT_PAT()").collect()
        return result[0][0] if result else ""
    except Exception:
        return ""


# Colors
STATUS_COLORS = {
    "READY": "#00c853",
    "RUNNING": "#00c853",
    "PENDING": "#ff9800",
    "FAILED": "#f44336",
    "DONE": "#2196f3",
    "SUSPENDED": "#9e9e9e",
    "UNKNOWN": "#757575",
    "TERMINATING": "#ff5722",
    "ACTIVE": "#00c853",
    "IDLE": "#ff9800",
}


# ---------------------------------------------------------------------------
# Snowflake connection (SiS Container Runtime)
# ---------------------------------------------------------------------------


def _get_connection():
    """Return SnowflakeConnection — no @st.cache_resource so the session token
    can be refreshed when the underlying Snowflake session expires (~1-4 h)."""
    return st.connection("snowflake")


def get_session():
    """Get Snowpark session via st.connection."""
    return _get_connection().session()


def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame via Snowpark session.

    Column names are normalised to lowercase so that downstream code
    works identically regardless of whether the result set comes from
    a SHOW command (UPPER) or a SELECT with aliases (lower/mixed).
    """
    try:
        session = get_session()
        session.sql(f"USE DATABASE {SNOWFLAKE_DATABASE}").collect()
        session.sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}").collect()
        df = session.sql(sql).to_pandas()
        # Snowpark to_pandas() wraps SHOW-command column names in double
        # quotes (e.g. '"name"' instead of 'name').  Strip quotes and
        # normalise to lowercase so downstream code works uniformly.
        raw_cols = list(df.columns)
        df.columns = [c.strip('"').lower() for c in df.columns]
        # Store debug info in session state
        if "_debug_queries" not in st.session_state:
            st.session_state["_debug_queries"] = []
        st.session_state["_debug_queries"].append(
            {"sql": sql[:80], "raw_cols": raw_cols, "rows": len(df)}
        )
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        if "_debug_queries" not in st.session_state:
            st.session_state["_debug_queries"] = []
        st.session_state["_debug_queries"].append(
            {"sql": sql[:80], "raw_cols": [], "rows": -1, "error": str(e)}
        )
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Prefect API helpers
# ---------------------------------------------------------------------------


def prefect_api_request(
    method: str, endpoint: str, payload: dict | None = None
) -> dict | list | None:
    """Make an authenticated request to the Prefect API via SPCS endpoint."""
    url = f"{PREFECT_API_URL}{endpoint}"
    headers = {"Content-Type": "application/json"}
    pat = _get_pat()
    if pat:
        headers["Authorization"] = f'Snowflake Token="{pat}"'
    try:
        resp = requests.request(method, url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_flow_runs(limit: int = 50, hours_back: int = 24) -> pd.DataFrame:
    """Fetch recent flow runs from Prefect API."""
    since = (datetime.now(UTC) - timedelta(hours=hours_back)).isoformat()
    data = prefect_api_request(
        "POST",
        "/flow_runs/filter",
        {
            "sort": "START_TIME_DESC",
            "limit": limit,
            "flow_runs": {"operator": "and_", "start_time": {"after_": since}},
        },
    )
    if not data:
        return pd.DataFrame()
    rows = []
    for r in data:
        duration = None
        if r.get("total_run_time"):
            duration = r["total_run_time"]
        rows.append(
            {
                "id": r["id"][:8],
                "name": r.get("name", ""),
                "flow_id": r.get("flow_id", "")[:8],
                "state": r.get("state", {}).get("type", "UNKNOWN"),
                "state_name": r.get("state", {}).get("name", "Unknown"),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "duration_s": duration,
                "work_pool": r.get("work_pool_name", ""),
                "deployment": r.get("deployment_id", "")[:8] if r.get("deployment_id") else "",
            }
        )
    return pd.DataFrame(rows)


def get_work_pools_status() -> pd.DataFrame:
    """Fetch work pool status from Prefect API."""
    data = prefect_api_request("POST", "/work_pools/filter", {})
    if not data:
        return pd.DataFrame()
    rows = []
    for p in data:
        rows.append(
            {
                "name": p["name"],
                "type": p.get("type", ""),
                "status": p.get("status", "UNKNOWN"),
                "is_paused": p.get("is_paused", False),
                "concurrency_limit": p.get("concurrency_limit"),
                "default_queue_id": str(p.get("default_queue_id", ""))[:8],
            }
        )
    return pd.DataFrame(rows)


def get_work_pool_workers(pool_name: str) -> list[dict]:
    """Fetch workers for a specific work pool."""
    data = prefect_api_request("POST", f"/work_pools/{pool_name}/workers/filter", {})
    return data or []


def get_deployments() -> pd.DataFrame:
    """Fetch all deployments from Prefect API."""
    data = prefect_api_request("POST", "/deployments/filter", {"limit": 100})
    if not data:
        return pd.DataFrame()
    rows = []
    for d in data:
        schedule_active = False
        schedules = d.get("schedules", [])
        if schedules:
            schedule_active = any(s.get("active", False) for s in schedules)
        rows.append(
            {
                "name": d["name"],
                "flow_id": d.get("flow_id", "")[:8],
                "work_pool": d.get("work_pool_name", ""),
                "is_schedule_active": schedule_active,
                "paused": d.get("paused", False),
                "tags": ", ".join(d.get("tags", [])),
                "created": d.get("created"),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# SPCS data fetchers
# ---------------------------------------------------------------------------


def get_service_containers() -> pd.DataFrame:
    """Get container status for all Prefect SPCS services."""
    frames = []
    for svc in PREFECT_SERVICES:
        df = run_query(
            f"SHOW SERVICE CONTAINERS IN SERVICE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{svc}"
        )
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def get_service_summary() -> pd.DataFrame:
    """Get service-level status from SHOW SERVICES."""
    df = run_query(f"SHOW SERVICES IN SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    if df.empty:
        return df
    mask = df["name"].isin(PREFECT_SERVICES)
    return df[mask].reset_index(drop=True)


def get_compute_pools() -> pd.DataFrame:
    """Get compute pool status."""
    df = run_query("SHOW COMPUTE POOLS")
    if df.empty:
        return df
    mask = df["name"].isin(PREFECT_COMPUTE_POOLS)
    return df[mask].reset_index(drop=True)


def get_service_instances() -> pd.DataFrame:
    """Get instance details for all Prefect services."""
    frames = []
    for svc in PREFECT_SERVICES:
        df = run_query(
            f"SHOW SERVICE INSTANCES IN SERVICE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{svc}"
        )
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def get_service_metrics(service: str, hours: int = 1) -> pd.DataFrame:
    """Get platform metrics for a service (requires platformMonitor)."""
    return run_query(f"""
        SELECT * FROM TABLE({SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{service}!SPCS_GET_METRICS(
            start_time => dateadd('hour', -{hours}, current_timestamp())
        ))
    """)


def get_service_events(service: str, hours: int = 6) -> pd.DataFrame:
    """Get lifecycle events for a service."""
    return run_query(f"""
        SELECT * FROM TABLE({SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{service}!SPCS_GET_EVENTS(
            start_time => dateadd('hour', -{hours}, current_timestamp())
        ))
    """)


def get_service_logs(service: str, minutes: int = 30) -> pd.DataFrame:
    """Get recent container logs for a service."""
    return run_query(f"""
        SELECT * FROM TABLE({SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{service}!SPCS_GET_LOGS(
            start_time => dateadd('minute', -{minutes}, current_timestamp())
        ))
        ORDER BY TIMESTAMP DESC
        LIMIT 200
    """)


def get_event_table_logs(hours: int = 1) -> pd.DataFrame:
    """Get aggregated log counts from the event table."""
    return run_query(f"""
        SELECT
            RESOURCE_ATTRIBUTES:"snow.service.name"::varchar AS service_name,
            RESOURCE_ATTRIBUTES:"snow.service.container.name"::varchar AS container_name,
            DATE_TRUNC('minute', timestamp) AS minute,
            COUNT(*) AS log_count,
            SUM(CASE WHEN RECORD:severity_text::varchar IN ('ERROR','FATAL','CRITICAL')
                     THEN 1 ELSE 0 END) AS error_count
        FROM snowflake.telemetry.events
        WHERE timestamp > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
          AND RECORD_TYPE = 'LOG'
          AND RESOURCE_ATTRIBUTES:"snow.service.name" IN
              ('PF_SERVER','PF_WORKER','PF_REDIS','PF_SERVICES')
        GROUP BY 1, 2, 3
        ORDER BY 3 DESC
    """)


def get_event_table_metrics(hours: int = 1) -> pd.DataFrame:
    """Get CPU/memory metrics from the event table (requires platformMonitor enabled)."""
    return run_query(f"""
        SELECT
            timestamp,
            RESOURCE_ATTRIBUTES:"snow.service.name"::varchar AS service_name,
            RESOURCE_ATTRIBUTES:"snow.service.container.name"::varchar AS container_name,
            RECORD:"metric"."name"::varchar AS metric_name,
            VALUE::float AS metric_value
        FROM snowflake.telemetry.events
        WHERE timestamp > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
          AND RECORD_TYPE = 'METRIC'
          AND RESOURCE_ATTRIBUTES:"snow.service.name" IN
              ('PF_SERVER','PF_WORKER','PF_REDIS','PF_SERVICES')
        ORDER BY timestamp DESC
    """)


def get_event_table_events(hours: int = 6) -> pd.DataFrame:
    """Get lifecycle events from event table."""
    return run_query(f"""
        SELECT
            timestamp,
            RESOURCE_ATTRIBUTES:"snow.service.name"::varchar AS service_name,
            RESOURCE_ATTRIBUTES:"snow.service.container.name"::varchar AS container_name,
            RECORD:severity_text::varchar AS severity,
            RECORD:name::varchar AS event_name,
            VALUE AS event_details
        FROM snowflake.telemetry.events
        WHERE timestamp > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
          AND RECORD_TYPE = 'EVENT'
          AND RESOURCE_ATTRIBUTES:"snow.service.name" IN
              ('PF_SERVER','PF_WORKER','PF_REDIS','PF_SERVICES')
        ORDER BY timestamp DESC
    """)


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------


def render_kpi(col, label: str, value, delta: str | None = None, status: str | None = None):
    """Render a KPI metric in a column."""
    if status:
        color = STATUS_COLORS.get(status, "#757575")
        col.markdown(
            f"<div style='text-align:center;'>"
            f"<div style='font-size:0.85rem; color:#888;'>{label}</div>"
            f"<div style='font-size:2rem; font-weight:bold; color:{color};'>{value}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    else:
        col.metric(label, value, delta=delta)


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------


def page_overview():
    st.header("Platform Overview")

    # --- Top-level KPIs ---
    services_df = get_service_containers()
    pools_df = get_compute_pools()

    col1, col2, col3, col4, col5 = st.columns(5)

    # Services health
    if not services_df.empty:
        running = len(services_df[services_df["status"] == "READY"])
        total = len(services_df)
        total_restarts = services_df["restart_count"].astype(int).sum()
        render_kpi(
            col1,
            "SPCS Containers",
            f"{running}/{total}",
            status="READY" if running == total else "FAILED",
        )
        render_kpi(
            col2,
            "Container Restarts",
            str(total_restarts),
            status="READY" if total_restarts == 0 else "FAILED",
        )
    else:
        render_kpi(col1, "SPCS Containers", "N/A")
        render_kpi(col2, "Container Restarts", "N/A")

    # Compute pools
    if not pools_df.empty:
        active_pools = len(pools_df[pools_df["state"] == "ACTIVE"])
        total_nodes = pools_df["active_nodes"].astype(int).sum()
        render_kpi(
            col3,
            "Compute Pools",
            f"{active_pools}/{len(pools_df)}",
            status="READY" if active_pools == len(pools_df) else "PENDING",
        )
        render_kpi(col4, "Active Nodes", str(total_nodes))
    else:
        render_kpi(col3, "Compute Pools", "N/A")
        render_kpi(col4, "Active Nodes", "N/A")

    # Flow runs (from Prefect API)
    flow_runs_df = get_flow_runs(limit=100, hours_back=24)
    if not flow_runs_df.empty:
        completed = len(flow_runs_df[flow_runs_df["state"] == "COMPLETED"])
        failed = len(flow_runs_df[flow_runs_df["state"] == "FAILED"])
        total_runs = len(flow_runs_df)
        success_rate = f"{completed / total_runs * 100:.0f}%" if total_runs > 0 else "N/A"
        render_kpi(
            col5,
            "Flow Runs (24h)",
            f"{total_runs} ({success_rate})",
            status="READY" if failed == 0 else "FAILED",
        )
    else:
        render_kpi(col5, "Flow Runs (24h)", "N/A")

    st.divider()

    # --- Service Status Table ---
    st.subheader("Service Health")
    if not services_df.empty:
        display_df = services_df[
            [
                "service_name",
                "container_name",
                "status",
                "message",
                "restart_count",
                "start_time",
                "image_name",
            ]
        ].copy()
        display_df["image"] = display_df["image_name"].apply(
            lambda x: x.split("/")[-1] if pd.notna(x) else ""
        )
        display_df["uptime"] = display_df["start_time"].apply(_calc_uptime)
        display_df = display_df.drop(columns=["image_name"])
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "restart_count": st.column_config.NumberColumn("Restarts", format="%d"),
                "status": st.column_config.TextColumn("Status"),
            },
            hide_index=True,
        )

    # --- Compute Pool Table ---
    st.subheader("Compute Pools")
    if not pools_df.empty:
        pool_display = pools_df[
            [
                "name",
                "state",
                "instance_family",
                "min_nodes",
                "max_nodes",
                "active_nodes",
                "idle_nodes",
                "num_services",
                "num_jobs",
                "auto_suspend_secs",
            ]
        ].copy()
        pool_display.columns = [
            "Pool",
            "State",
            "Instance Type",
            "Min",
            "Max",
            "Active Nodes",
            "Idle Nodes",
            "Services",
            "Jobs",
            "Auto-Suspend (s)",
        ]
        st.dataframe(pool_display, use_container_width=True, hide_index=True)

    # --- Flow Run Summary ---
    st.subheader("Recent Flow Runs")
    if not flow_runs_df.empty:
        # State distribution chart
        state_counts = flow_runs_df["state_name"].value_counts().reset_index()
        state_counts.columns = ["State", "Count"]
        color_map = {
            "Completed": "#00c853",
            "Failed": "#f44336",
            "Running": "#2196f3",
            "Pending": "#ff9800",
            "Cancelled": "#9e9e9e",
            "Cancelling": "#ff5722",
            "Crashed": "#d32f2f",
        }
        col_chart, col_table = st.columns([1, 2])
        with col_chart:
            fig = px.pie(
                state_counts,
                names="State",
                values="Count",
                color="State",
                color_discrete_map=color_map,
                hole=0.4,
            )
            fig.update_layout(
                margin=dict(t=20, b=20, l=20, r=20),
                height=280,
                legend=dict(orientation="h", y=-0.1),
            )
            st.plotly_chart(fig, use_container_width=True)
        with col_table:
            runs_display = (
                flow_runs_df[["name", "state_name", "work_pool", "start_time", "duration_s"]]
                .head(15)
                .copy()
            )
            runs_display.columns = [
                "Run Name",
                "State",
                "Work Pool",
                "Start Time",
                "Duration (s)",
            ]
            st.dataframe(runs_display, use_container_width=True, hide_index=True)
    else:
        st.info("No flow runs in the last 24 hours, or Prefect API is unreachable.")


def _calc_uptime(start_time_str: str | None) -> str:
    """Calculate human-readable uptime from a start time string."""
    if not start_time_str or pd.isna(start_time_str):
        return "N/A"
    try:
        clean = str(start_time_str).strip('"')
        start = pd.Timestamp(clean)
        if start.tz is None:
            start = start.tz_localize("UTC")
        now = pd.Timestamp.now(tz="UTC")
        delta = now - start
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        mins = rem // 60
        if days > 0:
            return f"{days}d {hours}h {mins}m"
        return f"{hours}h {mins}m"
    except Exception:
        return "N/A"


# ---------------------------------------------------------------------------
# Page: Compute Pools
# ---------------------------------------------------------------------------


def page_compute_pools():
    st.header("Compute Pools")

    pools_df = get_compute_pools()
    if pools_df.empty:
        st.warning("No compute pool data available.")
        return

    # Capacity gauges
    cols = st.columns(len(pools_df))
    for i, (_, pool) in enumerate(pools_df.iterrows()):
        with cols[i]:
            active = int(pool["active_nodes"])
            max_n = int(pool["max_nodes"])
            utilization = active / max_n * 100 if max_n > 0 else 0

            fig = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=utilization,
                    number={"suffix": "%"},
                    title={"text": pool["name"].replace("PREFECT_", "")},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": "#2196f3"},
                        "steps": [
                            {"range": [0, 60], "color": "#e3f2fd"},
                            {"range": [60, 85], "color": "#fff3e0"},
                            {"range": [85, 100], "color": "#ffebee"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 4},
                            "thickness": 0.75,
                            "value": 90,
                        },
                    },
                )
            )
            fig.update_layout(height=220, margin=dict(t=40, b=10, l=30, r=30))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{active}/{max_n} nodes | {pool['instance_family']} | {pool['state']}")

    st.divider()

    # Instance details
    instances_df = get_service_instances()
    st.subheader("Service Instances")
    if not instances_df.empty:
        display_cols = [
            c
            for c in [
                "service_name",
                "instance_id",
                "status",
                "ip_address",
                "creation_time",
                "start_time",
            ]
            if c in instances_df.columns
        ]
        st.dataframe(instances_df[display_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No service instance data available.")


# ---------------------------------------------------------------------------
# Page: Services
# ---------------------------------------------------------------------------


def page_services():
    st.header("SPCS Services")

    selected_service = st.selectbox(
        "Select Service",
        PREFECT_SERVICES,
        format_func=lambda x: f"{x} ({'worker' if 'WORKER' in x else 'server' if 'SERVER' in x else 'services' if 'SERVICES' in x else 'redis'})",
    )

    tab_status, tab_metrics, tab_logs, tab_events = st.tabs(["Status", "Metrics", "Logs", "Events"])

    # --- Status Tab ---
    with tab_status:
        containers_df = run_query(
            f"SHOW SERVICE CONTAINERS IN SERVICE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{selected_service}"
        )
        if not containers_df.empty:
            for _, c in containers_df.iterrows():
                status = c.get("status", "UNKNOWN")
                color = STATUS_COLORS.get(status, "#757575")
                st.markdown(
                    f"### Container: `{c.get('container_name', 'N/A')}`\n"
                    f"**Status:** <span style='color:{color}; font-weight:bold;'>"
                    f"{status}</span> &mdash; {c.get('message', '')}\n\n"
                    f"**Image:** `{str(c.get('image_name', '')).split('/')[-1]}`\n\n"
                    f"**Restarts:** {c.get('restart_count', 0)} | "
                    f"**Started:** {c.get('start_time', 'N/A')} | "
                    f"**Uptime:** {_calc_uptime(c.get('start_time'))}\n\n"
                    f"**Image Digest:** `{str(c.get('image_digest', ''))[:20]}...`",
                    unsafe_allow_html=True,
                )
        else:
            st.warning("No container data available.")

    # --- Metrics Tab ---
    with tab_metrics:
        hours = st.slider("Hours back", 1, 24, 1, key="metrics_hours")

        # Try SPCS_GET_METRICS first
        metrics_df = get_service_metrics(selected_service, hours)
        if not metrics_df.empty:
            st.success(f"Found {len(metrics_df)} metric data points via SPCS_GET_METRICS")

            # CPU chart
            cpu_df = metrics_df[metrics_df["metric_name"] == "container.cpu.usage"]
            if not cpu_df.empty:
                fig = px.line(
                    cpu_df,
                    x="timestamp",
                    y="value",
                    color="container_name",
                    title="CPU Usage (cores)",
                )
                fig.update_layout(height=300, margin=dict(t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

            # Memory chart
            mem_df = metrics_df[metrics_df["metric_name"] == "container.memory.usage"]
            if not mem_df.empty:
                mem_df = mem_df.copy()
                mem_df["value_mb"] = pd.to_numeric(mem_df["value"], errors="coerce") / 1024 / 1024
                fig = px.line(
                    mem_df,
                    x="timestamp",
                    y="value_mb",
                    color="container_name",
                    title="Memory Usage (MB)",
                )
                fig.update_layout(height=300, margin=dict(t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

            # All metrics table
            with st.expander("Raw Metrics"):
                st.dataframe(metrics_df, use_container_width=True, hide_index=True)
        else:
            # Fallback to event table
            et_metrics = get_event_table_metrics(hours)
            if not et_metrics.empty:
                filtered = et_metrics[et_metrics["service_name"] == selected_service]
                if not filtered.empty:
                    st.success(f"Found {len(filtered)} metric data points via event table")
                    for metric_name in filtered["metric_name"].unique():
                        mdf = filtered[filtered["metric_name"] == metric_name]
                        fig = px.line(
                            mdf,
                            x="timestamp",
                            y="metric_value",
                            color="container_name",
                            title=metric_name,
                        )
                        fig.update_layout(height=250, margin=dict(t=40, b=20))
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(
                        "No metrics found for this service. Metrics require `platformMonitor` "
                        "in the service spec. Specs have been updated -- ALTER the services to apply."
                    )
            else:
                st.info(
                    "No metrics available. After ALTERing services with the updated specs "
                    "(which now include `platformMonitor`), metrics will appear here within minutes."
                )

    # --- Logs Tab ---
    with tab_logs:
        minutes = st.slider("Minutes back", 5, 120, 30, key="logs_minutes")
        log_level = st.multiselect(
            "Filter log level",
            ["INFO", "WARNING", "ERROR", "DEBUG", "FATAL"],
            default=["INFO", "WARNING", "ERROR"],
        )

        logs_df = get_service_logs(selected_service, minutes)
        if not logs_df.empty:
            st.caption(f"{len(logs_df)} log entries in the last {minutes} minutes")

            # Error count summary
            if "log" in logs_df.columns:
                error_count = (
                    logs_df["log"]
                    .str.contains("ERROR|FATAL|Exception|Traceback", case=False, na=False)
                    .sum()
                )
                warn_count = logs_df["log"].str.contains("WARNING|WARN", case=False, na=False).sum()
                c1, c2, c3 = st.columns(3)
                render_kpi(c1, "Total Lines", str(len(logs_df)))
                render_kpi(
                    c2,
                    "Errors",
                    str(error_count),
                    status="READY" if error_count == 0 else "FAILED",
                )
                render_kpi(
                    c3,
                    "Warnings",
                    str(warn_count),
                    status="READY" if warn_count == 0 else "PENDING",
                )

                # Filter by level
                if log_level:
                    pattern = "|".join(log_level)
                    mask = logs_df["log"].str.contains(pattern, case=False, na=False)
                    logs_df = logs_df[mask]

                # Log viewer
                for _, row in logs_df.iterrows():
                    log_line = str(row.get("log", ""))
                    ts = str(row.get("timestamp", "")).strip('"')[:19]
                    container = row.get("container_name", "")

                    # Color-code by level
                    if "ERROR" in log_line or "FATAL" in log_line or "Exception" in log_line:
                        color = "#f44336"
                    elif "WARNING" in log_line or "WARN" in log_line:
                        color = "#ff9800"
                    elif "DEBUG" in log_line:
                        color = "#9e9e9e"
                    else:
                        color = "#e0e0e0"

                    st.markdown(
                        f"<code style='color:{color}; font-size:0.78rem;'>"
                        f"[{ts}] [{container}] {log_line}</code>",
                        unsafe_allow_html=True,
                    )
        else:
            st.info("No logs available for this time range.")

    # --- Events Tab ---
    with tab_events:
        event_hours = st.slider("Hours back", 1, 48, 6, key="events_hours")
        events_df = get_service_events(selected_service, event_hours)
        if not events_df.empty:
            st.dataframe(events_df, use_container_width=True, hide_index=True)
        else:
            # Try event table
            et_events = get_event_table_events(event_hours)
            if not et_events.empty:
                filtered = et_events[et_events["service_name"] == selected_service]
                if not filtered.empty:
                    st.dataframe(filtered, use_container_width=True, hide_index=True)
                else:
                    st.info(
                        "No lifecycle events found for this service in the selected time range."
                    )
            else:
                st.info(
                    "No events found. Container lifecycle events (start/stop/restart) appear here."
                )


# ---------------------------------------------------------------------------
# Page: Work Pools & Hybrid Workers
# ---------------------------------------------------------------------------


def page_work_pools():
    st.header("Work Pools & Hybrid Workers")

    pools_df = get_work_pools_status()
    if pools_df.empty:
        st.warning(
            "Cannot reach Prefect API. Check that the dashboard EAI allows the SPCS endpoint."
        )
        return

    # Work pool overview
    st.subheader("Work Pool Status")
    cols = st.columns(len(pools_df))
    for i, (_, pool) in enumerate(pools_df.iterrows()):
        with cols[i]:
            status = pool.get("status", "UNKNOWN")
            name = pool["name"]
            pool_type = pool.get("type", "")

            is_paused = pool.get("is_paused", False)
            display_status = "SUSPENDED" if is_paused else status

            color = STATUS_COLORS.get(display_status, "#757575")
            st.markdown(
                f"<div style='text-align:center; padding:10px; border:1px solid #333; border-radius:8px;'>"
                f"<div style='font-size:0.8rem; color:#888;'>{pool_type}</div>"
                f"<div style='font-size:1.2rem; font-weight:bold;'>{name}</div>"
                f"<div style='font-size:1.5rem; color:{color};'>{display_status}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.divider()

    # Worker details per pool
    st.subheader("Workers")
    for pool_name in WORK_POOLS:
        workers = get_work_pool_workers(pool_name)
        with st.expander(f"{pool_name} ({len(workers)} worker{'s' if len(workers) != 1 else ''})"):
            if workers:
                worker_rows = []
                for w in workers:
                    last_seen = w.get("last_heartbeat_time", "N/A")
                    worker_rows.append(
                        {
                            "name": w.get("name", ""),
                            "status": w.get("status", "UNKNOWN"),
                            "last_heartbeat": last_seen,
                            "created": w.get("created", ""),
                        }
                    )
                st.dataframe(pd.DataFrame(worker_rows), use_container_width=True, hide_index=True)
            else:
                st.info(f"No workers registered for {pool_name}.")

    st.divider()

    # Deployments
    st.subheader("Deployments")
    deployments_df = get_deployments()
    if not deployments_df.empty:
        st.caption(f"{len(deployments_df)} deployments")
        st.dataframe(deployments_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: Flow Runs
# ---------------------------------------------------------------------------


def page_flow_runs():
    st.header("Flow Runs")

    col_hours, col_limit = st.columns(2)
    hours_back = col_hours.slider("Hours back", 1, 168, 24, key="fr_hours")
    limit = col_limit.slider("Max runs", 10, 500, 100, key="fr_limit")

    flow_runs_df = get_flow_runs(limit=limit, hours_back=hours_back)
    if flow_runs_df.empty:
        st.warning("No flow runs found, or Prefect API is unreachable.")
        return

    # State breakdown
    st.subheader("State Distribution")
    state_counts = flow_runs_df["state_name"].value_counts().reset_index()
    state_counts.columns = ["State", "Count"]
    color_map = {
        "Completed": "#00c853",
        "Failed": "#f44336",
        "Running": "#2196f3",
        "Pending": "#ff9800",
        "Cancelled": "#9e9e9e",
        "Crashed": "#d32f2f",
    }
    fig = px.bar(state_counts, x="State", y="Count", color="State", color_discrete_map=color_map)
    fig.update_layout(height=250, margin=dict(t=20, b=20), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Runs by work pool
    st.subheader("Runs by Work Pool")
    pool_counts = flow_runs_df["work_pool"].value_counts().reset_index()
    pool_counts.columns = ["Work Pool", "Count"]
    fig = px.bar(pool_counts, x="Work Pool", y="Count", color="Work Pool")
    fig.update_layout(height=250, margin=dict(t=20, b=20), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Duration analysis (for completed runs)
    completed = flow_runs_df[flow_runs_df["state"] == "COMPLETED"].copy()
    if not completed.empty and "duration_s" in completed.columns:
        completed["duration_s"] = pd.to_numeric(completed["duration_s"], errors="coerce")
        valid = completed.dropna(subset=["duration_s"])
        if not valid.empty:
            st.subheader("Duration Analysis (Completed Runs)")
            c1, c2, c3 = st.columns(3)
            c1.metric("Mean", f"{valid['duration_s'].mean():.1f}s")
            c2.metric("Median", f"{valid['duration_s'].median():.1f}s")
            c3.metric("P95", f"{valid['duration_s'].quantile(0.95):.1f}s")

            fig = px.histogram(
                valid, x="duration_s", nbins=30, title="Duration Distribution (seconds)"
            )
            fig.update_layout(height=250, margin=dict(t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.subheader("All Runs")
    st.dataframe(flow_runs_df, use_container_width=True, hide_index=True)

    # Failed runs detail
    failed = flow_runs_df[flow_runs_df["state"] == "FAILED"]
    if not failed.empty:
        st.subheader(f"Failed Runs ({len(failed)})")
        st.dataframe(
            failed[["name", "work_pool", "start_time", "duration_s"]],
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# Page: Logs & Events
# ---------------------------------------------------------------------------


def page_logs_events():
    st.header("Logs & Events (Aggregated)")

    tab_logs, tab_events, tab_log_rate = st.tabs(["Log Volume", "Lifecycle Events", "Error Rate"])

    with tab_logs:
        hours = st.slider("Hours back", 1, 24, 1, key="agg_log_hours")
        log_agg = get_event_table_logs(hours)
        if not log_agg.empty:
            # Log volume over time
            by_minute = (
                log_agg.groupby(["minute", "service_name"])
                .agg({"log_count": "sum", "error_count": "sum"})
                .reset_index()
            )
            fig = px.area(
                by_minute,
                x="minute",
                y="log_count",
                color="service_name",
                title="Log Volume per Minute",
            )
            fig.update_layout(height=350, margin=dict(t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

            # Log count by service
            by_service = (
                log_agg.groupby("service_name")
                .agg({"log_count": "sum", "error_count": "sum"})
                .reset_index()
            )
            by_service.columns = ["Service", "Total Logs", "Error Logs"]
            st.dataframe(by_service, use_container_width=True, hide_index=True)
        else:
            st.info("No log data found in the event table for this time range.")

    with tab_events:
        event_hours = st.slider("Hours back", 1, 48, 6, key="agg_event_hours")
        events = get_event_table_events(event_hours)
        if not events.empty:
            st.caption(f"{len(events)} events")
            st.dataframe(events, use_container_width=True, hide_index=True)
        else:
            st.info("No lifecycle events found.")

    with tab_log_rate:
        hours = st.slider("Hours back", 1, 24, 3, key="err_rate_hours")
        log_agg = get_event_table_logs(hours)
        if not log_agg.empty:
            by_minute = (
                log_agg.groupby(["minute", "service_name"])
                .agg({"error_count": "sum"})
                .reset_index()
            )
            fig = px.line(
                by_minute,
                x="minute",
                y="error_count",
                color="service_name",
                title="Error Rate per Minute",
            )
            fig.update_layout(height=350, margin=dict(t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No error data found.")


# ---------------------------------------------------------------------------
# Page: Metrics (Resource Utilization)
# ---------------------------------------------------------------------------


def page_metrics():
    st.header("Resource Metrics")

    hours = st.slider("Hours back", 1, 24, 1, key="resource_hours")

    # Try event table for all services
    et_metrics = get_event_table_metrics(hours)
    if not et_metrics.empty:
        # CPU chart
        cpu_df = et_metrics[et_metrics["metric_name"] == "container.cpu.usage"]
        if not cpu_df.empty:
            st.subheader("CPU Usage (cores)")
            fig = px.line(
                cpu_df,
                x="timestamp",
                y="metric_value",
                color="service_name",
                line_dash="container_name",
            )
            fig.update_layout(height=350, margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

        # Memory chart
        mem_df = et_metrics[et_metrics["metric_name"] == "container.memory.usage"]
        if not mem_df.empty:
            st.subheader("Memory Usage")
            mem_df = mem_df.copy()
            mem_df["value_mb"] = mem_df["metric_value"] / 1024 / 1024
            fig = px.line(
                mem_df,
                x="timestamp",
                y="value_mb",
                color="service_name",
                line_dash="container_name",
            )
            fig.update_yaxes(title="MB")
            fig.update_layout(height=350, margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

        # All unique metrics
        st.subheader("All Available Metrics")
        metric_names = et_metrics["metric_name"].unique()
        for mn in sorted(metric_names):
            if mn in ("container.cpu.usage", "container.memory.usage"):
                continue
            subset = et_metrics[et_metrics["metric_name"] == mn]
            with st.expander(f"{mn} ({len(subset)} points)"):
                fig = px.line(subset, x="timestamp", y="metric_value", color="service_name")
                fig.update_layout(height=250, margin=dict(t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(
            "No resource metrics available yet.\n\n"
            "**To enable metrics:**\n"
            "1. The service specs now include `platformMonitor` with `system` + `system_limits` groups.\n"
            "2. ALTER each service to apply the updated spec.\n"
            "3. Metrics will start flowing to the event table within minutes.\n\n"
            "**ALTER commands:**\n"
            "```sql\n"
            "ALTER SERVICE PF_SERVER FROM SPECIFICATION_FILE = 'pf_server.yaml';\n"
            "ALTER SERVICE PF_SERVICES FROM SPECIFICATION_FILE = 'pf_services.yaml';\n"
            "ALTER SERVICE PF_REDIS FROM SPECIFICATION_FILE = 'pf_redis.yaml';\n"
            "ALTER SERVICE PF_WORKER FROM SPECIFICATION_FILE = 'pf_worker.yaml';\n"
            "```"
        )

    # Per-service SPCS_GET_METRICS fallback
    st.divider()
    st.subheader("Per-Service Metrics (SPCS_GET_METRICS)")
    for svc in PREFECT_SERVICES:
        df = get_service_metrics(svc, hours)
        if not df.empty:
            with st.expander(f"{svc} ({len(df)} data points)"):
                st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Prefect SPCS Observability",
        page_icon=":mag:",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Sidebar navigation
    with st.sidebar:
        st.title("SPCS Observability")
        st.caption("Prefect on Snowpark Container Services")

        page = st.radio(
            "Navigate",
            [
                "Overview",
                "Compute Pools",
                "Services",
                "Work Pools & Workers",
                "Flow Runs",
                "Logs & Events",
                "Resource Metrics",
            ],
            index=0,
        )

        st.divider()

        # Auto-refresh
        auto_refresh = st.checkbox("Auto-refresh", value=False)
        refresh_interval = st.selectbox("Interval", [15, 30, 60, 120], index=1)

        if st.button("Refresh Now"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption(f"Database: `{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}`")
        st.caption(f"API: `{PREFECT_API_URL[:50]}...`")
        st.caption(f"Runtime: `{'LOCAL' if IS_LOCAL else 'CONTAINER'}`")

        # Debug panel
        with st.expander("Debug: Query Log"):
            debug_queries = st.session_state.get("_debug_queries", [])
            if debug_queries:
                for q in debug_queries:
                    err = q.get("error")
                    if err:
                        st.error(f"`{q['sql']}` -> ERROR: {err}")
                    else:
                        st.code(
                            f"{q['sql']}\n  rows={q['rows']}  raw_cols={q['raw_cols']}",
                            language="text",
                        )
            else:
                st.info("No queries executed yet.")

    # Route pages
    page_map = {
        "Overview": page_overview,
        "Compute Pools": page_compute_pools,
        "Services": page_services,
        "Work Pools & Workers": page_work_pools,
        "Flow Runs": page_flow_runs,
        "Logs & Events": page_logs_events,
        "Resource Metrics": page_metrics,
    }
    page_map[page]()

    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
