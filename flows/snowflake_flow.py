"""Flow that queries Snowflake from the SPCS worker.

Demonstrates Snowflake connectivity from within a Prefect worker
running on Snowpark Container Services.
"""

from hooks import on_flow_failure
from prefect import flow, task
from shared_utils import get_snowflake_connection

# Safe demo queries — no user-supplied SQL allowed.
DEMO_QUERY = "SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_USER() AS user, CURRENT_ROLE() AS role"


@task
def query_snowflake() -> list:
    """Execute the demo query against Snowflake using the shared connection helper.

    Auth strategy (from shared_utils.get_snowflake_connection):
      1. Inside SPCS: uses OAuth token from /snowflake/session/token
      2. SNOWFLAKE_PAT set: uses programmatic access token
      3. Fallback: uses SNOWFLAKE_USER + SNOWFLAKE_PASSWORD env vars

    All queries are tagged with QUERY_TAG for cost attribution.
    """
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(DEMO_QUERY)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print(f"Query returned {len(rows)} rows with columns: {columns}")
        return rows
    finally:
        conn.close()


@flow(
    name="snowflake-etl",
    log_prints=True,
    on_failure=[on_flow_failure],
)
def snowflake_etl():
    """Run a Snowflake query from the Prefect worker."""
    rows = query_snowflake()
    for row in rows:
        print(f"  -> {row}")
    return rows


if __name__ == "__main__":
    snowflake_etl()
