"""Revenue analytics flow — multi-file subfolder deployment test.

Proves that a flow in a subdirectory can import from a sibling helper
module.  Tests SPCS stage subdirectory mounts and GCP bind mounts.
"""

from hooks import on_flow_failure
from prefect import flow, task

# --- THIS IS THE KEY TEST ---
# Sibling import: Prefect adds the script's parent dir to sys.path,
# so "from sf_helpers import ..." resolves to analytics/sf_helpers.py
from sf_helpers import execute_ddl, execute_query

TABLE = "PREFECT_DB.PREFECT_SCHEMA.E2E_REVENUE_BY_NATION"

REVENUE_BY_NATION_SQL = """
    SELECT
        n.N_NAME        AS NATION,
        COUNT(DISTINCT o.O_CUSTKEY) AS CUSTOMERS,
        COUNT(*)        AS ORDERS,
        SUM(o.O_TOTALPRICE) AS REVENUE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS   o
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER  c
      ON o.O_CUSTKEY = c.C_CUSTKEY
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION    n
      ON c.C_NATIONKEY = n.N_NATIONKEY
    WHERE o.O_ORDERDATE BETWEEN '1997-01-01' AND '1997-12-31'
    GROUP BY n.N_NAME
"""


@task
def get_revenue_by_nation() -> list:
    """Query 1997 revenue by nation from TPCH sample data."""
    sql = REVENUE_BY_NATION_SQL + "    ORDER BY REVENUE DESC"
    columns, rows = execute_query(sql)
    print(f"Revenue query: {len(rows)} nations, columns: {columns}")
    for r in rows[:5]:
        print(f"  {r[0]}: {r[1]} customers, {r[2]} orders, ${r[3]:,.2f}")
    print(f"  ... ({len(rows) - 5} more)")
    assert len(rows) == 25, f"Expected 25 nations, got {len(rows)}"
    print("PASS: get_revenue_by_nation")
    return rows


@task
def create_revenue_table(rows: list) -> int:
    """Create E2E_REVENUE_BY_NATION from the query results."""
    ddl = f"CREATE OR REPLACE TABLE {TABLE} AS {REVENUE_BY_NATION_SQL}"
    execute_ddl(ddl)

    _, count_rows = execute_query(f"SELECT COUNT(*) FROM {TABLE}")
    row_count = count_rows[0][0]
    print(f"E2E_REVENUE_BY_NATION created with {row_count} rows")
    assert row_count == 25, f"Expected 25 rows, got {row_count}"
    print("PASS: create_revenue_table")
    return row_count


@task
def verify_and_cleanup() -> dict:
    """Verify table contents, then clean up."""
    _, rows = execute_query(f"SELECT NATION, REVENUE FROM {TABLE} ORDER BY REVENUE DESC LIMIT 3")
    top_3 = {r[0]: float(r[1]) for r in rows}
    print(f"Top 3 nations by revenue: {top_3}")
    assert len(top_3) == 3

    execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    print("PASS: verify_and_cleanup — table dropped")
    return top_3


@flow(
    name="analytics-revenue",
    log_prints=True,
    retries=2,
    retry_delay_seconds=[10, 20],
    on_failure=[on_flow_failure],
)
def analytics_revenue():
    """Revenue analytics pipeline — multi-file subfolder test.

    Proves:
      - Subdirectory entrypoint (analytics/revenue_flow.py)
      - Sibling module import (from sf_helpers import ...)
      - Full DDL/DML lifecycle
    """
    print("=" * 60)
    print("ANALYTICS REVENUE — MULTI-FILE SUBFOLDER TEST")
    print("=" * 60)

    rows = get_revenue_by_nation()
    row_count = create_revenue_table(rows)
    top_3 = verify_and_cleanup()

    print("=" * 60)
    print("ANALYTICS REVENUE — ALL CHECKS PASSED")
    print(f"  Nations:    {row_count}")
    print(f"  Top 3:      {list(top_3.keys())}")
    print("  Import test: sf_helpers sibling import OK")
    print("=" * 60)

    return {
        "nations": row_count,
        "top_3": top_3,
        "import_test": "sf_helpers sibling import OK",
        "status": "ALL PASSED",
    }


if __name__ == "__main__":
    analytics_revenue()
