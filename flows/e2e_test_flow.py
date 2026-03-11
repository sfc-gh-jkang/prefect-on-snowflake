"""End-to-end pipeline test: sample data → table → dynamic table → verify → cleanup.

Exercises real Snowflake DDL/DML from a Prefect worker to prove the full
stack works.  Runs identically on SPCS (OAuth token) and GCP (PAT) workers.
"""

import time

from hooks import on_flow_failure
from prefect import flow, task
from shared_utils import execute_ddl, execute_query

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def query_sample_data() -> list:
    """Query TPCH sample data: top 10 customers by order count in 1997."""
    query = """
        SELECT
            c.C_NAME,
            c.C_NATIONKEY,
            COUNT(*)        AS ORDER_COUNT,
            SUM(o.O_TOTALPRICE) AS TOTAL_SPEND
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS   o
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER  c
          ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE o.O_ORDERDATE BETWEEN '1997-01-01' AND '1997-12-31'
        GROUP BY c.C_NAME, c.C_NATIONKEY
        ORDER BY ORDER_COUNT DESC
        LIMIT 10
    """
    columns, rows = execute_query(query)
    print(f"Sample data query returned {len(rows)} rows, columns: {columns}")
    for r in rows:
        print(f"  {r[0]}: {r[2]} orders, ${r[3]:,.2f} total")
    assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"
    print("PASS: query_sample_data")
    return rows


@task
def create_source_table() -> int:
    """Create E2E_ORDERS table from sample data."""
    ddl = """
        CREATE OR REPLACE TABLE PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS AS
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            n.N_NAME        AS NATION,
            COUNT(*)        AS ORDER_COUNT,
            SUM(o.O_TOTALPRICE) AS TOTAL_SPEND
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS   o
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER  c
          ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION    n
          ON c.C_NATIONKEY = n.N_NATIONKEY
        WHERE o.O_ORDERDATE BETWEEN '1997-01-01' AND '1997-12-31'
        GROUP BY c.C_CUSTKEY, c.C_NAME, n.N_NAME
    """
    execute_ddl(ddl)

    # Verify
    columns, rows = execute_query("SELECT COUNT(*) FROM PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS")
    row_count = rows[0][0]
    print(f"E2E_ORDERS created with {row_count} rows")
    assert row_count > 0, "E2E_ORDERS is empty"
    print("PASS: create_source_table")
    return row_count


@task
def create_dynamic_table() -> str:
    """Create E2E_ORDERS_SUMMARY dynamic table aggregating by nation."""
    ddl = """
        CREATE OR REPLACE DYNAMIC TABLE PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS_SUMMARY
            TARGET_LAG = '1 minute'
            WAREHOUSE  = COMPUTE_WH
        AS
        SELECT
            NATION,
            COUNT(*)            AS CUSTOMER_COUNT,
            SUM(ORDER_COUNT)    AS TOTAL_ORDERS,
            SUM(TOTAL_SPEND)    AS TOTAL_SPEND,
            AVG(TOTAL_SPEND)    AS AVG_CUSTOMER_SPEND
        FROM PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS
        GROUP BY NATION
    """
    result = execute_ddl(ddl)
    print(f"Dynamic table DDL result: {result}")
    print("PASS: create_dynamic_table")
    return result


@task
def verify_dynamic_table() -> list:
    """Wait for the dynamic table to refresh, then verify its data."""
    # Poll for up to 90 seconds for the DT to have data
    for attempt in range(1, 19):
        try:
            columns, rows = execute_query(
                "SELECT * FROM PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS_SUMMARY ORDER BY TOTAL_ORDERS DESC"
            )
            if rows:
                print(f"Dynamic table has {len(rows)} nation rows (attempt {attempt})")
                for r in rows:
                    print(f"  {r[0]}: {r[1]} customers, {r[2]} orders, ${r[3]:,.2f}")
                assert len(rows) > 0, "Dynamic table returned 0 rows"
                print("PASS: verify_dynamic_table")
                return rows
        except Exception as e:
            print(f"  Attempt {attempt}: {e}")
        time.sleep(5)

    raise RuntimeError("Dynamic table did not populate within 90 seconds")


@task
def cleanup() -> None:
    """Drop test tables (dynamic table first due to dependency)."""
    execute_ddl("DROP TABLE IF EXISTS PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS_SUMMARY")
    execute_ddl("DROP TABLE IF EXISTS PREFECT_DB.PREFECT_SCHEMA.E2E_ORDERS")
    print("PASS: cleanup — both tables dropped")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="e2e-pipeline-test",
    log_prints=True,
    retries=1,
    retry_delay_seconds=60,
    on_failure=[on_flow_failure],
)
def e2e_pipeline_test(skip_cleanup: bool = False):
    """End-to-end Snowflake pipeline test.

    1. Query sample data (read-only)
    2. Create source table via CTAS
    3. Create dynamic table on top
    4. Verify dynamic table refreshed with data
    5. Clean up both tables (unless skip_cleanup=True)
    """
    print("=" * 60)
    print("E2E PIPELINE TEST — START")
    print("=" * 60)

    sample_rows = query_sample_data()
    row_count = create_source_table()
    create_dynamic_table()
    summary_rows = verify_dynamic_table()
    if not skip_cleanup:
        cleanup()
    else:
        print("Skipping cleanup — tables left for downstream checks.")

    print("=" * 60)
    print("E2E PIPELINE TEST — ALL CHECKS PASSED")
    print("  Sample query:    10 rows")
    print(f"  Source table:    {row_count} rows")
    print(f"  Dynamic table:   {len(summary_rows)} nation summaries")
    print("=" * 60)

    return {
        "sample_rows": len(sample_rows),
        "source_table_rows": row_count,
        "dynamic_table_nations": len(summary_rows),
        "status": "ALL PASSED",
    }


if __name__ == "__main__":
    e2e_pipeline_test()
