"""Quarterly revenue report — deep nested import stress test.

Entrypoint: analytics/reports/quarterly_flow.py:quarterly_report
Working dir: /opt/prefect/flows

Tests 3 import patterns simultaneously:
  1. Sibling import     — from formatters import ...       (via parent_path)
  2. Parent-package     — from analytics.sf_helpers import ... (via working_directory)
  3. Top-level root     — from shared_utils import ...     (via working_directory)
"""

# --- Pattern 2: Parent-package absolute import ---
from analytics.sf_helpers import execute_ddl, execute_query

# --- Pattern 1: Sibling import (same directory: analytics/reports/) ---
from formatters import format_currency, format_row

# --- Hooks ---
from hooks import on_flow_failure
from prefect import flow, task

# --- Pattern 3: Top-level root module import ---
from shared_utils import APP_VERSION, table_name

TABLE = table_name("E2E_QUARTERLY_REVENUE")


@task
def query_quarterly_revenue() -> list:
    """Query 1997 revenue by nation and quarter from TPCH sample data."""
    sql = """
        SELECT
            n.N_NAME AS NATION,
            QUARTER(o.O_ORDERDATE) AS QTR,
            COUNT(*) AS ORDERS,
            SUM(o.O_TOTALPRICE) AS REVENUE
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c
          ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
          ON c.C_NATIONKEY = n.N_NATIONKEY
        WHERE o.O_ORDERDATE BETWEEN '1997-01-01' AND '1997-12-31'
        GROUP BY n.N_NAME, QUARTER(o.O_ORDERDATE)
        ORDER BY REVENUE DESC
    """
    columns, rows = execute_query(sql)
    print(f"Quarterly query: {len(rows)} rows (25 nations x 4 quarters = 100)")
    assert len(rows) == 100, f"Expected 100 rows, got {len(rows)}"

    total = sum(r[3] for r in rows)
    for r in rows[:3]:
        pct = float(r[3]) / float(total) * 100
        print(format_row(f"{r[0]} Q{r[1]}", float(r[3]), pct))
    print(f"  ... ({len(rows) - 3} more)")
    print("PASS: query_quarterly_revenue")
    return rows


@task
def create_quarterly_table(rows: list) -> int:
    """Create E2E_QUARTERLY_REVENUE table."""
    ddl = f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            n.N_NAME AS NATION,
            QUARTER(o.O_ORDERDATE) AS QTR,
            COUNT(*) AS ORDERS,
            SUM(o.O_TOTALPRICE) AS REVENUE
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c
          ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
          ON c.C_NATIONKEY = n.N_NATIONKEY
        WHERE o.O_ORDERDATE BETWEEN '1997-01-01' AND '1997-12-31'
        GROUP BY n.N_NAME, QUARTER(o.O_ORDERDATE)
    """
    execute_ddl(ddl)

    _, count_rows = execute_query(f"SELECT COUNT(*) FROM {TABLE}")
    row_count = count_rows[0][0]
    print(f"Table created: {TABLE} ({row_count} rows)")
    print("  Created by shared_utils.table_name() — top-level import OK")
    assert row_count == 100, f"Expected 100 rows, got {row_count}"
    print("PASS: create_quarterly_table")
    return row_count


@task
def verify_and_cleanup() -> dict:
    """Verify data, format with formatters module, then cleanup."""
    _, rows = execute_query(
        f"SELECT NATION, SUM(REVENUE) AS TOTAL FROM {TABLE} "
        f"GROUP BY NATION ORDER BY TOTAL DESC LIMIT 3"
    )
    top_3 = {}
    total_all = sum(float(r[1]) for r in rows)
    for r in rows:
        pct = float(r[1]) / total_all * 100
        top_3[r[0]] = format_currency(float(r[1]))
        print(format_row(r[0], float(r[1]), pct))
    assert len(top_3) == 3

    execute_ddl(f"DROP TABLE IF EXISTS {TABLE}")
    print("PASS: verify_and_cleanup — table dropped")
    return top_3


@flow(
    name="quarterly-report",
    log_prints=True,
    retries=1,
    retry_delay_seconds=30,
    on_failure=[on_flow_failure],
)
def quarterly_report():
    """Quarterly revenue report — deep nested import stress test.

    Proves:
      - Deep entrypoint (analytics/reports/quarterly_flow.py)
      - Sibling import (from formatters import ...)
      - Parent-package import (from analytics.sf_helpers import ...)
      - Top-level import (from shared_utils import ...)
      - Stage v2 mount serves new files without restart
    """
    print("=" * 60)
    print("QUARTERLY REPORT — DEEP NESTED IMPORT STRESS TEST")
    print(f"  App version: {APP_VERSION}")
    print("=" * 60)

    rows = query_quarterly_revenue()
    row_count = create_quarterly_table(rows)
    top_3 = verify_and_cleanup()

    print("=" * 60)
    print("QUARTERLY REPORT — ALL CHECKS PASSED")
    print(f"  Rows:           {row_count}")
    print(f"  Top 3:          {list(top_3.keys())}")
    print("  Import tests:")
    print("    formatters (sibling):       OK")
    print("    analytics.sf_helpers (pkg):  OK")
    print("    shared_utils (root):         OK")
    print("=" * 60)

    return {
        "rows": row_count,
        "top_3": top_3,
        "imports": {
            "sibling": "formatters OK",
            "parent_package": "analytics.sf_helpers OK",
            "root_module": "shared_utils OK",
        },
        "status": "ALL PASSED",
    }


if __name__ == "__main__":
    quarterly_report()
