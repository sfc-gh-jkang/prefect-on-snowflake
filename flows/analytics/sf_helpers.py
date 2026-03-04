"""Shared Snowflake connection and query helpers for analytics flows.

Thin wrapper around shared_utils — keeps the same public API so that
sibling imports (``from sf_helpers import ...``) and package imports
(``from analytics.sf_helpers import ...``) continue to work.

All connections now include QUERY_TAG for cost attribution.
"""

from shared_utils import (
    execute_ddl as _execute_ddl,
)
from shared_utils import (
    execute_query as _execute_query,
)
from shared_utils import (
    get_snowflake_connection,
)


def get_connection():
    """Return a snowflake.connector connection with QUERY_TAG."""
    return get_snowflake_connection()


def execute_query(sql: str) -> tuple:
    """Execute a query and return (columns, rows)."""
    return _execute_query(sql)


def execute_ddl(sql: str) -> str:
    """Execute a DDL statement and return the status message."""
    return _execute_ddl(sql)
