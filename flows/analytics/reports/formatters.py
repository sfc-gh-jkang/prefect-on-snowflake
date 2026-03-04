"""Report formatting helpers — sibling module in analytics/reports/.

Imported by quarterly_flow.py via sibling import (parent_path on sys.path).
"""


def format_currency(value: float) -> str:
    """Format a number as USD currency."""
    return f"${value:,.2f}"


def format_pct(value: float) -> str:
    """Format a fraction as a percentage."""
    return f"{value:.1f}%"


def format_row(nation: str, revenue: float, pct: float) -> str:
    """Format a single nation revenue row for display."""
    return f"  {nation:<20s} {format_currency(revenue):>20s}  ({format_pct(pct)})"
