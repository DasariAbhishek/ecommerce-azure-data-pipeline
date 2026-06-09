"""Run a few business questions against the served warehouse.

Demonstrates the consumption layer (what a BI analyst / Power BI would query).
Run after the pipeline completes:  python -m ecommerce_pipeline.analyze
"""
from __future__ import annotations

import duckdb

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("analyze")

QUERIES = {
    "Total revenue & orders": """
        SELECT ROUND(SUM(revenue), 2) AS total_revenue,
               SUM(orders)            AS total_orders
        FROM mart_daily_sales
    """,
    "Top 5 categories by revenue": """
        SELECT category, revenue, units_sold
        FROM mart_sales_by_category
        ORDER BY revenue DESC
        LIMIT 5
    """,
    "Monthly revenue trend": """
        SELECT year, month, ROUND(SUM(revenue), 2) AS revenue
        FROM mart_daily_sales
        GROUP BY year, month
        ORDER BY year, month
    """,
    "Top 5 customers by lifetime value": """
        SELECT full_name, country, segment, lifetime_value
        FROM mart_top_customers
        ORDER BY lifetime_value DESC
        LIMIT 5
    """,
}


def main() -> None:
    cfg = load_config()
    con = duckdb.connect(cfg.abs("warehouse"), read_only=True)
    for title, sql in QUERIES.items():
        print(f"\n=== {title} ===")
        print(con.sql(sql).df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
