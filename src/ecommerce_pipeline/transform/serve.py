"""Serving layer: publish Gold marts into a SQL warehouse.

Locally this is DuckDB (a zero-config analytical DB). On Azure this step maps
to loading Gold Delta tables into Synapse Serverless / a Dedicated SQL Pool, or
exposing them via Databricks SQL. BI tools (Power BI) connect here.
"""
from __future__ import annotations

import glob
import os

import duckdb

from ecommerce_pipeline.config import Config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("serve")

# Gold datasets to publish as warehouse tables.
GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
    "mart_daily_sales",
    "mart_sales_by_category",
    "mart_top_customers",
]


def publish_to_warehouse(cfg: Config) -> None:
    gold = cfg.abs("gold")
    wh_path = cfg.abs("warehouse")
    os.makedirs(os.path.dirname(wh_path), exist_ok=True)

    con = duckdb.connect(wh_path)
    for tbl in GOLD_TABLES:
        # Parquet may be a directory of part-files; read them all with a glob.
        pattern = os.path.join(gold, tbl, "*.parquet")
        if not glob.glob(pattern):
            log.warning("No parquet found for %s, skipping", tbl)
            continue
        con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{pattern}')")
        n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info("Warehouse table %-24s %s rows", tbl, n)
    con.close()
    log.info("Serving warehouse ready -> %s", wh_path)
