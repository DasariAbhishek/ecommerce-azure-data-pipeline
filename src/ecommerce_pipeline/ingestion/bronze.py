"""Bronze layer: land raw source files into the lake, as-is.

Principles:
  * No business logic, no cleaning. Bronze is the immutable system of record.
  * Add ingestion metadata (_ingest_ts, _source_file) for lineage & auditing.
  * Stored as Parquet locally; on Azure this would be Delta on ADLS Gen2.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from ecommerce_pipeline.config import Config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("bronze")


def _add_audit_cols(df, source_file: str):
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.lit(source_file))
    )


def build_bronze(spark: SparkSession, cfg: Config) -> None:
    landing = cfg.abs("landing")
    bronze = cfg.abs("bronze")

    # customers (CSV)
    customers = spark.read.option("header", True).csv(f"{landing}/customers.csv")
    customers = _add_audit_cols(customers, "customers.csv")
    customers.write.mode("overwrite").parquet(f"{bronze}/customers")
    log.info("Bronze customers: %s rows", customers.count())

    # products (CSV)
    products = spark.read.option("header", True).csv(f"{landing}/products.csv")
    products = _add_audit_cols(products, "products.csv")
    products.write.mode("overwrite").parquet(f"{bronze}/products")
    log.info("Bronze products: %s rows", products.count())

    # orders (JSON Lines, nested)
    orders = spark.read.json(f"{landing}/orders.json")
    orders = _add_audit_cols(orders, "orders.json")
    orders.write.mode("overwrite").parquet(f"{bronze}/orders")
    log.info("Bronze orders: %s rows", orders.count())
