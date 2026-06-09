"""End-to-end orchestrator.

Runs the full medallion pipeline in order. This is the local equivalent of the
Azure Data Factory pipeline defined in orchestration/adf/, where each stage
below is an ADF activity (Copy / Databricks Notebook / Stored Proc).

Usage:
    python -m ecommerce_pipeline.pipeline                 # full run
    python -m ecommerce_pipeline.pipeline --stage silver  # single stage
    python -m ecommerce_pipeline.pipeline --no-generate   # reuse landing data
"""
from __future__ import annotations

import argparse
import time

from ecommerce_pipeline.config import load_config
from ecommerce_pipeline.ingestion import bronze, generate_data
from ecommerce_pipeline.quality import checks
from ecommerce_pipeline.transform import gold, serve, silver
from ecommerce_pipeline.utils.logging_utils import get_logger
from ecommerce_pipeline.utils.spark_session import get_spark

log = get_logger("pipeline")


def run_quality_gate(spark, cfg) -> None:
    """Validate Silver before it is allowed to feed Gold."""
    q = cfg.quality
    si = cfg.abs("silver")
    customers = spark.read.parquet(f"{si}/customers")
    products = spark.read.parquet(f"{si}/products")
    order_items = spark.read.parquet(f"{si}/order_items")

    results = [
        checks.expect_unique(customers, "customer_id"),
        checks.expect_unique(products, "product_id"),
        checks.expect_not_null(order_items, "customer_id", q["max_null_ratio"]),
        checks.expect_non_negative(products, "unit_price"),
        checks.expect_non_negative(order_items, "net_amount"),
        checks.expect_min_rows(order_items, q["min_rows_silver_orders"], "order_items"),
    ]
    checks.run_checks(results, q["fail_on_error"])


def main() -> None:
    parser = argparse.ArgumentParser(description="E-commerce medallion pipeline")
    parser.add_argument(
        "--stage",
        choices=["all", "bronze", "silver", "gold", "serve"],
        default="all",
    )
    parser.add_argument("--no-generate", action="store_true",
                        help="Skip synthetic data generation (reuse landing zone)")
    args = parser.parse_args()

    cfg = load_config()
    t0 = time.time()
    log.info("=== Pipeline start: %s ===", cfg.project_name)

    if not args.no_generate and args.stage in ("all", "bronze"):
        log.info(">>> Stage 0: generate source data")
        generate_data.generate(cfg)

    spark = get_spark(cfg.project_name)
    spark.sparkContext.setLogLevel("ERROR")

    try:
        if args.stage in ("all", "bronze"):
            log.info(">>> Stage 1: BRONZE (raw ingestion)")
            bronze.build_bronze(spark, cfg)

        if args.stage in ("all", "silver"):
            log.info(">>> Stage 2: SILVER (clean & conform)")
            silver.build_silver(spark, cfg)
            log.info(">>> Stage 2b: DATA QUALITY GATE")
            run_quality_gate(spark, cfg)

        if args.stage in ("all", "gold"):
            log.info(">>> Stage 3: GOLD (star schema + marts)")
            gold.build_gold(spark, cfg)

        if args.stage in ("all", "serve"):
            log.info(">>> Stage 4: SERVE (publish to warehouse)")
            serve.publish_to_warehouse(cfg)

    finally:
        spark.stop()

    log.info("=== Pipeline complete in %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
