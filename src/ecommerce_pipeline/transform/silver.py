"""Silver layer: clean, type-cast, deduplicate, conform, explode.

Outputs three conformed, query-ready tables:
  * silver.customers   (deduped, standardized email/country)
  * silver.products    (typed prices, invalid prices dropped)
  * silver.order_items (orders exploded to one row per line item, deduped)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.config import Config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("silver")


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("email", F.lower(F.trim(F.col("email"))))
          .withColumn("country", F.initcap(F.trim(F.col("country"))))
          .withColumn(
              "country",
              F.when(F.col("country") == "", F.lit("Unknown")).otherwise(F.col("country")),
          )
          .withColumn("signup_date", F.to_date("signup_date"))
          .dropDuplicates(["customer_id"])
          .select(
              "customer_id", "full_name", "email", "country", "city",
              "segment", "signup_date",
          )
    )


def clean_products(df: DataFrame) -> DataFrame:
    typed = (
        df.withColumn("unit_price", F.col("unit_price").cast("double"))
          .withColumn("unit_cost", F.col("unit_cost").cast("double"))
    )
    # Drop catalog rows with invalid (<=0) prices — a real data-quality rule.
    return (
        typed.filter(F.col("unit_price") > 0)
             .dropDuplicates(["product_id"])
             .select(
                 "product_id", "product_name", "category", "brand",
                 "unit_price", "unit_cost",
             )
    )


def clean_order_items(orders: DataFrame) -> DataFrame:
    # Deduplicate at-least-once order events: keep the first seen per order_id.
    w = Window.partitionBy("order_id").orderBy("_ingest_ts")
    deduped = (
        orders.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
    )

    exploded = (
        deduped
        .withColumn("item", F.explode("items"))
        .select(
            "order_id",
            "customer_id",
            F.to_timestamp("order_ts").alias("order_ts"),
            "channel",
            "payment_method",
            "status",
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").cast("int").alias("quantity"),
            F.col("item.unit_price").cast("double").alias("unit_price"),
            F.col("item.discount_pct").cast("double").alias("discount_pct"),
        )
    )

    # Derived monetary measures (revenue net of discount).
    enriched = (
        exploded
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "net_amount",
            F.round(F.col("gross_amount") * (1 - F.col("discount_pct")), 2),
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn(
            "order_item_id",
            F.concat_ws("-", "order_id", "product_id"),
        )
    )
    # Keep only sane line items.
    return enriched.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))


def build_silver(spark: SparkSession, cfg: Config) -> None:
    bronze = cfg.abs("bronze")
    silver = cfg.abs("silver")

    customers = clean_customers(spark.read.parquet(f"{bronze}/customers"))
    customers.write.mode("overwrite").parquet(f"{silver}/customers")
    log.info("Silver customers: %s rows", customers.count())

    products = clean_products(spark.read.parquet(f"{bronze}/products"))
    products.write.mode("overwrite").parquet(f"{silver}/products")
    log.info("Silver products: %s rows", products.count())

    order_items = clean_order_items(spark.read.parquet(f"{bronze}/orders"))
    (
        order_items.write.mode("overwrite")
        .partitionBy("order_date")
        .parquet(f"{silver}/order_items")
    )
    log.info("Silver order_items: %s rows", order_items.count())
