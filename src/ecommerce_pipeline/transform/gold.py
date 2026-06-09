"""Gold layer: star schema + curated business marts.

Dimensional model (Kimball-style):
  * dim_customer
  * dim_product
  * dim_date
  * fact_sales            (grain = one order line item)

Business marts (ready for BI / Power BI):
  * mart_daily_sales
  * mart_sales_by_category
  * mart_top_customers
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.config import Config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("gold")


def build_dim_date(order_items: DataFrame) -> DataFrame:
    return (
        order_items.select("order_date")
        .distinct()
        .filter(F.col("order_date").isNotNull())
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("order_date"))
        .withColumn("quarter", F.quarter("order_date"))
        .withColumn("month", F.month("order_date"))
        .withColumn("month_name", F.date_format("order_date", "MMMM"))
        .withColumn("day_of_week", F.date_format("order_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("order_date").isin(1, 7))
        .select(
            "date_key", "order_date", "year", "quarter", "month",
            "month_name", "day_of_week", "is_weekend",
        )
    )


def build_fact_sales(order_items: DataFrame) -> DataFrame:
    return (
        order_items
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .select(
            "order_item_id", "order_id", "customer_id", "product_id",
            "date_key", "order_ts", "channel", "payment_method", "status",
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "net_amount",
        )
    )


def build_gold(spark: SparkSession, cfg: Config) -> None:
    silver = cfg.abs("silver")
    gold = cfg.abs("gold")

    customers = spark.read.parquet(f"{silver}/customers")
    products = spark.read.parquet(f"{silver}/products")
    order_items = spark.read.parquet(f"{silver}/order_items")

    # ---- dimensions ---------------------------------------------------------
    dim_customer = customers.select(
        "customer_id", "full_name", "email", "country", "city",
        "segment", "signup_date",
    )
    dim_product = products.select(
        "product_id", "product_name", "category", "brand",
        "unit_price", "unit_cost",
    )
    dim_date = build_dim_date(order_items)

    dim_customer.write.mode("overwrite").parquet(f"{gold}/dim_customer")
    dim_product.write.mode("overwrite").parquet(f"{gold}/dim_product")
    dim_date.write.mode("overwrite").parquet(f"{gold}/dim_date")

    # ---- fact ---------------------------------------------------------------
    fact_sales = build_fact_sales(order_items)
    fact_sales.write.mode("overwrite").parquet(f"{gold}/fact_sales")
    log.info("Gold fact_sales: %s rows", fact_sales.count())

    # Restrict revenue marts to realized sales (exclude cancelled/returned).
    realized = fact_sales.filter(F.col("status") == "completed")

    # ---- marts --------------------------------------------------------------
    mart_daily = (
        realized.join(dim_date.select("date_key", "order_date", "year", "month"),
                      on="date_key", how="left")
        .groupBy("order_date", "year", "month")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
        )
        .orderBy("order_date")
    )
    mart_daily.write.mode("overwrite").parquet(f"{gold}/mart_daily_sales")

    mart_category = (
        realized.join(dim_product.select("product_id", "category"),
                      on="product_id", how="left")
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
    )
    mart_category.write.mode("overwrite").parquet(f"{gold}/mart_sales_by_category")

    mart_top_customers = (
        realized.join(dim_customer.select("customer_id", "full_name", "country", "segment"),
                      on="customer_id", how="left")
        .groupBy("customer_id", "full_name", "country", "segment")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_amount"), 2).alias("lifetime_value"),
        )
        .orderBy(F.desc("lifetime_value"))
        .limit(100)
    )
    mart_top_customers.write.mode("overwrite").parquet(f"{gold}/mart_top_customers")

    log.info("Gold marts written: daily_sales, sales_by_category, top_customers")
