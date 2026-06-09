# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver → Gold
# MAGIC Builds the star schema (dims + fact_sales) and curated business marts as
# MAGIC Delta tables, ready for Databricks SQL / Synapse / Power BI.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
SILVER = f"abfss://silver@ecom{env}lake.dfs.core.windows.net"
GOLD = f"abfss://gold@ecom{env}lake.dfs.core.windows.net"

# COMMAND ----------
from pyspark.sql import functions as F

customers = spark.read.format("delta").load(f"{SILVER}/customers")
products = spark.read.format("delta").load(f"{SILVER}/products")
order_items = spark.read.format("delta").load(f"{SILVER}/order_items")

# --- dimensions ---
customers.write.format("delta").mode("overwrite").save(f"{GOLD}/dim_customer")
products.write.format("delta").mode("overwrite").save(f"{GOLD}/dim_product")

dim_date = (
    order_items.select("order_date").distinct().filter(F.col("order_date").isNotNull())
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("month_name", F.date_format("order_date", "MMMM"))
)
dim_date.write.format("delta").mode("overwrite").save(f"{GOLD}/dim_date")

# --- fact ---
fact_sales = order_items.withColumn(
    "date_key", F.date_format("order_date", "yyyyMMdd").cast("int")
)
fact_sales.write.format("delta").mode("overwrite").save(f"{GOLD}/fact_sales")

# --- marts ---
realized = fact_sales.filter(F.col("status") == "completed")

(realized.groupBy("order_date")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.sum("quantity").alias("units_sold"),
         F.round(F.sum("net_amount"), 2).alias("revenue"))
    .write.format("delta").mode("overwrite").save(f"{GOLD}/mart_daily_sales"))

(realized.join(products.select("product_id", "category"), "product_id", "left")
    .groupBy("category")
    .agg(F.round(F.sum("net_amount"), 2).alias("revenue"),
         F.sum("quantity").alias("units_sold"))
    .write.format("delta").mode("overwrite").save(f"{GOLD}/mart_sales_by_category"))

print("Gold build complete.")
