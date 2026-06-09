# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze → Silver
# MAGIC Cleans and conforms raw Bronze data into query-ready Silver Delta tables.
# MAGIC On Databricks the `spark` session already exists; ADLS paths use `abfss://`.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

BRONZE = f"abfss://bronze@ecom{env}lake.dfs.core.windows.net"
SILVER = f"abfss://silver@ecom{env}lake.dfs.core.windows.net"

# COMMAND ----------
from pyspark.sql import functions as F, Window

# --- customers ---
customers = (
    spark.read.format("delta").load(f"{BRONZE}/customers")
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("country", F.initcap(F.trim("country")))
    .withColumn("country", F.when(F.col("country") == "", "Unknown").otherwise(F.col("country")))
    .withColumn("signup_date", F.to_date("signup_date"))
    .dropDuplicates(["customer_id"])
)
customers.write.format("delta").mode("overwrite").save(f"{SILVER}/customers")

# --- products ---
products = (
    spark.read.format("delta").load(f"{BRONZE}/products")
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("unit_cost", F.col("unit_cost").cast("double"))
    .filter(F.col("unit_price") > 0)
    .dropDuplicates(["product_id"])
)
products.write.format("delta").mode("overwrite").save(f"{SILVER}/products")

# --- order_items (dedupe at-least-once events, then explode) ---
orders = spark.read.format("delta").load(f"{BRONZE}/orders")
w = Window.partitionBy("order_id").orderBy("_ingest_ts")
order_items = (
    orders.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    .withColumn("item", F.explode("items"))
    .select(
        "order_id", "customer_id", F.to_timestamp("order_ts").alias("order_ts"),
        "channel", "payment_method", "status",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").cast("int").alias("quantity"),
        F.col("item.unit_price").cast("double").alias("unit_price"),
        F.col("item.discount_pct").cast("double").alias("discount_pct"),
    )
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("net_amount", F.round(F.col("gross_amount") * (1 - F.col("discount_pct")), 2))
    .withColumn("order_date", F.to_date("order_ts"))
    .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
)
(order_items.write.format("delta").mode("overwrite")
    .partitionBy("order_date").save(f"{SILVER}/order_items"))

print("Silver build complete.")
