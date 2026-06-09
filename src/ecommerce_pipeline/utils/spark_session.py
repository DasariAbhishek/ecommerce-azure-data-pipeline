"""Spark session factory.

Locally this returns a single-node Spark session. On Azure Databricks the
`spark` object already exists, so the notebooks simply reuse the active session.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "ecommerce-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Keep the local run lean and deterministic
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
