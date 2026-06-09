"""Shared pytest fixtures: a session-scoped local Spark session."""
import pytest

from ecommerce_pipeline.utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("pytest-ecommerce")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
