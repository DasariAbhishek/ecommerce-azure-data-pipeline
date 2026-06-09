"""Unit tests for the data-quality framework."""
import pytest

from ecommerce_pipeline.quality import checks


def test_expect_unique_detects_duplicates(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    assert checks.expect_unique(df, "id").passed is False
    df2 = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    assert checks.expect_unique(df2, "id").passed is True


def test_expect_non_negative(spark):
    df = spark.createDataFrame([(5.0,), (-1.0,)], ["amount"])
    assert checks.expect_non_negative(df, "amount").passed is False


def test_run_checks_raises_when_failing():
    results = [checks.CheckResult("x", False, "boom")]
    with pytest.raises(ValueError):
        checks.run_checks(results, fail_on_error=True)


def test_run_checks_silent_when_passing():
    results = [checks.CheckResult("x", True, "ok")]
    checks.run_checks(results, fail_on_error=True)  # should not raise
