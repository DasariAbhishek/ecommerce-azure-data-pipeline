"""A tiny, dependency-free data-quality framework.

Each check returns a CheckResult; the runner raises if any check fails and the
config has `fail_on_error: true`. This mirrors how teams gate Silver/Gold
promotion in production (Great Expectations / Deequ style, kept lightweight).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("quality")


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def expect_unique(df: DataFrame, col: str) -> CheckResult:
    total = df.count()
    distinct = df.select(col).distinct().count()
    ok = total == distinct
    return CheckResult(
        f"unique[{col}]", ok,
        f"{total} rows / {distinct} distinct",
    )


def expect_not_null(df: DataFrame, col: str, max_null_ratio: float) -> CheckResult:
    total = df.count()
    nulls = df.filter(F.col(col).isNull()).count()
    ratio = (nulls / total) if total else 0.0
    ok = ratio <= max_null_ratio
    return CheckResult(
        f"not_null[{col}]", ok,
        f"null ratio {ratio:.4f} <= {max_null_ratio}",
    )


def expect_min_rows(df: DataFrame, min_rows: int, label: str) -> CheckResult:
    n = df.count()
    return CheckResult(f"min_rows[{label}]", n >= min_rows, f"{n} >= {min_rows}")


def expect_non_negative(df: DataFrame, col: str) -> CheckResult:
    bad = df.filter(F.col(col) < 0).count()
    return CheckResult(f"non_negative[{col}]", bad == 0, f"{bad} negative values")


def run_checks(results: list[CheckResult], fail_on_error: bool) -> None:
    failed = [r for r in results if not r.passed]
    for r in results:
        emoji = "PASS" if r.passed else "FAIL"
        log.info("[%s] %-22s %s", emoji, r.name, r.detail)
    if failed and fail_on_error:
        names = ", ".join(r.name for r in failed)
        raise ValueError(f"Data quality checks failed: {names}")
