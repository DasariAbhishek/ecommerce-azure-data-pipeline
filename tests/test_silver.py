"""Unit tests for Silver cleaning transformations."""
from ecommerce_pipeline.transform.silver import (
    clean_customers,
    clean_products,
    clean_order_items,
)


def test_clean_customers_standardizes_and_dedupes(spark):
    rows = [
        ("C1", "Asha Rao", "ASHA@MAIL.COM ", "  india ", "Pune", "consumer", "2023-01-01"),
        ("C1", "Asha Rao", "asha@mail.com", "India", "Pune", "consumer", "2023-01-01"),  # dup
        ("C2", "Ben Lee", "ben@mail.com", "", "Austin", "enterprise", "2023-02-01"),     # null country
    ]
    cols = ["customer_id", "full_name", "email", "country", "city", "segment", "signup_date"]
    out = clean_customers(spark.createDataFrame(rows, cols)).collect()
    by_id = {r["customer_id"]: r for r in out}

    assert len(out) == 2                          # deduped
    assert by_id["C1"]["email"] == "asha@mail.com"  # lowered & trimmed
    assert by_id["C1"]["country"] == "India"        # initcap & trimmed
    assert by_id["C2"]["country"] == "Unknown"      # empty -> Unknown


def test_clean_products_drops_invalid_prices(spark):
    rows = [
        ("P1", "Widget", "Home", "OXO", "12.50", "5.0"),
        ("P2", "Bad", "Home", "OXO", "-9.0", "5.0"),   # negative price -> dropped
    ]
    cols = ["product_id", "product_name", "category", "brand", "unit_price", "unit_cost"]
    out = clean_products(spark.createDataFrame(rows, cols)).collect()
    assert [r["product_id"] for r in out] == ["P1"]
    assert out[0]["unit_price"] == 12.5            # cast to double


def test_clean_order_items_dedupes_and_computes_amounts(spark):
    item = {"product_id": "P1", "quantity": 2, "unit_price": 10.0, "discount_pct": 0.1}
    rows = [
        ("O1", "C1", "2024-03-01T10:00:00", "web", "upi", "completed", [item], "2024-03-01 00:00:01"),
        ("O1", "C1", "2024-03-01T10:00:00", "web", "upi", "completed", [item], "2024-03-01 00:00:02"),  # dup event
    ]
    cols = ["order_id", "customer_id", "order_ts", "channel", "payment_method",
            "status", "items", "_ingest_ts"]
    out = clean_order_items(spark.createDataFrame(rows, cols)).collect()

    assert len(out) == 1                           # at-least-once event deduped
    r = out[0]
    assert r["gross_amount"] == 20.0               # 2 * 10
    assert r["net_amount"] == 18.0                 # 20 * (1 - 0.1)
    assert r["order_item_id"] == "O1-P1"
