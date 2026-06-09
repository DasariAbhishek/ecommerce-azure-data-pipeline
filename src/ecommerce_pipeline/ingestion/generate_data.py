"""Synthetic source-system data generator.

Simulates three operational source systems landing files in a raw drop zone:
  - customers.csv   (CRM export)
  - products.csv    (PIM / catalog export)
  - orders.json     (order-management system, nested line items)

A configurable fraction of rows is intentionally "dirtied" (nulls, bad casing,
negative amounts, duplicates) so the Silver layer's cleaning and the data
quality checks have realistic problems to solve.
"""
from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from ecommerce_pipeline.config import Config, load_config
from ecommerce_pipeline.utils.logging_utils import get_logger

log = get_logger("ingestion.generate")

CATEGORIES = {
    "Electronics": ["Acer", "Sony", "Samsung", "Anker", "Logitech"],
    "Home & Kitchen": ["Instant Pot", "Ninja", "OXO", "Lodge"],
    "Apparel": ["Nike", "Levi's", "Uniqlo", "H&M"],
    "Books": ["Penguin", "OReilly", "Manning"],
    "Sports": ["Decathlon", "Wilson", "Adidas"],
    "Beauty": ["Maybelline", "Nivea", "LOreal"],
}
CHANNELS = ["web", "mobile_app", "marketplace", "in_store"]
PAYMENTS = ["credit_card", "paypal", "upi", "gift_card", "cod"]
STATUSES = ["completed", "completed", "completed", "returned", "cancelled"]
SEGMENTS = ["consumer", "small_business", "enterprise"]


def _daterange(cfg) -> tuple[datetime, datetime]:
    start = datetime.strptime(cfg["start_date"], "%Y-%m-%d")
    end = datetime.strptime(cfg["end_date"], "%Y-%m-%d")
    return start, end


def generate(config: Config | None = None) -> dict[str, str]:
    cfg = config or load_config()
    g = cfg.data_generation
    random.seed(g["seed"])
    fake = Faker()
    Faker.seed(g["seed"])

    landing = Path(cfg.abs("landing"))
    landing.mkdir(parents=True, exist_ok=True)
    start, end = _daterange(g)
    span_days = (end - start).days
    dirty = g["dirty_ratio"]

    # ---- customers ----------------------------------------------------------
    customers_path = landing / "customers.csv"
    customer_ids = []
    with open(customers_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["customer_id", "full_name", "email", "country", "city",
             "segment", "signup_date"]
        )
        for i in range(1, g["num_customers"] + 1):
            cid = f"C{i:06d}"
            customer_ids.append(cid)
            signup = start - timedelta(days=random.randint(0, 900))
            email = fake.email()
            country = fake.country()
            # inject dirt: random casing on email, occasional null country
            if random.random() < dirty:
                email = email.upper()
            if random.random() < dirty:
                country = ""
            w.writerow([
                cid, fake.name(), email, country, fake.city(),
                random.choice(SEGMENTS), signup.strftime("%Y-%m-%d"),
            ])
    log.info("Wrote %s customers -> %s", g["num_customers"], customers_path)

    # ---- products -----------------------------------------------------------
    products_path = landing / "products.csv"
    product_catalog = []
    with open(products_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["product_id", "product_name", "category", "brand",
             "unit_price", "unit_cost"]
        )
        for i in range(1, g["num_products"] + 1):
            pid = f"P{i:05d}"
            category = random.choice(list(CATEGORIES))
            brand = random.choice(CATEGORIES[category])
            cost = round(random.uniform(3, 400), 2)
            price = round(cost * random.uniform(1.2, 2.6), 2)
            product_catalog.append((pid, price))
            # inject dirt: occasional negative price
            if random.random() < dirty:
                price = -price
            w.writerow([
                pid, f"{brand} {fake.word().title()} {random.randint(100,999)}",
                category, brand, price, cost,
            ])
    log.info("Wrote %s products -> %s", g["num_products"], products_path)

    # ---- orders (nested line items) ----------------------------------------
    orders_path = landing / "orders.json"
    price_lookup = dict(product_catalog)
    orders = []
    for i in range(1, g["num_orders"] + 1):
        oid = f"O{i:08d}"
        cid = random.choice(customer_ids)
        order_dt = start + timedelta(
            days=random.randint(0, span_days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        n_items = random.randint(1, 5)
        items = []
        for _ in range(n_items):
            pid, base_price = random.choice(product_catalog)
            qty = random.randint(1, 4)
            discount = round(random.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
            items.append({
                "product_id": pid,
                "quantity": qty,
                "unit_price": base_price,
                "discount_pct": discount,
            })
        order = {
            "order_id": oid,
            "customer_id": cid,
            "order_ts": order_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "channel": random.choice(CHANNELS),
            "payment_method": random.choice(PAYMENTS),
            "status": random.choice(STATUSES),
            "items": items,
        }
        orders.append(order)
        # inject dirt: exact duplicate order event (late-arriving / at-least-once)
        if random.random() < dirty:
            orders.append(json.loads(json.dumps(order)))

    with open(orders_path, "w", encoding="utf-8") as fh:
        for o in orders:
            fh.write(json.dumps(o) + "\n")  # JSON Lines, like an event export
    log.info("Wrote %s order events -> %s", len(orders), orders_path)

    return {
        "customers": str(customers_path),
        "products": str(products_path),
        "orders": str(orders_path),
    }


if __name__ == "__main__":
    generate()
