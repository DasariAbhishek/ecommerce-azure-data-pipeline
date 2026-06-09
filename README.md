# E-Commerce Sales Analytics — End-to-End Data Engineering Pipeline

A production-style **medallion (Bronze → Silver → Gold)** data pipeline for an
e-commerce business. It ingests raw data from three simulated source systems,
cleans and conforms it with **PySpark**, enforces **data quality gates**, builds
a **Kimball star schema** with business marts, and serves the results to a SQL
warehouse for BI.

The repo is **fully runnable on a laptop** (local Spark + DuckDB) *and* ships
**Azure deployment assets** — Bicep/Terraform IaC, an Azure Data Factory
pipeline, and Databricks notebooks — so the exact same logic runs on
ADLS Gen2 + Databricks + Synapse.

> Built by **Abhishek Dasari** — Azure Data Platform Engineer.

---

## Architecture

```
                      ┌──────────────────────────────────────────────────────────┐
   Source systems     │                  Medallion Lakehouse                       │
 ─────────────────    │                                                            │
  CRM  → customers.csv │   BRONZE            SILVER              GOLD               │
  PIM  → products.csv  │   (raw, immutable)  (clean, conformed)  (star schema +    │
  OMS  → orders.json   │                                          business marts)  │
        │              │   ┌─────────┐      ┌──────────┐        ┌──────────────┐   │
        ▼              │   │ as-is   │  →   │ dedupe   │   →    │ dim_customer │   │
   ┌──────────┐        │   │ + audit │      │ typecast │        │ dim_product  │   │
   │ Landing  │  ───►  │   │ columns │      │ explode  │        │ dim_date     │   │
   │  zone    │        │   └─────────┘      │ validate │        │ fact_sales   │   │
   └──────────┘        │        ▲           └──────────┘        │ mart_*       │   │
                       │        │                 │             └──────┬───────┘   │
                       │   Spark ingest      Data Quality Gate         │           │
                       └─────────────────────────┼────────────────────┼───────────┘
                                                  │                    ▼
                                          fail-fast if              ┌──────────────┐
                                          checks fail               │  Warehouse   │ → BI / Power BI
                                                                    │ (DuckDB /    │
                                                                    │  Synapse)    │
                                                                    └──────────────┘

  Orchestration:  local  → ecommerce_pipeline.pipeline   |   cloud → Azure Data Factory (pl_ecommerce_medallion)
  Compute:        local  → Spark local[*]                |   cloud → Azure Databricks
  Storage:        local  → ./data/lakehouse (Parquet)    |   cloud → ADLS Gen2 (Delta)
```

A rendered diagram is in [`docs/architecture.md`](docs/architecture.md).

---

## What this project demonstrates

| Concept | Where |
|---|---|
| Medallion architecture (Bronze/Silver/Gold) | `src/ecommerce_pipeline/{ingestion,transform}` |
| PySpark transformations (joins, window dedup, explode of nested JSON) | `transform/silver.py`, `transform/gold.py` |
| Dimensional modeling (star schema: dims + fact) | `transform/gold.py` |
| Data quality framework with fail-fast gate | `quality/checks.py`, `pipeline.py` |
| Idempotent, parameterized, stage-able orchestration | `pipeline.py` |
| Config-driven design (one YAML, no hard-coded paths) | `configs/pipeline_config.yaml` |
| Unit tests for transformations & DQ | `tests/` |
| Infrastructure as Code (Bicep **and** Terraform) | `infra/` |
| Azure Data Factory pipeline (Copy + Databricks activities) | `orchestration/adf/` |
| Databricks notebooks (Delta on ADLS Gen2) | `notebooks/` |
| CI (lint + tests + smoke run) | `.github/workflows/ci.yml` |

---

## Data model (Gold star schema)

```
        dim_customer            dim_date              dim_product
        ────────────            ────────              ───────────
        customer_id (PK)        date_key (PK)         product_id (PK)
        full_name               year/quarter/month    category / brand
        country / segment       day_of_week           unit_price / unit_cost
              │                      │                       │
              └──────────────┬───────┴───────────┬───────────┘
                             ▼                    ▼
                         ┌───────────────────────────────┐
                         │          fact_sales           │  grain = 1 order line item
                         │  customer_id · product_id ·   │
                         │  date_key · quantity ·        │
                         │  net_amount · status · channel│
                         └───────────────────────────────┘

  Business marts: mart_daily_sales · mart_sales_by_category · mart_top_customers
```

---

## Quick start (local — no cloud account needed)

**Prerequisites:** Python 3.9+ and Java 11/17 (required by Spark).

```bash
# 1. install
pip install -r requirements.txt
pip install -e .

# 2. run the full pipeline (generates data → bronze → silver → DQ → gold → serve)
python -m ecommerce_pipeline.pipeline

# 3. ask the warehouse some business questions
python -m ecommerce_pipeline.analyze

# 4. run the tests
pytest -q
```

Or use the Makefile: `make install && make run && make analyze && make test`.

> On macOS/Linux, if Spark complains about resolving the hostname, run
> `export SPARK_LOCAL_IP=127.0.0.1` first.

### Sample output

```
>>> Stage 2b: DATA QUALITY GATE
[PASS] unique[customer_id]      2000 rows / 2000 distinct
[PASS] unique[product_id]       288 rows / 288 distinct
[PASS] not_null[customer_id]    null ratio 0.0000 <= 0.02
[PASS] non_negative[unit_price] 0 negative values
[PASS] min_rows[order_items]    12061 >= ...

=== Top 5 categories by revenue ===
      category    revenue  units_sold
         Books 1289790.55       2683
Home & Kitchen 1244405.95       3269
        Sports 1197590.64       3434
```

(The pipeline intentionally injects ~4% dirty records — duplicate order events,
negative prices, missing countries, inconsistent casing — so the cleaning logic
and quality checks have realistic problems to solve.)

---

## Project structure

```
ecommerce-azure-data-pipeline/
├── src/ecommerce_pipeline/
│   ├── pipeline.py              # orchestrator (local ADF equivalent)
│   ├── config.py               # typed YAML config loader
│   ├── analyze.py              # sample BI queries against the warehouse
│   ├── ingestion/
│   │   ├── generate_data.py    # synthetic source-system data (Faker)
│   │   └── bronze.py           # raw → Bronze (+ audit columns)
│   ├── transform/
│   │   ├── silver.py           # clean / dedupe / conform / explode
│   │   ├── gold.py             # star schema + business marts
│   │   └── serve.py            # publish Gold → DuckDB warehouse
│   ├── quality/checks.py       # lightweight data-quality framework
│   └── utils/                  # spark session, logging
├── configs/pipeline_config.yaml
├── notebooks/                  # Databricks notebooks (Delta on ADLS Gen2)
├── orchestration/adf/          # Azure Data Factory pipeline JSON
├── infra/{bicep,terraform}/    # Azure IaC
├── tests/                      # pytest unit tests
└── .github/workflows/ci.yml    # CI
```

---

## Deploying to Azure

The local components map 1:1 to Azure services:

| Local | Azure |
|---|---|
| `data/lakehouse` (Parquet) | ADLS Gen2 containers `bronze`/`silver`/`gold` (Delta) |
| `pipeline.py` orchestrator | Azure Data Factory `pl_ecommerce_medallion` |
| Spark `local[*]` | Azure Databricks |
| DuckDB warehouse | Synapse Serverless / Dedicated SQL Pool |

```bash
# provision infrastructure (choose one)
az deployment group create -g <rg> -f infra/bicep/main.bicep -p env=dev
#   or
cd infra/terraform && terraform init && terraform apply -var="env=dev"
```

Then import the ADF pipeline (`orchestration/adf/`) and Databricks notebooks
(`notebooks/`) and point them at your storage account.

---

## License

MIT — see [LICENSE](LICENSE).
