# LinkedIn post

Pick whichever version fits your voice. Attach the architecture image
(`docs/architecture.svg`, exported as PNG) and link your GitHub repo.

---

## Version 1 — story / build-in-public (recommended)

I built an end-to-end Data Engineering project this week — and made it run on a laptop AND on Azure. 🚀

Most "data engineering projects" are just a notebook with one CSV. I wanted something that actually looks like production. So I built a full **medallion (Bronze → Silver → Gold)** e-commerce sales pipeline.

What it does, end to end:
🥉 Bronze — ingests raw data from 3 simulated source systems (CRM, product catalog, an order-management system that drops nested JSON events) and keeps it immutable with audit columns.
🥈 Silver — cleans it with PySpark: de-duplicates "at-least-once" order events with window functions, explodes nested line items, type-casts, and standardizes messy fields.
🛡️ Data Quality Gate — the pipeline FAILS FAST if uniqueness / null-ratio / non-negative checks don't pass. Bad data never reaches Gold.
🥇 Gold — a Kimball star schema (dim_customer, dim_product, dim_date, fact_sales) + business marts for daily sales, category performance, and top customers.
📊 Serving — published to a SQL warehouse, ready for Power BI.

The best part: the same logic is **cloud-ready**. The repo also ships:
✅ Bicep + Terraform to provision ADLS Gen2, Data Factory & Databricks
✅ An Azure Data Factory pipeline (Copy + Databricks activities)
✅ Databricks notebooks writing Delta tables
✅ Unit tests + GitHub Actions CI

Local stack: PySpark + DuckDB. Azure stack: ADF + Databricks + ADLS Gen2 + Synapse.

I intentionally injected ~4% dirty records (duplicate events, negative prices, missing values) so the cleaning and quality checks have real problems to solve — because that's the actual job. 😅

Code + README + architecture diagram are on GitHub 👇
[your repo link]

Would love feedback from other data engineers — what would you add next? Streaming? CDC? dbt for the Gold layer?

#DataEngineering #Azure #PySpark #Databricks #ETL #DataPipeline #BigData #Analytics #Spark #DataQuality

---

## Version 2 — short & punchy

Stop building "data projects" that are just one notebook + one CSV. 🛑

I built a full **end-to-end medallion pipeline** (Bronze → Silver → Gold) for e-commerce sales — runnable on a laptop, deployable to Azure.

🥉 Raw ingestion from 3 source systems (incl. nested JSON order events)
🥈 PySpark cleaning: window dedup, explode, type-casting
🛡️ A data quality gate that fails fast on bad data
🥇 Kimball star schema + business marts → SQL warehouse → Power BI

Plus Bicep/Terraform IaC, an Azure Data Factory pipeline, Databricks notebooks, unit tests, and CI.

Local: PySpark + DuckDB. Cloud: ADF + Databricks + ADLS Gen2 + Synapse.

Full code + diagram on GitHub 👇
[your repo link]

#DataEngineering #Azure #PySpark #Databricks #ETL #DataPipeline #Spark

---

## Tips for the post
- Post the **architecture image** as the first media — visuals get ~2x reach.
- Pin a first comment with the GitHub link (LinkedIn slightly downranks posts with outbound links in the body; putting it in a comment helps).
- Best times to post: Tue–Thu, 8–10am your timezone.
- End with a question (Version 1 does) to drive comments.
- Reply to every comment in the first 2 hours.
