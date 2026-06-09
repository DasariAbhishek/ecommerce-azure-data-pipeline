# Architecture

End-to-end flow of the e-commerce medallion pipeline. GitHub renders the
Mermaid diagram below automatically.

```mermaid
flowchart LR
    subgraph SRC["Source systems"]
        A1[CRM<br/>customers.csv]
        A2[PIM<br/>products.csv]
        A3[OMS<br/>orders.json]
    end

    A1 --> L[Landing zone]
    A2 --> L
    A3 --> L

    subgraph LAKE["Medallion Lakehouse"]
        direction LR
        B["BRONZE<br/>raw + audit cols"]
        S["SILVER<br/>clean · dedupe<br/>typecast · explode"]
        G["GOLD<br/>star schema + marts"]
        B --> S
        S --> Q{Data Quality<br/>Gate}
        Q -->|pass| G
        Q -->|fail| X[Stop pipeline]
    end

    L --> B
    G --> W[(Warehouse<br/>DuckDB / Synapse)]
    W --> BI[BI / Power BI]

    classDef bronze fill:#cd7f32,color:#fff;
    classDef silver fill:#9ca3af,color:#fff;
    classDef gold fill:#d4af37,color:#fff;
    class B bronze;
    class S silver;
    class G gold;
```

## Star schema

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ FACT_SALES : has
    DIM_PRODUCT  ||--o{ FACT_SALES : has
    DIM_DATE     ||--o{ FACT_SALES : has

    FACT_SALES {
        string order_item_id
        string order_id
        string customer_id FK
        string product_id FK
        int    date_key FK
        int    quantity
        double net_amount
        string status
        string channel
    }
    DIM_CUSTOMER {
        string customer_id PK
        string full_name
        string country
        string segment
    }
    DIM_PRODUCT {
        string product_id PK
        string category
        string brand
        double unit_price
    }
    DIM_DATE {
        int    date_key PK
        int    year
        int    month
        string day_of_week
    }
```

## Local ↔ Azure mapping

| Stage | Local | Azure |
|---|---|---|
| Orchestration | `ecommerce_pipeline.pipeline` | Azure Data Factory |
| Compute | Spark `local[*]` | Azure Databricks |
| Storage | `./data/lakehouse` (Parquet) | ADLS Gen2 (Delta) |
| Serving | DuckDB | Synapse / Databricks SQL |
