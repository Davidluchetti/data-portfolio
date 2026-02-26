# 🏗️ Medallion Architecture (Bronze/Silver/Gold)

**EN** | [PT](#pt---arquitetura-medallion)

A three-layer architectural pattern for building scalable, maintainable data lakes on cloud data warehouses (Snowflake, Databricks, Azure Synapse, GCP BigQuery).

---

## 📚 Overview

```
┌─────────────────────────────────────────────────────┐
│ GOLD (Analytics Layer)                              │
│ - Fact & dimension tables                           │
│ - Pre-computed aggregations                         │
│ - Business metrics ready for BI tools               │
└─────────────────────────────────────────────────────┘
                         ▲
                         │ (Transforms)
┌─────────────────────────────────────────────────────┐
│ SILVER (Business Layer)                             │
│ - Data quality validated                            │
│ - Business rules applied                            │
│ - SCD Type 2 dimensions                             │
│ - Deduplication & enrichment                        │
└─────────────────────────────────────────────────────┘
                         ▲
                         │ (CDC Merge)
┌─────────────────────────────────────────────────────┐
│ BRONZE (Raw Layer)                                  │
│ - Raw data ingestion (append-only)                  │
│ - Minimal schema validation                         │
│ - Historical records preserved                      │
│ - Delta Lake format with versioning                 │
└─────────────────────────────────────────────────────┘
                         ▲
                         │
              (Source Systems)
```

---

## 🥉 BRONZE Layer

**Purpose:** Raw ingestion with minimal transformation, preserving complete data lineage.

### Design Principles
- **Append-only inserts** — Never delete/update
- **Schema inference** — Auto-detect types on first load
- **Metadata capture** — `_loaded_at`, `_source_system`, `_file_path`
- **Versioning** — Delta Lake time-travel for historical queries

### Example Schema
```sql
CREATE TABLE bronze.orders_raw (
    _load_timestamp TIMESTAMP DEFAULT current_timestamp(),
    _source_file STRING,
    _row_number INT,
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2),
    status STRING,
    raw_json STRING  -- Store original JSON for full lineage
);
```

### Loading Pattern (PySpark)
```python
# Auto-infer schema from CSV/JSON
df = spark.read.option("inferSchema", "true").csv("s3://raw/orders/2026-02-26/")

# Add metadata columns
from pyspark.sql.functions import input_file_name, current_timestamp
df = df.withColumn("_loaded_at", current_timestamp())
df = df.withColumn("_source_file", input_file_name())

# Append to bronze table
df.write.format("delta").mode("append").saveAsTable("bronze.orders_raw")
```

### Key Characteristics
| Property | Value |
|----------|-------|
| **Update frequency** | Hourly/Daily (batch) or real-time (streaming) |
| **Retention** | 1-2 years (cost optimization via partitioning) |
| **Data quality checks** | Minimal (schema only) |
| **Consumer readiness** | 0% (raw data, not for BI) |

---

## 🥈 SILVER Layer

**Purpose:** Apply business rules, data quality, and deduplication. Serve as single source of truth for analysts.

### Transformations Applied
1. **Type casting** — Ensure correct data types
2. **Deduplication** — Handle duplicate records (keep latest by date)
3. **Validation** — Filter invalid records (negative amounts, future dates)
4. **Enrichment** — Join with reference data (e.g., customer master)
5. **SCD Type 2** — Track dimension changes with effective dates
6. **Column renaming** — From snake_case source to domain naming

### Example: Silver Orders
```sql
CREATE TABLE silver.orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2),
    status STRING,

    -- SCD Type 2 metadata
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,

    -- Audit columns
    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    CONSTRAINT pk_orders PRIMARY KEY (order_id, effective_date)
);
```

### Deduplication Logic (PySpark)
```python
from pyspark.sql.window import Window

# Keep latest record per order_id
window_spec = Window.partitionBy("order_id").orderBy(desc("_loaded_at"))
deduped_df = df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")
```

### SCD Type 2 Merge (Snowflake)
```sql
MERGE INTO silver.orders AS target
USING (
    SELECT * FROM bronze.orders_raw
    WHERE _loaded_at > (SELECT MAX(updated_at) FROM silver.orders)
) AS source
ON target.order_id = source.order_id

WHEN MATCHED AND target.order_amount != source.order_amount THEN
    UPDATE SET
        end_date = CURRENT_DATE - 1,
        is_current = FALSE

WHEN NOT MATCHED THEN
    INSERT (order_id, order_amount, effective_date, is_current)
    VALUES (source.order_id, source.order_amount, CURRENT_DATE, TRUE);
```

### Key Characteristics
| Property | Value |
|----------|-------|
| **Update frequency** | Daily (after bronze load) |
| **Retention** | 3-5 years (full history via SCD Type 2) |
| **Data quality checks** | Schema, referential integrity, business rules |
| **Consumer readiness** | 80% (most analysts use silver) |

---

## 🥇 GOLD Layer

**Purpose:** Analytics-ready, pre-aggregated tables optimized for BI tools and reporting.

### Typical GOLD Tables

#### Fact Tables (Events)
```sql
CREATE TABLE gold.fact_orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    month_year DATE,

    order_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),

    status STRING,

    PRIMARY KEY (order_id)
);
```

#### Dimension Tables (Attributes)
```sql
CREATE TABLE gold.dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name STRING,
    country STRING,
    segment STRING,
    lifetime_value DECIMAL(15,2),
    last_order_date DATE
);
```

#### Pre-computed Aggregations
```sql
CREATE TABLE gold.agg_monthly_sales (
    month_year DATE,
    customer_id INT,

    total_orders INT,
    total_amount DECIMAL(15,2),
    avg_order_size DECIMAL(10,2),

    PRIMARY KEY (month_year, customer_id)
);
```

### Materialized View (Auto-refresh)
```sql
CREATE MATERIALIZED VIEW gold.agg_monthly_sales AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month_year,
    o.customer_id,
    COUNT(*) AS total_orders,
    SUM(o.order_amount) AS total_amount,
    AVG(o.order_amount) AS avg_order_size
FROM silver.orders o
WHERE is_current = TRUE
GROUP BY 1, 2;

-- Auto-refresh daily
ALTER MATERIALIZED VIEW gold.agg_monthly_sales SET
    MATERIALIZED VIEW PROPERTIES (
        'refresh_interval' = '24 hours'
    );
```

### Key Characteristics
| Property | Value |
|----------|-------|
| **Update frequency** | Daily/Weekly (usually scheduled) |
| **Retention** | 5-10 years (historical trends) |
| **Data quality checks** | All checks passed from silver |
| **Consumer readiness** | 100% (BI-ready, fast queries) |

---

## 🔄 Incremental Loading Strategy

### Watermark-based CDC
```python
# Get last processed timestamp
last_watermark = spark.sql("SELECT MAX(updated_at) FROM silver.orders").collect()[0][0]

# Load only new/changed records
new_df = spark.read \
    .option("url", "jdbc:postgresql://source...") \
    .option("query", f"""
        SELECT * FROM source.orders
        WHERE updated_at > '{last_watermark}'
    """) \
    .load()

# Transform and merge into silver
# (See SCD Type 2 merge above)
```

### Delta Lake Time Travel
```python
# Query bronze table as of 1 week ago
df_past = spark.read.option("versionAsOf", 0).table("bronze.orders_raw")

# Audit diff between current and past
df_past.exceptAll(spark.read.table("bronze.orders_raw"))
```

---

## 📊 Partitioning Strategy

### Date-based Partitioning (Recommended)
```sql
CREATE TABLE bronze.orders_raw (
    ...columns...
)
PARTITIONED BY (load_date DATE)
CLUSTER BY (customer_id);  -- Snowflake clustering
```

**Benefits:**
- ✅ Prune old data efficiently (only scan relevant dates)
- ✅ Supports time-based retention policies
- ✅ Natural alignment with business calendar

### Partition Elimination Example
```sql
-- Only scans 2 days of data
SELECT * FROM bronze.orders_raw
WHERE load_date >= '2026-02-25'
  AND order_amount > 1000;
```

---

## 🚀 Medallion in Practice: Real Flow

```
1. Data Lands (Source)
   ↓
2. BRONZE: Raw table receives daily snapshot (INSERT only)
   bronze.orders_raw (100K rows)
   ↓
3. SILVER: Dedup, validate, SCD Type 2 merge
   silver.orders (80K rows, deduplicated, 5-year history)
   ↓
4. GOLD: Aggregate for analytics
   gold.fact_orders (80K rows, optimized for BI)
   gold.dim_customers (2K rows, denormalized)
   gold.agg_monthly_sales (materialized view)
   ↓
5. BI Tools (Tableau, Power BI, Looker)
   Query only from GOLD
```

---

## 💰 Cost Optimization

| Layer | Strategy |
|-------|----------|
| **BRONZE** | Partition by year, drop partitions older than 2 years |
| **SILVER** | Keep full SCD Type 2 history (5 years) for audit trail |
| **GOLD** | Materialize only top 20 views; rest as ON-DEMAND queries |

**Snowflake example:**
```sql
ALTER TABLE bronze.orders_raw
DROP PARTITION WHERE load_date < '2024-02-26';

SELECT SUM(bytes) / (1024*1024*1024) AS GB
FROM information_schema.table_storage_metrics
WHERE table_name = 'orders_raw';
```

---

## 🎯 Best Practices

1. ✅ **Never update BRONZE** — Append-only ensures auditability
2. ✅ **SCD Type 2 only in SILVER** — Keep historical truth
3. ✅ **Pre-aggregate in GOLD** — Push compute to load time, not query time
4. ✅ **Partition by date** — Essential for performance at scale
5. ✅ **Data quality in SILVER** — Filter noise before analysts see it
6. ✅ **Monitor GOLD freshness** — Set SLAs for refresh frequency

---

<br/>

# 🇧🇷 PT — Arquitetura Medallion

Padrão de três camadas para construir data lakes escaláveis em Snowflake, Databricks, Azure Synapse.

## Resumo

- **BRONZE:** Dados brutos, append-only, sem transformações
- **SILVER:** Regras de negócio aplicadas, SCD Type 2, fonte única de verdade
- **GOLD:** Analytics-ready, pré-agregado, otimizado para BI

## Fluxo

Bronze (100K) → Silver (80K deduplic., histórico 5 anos) → Gold (80K fact + dim)

## Práticas

✅ BRONZE append-only
✅ SCD Type 2 em SILVER
✅ Particionar por data
✅ Pré-agregar em GOLD
✅ Data quality em SILVER

Veja `cdc-scd-patterns.md` para implementação de SCD Type 2 detalhada.
