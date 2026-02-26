# 🔄 CDC & SCD Patterns

**EN** | [PT](#pt---padrões-cdc--scd)

Comprehensive guide to Change Data Capture (CDC) and Slowly Changing Dimensions (SCD) patterns used in production data warehouses.

---

## 📋 Quick Reference

| Pattern | Method | Use Case | History Kept? |
|---------|--------|----------|---------------|
| **CDC Full Sync** | Full load daily | Initial setup, slow-changing source | No (snapshot) |
| **CDC Incremental** | Load only changes | High-frequency updates | No (latest only) |
| **CDC Log-based** | Transaction log streaming | Real-time ELT, PostgreSQL logical decode | Yes (audit) |
| **SCD Type 1** | Overwrite | Non-domain fields (misspellings) | No |
| **SCD Type 2** | New row + dates | Track history (customer moves) | Yes (full history) |
| **SCD Type 3** | Previous + current | Limited history (price tracking) | Yes (limited) |

---

## 🔍 CDC Patterns

### Pattern 1: Full Sync (Daily Snapshot)

**Use case:** Source is small, or first-time load.

```python
# PySpark: Load entire source daily
df = spark.read.jdbc(url=source_url, table="orders", properties=conn_props)

# Drop & recreate bronze table daily
df.write.format("delta").mode("overwrite").saveAsTable("bronze.orders")
```

**Pros:**
- ✅ Simple to implement
- ✅ No watermark tracking needed

**Cons:**
- ❌ Inefficient for large tables
- ❌ Doesn't track deletes

---

### Pattern 2: Incremental CDC (Watermark-based)

**Use case:** High-frequency updates, large tables.

```python
# 1. Get last processed timestamp
last_timestamp = spark.sql("""
    SELECT COALESCE(MAX(updated_at), '2020-01-01') FROM silver.orders
""").collect()[0][0]

# 2. Load only new/changed records
changed_df = spark.read.jdbc(
    url=source_url,
    query=f"SELECT * FROM orders WHERE updated_at > '{last_timestamp}'",
    properties=conn_props
)

# 3. Append to bronze (or merge into silver with SCD Type 2)
changed_df.write.format("delta").mode("append").saveAsTable("bronze.orders")
```

**Pros:**
- ✅ Efficient (loads only changes)
- ✅ Scalable for large tables

**Cons:**
- ⚠️ Requires `updated_at` column in source
- ⚠️ Can't track deletes (source doesn't record them)

**Watermark column requirements:**
- Source must have `updated_at` or `modified_date` timestamp
- Must be populated on INSERT and UPDATE
- Ideally indexed for performance

---

### Pattern 3: Log-based CDC (Real-time Streaming)

**Use case:** Real-time requirements, PostgreSQL/MySQL/Oracle.

#### PostgreSQL Logical Decoding (via Debezium)
```bash
# Enable logical replication in PostgreSQL
# postgresql.conf:
# wal_level = logical
# max_wal_senders = 10

# Debezium connector (Docker-based)
docker-compose up -d kafka zookeeper postgres

# Send CDC events to Kafka topic "cdc.orders"
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json
```

**Output format (Kafka):**
```json
{
  "before": { "order_id": 123, "amount": 100 },
  "after": { "order_id": 123, "amount": 150 },
  "op": "u",  // u=update, c=create, d=delete
  "ts_ms": 1677345600000
}
```

**Consume in PySpark:**
```python
from pyspark.sql.functions import from_json, col

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cdc.orders") \
    .load()

# Parse CDC envelope
schema = """
{
  "before": { "order_id": INT, "amount": DECIMAL },
  "after": { "order_id": INT, "amount": DECIMAL },
  "op": STRING
}
"""

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("cdc")
).select("cdc.*")

# Filter operations
updates_df = parsed_df.filter(col("op") == "u")
deletes_df = parsed_df.filter(col("op") == "d")
```

**Pros:**
- ✅ Real-time (millisecond latency)
- ✅ Tracks deletes
- ✅ Audit trail via before/after

**Cons:**
- ❌ Complex infrastructure (Kafka, Debezium)
- ❌ Requires streaming ETL (Spark, Flink)
- ❌ Higher operational overhead

---

## 👥 SCD Patterns

### Type 1: Overwrite (No History)

**Use case:** Non-domain attributes (typos, corrections).

```sql
-- Current table (no history kept)
CREATE TABLE silver.customers (
    customer_id INT PRIMARY KEY,
    name STRING,
    email STRING,
    country STRING  -- Updates overwrite
);

-- Insert or update (always latest)
UPDATE silver.customers
SET email = 'john.new@example.com'
WHERE customer_id = 123;
```

**Example:**
```
Day 1: Customer email = john@old.com
Day 2: Customer email = john@new.com (old value lost)
```

---

### Type 2: New Row (Full History)

**Use case:** Track historical changes (address changes, segment changes).

```sql
-- Table with effective dates
CREATE TABLE silver.customers (
    customer_id INT,
    name STRING,
    address STRING,

    -- SCD Type 2 columns
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,

    PRIMARY KEY (customer_id, effective_date)
);

-- Historical data
SELECT * FROM silver.customers WHERE customer_id = 123;
-- Row 1: address='123 Old St', effective_date=2024-01-01, end_date=2025-10-15, is_current=FALSE
-- Row 2: address='456 New St', effective_date=2025-10-16, end_date=NULL, is_current=TRUE
```

#### Implementation: Snowflake MERGE
```sql
MERGE INTO silver.customers AS target
USING (
    SELECT
        c.customer_id,
        c.name,
        c.address,
        CURRENT_DATE AS effective_date
    FROM bronze.customers_raw c
    WHERE c._loaded_at > (SELECT MAX(updated_at) FROM silver.customers)
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE

-- Address changed → close old row, insert new
WHEN MATCHED AND target.address != source.address THEN UPDATE SET
    end_date = CURRENT_DATE - 1,
    is_current = FALSE

-- New customer
WHEN NOT MATCHED THEN INSERT
    (customer_id, name, address, effective_date, is_current)
    VALUES (source.customer_id, source.name, source.address, source.effective_date, TRUE);
```

**Result:**
```
Day 1: address='123 Old St' (is_current=TRUE)
Day 2: address='456 New St' (old row: is_current=FALSE, new row: is_current=TRUE)
```

#### Query for Current State
```sql
SELECT * FROM silver.customers
WHERE is_current = TRUE;
```

#### Query Historical Timeline
```sql
SELECT
    customer_id, address, effective_date, end_date
FROM silver.customers
WHERE customer_id = 123
ORDER BY effective_date;

-- Shows complete timeline:
-- 2024-01-01 to 2025-10-15: 123 Old St
-- 2025-10-16 to present: 456 New St
```

**Pros:**
- ✅ Full audit trail
- ✅ Answer "what was the value on DATE X?"
- ✅ Analyze trends (segment changes, address patterns)

**Cons:**
- ⚠️ Table grows over time (storage cost)
- ⚠️ Slower queries (filter by is_current or effective_date)

---

### Type 3: Previous + Current (Limited History)

**Use case:** Track only current and previous value (price, plan).

```sql
-- Keep current and previous
CREATE TABLE silver.products (
    product_id INT PRIMARY KEY,
    current_price DECIMAL(10,2),
    previous_price DECIMAL(10,2),
    price_changed_date DATE
);

-- Example
SELECT * FROM silver.products WHERE product_id = 999;
-- product_id=999, current_price=99.99, previous_price=89.99, price_changed_date=2026-02-20
```

#### Update Logic
```sql
UPDATE silver.products
SET
    previous_price = current_price,
    current_price = 79.99,
    price_changed_date = CURRENT_DATE
WHERE product_id = 999
  AND current_price != 79.99;
```

**Pros:**
- ✅ Minimal storage (only 2 versions)
- ✅ Fast queries

**Cons:**
- ❌ Limited history (can't see all past values)
- ❌ No audit trail before previous

---

## 🎯 Choosing Your Pattern

```
Do you need REAL-TIME data?
  ├─ YES → CDC Log-based (Debezium, PostgreSQL logical decoding)
  └─ NO → Proceed below...

Is the table LARGE (>1M rows)?
  ├─ YES → CDC Incremental + SCD Type 2
  │          (Load only changes, track history)
  └─ NO → CDC Full Sync + SCD Type 1/2
          (Reload all daily)

Do you need FULL HISTORY?
  ├─ YES → SCD Type 2 (keep all rows, add effective dates)
  └─ NO → SCD Type 1 (overwrite, no history)

Is history SIZE a concern?
  ├─ YES → SCD Type 3 (keep only previous + current)
  └─ NO → SCD Type 2 (full historical timeline)
```

---

## 📊 Performance Comparison

| Scenario | CDC Method | SCD Type | Load Time | Query Time | Storage |
|----------|-----------|----------|-----------|-----------|---------|
| Customer master (slow change) | Full sync daily | Type 2 | 5 min | Fast (current only) | Medium |
| Orders (fast change) | Incremental | Type 1 | 1 min | Fast (latest) | Low |
| Price history | Incremental | Type 3 | 2 min | Medium (filter) | Very Low |
| Real-time events | Log-based | Type 1 | Streaming | Slow (high volume) | Very High |

---

## 🛠️ Implementation Checklist

- [ ] Identify change detection method (timestamp, sequence, hash)
- [ ] Test watermark logic (handle late arrivals, duplicates)
- [ ] Choose SCD type based on business requirements
- [ ] Implement effective/end date columns
- [ ] Add `is_current` flag for easy filtering
- [ ] Set up CDC job schedule (hourly, daily)
- [ ] Add data quality checks (row count validation)
- [ ] Document retention policy (keep history 3-5 years)

---

<br/>

# 🇧🇷 PT — Padrões CDC & SCD

## Resumo Rápido

| Padrão | Método | Caso de Uso | Histórico? |
|--------|--------|-----------|----------|
| CDC Full | Reload total | Primeira carga | Não |
| CDC Incremental | Apenas mudanças | Updates frequentes | Não |
| CDC Log-based | Kafka/Debezium | Real-time | Sim |
| **SCD Type 1** | Sobrescreve | Não precisa histórico | Não |
| **SCD Type 2** | Nova linha + datas | Rastreia mudanças | Sim |
| **SCD Type 3** | Anterior + atual | Histórico limitado | Sim (2 valores) |

## Fluxo Típico

1. **Bronze:** CDC Incremental carrega mudanças diárias
2. **Silver:** SCD Type 2 merge rastreia histórico
3. **Gold:** Queries apenas de `is_current=TRUE`

## Exemplo Real: Cliente muda de endereço

```
2024-01-01: Silver insert (endereço='123 Old St', is_current=TRUE)
2025-10-16: CDC detecta mudança
            → Merge fecha old row (end_date=2025-10-15)
            → Insert new row (endereço='456 New St', is_current=TRUE)
```

Resultado: Timeline completa de endereços para auditoria.
