# 🚀 ETL/ELT Migration Guide: Legacy → Cloud

**EN** | [PT](#pt---guia-de-migração-etlelt)

Real-world strategies for migrating from legacy ETL tools (IBM DataStage, Informatica PowerCenter) to modern cloud stacks (Snowflake, Azure, Databricks, GCP).

---

## 📊 Context

**Real migrations executed:**
- ✅ **IBM DataStage → Snowflake** (Algar Telecom, 2025) — 50+ jobs, 300+ transformations
- ✅ **Informatica PowerCenter → Azure Data Factory** (Danone Group, 2023-2025) — 150+ mappings
- ✅ **SAP ECC export → GCP BigQuery** (BRF Analytics, 2023)

---

## 🔄 Migration Phases

```
Phase 1: Assessment     (2-4 weeks)
    ↓
Phase 2: POC           (4-6 weeks)
    ↓
Phase 3: Build         (8-12 weeks)
    ↓
Phase 4: Validation    (2-4 weeks)
    ↓
Phase 5: Cutover       (1-2 weeks)
    ↓
Phase 6: Decommission  (ongoing)
```

---

## 🔍 Phase 1: Assessment

### Document Legacy System

```
DataStage Job: DSG_LOAD_ORDERS
├── Input: SQL query from Oracle (daily_orders.sql)
├── Transform:
│   ├── Stage 1: Aggregation (SUM by customer)
│   ├── Stage 2: Lookup (join with pricing_master)
│   └── Stage 3: Aggregation (SUM by region)
├── Output: Flat file to /data/output/orders.csv
└── Schedule: Daily at 2 AM UTC (cron)
```

### Spreadsheet Inventory
Create a mapping table:

| Job ID | Legacy Tool | Name | Sources | Targets | Frequency | Complexity | Notes |
|--------|-------------|------|---------|---------|-----------|-----------|-------|
| DSG-001 | DataStage | Load Orders | Oracle | CSV | Daily | Medium | Uses lookup table |
| DSG-002 | DataStage | Aggregate Sales | CSV | HIVE | Monthly | High | Complex join logic |
| IPC-001 | Informatica | Customer Master | Multiple | Hive | Hourly | High | 8 nested lookups |

### Estimate Effort
```
• High complexity jobs (4+ lookups, complex logic) = 5-8 days
• Medium complexity (2-3 lookups) = 2-3 days
• Simple (extract + filter) = 0.5-1 day

Total estimate: 150 jobs × avg 2 days = 300 days
→ With parallel team (3 people) = ~100 days = 4-5 months
```

---

## 🏗️ Phase 2: POC (Proof of Concept)

### Select 2-3 Representative Jobs

Pick:
1. Simple job (validate tooling/patterns)
2. Medium job (test complex logic)
3. Complex job (validate performance at scale)

### Snowflake POC Example: "Load Orders" Job

**Legacy DataStage job:**
```
[Oracle Query] → [Aggregation] → [Lookup Join] → [CSV Output]
```

**Cloud equivalent (Snowflake + PySpark):**
```python
# Step 1: Extract from Oracle (Snowflake external connector)
orders_df = spark.read \
    .option("url", "jdbc:oracle:thin:@oracle.company.com:1521:PROD") \
    .option("query", "SELECT * FROM orders WHERE created_date >= trunc(sysdate)") \
    .load()

# Step 2: Aggregate by customer (PySpark)
from pyspark.sql.functions import sum, col

aggregated = orders_df \
    .groupBy("customer_id") \
    .agg(
        sum("order_amount").alias("total_amount"),
        count("*").alias("order_count")
    )

# Step 3: Lookup join with pricing master
pricing = spark.read.table("silver.pricing_master")

joined = aggregated.join(
    pricing,
    on="pricing_id",
    how="left"
)

# Step 4: Write to Snowflake
joined.write.format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .mode("overwrite") \
    .option("dbtable", "bronze.orders_loaded") \
    .save()
```

**Validation:**
```bash
# Compare row counts
SELECT COUNT(*) FROM bronze.orders_loaded;  -- Snowflake
SELECT COUNT(*) FROM /data/output/orders.csv;  -- Legacy output

# Compare data (first 100 rows)
SELECT * FROM bronze.orders_loaded EXCEPT
SELECT * FROM bronze.orders_legacy LIMIT 100;
```

---

## 🏗️ Phase 3: Build & Implement

### Standardize Patterns

**Pattern 1: Simple Extract → Load**
```python
# Template for all basic ELT jobs
source_df = spark.read.jdbc(url, table, conn_props)
transformed = source_df.filter(...).select(...)
transformed.write.format("delta").mode("overwrite").saveAsTable(target)
```

**Pattern 2: SCD Type 2 Merge**
```sql
-- Template for all slowly changing dimensions
MERGE INTO silver.dim_customer AS target
USING new_data AS source
ON target.cust_id = source.cust_id AND target.is_current = TRUE
WHEN MATCHED AND target.address != source.address THEN
    UPDATE SET end_date = current_date() - 1, is_current = FALSE
WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...);
```

**Pattern 3: Incremental Load with Watermark**
```python
# Template for high-volume tables
last_timestamp = spark.sql("SELECT MAX(updated_at) FROM bronze.table").collect()[0][0]
new_data = spark.read.jdbc(...query f"WHERE updated_at > '{last_timestamp'}")
new_data.write.format("delta").mode("append").saveAsTable("bronze.table")
```

### Parallel Migration

**Team structure:**
```
Team 1 (5 devs) → Migrate 50 DataStage jobs
Team 2 (5 devs) → Migrate 100 Informatica jobs
Team 3 (2 devs) → Platform (orchestration, monitoring, testing)
```

**Timeline (example):**
```
Week 1-2:   Team training (Spark, Snowflake, Git workflows)
Week 3-8:   Build phase (job migration)
Week 9-10:  UAT & validation
Week 11:    Cutover prep
Week 12:    Production cutover
```

---

## ✅ Phase 4: Validation

### Data Quality Checks

```python
# Row count validation
legacy_count = 10_500_000
cloud_count = spark.sql("SELECT COUNT(*) FROM cloud.table").collect()[0][0]
assert abs(legacy_count - cloud_count) < 100, f"Row count mismatch: {legacy_count} vs {cloud_count}"

# Hash validation (for unchanged data)
from pyspark.sql.functions import md5, concat_ws

legacy_hash = md5(concat_ws("|", *legacy_columns)).alias("row_hash")
cloud_hash = md5(concat_ws("|", *cloud_columns)).alias("row_hash")

mismatches = legacy.join(cloud, on="row_hash", how="anti")
assert mismatches.count() == 0, f"Found {mismatches.count()} mismatched rows"

# Sample validation (spot check)
legacy_sample = spark.read.csv("/legacy/output.csv").sample(0.1).collect()
cloud_sample = spark.sql("SELECT * FROM cloud.table").sample(0.1).collect()
# Manual comparison of a few rows
```

### Performance Validation

```sql
-- Legacy: 50 minutes → Cloud: should be < 5 minutes (10x faster)
-- If slower, add clustering, increase warehouse size, or optimize SQL

-- Check Snowflake query profile
SELECT * FROM table(get_query_history(limit=>10, result_limit=>10));

-- Check resource usage
SHOW WAREHOUSES;  -- CPU, memory utilization
```

---

## 🚀 Phase 5: Cutover Strategy

### Big Bang vs Phased

**Option A: Phased Cutover (Recommended)**
```
Week 1: Daily jobs only (low risk, easy rollback)
Week 2: Add weekly jobs
Week 3: Add monthly jobs
Week 4: Complete cutover, shut down legacy
```

**Option B: Big Bang**
```
Sunday 2 AM: Stop legacy
Sunday 6 AM: Start cloud jobs
Sunday 4 PM: Verify results
Benefit: Single cutover event
Risk: High pressure if issues found
```

### Dual Run Period (Recommended)

```
Timeline:
Jan 1-31:   Legacy + Cloud run in parallel, compare results
Feb 1-28:   If results match, legacy is read-only (backup only)
Mar 1:      Decommission legacy
```

---

## 🔧 Technical Patterns: Tool Conversions

### IBM DataStage → Snowflake/PySpark

| DataStage Component | Cloud Equivalent |
|-------------------|------------------|
| **Job** | Spark job + Airflow DAG |
| **Stage** | PySpark transformation function |
| **Lookup** | DataFrame join |
| **Aggregation** | `.groupBy().agg()` |
| **Filter** | `.filter()` |
| **Column derivation** | `.withColumn()` |
| **Pivot** | `.pivot()` |
| **Output dataset** | Delta table in Snowflake |

**Example mapping:**
```
DataStage:
├─ Oracle input → Read
├─ Aggregation stage (SUM by cust_id) → groupBy + agg
├─ Lookup join (with pricing) → join
└─ CSV output → write to Snowflake

PySpark:
df = spark.read.jdbc(...)           # Oracle input
df = df.groupBy(...).agg(...)       # Aggregation
df = df.join(pricing, ...)          # Lookup
df.write.format("snowflake").save() # Output
```

### Informatica PowerCenter → Azure Data Factory (ADF)

| Informatica | Azure Data Factory |
|------------|-------------------|
| **Mapping** | Data Flow activity |
| **Transformation** | Mapping data flow |
| **Lookup** | Lookup activity |
| **Expression** | Derived Column |
| **Pipeline** | Pipeline (orchestration) |
| **Scheduler** | Pipeline trigger (time-based) |

**Example:**
```
Informatica Mapping → ADF Data Flow (visual + code)
├─ Source (SQL Server) → Source activity
├─ Transformations (filters, joins) → Data Flow
└─ Target (Snowflake) → Sink activity
```

---

## 📋 Migration Checklist

- [ ] **Phase 1 — Assessment**
  - [ ] Inventory all jobs/mappings
  - [ ] Document data flows
  - [ ] Estimate effort and timeline
  - [ ] Select POC jobs

- [ ] **Phase 2 — POC**
  - [ ] Set up cloud environment (Snowflake, Spark)
  - [ ] Implement 3 representative jobs
  - [ ] Validate results match legacy
  - [ ] Document patterns/templates

- [ ] **Phase 3 — Build**
  - [ ] Migrate remaining jobs (parallel teams)
  - [ ] Implement monitoring/alerting
  - [ ] Create runbooks for support team
  - [ ] Set up rollback procedures

- [ ] **Phase 4 — Validation**
  - [ ] Data quality checks (row count, hash, sampling)
  - [ ] Performance validation (meet SLAs)
  - [ ] UAT with business team
  - [ ] Security review (access controls, PII)

- [ ] **Phase 5 — Cutover**
  - [ ] Scheduled cutover window
  - [ ] Dual-run validation (legacy + cloud)
  - [ ] Incident response plan
  - [ ] Rollback plan

- [ ] **Phase 6 — Decommission**
  - [ ] Monitor cloud jobs (30 days)
  - [ ] Archive legacy systems
  - [ ] Decommission infrastructure
  - [ ] Retrain operations team

---

## 💰 Cost Comparison

### Example: 50GB daily load

**Legacy (DataStage on-premise):**
```
Hardware: $300K / year
Software licenses: $150K / year
Staff (2 engineers): $300K / year
→ Total: $750K / year
```

**Cloud (Snowflake + Spark):**
```
Snowflake compute: ~$50K / year (daily 15-min queries)
Snowflake storage: ~$10K / year (50GB × $4/TB)
Spark cluster: ~$20K / year (auto-scaling)
Staff (1 engineer): $150K / year
→ Total: ~$230K / year
→ Savings: ~70% ($520K/year)
```

---

## 🚨 Common Pitfalls & Solutions

| Pitfall | Solution |
|---------|----------|
| **Data type mismatches** | Use schema mapping doc, validate types explicitly |
| **Timezone issues** | Always use UTC, document source timezone |
| **Null handling** | Test edge cases (all NULLs, no NULLs) |
| **Performance regression** | Benchmark legacy job, set target time, add clustering |
| **Late arriving data** | Implement watermark logic with grace period (24-48h) |
| **Dependency chains** | Use DAG scheduler (Airflow), graph dependencies |

---

<br/>

# 🇧🇷 PT — Guia de Migração ETL/ELT

## Contexto

Migrações reais executadas:
- ✅ IBM DataStage → Snowflake (Algar Telecom, 2025)
- ✅ Informatica PowerCenter → Azure Data Factory (Danone, 2023-25)
- ✅ SAP ECC → GCP BigQuery (BRF, 2023)

## 6 Fases

1. **Assessment** — Inventariar 50+ jobs, estimar esforço
2. **POC** — Testar 3 jobs representativos, validar padrões
3. **Build** — Migrar em paralelo (teams), 8-12 semanas
4. **Validation** — Data quality (row count, hash, performance)
5. **Cutover** — Phased ou Big Bang, dual-run 30 dias
6. **Decommission** — Archive, retirement

## Economia

**Antes (DataStage on-prem):** $750K/ano
**Depois (Snowflake cloud):** $230K/ano
**Economia:** ~70% ($520K/ano)

## Timeline Típica

```
Semana 1-2:   Treinamento
Semana 3-8:   Build (migration de jobs)
Semana 9-10:  UAT & validação
Semana 11:    Cutover prep
Semana 12+:   Produção, monitoring 30 dias
```

Ver `medallion-architecture.md` e `cdc-scd-patterns.md` para implementação detalhada.
