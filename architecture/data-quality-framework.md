# 🧪 Data Quality Framework

**EN** | [PT](#pt---framework-de-qualidade-de-dados)

Production-grade data quality testing and monitoring strategy with SLAs, metrics, and alerting.

---

## 📊 Quality Dimensions

```
Data Quality
├─ Completeness (% of non-null values)
├─ Accuracy (% of values matching business rules)
├─ Consistency (values match across tables)
├─ Timeliness (data loaded within SLA)
└─ Uniqueness (no unexpected duplicates)
```

---

## 🎯 Quality Metrics & SLAs

### Completeness SLA

```
Silver layer: ≥ 99% non-null in key columns
├─ customer_id: 100% (PK, must have)
├─ order_date: 100% (critical for partitioning)
├─ order_amount: 99% (allow 1% unknowns)
└─ status: 98% (can be inferred)

Gold layer: 99.5% complete
└─ Only publish fact/dimension if > 99.5% non-null
```

**Implementation:**
```python
# Check completeness
def validate_completeness(df, column, min_pct=0.99):
    non_null_count = df.filter(col(column).isNotNull()).count()
    total_count = df.count()
    pct_complete = non_null_count / total_count

    assert pct_complete >= min_pct, \
        f"{column}: {pct_complete:.2%} complete (expected {min_pct:.2%})"
```

### Accuracy SLA

```
Business rules that must hold:
├─ order_amount > 0 (100% of orders)
├─ order_date ≤ current_date (100% of orders)
├─ status IN ('pending', 'confirmed', ...) (100% of orders)
├─ customer_id must exist in silver.customers (100% of orders)
└─ discount_pct BETWEEN 0 AND 100 (100% of discounts)
```

**Implementation:**
```python
def validate_accuracy(df):
    # Check order_amount > 0
    invalid_amounts = df.filter(col("order_amount") <= 0).count()
    assert invalid_amounts == 0, f"Found {invalid_amounts} non-positive amounts"

    # Check order_date is not future
    future_dates = df.filter(col("order_date") > current_date()).count()
    assert future_dates == 0, f"Found {future_dates} future-dated orders"

    # Check status is valid
    valid_statuses = {'pending', 'confirmed', 'shipped', 'delivered'}
    invalid_statuses = set(df.select("status").distinct().collect()) - valid_statuses
    assert len(invalid_statuses) == 0, f"Invalid statuses: {invalid_statuses}"
```

### Consistency SLA

```
Cross-table validation:
├─ Every order.customer_id exists in customer table (100%)
├─ Every order.product_id exists in product table (100%)
├─ No duplicate rows with same (order_id, effective_date) (100%)
└─ SCD Type 2: no customer has 2 is_current=TRUE rows (100%)
```

**Implementation:**
```python
def validate_consistency(orders_df, customers_df):
    # FK check: customer_id must exist
    missing_customers = orders_df \
        .join(customers_df.select("customer_id"), on="customer_id", how="anti") \
        .count()
    assert missing_customers == 0, f"Found {missing_customers} orders with missing customer"

    # SCD Type 2: no duplicate current rows
    duplicate_current = orders_df \
        .filter(col("is_current") == True) \
        .groupBy("customer_id").count() \
        .filter(col("count") > 1).count()
    assert duplicate_current == 0, f"Found {duplicate_current} customers with duplicate current rows"
```

### Timeliness SLA

```
Load windows:
├─ Daily loads: complete by 4 AM UTC (1 hour from 3 AM start)
├─ Hourly loads: complete by :30 of each hour
├─ Real-time: 99th percentile latency < 60 seconds
└─ Monthly loads: complete within 3 days of period end
```

**Implementation:**
```python
def validate_timeliness(table_name, max_load_minutes=60):
    from airflow.hooks.postgres_hook import PostgresHook
    import time

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")

    # Get last load timestamp
    last_load = hook.get_first(f"""
        SELECT MAX(updated_at) FROM {table_name}
    """)[0][0]

    # Calculate latency
    minutes_since_load = (datetime.now() - last_load).total_seconds() / 60

    assert minutes_since_load <= max_load_minutes, \
        f"{table_name} hasn't been updated in {minutes_since_load:.0f} minutes (SLA: {max_load_minutes})"
```

### Uniqueness SLA

```
Row uniqueness:
├─ PK (order_id): 100% unique
├─ Deduplication in silver: no exact duplicates
└─ SCD Type 2: (customer_id, effective_date) = PK
```

**Implementation:**
```python
def validate_uniqueness(df, columns):
    total = df.count()
    unique = df.select(columns).distinct().count()
    dup_count = total - unique

    assert dup_count == 0, f"Found {dup_count} duplicate rows on columns {columns}"
```

---

## 🚨 Monitoring & Alerting

### Real-time Metrics

```python
# Capture metrics after each load
from datetime import datetime

metrics_log = {
    "timestamp": datetime.now(),
    "table": "silver.orders",
    "total_rows": 8_400,
    "null_pct": {"customer_id": 0, "order_amount": 0.01},
    "validation_passed": True,
    "load_duration_minutes": 1.5,
}

# Log to audit table
INSERT INTO audit.data_quality_metrics
VALUES (
    'silver.orders',
    8400,
    0.99,  -- completeness %
    100,   -- accuracy checks passed
    1.5,   -- load duration (min)
    True,  -- overall pass/fail
    current_timestamp()
);
```

### Dashboard Metrics (for BI team)

```sql
-- Create dashboard data source
CREATE OR REPLACE VIEW gold.data_quality_dashboard AS
SELECT
    table_name,
    DATE_TRUNC('day', metric_date) AS day,
    AVG(total_rows) AS avg_rows,
    AVG(null_pct) AS avg_null_pct,
    SUM(CASE WHEN validation_passed THEN 1 ELSE 0 END) * 100 / COUNT(*) AS pass_rate,
    AVG(load_duration_minutes) AS avg_load_minutes
FROM audit.data_quality_metrics
WHERE metric_date >= current_date() - 30
GROUP BY 1, 2
ORDER BY 1, 2 DESC;
```

### Slack Alerting

```python
def send_quality_alert(severity, message):
    import requests

    webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

    color = "🔴 CRITICAL" if severity == "critical" else "🟡 WARNING"

    payload = {
        "attachments": [{
            "color": "ff0000" if severity == "critical" else "ffaa00",
            "title": f"{color} Data Quality Alert",
            "text": message,
            "ts": int(datetime.now().timestamp())
        }]
    }

    requests.post(webhook_url, json=payload)

# Usage
if row_count_drop_pct > 0.2:
    send_quality_alert(
        "critical",
        f"silver.orders: row count dropped 25% (expected {prev_count}, got {current_count})"
    )
```

---

## 📈 Progressive Validation Strategy

### Layer-by-layer validation

```
BRONZE
├─ Schema check (columns exist, types correct)
├─ Row count check (not zero)
└─ Basic null check (not all NULLs)
    ↓ If passes → proceed to SILVER
    ↓ If fails → ALERT, don't transform

SILVER
├─ Schema check
├─ All bronze validations + FK/uniqueness
├─ SCD Type 2 logic (effective_date, is_current correct)
└─ Business rule validation (amounts > 0, dates valid)
    ↓ If passes → proceed to GOLD
    ↓ If fails → ALERT, keep previous version

GOLD
├─ All silver validations
├─ Fact table grain check (no duplicate keys)
├─ Aggregation checks (sum matches source)
└─ Materialized view refresh successful
    ↓ If passes → publish to BI tools
    ↓ If fails → ALERT, rollback
```

### Validation Framework (Pseudocode)

```python
class DataQualityValidator:
    def __init__(self, table_name, spark):
        self.table = table_name
        self.spark = spark
        self.results = []

    def validate_schema(self):
        """Check schema correctness"""
        ...
        self.results.append({"check": "schema", "passed": True})

    def validate_completeness(self):
        """Check nulls"""
        ...
        self.results.append({"check": "completeness", "passed": True})

    def validate_accuracy(self):
        """Check business rules"""
        ...
        self.results.append({"check": "accuracy", "passed": True})

    def validate_consistency(self):
        """Check FK/uniqueness"""
        ...
        self.results.append({"check": "consistency", "passed": True})

    def run_all(self):
        """Execute all validations"""
        self.validate_schema()
        self.validate_completeness()
        self.validate_accuracy()
        self.validate_consistency()

        overall_pass = all(r["passed"] for r in self.results)

        if not overall_pass:
            failed = [r["check"] for r in self.results if not r["passed"]]
            raise Exception(f"Quality checks failed: {failed}")

        return self.results

# Usage
validator = DataQualityValidator("silver.orders", spark)
results = validator.run_all()
print(f"All validations passed: {len(results)} checks")
```

---

## 📋 Quality Test Checklist

- [ ] **Schema validation**
  - [ ] All required columns present
  - [ ] Correct data types
  - [ ] Correct nullability (PK non-null, optional nullable)

- [ ] **Completeness**
  - [ ] PK columns: 100% non-null
  - [ ] Critical columns: ≥ 99% non-null
  - [ ] Log null counts by column

- [ ] **Accuracy**
  - [ ] No negative amounts
  - [ ] No future-dated records
  - [ ] Status values in valid set
  - [ ] Date ranges realistic

- [ ] **Consistency**
  - [ ] FK constraints (customer_id exists in dim)
  - [ ] No unexpected duplicates
  - [ ] SCD Type 2: max 1 is_current per dimension key

- [ ] **Timeliness**
  - [ ] Load completed within SLA window
  - [ ] Data freshness ≤ 24 hours old

- [ ] **Performance**
  - [ ] Query execution < SLA (e.g., < 2 seconds)
  - [ ] No unexpected slowdowns

---

## 🎯 Best Practices

1. ✅ **Test early, test often** — Validate at bronze, silver, gold
2. ✅ **Fail fast** — Stop pipeline if critical checks fail
3. ✅ **Alert on anomalies** — Row count drops > 20%, latency increases
4. ✅ **Log everything** — Audit table with every check result
5. ✅ **SLA-driven** — Define quality targets before building
6. ✅ **Automate testing** — No manual validation
7. ✅ **Visualize metrics** — Dashboard showing 30-day trend
8. ✅ **Version schema** — Track schema changes over time

---

<br/>

# 🇧🇷 PT — Framework de Qualidade de Dados

## Dimensões de Qualidade

- ✅ **Completeness**: % de valores não-nulos
- ✅ **Accuracy**: % aderindo às regras de negócio
- ✅ **Consistency**: valores alinhados entre tabelas
- ✅ **Timeliness**: carregado dentro do SLA
- ✅ **Uniqueness**: sem duplicatas não-esperadas

## SLAs Típicos

| Layer | Métrica | SLA |
|-------|---------|-----|
| **BRONZE** | Completeness | ≥ 98% |
| **SILVER** | Accuracy | 100% (rejeita inválidos) |
| **GOLD** | Timeliness | ≤ 1 hora de atraso |

## Estratégia de Validação Progressiva

1. **BRONZE**: Schema + row count + não-nulls
2. **SILVER**: FKs + SCD Type 2 + regras de negócio
3. **GOLD**: Grain check + aggregation check + refresh OK

## Alertas

- ❌ Row count drop > 20% → CRÍTICO
- ❌ Load delay > SLA → CRÍTICO
- ⚠️ Null % acima de 2% → WARNING
- ⚠️ Query duration 2x baseline → WARNING

Veja `pytest_data_quality.py` para implementação Pytest.
