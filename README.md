# Data Engineering Portfolio

Personal reference of architecture patterns, migration strategies and reusable templates from production data platforms.

All code samples are generic extractions — no proprietary logic.

---

## Contents

```
├── architecture/
│   ├── medallion-architecture.md     # Bronze/Silver/Gold layer design
│   ├── cdc-scd-patterns.md           # CDC + SCD Types 1/2/3
│   ├── etl-migration-guide.md        # DataStage/Informatica → Cloud
│   └── data-quality-framework.md     # Testing and SLA patterns
│
└── templates/
    ├── airflow_dag_template.py        # Production DAG with retry, sensors, alerting
    ├── pytest_data_quality.py         # Data quality test framework
    ├── snowflake_tasks_streams.sql    # Snowflake Tasks + Streams + Zero-Copy
    ├── pyspark_medallion.py           # PySpark medallion pattern (Bronze→Gold)
    └── azure_adf_template.json        # Azure Data Factory pipeline template
```

---

## Architecture Patterns

### Medallion (Bronze / Silver / Gold)

Three-layer pattern for data lakes on Snowflake, Databricks or Azure Synapse.

```
GOLD    — analytics-ready: fact/dim tables, pre-aggregated metrics
SILVER  — business rules applied: SCD Type 2, deduplication, enrichment
BRONZE  — raw ingestion: append-only, schema inference, metadata capture
```

See [`architecture/medallion-architecture.md`](./architecture/medallion-architecture.md)

### CDC + SCD

Change Data Capture combined with Slowly Changing Dimensions.

| Pattern | When to use |
|---------|-------------|
| CDC full sync | Initial loads, daily snapshots |
| CDC incremental | New/changed records since last run |
| CDC log-based | Real-time from DB transaction logs |
| SCD Type 1 | Overwrite (no history needed) |
| SCD Type 2 | Full history with effective dates |
| SCD Type 3 | Limited history (prev + current values) |

See [`architecture/cdc-scd-patterns.md`](./architecture/cdc-scd-patterns.md)

### Legacy ETL Migrations

Real migrations:
- IBM DataStage → Snowflake (Algar Telecom)
- Informatica PowerCenter → Azure Data Factory (Danone Group)
- PowerCenter → Informatica IICS (BRFConsulting / RedPill Analytics)
- Legacy pipelines → GCP BigQuery (Tenbu)

See [`architecture/etl-migration-guide.md`](./architecture/etl-migration-guide.md)

---

## Templates

**`airflow_dag_template.py`** — production DAG with:
- Slack alerting on failure
- Row-count and hash data quality checks
- HttpSensor for upstream dependencies
- Exponential retry backoff
- Parallel task groups

**`pytest_data_quality.py`** — test patterns for:
- Schema validation (col types, nullability)
- Referential integrity checks
- Business rule assertions
- SLA/performance tests

**`snowflake_tasks_streams.sql`** — Snowflake advanced features:
- Continuous pipelines via Streams + Tasks
- Zero-copy cloning for dev environments
- Metadata-driven dynamic SQL
- Row-level security (masking policies)

**`pyspark_medallion.py`** — Databricks Delta Lake:
- Bronze: auto-schema, raw preservation
- Silver: SCD Type 2, business rules
- Gold: aggregations, denormalization

**`azure_adf_template.json`** — parameterized ADF pipeline template

---

## Context

Patterns extracted from experience across:
- Financial services — XP Investimentos (equities/trading data)
- Telecom — Algar Telecom (billing + network events)
- FMCG — Danone Group (global supply chain)

---

## Links

- [Portfolio](https://davidluchetti.github.io) | [LinkedIn](https://linkedin.com/in/david-luchetti-b04ab3182)
- [Resume EN](https://drive.google.com/file/d/10E7iH3LXhXyFP2Mm43qmwXP6JlnGBx6z/view) | [Currículo PT](https://drive.google.com/file/d/1LYSJQUTE6V0e8ZoWUK1zlIsuafk2Xwlq/view)
