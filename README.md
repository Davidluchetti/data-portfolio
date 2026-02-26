# 📊 Data Engineering Portfolio

**EN** | [PT](#pt-portfólio-de-engenharia-de-dados)

Senior Data Engineer portfolio showcasing **architecture patterns**, **migration strategies**, and **reusable templates** for building scalable data platforms.

---

## 🎯 Overview

This repository demonstrates core competencies in:
- ✅ **Medallion Architecture** (Bronze/Silver/Gold layers)
- ✅ **ETL/ELT pipelines** (Snowflake, Azure, Databricks, GCP)
- ✅ **Legacy system migrations** (IBM DataStage → Snowflake, Informatica → Azure)
- ✅ **CDC & SCD patterns** (Type 1, 2, 3 slowly changing dimensions)
- ✅ **Data quality & testing** (Pytest, SonarQube, row-count validation)
- ✅ **Cloud platforms** (Azure, AWS, GCP, Snowflake, Databricks)
- ✅ **Orchestration** (Apache Airflow, Azure Pipelines, GitHub Actions)

> 💡 All code samples are **generic, reusable, and production-tested** patterns extracted from real-world data platforms. No proprietary code included.

---

## 📁 Repository Structure

```
data-portfolio/
├── architecture/               # Design patterns & migration strategies
│   ├── medallion-architecture.md     # Bronze/Silver/Gold layer strategy
│   ├── cdc-scd-patterns.md          # Change Data Capture + SCD Types 1/2/3
│   ├── etl-migration-guide.md       # DataStage → Snowflake migration
│   └── data-quality-framework.md    # Testing & validation patterns
│
├── templates/                  # Reusable code templates
│   ├── airflow_dag_template.py      # Production Airflow DAG template
│   ├── pytest_data_quality.py       # Data quality testing framework
│   ├── snowflake_tasks_streams.sql  # Snowflake Tasks + Streams + Zero-Copy
│   ├── pyspark_medallion.py         # PySpark Medallion pattern
│   └── azure_adf_template.json      # Azure Data Factory template
│
├── diagrams/                   # Architecture diagrams
│   ├── medallion-layers.png         # Medallion Architecture visual
│   ├── migration-flow.png           # DataStage/Informatica → Cloud
│   └── cdc-scd-comparison.png       # CDC vs SCD comparison matrix
│
└── README.md                   # This file
```

---

## 🏗️ Architecture Patterns

### 1️⃣ Medallion Architecture (Bronze/Silver/Gold)

**Use case:** Multi-layered data warehouse for analytics at scale.

```
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Analytics Ready)                                │
│ ├─ Fact tables (sales, revenue, metrics)                   │
│ ├─ Dimension tables (customers, products, dates)           │
│ └─ Pre-computed aggregations (materialized views)          │
├─────────────────────────────────────────────────────────────┤
│ SILVER LAYER (Business Rules Applied)                      │
│ ├─ Data quality checks passed                              │
│ ├─ Business logic transformations                          │
│ ├─ SCD Type 2 dimensions (with effective dates)            │
│ └─ Deduplication & normalization                           │
├─────────────────────────────────────────────────────────────┤
│ BRONZE LAYER (Raw Ingestion)                               │
│ ├─ Raw data from source systems                            │
│ ├─ Minimal transformations (schema validation only)        │
│ ├─ Historical records preserved (CDC append-mode)          │
│ └─ Delta Lake format with versioning                       │
└─────────────────────────────────────────────────────────────┘
```

**Tech stack:** Databricks Delta Lake, Snowflake, Azure Synapse

---

### 2️⃣ CDC (Change Data Capture) + SCD (Slowly Changing Dimensions)

**Use case:** Track historical changes in dimensions while maintaining data lineage.

| Pattern | Use Case | Example |
|---------|----------|---------|
| **CDC - Full Sync** | Initial load + periodic full snapshots | Daily snapshot of all customers |
| **CDC - Incremental** | Only changed records since last run | New orders, updated orders |
| **CDC - Log-based** | Real-time streaming from transaction logs | PostgreSQL logical decoding |
| **SCD Type 1** | Overwrite (no history) | Country name change |
| **SCD Type 2** | New row + effective dates (full history) | Customer address change |
| **SCD Type 3** | Previous + current values (limited history) | Product price tracking |

**Implementation:** See `architecture/cdc-scd-patterns.md`

---

### 3️⃣ ETL/ELT Migrations (Legacy → Cloud)

**Real-world migrations:**
- ✅ **IBM DataStage** → Snowflake (Algar Telecom)
- ✅ **Informatica PowerCenter** → Azure Data Factory (Danone Group)
- ✅ **SAP extraction** → GCP BigQuery (BRF Analytics)

**Common challenges & solutions:** See `architecture/etl-migration-guide.md`

---

## 🔧 Reusable Templates

### 🪜 Airflow DAG Template
Production-ready template for orchestrating ETL pipelines:

```python
# See: templates/airflow_dag_template.py
# Features:
# - Error alerting via Slack
# - Data quality checks (row count, hash validation)
# - Sensor-based dependencies (wait for source files)
# - Retry logic with exponential backoff
# - Parallel task execution
```

### 🧪 Pytest Data Quality Framework
Framework for testing data quality at each layer:

```python
# See: templates/pytest_data_quality.py
# Test patterns:
# - Schema validation (col types, nullability)
# - Referential integrity (FK checks)
# - Business logic validation (range checks, duplicate detection)
# - Performance assertions (SLA compliance)
```

### ❄️ Snowflake Advanced Features
Modern Snowflake patterns (Tasks, Streams, Zero-Copy Cloning):

```sql
-- See: templates/snowflake_tasks_streams.sql
-- Features:
-- - Continuous data pipeline via Streams + Tasks
-- - Zero-Copy Cloning for dev/test environments
-- - Dynamic SQL for metadata-driven pipelines
-- - Row-level security (masking policies)
```

### 🔥 PySpark Medallion Pattern
Scalable multi-layer processing on Databricks:

```python
# See: templates/pyspark_medallion.py
# Processing:
# - Bronze: Auto-infer schema, preserve raw data
# - Silver: Apply business rules, SCD Type 2
# - Gold: Aggregate, denormalize for BI
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+ (for Airflow/PySpark templates)
- Snowflake account (free tier available)
- Docker (optional, for local Airflow)

### Installation

```bash
# Clone repo
git clone https://github.com/Davidluchetti/data-portfolio.git
cd data-portfolio

# (Optional) Create virtual environment
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate (Windows)

# Install dependencies (if applicable)
pip install apache-airflow pyspark pytest snowflake-connector-python
```

### Usage Examples

**1. Read Medallion Architecture guide:**
```bash
cat architecture/medallion-architecture.md
```

**2. Adapt Airflow template for your use case:**
```bash
cp templates/airflow_dag_template.py my_pipeline.py
# Edit task_id, dag_id, dependencies
```

**3. Run data quality tests:**
```bash
pytest templates/pytest_data_quality.py -v
```

---

## 📚 Architecture Documentation

| Document | Focus |
|----------|-------|
| [`medallion-architecture.md`](./architecture/medallion-architecture.md) | Layer design, partition strategy, incremental loading |
| [`cdc-scd-patterns.md`](./architecture/cdc-scd-patterns.md) | CDC techniques, SCD implementations, watermarks |
| [`etl-migration-guide.md`](./architecture/etl-migration-guide.md) | DataStage/Informatica → modern cloud migration |
| [`data-quality-framework.md`](./architecture/data-quality-framework.md) | Testing strategy, metrics, SLAs |

---

## 🛠️ Tech Stack Covered

| Category | Technologies |
|----------|---------------|
| **Cloud** | Snowflake, Azure, GCP BigQuery, AWS S3/Redshift, Databricks |
| **ETL/ELT** | Apache Airflow, Azure Data Factory, Informatica, IBM DataStage |
| **Languages** | Python, PySpark, SQL, Java |
| **Databases** | PostgreSQL, Oracle, SQL Server, MongoDB |
| **Testing** | Pytest, SonarQube, dbt tests |
| **DevOps** | GitHub Actions, Azure Pipelines, Docker |

---

## 📈 Real-World Context

**Experience implementing these patterns:**
- 🏦 **Financial Services** (XP Investimentos) — High-frequency trading data
- 📱 **Telecom** (Algar Telecom) — Billing + network event processing
- 📦 **FMCG** (Danone Group) — Global supply chain analytics

All patterns in this repo are battle-tested in production environments.

---

## 🤝 Contributing

This is a portfolio repo — contributions welcome for improvements, additional patterns, or clarifications.

For questions or suggestions:
- 📧 Email: [LinkedIn](https://linkedin.com/in/david-luchetti-b04ab3182)
- 📄 Resume: [Senior Data Engineer EN](https://github.com/Davidluchetti/cv/raw/main/David_Luchetti_Senior_Data_Engineer_EN.pdf) | [PT](https://github.com/Davidluchetti/cv/raw/main/David_Luchetti_Engenheiro_Dados_Senior_PT.pdf)

---

## 📄 License

All code and documentation are provided as examples for learning and professional reference.

---

<br/>

# 🇧🇷 PT — Portfólio de Engenharia de Dados

Portfólio de **Engenheiro de Dados Sênior** com mais de 6 anos de experiência, mostrando **padrões de arquitetura**, **estratégias de migração** e **templates reutilizáveis** para construir plataformas de dados escaláveis.

---

## 🎯 Visão Geral

Este repositório demonstra competências core em:
- ✅ **Arquitetura Medallion** (camadas Bronze/Silver/Gold)
- ✅ **Pipelines ETL/ELT** (Snowflake, Azure, Databricks, GCP)
- ✅ **Migrações de sistemas legados** (IBM DataStage → Snowflake, Informatica → Azure)
- ✅ **Padrões CDC & SCD** (Tipos 1, 2, 3)
- ✅ **Qualidade de dados & testes** (Pytest, SonarQube, validação de row-count)
- ✅ **Plataformas cloud** (Azure, AWS, GCP, Snowflake, Databricks)
- ✅ **Orquestração** (Apache Airflow, Azure Pipelines, GitHub Actions)

> 💡 Todos os exemplos de código são **padrões genéricos, reutilizáveis e testados em produção**, extraídos de plataformas de dados reais. Nenhum código proprietário incluído.

---

## 📁 Estrutura do Repositório

```
data-portfolio/
├── architecture/               # Padrões de design & estratégias de migração
│   ├── medallion-architecture.md     # Estratégia Bronze/Silver/Gold
│   ├── cdc-scd-patterns.md          # CDC + Tipos SCD 1/2/3
│   ├── etl-migration-guide.md       # Migração DataStage → Snowflake
│   └── data-quality-framework.md    # Padrões de testes & validação
│
├── templates/                  # Templates de código reutilizáveis
│   ├── airflow_dag_template.py      # DAG Airflow para produção
│   ├── pytest_data_quality.py       # Framework de testes de qualidade
│   ├── snowflake_tasks_streams.sql  # Snowflake Tasks + Streams
│   ├── pyspark_medallion.py         # PySpark padrão Medallion
│   └── azure_adf_template.json      # Template Azure Data Factory
│
├── diagrams/                   # Diagramas de arquitetura
│   ├── medallion-layers.png         # Visual Medallion Architecture
│   ├── migration-flow.png           # Fluxo de migração
│   └── cdc-scd-comparison.png       # Comparação CDC vs SCD
│
└── README.md                   # Este arquivo
```

---

## 🏗️ Padrões de Arquitetura

### Medallion (Bronze/Silver/Gold)
Camadas de processamento com data lake Snowflake/Databricks. Bronze: dados brutos. Silver: regras de negócio aplicadas. Gold: pronto para analytics.

### CDC + SCD
Captura de mudanças + dimensões que mudam lentamente. Rastreamento de histórico completo com SCD Type 2.

### Migrações ETL/ELT
Estratégias reais de migração de DataStage, Informatica PowerCenter para cloud moderno.

---

## 🚀 Próximos Passos

1. ✅ Clone o repo
2. ✅ Leia `architecture/medallion-architecture.md`
3. ✅ Adapte templates para seu caso de uso
4. ✅ Rode testes de qualidade (`pytest`)

---

## 📞 Contato

- **GitHub:** [Davidluchetti](https://github.com/Davidluchetti)
- **LinkedIn:** [david-luchetti](https://linkedin.com/in/david-luchetti-b04ab3182)
- **Currículo:** [PT](https://github.com/Davidluchetti/cv/raw/main/David_Luchetti_Engenheiro_Dados_Senior_PT.pdf) | [EN](https://github.com/Davidluchetti/cv/raw/main/David_Luchetti_Senior_Data_Engineer_EN.pdf)

---

**Atualizado:** Fevereiro 2026
