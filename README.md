# Enterprise ETL Pipeline & Data Warehouse Synchronizer

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Data Processing](https://img.shields.io/badge/Polars-High--Performance-orange.svg)](https://pola.rs/)
[![Data Validation](https://img.shields.io/badge/Pydantic-v2-green.svg)](https://docs.pydantic.dev/)
[![Database](https://img.shields.io/badge/SQLAlchemy-2.0-red.svg)](https://www.sqlalchemy.org/)
[![Orchestration](https://img.shields.io/badge/Apache%20Airflow-2.9+-teal.svg)](https://airflow.apache.org/)
[![Container](https://img.shields.io/badge/Docker-Multi--Stage-blue.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Pytest-49%20Passing-brightgreen.svg)](https://pytest.org)

An enterprise-grade, resilient Python ETL (Extract, Transform, Load) pipeline and data synchronization engine designed to:
1. Extract business data from third-party APIs (**Stripe** & **Salesforce**) with token-bucket rate limiting, cursor pagination, and exponential backoff retries.
2. Stage partitioned GZIP raw JSON in **AWS S3** Data Lake.
3. Clean, normalize, and reconcile disparate data using **Polars** into a curated canonical **Unified Schema**.
4. Perform dialect-aware **idempotent upserts (`ON CONFLICT DO UPDATE` / `MERGE INTO`)** into target Data Warehouses (**PostgreSQL**, **Snowflake**, or **SQLite**).
5. Orchestrate schedules with **Apache Airflow DAGs**, broadcast rich **Slack Block Kit** and **HTML Email** alerts, and deploy via **Docker Compose** & **GitHub Actions CI/CD**.

---

## 🏗️ Architecture Overview

```
                                  WEEK 1: EXTRACTION & DATA LAKE STAGING
┌──────────────────────────┐             ┌────────────────────────────────────────────────────────┐
│  Third-Party APIs        │             │  Resilient Extractor Core                              │
│  • Stripe REST API       │ ──────────► │  • Token-Bucket Rate Limiter (Max RPS & Bursts)        │
│  • Salesforce REST/SOQL  │  HTTP / SSL │  • Tenacity Exponential Backoff (429 Retry-After & 5xx)│
└──────────────────────────┘             │  • Cursor Pagination (starting_after & QueryLocator)   │
                                         └───────────────────────────┬────────────────────────────┘
                                                                     │
                                                                     ▼
                                         ┌────────────────────────────────────────────────────────┐
                                         │  AWS S3 Partitioned Raw Data Lake                      │
                                         │  raw/<source>/<entity>/year=YYYY/month=MM/day=DD/      │
                                         │  • GZIP Compressed JSON Batches                        │
                                         │  • Incremental Watermark State Tracking                │
                                         └───────────────────────────┬────────────────────────────┘
                                                                     │
                                  WEEK 2: POLARS TRANSFORMATION & CANONICAL MAPPING
                                                                     ▼
                                         ┌────────────────────────────────────────────────────────┐
                                         │  Polars & Pandas Transformation Engine                 │
                                         │  • Null Imputation & Whitespace Cleaning               │
                                         │  • UTC ISO-8601 Timestamp Standardization              │
                                         │  • Cents-to-Dollars Currency Normalization             │
                                         │  • Cross-Source Entity Deduplication (Stripe + SF)     │
                                         └───────────────────────────┬────────────────────────────┘
                                                                     │
                                  WEEK 3: WAREHOUSE LOADING & DIALECT-AWARE UPSERT
                                                                     ▼
                                         ┌────────────────────────────────────────────────────────┐
                                         │  SQLAlchemy 2.0 Warehouse Loading Engine               │
                                         │  • PostgreSQL / SQLite: ON CONFLICT DO UPDATE          │
                                         │  • Snowflake: Atomic MERGE INTO strategy               │
                                         │  • Tables: dim_customers, fact_transactions,           │
                                         │            fact_subscriptions, dim_companies           │
                                         │  • Audit Trail: etl_load_audit execution logging       │
                                         └───────────────────────────┬────────────────────────────┘
                                                                     │
                                  WEEK 4: ORCHESTRATION, ALERTING & DEPLOYMENT
                                                                     ▼
┌──────────────────────────────────────┬──────────────────────────────────┬───────────────────────┐
│ Apache Airflow DAGs (`dags/`)        │ Alerting Engine (`notifications/`)│ Docker & CI/CD        │
│ • Daily Full ETL DAG                 │ • Slack Block Kit Alerting       │ • Multi-Stage Docker  │
│ • Hourly Incremental Watermark DAG   │ • HTML Email / SMTP Alerting     │ • docker-compose stack│
│ • TaskFlow API + Failure Callbacks   │ • Multi-Channel Alert Manager    │ • GitHub Actions CI/CD│
└──────────────────────────────────────┴──────────────────────────────────┴───────────────────────┘
```

---

## 📅 Project Implementation Breakdown

### 🔹 Week 1: API Integration & Data Extraction
* **Day 1-2: Pydantic Data Models & Environment Management**
  * Configured `pydantic-settings` (`config/settings.py`) with secure environment variable loading and secret masking.
  * Defined raw models for Stripe (`StripeCustomer`, `StripeCharge`, `StripeInvoice`, `StripeSubscription`) and Salesforce (`SalesforceAccount`, `SalesforceContact`, `SalesforceOpportunity`, `SalesforceOrder`).
* **Day 3-5: Extraction Scripts with Cursor-Based Pagination**
  * `StripeExtractor`: Implemented cursor-based pagination using `starting_after` and incremental filtering via `created[gte]`.
  * `SalesforceExtractor`: Implemented SOQL queries with `nextRecordsUrl` QueryLocator pagination and `LastModifiedDate` watermarking.
* **Day 6-7: Rate-Limit Handling & AWS S3 Staging**
  * Developed `TokenBucketRateLimiter` and `tenacity` exponential retry policies handling HTTP 429 (`Retry-After` header) and 5xx errors.
  * Developed `S3DataLakeWriter` for partitioned GZIP raw JSON storage (`raw/{source}/{entity}/year=YYYY/month=MM/day=DD/`) with local fallback.

---

### 🔹 Week 2: Data Transformation & Validation
* **Day 1-3: Polars/Pandas Cleaning & Normalization**
  * Vectorized string trimming, email normalization, and null imputation (`transformers/cleaners.py`).
  * Standardized all timestamps to UTC ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`).
  * Converted currency amounts from integer cents into 2-decimal standard units (e.g. 5000 cents -> 50.00 USD).
* **Day 4-6: Schema Mapping & Unified Canonical Representation**
  * Built `StripeDataTransformer` and `SalesforceDataTransformer` to map disparate source entities to `UnifiedCustomer`, `UnifiedTransaction`, and `UnifiedSubscription`.
  * Built `UnifiedSchemaMapper` for cross-source entity deduplication matching on normalized emails.
* **Day 7: Pytest Unit & Integration Testing Suite**
  * Comprehensive test suite validating data models, cursor pagination, rate limits, S3 compression, data cleaning, schema mapping, and end-to-end pipeline execution.

---

### 🔹 Week 3: Data Loading & Database Sync
* **Day 1-3: SQLAlchemy 2.0 Warehouse Connection & ORM Schemas**
  * Configured `WarehouseConnectionManager` supporting **PostgreSQL**, **Snowflake**, and **SQLite** (local dev / testing).
  * Built ORM models (`warehouse/schema.py`) for `DimCustomer`, `FactTransaction`, `FactSubscription`, `DimCompany`, and `ETLLoadAudit`.
* **Day 4-6: Dialect-Aware Upsert Logic**
  * Implemented `WarehouseUpsertLoader` with native `ON CONFLICT DO UPDATE` for PostgreSQL and SQLite, and `MERGE INTO` staging logic for Snowflake.
  * Added chunking, in-memory batch deduplication, and accurate inserted vs. updated row counting.
* **Day 7: End-to-End Extraction, Transformation & Loading Integration**
  * Integrated database syncing into `pipeline/runner.py`.
  * Built automated idempotency verification tests ensuring zero duplicated records on successive runs.

---

### 🔹 Week 4: Orchestration, Monitoring & Deployment
* **Day 1-3: Apache Airflow DAGs**
  * `enterprise_etl_daily_dag.py`: Full daily pipeline (Pre-flight check -> Parallel extraction -> Polars transformation -> Data Quality Gate -> Warehouse Upsert -> Success Notification).
  * `enterprise_etl_hourly_incremental_dag.py`: Hourly watermark-based micro-batch sync.
  * `airflow_utils.py`: Failure callbacks and context extractors.
* **Day 4-5: Slack & Email Alerting Mechanisms**
  * `SlackNotifier`: Rich Block Kit alerts with visual indicators (🟢 Success, 🟡 Quality Warning, 🔴 Failure) and execution metrics.
  * `EmailNotifier`: Modern responsive HTML email alerts sent via SMTP.
  * `AlertManager`: Unified dispatcher routing pipeline events and data quality warnings.
* **Day 6-7: Containerization & CI/CD Pipelines**
  * `Dockerfile`: Multi-stage, production-ready Python 3.11 container with non-root security.
  * `docker-compose.yml`: Complete local stack with PostgreSQL Warehouse, Airflow Webserver, Airflow Scheduler, and ETL Runner.
  * `.github/workflows/ci_cd.yml`: GitHub Actions workflow automating linting, testing, and Docker builds.

---

## 📁 Repository Structure

```
d:/ETL_PIPE_LINE_PROJECT/
├── config/
│   ├── __init__.py
│   └── settings.py                     # Pydantic Settings (API, S3, Warehouse, Notifications, Airflow)
├── models/
│   ├── __init__.py
│   ├── raw/
│   │   ├── stripe_models.py            # Stripe raw Pydantic schemas
│   │   └── salesforce_models.py        # Salesforce raw Pydantic schemas
│   └── unified/
│       └── canonical_models.py         # Unified Canonical schemas (UnifiedCustomer, UnifiedTransaction, etc.)
├── extractors/
│   ├── __init__.py
│   ├── base.py                         # BaseExtractor with Token-Bucket limiter, retries, S3 staging
│   ├── stripe_extractor.py             # Stripe cursor pagination (starting_after)
│   └── salesforce_extractor.py         # Salesforce SOQL QueryLocator pagination
├── storage/
│   ├── __init__.py
│   ├── s3_client.py                    # AWS S3 partitioned raw JSON writer & local staging
│   └── state_store.py                  # High-watermark & cursor persistence
├── mock_api/
│   ├── __init__.py
│   ├── stripe_mock.py                  # Mock Stripe pagination & rate-limit simulator
│   └── salesforce_mock.py              # Mock Salesforce SOQL query simulator
├── transformers/
│   ├── __init__.py
│   ├── cleaners.py                     # Polars data cleaners (dates, currency, nulls, emails)
│   ├── stripe_transformer.py           # Stripe Polars transformation to canonical schemas
│   ├── salesforce_transformer.py       # Salesforce Polars transformation to canonical schemas
│   └── unified_mapper.py               # Cross-source entity deduplication & quality validation
├── warehouse/
│   ├── __init__.py
│   ├── connection.py                   # SQLAlchemy 2.0 Engine & Session lifecycle manager
│   ├── schema.py                       # Dimension, Fact, and Audit table ORM definitions
│   └── loader.py                       # Dialect-aware Upsert (ON CONFLICT DO UPDATE) Engine
├── notifications/
│   ├── __init__.py
│   ├── slack.py                        # Slack Block Kit notification dispatcher
│   ├── email.py                        # SMTP HTML email reporting
│   └── manager.py                      # Central multi-channel AlertManager
├── dags/
│   ├── __init__.py
│   ├── airflow_utils.py                # Airflow failure callbacks & task hooks
│   ├── enterprise_etl_daily_dag.py     # Airflow Daily Full ETL DAG
│   └── enterprise_etl_hourly_incremental_dag.py # Airflow Hourly Incremental Sync DAG
├── pipeline/
│   ├── __init__.py
│   └── runner.py                       # Unified CLI runner with Rich tables
├── tests/
│   ├── conftest.py                     # Test fixtures
│   ├── test_models.py                  # Pydantic model validation tests
│   ├── test_extractors.py              # Cursor pagination tests
│   ├── test_rate_limiter_s3.py         # Rate-limit backoff & S3 staging tests
│   ├── test_cleaners.py                # Polars cleaning tests
│   ├── test_transformers.py            # Schema unification tests
│   ├── test_end_to_end.py              # Week 2 integration tests
│   ├── test_warehouse_connection.py    # Week 3 SQLAlchemy connection tests
│   ├── test_upsert_loader.py           # Week 3 Upsert & idempotency tests
│   ├── test_notifications.py           # Week 4 Slack & Email alerting tests
│   ├── test_airflow_dags.py            # Week 4 Airflow DAG tests
│   └── test_end_to_end_loading.py      # Full 4-Week End-to-End integration test
├── .github/workflows/
│   └── ci_cd.yml                       # GitHub Actions CI/CD Pipeline
├── Dockerfile                          # Multi-stage production container definition
├── docker-compose.yml                  # Local development stack (Postgres + Airflow + ETL)
├── .dockerignore
├── .env.example
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 🚀 Quickstart & Installation

### 1. Clone & Install Dependencies
```bash
git clone <repo-url>
cd ETL_PIPE_LINE_PROJECT
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
*(Default settings are pre-configured for local offline mock testing mode!)*

---

## ⚡ Running the Pipeline

### Run Full 4-Week Pipeline (Mock Mode — Extract + Transform + Quality Check + Warehouse Upsert)
```bash
python -m pipeline.runner --source all --mode mock
```

### Reset High-Watermark State & Re-sync
```bash
python -m pipeline.runner --source all --mode mock --clean-state
```

### Run Extraction Only for Stripe or Salesforce
```bash
python -m pipeline.runner --source stripe --mode mock
python -m pipeline.runner --source salesforce --mode mock
```

### Connect to External PostgreSQL / Snowflake Warehouse
```bash
python -m pipeline.runner --source all --mode live --db-url "postgresql+psycopg2://postgres:password@localhost:5432/enterprise_dw"
```

---

## 🐳 Docker & Airflow Orchestration

### Start Complete Stack (PostgreSQL Data Warehouse + Apache Airflow UI)
```bash
docker compose up -d
```

- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Username: `admin` / Password: `admin`)
- **PostgreSQL Warehouse**: `localhost:5432` (Database: `enterprise_dw`, User: `postgres`, Password: `postgrespassword`)

### Run ETL Container Standalone
```bash
docker build -t enterprise-etl-pipeline:latest .
docker run --rm enterprise-etl-pipeline:latest --source all --mode mock
```

---

## 🧪 Running the Pytest Suite

Run all **49** unit, integration, and data quality tests:
```bash
pytest -v --tb=short
```

To run tests by milestone:
```bash
# Week 1: Models, Pagination, Rate Limiting & S3
pytest tests/test_models.py tests/test_extractors.py tests/test_rate_limiter_s3.py -v

# Week 2: Polars Cleaning, Transformation & Quality Validation
pytest tests/test_cleaners.py tests/test_transformers.py tests/test_end_to_end.py -v

# Week 3: SQLAlchemy Connection & Upsert Loader
pytest tests/test_warehouse_connection.py tests/test_upsert_loader.py -v

# Week 4: Slack/Email Alerts, Airflow DAGs & Full Pipeline Sync
pytest tests/test_notifications.py tests/test_airflow_dags.py tests/test_end_to_end_loading.py -v
```

---

## 📊 Curated Canonical Schemas & Warehouse Tables

| Target Table | Canonical Model | Key Attributes | Upsert Conflict Resolution |
|---|---|---|---|
| `dim_customers` | `UnifiedCustomer` | `unified_customer_id`, `email`, `full_name`, `company_name`, `preferred_currency` | `ON CONFLICT (unified_customer_id) DO UPDATE` |
| `fact_transactions` | `UnifiedTransaction` | `unified_transaction_id`, `amount`, `amount_refunded`, `net_amount`, `status`, `currency` | `ON CONFLICT (unified_transaction_id) DO UPDATE` |
| `fact_subscriptions` | `UnifiedSubscription` | `unified_subscription_id`, `status`, `mrr_amount`, `current_period_start`, `current_period_end` | `ON CONFLICT (unified_subscription_id) DO UPDATE` |
| `dim_companies` | `UnifiedCompanyAccount` | `unified_account_id`, `company_name`, `industry`, `annual_revenue`, `employee_count` | `ON CONFLICT (unified_account_id) DO UPDATE` |
| `etl_load_audit` | `ETLLoadAudit` | `batch_id`, `target_table`, `rows_processed`, `rows_inserted`, `rows_updated`, `duration_seconds` | Append-only execution audit log |
