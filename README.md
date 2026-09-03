# Enterprise ETL Pipeline & Data Warehouse Synchronizer

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Data Processing](https://img.shields.io/badge/Polars-High--Performance-orange.svg)](https://pola.rs/)
[![Data Validation](https://img.shields.io/badge/Pydantic-v2-green.svg)](https://docs.pydantic.dev/)
[![Tests](https://img.shields.io/badge/Pytest-Passing-brightgreen.svg)](https://pytest.org)

An enterprise-grade, resilient Python ETL (Extract, Transform, Load) pipeline designed to extract business data from disparate third-party APIs (**Stripe** & **Salesforce**), handle rate limits, cursor-based pagination, and resilient retries, stage raw partitioned JSON in **AWS S3**, clean and standardize data using **Polars**, map fields into a canonical **Unified Data Warehouse Schema**, and validate data quality with **Pytest**.

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
                                  WEEK 2: POLARS TRANSFORMATION & WAREHOUSE CANONICAL MAPPING
                                                                     ▼
                                         ┌────────────────────────────────────────────────────────┐
                                         │  Polars & Pandas Transformation Engine                 │
                                         │  • Null Imputation & Whitespace Cleaning               │
                                         │  • UTC ISO-8601 Timestamp Standardization              │
                                         │  • Cents-to-Dollars Currency Normalization             │
                                         │  • Cross-Source Entity Deduplication (Stripe + SF)     │
                                         └───────────────────────────┬────────────────────────────┘
                                                                     │
                                                                     ▼
                                         ┌────────────────────────────────────────────────────────┐
                                         │  Curated Warehouse Canonical Layer                     │
                                         │  • dim_customers.parquet     (UnifiedCustomer)         │
                                         │  • fact_transactions.parquet (UnifiedTransaction)      │
                                         │  • fact_subscriptions.parquet(UnifiedSubscription)     │
                                         │  • dim_companies.parquet     (UnifiedCompanyAccount)   │
                                         │  • Data Quality Validation (Pytest + Pydantic)         │
                                         └────────────────────────────────────────────────────────┘
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

## 📁 Repository Structure

```
d:/ETL_PIPE_LINE_PROJECT/
├── config/
│   ├── __init__.py
│   └── settings.py               # Pydantic Settings management (.env validation, secrets, AWS)
├── models/
│   ├── __init__.py
│   ├── raw/
│   │   ├── __init__.py
│   │   ├── stripe_models.py      # Stripe raw Pydantic schemas
│   │   └── salesforce_models.py  # Salesforce raw Pydantic schemas
│   └── unified/
│       ├── __init__.py
│       └── canonical_models.py   # Unified Canonical models (UnifiedCustomer, UnifiedTransaction, etc.)
├── extractors/
│   ├── __init__.py
│   ├── base.py                   # BaseExtractor with Token-Bucket limiter, retries, S3 staging
│   ├── stripe_extractor.py       # Stripe cursor pagination (starting_after)
│   └── salesforce_extractor.py   # Salesforce SOQL QueryLocator pagination
├── storage/
│   ├── __init__.py
│   ├── s3_client.py              # AWS S3 partitioned raw JSON writer & local staging
│   └── state_store.py            # High-watermark & cursor persistence
├── mock_api/
│   ├── __init__.py
│   ├── stripe_mock.py            # Mock Stripe pagination & rate-limit simulator
│   └── salesforce_mock.py        # Mock Salesforce SOQL query & QueryLocator simulator
├── transformers/
│   ├── __init__.py
│   ├── cleaners.py               # Polars data cleaners (dates, currency, nulls, emails)
│   ├── stripe_transformer.py     # Stripe Polars transformation to canonical schemas
│   ├── salesforce_transformer.py # Salesforce Polars transformation to canonical schemas
│   └── unified_mapper.py         # Cross-source entity deduplication & quality validation
├── pipeline/
│   ├── __init__.py
│   └── runner.py                 # CLI orchestration pipeline runner with Rich tables
├── tests/
│   ├── __init__.py
│   ├── conftest.py               # Shared test fixtures & mock environments
│   ├── test_models.py            # Day 1-2: Pydantic model validation tests
│   ├── test_extractors.py        # Day 3-5: Cursor pagination tests
│   ├── test_rate_limiter_s3.py   # Day 6-7: Rate-limit backoff & S3 staging tests
│   ├── test_cleaners.py          # Week 2 Day 1-3: Polars cleaning tests
│   ├── test_transformers.py      # Week 2 Day 4-6: Schema mapping & unification tests
│   └── test_end_to_end.py        # Week 2 Day 7: Full pipeline end-to-end integration tests
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
*(Default settings are already pre-configured for local mock testing mode!)*

---

## ⚡ Running the Pipeline

### Run Full Pipeline (Mock Mode - Zero External Setup Required)
```bash
python -m pipeline.runner --source all --mode mock
```

### Run Extraction for a Specific Source
```bash
python -m pipeline.runner --source stripe --mode mock
python -m pipeline.runner --source salesforce --mode mock
```

### Reset High-Watermark State
```bash
python -m pipeline.runner --source all --mode mock --clean-state
```

---

## 🧪 Running the Pytest Suite

Run all unit, integration, and data quality tests:
```bash
pytest -v --tb=short
```

To run tests by phase:
```bash
# Week 1 Day 1-2: Pydantic Models & Settings
pytest tests/test_models.py -v

# Week 1 Day 3-5: Cursor Pagination
pytest tests/test_extractors.py -v

# Week 1 Day 6-7: Rate Limiting & S3 Storage
pytest tests/test_rate_limiter_s3.py -v

# Week 2 Day 1-3: Polars Cleaning
pytest tests/test_cleaners.py -v

# Week 2 Day 4-6: Schema Unification
pytest tests/test_transformers.py -v

# Week 2 Day 7: End-to-End Pipeline & Quality Validation
pytest tests/test_end_to_end.py -v
```

---

## 📊 Curated Canonical Schemas

| Entity | Canonical Model | Output Files | Key Attributes |
|---|---|---|---|
| **Customers** | `UnifiedCustomer` | `dim_customers.parquet`, `.json` | `unified_customer_id`, `email`, `full_name`, `company_name`, `preferred_currency` |
| **Transactions** | `UnifiedTransaction` | `fact_transactions.parquet`, `.json` | `unified_transaction_id`, `amount`, `amount_refunded`, `net_amount`, `status`, `currency` |
| **Subscriptions**| `UnifiedSubscription` | `fact_subscriptions.parquet`, `.json`| `unified_subscription_id`, `status`, `mrr_amount`, `current_period_start`, `current_period_end` |
| **Companies** | `UnifiedCompanyAccount`| `dim_companies.parquet`, `.json` | `unified_account_id`, `company_name`, `industry`, `annual_revenue`, `employee_count` |
