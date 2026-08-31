"""
Salesforce Raw-to-Unified Transformer using Polars.
Transforms staged raw Salesforce JSON payloads into Unified Canonical Entities.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import polars as pl

from transformers.cleaners import (
    clean_and_normalize_strings,
    handle_null_imputations,
    normalize_emails,
    standardize_currency_amounts,
    standardize_currency_codes,
    standardize_timestamps,
)

logger = logging.getLogger(__name__)


def _generate_deterministic_id(prefix: str, *keys: str) -> str:
    """Generates a deterministic hash-based ID."""
    seed = "::".join(str(k) for k in keys)
    hash_digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{hash_digest}"


class SalesforceDataTransformer:
    """Transforms raw Salesforce records into canonical warehouse schemas using Polars."""

    @classmethod
    def transform_accounts(
        cls, records: List[Dict[str, Any]]
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Transforms raw Salesforce Account records into:
        1. UnifiedCompanyAccount DataFrame
        2. UnifiedCustomer (B2B Account Level) DataFrame
        """
        company_schema = {
            "unified_account_id": pl.String,
            "source_system": pl.String,
            "source_account_id": pl.String,
            "company_name": pl.String,
            "industry": pl.String,
            "annual_revenue": pl.Float64,
            "employee_count": pl.Int64,
            "website": pl.String,
            "phone": pl.String,
            "billing_country": pl.String,
            "created_at": pl.String,
            "updated_at": pl.String,
            "ingestion_timestamp": pl.String,
        }
        cust_schema = {
            "unified_customer_id": pl.String,
            "source_system": pl.String,
            "source_id": pl.String,
            "stripe_customer_id": pl.String,
            "salesforce_account_id": pl.String,
            "salesforce_contact_id": pl.String,
            "email": pl.String,
            "full_name": pl.String,
            "first_name": pl.String,
            "last_name": pl.String,
            "company_name": pl.String,
            "phone": pl.String,
            "billing_street": pl.String,
            "billing_city": pl.String,
            "billing_state": pl.String,
            "billing_postal_code": pl.String,
            "billing_country": pl.String,
            "preferred_currency": pl.String,
            "is_active": pl.Boolean,
            "source_created_at": pl.String,
            "source_updated_at": pl.String,
            "ingestion_timestamp": pl.String,
        }

        if not records:
            return pl.DataFrame(schema=company_schema), pl.DataFrame(schema=cust_schema)

        normalized = []
        for r in records:
            normalized.append({
                "Id": str(r.get("Id", "")),
                "Name": str(r.get("Name", "Unnamed Account")),
                "Industry": r.get("Industry"),
                "AnnualRevenue": r.get("AnnualRevenue", 0.0),
                "NumberOfEmployees": r.get("NumberOfEmployees"),
                "Website": r.get("Website"),
                "Phone": r.get("Phone"),
                "BillingStreet": r.get("BillingStreet"),
                "BillingCity": r.get("BillingCity"),
                "BillingState": r.get("BillingState"),
                "BillingPostalCode": r.get("BillingPostalCode"),
                "BillingCountry": r.get("BillingCountry"),
                "CreatedDate": r.get("CreatedDate"),
                "LastModifiedDate": r.get("LastModifiedDate"),
            })

        df = pl.DataFrame(normalized)

        # 1. Clean strings & standardize timestamps
        df = clean_and_normalize_strings(df)
        df = standardize_timestamps(df, timestamp_cols=["CreatedDate", "LastModifiedDate"])
        df = standardize_currency_amounts(
            df, amount_col="AnnualRevenue", is_in_cents=False, target_col="annual_revenue_std"
        )

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_ids = df["Id"].to_list()

        # --- 1. Build UnifiedCompanyAccount DataFrame ---
        account_ids = [_generate_deterministic_id("org", "salesforce", sid) for sid in source_ids]

        company_df = df.with_columns([
            pl.Series("unified_account_id", account_ids, dtype=pl.String),
            pl.lit("salesforce", dtype=pl.String).alias("source_system"),
            pl.col("Id").cast(pl.String).alias("source_account_id"),
            pl.col("Name").cast(pl.String).alias("company_name"),
            pl.col("Industry").cast(pl.String).alias("industry"),
            pl.col("annual_revenue_std").alias("annual_revenue"),
            pl.col("NumberOfEmployees").cast(pl.Int64, strict=False).alias("employee_count"),
            pl.col("Website").cast(pl.String).alias("website"),
            pl.col("Phone").cast(pl.String).alias("phone"),
            pl.col("BillingCountry").cast(pl.String).alias("billing_country"),
            pl.col("CreatedDate").cast(pl.String).alias("created_at"),
            pl.col("LastModifiedDate").cast(pl.String).alias("updated_at"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        company_cols = list(company_schema.keys())
        company_df = company_df.select(company_cols)

        # --- 2. Build UnifiedCustomer (Account Level) DataFrame ---
        cust_ids = [_generate_deterministic_id("cust", "salesforce_acc", sid) for sid in source_ids]

        customer_df = df.with_columns([
            pl.Series("unified_customer_id", cust_ids, dtype=pl.String),
            pl.lit("salesforce", dtype=pl.String).alias("source_system"),
            pl.col("Id").cast(pl.String).alias("source_id"),
            pl.lit(None, dtype=pl.String).alias("stripe_customer_id"),
            pl.col("Id").cast(pl.String).alias("salesforce_account_id"),
            pl.lit(None, dtype=pl.String).alias("salesforce_contact_id"),
            pl.lit(None, dtype=pl.String).alias("email"),
            pl.col("Name").cast(pl.String).alias("full_name"),
            pl.lit(None, dtype=pl.String).alias("first_name"),
            pl.lit(None, dtype=pl.String).alias("last_name"),
            pl.col("Name").cast(pl.String).alias("company_name"),
            pl.col("Phone").cast(pl.String).alias("phone"),
            pl.col("BillingStreet").cast(pl.String).alias("billing_street"),
            pl.col("BillingCity").cast(pl.String).alias("billing_city"),
            pl.col("BillingState").cast(pl.String).alias("billing_state"),
            pl.col("BillingPostalCode").cast(pl.String).alias("billing_postal_code"),
            pl.col("BillingCountry").cast(pl.String).alias("billing_country"),
            pl.lit("USD", dtype=pl.String).alias("preferred_currency"),
            pl.lit(True, dtype=pl.Boolean).alias("is_active"),
            pl.col("CreatedDate").cast(pl.String).alias("source_created_at"),
            pl.col("LastModifiedDate").cast(pl.String).alias("source_updated_at"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        cust_cols = list(cust_schema.keys())
        customer_df = customer_df.select(cust_cols)

        return company_df, customer_df

    @classmethod
    def transform_contacts(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Salesforce Contact records into UnifiedCustomer DataFrame."""
        cust_schema = {
            "unified_customer_id": pl.String,
            "source_system": pl.String,
            "source_id": pl.String,
            "stripe_customer_id": pl.String,
            "salesforce_account_id": pl.String,
            "salesforce_contact_id": pl.String,
            "email": pl.String,
            "full_name": pl.String,
            "first_name": pl.String,
            "last_name": pl.String,
            "company_name": pl.String,
            "phone": pl.String,
            "billing_street": pl.String,
            "billing_city": pl.String,
            "billing_state": pl.String,
            "billing_postal_code": pl.String,
            "billing_country": pl.String,
            "preferred_currency": pl.String,
            "is_active": pl.Boolean,
            "source_created_at": pl.String,
            "source_updated_at": pl.String,
            "ingestion_timestamp": pl.String,
        }

        if not records:
            return pl.DataFrame(schema=cust_schema)

        normalized = []
        for r in records:
            normalized.append({
                "Id": str(r.get("Id", "")),
                "AccountId": r.get("AccountId"),
                "FirstName": r.get("FirstName"),
                "LastName": str(r.get("LastName", "Unknown")),
                "Email": r.get("Email"),
                "Phone": r.get("Phone"),
                "Title": r.get("Title"),
                "Department": r.get("Department"),
                "CreatedDate": r.get("CreatedDate"),
                "LastModifiedDate": r.get("LastModifiedDate"),
            })

        df = pl.DataFrame(normalized)

        # 1. Clean strings & normalize emails
        df = clean_and_normalize_strings(df)
        df = normalize_emails(df, email_col="Email")
        df = standardize_timestamps(df, timestamp_cols=["CreatedDate", "LastModifiedDate"])

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_ids = df["Id"].to_list()

        cust_ids = [_generate_deterministic_id("cust", "salesforce_con", sid) for sid in source_ids]

        df = df.with_columns([
            pl.Series("unified_customer_id", cust_ids, dtype=pl.String),
            pl.lit("salesforce", dtype=pl.String).alias("source_system"),
            pl.col("Id").cast(pl.String).alias("source_id"),
            pl.lit(None, dtype=pl.String).alias("stripe_customer_id"),
            pl.col("AccountId").cast(pl.String).alias("salesforce_account_id"),
            pl.col("Id").cast(pl.String).alias("salesforce_contact_id"),
            pl.col("Email").cast(pl.String).alias("email"),
            (
                pl.when(pl.col("FirstName").is_not_null())
                .then(pl.col("FirstName") + " " + pl.col("LastName"))
                .otherwise(pl.col("LastName"))
            ).cast(pl.String).alias("full_name"),
            pl.col("FirstName").cast(pl.String).alias("first_name"),
            pl.col("LastName").cast(pl.String).alias("last_name"),
            pl.lit(None, dtype=pl.String).alias("company_name"),
            pl.col("Phone").cast(pl.String).alias("phone"),
            pl.lit(None, dtype=pl.String).alias("billing_street"),
            pl.lit(None, dtype=pl.String).alias("billing_city"),
            pl.lit(None, dtype=pl.String).alias("billing_state"),
            pl.lit(None, dtype=pl.String).alias("billing_postal_code"),
            pl.lit(None, dtype=pl.String).alias("billing_country"),
            pl.lit("USD", dtype=pl.String).alias("preferred_currency"),
            pl.lit(True, dtype=pl.Boolean).alias("is_active"),
            pl.col("CreatedDate").cast(pl.String).alias("source_created_at"),
            pl.col("LastModifiedDate").cast(pl.String).alias("source_updated_at"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        cust_cols = list(cust_schema.keys())
        return df.select(cust_cols)

    @classmethod
    def transform_opportunities(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Salesforce Opportunity records into UnifiedTransaction DataFrame."""
        tx_schema = {
            "unified_transaction_id": pl.String,
            "source_system": pl.String,
            "source_transaction_id": pl.String,
            "unified_customer_id": pl.String,
            "source_customer_id": pl.String,
            "customer_email": pl.String,
            "amount": pl.Float64,
            "amount_refunded": pl.Float64,
            "net_amount": pl.Float64,
            "currency": pl.String,
            "status": pl.String,
            "payment_method": pl.String,
            "is_paid": pl.Boolean,
            "is_refunded": pl.Boolean,
            "transaction_date": pl.String,
            "ingestion_timestamp": pl.String,
        }

        if not records:
            return pl.DataFrame(schema=tx_schema)

        normalized = []
        for r in records:
            normalized.append({
                "Id": str(r.get("Id", "")),
                "AccountId": r.get("AccountId"),
                "Amount": r.get("Amount", 0.0) or 0.0,
                "StageName": r.get("StageName", "New"),
                "IsWon": bool(r.get("IsWon", False)),
                "IsClosed": bool(r.get("IsClosed", False)),
                "CreatedDate": r.get("CreatedDate"),
                "LastModifiedDate": r.get("LastModifiedDate"),
            })

        df = pl.DataFrame(normalized)

        df = clean_and_normalize_strings(df)
        df = standardize_timestamps(df, timestamp_cols=["CreatedDate", "LastModifiedDate"])
        df = standardize_currency_amounts(df, amount_col="Amount", is_in_cents=False, target_col="amount_std")

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_ids = df["Id"].to_list()
        acc_ids = df["AccountId"].to_list()

        unified_tx_ids = [_generate_deterministic_id("tx", "salesforce_opp", sid) for sid in source_ids]
        unified_cust_ids = [
            _generate_deterministic_id("cust", "salesforce_acc", aid) if aid else None
            for aid in acc_ids
        ]

        df = df.with_columns([
            pl.Series("unified_transaction_id", unified_tx_ids, dtype=pl.String),
            pl.lit("salesforce", dtype=pl.String).alias("source_system"),
            pl.col("Id").cast(pl.String).alias("source_transaction_id"),
            pl.Series("unified_customer_id", unified_cust_ids, dtype=pl.String),
            pl.col("AccountId").cast(pl.String).alias("source_customer_id"),
            pl.lit(None, dtype=pl.String).alias("customer_email"),
            pl.col("amount_std").alias("amount"),
            pl.lit(0.0, dtype=pl.Float64).alias("amount_refunded"),
            pl.col("amount_std").alias("net_amount"),
            pl.lit("USD", dtype=pl.String).alias("currency"),
            (
                pl.when(pl.col("IsWon") == True)
                .then(pl.lit("succeeded", dtype=pl.String))
                .when(pl.col("IsClosed") == True)
                .then(pl.lit("failed", dtype=pl.String))
                .otherwise(pl.lit("pending", dtype=pl.String))
            ).alias("status"),
            pl.lit("Salesforce Invoice / Deal", dtype=pl.String).alias("payment_method"),
            pl.col("IsWon").cast(pl.Boolean).alias("is_paid"),
            pl.lit(False, dtype=pl.Boolean).alias("is_refunded"),
            pl.col("CreatedDate").cast(pl.String).alias("transaction_date"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        return df.select(list(tx_schema.keys()))

    @classmethod
    def transform_orders(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Salesforce Order records into UnifiedTransaction DataFrame."""
        tx_schema = {
            "unified_transaction_id": pl.String,
            "source_system": pl.String,
            "source_transaction_id": pl.String,
            "unified_customer_id": pl.String,
            "source_customer_id": pl.String,
            "customer_email": pl.String,
            "amount": pl.Float64,
            "amount_refunded": pl.Float64,
            "net_amount": pl.Float64,
            "currency": pl.String,
            "status": pl.String,
            "payment_method": pl.String,
            "is_paid": pl.Boolean,
            "is_refunded": pl.Boolean,
            "transaction_date": pl.String,
            "ingestion_timestamp": pl.String,
        }

        if not records:
            return pl.DataFrame(schema=tx_schema)

        normalized = []
        for r in records:
            normalized.append({
                "Id": str(r.get("Id", "")),
                "AccountId": r.get("AccountId"),
                "OrderNumber": str(r.get("OrderNumber", "")),
                "Status": str(r.get("Status", "Draft")),
                "TotalAmount": r.get("TotalAmount", 0.0) or 0.0,
                "CreatedDate": r.get("CreatedDate"),
                "LastModifiedDate": r.get("LastModifiedDate"),
            })

        df = pl.DataFrame(normalized)

        df = clean_and_normalize_strings(df)
        df = standardize_timestamps(df, timestamp_cols=["CreatedDate", "LastModifiedDate"])
        df = standardize_currency_amounts(
            df, amount_col="TotalAmount", is_in_cents=False, target_col="amount_std"
        )

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_ids = df["Id"].to_list()
        acc_ids = df["AccountId"].to_list()

        unified_tx_ids = [_generate_deterministic_id("tx", "salesforce_ord", sid) for sid in source_ids]
        unified_cust_ids = [
            _generate_deterministic_id("cust", "salesforce_acc", aid) if aid else None
            for aid in acc_ids
        ]

        df = df.with_columns([
            pl.Series("unified_transaction_id", unified_tx_ids, dtype=pl.String),
            pl.lit("salesforce", dtype=pl.String).alias("source_system"),
            pl.col("Id").cast(pl.String).alias("source_transaction_id"),
            pl.Series("unified_customer_id", unified_cust_ids, dtype=pl.String),
            pl.col("AccountId").cast(pl.String).alias("source_customer_id"),
            pl.lit(None, dtype=pl.String).alias("customer_email"),
            pl.col("amount_std").alias("amount"),
            pl.lit(0.0, dtype=pl.Float64).alias("amount_refunded"),
            pl.col("amount_std").alias("net_amount"),
            pl.lit("USD", dtype=pl.String).alias("currency"),
            (
                pl.when(pl.col("Status") == "Activated")
                .then(pl.lit("succeeded", dtype=pl.String))
                .otherwise(pl.lit("pending", dtype=pl.String))
            ).alias("status"),
            pl.lit("Salesforce Direct Order", dtype=pl.String).alias("payment_method"),
            (pl.col("Status") == "Activated").cast(pl.Boolean).alias("is_paid"),
            pl.lit(False, dtype=pl.Boolean).alias("is_refunded"),
            pl.col("CreatedDate").cast(pl.String).alias("transaction_date"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        return df.select(list(tx_schema.keys()))
