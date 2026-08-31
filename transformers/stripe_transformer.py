"""
Stripe Raw-to-Unified Transformer using Polars.
Transforms staged raw Stripe JSON payloads into Unified Canonical Entities.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import polars as pl

from models.unified.canonical_models import (
    UnifiedCustomer,
    UnifiedSubscription,
    UnifiedTransaction,
)
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


class StripeDataTransformer:
    """Transforms raw Stripe data into canonical warehouse schemas using Polars."""

    @classmethod
    def transform_customers(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Stripe customer records into UnifiedCustomer DataFrame."""
        if not records:
            return pl.DataFrame(schema={
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
            })

        flattened = []
        for r in records:
            address = r.get("address") or {}
            flattened.append({
                "source_id": str(r.get("id", "")),
                "email": r.get("email"),
                "full_name": r.get("name"),
                "phone": r.get("phone"),
                "company_name": r.get("description"),
                "currency": r.get("currency") or "USD",
                "balance": r.get("balance", 0) or 0,
                "delinquent": bool(r.get("delinquent", False)),
                "created_at": r.get("created"),
                "billing_street": address.get("line1"),
                "billing_city": address.get("city"),
                "billing_state": address.get("state"),
                "billing_postal_code": address.get("postal_code"),
                "billing_country": address.get("country"),
            })

        df = pl.DataFrame(flattened)

        # 1. Clean strings & normalize emails
        df = clean_and_normalize_strings(df)
        df = normalize_emails(df, email_col="email")

        # 2. Standardize timestamps
        df = standardize_timestamps(df, timestamp_cols=["created_at"])

        # 3. Standardize currency & amount
        df = standardize_currency_codes(df, currency_col="currency")
        df = standardize_currency_amounts(df, amount_col="balance", is_in_cents=True, target_col="balance_amount")

        # 4. Compute canonical IDs and metadata
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        unified_ids = [
            _generate_deterministic_id("cust", "stripe", sid)
            for sid in df["source_id"].to_list()
        ]

        df = df.with_columns([
            pl.Series("unified_customer_id", unified_ids, dtype=pl.String),
            pl.lit("stripe", dtype=pl.String).alias("source_system"),
            pl.col("source_id").cast(pl.String).alias("stripe_customer_id"),
            pl.lit(None, dtype=pl.String).alias("salesforce_account_id"),
            pl.lit(None, dtype=pl.String).alias("salesforce_contact_id"),
            pl.lit(None, dtype=pl.String).alias("first_name"),
            pl.lit(None, dtype=pl.String).alias("last_name"),
            pl.col("company_name").cast(pl.String),
            pl.col("currency").cast(pl.String).alias("preferred_currency"),
            pl.lit(True, dtype=pl.Boolean).alias("is_active"),
            pl.col("created_at").cast(pl.String).alias("source_created_at"),
            pl.col("created_at").cast(pl.String).alias("source_updated_at"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        target_columns = [
            "unified_customer_id", "source_system", "source_id", "stripe_customer_id",
            "salesforce_account_id", "salesforce_contact_id", "email", "full_name",
            "first_name", "last_name", "company_name", "phone", "billing_street",
            "billing_city", "billing_state", "billing_postal_code", "billing_country",
            "preferred_currency", "is_active", "source_created_at", "source_updated_at",
            "ingestion_timestamp",
        ]

        for col in target_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=pl.String).alias(col))

        return df.select(target_columns)

    @classmethod
    def transform_charges(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Stripe charge records into UnifiedTransaction DataFrame."""
        if not records:
            return pl.DataFrame(schema={
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
            })

        normalized_records = []
        for r in records:
            normalized_records.append({
                "id": str(r.get("id", "")),
                "customer": r.get("customer"),
                "amount": r.get("amount", 0) or 0,
                "amount_refunded": r.get("amount_refunded", 0) or 0,
                "currency": r.get("currency") or "usd",
                "status": r.get("status") or "pending",
                "paid": bool(r.get("paid", False)),
                "refunded": bool(r.get("refunded", False)),
                "payment_method": r.get("payment_method"),
                "created": r.get("created"),
            })

        df = pl.DataFrame(normalized_records)

        # 1. Clean strings & standardize timestamps
        df = clean_and_normalize_strings(df)
        df = standardize_timestamps(df, timestamp_cols=["created"])

        # 2. Convert amounts from cents to standard float
        df = standardize_currency_amounts(df, amount_col="amount", is_in_cents=True, target_col="amount_std")
        df = standardize_currency_amounts(
            df, amount_col="amount_refunded", is_in_cents=True, target_col="amount_refunded_std"
        )
        df = standardize_currency_codes(df, currency_col="currency")

        # 3. Handle null imputations
        df = handle_null_imputations(df, {"paid": False, "refunded": False, "status": "pending"})

        # 4. Map unified fields
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        source_ids = df["id"].to_list()
        customer_ids = df["customer"].to_list()

        unified_tx_ids = [_generate_deterministic_id("tx", "stripe", sid) for sid in source_ids]
        unified_cust_ids = [
            _generate_deterministic_id("cust", "stripe", cid) if cid else None
            for cid in customer_ids
        ]

        df = df.with_columns([
            pl.Series("unified_transaction_id", unified_tx_ids, dtype=pl.String),
            pl.lit("stripe", dtype=pl.String).alias("source_system"),
            pl.col("id").cast(pl.String).alias("source_transaction_id"),
            pl.Series("unified_customer_id", unified_cust_ids, dtype=pl.String),
            pl.col("customer").cast(pl.String).alias("source_customer_id"),
            pl.lit(None, dtype=pl.String).alias("customer_email"),
            pl.col("amount_std").alias("amount"),
            pl.col("amount_refunded_std").alias("amount_refunded"),
            (pl.col("amount_std") - pl.col("amount_refunded_std")).round(2).alias("net_amount"),
            pl.col("currency").cast(pl.String),
            pl.col("status").str.to_lowercase().alias("status"),
            pl.col("payment_method").cast(pl.String),
            pl.col("paid").cast(pl.Boolean).alias("is_paid"),
            pl.col("refunded").cast(pl.Boolean).alias("is_refunded"),
            pl.col("created").cast(pl.String).alias("transaction_date"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        target_cols = [
            "unified_transaction_id", "source_system", "source_transaction_id",
            "unified_customer_id", "source_customer_id", "customer_email",
            "amount", "amount_refunded", "net_amount", "currency", "status",
            "payment_method", "is_paid", "is_refunded", "transaction_date",
            "ingestion_timestamp",
        ]
        return df.select(target_cols)

    @classmethod
    def transform_subscriptions(cls, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """Transforms raw Stripe subscription records into UnifiedSubscription DataFrame."""
        if not records:
            return pl.DataFrame(schema={
                "unified_subscription_id": pl.String,
                "source_system": pl.String,
                "source_subscription_id": pl.String,
                "unified_customer_id": pl.String,
                "source_customer_id": pl.String,
                "status": pl.String,
                "mrr_amount": pl.Float64,
                "currency": pl.String,
                "current_period_start": pl.String,
                "current_period_end": pl.String,
                "cancel_at_period_end": pl.Boolean,
                "created_at": pl.String,
                "ingestion_timestamp": pl.String,
            })

        normalized = []
        for r in records:
            normalized.append({
                "id": str(r.get("id", "")),
                "customer": r.get("customer"),
                "status": r.get("status") or "active",
                "current_period_start": r.get("current_period_start"),
                "current_period_end": r.get("current_period_end"),
                "cancel_at_period_end": bool(r.get("cancel_at_period_end", False)),
                "created": r.get("created"),
            })

        df = pl.DataFrame(normalized)

        # 1. Clean strings & standardize timestamps
        df = clean_and_normalize_strings(df)
        df = standardize_timestamps(
            df,
            timestamp_cols=["created", "current_period_start", "current_period_end"],
        )

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_ids = df["id"].to_list()
        customer_ids = df["customer"].to_list()

        unified_sub_ids = [_generate_deterministic_id("sub", "stripe", sid) for sid in source_ids]
        unified_cust_ids = [
            _generate_deterministic_id("cust", "stripe", cid) if cid else None
            for cid in customer_ids
        ]

        df = df.with_columns([
            pl.Series("unified_subscription_id", unified_sub_ids, dtype=pl.String),
            pl.lit("stripe", dtype=pl.String).alias("source_system"),
            pl.col("id").cast(pl.String).alias("source_subscription_id"),
            pl.Series("unified_customer_id", unified_cust_ids, dtype=pl.String),
            pl.col("customer").cast(pl.String).alias("source_customer_id"),
            pl.col("status").str.to_lowercase().alias("status"),
            pl.lit(0.0, dtype=pl.Float64).alias("mrr_amount"),
            pl.lit("USD", dtype=pl.String).alias("currency"),
            pl.col("current_period_start").cast(pl.String),
            pl.col("current_period_end").cast(pl.String),
            pl.col("cancel_at_period_end").cast(pl.Boolean).fill_null(False),
            pl.col("created").cast(pl.String).alias("created_at"),
            pl.lit(now_iso, dtype=pl.String).alias("ingestion_timestamp"),
        ])

        target_cols = [
            "unified_subscription_id", "source_system", "source_subscription_id",
            "unified_customer_id", "source_customer_id", "status", "mrr_amount",
            "currency", "current_period_start", "current_period_end",
            "cancel_at_period_end", "created_at", "ingestion_timestamp",
        ]
        return df.select(target_cols)
