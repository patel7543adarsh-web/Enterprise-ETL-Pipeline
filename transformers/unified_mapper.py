"""
Unified Schema Mapper and Data Quality Validator.
Reconciles disparate customer and transaction records across Stripe and Salesforce.
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional, Type
import polars as pl
from pydantic import BaseModel

from models.unified.canonical_models import (
    DataQualityReport,
    UnifiedCustomer,
    UnifiedSubscription,
    UnifiedTransaction,
)

logger = logging.getLogger(__name__)

CUSTOMER_SCHEMA = {
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

TRANSACTION_SCHEMA = {
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


def _align_dataframe_schema(df: pl.DataFrame, target_schema: Dict[str, Any]) -> pl.DataFrame:
    """Ensures DataFrame matches exact target schema and casts all columns to proper types."""
    if len(df) == 0:
        return pl.DataFrame(schema=target_schema)

    exprs = []
    for col_name, dtype in target_schema.items():
        if col_name in df.columns:
            exprs.append(pl.col(col_name).cast(dtype, strict=False).alias(col_name))
        else:
            exprs.append(pl.lit(None, dtype=dtype).alias(col_name))

    return df.select(exprs)


class UnifiedSchemaMapper:
    """
    Performs cross-source entity resolution, schema merging, and data quality validation.
    """

    @classmethod
    def merge_customers(
        cls,
        stripe_df: Optional[pl.DataFrame] = None,
        salesforce_df: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Merges and deduplicates customer entities from Stripe and Salesforce.
        Performs entity resolution by matching on normalized email address.
        """
        dfs_to_concat = []
        if stripe_df is not None and len(stripe_df) > 0:
            dfs_to_concat.append(_align_dataframe_schema(stripe_df, CUSTOMER_SCHEMA))
        if salesforce_df is not None and len(salesforce_df) > 0:
            dfs_to_concat.append(_align_dataframe_schema(salesforce_df, CUSTOMER_SCHEMA))

        if not dfs_to_concat:
            return pl.DataFrame(schema=CUSTOMER_SCHEMA)

        combined = pl.concat(dfs_to_concat, how="vertical_relaxed")

        # Split into records with valid email (can be deduplicated) vs no email
        with_email = combined.filter(pl.col("email").is_not_null())
        without_email = combined.filter(pl.col("email").is_null())

        if len(with_email) > 0:
            deduped_records = []
            for email, group in with_email.group_by("email"):
                rows = group.to_dicts()
                merged_row = cls._reconcile_customer_rows(email[0], rows)
                deduped_records.append(merged_row)

            deduped_df = _align_dataframe_schema(pl.DataFrame(deduped_records), CUSTOMER_SCHEMA)
            result = pl.concat([deduped_df, without_email], how="vertical_relaxed")
        else:
            result = combined

        logger.info(f"Customer reconciliation complete: {len(combined)} raw records merged into {len(result)} canonical profiles.")
        return result

    @classmethod
    def _reconcile_customer_rows(cls, email: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merges multiple customer records matching the same email into a single profile."""
        stripe_row = next((r for r in rows if r.get("source_system") == "stripe"), {})
        sf_row = next((r for r in rows if r.get("source_system") == "salesforce"), {})

        source_system = "merged" if (stripe_row and sf_row) else (stripe_row.get("source_system") or sf_row.get("source_system") or "unknown")

        hash_seed = f"cust_email_{email}"
        unified_id = f"cust_{hashlib.sha256(hash_seed.encode('utf-8')).hexdigest()[:16]}"

        merged = {
            "unified_customer_id": unified_id,
            "source_system": source_system,
            "source_id": stripe_row.get("source_id") or sf_row.get("source_id") or unified_id,
            "stripe_customer_id": stripe_row.get("stripe_customer_id"),
            "salesforce_account_id": sf_row.get("salesforce_account_id"),
            "salesforce_contact_id": sf_row.get("salesforce_contact_id"),
            "email": email,
            "full_name": sf_row.get("full_name") or stripe_row.get("full_name"),
            "first_name": sf_row.get("first_name"),
            "last_name": sf_row.get("last_name"),
            "company_name": sf_row.get("company_name") or stripe_row.get("company_name"),
            "phone": sf_row.get("phone") or stripe_row.get("phone"),
            "billing_street": sf_row.get("billing_street") or stripe_row.get("billing_street"),
            "billing_city": sf_row.get("billing_city") or stripe_row.get("billing_city"),
            "billing_state": sf_row.get("billing_state") or stripe_row.get("billing_state"),
            "billing_postal_code": sf_row.get("billing_postal_code") or stripe_row.get("billing_postal_code"),
            "billing_country": sf_row.get("billing_country") or stripe_row.get("billing_country"),
            "preferred_currency": stripe_row.get("preferred_currency") or "USD",
            "is_active": True,
            "source_created_at": stripe_row.get("source_created_at") or sf_row.get("source_created_at"),
            "source_updated_at": sf_row.get("source_updated_at") or stripe_row.get("source_updated_at"),
            "ingestion_timestamp": stripe_row.get("ingestion_timestamp") or sf_row.get("ingestion_timestamp"),
        }
        return merged

    @classmethod
    def merge_transactions(
        cls,
        stripe_tx_df: Optional[pl.DataFrame] = None,
        salesforce_tx_df: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """Merges transaction DataFrames from Stripe and Salesforce."""
        dfs = []
        if stripe_tx_df is not None and len(stripe_tx_df) > 0:
            dfs.append(_align_dataframe_schema(stripe_tx_df, TRANSACTION_SCHEMA))
        if salesforce_tx_df is not None and len(salesforce_tx_df) > 0:
            dfs.append(_align_dataframe_schema(salesforce_tx_df, TRANSACTION_SCHEMA))

        if not dfs:
            return pl.DataFrame(schema=TRANSACTION_SCHEMA)

        return pl.concat(dfs, how="vertical_relaxed")

    @classmethod
    def validate_dataset(
        cls,
        df: pl.DataFrame,
        model_cls: Type[BaseModel],
        dataset_name: str,
        threshold_percent: float = 95.0,
    ) -> DataQualityReport:
        """
        Validates an entire Polars DataFrame against a canonical Pydantic model.
        Returns a DataQualityReport with metrics and error diagnostics.
        """
        total_records = len(df)
        if total_records == 0:
            return DataQualityReport(
                dataset_name=dataset_name,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                quality_score_percent=100.0,
                passed_threshold=True,
            )

        valid_count = 0
        invalid_count = 0
        validation_errors = []

        # Check null counts by column
        null_counts = {}
        for col in df.columns:
            null_counts[col] = df[col].null_count()

        # Validate rows through Pydantic
        records = df.to_dicts()
        for idx, row in enumerate(records):
            try:
                model_cls.model_validate(row)
                valid_count += 1
            except Exception as e:
                invalid_count += 1
                if len(validation_errors) < 10:
                    validation_errors.append(f"Row {idx} failed: {str(e)[:150]}")

        quality_score = (valid_count / total_records) * 100.0 if total_records > 0 else 100.0
        passed = quality_score >= threshold_percent

        logger.info(
            f"Data Quality Report for '{dataset_name}': {valid_count}/{total_records} valid ({quality_score:.1f}%). Passed: {passed}"
        )

        return DataQualityReport(
            dataset_name=dataset_name,
            total_records=total_records,
            valid_records=valid_count,
            invalid_records=invalid_count,
            null_counts_by_field=null_counts,
            validation_errors=validation_errors,
            quality_score_percent=round(quality_score, 2),
            passed_threshold=passed,
        )
