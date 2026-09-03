"""
Tests for Week 2 Day 4-6: Schema Mapping & Unified Canonical Representation.
"""

from datetime import datetime, timezone
import polars as pl
import pytest

from models.unified.canonical_models import (
    UnifiedCompanyAccount,
    UnifiedCustomer,
    UnifiedSubscription,
    UnifiedTransaction,
)
from transformers.salesforce_transformer import SalesforceDataTransformer
from transformers.stripe_transformer import StripeDataTransformer
from transformers.unified_mapper import UnifiedSchemaMapper


class TestStripeTransformer:
    """Tests Stripe raw to unified canonical transformations."""

    def test_transform_customers(self):
        raw = [
            {
                "id": "cus_001",
                "email": "JANE@EXAMPLE.COM",
                "name": "Jane Doe",
                "created": 1704067200,
                "currency": "usd",
                "balance": 2500,
                "address": {"city": "New York", "country": "US"},
            }
        ]
        df = StripeDataTransformer.transform_customers(raw)
        assert len(df) == 1
        assert df["source_system"][0] == "stripe"
        assert df["email"][0] == "jane@example.com"
        assert df["preferred_currency"][0] == "USD"
        assert df["billing_city"][0] == "New York"
        assert df["unified_customer_id"][0].startswith("cust_")

    def test_transform_charges(self):
        raw = [
            {
                "id": "ch_001",
                "customer": "cus_001",
                "amount": 10000,
                "amount_refunded": 2000,
                "currency": "usd",
                "status": "succeeded",
                "paid": True,
                "refunded": False,
                "created": 1704067200,
            }
        ]
        df = StripeDataTransformer.transform_charges(raw)
        assert len(df) == 1
        assert df["amount"][0] == 100.00
        assert df["amount_refunded"][0] == 20.00
        assert df["net_amount"][0] == 80.00
        assert df["status"][0] == "succeeded"


class TestSalesforceTransformer:
    """Tests Salesforce raw to unified canonical transformations."""

    def test_transform_accounts(self):
        raw = [
            {
                "Id": "001000000000001AAA",
                "Name": "Big Enterprise Corp",
                "Industry": "Technology",
                "AnnualRevenue": 5000000.0,
                "NumberOfEmployees": 250,
                "CreatedDate": "2024-01-01T00:00:00.000Z",
                "LastModifiedDate": "2024-01-02T00:00:00.000Z",
            }
        ]
        company_df, customer_df = SalesforceDataTransformer.transform_accounts(raw)
        assert len(company_df) == 1
        assert company_df["company_name"][0] == "Big Enterprise Corp"
        assert company_df["annual_revenue"][0] == 5000000.0
        assert company_df["unified_account_id"][0].startswith("org_")

        assert len(customer_df) == 1
        assert customer_df["salesforce_account_id"][0] == "001000000000001AAA"

    def test_transform_opportunities(self):
        raw = [
            {
                "Id": "006000000000001AAA",
                "AccountId": "001000000000001AAA",
                "Amount": 45000.0,
                "IsWon": True,
                "IsClosed": True,
                "CreatedDate": "2024-01-10T00:00:00.000Z",
                "LastModifiedDate": "2024-01-10T00:00:00.000Z",
            }
        ]
        df = SalesforceDataTransformer.transform_opportunities(raw)
        assert len(df) == 1
        assert df["amount"][0] == 45000.0
        assert df["status"][0] == "succeeded"
        assert df["is_paid"][0] is True


class TestUnifiedMapper:
    """Tests cross-source entity deduplication and data quality validation."""

    def test_merge_customers_deduplication(self):
        # 1. Stripe customer with email 'test@company.com'
        stripe_raw = [{
            "id": "cus_100",
            "email": "test@company.com",
            "name": "Stripe Name",
            "created": 1704067200,
            "currency": "usd",
        }]
        stripe_df = StripeDataTransformer.transform_customers(stripe_raw)

        # 2. Salesforce contact with matching email 'test@company.com'
        sf_raw = [{
            "Id": "003100",
            "AccountId": "001100",
            "FirstName": "Salesforce",
            "LastName": "User",
            "Email": "test@company.com",
            "Phone": "+1-555-1234",
            "CreatedDate": "2024-01-01T00:00:00Z",
            "LastModifiedDate": "2024-01-01T00:00:00Z",
        }]
        sf_df = SalesforceDataTransformer.transform_contacts(sf_raw)

        # Merge them
        merged_df = UnifiedSchemaMapper.merge_customers(stripe_df=stripe_df, salesforce_df=sf_df)

        # Should deduplicate 2 records into 1 merged canonical customer profile
        assert len(merged_df) == 1
        merged_row = merged_df.to_dicts()[0]
        assert merged_row["email"] == "test@company.com"
        assert merged_row["source_system"] == "merged"
        assert merged_row["stripe_customer_id"] == "cus_100"
        assert merged_row["salesforce_contact_id"] == "003100"
        assert merged_row["phone"] == "+1-555-1234"

    def test_validate_dataset_quality(self):
        stripe_raw = [
            {"id": "cus_1", "email": "a@example.com", "created": 1704067200},
            {"id": "cus_2", "email": "b@example.com", "created": 1704067200},
        ]
        df = StripeDataTransformer.transform_customers(stripe_raw)

        report = UnifiedSchemaMapper.validate_dataset(
            df=df,
            model_cls=UnifiedCustomer,
            dataset_name="Test Customers",
        )

        assert report.total_records == 2
        assert report.valid_records == 2
        assert report.invalid_records == 0
        assert report.quality_score_percent == 100.0
        assert report.passed_threshold is True
