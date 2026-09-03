"""
Tests for Week 1 Day 1-2: Pydantic Data Models and Settings Validation.
"""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from config.settings import AppSettings, StripeSettings, SalesforceSettings, StorageSettings
from models.raw.stripe_models import (
    StripeAddress,
    StripeCharge,
    StripeCustomer,
    StripeInvoice,
    StripeSubscription,
    StripeListResponse,
)
from models.raw.salesforce_models import (
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    SalesforceOrder,
    SalesforceQueryResult,
)
from models.unified.canonical_models import (
    UnifiedCustomer,
    UnifiedTransaction,
    UnifiedSubscription,
    UnifiedCompanyAccount,
)


class TestStripeModels:
    """Validates Stripe raw Pydantic schemas and type coercion."""

    def test_stripe_customer_valid(self):
        raw = {
            "id": "cus_123",
            "object": "customer",
            "email": "USER@Example.COM",
            "name": "Jane Doe",
            "created": 1704067200,  # 2024-01-01 00:00:00 UTC
            "balance": 5000,
            "address": {
                "city": "San Francisco",
                "state": "CA",
                "country": "US",
            },
        }
        customer = StripeCustomer.model_validate(raw)
        assert customer.id == "cus_123"
        assert customer.email == "USER@Example.COM"
        assert customer.created == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert customer.balance == 5000
        assert customer.address.city == "San Francisco"

    def test_stripe_customer_invalid_missing_id(self):
        with pytest.raises(ValidationError):
            StripeCustomer.model_validate({"name": "No ID"})

    def test_stripe_charge_valid(self):
        raw = {
            "id": "ch_999",
            "amount": 2500,
            "amount_refunded": 0,
            "currency": "usd",
            "customer": "cus_123",
            "paid": True,
            "status": "succeeded",
            "created": 1704153600,
        }
        charge = StripeCharge.model_validate(raw)
        assert charge.id == "ch_999"
        assert charge.amount == 2500
        assert charge.currency == "usd"
        assert charge.status == "succeeded"

    def test_stripe_list_response(self):
        raw = {
            "object": "list",
            "data": [
                {"id": "cus_1", "created": 1704067200},
                {"id": "cus_2", "created": 1704067200},
            ],
            "has_more": True,
        }
        resp = StripeListResponse[StripeCustomer].model_validate(raw)
        assert len(resp.data) == 2
        assert resp.has_more is True
        assert resp.data[0].id == "cus_1"


class TestSalesforceModels:
    """Validates Salesforce raw Pydantic schemas and SOQL response parsing."""

    def test_salesforce_account_valid(self):
        raw = {
            "Id": "001000000000001AAA",
            "Name": "Acme Global",
            "Type": "Customer - Direct",
            "AnnualRevenue": 1500000.0,
            "BillingCity": "New York",
            "BillingCountry": "USA",
            "CreatedDate": "2024-01-15T10:30:00.000+0000",
            "LastModifiedDate": "2024-02-01T15:45:00.000+0000",
        }
        account = SalesforceAccount.model_validate(raw)
        assert account.id == "001000000000001AAA"
        assert account.name == "Acme Global"
        assert account.annual_revenue == 1500000.0
        assert account.billing_city == "New York"
        assert account.created_date.year == 2024

    def test_salesforce_contact_valid(self):
        raw = {
            "Id": "003000000000001AAA",
            "AccountId": "001000000000001AAA",
            "FirstName": "Alice",
            "LastName": "Smith",
            "Email": "alice@acme.com",
            "CreatedDate": "2024-01-15T10:30:00.000Z",
            "LastModifiedDate": "2024-02-01T15:45:00.000Z",
        }
        contact = SalesforceContact.model_validate(raw)
        assert contact.id == "003000000000001AAA"
        assert contact.first_name == "Alice"
        assert contact.last_name == "Smith"

    def test_salesforce_opportunity_valid(self):
        raw = {
            "Id": "006000000000001AAA",
            "AccountId": "001000000000001AAA",
            "Name": "Q1 Expansion",
            "Amount": 75000.50,
            "StageName": "Closed Won",
            "IsWon": True,
            "CreatedDate": "2024-01-15T10:30:00.000Z",
            "LastModifiedDate": "2024-02-01T15:45:00.000Z",
        }
        opp = SalesforceOpportunity.model_validate(raw)
        assert opp.amount == 75000.50
        assert opp.is_won is True


class TestUnifiedModels:
    """Validates Unified Canonical data models."""

    def test_unified_customer_valid(self):
        cust = UnifiedCustomer(
            unified_customer_id="cust_12345",
            source_system="stripe",
            source_id="cus_999",
            email="user@example.com",
            full_name="John Doe",
            preferred_currency="USD",
        )
        assert cust.unified_customer_id == "cust_12345"
        assert cust.source_system == "stripe"
        assert cust.preferred_currency == "USD"
        assert cust.is_active is True

    def test_unified_transaction_valid(self):
        tx = UnifiedTransaction(
            unified_transaction_id="tx_12345",
            source_system="salesforce",
            source_transaction_id="0060001",
            amount=5000.00,
            amount_refunded=0.0,
            net_amount=5000.00,
            currency="USD",
            status="succeeded",
            transaction_date=datetime.now(timezone.utc),
        )
        assert tx.amount == 5000.00
        assert tx.currency == "USD"
        assert tx.status == "succeeded"
