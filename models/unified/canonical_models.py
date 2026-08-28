"""
Unified Canonical Data Models for the Data Warehouse Layer.
Standardized across disparate sources (Stripe, Salesforce, etc.).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


class UnifiedCustomer(BaseModel):
    """
    Unified Customer Entity.
    Standardized schema for customers across Stripe and Salesforce.
    """
    model_config = ConfigDict(extra="ignore")

    unified_customer_id: str = Field(description="Deterministic hash or UUID for customer entity")
    source_system: Literal["stripe", "salesforce", "merged"] = Field(description="Originating system")
    source_id: str = Field(description="Primary ID in source system")
    salesforce_account_id: Optional[str] = None
    salesforce_contact_id: Optional[str] = None
    stripe_customer_id: Optional[str] = None
    email: Optional[str] = Field(default=None, description="Normalized lowercase email")
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    phone: Optional[str] = None
    billing_street: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_postal_code: Optional[str] = None
    billing_country: Optional[str] = Field(default=None, description="Standardized ISO country code")
    preferred_currency: str = Field(default="USD", description="Standardized 3-letter currency code")
    is_active: bool = Field(default=True)
    source_created_at: Optional[datetime] = None
    source_updated_at: Optional[datetime] = None
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Warehouse ingestion timestamp (UTC)"
    )


class UnifiedTransaction(BaseModel):
    """
    Unified Financial Transaction Entity.
    Standardized schema for payment transactions, charges, and orders.
    """
    model_config = ConfigDict(extra="ignore")

    unified_transaction_id: str = Field(description="Deterministic hash or UUID for transaction")
    source_system: Literal["stripe", "salesforce"] = Field(description="Originating system")
    source_transaction_id: str = Field(description="Source transaction/order ID")
    unified_customer_id: Optional[str] = None
    source_customer_id: Optional[str] = None
    customer_email: Optional[str] = None
    amount: float = Field(description="Standardized decimal transaction amount (in main currency unit, e.g. Dollars)")
    amount_refunded: float = Field(default=0.0, description="Standardized decimal refunded amount")
    net_amount: float = Field(default=0.0, description="amount minus amount_refunded")
    currency: str = Field(default="USD", description="Upper-case 3-letter ISO code")
    status: str = Field(description="Standardized status: succeeded, failed, pending, refunded")
    payment_method: Optional[str] = None
    is_paid: bool = Field(default=True)
    is_refunded: bool = Field(default=False)
    transaction_date: datetime = Field(description="UTC timestamp of transaction")
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UnifiedSubscription(BaseModel):
    """
    Unified Subscription Entity.
    Standardized recurring billing contract representation.
    """
    model_config = ConfigDict(extra="ignore")

    unified_subscription_id: str = Field(description="Deterministic hash or UUID")
    source_system: Literal["stripe", "salesforce"] = Field(description="Originating system")
    source_subscription_id: str = Field(description="ID in source system")
    unified_customer_id: Optional[str] = None
    source_customer_id: str = Field(description="Customer ID in source system")
    status: str = Field(description="Standardized status: active, trialing, past_due, canceled")
    mrr_amount: float = Field(default=0.0, description="Monthly Recurring Revenue in standard currency")
    currency: str = Field(default="USD")
    current_period_start: Optional[datetime] = None
    current_period_end: Optional[datetime] = None
    cancel_at_period_end: bool = Field(default=False)
    created_at: datetime = Field(description="Subscription created timestamp (UTC)")
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class UnifiedCompanyAccount(BaseModel):
    """
    Unified B2B Account / Organization Entity.
    """
    model_config = ConfigDict(extra="ignore")

    unified_account_id: str = Field(description="Deterministic hash or UUID")
    source_system: Literal["stripe", "salesforce"] = Field(description="Originating system")
    source_account_id: str = Field(description="ID in source system")
    company_name: str
    industry: Optional[str] = None
    annual_revenue: Optional[float] = None
    employee_count: Optional[int] = None
    website: Optional[str] = None
    phone: Optional[str] = None
    billing_country: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IngestionMetadata(BaseModel):
    """Metadata tracking each batch run and S3 partition path."""
    batch_id: str
    source: str
    entity: str
    records_extracted: int
    records_transformed: int
    s3_raw_path: str
    s3_transformed_path: Optional[str] = None
    start_time: datetime
    end_time: datetime
    status: Literal["SUCCESS", "FAILED", "PARTIAL"]
    error_message: Optional[str] = None


class DataQualityReport(BaseModel):
    """Data quality validation summary report."""
    dataset_name: str
    total_records: int
    valid_records: int
    invalid_records: int
    null_counts_by_field: Dict[str, int] = Field(default_factory=dict)
    validation_errors: List[str] = Field(default_factory=list)
    quality_score_percent: float = Field(default=100.0)
    passed_threshold: bool = Field(default=True)
