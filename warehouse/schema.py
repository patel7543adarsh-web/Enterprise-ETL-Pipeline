"""
SQLAlchemy ORM Schemas for the Unified Canonical Data Warehouse.
Defines dimension tables, fact tables, and pipeline audit logs.
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimCustomer(Base):
    """Unified Customer Dimension Table."""
    __tablename__ = "dim_customers"

    unified_customer_id = Column(String(64), primary_key=True, doc="Deterministic customer hash UUID")
    source_system = Column(String(32), nullable=False, doc="Originating source system")
    source_id = Column(String(128), nullable=False, doc="Primary ID in source system")
    salesforce_account_id = Column(String(64), nullable=True)
    salesforce_contact_id = Column(String(64), nullable=True)
    stripe_customer_id = Column(String(64), nullable=True)
    email = Column(String(255), nullable=True, index=True)
    full_name = Column(String(255), nullable=True)
    first_name = Column(String(128), nullable=True)
    last_name = Column(String(128), nullable=True)
    company_name = Column(String(255), nullable=True)
    phone = Column(String(64), nullable=True)
    billing_street = Column(String(255), nullable=True)
    billing_city = Column(String(128), nullable=True)
    billing_state = Column(String(128), nullable=True)
    billing_postal_code = Column(String(32), nullable=True)
    billing_country = Column(String(32), nullable=True)
    preferred_currency = Column(String(8), default="USD", nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    source_created_at = Column(DateTime(timezone=True), nullable=True)
    source_updated_at = Column(DateTime(timezone=True), nullable=True)
    ingestion_timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_dim_customers_source_id", "source_system", "source_id"),
    )


class FactTransaction(Base):
    """Unified Financial Transaction Fact Table."""
    __tablename__ = "fact_transactions"

    unified_transaction_id = Column(String(64), primary_key=True, doc="Deterministic transaction hash UUID")
    source_system = Column(String(32), nullable=False)
    source_transaction_id = Column(String(128), nullable=False)
    unified_customer_id = Column(String(64), nullable=True, index=True)
    source_customer_id = Column(String(128), nullable=True)
    customer_email = Column(String(255), nullable=True, index=True)
    amount = Column(Float, nullable=False)
    amount_refunded = Column(Float, default=0.0, nullable=False)
    net_amount = Column(Float, default=0.0, nullable=False)
    currency = Column(String(8), default="USD", nullable=False)
    status = Column(String(32), nullable=False)
    payment_method = Column(String(64), nullable=True)
    is_paid = Column(Boolean, default=True, nullable=False)
    is_refunded = Column(Boolean, default=False, nullable=False)
    transaction_date = Column(DateTime(timezone=True), nullable=False)
    ingestion_timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_fact_transactions_source", "source_system", "source_transaction_id"),
    )


class FactSubscription(Base):
    """Unified Subscription Fact Table."""
    __tablename__ = "fact_subscriptions"

    unified_subscription_id = Column(String(64), primary_key=True, doc="Deterministic subscription hash UUID")
    source_system = Column(String(32), nullable=False)
    source_subscription_id = Column(String(128), nullable=False)
    unified_customer_id = Column(String(64), nullable=True, index=True)
    source_customer_id = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False)
    mrr_amount = Column(Float, default=0.0, nullable=False)
    currency = Column(String(8), default="USD", nullable=False)
    current_period_start = Column(DateTime(timezone=True), nullable=True)
    current_period_end = Column(DateTime(timezone=True), nullable=True)
    cancel_at_period_end = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    ingestion_timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_fact_subscriptions_source", "source_system", "source_subscription_id"),
    )


class DimCompany(Base):
    """Unified Company Dimension Table."""
    __tablename__ = "dim_companies"

    unified_account_id = Column(String(64), primary_key=True, doc="Deterministic account hash UUID")
    source_system = Column(String(32), nullable=False)
    source_account_id = Column(String(128), nullable=False)
    company_name = Column(String(255), nullable=False)
    industry = Column(String(128), nullable=True)
    annual_revenue = Column(Float, nullable=True)
    employee_count = Column(Integer, nullable=True)
    website = Column(String(255), nullable=True)
    phone = Column(String(64), nullable=True)
    billing_country = Column(String(32), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)
    ingestion_timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_dim_companies_source", "source_system", "source_account_id"),
    )


class ETLLoadAudit(Base):
    """ETL Warehouse Sync Run Audit & Watermark Log."""
    __tablename__ = "etl_load_audit"

    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), nullable=False, index=True)
    target_table = Column(String(64), nullable=False)
    rows_processed = Column(Integer, default=0, nullable=False)
    rows_inserted = Column(Integer, default=0, nullable=False)
    rows_updated = Column(Integer, default=0, nullable=False)
    status = Column(String(32), default="SUCCESS", nullable=False)
    duration_seconds = Column(Float, default=0.0, nullable=False)
    error_message = Column(Text, nullable=True)
    synced_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


def init_warehouse_schema(engine) -> None:
    """Creates all data warehouse tables and indexes if they do not exist."""
    Base.metadata.create_all(bind=engine)


def drop_warehouse_schema(engine) -> None:
    """Drops all data warehouse tables (useful for test tear-downs)."""
    Base.metadata.drop_all(bind=engine)
