"""
Unit & Integration Tests for Warehouse High-Performance Upsert Loader.
Tests incremental updates, deduplication, chunking, and idempotency.
"""

from datetime import datetime, timezone
import polars as pl
import pytest
from sqlalchemy import select
from models.unified.canonical_models import UnifiedCustomer, UnifiedTransaction
from warehouse.connection import WarehouseConnectionManager
from warehouse.loader import WarehouseUpsertLoader
from warehouse.schema import DimCompany, DimCustomer, ETLLoadAudit, FactSubscription, FactTransaction, init_warehouse_schema


@pytest.fixture
def upsert_loader():
    """Provides a WarehouseUpsertLoader instance connected to an in-memory SQLite DB."""
    mgr = WarehouseConnectionManager(custom_url="sqlite:///:memory:")
    init_warehouse_schema(mgr.engine)
    loader = WarehouseUpsertLoader(connection_manager=mgr, chunk_size=5)
    return loader


def test_upsert_initial_insert_customers(upsert_loader):
    """Tests initial insert of customers dataset."""
    customers_data = [
        {
            "unified_customer_id": "cust_1",
            "source_system": "stripe",
            "source_id": "cus_1",
            "email": "alice@example.com",
            "full_name": "Alice Smith",
            "company_name": "Acme Inc",
            "preferred_currency": "USD",
        },
        {
            "unified_customer_id": "cust_2",
            "source_system": "salesforce",
            "source_id": "sf_2",
            "email": "bob@example.com",
            "full_name": "Bob Jones",
            "company_name": "Tech Corp",
            "preferred_currency": "USD",
        },
    ]

    res = upsert_loader.upsert_records("dim_customers", customers_data)
    assert res["status"] == "SUCCESS"
    assert res["rows_processed"] == 2
    assert res["rows_inserted"] == 2
    assert res["rows_updated"] == 0

    # Query table directly
    with upsert_loader.conn_mgr.session_scope() as session:
        count = session.query(DimCustomer).count()
        assert count == 2


def test_upsert_idempotency_and_update_behavior(upsert_loader):
    """
    Tests that second run with modified attributes performs UPDATE without duplicating rows,
    and brand new records perform INSERT.
    """
    # Batch 1: Insert initial 2 records
    initial_batch = [
        {"unified_customer_id": "c1", "source_system": "stripe", "source_id": "s1", "email": "a@test.com", "full_name": "Old Name A"},
        {"unified_customer_id": "c2", "source_system": "stripe", "source_id": "s2", "email": "b@test.com", "full_name": "Old Name B"},
    ]
    upsert_loader.upsert_records("dim_customers", initial_batch)

    # Batch 2: Update c1 (new full_name), leave c2 unchanged, add brand new c3
    second_batch = [
        {"unified_customer_id": "c1", "source_system": "stripe", "source_id": "s1", "email": "a@test.com", "full_name": "Updated Name A"},
        {"unified_customer_id": "c2", "source_system": "stripe", "source_id": "s2", "email": "b@test.com", "full_name": "Old Name B"},
        {"unified_customer_id": "c3", "source_system": "salesforce", "source_id": "s3", "email": "c@test.com", "full_name": "New Name C"},
    ]
    res2 = upsert_loader.upsert_records("dim_customers", second_batch)

    assert res2["rows_processed"] == 3
    assert res2["rows_inserted"] == 1  # only c3 is new
    assert res2["rows_updated"] == 2   # c1 and c2 exist

    with upsert_loader.conn_mgr.session_scope() as session:
        # Total rows must be 3 (No duplicates!)
        assert session.query(DimCustomer).count() == 3
        
        # Verify c1 full_name was updated
        c1 = session.query(DimCustomer).filter_by(unified_customer_id="c1").first()
        assert c1.full_name == "Updated Name A"


def test_upsert_from_polars_dataframe(upsert_loader):
    """Tests loading data directly from a Polars DataFrame."""
    df = pl.DataFrame({
        "unified_transaction_id": ["tx_100", "tx_101"],
        "source_system": ["stripe", "stripe"],
        "source_transaction_id": ["ch_100", "ch_101"],
        "amount": [150.00, 299.50],
        "amount_refunded": [0.0, 50.0],
        "net_amount": [150.00, 249.50],
        "currency": ["USD", "USD"],
        "status": ["succeeded", "succeeded"],
        "transaction_date": [datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc), datetime(2026, 1, 16, 12, 0, 0, tzinfo=timezone.utc)],
    })

    res = upsert_loader.upsert_records("fact_transactions", df)
    assert res["status"] == "SUCCESS"
    assert res["rows_processed"] == 2
    assert res["rows_inserted"] == 2

    with upsert_loader.conn_mgr.session_scope() as session:
        assert session.query(FactTransaction).count() == 2
        tx = session.query(FactTransaction).filter_by(unified_transaction_id="tx_100").first()
        assert tx.amount == 150.00


def test_upsert_chunking_with_large_batch(upsert_loader):
    """Tests that chunking splits records correctly and loads all items."""
    # Create 25 records (with chunk_size=5 -> 5 chunks)
    batch = [
        {
            "unified_account_id": f"acc_{i}",
            "source_system": "salesforce",
            "source_account_id": f"sf_acc_{i}",
            "company_name": f"Enterprise Company {i}",
            "annual_revenue": float(i * 10000),
            "employee_count": i * 10,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        for i in range(25)
    ]

    res = upsert_loader.upsert_records("dim_companies", batch)
    assert res["rows_processed"] == 25
    assert res["rows_inserted"] == 25

    with upsert_loader.conn_mgr.session_scope() as session:
        assert session.query(DimCompany).count() == 25


def test_audit_log_recording(upsert_loader):
    """Verifies that load metrics are saved to etl_load_audit table."""
    data = [
        {
            "unified_subscription_id": "sub_audit_1",
            "source_system": "stripe",
            "source_subscription_id": "sub_1",
            "source_customer_id": "cus_1",
            "status": "active",
            "mrr_amount": 99.00,
            "currency": "USD",
            "created_at": datetime.now(timezone.utc),
        }
    ]

    upsert_loader.upsert_records("fact_subscriptions", data, batch_id="test_batch_001")

    with upsert_loader.conn_mgr.session_scope() as session:
        audit = session.query(ETLLoadAudit).filter_by(batch_id="test_batch_001").first()
        assert audit is not None
        assert audit.target_table == "fact_subscriptions"
        assert audit.rows_processed == 1
        assert audit.rows_inserted == 1
        assert audit.status == "SUCCESS"
