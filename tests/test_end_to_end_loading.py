"""
End-to-End Multi-Week Integration Test Suite.
Tests full pipeline flow: API Extract -> S3 Stage -> Polars Clean & Map -> Warehouse Upsert -> Idempotency Verification.
"""

from pathlib import Path
import pytest
from sqlalchemy import select
from config.settings import AppSettings
from pipeline.runner import ETLPipelineRunner
from warehouse.schema import DimCompany, DimCustomer, ETLLoadAudit, FactSubscription, FactTransaction


@pytest.fixture
def test_runner(tmp_path):
    """Initializes an ETLPipelineRunner with isolated temporary local staging and in-memory SQLite DB."""
    settings = AppSettings()
    settings.storage.local_storage_path = str(tmp_path / "staging")
    settings.warehouse.db_type = "sqlite"
    settings.warehouse.sqlite_db_path = str(tmp_path / "test_warehouse.db")

    runner = ETLPipelineRunner(
        settings=settings,
        use_mock=True,
        custom_db_url=f"sqlite:///{tmp_path}/test_warehouse.db",
    )
    return runner


def test_full_pipeline_end_to_end_extraction_transformation_and_loading(test_runner):
    """
    Executes all 4 phases end-to-end:
    1. Extract Stripe & Salesforce mock data to local staging
    2. Polars clean & merge into unified canonical DataFrames
    3. Validate data quality
    4. Upsert canonical records into target Data Warehouse tables
    5. Verify tables are populated and audit logs exist
    6. Re-run pipeline to verify idempotency (0 duplicates, records updated)
    """
    # -------------------------------------------------------------
    # PASS 1: Initial Ingestion & Sync
    # -------------------------------------------------------------
    # 1. Week 1: Extract
    ext_results = test_runner.run_week1_extraction(sources=["all"])
    assert len(ext_results) == 8  # 4 Stripe + 4 Salesforce entities
    assert sum(r["total_records"] for r in ext_results.values()) > 0

    # 2. Week 2: Transform & Clean
    trans_results = test_runner.run_week2_transformation()
    assert trans_results["customers_count"] > 0
    assert trans_results["transactions_count"] > 0
    assert trans_results["subscriptions_count"] > 0
    assert trans_results["companies_count"] > 0

    # Verify quality gates passed
    for qr in trans_results["quality_reports"]:
        assert qr.passed_threshold is True

    # 3. Week 3: Upsert into Data Warehouse
    load_results = test_runner.run_week3_loading(trans_results, batch_id="e2e_pass_1")
    assert "dim_customers" in load_results
    assert "fact_transactions" in load_results
    assert "fact_subscriptions" in load_results
    assert "dim_companies" in load_results

    # Verify initial insert counts
    cust_res = load_results["dim_customers"]
    assert cust_res["rows_inserted"] == trans_results["customers_count"]
    assert cust_res["rows_updated"] == 0

    # Verify direct database table queries
    with test_runner.warehouse_conn_mgr.session_scope() as session:
        db_cust_count = session.query(DimCustomer).count()
        db_tx_count = session.query(FactTransaction).count()
        db_sub_count = session.query(FactSubscription).count()
        db_comp_count = session.query(DimCompany).count()
        db_audit_count = session.query(ETLLoadAudit).count()

        assert db_cust_count == trans_results["customers_count"]
        assert db_tx_count == trans_results["transactions_count"]
        assert db_sub_count == trans_results["subscriptions_count"]
        assert db_comp_count == trans_results["companies_count"]
        assert db_audit_count >= 4

    # -------------------------------------------------------------
    # PASS 2: Incremental Sync Idempotency Check
    # -------------------------------------------------------------
    # Re-running the upsert stage with the same data should update existing records without creating duplicates
    load_results_2 = test_runner.run_week3_loading(trans_results, batch_id="e2e_pass_2")
    cust_res_2 = load_results_2["dim_customers"]

    assert cust_res_2["rows_inserted"] == 0
    assert cust_res_2["rows_updated"] == trans_results["customers_count"]

    # Verify database table row counts remain EXACTLY the same (Zero duplicate records!)
    with test_runner.warehouse_conn_mgr.session_scope() as session:
        assert session.query(DimCustomer).count() == db_cust_count
        assert session.query(FactTransaction).count() == db_tx_count
        assert session.query(FactSubscription).count() == db_sub_count
        assert session.query(DimCompany).count() == db_comp_count
