"""
Unit Tests for Airflow DAGs and Task Callables.
"""

from unittest.mock import patch
import pytest
from dags.airflow_utils import airflow_task_failure_callback, airflow_task_success_callback
from dags.enterprise_etl_daily_dag import (
    task_data_quality_gate,
    task_extract_salesforce,
    task_extract_stripe,
    task_load_warehouse_upsert,
    task_notify_success,
    task_pre_flight_check,
    task_transform_and_clean,
)
from dags.enterprise_etl_hourly_incremental_dag import task_incremental_sync


def test_daily_dag_task_callables():
    """Validates the execution of individual task callable functions in the daily DAG."""
    # 1. Pre-flight check
    pre_res = task_pre_flight_check()
    assert pre_res["status"] == "HEALTHY"

    # 2. Extract Stripe
    ext_stripe_res = task_extract_stripe()
    assert "stripe_customers" in ext_stripe_res

    # 3. Extract Salesforce
    ext_sf_res = task_extract_salesforce()
    assert "salesforce_accounts" in ext_sf_res

    # 4. Transform & Clean
    trans_res = task_transform_and_clean()
    assert trans_res["customers_count"] > 0

    # 5. Data Quality Gate
    dq_res = task_data_quality_gate()
    assert dq_res["status"] == "PASSED"

    # 6. Load Warehouse Upsert
    load_res = task_load_warehouse_upsert()
    assert "dim_customers" in load_res
    assert load_res["dim_customers"]["status"] == "SUCCESS"

    # 7. Notify Success
    task_notify_success()


def test_hourly_dag_incremental_sync_callable():
    """Validates the hourly incremental sync task callable."""
    sync_res = task_incremental_sync()
    assert "extraction" in sync_res
    assert "loading" in sync_res
    assert sync_res["loading"]["dim_customers"]["status"] == "SUCCESS"


def test_airflow_callbacks():
    """Tests Airflow success and failure callback invocations."""
    mock_context = {
        "task_instance": None,
        "dag": None,
        "execution_date": "2026-09-11T00:00:00Z",
        "exception": ValueError("Simulated task error"),
    }
    # Call failure callback
    airflow_task_failure_callback(mock_context)

    # Call success callback
    airflow_task_success_callback(mock_context)
