"""
Enterprise ETL Pipeline - Hourly Incremental Sync DAG.
Runs micro-batch extractions based on high-watermarks for rapid downstream synchronization.
"""

from datetime import datetime, timedelta
import logging
from typing import Any, Dict

from dags.airflow_utils import airflow_task_failure_callback, airflow_task_success_callback
from notifications.manager import get_alert_manager
from pipeline.runner import ETLPipelineRunner

logger = logging.getLogger("dags.enterprise_etl_hourly")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": airflow_task_failure_callback,
}


def task_incremental_sync(**context) -> Dict[str, Any]:
    """Executes high-watermark incremental sync across Stripe and Salesforce."""
    logger.info("Executing Hourly Incremental ETL Sync...")
    runner = ETLPipelineRunner(use_mock=True)
    
    # 1. Extraction (only new/modified since watermark)
    ext_results = runner.run_week1_extraction(sources=["all"])
    
    # 2. Polars transformation
    trans_results = runner.run_week2_transformation()
    
    # 3. Warehouse Upsert
    load_results = runner.run_week3_loading(trans_results)

    return {
        "extraction": ext_results,
        "loading": load_results,
    }


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    dag = DAG(
        dag_id="enterprise_etl_hourly_incremental_dag",
        default_args=default_args,
        description="Hourly Incremental Watermark ETL Sync to Data Warehouse",
        schedule_interval="0 * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["enterprise", "incremental", "hourly", "warehouse"],
    )

    with dag:
        sync_task = PythonOperator(
            task_id="incremental_sync_all_sources",
            python_callable=task_incremental_sync,
            on_success_callback=airflow_task_success_callback,
        )

except ImportError:
    dag = None
