"""
Airflow Utility Functions and Callbacks.
Provides failure alerting hooks, context adapters, and resilient fallback execution.
"""

import logging
from typing import Any, Dict, Optional

from notifications.manager import get_alert_manager

logger = logging.getLogger("dags.airflow_utils")


def airflow_task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Standard failure callback invoked by Airflow when a task instance fails.
    Dispatches rich alerts via Slack and Email.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "enterprise_etl_dag"
    execution_date = str(context.get("execution_date", ""))
    exception = context.get("exception")

    alert_mgr = get_alert_manager()
    alert_mgr.notify_pipeline_failure(
        pipeline_name=f"Airflow DAG: {dag_id}",
        error_message=f"Task '{task_id}' failed during execution for date {execution_date}",
        stage=task_id,
        exception=exception if isinstance(exception, Exception) else None,
        metrics={
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date,
            "try_number": str(ti.try_number if ti else 1),
        },
    )


def airflow_task_success_callback(context: Dict[str, Any]) -> None:
    """
    Success callback invoked upon successful DAG or Task completion.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "enterprise_etl_dag"
    logger.info(f"Airflow Task Succeeded: {dag_id}.{task_id}")
