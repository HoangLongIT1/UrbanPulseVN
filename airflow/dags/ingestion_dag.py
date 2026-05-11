"""
UrbanPulse VN — Airflow DAG: Batch Ingestion Pipeline.

Schedules the ingestion pipeline to run daily at 06:00 ICT (23:00 UTC).
Extracts data from all 7 sources (APIs + crawlers) and loads to Bronze layer
(PostgreSQL + MinIO).

Trigger modes:
    - Scheduled: daily at 23:00 UTC
    - Manual: trigger with conf={"mode": "seed"} for historical backfill
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "urbanpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _determine_run_mode(**context) -> str:
    """Branch: return task_id based on trigger config (daily vs seed)."""
    conf = context["dag_run"].conf or {}
    mode = conf.get("mode", "daily")
    if mode == "seed":
        return "run_seed_ingestion"
    return "run_daily_ingestion"


def _run_ingestion(is_seed: bool = False) -> None:
    """Import and execute the IngestionPipeline."""
    import sys
    import os

    # Ensure project root is on the path when running inside Airflow container
    project_root = os.environ.get("URBANPULSE_ROOT", "/opt/airflow")
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from utils.helper import setup_logging
    from ingestion.pipeline import IngestionPipeline

    setup_logging()
    pipeline = IngestionPipeline()
    try:
        pipeline.run_all(is_seed=is_seed)
    finally:
        pipeline.close()


def _run_daily_ingestion() -> None:
    _run_ingestion(is_seed=False)


def _run_seed_ingestion() -> None:
    _run_ingestion(is_seed=True)


def _check_bronze_health(**context) -> None:
    """
    Lightweight sanity check: verify that at least one source loaded data
    into PostgreSQL bronze schema in the last 24 hours.
    Raises RuntimeError to fail the task if no data is found.
    """
    import os
    import psycopg2
    from datetime import timezone

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "urbanpulse-postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "urbanpulse"),
        user=os.environ.get("POSTGRES_USER", "urbanpulse"),
        password=os.environ.get("POSTGRES_PASSWORD", "urbanpulse_dev_2024"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM bronze.ingestion_log
                WHERE created_at >= NOW() - INTERVAL '25 hours'
                  AND status = 'SUCCESS'
                """
            )
            count = cur.fetchone()[0]
        if count == 0:
            raise RuntimeError(
                "Bronze health check FAILED: no successful ingestion runs in last 25h."
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="urbanpulse_ingestion",
    description="Daily batch ingestion from 7 data sources into Bronze layer",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 23 * * *",   # 06:00 ICT daily
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "urbanpulse"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="determine_run_mode",
        python_callable=_determine_run_mode,
    )

    daily_ingest = PythonOperator(
        task_id="run_daily_ingestion",
        python_callable=_run_daily_ingestion,
        execution_timeout=timedelta(hours=1),
    )

    seed_ingest = PythonOperator(
        task_id="run_seed_ingestion",
        python_callable=_run_seed_ingestion,
        execution_timeout=timedelta(hours=3),
    )

    health_check = PythonOperator(
        task_id="bronze_health_check",
        python_callable=_check_bronze_health,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # runs after either branch
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Pipeline
    start >> branch >> [daily_ingest, seed_ingest] >> health_check >> end
