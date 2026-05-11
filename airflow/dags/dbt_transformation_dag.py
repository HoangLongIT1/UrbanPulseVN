"""
UrbanPulse VN — Airflow DAG: dbt Transformation Pipeline.

Runs the dbt Bronze → Silver → Gold transformation after ingestion completes.
Triggered externally by the ingestion DAG or manually.

Steps:
    1. dbt seed   — load static CSV tables (cities, rivers, standards)
    2. dbt run    — build all 19 models (staging → intermediate → marts)
    3. dbt test   — run all 60 data quality tests
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Path to dbt project inside the Airflow worker container
DBT_PROJECT_DIR = "/opt/airflow/dbt_transform"
DBT_PROFILES_DIR = "/opt/airflow/dbt_transform"

# Shared env for all BashOperators
DBT_ENV = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    # Pass DB credentials from Airflow worker env into dbt
    "POSTGRES_HOST": "urbanpulse-postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "urbanpulse",
    "POSTGRES_USER": "urbanpulse",
    "POSTGRES_PASSWORD": "urbanpulse_dev_2024",
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="urbanpulse_dbt_transformation",
    description="dbt Bronze→Silver→Gold transformation (19 models, 60 tests)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,   # triggered externally or manually — not on a cron
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "silver", "gold", "transformation", "urbanpulse"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Step 1: Load static seed tables (cities, rivers, pollutant standards)
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt seed --profiles-dir $DBT_PROFILES_DIR --no-use-colors"
        ),
        env=DBT_ENV,
        execution_timeout=timedelta(minutes=10),
    )

    # Step 2: Build all models Bronze → Silver → Gold
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir $DBT_PROFILES_DIR --no-use-colors"
        ),
        env=DBT_ENV,
        execution_timeout=timedelta(minutes=30),
    )

    # Step 3: Run all 60 data quality tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir $DBT_PROFILES_DIR --no-use-colors"
        ),
        env=DBT_ENV,
        execution_timeout=timedelta(minutes=15),
    )

    end = EmptyOperator(task_id="end")

    # Pipeline: seed → run → test
    start >> dbt_seed >> dbt_run >> dbt_test >> end
