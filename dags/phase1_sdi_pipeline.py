"""Phase 1 SDI pipeline (Airflow 3).

generate -> ingest + validate -> star schema -> analytical queries -> collect DE metrics
Each task only calls one function from scripts/, so the DAG file stays a thin orchestration layer.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="phase1_sdi_pipeline",
    default_args=default_args,
    description="End-to-end Pharma Cold Chain SDI pipeline (Phase 1)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pharma", "coldchain", "milestone1"],
)
def phase1_sdi_pipeline():

    @task
    def synthesize_incremental_batches():
        from scripts.oversample import generate_batches
        generate_batches(num_batches=2)

    @task
    def stage_and_validate_postgres():
        from scripts.ingest_staging import run_ingestion
        run_ingestion()

    @task
    def transform_duckdb_star_schema():
        from scripts.build_datamart import build_star_schema
        build_star_schema()

    @task
    def run_analytical_and_storage_queries():
        from scripts.run_queries import run_analytical_layer
        run_analytical_layer()

    @task
    def collect_pipeline_metrics():
        from scripts.collect_metrics import collect_all
        collect_all()

    (synthesize_incremental_batches()
     >> stage_and_validate_postgres()
     >> transform_duckdb_star_schema()
     >> run_analytical_and_storage_queries()
     >> collect_pipeline_metrics())


phase1_sdi_pipeline()
