import sys
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Set path for direct script imports inside the container
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='phase1_sdi_pipeline',
    default_args=default_args,
    description='End-to-End Pharma Cold Chain SDI Pipeline',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['pharma', 'coldchain', 'milestone1']
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

    t1 = synthesize_incremental_batches()
    t2 = stage_and_validate_postgres()
    t3 = transform_duckdb_star_schema()
    t4 = run_analytical_and_storage_queries()

    t1 >> t2 >> t3 >> t4

phase1_sdi_pipeline()