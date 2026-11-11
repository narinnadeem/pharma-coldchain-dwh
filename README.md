# Pharma Cold Chain Data Warehouse (Phase 1, Milestone 1)

Course project for Intelligent Data Architectures. Phase 1 builds the Sales and Demand Intelligence (SDI) mart from raw files to an analytical query layer, orchestrated by Apache Airflow.

## Architecture
generate (oversample.py) -> ingest + validate (ingest_staging.py, PostgreSQL) -> star schema (build_datamart.py, DuckDB) -> queries (run_queries.py), orchestrated by the Airflow DAG `phase1_sdi_pipeline`.

See `docs/star_schema_diagram.png` for the data model and `docs/` for the reports.

## Repository layout
- `dags/` Airflow DAG
- `scripts/` the four pipeline scripts
- `sql/` staging schema DDL
- `docs/` diagram and reports
- `docker-compose.yaml` Airflow, Redis, Postgres and the warehouse database
- `data/` is NOT in the repo (large files). Put the three source CSVs in `data/raw/`.

## Setup (Windows, Docker Desktop with WSL 2)
1. Clone the repo, create `data/raw`, `data/landing`, `logs`, `config`, `plugins`.
2. Put `01_master_data.csv`, `05_pharmacy_sales_data.csv`, `06_hospital_demand_data.csv` into `data/raw/`.
3. Create `.env` with `AIRFLOW_UID=50000` and `FERNET_KEY=<your own key>`.
4. Give Docker at least 5 GB RAM (`%USERPROFILE%\.wslconfig`: memory=5GB) and keep 20 GB free disk.
5. `docker compose up airflow-init`, then `docker compose up -d`. Wait a few minutes (packages install at startup).
6. Airflow UI: http://localhost:8080. Staging database: localhost:5433, db `staging`, user `dwh`.
7. Create the staging tables by running `sql/01_staging_schema.sql`.

## Run the pipeline
Trigger `phase1_sdi_pipeline` in the Airflow UI, or run the scripts in order from the project root:
`python scripts\oversample.py`, `python scripts\ingest_staging.py`, `python scripts\build_datamart.py`, `python scripts\run_queries.py`

## Status
Scripts, star schema (with SCD Type 2) and queries work. Known open issue: the DAG's first task fails when triggered from the Airflow UI (it succeeds under `airflow tasks test`). Under investigation. File-watcher not built yet.
