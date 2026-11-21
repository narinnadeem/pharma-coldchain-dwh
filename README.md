# Pharma Cold Chain Data Warehouse (Phase 1, Milestone 1)

Course project for **Intelligent Data Architectures** (Fall 2026). Phase 1 builds the Sales and Demand Intelligence (SDI) data mart from raw files up to the analytical query layer, orchestrated by Apache Airflow, with Grafana data engineering dashboards.

## Group members
| Name | Roll no. | GitHub |
|------|----------|--------|
| Narin Nadeem | 29167 | [narinnadeem](https://github.com/narinnadeem) |
| Ayesha Salahuddin | 29157 | [ayeshasalahuddin](https://github.com/ayeshasalahuddin) |

## Milestone 1 deliverables
| Deliverable | Location |
|-------------|----------|
| Project report (PDF, 5 pages) | `Milestone.1.SDI.DWH.Report.pdf` |
| Excel file | `Milestone.1.SDI.DWH.Excel.xlsx` |
| Demo video (max 5 min) | `Milestone.1.SDI.DWH.Video.mp4` (or the link in `Milestone.1.SDI.DWH.Video.txt`) |
| Oversampling methodology note | `Milestone.1.SDI.DWH.Oversampling.pdf` |
| All code | `dags/`, `scripts/`, `sql/`, `grafana/`, `docker-compose.yaml`, `Dockerfile` |
| Oversampled data | `oversampled_data/` |
| Data dictionary | `data_dictionary/` |
| GitHub link file | `Milestone.1.SDI.DWH.GitHub.txt` |

## Architecture
`oversample.py` -> `data/landing` -> `ingest_staging.py` (validation, PostgreSQL staging) -> `build_datamart.py` (DuckDB star schema, SCD Type 2 on Dim_Medicine) -> `run_queries.py` (timed analytical queries, summary table) -> `collect_metrics.py` (metrics tables for Grafana).

Airflow DAG `phase1_sdi_pipeline`:
`synthesize_incremental_batches >> stage_and_validate_postgres >> transform_duckdb_star_schema >> run_analytical_and_storage_queries >> collect_pipeline_metrics`

Star schema diagram: `docs/star_schema_diagram.png`

## Repository layout
- `dags/` Airflow DAG (`phase1_sdi_pipeline.py`)
- `scripts/` pipeline scripts (`oversample.py`, `ingest_staging.py`, `build_datamart.py`, `run_queries.py`, `collect_metrics.py`) and `make_data_dictionary.py`
- `sql/` staging schema DDL
- `grafana/` Grafana setup and the three data engineering dashboards
- `docs/` star schema diagram, run logs, dashboard screenshots (`docs/dashboards/`)
- `oversampled_data/` the generated incremental batches (sales and hospital demand CSVs)
- `data_dictionary/` generated data dictionary (raw files, mart tables, pipeline metrics, query log)
- `data/` is NOT in the repo (large files). Put the three source CSVs in `data/raw/`.

## Setup (Windows, Docker Desktop with WSL 2)
1. Clone the repo and create `data/raw`, `data/landing`, `logs`, `config`, `plugins`.
2. Put `01_master_data.csv`, `05_pharmacy_sales_data.csv`, `06_hospital_demand_data.csv` into `data/raw/`.
3. Create `.env` with `AIRFLOW_UID=50000` and `FERNET_KEY=<your own key>`.
4. Make sure `AIRFLOW__API_AUTH__JWT_SECRET` is set in `docker-compose.yaml` so all Airflow services share one token secret.
5. Give Docker at least 5 GB RAM (`%USERPROFILE%\.wslconfig`: `memory=5GB`) and keep 20 GB free disk.
6. `docker compose up airflow-init`, then `docker compose up -d`. Wait a few minutes (packages install at startup).
7. Airflow UI: http://localhost:8080. Staging database: `localhost:5433`, db `staging`, user `dwh`. Grafana: http://localhost:3000.
8. Create the staging tables by running `sql/01_staging_schema.sql`.

## Run the pipeline
Trigger `phase1_sdi_pipeline` in the Airflow UI, or from the command line:
```
docker compose exec airflow-worker airflow dags test phase1_sdi_pipeline
```
Each run creates 2 new batches (10,000 sales rows and 756 hospital rows each), loads them, rebuilds the star schema, runs the queries and refreshes the dashboard metrics.

## Results (latest runs)
- All 5 tasks succeed from the Airflow UI (7 min 38 s on a 7.7 GB laptop; the star schema build takes about 80 s when memory is free).
- 580,000 sales rows and 33,264 hospital demand rows in both staging and the mart; 0 rows rejected by validation.
- Data covers 2023-01-01 to 2026-08-28 (44 year-months).
- 3 analytical queries: average 44.98 ms, slowest 106.63 ms (all sub-second).
- Grafana dashboards: DE 1 Batch Ingestion, DE 2 Storage, DE 3 Query Performance (screenshots in `docs/dashboards/` and the report appendix).

## Status and next steps
- Milestone 1 scope (raw data up to the query layer) is complete.
- Planned for Milestone 2: file-watcher trigger on `data/landing`, completed BI dashboards, two live refreshes in the demo.
- Phase 2 (Inventory, Expiry and Replenishment mart) and Phase 3 (Traceability and IoT) follow.
