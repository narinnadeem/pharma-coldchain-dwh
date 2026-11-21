# Pharma Cold Chain DWH + BI - Phase 1: Sales & Demand Intelligence (SDI)

Course: Intelligent Data Architectures, Fall 2026 | Milestone 1 (scope: raw data up to the analytical query layer)

## Group members
| Name | Roll no. | GitHub |
|------|----------|--------|
| Narin Nadeem | 29167 | [narinnadeem](https://github.com/narinnadeem) |
| Ayesha Salahuddin | 29157 | - |

## Milestone 1 deliverables
| Deliverable | Location |
|-------------|----------|
| Project report (PDF) | `Milestone.1.SDI.DWH.Report.pdf` |
| Excel file | `Milestone.1.SDI.DWH.Excel.xlsx` |
| Demo video (max 5 min) | `Milestone.1.SDI.DWH.Video` (see file or link in `Milestone.1.SDI.DWH.Video.txt`) |
| Oversampling methodology note | `Milestone.1.SDI.DWH.Oversampling.pdf` |
| All code (Airflow DAG, scripts, SQL) | `dags/`, `scripts/`, `sql/`, `docker-compose.yaml` |
| Oversampled data + data dictionary | `oversampled_data/`, `data_dictionary/` |

## Architecture
`oversample.py` -> `data/landing` -> `ingest_staging.py` -> PostgreSQL staging -> `build_datamart.py` -> DuckDB star schema (SCD Type 2 on Dim_Medicine) -> `run_queries.py` -> timed analytical queries + summary table.

Airflow DAG: `phase1_sdi_pipeline`
`synthesize_incremental_batches >> stage_and_validate_postgres >> transform_duckdb_star_schema >> run_analytical_and_storage_queries`

Star schema diagram: `docs/star_schema_diagram.png`

## How to run (Windows, Docker Desktop)
```powershell
cd D:\pharma-coldchain-dwh
docker compose up -d          # wait 3-5 minutes, containers install packages at startup
# Airflow UI: http://localhost:8080  (user: admin)
docker compose exec airflow-scheduler airflow dags test phase1_sdi_pipeline 2026-11-11
```
Raw files (`data/raw`) are provided by the instructor and are not stored in this repository (the sales file is 208 MB). Place them in `data/raw` before running.

## Key results (last full run)
- All 5 tasks SUCCESS from the Airflow UI (7 min 38 s)
- 580,000 sales rows and 33,264 hospital demand rows in both staging and the mart
- Analytical queries: avg 44.98 ms over 15 runs; DuckDB file 26.5 MB; Grafana dashboards DE 1-3
