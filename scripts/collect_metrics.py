"""Collects storage and query metrics into Postgres for the Grafana data engineering dashboards.

Runs as the last task of the DAG (or by hand: python scripts\\collect_metrics.py).
Each run adds a new snapshot, so the dashboards show how the warehouse grows with every refresh.
"""
import os
from datetime import datetime, timezone

import duckdb
import psycopg2

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse.duckdb")
IN_DOCKER = os.path.exists("/.dockerenv")
DB_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "warehouse-db" if IN_DOCKER else "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", 5432 if IN_DOCKER else 5433)),
    "dbname": "staging",
    "user": "dwh",
    "password": "dwh123",
}

STAGING_TABLES = ["stg_master_data", "stg_pharmacy_sales", "stg_hospital_demand", "stg_rejected_records"]
STAGING_DATES = [("stg_pharmacy_sales", "salesdate"), ("stg_hospital_demand", "demandmonth")]
MART_FACTS = ["Fact_Sales", "Fact_HospitalDemand"]

DDL = """
CREATE TABLE IF NOT EXISTS staging.metrics_storage (
    snapshot_ts TIMESTAMP NOT NULL, layer TEXT NOT NULL, table_name TEXT NOT NULL, row_count BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS staging.metrics_footprint (
    snapshot_ts TIMESTAMP NOT NULL, component TEXT NOT NULL, size_mb NUMERIC(12,2) NOT NULL);
CREATE TABLE IF NOT EXISTS staging.metrics_date_coverage (
    snapshot_ts TIMESTAMP NOT NULL, layer TEXT NOT NULL, table_name TEXT NOT NULL, date_column TEXT,
    min_date DATE, max_date DATE, span_days INT, year_months INT);
CREATE TABLE IF NOT EXISTS staging.metrics_query_log (
    log_id INT, query_name TEXT, execution_time_ms DOUBLE PRECISION, rows_returned BIGINT,
    executed_at TIMESTAMP, collected_at TIMESTAMP DEFAULT now(),
    CONSTRAINT uq_metrics_query_log UNIQUE (log_id, query_name, executed_at));
"""


def collect_all():
    print("=" * 65)
    print("COLLECTING STORAGE AND QUERY METRICS FOR THE DE DASHBOARDS")
    print("=" * 65)
    snap = datetime.now(timezone.utc).replace(tzinfo=None)
    pg = psycopg2.connect(**DB_CONFIG)
    cur = pg.cursor()
    cur.execute(DDL)
    pg.commit()

    storage_rows, coverage_rows = [], []

    # ---- staging layer (Postgres) ----
    for t in STAGING_TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM staging.{t}")
            storage_rows.append((snap, "staging", t, cur.fetchone()[0]))
        except Exception as exc:
            pg.rollback()
            print(f"  [skip] staging.{t}: {exc}")
    for t, col in STAGING_DATES:
        try:
            cur.execute(f"SELECT MIN({col}::date), MAX({col}::date), "
                        f"COUNT(DISTINCT to_char({col}::date, 'YYYY-MM')) FROM staging.{t}")
            mn, mx, ym = cur.fetchone()
            coverage_rows.append((snap, "staging", t, col, mn, mx, (mx - mn).days if mn and mx else None, ym))
        except Exception as exc:
            pg.rollback()
            print(f"  [skip] date coverage staging.{t}: {exc}")

    # ---- mart layer (DuckDB) ----
    query_rows = []
    duck_mb = None
    if os.path.exists(DUCKDB_PATH):
        duck_mb = os.path.getsize(DUCKDB_PATH) / 1048576
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            tables = [r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'").fetchall()]
            for t in tables:
                storage_rows.append((snap, "mart", t, con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]))
            for fact in MART_FACTS:
                if fact in tables:
                    mn, mx, ym = con.execute(
                        f"SELECT MIN(d.FullDate), MAX(d.FullDate), COUNT(DISTINCT d.Year * 100 + d.Month) "
                        f"FROM {fact} f JOIN Dim_Date d ON f.DateKey = d.DateKey").fetchone()
                    coverage_rows.append((snap, "mart", fact, "Dim_Date.FullDate", mn, mx,
                                          (mx - mn).days if mn and mx else None, ym))
            if "Query_Execution_Log" in tables:
                query_rows = con.execute(
                    "SELECT log_id, query_name, execution_time_ms, rows_returned, executed_at "
                    "FROM Query_Execution_Log").fetchall()
        finally:
            con.close()
    else:
        print(f"  [skip] DuckDB file not found: {DUCKDB_PATH}")

    # ---- footprint ----
    cur.execute("SELECT pg_database_size(current_database())")
    pg_mb = cur.fetchone()[0] / 1048576
    footprint = [(snap, "postgres_staging_db", round(pg_mb, 2))]
    if duck_mb is not None:
        footprint.append((snap, "duckdb_mart_file", round(duck_mb, 2)))

    # ---- write ----
    cur.executemany("INSERT INTO staging.metrics_storage VALUES (%s,%s,%s,%s)", storage_rows)
    cur.executemany("INSERT INTO staging.metrics_date_coverage VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", coverage_rows)
    cur.executemany("INSERT INTO staging.metrics_footprint VALUES (%s,%s,%s)", footprint)
    cur.executemany(
        "INSERT INTO staging.metrics_query_log (log_id, query_name, execution_time_ms, rows_returned, executed_at) "
        "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", query_rows)
    pg.commit()
    cur.close()
    pg.close()

    print(f"  table row counts recorded : {len(storage_rows)}")
    print(f"  date coverage rows        : {len(coverage_rows)}")
    print("  footprint (MB)            : " + ", ".join(f"{c}={s}" for _, c, s in footprint))
    print(f"  query log rows synced     : {len(query_rows)}")
    print("Done.")


if __name__ == "__main__":
    collect_all()
