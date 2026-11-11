"""
Ingestion layer: loads raw + landing CSVs into Postgres staging.

Idempotent / incremental:
  * staging.ingested_files is a registry of files already loaded -> a rerun skips them,
    so history accumulates and nothing is ever loaded twice.
  * First run (empty registry, or empty staging) bootstraps: truncate + load everything.
  * Each file loads in ONE transaction (data + registry + metric), so a failure leaves no partial load.
  * Failures are logged as FAILED in pipeline_metrics_batch (real success-rate metric),
    then the task raises so Airflow shows it red.
"""
import os
import io
import glob
import time
import json
import pandas as pd
import psycopg2

BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
LANDING_DIR = os.path.join(BASE_DIR, "data", "landing")

DB_CONFIG = {
    "host": os.getenv("WAREHOUSE_HOST", "warehouse-db" if os.path.exists("/.dockerenv") else "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", 5432 if os.path.exists("/.dockerenv") else 5433)),
    "dbname": "staging",
    "user": "dwh",
    "password": "dwh123"
}

REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS staging.ingested_files (
    source_file  TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    rows_loaded  BIGINT NOT NULL,
    loaded_at    TIMESTAMP NOT NULL DEFAULT now()
);
"""

RULES_MASTER = {
    "Null MedicineID": lambda d: d['MedicineID'].notna(),
    "Non-positive wholesale price": lambda d: d['WholesalePriceUSD'] > 0
}
RULES_SALES = {
    "Null SalesTransactionID": lambda d: d['SalesTransactionID'].notna(),
    "Non-positive UnitRetailPriceUSD": lambda d: d['UnitRetailPriceUSD'] > 0
}
RULES_HOSP = {
    "Null HospitalDemandRecordID": lambda d: d['HospitalDemandRecordID'].notna(),
    "Negative Consumed Quantity": lambda d: d['ConsumedQuantityPacks'] >= 0
}


def log_metric(cur, filename, start_time, rows, status, error=None):
    end_time = time.time()
    cur.execute("""
        INSERT INTO staging.pipeline_metrics_batch
        (source_file, load_start, load_end, duration_seconds, rows_processed, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (filename, pd.to_datetime(start_time, unit='s'), pd.to_datetime(end_time, unit='s'),
          round(end_time - start_time, 4), rows, status, error))


def fast_copy(conn, df, table_name):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='\\N')
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY staging.{table_name} FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buffer)


def validate_and_reject(conn, df, source_name, id_col, rules):
    valid_mask = pd.Series(True, index=df.index)
    rejection_rows = []
    for rule_name, condition in rules.items():
        failed = ~condition(df)
        if failed.any():
            for idx in df[failed].index:
                rejection_rows.append((source_name, str(df.loc[idx, id_col]), rule_name,
                                       json.dumps(df.loc[idx].to_dict(), default=str)))
            valid_mask = valid_mask & (~failed)
    if rejection_rows:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO staging.stg_rejected_records
                (source_file, record_identifier, rejection_reason, raw_payload)
                VALUES (%s, %s, %s, %s);
            """, rejection_rows)
        print(f"    [!] Audited {len(rejection_rows)} rejected rows from {source_name}")
    return df[valid_mask].copy()


def build_job_list():
    """(path, staging table, id column, rules) in load order: master, sales (raw then batches), hospital."""
    jobs = [(os.path.join(RAW_DIR, "01_master_data.csv"), "stg_master_data", "MedicineID", RULES_MASTER)]
    for p in [os.path.join(RAW_DIR, "05_pharmacy_sales_data.csv")] + sorted(glob.glob(os.path.join(LANDING_DIR, "incremental_sales_*.csv"))):
        jobs.append((p, "stg_pharmacy_sales", "SalesTransactionID", RULES_SALES))
    for p in [os.path.join(RAW_DIR, "06_hospital_demand_data.csv")] + sorted(glob.glob(os.path.join(LANDING_DIR, "incremental_hosp_*.csv"))):
        jobs.append((p, "stg_hospital_demand", "HospitalDemandRecordID", RULES_HOSP))
    return jobs


def run_ingestion():
    print("=" * 65)
    print("INGESTION & VALIDATION PIPELINE (RAW + LANDING, INCREMENTAL)")
    print("=" * 65)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(REGISTRY_DDL)
    conn.commit()
    cur.execute("SELECT source_file FROM staging.ingested_files")
    loaded = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT count(*) FROM staging.stg_pharmacy_sales")
    sales_rows = cur.fetchone()[0]

    if not loaded or sales_rows == 0:
        print("Bootstrap: empty registry/staging -> clean full load")
        cur.execute("TRUNCATE TABLE staging.stg_master_data, staging.stg_pharmacy_sales, "
                    "staging.stg_hospital_demand, staging.stg_rejected_records CASCADE;")
        cur.execute("DELETE FROM staging.ingested_files;")
        conn.commit()
        loaded = set()

    new_files, skipped, failures = 0, 0, []
    for path, table, id_col, rules in build_job_list():
        fn = os.path.basename(path)
        if fn in loaded:
            skipped += 1
            print(f"Skipping {fn} (already loaded)")
            continue
        t0 = time.time()
        try:
            df = pd.read_csv(path)
            clean = validate_and_reject(conn, df, fn, id_col, rules)
            fast_copy(conn, clean, table)
            cur.execute("INSERT INTO staging.ingested_files (source_file, target_table, rows_loaded) VALUES (%s, %s, %s)",
                        (fn, table, len(clean)))
            log_metric(cur, fn, t0, len(clean), "SUCCESS")
            conn.commit()
            new_files += 1
            print(f"Loaded {fn}: {len(clean):,} valid rows -> staging.{table}")
        except Exception as exc:
            conn.rollback()
            log_metric(cur, fn, t0, 0, "FAILED", str(exc)[:500])
            conn.commit()
            failures.append(fn)
            print(f"[X] FAILED {fn}: {exc}")

    print(f"\nDone: {new_files} new file(s) loaded, {skipped} skipped.")
    for t in ("stg_master_data", "stg_pharmacy_sales", "stg_hospital_demand"):
        cur.execute(f"SELECT count(*) FROM staging.{t}")
        print(f"  staging.{t}: {cur.fetchone()[0]:,} rows")

    cur.close()
    conn.close()
    if failures:
        raise RuntimeError(f"Ingestion failed for: {', '.join(failures)}")


if __name__ == "__main__":
    run_ingestion()