r"""
Generate the data dictionary for the Milestone 1 submission.
Run from the project root (D:\pharma-coldchain-dwh):
    python scripts/make_data_dictionary.py
Output: data_dictionary/Milestone.1.SDI.DWH.DataDictionary.xlsx  (+ one CSV per sheet)
Sheets: raw_files (every column of every CSV), mart_tables (DuckDB), pipeline_metrics (Postgres, optional),
        query_log (DuckDB, optional).
"""
import os, glob
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "data_dictionary")
os.makedirs(OUT, exist_ok=True)

def describe_csv(path):
    df = pd.read_csv(path)
    rows = []
    for col in df.columns:
        s = df[col]
        non_null = s.dropna()
        example = str(non_null.iloc[0]) if len(non_null) else ""
        rng = ""
        if pd.api.types.is_numeric_dtype(s) and len(non_null):
            rng = f"{non_null.min()} .. {non_null.max()}"
        rows.append({
            "file": os.path.basename(path), "rows_in_file": len(df), "column": col, "dtype": str(s.dtype),
            "null_count": int(s.isna().sum()), "distinct_values": int(s.nunique()), "min_max": rng, "example": example,
            "description": "",  # fill in the business meaning (one short sentence per column)
        })
    return rows

raw_rows = []
for folder in ("data/raw", "data/landing"):
    for p in sorted(glob.glob(os.path.join(ROOT, folder, "*.csv"))):
        # landing has many batch files with the same columns: describe only the first of each type
        if folder == "data/landing" and any(r["file"].split("_batch")[0] == os.path.basename(p).split("_batch")[0] for r in raw_rows):
            continue
        raw_rows.extend(describe_csv(p))
raw_df = pd.DataFrame(raw_rows)

sheets = {"raw_files": raw_df}

try:
    import duckdb
    con = duckdb.connect(os.path.join(ROOT, "data", "warehouse.duckdb"), read_only=True)
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1").fetchall()
    mart = []
    for (t,) in tables:
        n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        for cid, name, typ, *_ in con.execute(f'PRAGMA table_info("{t}")').fetchall():
            mart.append({"table": t, "rows": n, "column": name, "type": typ, "description": ""})
    sheets["mart_tables"] = pd.DataFrame(mart)
    for cand in ("Query_Execution_Log", "query_execution_log"):
        try:
            sheets["query_log"] = con.execute(f'SELECT * FROM "{cand}"').fetchdf(); break
        except Exception:
            pass
    con.close()
except Exception as e:
    print("DuckDB part skipped:", e)

try:
    import psycopg2
    pg = psycopg2.connect(host="localhost", port=5433, dbname="staging", user="dwh", password="dwh123")
    sheets["pipeline_metrics"] = pd.read_sql("SELECT * FROM staging.pipeline_metrics_batch ORDER BY 1", pg)
    pg.close()
except Exception as e:
    print("Postgres part skipped (is warehouse-db running?):", e)

xlsx = os.path.join(OUT, "Milestone.1.SDI.DWH.DataDictionary.xlsx")
with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
    for name, df in sheets.items():
        df.to_excel(w, sheet_name=name, index=False)
        ws = w.sheets[name]
        for i, col in enumerate(df.columns, 1):
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(max(len(str(col)) + 4, 14), 45)
        ws.freeze_panes = "A2"
        df.to_csv(os.path.join(OUT, f"{name}.csv"), index=False)
print("Written:", xlsx)
for k, v in sheets.items(): print(f"  {k}: {len(v)} rows")
